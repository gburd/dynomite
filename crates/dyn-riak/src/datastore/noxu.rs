//! Bridge from [`crate`] to the in-process Noxu DB storage engine.
//!
//! `NoxuDatastore` opens a single [`noxu_db::Environment`] per process
//! and one default [`noxu_db::Database`] for the keyspace. Every put,
//! get and delete is auto-committed against that database.
//!
//! Three kinds of records share the database keyspace:
//!
//! * Primary K/V records under `K\0{bucket}\0{key}` -> value.
//! * Forward 2i records under
//!   `I\0{bucket}\0{index_name}\0<u32-be value-len>{value}{key}`
//!   -> empty body. The fixed-width length prefix on the value
//!   keeps prefix scans unambiguous when value bytes contain the
//!   structural separator.
//! * Reverse 2i records under `R\0{bucket}\0{key}` ->
//!   length-prefixed list of `(index_name, encoded_value)` pairs.
//!   The reverse mapping lets a delete or update find every forward
//!   index entry to clean up.
//!
//! The encoded value is fixed 8-byte big-endian for `_int` indexes
//! (so range scans iterate in numeric order) and the raw bytes for
//! `_bin` indexes. Index names ending in neither suffix are accepted
//! verbatim and treated as `_bin` for the purposes of range scans.

use std::path::Path;
use std::sync::{Arc, Mutex};

use dynomite::embed::hooks::{BoxFuture, Datastore, DatastoreError, Protocol};
use dynomite::msg::{Msg, MsgType};
use noxu_db::{
    Cursor, CursorConfig, Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    Get, NoxuError, OperationStatus,
};

/// Storage prefix for primary K/V records.
const PRIMARY_TAG: &[u8] = b"K\0";
/// Storage prefix for forward 2i records.
const FWD_TAG: &[u8] = b"I\0";
/// Storage prefix for reverse 2i records.
const REV_TAG: &[u8] = b"R\0";
/// Single-byte field separator used between bucket / index_name
/// segments. We forbid the byte from appearing in bucket names
/// and index names so prefix scans terminate cleanly.
const SEP: u8 = 0;
/// Suffix on integer index names. The trailing 8 bytes of the
/// raw byte values stored under such an index are interpreted
/// as a big-endian `u64`.
pub const INT_SUFFIX: &[u8] = b"_int";
/// Suffix on binary / string index names.
pub const BIN_SUFFIX: &[u8] = b"_bin";
/// Width of the encoded `_int` value in bytes.
const INT_ENCODED_WIDTH: usize = 8;

/// Errors produced by [`NoxuDatastore`] operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NoxuDatastoreError {
    /// Failed to open the environment, the database, or perform an
    /// operation on the database.
    #[error("noxu: {0}")]
    Noxu(#[from] NoxuError),
    /// Bucket or index_name contains the structural separator
    /// byte. The 2i path uses byte 0 to separate the bucket and
    /// index_name fields; bucket names and index names must not
    /// embed it.
    #[error("noxu: invalid name: {what} contains a NUL byte")]
    InvalidName {
        /// Which input was bad: `\"bucket\"` or `\"index_name\"`.
        what: &'static str,
    },
    /// An `_int` index value was not exactly 8 bytes wide on a
    /// path where the encoded value is required to be the
    /// fixed-width big-endian form.
    #[error("noxu: bad _int index value: expected 8 bytes, got {got}")]
    BadIntValue {
        /// Length of the offending value, in bytes.
        got: usize,
    },
    /// Reverse-index record was unreadable. Indicates corruption
    /// or a schema mismatch; the reverse map cannot be parsed,
    /// so the cleanup path cannot find the forward entries.
    #[error("noxu: corrupt reverse index entry for bucket={bucket:?}, key={key:?}")]
    CorruptReverse {
        /// Bucket name as bytes.
        bucket: Vec<u8>,
        /// Key name as bytes.
        key: Vec<u8>,
    },
}

/// Riak-shaped storage bridge backed by Noxu DB.
///
/// Construct with [`NoxuDatastore::open`] (production) or
/// [`NoxuDatastore::open_in`] (tests / examples). The struct is cheap
/// to clone via [`Arc`].
#[derive(Clone)]
pub struct NoxuDatastore {
    inner: Arc<Inner>,
}

struct Inner {
    /// The Noxu environment is held alive for the lifetime of the
    /// datastore so the database handle remains valid.
    _env: Mutex<Environment>,
    /// Default database used for every record kind (primary KV,
    /// forward 2i, reverse 2i). Distinct prefixes keep the three
    /// keyspaces disjoint.
    db: Mutex<Database>,
}

/// Encoded form of an index value the caller asked us to store.
#[derive(Clone, Debug)]
pub struct EncodedIndexValue {
    /// Bytes ready to write under the forward-2i prefix.
    pub bytes: Vec<u8>,
}

impl NoxuDatastore {
    /// Open or create a Noxu environment rooted at `path` and the
    /// default database used by the Riak bridge.
    ///
    /// The `path` directory must be writable. A fresh environment is
    /// created if `allow_create` semantics permit; otherwise the
    /// underlying [`NoxuError`] is surfaced unchanged.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    /// use dyn_riak::datastore::NoxuDatastore;
    /// let ds = NoxuDatastore::open(Path::new("/var/lib/dynomite/riak"))
    ///     .expect("open noxu environment");
    /// ds.put(b"hello", b"world").expect("put");
    /// assert_eq!(ds.get(b"hello").unwrap().as_deref(), Some(&b"world"[..]));
    /// ```
    pub fn open(path: &Path) -> Result<Self, NoxuDatastoreError> {
        Self::open_with_db_name(path, "riak.objects")
    }

    /// Variant of [`Self::open`] that lets the caller pick the
    /// database name. Used by tests that need disjoint environments
    /// in the same process.
    pub fn open_with_db_name(path: &Path, db_name: &str) -> Result<Self, NoxuDatastoreError> {
        let env_config = EnvironmentConfig::new(path.to_path_buf())
            .with_allow_create(true)
            .with_transactional(false);
        let env = Environment::open(env_config)?;

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(false);
        let db = env.open_database(None, db_name, &db_config)?;

        Ok(Self {
            inner: Arc::new(Inner {
                _env: Mutex::new(env),
                db: Mutex::new(db),
            }),
        })
    }

    /// Convenience constructor that opens the datastore inside an
    /// existing temp directory. Equivalent to [`Self::open`] but
    /// makes intent obvious in tests.
    pub fn open_in(dir: &Path) -> Result<Self, NoxuDatastoreError> {
        Self::open(dir)
    }

    /// Look up `key` against the raw keyspace. Returns `Ok(None)`
    /// for a missing key.
    ///
    /// Operations against the bucket/key pair go through
    /// [`Self::get_object`] / [`Self::put_object`] /
    /// [`Self::delete_object`]; this raw form is kept for the v0.0.1
    /// trampoline tests and the embedding examples that predate the
    /// 2i shape.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, NoxuDatastoreError> {
        let mut value = DatabaseEntry::new();
        let key_entry = DatabaseEntry::from_bytes(key);
        let db = self.lock_db();
        match db.get(None, &key_entry, &mut value)? {
            OperationStatus::Success => Ok(Some(value.data().to_vec())),
            OperationStatus::NotFound | OperationStatus::KeyExists => Ok(None),
        }
    }

    /// Insert or overwrite the value at `key` against the raw
    /// keyspace. See [`Self::get`].
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), NoxuDatastoreError> {
        let key_entry = DatabaseEntry::from_bytes(key);
        let value_entry = DatabaseEntry::from_bytes(value);
        let db = self.lock_db();
        db.put(None, &key_entry, &value_entry)?;
        Ok(())
    }

    /// Remove the value at `key` against the raw keyspace.
    /// Returns `Ok(false)` if the key was absent, `Ok(true)` if
    /// it was present and is now removed.
    pub fn delete(&self, key: &[u8]) -> Result<bool, NoxuDatastoreError> {
        let key_entry = DatabaseEntry::from_bytes(key);
        let db = self.lock_db();
        match db.delete(None, &key_entry)? {
            OperationStatus::Success => Ok(true),
            OperationStatus::NotFound | OperationStatus::KeyExists => Ok(false),
        }
    }

    /// Look up the object stored under `(bucket, key)` against
    /// the primary K/V layer. Returns `Ok(None)` for a missing
    /// object.
    pub fn get_object(
        &self,
        bucket: &[u8],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, NoxuDatastoreError> {
        validate_no_separator(bucket, "bucket")?;
        let storage_key = primary_key(bucket, key);
        let mut value = DatabaseEntry::new();
        let key_entry = DatabaseEntry::from_bytes(&storage_key);
        let db = self.lock_db();
        match db.get(None, &key_entry, &mut value)? {
            OperationStatus::Success => Ok(Some(value.data().to_vec())),
            OperationStatus::NotFound | OperationStatus::KeyExists => Ok(None),
        }
    }

    /// Insert or overwrite the object at `(bucket, key)` against
    /// the primary K/V layer, fanning the supplied `indexes`
    /// out into the forward / reverse 2i layers.
    ///
    /// `indexes` is a list of `(index_name, value)` entries;
    /// `index_name` selects the encoding (`_int` for big-endian
    /// integer values, anything else for raw bytes). On overwrite
    /// any forward 2i entries previously associated with
    /// `(bucket, key)` are removed before the new ones are
    /// written, so a sequence of puts produces the same final
    /// state as a delete-then-put.
    ///
    /// # Errors
    ///
    /// [`NoxuDatastoreError::InvalidName`] when `bucket` or any
    /// `index_name` contains a NUL byte.
    /// [`NoxuDatastoreError::BadIntValue`] when an `_int` index
    /// is supplied with a value that cannot be parsed as an
    /// 8-byte big-endian unsigned integer.
    pub fn put_object(
        &self,
        bucket: &[u8],
        key: &[u8],
        value: &[u8],
        indexes: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), NoxuDatastoreError> {
        validate_no_separator(bucket, "bucket")?;
        for (name, _) in indexes {
            validate_no_separator(name, "index_name")?;
        }
        // Encode every index value up front so a malformed `_int`
        // is rejected before the primary write happens.
        let encoded: Vec<(Vec<u8>, Vec<u8>)> = indexes
            .iter()
            .map(|(name, value)| {
                let enc = encode_index_value(name, value)?;
                Ok::<_, NoxuDatastoreError>((name.clone(), enc))
            })
            .collect::<Result<_, _>>()?;

        let db = self.lock_db();
        // Clean up any pre-existing forward index entries via the
        // reverse map.
        Self::clear_forward_for_locked(&db, bucket, key)?;
        // Primary K/V write.
        let primary = primary_key(bucket, key);
        db.put(
            None,
            &DatabaseEntry::from_bytes(&primary),
            &DatabaseEntry::from_bytes(value),
        )?;
        // Forward 2i writes.
        for (name, enc) in &encoded {
            let fk = forward_key(bucket, name, enc, key);
            db.put(
                None,
                &DatabaseEntry::from_bytes(&fk),
                &DatabaseEntry::from_bytes(b""),
            )?;
        }
        // Reverse 2i write (replace).
        let rk = reverse_key(bucket, key);
        if encoded.is_empty() {
            // No indexes -> drop any stale reverse entry.
            db.delete(None, &DatabaseEntry::from_bytes(&rk))?;
        } else {
            let rv = encode_reverse_value(&encoded);
            db.put(
                None,
                &DatabaseEntry::from_bytes(&rk),
                &DatabaseEntry::from_bytes(&rv),
            )?;
        }
        Ok(())
    }

    /// Remove the object at `(bucket, key)` from the primary
    /// K/V layer and clean up every forward / reverse 2i entry
    /// associated with it.
    ///
    /// Returns `Ok(false)` when neither the object nor any 2i
    /// entry existed; `Ok(true)` otherwise.
    pub fn delete_object(&self, bucket: &[u8], key: &[u8]) -> Result<bool, NoxuDatastoreError> {
        validate_no_separator(bucket, "bucket")?;
        let db = self.lock_db();
        let removed_2i = Self::clear_forward_for_locked(&db, bucket, key)?;
        let primary = primary_key(bucket, key);
        let removed_pri = match db.delete(None, &DatabaseEntry::from_bytes(&primary))? {
            OperationStatus::Success => true,
            OperationStatus::NotFound | OperationStatus::KeyExists => false,
        };
        Ok(removed_pri || removed_2i)
    }

    /// Equality query against the 2i layer.
    ///
    /// Returns the list of object keys whose `index_name` value
    /// equals `value`. The matched keys are returned in
    /// lexicographic order, mirroring how Riak's `pagination_sort`
    /// flag behaves with the default ordering.
    pub fn index_eq(
        &self,
        bucket: &[u8],
        index_name: &[u8],
        value: &[u8],
    ) -> Result<Vec<Vec<u8>>, NoxuDatastoreError> {
        validate_no_separator(bucket, "bucket")?;
        validate_no_separator(index_name, "index_name")?;
        let encoded = encode_index_value(index_name, value)?;
        let prefix = forward_prefix_with_value(bucket, index_name, &encoded);
        let db = self.lock_db();
        let mut results = Vec::new();
        scan_prefix(&db, &prefix, |full_key, _val| {
            // Everything after the prefix is the key bytes.
            let suffix = &full_key[prefix.len()..];
            results.push(suffix.to_vec());
            Ok(())
        })?;
        Ok(results)
    }

    /// Range query against the 2i layer.
    ///
    /// `range_min` and `range_max` are interpreted in the same
    /// encoding as values stored under the index: big-endian 8
    /// bytes for `_int` indexes, raw bytes for `_bin` (and any
    /// other) indexes. Both bounds are inclusive.
    pub fn index_range(
        &self,
        bucket: &[u8],
        index_name: &[u8],
        range_min: &[u8],
        range_max: &[u8],
    ) -> Result<Vec<Vec<u8>>, NoxuDatastoreError> {
        validate_no_separator(bucket, "bucket")?;
        validate_no_separator(index_name, "index_name")?;
        let min_enc = encode_index_value(index_name, range_min)?;
        let max_enc = encode_index_value(index_name, range_max)?;
        let prefix = forward_prefix(bucket, index_name);
        let db = self.lock_db();
        let mut results = Vec::new();
        scan_prefix(&db, &prefix, |full_key, _val| {
            // suffix = <u32-be value-len><value bytes><key bytes>
            let suffix = &full_key[prefix.len()..];
            if suffix.len() < 4 {
                return Ok(());
            }
            let mut len_bytes = [0u8; 4];
            len_bytes.copy_from_slice(&suffix[..4]);
            let value_len = u32::from_be_bytes(len_bytes) as usize;
            if suffix.len() < 4 + value_len {
                return Ok(());
            }
            let value_bytes = &suffix[4..4 + value_len];
            if value_bytes < min_enc.as_slice() || value_bytes > max_enc.as_slice() {
                return Ok(());
            }
            let key_bytes = &suffix[4 + value_len..];
            results.push(key_bytes.to_vec());
            Ok(())
        })?;
        Ok(results)
    }

    /// Internal: read a reverse-index record for `(bucket, key)`,
    /// then delete every forward index entry it references. Also
    /// removes the reverse record on success.
    ///
    /// Returns `true` when at least one forward / reverse entry
    /// was removed, `false` otherwise. Holds the database guard
    /// for the entire scan / delete sequence.
    fn clear_forward_for_locked(
        db: &Database,
        bucket: &[u8],
        key: &[u8],
    ) -> Result<bool, NoxuDatastoreError> {
        let rk = reverse_key(bucket, key);
        let mut value = DatabaseEntry::new();
        let key_entry = DatabaseEntry::from_bytes(&rk);
        match db.get(None, &key_entry, &mut value)? {
            OperationStatus::Success => {
                let pairs = decode_reverse_value(value.data()).ok_or_else(|| {
                    NoxuDatastoreError::CorruptReverse {
                        bucket: bucket.to_vec(),
                        key: key.to_vec(),
                    }
                })?;
                for (name, encoded) in &pairs {
                    let fk = forward_key(bucket, name, encoded, key);
                    db.delete(None, &DatabaseEntry::from_bytes(&fk))?;
                }
                db.delete(None, &DatabaseEntry::from_bytes(&rk))?;
                Ok(true)
            }
            OperationStatus::NotFound | OperationStatus::KeyExists => Ok(false),
        }
    }

    fn lock_db(&self) -> std::sync::MutexGuard<'_, Database> {
        // The Mutex is only contended by the datastore itself; a
        // poisoned guard means another caller paniced mid-op, in
        // which case the database state is undefined and the
        // process should not continue silently. Recovering the
        // guard preserves liveness so callers see the failure as
        // a NoxuError on their next operation rather than a
        // poison panic propagating out of an arbitrary public
        // method.
        self.inner
            .db
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Walk every primary K/V record in storage order,
    /// calling `f` once per `(bucket, key, value)` triple.
    ///
    /// Used by the AAE rebuild path to construct the merkle
    /// tree directly from the storage cursor instead of
    /// looping through the public `Datastore` API. Walking the
    /// underlying cursor in storage order is cache-friendlier
    /// than re-issuing one `riak_get` per key: each step is a
    /// single B-tree advance, no per-key MVCC handshake, and
    /// no per-key lock toggle.
    ///
    /// Forward 2i and reverse 2i records are skipped
    /// transparently; the callback only sees primary records.
    /// The callback returns `Result<(), NoxuDatastoreError>`
    /// so it can short-circuit on its own errors; the iteration
    /// stops at the first error and returns it to the caller.
    ///
    /// # Errors
    ///
    /// Surfaces the underlying [`NoxuError`] for the cursor
    /// open / advance / close steps, plus any error the
    /// caller's `f` returns.
    pub fn fold_primary<F>(&self, mut f: F) -> Result<(), NoxuDatastoreError>
    where
        F: FnMut(&[u8], &[u8], &[u8]) -> Result<(), NoxuDatastoreError>,
    {
        let db = self.lock_db();
        full_scan(&db, |full_key, value| {
            // The three record kinds share the database
            // keyspace. Filter for the primary prefix here
            // rather than seeding the cursor at the prefix:
            // SearchGte over a small prefix can interact
            // poorly with Noxu's BIN-prefix compression on
            // a freshly-rebalanced tree (debug-only assert
            // in noxu_tree::tree::compress_key). A full
            // first-to-last walk avoids the seek entirely.
            if !full_key.starts_with(PRIMARY_TAG) {
                return Ok(());
            }
            let suffix = &full_key[PRIMARY_TAG.len()..];
            let Some(sep_idx) = suffix.iter().position(|b| *b == SEP) else {
                // Malformed key; skip rather than abort the
                // sweep so a single bad record cannot stall
                // a full AAE rebuild.
                return Ok(());
            };
            let (bucket, rest) = suffix.split_at(sep_idx);
            // rest starts with the SEP byte; advance past it.
            let key = &rest[1..];
            f(bucket, key, value)
        })
    }
}

impl Datastore for NoxuDatastore {
    fn protocol(&self) -> Protocol {
        Protocol::Custom
    }

    fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
        // The Riak PBC server calls into [`Self::get_object`],
        // [`Self::put_object`], [`Self::delete_object`], and the
        // index queries directly. The Datastore trait surface is
        // a counter-only trampoline so the substrate's request
        // accounting still fires for embedders that route
        // requests through it.
        Box::pin(async move {
            let mut rsp = Msg::new(req.id(), MsgType::Unknown, false);
            rsp.set_parent_id(req.id());
            Ok(rsp)
        })
    }

    fn riak_get<'a>(
        &'a self,
        bucket: &'a [u8],
        key: &'a [u8],
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, DatastoreError>> {
        Box::pin(async move {
            self.get_object(bucket, key)
                .map_err(|e| DatastoreError::Backend(e.to_string()))
        })
    }

    fn riak_put<'a>(
        &'a self,
        bucket: &'a [u8],
        key: &'a [u8],
        value: &'a [u8],
        indexes: &'a [(Vec<u8>, Vec<u8>)],
    ) -> BoxFuture<'a, Result<(), DatastoreError>> {
        Box::pin(async move {
            self.put_object(bucket, key, value, indexes)
                .map_err(|e| DatastoreError::Backend(e.to_string()))
        })
    }

    fn riak_delete<'a>(
        &'a self,
        bucket: &'a [u8],
        key: &'a [u8],
    ) -> BoxFuture<'a, Result<bool, DatastoreError>> {
        Box::pin(async move {
            self.delete_object(bucket, key)
                .map_err(|e| DatastoreError::Backend(e.to_string()))
        })
    }

    fn riak_index_eq<'a>(
        &'a self,
        bucket: &'a [u8],
        index_name: &'a [u8],
        value: &'a [u8],
    ) -> BoxFuture<'a, Result<Vec<Vec<u8>>, DatastoreError>> {
        Box::pin(async move {
            self.index_eq(bucket, index_name, value)
                .map_err(|e| DatastoreError::Backend(e.to_string()))
        })
    }

    fn riak_index_range<'a>(
        &'a self,
        bucket: &'a [u8],
        index_name: &'a [u8],
        min: &'a [u8],
        max: &'a [u8],
    ) -> BoxFuture<'a, Result<Vec<Vec<u8>>, DatastoreError>> {
        Box::pin(async move {
            self.index_range(bucket, index_name, min, max)
                .map_err(|e| DatastoreError::Backend(e.to_string()))
        })
    }
}

// ---- key encoders ----

/// Encode a primary K/V key as `K\0{bucket}\0{key}`.
fn primary_key(bucket: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PRIMARY_TAG.len() + bucket.len() + 1 + key.len());
    out.extend_from_slice(PRIMARY_TAG);
    out.extend_from_slice(bucket);
    out.push(SEP);
    out.extend_from_slice(key);
    out
}

/// Encode a forward-2i prefix that covers every value for a
/// given `(bucket, index_name)` pair.
fn forward_prefix(bucket: &[u8], index_name: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(FWD_TAG.len() + bucket.len() + 1 + index_name.len() + 1);
    out.extend_from_slice(FWD_TAG);
    out.extend_from_slice(bucket);
    out.push(SEP);
    out.extend_from_slice(index_name);
    out.push(SEP);
    out
}

/// Encode a forward-2i prefix that covers every key for a given
/// `(bucket, index_name, encoded value)` triple. The trailing
/// bytes after the prefix are always the original object key.
fn forward_prefix_with_value(bucket: &[u8], index_name: &[u8], encoded_value: &[u8]) -> Vec<u8> {
    let mut out = forward_prefix(bucket, index_name);
    out.extend_from_slice(
        &u32::try_from(encoded_value.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    out.extend_from_slice(encoded_value);
    out
}

/// Encode the full forward-2i record key for a given object.
fn forward_key(bucket: &[u8], index_name: &[u8], encoded_value: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = forward_prefix_with_value(bucket, index_name, encoded_value);
    out.extend_from_slice(key);
    out
}

/// Encode a reverse-2i record key as `R\0{bucket}\0{key}`.
fn reverse_key(bucket: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(REV_TAG.len() + bucket.len() + 1 + key.len());
    out.extend_from_slice(REV_TAG);
    out.extend_from_slice(bucket);
    out.push(SEP);
    out.extend_from_slice(key);
    out
}

/// Serialise the list of `(index_name, encoded_value)` pairs
/// stored under a reverse-2i record. Format:
/// `<u32-be count>(<u32-be name-len>{name}<u32-be value-len>{value})*`.
fn encode_reverse_value(pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&u32::try_from(pairs.len()).unwrap_or(u32::MAX).to_be_bytes());
    for (name, value) in pairs {
        out.extend_from_slice(&u32::try_from(name.len()).unwrap_or(u32::MAX).to_be_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(&u32::try_from(value.len()).unwrap_or(u32::MAX).to_be_bytes());
        out.extend_from_slice(value);
    }
    out
}

/// Inverse of [`encode_reverse_value`]. Returns `None` on a
/// truncated buffer.
fn decode_reverse_value(buf: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut p = 0usize;
    let count = read_u32_be(buf, &mut p)?;
    let mut out = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let name_len = read_u32_be(buf, &mut p)? as usize;
        if p + name_len > buf.len() {
            return None;
        }
        let name = buf[p..p + name_len].to_vec();
        p += name_len;
        let value_len = read_u32_be(buf, &mut p)? as usize;
        if p + value_len > buf.len() {
            return None;
        }
        let value = buf[p..p + value_len].to_vec();
        p += value_len;
        out.push((name, value));
    }
    Some(out)
}

fn read_u32_be(buf: &[u8], p: &mut usize) -> Option<u32> {
    if *p + 4 > buf.len() {
        return None;
    }
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&buf[*p..*p + 4]);
    *p += 4;
    Some(u32::from_be_bytes(bytes))
}

/// Encode an index value for storage according to the index
/// name's suffix. Integer indexes (`_int`) are stored as 8 bytes
/// big-endian so range scans iterate in numeric order; binary
/// (`_bin`) and unsuffixed indexes are stored verbatim.
///
/// Accepts `_int` values supplied as either:
///
/// * an exactly 8-byte buffer (passed through verbatim), or
/// * an ASCII decimal representation parseable as `u64`
///   (typical Riak client usage), in which case the parsed
///   integer is re-encoded as big-endian.
fn encode_index_value(index_name: &[u8], value: &[u8]) -> Result<Vec<u8>, NoxuDatastoreError> {
    if name_is_int(index_name) {
        if value.len() == INT_ENCODED_WIDTH {
            return Ok(value.to_vec());
        }
        let s = std::str::from_utf8(value)
            .map_err(|_| NoxuDatastoreError::BadIntValue { got: value.len() })?;
        let parsed: u64 = s
            .parse()
            .map_err(|_| NoxuDatastoreError::BadIntValue { got: value.len() })?;
        Ok(parsed.to_be_bytes().to_vec())
    } else {
        Ok(value.to_vec())
    }
}

fn name_is_int(index_name: &[u8]) -> bool {
    index_name.ends_with(INT_SUFFIX)
}

fn validate_no_separator(value: &[u8], what: &'static str) -> Result<(), NoxuDatastoreError> {
    if value.contains(&SEP) {
        return Err(NoxuDatastoreError::InvalidName { what });
    }
    Ok(())
}

/// Iterate every record whose key starts with `prefix`, calling
/// `f` once per record. The iteration uses a `SearchGte` cursor
/// seeded at `prefix` and stops as soon as the cursor exits the
/// prefix or hits the end of the database.
fn scan_prefix<F>(db: &Database, prefix: &[u8], mut f: F) -> Result<(), NoxuDatastoreError>
where
    F: FnMut(&[u8], &[u8]) -> Result<(), NoxuDatastoreError>,
{
    let mut cursor: Cursor = db.open_cursor(None, Some(&CursorConfig::new()))?;
    let mut key = DatabaseEntry::from_bytes(prefix);
    let mut value = DatabaseEntry::new();
    let mut status = cursor.get(&mut key, &mut value, Get::SearchGte, None)?;
    while matches!(status, OperationStatus::Success) {
        let k = key.data();
        if !k.starts_with(prefix) {
            break;
        }
        f(k, value.data())?;
        // Advance to the next record.
        status = cursor.get(&mut key, &mut value, Get::Next, None)?;
    }
    let _ = cursor.close();
    Ok(())
}

/// Iterate every record in the database, in storage order,
/// calling `f` once per record. Used by [`NoxuDatastore::
/// fold_primary`]; the callback is responsible for the
/// per-record prefix filter.
///
/// We prefer this over [`scan_prefix`] for full-database walks
/// because the SearchGte seek that opens [`scan_prefix`] can
/// trip a debug-only assert in `noxu-tree`'s BIN-prefix
/// compression on certain tree shapes. A bare `Get::First`
/// followed by `Get::Next` advances avoids the seek path.
fn full_scan<F>(db: &Database, mut f: F) -> Result<(), NoxuDatastoreError>
where
    F: FnMut(&[u8], &[u8]) -> Result<(), NoxuDatastoreError>,
{
    let mut cursor: Cursor = db.open_cursor(None, Some(&CursorConfig::new()))?;
    let mut key = DatabaseEntry::new();
    let mut value = DatabaseEntry::new();
    let mut status = cursor.get(&mut key, &mut value, Get::First, None)?;
    while matches!(status, OperationStatus::Success) {
        f(key.data(), value.data())?;
        status = cursor.get(&mut key, &mut value, Get::Next, None)?;
    }
    let _ = cursor.close();
    Ok(())
}

// Re-export the encoding helper so the integration test can
// construct expected wire-form values without re-deriving the
// schema.
#[doc(hidden)]
pub fn encode_index_value_for_test(
    index_name: &[u8],
    value: &[u8],
) -> Result<Vec<u8>, NoxuDatastoreError> {
    encode_index_value(index_name, value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn put_get_delete_round_trips() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");

        ds.put(b"alpha", b"one").expect("put alpha");
        ds.put(b"beta", b"two").expect("put beta");

        assert_eq!(ds.get(b"alpha").unwrap().as_deref(), Some(&b"one"[..]));
        assert_eq!(ds.get(b"beta").unwrap().as_deref(), Some(&b"two"[..]));
        assert_eq!(ds.get(b"missing").unwrap(), None);

        assert!(ds.delete(b"alpha").unwrap());
        assert!(!ds.delete(b"alpha").unwrap());
        assert_eq!(ds.get(b"alpha").unwrap(), None);
        assert_eq!(ds.get(b"beta").unwrap().as_deref(), Some(&b"two"[..]));
    }

    #[test]
    fn put_overwrites_existing_value() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");

        ds.put(b"k", b"v1").expect("put v1");
        ds.put(b"k", b"v2").expect("put v2");
        assert_eq!(ds.get(b"k").unwrap().as_deref(), Some(&b"v2"[..]));
    }

    #[tokio::test]
    async fn datastore_dispatch_is_a_trampoline() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        let req = Msg::new(7, MsgType::Unknown, true);
        let rsp = <NoxuDatastore as Datastore>::dispatch(&ds, req)
            .await
            .expect("dispatch");
        assert_eq!(rsp.parent_id(), 7);
    }

    #[test]
    fn put_object_round_trips() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        ds.put_object(b"users", b"alice", b"hello", &[])
            .expect("put");
        assert_eq!(
            ds.get_object(b"users", b"alice").unwrap().as_deref(),
            Some(&b"hello"[..])
        );
        assert!(ds.get_object(b"users", b"bob").unwrap().is_none());
        assert!(ds.delete_object(b"users", b"alice").unwrap());
        assert!(ds.get_object(b"users", b"alice").unwrap().is_none());
    }

    #[test]
    fn index_eq_returns_matching_keys() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        ds.put_object(
            b"users",
            b"alice",
            b"v1",
            &[(b"age_int".to_vec(), b"42".to_vec())],
        )
        .expect("put alice");
        ds.put_object(
            b"users",
            b"bob",
            b"v2",
            &[(b"age_int".to_vec(), b"42".to_vec())],
        )
        .expect("put bob");
        ds.put_object(
            b"users",
            b"carol",
            b"v3",
            &[(b"age_int".to_vec(), b"99".to_vec())],
        )
        .expect("put carol");

        let mut hits = ds.index_eq(b"users", b"age_int", b"42").unwrap();
        hits.sort();
        assert_eq!(hits, vec![b"alice".to_vec(), b"bob".to_vec()]);

        let hits99 = ds.index_eq(b"users", b"age_int", b"99").unwrap();
        assert_eq!(hits99, vec![b"carol".to_vec()]);

        let none = ds.index_eq(b"users", b"age_int", b"7").unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn index_eq_handles_bin_indexes() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        ds.put_object(
            b"users",
            b"alice",
            b"v1",
            &[(b"city_bin".to_vec(), b"seattle".to_vec())],
        )
        .unwrap();
        ds.put_object(
            b"users",
            b"bob",
            b"v2",
            &[(b"city_bin".to_vec(), b"portland".to_vec())],
        )
        .unwrap();

        let hits = ds.index_eq(b"users", b"city_bin", b"seattle").unwrap();
        assert_eq!(hits, vec![b"alice".to_vec()]);
    }

    #[test]
    fn index_range_returns_keys_in_range() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        for (key, age) in [
            (&b"alice"[..], "10"),
            (&b"bob"[..], "15"),
            (&b"carol"[..], "20"),
            (&b"dave"[..], "25"),
            (&b"erin"[..], "30"),
        ] {
            ds.put_object(
                b"users",
                key,
                b"v",
                &[(b"age_int".to_vec(), age.as_bytes().to_vec())],
            )
            .unwrap();
        }
        let mut hits = ds.index_range(b"users", b"age_int", b"15", b"25").unwrap();
        hits.sort();
        assert_eq!(
            hits,
            vec![b"bob".to_vec(), b"carol".to_vec(), b"dave".to_vec()]
        );
    }

    #[test]
    fn delete_object_clears_2i_entries() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        ds.put_object(
            b"users",
            b"alice",
            b"v",
            &[
                (b"age_int".to_vec(), b"42".to_vec()),
                (b"city_bin".to_vec(), b"seattle".to_vec()),
            ],
        )
        .unwrap();
        assert_eq!(
            ds.index_eq(b"users", b"age_int", b"42").unwrap(),
            vec![b"alice".to_vec()]
        );

        assert!(ds.delete_object(b"users", b"alice").unwrap());
        assert!(ds.index_eq(b"users", b"age_int", b"42").unwrap().is_empty());
        assert!(ds
            .index_eq(b"users", b"city_bin", b"seattle")
            .unwrap()
            .is_empty());
        // Reverse map should be empty too.
        assert!(ds.get_object(b"users", b"alice").unwrap().is_none());
    }

    #[test]
    fn update_replaces_index_entries() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        ds.put_object(
            b"users",
            b"alice",
            b"v1",
            &[(b"age_int".to_vec(), b"30".to_vec())],
        )
        .unwrap();
        ds.put_object(
            b"users",
            b"alice",
            b"v2",
            &[(b"age_int".to_vec(), b"31".to_vec())],
        )
        .unwrap();
        assert!(ds.index_eq(b"users", b"age_int", b"30").unwrap().is_empty());
        assert_eq!(
            ds.index_eq(b"users", b"age_int", b"31").unwrap(),
            vec![b"alice".to_vec()]
        );
    }

    #[test]
    fn invalid_bucket_with_separator_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        let bad = vec![b'b', 0u8, b'a', b'd'];
        let err = ds.put_object(&bad, b"k", b"v", &[]);
        assert!(matches!(
            err,
            Err(NoxuDatastoreError::InvalidName { what: "bucket" })
        ));
    }

    #[test]
    fn bad_int_value_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        let err = ds.put_object(
            b"users",
            b"alice",
            b"v",
            &[(b"age_int".to_vec(), b"not-a-number".to_vec())],
        );
        assert!(matches!(err, Err(NoxuDatastoreError::BadIntValue { .. })));
    }

    #[test]
    fn reverse_codec_round_trip() {
        let pairs = vec![
            (b"a_int".to_vec(), 42u64.to_be_bytes().to_vec()),
            (b"b_bin".to_vec(), b"hello".to_vec()),
        ];
        let buf = encode_reverse_value(&pairs);
        assert_eq!(decode_reverse_value(&buf), Some(pairs));
        // Empty buffer is None.
        assert_eq!(decode_reverse_value(&[]), None);
    }
}
