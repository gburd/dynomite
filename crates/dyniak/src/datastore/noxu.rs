//! Bridge from [`crate`] to the in-process Noxu DB storage engine.
//!
//! `NoxuDatastore` opens a single [`noxu::Environment`] per process
//! and one default [`noxu::Database`] for the keyspace. Every put,
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
use noxu::{
    Cursor, CursorConfig, Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    Get, NoxuError, OperationStatus, Transaction,
};

use crate::txn::{TransactionalStore, TxnBatch, TxnOp, TxnOutcome, TxnStoreError};

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
    /// A transaction was requested against a datastore that was not
    /// opened with [`NoxuDatastore::open_transactional`]. The
    /// auto-commit get/put/delete paths work on any datastore, but
    /// the multi-key transaction API needs a transactional
    /// environment.
    #[error("noxu: datastore is not transactional; open with open_transactional")]
    NotTransactional,
    /// An X/Open XA resource-manager call failed. Carries the engine's
    /// own message. Surfaced by the cross-node coordinator in
    /// [`crate::datastore::xa`].
    #[error("noxu xa: {0}")]
    Xa(String),
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
    /// datastore so the database handle remains valid, and is the
    /// source of [`Transaction`] handles for the multi-key
    /// transaction API.
    env: Mutex<Environment>,
    /// Default database used for every record kind (primary KV,
    /// forward 2i, reverse 2i). Distinct prefixes keep the three
    /// keyspaces disjoint.
    db: Mutex<Database>,
    /// Whether the environment and database were opened with
    /// transactions enabled. [`NoxuDatastore::transaction`] requires
    /// this; the auto-commit get/put/delete paths work either way.
    transactional: bool,
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
    /// use dyniak::datastore::NoxuDatastore;
    /// let ds = NoxuDatastore::open(Path::new("/var/lib/dynomite/riak"))
    ///     .expect("open noxu environment");
    /// ds.put(b"hello", b"world").expect("put");
    /// assert_eq!(ds.get(b"hello").unwrap().as_deref(), Some(&b"world"[..]));
    /// ```
    pub fn open(path: &Path) -> Result<Self, NoxuDatastoreError> {
        Self::open_with_db_name(path, "riak.objects")
    }

    /// Open or create a transactional Noxu environment rooted at
    /// `path` and the default database used by the Riak bridge.
    ///
    /// Identical to [`Self::open`] except the environment and the
    /// database are opened with transactions enabled. The
    /// `data_store: dyniak` production path uses this constructor
    /// so the later cross-key XA work has a transactional
    /// environment to build on.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    /// use dyniak::datastore::NoxuDatastore;
    /// let ds = NoxuDatastore::open_transactional(Path::new("/var/lib/dynomite/riak"))
    ///     .expect("open transactional noxu environment");
    /// ds.put(b"hello", b"world").expect("put");
    /// ```
    pub fn open_transactional(path: &Path) -> Result<Self, NoxuDatastoreError> {
        Self::open_with_options(path, "riak.objects", true)
    }

    /// Variant of [`Self::open`] that lets the caller pick the
    /// database name. Used by tests that need disjoint environments
    /// in the same process.
    pub fn open_with_db_name(path: &Path, db_name: &str) -> Result<Self, NoxuDatastoreError> {
        Self::open_with_options(path, db_name, false)
    }

    /// Open the environment and the default database, selecting
    /// whether the environment and database are transactional.
    ///
    /// `transactional == false` reproduces the historical
    /// auto-commit behaviour used by the tests and embedding
    /// examples; `transactional == true` is the
    /// `data_store: dyniak` production path.
    fn open_with_options(
        path: &Path,
        db_name: &str,
        transactional: bool,
    ) -> Result<Self, NoxuDatastoreError> {
        let env_config = EnvironmentConfig::new(path.to_path_buf())
            .with_allow_create(true)
            .with_transactional(transactional);
        let env = Environment::open(env_config)?;

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(transactional);
        let db = env.open_database(None, db_name, &db_config)?;

        Ok(Self {
            inner: Arc::new(Inner {
                env: Mutex::new(env),
                db: Mutex::new(db),
                transactional,
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
            OperationStatus::NotFound | OperationStatus::KeyExists | OperationStatus::KeyEmpty => {
                Ok(None)
            }
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
            OperationStatus::NotFound | OperationStatus::KeyExists | OperationStatus::KeyEmpty => {
                Ok(false)
            }
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
        let db = self.lock_db();
        get_object_in(&db, None, bucket, key)
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
        let db = self.lock_db();
        put_object_in(&db, None, bucket, key, value, indexes)
    }

    /// Remove the object at `(bucket, key)` from the primary
    /// K/V layer and clean up every forward / reverse 2i entry
    /// associated with it.
    ///
    /// Returns `Ok(false)` when neither the object nor any 2i
    /// entry existed; `Ok(true)` otherwise.
    pub fn delete_object(&self, bucket: &[u8], key: &[u8]) -> Result<bool, NoxuDatastoreError> {
        let db = self.lock_db();
        delete_object_in(&db, None, bucket, key)
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
        scan_prefix(&db, None, &prefix, |full_key, _val| {
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
        scan_prefix(&db, None, &prefix, |full_key, _val| {
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
        // Seek straight to the primary-tag prefix; everything
        // before it is bookkeeping (forward / reverse 2i
        // records) which the AAE rebuild does not need.
        // Earlier versions of Noxu (pre-v1.5.0) panicked in
        // `noxu_tree::tree::compress_key` when SearchGte was
        // seeded at a 2-byte prefix on a many-key tree;
        // commit `d76b755` fixed that, so the cursor seek is
        // safe again.
        scan_prefix(&db, None, PRIMARY_TAG, |full_key, value| {
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

    fn lock_env(&self) -> std::sync::MutexGuard<'_, Environment> {
        // Same poison-recovery rationale as [`Self::lock_db`]: a
        // poisoned environment guard means a prior caller paniced
        // mid-operation; recovering the guard keeps the datastore
        // live so the failure surfaces as a NoxuError on the next
        // operation rather than a poison panic from an unrelated
        // call site.
        self.inner
            .env
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Run `f` inside a single Noxu transaction, committing on
    /// `Ok` and rolling back on `Err` (or on panic, via the
    /// transaction's `Drop`).
    ///
    /// Every put / delete issued through the [`NoxuTxn`] handle --
    /// including the multi-record primary + forward-2i +
    /// reverse-2i fan-out of a single `put_object` -- participates
    /// in the one transaction, so a batch of object writes is
    /// applied atomically: either all records land or none do.
    ///
    /// The closure's error type `E` is generic so callers can carry
    /// their own abort signal; any [`NoxuDatastoreError`] raised
    /// while beginning or committing the transaction is converted
    /// into `E` via [`From`].
    ///
    /// # Errors
    ///
    /// Returns [`NoxuDatastoreError::NotTransactional`] (lifted into
    /// `E`) when the datastore was not opened with
    /// [`Self::open_transactional`]. Propagates the closure's error
    /// unchanged after rolling back, and surfaces any commit error.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    /// use dyniak::datastore::{NoxuDatastore, NoxuDatastoreError};
    /// let ds = NoxuDatastore::open_transactional(Path::new("/var/lib/dyn/riak"))
    ///     .expect("open transactional");
    /// ds.transaction(|tx| {
    ///     tx.put_object(b"users", b"alice", b"v1", &[])?;
    ///     tx.delete_object(b"users", b"bob")?;
    ///     Ok::<_, NoxuDatastoreError>(())
    /// })
    /// .expect("commit");
    /// ```
    pub fn transaction<F, R, E>(&self, f: F) -> Result<R, E>
    where
        F: FnOnce(&NoxuTxn<'_>) -> Result<R, E>,
        E: From<NoxuDatastoreError>,
    {
        if !self.inner.transactional {
            return Err(E::from(NoxuDatastoreError::NotTransactional));
        }
        // Begin the transaction while holding the environment guard,
        // then release it: the returned `Transaction` owns its own
        // handle to the engine and does not borrow the environment,
        // so the commit / abort below works without the env lock.
        let txn = {
            let env = self.lock_env();
            env.begin_transaction(None)
                .map_err(NoxuDatastoreError::from)?
        };
        // Hold the database guard for the whole transaction so the
        // multi-record fan-out of each op is serialized against the
        // auto-commit path and any other transaction on this
        // datastore.
        let db = self.lock_db();
        let handle = NoxuTxn { db: &db, txn: &txn };
        match f(&handle) {
            Ok(value) => {
                txn.commit().map_err(NoxuDatastoreError::from)?;
                Ok(value)
            }
            Err(err) => {
                // Best-effort rollback; the original closure error is
                // the one the caller cares about, so an abort failure
                // is swallowed (the Transaction's Drop also aborts).
                let _ = txn.abort();
                Err(err)
            }
        }
    }
}

/// Transaction handle handed to the closure passed to
/// [`NoxuDatastore::transaction`].
///
/// Every method mirrors a [`NoxuDatastore`] auto-commit method but
/// performs its work inside the enclosing transaction, so the whole
/// closure commits or rolls back as a unit. The handle borrows the
/// datastore's locked database and the open transaction; it cannot
/// outlive the closure.
pub struct NoxuTxn<'a> {
    db: &'a Database,
    txn: &'a Transaction,
}

impl NoxuTxn<'_> {
    /// Transactional [`NoxuDatastore::put_object`].
    ///
    /// # Errors
    ///
    /// Same error contract as [`NoxuDatastore::put_object`].
    pub fn put_object(
        &self,
        bucket: &[u8],
        key: &[u8],
        value: &[u8],
        indexes: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), NoxuDatastoreError> {
        put_object_in(self.db, Some(self.txn), bucket, key, value, indexes)
    }

    /// Transactional [`NoxuDatastore::delete_object`].
    ///
    /// # Errors
    ///
    /// Same error contract as [`NoxuDatastore::delete_object`].
    pub fn delete_object(&self, bucket: &[u8], key: &[u8]) -> Result<bool, NoxuDatastoreError> {
        delete_object_in(self.db, Some(self.txn), bucket, key)
    }

    /// Transactional [`NoxuDatastore::get_object`]. Reads observe
    /// the transaction's own uncommitted writes.
    ///
    /// # Errors
    ///
    /// Same error contract as [`NoxuDatastore::get_object`].
    pub fn get_object(
        &self,
        bucket: &[u8],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, NoxuDatastoreError> {
        get_object_in(self.db, Some(self.txn), bucket, key)
    }

    /// Apply one [`TxnOp`] inside the transaction.
    ///
    /// # Errors
    ///
    /// Surfaces the per-op error contract of
    /// [`Self::put_object`] / [`Self::delete_object`].
    pub fn apply(&self, op: &TxnOp) -> Result<(), NoxuDatastoreError> {
        match op {
            TxnOp::Put {
                bucket,
                key,
                value,
                indexes,
            } => self.put_object(bucket, key, value, indexes),
            TxnOp::Delete { bucket, key } => self.delete_object(bucket, key).map(|_| ()),
        }
    }
}

impl Datastore for NoxuDatastore {
    fn protocol(&self) -> Protocol {
        Protocol::Custom
    }

    fn as_any(&self) -> Option<&(dyn std::any::Any + 'static)> {
        Some(self)
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

/// Map a [`NoxuDatastoreError`] onto the wire-facing
/// [`TxnStoreError`], routing engine-reported serialization
/// failures (deadlock / lock-timeout) into
/// [`TxnStoreError::Conflict`] so a client can retry.
pub(crate) fn map_txn_store_error(err: &NoxuDatastoreError) -> TxnStoreError {
    if let NoxuDatastoreError::Noxu(inner) = err {
        let text = inner.to_string();
        let lowered = text.to_ascii_lowercase();
        if lowered.contains("deadlock")
            || lowered.contains("lock timeout")
            || lowered.contains("locktimeout")
            || lowered.contains("conflict")
            || lowered.contains("would block")
        {
            return TxnStoreError::Conflict(text);
        }
    }
    TxnStoreError::Backend(err.to_string())
}

/// Internal error carried out of the [`TransactionalStore::execute_batch`]
/// closure so a deliberate abort (`force_abort`) and a genuine engine
/// error stay distinguishable on the way out.
enum BatchErr {
    /// The caller asked the batch to roll back after applying.
    Abort,
    /// The engine rejected an operation.
    Store(NoxuDatastoreError),
}

impl From<NoxuDatastoreError> for BatchErr {
    fn from(e: NoxuDatastoreError) -> Self {
        Self::Store(e)
    }
}

impl TransactionalStore for NoxuDatastore {
    fn execute_batch(&self, batch: &TxnBatch) -> Result<TxnOutcome, TxnStoreError> {
        if batch.ops.is_empty() {
            return Err(TxnStoreError::EmptyBatch);
        }

        let force_abort = batch.force_abort;
        let result: Result<(), BatchErr> = self.transaction(|tx| {
            for op in &batch.ops {
                tx.apply(op).map_err(BatchErr::Store)?;
            }
            if force_abort {
                return Err(BatchErr::Abort);
            }
            Ok(())
        });

        match result {
            Ok(()) => Ok(TxnOutcome::Committed {
                operations: batch.ops.len(),
            }),
            Err(BatchErr::Abort) => Ok(TxnOutcome::Aborted {
                reason: "client requested abort".to_string(),
            }),
            Err(BatchErr::Store(e)) => Err(map_txn_store_error(&e)),
        }
    }
}

// ---- record operations (auto-commit or transactional) ----
//
// These take an `Option<&Transaction>`: `None` reproduces the
// historical auto-commit behaviour; `Some(txn)` enrols every record
// write in the supplied transaction so a single `put_object` (which
// touches the primary record plus its forward / reverse 2i records)
// is atomic, and so is a whole batch of object writes.

/// Look up `(bucket, key)` against the primary K/V layer.
pub(crate) fn get_object_in(
    db: &Database,
    txn: Option<&Transaction>,
    bucket: &[u8],
    key: &[u8],
) -> Result<Option<Vec<u8>>, NoxuDatastoreError> {
    validate_no_separator(bucket, "bucket")?;
    let storage_key = primary_key(bucket, key);
    let mut value = DatabaseEntry::new();
    let key_entry = DatabaseEntry::from_bytes(&storage_key);
    match db.get(txn, &key_entry, &mut value)? {
        OperationStatus::Success => Ok(Some(value.data().to_vec())),
        OperationStatus::NotFound | OperationStatus::KeyExists | OperationStatus::KeyEmpty => {
            Ok(None)
        }
    }
}

/// Insert or overwrite `(bucket, key)` and refresh its 2i entries.
pub(crate) fn put_object_in(
    db: &Database,
    txn: Option<&Transaction>,
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

    // Clean up any pre-existing forward index entries via the
    // reverse map.
    clear_forward_for(db, txn, bucket, key)?;
    // Primary K/V write.
    let primary = primary_key(bucket, key);
    db.put(
        txn,
        &DatabaseEntry::from_bytes(&primary),
        &DatabaseEntry::from_bytes(value),
    )?;
    // Forward 2i writes.
    for (name, enc) in &encoded {
        let fk = forward_key(bucket, name, enc, key);
        db.put(
            txn,
            &DatabaseEntry::from_bytes(&fk),
            &DatabaseEntry::from_bytes(b""),
        )?;
    }
    // Reverse 2i write (replace).
    let rk = reverse_key(bucket, key);
    if encoded.is_empty() {
        // No indexes -> drop any stale reverse entry.
        db.delete(txn, &DatabaseEntry::from_bytes(&rk))?;
    } else {
        let rv = encode_reverse_value(&encoded);
        db.put(
            txn,
            &DatabaseEntry::from_bytes(&rk),
            &DatabaseEntry::from_bytes(&rv),
        )?;
    }
    Ok(())
}

/// Remove `(bucket, key)` and clean up its 2i entries.
pub(crate) fn delete_object_in(
    db: &Database,
    txn: Option<&Transaction>,
    bucket: &[u8],
    key: &[u8],
) -> Result<bool, NoxuDatastoreError> {
    validate_no_separator(bucket, "bucket")?;
    let removed_2i = clear_forward_for(db, txn, bucket, key)?;
    let primary = primary_key(bucket, key);
    let removed_pri = match db.delete(txn, &DatabaseEntry::from_bytes(&primary))? {
        OperationStatus::Success => true,
        OperationStatus::NotFound | OperationStatus::KeyExists | OperationStatus::KeyEmpty => false,
    };
    Ok(removed_pri || removed_2i)
}

/// Read the reverse-index record for `(bucket, key)` then delete
/// every forward index entry it references, plus the reverse record
/// itself. Returns `true` when at least one entry was removed.
fn clear_forward_for(
    db: &Database,
    txn: Option<&Transaction>,
    bucket: &[u8],
    key: &[u8],
) -> Result<bool, NoxuDatastoreError> {
    let rk = reverse_key(bucket, key);
    let mut value = DatabaseEntry::new();
    let key_entry = DatabaseEntry::from_bytes(&rk);
    match db.get(txn, &key_entry, &mut value)? {
        OperationStatus::Success => {
            let pairs = decode_reverse_value(value.data()).ok_or_else(|| {
                NoxuDatastoreError::CorruptReverse {
                    bucket: bucket.to_vec(),
                    key: key.to_vec(),
                }
            })?;
            for (name, encoded) in &pairs {
                let fk = forward_key(bucket, name, encoded, key);
                db.delete(txn, &DatabaseEntry::from_bytes(&fk))?;
            }
            db.delete(txn, &DatabaseEntry::from_bytes(&rk))?;
            Ok(true)
        }
        OperationStatus::NotFound | OperationStatus::KeyExists | OperationStatus::KeyEmpty => {
            Ok(false)
        }
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
///
/// `txn` is `None` for an auto-commit scan or `Some(txn)` to open
/// the cursor inside an existing transaction.
fn scan_prefix<F>(
    db: &Database,
    txn: Option<&Transaction>,
    prefix: &[u8],
    mut f: F,
) -> Result<(), NoxuDatastoreError>
where
    F: FnMut(&[u8], &[u8]) -> Result<(), NoxuDatastoreError>,
{
    let mut cursor: Cursor = db.open_cursor(txn, Some(&CursorConfig::new()))?;
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

/// Re-export the encoding helper so the integration test can
/// construct expected wire-form values without re-deriving the
/// schema.
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

    #[test]
    fn open_transactional_round_trips() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_transactional(dir.path()).expect("open transactional");
        ds.put_object(
            b"users",
            b"alice",
            b"hello",
            &[(b"age_int".to_vec(), b"42".to_vec())],
        )
        .expect("put");
        assert_eq!(
            ds.get_object(b"users", b"alice").unwrap().as_deref(),
            Some(&b"hello"[..])
        );
        assert_eq!(
            ds.index_eq(b"users", b"age_int", b"42").unwrap(),
            vec![b"alice".to_vec()]
        );
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

    #[test]
    fn transaction_commits_multiple_keys_atomically() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_transactional(dir.path()).expect("open");
        ds.transaction(|tx| {
            tx.put_object(b"users", b"alice", b"a", &[])?;
            tx.put_object(b"users", b"bob", b"b", &[])?;
            tx.put_object(b"users", b"carol", b"c", &[])?;
            Ok::<_, NoxuDatastoreError>(())
        })
        .expect("commit");
        assert_eq!(
            ds.get_object(b"users", b"alice").unwrap().as_deref(),
            Some(&b"a"[..])
        );
        assert_eq!(
            ds.get_object(b"users", b"bob").unwrap().as_deref(),
            Some(&b"b"[..])
        );
        assert_eq!(
            ds.get_object(b"users", b"carol").unwrap().as_deref(),
            Some(&b"c"[..])
        );
    }

    #[test]
    fn transaction_rolls_back_every_write_on_error() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_transactional(dir.path()).expect("open");
        // Seed one key so we can prove the in-txn overwrite is undone too.
        ds.put_object(b"users", b"alice", b"original", &[])
            .expect("seed");
        let result: Result<(), NoxuDatastoreError> = ds.transaction(|tx| {
            tx.put_object(b"users", b"alice", b"changed", &[])?;
            tx.put_object(b"users", b"bob", b"b", &[])?;
            Err(NoxuDatastoreError::NotTransactional)
        });
        assert!(result.is_err());
        // None of the writes survive: alice keeps its seeded value and
        // bob never appears.
        assert_eq!(
            ds.get_object(b"users", b"alice").unwrap().as_deref(),
            Some(&b"original"[..])
        );
        assert!(ds.get_object(b"users", b"bob").unwrap().is_none());
    }

    #[test]
    fn transaction_requires_transactional_environment() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        let result: Result<(), NoxuDatastoreError> =
            ds.transaction(|tx| tx.put_object(b"users", b"alice", b"v", &[]));
        assert!(matches!(result, Err(NoxuDatastoreError::NotTransactional)));
    }

    #[test]
    fn transaction_2i_records_commit_and_roll_back_atomically() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_transactional(dir.path()).expect("open");
        // Commit: a put that fans out into primary + forward + reverse
        // 2i records is visible on every layer after commit.
        ds.transaction(|tx| {
            tx.put_object(
                b"users",
                b"alice",
                b"v",
                &[(b"age_int".to_vec(), b"42".to_vec())],
            )
        })
        .expect("commit");
        assert_eq!(
            ds.get_object(b"users", b"alice").unwrap().as_deref(),
            Some(&b"v"[..])
        );
        assert_eq!(
            ds.index_eq(b"users", b"age_int", b"42").unwrap(),
            vec![b"alice".to_vec()]
        );

        // Roll back: a put inside an aborting txn leaves neither the
        // primary record nor the forward 2i entry behind.
        let result: Result<(), NoxuDatastoreError> = ds.transaction(|tx| {
            tx.put_object(
                b"users",
                b"bob",
                b"v",
                &[(b"age_int".to_vec(), b"99".to_vec())],
            )?;
            Err(NoxuDatastoreError::NotTransactional)
        });
        assert!(result.is_err());
        assert!(ds.get_object(b"users", b"bob").unwrap().is_none());
        assert!(ds.index_eq(b"users", b"age_int", b"99").unwrap().is_empty());
    }

    #[test]
    fn execute_batch_commits_and_reports_count() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_transactional(dir.path()).expect("open");
        let batch = TxnBatch {
            ops: vec![
                TxnOp::Put {
                    bucket: b"users".to_vec(),
                    key: b"alice".to_vec(),
                    value: b"a".to_vec(),
                    indexes: vec![(b"age_int".to_vec(), b"42".to_vec())],
                },
                TxnOp::Put {
                    bucket: b"users".to_vec(),
                    key: b"bob".to_vec(),
                    value: b"b".to_vec(),
                    indexes: vec![],
                },
            ],
            force_abort: false,
        };
        let outcome = ds.execute_batch(&batch).expect("commit");
        assert_eq!(outcome, TxnOutcome::Committed { operations: 2 });
        assert_eq!(
            ds.get_object(b"users", b"alice").unwrap().as_deref(),
            Some(&b"a"[..])
        );
        assert_eq!(
            ds.index_eq(b"users", b"age_int", b"42").unwrap(),
            vec![b"alice".to_vec()]
        );
    }

    #[test]
    fn execute_batch_force_abort_leaves_no_writes() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_transactional(dir.path()).expect("open");
        let batch = TxnBatch {
            ops: vec![
                TxnOp::Put {
                    bucket: b"users".to_vec(),
                    key: b"alice".to_vec(),
                    value: b"a".to_vec(),
                    indexes: vec![(b"age_int".to_vec(), b"42".to_vec())],
                },
                TxnOp::Delete {
                    bucket: b"users".to_vec(),
                    key: b"ghost".to_vec(),
                },
            ],
            force_abort: true,
        };
        let outcome = ds.execute_batch(&batch).expect("abort is not an error");
        assert!(matches!(outcome, TxnOutcome::Aborted { .. }));
        assert!(ds.get_object(b"users", b"alice").unwrap().is_none());
        assert!(ds.index_eq(b"users", b"age_int", b"42").unwrap().is_empty());
    }

    #[test]
    fn execute_batch_rejects_empty() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_transactional(dir.path()).expect("open");
        let batch = TxnBatch::default();
        assert!(matches!(
            ds.execute_batch(&batch),
            Err(TxnStoreError::EmptyBatch)
        ));
    }

    #[test]
    fn execute_batch_as_any_downcasts_to_transactional_store() {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_transactional(dir.path()).expect("open");
        let erased: Arc<dyn Datastore> = Arc::new(ds);
        let any = <dyn Datastore>::as_any(erased.as_ref()).expect("as_any");
        let store = any
            .downcast_ref::<NoxuDatastore>()
            .expect("downcast to NoxuDatastore");
        let batch = TxnBatch {
            ops: vec![TxnOp::Put {
                bucket: b"users".to_vec(),
                key: b"alice".to_vec(),
                value: b"a".to_vec(),
                indexes: vec![],
            }],
            force_abort: false,
        };
        let outcome = store.execute_batch(&batch).expect("commit");
        assert_eq!(outcome, TxnOutcome::Committed { operations: 1 });
    }
}
