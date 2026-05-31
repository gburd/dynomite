//! Persist a [`TextIndex`] to an embedded Noxu DB environment
//! and rehydrate it later.
//!
//! The persister exposes a small public surface around three
//! Noxu sub-databases that share a single environment:
//!
//! * `docs` -- key = doc id (4-byte big-endian `u32`),
//!   value = the doc's raw bytes. This is the source of truth
//!   for tier-4 substring rechecks and is the only sub-database
//!   strictly required to reconstruct a [`TextIndex`].
//! * `postings` -- key = trigram hash (8-byte big-endian
//!   `u64`), value = a [`roaring::RoaringBitmap`] of doc ids
//!   serialised via [`roaring::RoaringBitmap::serialize_into`].
//!   Persisted so an operator can audit the inverted index
//!   without reloading the corpus, and so a future fast-load
//!   path can avoid re-extracting trigrams.
//! * `bloom` -- key = doc id (4-byte big-endian `u32`),
//!   value = the per-doc trigram bloom filter encoded via
//!   [`bincode::serialize`] over the [`crate::bloom::BloomFilter`]
//!   struct. The encoded layout starts with a length-prefixed
//!   bit vector followed by the hash count and the total bit
//!   count, in declaration order.
//!
//! # Atomicity
//!
//! [`NoxuPersister::snapshot`] writes everything inside a single
//! Noxu transaction so a partial snapshot never leaks.
//! [`NoxuPersister::append_doc`] is also transactional: a new
//! doc and its postings / bloom records land or do not land
//! together.
//!
//! # Round-trip contract
//!
//! [`NoxuPersister::load`] returns a [`TextIndex`] whose
//! `search_substring` (and, in future phases, `search_regex`)
//! results are byte-for-byte identical to the source index for
//! every query. The returned index is rebuilt from the `docs`
//! sub-database so the postings and bloom records on disk are
//! redundant relative to the doc corpus; they are still
//! written so callers that audit the on-disk shape see the
//! full index state, and so a future "fast load" path can
//! consume them without changing this module's external
//! contract.
//!
//! # Stale records
//!
//! `snapshot` overwrites every record currently in the three
//! sub-databases with the in-memory state. Doc ids that were
//! removed from the index since the previous snapshot have
//! their docs / bloom / postings entries deleted before the
//! new state is written so the reload path observes the same
//! doc set as the in-memory index.
//!
//! # Examples
//!
//! ```no_run
//! use dyntext::index::TextIndex;
//! use dyntext::persist::{NoxuPersister, PersistConfig};
//!
//! let cfg = PersistConfig {
//!     env_path: "/tmp/dyntext-example".into(),
//!     ..PersistConfig::default()
//! };
//! let persister = NoxuPersister::open(cfg).expect("open noxu env");
//!
//! let mut idx = TextIndex::new();
//! idx.insert(b"hello world".to_vec());
//! persister.snapshot(&idx).expect("snapshot");
//!
//! let restored = persister.load().expect("load");
//! assert_eq!(restored.doc_count(), idx.doc_count());
//! ```

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use noxu::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get, NoxuError,
    OperationStatus, Transaction, TransactionConfig,
};
use roaring::RoaringBitmap;

use crate::index::{IndexedDoc, TextIndex};
use crate::trigram::extract_trigram_set;

/// Width of an encoded doc-id key in bytes (`u32` big-endian).
const DOC_KEY_LEN: usize = 4;
/// Width of an encoded trigram hash key in bytes (`u64` big-endian).
const TRIGRAM_KEY_LEN: usize = 8;

/// Errors produced by [`NoxuPersister`] operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PersistError {
    /// A Noxu-level error from the environment, database, or
    /// transaction layer.
    #[error("noxu: {0}")]
    Noxu(#[from] NoxuError),
    /// A Roaring bitmap could not be serialised or deserialised.
    /// The wrapped error is the I/O error emitted by the
    /// `roaring` crate's `serialize_into` / `deserialize_from`
    /// helpers.
    #[error("roaring io: {0}")]
    RoaringIo(#[from] std::io::Error),
    /// A bloom filter blob could not be encoded or decoded by
    /// `bincode`.
    #[error("bincode: {0}")]
    Bincode(String),
    /// A stored key did not match the expected width or shape.
    #[error("corrupt key in {db} db: expected {expected} bytes, got {got}")]
    CorruptKey {
        /// Which sub-database the offending key came from.
        db: &'static str,
        /// Expected key width in bytes.
        expected: usize,
        /// Actual key width in bytes.
        got: usize,
    },
}

impl From<bincode::Error> for PersistError {
    fn from(err: bincode::Error) -> Self {
        PersistError::Bincode(err.to_string())
    }
}

/// Configuration for opening a [`NoxuPersister`].
///
/// A `PersistConfig` describes the on-disk location of the
/// Noxu environment and the names of the three sub-databases
/// the persister uses. The defaults are reasonable for tests;
/// production deployments should set `env_path` to a durable
/// directory and pick a `cache_size_bytes` matched to the
/// expected working set.
///
/// # Examples
///
/// ```
/// use dyntext::persist::PersistConfig;
/// let cfg = PersistConfig {
///     env_path: "/var/lib/dynomite/dyntext".into(),
///     ..PersistConfig::default()
/// };
/// assert_eq!(cfg.docs_db_name, "docs");
/// assert_eq!(cfg.postings_db_name, "postings");
/// assert_eq!(cfg.bloom_db_name, "bloom");
/// ```
#[derive(Debug, Clone)]
pub struct PersistConfig {
    /// Directory that hosts the Noxu environment. Created on
    /// first use if it does not exist.
    pub env_path: PathBuf,
    /// Name of the docs sub-database (key = doc id, value =
    /// raw text).
    pub docs_db_name: String,
    /// Name of the postings sub-database (key = trigram hash,
    /// value = serialised Roaring bitmap).
    pub postings_db_name: String,
    /// Name of the bloom sub-database (key = doc id, value =
    /// bincode-encoded `BloomFilter`).
    pub bloom_db_name: String,
    /// Cache budget for the Noxu environment, in bytes.
    pub cache_size_bytes: u64,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            env_path: PathBuf::from("/var/lib/dynomite/dyntext"),
            docs_db_name: "docs".to_string(),
            postings_db_name: "postings".to_string(),
            bloom_db_name: "bloom".to_string(),
            cache_size_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Persister that bridges a [`TextIndex`] to a Noxu DB
/// environment. Cheap to clone via [`Arc`].
#[derive(Clone)]
pub struct NoxuPersister {
    inner: Arc<Inner>,
}

struct Inner {
    /// The Noxu environment is held alive for the lifetime of
    /// the persister so the database handles remain valid.
    env: Mutex<Environment>,
    /// Doc-id -> raw text.
    docs: Mutex<Database>,
    /// Trigram hash -> serialised Roaring bitmap.
    postings: Mutex<Database>,
    /// Doc-id -> bincode-encoded bloom filter.
    bloom: Mutex<Database>,
}

impl NoxuPersister {
    /// Open or create the Noxu environment described by `cfg`
    /// and the three sub-databases the persister uses.
    ///
    /// # Errors
    ///
    /// Surfaces any underlying [`NoxuError`] from the
    /// environment open / database open path.
    pub fn open(cfg: PersistConfig) -> Result<Self, PersistError> {
        let PersistConfig {
            env_path,
            docs_db_name,
            postings_db_name,
            bloom_db_name,
            cache_size_bytes,
        } = cfg;
        let env_config = EnvironmentConfig::new(env_path)
            .with_allow_create(true)
            .with_transactional(true)
            .with_cache_size(cache_size_bytes);
        let env = Environment::open(env_config)?;

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let docs = env.open_database(None, &docs_db_name, &db_config)?;
        let postings = env.open_database(None, &postings_db_name, &db_config)?;
        let bloom = env.open_database(None, &bloom_db_name, &db_config)?;

        Ok(Self {
            inner: Arc::new(Inner {
                env: Mutex::new(env),
                docs: Mutex::new(docs),
                postings: Mutex::new(postings),
                bloom: Mutex::new(bloom),
            }),
        })
    }

    /// Atomically replace every record in the three
    /// sub-databases with the contents of `idx`.
    ///
    /// On return the on-disk state is identical to what
    /// [`Self::load`] would produce when called against the
    /// same in-memory index. Stale doc / posting / bloom
    /// entries from a previous snapshot are deleted inside
    /// the same transaction so a partial overwrite cannot
    /// leak.
    ///
    /// # Errors
    ///
    /// Returns [`PersistError::Noxu`] for transaction or I/O
    /// failures, [`PersistError::RoaringIo`] when a postings
    /// list cannot be serialised, and
    /// [`PersistError::Bincode`] when a bloom filter cannot be
    /// encoded.
    pub fn snapshot(&self, idx: &TextIndex) -> Result<(), PersistError> {
        let env = self.lock_env();
        let docs = self.lock_docs();
        let postings = self.lock_postings();
        let bloom = self.lock_bloom();
        let handles = DbHandles {
            docs: &docs,
            postings: &postings,
            bloom: &bloom,
        };

        let txn = env.begin_transaction(Some(&TransactionConfig::default()))?;

        match snapshot_locked(&txn, &handles, idx) {
            Ok(()) => {
                txn.commit()?;
                Ok(())
            }
            Err(err) => {
                let _ = txn.abort();
                Err(err)
            }
        }
    }

    /// Append the records for a single doc to the on-disk
    /// state.
    ///
    /// `doc_id` must exist in `idx`; the doc's text is read
    /// from the index and its trigram set is recomputed so the
    /// postings and bloom records reflect the current state of
    /// `idx`. Trigrams shared with other docs have their
    /// existing bitmap on disk overwritten with the live
    /// bitmap from `idx.postings()`.
    ///
    /// The three writes (docs, bloom, postings) happen inside
    /// a single transaction.
    ///
    /// # Errors
    ///
    /// Returns [`PersistError::Noxu`] if `doc_id` is not in
    /// the index (the public surface deliberately rejects
    /// "append nothing" rather than silently no-op).
    pub fn append_doc(&self, doc_id: u32, idx: &TextIndex) -> Result<(), PersistError> {
        let Some(doc) = idx.docs().get(&doc_id) else {
            return Err(PersistError::Noxu(NoxuError::IllegalArgument(format!(
                "append_doc: doc id {doc_id} is not present in the index"
            ))));
        };

        let env = self.lock_env();
        let docs = self.lock_docs();
        let postings = self.lock_postings();
        let bloom = self.lock_bloom();
        let handles = DbHandles {
            docs: &docs,
            postings: &postings,
            bloom: &bloom,
        };

        let txn = env.begin_transaction(Some(&TransactionConfig::default()))?;
        match append_locked(&txn, &handles, doc_id, doc, idx) {
            Ok(()) => {
                txn.commit()?;
                Ok(())
            }
            Err(err) => {
                let _ = txn.abort();
                Err(err)
            }
        }
    }

    /// Re-open the persisted state and return a fresh
    /// [`TextIndex`] with the same observable behaviour as
    /// the index at the time of the last snapshot or the
    /// sequence of `append_doc` calls.
    ///
    /// The reconstruction path walks the docs sub-database in
    /// doc-id order and re-inserts each doc through
    /// [`TextIndex::insert`]. Doc ids are preserved exactly
    /// because [`TextIndex::insert`] assigns them
    /// monotonically starting at zero and the docs are walked
    /// in numeric order; the postings and bloom records on
    /// disk are not consulted on this path because the index
    /// regenerates equivalent state from the doc text via the
    /// same code path used at original insert time.
    ///
    /// An empty environment yields an empty index.
    ///
    /// # Errors
    ///
    /// Surfaces [`NoxuError`] from the cursor scan and
    /// [`PersistError::CorruptKey`] when a docs key is not
    /// the expected fixed-width form.
    pub fn load(&self) -> Result<TextIndex, PersistError> {
        let docs = self.lock_docs();
        let mut idx = TextIndex::new();
        let mut last_id: Option<u32> = None;
        for_each_record(&docs, |key, value| {
            if key.len() != DOC_KEY_LEN {
                return Err(PersistError::CorruptKey {
                    db: "docs",
                    expected: DOC_KEY_LEN,
                    got: key.len(),
                });
            }
            let mut buf = [0u8; DOC_KEY_LEN];
            buf.copy_from_slice(key);
            let doc_id = u32::from_be_bytes(buf);
            // Pad with empty placeholders so the public
            // monotonic id assignment in `TextIndex::insert`
            // hits exactly `doc_id`. The cursor walk is in
            // ascending order so `last_id` tracks the highest
            // id we have already produced; the gap fill below
            // never exceeds `doc_id` itself.
            let next_expected = match last_id {
                Some(prev) => prev.checked_add(1).unwrap_or(prev),
                None => 0,
            };
            for filler in next_expected..doc_id {
                let assigned = idx.insert(Vec::new());
                debug_assert_eq!(assigned, filler);
                idx.remove(assigned);
            }
            let assigned = idx.insert(value.to_vec());
            debug_assert_eq!(assigned, doc_id);
            last_id = Some(doc_id);
            Ok(())
        })?;
        Ok(idx)
    }

    /// Borrow the path of the underlying Noxu environment.
    ///
    /// Useful for logging and for tests that want to assert
    /// the on-disk layout from the outside.
    pub fn env_path(&self) -> PathBuf {
        let env = self.lock_env();
        env.get_home().to_path_buf()
    }

    fn lock_env(&self) -> MutexGuard<'_, Environment> {
        self.inner
            .env
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_docs(&self) -> MutexGuard<'_, Database> {
        self.inner
            .docs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_postings(&self) -> MutexGuard<'_, Database> {
        self.inner
            .postings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_bloom(&self) -> MutexGuard<'_, Database> {
        self.inner
            .bloom
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Borrowed handles to the three sub-databases the persister
/// writes during a single transactional operation.
struct DbHandles<'a> {
    docs: &'a Database,
    postings: &'a Database,
    bloom: &'a Database,
}

/// Snapshot body that runs inside the transaction supplied
/// by [`NoxuPersister::snapshot`].
fn snapshot_locked(
    txn: &Transaction,
    handles: &DbHandles<'_>,
    idx: &TextIndex,
) -> Result<(), PersistError> {
    // Wipe stale records first so a doc removed from the
    // in-memory index disappears from disk.
    clear_database(handles.docs, Some(txn))?;
    clear_database(handles.bloom, Some(txn))?;
    clear_database(handles.postings, Some(txn))?;

    let mut all_trigrams: BTreeSet<u64> = BTreeSet::new();
    for (doc_id, doc) in idx.docs() {
        put_doc(txn, handles.docs, *doc_id, &doc.text)?;
        put_bloom(txn, handles.bloom, *doc_id, doc)?;
        for t in extract_trigram_set(&doc.text) {
            all_trigrams.insert(t);
        }
    }

    for trigram in all_trigrams {
        if let Some(bitmap) = idx.postings().lookup(trigram) {
            put_postings(txn, handles.postings, trigram, bitmap)?;
        }
    }
    Ok(())
}

/// Append body that runs inside the transaction supplied by
/// [`NoxuPersister::append_doc`].
fn append_locked(
    txn: &Transaction,
    handles: &DbHandles<'_>,
    doc_id: u32,
    doc: &IndexedDoc,
    idx: &TextIndex,
) -> Result<(), PersistError> {
    put_doc(txn, handles.docs, doc_id, &doc.text)?;
    put_bloom(txn, handles.bloom, doc_id, doc)?;
    for trigram in extract_trigram_set(&doc.text) {
        if let Some(bitmap) = idx.postings().lookup(trigram) {
            put_postings(txn, handles.postings, trigram, bitmap)?;
        }
    }
    Ok(())
}

/// Encode a doc id as a 4-byte big-endian key.
fn doc_key(doc_id: u32) -> [u8; DOC_KEY_LEN] {
    doc_id.to_be_bytes()
}

/// Encode a trigram hash as an 8-byte big-endian key.
fn trigram_key(trigram: u64) -> [u8; TRIGRAM_KEY_LEN] {
    trigram.to_be_bytes()
}

/// Write a doc record (raw text) under its 4-byte id key.
fn put_doc(
    txn: &Transaction,
    docs_db: &Database,
    doc_id: u32,
    text: &[u8],
) -> Result<(), PersistError> {
    let key = doc_key(doc_id);
    docs_db.put(
        Some(txn),
        &DatabaseEntry::from_bytes(&key),
        &DatabaseEntry::from_bytes(text),
    )?;
    Ok(())
}

/// Write a doc's bloom filter under its 4-byte id key.
fn put_bloom(
    txn: &Transaction,
    bloom_db: &Database,
    doc_id: u32,
    doc: &IndexedDoc,
) -> Result<(), PersistError> {
    let key = doc_key(doc_id);
    let value = bincode::serialize(&doc.bloom)?;
    bloom_db.put(
        Some(txn),
        &DatabaseEntry::from_bytes(&key),
        &DatabaseEntry::from_bytes(&value),
    )?;
    Ok(())
}

/// Write a Roaring bitmap under its 8-byte trigram key.
fn put_postings(
    txn: &Transaction,
    postings_db: &Database,
    trigram: u64,
    bitmap: &RoaringBitmap,
) -> Result<(), PersistError> {
    let key = trigram_key(trigram);
    let mut buf: Vec<u8> = Vec::with_capacity(bitmap.serialized_size());
    bitmap.serialize_into(&mut buf)?;
    postings_db.put(
        Some(txn),
        &DatabaseEntry::from_bytes(&key),
        &DatabaseEntry::from_bytes(&buf),
    )?;
    Ok(())
}

/// Iterate every record in `db` (in storage order), invoking
/// `f` once per `(key, value)` pair. Iteration uses a
/// non-transactional cursor; callers that need a consistent
/// snapshot must arrange that themselves.
fn for_each_record<F>(db: &Database, mut f: F) -> Result<(), PersistError>
where
    F: FnMut(&[u8], &[u8]) -> Result<(), PersistError>,
{
    let mut cursor = db.open_cursor(None, None)?;
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

/// Delete every record in `db` while running inside (or
/// outside) the supplied transaction.
fn clear_database(db: &Database, txn: Option<&Transaction>) -> Result<(), PersistError> {
    // Collect keys first, then delete. Doing the delete inside
    // the cursor walk would risk invalidating the cursor
    // position on some Noxu engine implementations; the
    // collect-then-delete shape is portable and the cost is a
    // single linear pass.
    let mut keys: Vec<Vec<u8>> = Vec::new();
    {
        let mut cursor = db.open_cursor(txn, None)?;
        let mut key = DatabaseEntry::new();
        let mut value = DatabaseEntry::new();
        let mut status = cursor.get(&mut key, &mut value, Get::First, None)?;
        while matches!(status, OperationStatus::Success) {
            keys.push(key.data().to_vec());
            status = cursor.get(&mut key, &mut value, Get::Next, None)?;
        }
        let _ = cursor.close();
    }
    for k in keys {
        db.delete(txn, &DatabaseEntry::from_bytes(&k))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_persister(dir: &TempDir) -> NoxuPersister {
        let cfg = PersistConfig {
            env_path: dir.path().to_path_buf(),
            ..PersistConfig::default()
        };
        NoxuPersister::open(cfg).expect("open persister")
    }

    #[test]
    fn open_creates_environment_and_databases() {
        let dir = TempDir::new().expect("tempdir");
        let p = open_persister(&dir);
        assert_eq!(p.env_path(), dir.path());
    }

    #[test]
    fn doc_key_encodes_big_endian_u32() {
        assert_eq!(doc_key(0), [0u8, 0, 0, 0]);
        assert_eq!(doc_key(1), [0u8, 0, 0, 1]);
        assert_eq!(doc_key(0x0102_0304), [1u8, 2, 3, 4]);
    }

    #[test]
    fn trigram_key_encodes_big_endian_u64() {
        assert_eq!(trigram_key(0), [0u8; 8]);
        assert_eq!(
            trigram_key(0x0102_0304_0506_0708),
            [1u8, 2, 3, 4, 5, 6, 7, 8]
        );
    }

    #[test]
    fn snapshot_then_load_empty_index_round_trips() {
        let dir = TempDir::new().expect("tempdir");
        let p = open_persister(&dir);
        let idx = TextIndex::new();
        p.snapshot(&idx).expect("snapshot empty");
        let restored = p.load().expect("load empty");
        assert_eq!(restored.doc_count(), 0);
    }

    #[test]
    fn append_doc_rejects_unknown_id() {
        let dir = TempDir::new().expect("tempdir");
        let p = open_persister(&dir);
        let idx = TextIndex::new();
        let err = p.append_doc(7, &idx).unwrap_err();
        assert!(matches!(err, PersistError::Noxu(_)));
    }
}
