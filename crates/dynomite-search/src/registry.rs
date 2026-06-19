//! Vector index registry.
//!
//! [`VectorRegistry`] is the per-server map of index name to
//! [`VectorTable`]. It is the single source of truth that the
//! FT.* command handlers in [`crate::ft`] consult to dispatch
//! `FT.CREATE` / `FT.SEARCH` / `FT.INFO` / `FT.DROPINDEX`.
//!
//! Concurrency model:
//!
//! * The registry is held behind a [`parking_lot::RwLock`] over
//!   a [`BTreeMap`] keyed by index name. Reads (lookups, FT.LIST)
//!   take the read lock; mutations (create / drop) take the
//!   write lock.
//! * Each [`VectorTable`] is wrapped in [`Arc`] so a reader can
//!   drop the lock immediately after a lookup and continue to
//!   work against a stable handle.
//! * The underlying [`dynvec::Engine`] inside each table is
//!   itself an [`Arc`]-wrapped storage handle; read paths
//!   (FT.SEARCH) do not block write paths (HSET / FT.ADD).

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::schema::{IndexAlgorithm, MetadataFieldType, VectorSchema};
use crate::sugest_registry::SuggestionRegistry;
use dyntext::TextIndex;
use dynvec::Engine;

/// File name of the registry snapshot under the configured
/// persistence directory.
const SNAPSHOT_FILE: &str = "search-snapshot.cbor";

/// Errors returned by the registry.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RegistryError {
    /// An index with that name already exists.
    #[error("index already exists: {0}")]
    AlreadyExists(String),
    /// No index registered under that name.
    #[error("index not found: {0}")]
    NotFound(String),
    /// The schema asks for an algorithm the engine does not
    /// implement yet (today: [`IndexAlgorithm::Flat`]).
    #[error("unsupported index algorithm: {0:?}")]
    UnsupportedAlgorithm(IndexAlgorithm),
    /// Engine-level failure during [`Engine::in_memory`].
    #[error("engine: {0}")]
    Engine(#[from] dynvec::storage::StoreError),
}

/// Errors raised by the snapshot persistence path.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SnapshotError {
    /// Filesystem failure reading or writing the snapshot.
    #[error("snapshot io: {0}")]
    Io(#[from] io::Error),
    /// The snapshot bytes could not be encoded to CBOR.
    #[error("snapshot encode: {0}")]
    Encode(String),
    /// The snapshot bytes could not be decoded from CBOR.
    #[error("snapshot decode: {0}")]
    Decode(String),
    /// Replaying a snapshotted index back into the registry
    /// failed (duplicate name, unsupported algorithm, or an
    /// engine refusal).
    #[error("snapshot replay: {0}")]
    Replay(#[from] RegistryError),
    /// A snapshotted vector payload could not be decoded back
    /// to `f32` for re-insertion into the engine.
    #[error("snapshot vector decode: {0}")]
    VectorDecode(String),
}

/// Per-`TEXT` schema field state.
///
/// Couples one [`dyntext::TextIndex`] (trigram + bloom
/// inverted index) with the bookkeeping the FT.* surface
/// needs to map between user-visible document keys and the
/// internal monotonic doc ids the text index hands back. The
/// pairing is one [`TextFieldIndex`] per `TEXT` schema field
/// per registered index; the registry initialises one for
/// every metadata field of type [`MetadataFieldType::Text`]
/// at FT.CREATE time.
#[derive(Debug, Default)]
pub struct TextFieldIndex {
    /// Trigram + bloom inverted index over the field's bytes.
    pub index: TextIndex,
    /// Internal text-doc-id -> user-visible document key.
    pub doc_to_key: BTreeMap<u32, Vec<u8>>,
    /// User-visible document key -> internal text-doc-id.
    /// Used to evict the prior entry when the same key is
    /// re-HSET-ed under an updated field value.
    pub key_to_doc: BTreeMap<Vec<u8>, u32>,
}

/// Pair of (document key, raw text bytes) returned by the
/// per-text-field search helpers on [`VectorTable`]. Each
/// hit echoes the user-visible document key plus the
/// original bytes the FT.* surface stored under the queried
/// `TEXT` field, so callers can render the response without
/// a second round trip to the dynvec engine.
pub type TextHit = (Vec<u8>, Vec<u8>);

/// Result of a regex query through the trigram-backed text
/// index. The outer [`Option`] is `None` when no `TEXT`
/// field by that name is declared; the inner [`Result`]
/// surfaces a regex compilation error.
pub type TextRegexResult = Option<Result<Vec<TextHit>, dyntext::regex_ast::RegexError>>;

/// Result of an approximate-regex query through the TRE
/// engine. The outer [`Option`] is `None` when no `TEXT`
/// field by that name is declared; the inner [`Result`]
/// surfaces a TRE-engine compilation or matching error.
pub type TextRegexApproxResult = Option<Result<Vec<TextHit>, dyntext::TreError>>;

/// One registered vector index.
///
/// A [`VectorTable`] couples the protocol-level [`VectorSchema`]
/// (what the client asked for) with the storage-level
/// [`Engine`] (what is actually persisted). The pair is
/// immutable for the lifetime of the index; rebuilding a
/// schema means dropping and recreating the table.
///
/// Alongside the schema and engine, the table tracks the set
/// of document keys that the FT.* surface has indexed via
/// HSET interception. The set is used by
/// [`VectorRegistry::drop_with_dd`] to enumerate the
/// underlying hash documents that should also be removed.
#[derive(Debug)]
pub struct VectorTable {
    /// Index name (the FT.CREATE first argument).
    pub name: String,
    /// Compiled schema.
    pub schema: VectorSchema,
    /// Storage + index engine.
    pub engine: Engine,
    /// Document keys observed by the HSET interception path.
    indexed_keys: Mutex<BTreeSet<Vec<u8>>>,
    /// Per-`TEXT`-field trigram index map. The map is keyed
    /// by schema field name and is initialised with one
    /// entry per `TEXT` field declared in the schema. The
    /// keys are stable for the lifetime of the table; only
    /// the per-entry [`TextFieldIndex`] state mutates as
    /// HSETs land.
    text_indexes: Mutex<BTreeMap<String, TextFieldIndex>>,
}

impl VectorTable {
    /// Record `key` as having been indexed. Idempotent.
    pub fn record_indexed_key(&self, key: Vec<u8>) {
        self.indexed_keys.lock().insert(key);
    }

    /// Snapshot the set of indexed keys.
    #[must_use]
    pub fn indexed_keys(&self) -> Vec<Vec<u8>> {
        self.indexed_keys.lock().iter().cloned().collect()
    }

    /// True when the schema declares a `TEXT` field named
    /// `field`. The check is case-sensitive (the FT.CREATE
    /// parser preserves the field name verbatim). After an
    /// `FT.ALTER ADD <field> TEXT` the schema vector remains
    /// frozen (it lives on an immutable `Arc<VectorTable>`),
    /// so this method also consults the runtime
    /// [`TextFieldIndex`] map: a field that the registry has
    /// provisioned a trigram index for is treated as a TEXT
    /// field for the lifetime of the table.
    #[must_use]
    pub fn has_text_field(&self, field: &str) -> bool {
        let in_schema = self
            .schema
            .metadata_fields
            .iter()
            .any(|f| f.field_type == MetadataFieldType::Text && f.name == field);
        if in_schema {
            return true;
        }
        self.text_indexes.lock().contains_key(field)
    }

    /// Provision a runtime [`TextFieldIndex`] for `field`.
    ///
    /// Used by `FT.ALTER ADD <field> TEXT` to extend an
    /// already-registered table with a new text-indexed
    /// field. Idempotent: a second call for the same field
    /// is a no-op and returns `false`.
    ///
    /// Returns `true` when a new index slot was provisioned,
    /// `false` when the field was already known (either as
    /// part of the original schema or because a prior
    /// `FT.ALTER` provisioned it).
    pub fn add_text_field(&self, field: &str) -> bool {
        let mut guard = self.text_indexes.lock();
        if guard.contains_key(field) {
            return false;
        }
        guard.insert(field.to_string(), TextFieldIndex::default());
        true
    }

    /// Snapshot the set of TEXT fields known to this table:
    /// the original `SCHEMA` declarations plus anything
    /// provisioned later through [`Self::add_text_field`].
    /// Names are returned in lexicographic order.
    #[must_use]
    pub fn text_field_names(&self) -> Vec<String> {
        let mut names: BTreeSet<String> = BTreeSet::new();
        for f in &self.schema.metadata_fields {
            if f.field_type == MetadataFieldType::Text {
                names.insert(f.name.clone());
            }
        }
        for k in self.text_indexes.lock().keys() {
            names.insert(k.clone());
        }
        names.into_iter().collect()
    }

    /// True when the registry has provisioned a [`TextIndex`]
    /// for `field`. The check returns `true` exactly when
    /// [`Self::has_text_field`] returns `true`; exposed
    /// separately so wire-level tests can assert that the
    /// FT.CREATE path actually populated the registry rather
    /// than just recorded the schema.
    #[must_use]
    pub fn has_text_index(&self, field: &str) -> bool {
        self.text_indexes.lock().contains_key(field)
    }

    /// Number of documents currently indexed under `field`.
    /// Returns `None` when no `TEXT` field by that name is
    /// declared in the schema.
    #[must_use]
    pub fn text_index_doc_count(&self, field: &str) -> Option<usize> {
        self.text_indexes
            .lock()
            .get(field)
            .map(|state| state.index.doc_count())
    }

    /// Insert `text` into the [`TextIndex`] for `field`,
    /// associating it with the user-visible `key`. If the
    /// same `key` had a prior entry under this field it is
    /// removed first so the postings index never accumulates
    /// stale doc ids.
    ///
    /// No-op when the schema has no `TEXT` field by that
    /// name; callers can therefore call this for every
    /// HSET field/value pair without prior schema lookup.
    pub fn upsert_text_field(&self, field: &str, key: &[u8], text: &[u8]) {
        let mut guard = self.text_indexes.lock();
        let Some(state) = guard.get_mut(field) else {
            return;
        };
        if let Some(prev_id) = state.key_to_doc.remove(key) {
            state.doc_to_key.remove(&prev_id);
            state.index.remove(prev_id);
        }
        let doc_id = state.index.insert(text.to_vec());
        state.doc_to_key.insert(doc_id, key.to_vec());
        state.key_to_doc.insert(key.to_vec(), doc_id);
    }

    /// Run an exact-substring lookup against the [`TextIndex`]
    /// registered under `field`. Returns the user-visible
    /// keys whose stored text contains `query` as a contiguous
    /// byte substring, paired with the original text bytes.
    ///
    /// Returns `None` when no `TEXT` field by that name is
    /// declared in the schema. Callers translate that into a
    /// `-ERR` reply.
    #[must_use]
    pub fn search_text_substring(&self, field: &str, query: &[u8]) -> Option<Vec<TextHit>> {
        let guard = self.text_indexes.lock();
        let state = guard.get(field)?;
        let mut hits: Vec<TextHit> = Vec::new();
        for doc_id in state.index.search_substring(query) {
            let Some(key) = state.doc_to_key.get(&doc_id) else {
                continue;
            };
            let Some(doc) = state.index.docs().get(&doc_id) else {
                continue;
            };
            hits.push((key.clone(), doc.text.clone()));
        }
        Some(hits)
    }

    /// Run an exact-regex lookup against the [`TextIndex`]
    /// registered under `field`. Returns the user-visible
    /// keys whose stored text matches `pattern`, paired with
    /// the original text bytes.
    ///
    /// Returns `None` when no `TEXT` field by that name is
    /// declared in the schema, or `Some(Err(...))` when the
    /// pattern fails to compile.
    pub fn search_text_regex(&self, field: &str, pattern: &str) -> TextRegexResult {
        let guard = self.text_indexes.lock();
        let state = guard.get(field)?;
        let result = state.index.search_regex(pattern).map(|ids| {
            let mut out: Vec<TextHit> = Vec::new();
            for doc_id in ids {
                let Some(key) = state.doc_to_key.get(&doc_id) else {
                    continue;
                };
                let Some(doc) = state.index.docs().get(&doc_id) else {
                    continue;
                };
                out.push((key.clone(), doc.text.clone()));
            }
            out
        });
        Some(result)
    }

    /// Run an approximate-regex lookup against the
    /// [`TextIndex`] registered under `field` with up to
    /// `max_errors` edit operations. Returns the user-visible
    /// keys whose stored text approximately matches `pattern`,
    /// paired with the original text bytes.
    ///
    /// Returns `None` when no `TEXT` field by that name is
    /// declared in the schema, or `Some(Err(...))` when the
    /// pattern fails to compile through the TRE engine.
    pub fn search_text_regex_approx(
        &self,
        field: &str,
        pattern: &str,
        max_errors: u16,
    ) -> TextRegexApproxResult {
        let guard = self.text_indexes.lock();
        let state = guard.get(field)?;
        let result = state
            .index
            .search_regex_approx(pattern, max_errors)
            .map(|ids| {
                let mut out: Vec<TextHit> = Vec::new();
                for doc_id in ids {
                    let Some(key) = state.doc_to_key.get(&doc_id) else {
                        continue;
                    };
                    let Some(doc) = state.index.docs().get(&doc_id) else {
                        continue;
                    };
                    out.push((key.clone(), doc.text.clone()));
                }
                out
            });
        Some(result)
    }
}

/// Snapshot view of one registered index.
///
/// Returned by [`VectorRegistry::info`] for the FT.INFO command
/// path. Kept distinct from [`VectorTable`] so the FT.INFO
/// handler can serialise a stable, copy-safe summary without
/// locking the registry across the response.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorTableInfo {
    /// Index name.
    pub name: String,
    /// Frozen vector dimension.
    pub dim: u16,
    /// Distance metric.
    pub distance: crate::schema::DistanceMetric,
    /// Index algorithm.
    pub algorithm: IndexAlgorithm,
    /// Live (non-tombstoned) row count.
    pub live_rows: usize,
    /// Number of tracked rows (live + soft-deleted).
    pub tracked_rows: usize,
}

/// Per-server vector index registry.
///
/// The registry owns the [`VectorTable`] map; FT.* command
/// handlers consult it on every command. Construct one with
/// [`VectorRegistry::new`] (typically as a field on the
/// dynomite engine context) and clone the
/// returned handle freely; clones share state.
#[derive(Clone, Default)]
pub struct VectorRegistry {
    inner: Arc<RwLock<BTreeMap<String, Arc<VectorTable>>>>,
    /// Persistence directory. `None` keeps the registry purely
    /// in-memory (identical to the historical behaviour); when
    /// set, [`VectorRegistry::save`] writes a CBOR snapshot
    /// under it and [`VectorRegistry::open`] reloads it.
    persist_dir: Option<PathBuf>,
}

impl std::fmt::Debug for VectorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<String> = self.inner.read().keys().cloned().collect();
        f.debug_struct("VectorRegistry")
            .field("indexes", &names)
            .field("persist_dir", &self.persist_dir)
            .finish()
    }
}

impl VectorRegistry {
    /// Build an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new index.
    ///
    /// The schema's [`IndexAlgorithm`] is validated against the
    /// engine's capabilities; today only
    /// [`IndexAlgorithm::Hnsw`] is supported. The engine is
    /// instantiated as an in-memory [`dynvec::Engine`]; on-disk
    /// backends will plug in once the Noxu storage path lands.
    ///
    /// # Errors
    ///
    /// * [`RegistryError::AlreadyExists`] when `name` is in use.
    /// * [`RegistryError::UnsupportedAlgorithm`] when the
    ///   schema selects an algorithm we do not implement.
    /// * [`RegistryError::Engine`] when the underlying engine
    ///   refuses the schema.
    pub fn create(&self, name: String, schema: VectorSchema) -> Result<(), RegistryError> {
        if !matches!(schema.algorithm, IndexAlgorithm::Hnsw) {
            return Err(RegistryError::UnsupportedAlgorithm(schema.algorithm));
        }
        let mut guard = self.inner.write();
        if guard.contains_key(&name) {
            return Err(RegistryError::AlreadyExists(name));
        }
        let engine_schema = schema.to_engine_schema(&name);
        let engine = Engine::in_memory(engine_schema)?;
        let mut text_indexes: BTreeMap<String, TextFieldIndex> = BTreeMap::new();
        for f in &schema.metadata_fields {
            if f.field_type == MetadataFieldType::Text {
                text_indexes.insert(f.name.clone(), TextFieldIndex::default());
            }
        }
        let table = VectorTable {
            name: name.clone(),
            schema,
            engine,
            indexed_keys: Mutex::new(BTreeSet::new()),
            text_indexes: Mutex::new(text_indexes),
        };
        guard.insert(name, Arc::new(table));
        Ok(())
    }

    /// Drop the index `name`.
    ///
    /// Returns the prior table (so callers can decide whether
    /// to also delete underlying documents, mimicking the
    /// `FT.DROPINDEX ... DD` flag).
    ///
    /// # Errors
    ///
    /// [`RegistryError::NotFound`] when no index is registered
    /// under `name`.
    pub fn drop(&self, name: &str) -> Result<Arc<VectorTable>, RegistryError> {
        let mut guard = self.inner.write();
        guard
            .remove(name)
            .ok_or_else(|| RegistryError::NotFound(name.to_string()))
    }

    /// Drop the index `name` and return the set of document
    /// keys that the FT.* surface had observed under it.
    ///
    /// Used by `FT.DROPINDEX ... DD` to enumerate the hash
    /// documents the caller should also delete from the
    /// underlying datastore.
    ///
    /// # Errors
    ///
    /// [`RegistryError::NotFound`] when no index is registered
    /// under `name`.
    pub fn drop_with_dd(&self, name: &str) -> Result<Vec<Vec<u8>>, RegistryError> {
        let table = self.drop(name)?;
        Ok(table.indexed_keys())
    }

    /// Look up a registered table by name.
    ///
    /// Returns a cloned [`Arc`] so the caller can drop the
    /// registry lock immediately after the lookup.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Arc<VectorTable>> {
        self.inner.read().get(name).cloned()
    }

    /// List every registered index by name, sorted
    /// alphabetically.
    #[must_use]
    pub fn list(&self) -> Vec<String> {
        self.inner.read().keys().cloned().collect()
    }

    /// Snapshot the FT.INFO view of `name`.
    #[must_use]
    pub fn info(&self, name: &str) -> Option<VectorTableInfo> {
        let table = self.get(name)?;
        let stats = table.engine.stats().ok()?;
        Some(VectorTableInfo {
            name: table.name.clone(),
            dim: table.schema.dim,
            distance: table.schema.distance,
            algorithm: table.schema.algorithm,
            live_rows: stats.live_rows,
            tracked_rows: stats.tracked_rows,
        })
    }

    /// Open a registry backed by the persistence directory
    /// `dir`.
    ///
    /// When a snapshot file already exists under `dir` it is
    /// loaded so every index (schema, indexed documents, text
    /// fields) and every suggestion dictionary is reconstructed
    /// without the client re-issuing `FT.CREATE` or re-feeding
    /// data. When no snapshot exists the registry starts empty.
    /// In both cases later calls to [`VectorRegistry::save`]
    /// write back to the same `dir`.
    ///
    /// The supplied [`SuggestionRegistry`] is populated in place
    /// with any snapshotted suggestion dictionaries; pass the
    /// same handle the [`crate::SearchExtension`] will serve
    /// FT.SUG* from.
    ///
    /// # Errors
    ///
    /// * [`SnapshotError::Io`] when `dir` cannot be created or
    ///   the snapshot file cannot be read.
    /// * [`SnapshotError::Decode`] when the snapshot bytes are
    ///   not valid CBOR.
    /// * [`SnapshotError::Replay`] / [`SnapshotError::VectorDecode`]
    ///   when a snapshotted index cannot be reconstructed.
    pub fn open(
        dir: impl Into<PathBuf>,
        suggestions: &SuggestionRegistry,
    ) -> Result<Self, SnapshotError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let reg = Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            persist_dir: Some(dir.clone()),
        };
        let path = dir.join(SNAPSHOT_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(reg),
            Err(e) => return Err(SnapshotError::Io(e)),
        };
        let snapshot: RegistrySnapshot =
            ciborium::from_reader(&bytes[..]).map_err(|e| SnapshotError::Decode(e.to_string()))?;
        reg.load_snapshot(&snapshot, suggestions)?;
        Ok(reg)
    }

    /// True when this registry persists to disk (was built via
    /// [`VectorRegistry::open`]).
    #[must_use]
    pub fn is_persistent(&self) -> bool {
        self.persist_dir.is_some()
    }

    /// Write a full snapshot of the registry plus `suggestions`
    /// to the persistence directory.
    ///
    /// The write is atomic: the snapshot is written to a sibling
    /// `*.tmp` file, flushed, and renamed over the live file, so
    /// a crash mid-write never leaves a half-written snapshot
    /// (the prior good snapshot, or no snapshot, survives). A
    /// leftover `*.tmp` from an interrupted write is simply
    /// overwritten on the next save and is never read by
    /// [`VectorRegistry::open`].
    ///
    /// A registry built with [`VectorRegistry::new`] has no
    /// persistence directory; `save` is then a no-op that
    /// returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// * [`SnapshotError::Encode`] when the state will not
    ///   serialise to CBOR.
    /// * [`SnapshotError::Io`] when the temp file cannot be
    ///   written, flushed, or renamed.
    pub fn save(&self, suggestions: &SuggestionRegistry) -> Result<(), SnapshotError> {
        let Some(dir) = self.persist_dir.as_ref() else {
            return Ok(());
        };
        let snapshot = self.build_snapshot(suggestions);
        let mut buf = Vec::new();
        ciborium::into_writer(&snapshot, &mut buf)
            .map_err(|e| SnapshotError::Encode(e.to_string()))?;
        let path = dir.join(SNAPSHOT_FILE);
        let tmp = dir.join(format!("{SNAPSHOT_FILE}.tmp"));
        write_atomic(&tmp, &path, &buf)?;
        Ok(())
    }

    /// Build the serialisable snapshot of the live registry and
    /// the supplied suggestion registry.
    fn build_snapshot(&self, suggestions: &SuggestionRegistry) -> RegistrySnapshot {
        let mut indexes = Vec::new();
        let guard = self.inner.read();
        for (name, table) in guard.iter() {
            indexes.push(table.to_snapshot(name));
        }
        drop(guard);
        RegistrySnapshot {
            indexes,
            suggestions: suggestions.to_snapshot(),
        }
    }

    /// Reconstruct every snapshotted index and suggestion
    /// dictionary into this registry and `suggestions`.
    fn load_snapshot(
        &self,
        snapshot: &RegistrySnapshot,
        suggestions: &SuggestionRegistry,
    ) -> Result<(), SnapshotError> {
        for idx in &snapshot.indexes {
            self.create(idx.name.clone(), idx.schema.clone())?;
            let table = self
                .get(&idx.name)
                .ok_or_else(|| RegistryError::NotFound(idx.name.clone()))?;
            // Provision any TEXT field added through FT.ALTER
            // after FT.CREATE; schema-declared TEXT fields are
            // already provisioned by `create`.
            for field in &idx.text_alter_fields {
                table.add_text_field(field);
            }
            for doc in &idx.docs {
                let vector = decode_vector(&doc.vector)?;
                let metadata = doc.metadata.clone();
                table
                    .engine
                    .upsert(doc.key.clone(), &vector, metadata)
                    .map_err(RegistryError::Engine)?;
                table.record_indexed_key(doc.key.clone());
            }
            for field in &idx.text_fields {
                for (key, bytes) in &field.entries {
                    table.upsert_text_field(&field.field, key, bytes);
                }
            }
        }
        suggestions.load_snapshot(&snapshot.suggestions);
        Ok(())
    }
}

/// Decode a snapshotted vector payload back to `f32`.
fn decode_vector(ev: &dynvec::encoding::EncodedVector) -> Result<Vec<f32>, SnapshotError> {
    ev.codec
        .encoder()
        .decode(ev)
        .map_err(|e| SnapshotError::VectorDecode(e.to_string()))
}

/// Atomically replace `final_path` with `bytes`: write `tmp`,
/// flush + fsync, then rename over the target.
fn write_atomic(tmp: &Path, final_path: &Path, bytes: &[u8]) -> Result<(), io::Error> {
    use std::io::Write;
    let mut f = std::fs::File::create(tmp)?;
    f.write_all(bytes)?;
    f.flush()?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(tmp, final_path)
}

/// Top-level on-disk snapshot of the whole search surface.
#[derive(Debug, Serialize, Deserialize)]
struct RegistrySnapshot {
    /// One entry per registered index.
    indexes: Vec<IndexSnapshot>,
    /// Suggestion dictionaries (FT.SUG*).
    suggestions: SuggestionsSnapshot,
}

/// On-disk snapshot of one registered index.
#[derive(Debug, Serialize, Deserialize)]
struct IndexSnapshot {
    /// Index name.
    name: String,
    /// Compiled schema.
    schema: VectorSchema,
    /// Indexed documents (vector + metadata), keyed by the
    /// user-visible document key.
    docs: Vec<DocSnapshot>,
    /// Per-TEXT-field stored bytes, so the trigram index can be
    /// rebuilt on load.
    text_fields: Vec<TextFieldSnapshot>,
    /// TEXT fields provisioned through FT.ALTER after FT.CREATE
    /// (not present in the original schema). Replayed via
    /// [`VectorTable::add_text_field`] so `has_text_field`
    /// reports them after a reload.
    text_alter_fields: Vec<String>,
}

/// One indexed document inside an [`IndexSnapshot`].
#[derive(Debug, Serialize, Deserialize)]
struct DocSnapshot {
    /// User-visible document key.
    key: Vec<u8>,
    /// Encoded vector payload (decoded back to `f32` on load).
    vector: dynvec::encoding::EncodedVector,
    /// Per-row metadata as stored by the engine.
    metadata: std::collections::HashMap<String, serde_json::Value>,
}

/// Per-TEXT-field snapshot: the stored bytes per document key.
#[derive(Debug, Serialize, Deserialize)]
struct TextFieldSnapshot {
    /// Schema field name.
    field: String,
    /// `(document key, stored bytes)` in document-id order.
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Snapshot of the suggestion registry: one entry per
/// suggestion key.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct SuggestionsSnapshot {
    /// `(suggestion key, [(value, score, payload)])`.
    pub(crate) dicts: Vec<SuggestionDictSnapshot>,
}

/// Snapshot of one suggestion dictionary.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SuggestionDictSnapshot {
    /// FT.SUG* key (binary-safe).
    pub(crate) key: Vec<u8>,
    /// `(suggestion bytes, score, optional payload)`.
    pub(crate) entries: Vec<(Vec<u8>, f64, Option<Vec<u8>>)>,
}

impl VectorTable {
    /// Build the on-disk snapshot for this table. The vector
    /// payloads are read back out of the engine (the engine is
    /// the source of truth for stored vectors); text-field
    /// bytes come from the per-field trigram-index doc store.
    fn to_snapshot(&self, name: &str) -> IndexSnapshot {
        let mut docs = Vec::new();
        for key in self.indexed_keys() {
            // A key recorded as indexed but absent from the
            // engine (e.g. deleted out-of-band) is skipped
            // rather than failing the whole snapshot.
            if let Ok(Some(row)) = self.engine.get(&key) {
                docs.push(DocSnapshot {
                    key,
                    vector: row.vector,
                    metadata: row.metadata,
                });
            }
        }
        let guard = self.text_indexes.lock();
        let mut text_fields = Vec::new();
        let mut text_alter_fields = Vec::new();
        let schema_text: BTreeSet<&str> = self
            .schema
            .metadata_fields
            .iter()
            .filter(|f| f.field_type == MetadataFieldType::Text)
            .map(|f| f.name.as_str())
            .collect();
        for (field, state) in guard.iter() {
            if !schema_text.contains(field.as_str()) {
                text_alter_fields.push(field.clone());
            }
            let mut entries = Vec::new();
            for (doc_id, key) in &state.doc_to_key {
                if let Some(doc) = state.index.docs().get(doc_id) {
                    entries.push((key.clone(), doc.text.clone()));
                }
            }
            text_fields.push(TextFieldSnapshot {
                field: field.clone(),
                entries,
            });
        }
        IndexSnapshot {
            name: name.to_string(),
            schema: self.schema.clone(),
            docs,
            text_fields,
            text_alter_fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DistanceMetric, IndexAlgorithm, VectorType};

    fn schema(algorithm: IndexAlgorithm) -> VectorSchema {
        VectorSchema {
            vector_field: "vec".to_string(),
            vector_type: VectorType::Float32,
            dim: 4,
            distance: DistanceMetric::Cosine,
            algorithm,
            prefixes: Vec::new(),
            metadata_fields: Vec::new(),
        }
    }

    #[test]
    fn create_and_get_returns_table() {
        let reg = VectorRegistry::new();
        reg.create("idx".to_string(), schema(IndexAlgorithm::Hnsw))
            .unwrap();
        let table = reg.get("idx").expect("table present");
        assert_eq!(table.name, "idx");
        assert_eq!(table.schema.dim, 4);
    }

    #[test]
    fn duplicate_name_errors() {
        let reg = VectorRegistry::new();
        reg.create("idx".to_string(), schema(IndexAlgorithm::Hnsw))
            .unwrap();
        let err = reg
            .create("idx".to_string(), schema(IndexAlgorithm::Hnsw))
            .unwrap_err();
        assert!(matches!(err, RegistryError::AlreadyExists(_)));
    }

    #[test]
    fn unsupported_algorithm_errors() {
        let reg = VectorRegistry::new();
        let err = reg
            .create("idx".to_string(), schema(IndexAlgorithm::Flat))
            .unwrap_err();
        assert!(matches!(err, RegistryError::UnsupportedAlgorithm(_)));
    }
}
