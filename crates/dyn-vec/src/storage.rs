//! Vector row storage.
//!
//! [`VectorStore`] persists [`VectorRow`] records keyed by
//! `(table, row_key)` and maintains a per-table HNSW index for
//! ANN search. The MVP ships a [`MemoryStore`] backend that
//! satisfies the [`Backend`] trait without external dependencies;
//! a Noxu-backed implementation lives behind the optional `noxu`
//! feature in [`crate::storage::noxu_backend`].
//!
//! Layout:
//!
//! * Per [`VectorTable`] there is one HNSW index, one map of
//!   live rows, and one tombstone counter.
//! * Inserts update both the row map and the index in lockstep.
//! * Deletes soft-delete the index node and remove the row.
//!
//! Concurrency: every public method takes the table's `Mutex` so
//! the index and row map stay in sync without a more elaborate
//! transaction shape.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::distance::Distance;
use crate::encoding::{Codec, EncodedVector, EncodingError};
use crate::index::{HnswIndex, HnswParams, IndexError, NodeId, SearchResult};
use crate::turbo_index::TurboTable;

/// Bucket key: an opaque byte string supplied by the client.
pub type RowKey = Vec<u8>;

/// Per-table schema.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableSchema {
    /// Table name.
    pub name: String,
    /// Frozen vector dimension.
    pub dim: u16,
    /// Storage codec for the vector column.
    pub codec: Codec,
    /// Distance metric for ANN search.
    pub distance: Distance,
    /// HNSW tuning.
    pub hnsw: HnswParams,
}

/// A persisted vector row.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorRow {
    /// Row key.
    pub key: RowKey,
    /// Encoded vector data (codec is on the payload).
    pub vector: EncodedVector,
    /// Free-form per-row metadata; used by clients to filter,
    /// label, or reconcile.
    pub metadata: HashMap<String, serde_json::Value>,
    /// Creation timestamp, milliseconds since the Unix epoch.
    pub created_at: u64,
    /// Last-update timestamp, milliseconds since the Unix
    /// epoch.
    pub updated_at: u64,
}

/// Errors returned by [`VectorStore`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    /// Table not registered with this store.
    #[error("table not found: {0}")]
    UnknownTable(String),
    /// Table already exists.
    #[error("table already exists: {0}")]
    TableExists(String),
    /// Row dimension does not match the table dimension.
    #[error("dimension mismatch: table {table} expects {expected}, got {got}")]
    DimensionMismatch {
        /// Table name.
        table: String,
        /// Frozen table dimension.
        expected: u16,
        /// Caller's vector dimension.
        got: u16,
    },
    /// Row not found.
    #[error("row not found in table {table}: {key:?}")]
    RowNotFound {
        /// Table name.
        table: String,
        /// Row key.
        key: RowKey,
    },
    /// Encoding failure.
    #[error("encoding: {0}")]
    Encoding(#[from] EncodingError),
    /// Index failure.
    #[error("index: {0}")]
    Index(#[from] IndexError),
    /// Backend storage failure.
    #[error("backend: {0}")]
    Backend(String),
}

/// Storage backend trait.
///
/// Implementations are responsible for persisting / fetching
/// the row map and the HNSW snapshot. The MVP ships an
/// in-memory backend ([`MemoryBackend`]); a Noxu-backed
/// backend lives behind the `noxu` feature.
pub trait Backend: Send + Sync {
    /// Persist `row` under `(table, key)`. Overwrites prior
    /// values.
    fn put_row(&self, table: &str, key: &[u8], row: &VectorRow) -> Result<(), StoreError>;

    /// Fetch the row at `(table, key)`. Returns `None` for a
    /// missing row.
    fn get_row(&self, table: &str, key: &[u8]) -> Result<Option<VectorRow>, StoreError>;

    /// Remove `(table, key)`. Returns `true` when present,
    /// `false` when absent.
    fn delete_row(&self, table: &str, key: &[u8]) -> Result<bool, StoreError>;

    /// Iterate every `(key, row)` in `table` in unspecified
    /// order. Used to rebuild the HNSW index on startup.
    fn for_each_row(&self, table: &str, f: &mut RowVisitor<'_>) -> Result<(), StoreError>;

    /// Persist a [`TableSchema`].
    fn put_schema(&self, schema: &TableSchema) -> Result<(), StoreError>;

    /// List every persisted [`TableSchema`].
    fn list_schemas(&self) -> Result<Vec<TableSchema>, StoreError>;
}

/// Callback type used by [`Backend::for_each_row`].
pub type RowVisitor<'a> = dyn FnMut(&[u8], &VectorRow) -> Result<(), StoreError> + 'a;

/// In-memory backend. Satisfies [`Backend`] without writing to
/// disk; useful for tests and for embedders that want a
/// fully-volatile vector cache.
#[derive(Default)]
pub struct MemoryBackend {
    rows: RwLock<HashMap<String, HashMap<Vec<u8>, VectorRow>>>,
    schemas: RwLock<HashMap<String, TableSchema>>,
}

impl MemoryBackend {
    /// Build an empty memory backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Backend for MemoryBackend {
    fn put_row(&self, table: &str, key: &[u8], row: &VectorRow) -> Result<(), StoreError> {
        let mut rows = self.rows.write();
        let entry = rows.entry(table.to_string()).or_default();
        entry.insert(key.to_vec(), row.clone());
        Ok(())
    }

    fn get_row(&self, table: &str, key: &[u8]) -> Result<Option<VectorRow>, StoreError> {
        let rows = self.rows.read();
        Ok(rows.get(table).and_then(|m| m.get(key).cloned()))
    }

    fn delete_row(&self, table: &str, key: &[u8]) -> Result<bool, StoreError> {
        let mut rows = self.rows.write();
        Ok(rows.get_mut(table).is_some_and(|m| m.remove(key).is_some()))
    }

    fn for_each_row(&self, table: &str, f: &mut RowVisitor<'_>) -> Result<(), StoreError> {
        let rows = self.rows.read();
        if let Some(m) = rows.get(table) {
            for (k, v) in m {
                f(k, v)?;
            }
        }
        Ok(())
    }

    fn put_schema(&self, schema: &TableSchema) -> Result<(), StoreError> {
        self.schemas
            .write()
            .insert(schema.name.clone(), schema.clone());
        Ok(())
    }

    fn list_schemas(&self) -> Result<Vec<TableSchema>, StoreError> {
        Ok(self.schemas.read().values().cloned().collect())
    }
}

/// In-process state for one table: its schema, its ANN
/// container, and the mapping from row keys to internal node
/// ids.
struct TableState {
    schema: TableSchema,
    ann: AnnContainer,
    /// Maps a row key to its `NodeId` in the ANN container.
    key_to_node: HashMap<RowKey, NodeId>,
    /// Inverse: NodeId to key. Allows search results to be
    /// hydrated without round-tripping through the row map.
    node_to_key: HashMap<NodeId, RowKey>,
    /// Monotonic counter for internal node ids. Distinct from
    /// the row key so that re-inserting after a delete does not
    /// collide with the soft-deleted index node.
    next_node_id: NodeId,
}

/// Per-table ANN container. The HNSW path is the default; the
/// turbovec path is selected when the table's codec is one of
/// the `Turbovec*` variants. Both shapes expose the same
/// {insert, delete, search, len} surface so [`VectorStore`] can
/// dispatch on codec without sprinkling enum matches across the
/// hot paths.
enum AnnContainer {
    Hnsw(HnswIndex),
    Turbo(TurboTable),
}

impl AnnContainer {
    fn new(schema: &TableSchema) -> Result<Self, StoreError> {
        if let Some(bits) = schema.codec.turbovec_bits() {
            let table = TurboTable::new(schema.distance, schema.dim, bits)?;
            Ok(Self::Turbo(table))
        } else {
            Ok(Self::Hnsw(HnswIndex::new(schema.distance, schema.hnsw)))
        }
    }

    fn insert(&mut self, id: NodeId, vector: Vec<f32>) -> Result<(), IndexError> {
        match self {
            Self::Hnsw(idx) => idx.insert(id, vector),
            Self::Turbo(t) => t.insert(id, vector),
        }
    }

    fn delete(&mut self, id: NodeId) -> bool {
        match self {
            Self::Hnsw(idx) => idx.delete(id),
            Self::Turbo(t) => t.delete(id),
        }
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<SearchResult>, IndexError> {
        match self {
            Self::Hnsw(idx) => idx.search(query, k, ef),
            Self::Turbo(t) => t.search(query, k, ef),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Hnsw(idx) => idx.len(),
            Self::Turbo(t) => t.len(),
        }
    }
}

/// Per-table store front.
pub struct VectorStore {
    backend: Arc<dyn Backend>,
    tables: RwLock<HashMap<String, Arc<parking_lot::Mutex<TableState>>>>,
}

impl VectorStore {
    /// Build a new store on top of `backend` and rehydrate every
    /// schema / row that the backend already persists. The
    /// rehydration walks every row in every table and rebuilds
    /// the HNSW indexes from scratch; for the MVP this is
    /// preferable to persisting the HNSW topology because the
    /// table sizes we care about (up to ~10 million vectors)
    /// rebuild in seconds.
    ///
    /// # Errors
    ///
    /// Surfaces any backend error encountered during the
    /// rehydration walk.
    pub fn open(backend: Arc<dyn Backend>) -> Result<Self, StoreError> {
        let tables = RwLock::new(HashMap::new());
        let store = Self { backend, tables };
        let schemas = store.backend.list_schemas()?;
        for schema in schemas {
            store.rehydrate_table(&schema)?;
        }
        Ok(store)
    }

    /// Build a fresh in-memory store. Convenience for tests and
    /// embedders that do not need persistence.
    #[must_use]
    pub fn in_memory() -> Self {
        Self {
            backend: Arc::new(MemoryBackend::new()),
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new [`TableSchema`].
    ///
    /// # Errors
    ///
    /// [`StoreError::TableExists`] when the schema's name is
    /// already in use.
    pub fn create_table(&self, schema: TableSchema) -> Result<(), StoreError> {
        let mut tables = self.tables.write();
        if tables.contains_key(&schema.name) {
            return Err(StoreError::TableExists(schema.name));
        }
        let state = TableState {
            schema: schema.clone(),
            ann: AnnContainer::new(&schema)?,
            key_to_node: HashMap::new(),
            node_to_key: HashMap::new(),
            next_node_id: 1,
        };
        self.backend.put_schema(&schema)?;
        tables.insert(
            schema.name.clone(),
            Arc::new(parking_lot::Mutex::new(state)),
        );
        Ok(())
    }

    /// List every registered table.
    pub fn tables(&self) -> Vec<TableSchema> {
        self.tables
            .read()
            .values()
            .map(|s| s.lock().schema.clone())
            .collect()
    }

    /// Insert or overwrite a vector row.
    ///
    /// The `vector` slice is encoded with the table's codec and
    /// fed to the HNSW index in `f32` form. Re-inserts (same
    /// key) replace the prior row in the row store and link a
    /// fresh HNSW node, soft-deleting the prior one.
    ///
    /// # Errors
    ///
    /// [`StoreError::UnknownTable`] when the table is not
    /// registered, [`StoreError::DimensionMismatch`] when the
    /// vector's dimension does not match the table dimension,
    /// and [`StoreError::Encoding`] / [`StoreError::Index`] /
    /// [`StoreError::Backend`] for the underlying failures.
    pub fn upsert(
        &self,
        table: &str,
        key: RowKey,
        vector: &[f32],
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<(), StoreError> {
        let state = self.table_state(table)?;
        let mut state = state.lock();
        let dim = u16::try_from(vector.len()).unwrap_or(u16::MAX);
        if dim != state.schema.dim {
            return Err(StoreError::DimensionMismatch {
                table: table.to_string(),
                expected: state.schema.dim,
                got: dim,
            });
        }
        let codec_encoder = state.schema.codec.encoder();
        let encoded = codec_encoder.encode(vector)?;
        let now = now_millis();
        let prior = self.backend.get_row(table, &key)?;
        let row = VectorRow {
            key: key.clone(),
            vector: encoded,
            metadata,
            created_at: prior.as_ref().map_or(now, |r| r.created_at),
            updated_at: now,
        };
        self.backend.put_row(table, &key, &row)?;
        if let Some(&old_node) = state.key_to_node.get(&key) {
            state.ann.delete(old_node);
            state.node_to_key.remove(&old_node);
        }
        let node_id = state.next_node_id;
        state.next_node_id += 1;
        state.ann.insert(node_id, vector.to_vec())?;
        state.key_to_node.insert(key.clone(), node_id);
        state.node_to_key.insert(node_id, key);
        Ok(())
    }

    /// Fetch the row at `(table, key)`.
    ///
    /// # Errors
    ///
    /// [`StoreError::UnknownTable`] for an unregistered table;
    /// [`StoreError::Backend`] for a backend failure.
    pub fn get(&self, table: &str, key: &[u8]) -> Result<Option<VectorRow>, StoreError> {
        let _ = self.table_state(table)?;
        self.backend.get_row(table, key)
    }

    /// Delete the row at `(table, key)`. Returns `true` when
    /// present.
    ///
    /// # Errors
    ///
    /// [`StoreError::UnknownTable`] for an unregistered table;
    /// [`StoreError::Backend`] for a backend failure.
    pub fn delete(&self, table: &str, key: &[u8]) -> Result<bool, StoreError> {
        let state = self.table_state(table)?;
        let mut state = state.lock();
        let removed = self.backend.delete_row(table, key)?;
        if let Some(node_id) = state.key_to_node.remove(key) {
            state.ann.delete(node_id);
            state.node_to_key.remove(&node_id);
        }
        Ok(removed)
    }

    /// Run a top-`k` ANN search against `table` with `query`.
    ///
    /// `ef` overrides the index's default search beam width.
    ///
    /// # Errors
    ///
    /// [`StoreError::UnknownTable`] for an unregistered table;
    /// [`StoreError::DimensionMismatch`] when the query
    /// dimension does not match the table dimension.
    pub fn search(
        &self,
        table: &str,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<(VectorRow, f32)>, StoreError> {
        let state = self.table_state(table)?;
        let state = state.lock();
        let dim = u16::try_from(query.len()).unwrap_or(u16::MAX);
        if dim != state.schema.dim {
            return Err(StoreError::DimensionMismatch {
                table: table.to_string(),
                expected: state.schema.dim,
                got: dim,
            });
        }
        let hits: Vec<SearchResult> = state.ann.search(query, k, ef)?;
        let mut out = Vec::with_capacity(hits.len());
        for hit in hits {
            if let Some(key) = state.node_to_key.get(&hit.id) {
                if let Some(row) = self.backend.get_row(table, key)? {
                    out.push((row, hit.score));
                }
            }
        }
        Ok(out)
    }

    /// Per-table snapshot statistics: live row count, soft-
    /// deleted node count, dimension, codec.
    ///
    /// # Errors
    ///
    /// [`StoreError::UnknownTable`] for an unregistered table.
    pub fn stats(&self, table: &str) -> Result<TableStats, StoreError> {
        let state = self.table_state(table)?;
        let state = state.lock();
        Ok(TableStats {
            name: state.schema.name.clone(),
            dim: state.schema.dim,
            codec: state.schema.codec,
            distance: state.schema.distance,
            live_rows: state.ann.len(),
            tracked_rows: state.key_to_node.len(),
        })
    }

    fn table_state(&self, table: &str) -> Result<Arc<parking_lot::Mutex<TableState>>, StoreError> {
        self.tables
            .read()
            .get(table)
            .cloned()
            .ok_or_else(|| StoreError::UnknownTable(table.to_string()))
    }

    fn rehydrate_table(&self, schema: &TableSchema) -> Result<(), StoreError> {
        let state = TableState {
            schema: schema.clone(),
            ann: AnnContainer::new(schema)?,
            key_to_node: HashMap::new(),
            node_to_key: HashMap::new(),
            next_node_id: 1,
        };
        let cell = Arc::new(parking_lot::Mutex::new(state));
        self.tables
            .write()
            .insert(schema.name.clone(), cell.clone());
        let mut guard = cell.lock();
        let encoder = guard.schema.codec.encoder();
        let mut to_insert: Vec<(NodeId, RowKey, Vec<f32>)> = Vec::new();
        let table_name = schema.name.clone();
        let mut next = 1u64;
        self.backend.for_each_row(&table_name, &mut |k, row| {
            let v = encoder.decode(&row.vector)?;
            to_insert.push((next, k.to_vec(), v));
            next += 1;
            Ok(())
        })?;
        for (node, key, v) in to_insert {
            guard.ann.insert(node, v)?;
            guard.key_to_node.insert(key.clone(), node);
            guard.node_to_key.insert(node, key);
            guard.next_node_id = node + 1;
        }
        Ok(())
    }
}

/// Snapshot statistics for a table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableStats {
    /// Table name.
    pub name: String,
    /// Frozen vector dimension.
    pub dim: u16,
    /// Storage codec.
    pub codec: Codec,
    /// Distance metric.
    pub distance: Distance,
    /// Live (non-tombstoned) rows in the HNSW index.
    pub live_rows: usize,
    /// Rows currently tracked by the row map.
    pub tracked_rows: usize,
}

fn now_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::HnswParams;

    fn schema(name: &str, dim: u16) -> TableSchema {
        TableSchema {
            name: name.to_string(),
            dim,
            codec: Codec::Int8Quantized,
            distance: Distance::Euclidean,
            hnsw: HnswParams::default(),
        }
    }

    #[test]
    fn create_and_list_tables() {
        let store = VectorStore::in_memory();
        store.create_table(schema("t", 4)).unwrap();
        let tables = store.tables();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "t");
        assert_eq!(tables[0].dim, 4);
    }

    #[test]
    fn duplicate_table_rejected() {
        let store = VectorStore::in_memory();
        store.create_table(schema("t", 4)).unwrap();
        assert!(matches!(
            store.create_table(schema("t", 4)),
            Err(StoreError::TableExists(_))
        ));
    }

    #[test]
    fn upsert_get_delete_round_trip() {
        let store = VectorStore::in_memory();
        store.create_table(schema("t", 3)).unwrap();
        store
            .upsert("t", b"a".to_vec(), &[1.0, 2.0, 3.0], HashMap::new())
            .unwrap();
        let row = store.get("t", b"a").unwrap().expect("row present");
        assert_eq!(row.key, b"a");
        assert_eq!(row.vector.dim, 3);
        assert!(store.delete("t", b"a").unwrap());
        assert!(store.get("t", b"a").unwrap().is_none());
        assert!(!store.delete("t", b"a").unwrap());
    }

    #[test]
    fn dimension_mismatch_rejected() {
        let store = VectorStore::in_memory();
        store.create_table(schema("t", 3)).unwrap();
        assert!(matches!(
            store.upsert("t", b"a".to_vec(), &[1.0, 2.0], HashMap::new()),
            Err(StoreError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn search_returns_nearest_first() {
        let store = VectorStore::in_memory();
        store.create_table(schema("t", 2)).unwrap();
        for (k, v) in [
            (&b"origin"[..], [0.0_f32, 0.0]),
            (&b"unit_x"[..], [1.0, 0.0]),
            (&b"unit_y"[..], [0.0, 1.0]),
            (&b"diag"[..], [1.0, 1.0]),
        ] {
            store.upsert("t", k.to_vec(), &v, HashMap::new()).unwrap();
        }
        let res = store.search("t", &[0.05, 0.05], 1, None).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0.key, b"origin");
    }

    #[test]
    fn rehydrate_rebuilds_index() {
        let backend = Arc::new(MemoryBackend::new());
        let store = VectorStore::open(backend.clone()).unwrap();
        store.create_table(schema("t", 2)).unwrap();
        for i in 0..10_u8 {
            let k = format!("k{i}").into_bytes();
            let v = [f32::from(i), f32::from(i) * 2.0];
            store.upsert("t", k, &v, HashMap::new()).unwrap();
        }
        // Drop the store and reopen on the same backend.
        drop(store);
        let reopened = VectorStore::open(backend).unwrap();
        let stats = reopened.stats("t").unwrap();
        assert_eq!(stats.live_rows, 10);
        let res = reopened.search("t", &[3.0, 6.0], 1, None).unwrap();
        assert_eq!(res[0].0.key, b"k3");
    }

    #[test]
    fn stats_reports_live_rows() {
        let store = VectorStore::in_memory();
        store.create_table(schema("t", 2)).unwrap();
        store
            .upsert("t", b"a".to_vec(), &[1.0, 2.0], HashMap::new())
            .unwrap();
        store
            .upsert("t", b"b".to_vec(), &[3.0, 4.0], HashMap::new())
            .unwrap();
        let s = store.stats("t").unwrap();
        assert_eq!(s.live_rows, 2);
        assert_eq!(s.tracked_rows, 2);
    }
}
