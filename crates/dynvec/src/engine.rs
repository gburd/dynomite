//! Per-table vector engine handle.
//!
//! [`Engine`] is the public unit the Redis-Stack-style FT.*
//! command handlers in `dynomite::vector` hand out. Each engine
//! owns one [`VectorStore`] table; concurrent engines for
//! different tables are independent.
//!
//! The shape is intentionally narrow: the FT.* command
//! pathway only needs upsert / get / delete / search / stats /
//! drop, plus a few accessors for introspection.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use crate::storage::{RowKey, StoreError, TableSchema, TableStats, VectorRow, VectorStore};

/// Per-table engine handle.
///
/// One engine wraps one [`VectorStore`] table. The store is
/// held behind an [`Arc`] so multiple handles can share the
/// same underlying state (the registry never duplicates a
/// store; it just hands out further [`Engine`] clones).
#[derive(Clone)]
pub struct Engine {
    store: Arc<VectorStore>,
    table: String,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

impl Engine {
    /// Build a fresh in-memory engine for `schema`.
    ///
    /// The schema's `name` becomes the engine's table name. The
    /// returned engine is the only handle on the new store; if
    /// the registry wants a second handle it should
    /// [`Engine::clone`] this one.
    ///
    /// # Errors
    ///
    /// Surfaces any [`StoreError`] from
    /// [`VectorStore::create_table`].
    pub fn in_memory(schema: TableSchema) -> Result<Self, StoreError> {
        let store = Arc::new(VectorStore::in_memory());
        let table = schema.name.clone();
        store.create_table(schema)?;
        Ok(Self { store, table })
    }

    /// Wrap an existing [`VectorStore`] that already holds the
    /// table. Used by embedders that want to share a store
    /// across many engines.
    #[must_use]
    pub fn with_store(store: Arc<VectorStore>, table: String) -> Self {
        Self { store, table }
    }

    /// Bound table name.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table
    }

    /// Borrow the underlying [`VectorStore`].
    #[must_use]
    pub fn store(&self) -> &Arc<VectorStore> {
        &self.store
    }

    /// Insert or overwrite a vector row.
    ///
    /// # Errors
    ///
    /// Forwards every [`StoreError`] from
    /// [`VectorStore::upsert`].
    pub fn upsert(
        &self,
        key: RowKey,
        vector: &[f32],
        metadata: HashMap<String, Value>,
    ) -> Result<(), StoreError> {
        self.store.upsert(&self.table, key, vector, metadata)
    }

    /// Fetch the row at `key`.
    ///
    /// # Errors
    ///
    /// Forwards every [`StoreError`] from [`VectorStore::get`].
    pub fn get(&self, key: &[u8]) -> Result<Option<VectorRow>, StoreError> {
        self.store.get(&self.table, key)
    }

    /// Delete the row at `key`.
    ///
    /// # Errors
    ///
    /// Forwards every [`StoreError`] from
    /// [`VectorStore::delete`].
    pub fn delete(&self, key: &[u8]) -> Result<bool, StoreError> {
        self.store.delete(&self.table, key)
    }

    /// Run a top-`k` ANN search.
    ///
    /// # Errors
    ///
    /// Forwards every [`StoreError`] from
    /// [`VectorStore::search`].
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<(VectorRow, f32)>, StoreError> {
        self.store.search(&self.table, query, k, ef)
    }

    /// Per-table snapshot statistics.
    ///
    /// # Errors
    ///
    /// Forwards every [`StoreError`] from
    /// [`VectorStore::stats`].
    pub fn stats(&self) -> Result<TableStats, StoreError> {
        self.store.stats(&self.table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::Distance;
    use crate::encoding::Codec;
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
    fn engine_round_trips_a_row() {
        let engine = Engine::in_memory(schema("t", 3)).unwrap();
        engine
            .upsert(b"a".to_vec(), &[1.0, 2.0, 3.0], HashMap::new())
            .unwrap();
        let row = engine.get(b"a").unwrap().expect("row present");
        assert_eq!(row.key, b"a");
        assert_eq!(row.vector.dim, 3);
        assert_eq!(engine.table_name(), "t");
        let stats = engine.stats().unwrap();
        assert_eq!(stats.live_rows, 1);
    }

    #[test]
    fn engine_search_returns_nearest_first() {
        let engine = Engine::in_memory(schema("t", 2)).unwrap();
        for (k, v) in [
            (&b"origin"[..], [0.0_f32, 0.0]),
            (&b"unit_x"[..], [1.0, 0.0]),
            (&b"unit_y"[..], [0.0, 1.0]),
        ] {
            engine.upsert(k.to_vec(), &v, HashMap::new()).unwrap();
        }
        let res = engine.search(&[0.05, 0.05], 1, None).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0.key, b"origin");
    }
}
