//! Vector index registry.
//!
//! [`VectorRegistry`] is the per-server map of index name to
//! [`VectorTable`]. It is the single source of truth that the
//! FT.* command handlers (Phase C) will consult to dispatch
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
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use crate::vector::schema::{IndexAlgorithm, VectorSchema};
use dynvec::Engine;

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
    pub distance: crate::vector::schema::DistanceMetric,
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
/// dynomite [`crate::core::context::Context`]) and clone the
/// returned handle freely; clones share state.
#[derive(Clone, Default)]
pub struct VectorRegistry {
    inner: Arc<RwLock<BTreeMap<String, Arc<VectorTable>>>>,
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
        let table = VectorTable {
            name: name.clone(),
            schema,
            engine,
            indexed_keys: Mutex::new(BTreeSet::new()),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::schema::{DistanceMetric, IndexAlgorithm, VectorType};

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
