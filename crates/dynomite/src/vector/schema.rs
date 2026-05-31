//! Schema types for the vector subsystem.
//!
//! These types model the subset of the RediSearch FT.CREATE
//! grammar that `dynomite::vector` will eventually parse and
//! act on. Phase B (this commit) lands the data shapes; Phase
//! C will add the FT.* command parser that builds them and
//! routes through [`super::registry::VectorRegistry`].
//!
//! The schema types are intentionally distinct from the engine
//! types in [`dynvec`]. The engine types are storage-level (a
//! [`dynvec::Codec`] is a storage codec, a [`dynvec::Distance`]
//! is a scoring function); the schema types are the
//! protocol-level shape clients send over the wire. The
//! conversion functions on [`VectorType`], [`DistanceMetric`],
//! and [`IndexAlgorithm`] turn one into the other.

use serde::{Deserialize, Serialize};

use dynvec::distance::Distance as EngineDistance;
use dynvec::encoding::Codec as EngineCodec;

/// RediSearch-flavoured vector field type.
///
/// Today the engine supports `Float32` (mapped to
/// [`EngineCodec::Fp16`] on disk; the API is float32-in /
/// float32-out) and `Float16` (the same Fp16 codec exposed
/// directly). `Int8` is reserved for the
/// [`EngineCodec::Int8Quantized`] path. Future codecs (PQ, BQ)
/// will land as new variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum VectorType {
    /// IEEE 754 single-precision floats (RediSearch `FLOAT32`).
    Float32,
    /// IEEE 754 half-precision floats (RediSearch `FLOAT16`).
    Float16,
    /// Per-vector int8 quantisation. Not in stock RediSearch
    /// but exposed under our extension namespace.
    Int8,
}

impl VectorType {
    /// Map to the storage-layer codec.
    #[must_use]
    pub fn engine_codec(self) -> EngineCodec {
        match self {
            Self::Float32 | Self::Float16 => EngineCodec::Fp16,
            Self::Int8 => EngineCodec::Int8Quantized,
        }
    }
}

/// RediSearch distance metric.
///
/// Mirrors the wire-level keywords (`L2`, `IP`, `COSINE`).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DistanceMetric {
    /// Squared L2 distance.
    L2,
    /// Inner product (negated dot product, smaller-is-closer).
    InnerProduct,
    /// Cosine distance.
    Cosine,
}

impl DistanceMetric {
    /// Map to the engine's distance enum.
    #[must_use]
    pub fn engine_distance(self) -> EngineDistance {
        match self {
            Self::L2 => EngineDistance::Euclidean,
            Self::InnerProduct => EngineDistance::DotProduct,
            Self::Cosine => EngineDistance::Cosine,
        }
    }
}

/// Index algorithm.
///
/// RediSearch ships HNSW and FLAT (brute-force). This crate
/// only implements HNSW today; the FLAT variant exists so the
/// FT.CREATE parser can record the operator's request and the
/// command handler can return a `not-implemented` error
/// rather than silently downgrade.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum IndexAlgorithm {
    /// Hierarchical navigable small world graph.
    Hnsw,
    /// Brute-force scan (every vector compared on every
    /// query).
    Flat,
}

/// RediSearch metadata field type.
///
/// FT.CREATE accepts schema fields that decorate the indexed
/// document with searchable metadata. Phase B (this commit)
/// records the field set; Phase C will wire the per-field
/// search semantics into the FT.SEARCH evaluator.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum MetadataFieldType {
    /// Tokenised free text (RediSearch `TEXT`).
    Text,
    /// Numeric range (RediSearch `NUMERIC`).
    Numeric,
    /// Discrete tag (RediSearch `TAG`).
    Tag,
    /// Geographic coordinate (RediSearch `GEO`).
    Geo,
}

/// One non-vector schema field.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataField {
    /// Field name on the underlying hash document.
    pub name: String,
    /// Field type.
    pub field_type: MetadataFieldType,
    /// Per-tag separator for `TAG` fields. `None` selects the
    /// RediSearch default (`,`). Ignored for non-`TAG` field
    /// types. The separator is a single ASCII byte; the
    /// FT.CREATE parser rejects multi-byte strings.
    #[serde(default)]
    pub tag_separator: Option<u8>,
}

impl MetadataField {
    /// Effective TAG separator. Returns the configured value
    /// for `TAG` fields, or `b','` (the RediSearch default)
    /// when none was supplied. Defined for non-`TAG` fields
    /// too so callers can use it uniformly; the value is
    /// meaningless outside the `TAG` path.
    #[must_use]
    pub fn effective_tag_separator(&self) -> u8 {
        self.tag_separator.unwrap_or(b',')
    }
}

/// Compiled FT.CREATE schema.
///
/// One [`VectorSchema`] describes one index: the vector field,
/// its dimension and codec, the distance metric, the chosen
/// algorithm, the prefix set (FT.CREATE `PREFIX <n> ...`), and
/// the set of non-vector metadata fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VectorSchema {
    /// Name of the document field that carries the vector.
    pub vector_field: String,
    /// Codec.
    pub vector_type: VectorType,
    /// Frozen vector dimension.
    pub dim: u16,
    /// Distance metric.
    pub distance: DistanceMetric,
    /// Index algorithm.
    pub algorithm: IndexAlgorithm,
    /// Document key prefixes (`FT.CREATE ... PREFIX <n>
    /// <prefix>...`). HSET commands whose key starts with any
    /// of these prefixes get intercepted and indexed.
    pub prefixes: Vec<Vec<u8>>,
    /// Non-vector schema fields.
    pub metadata_fields: Vec<MetadataField>,
}

impl VectorSchema {
    /// Build the corresponding [`dynvec::TableSchema`] for a
    /// table named `table_name`.
    ///
    /// Picks default HNSW tuning; future revisions will pull
    /// the parameters out of the FT.CREATE clause.
    #[must_use]
    pub fn to_engine_schema(&self, table_name: &str) -> dynvec::storage::TableSchema {
        dynvec::storage::TableSchema {
            name: table_name.to_string(),
            dim: self.dim,
            codec: self.vector_type.engine_codec(),
            distance: self.distance.engine_distance(),
            hnsw: dynvec::index::HnswParams::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_type_maps_to_engine_codec() {
        assert_eq!(VectorType::Float32.engine_codec(), EngineCodec::Fp16);
        assert_eq!(VectorType::Float16.engine_codec(), EngineCodec::Fp16);
        assert_eq!(VectorType::Int8.engine_codec(), EngineCodec::Int8Quantized);
    }

    #[test]
    fn distance_metric_maps_to_engine_distance() {
        assert_eq!(
            DistanceMetric::L2.engine_distance(),
            EngineDistance::Euclidean
        );
        assert_eq!(
            DistanceMetric::InnerProduct.engine_distance(),
            EngineDistance::DotProduct
        );
        assert_eq!(
            DistanceMetric::Cosine.engine_distance(),
            EngineDistance::Cosine
        );
    }

    #[test]
    fn schema_compiles_to_engine_schema() {
        let schema = VectorSchema {
            vector_field: "vec".to_string(),
            vector_type: VectorType::Float32,
            dim: 16,
            distance: DistanceMetric::Cosine,
            algorithm: IndexAlgorithm::Hnsw,
            prefixes: Vec::new(),
            metadata_fields: vec![MetadataField {
                name: "title".to_string(),
                field_type: MetadataFieldType::Text,
                tag_separator: None,
            }],
        };
        let engine = schema.to_engine_schema("docs");
        assert_eq!(engine.name, "docs");
        assert_eq!(engine.dim, 16);
        assert_eq!(engine.codec, EngineCodec::Fp16);
        assert_eq!(engine.distance, EngineDistance::Cosine);
    }
}
