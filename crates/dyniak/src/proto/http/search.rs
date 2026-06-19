//! Text-search and vector-KNN bridge over Riak buckets.
//!
//! `proto::http::search` lights up the optional search surface the
//! HTTP gateway exposes when the crate is built with the `search`
//! Cargo feature and the server is wired with a
//! [`dynomite_search::VectorRegistry`]. It maps a small REST surface
//! onto the registry's text and vector machinery so operators can
//! declare per-field indexes on the objects they `PUT` and query them
//! back as matching keys.
//!
//! # Index-to-field mapping
//!
//! The object body that a `PUT /buckets/{b}/keys/{k}` stores is an
//! [`crate::proto::http::object::HttpObject`] whose `value` payload is
//! interpreted, for indexing purposes, as a JSON document (a JSON
//! object at the top level). The mapping from logical document fields
//! to indexes is:
//!
//! * **Text indexes** -- `PUT /buckets/{b}/index/text/{field}`
//!   declares a text index on the document key `{field}`. On every
//!   subsequent object write whose JSON payload carries a string value
//!   under `{field}`, that string is fed into the trigram-backed text
//!   index for the bucket. Substring and (approximate) regex queries
//!   run against it.
//! * **Vector indexes** -- `POST /buckets/{b}/index/vector` creates a
//!   single vector index for the bucket. The JSON body selects the
//!   dimension, distance metric, codec, and (optionally) the document
//!   field that carries the vector (default `_vector`). On every
//!   object write whose JSON payload carries a numeric array under
//!   that field, the array is upserted into the HNSW engine keyed by
//!   the object key; the remaining top-level document fields are
//!   carried along as row metadata so KNN queries can post-filter on
//!   them.
//!
//! Two registry index names back each bucket: `text:{bucket}` holds
//! the text fields and `vec:{bucket}` holds the vector engine. (A
//! bucket whose name embeds a `:` could in principle collide with this
//! scheme; bucket names in practice are flat identifiers, so the
//! collision is documented rather than guarded.)
//!
//! When the server is built with the `search` feature but no registry
//! was wired in, every route in this module replies
//! `501 Not Implemented`, mirroring how the transaction endpoint
//! behaves against a non-transactional backend.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use hyper::header::{ACCEPT, CONTENT_TYPE};
use hyper::{HeaderMap, Response, StatusCode};
use prost::Message;
use serde::{Deserialize, Serialize};

use dyn_encoding::{CborCodec, CodecRegistry, JsonCodec, ProtobufCodec, WireTypeId, WireValue};
use dynomite_search::{DistanceMetric, IndexAlgorithm, VectorRegistry, VectorSchema, VectorType};

use crate::proto::http::content_type::select_codec;
use crate::proto::http::routes::{
    encoded_response, header_str, header_str_opt, not_acceptable_response, text_response,
    ResponseBody,
};

/// Default number of nearest neighbours returned by a KNN query when
/// the request body omits `k`.
const DEFAULT_K: usize = 10;

/// Default document field that carries the vector payload when an
/// index-create body omits `field`.
const DEFAULT_VECTOR_FIELD: &str = "_vector";

/// Shared per-server search state: the vector-index registry the
/// HTTP search routes consult.
///
/// The state is cheap to clone (the registry is an [`Arc`] internally)
/// and is wired into the gateway alongside the datastore by
/// [`crate::serve_http_with_search`].
#[derive(Clone)]
pub struct SearchState {
    registry: Arc<VectorRegistry>,
}

impl std::fmt::Debug for SearchState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchState").finish_non_exhaustive()
    }
}

impl SearchState {
    /// Wrap a [`VectorRegistry`] handle in a [`SearchState`].
    #[must_use]
    pub fn new(registry: Arc<VectorRegistry>) -> Self {
        Self { registry }
    }

    /// Borrow the wrapped registry.
    #[must_use]
    pub fn registry(&self) -> &Arc<VectorRegistry> {
        &self.registry
    }

    /// Registry index name backing the text fields of `bucket`.
    fn text_index(bucket: &str) -> String {
        format!("text:{bucket}")
    }

    /// Registry index name backing the vector engine of `bucket`.
    fn vector_index(bucket: &str) -> String {
        format!("vec:{bucket}")
    }

    /// Declare a text index on `field` of `bucket`.
    ///
    /// Lazily creates the bucket's text-index table with a minimal
    /// placeholder vector schema (the table's vector engine is unused
    /// for a pure text index) and provisions a trigram index for the
    /// field. Returns `true` when a new field slot was provisioned,
    /// `false` when the field was already declared.
    fn declare_text_field(&self, bucket: &str, field: &str) -> Result<bool, SearchError> {
        let name = Self::text_index(bucket);
        if self.registry.get(&name).is_none() {
            // The placeholder schema gives the table a (tiny, unused)
            // vector engine; the text fields live in the runtime
            // text-index map provisioned below.
            let schema = VectorSchema {
                vector_field: DEFAULT_VECTOR_FIELD.to_string(),
                vector_type: VectorType::Float32,
                dim: 1,
                distance: DistanceMetric::Cosine,
                algorithm: IndexAlgorithm::Hnsw,
                prefixes: Vec::new(),
                metadata_fields: Vec::new(),
            };
            // A concurrent declare may have created the table between
            // the `get` and here; `AlreadyExists` is benign.
            match self.registry.create(name.clone(), schema) {
                Ok(()) | Err(dynomite_search::RegistryError::AlreadyExists(_)) => {}
                Err(e) => return Err(SearchError::Registry(e.to_string())),
            }
        }
        let table = self
            .registry
            .get(&name)
            .ok_or_else(|| SearchError::Registry("text index vanished after create".to_string()))?;
        Ok(table.add_text_field(field))
    }

    /// Create the vector index for `bucket` from a decoded request.
    fn create_vector_index(
        &self,
        bucket: &str,
        req: &VectorIndexCreate,
    ) -> Result<(), SearchError> {
        if req.dim == 0 {
            return Err(SearchError::BadRequest("dim must be >= 1".to_string()));
        }
        let schema = VectorSchema {
            vector_field: req
                .field
                .clone()
                .unwrap_or_else(|| DEFAULT_VECTOR_FIELD.to_string()),
            vector_type: parse_codec(req.codec.as_deref())?,
            dim: req.dim,
            distance: parse_metric(req.metric.as_deref())?,
            algorithm: IndexAlgorithm::Hnsw,
            prefixes: Vec::new(),
            metadata_fields: Vec::new(),
        };
        match self.registry.create(Self::vector_index(bucket), schema) {
            Ok(()) => Ok(()),
            Err(e @ dynomite_search::RegistryError::AlreadyExists(_)) => {
                Err(SearchError::AlreadyExists(e.to_string()))
            }
            Err(e) => Err(SearchError::Registry(e.to_string())),
        }
    }

    /// Feed the declared indexes for `bucket` from a freshly written
    /// object payload.
    ///
    /// `value` is the object's payload (the `value` field of the
    /// stored [`crate::proto::http::object::HttpObject`]). It is parsed
    /// as a JSON document; failures to parse, missing fields, and
    /// per-field type mismatches are silently skipped so that the
    /// object write itself is never affected by indexing. Genuine
    /// engine errors on the vector path are logged and swallowed for
    /// the same reason.
    pub fn index_object(&self, bucket: &str, key: &[u8], value: &[u8]) {
        let Ok(doc) = serde_json::from_slice::<serde_json::Value>(value) else {
            return;
        };
        let Some(obj) = doc.as_object() else {
            return;
        };

        if let Some(table) = self.registry.get(&Self::text_index(bucket)) {
            for field in table.text_field_names() {
                if let Some(text) = obj.get(&field).and_then(serde_json::Value::as_str) {
                    table.upsert_text_field(&field, key, text.as_bytes());
                }
            }
        }

        if let Some(table) = self.registry.get(&Self::vector_index(bucket)) {
            let field = table.schema.vector_field.clone();
            if let Some(vector) = obj.get(&field).and_then(json_to_f32_vec) {
                let metadata = row_metadata(obj, &field);
                if let Err(e) = table.engine.upsert(key.to_vec(), &vector, metadata) {
                    tracing::warn!(
                        bucket,
                        key = %String::from_utf8_lossy(key),
                        error = %e,
                        "vector index upsert failed; object stored, row not indexed"
                    );
                }
            }
        }
    }

    /// Substring search over the text index for `bucket`/`field`.
    fn search_substring(
        &self,
        bucket: &str,
        field: &str,
        query: &[u8],
    ) -> Result<Vec<TextHitJson>, SearchError> {
        let table = self
            .registry
            .get(&Self::text_index(bucket))
            .ok_or(SearchError::NoTextField)?;
        let hits = table
            .search_text_substring(field, query)
            .ok_or(SearchError::NoTextField)?;
        Ok(render_text_hits(hits))
    }

    /// Regex search over the text index for `bucket`/`field`. `k == 0`
    /// runs an exact regex match; `k >= 1` runs an approximate match
    /// with up to `k` edit operations.
    fn search_regex(
        &self,
        bucket: &str,
        field: &str,
        pattern: &str,
        k: u16,
    ) -> Result<Vec<TextHitJson>, SearchError> {
        let table = self
            .registry
            .get(&Self::text_index(bucket))
            .ok_or(SearchError::NoTextField)?;
        if k == 0 {
            match table.search_text_regex(field, pattern) {
                None => Err(SearchError::NoTextField),
                Some(Ok(hits)) => Ok(render_text_hits(hits)),
                Some(Err(e)) => Err(SearchError::BadRequest(format!("regex: {e}"))),
            }
        } else {
            match table.search_text_regex_approx(field, pattern, k) {
                None => Err(SearchError::NoTextField),
                Some(Ok(hits)) => Ok(render_text_hits(hits)),
                Some(Err(e)) => Err(SearchError::BadRequest(format!("regex: {e}"))),
            }
        }
    }

    /// KNN search over the vector engine for `bucket`.
    fn search_vector(
        &self,
        bucket: &str,
        req: &VectorQuery,
    ) -> Result<Vec<VectorHitJson>, SearchError> {
        let table = self
            .registry
            .get(&Self::vector_index(bucket))
            .ok_or(SearchError::NoVectorIndex)?;
        let k = req.k.max(1);
        // Over-fetch when a metadata filter is in play so post-filtering
        // does not starve the result set.
        let fetch = if req.filter.is_some() {
            k.saturating_mul(4).max(k)
        } else {
            k
        };
        let raw = table
            .engine
            .search(&req.query, fetch, req.ef)
            .map_err(|e| SearchError::BadRequest(format!("vector search: {e}")))?;
        let mut hits = Vec::new();
        for (row, distance) in raw {
            if let Some(filter) = &req.filter {
                if !metadata_matches(&row.metadata, filter) {
                    continue;
                }
            }
            hits.push(VectorHitJson {
                key: String::from_utf8_lossy(&row.key).into_owned(),
                distance,
            });
            if hits.len() >= k {
                break;
            }
        }
        Ok(hits)
    }

    /// Summarise the indexes declared for `bucket`.
    fn list_indexes(&self, bucket: &str) -> IndexListResponse {
        let text_fields = self
            .registry
            .get(&Self::text_index(bucket))
            .map(|t| t.text_field_names())
            .unwrap_or_default();
        let vector = self.registry.get(&Self::vector_index(bucket)).map(|t| {
            let live_rows = t.engine.stats().map(|s| s.live_rows as u64).unwrap_or(0);
            VectorIndexInfoJson {
                name: t.name.clone(),
                field: t.schema.vector_field.clone(),
                dim: u32::from(t.schema.dim),
                metric: metric_label(t.schema.distance).to_string(),
                codec: codec_label(t.schema.vector_type).to_string(),
                live_rows,
            }
        });
        IndexListResponse {
            bucket: bucket.to_string(),
            text_fields,
            vector,
        }
    }
}

/// Internal error shape for the search handlers.
#[derive(Debug)]
enum SearchError {
    /// No text field by that name is declared for the bucket.
    NoTextField,
    /// No vector index is declared for the bucket.
    NoVectorIndex,
    /// A vector index already exists for the bucket.
    AlreadyExists(String),
    /// The request was malformed (bad regex, dim mismatch, ...).
    BadRequest(String),
    /// The registry refused the operation.
    Registry(String),
}

impl SearchError {
    /// Map onto an HTTP error response.
    fn into_response(self) -> Response<ResponseBody> {
        match self {
            Self::NoTextField => text_response(StatusCode::NOT_FOUND, "no text index for field"),
            Self::NoVectorIndex => {
                text_response(StatusCode::NOT_FOUND, "no vector index for bucket")
            }
            Self::AlreadyExists(m) => text_response(StatusCode::CONFLICT, &m),
            Self::BadRequest(m) => text_response(StatusCode::BAD_REQUEST, &m),
            Self::Registry(m) => {
                text_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("registry: {m}"))
            }
        }
    }
}

// ------------------------------------------------------------------
// Route handlers (called from `routes::handle_route`).
// ------------------------------------------------------------------

/// `PUT /buckets/{bucket}/index/text/{field}` -- declare a text index.
pub(crate) fn declare_text_index(
    state: Option<&SearchState>,
    bucket: &str,
    field: &str,
    headers: &HeaderMap,
) -> Response<ResponseBody> {
    let Some(state) = state else {
        return not_configured();
    };
    let Some(ct) = negotiate(headers) else {
        return not_acceptable_response();
    };
    match state.declare_text_field(bucket, field) {
        Ok(created) => encode_value(
            ct,
            &IndexAck {
                bucket: bucket.to_string(),
                kind: "text".to_string(),
                field: field.to_string(),
                created,
            },
        ),
        Err(e) => e.into_response(),
    }
}

/// `POST /buckets/{bucket}/index/vector` -- create a vector index.
pub(crate) fn create_vector_index(
    state: Option<&SearchState>,
    bucket: &str,
    headers: &HeaderMap,
    body: &Bytes,
) -> Response<ResponseBody> {
    let Some(state) = state else {
        return not_configured();
    };
    let Some(ct) = negotiate(headers) else {
        return not_acceptable_response();
    };
    let req: VectorIndexCreate = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            return text_response(StatusCode::BAD_REQUEST, &format!("index decode: {e}"));
        }
    };
    match state.create_vector_index(bucket, &req) {
        Ok(()) => encode_value(
            ct,
            &IndexAck {
                bucket: bucket.to_string(),
                kind: "vector".to_string(),
                field: req
                    .field
                    .unwrap_or_else(|| DEFAULT_VECTOR_FIELD.to_string()),
                created: true,
            },
        ),
        Err(e) => e.into_response(),
    }
}

/// `GET /buckets/{bucket}/index` -- list declared indexes.
pub(crate) fn list_indexes(
    state: Option<&SearchState>,
    bucket: &str,
    headers: &HeaderMap,
) -> Response<ResponseBody> {
    let Some(state) = state else {
        return not_configured();
    };
    let Some(ct) = negotiate(headers) else {
        return not_acceptable_response();
    };
    encode_value(ct, &state.list_indexes(bucket))
}

/// `GET /buckets/{bucket}/search/text/{field}?q=<substr>`.
pub(crate) fn search_text(
    state: Option<&SearchState>,
    bucket: &str,
    field: &str,
    query: Option<&str>,
    headers: &HeaderMap,
) -> Response<ResponseBody> {
    let Some(state) = state else {
        return not_configured();
    };
    let Some(ct) = negotiate(headers) else {
        return not_acceptable_response();
    };
    let Some(q) = query_param(query, "q") else {
        return text_response(StatusCode::BAD_REQUEST, "missing query parameter `q`");
    };
    match state.search_substring(bucket, field, q.as_bytes()) {
        Ok(hits) => encode_value(
            ct,
            &TextSearchResponse {
                field: field.to_string(),
                hits,
            },
        ),
        Err(e) => e.into_response(),
    }
}

/// `GET /buckets/{bucket}/search/regex/{field}?pattern=<re>&k=<edits>`.
pub(crate) fn search_regex(
    state: Option<&SearchState>,
    bucket: &str,
    field: &str,
    query: Option<&str>,
    headers: &HeaderMap,
) -> Response<ResponseBody> {
    let Some(state) = state else {
        return not_configured();
    };
    let Some(ct) = negotiate(headers) else {
        return not_acceptable_response();
    };
    let Some(pattern) = query_param(query, "pattern") else {
        return text_response(StatusCode::BAD_REQUEST, "missing query parameter `pattern`");
    };
    let k = match query_param(query, "k") {
        None => 0u16,
        Some(s) => match s.parse::<u16>() {
            Ok(v) => v,
            Err(_) => {
                return text_response(StatusCode::BAD_REQUEST, "k must be a non-negative integer");
            }
        },
    };
    match state.search_regex(bucket, field, &pattern, k) {
        Ok(hits) => encode_value(
            ct,
            &TextSearchResponse {
                field: field.to_string(),
                hits,
            },
        ),
        Err(e) => e.into_response(),
    }
}

/// `POST /buckets/{bucket}/search/vector` -- KNN query.
pub(crate) fn search_vector(
    state: Option<&SearchState>,
    bucket: &str,
    headers: &HeaderMap,
    body: &Bytes,
) -> Response<ResponseBody> {
    let Some(state) = state else {
        return not_configured();
    };
    let Some(ct) = negotiate(headers) else {
        return not_acceptable_response();
    };
    let req: VectorQuery = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            return text_response(StatusCode::BAD_REQUEST, &format!("query decode: {e}"));
        }
    };
    if req.query.is_empty() {
        return text_response(StatusCode::BAD_REQUEST, "query vector must not be empty");
    }
    match state.search_vector(bucket, &req) {
        Ok(hits) => encode_value(ct, &VectorSearchResponse { hits }),
        Err(e) => e.into_response(),
    }
}

/// `501 Not Implemented` reply used when no registry is configured.
fn not_configured() -> Response<ResponseBody> {
    text_response(
        StatusCode::NOT_IMPLEMENTED,
        "the server was not configured with a search registry",
    )
}

// ------------------------------------------------------------------
// Negotiation + encoding helpers.
// ------------------------------------------------------------------

/// Pick the response content-type from the request headers, honoring
/// the `Accept` header and falling back to the request `Content-Type`.
fn negotiate(headers: &HeaderMap) -> Option<&'static str> {
    let accept = header_str(headers, ACCEPT);
    let req_ct = header_str_opt(headers, CONTENT_TYPE);
    select_codec(accept, req_ct)
}

/// Encode `value` under the negotiated codec `ct` into a `200 OK`
/// response. A codec-registry miss is a `406`; an encode failure is a
/// `500`.
fn encode_value<T: WireValue>(ct: &'static str, value: &T) -> Response<ResponseBody> {
    let Some(codec) = search_codecs().for_content_type(ct) else {
        return not_acceptable_response();
    };
    match codec.encode(value) {
        Ok(bytes) => encoded_response(ct, bytes),
        Err(e) => text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("response encode: {e}"),
        ),
    }
}

/// Codec registry shared by the search responses.
///
/// Holds JSON, CBOR, and protobuf codecs with every search response
/// type registered, keyed by the canonical content-type strings the
/// gateway negotiates.
fn search_codecs() -> &'static CodecRegistry {
    use std::sync::OnceLock;
    static REGISTRY: OnceLock<CodecRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut json = JsonCodec::new();
        register_all_json(&mut json);
        let mut cbor = CborCodec::new();
        register_all_cbor(&mut cbor);
        let mut protobuf = ProtobufCodec::new();
        register_all_protobuf(&mut protobuf);

        let mut registry = CodecRegistry::new();
        registry.register(json);
        registry.register(cbor);
        registry.register(protobuf);
        registry
    })
}

fn register_all_json(codec: &mut JsonCodec) {
    codec.register::<TextSearchResponse>();
    codec.register::<VectorSearchResponse>();
    codec.register::<IndexListResponse>();
    codec.register::<IndexAck>();
}

fn register_all_cbor(codec: &mut CborCodec) {
    codec.register::<TextSearchResponse>();
    codec.register::<VectorSearchResponse>();
    codec.register::<IndexListResponse>();
    codec.register::<IndexAck>();
}

fn register_all_protobuf(codec: &mut ProtobufCodec) {
    codec.register::<TextSearchResponse>();
    codec.register::<VectorSearchResponse>();
    codec.register::<IndexListResponse>();
    codec.register::<IndexAck>();
}

// ------------------------------------------------------------------
// Field / value mapping helpers.
// ------------------------------------------------------------------

/// Render registry text hits as the wire response shape.
fn render_text_hits(hits: Vec<dynomite_search::TextHit>) -> Vec<TextHitJson> {
    hits.into_iter()
        .map(|(key, text)| TextHitJson {
            key: String::from_utf8_lossy(&key).into_owned(),
            text: String::from_utf8_lossy(&text).into_owned(),
        })
        .collect()
}

/// Interpret a JSON value as a vector of `f32`. Returns `None` when the
/// value is not an array of numbers. Each element is converted through
/// serde's own numeric deserialiser, so a non-numeric element yields
/// `None` rather than a lossy cast in this crate.
fn json_to_f32_vec(value: &serde_json::Value) -> Option<Vec<f32>> {
    let arr = value.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let n: f32 = serde_json::from_value(item.clone()).ok()?;
        out.push(n);
    }
    Some(out)
}

/// Build the row metadata map for a document, excluding the field that
/// carries the vector itself.
fn row_metadata(
    obj: &serde_json::Map<String, serde_json::Value>,
    vector_field: &str,
) -> HashMap<String, serde_json::Value> {
    obj.iter()
        .filter(|(k, _)| k.as_str() != vector_field)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// True when every constraint in `filter` is matched exactly by the
/// row's metadata.
fn metadata_matches(
    metadata: &HashMap<String, serde_json::Value>,
    filter: &BTreeMap<String, serde_json::Value>,
) -> bool {
    filter
        .iter()
        .all(|(k, want)| metadata.get(k).is_some_and(|got| got == want))
}

/// Parse a distance-metric label. Defaults to cosine.
fn parse_metric(label: Option<&str>) -> Result<DistanceMetric, SearchError> {
    match label.map(str::to_ascii_lowercase).as_deref() {
        None | Some("cosine") => Ok(DistanceMetric::Cosine),
        Some("l2" | "euclidean") => Ok(DistanceMetric::L2),
        Some("ip" | "inner_product" | "dot") => Ok(DistanceMetric::InnerProduct),
        Some(other) => Err(SearchError::BadRequest(format!("unknown metric `{other}`"))),
    }
}

/// Parse a codec label. Defaults to float32.
fn parse_codec(label: Option<&str>) -> Result<VectorType, SearchError> {
    match label.map(str::to_ascii_lowercase).as_deref() {
        None | Some("float32" | "f32") => Ok(VectorType::Float32),
        Some("float16" | "f16") => Ok(VectorType::Float16),
        Some("int8") => Ok(VectorType::Int8),
        Some(other) => Err(SearchError::BadRequest(format!("unknown codec `{other}`"))),
    }
}

/// Stable label for a distance metric (used in the list response).
fn metric_label(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::L2 => "l2",
        DistanceMetric::InnerProduct => "ip",
        _ => "unknown",
    }
}

/// Stable label for a codec (used in the list response).
fn codec_label(codec: VectorType) -> &'static str {
    match codec {
        VectorType::Float32 => "float32",
        VectorType::Float16 => "float16",
        VectorType::Int8 => "int8",
        _ => "unknown",
    }
}

/// Extract a single query-string parameter, percent-decoding its value.
///
/// `query` is the raw `&`-separated query string (the part after `?`).
/// Returns `None` when the key is absent. `+` is decoded to a space
/// (the conventional `application/x-www-form-urlencoded` rule).
fn query_param(query: Option<&str>, key: &str) -> Option<String> {
    let q = query?;
    for pair in q.split('&') {
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("");
        if k == key {
            let raw = it.next().unwrap_or("");
            return Some(percent_decode(raw));
        }
    }
    None
}

/// Decode percent-escapes and `+`-as-space in a query-string value.
/// Invalid escapes are passed through verbatim. The output is a UTF-8
/// string built from the decoded bytes (lossily, so a malformed escape
/// sequence cannot panic).
fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = hex_val(bytes[i + 1]);
                let lo = hex_val(bytes[i + 2]);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi << 4) | lo);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Map an ASCII hex digit to its value, or `None` when not hex.
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ------------------------------------------------------------------
// Request / response wire types.
// ------------------------------------------------------------------

/// Body of `POST /buckets/{bucket}/index/vector`.
#[derive(Debug, Default, Deserialize)]
struct VectorIndexCreate {
    /// Vector dimension (required, >= 1).
    dim: u16,
    /// Distance metric label (`cosine` default).
    #[serde(default)]
    metric: Option<String>,
    /// Codec label (`float32` default).
    #[serde(default)]
    codec: Option<String>,
    /// Document field carrying the vector (`_vector` default).
    #[serde(default)]
    field: Option<String>,
}

/// Body of `POST /buckets/{bucket}/search/vector`.
#[derive(Debug, Default, Deserialize)]
struct VectorQuery {
    /// Query vector.
    #[serde(default)]
    query: Vec<f32>,
    /// Number of neighbours to return.
    #[serde(default = "default_k")]
    k: usize,
    /// Optional HNSW search-expansion factor.
    #[serde(default)]
    ef: Option<usize>,
    /// Optional exact-match metadata filter applied to candidate rows.
    #[serde(default)]
    filter: Option<BTreeMap<String, serde_json::Value>>,
}

fn default_k() -> usize {
    DEFAULT_K
}

/// One text-search hit: the matching object key plus the stored text.
#[derive(Clone, PartialEq, Eq, Message, Serialize, Deserialize)]
pub struct TextHitJson {
    /// Object key (the HTTP path key) that matched.
    #[prost(string, tag = "1")]
    pub key: String,
    /// Stored text bytes the index matched against (UTF-8, lossy).
    #[prost(string, tag = "2")]
    #[serde(default)]
    pub text: String,
}

/// Response to a text or regex search.
#[derive(Clone, PartialEq, Eq, Message, Serialize, Deserialize)]
pub struct TextSearchResponse {
    /// Field that was searched.
    #[prost(string, tag = "1")]
    pub field: String,
    /// Matching hits, in index order.
    #[prost(message, repeated, tag = "2")]
    #[serde(default)]
    pub hits: Vec<TextHitJson>,
}

impl WireValue for TextSearchResponse {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.http.search.TextResponse")
    }
}

/// One KNN hit: the matching object key plus its distance.
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct VectorHitJson {
    /// Object key (the HTTP path key) that matched.
    #[prost(string, tag = "1")]
    pub key: String,
    /// Distance / score from the query vector (smaller is closer).
    #[prost(float, tag = "2")]
    pub distance: f32,
}

/// Response to a vector KNN search.
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct VectorSearchResponse {
    /// Matching hits, ordered nearest first.
    #[prost(message, repeated, tag = "1")]
    #[serde(default)]
    pub hits: Vec<VectorHitJson>,
}

impl WireValue for VectorSearchResponse {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.http.search.VectorResponse")
    }
}

/// Summary of a bucket's vector index.
#[derive(Clone, PartialEq, Eq, Message, Serialize, Deserialize)]
pub struct VectorIndexInfoJson {
    /// Registry index name (`vec:{bucket}`).
    #[prost(string, tag = "1")]
    pub name: String,
    /// Document field carrying the vector.
    #[prost(string, tag = "2")]
    pub field: String,
    /// Vector dimension.
    #[prost(uint32, tag = "3")]
    pub dim: u32,
    /// Distance metric label.
    #[prost(string, tag = "4")]
    pub metric: String,
    /// Codec label.
    #[prost(string, tag = "5")]
    pub codec: String,
    /// Live (non-tombstoned) row count.
    #[prost(uint64, tag = "6")]
    pub live_rows: u64,
}

/// Response to `GET /buckets/{bucket}/index`.
#[derive(Clone, PartialEq, Eq, Message, Serialize, Deserialize)]
pub struct IndexListResponse {
    /// Bucket name.
    #[prost(string, tag = "1")]
    pub bucket: String,
    /// Declared text-index fields.
    #[prost(string, repeated, tag = "2")]
    #[serde(default)]
    pub text_fields: Vec<String>,
    /// Vector index summary, when one is declared.
    #[prost(message, optional, tag = "3")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorIndexInfoJson>,
}

impl WireValue for IndexListResponse {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.http.search.IndexList")
    }
}

/// Acknowledgement for an index-management call.
#[derive(Clone, PartialEq, Eq, Message, Serialize, Deserialize)]
pub struct IndexAck {
    /// Bucket the index belongs to.
    #[prost(string, tag = "1")]
    pub bucket: String,
    /// Index kind: `text` or `vector`.
    #[prost(string, tag = "2")]
    pub kind: String,
    /// Field name (text indexes; empty for vector).
    #[prost(string, tag = "3")]
    #[serde(default)]
    pub field: String,
    /// True when the call provisioned a new index slot.
    #[prost(bool, tag = "4")]
    pub created: bool,
}

impl WireValue for IndexAck {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.http.search.IndexAck")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_decode_handles_escapes_and_plus() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("a+b"), "a b");
        assert_eq!(percent_decode("plain"), "plain");
        // Malformed escape is passed through verbatim.
        assert_eq!(percent_decode("100%zz"), "100%zz");
    }

    #[test]
    fn query_param_extracts_and_decodes() {
        let q = Some("q=foo%20bar&pattern=ab.%2A&k=2");
        assert_eq!(query_param(q, "q").as_deref(), Some("foo bar"));
        assert_eq!(query_param(q, "pattern").as_deref(), Some("ab.*"));
        assert_eq!(query_param(q, "k").as_deref(), Some("2"));
        assert_eq!(query_param(q, "missing"), None);
        assert_eq!(query_param(None, "q"), None);
    }

    #[test]
    fn json_to_f32_vec_parses_numeric_array() {
        let v = serde_json::json!([1.0, 2, 3.5]);
        assert_eq!(json_to_f32_vec(&v), Some(vec![1.0_f32, 2.0, 3.5]));
        assert_eq!(json_to_f32_vec(&serde_json::json!("nope")), None);
        assert_eq!(json_to_f32_vec(&serde_json::json!([1, "x"])), None);
    }

    #[test]
    fn metric_and_codec_parse_with_defaults() {
        assert_eq!(parse_metric(None).unwrap(), DistanceMetric::Cosine);
        assert_eq!(parse_metric(Some("L2")).unwrap(), DistanceMetric::L2);
        assert_eq!(
            parse_metric(Some("ip")).unwrap(),
            DistanceMetric::InnerProduct
        );
        assert!(parse_metric(Some("bogus")).is_err());
        assert_eq!(parse_codec(None).unwrap(), VectorType::Float32);
        assert_eq!(parse_codec(Some("int8")).unwrap(), VectorType::Int8);
        assert!(parse_codec(Some("bogus")).is_err());
    }

    #[test]
    fn metadata_matches_requires_all_constraints() {
        let mut meta = HashMap::new();
        meta.insert("city".to_string(), serde_json::json!("seattle"));
        meta.insert("age".to_string(), serde_json::json!(30));
        let mut filter = BTreeMap::new();
        filter.insert("city".to_string(), serde_json::json!("seattle"));
        assert!(metadata_matches(&meta, &filter));
        filter.insert("age".to_string(), serde_json::json!(31));
        assert!(!metadata_matches(&meta, &filter));
    }

    #[test]
    fn search_responses_round_trip_through_codecs() {
        let resp = VectorSearchResponse {
            hits: vec![
                VectorHitJson {
                    key: "a".to_string(),
                    distance: 0.1,
                },
                VectorHitJson {
                    key: "b".to_string(),
                    distance: 0.5,
                },
            ],
        };
        for ct in [
            "application/json",
            "application/cbor",
            "application/x-protobuf",
        ] {
            let codec = search_codecs().for_content_type(ct).expect("codec present");
            let bytes = codec.encode(&resp).expect("encode");
            let back = codec
                .decode(VectorSearchResponse::wire_type_id(), &bytes)
                .expect("decode");
            let back = back
                .as_any()
                .downcast_ref::<VectorSearchResponse>()
                .expect("downcast");
            assert_eq!(back, &resp);
        }
    }

    #[test]
    fn declare_text_field_is_idempotent_per_field() {
        let registry = Arc::new(VectorRegistry::new());
        let state = SearchState::new(registry);
        assert!(state.declare_text_field("docs", "title").unwrap());
        // Same field again is a no-op.
        assert!(!state.declare_text_field("docs", "title").unwrap());
        // A second field provisions a new slot.
        assert!(state.declare_text_field("docs", "body").unwrap());
        let listing = state.list_indexes("docs");
        assert_eq!(listing.text_fields, vec!["body", "title"]);
        assert!(listing.vector.is_none());
    }

    #[test]
    fn index_and_search_text_round_trip() {
        let registry = Arc::new(VectorRegistry::new());
        let state = SearchState::new(registry);
        state.declare_text_field("docs", "title").unwrap();
        state.index_object("docs", b"k1", br#"{"title":"the quick brown fox"}"#);
        state.index_object("docs", b"k2", br#"{"title":"lazy dog sleeps"}"#);
        let hits = state.search_substring("docs", "title", b"quick").unwrap();
        let keys: Vec<&str> = hits.iter().map(|h| h.key.as_str()).collect();
        assert_eq!(keys, vec!["k1"]);
    }

    #[test]
    fn create_and_search_vector_round_trip() {
        let registry = Arc::new(VectorRegistry::new());
        let state = SearchState::new(registry);
        let req = VectorIndexCreate {
            dim: 2,
            metric: Some("l2".to_string()),
            codec: Some("float32".to_string()),
            field: None,
        };
        state.create_vector_index("docs", &req).unwrap();
        state.index_object("docs", b"origin", br#"{"_vector":[0.0,0.0]}"#);
        state.index_object("docs", b"unit_x", br#"{"_vector":[1.0,0.0]}"#);
        state.index_object("docs", b"unit_y", br#"{"_vector":[0.0,1.0]}"#);
        let query = VectorQuery {
            query: vec![0.05, 0.05],
            k: 1,
            ef: None,
            filter: None,
        };
        let hits = state.search_vector("docs", &query).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key, "origin");
    }

    // ---- route-handler error and happy arms ------------------------

    fn json_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(ACCEPT, "application/json".parse().unwrap());
        h
    }

    fn bad_accept() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(ACCEPT, "application/x-nonsense".parse().unwrap());
        h
    }

    fn state() -> SearchState {
        SearchState::new(Arc::new(VectorRegistry::new()))
    }

    #[test]
    fn handlers_without_registry_reply_501() {
        let h = json_headers();
        let body = Bytes::new();
        assert_eq!(
            declare_text_index(None, "b", "f", &h).status(),
            StatusCode::NOT_IMPLEMENTED
        );
        assert_eq!(
            create_vector_index(None, "b", &h, &body).status(),
            StatusCode::NOT_IMPLEMENTED
        );
        assert_eq!(
            list_indexes(None, "b", &h).status(),
            StatusCode::NOT_IMPLEMENTED
        );
        assert_eq!(
            search_text(None, "b", "f", Some("q=x"), &h).status(),
            StatusCode::NOT_IMPLEMENTED
        );
        assert_eq!(
            search_regex(None, "b", "f", Some("pattern=x"), &h).status(),
            StatusCode::NOT_IMPLEMENTED
        );
        assert_eq!(
            search_vector(None, "b", &h, &body).status(),
            StatusCode::NOT_IMPLEMENTED
        );
    }

    #[test]
    fn handlers_with_unacceptable_accept_reply_406() {
        let s = state();
        let h = bad_accept();
        let body = Bytes::new();
        assert_eq!(
            declare_text_index(Some(&s), "b", "f", &h).status(),
            StatusCode::NOT_ACCEPTABLE
        );
        assert_eq!(
            create_vector_index(Some(&s), "b", &h, &body).status(),
            StatusCode::NOT_ACCEPTABLE
        );
        assert_eq!(
            list_indexes(Some(&s), "b", &h).status(),
            StatusCode::NOT_ACCEPTABLE
        );
        assert_eq!(
            search_text(Some(&s), "b", "f", Some("q=x"), &h).status(),
            StatusCode::NOT_ACCEPTABLE
        );
        assert_eq!(
            search_regex(Some(&s), "b", "f", Some("pattern=x"), &h).status(),
            StatusCode::NOT_ACCEPTABLE
        );
        assert_eq!(
            search_vector(Some(&s), "b", &h, &body).status(),
            StatusCode::NOT_ACCEPTABLE
        );
    }

    #[test]
    fn declare_then_list_index_ack_is_200() {
        let s = state();
        let h = json_headers();
        assert_eq!(
            declare_text_index(Some(&s), "b", "title", &h).status(),
            StatusCode::OK
        );
        assert_eq!(list_indexes(Some(&s), "b", &h).status(), StatusCode::OK);
    }

    #[test]
    fn create_vector_index_handler_paths() {
        let s = state();
        let h = json_headers();
        // Malformed JSON body -> 400.
        assert_eq!(
            create_vector_index(Some(&s), "b", &h, &Bytes::from_static(b"not json")).status(),
            StatusCode::BAD_REQUEST
        );
        // Valid create -> 200.
        let body = Bytes::from_static(br#"{"dim":3,"metric":"cosine"}"#);
        assert_eq!(
            create_vector_index(Some(&s), "b", &h, &body).status(),
            StatusCode::OK
        );
        // Re-create the same index -> 409 Conflict.
        assert_eq!(
            create_vector_index(Some(&s), "b", &h, &body).status(),
            StatusCode::CONFLICT
        );
        // dim == 0 -> 400 from the engine.
        let zero = Bytes::from_static(br#"{"dim":0}"#);
        assert_eq!(
            create_vector_index(Some(&s), "other", &h, &zero).status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn search_text_handler_paths() {
        let s = state();
        let h = json_headers();
        // Missing `q` -> 400.
        assert_eq!(
            search_text(Some(&s), "b", "f", None, &h).status(),
            StatusCode::BAD_REQUEST
        );
        // No text index declared for the field -> NoTextField (404).
        assert_eq!(
            search_text(Some(&s), "b", "f", Some("q=x"), &h).status(),
            StatusCode::NOT_FOUND
        );
        // Declared + indexed -> 200.
        s.declare_text_field("b", "f").unwrap();
        s.index_object("b", b"k", br#"{"f":"hello world"}"#);
        assert_eq!(
            search_text(Some(&s), "b", "f", Some("q=hello"), &h).status(),
            StatusCode::OK
        );
    }

    #[test]
    fn search_regex_handler_paths() {
        let s = state();
        let h = json_headers();
        // Missing pattern -> 400.
        assert_eq!(
            search_regex(Some(&s), "b", "f", None, &h).status(),
            StatusCode::BAD_REQUEST
        );
        // Bad `k` -> 400.
        assert_eq!(
            search_regex(Some(&s), "b", "f", Some("pattern=x&k=notnum"), &h).status(),
            StatusCode::BAD_REQUEST
        );
        // Declared field, valid pattern + k -> 200.
        s.declare_text_field("b", "f").unwrap();
        s.index_object("b", b"k", br#"{"f":"abcdef"}"#);
        assert_eq!(
            search_regex(Some(&s), "b", "f", Some("pattern=abc&k=1"), &h).status(),
            StatusCode::OK
        );
    }

    #[test]
    fn search_vector_handler_paths() {
        let s = state();
        let h = json_headers();
        // Malformed JSON -> 400.
        assert_eq!(
            search_vector(Some(&s), "b", &h, &Bytes::from_static(b"not json")).status(),
            StatusCode::BAD_REQUEST
        );
        // Empty query vector -> 400.
        let empty = Bytes::from_static(br#"{"query":[],"k":1}"#);
        assert_eq!(
            search_vector(Some(&s), "b", &h, &empty).status(),
            StatusCode::BAD_REQUEST
        );
        // No vector index for the bucket -> NoVectorIndex (404).
        let q = Bytes::from_static(br#"{"query":[0.1,0.2],"k":1}"#);
        assert_eq!(
            search_vector(Some(&s), "b", &h, &q).status(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn search_error_into_response_maps_every_variant() {
        assert_eq!(
            SearchError::NoTextField.into_response().status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            SearchError::NoVectorIndex.into_response().status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            SearchError::AlreadyExists("x".into())
                .into_response()
                .status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            SearchError::BadRequest("x".into()).into_response().status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            SearchError::Registry("x".into()).into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
