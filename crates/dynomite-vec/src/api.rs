//! HTTP API for the vector engine.
//!
//! Routes:
//!
//! ```text
//!   GET    /tables                         -> 200 OK, list TableSchema
//!   POST   /tables                         -> 201 Created (CreateTableRequest body)
//!   GET    /tables/{name}/stats            -> 200 OK / 404
//!   POST   /tables/{name}/vectors          -> 201 Created (UpsertRequest body)
//!   GET    /tables/{name}/vectors/{key}    -> 200 OK / 404
//!   DELETE /tables/{name}/vectors/{key}    -> 204 / 404
//!   POST   /tables/{name}/search           -> 200 OK (SearchRequest body)
//!   GET    /healthz                        -> 200 OK / "ok"
//! ```
//!
//! The server is built on `hyper` directly, mirroring the
//! shape used by [`dyniak::proto::http`] so the workspace
//! does not pick up an additional HTTP framework just for this
//! crate.
//!
//! # CQL future
//!
//! The CQL native protocol is the canonical wire surface for a
//! ScyllaDB-shaped client. It is not implemented in this MVP;
//! see `docs/dynvec/cql-stretch.md` for the protocol shape
//! and how a future implementation would integrate. Every
//! decision point that a CQL surface would need to reach into
//! is annotated with `// CQL future:` in the code.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use crate::distance::Distance;
use crate::encoding::Codec;
use crate::index::HnswParams;
use crate::storage::{IndexAlgorithm, StoreError, TableSchema, VectorStore};

/// JSON body for `POST /tables`.
///
/// `hnsw` is optional; the default tuning lives in
/// [`HnswParams::default`].
#[derive(Clone, Debug, Deserialize)]
pub struct CreateTableRequest {
    /// Table name.
    pub name: String,
    /// Frozen vector dimension.
    pub dim: u16,
    /// Storage codec.
    #[serde(default = "default_codec")]
    pub codec: Codec,
    /// Distance metric.
    #[serde(default = "default_distance")]
    pub distance: Distance,
    /// HNSW tuning override.
    #[serde(default)]
    pub hnsw: Option<HnswParams>,
    /// ANN index algorithm override.
    #[serde(default)]
    pub algorithm: Option<IndexAlgorithm>,
}

const fn default_codec() -> Codec {
    Codec::Int8Quantized
}
const fn default_distance() -> Distance {
    Distance::Cosine
}

/// JSON body for `POST /tables/{name}/vectors`.
#[derive(Clone, Debug, Deserialize)]
pub struct UpsertRequest {
    /// Row key (UTF-8). For non-UTF-8 keys, callers can use the
    /// PBC surface or wrap the bytes in base64; the JSON API
    /// covers the common case.
    pub key: String,
    /// `f32` vector. Length must equal the table's frozen dim.
    pub vector: Vec<f32>,
    /// Free-form per-row metadata.
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

/// JSON body for `POST /tables/{name}/search`.
#[derive(Clone, Debug, Deserialize)]
pub struct SearchHttpRequest {
    /// Query vector.
    pub vector: Vec<f32>,
    /// Number of results.
    pub k: usize,
    /// Optional `ef_search` override.
    #[serde(default)]
    pub ef: Option<usize>,
}

/// One result entry on a `POST /search` response.
#[derive(Clone, Debug, Serialize)]
pub struct SearchHit {
    /// Row key.
    pub key: String,
    /// Distance score; smaller is closer.
    pub score: f32,
    /// Per-row metadata (echoed verbatim).
    pub metadata: HashMap<String, serde_json::Value>,
}

/// `POST /search` response body.
#[derive(Clone, Debug, Serialize)]
pub struct SearchHttpResponse {
    /// Top-K matches.
    pub hits: Vec<SearchHit>,
}

/// `GET /tables/{name}/vectors/{key}` response body.
#[derive(Clone, Debug, Serialize)]
pub struct GetVectorResponse {
    /// Row key.
    pub key: String,
    /// Decoded vector. Sent in `f32` for client convenience;
    /// the on-disk form is whatever codec the table picked.
    pub vector: Vec<f32>,
    /// Per-row metadata.
    pub metadata: HashMap<String, serde_json::Value>,
    /// Codec used for storage.
    pub codec: Codec,
    /// L2 norm of the decoded vector. Useful for inspection.
    pub l2_norm: f32,
}

/// Run the HTTP accept loop on `listener` against `store`.
///
/// One tokio task is spawned per accepted connection. The task
/// dispatches the inbound request through [`dispatch`] and
/// returns whatever response the dispatcher produced.
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
pub async fn serve(listener: TcpListener, store: Arc<VectorStore>) -> std::io::Result<()> {
    loop {
        let (sock, peer) = listener.accept().await?;
        let store = Arc::clone(&store);
        let io = TokioIo::new(sock);
        tokio::spawn(async move {
            let svc = service_fn(move |req| {
                let store = Arc::clone(&store);
                async move {
                    let resp = dispatch(req, store).await;
                    Ok::<_, Infallible>(resp)
                }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                tracing::warn!(%peer, error = %e, "dynvec http connection error");
            }
        });
    }
}

/// Pre-built body type used by every response.
pub type ResponseBody = Full<Bytes>;

/// Dispatch one HTTP request.
///
/// Public so an embedder can plug it into a different transport
/// (TLS, h2, or a unit-test harness) without re-implementing
/// the route table.
pub async fn dispatch(req: Request<Incoming>, store: Arc<VectorStore>) -> Response<ResponseBody> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();

    match (method.clone(), segments.as_slice()) {
        (Method::GET, ["healthz"]) => text(StatusCode::OK, "ok"),
        (Method::GET, ["tables"]) => list_tables(&store),
        (Method::POST, ["tables"]) => create_table(req, &store).await,
        (Method::GET, ["tables", name, "stats"]) => stats(&store, name),
        (Method::POST, ["tables", name, "vectors"]) => upsert(req, &store, name).await,
        (Method::GET, ["tables", name, "vectors", key]) => get_vector(&store, name, key),
        (Method::DELETE, ["tables", name, "vectors", key]) => delete_vector(&store, name, key),
        (Method::POST, ["tables", name, "search"]) => search(req, &store, name).await,
        _ => text(StatusCode::NOT_FOUND, "not found"),
    }
}

fn list_tables(store: &VectorStore) -> Response<ResponseBody> {
    let tables = store.tables();
    json(StatusCode::OK, &tables)
}

async fn create_table(req: Request<Incoming>, store: &VectorStore) -> Response<ResponseBody> {
    let body = match read_body(req).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let parsed: CreateTableRequest = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return text(StatusCode::BAD_REQUEST, &format!("bad json: {e}")),
    };
    let schema = TableSchema {
        name: parsed.name.clone(),
        dim: parsed.dim,
        codec: parsed.codec,
        distance: parsed.distance,
        hnsw: parsed.hnsw.unwrap_or_default(),
        algorithm: parsed.algorithm.unwrap_or_default(),
    };
    match store.create_table(schema.clone()) {
        Ok(()) => json(StatusCode::CREATED, &schema),
        Err(StoreError::TableExists(_)) => text(StatusCode::CONFLICT, "table exists"),
        Err(e) => text(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn stats(store: &VectorStore, name: &str) -> Response<ResponseBody> {
    match store.stats(name) {
        Ok(s) => json(StatusCode::OK, &s),
        Err(StoreError::UnknownTable(_)) => text(StatusCode::NOT_FOUND, "table not found"),
        Err(e) => text(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

async fn upsert(
    req: Request<Incoming>,
    store: &VectorStore,
    table: &str,
) -> Response<ResponseBody> {
    let body = match read_body(req).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let parsed: UpsertRequest = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return text(StatusCode::BAD_REQUEST, &format!("bad json: {e}")),
    };
    // CQL future: routing by partition hash happens here in a
    // distributed deployment. The MVP runs single-node so the
    // upsert lands locally.
    match store.upsert(
        table,
        parsed.key.into_bytes(),
        &parsed.vector,
        parsed.metadata,
    ) {
        Ok(()) => text(StatusCode::CREATED, "ok"),
        Err(StoreError::UnknownTable(_)) => text(StatusCode::NOT_FOUND, "table not found"),
        Err(StoreError::DimensionMismatch { expected, got, .. }) => text(
            StatusCode::BAD_REQUEST,
            &format!("dim mismatch: expected {expected}, got {got}"),
        ),
        Err(e) => text(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn get_vector(store: &VectorStore, table: &str, key: &str) -> Response<ResponseBody> {
    match store.get(table, key.as_bytes()) {
        Ok(Some(row)) => {
            let codec = row.vector.codec;
            let encoder = codec.encoder();
            let decoded = match encoder.decode(&row.vector) {
                Ok(v) => v,
                Err(e) => {
                    return text(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
                }
            };
            let resp = GetVectorResponse {
                key: key.to_string(),
                l2_norm: row.vector.l2_norm(),
                vector: decoded,
                metadata: row.metadata,
                codec,
            };
            json(StatusCode::OK, &resp)
        }
        Ok(None) => text(StatusCode::NOT_FOUND, "row not found"),
        Err(StoreError::UnknownTable(_)) => text(StatusCode::NOT_FOUND, "table not found"),
        Err(e) => text(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn delete_vector(store: &VectorStore, table: &str, key: &str) -> Response<ResponseBody> {
    match store.delete(table, key.as_bytes()) {
        Ok(true) => no_content(),
        Ok(false) => text(StatusCode::NOT_FOUND, "row not found"),
        Err(StoreError::UnknownTable(_)) => text(StatusCode::NOT_FOUND, "table not found"),
        Err(e) => text(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

async fn search(
    req: Request<Incoming>,
    store: &VectorStore,
    table: &str,
) -> Response<ResponseBody> {
    let body = match read_body(req).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let parsed: SearchHttpRequest = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return text(StatusCode::BAD_REQUEST, &format!("bad json: {e}")),
    };
    // CQL future: this is where the distributed coordinator
    // (cluster_query::run) takes over. The MVP routes the
    // search to the local store.
    match store.search(table, &parsed.vector, parsed.k, parsed.ef) {
        Ok(rows) => {
            let hits = rows
                .into_iter()
                .map(|(row, score)| SearchHit {
                    key: String::from_utf8_lossy(&row.key).into_owned(),
                    score,
                    metadata: row.metadata,
                })
                .collect();
            json(StatusCode::OK, &SearchHttpResponse { hits })
        }
        Err(StoreError::UnknownTable(_)) => text(StatusCode::NOT_FOUND, "table not found"),
        Err(StoreError::DimensionMismatch { expected, got, .. }) => text(
            StatusCode::BAD_REQUEST,
            &format!("dim mismatch: expected {expected}, got {got}"),
        ),
        Err(e) => text(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

async fn read_body(req: Request<Incoming>) -> Result<Bytes, Response<ResponseBody>> {
    let bytes = req
        .into_body()
        .collect()
        .await
        .map_err(|e| text(StatusCode::BAD_REQUEST, &format!("bad body: {e}")))?
        .to_bytes();
    Ok(bytes)
}

fn json<T: Serialize>(code: StatusCode, body: &T) -> Response<ResponseBody> {
    let payload = serde_json::to_vec(body)
        .unwrap_or_else(|e| format!("{{\"error\":\"json encode: {e}\"}}").into_bytes());
    let mut resp = Response::new(Full::new(Bytes::from(payload)));
    *resp.status_mut() = code;
    resp.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    resp
}

fn text(code: StatusCode, body: &str) -> Response<ResponseBody> {
    let mut resp = Response::new(Full::new(Bytes::from(body.to_string())));
    *resp.status_mut() = code;
    resp.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    resp
}

fn no_content() -> Response<ResponseBody> {
    let mut resp = Response::new(Full::new(Bytes::new()));
    *resp.status_mut() = StatusCode::NO_CONTENT;
    resp
}
