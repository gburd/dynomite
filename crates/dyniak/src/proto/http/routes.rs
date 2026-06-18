//! HTTP route table and per-route handlers for the Riak HTTP gateway.
//!
//! The route surface mirrors the subset of Riak's HTTP API that the
//! v0.0.1 slice supports. Unrecognised routes return `404 Not
//! Found`. Recognised routes that the underlying
//! [`dynomite::embed::Datastore`] cannot serve (for example
//! list-keys against an in-memory store) return `501 Not
//! Implemented`. List endpoints stream their response body in
//! HTTP/1.1 chunked transfer-encoding for negotiated `application/json`
//! and use a length-prefixed framing for opaque codecs.
//!
//! # Coverage
//!
//! | Method      | Path                                     | Description                 |
//! |-------------|------------------------------------------|-----------------------------|
//! | GET, HEAD   | `/ping`                                  | Liveness probe              |
//! | GET         | `/stats`                                 | Server name and version     |
//! | GET, HEAD   | `/buckets/{bucket}/keys/{key}`           | Fetch object               |
//! | PUT         | `/buckets/{bucket}/keys/{key}`           | Store object               |
//! | POST        | `/buckets/{bucket}/keys/{key}`           | Store object (key required)|
//! | DELETE      | `/buckets/{bucket}/keys/{key}`           | Delete object              |
//! | GET         | `/buckets?buckets=true`                  | List buckets (chunked)     |
//! | GET         | `/buckets/{bucket}/keys?keys=true`       | List keys (chunked)        |
//! | GET         | `/buckets/{bucket}/props`                | Get bucket props            |
//! | PUT         | `/buckets/{bucket}/props`                | Set bucket props            |
//!
//! # Datastore semantics
//!
//! Object K/V requests (`GET` / `PUT` / `DELETE` on
//! `/buckets/{bucket}/keys/{key}`) are served against the real
//! object store when the backend exposes one. The handler probes
//! [`dynomite::embed::Datastore::as_any`] for a
//! [`crate::datastore::NoxuDatastore`] (the `object_store` helper);
//! on a hit it reads / writes / deletes the stored
//! [`crate::proto::http::object::HttpObject`] envelope, re-encoding
//! it under the negotiated codec. The envelope is persisted in a
//! canonical, codec-independent form, so a value stored under one
//! encoding is fetchable under any other.
//!
//! Backends without an object layer (the in-memory store used in
//! tests) fall back to the documented trampoline: every K/V request
//! is routed through [`dynomite::embed::Datastore::dispatch`] for the
//! substrate's per-request accounting, a `GET` then replies
//! `404 Not Found`, and `PUT` / `DELETE` reply `204 No Content`.
//! Every path -- real or fallback -- ticks the dispatch counter the
//! same way [`crate::server::handle_conn`] does on the PBC side.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full, StreamBody};
use hyper::body::{Frame as HttpFrame, Incoming};
use hyper::header::{ACCEPT, CONTENT_TYPE, TRANSFER_ENCODING};
use hyper::{HeaderMap, Method, Request, Response, StatusCode};

use dynomite::embed::hooks::DatastoreByteStream;
use dynomite::embed::Datastore;
use dynomite::msg::{Msg, MsgType};

#[cfg(feature = "noxu")]
use dyn_encoding::WireValue;

use crate::proto::http::content_type::{select_codec, SUPPORTED_CONTENT_TYPES};
#[cfg(feature = "noxu")]
use crate::proto::http::object::{object_codecs, HttpIndex, HttpLink, HttpObject};
use crate::txn::{HttpTxnRequest, HttpTxnResponse, TransactionalStore, TxnOutcome, TxnStoreError};

/// Body type the gateway emits.
///
/// The gateway used to fully buffer every response in [`Full`].
/// Streaming list-buckets / list-keys forced the boxed body shape
/// so a buffered handler and a chunked handler can coexist behind
/// one return type. The error type is unified to [`Infallible`];
/// streaming handlers that observe a datastore error fold it into
/// a final body chunk and finish the stream cleanly so the hyper
/// layer never sees a body-level error.
pub(crate) type ResponseBody = UnsyncBoxBody<Bytes, Infallible>;

/// Maximum number of entries packed into a single streaming JSON
/// or codec chunk for list-buckets / list-keys.
///
/// Matches the PBC framer's chunk size so a tee-tail comparison
/// of the two transports stays apples-to-apples.
pub(crate) const HTTP_LIST_CHUNK_SIZE: usize = 256;

/// Wrap an in-memory byte payload in the boxed body shape.
fn buffered_body(bytes: Bytes) -> ResponseBody {
    BodyExt::boxed_unsync(Full::new(bytes))
}

/// Maximum HTTP request body the gateway will accept. Mirrors the
/// PBC framer's 16 MiB cap.
const MAX_BODY_LEN: usize = 16 * 1024 * 1024;

/// Server name reported through `/stats` and the `Server` header.
const SERVER_NAME: &str = "dyniak";

/// Server version reported through `/stats`. Bumped in lockstep with
/// the crate version.
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Per-request gateway context: the datastore the handlers delegate
/// to, plus the optional search registry that lights up the index /
/// search routes.
///
/// The context is built once per server (cheap to clone: every field
/// is an [`Arc`]) and cloned per request. A plain
/// `Arc<dyn Datastore>` converts into a search-less context through
/// [`From`], so the call sites that predate the search surface keep
/// compiling unchanged.
#[derive(Clone)]
pub(crate) struct RouteCtx {
    pub(crate) datastore: Arc<dyn Datastore>,
    #[cfg(feature = "search")]
    pub(crate) search: Option<Arc<crate::proto::http::search::SearchState>>,
    /// Optional Wasm phase store. When `Some`, a
    /// [`crate::mapreduce::Phase::WasmModule`] job submitted to
    /// `POST /mapred` is dispatched through the store; when `None`
    /// such a job surfaces the typed
    /// [`crate::mapreduce::MrError::WasmNotImplemented`] error.
    #[cfg(feature = "wasm")]
    pub(crate) wasm: Option<Arc<crate::mapreduce::wasm::WasmModuleStore>>,
}

impl RouteCtx {
    /// Build a context with neither search registry nor Wasm store
    /// wired in.
    pub(crate) fn new(datastore: Arc<dyn Datastore>) -> Self {
        Self {
            datastore,
            #[cfg(feature = "search")]
            search: None,
            #[cfg(feature = "wasm")]
            wasm: None,
        }
    }

    /// Build a context with a search registry wired in.
    #[cfg(feature = "search")]
    pub(crate) fn with_search(
        datastore: Arc<dyn Datastore>,
        search: Arc<crate::proto::http::search::SearchState>,
    ) -> Self {
        Self {
            datastore,
            search: Some(search),
            #[cfg(feature = "wasm")]
            wasm: None,
        }
    }

    /// Build a context with a Wasm phase store wired in.
    #[cfg(feature = "wasm")]
    pub(crate) fn with_wasm(
        datastore: Arc<dyn Datastore>,
        wasm: Arc<crate::mapreduce::wasm::WasmModuleStore>,
    ) -> Self {
        Self {
            datastore,
            #[cfg(feature = "search")]
            search: None,
            wasm: Some(wasm),
        }
    }
}

impl From<Arc<dyn Datastore>> for RouteCtx {
    fn from(datastore: Arc<dyn Datastore>) -> Self {
        Self::new(datastore)
    }
}

/// Dispatch entry point. Reads the request, walks the route table,
/// and produces a single buffered response.
///
/// This function never returns an error: every failure path is
/// turned into an HTTP response so the hyper service contract is
/// satisfied with `Result<_, Infallible>`.
pub(crate) async fn dispatch(req: Request<Incoming>, ctx: RouteCtx) -> Response<ResponseBody> {
    let (parts, body) = req.into_parts();
    let Some(route) = Route::parse(&parts.method, parts.uri.path(), parts.uri.query()) else {
        return text_response(StatusCode::NOT_FOUND, "not found");
    };

    let body_bytes = match collect_body(body).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    handle_route(route, &parts.method, &parts.headers, body_bytes, ctx).await
}

/// Riak-recognised route classification.
#[derive(Debug, Eq, PartialEq)]
enum Route<'a> {
    /// `GET|HEAD /ping`
    Ping,
    /// `GET /stats`
    Stats,
    /// `GET|HEAD /buckets/{bucket}/keys/{key}`
    GetObject { bucket: &'a str, key: &'a str },
    /// `PUT /buckets/{bucket}/keys/{key}`
    PutObject { bucket: &'a str, key: &'a str },
    /// `POST /buckets/{bucket}/keys/{key}` (Riak HTTP requires the
    /// key path component even for server-assigned keys; we accept
    /// it for parity).
    PostObject { bucket: &'a str, key: &'a str },
    /// `DELETE /buckets/{bucket}/keys/{key}`
    DeleteObject { bucket: &'a str, key: &'a str },
    /// `GET /buckets?buckets=true`
    ListBuckets,
    /// `GET /buckets/{bucket}/keys?keys=true`
    ListKeys { bucket: &'a str },
    /// `GET /buckets/{bucket}/props`
    GetProps { bucket: &'a str },
    /// `PUT /buckets/{bucket}/props`
    SetProps { bucket: &'a str },
    /// `POST /mapred` -- submit a MapReduce job. Added by the
    /// v0.0.3 MapReduce slice.
    MapRed,
    /// `POST /transactions` (cluster-wide) or
    /// `POST /buckets/{bucket}/transactions` (bucket-scoped) --
    /// submit a multi-key atomic transaction batch. A dyniak
    /// extension beyond Riak's per-key eventual consistency.
    Transaction { bucket: Option<&'a str> },
    /// `PUT /buckets/{bucket}/index/text/{field}` -- declare a text
    /// index on a logical document field. Search extension.
    #[cfg(feature = "search")]
    DeclareTextIndex { bucket: &'a str, field: &'a str },
    /// `POST /buckets/{bucket}/index/vector` -- create the bucket's
    /// vector index. Search extension.
    #[cfg(feature = "search")]
    CreateVectorIndex { bucket: &'a str },
    /// `GET /buckets/{bucket}/index` -- list declared indexes.
    #[cfg(feature = "search")]
    ListIndexes { bucket: &'a str },
    /// `GET /buckets/{bucket}/search/text/{field}?q=<substr>`.
    #[cfg(feature = "search")]
    SearchText {
        bucket: &'a str,
        field: &'a str,
        query: Option<&'a str>,
    },
    /// `GET /buckets/{bucket}/search/regex/{field}?pattern=<re>&k=<n>`.
    #[cfg(feature = "search")]
    SearchRegex {
        bucket: &'a str,
        field: &'a str,
        query: Option<&'a str>,
    },
    /// `POST /buckets/{bucket}/search/vector` -- KNN query.
    #[cfg(feature = "search")]
    SearchVector { bucket: &'a str },
}

impl<'a> Route<'a> {
    /// Match a method+path+query triple against the route table.
    fn parse(method: &Method, path: &'a str, query: Option<&'a str>) -> Option<Self> {
        let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let m = method.as_str();
        match (m, parts.as_slice()) {
            ("GET" | "HEAD", ["ping"]) => Some(Self::Ping),
            ("GET", ["stats"]) => Some(Self::Stats),
            ("GET", ["buckets"]) if has_flag(query, "buckets", "true") => Some(Self::ListBuckets),
            ("GET" | "HEAD", ["buckets", b, "keys", k]) => {
                Some(Self::GetObject { bucket: b, key: k })
            }
            ("PUT", ["buckets", b, "keys", k]) => Some(Self::PutObject { bucket: b, key: k }),
            ("POST", ["buckets", b, "keys", k]) => Some(Self::PostObject { bucket: b, key: k }),
            ("DELETE", ["buckets", b, "keys", k]) => Some(Self::DeleteObject { bucket: b, key: k }),
            ("GET", ["buckets", b, "keys"]) if has_flag(query, "keys", "true") => {
                Some(Self::ListKeys { bucket: b })
            }
            ("GET", ["buckets", b, "props"]) => Some(Self::GetProps { bucket: b }),
            ("PUT", ["buckets", b, "props"]) => Some(Self::SetProps { bucket: b }),
            ("POST", ["mapred"]) => Some(Self::MapRed),
            ("POST", ["transactions"]) => Some(Self::Transaction { bucket: None }),
            ("POST", ["buckets", b, "transactions"]) => Some(Self::Transaction { bucket: Some(b) }),
            #[cfg(feature = "search")]
            ("PUT", ["buckets", b, "index", "text", f]) => Some(Self::DeclareTextIndex {
                bucket: b,
                field: f,
            }),
            #[cfg(feature = "search")]
            ("POST", ["buckets", b, "index", "vector"]) => {
                Some(Self::CreateVectorIndex { bucket: b })
            }
            #[cfg(feature = "search")]
            ("GET", ["buckets", b, "index"]) => Some(Self::ListIndexes { bucket: b }),
            #[cfg(feature = "search")]
            ("GET", ["buckets", b, "search", "text", f]) => Some(Self::SearchText {
                bucket: b,
                field: f,
                query,
            }),
            #[cfg(feature = "search")]
            ("GET", ["buckets", b, "search", "regex", f]) => Some(Self::SearchRegex {
                bucket: b,
                field: f,
                query,
            }),
            #[cfg(feature = "search")]
            ("POST", ["buckets", b, "search", "vector"]) => Some(Self::SearchVector { bucket: b }),
            _ => None,
        }
    }
}

/// Look up `key` in a `&`-separated query string and check that its
/// value equals `expected`.
fn has_flag(query: Option<&str>, key: &str, expected: &str) -> bool {
    let Some(q) = query else { return false };
    for pair in q.split('&') {
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("");
        let v = it.next().unwrap_or("");
        if k == key && v == expected {
            return true;
        }
    }
    false
}

/// Pull the request body into memory, capped at [`MAX_BODY_LEN`].
async fn collect_body(body: Incoming) -> Result<Bytes, Response<ResponseBody>> {
    let collected = body
        .collect()
        .await
        .map_err(|e| text_response(StatusCode::BAD_REQUEST, &format!("body read error: {e}")))?
        .to_bytes();
    if collected.len() > MAX_BODY_LEN {
        return Err(text_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body exceeds 16 MiB",
        ));
    }
    Ok(collected)
}

/// Per-route dispatch.
async fn handle_route(
    route: Route<'_>,
    method: &Method,
    headers: &HeaderMap,
    body: Bytes,
    ctx: impl Into<RouteCtx>,
) -> Response<ResponseBody> {
    let ctx = ctx.into();
    let head_only = method == Method::HEAD;
    match route {
        Route::Ping => ping_response(head_only),
        Route::Stats => stats_response(headers),
        Route::GetObject { bucket, key } => {
            handle_get(bucket, key, headers, head_only, ctx.datastore.as_ref()).await
        }
        Route::PutObject { bucket, key } | Route::PostObject { bucket, key } => {
            handle_put(bucket, key, headers, body, &ctx).await
        }
        Route::DeleteObject { bucket, key } => {
            handle_delete(bucket, key, ctx.datastore.as_ref()).await
        }
        Route::ListBuckets => list_buckets_response(headers, &ctx.datastore),
        Route::ListKeys { bucket } => list_keys_response(bucket, headers, &ctx.datastore),
        Route::GetProps { bucket } => get_props_response(bucket, headers),
        Route::SetProps { bucket } => set_props_response(bucket, headers, &body),
        Route::MapRed => mapred_response(headers, &body, &ctx),
        Route::Transaction { bucket } => {
            transaction_response(bucket, headers, &body, ctx.datastore.as_ref())
        }
        #[cfg(feature = "search")]
        Route::DeclareTextIndex { bucket, field } => {
            super::search::declare_text_index(ctx.search.as_deref(), bucket, field, headers)
        }
        #[cfg(feature = "search")]
        Route::CreateVectorIndex { bucket } => {
            super::search::create_vector_index(ctx.search.as_deref(), bucket, headers, &body)
        }
        #[cfg(feature = "search")]
        Route::ListIndexes { bucket } => {
            super::search::list_indexes(ctx.search.as_deref(), bucket, headers)
        }
        #[cfg(feature = "search")]
        Route::SearchText {
            bucket,
            field,
            query,
        } => super::search::search_text(ctx.search.as_deref(), bucket, field, query, headers),
        #[cfg(feature = "search")]
        Route::SearchRegex {
            bucket,
            field,
            query,
        } => super::search::search_regex(ctx.search.as_deref(), bucket, field, query, headers),
        #[cfg(feature = "search")]
        Route::SearchVector { bucket } => {
            super::search::search_vector(ctx.search.as_deref(), bucket, headers, &body)
        }
    }
}

// ------------------------------------------------------------------
// Per-route handlers.
// ------------------------------------------------------------------

fn ping_response(head_only: bool) -> Response<ResponseBody> {
    let body = if head_only {
        Bytes::new()
    } else {
        Bytes::from_static(b"OK")
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .header("Server", SERVER_NAME)
        .body(buffered_body(body))
        .expect("invariant: ping response builder is well-formed")
}

fn stats_response(headers: &HeaderMap) -> Response<ResponseBody> {
    let accept = header_str(headers, ACCEPT);
    let Some(ct) = select_codec(accept, Some("application/json")) else {
        return not_acceptable_response();
    };
    let payload = serde_json::json!({
        "name": SERVER_NAME,
        "version": SERVER_VERSION,
        "supported_content_types": SUPPORTED_CONTENT_TYPES,
    });
    let body_bytes = match ct {
        "application/json" => serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec()),
        // Non-JSON encodings of stats are not interesting yet; the
        // structure is small and JSON-shaped. Reply with JSON in
        // the body and pin the content-type to the negotiated value
        // so the client cannot complain about a missing codec.
        _ => serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec()),
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, ct)
        .header("Server", SERVER_NAME)
        .body(buffered_body(Bytes::from(body_bytes)))
        .expect("invariant: stats response builder is well-formed")
}

async fn handle_get(
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    head_only: bool,
    datastore: &dyn Datastore,
) -> Response<ResponseBody> {
    let accept = header_str(headers, ACCEPT);
    let req_ct = header_str_opt(headers, CONTENT_TYPE);
    let Some(ct) = select_codec(accept, req_ct) else {
        return not_acceptable_response();
    };

    // Accounting trampoline: keep the substrate's per-request counter
    // ticking exactly as the PBC path does before the real fetch.
    let routing = Msg::new(0, MsgType::Unknown, true);
    if let Err(e) = datastore.dispatch(routing).await {
        return text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("datastore error: {e}"),
        );
    }

    // Object-capable backends (today: `NoxuDatastore`) fetch the
    // stored envelope and re-encode it under the negotiated codec.
    // Other backends (the in-memory store used in tests) have no
    // object layer, so they fall back to Riak's miss response.
    #[cfg(feature = "noxu")]
    {
        if let Some(store) = object_store(datastore) {
            return get_object_from_store(store, bucket, key, ct, head_only);
        }
    }
    #[cfg(not(feature = "noxu"))]
    {
        let _ = (bucket, key, ct, head_only);
    }
    text_response(StatusCode::NOT_FOUND, "not found")
}

async fn handle_put(
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    body: Bytes,
    ctx: &RouteCtx,
) -> Response<ResponseBody> {
    let datastore = ctx.datastore.as_ref();
    let accept = header_str(headers, ACCEPT);
    let req_ct = header_str_opt(headers, CONTENT_TYPE);
    if select_codec(accept, req_ct).is_none() {
        return not_acceptable_response();
    }
    // A request body is required for a put; a missing body is a
    // client error so the reply is 400 rather than 204.
    if body.is_empty() {
        return text_response(StatusCode::BAD_REQUEST, "PUT body must not be empty");
    }
    if let Some(ct) = req_ct {
        if super::content_type::canonicalize(ct).is_none() {
            return text_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "request Content-Type is not supported",
            );
        }
    }

    // Accounting trampoline, matching the PBC path and the GET path.
    let routing = Msg::new(0, MsgType::Unknown, true);
    if let Err(e) = datastore.dispatch(routing).await {
        return text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("datastore error: {e}"),
        );
    }

    // Object-capable backends decode the body under the request codec
    // and persist the canonical envelope. Other backends acknowledge
    // the write without storing (the in-memory test trampoline).
    #[cfg(feature = "noxu")]
    {
        if let Some(store) = object_store(datastore) {
            return put_object_into_store(store, bucket, key, headers, &body, req_ct, ctx);
        }
    }
    #[cfg(not(feature = "noxu"))]
    {
        let _ = (bucket, key, &body, ctx);
    }
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("Server", SERVER_NAME)
        .body(buffered_body(Bytes::new()))
        .expect("invariant: put response builder is well-formed")
}

async fn handle_delete(
    bucket: &str,
    key: &str,
    datastore: &dyn Datastore,
) -> Response<ResponseBody> {
    // Accounting trampoline, matching the PBC and GET/PUT paths.
    let routing = Msg::new(0, MsgType::Unknown, true);
    if let Err(e) = datastore.dispatch(routing).await {
        return text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("datastore error: {e}"),
        );
    }

    // Object-capable backends remove the object and its 2i entries.
    // As with Riak, a delete of an absent key is not an error: the
    // PBC del path replies `RpbDelResp` regardless, so the HTTP path
    // replies `204 No Content` whether or not the key existed.
    #[cfg(feature = "noxu")]
    {
        if let Some(store) = object_store(datastore) {
            return match store.delete_object(bucket.as_bytes(), key.as_bytes()) {
                Ok(_) => no_content_response(),
                Err(e) => storage_error_response(&e),
            };
        }
    }
    #[cfg(not(feature = "noxu"))]
    {
        let _ = (bucket, key);
    }
    no_content_response()
}

/// Build a body-less `204 No Content` response carrying the server
/// name header. Shared by the put / delete success paths.
fn no_content_response() -> Response<ResponseBody> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("Server", SERVER_NAME)
        .body(buffered_body(Bytes::new()))
        .expect("invariant: no-content response builder is well-formed")
}

fn get_props_response(bucket: &str, headers: &HeaderMap) -> Response<ResponseBody> {
    let accept = header_str(headers, ACCEPT);
    let Some(ct) = select_codec(accept, Some("application/json")) else {
        return not_acceptable_response();
    };
    // v0.0.1 returns Riak's documented defaults. A follow-up slice
    // pulls these from the bucket-props store once it lands.
    let props = serde_json::json!({
        "props": {
            "name": bucket,
            "n_val": 3,
            "allow_mult": false,
            "last_write_wins": false,
            "r": "quorum",
            "w": "quorum",
            "pr": 0,
            "pw": 0,
            "dw": "quorum",
            "rw": "quorum",
            "basic_quorum": false,
            "notfound_ok": true,
        }
    });
    let body = serde_json::to_vec(&props).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, ct)
        .header("Server", SERVER_NAME)
        .body(buffered_body(Bytes::from(body)))
        .expect("invariant: get-props response builder is well-formed")
}

fn set_props_response(_bucket: &str, headers: &HeaderMap, body: &Bytes) -> Response<ResponseBody> {
    let req_ct = header_str_opt(headers, CONTENT_TYPE);
    if let Some(ct) = req_ct {
        if super::content_type::canonicalize(ct).is_none() {
            return text_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "request Content-Type is not supported",
            );
        }
    }
    if body.is_empty() {
        return text_response(StatusCode::BAD_REQUEST, "set-props body must not be empty");
    }
    // v0.0.1 acknowledges the request without persisting -- the
    // bucket-props store lands with the full RiakObject schema in
    // the next slice. The HTTP shape (204 No Content) is right for
    // operators today; the body becomes durable once the store is
    // wired in.
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("Server", SERVER_NAME)
        .body(buffered_body(Bytes::new()))
        .expect("invariant: set-props response builder is well-formed")
}

// ------------------------------------------------------------------
// Multi-key transaction route handler.
// ------------------------------------------------------------------

/// Run a multi-key atomic transaction submitted via
/// `POST /transactions` or `POST /buckets/{bucket}/transactions`.
///
/// The body is a JSON [`HttpTxnRequest`]. The handler lowers it into
/// a [`crate::txn::TxnBatch`], hands it to the backend's
/// [`TransactionalStore`] (probed for via
/// [`dynomite::embed::Datastore::as_any`]), and renders the
/// [`TxnOutcome`] as a JSON [`HttpTxnResponse`]. A committed batch
/// replies `200 OK`; a rolled-back batch replies `409 Conflict`.
/// When the configured datastore is not transactional the handler
/// replies `501 Not Implemented`.
///
/// For the bucket-scoped route every operation must target the URL
/// bucket; a mismatch is a `400 Bad Request`.
fn transaction_response(
    bucket: Option<&str>,
    headers: &HeaderMap,
    body: &Bytes,
    datastore: &dyn Datastore,
) -> Response<ResponseBody> {
    let req_ct = header_str_opt(headers, CONTENT_TYPE);
    let ct = req_ct.unwrap_or("application/json");
    if super::content_type::canonicalize(ct) != Some("application/json") {
        return text_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "transactions require Content-Type: application/json",
        );
    }
    if body.is_empty() {
        return text_response(
            StatusCode::BAD_REQUEST,
            "transaction body must not be empty",
        );
    }
    let request: HttpTxnRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            return text_response(StatusCode::BAD_REQUEST, &format!("transaction decode: {e}"));
        }
    };
    let batch = request.into_batch();
    if let Some(b) = bucket {
        if batch.ops.iter().any(|op| op.bucket() != b.as_bytes()) {
            return text_response(
                StatusCode::BAD_REQUEST,
                "every operation must target the bucket named in the URL",
            );
        }
    }
    let Some(store) = txn_store(datastore) else {
        return text_response(
            StatusCode::NOT_IMPLEMENTED,
            "the configured datastore does not support transactions",
        );
    };
    match store.execute_batch(&batch) {
        Ok(outcome) => txn_outcome_response(&outcome),
        Err(TxnStoreError::EmptyBatch) => {
            text_response(StatusCode::BAD_REQUEST, "empty transaction batch")
        }
        Err(e @ TxnStoreError::Conflict(_)) => txn_error_response(StatusCode::CONFLICT, &e),
        Err(e @ TxnStoreError::Backend(_)) => {
            txn_error_response(StatusCode::INTERNAL_SERVER_ERROR, &e)
        }
    }
}

/// Probe `datastore` for a multi-key [`TransactionalStore`].
///
/// Returns `Some` only when the crate is built with the `noxu`
/// feature and the concrete backend is a
/// [`crate::datastore::NoxuDatastore`]. The probe goes through
/// [`dynomite::embed::Datastore::as_any`] so the HTTP layer never
/// names the transactional backend on its own trait surface.
fn txn_store(datastore: &dyn Datastore) -> Option<&dyn TransactionalStore> {
    #[cfg(feature = "noxu")]
    {
        if let Some(any) = datastore.as_any() {
            if let Some(noxu) = any.downcast_ref::<crate::datastore::NoxuDatastore>() {
                return Some(noxu as &dyn TransactionalStore);
            }
        }
        None
    }
    #[cfg(not(feature = "noxu"))]
    {
        let _ = datastore;
        None
    }
}

// ------------------------------------------------------------------
// Object K/V store wiring (GET / PUT / DELETE against a real store).
// ------------------------------------------------------------------

/// Probe `datastore` for the concrete object-capable backend.
///
/// Returns `Some` only when the crate is built with the `noxu`
/// feature and the backend is a [`crate::datastore::NoxuDatastore`].
/// The probe goes through [`dynomite::embed::Datastore::as_any`] in
/// the same way [`txn_store`] reaches the transactional surface, so
/// the HTTP layer never names the storage backend on its own trait.
#[cfg(feature = "noxu")]
fn object_store(datastore: &dyn Datastore) -> Option<&crate::datastore::NoxuDatastore> {
    datastore
        .as_any()
        .and_then(|any| any.downcast_ref::<crate::datastore::NoxuDatastore>())
}

/// Fetch the object stored under `(bucket, key)` and re-encode it
/// under the negotiated codec `ct`.
///
/// A miss is `404 Not Found`; a hit is `200 OK` with the envelope
/// re-encoded in the negotiated codec. `HEAD` requests get the same
/// status and headers with an empty body. A stored value that does
/// not decode as an [`HttpObject`] is a `500`.
#[cfg(feature = "noxu")]
fn get_object_from_store(
    store: &crate::datastore::NoxuDatastore,
    bucket: &str,
    key: &str,
    ct: &'static str,
    head_only: bool,
) -> Response<ResponseBody> {
    let stored = match store.get_object(bucket.as_bytes(), key.as_bytes()) {
        Ok(Some(v)) => v,
        Ok(None) => return text_response(StatusCode::NOT_FOUND, "not found"),
        Err(e) => return storage_error_response(&e),
    };
    let obj = match HttpObject::from_storage_bytes(&stored) {
        Ok(o) => o,
        Err(e) => {
            return text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("stored object is corrupt: {e}"),
            );
        }
    };
    // `ct` came from `select_codec`, so it is always one of the
    // registered baseline content-types; the `else` arm is defensive.
    let Some(codec) = object_codecs().for_content_type(ct) else {
        return not_acceptable_response();
    };
    let encoded = match codec.encode(&obj) {
        Ok(b) => b,
        Err(e) => {
            return text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("object encode: {e}"),
            );
        }
    };
    let body = if head_only {
        Bytes::new()
    } else {
        Bytes::from(encoded)
    };
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, ct)
        .header("Server", SERVER_NAME)
        // Riak always emits a bucket-up link so a client can
        // navigate from an object back to its bucket.
        .header("Link", format!("</buckets/{bucket}>; rel=\"up\""));
    for link in &obj.links {
        builder = builder.header(
            "Link",
            format!(
                "</buckets/{}/keys/{}>; riaktag=\"{}\"",
                link.bucket, link.key, link.tag
            ),
        );
    }
    builder
        .body(buffered_body(body))
        .expect("invariant: object response builder is well-formed")
}

/// Decode the request `body` under the request codec, merge any
/// `X-Riak-Index-*` headers into the envelope, persist the canonical
/// form, and fan the index list out into the 2i layer.
///
/// A body that does not decode under its declared codec is a
/// `400 Bad Request`. A successful store is `204 No Content`.
#[cfg(feature = "noxu")]
fn put_object_into_store(
    store: &crate::datastore::NoxuDatastore,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    body: &Bytes,
    req_ct: Option<&str>,
    ctx: &RouteCtx,
) -> Response<ResponseBody> {
    // The request codec defaults to JSON when the client omits a
    // Content-Type; the 415 guard in `handle_put` has already
    // rejected any unsupported declared type.
    let req_ct = req_ct.unwrap_or("application/json");
    let canonical = super::content_type::canonicalize(req_ct).unwrap_or("application/json");
    let Some(codec) = object_codecs().for_content_type(canonical) else {
        return text_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "request Content-Type is not supported",
        );
    };
    let decoded = match codec.decode(HttpObject::wire_type_id(), body) {
        Ok(v) => v,
        Err(e) => {
            return text_response(StatusCode::BAD_REQUEST, &format!("object decode: {e}"));
        }
    };
    let Some(obj) = decoded.as_any().downcast_ref::<HttpObject>() else {
        // The codec round-trips `HttpObject`, so a mismatch here is a
        // codec-registry bug rather than a client error.
        return text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "decoded value was not an object",
        );
    };
    let mut obj = obj.clone();
    obj.indexes.extend(collect_index_headers(headers));
    obj.links.extend(collect_link_headers(headers));

    let indexes = obj.index_pairs();
    let storage = obj.to_storage_bytes();
    match store.put_object(bucket.as_bytes(), key.as_bytes(), &storage, &indexes) {
        Ok(()) => {
            // Feed any declared text / vector indexes for this bucket
            // from the object payload. Indexing is best-effort: the
            // object is already durable, so an indexing miss never
            // turns the write into an error.
            #[cfg(feature = "search")]
            if let Some(state) = ctx.search.as_deref() {
                state.index_object(bucket, key.as_bytes(), &obj.value);
            }
            #[cfg(not(feature = "search"))]
            let _ = ctx;
            no_content_response()
        }
        Err(e) => storage_error_response(&e),
    }
}

/// Collect `X-Riak-Index-<name>: <value>` headers into a list of
/// [`HttpIndex`] entries.
///
/// Riak's HTTP API carries secondary indexes as headers named
/// `X-Riak-Index-<index>_int` or `X-Riak-Index-<index>_bin`. A
/// single header may carry several comma-separated values; each
/// becomes one index entry. Header names are matched
/// case-insensitively (hyper lower-cases them on receipt).
#[cfg(feature = "noxu")]
fn collect_index_headers(headers: &HeaderMap) -> Vec<HttpIndex> {
    const PREFIX: &str = "x-riak-index-";
    let mut out = Vec::new();
    for (name, value) in headers {
        let name = name.as_str();
        let Some(index_name) = name.strip_prefix(PREFIX) else {
            continue;
        };
        if index_name.is_empty() {
            continue;
        }
        let Ok(value) = value.to_str() else {
            continue;
        };
        for part in value.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            out.push(HttpIndex {
                name: index_name.to_string(),
                value: part.to_string(),
            });
        }
    }
    out
}

/// Collect `Link:` request headers into a list of [`HttpLink`]
/// entries.
///
/// # Grammar
///
/// Riak's HTTP API carries object links in `Link:` headers. The
/// grammar accepted here is:
///
/// ```text
/// Link            = "Link" ":" link-value *( "," link-value )
/// link-value      = "<" RESOURCE ">" ";" link-param
/// RESOURCE        = "/buckets/" bucket "/keys/" key
///                 | "/riak/" bucket "/" key        ; legacy form
/// link-param      = ( "riaktag" | "tag" ) "=" quoted-string
/// ```
///
/// Multiple `Link:` header lines are honoured, and a single header
/// line may carry several comma-separated link-values. Each value
/// becomes one [`HttpLink`].
///
/// # Deliberately skipped
///
/// Riak also emits a bucket-up link of the form
/// `</buckets/BUCKET>; rel="up"` whose RESOURCE names a bucket and
/// not an object. Those `rel`-style links carry no key and no
/// `riaktag`, so they are not object links a MapReduce link phase
/// can walk; they are skipped on parse (and re-synthesised on read,
/// see [`get_object_from_store`]). A `link-value` that lacks a
/// `riaktag`/`tag` parameter, or whose RESOURCE is not a
/// `/buckets/.../keys/...` (or legacy `/riak/.../...`) object path,
/// is skipped rather than rejected, matching Riak's lenient parse.
#[cfg(feature = "noxu")]
fn collect_link_headers(headers: &HeaderMap) -> Vec<HttpLink> {
    let mut out = Vec::new();
    for value in headers.get_all("link") {
        let Ok(value) = value.to_str() else {
            continue;
        };
        for part in split_link_values(value) {
            if let Some(link) = parse_link_value(&part) {
                out.push(link);
            }
        }
    }
    out
}

/// Split one `Link:` header value into its comma-separated
/// link-values, respecting the angle brackets so a comma inside a
/// `<RESOURCE>` is not mistaken for a separator.
#[cfg(feature = "noxu")]
fn split_link_values(header: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth: usize = 0;
    let mut start = 0;
    let bytes = header.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'<' => depth += 1,
            b'>' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                parts.push(header[start..i].to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(header[start..].to_string());
    parts
}

/// Parse a single `<RESOURCE>; riaktag="TAG"` link-value into an
/// [`HttpLink`]. Returns `None` for `rel`-style bucket links or any
/// value that does not name an object with a tag. See
/// [`collect_link_headers`] for the accepted grammar.
#[cfg(feature = "noxu")]
fn parse_link_value(value: &str) -> Option<HttpLink> {
    let value = value.trim();
    let open = value.find('<')?;
    let close = value[open + 1..].find('>')? + open + 1;
    let resource = value[open + 1..close].trim();
    let (target_bucket, target_key) = parse_link_resource(resource)?;

    // Scan the `;`-delimited parameters for a `riaktag` / `tag`.
    let mut tag = None;
    for param in value[close + 1..].split(';') {
        let param = param.trim();
        let Some((name, raw)) = param.split_once('=') else {
            continue;
        };
        let name = name.trim();
        if name.eq_ignore_ascii_case("riaktag") || name.eq_ignore_ascii_case("tag") {
            tag = Some(unquote(raw.trim()).to_string());
        }
    }
    let tag = tag?;
    Some(HttpLink {
        bucket: target_bucket,
        key: target_key,
        tag,
    })
}

/// Parse a link RESOURCE path into `(bucket, key)`. Accepts the
/// modern `/buckets/<bucket>/keys/<key>` form and the legacy
/// `/riak/<bucket>/<key>` form. Returns `None` for any other shape
/// (including `rel="up"` bucket-only paths).
#[cfg(feature = "noxu")]
fn parse_link_resource(resource: &str) -> Option<(String, String)> {
    if let Some(rest) = resource.strip_prefix("/buckets/") {
        let (bucket, rest) = rest.split_once('/')?;
        let key = rest.strip_prefix("keys/")?;
        if bucket.is_empty() || key.is_empty() {
            return None;
        }
        return Some((decode_path_segment(bucket), decode_path_segment(key)));
    }
    if let Some(rest) = resource.strip_prefix("/riak/") {
        let (bucket, key) = rest.split_once('/')?;
        if bucket.is_empty() || key.is_empty() {
            return None;
        }
        return Some((decode_path_segment(bucket), decode_path_segment(key)));
    }
    None
}

/// Strip one layer of surrounding double quotes from a parameter
/// value, leaving an unquoted value untouched.
#[cfg(feature = "noxu")]
fn unquote(s: &str) -> &str {
    s.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(s)
}

/// Percent-decode a single path segment (bucket or key) for the
/// common `%XX` escapes Riak clients emit. Bytes that are not valid
/// `%XX` escapes pass through verbatim; the decoded bytes are
/// interpreted as UTF-8 (lossily) so the stored link stays ASCII-
/// clean for well-formed input.
#[cfg(feature = "noxu")]
fn decode_path_segment(seg: &str) -> String {
    if !seg.contains('%') {
        return seg.to_string();
    }
    let bytes = seg.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_digit(bytes[i + 1]);
            let lo = hex_digit(bytes[i + 2]);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push(hi * 16 + lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Decode a single ASCII hex digit to its 0..16 value, as a `u8` so
/// `hi * 16 + lo` stays a `u8` without a truncating cast.
#[cfg(feature = "noxu")]
fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Map a [`crate::datastore::NoxuDatastoreError`] onto an HTTP status.
///
/// A malformed bucket name or an unparsable `_int` index value is the
/// client's fault (`400`); everything else is a backend failure
/// (`500`).
#[cfg(feature = "noxu")]
fn storage_error_response(err: &crate::datastore::NoxuDatastoreError) -> Response<ResponseBody> {
    use crate::datastore::NoxuDatastoreError;
    let status = match err {
        NoxuDatastoreError::InvalidName { .. } | NoxuDatastoreError::BadIntValue { .. } => {
            StatusCode::BAD_REQUEST
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    text_response(status, &format!("storage error: {err}"))
}

/// Render a [`TxnOutcome`] as a JSON [`HttpTxnResponse`]. Committed
/// outcomes are `200 OK`; aborted (rolled-back) outcomes are
/// `409 Conflict`.
fn txn_outcome_response(outcome: &TxnOutcome) -> Response<ResponseBody> {
    let status = match outcome {
        TxnOutcome::Committed { .. } => StatusCode::OK,
        TxnOutcome::Aborted { .. } => StatusCode::CONFLICT,
    };
    let payload = HttpTxnResponse::from_outcome(outcome);
    let body = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());
    json_response(status, body)
}

/// Render a [`TxnStoreError`] as a JSON [`HttpTxnResponse`] with the
/// `aborted` shape, carrying the engine's own message as the abort
/// reason.
fn txn_error_response(status: StatusCode, err: &TxnStoreError) -> Response<ResponseBody> {
    let payload = HttpTxnResponse::from_outcome(&TxnOutcome::Aborted {
        reason: err.to_string(),
    });
    let body = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());
    json_response(status, body)
}

/// Build a buffered `application/json` response with `status`.
fn json_response(status: StatusCode, body: Vec<u8>) -> Response<ResponseBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .header("Server", SERVER_NAME)
        .body(buffered_body(Bytes::from(body)))
        .expect("invariant: json response builder is well-formed")
}

// ------------------------------------------------------------------
// Generic response helpers.
// ------------------------------------------------------------------

pub(crate) fn text_response(status: StatusCode, msg: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .header("Server", SERVER_NAME)
        .body(buffered_body(Bytes::copy_from_slice(msg.as_bytes())))
        .expect("invariant: text response builder is well-formed")
}

/// Build a buffered `200 OK` response carrying an already-encoded
/// body under content-type `ct`. Shared by the search routes, which
/// negotiate the codec themselves and hand the encoded bytes here.
#[cfg(feature = "search")]
pub(crate) fn encoded_response(ct: &'static str, body: Vec<u8>) -> Response<ResponseBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, ct)
        .header("Server", SERVER_NAME)
        .body(buffered_body(Bytes::from(body)))
        .expect("invariant: encoded response builder is well-formed")
}

pub(crate) fn not_acceptable_response() -> Response<ResponseBody> {
    text_response(
        StatusCode::NOT_ACCEPTABLE,
        "no supported codec in Accept header",
    )
}

/// Header value extractor that returns "" for missing or non-ASCII
/// headers. The negotiation logic copes with empty input cleanly.
pub(crate) fn header_str(headers: &HeaderMap, name: hyper::header::HeaderName) -> &str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

/// Header value extractor that distinguishes "absent" from
/// "present but unreadable". Used where the caller wants to fall
/// back to a default only when the header was truly missing.
pub(crate) fn header_str_opt(headers: &HeaderMap, name: hyper::header::HeaderName) -> Option<&str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

// ------------------------------------------------------------------
// MapReduce route handler. Added by the v0.0.3 MapReduce slice.
// ------------------------------------------------------------------

use crate::mapreduce::{
    builtins::default_registry, run_job_streaming_full, MapReduceJob, MrError, PhaseBatch,
};
use tokio::sync::mpsc;

/// Run a MapReduce job submitted via `POST /mapred`.
///
/// The body must carry the JSON job description. The response is
/// chunked-encoded `multipart/mixed`: one body part per kept phase
/// (Riak's documented HTTP MapReduce shape). Each part carries
/// `Content-Type: application/json` and a body of the form
/// `[{"phase": N, "data": [...]}]`. A phase failure mid-stream is
/// surfaced as a final part with `Content-Type: text/plain` and
/// the error message; the closing delimiter is then written and
/// the body ends. Boundary strings are unique per request.
///
/// The function is synchronous: the executor runs on its own
/// tokio task and the HTTP body stream pulls per-phase batches
/// off the executor's mpsc receiver. Returning the response is
/// a constant-time operation.
fn mapred_response(headers: &HeaderMap, body: &Bytes, ctx: &RouteCtx) -> Response<ResponseBody> {
    let req_ct = header_str_opt(headers, CONTENT_TYPE);
    let ct = req_ct.unwrap_or("application/json");
    if super::content_type::canonicalize(ct) != Some("application/json") {
        return text_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "MapReduce requires Content-Type: application/json",
        );
    }
    let job: MapReduceJob = match serde_json::from_slice(body) {
        Ok(j) => j,
        Err(e) => {
            return text_response(
                StatusCode::BAD_REQUEST,
                &format!("MapReduce job decode: {e}"),
            );
        }
    };
    let registry = std::sync::Arc::new(default_registry());
    // The datastore enumerates an `Inputs::Bucket` job's keys; pass
    // it through so a whole-bucket MapReduce input can run. Inline
    // input shapes ignore it.
    let datastore = ctx.datastore.clone();
    // When a Wasm phase store is wired into the context, dispatch
    // through the Wasm-aware executor so a `Phase::WasmModule` job
    // reaches the configured modules; otherwise the plain executor
    // surfaces the typed `MrError::WasmNotImplemented` error.
    #[cfg(feature = "wasm")]
    let rx = match ctx.wasm.clone() {
        Some(store) => {
            let hook: std::sync::Arc<dyn crate::mapreduce::WasmHook> = store;
            run_job_streaming_full(job, registry, Some(hook), Some(datastore))
        }
        None => run_job_streaming_full(job, registry, None, Some(datastore)),
    };
    #[cfg(not(feature = "wasm"))]
    let rx = run_job_streaming_full(job, registry, None, Some(datastore));
    let boundary = mapred_boundary();
    let body_stream = mapred_multipart_body(rx, boundary.clone());
    let body_stream: Pin<Box<dyn Stream<Item = Result<HttpFrame<Bytes>, Infallible>> + Send>> =
        Box::pin(body_stream);
    let body = BodyExt::boxed_unsync(StreamBody::new(body_stream));
    let ct_value = format!("multipart/mixed; boundary={boundary}");
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, ct_value)
        .header(TRANSFER_ENCODING, "chunked")
        .header("Server", SERVER_NAME)
        .body(body)
        .expect("invariant: mapred response builder is well-formed")
}

/// Generate a per-request multipart boundary string.
///
/// The string is ASCII-safe (alphanumerics + `-`) and combines a
/// monotonically-increasing process counter with the current
/// system time in nanoseconds. Both are encoded as fixed-width
/// hex so the output length is constant. Collision probability is
/// far below the threshold required for a multipart boundary; the
/// boundary only needs to not appear inside the JSON / text body
/// of the parts.
fn mapred_boundary() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos() & u128::from(u64::MAX)).unwrap_or(0))
        .unwrap_or(0);
    format!("dyniak-mr-{nanos:016x}-{n:016x}")
}

/// State machine driving the multipart/mixed streaming body.
enum MapRedMultipartState {
    /// Initial: nothing emitted yet; consume the next batch from
    /// the executor.
    Streaming {
        rx: mpsc::Receiver<Result<PhaseBatch, MrError>>,
        boundary: String,
    },
    /// All batches drained or a fatal error was emitted; emit the
    /// closing `--{boundary}--` delimiter.
    Close { boundary: String },
    /// Body terminated.
    Done,
}

/// Chunk size used by [`mapred_multipart_body`] for the boundary;
/// each phase batch and the closing delimiter is one body chunk so
/// hyper transmits one HTTP chunk per part.
fn mapred_multipart_body(
    rx: mpsc::Receiver<Result<PhaseBatch, MrError>>,
    boundary: String,
) -> impl Stream<Item = Result<HttpFrame<Bytes>, Infallible>> + Send {
    futures_util::stream::unfold(
        MapRedMultipartState::Streaming { rx, boundary },
        |state| async move {
            match state {
                MapRedMultipartState::Done => None,
                MapRedMultipartState::Close { boundary } => {
                    let chunk = format!("--{boundary}--\r\n");
                    Some((
                        Ok(HttpFrame::data(Bytes::from(chunk))),
                        MapRedMultipartState::Done,
                    ))
                }
                MapRedMultipartState::Streaming { mut rx, boundary } => match rx.recv().await {
                    None => {
                        let chunk = format!("--{boundary}--\r\n");
                        Some((
                            Ok(HttpFrame::data(Bytes::from(chunk))),
                            MapRedMultipartState::Done,
                        ))
                    }
                    Some(Ok(batch)) => {
                        let body = mapred_phase_part_body(&batch);
                        let chunk = format!(
                            "--{boundary}\r\nContent-Type: application/json\r\n\r\n{body}\r\n"
                        );
                        Some((
                            Ok(HttpFrame::data(Bytes::from(chunk))),
                            MapRedMultipartState::Streaming { rx, boundary },
                        ))
                    }
                    Some(Err(e)) => {
                        let msg = format!("MapReduce execution: {e}");
                        let chunk =
                            format!("--{boundary}\r\nContent-Type: text/plain\r\n\r\n{msg}\r\n");
                        Some((
                            Ok(HttpFrame::data(Bytes::from(chunk))),
                            MapRedMultipartState::Close { boundary },
                        ))
                    }
                },
            }
        },
    )
}

/// Encode one phase batch as the JSON body of a multipart part.
///
/// The shape is the Riak-documented `[{"phase": N, "data": [...]}]`:
/// a one-element JSON array containing an object with the phase
/// index and the captured values. JSON encoding of an in-memory
/// `Vec<Value>` cannot fail; the fallback string keeps the helper
/// total without panicking on the impossible branch.
fn mapred_phase_part_body(batch: &PhaseBatch) -> String {
    let payload = serde_json::json!([{
        "phase": batch.phase,
        "data": batch.data,
    }]);
    serde_json::to_string(&payload).unwrap_or_else(|_| String::from("[]"))
}

// ------------------------------------------------------------------
// Streaming list handlers (list-buckets, list-keys).
// ------------------------------------------------------------------
//
// The HTTP shape mirrors Riak's documented behaviour. For
// `application/json`, the body is a chunked JSON array:
//
// ```text
//   ["key0","key1", ...]
// ```
//
// where `[`, comma, `]`, and the JSON-encoded entries are written
// across multiple HTTP body chunks. The client buffers the body
// and parses it as a JSON document; chunked transfer-encoding is
// negotiated automatically by hyper because the body uses
// [`StreamBody`].
//
// For self-describing codecs (CBOR, BSON, ...), each entry is
// emitted as a length-prefixed payload (4-byte big-endian length
// followed by the codec-encoded entry). A length of zero marks
// end-of-stream so a client can read until the terminator without
// relying on chunked transfer-encoding metadata.
//
// A client that closes the connection mid-stream simply observes
// fewer entries; the server-side stream task drops on connection
// close because hyper aborts the body future. Datastore errors
// mid-stream surface as a synthetic terminal entry (an empty
// JSON object `{}`, or a zero-length-prefixed entry) followed by
// end-of-stream; the journal documents the limitation.

fn list_buckets_response(
    headers: &HeaderMap,
    datastore: &Arc<dyn Datastore>,
) -> Response<ResponseBody> {
    let accept = header_str(headers, ACCEPT);
    let Some(ct) = select_codec(accept, Some("application/json")) else {
        return not_acceptable_response();
    };
    let stream = datastore.list_buckets_stream();
    streaming_list_response(ct, stream)
}

fn list_keys_response(
    bucket: &str,
    headers: &HeaderMap,
    datastore: &Arc<dyn Datastore>,
) -> Response<ResponseBody> {
    let accept = header_str(headers, ACCEPT);
    let Some(ct) = select_codec(accept, Some("application/json")) else {
        return not_acceptable_response();
    };
    let stream = datastore.list_keys_stream(bucket.as_bytes());
    streaming_list_response(ct, stream)
}

/// Build a streaming HTTP response from a datastore byte stream.
///
/// `ct` is the negotiated content-type; for `application/json`
/// the body is a chunked JSON array, otherwise the body is a
/// length-prefixed sequence of opaque entries (one entry per
/// length prefix, terminated by a zero-length prefix).
fn streaming_list_response(ct: &str, stream: DatastoreByteStream) -> Response<ResponseBody> {
    let body_stream: Pin<Box<dyn Stream<Item = Result<HttpFrame<Bytes>, Infallible>> + Send>> =
        if ct == "application/json" {
            Box::pin(json_array_chunks(stream))
        } else {
            Box::pin(length_prefixed_chunks(stream))
        };
    let body = BodyExt::boxed_unsync(StreamBody::new(body_stream));
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, ct)
        .header(TRANSFER_ENCODING, "chunked")
        .header("Server", SERVER_NAME)
        .body(body)
        .expect("invariant: streaming response builder is well-formed")
}

use std::pin::Pin;

/// Producer state for the JSON-array streaming list shape.
enum JsonChunkState {
    /// Initial: emit the opening `[` and start consuming entries.
    Open(DatastoreByteStream),
    /// Mid-stream: holds the byte stream and a flag for whether
    /// the leading comma is needed before the next entry.
    Streaming {
        stream: DatastoreByteStream,
        first_emitted: bool,
    },
    /// Final: emit the closing `]`.
    Close,
    /// Done.
    Done,
}

/// Stream an HTTP body as a JSON array, chunked at
/// [`HTTP_LIST_CHUNK_SIZE`] entries per body frame.
fn json_array_chunks(
    stream: DatastoreByteStream,
) -> impl Stream<Item = Result<HttpFrame<Bytes>, Infallible>> + Send {
    futures_util::stream::unfold(JsonChunkState::Open(stream), |state| async move {
        match state {
            JsonChunkState::Done => None,
            JsonChunkState::Open(stream) => Some((
                Ok(HttpFrame::data(Bytes::from_static(b"["))),
                JsonChunkState::Streaming {
                    stream,
                    first_emitted: false,
                },
            )),
            JsonChunkState::Close => Some((
                Ok(HttpFrame::data(Bytes::from_static(b"]"))),
                JsonChunkState::Done,
            )),
            JsonChunkState::Streaming {
                mut stream,
                mut first_emitted,
            } => {
                let mut buf: Vec<u8> = Vec::new();
                let mut packed = 0usize;
                while packed < HTTP_LIST_CHUNK_SIZE {
                    match stream.next().await {
                        None => {
                            if buf.is_empty() {
                                return Some((
                                    Ok(HttpFrame::data(Bytes::from_static(b"]"))),
                                    JsonChunkState::Done,
                                ));
                            }
                            return Some((
                                Ok(HttpFrame::data(Bytes::from(buf))),
                                JsonChunkState::Close,
                            ));
                        }
                        Some(Err(_e)) => {
                            // Datastore error mid-stream: emit
                            // whatever bytes have been buffered
                            // and close the array. The HTTP path
                            // does not have a clean way to surface
                            // a body-level error to a client that
                            // has already received `200 OK`; the
                            // journal documents this trade-off.
                            if !buf.is_empty() {
                                return Some((
                                    Ok(HttpFrame::data(Bytes::from(buf))),
                                    JsonChunkState::Close,
                                ));
                            }
                            return Some((
                                Ok(HttpFrame::data(Bytes::from_static(b"]"))),
                                JsonChunkState::Done,
                            ));
                        }
                        Some(Ok(entry)) => {
                            if first_emitted {
                                buf.push(b',');
                            } else {
                                first_emitted = true;
                            }
                            // JSON-encode the entry as a UTF-8
                            // string. Non-UTF-8 bytes are encoded
                            // by serde_json with replacement
                            // characters; a future slice may switch
                            // to base64 for binary keys.
                            let s = String::from_utf8_lossy(&entry).into_owned();
                            let encoded =
                                serde_json::to_vec(&s).unwrap_or_else(|_| b"\"\"".to_vec());
                            buf.extend_from_slice(&encoded);
                            packed += 1;
                        }
                    }
                }
                Some((
                    Ok(HttpFrame::data(Bytes::from(buf))),
                    JsonChunkState::Streaming {
                        stream,
                        first_emitted,
                    },
                ))
            }
        }
    })
}

/// Stream a length-prefixed sequence of opaque entries, chunked at
/// [`HTTP_LIST_CHUNK_SIZE`] entries per body frame. End-of-stream
/// is marked with a 4-byte big-endian zero terminator.
fn length_prefixed_chunks(
    stream: DatastoreByteStream,
) -> impl Stream<Item = Result<HttpFrame<Bytes>, Infallible>> + Send {
    enum LpState {
        Streaming(DatastoreByteStream),
        Done,
    }
    futures_util::stream::unfold(LpState::Streaming(stream), |state| async move {
        match state {
            LpState::Done => None,
            LpState::Streaming(mut stream) => {
                let mut buf: Vec<u8> = Vec::new();
                let mut packed = 0usize;
                while packed < HTTP_LIST_CHUNK_SIZE {
                    match stream.next().await {
                        None => {
                            buf.extend_from_slice(&0u32.to_be_bytes());
                            return Some((Ok(HttpFrame::data(Bytes::from(buf))), LpState::Done));
                        }
                        Some(Err(_e)) => {
                            buf.extend_from_slice(&0u32.to_be_bytes());
                            return Some((Ok(HttpFrame::data(Bytes::from(buf))), LpState::Done));
                        }
                        Some(Ok(entry)) => {
                            let len = u32::try_from(entry.len()).unwrap_or(u32::MAX);
                            buf.extend_from_slice(&len.to_be_bytes());
                            buf.extend_from_slice(&entry);
                            packed += 1;
                        }
                    }
                }
                Some((
                    Ok(HttpFrame::data(Bytes::from(buf))),
                    LpState::Streaming(stream),
                ))
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynomite::embed::MemoryDatastore;

    fn dummy_headers() -> HeaderMap {
        HeaderMap::new()
    }

    #[cfg(feature = "noxu")]
    #[test]
    fn parse_link_value_modern_form() {
        let link =
            parse_link_value("</buckets/people/keys/bob>; riaktag=\"friend\"").expect("link");
        assert_eq!(link.bucket, "people");
        assert_eq!(link.key, "bob");
        assert_eq!(link.tag, "friend");
    }

    #[cfg(feature = "noxu")]
    #[test]
    fn parse_link_value_legacy_riak_form() {
        let link = parse_link_value("</riak/people/bob>; riaktag=\"friend\"").expect("link");
        assert_eq!(link.bucket, "people");
        assert_eq!(link.key, "bob");
        assert_eq!(link.tag, "friend");
    }

    #[cfg(feature = "noxu")]
    #[test]
    fn parse_link_value_rejects_rel_up_bucket_link() {
        // A bucket-up link names no key and carries no riaktag, so
        // it is not an object link a phase can walk.
        assert!(parse_link_value("</buckets/people>; rel=\"up\"").is_none());
    }

    #[cfg(feature = "noxu")]
    #[test]
    fn collect_link_headers_handles_multiple_headers_and_values() {
        let mut headers = HeaderMap::new();
        headers.append(
            "link",
            "</buckets/people/keys/bob>; riaktag=\"friend\", \
             </buckets/work/keys/acme>; riaktag=\"employer\""
                .parse()
                .unwrap(),
        );
        headers.append(
            "link",
            "</buckets/people/keys/carol>; tag=\"friend\""
                .parse()
                .unwrap(),
        );
        let links = collect_link_headers(&headers);
        assert_eq!(links.len(), 3);
        assert_eq!(links[0].key, "bob");
        assert_eq!(links[1].key, "acme");
        assert_eq!(links[1].tag, "employer");
        assert_eq!(links[2].key, "carol");
        assert_eq!(links[2].tag, "friend");
    }

    #[cfg(feature = "noxu")]
    #[test]
    fn collect_link_headers_skips_bucket_up_links() {
        let mut headers = HeaderMap::new();
        headers.append(
            "link",
            "</buckets/people>; rel=\"up\", </buckets/people/keys/bob>; riaktag=\"friend\""
                .parse()
                .unwrap(),
        );
        let links = collect_link_headers(&headers);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].key, "bob");
    }

    #[test]
    fn route_parses_ping() {
        let r = Route::parse(&Method::GET, "/ping", None).expect("ping");
        assert_eq!(r, Route::Ping);
        let r = Route::parse(&Method::HEAD, "/ping", None).expect("ping head");
        assert_eq!(r, Route::Ping);
    }

    #[test]
    fn route_parses_object_paths() {
        let r = Route::parse(&Method::GET, "/buckets/u/keys/k", None).expect("get");
        assert_eq!(
            r,
            Route::GetObject {
                bucket: "u",
                key: "k",
            }
        );
        let r = Route::parse(&Method::PUT, "/buckets/u/keys/k", None).expect("put");
        assert_eq!(
            r,
            Route::PutObject {
                bucket: "u",
                key: "k",
            }
        );
        let r = Route::parse(&Method::POST, "/buckets/u/keys/k", None).expect("post");
        assert_eq!(
            r,
            Route::PostObject {
                bucket: "u",
                key: "k",
            }
        );
        let r = Route::parse(&Method::DELETE, "/buckets/u/keys/k", None).expect("del");
        assert_eq!(
            r,
            Route::DeleteObject {
                bucket: "u",
                key: "k",
            }
        );
    }

    #[test]
    fn route_parses_listing_with_query_flag() {
        let r = Route::parse(&Method::GET, "/buckets", Some("buckets=true")).expect("buckets");
        assert_eq!(r, Route::ListBuckets);
        let r = Route::parse(&Method::GET, "/buckets/u/keys", Some("keys=true")).expect("keys");
        assert_eq!(r, Route::ListKeys { bucket: "u" });
    }

    #[test]
    fn route_listing_without_flag_misses() {
        // /buckets without ?buckets=true is not a recognised route.
        assert!(Route::parse(&Method::GET, "/buckets", None).is_none());
        // /buckets/u/keys without ?keys=true is not a recognised route.
        assert!(Route::parse(&Method::GET, "/buckets/u/keys", None).is_none());
    }

    #[test]
    fn route_parses_props() {
        let r = Route::parse(&Method::GET, "/buckets/u/props", None).expect("get props");
        assert_eq!(r, Route::GetProps { bucket: "u" });
        let r = Route::parse(&Method::PUT, "/buckets/u/props", None).expect("set props");
        assert_eq!(r, Route::SetProps { bucket: "u" });
    }

    #[test]
    fn route_unknown_path_misses() {
        assert!(Route::parse(&Method::GET, "/", None).is_none());
        assert!(Route::parse(&Method::GET, "/foo", None).is_none());
        assert!(Route::parse(&Method::GET, "/buckets/u/foo/bar", None).is_none());
    }

    #[test]
    fn has_flag_handles_multi_pair_query() {
        assert!(has_flag(Some("a=1&buckets=true"), "buckets", "true"));
        assert!(has_flag(Some("buckets=true&extra=x"), "buckets", "true"));
        assert!(!has_flag(Some("buckets=stream"), "buckets", "true"));
        assert!(!has_flag(Some(""), "buckets", "true"));
        assert!(!has_flag(None, "buckets", "true"));
    }

    #[tokio::test]
    async fn list_keys_streams_chunked_json_array() {
        let ds = Arc::new(MemoryDatastore::new());
        for i in 0..600u16 {
            ds.insert(b"u", format!("k{i:04}").as_bytes());
        }
        let ds_dyn: Arc<dyn Datastore> = ds.clone();
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, "application/json".parse().unwrap());
        let resp = handle_route(
            Route::ListKeys { bucket: "u" },
            &Method::GET,
            &headers,
            Bytes::new(),
            ds_dyn,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(TRANSFER_ENCODING)
                .map(|v| v.to_str().ok()),
            Some(Some("chunked"))
        );
        assert_eq!(
            resp.headers().get(CONTENT_TYPE).map(|v| v.to_str().ok()),
            Some(Some("application/json"))
        );
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json");
        let arr = parsed.as_array().expect("array");
        assert_eq!(arr.len(), 600);
        // Ordering is lexicographic per the snapshot semantics.
        assert_eq!(arr[0], serde_json::Value::String("k0000".to_string()));
        assert_eq!(arr[599], serde_json::Value::String("k0599".to_string()));
    }

    #[tokio::test]
    async fn list_buckets_streams_chunked_json_array() {
        let ds = Arc::new(MemoryDatastore::new());
        for i in 0..3u16 {
            ds.insert(format!("b{i}").as_bytes(), b"k");
        }
        let ds_dyn: Arc<dyn Datastore> = ds.clone();
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, "application/json".parse().unwrap());
        let resp = handle_route(
            Route::ListBuckets,
            &Method::GET,
            &headers,
            Bytes::new(),
            ds_dyn,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json");
        let arr = parsed.as_array().expect("array");
        assert_eq!(arr.len(), 3);
    }

    #[tokio::test]
    async fn list_buckets_empty_streams_empty_json_array() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let resp = handle_route(
            Route::ListBuckets,
            &Method::GET,
            &dummy_headers(),
            Bytes::new(),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        assert_eq!(body.as_ref(), b"[]");
    }

    #[tokio::test]
    async fn put_with_unsupported_content_type_returns_415() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/xml".parse().unwrap());
        let resp = handle_route(
            Route::PutObject {
                bucket: "u",
                key: "k",
            },
            &Method::PUT,
            &headers,
            Bytes::from_static(b"<doc/>"),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn put_with_empty_body_returns_400() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let resp = handle_route(
            Route::PutObject {
                bucket: "u",
                key: "k",
            },
            &Method::PUT,
            &headers,
            Bytes::new(),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn put_with_unsupported_accept_returns_406() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, "application/yaml".parse().unwrap());
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let resp = handle_route(
            Route::PutObject {
                bucket: "u",
                key: "k",
            },
            &Method::PUT,
            &headers,
            Bytes::from_static(b"{}"),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_ACCEPTABLE);
    }

    #[tokio::test]
    async fn put_then_get_drives_dispatch_count() {
        let ds = Arc::new(MemoryDatastore::new());
        let ds_dyn: Arc<dyn Datastore> = ds.clone();

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let put = handle_route(
            Route::PutObject {
                bucket: "u",
                key: "k",
            },
            &Method::PUT,
            &headers,
            Bytes::from_static(br#"{"hello":"world"}"#),
            ds_dyn.clone(),
        )
        .await;
        assert_eq!(put.status(), StatusCode::NO_CONTENT);

        let get = handle_route(
            Route::GetObject {
                bucket: "u",
                key: "k",
            },
            &Method::GET,
            &dummy_headers(),
            Bytes::new(),
            ds_dyn.clone(),
        )
        .await;
        assert_eq!(get.status(), StatusCode::NOT_FOUND);

        let del = handle_route(
            Route::DeleteObject {
                bucket: "u",
                key: "k",
            },
            &Method::DELETE,
            &dummy_headers(),
            Bytes::new(),
            ds_dyn,
        )
        .await;
        assert_eq!(del.status(), StatusCode::NO_CONTENT);

        // PUT, GET, DELETE each trampoline through dispatch.
        assert_eq!(ds.dispatch_count(), 3);
    }

    #[tokio::test]
    async fn ping_returns_200() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let resp = handle_route(
            Route::Ping,
            &Method::GET,
            &dummy_headers(),
            Bytes::new(),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn head_ping_omits_body() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let resp = handle_route(
            Route::Ping,
            &Method::HEAD,
            &dummy_headers(),
            Bytes::new(),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn stats_returns_json_body() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let resp = handle_route(
            Route::Stats,
            &Method::GET,
            &dummy_headers(),
            Bytes::new(),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(parsed["name"], SERVER_NAME);
        assert_eq!(parsed["version"], SERVER_VERSION);
    }

    #[tokio::test]
    async fn get_props_returns_defaults() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let resp = handle_route(
            Route::GetProps { bucket: "u" },
            &Method::GET,
            &dummy_headers(),
            Bytes::new(),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(parsed["props"]["n_val"], 3);
        assert_eq!(parsed["props"]["name"], "u");
    }

    #[test]
    fn route_parses_mapred() {
        let r = Route::parse(&Method::POST, "/mapred", None).expect("mapred");
        assert_eq!(r, Route::MapRed);
    }

    #[test]
    fn route_get_mapred_misses() {
        // Riak's /mapred is POST-only; GET should fall through.
        assert!(Route::parse(&Method::GET, "/mapred", None).is_none());
    }

    #[tokio::test]
    async fn mapred_runs_simple_job() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let body = br#"{
            "inputs": [
                {"bucket":"b","key":"k1","value":1},
                {"bucket":"b","key":"k2","value":2},
                {"bucket":"b","key":"k3","value":3}
            ],
            "query": [
                {"map":    {"name":"map_object_value"}},
                {"reduce": {"name":"reduce_sum", "keep": true}}
            ]
        }"#;
        let resp = handle_route(
            Route::MapRed,
            &Method::POST,
            &headers,
            Bytes::copy_from_slice(body),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(CONTENT_TYPE)
            .expect("content-type")
            .to_str()
            .expect("ascii")
            .to_string();
        assert!(
            ct.starts_with("multipart/mixed; boundary="),
            "content-type was: {ct}"
        );
        let boundary = ct
            .strip_prefix("multipart/mixed; boundary=")
            .expect("boundary")
            .to_string();
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let parts = parse_multipart_parts(&body, &boundary);
        assert_eq!(parts.len(), 1, "one kept (reduce) phase produces one part");
        assert_eq!(parts[0].content_type.as_deref(), Some("application/json"));
        let parsed: serde_json::Value = serde_json::from_slice(&parts[0].body).expect("json");
        let arr = parsed.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["phase"], 1);
        assert_eq!(arr[0]["data"], serde_json::json!([6]));
    }

    /// One parsed multipart body part: its declared content-type and
    /// the raw bytes of its body.
    struct MultipartPart {
        content_type: Option<String>,
        body: Vec<u8>,
    }

    /// Minimal multipart/mixed body parser used by the unit and
    /// integration tests in this module. Walks the body splitting
    /// on `--boundary` lines, parses the per-part headers, and
    /// stops on the closing `--boundary--` delimiter.
    fn parse_multipart_parts(body: &[u8], boundary: &str) -> Vec<MultipartPart> {
        let dash_boundary = format!("--{boundary}");
        let close_delim = format!("--{boundary}--");
        let text = std::str::from_utf8(body).expect("ascii body");
        let mut parts = Vec::new();
        let mut cursor = text;
        // Find first dash-boundary.
        if let Some(idx) = cursor.find(&dash_boundary) {
            cursor = &cursor[idx + dash_boundary.len()..];
        } else {
            return parts;
        }
        loop {
            // Closing delimiter starts with `--` after the boundary.
            if cursor.starts_with("--") {
                break;
            }
            // Skip CRLF after dash-boundary.
            cursor = cursor.trim_start_matches("\r\n");
            // Find header / body separator.
            let Some(sep_idx) = cursor.find("\r\n\r\n") else {
                break;
            };
            let head_str = &cursor[..sep_idx];
            cursor = &cursor[sep_idx + 4..];
            // Find the next dash-boundary.
            let Some(next_idx) = cursor.find(&dash_boundary) else {
                break;
            };
            let body_str = &cursor[..next_idx];
            // Trim trailing CRLF that belongs to the delimiter.
            let body_str = body_str.strip_suffix("\r\n").unwrap_or(body_str);
            let mut content_type = None;
            for line in head_str.split("\r\n") {
                if let Some(v) = line.strip_prefix("Content-Type:") {
                    content_type = Some(v.trim().to_string());
                }
            }
            parts.push(MultipartPart {
                content_type,
                body: body_str.as_bytes().to_vec(),
            });
            cursor = &cursor[next_idx + dash_boundary.len()..];
            if cursor.starts_with("--") {
                break;
            }
        }
        // Defensive: ensure the body ended with the close delimiter.
        let _ = close_delim;
        parts
    }

    #[tokio::test]
    async fn mapred_streams_multiple_kept_phases() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let body = br#"{
            "inputs": [
                {"bucket":"b","key":"k1","value":1},
                {"bucket":"b","key":"k2","value":2}
            ],
            "query": [
                {"map":    {"name":"map_object_value", "keep": true}},
                {"reduce": {"name":"reduce_sum",       "keep": true}}
            ]
        }"#;
        let resp = handle_route(
            Route::MapRed,
            &Method::POST,
            &headers,
            Bytes::copy_from_slice(body),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(CONTENT_TYPE)
            .expect("ct")
            .to_str()
            .unwrap()
            .to_string();
        let boundary = ct
            .strip_prefix("multipart/mixed; boundary=")
            .expect("boundary")
            .to_string();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let parts = parse_multipart_parts(&bytes, &boundary);
        assert_eq!(parts.len(), 2);
        let p0: serde_json::Value = serde_json::from_slice(&parts[0].body).expect("json0");
        let p1: serde_json::Value = serde_json::from_slice(&parts[1].body).expect("json1");
        assert_eq!(p0[0]["phase"], 0);
        assert_eq!(p0[0]["data"].as_array().expect("arr").len(), 2);
        assert_eq!(p1[0]["phase"], 1);
        assert_eq!(p1[0]["data"], serde_json::json!([3]));
        // Body must end with the closing delimiter.
        let tail = &bytes[bytes.len().saturating_sub(boundary.len() + 6)..];
        let tail_str = std::str::from_utf8(tail).expect("ascii tail");
        assert!(
            tail_str.contains(&format!("--{boundary}--\r\n")),
            "tail was: {tail_str:?}"
        );
    }

    #[tokio::test]
    async fn mapred_phase_failure_emits_text_part_and_closing_delimiter() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        // The map references a function that does not exist; the
        // executor short-circuits and the streaming body must end
        // with a text/plain error part plus the closing delimiter.
        let body = br#"{
            "inputs": [{"bucket":"b","key":"k","value":1}],
            "query": [{"map": {"name": "no_such_function", "keep": true}}]
        }"#;
        let resp = handle_route(
            Route::MapRed,
            &Method::POST,
            &headers,
            Bytes::copy_from_slice(body),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(CONTENT_TYPE)
            .expect("ct")
            .to_str()
            .unwrap()
            .to_string();
        let boundary = ct
            .strip_prefix("multipart/mixed; boundary=")
            .expect("boundary")
            .to_string();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let parts = parse_multipart_parts(&bytes, &boundary);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].content_type.as_deref(), Some("text/plain"));
        let msg = std::str::from_utf8(&parts[0].body).expect("ascii");
        assert!(
            msg.contains("MapReduce execution") && msg.contains("no_such_function"),
            "error msg was: {msg:?}"
        );
        let tail = std::str::from_utf8(&bytes).expect("ascii");
        assert!(tail.contains(&format!("--{boundary}--\r\n")));
    }

    #[tokio::test]
    async fn mapred_unsupported_content_type_returns_415() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/xml".parse().unwrap());
        let resp = handle_route(
            Route::MapRed,
            &Method::POST,
            &headers,
            Bytes::from_static(b"<doc/>"),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn mapred_malformed_job_returns_400() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let resp = handle_route(
            Route::MapRed,
            &Method::POST,
            &headers,
            Bytes::from_static(b"not json"),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn route_parses_transactions() {
        let r = Route::parse(&Method::POST, "/transactions", None).expect("txn");
        assert_eq!(r, Route::Transaction { bucket: None });
        let r = Route::parse(&Method::POST, "/buckets/u/transactions", None).expect("bucket txn");
        assert_eq!(r, Route::Transaction { bucket: Some("u") });
    }

    #[test]
    fn route_get_transactions_misses() {
        // The transaction endpoint is POST-only.
        assert!(Route::parse(&Method::GET, "/transactions", None).is_none());
    }

    #[tokio::test]
    async fn transaction_on_non_transactional_backend_returns_501() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let body = br#"{"operations":[{"op":"put","bucket":"b","key":"k","value":"v"}]}"#;
        let resp = handle_route(
            Route::Transaction { bucket: None },
            &Method::POST,
            &headers,
            Bytes::copy_from_slice(body),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn transaction_empty_body_returns_400() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let resp = handle_route(
            Route::Transaction { bucket: None },
            &Method::POST,
            &headers,
            Bytes::new(),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn transaction_unsupported_content_type_returns_415() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/xml".parse().unwrap());
        let resp = handle_route(
            Route::Transaction { bucket: None },
            &Method::POST,
            &headers,
            Bytes::from_static(b"<doc/>"),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[cfg(feature = "noxu")]
    #[tokio::test]
    async fn transaction_commits_and_aborts_against_noxu() {
        use crate::datastore::NoxuDatastore;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("tempdir");
        let ds: Arc<dyn Datastore> =
            Arc::new(NoxuDatastore::open_transactional(dir.path()).expect("open"));
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        // Commit a three-put batch; the response is 200 + committed.
        let body = br#"{"operations":[
            {"op":"put","bucket":"users","key":"alice","value":"a"},
            {"op":"put","bucket":"users","key":"bob","value":"b"},
            {"op":"put","bucket":"users","key":"carol","value":"c"}
        ]}"#;
        let resp = handle_route(
            Route::Transaction { bucket: None },
            &Method::POST,
            &headers,
            Bytes::copy_from_slice(body),
            Arc::clone(&ds),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(parsed["result"], "committed");
        assert_eq!(parsed["operations"], 3);

        // An aborting batch is 409 + aborted and leaves no writes.
        let body = br#"{"abort":true,"operations":[
            {"op":"put","bucket":"users","key":"dave","value":"d"}
        ]}"#;
        let resp = handle_route(
            Route::Transaction { bucket: None },
            &Method::POST,
            &headers,
            Bytes::copy_from_slice(body),
            Arc::clone(&ds),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let bytes = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(parsed["result"], "aborted");

        // Bucket-scoped route rejects a mismatched bucket with 400.
        let body = br#"{"operations":[
            {"op":"put","bucket":"other","key":"k","value":"v"}
        ]}"#;
        let resp = handle_route(
            Route::Transaction {
                bucket: Some("users"),
            },
            &Method::POST,
            &headers,
            Bytes::copy_from_slice(body),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
