//! HTTP route table and per-route handlers for the Riak HTTP gateway.
//!
//! The route surface mirrors the subset of Riak's HTTP API that the
//! v0.0.1 slice supports. Unrecognised routes return `404 Not
//! Found`. Recognised routes that the underlying
//! [`dynomite::embed::Datastore`] cannot serve (for example
//! list-keys against an in-memory store) return `501 Not
//! Implemented`.
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
//! | GET         | `/buckets?buckets=true`                  | List buckets (501 v0.0.1)  |
//! | GET         | `/buckets/{bucket}/keys?keys=true`       | List keys (501 v0.0.1)     |
//! | GET         | `/buckets/{bucket}/props`                | Get bucket props            |
//! | PUT         | `/buckets/{bucket}/props`                | Set bucket props            |
//!
//! # Datastore semantics
//!
//! The handler trampolines K/V requests through
//! [`dynomite::embed::Datastore::dispatch`] in the same way
//! [`crate::server::handle_conn`] does. The substrate's accounting
//! ticks per request; the Riak-specific K/V semantics land in a
//! follow-up slice along with the [`crate::datastore`] richer
//! trait.

use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{ACCEPT, CONTENT_TYPE};
use hyper::{HeaderMap, Method, Request, Response, StatusCode};

use dynomite::embed::Datastore;
use dynomite::msg::{Msg, MsgType};

use crate::proto::http::content_type::{select_codec, SUPPORTED_CONTENT_TYPES};

/// Body type the gateway emits. Single-shot, fully buffered.
pub(crate) type ResponseBody = Full<Bytes>;

/// Maximum HTTP request body the gateway will accept. Mirrors the
/// PBC framer's 16 MiB cap.
const MAX_BODY_LEN: usize = 16 * 1024 * 1024;

/// Server name reported through `/stats` and the `Server` header.
const SERVER_NAME: &str = "dyn-riak";

/// Server version reported through `/stats`. Bumped in lockstep with
/// the crate version.
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Dispatch entry point. Reads the request, walks the route table,
/// and produces a single buffered response.
///
/// This function never returns an error: every failure path is
/// turned into an HTTP response so the hyper service contract is
/// satisfied with `Result<_, Infallible>`.
pub(crate) async fn dispatch(
    req: Request<Incoming>,
    datastore: Arc<dyn Datastore>,
) -> Response<ResponseBody> {
    let (parts, body) = req.into_parts();
    let Some(route) = Route::parse(&parts.method, parts.uri.path(), parts.uri.query()) else {
        return text_response(StatusCode::NOT_FOUND, "not found");
    };

    let body_bytes = match collect_body(body).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    handle_route(route, &parts.method, &parts.headers, body_bytes, datastore).await
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
    datastore: Arc<dyn Datastore>,
) -> Response<ResponseBody> {
    let head_only = method == Method::HEAD;
    match route {
        Route::Ping => ping_response(head_only),
        Route::Stats => stats_response(headers),
        Route::GetObject { bucket, key } => {
            handle_get(bucket, key, headers, head_only, datastore.as_ref()).await
        }
        Route::PutObject { bucket, key } | Route::PostObject { bucket, key } => {
            handle_put(bucket, key, headers, body, datastore.as_ref()).await
        }
        Route::DeleteObject { bucket, key } => handle_delete(bucket, key, datastore.as_ref()).await,
        Route::ListBuckets | Route::ListKeys { .. } => not_implemented_response(
            "listing is not supported by the in-process datastore in v0.0.1",
        ),
        Route::GetProps { bucket } => get_props_response(bucket, headers),
        Route::SetProps { bucket } => set_props_response(bucket, headers, &body),
        Route::MapRed => mapred_response(headers, &body).await,
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
        .body(Full::new(body))
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
        .body(Full::new(Bytes::from(body_bytes)))
        .expect("invariant: stats response builder is well-formed")
}

async fn handle_get(
    _bucket: &str,
    _key: &str,
    headers: &HeaderMap,
    head_only: bool,
    datastore: &dyn Datastore,
) -> Response<ResponseBody> {
    let accept = header_str(headers, ACCEPT);
    let req_ct = header_str_opt(headers, CONTENT_TYPE);
    let Some(ct) = select_codec(accept, req_ct) else {
        return not_acceptable_response();
    };

    let routing = Msg::new(0, MsgType::Unknown, true);
    if let Err(e) = datastore.dispatch(routing).await {
        return text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("datastore error: {e}"),
        );
    }

    // The trampoline returns no content. For the v0.0.1 slice the
    // Riak HTTP gateway treats that as "key not found" -- 404 with
    // no body, which is exactly what Riak emits when a fetch misses.
    let _ = ct; // negotiated content-type would describe the body if there were one.
    let _ = head_only; // 404 has no body either way.
    text_response(StatusCode::NOT_FOUND, "not found")
}

async fn handle_put(
    _bucket: &str,
    _key: &str,
    headers: &HeaderMap,
    body: Bytes,
    datastore: &dyn Datastore,
) -> Response<ResponseBody> {
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

    let routing = Msg::new(0, MsgType::Unknown, true);
    if let Err(e) = datastore.dispatch(routing).await {
        return text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("datastore error: {e}"),
        );
    }
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("Server", SERVER_NAME)
        .body(Full::new(Bytes::new()))
        .expect("invariant: put response builder is well-formed")
}

async fn handle_delete(
    _bucket: &str,
    _key: &str,
    datastore: &dyn Datastore,
) -> Response<ResponseBody> {
    let routing = Msg::new(0, MsgType::Unknown, true);
    if let Err(e) = datastore.dispatch(routing).await {
        return text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("datastore error: {e}"),
        );
    }
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("Server", SERVER_NAME)
        .body(Full::new(Bytes::new()))
        .expect("invariant: delete response builder is well-formed")
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
        .body(Full::new(Bytes::from(body)))
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
        .body(Full::new(Bytes::new()))
        .expect("invariant: set-props response builder is well-formed")
}

// ------------------------------------------------------------------
// Generic response helpers.
// ------------------------------------------------------------------

fn text_response(status: StatusCode, msg: &str) -> Response<ResponseBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .header("Server", SERVER_NAME)
        .body(Full::new(Bytes::copy_from_slice(msg.as_bytes())))
        .expect("invariant: text response builder is well-formed")
}

fn not_implemented_response(msg: &str) -> Response<ResponseBody> {
    text_response(StatusCode::NOT_IMPLEMENTED, msg)
}

fn not_acceptable_response() -> Response<ResponseBody> {
    text_response(
        StatusCode::NOT_ACCEPTABLE,
        "no supported codec in Accept header",
    )
}

/// Header value extractor that returns "" for missing or non-ASCII
/// headers. The negotiation logic copes with empty input cleanly.
fn header_str(headers: &HeaderMap, name: hyper::header::HeaderName) -> &str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

/// Header value extractor that distinguishes "absent" from
/// "present but unreadable". Used where the caller wants to fall
/// back to a default only when the header was truly missing.
fn header_str_opt(headers: &HeaderMap, name: hyper::header::HeaderName) -> Option<&str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

// ------------------------------------------------------------------
// MapReduce route handler. Added by the v0.0.3 MapReduce slice.
// ------------------------------------------------------------------

use crate::mapreduce::{builtins::default_registry, run_job, MapReduceJob};

/// Run a MapReduce job submitted via `POST /mapred`.
///
/// The body must carry the JSON job description. The response is a
/// JSON array of `{phase, value}` objects (one per captured
/// output) with `Content-Type: application/json`. Streaming
/// (chunked frames per phase) is documented as deferred in
/// `docs/journal/2026-05-24-dyn-riak-mapreduce.md`; the v0.0.3
/// slice emits one buffered response.
async fn mapred_response(headers: &HeaderMap, body: &Bytes) -> Response<ResponseBody> {
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
    let outputs = match run_job(job, registry).await {
        Ok(o) => o,
        Err(e) => {
            return text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("MapReduce execution: {e}"),
            );
        }
    };
    let body_bytes = match serde_json::to_vec(&outputs) {
        Ok(b) => b,
        Err(e) => {
            return text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("MapReduce response encode: {e}"),
            );
        }
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .header("Server", SERVER_NAME)
        .body(Full::new(Bytes::from(body_bytes)))
        .expect("invariant: mapred response builder is well-formed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynomite::embed::MemoryDatastore;

    fn dummy_headers() -> HeaderMap {
        HeaderMap::new()
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
    async fn list_keys_returns_501() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let resp = handle_route(
            Route::ListKeys { bucket: "u" },
            &Method::GET,
            &dummy_headers(),
            Bytes::new(),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn list_buckets_returns_501() {
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let resp = handle_route(
            Route::ListBuckets,
            &Method::GET,
            &dummy_headers(),
            Bytes::new(),
            ds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
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
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json");
        let arr = parsed.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["phase"], 1);
        assert_eq!(arr[0]["value"], serde_json::json!(6));
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
}
