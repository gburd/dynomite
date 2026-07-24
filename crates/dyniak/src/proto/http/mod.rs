//! Riak HTTP gateway transport.
//!
//! `proto::http` is the sibling of [`crate::proto::pb`] for the Riak
//! HTTP API. It exposes the same Riak operation surface over a
//! conventional HTTP/1.1 server backed by `hyper` + `hyper-util`, so
//! operators can put a browser-friendly K/V endpoint in front of the
//! same [`dynomite::embed::Datastore`] the PBC server already uses.
//!
//! # Public surface
//!
//! * [`serve_http`] -- TCP accept loop driving a `hyper`
//!   `http1::serve_connection` per accepted client.
//! * [`routes`] -- the route table and per-route handlers (internal,
//!   re-exported only for testing through `pub(crate)`).
//! * [`content_type`] -- `Accept` / `Content-Type` negotiation
//!   ([`content_type::select_codec`]).
//!
//! # Wire shape
//!
//! Riak's HTTP layer is a conventional REST-ish JSON API:
//!
//! ```text
//!   GET    /ping                            -> 200 OK / "OK"
//!   GET    /stats                           -> 200 OK / { name, version, ... }
//!   GET    /buckets/{bucket}/keys/{key}     -> 200 / 404
//!   PUT    /buckets/{bucket}/keys/{key}     -> 204
//!   POST   /buckets/{bucket}/keys/{key}     -> 204 (server-assigned key path required)
//!   DELETE /buckets/{bucket}/keys/{key}     -> 204
//!   GET    /buckets?buckets=true            -> 501 (in-memory store cannot enumerate)
//!   GET    /buckets/{bucket}/keys?keys=true -> 501 (in-memory store cannot enumerate)
//!   GET    /buckets/{bucket}/props          -> 200 / { props: { ... } }
//!   PUT    /buckets/{bucket}/props          -> 204
//! ```
//!
//! # Datastore semantics
//!
//! Object K/V endpoints are served against the real store when the
//! backend exposes one (today [`crate::datastore::NoxuDatastore`]):
//! a `PUT` decodes the request body under its `Content-Type` codec
//! into an [`crate::proto::http::object::HttpObject`], persists a
//! canonical codec-independent form plus its secondary indexes, and
//! a `GET` re-encodes the stored envelope under the negotiated
//! `Accept` codec. Because storage holds the decoded object rather
//! than the raw bytes, a value stored as `application/json` is
//! fetchable as `application/cbor` or `application/x-protobuf`.
//! Backends without an object layer (the in-memory store used in
//! tests) fall back to a trampoline through
//! [`dynomite::embed::Datastore::dispatch`]: `GET` replies
//! `404 Not Found`, `PUT` / `DELETE` reply `204 No Content`.
//! List-keys and list-buckets stream their response body
//! chunk-by-chunk via
//! [`dynomite::embed::Datastore::list_buckets_stream`] and
//! [`dynomite::embed::Datastore::list_keys_stream`].

use std::sync::Arc;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use dynomite::embed::Datastore;

use crate::error::RiakError;

pub mod content_type;
pub mod object;
pub mod routes;
#[cfg(feature = "search")]
pub mod search;

pub use crate::proto::http::content_type::{select_codec, SUPPORTED_CONTENT_TYPES};
#[cfg(feature = "search")]
pub use crate::proto::http::search::SearchState;

/// Run the HTTP accept loop on `listener`.
///
/// One tokio task is spawned per accepted connection. Per-connection
/// failures are logged at `tracing::warn!` and otherwise swallowed
/// so a misbehaving client cannot bring the listener down.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use tokio::net::TcpListener;
/// use dyniak::serve_http;
/// use dynomite::embed::{Datastore, MemoryDatastore};
///
/// # tokio::runtime::Builder::new_current_thread()
/// #     .enable_all().build().unwrap().block_on(async {
/// let listener = TcpListener::bind("127.0.0.1:8098").await.unwrap();
/// let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
/// let _handle = tokio::spawn(serve_http(listener, ds));
/// # });
/// ```
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
pub async fn serve_http(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
) -> Result<(), RiakError> {
    serve_http_ctx(listener, routes::RouteCtx::new(datastore)).await
}

/// Run the HTTP accept loop with cross-node replica routing wired
/// in. An object `PUT` / `DELETE` fans a
/// [`crate::router::PeerOp`] out to every replica on the key's
/// preference list (fire-and-forget) before persisting locally,
/// mirroring [`crate::serve_pbc_with_routing`]. Object reads /
/// lists / transactions behave exactly like [`serve_http`].
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
pub async fn serve_http_with_routing(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    hooks: crate::router::RoutingHooks,
) -> Result<(), RiakError> {
    serve_http_ctx(listener, routes::RouteCtx::new(datastore).set_hooks(hooks)).await
}

/// TLS-terminating variant of [`serve_http_with_routing`].
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
pub async fn serve_http_tls_with_routing(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    hooks: crate::router::RoutingHooks,
    acceptor: tokio_rustls::TlsAcceptor,
) -> Result<(), RiakError> {
    serve_http_tls_ctx(
        listener,
        routes::RouteCtx::new(datastore).set_hooks(hooks),
        acceptor,
    )
    .await
}

/// Run the HTTP accept loop on `listener`, wiring the search
/// registry into the gateway alongside the datastore so the index
/// and search routes are served. Behaves exactly like
/// [`serve_http`] for the object / list / transaction routes.
///
/// Available only when the crate is built with the `search` Cargo
/// feature.
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
#[cfg(feature = "search")]
pub async fn serve_http_with_search(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    registry: Arc<dynomite_search::VectorRegistry>,
) -> Result<(), RiakError> {
    let ctx = routes::RouteCtx::with_search(datastore, Arc::new(SearchState::new(registry)));
    serve_http_ctx(listener, ctx).await
}

/// Run the HTTP accept loop on `listener`, wiring a Wasm phase
/// store into the gateway alongside the datastore so a
/// `POST /mapred` job referencing a
/// [`crate::mapreduce::Phase::WasmModule`] reaches the configured
/// modules. Behaves exactly like [`serve_http`] for every other
/// route.
///
/// Available only when the crate is built with the `wasm` Cargo
/// feature.
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
#[cfg(feature = "wasm")]
pub async fn serve_http_with_wasm(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    wasm: Arc<crate::mapreduce::wasm::WasmModuleStore>,
) -> Result<(), RiakError> {
    let ctx = routes::RouteCtx::with_wasm(datastore, wasm);
    serve_http_ctx(listener, ctx).await
}

/// Run the HTTP accept loop on `listener`, wiring both a search
/// registry and a Wasm phase store into the gateway. A single
/// `data_store: dyniak` pool can configure both surfaces at once,
/// so this entry point carries both into one internal route context.
///
/// Available only when the crate is built with both the `search`
/// and `wasm` Cargo features.
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
#[cfg(all(feature = "search", feature = "wasm"))]
pub async fn serve_http_with_search_and_wasm(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    registry: Arc<dynomite_search::VectorRegistry>,
    wasm: Arc<crate::mapreduce::wasm::WasmModuleStore>,
) -> Result<(), RiakError> {
    let ctx = routes::RouteCtx::with_search(datastore, Arc::new(SearchState::new(registry)))
        .set_wasm(wasm);
    serve_http_ctx(listener, ctx).await
}
async fn serve_http_ctx(listener: TcpListener, ctx: routes::RouteCtx) -> Result<(), RiakError> {
    loop {
        let (sock, peer) = listener.accept().await?;
        let ctx = ctx.clone();
        let io = TokioIo::new(sock);
        tokio::spawn(async move {
            let svc = service_fn(move |req| {
                let ctx = ctx.clone();
                async move {
                    let resp = routes::dispatch(req, ctx).await;
                    Ok::<_, std::convert::Infallible>(resp)
                }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                tracing::warn!(%peer, error = %e, "riak http connection ended with error");
            }
        });
    }
}

/// Run the HTTP accept loop on `listener`, terminating TLS via
/// `acceptor` for every accepted connection. Per-connection
/// handshake failures are logged at `tracing::warn!` and
/// otherwise swallowed.
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
pub async fn serve_http_tls(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    acceptor: TlsAcceptor,
) -> Result<(), RiakError> {
    serve_http_tls_ctx(listener, routes::RouteCtx::new(datastore), acceptor).await
}

/// TLS-terminating variant of [`serve_http_with_search`]. Available
/// only when the crate is built with the `search` Cargo feature.
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
#[cfg(feature = "search")]
pub async fn serve_http_tls_with_search(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    registry: Arc<dynomite_search::VectorRegistry>,
    acceptor: TlsAcceptor,
) -> Result<(), RiakError> {
    let ctx = routes::RouteCtx::with_search(datastore, Arc::new(SearchState::new(registry)));
    serve_http_tls_ctx(listener, ctx, acceptor).await
}

/// TLS-terminating variant of [`serve_http_with_wasm`]. Available
/// only when the crate is built with the `wasm` Cargo feature.
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
#[cfg(feature = "wasm")]
pub async fn serve_http_tls_with_wasm(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    wasm: Arc<crate::mapreduce::wasm::WasmModuleStore>,
    acceptor: TlsAcceptor,
) -> Result<(), RiakError> {
    let ctx = routes::RouteCtx::with_wasm(datastore, wasm);
    serve_http_tls_ctx(listener, ctx, acceptor).await
}

/// TLS-terminating variant of [`serve_http_with_search_and_wasm`].
/// Available only when the crate is built with both the `search`
/// and `wasm` Cargo features.
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
#[cfg(all(feature = "search", feature = "wasm"))]
pub async fn serve_http_tls_with_search_and_wasm(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    registry: Arc<dynomite_search::VectorRegistry>,
    wasm: Arc<crate::mapreduce::wasm::WasmModuleStore>,
    acceptor: TlsAcceptor,
) -> Result<(), RiakError> {
    let ctx = routes::RouteCtx::with_search(datastore, Arc::new(SearchState::new(registry)))
        .set_wasm(wasm);
    serve_http_tls_ctx(listener, ctx, acceptor).await
}

/// Shared TLS-terminating accept loop against a prebuilt
/// [`routes::RouteCtx`].
async fn serve_http_tls_ctx(
    listener: TcpListener,
    ctx: routes::RouteCtx,
    acceptor: TlsAcceptor,
) -> Result<(), RiakError> {
    loop {
        let (sock, peer) = listener.accept().await?;
        let ctx = ctx.clone();
        let acc = acceptor.clone();
        tokio::spawn(async move {
            let tls = match acc.accept(sock).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(%peer, error = %e, "riak http tls handshake failed");
                    return;
                }
            };
            let io = TokioIo::new(tls);
            let svc = service_fn(move |req| {
                let ctx = ctx.clone();
                async move {
                    let resp = routes::dispatch(req, ctx).await;
                    Ok::<_, std::convert::Infallible>(resp)
                }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                tracing::warn!(%peer, error = %e, "riak http tls connection ended with error");
            }
        });
    }
}
