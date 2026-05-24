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
//! For the v0.0.1 slice the gateway trampolines K/V operations
//! through [`dynomite::embed::Datastore::dispatch`] in the same shape
//! the PBC server uses. The substrate's accounting ticks per
//! request; the Riak-aware K/V trait that turns a `dispatch` into a
//! real fetch / store / delete lands in a follow-up slice. List-keys
//! and list-buckets always return `501 Not Implemented` for the
//! in-process datastore until the streaming list path lands.

use std::sync::Arc;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use dynomite::embed::Datastore;

use crate::error::RiakError;

pub mod content_type;
pub mod routes;

pub use crate::proto::http::content_type::{select_codec, SUPPORTED_CONTENT_TYPES};

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
/// use dyn_riak::serve_http;
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
    loop {
        let (sock, peer) = listener.accept().await?;
        let datastore = Arc::clone(&datastore);
        let io = TokioIo::new(sock);
        tokio::spawn(async move {
            let svc = service_fn(move |req| {
                let ds = Arc::clone(&datastore);
                async move {
                    let resp = routes::dispatch(req, ds).await;
                    Ok::<_, std::convert::Infallible>(resp)
                }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                tracing::warn!(%peer, error = %e, "riak http connection ended with error");
            }
        });
    }
}
