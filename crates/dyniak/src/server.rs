//! TCP accept loop and per-connection driver for the Riak PBC transport.
//!
//! The driver is split into two layers:
//!
//! 1. [`serve_pbc`] -- the public `tokio::net::TcpListener` accept
//!    loop. One task per accepted connection. Returns when the
//!    listener is dropped.
//! 2. [`handle_conn`] -- the per-connection state machine, generic
//!    over `AsyncRead + AsyncWrite`. Tests drive it with
//!    `tokio::io::duplex` to exercise the dispatch logic without a
//!    real socket.
//!
//! # Datastore semantics
//!
//! The handler delegates K/V execution to the
//! [`dynomite::embed::Datastore`] handed in. The trait's
//! `dispatch(Msg)` surface is the substrate's existing seam for
//! the RESP/Memcached/custom protocols; on top of it the trait
//! exposes Riak-aware `riak_get` / `riak_put` / `riak_delete` /
//! `riak_index_*` / `list_keys_stream` methods. The handler routes
//! each request to those methods and trampolines a synthesized
//! [`dynomite::msg::Msg`] through `dispatch` so the substrate's
//! per-request accounting still fires. A datastore that does not
//! implement the Riak K/V layer reports
//! [`dynomite::embed::hooks::DatastoreError::Unsupported`]; the
//! handler then falls back to empty responses so the in-memory
//! `MemoryDatastore` trampoline keeps working.
//!
//! In practice this means:
//!
//! * `RpbPing` -- never reaches the datastore; replied with
//!   `RpbPingResp` directly.
//! * `RpbGetReq` -- routed through
//!   [`dynomite::embed::Datastore::dispatch`] for accounting, then
//!   fetched via `riak_get`; a present object becomes a
//!   single-content `RpbGetResp` carrying value, content-type, 2i
//!   entries, and links. An absent object or an `Unsupported`
//!   datastore yields an empty `RpbGetResp`.
//! * `RpbPutReq` -- persists the `RpbContent` payload in the same
//!   canonical `HttpObject` storage form the HTTP gateway writes,
//!   so a put over one transport reads back over the other.
//!   Server-assigned keys are not implemented: a request without a
//!   key is rejected.
//! * `RpbDelReq` -- routed for accounting, then deleted via
//!   `riak_delete`; replied with the body-less `RpbDelResp`.
//! * `RpbServerInfoReq` -- replied with the crate name and version
//!   directly; no datastore interaction.
//! * `RpbGetBucketReq` -- replied with the bucket's resolved
//!   [`RpbBucketProps`] from the bucket-property registry when
//!   routing hooks are wired; without hooks it returns conservative
//!   defaults (`n_val = 3`, `allow_mult = false`).
//! * `RpbSetBucketReq` -- the supplied properties are persisted
//!   through the bucket-property registry when routing hooks are
//!   wired, so they round-trip through later `RpbGetBucketReq`
//!   frames; without hooks the write is accepted and acknowledged
//!   with an empty `RpbSetBucketResp`.
//! * `RpbListBucketsReq`, `RpbListKeysReq`, `RpbIndexReq` --
//!   streamed from the datastore's `list_keys_stream` /
//!   `riak_index_*` methods; a datastore that does not implement
//!   them replies with an [`RpbErrorResp`] carrying a
//!   `"not implemented for this datastore"` message.

use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use futures_util::StreamExt;
use prost::Message as _;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;

use dynomite::cluster::admin_rpc::{
    ClusterAdmin, ClusterChange, ClusterChangeKind, ClusterError, NoopClusterAdmin, PeerSnapshot,
};
use dynomite::embed::hooks::{DatastoreByteStream, DatastoreError};
use dynomite::embed::Datastore;
use dynomite::msg::{Msg, MsgType};

use crate::aae::status::{AaeStatusProvider, AaeStatusSnapshot, NoopAaeStatusProvider};
use crate::error::RiakError;
use crate::mapreduce::{MrError, PhaseBatch};
use crate::proto::http::object::{HttpIndex, HttpLink, HttpObject};
use crate::proto::pb::framer::{read_frame, write_frame, Frame};
use crate::proto::pb::mapreduce::{RpbMapRedReq, RpbMapRedResp};
use crate::proto::pb::messages::{
    DynRpbAaePeerStatus, DynRpbAaeStatusReq, DynRpbAaeStatusResp, DynRpbClusterCommitReq,
    DynRpbClusterCommitResp, DynRpbClusterJoinReq, DynRpbClusterJoinResp, DynRpbClusterLeaveReq,
    DynRpbClusterLeaveResp, DynRpbClusterPlanReq, DynRpbClusterPlanResp, DynRpbListPeersReq,
    DynRpbListPeersResp, DynRpbPeerInfo, DynRpbStagedChange, MessageCode, RpbBucketProps,
    RpbContent, RpbDelReq, RpbErrorResp, RpbGetBucketReq, RpbGetBucketResp, RpbGetReq, RpbGetResp,
    RpbGetServerInfoResp, RpbIndexReq, RpbIndexResp, RpbLink, RpbListBucketsReq,
    RpbListBucketsResp, RpbListKeysReq, RpbListKeysResp, RpbPair, RpbPingReq, RpbPingResp,
    RpbPutReq, RpbPutResp, RpbServerInfoReq, RpbSetBucketReq, RpbSetBucketResp,
    DYN_STAGED_CHANGE_ADD, DYN_STAGED_CHANGE_REMOVE, INDEX_QUERY_TYPE_EQ, INDEX_QUERY_TYPE_RANGE,
};
use crate::router::{PeerOp, RoutingHooks};

/// Maximum number of bucket / key entries packed into a single
/// streaming `RpbListBucketsResp` / `RpbListKeysResp` frame.
///
/// Riak's reference server uses an implementation-defined chunk
/// size; 256 entries per frame is comfortably below the framer's
/// 16 MiB cap for any practical key size while keeping per-frame
/// overhead low.
pub(crate) const LIST_CHUNK_SIZE: usize = 256;

/// A boxed stream of outbound PBC frames.
///
/// Most ops produce a single-frame stream; list-buckets and
/// list-keys produce a multi-frame stream where every frame except
/// the terminator carries `done = false` (or absent) and the final
/// frame carries `done = true`.
pub(crate) type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, RiakError>> + Send>>;

/// Run the PBC accept loop on `listener`.
///
/// The function returns once the listener errors permanently. Each
/// accepted connection gets its own task; per-connection failures are
/// logged at `tracing::warn!` and otherwise swallowed so a misbehaving
/// client cannot bring the listener down.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use tokio::net::TcpListener;
/// use dyniak::serve_pbc;
/// use dynomite::embed::{Datastore, MemoryDatastore};
///
/// # tokio::runtime::Builder::new_current_thread()
/// #     .enable_all().build().unwrap().block_on(async {
/// let listener = TcpListener::bind("127.0.0.1:8087").await.unwrap();
/// let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
/// let _handle = tokio::spawn(serve_pbc(listener, ds));
/// # });
/// ```
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
pub async fn serve_pbc(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
) -> Result<(), RiakError> {
    let admin: Arc<dyn ClusterAdmin> = Arc::new(NoopClusterAdmin);
    serve_pbc_inner(listener, datastore, admin, None).await
}

/// Run the PBC accept loop on `listener` with a custom
/// [`ClusterAdmin`] handle wired into the dispatch path.
///
/// Use this when the embedding wants the admin RPCs
/// (`DynRpbListPeersReq`, `DynRpbClusterJoinReq`,
/// `DynRpbClusterLeaveReq`, `DynRpbClusterPlanReq`,
/// `DynRpbClusterCommitReq`) to drive a real
/// [`dynomite::cluster::PoolClusterAdmin`] instead of the
/// always-empty [`NoopClusterAdmin`].
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
pub async fn serve_pbc_with_admin(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
) -> Result<(), RiakError> {
    serve_pbc_inner(listener, datastore, admin, None).await
}

/// Run the PBC accept loop on `listener`, terminating TLS for
/// every accepted connection via `acceptor`.
///
/// Per-connection TLS-handshake failures are logged at
/// `tracing::warn!` and otherwise ignored, matching the
/// plaintext-server policy of swallowing per-connection errors.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use tokio::net::TcpListener;
/// use tokio_rustls::TlsAcceptor;
/// use dyniak::serve_pbc_tls;
/// use dynomite::embed::{Datastore, MemoryDatastore};
///
/// # async fn demo(acceptor: TlsAcceptor) -> std::io::Result<()> {
/// let listener = TcpListener::bind("127.0.0.1:8087").await?;
/// let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
/// let _h = tokio::spawn(serve_pbc_tls(listener, ds, acceptor));
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
pub async fn serve_pbc_tls(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    acceptor: TlsAcceptor,
) -> Result<(), RiakError> {
    let admin: Arc<dyn ClusterAdmin> = Arc::new(NoopClusterAdmin);
    serve_pbc_inner(listener, datastore, admin, Some(acceptor)).await
}

/// As [`serve_pbc_with_admin`], terminating TLS for every
/// accepted connection through `acceptor`.
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
pub async fn serve_pbc_tls_with_admin(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
    acceptor: TlsAcceptor,
) -> Result<(), RiakError> {
    serve_pbc_inner(listener, datastore, admin, Some(acceptor)).await
}

/// Run the PBC accept loop on a QUIC `listener`.
///
/// This is the QUIC sibling of [`serve_pbc`]: it loops on
/// [`dynomite::net::quic::QuicListener::accept`], which yields a
/// [`dynomite::net::quic::QuicTransport`] per connected client,
/// and feeds that transport into the exact same per-connection
/// handler the TCP and TLS-over-TCP paths use. The PBC framing
/// is byte-for-byte identical over QUIC; only the underlying
/// transport changes (each accepted connection is carried over a
/// single QUIC bidirectional stream).
///
/// Per-connection failures are logged at `tracing::warn!` and
/// otherwise swallowed so a misbehaving client cannot bring the
/// listener down, matching the plaintext / TLS accept loops.
///
/// Available only when the crate is built with the `quic` Cargo
/// feature.
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
#[cfg(feature = "quic")]
pub async fn serve_pbc_quic(
    listener: dynomite::net::quic::QuicListener,
    datastore: Arc<dyn Datastore>,
) -> Result<(), RiakError> {
    let admin: Arc<dyn ClusterAdmin> = Arc::new(NoopClusterAdmin);
    serve_pbc_quic_inner(listener, datastore, admin).await
}

/// As [`serve_pbc_quic`], with a custom [`ClusterAdmin`] handle
/// wired into the dispatch path.
///
/// Available only when the crate is built with the `quic` Cargo
/// feature.
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
#[cfg(feature = "quic")]
pub async fn serve_pbc_quic_with_admin(
    listener: dynomite::net::quic::QuicListener,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
) -> Result<(), RiakError> {
    serve_pbc_quic_inner(listener, datastore, admin).await
}

#[cfg(feature = "quic")]
async fn serve_pbc_quic_inner(
    listener: dynomite::net::quic::QuicListener,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
) -> Result<(), RiakError> {
    let aae_status: Arc<dyn AaeStatusProvider> = Arc::new(NoopAaeStatusProvider);
    loop {
        let transport = listener.accept().await?;
        let peer = transport.peer_addr_socket();
        let datastore = Arc::clone(&datastore);
        let admin = Arc::clone(&admin);
        let aae = Arc::clone(&aae_status);
        tokio::spawn(async move {
            if let Err(e) = handle_conn_full(transport, datastore, admin, None, aae).await {
                tracing::warn!(%peer, error = %e, "riak pbc quic connection ended with error");
            }
        });
    }
}

/// As [`serve_pbc_with_admin`], with [`RoutingHooks`] wired into
/// the request path.
///
/// When hooks are wired, `RpbGetReq` / `RpbPutReq` / `RpbDelReq`
/// frames are first routed through
/// [`crate::router::BucketRouter::route`]; the resulting plan
/// drives a per-replica fan-out via
/// [`crate::router::PeerOutbound::dispatch`] before the local
/// datastore call. Topology-strategy buckets fall through to
/// the existing pipeline (no per-replica fan-out happens at
/// this layer; the existing
/// [`dynomite::cluster::dispatch::ClusterDispatcher`] is the
/// source of truth there).
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
pub async fn serve_pbc_with_routing(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
    hooks: RoutingHooks,
) -> Result<(), RiakError> {
    serve_pbc_inner_with_hooks(listener, datastore, admin, None, Some(hooks)).await
}

/// As [`serve_pbc_with_admin`], with an [`AaeStatusProvider`]
/// wired into the dispatch path so the new
/// `DynRpbAaeStatusReq` admin op returns live AAE state.
/// Routing hooks are not configured by this entry point.
///
/// # Errors
///
/// Returns the first `accept` error the listener surfaces.
pub async fn serve_pbc_with_aae_status(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
    aae_status: Arc<dyn AaeStatusProvider>,
) -> Result<(), RiakError> {
    serve_pbc_full(listener, datastore, admin, None, None, Some(aae_status)).await
}

async fn serve_pbc_inner(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
    acceptor: Option<TlsAcceptor>,
) -> Result<(), RiakError> {
    serve_pbc_inner_with_hooks(listener, datastore, admin, acceptor, None).await
}

async fn serve_pbc_inner_with_hooks(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
    acceptor: Option<TlsAcceptor>,
    hooks: Option<RoutingHooks>,
) -> Result<(), RiakError> {
    serve_pbc_full(listener, datastore, admin, acceptor, hooks, None).await
}

async fn serve_pbc_full(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
    acceptor: Option<TlsAcceptor>,
    hooks: Option<RoutingHooks>,
    aae_status: Option<Arc<dyn AaeStatusProvider>>,
) -> Result<(), RiakError> {
    let aae_status: Arc<dyn AaeStatusProvider> =
        aae_status.unwrap_or_else(|| Arc::new(NoopAaeStatusProvider));
    loop {
        let (sock, peer) = listener.accept().await?;
        // Disable Nagle: PBC responses are written as one buffer and
        // flushed, so Nagle waiting on a delayed ACK only adds latency.
        let _ = sock.set_nodelay(true);
        let datastore = Arc::clone(&datastore);
        let admin = Arc::clone(&admin);
        let aae = Arc::clone(&aae_status);
        let hooks = hooks.clone();
        match acceptor.as_ref() {
            Some(acc) => {
                let acc = acc.clone();
                tokio::spawn(async move {
                    match acc.accept(sock).await {
                        Ok(tls) => {
                            if let Err(e) =
                                handle_conn_full(tls, datastore, admin, hooks, aae).await
                            {
                                tracing::warn!(
                                    %peer,
                                    error = %e,
                                    "riak pbc tls connection ended with error"
                                );
                            }
                        }
                        Err(e) => tracing::warn!(
                            %peer,
                            error = %e,
                            "riak pbc tls handshake failed"
                        ),
                    }
                });
            }
            None => {
                tokio::spawn(async move {
                    if let Err(e) = handle_conn_full(sock, datastore, admin, hooks, aae).await {
                        tracing::warn!(%peer, error = %e, "riak pbc connection ended with error");
                    }
                });
            }
        }
    }
}

/// Drive a single PBC connection over `stream` until the peer closes
/// or a fatal frame error occurs.
///
/// `handle_conn` is generic over the stream type so tests can drive
/// it with `tokio::io::duplex`.
///
/// # Errors
///
/// Returns the first wire-level or datastore error encountered. A
/// clean peer close (read-side EOF before the next length prefix) is
/// reported as [`RiakError::UnexpectedEof`] only when it occurs after
/// at least one length-prefix byte has been consumed; an EOF on a
/// fresh frame boundary is reported as `Ok(())`.
pub async fn handle_conn<S>(stream: S, datastore: Arc<dyn Datastore>) -> Result<(), RiakError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let admin: Arc<dyn ClusterAdmin> = Arc::new(NoopClusterAdmin);
    handle_conn_with_admin(stream, datastore, admin).await
}

/// As [`handle_conn`], with an explicit [`ClusterAdmin`] handle
/// wired into the admin RPC dispatch path.
///
/// # Errors
///
/// Returns the first wire-level or datastore error encountered.
pub async fn handle_conn_with_admin<S>(
    stream: S,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
) -> Result<(), RiakError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    handle_conn_with_hooks(stream, datastore, admin, None).await
}

/// As [`handle_conn_with_admin`], with optional
/// [`RoutingHooks`] threaded through the per-frame dispatcher.
///
/// # Errors
///
/// Returns the first wire-level or datastore error encountered.
pub async fn handle_conn_with_hooks<S>(
    stream: S,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
    hooks: Option<RoutingHooks>,
) -> Result<(), RiakError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let aae_status: Arc<dyn AaeStatusProvider> = Arc::new(NoopAaeStatusProvider);
    handle_conn_full(stream, datastore, admin, hooks, aae_status).await
}

/// Drive a single PBC connection over `stream`, threading an
/// [`AaeStatusProvider`] handle through the dispatcher so the
/// new `DynRpbAaeStatusReq` / `DynRpbAaeStatusResp` admin op
/// can return live data.
///
/// # Errors
///
/// Returns the first wire-level or datastore error encountered.
pub async fn handle_conn_with_aae_status<S>(
    stream: S,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
    hooks: Option<RoutingHooks>,
    aae_status: Arc<dyn AaeStatusProvider>,
) -> Result<(), RiakError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    handle_conn_full(stream, datastore, admin, hooks, aae_status).await
}

async fn handle_conn_full<S>(
    stream: S,
    datastore: Arc<dyn Datastore>,
    admin: Arc<dyn ClusterAdmin>,
    hooks: Option<RoutingHooks>,
    aae_status: Arc<dyn AaeStatusProvider>,
) -> Result<(), RiakError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    // Buffer the read half: read_frame issues three small reads (length
    // prefix, code byte, body) per request. On a raw TCP socket that is
    // three syscalls per op; a BufReader coalesces them into one kernel
    // read of whatever is already buffered, which is the bulk of the
    // remaining TCP-vs-QUIC latency gap (QUIC reads from quiche's
    // in-memory recv buffer, so its small reads are already cheap).
    let mut reader = tokio::io::BufReader::new(reader);
    loop {
        let frame = match read_frame(&mut reader).await {
            Ok(f) => f,
            Err(RiakError::UnexpectedEof { .. }) => {
                return Ok(());
            }
            Err(other) => return Err(other),
        };

        let mut response = process_frame(
            &frame,
            datastore.as_ref(),
            admin.as_ref(),
            hooks.as_ref(),
            aae_status.as_ref(),
        )
        .await?;
        while let Some(item) = response.next().await {
            let f = item?;
            write_frame(&mut writer, &f).await?;
        }
    }
}

/// Per-frame dispatch. Pure function aside from the datastore
/// call. Returns a stream of one or more frames; list-buckets and
/// list-keys produce multi-frame streams chunked at
/// [`LIST_CHUNK_SIZE`].
async fn process_frame(
    frame: &Frame,
    datastore: &dyn Datastore,
    admin: &dyn ClusterAdmin,
    hooks: Option<&RoutingHooks>,
    aae_status: &dyn AaeStatusProvider,
) -> Result<FrameStream, RiakError> {
    let code = MessageCode::from_u8(frame.code).map_err(RiakError::UnknownMessageCode)?;
    let stream: FrameStream = match code {
        MessageCode::PingReq => single_frame(handle_ping(&frame.body)?),
        MessageCode::ServerInfoReq => single_frame(handle_server_info(&frame.body)?),
        MessageCode::GetReq => single_frame(handle_get(&frame.body, datastore, hooks).await?),
        MessageCode::PutReq => single_frame(handle_put(&frame.body, datastore, hooks).await?),
        MessageCode::DelReq => single_frame(handle_del(&frame.body, datastore, hooks).await?),
        MessageCode::GetBucketReq => single_frame(handle_get_bucket(&frame.body, hooks)?),
        MessageCode::SetBucketReq => single_frame(handle_set_bucket(&frame.body, hooks)?),
        MessageCode::ListBucketsReq => handle_list_buckets(&frame.body, datastore)?,
        MessageCode::ListKeysReq => handle_list_keys(&frame.body, datastore)?,
        MessageCode::IndexReq => handle_index(&frame.body, datastore).await?,
        MessageCode::MapRedReq => handle_mapreduce(&frame.body),
        MessageCode::DynListPeersReq => single_frame(handle_list_peers(&frame.body, admin)?),
        MessageCode::DynClusterJoinReq => single_frame(handle_cluster_join(&frame.body, admin)?),
        MessageCode::DynClusterLeaveReq => single_frame(handle_cluster_leave(&frame.body, admin)?),
        MessageCode::DynClusterPlanReq => single_frame(handle_cluster_plan(&frame.body, admin)?),
        MessageCode::DynClusterCommitReq => {
            single_frame(handle_cluster_commit(&frame.body, admin)?)
        }
        MessageCode::DynAaeStatusReq => single_frame(handle_aae_status(&frame.body, aae_status)?),
        MessageCode::DtUpdateReq => {
            single_frame(handle_dt_update(&frame.body, datastore, hooks).await?)
        }
        MessageCode::DtFetchReq => {
            single_frame(handle_dt_fetch(&frame.body, datastore, hooks).await?)
        }
        // Response codes are illegal inbound.
        MessageCode::ErrorResp
        | MessageCode::PingResp
        | MessageCode::GetServerInfoResp
        | MessageCode::GetResp
        | MessageCode::PutResp
        | MessageCode::DelResp
        | MessageCode::ListBucketsResp
        | MessageCode::ListKeysResp
        | MessageCode::GetBucketResp
        | MessageCode::SetBucketResp
        | MessageCode::DtUpdateResp
        | MessageCode::DtFetchResp
        | MessageCode::IndexResp
        | MessageCode::MapRedResp
        | MessageCode::DynListPeersResp
        | MessageCode::DynClusterJoinResp
        | MessageCode::DynClusterLeaveResp
        | MessageCode::DynClusterPlanResp
        | MessageCode::DynClusterCommitResp
        | MessageCode::DynAaeStatusResp => {
            let body = RpbErrorResp {
                errmsg: format!("unsupported inbound message code: {}", frame.code).into_bytes(),
                errcode: 0,
            }
            .encode_to_vec();
            single_frame(Frame::new(MessageCode::ErrorResp.as_u8(), body))
        }
    };
    Ok(stream)
}

/// Wrap a single [`Frame`] in a one-item [`FrameStream`].
fn single_frame(f: Frame) -> FrameStream {
    Box::pin(futures_util::stream::once(async move { Ok(f) }))
}

fn handle_ping(body: &[u8]) -> Result<Frame, RiakError> {
    // Body must decode (it is empty in conforming clients) but we
    // permit padding-tolerant clients by ignoring unknown trailing
    // bytes; `prost` already does this.
    let _ = RpbPingReq::decode(body)?;
    let resp = RpbPingResp::default();
    Ok(Frame::new(
        MessageCode::PingResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

fn handle_server_info(body: &[u8]) -> Result<Frame, RiakError> {
    let _ = RpbServerInfoReq::decode(body)?;
    let resp = RpbGetServerInfoResp {
        node: Some(b"dyniak".to_vec()),
        server_version: Some(format!("dyniak {}", env!("CARGO_PKG_VERSION")).into_bytes()),
    };
    Ok(Frame::new(
        MessageCode::GetServerInfoResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

/// Build an [`RpbContent`] from the canonical `HttpObject` storage
/// form written by both transports.
///
/// The HTTP gateway and the PBC put path both persist an
/// [`HttpObject`] protobuf blob, so a fetched value normally decodes
/// to an envelope carrying the object payload, its declared
/// content-type, its 2i entries, and its links. Bytes that predate
/// the shared storage form (or that some other writer stored raw)
/// fail to decode as an envelope; those are returned as a bare
/// `RpbContent` whose `value` is the raw bytes so a value never
/// disappears on read.
/// Advance the per-object causal context for a write coordinated by
/// `actor`.
///
/// Public wrapper over the internal advance so the HTTP object path
/// (a different module) tracks causality identically to the PBC path.
/// Not part of the stable API.
#[doc(hidden)]
#[must_use]
pub fn advance_object_context(prior: &[u8], actor: &[u8]) -> Vec<u8> {
    advance_context(prior, actor)
}

/// Wall-clock seconds since the Unix epoch, stamped on an object write
/// so the reaper can compute its age for TTL expiry. Clock going
/// backwards or before the epoch yields `0` (treated as unknown age,
/// never expired).
#[must_use]
pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Advance the per-object causal context for a write coordinated by
/// `actor`.
///
/// Decodes the prior context the client read, increments `actor`'s dot,
/// and re-encodes. A write produces a context that causally succeeds
/// what its coordinator read; two writes coordinated by different
/// actors from the same base are concurrent (siblings), which is what
/// lets a read detect supersede-vs-concurrent.
fn advance_context(prior: &[u8], actor: &[u8]) -> Vec<u8> {
    let mut vc = crate::vclock::VClock::decode(prior);
    vc.advance(actor);
    vc.encode()
}

/// Compare a client-supplied read context against a stored object's
/// context. Returns `Some(Ordering)` when the two are causally ordered
/// and `None` when they are concurrent (a conflict). An empty context
/// on either side is the bottom vector (dominated by any real write).
fn context_cmp(client: &[u8], stored: &[u8]) -> Option<std::cmp::Ordering> {
    crate::vclock::VClock::decode(client).partial_cmp(&crate::vclock::VClock::decode(stored))
}

/// Public wrapper over [`resolve_write`] so the HTTP object path (a
/// different module) resolves a sibling-aware write identically to the
/// PBC path. Not part of the stable API.
#[doc(hidden)]
#[must_use]
pub fn resolve_object_write(
    stored: &crate::proto::http::object::SiblingSet,
    new_obj: &HttpObject,
    allow_mult: bool,
) -> crate::proto::http::object::SiblingSet {
    resolve_write(stored, new_obj, allow_mult)
}

/// Resolve a write against the stored sibling set, producing the new
/// stored set.
///
/// The new object's `context` must already be advanced (via
/// [`advance_context`]) so it strictly dominates the context the client
/// read. Resolution:
///
/// * every stored sibling the new write causally dominates is dropped
///   (superseded);
/// * if a stored sibling causally dominates the new write (a stale,
///   late-arriving write), the stored set is returned unchanged;
/// * otherwise the new write joins the set. When `allow_mult` is false,
///   the set is then collapsed to a single value -- the causal winner,
///   ties broken by the value bytes -- matching Riak's
///   `last_write_wins` / no-siblings behaviour. When `allow_mult` is
///   true, concurrent siblings are retained.
///
/// This never drops a causally-newer write, and under `allow_mult`
/// never drops a concurrent write, matching `model-tests::causal_object`.
fn resolve_write(
    stored: &crate::proto::http::object::SiblingSet,
    new_obj: &HttpObject,
    allow_mult: bool,
) -> crate::proto::http::object::SiblingSet {
    use crate::proto::http::object::SiblingSet;
    // A stored sibling that the new write dominates is superseded.
    // A stored sibling that dominates the new write makes it stale.
    let dominates =
        |a: &[u8], b: &[u8]| matches!(context_cmp(a, b), Some(std::cmp::Ordering::Greater));
    if stored
        .siblings
        .iter()
        .any(|s| dominates(&s.context, &new_obj.context))
    {
        // The new write is causally behind a stored value; keep stored.
        return stored.clone();
    }
    let mut kept: Vec<HttpObject> = stored
        .siblings
        .iter()
        .filter(|s| !dominates(&new_obj.context, &s.context))
        .cloned()
        .collect();
    kept.push(new_obj.clone());
    if !allow_mult && kept.len() > 1 {
        // Collapse to one deterministic winner: the causal maximum,
        // ties broken by value bytes so the choice is order-independent.
        let winner = kept
            .into_iter()
            .reduce(|a, b| {
                match context_cmp(&b.context, &a.context) {
                    Some(std::cmp::Ordering::Greater) => b,
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => a,
                    None => {
                        // Concurrent: break the tie by value bytes.
                        if b.value > a.value {
                            b
                        } else {
                            a
                        }
                    }
                }
            })
            .expect("invariant: kept has at least the new write");
        return SiblingSet::single(winner);
    }
    SiblingSet { siblings: kept }
}

/// Whether `remote` is strictly behind `merged`: `merged` holds a
/// sibling that `remote` does not have an equal-or-dominating value
/// for. Used to decide whether a replica needs a read-repair push.
fn sibling_set_is_behind(
    remote: &crate::proto::http::object::SiblingSet,
    merged: &crate::proto::http::object::SiblingSet,
) -> bool {
    let dominates_or_eq = |a: &[u8], b: &[u8]| {
        matches!(
            context_cmp(a, b),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        )
    };
    // For every sibling in the merged frontier, the remote must hold a
    // sibling whose context dominates-or-equals it AND whose value
    // matches; otherwise the remote is missing part of the frontier.
    merged.siblings.iter().any(|m| {
        !remote
            .siblings
            .iter()
            .any(|r| r.value == m.value && dominates_or_eq(&r.context, &m.context))
    })
}

/// Merge a remote replica's sibling set into the local one, keeping the
/// causal frontier of the union: drop any sibling causally dominated by
/// another, retain concurrent ones. `allow_mult` controls the final
/// collapse (a single deterministic value when siblings are disabled),
/// matching [`resolve_write`]. This is the read-coordination merge: a
/// coordinated read folds every replica's set through this to converge
/// on the union frontier, so a read at any node returns the same
/// value(s) as a read at the most up-to-date replica.
fn merge_sibling_sets(
    local: &crate::proto::http::object::SiblingSet,
    remote: &crate::proto::http::object::SiblingSet,
    allow_mult: bool,
) -> crate::proto::http::object::SiblingSet {
    let mut acc = local.clone();
    for sib in &remote.siblings {
        // Fold each remote sibling in as if it were an incoming write.
        // resolve_write drops it if dominated, supersedes locals it
        // dominates, and retains it when concurrent -- exactly the
        // union-frontier merge. allow_mult=true here so the frontier is
        // preserved during the fold; the caller collapses once at the
        // end when siblings are disabled.
        acc = resolve_write(&acc, sib, true);
    }
    if !allow_mult && acc.siblings.len() > 1 {
        // Collapse the converged frontier to one deterministic value.
        let winner = acc
            .siblings
            .into_iter()
            .reduce(|a, b| match context_cmp(&b.context, &a.context) {
                Some(std::cmp::Ordering::Greater) => b,
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => a,
                None => {
                    if b.value > a.value {
                        b
                    } else {
                        a
                    }
                }
            })
            .expect("invariant: a non-empty set collapses to one");
        return crate::proto::http::object::SiblingSet::single(winner);
    }
    acc
}

/// Map a decoded [`HttpObject`] to a PBC [`RpbContent`].
fn pbc_content_from_object(obj: &HttpObject) -> RpbContent {
    RpbContent {
        value: obj.value.clone(),
        content_type: obj.content_type.clone().map(String::into_bytes),
        links: obj.links.iter().map(http_link_to_rpb).collect(),
        indexes: obj
            .indexes
            .iter()
            .map(|i| RpbPair {
                key: i.name.clone().into_bytes(),
                value: Some(i.value.clone().into_bytes()),
            })
            .collect(),
        ..RpbContent::default()
    }
}

/// Public wrapper over [`join_contexts`] for the HTTP object path.
/// Not part of the stable API.
#[doc(hidden)]
pub fn join_object_contexts<'a>(contexts: impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
    join_contexts(contexts)
}

/// Join a set of encoded causal contexts into one that causally
/// succeeds (or equals) every input (element-wise maximum of the
/// version vectors). An empty or all-empty input yields the empty
/// context. Used on a sibling read so the returned context lets a
/// client's resolving write supersede every sibling.
fn join_contexts<'a>(contexts: impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut acc = crate::vclock::VClock::new();
    for c in contexts {
        acc.merge(&crate::vclock::VClock::decode(c));
    }
    acc.encode()
}

/// Map a storage-form [`HttpLink`] (string fields) into the PBC
/// [`RpbLink`] (optional-bytes fields). Empty components map to
/// `None`, matching how a Riak client omits absent fields.
fn http_link_to_rpb(link: &HttpLink) -> RpbLink {
    let opt = |s: &str| {
        if s.is_empty() {
            None
        } else {
            Some(s.as_bytes().to_vec())
        }
    };
    RpbLink {
        bucket: opt(&link.bucket),
        key: opt(&link.key),
        tag: opt(&link.tag),
    }
}

/// Map a PBC [`RpbLink`] (optional-bytes fields) into a storage-form
/// [`HttpLink`] (string fields).
///
/// `RpbLink` carries each component as `optional bytes`; `HttpLink`
/// carries each as `String`. A missing component decodes to the
/// empty string. Non-UTF-8 bytes are decoded lossily
/// (`String::from_utf8_lossy`), matching the PBC 2i index path and
/// the HTTP link-header path, both of which already treat link and
/// index components as text. Lossy decoding can perturb a non-UTF-8
/// link component on the round-trip; this is documented as a
/// deliberate deviation in `docs/parity.md` (link components are
/// expected to be valid UTF-8 in practice -- bucket and key names
/// and `riaktag` values are text).
fn rpb_link_to_http(link: &RpbLink) -> HttpLink {
    let text = |b: &Option<Vec<u8>>| {
        b.as_deref()
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .unwrap_or_default()
    };
    HttpLink {
        bucket: text(&link.bucket),
        key: text(&link.key),
        tag: text(&link.tag),
    }
}

/// Read-repair: push the converged sibling set to every replica whose
/// reply was strictly behind the merged frontier, and repair the
/// coordinator's own local store so a subsequent local read is already
/// converged. Fire and forget; a miss is reconciled by the next read
/// or by anti-entropy. A no-op when the merged set is empty.
async fn read_repair_behind(
    hooks: &RoutingHooks,
    decision: &crate::router::RouteDecision,
    datastore: &dyn Datastore,
    bucket: &[u8],
    key: &[u8],
    merged: &crate::proto::http::object::SiblingSet,
    replies: &[(u32, crate::proto::http::object::SiblingSet)],
) {
    if merged.siblings.is_empty() {
        return;
    }
    let merged_bytes = merged.to_storage_bytes();
    for (peer_idx, remote) in replies {
        if sibling_set_is_behind(remote, merged) {
            hooks
                .outbound
                .dispatch(
                    *peer_idx,
                    PeerOp::RepairPut {
                        bucket_type: decision.bucket_type.clone(),
                        bucket: bucket.to_vec(),
                        key: key.to_vec(),
                        storage: merged_bytes.clone(),
                    },
                )
                .await;
        }
    }
    let _ = datastore.riak_put(bucket, key, &merged_bytes, &[]).await;
}

/// Tally produced by [`fan_read_replicas`]: the converged sibling set
/// plus response counts split by primary-vs-fallback origin so the
/// caller can enforce R (against [`Self::responses`]) and PR (against
/// [`Self::primary_responses`]) independently.
struct ReadTally {
    merged: crate::proto::http::object::SiblingSet,
    /// Total responses (local + every replica that answered, primary
    /// or fallback), counted toward the read quorum R.
    responses: u32,
    /// Responses that came from a PRIMARY-owner replica (or the local
    /// node, when the plan does not mark it a fallback stand-in),
    /// counted toward the primary-read quorum PR.
    primary_responses: u32,
}

/// Fan a `Get` read-coordination query to every OTHER replica in
/// `decision`'s plan, merging each reply's sibling set into `merged`
/// and read-repairing replicas that were behind. Counts `responses`
/// (every reply, primary or fallback) and `primary_responses` (only
/// replies from a non-fallback target) so the caller can enforce R and
/// PR separately.
async fn fan_read_replicas(
    hooks: &RoutingHooks,
    decision: &crate::router::RouteDecision,
    datastore: &dyn Datastore,
    bucket: &[u8],
    key: &[u8],
    mut merged: crate::proto::http::object::SiblingSet,
    allow_mult: bool,
) -> ReadTally {
    use crate::proto::http::object::SiblingSet;
    let replicas = decision.replica_list();
    // The local node's own read already happened before this fan; it
    // counts as one response toward R, and toward PR unless the plan
    // explicitly marks the local peer a fallback stand-in (it never
    // does today -- see `crate::replication::plan_replicas`'s doc
    // comment -- but the check is real so it holds once a liveness
    // source substitutes fallbacks).
    let local_is_fallback = replicas
        .iter()
        .find(|t| t.peer_idx == hooks.local_peer_idx)
        .is_some_and(|t| t.is_fallback);
    let mut responses: u32 = 1;
    let mut primary_responses: u32 = u32::from(!local_is_fallback);
    let mut replies: Vec<(u32, SiblingSet)> = Vec::new();
    for replica in &replicas {
        if replica.peer_idx == hooks.local_peer_idx {
            continue;
        }
        let reply = hooks
            .outbound
            .request(
                replica.peer_idx,
                PeerOp::Get {
                    bucket_type: decision.bucket_type.clone(),
                    bucket: bucket.to_vec(),
                    key: key.to_vec(),
                },
            )
            .await;
        if let Some(bytes) = reply {
            // A reply (even empty = not-found) is a response toward R,
            // and toward PR when it came from a primary-owner target.
            responses = responses.saturating_add(1);
            if !replica.is_fallback {
                primary_responses = primary_responses.saturating_add(1);
            }
            let remote = SiblingSet::from_storage_bytes(&bytes).unwrap_or_default();
            if !remote.siblings.is_empty() {
                merged = merge_sibling_sets(&merged, &remote, allow_mult);
                replies.push((replica.peer_idx, remote));
            }
        }
    }
    read_repair_behind(hooks, decision, datastore, bucket, key, &merged, &replies).await;
    ReadTally {
        merged,
        responses,
        primary_responses,
    }
}

/// Enforce the read quorum R (always) and the primary-read quorum PR
/// (when `pr_floor > 0`) against the counts [`fan_read_replicas`]
/// gathered. `target_replicas` / `target_primaries` are the reachable
/// ceilings (a knob above the replica count degrades to "all"
/// reachable replicas, same as R/W elsewhere).
///
/// Availability caveat (mirrors the write path): on a fire-and-forget
/// transport `request` returns `None` for every replica, so `responses`
/// never rises above the local read. Enforcement is skipped in that
/// case (single-node-visible fallback) so a transport without
/// request/response does not fail every read; anti-entropy is the
/// backstop.
fn check_read_quorum(
    responses: u32,
    primary_responses: u32,
    r_floor: u32,
    pr_floor: u32,
    target_replicas: u32,
    target_primaries: u32,
) -> Result<(), String> {
    let transport_answered = responses > 1 || target_replicas <= 1;
    if !transport_answered {
        return Ok(());
    }
    let read_needed = r_floor.min(target_replicas.max(1));
    if responses < read_needed {
        return Err(format!(
            "riak get: read quorum not met ({responses}/{read_needed} responses)"
        ));
    }
    if pr_floor > 0 {
        let primary_needed = pr_floor.min(target_primaries.max(1));
        if primary_responses < primary_needed {
            return Err(format!(
                "riak get: primary read quorum not met ({primary_responses}/{primary_needed} primary responses)"
            ));
        }
    }
    Ok(())
}

async fn handle_get(
    body: &[u8],
    datastore: &dyn Datastore,
    hooks: Option<&RoutingHooks>,
) -> Result<Frame, RiakError> {
    use crate::proto::http::object::SiblingSet;
    let req = RpbGetReq::decode(body)?;
    // Trampoline through the substrate so dispatch counts tick.
    let routing = Msg::new(0, MsgType::Unknown, true);
    datastore.dispatch(routing).await?;

    // Local sibling set for the key (empty when absent / unsupported).
    let local = match datastore.riak_get(&req.bucket, &req.key).await {
        Ok(Some(v)) => SiblingSet::from_storage_bytes(&v).unwrap_or_default(),
        Ok(None) | Err(DatastoreError::Unsupported(_)) => SiblingSet::default(),
        Err(e) => return Ok(error_frame(format!("riak get: {e}"))),
    };

    // Read coordination: fan a Get to every OTHER replica of the key,
    // merge each returned sibling set into the local one (keeping the
    // causal frontier of the union), and read-repair replicas that
    // were behind. A read at ANY node -- replica or not -- then
    // returns the converged value(s), matching Riak's merge-on-read +
    // read-repair. On a fire-and-forget transport `request` returns
    // `None` and we fall back to the local value with anti-entropy as
    // the backstop.
    let mut merged = local;
    let mut responses: u32 = 1;
    let mut primary_responses: u32 = 1;
    let mut r_floor: u32 = 1;
    let mut pr_floor: u32 = 0;
    let mut target_replicas: u32 = 1;
    let mut target_primaries: u32 = 1;
    if let Some(hooks) = hooks {
        let bucket_type = req.r#type.as_deref().unwrap_or(b"");
        if let Ok(decision) = hooks.router.try_route(bucket_type, &req.bucket, &req.key) {
            let props = hooks.router.registry().resolve(bucket_type, &req.bucket);
            let allow_mult = props.effective_allow_mult();
            let n_val = props.effective_n_val();
            r_floor = props.effective_r(n_val, req.r);
            pr_floor = props.effective_pr(n_val, req.pr);
            let replicas = decision.replica_list();
            target_replicas = u32::try_from(replicas.len().max(1)).unwrap_or(u32::MAX);
            target_primaries =
                u32::try_from(decision.primary_replica_count().max(1)).unwrap_or(u32::MAX);
            let tally = fan_read_replicas(
                hooks,
                &decision,
                datastore,
                &req.bucket,
                &req.key,
                merged,
                allow_mult,
            )
            .await;
            merged = tally.merged;
            responses = tally.responses;
            primary_responses = tally.primary_responses;
        }
    }

    if let Err(reason) = check_read_quorum(
        responses,
        primary_responses,
        r_floor,
        pr_floor,
        target_replicas,
        target_primaries,
    ) {
        return Ok(error_frame(reason));
    }

    // Emit the converged sibling set. One value -> one `RpbContent`;
    // concurrent siblings -> one `RpbContent` each (the client
    // resolves). The returned context is the causal join of the
    // frontier so a resolving write supersedes them all.
    let joined = join_contexts(merged.siblings.iter().map(|o| o.context.as_slice()));
    let content: Vec<RpbContent> = merged
        .siblings
        .iter()
        .map(pbc_content_from_object)
        .collect();
    let resp = RpbGetResp {
        content,
        vclock: if joined.is_empty() {
            None
        } else {
            Some(joined)
        },
        ..RpbGetResp::default()
    };
    Ok(Frame::new(
        MessageCode::GetResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

/// Tally produced by [`fan_write_replicas`]: ack counts split by
/// total (W), primary-vs-fallback origin (PW), and durability (DW), so
/// the caller can enforce all three quorums independently.
struct WriteTally {
    /// Total acks (local + every replica ack, primary or fallback),
    /// counted toward the write quorum W.
    acks: u32,
    /// Acks from a PRIMARY-owner target (or the local node, when not a
    /// fallback stand-in), counted toward the primary-write quorum PW.
    primary_acks: u32,
    /// Acks confirmed durable (the local write's own durability, plus
    /// every [`ACK_STORED_DURABLE`] reply), counted toward the
    /// durable-write quorum DW.
    durable_acks: u32,
    /// Reachable target count = replica set size (the ceiling W is
    /// clamped against).
    target: u32,
    /// Reachable primary target count (the ceiling PW is clamped
    /// against).
    target_primaries: u32,
    /// Non-local targets fanned to; used to detect a fire-and-forget
    /// transport (no remote targets means the local ack alone is
    /// authoritative, same as the read path's availability caveat).
    remote_targets: u32,
    /// Whether the local write itself succeeded (seeds `acks`). Kept
    /// alongside `acks` so [`check_write_quorum`] can tell a "no
    /// remote replies yet" tally from a "local write failed" tally
    /// without re-deriving it from `acks` (which would be ambiguous
    /// once remote acks start arriving).
    local_ack: bool,
}

/// Fan the resolved SiblingSet `storage` to `decision`'s OTHER
/// replicas via `RepairPut`, counting acks for W, PW, and DW. Returns
/// [`WriteTally`]; the caller enforces the three quorums via
/// [`check_write_quorum`]. `local_ok` / `local_durable` seed the local
/// node's own contribution (it always writes locally before fanning).
async fn fan_write_replicas(
    hooks: &RoutingHooks,
    decision: &crate::router::RouteDecision,
    bucket: &[u8],
    key: &[u8],
    storage: &[u8],
    local_ok: bool,
    local_durable: bool,
) -> WriteTally {
    let replicas = decision.replica_list();
    let target = u32::try_from(replicas.len().max(1)).unwrap_or(u32::MAX);
    let target_primaries =
        u32::try_from(decision.primary_replica_count().max(1)).unwrap_or(u32::MAX);
    let local_is_fallback = replicas
        .iter()
        .find(|t| t.peer_idx == hooks.local_peer_idx)
        .is_some_and(|t| t.is_fallback);
    let mut acks: u32 = u32::from(local_ok);
    let mut primary_acks: u32 = u32::from(local_ok && !local_is_fallback);
    let mut durable_acks: u32 = u32::from(local_ok && local_durable);
    let mut remote_targets = 0u32;
    for replica in &replicas {
        if replica.peer_idx == hooks.local_peer_idx {
            continue;
        }
        remote_targets = remote_targets.saturating_add(1);
        let op = PeerOp::RepairPut {
            bucket_type: decision.bucket_type.clone(),
            bucket: bucket.to_vec(),
            key: key.to_vec(),
            storage: storage.to_vec(),
        };
        // Prefer the acked request path (counts toward W / PW / DW).
        // If the transport does not answer (returns None -- a
        // fire-and-forget transport that did not deliver), fall back
        // to a plain dispatch so the write still fans out (delivery is
        // not lost; the quorum counts just go unacked and are treated
        // as best-effort below).
        match hooks.outbound.request(replica.peer_idx, op.clone()).await {
            Some(ack) if !ack.is_empty() => {
                acks = acks.saturating_add(1);
                if !replica.is_fallback {
                    primary_acks = primary_acks.saturating_add(1);
                }
                if ack.first() == Some(&crate::router::ACK_STORED_DURABLE) {
                    durable_acks = durable_acks.saturating_add(1);
                }
            }
            Some(_) => {}
            None => hooks.outbound.dispatch(replica.peer_idx, op).await,
        }
    }
    WriteTally {
        acks,
        primary_acks,
        durable_acks,
        target,
        target_primaries,
        remote_targets,
        local_ack: local_ok,
    }
}

/// Enforce W (always), PW (when `pw > 0`), and DW (always -- DW
/// defaults to `quorum`, unlike PW/PR's `0` default) against a
/// [`WriteTally`]. Availability caveat (mirrors the read path): on a
/// fire-and-forget transport no acks come back beyond the local one,
/// so enforcement is skipped when there were remote targets but none
/// answered; a transport that has nothing to fan to (no remote
/// targets) is fully authoritative from the local ack alone.
fn check_write_quorum(tally: &WriteTally, w: u32, pw: u32, dw: u32) -> Result<(), String> {
    let transport_answered = tally.acks > u32::from(tally.local_ack) || tally.remote_targets == 0;
    if !transport_answered {
        return Ok(());
    }
    quorum_result(tally.acks, w, tally.target)?;
    if pw > 0 {
        let needed_pw = pw.min(tally.target_primaries.max(1));
        if tally.primary_acks < needed_pw {
            return Err(format!(
                "riak put: primary write quorum not met ({}/{needed_pw} primary acks)",
                tally.primary_acks
            ));
        }
    }
    let needed_dw = dw.min(tally.target.max(1));
    if tally.durable_acks < needed_dw {
        return Err(format!(
            "riak put: durable write quorum not met ({}/{needed_dw} durable acks)",
            tally.durable_acks
        ));
    }
    Ok(())
}

/// Decide a quorum outcome: satisfied when `acks >= w`. `w` is clamped
/// to the reachable `target` so a `w` above the replica count still
/// succeeds once every reachable replica acks (Riak treats `all` as N).
fn quorum_result(acks: u32, w: u32, target: u32) -> Result<(), String> {
    let needed = w.min(target.max(1));
    if acks >= needed {
        Ok(())
    } else {
        Err(format!(
            "riak put: write quorum not met ({acks}/{needed} acks)"
        ))
    }
}

/// Fan the resolved SiblingSet `storage` to the key's replicas and
/// enforce the write quorum `w`, the primary-write quorum `pw`, and
/// the durable-write quorum `dw`. With no routing hooks the local ack
/// alone is all there is: it satisfies `w`/`pw <= 1`, and `dw` if the
/// local write was durable.
///
/// `local_ok` is whether the coordinator's own local store succeeded
/// (it always attempts the write before fanning); `local_durable` is
/// whether that local write is known to have reached durable storage
/// (see [`crate::datastore::write_is_durable`]).
/// Write-quorum thresholds resolved for one put: `w` (write), `pw`
/// (primary-write), `dw` (durable-write). Grouped so
/// [`fan_repair_put_quorum`] stays inside the workspace's per-function
/// argument budget.
struct WriteQuorums {
    w: u32,
    pw: u32,
    dw: u32,
}

/// Replica-fan target for one put: the routing key plus the resolved
/// storage bytes to ship. Grouped alongside [`WriteQuorums`] so
/// [`fan_repair_put_quorum`] stays inside the workspace's per-function
/// argument budget.
struct PutTarget<'a> {
    bucket_type: &'a [u8],
    bucket: &'a [u8],
    key: &'a [u8],
    storage: &'a [u8],
}

async fn fan_repair_put_quorum(
    hooks: Option<&RoutingHooks>,
    datastore: &dyn Datastore,
    target: &PutTarget<'_>,
    quorums: &WriteQuorums,
    local_ok: bool,
) -> Result<(), String> {
    let WriteQuorums { w, pw, dw } = *quorums;
    let local_durable = local_ok && crate::datastore::write_is_durable(datastore);
    let local_only = || WriteTally {
        acks: u32::from(local_ok),
        primary_acks: u32::from(local_ok),
        durable_acks: u32::from(local_durable),
        target: 1,
        target_primaries: 1,
        remote_targets: 0,
        local_ack: local_ok,
    };
    let Some(hooks) = hooks else {
        return check_write_quorum(&local_only(), w, pw, dw);
    };
    let Ok(decision) = hooks
        .router
        .try_route(target.bucket_type, target.bucket, target.key)
    else {
        return check_write_quorum(&local_only(), w, pw, dw);
    };
    let tally = fan_write_replicas(
        hooks,
        &decision,
        target.bucket,
        target.key,
        target.storage,
        local_ok,
        local_durable,
    )
    .await;
    check_write_quorum(&tally, w, pw, dw)
}

/// Run the bucket's precommit hook over `value` if one is configured
/// and a runner is wired. Returns the value to store (possibly
/// Run the bucket's postcommit hook over the committed `value` if one
/// is configured and a runner is wired. Fire-and-forget: it returns
/// nothing a caller can act on, so it can only be called after the
/// write has already committed. A bucket without a `postcommit_module`,
/// or hooks without a runner, is a no-op.
fn run_postcommit(hooks: Option<&RoutingHooks>, bucket_type: &[u8], bucket: &[u8], value: &[u8]) {
    let Some(hooks) = hooks else {
        return;
    };
    let Some(runner) = hooks.postcommit.as_ref() else {
        return;
    };
    let Some(module) = hooks
        .router
        .registry()
        .resolve(bucket_type, bucket)
        .postcommit_module()
        .map(ToString::to_string)
    else {
        return;
    };
    runner.run(&module, value);
}

/// transformed), or an `Err(message)` to reject the write with an error
/// frame. A bucket without a `precommit_module`, or hooks without a
/// runner, passes the value through unchanged.
fn run_precommit(
    hooks: Option<&RoutingHooks>,
    bucket_type: &[u8],
    bucket: &[u8],
    value: Vec<u8>,
) -> Result<Vec<u8>, String> {
    let Some(hooks) = hooks else {
        return Ok(value);
    };
    let Some(runner) = hooks.precommit.as_ref() else {
        return Ok(value);
    };
    let Some(module) = hooks
        .router
        .registry()
        .resolve(bucket_type, bucket)
        .precommit_module()
        .map(ToString::to_string)
    else {
        return Ok(value);
    };
    match runner.run(&module, &value) {
        Ok(transformed) => Ok(transformed),
        Err(crate::router::PrecommitVeto::Rejected(reason)) => {
            Err(format!("riak put: precommit hook rejected: {reason}"))
        }
        Err(crate::router::PrecommitVeto::Error(msg)) => {
            Err(format!("riak put: precommit hook error: {msg}"))
        }
    }
}

/// Build the causal write envelope, resolve it against the stored
/// sibling set (running the bucket's precommit hook first), persist
/// the result locally, and report whether the local write landed.
///
/// Returns the canonical `SiblingSet` storage bytes and the local-ack
/// flag, or `Err(message)` when the precommit hook vetoes the write or
/// the local store reports a hard failure (not `Unsupported`, which is
/// tolerated exactly like the rest of the Riak K/V path).
async fn resolve_and_store_put(
    datastore: &dyn Datastore,
    hooks: Option<&RoutingHooks>,
    req: &RpbPutReq,
    key: &[u8],
    content: &RpbContent,
    indexes: &[(Vec<u8>, Vec<u8>)],
    new_context: Vec<u8>,
) -> Result<(Vec<u8>, bool, Vec<u8>), String> {
    let stored_set = match datastore.riak_get(&req.bucket, key).await {
        Ok(Some(bytes)) => {
            crate::proto::http::object::SiblingSet::from_storage_bytes(&bytes).unwrap_or_default()
        }
        _ => crate::proto::http::object::SiblingSet::default(),
    };
    let bucket_type = req.r#type.as_deref().unwrap_or(b"");
    let allow_mult = hooks.is_some_and(|h| {
        h.router
            .registry()
            .resolve(bucket_type, &req.bucket)
            .effective_allow_mult()
    });
    // Precommit hook: if the bucket names a precommit_module and a hook
    // runner is wired, run the write value through it before storing.
    // The hook may transform the value (accept) or veto the write
    // (reject -> error frame, nothing stored).
    let value = run_precommit(hooks, bucket_type, &req.bucket, content.value.clone())?;
    let committed_value = value.clone();
    let envelope = HttpObject {
        value,
        content_type: content
            .content_type
            .as_deref()
            .map(|c| String::from_utf8_lossy(c).into_owned()),
        indexes: indexes
            .iter()
            .map(|(n, v)| HttpIndex {
                name: String::from_utf8_lossy(n).into_owned(),
                value: String::from_utf8_lossy(v).into_owned(),
            })
            .collect(),
        links: content.links.iter().map(rpb_link_to_http).collect(),
        context: new_context,
        written_at_unix: now_unix(),
    };
    let resolved = resolve_write(&stored_set, &envelope, allow_mult);
    let storage = resolved.to_storage_bytes();
    let local_ok = match datastore
        .riak_put(&req.bucket, key, &storage, indexes)
        .await
    {
        Ok(()) | Err(DatastoreError::Unsupported(_)) => true,
        Err(e) => return Err(format!("riak put: {e}")),
    };
    Ok((storage, local_ok, committed_value))
}

/// Resolve the effective write / primary-write / durable-write quorums
/// for a put, applying per-request overrides over the bucket defaults.
/// Without routing hooks the single-node local ack is authoritative,
/// so W and DW default to `1` (satisfied by the local write alone) and
/// PW to `0` (no primary requirement, matching the no-hooks R/W path).
fn effective_write_quorums(
    hooks: Option<&RoutingHooks>,
    bucket_type: &[u8],
    bucket: &[u8],
    req: &RpbPutReq,
) -> (u32, u32, u32) {
    hooks.map_or((1, 0, 1), |h| {
        let props = h.router.registry().resolve(bucket_type, bucket);
        let n_val = props.effective_n_val();
        (
            props.effective_w(n_val, req.w),
            props.effective_pw(n_val, req.pw),
            props.effective_dw(n_val, req.dw),
        )
    })
}

async fn handle_put(
    body: &[u8],
    datastore: &dyn Datastore,
    hooks: Option<&RoutingHooks>,
) -> Result<Frame, RiakError> {
    let req = RpbPutReq::decode(body)?;
    let routing = Msg::new(0, MsgType::Unknown, true);
    datastore.dispatch(routing).await?;
    let key = match req.key.as_ref() {
        Some(k) if !k.is_empty() => k.clone(),
        _ => {
            // Server-assigned keys are not yet implemented; the
            // client must supply one.
            return Ok(error_frame(
                "riak put: server-assigned keys not implemented; supply 'key'".into(),
            ));
        }
    };
    // Riak nests the object payload in `RpbPutReq.content`
    // (`RpbContent` at tag 4). A request that omits it stores an
    // empty value, matching Riak's tolerance for a contentless put.
    let content = req.content.clone().unwrap_or_default();
    let indexes: Vec<(Vec<u8>, Vec<u8>)> = content
        .indexes
        .iter()
        .filter_map(|p| p.value.as_ref().map(|v| (p.key.clone(), v.clone())))
        .collect();
    // Sibling-aware causal write. Advance a fresh context from what the
    // CLIENT read (`req.vclock`); two writes coordinated by different
    // nodes from the same read context are concurrent, same-node
    // writes are ordered. The returned context is echoed in
    // `RpbPutResp.vclock` so the client round-trips it on its next
    // write.
    let actor = hooks.map_or_else(
        || b"local".to_vec(),
        |h| format!("{}:{}", h.local_actor.dc, h.local_actor.peer).into_bytes(),
    );
    let new_context = advance_context(&req.vclock.clone().unwrap_or_default(), &actor);
    let (storage, local_ok, committed_value) = match resolve_and_store_put(
        datastore,
        hooks,
        &req,
        &key,
        &content,
        &indexes,
        new_context.clone(),
    )
    .await
    {
        Ok(r) => r,
        Err(msg) => return Ok(error_frame(msg)),
    };
    // Fan the resolved SiblingSet storage to the key's replicas and
    // enforce W, PW, and DW (request > bucket default > quorum, except
    // PW which defaults to 0: no primary requirement). The local store
    // counts as one ack; each acking replica adds one.
    let bucket_type = req.r#type.as_deref().unwrap_or(b"");
    let (w, pw, dw) = effective_write_quorums(hooks, bucket_type, &req.bucket, &req);
    if let Err(reason) = fan_repair_put_quorum(
        hooks,
        datastore,
        &PutTarget {
            bucket_type,
            bucket: &req.bucket,
            key: &key,
            storage: &storage,
        },
        &WriteQuorums { w, pw, dw },
        local_ok,
    )
    .await
    {
        return Ok(error_frame(reason));
    }
    // Postcommit hook: fire-and-forget notification over the value that
    // just committed. Runs only after the write quorum is satisfied and
    // never changes the response.
    run_postcommit(hooks, bucket_type, &req.bucket, &committed_value);
    let resp = RpbPutResp {
        vclock: Some(new_context),
        ..RpbPutResp::default()
    };
    Ok(Frame::new(
        MessageCode::PutResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

async fn handle_del(
    body: &[u8],
    datastore: &dyn Datastore,
    hooks: Option<&RoutingHooks>,
) -> Result<Frame, RiakError> {
    let req = RpbDelReq::decode(body)?;
    if let Some(hooks) = hooks {
        let bucket_type = req.r#type.as_deref().unwrap_or(b"");
        let decision = match hooks.router.try_route(bucket_type, &req.bucket, &req.key) {
            Ok(d) => d,
            Err(e) => return Ok(error_frame(format!("riak del: {e}"))),
        };
        for replica in decision.replica_list() {
            hooks
                .outbound
                .dispatch(
                    replica.peer_idx,
                    PeerOp::Del {
                        bucket_type: decision.bucket_type.clone(),
                        bucket: req.bucket.clone(),
                        key: req.key.clone(),
                    },
                )
                .await;
        }
    }
    let routing = Msg::new(0, MsgType::Unknown, true);
    datastore.dispatch(routing).await?;
    match datastore.riak_delete(&req.bucket, &req.key).await {
        Ok(_) | Err(DatastoreError::Unsupported(_)) => {
            Ok(Frame::new(MessageCode::DelResp.as_u8(), Vec::new()))
        }
        Err(e) => Ok(error_frame(format!("riak del: {e}"))),
    }
}

/// Handle a `DtUpdateReq` (CRDT data-type update, PBC code 82).
///
/// Decodes the counter/set op, attributes it to this node's actor,
/// fans the OP (not a merged value) to every replica so each converges
/// by merge, and applies it to the local CRDT state. Single-key CRDT
/// updates are always accepted locally without a quorum, so this path
/// stays available under partition and ring churn.
async fn handle_dt_update(
    body: &[u8],
    datastore: &dyn Datastore,
    hooks: Option<&RoutingHooks>,
) -> Result<Frame, RiakError> {
    use crate::crdt_store::{CrdtStore, CrdtValue};
    use crate::proto::pb::{DtUpdateReq, DtUpdateResp};

    let req = DtUpdateReq::decode(body)?;
    let key = match req.key.as_ref() {
        Some(k) if !k.is_empty() => k.clone(),
        _ => {
            return Ok(error_frame(
                "riak dt_update: server-assigned keys not implemented; supply 'key'".into(),
            ))
        }
    };
    let actor = hooks.map_or_else(
        || crate::datatypes::ActorId::new("local", "local"),
        |h| h.local_actor.clone(),
    );
    // Translate the PBC DtOp into the internal CrdtOp attributed to
    // this node's actor.
    let Some(op) = req.op.as_ref().and_then(|o| dt_op_to_crdt(o, &actor)) else {
        return Ok(error_frame(
            "riak dt_update: unsupported or empty op (counter/set/register/flag/map/hll only)"
                .into(),
        ));
    };
    // A CRDT write always applies to the coordinator's LOCAL store
    // first: this accumulates THIS node's actor contribution (each node
    // has a distinct actor id, so per-actor counter columns sum rather
    // than overwrite on merge), and never waits on a quorum -- so the
    // write is always-available during partitions and ring changes. The
    // resulting merged full state is then fanned to every replica of
    // the key; each replica merges it idempotently (element-wise max),
    // so a re-delivered or reordered state cannot double-count. Because
    // the fan carries full state (not a delta), every replica that
    // receives it converges, and anti-entropy fills any replica a
    // fire-and-forget fan missed.
    let (value, state_bytes) =
        match CrdtStore::apply_borrowed_with_state(datastore, &req.bucket, &key, &op).await {
            Ok(r) => r,
            Err(e) => return Ok(error_frame(format!("riak dt_update: {e}"))),
        };
    if let Some(hooks) = hooks {
        let bucket_type = req.r#type.as_slice();
        if let Ok(decision) = hooks.router.try_route(bucket_type, &req.bucket, &key) {
            let wire = crate::crdt_store::to_state_wire(&state_bytes);
            for replica in decision.replica_list() {
                // Skip a fan to ourselves; we already applied locally.
                if replica.peer_idx == hooks.local_peer_idx {
                    continue;
                }
                hooks
                    .outbound
                    .dispatch(
                        replica.peer_idx,
                        crate::router::PeerOp::DtUpdate {
                            bucket_type: decision.bucket_type.clone(),
                            bucket: req.bucket.clone(),
                            key: key.clone(),
                            op: wire.clone(),
                        },
                    )
                    .await;
            }
        }
    }
    let mut resp = DtUpdateResp::default();
    match value {
        CrdtValue::Counter(n) => resp.counter_value = Some(n),
        CrdtValue::Set(elems) => resp.set_value = elems,
        CrdtValue::Register(v) => resp.register_value = Some(v),
        CrdtValue::Flag(b) => resp.flag_value = Some(b),
        CrdtValue::Map(fields) => resp.map_value = Some(Box::new(map_value_to_wire(&fields))),
        CrdtValue::Hll(n) => resp.hll_value = Some(n),
        CrdtValue::Missing => {}
    }
    Ok(Frame::new(
        MessageCode::DtUpdateResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

/// Handle a `DtFetchReq` (CRDT data-type fetch, PBC code 80).
/// Test-only entry point for the CRDT fetch handler with routing
/// hooks, so integration tests can exercise read coordination without
/// standing up a full PBC listener. Not part of the stable API.
#[doc(hidden)]
pub async fn handle_dt_fetch_for_test(
    body: &[u8],
    datastore: &dyn Datastore,
    hooks: Option<&RoutingHooks>,
) -> Result<crate::proto::pb::framer::Frame, RiakError> {
    handle_dt_fetch(body, datastore, hooks).await
}

async fn handle_dt_fetch(
    body: &[u8],
    datastore: &dyn Datastore,
    hooks: Option<&RoutingHooks>,
) -> Result<Frame, RiakError> {
    use crate::crdt_store::{CrdtStore, CrdtValue};
    use crate::datatypes::{TAG_COUNTER, TAG_FLAG, TAG_HLL, TAG_MAP, TAG_REGISTER, TAG_SET};
    use crate::proto::pb::{
        DtFetchReq, DtFetchResp, DATA_TYPE_COUNTER, DATA_TYPE_FLAG, DATA_TYPE_HLL, DATA_TYPE_MAP,
        DATA_TYPE_REGISTER, DATA_TYPE_SET,
    };

    let req = DtFetchReq::decode(body)?;
    // The bucket type selects the projection: `sets` -> OR-set,
    // `registers` -> LWW-register, `flags` -> EW-flag, `maps` ->
    // observed-remove map, `hlls`/`hll` -> HyperLogLog, anything
    // else -> counter (Riak's `counters` default).
    let (tag, dtype) = if req.r#type == b"sets" {
        (TAG_SET, DATA_TYPE_SET)
    } else if req.r#type == b"registers" {
        (TAG_REGISTER, DATA_TYPE_REGISTER)
    } else if req.r#type == b"flags" {
        (TAG_FLAG, DATA_TYPE_FLAG)
    } else if req.r#type == b"maps" {
        (TAG_MAP, DATA_TYPE_MAP)
    } else if req.r#type == b"hlls" || req.r#type == b"hll" {
        (TAG_HLL, DATA_TYPE_HLL)
    } else {
        (TAG_COUNTER, DATA_TYPE_COUNTER)
    };
    // Read coordination: fan a DtFetch to every OTHER replica of the
    // key, merge each returned state into the local state, and
    // project the converged value. This makes a fetch to ANY node --
    // replica or not -- return the full value, matching Riak's
    // merge-on-read for data types. On a transport without request/
    // response (fire-and-forget only) `request` returns `None` and we
    // fall back to the local value; anti-entropy is the backstop.
    let mut merged_state: Vec<u8> = match datastore.riak_get(&req.bucket, &req.key).await {
        Ok(Some(s)) => s,
        _ => Vec::new(),
    };
    if let Some(hooks) = hooks {
        if let Ok(decision) = hooks.router.try_route(&req.r#type, &req.bucket, &req.key) {
            for replica in decision.replica_list() {
                if replica.peer_idx == hooks.local_peer_idx {
                    continue;
                }
                let reply = hooks
                    .outbound
                    .request(
                        replica.peer_idx,
                        crate::router::PeerOp::DtFetch {
                            bucket_type: decision.bucket_type.clone(),
                            bucket: req.bucket.clone(),
                            key: req.key.clone(),
                            tag,
                        },
                    )
                    .await;
                if let Some(state) = reply {
                    if !state.is_empty() {
                        merged_state =
                            match crate::crdt_store::merge_two_states(&merged_state, &state) {
                                Ok(m) => m,
                                Err(_) => merged_state,
                            };
                    }
                }
            }
        }
    }
    // Project the merged state; an empty merged state is Missing.
    let value = if merged_state.is_empty() {
        CrdtValue::Missing
    } else {
        crate::crdt_store::project_state(&merged_state, tag).unwrap_or(CrdtValue::Missing)
    };
    // Persist the merged state locally so a subsequent local read is
    // already converged (read repair for the coordinating node). Best
    // effort: a failure just means the next fetch re-merges.
    if !matches!(value, CrdtValue::Missing) {
        let _ =
            CrdtStore::merge_state_borrowed(datastore, &req.bucket, &req.key, &merged_state).await;
    }
    let mut resp = DtFetchResp {
        r#type: dtype,
        ..DtFetchResp::default()
    };
    resp.value = crdt_value_to_dt_value(value);
    Ok(Frame::new(
        MessageCode::DtFetchResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

/// Project a [`crate::crdt_store::CrdtValue`] into the [`DtValue`]
/// wire envelope carried by a `DtFetchResp`. Returns `None` for
/// [`crate::crdt_store::CrdtValue::Missing`], matching Riak's
/// absent-`value` response for a key that does not exist.
fn crdt_value_to_dt_value(
    value: crate::crdt_store::CrdtValue,
) -> Option<crate::proto::pb::DtValue> {
    use crate::crdt_store::CrdtValue;
    use crate::proto::pb::DtValue;

    match value {
        CrdtValue::Counter(n) => Some(DtValue {
            counter_value: Some(n),
            ..DtValue::default()
        }),
        CrdtValue::Set(elems) => Some(DtValue {
            set_value: elems,
            ..DtValue::default()
        }),
        CrdtValue::Register(v) => Some(DtValue {
            register_value: Some(v),
            ..DtValue::default()
        }),
        CrdtValue::Flag(b) => Some(DtValue {
            flag_value: Some(b),
            ..DtValue::default()
        }),
        CrdtValue::Map(fields) => Some(DtValue {
            map_value: Some(Box::new(map_value_to_wire(&fields))),
            ..DtValue::default()
        }),
        CrdtValue::Hll(n) => Some(DtValue {
            hll_value: Some(n),
            ..DtValue::default()
        }),
        CrdtValue::Missing => None,
    }
}

/// Translate a PBC [`DtOp`] into the internal
/// [`crate::crdt_store::CrdtOp`], attributing it to `actor`. Returns
/// `None` for an empty or unsupported op. Counter, set, register,
/// flag, map, and HLL are all wired.
fn dt_op_to_crdt(
    op: &crate::proto::pb::DtOp,
    actor: &crate::datatypes::ActorId,
) -> Option<crate::crdt_store::CrdtOp> {
    use crate::crdt_store::CrdtOp;
    if let Some(c) = op.counter_op.as_ref() {
        return Some(CrdtOp::Counter {
            actor: actor.clone(),
            delta: c.increment.unwrap_or(0),
        });
    }
    if let Some(s) = op.set_op.as_ref() {
        return Some(CrdtOp::Set {
            actor: actor.clone(),
            adds: s.adds.clone(),
            removes: s.removes.clone(),
        });
    }
    if let Some(r) = op.register_op.as_ref() {
        return Some(CrdtOp::Register {
            actor: actor.clone(),
            value: r.value.clone(),
        });
    }
    if let Some(f) = op.flag_op.as_ref() {
        return Some(CrdtOp::Flag {
            actor: actor.clone(),
            enable: f.enable,
        });
    }
    if let Some(m) = op.map_op.as_ref() {
        return Some(CrdtOp::Map {
            actor: actor.clone(),
            ops: pbc_map_op_to_internal(m),
        });
    }
    if let Some(h) = op.hll_op.as_ref() {
        return Some(CrdtOp::Hll {
            actor: actor.clone(),
            items: h.add_value.clone(),
        });
    }
    None
}

/// Translate a PBC [`crate::proto::pb::MapOp`] batch into the
/// internal [`crate::datatypes::MapOp`] sequence [`Map::apply`]
/// consumes one at a time.
///
/// A field-level `SetOp` batch (multiple adds/removes in one
/// [`crate::proto::pb::ScalarOp`]) expands into one internal op per
/// element, since [`crate::datatypes::NestedOp::SetAdd`] /
/// [`crate::datatypes::NestedOp::SetRemove`] are each singular. A
/// field or update whose `field`/`op` is absent, or whose
/// `field_type` is not a recognized [`crate::datatypes::FieldType`]
/// wire code, is dropped rather than rejecting the whole batch.
fn pbc_map_op_to_internal(op: &crate::proto::pb::MapOp) -> Vec<crate::datatypes::MapOp> {
    use crate::datatypes::{FieldKey, FieldType, MapOp};

    let mut ops = Vec::with_capacity(op.updates.len() + op.removes.len());
    for update in &op.updates {
        let Some(field) = update.field.as_ref() else {
            continue;
        };
        let Some(field_type) = FieldType::from_wire(field.field_type) else {
            continue;
        };
        let Some(scalar) = update.op.as_ref() else {
            continue;
        };
        let key = FieldKey::new(field.name.clone(), field_type);
        for nested in pbc_scalar_op_to_nested(scalar) {
            ops.push(MapOp::Update {
                field: key.clone(),
                op: nested,
            });
        }
    }
    for field in &op.removes {
        if let Some(field_type) = FieldType::from_wire(field.field_type) {
            ops.push(MapOp::Remove {
                field: FieldKey::new(field.name.clone(), field_type),
            });
        }
    }
    ops
}

/// Translate one PBC [`crate::proto::pb::ScalarOp`] into zero or more
/// [`crate::datatypes::NestedOp`]s applied to the same field. A
/// register op without an explicit `ts_micros` is stamped with the
/// current wall clock, matching [`crate::datatypes::LwwRegister::assign_now`]'s
/// policy for the top-level `DtOp::register_op` path.
fn pbc_scalar_op_to_nested(op: &crate::proto::pb::ScalarOp) -> Vec<crate::datatypes::NestedOp> {
    use crate::datatypes::NestedOp;

    if let Some(c) = op.counter_op.as_ref() {
        return vec![NestedOp::Counter(c.increment.unwrap_or(0))];
    }
    if let Some(s) = op.set_op.as_ref() {
        let mut nested = Vec::with_capacity(s.adds.len() + s.removes.len());
        for a in &s.adds {
            nested.push(NestedOp::SetAdd(a.clone()));
        }
        for r in &s.removes {
            nested.push(NestedOp::SetRemove(r.clone()));
        }
        return nested;
    }
    if let Some(r) = op.register_op.as_ref() {
        let ts_micros = r.ts_micros.unwrap_or_else(now_micros);
        return vec![NestedOp::RegisterAssign {
            value: r.value.clone(),
            ts_micros,
        }];
    }
    if let Some(f) = op.flag_op.as_ref() {
        return vec![NestedOp::Flag(f.enable)];
    }
    if let Some(m) = op.map_op.as_ref() {
        return pbc_map_op_to_internal(m)
            .into_iter()
            .map(|inner| NestedOp::Map(Box::new(inner)))
            .collect();
    }
    Vec::new()
}

/// Current wall-clock time in microseconds since the Unix epoch,
/// clamped to `u64::MAX` on overflow. Used to stamp a map register
/// field's [`crate::datatypes::NestedOp::RegisterAssign`] when the
/// client did not supply an explicit timestamp.
fn now_micros() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

/// Project a [`crate::crdt_store::CrdtValue::Map`] field set into the
/// PBC [`crate::proto::pb::MapValue`] wire shape, recursing into
/// [`crate::datatypes::FieldValue::NestedMap`].
fn map_value_to_wire(
    fields: &std::collections::BTreeMap<crate::datatypes::FieldKey, crate::datatypes::FieldValue>,
) -> crate::proto::pb::MapValue {
    use crate::proto::pb::{MapEntry, MapField, MapValue};

    MapValue {
        entries: fields
            .iter()
            .map(|(key, value)| MapEntry {
                field: Some(MapField {
                    name: key.name.clone(),
                    field_type: key.field_type.to_wire(),
                }),
                value: Some(field_value_to_wire(value)),
            })
            .collect(),
    }
}

/// Project one [`crate::datatypes::FieldValue`] into the PBC
/// [`crate::proto::pb::ScalarValue`] wire shape.
fn field_value_to_wire(value: &crate::datatypes::FieldValue) -> crate::proto::pb::ScalarValue {
    use crate::datatypes::{Crdt, FieldValue};
    use crate::proto::pb::ScalarValue;

    match value {
        FieldValue::Counter(c) => ScalarValue {
            counter_value: Some(c.value()),
            ..ScalarValue::default()
        },
        FieldValue::OrSet(s) => ScalarValue {
            set_value: s.value().into_iter().collect(),
            ..ScalarValue::default()
        },
        FieldValue::LwwRegister(r) => ScalarValue {
            register_value: Some(r.value()),
            ..ScalarValue::default()
        },
        FieldValue::EwFlag(f) => ScalarValue {
            flag_value: Some(f.value()),
            ..ScalarValue::default()
        },
        FieldValue::NestedMap(m) => ScalarValue {
            map_value: Some(Box::new(map_value_to_wire(&m.value()))),
            ..ScalarValue::default()
        },
    }
}

/// Run a 2i secondary-index query against the datastore.
///
/// Both equality (`qtype = 0`) and range (`qtype = 1`) queries are
/// honoured. The result set is streamed back as a sequence of
/// [`RpbIndexResp`] frames carrying up to [`LIST_CHUNK_SIZE`] keys
/// each, finished with a body-less terminator frame whose `done =
/// Some(true)` flag is the only payload. Datastore errors mid-walk
/// translate to an `RpbErrorResp` and stop the stream.
///
/// Backwards compat: a client that ignores `done` and reads only
/// the first frame still gets a partial answer (the first chunk).
/// A datastore that does not implement 2i (the
/// [`MemoryDatastore`](dynomite::embed::MemoryDatastore) default)
/// emits a single error frame with `errmsg` describing the
/// limitation.
///
/// `pagination_sort`, `term_regex`, `continuation`, and
/// `cover_context` on the request are accepted but not acted on.
///
/// 2i queries do NOT trampoline through [`Datastore::dispatch`]
/// because the existing list / property paths (the only other
/// non-KV ops) also bypass it; the substrate's dispatch counter
/// is reserved for K/V operations.
async fn handle_index(body: &[u8], datastore: &dyn Datastore) -> Result<FrameStream, RiakError> {
    let req = RpbIndexReq::decode(body)?;
    let result = match req.qtype {
        INDEX_QUERY_TYPE_EQ => {
            let value = req.key.as_deref().unwrap_or(b"");
            datastore
                .riak_index_eq(&req.bucket, &req.index, value)
                .await
        }
        INDEX_QUERY_TYPE_RANGE => {
            let min = req.range_min.as_deref().unwrap_or(b"");
            let max = req.range_max.as_deref().unwrap_or(b"");
            datastore
                .riak_index_range(&req.bucket, &req.index, min, max)
                .await
        }
        other => {
            return Ok(single_frame(error_frame(format!(
                "riak index: unsupported qtype {other}; expected 0 (eq) or 1 (range)"
            ))));
        }
    };
    match result {
        Ok(mut keys) => {
            if let Some(cap) = req.max_results {
                let cap = cap as usize;
                if keys.len() > cap {
                    keys.truncate(cap);
                }
            }
            Ok(Box::pin(index_keys_to_frames(keys)))
        }
        Err(DatastoreError::Unsupported(_)) => Ok(single_frame(error_frame(
            "secondary-index queries not implemented for this datastore".into(),
        ))),
        Err(e) => Ok(single_frame(error_frame(format!("riak index: {e}")))),
    }
}

/// Producer state for the streaming 2i path. The result set is
/// already materialised in `Vec<Vec<u8>>`; chunking happens at the
/// server boundary so a client sees one frame per
/// [`LIST_CHUNK_SIZE`] keys plus a `done = true` terminator.
enum IndexChunkState {
    /// Streaming: the next chunk is drained from `keys`.
    Streaming { keys: Vec<Vec<u8>> },
    /// All keys delivered; emit the body-less terminator frame.
    Terminate,
    /// Stream finished.
    Done,
}

fn index_keys_to_frames(keys: Vec<Vec<u8>>) -> impl Stream<Item = Result<Frame, RiakError>> + Send {
    futures_util::stream::unfold(IndexChunkState::Streaming { keys }, |state| async move {
        match state {
            IndexChunkState::Done => None,
            IndexChunkState::Terminate => {
                let resp = RpbIndexResp {
                    keys: Vec::new(),
                    results: Vec::new(),
                    continuation: None,
                    done: Some(true),
                };
                let frame = Frame::new(MessageCode::IndexResp.as_u8(), resp.encode_to_vec());
                Some((Ok(frame), IndexChunkState::Done))
            }
            IndexChunkState::Streaming { mut keys } => {
                if keys.is_empty() {
                    // Empty result set: emit a single terminator
                    // frame so a client that reads exactly one
                    // frame still observes `done = true`.
                    let resp = RpbIndexResp {
                        keys: Vec::new(),
                        results: Vec::new(),
                        continuation: None,
                        done: Some(true),
                    };
                    let frame = Frame::new(MessageCode::IndexResp.as_u8(), resp.encode_to_vec());
                    return Some((Ok(frame), IndexChunkState::Done));
                }
                let take = LIST_CHUNK_SIZE.min(keys.len());
                let tail = keys.split_off(take);
                let chunk = keys; // first `take` entries
                let next = if tail.is_empty() {
                    IndexChunkState::Terminate
                } else {
                    IndexChunkState::Streaming { keys: tail }
                };
                let resp = RpbIndexResp {
                    keys: chunk,
                    results: Vec::new(),
                    continuation: None,
                    done: Some(false),
                };
                let frame = Frame::new(MessageCode::IndexResp.as_u8(), resp.encode_to_vec());
                Some((Ok(frame), next))
            }
        }
    })
}

/// Build an `RpbErrorResp` frame from a human-readable message.
fn error_frame(message: String) -> Frame {
    let resp = RpbErrorResp {
        errmsg: message.into_bytes(),
        errcode: 1,
    };
    Frame::new(MessageCode::ErrorResp.as_u8(), resp.encode_to_vec())
}

fn handle_get_bucket(body: &[u8], hooks: Option<&RoutingHooks>) -> Result<Frame, RiakError> {
    let req = RpbGetBucketReq::decode(body)?;
    // When a routing-hooks bundle is wired, the server consults
    // the bucket-properties registry so a freshly-set
    // [`RpbSetBucketReq`] round-trips through subsequent
    // [`RpbGetBucketReq`] frames. Without hooks the legacy
    // conservative defaults stay in force so existing tests
    // that build the substrate without a registry continue to
    // see the same response shape they always did.
    let props = if let Some(hooks) = hooks {
        let bucket_type = req.r#type.as_deref().unwrap_or(b"");
        let resolved = hooks.router.registry().resolve(bucket_type, &req.bucket);
        RpbBucketProps {
            n_val: Some(u32::from(resolved.effective_n_val())),
            allow_mult: Some(resolved.effective_allow_mult()),
            last_write_wins: Some(false),
            chash_keyfun: Some(resolved.effective_keyfun().to_wire()),
            chash_keyfun_module: resolved
                .effective_keyfun()
                .custom_module()
                .map(|s| s.as_bytes().to_vec()),
            replication_strategy: Some(resolved.effective_strategy().to_wire()),
            ttl_seconds: match resolved.effective_ttl_seconds() {
                0 => None,
                secs => Some(u32::try_from(secs).unwrap_or(u32::MAX)),
            },
            r: Some(resolved.effective_r(resolved.effective_n_val(), None)),
            w: Some(resolved.effective_w(resolved.effective_n_val(), None)),
            pr: Some(resolved.effective_pr(resolved.effective_n_val(), None)),
            pw: Some(resolved.effective_pw(resolved.effective_n_val(), None)),
            dw: Some(resolved.effective_dw(resolved.effective_n_val(), None)),
            ..RpbBucketProps::default()
        }
    } else {
        RpbBucketProps {
            n_val: Some(3),
            allow_mult: Some(false),
            last_write_wins: Some(false),
            ..RpbBucketProps::default()
        }
    };
    let resp = RpbGetBucketResp { props: Some(props) };
    Ok(Frame::new(
        MessageCode::GetBucketResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

fn handle_set_bucket(body: &[u8], hooks: Option<&RoutingHooks>) -> Result<Frame, RiakError> {
    let req = RpbSetBucketReq::decode(body)?;
    if let Some(hooks) = hooks {
        let bucket_type = req.r#type.as_deref().unwrap_or(b"");
        if let Some(props) = req.props.as_ref() {
            let mut bp = crate::bucket_props::BucketProps::default();
            if let Some(w) = props.chash_keyfun {
                if let Ok(kf) = crate::datatypes::keyfun::KeyFun::from_wire(w) {
                    if let crate::datatypes::keyfun::KeyFun::Custom(_) = kf {
                        // CUSTOM names its module out-of-band
                        // (Riak: {modfun, Mod, Fun}); dyniak takes
                        // the module id from the dyniak-extension
                        // `chash_keyfun_module` field. Reject the
                        // write when the module is unnamed or not
                        // registered so routing never silently
                        // changes to a missing keyfun.
                        let module_id = props
                            .chash_keyfun_module
                            .as_deref()
                            .map(|b| String::from_utf8_lossy(b).into_owned())
                            .unwrap_or_default();
                        if let Err(msg) = validate_custom_keyfun(hooks, &module_id) {
                            return Ok(error_frame(msg));
                        }
                        bp.keyfun =
                            Some(crate::datatypes::keyfun::KeyFun::Custom(module_id.clone()));
                        bp.custom_keyfun_module = Some(module_id);
                    } else {
                        bp.keyfun = Some(kf);
                    }
                }
            }
            if let Some(w) = props.replication_strategy {
                if let Ok(s) = crate::replication::ReplicationStrategy::from_wire(w) {
                    bp.strategy = Some(s);
                }
            }
            if let Some(n) = props.n_val {
                bp.n_val = Some(u8::try_from(n).unwrap_or(u8::MAX));
            }
            if let Some(am) = props.allow_mult {
                bp.allow_mult = Some(am);
            }
            if let Some(ttl) = props.ttl_seconds {
                bp.ttl_seconds = Some(u64::from(ttl));
            }
            // Quorum tunables: stored as bucket defaults so a later
            // read/write with no per-request override uses them.
            bp.r = props.r;
            bp.w = props.w;
            bp.pr = props.pr;
            bp.pw = props.pw;
            bp.dw = props.dw;
            hooks.router.registry().set(bucket_type, &req.bucket, bp);
        }
    }
    // Without a registry the supplied properties are dropped
    // (legacy behaviour); the response is an empty
    // acknowledgement either way so a conforming client treats
    // the call as successful.
    let resp = RpbSetBucketResp::default();
    Ok(Frame::new(
        MessageCode::SetBucketResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

/// Validate that a `CUSTOM` keyfun names a registered module.
///
/// Returns `Err(message)` (which the caller turns into an
/// `RpbErrorResp`) when the module id is empty or, with the
/// `wasm` feature, when no module with that id is registered in
/// the router's keyfun store. Without the `wasm` feature a
/// non-empty id is accepted (no store exists to consult), but a
/// `Custom` route will then surface a clean error at request time.
#[cfg(feature = "wasm")]
fn validate_custom_keyfun(hooks: &RoutingHooks, module_id: &str) -> Result<(), String> {
    if module_id.is_empty() {
        return Err(
            "set bucket: chash_keyfun CUSTOM requires a non-empty chash_keyfun_module".into(),
        );
    }
    match hooks.router.keyfun_store() {
        Some(store) if store.contains(module_id) => Ok(()),
        Some(_) => Err(format!(
            "set bucket: chash_keyfun CUSTOM module {module_id:?} is not registered"
        )),
        None => Err(
            "set bucket: chash_keyfun CUSTOM selected but no keyfun WASM store is configured"
                .into(),
        ),
    }
}

#[cfg(not(feature = "wasm"))]
fn validate_custom_keyfun(_hooks: &RoutingHooks, module_id: &str) -> Result<(), String> {
    if module_id.is_empty() {
        return Err(
            "set bucket: chash_keyfun CUSTOM requires a non-empty chash_keyfun_module".into(),
        );
    }
    Err("set bucket: chash_keyfun CUSTOM requires the 'wasm' feature".into())
}

/// Run a MapReduce job submitted via PBC and stream the per-phase
/// outputs back as a sequence of [`RpbMapRedResp`] frames.
///
/// The wire shape mirrors Riak's documented streaming contract:
///
/// * One non-terminal `RpbMapRedResp` frame per phase batch produced
///   by the executor, carrying `phase = Some(batch.phase)`,
///   `response = Some(json_bytes)`, and `done = Some(false)`.
/// * One body-less terminator `RpbMapRedResp` with
///   `phase = None`, `response = None`, and `done = Some(true)`.
/// * On any executor error, a single `RpbErrorResp` frame and the
///   stream closes; no terminator is emitted because the error
///   itself signals end-of-stream to the client.
///
/// The PBC framer rejects malformed requests up-front (decode error,
/// non-JSON content type) by emitting a single `RpbErrorResp`. A
/// client that reads only the first frame still observes a
/// well-formed answer: either the first phase batch (a partial
/// result) or a server error.
fn handle_mapreduce(body: &[u8]) -> FrameStream {
    use crate::mapreduce::{builtins::default_registry, run_job_streaming, MapReduceJob};

    let req = match RpbMapRedReq::decode(body) {
        Ok(r) => r,
        Err(e) => {
            let resp = RpbErrorResp {
                errmsg: format!("MapReduce request decode: {e}").into_bytes(),
                errcode: 1,
            };
            return single_frame(Frame::new(
                MessageCode::ErrorResp.as_u8(),
                resp.encode_to_vec(),
            ));
        }
    };
    if req.content_type != b"application/json" {
        let resp = RpbErrorResp {
            errmsg: format!(
                "unsupported MapReduce content-type: {}",
                String::from_utf8_lossy(&req.content_type)
            )
            .into_bytes(),
            errcode: 1,
        };
        return single_frame(Frame::new(
            MessageCode::ErrorResp.as_u8(),
            resp.encode_to_vec(),
        ));
    }
    let job: MapReduceJob = match serde_json::from_slice(&req.request) {
        Ok(j) => j,
        Err(e) => {
            let resp = RpbErrorResp {
                errmsg: format!("MapReduce job decode: {e}").into_bytes(),
                errcode: 1,
            };
            return single_frame(Frame::new(
                MessageCode::ErrorResp.as_u8(),
                resp.encode_to_vec(),
            ));
        }
    };

    let registry = Arc::new(default_registry());
    let rx = run_job_streaming(job, registry);
    Box::pin(mapreduce_response_stream(rx))
}

/// Producer state for the streaming MapReduce path.
enum MrStreamState {
    /// Pump the next [`PhaseBatch`] from the executor.
    Streaming(mpsc::Receiver<Result<PhaseBatch, MrError>>),
    /// Stream complete (or aborted by an error frame); no further
    /// items.
    Done,
}

/// Build the [`FrameStream`] backing [`handle_mapreduce`].
///
/// The state machine fans the executor's `Receiver<Result<PhaseBatch,
/// MrError>>` out to one `RpbMapRedResp`/`RpbErrorResp` frame per
/// poll. End-of-stream is signalled with a body-less
/// `RpbMapRedResp { done = Some(true) }` terminator. Executor
/// errors short-circuit to a single `RpbErrorResp` and close the
/// stream without a terminator: the error is the terminator.
fn mapreduce_response_stream(
    rx: mpsc::Receiver<Result<PhaseBatch, MrError>>,
) -> impl Stream<Item = Result<Frame, RiakError>> + Send {
    futures_util::stream::unfold(MrStreamState::Streaming(rx), |state| async move {
        match state {
            MrStreamState::Done => None,
            MrStreamState::Streaming(mut rx) => match rx.recv().await {
                None => {
                    let resp = RpbMapRedResp {
                        phase: None,
                        response: None,
                        done: Some(true),
                    };
                    let frame = Frame::new(MessageCode::MapRedResp.as_u8(), resp.encode_to_vec());
                    Some((Ok(frame), MrStreamState::Done))
                }
                Some(Ok(batch)) => {
                    // Each non-terminal frame mirrors the documented
                    // wire shape `[{ "phase": N, "data": [...] }]`,
                    // matching the HTTP `/mapred` multipart writer
                    // so a client that bridges the two transports
                    // sees byte-identical phase payloads.
                    let payload = serde_json::json!([{
                        "phase": batch.phase,
                        "data": batch.data,
                    }]);
                    let body = serde_json::to_vec(&payload).unwrap_or_else(|_| b"[]".to_vec());
                    let resp = RpbMapRedResp {
                        phase: Some(batch.phase),
                        response: Some(body),
                        done: Some(false),
                    };
                    let frame = Frame::new(MessageCode::MapRedResp.as_u8(), resp.encode_to_vec());
                    Some((Ok(frame), MrStreamState::Streaming(rx)))
                }
                Some(Err(e)) => {
                    let resp = RpbErrorResp {
                        errmsg: format!("MapReduce execution: {e}").into_bytes(),
                        errcode: 1,
                    };
                    let frame = Frame::new(MessageCode::ErrorResp.as_u8(), resp.encode_to_vec());
                    // No terminator: the error itself ends the
                    // stream. Riak's reference server behaves the
                    // same way; otherwise a peer that treats
                    // `done = true` as success would silently
                    // mask the failure.
                    Some((Ok(frame), MrStreamState::Done))
                }
            },
        }
    })
}

// ------------------------------------------------------------------
// Streaming list handlers.
// ------------------------------------------------------------------
//
// Both producers consume the datastore's `Bytes` stream, batch up to
// `LIST_CHUNK_SIZE` entries per outbound frame, and finish with a
// body-less `done = true` terminator. Datastore errors mid-stream
// translate to an `RpbErrorResp` and stop the producer.
//
// A client that ignores the `done` flag and just reads the first
// frame still works: it sees the first chunk, which is a partial
// list. The streaming-list journal entry documents this behaviour.

fn handle_list_buckets(body: &[u8], datastore: &dyn Datastore) -> Result<FrameStream, RiakError> {
    let _req = RpbListBucketsReq::decode(body)?;
    let stream = datastore.list_buckets_stream();
    Ok(Box::pin(buckets_to_frames(stream)))
}

fn handle_list_keys(body: &[u8], datastore: &dyn Datastore) -> Result<FrameStream, RiakError> {
    let req = RpbListKeysReq::decode(body)?;
    let stream = datastore.list_keys_stream(&req.bucket);
    Ok(Box::pin(keys_to_frames(stream)))
}

/// Producer state for the streaming list path.
enum ListChunkState {
    Streaming(DatastoreByteStream, Vec<Vec<u8>>),
    Terminate,
    Done,
}

fn buckets_to_frames(
    s: DatastoreByteStream,
) -> impl Stream<Item = Result<Frame, RiakError>> + Send {
    futures_util::stream::unfold(
        ListChunkState::Streaming(s, Vec::with_capacity(LIST_CHUNK_SIZE)),
        |state| async move {
            match state {
                ListChunkState::Done => None,
                ListChunkState::Terminate => {
                    let resp = RpbListBucketsResp {
                        buckets: Vec::new(),
                        done: Some(true),
                    };
                    let frame =
                        Frame::new(MessageCode::ListBucketsResp.as_u8(), resp.encode_to_vec());
                    Some((Ok(frame), ListChunkState::Done))
                }
                ListChunkState::Streaming(mut stream, mut buffer) => loop {
                    if buffer.len() >= LIST_CHUNK_SIZE {
                        let resp = RpbListBucketsResp {
                            buckets: buffer,
                            done: Some(false),
                        };
                        let frame =
                            Frame::new(MessageCode::ListBucketsResp.as_u8(), resp.encode_to_vec());
                        return Some((
                            Ok(frame),
                            ListChunkState::Streaming(stream, Vec::with_capacity(LIST_CHUNK_SIZE)),
                        ));
                    }
                    match stream.next().await {
                        Some(Ok(b)) => buffer.push(b.to_vec()),
                        Some(Err(e)) => {
                            let resp = RpbErrorResp {
                                errmsg: format!("list-buckets failed: {e}").into_bytes(),
                                errcode: 1,
                            };
                            let frame =
                                Frame::new(MessageCode::ErrorResp.as_u8(), resp.encode_to_vec());
                            return Some((Ok(frame), ListChunkState::Done));
                        }
                        None => {
                            if buffer.is_empty() {
                                let resp = RpbListBucketsResp {
                                    buckets: Vec::new(),
                                    done: Some(true),
                                };
                                let frame = Frame::new(
                                    MessageCode::ListBucketsResp.as_u8(),
                                    resp.encode_to_vec(),
                                );
                                return Some((Ok(frame), ListChunkState::Done));
                            }
                            let resp = RpbListBucketsResp {
                                buckets: buffer,
                                done: Some(false),
                            };
                            let frame = Frame::new(
                                MessageCode::ListBucketsResp.as_u8(),
                                resp.encode_to_vec(),
                            );
                            return Some((Ok(frame), ListChunkState::Terminate));
                        }
                    }
                },
            }
        },
    )
}

fn keys_to_frames(s: DatastoreByteStream) -> impl Stream<Item = Result<Frame, RiakError>> + Send {
    futures_util::stream::unfold(
        ListChunkState::Streaming(s, Vec::with_capacity(LIST_CHUNK_SIZE)),
        |state| async move {
            match state {
                ListChunkState::Done => None,
                ListChunkState::Terminate => {
                    let resp = RpbListKeysResp {
                        keys: Vec::new(),
                        done: Some(true),
                    };
                    let frame = Frame::new(MessageCode::ListKeysResp.as_u8(), resp.encode_to_vec());
                    Some((Ok(frame), ListChunkState::Done))
                }
                ListChunkState::Streaming(mut stream, mut buffer) => loop {
                    if buffer.len() >= LIST_CHUNK_SIZE {
                        let resp = RpbListKeysResp {
                            keys: buffer,
                            done: Some(false),
                        };
                        let frame =
                            Frame::new(MessageCode::ListKeysResp.as_u8(), resp.encode_to_vec());
                        return Some((
                            Ok(frame),
                            ListChunkState::Streaming(stream, Vec::with_capacity(LIST_CHUNK_SIZE)),
                        ));
                    }
                    match stream.next().await {
                        Some(Ok(b)) => buffer.push(b.to_vec()),
                        Some(Err(e)) => {
                            let resp = RpbErrorResp {
                                errmsg: format!("list-keys failed: {e}").into_bytes(),
                                errcode: 1,
                            };
                            let frame =
                                Frame::new(MessageCode::ErrorResp.as_u8(), resp.encode_to_vec());
                            return Some((Ok(frame), ListChunkState::Done));
                        }
                        None => {
                            if buffer.is_empty() {
                                let resp = RpbListKeysResp {
                                    keys: Vec::new(),
                                    done: Some(true),
                                };
                                let frame = Frame::new(
                                    MessageCode::ListKeysResp.as_u8(),
                                    resp.encode_to_vec(),
                                );
                                return Some((Ok(frame), ListChunkState::Done));
                            }
                            let resp = RpbListKeysResp {
                                keys: buffer,
                                done: Some(false),
                            };
                            let frame =
                                Frame::new(MessageCode::ListKeysResp.as_u8(), resp.encode_to_vec());
                            return Some((Ok(frame), ListChunkState::Terminate));
                        }
                    }
                },
            }
        },
    )
}

// ------------------------------------------------------------------
// Cluster admin handlers.
// ------------------------------------------------------------------

fn handle_list_peers(body: &[u8], admin: &dyn ClusterAdmin) -> Result<Frame, RiakError> {
    let _ = DynRpbListPeersReq::decode(body)?;
    let snaps = admin.list_peers();
    let resp = DynRpbListPeersResp {
        peers: snaps.iter().map(snapshot_to_pb).collect(),
    };
    Ok(Frame::new(
        MessageCode::DynListPeersResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

fn handle_cluster_join(body: &[u8], admin: &dyn ClusterAdmin) -> Result<Frame, RiakError> {
    let req = DynRpbClusterJoinReq::decode(body)?;
    let Ok(target_str) = std::str::from_utf8(&req.target) else {
        return Ok(error_frame("cluster-join: target is not UTF-8".into()));
    };
    let target = match target_str.parse::<std::net::SocketAddr>() {
        Ok(t) => t,
        Err(e) => {
            return Ok(error_frame(format!(
                "cluster-join: invalid target '{target_str}': {e}"
            )));
        }
    };
    match admin.cluster_join(target) {
        Ok(plan) => {
            let resp = DynRpbClusterJoinResp {
                change: Some(change_to_pb(&plan.change)),
            };
            Ok(Frame::new(
                MessageCode::DynClusterJoinResp.as_u8(),
                resp.encode_to_vec(),
            ))
        }
        Err(e) => Ok(error_frame(format_cluster_error("cluster-join", &e))),
    }
}

fn handle_cluster_leave(body: &[u8], admin: &dyn ClusterAdmin) -> Result<Frame, RiakError> {
    let req = DynRpbClusterLeaveReq::decode(body)?;
    match admin.cluster_leave(req.peer_idx) {
        Ok(plan) => {
            let resp = DynRpbClusterLeaveResp {
                change: Some(change_to_pb(&plan.change)),
            };
            Ok(Frame::new(
                MessageCode::DynClusterLeaveResp.as_u8(),
                resp.encode_to_vec(),
            ))
        }
        Err(e) => Ok(error_frame(format_cluster_error("cluster-leave", &e))),
    }
}

fn handle_cluster_plan(body: &[u8], admin: &dyn ClusterAdmin) -> Result<Frame, RiakError> {
    let _ = DynRpbClusterPlanReq::decode(body)?;
    let pending = admin.cluster_plan_pending();
    let resp = DynRpbClusterPlanResp {
        changes: pending.iter().map(change_to_pb).collect(),
    };
    Ok(Frame::new(
        MessageCode::DynClusterPlanResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

fn handle_cluster_commit(body: &[u8], admin: &dyn ClusterAdmin) -> Result<Frame, RiakError> {
    let _ = DynRpbClusterCommitReq::decode(body)?;
    let staged = admin.cluster_plan_pending();
    let applied = u32::try_from(staged.len()).unwrap_or(u32::MAX);
    match admin.cluster_commit() {
        Ok(()) => {
            let resp = DynRpbClusterCommitResp { applied };
            Ok(Frame::new(
                MessageCode::DynClusterCommitResp.as_u8(),
                resp.encode_to_vec(),
            ))
        }
        Err(e) => Ok(error_frame(format_cluster_error("cluster-commit", &e))),
    }
}

fn handle_aae_status(body: &[u8], aae: &dyn AaeStatusProvider) -> Result<Frame, RiakError> {
    let _ = DynRpbAaeStatusReq::decode(body)?;
    let snap: AaeStatusSnapshot = aae.current_status();
    let resp = DynRpbAaeStatusResp {
        peers: snap
            .peers
            .iter()
            .map(|p| DynRpbAaePeerStatus {
                peer_idx: p.peer_idx,
                dc: p.dc.as_bytes().to_vec(),
                rack: p.rack.as_bytes().to_vec(),
                last_exchange_unix: p.last_exchange_unix,
                divergent_keys_since_last_full_sweep: p.divergent_keys_since_last_full_sweep,
                repair_dispatched_total: p.repair_dispatched_total,
            })
            .collect(),
        snapshot_path: snap.snapshot_path.into_bytes(),
        snapshot_last_save_unix: snap.snapshot_last_save_unix,
        snapshot_last_load_unix: snap.snapshot_last_load_unix,
        snapshot_save_total: snap.snapshot_save_total,
        snapshot_load_total: snap.snapshot_load_total,
        snapshot_corruption_total: snap.snapshot_corruption_total,
        tree_n_time_buckets: snap.tree_n_time_buckets,
        tree_n_segments: snap.tree_n_segments,
        tree_time_window_seconds: snap.tree_time_window_seconds,
        tree_memory_estimate_bytes: snap.tree_memory_estimate_bytes,
    };
    Ok(Frame::new(
        MessageCode::DynAaeStatusResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

fn snapshot_to_pb(snap: &PeerSnapshot) -> DynRpbPeerInfo {
    DynRpbPeerInfo {
        idx: snap.idx,
        dc: snap.dc.as_bytes().to_vec(),
        rack: snap.rack.as_bytes().to_vec(),
        host: snap.host.as_bytes().to_vec(),
        port: u32::from(snap.port),
        tokens: snap
            .tokens
            .iter()
            .map(|t| t.to_string().into_bytes())
            .collect(),
        state: snap.state.name().as_bytes().to_vec(),
        is_local: snap.is_local,
        is_secure: None,
    }
}

fn change_to_pb(change: &ClusterChange) -> DynRpbStagedChange {
    let kind = match change.kind {
        ClusterChangeKind::Add => DYN_STAGED_CHANGE_ADD,
        ClusterChangeKind::Remove => DYN_STAGED_CHANGE_REMOVE,
    };
    let peer = change.peer.as_ref().map(|spec| DynRpbPeerInfo {
        idx: 0,
        dc: spec.dc.as_bytes().to_vec(),
        rack: spec.rack.as_bytes().to_vec(),
        host: spec.host.as_bytes().to_vec(),
        port: u32::from(spec.port),
        tokens: spec
            .tokens
            .iter()
            .map(|t| t.to_string().into_bytes())
            .collect(),
        state: Vec::new(),
        is_local: false,
        is_secure: Some(spec.is_secure),
    });
    DynRpbStagedChange {
        kind,
        peer_idx: change.peer_idx,
        peer,
    }
}

fn format_cluster_error(op: &str, err: &ClusterError) -> String {
    format!("{op}: {err}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynomite::embed::MemoryDatastore;
    use tokio::io::duplex;

    /// Build an object carrying the encoded context `ctx`.
    fn obj_with(value: &[u8], ctx: Vec<u8>) -> HttpObject {
        HttpObject {
            value: value.to_vec(),
            context: ctx,
            ..HttpObject::default()
        }
    }

    #[test]
    fn resolve_write_supersedes_a_causally_older_value() {
        use crate::proto::http::object::SiblingSet;
        let c1 = advance_context(&[], b"n1");
        let stored = SiblingSet::single(obj_with(b"v1", c1.clone()));
        let c2 = advance_context(&c1, b"n1");
        let out = resolve_write(&stored, &obj_with(b"v2", c2), false);
        assert_eq!(out.siblings.len(), 1, "newer write supersedes older");
        assert_eq!(out.siblings[0].value, b"v2");
    }

    #[test]
    fn resolve_write_ignores_a_stale_late_write() {
        use crate::proto::http::object::SiblingSet;
        let c1 = advance_context(&[], b"n1");
        let c2 = advance_context(&c1, b"n1");
        let stored = SiblingSet::single(obj_with(b"v2", c2));
        // A late write still carrying the OLD context c1 is causally
        // behind the stored c2 and must not overwrite it.
        let out = resolve_write(&stored, &obj_with(b"stale", c1), false);
        assert_eq!(out.siblings.len(), 1);
        assert_eq!(out.siblings[0].value, b"v2", "stale write is dropped");
    }

    #[test]
    fn resolve_write_retains_concurrent_siblings_under_allow_mult() {
        use crate::proto::http::object::SiblingSet;
        let a = advance_context(&[], b"n1");
        let b = advance_context(&[], b"n2");
        let stored = SiblingSet::single(obj_with(b"a", a));
        let out = resolve_write(&stored, &obj_with(b"b", b), true);
        assert_eq!(
            out.siblings.len(),
            2,
            "concurrent writes are both retained under allow_mult"
        );
        let vals: std::collections::BTreeSet<&[u8]> =
            out.siblings.iter().map(|o| o.value.as_slice()).collect();
        assert!(vals.contains(b"a".as_slice()) && vals.contains(b"b".as_slice()));
    }

    #[test]
    fn resolve_write_collapses_concurrent_without_allow_mult() {
        use crate::proto::http::object::SiblingSet;
        let a = advance_context(&[], b"n1");
        let b = advance_context(&[], b"n2");
        let stored = SiblingSet::single(obj_with(b"a", a));
        let out = resolve_write(&stored, &obj_with(b"b", b), false);
        assert_eq!(
            out.siblings.len(),
            1,
            "without allow_mult a concurrent write collapses to one value"
        );
    }

    #[tokio::test]
    async fn ping_round_trips_over_duplex() {
        let (client, server) = duplex(4096);
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let server_task = tokio::spawn(handle_conn(server, ds));

        let (mut client_r, mut client_w) = tokio::io::split(client);
        write_frame(
            &mut client_w,
            &Frame::new(MessageCode::PingReq.as_u8(), Vec::new()),
        )
        .await
        .unwrap();
        let resp = read_frame(&mut client_r).await.unwrap();
        assert_eq!(resp.code, MessageCode::PingResp.as_u8());
        assert!(resp.body.is_empty());

        // Closing the client side drives the server to a clean exit.
        drop(client_r);
        drop(client_w);
        // The server task observes EOF and returns Ok.
        let _ = server_task.await.unwrap();
    }

    #[tokio::test]
    async fn unknown_code_surfaces_error_to_caller() {
        // Code 99 is unused.
        let frame = Frame::new(99, Vec::new());
        let ds = MemoryDatastore::new();
        let Err(err) =
            process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider).await
        else {
            panic!("expected error for unknown code");
        };
        assert!(matches!(err, RiakError::UnknownMessageCode(99)));
    }

    /// Drain a [`FrameStream`] into a `Vec<Frame>` for tests.
    async fn collect_frames(mut s: FrameStream) -> Vec<Frame> {
        let mut out = Vec::new();
        while let Some(item) = s.next().await {
            out.push(item.expect("stream item"));
        }
        out
    }

    #[tokio::test]
    async fn response_codes_inbound_yield_error_resp() {
        // GetResp inbound is illegal but must not crash the server;
        // we reply with an RpbErrorResp instead.
        let frame = Frame::new(MessageCode::GetResp.as_u8(), Vec::new());
        let ds = MemoryDatastore::new();
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].code, MessageCode::ErrorResp.as_u8());
        let parsed = RpbErrorResp::decode(frames[0].body.as_slice()).expect("decode");
        assert!(!parsed.errmsg.is_empty());
    }

    #[tokio::test]
    async fn malformed_body_reports_decode_error() {
        // GetReq with a truncated length-delimited string field.
        let frame = Frame::new(MessageCode::GetReq.as_u8(), vec![0x0a, 0xff]);
        let ds = MemoryDatastore::new();
        let Err(err) =
            process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider).await
        else {
            panic!("expected decode error");
        };
        assert!(matches!(err, RiakError::Decode(_)));
    }

    #[tokio::test]
    async fn datastore_dispatch_is_invoked_for_kv_ops() {
        let ds = Arc::new(MemoryDatastore::new());
        let frame = Frame::new(
            MessageCode::PutReq.as_u8(),
            RpbPutReq {
                bucket: b"b".to_vec(),
                key: Some(b"k".to_vec()),
                content: Some(RpbContent {
                    value: b"v".to_vec(),
                    ..RpbContent::default()
                }),
                ..RpbPutReq::default()
            }
            .encode_to_vec(),
        );
        let _ = process_frame(
            &frame,
            ds.as_ref(),
            &NoopClusterAdmin,
            None,
            &NoopAaeStatusProvider,
        )
        .await
        .expect("ok");
        assert_eq!(ds.dispatch_count(), 1);
    }

    // ---- streaming list ----

    #[tokio::test]
    async fn list_buckets_empty_yields_one_terminator_frame() {
        let ds = MemoryDatastore::new();
        let frame = Frame::new(
            MessageCode::ListBucketsReq.as_u8(),
            RpbListBucketsReq::default().encode_to_vec(),
        );
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].code, MessageCode::ListBucketsResp.as_u8());
        let resp = RpbListBucketsResp::decode(frames[0].body.as_slice()).expect("decode");
        assert_eq!(resp.done, Some(true));
        assert!(resp.buckets.is_empty());
    }

    #[tokio::test]
    async fn list_keys_chunks_at_chunk_size() {
        let ds = MemoryDatastore::new();
        // 1000 keys -> 3 full chunks (256) + 1 partial (232) + 1 terminator = 5 frames.
        for i in 0..1000u16 {
            ds.insert(b"u", format!("k{i:04}").as_bytes());
        }
        let frame = Frame::new(
            MessageCode::ListKeysReq.as_u8(),
            RpbListKeysReq {
                bucket: b"u".to_vec(),
                ..RpbListKeysReq::default()
            }
            .encode_to_vec(),
        );
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        assert_eq!(frames.len(), 5, "expected 4 chunks plus a terminator");
        let mut total_keys = 0usize;
        for (i, f) in frames.iter().enumerate() {
            assert_eq!(f.code, MessageCode::ListKeysResp.as_u8());
            let resp = RpbListKeysResp::decode(f.body.as_slice()).expect("decode");
            if i == frames.len() - 1 {
                assert_eq!(resp.done, Some(true), "final frame must carry done=true");
                assert!(resp.keys.is_empty(), "terminator carries no keys");
            } else {
                assert!(
                    resp.done == Some(false) || resp.done.is_none(),
                    "non-terminator frame must not carry done=true"
                );
                total_keys += resp.keys.len();
                if i < 3 {
                    assert_eq!(resp.keys.len(), LIST_CHUNK_SIZE);
                } else {
                    assert_eq!(resp.keys.len(), 1000 - 3 * LIST_CHUNK_SIZE);
                }
            }
        }
        assert_eq!(total_keys, 1000);
    }

    #[tokio::test]
    async fn list_buckets_streams_multiple_buckets() {
        let ds = MemoryDatastore::new();
        for i in 0..512u16 {
            ds.insert(format!("b{i:04}").as_bytes(), b"k");
        }
        let frame = Frame::new(
            MessageCode::ListBucketsReq.as_u8(),
            RpbListBucketsReq::default().encode_to_vec(),
        );
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        // 512 / 256 = 2 full chunks, then one empty terminator.
        assert_eq!(frames.len(), 3);
        let last = RpbListBucketsResp::decode(frames[2].body.as_slice()).expect("decode");
        assert_eq!(last.done, Some(true));
        assert!(last.buckets.is_empty());
    }

    #[tokio::test]
    async fn list_keys_against_unsupported_datastore_yields_error_frame() {
        // A datastore that does not override the streaming methods
        // gets the default body which yields a single Unsupported
        // error.
        struct Noop;
        impl Datastore for Noop {
            fn protocol(&self) -> dynomite::embed::hooks::Protocol {
                dynomite::embed::hooks::Protocol::Custom
            }
            fn dispatch(
                &self,
                req: Msg,
            ) -> dynomite::embed::hooks::BoxFuture<
                '_,
                Result<Msg, dynomite::embed::hooks::DatastoreError>,
            > {
                Box::pin(async move {
                    let mut rsp = Msg::new(req.id(), MsgType::Unknown, false);
                    rsp.set_parent_id(req.id());
                    Ok(rsp)
                })
            }
        }
        let ds = Noop;
        let frame = Frame::new(
            MessageCode::ListKeysReq.as_u8(),
            RpbListKeysReq {
                bucket: b"u".to_vec(),
                ..RpbListKeysReq::default()
            }
            .encode_to_vec(),
        );
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].code, MessageCode::ErrorResp.as_u8());
        let resp = RpbErrorResp::decode(frames[0].body.as_slice()).expect("decode");
        assert!(
            resp.errmsg
                .windows(b"unsupported".len())
                .any(|w| w == b"unsupported"),
            "errmsg should mention the unsupported variant: {:?}",
            String::from_utf8_lossy(&resp.errmsg)
        );
    }

    // ---- streaming 2i (secondary index) ----

    /// Datastore that returns a configurable Vec<Vec<u8>> from
    /// `riak_index_eq`. Used to verify the chunk-size streaming
    /// behaviour without relying on a real index walker.
    struct ScriptedIndexStore {
        keys: Vec<Vec<u8>>,
    }

    impl Datastore for ScriptedIndexStore {
        fn protocol(&self) -> dynomite::embed::hooks::Protocol {
            dynomite::embed::hooks::Protocol::Custom
        }
        fn dispatch(
            &self,
            req: Msg,
        ) -> dynomite::embed::hooks::BoxFuture<
            '_,
            Result<Msg, dynomite::embed::hooks::DatastoreError>,
        > {
            Box::pin(async move {
                let mut rsp = Msg::new(req.id(), MsgType::Unknown, false);
                rsp.set_parent_id(req.id());
                Ok(rsp)
            })
        }
        fn riak_index_eq<'a>(
            &'a self,
            _bucket: &'a [u8],
            _index_name: &'a [u8],
            _value: &'a [u8],
        ) -> dynomite::embed::hooks::BoxFuture<
            'a,
            Result<Vec<Vec<u8>>, dynomite::embed::hooks::DatastoreError>,
        > {
            let keys = self.keys.clone();
            Box::pin(async move { Ok(keys) })
        }
        fn riak_index_range<'a>(
            &'a self,
            _bucket: &'a [u8],
            _index_name: &'a [u8],
            _min: &'a [u8],
            _max: &'a [u8],
        ) -> dynomite::embed::hooks::BoxFuture<
            'a,
            Result<Vec<Vec<u8>>, dynomite::embed::hooks::DatastoreError>,
        > {
            let keys = self.keys.clone();
            Box::pin(async move { Ok(keys) })
        }
    }

    #[tokio::test]
    async fn index_eq_streams_chunks_of_chunk_size() {
        // 1000 keys -> 3 full 256-key chunks + 1 partial (232) + 1
        // terminator = 5 frames, mirroring the list-keys layout.
        let mut keys = Vec::new();
        for i in 0..1000u16 {
            keys.push(format!("k{i:04}").as_bytes().to_vec());
        }
        let ds = ScriptedIndexStore { keys };
        let frame = Frame::new(
            MessageCode::IndexReq.as_u8(),
            RpbIndexReq {
                bucket: b"u".to_vec(),
                index: b"age_int".to_vec(),
                qtype: INDEX_QUERY_TYPE_EQ,
                key: Some(b"42".to_vec()),
                ..RpbIndexReq::default()
            }
            .encode_to_vec(),
        );
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        assert_eq!(frames.len(), 5, "4 chunks plus a terminator");
        let mut total = 0usize;
        for (i, f) in frames.iter().enumerate() {
            assert_eq!(f.code, MessageCode::IndexResp.as_u8());
            let resp = RpbIndexResp::decode(f.body.as_slice()).expect("decode");
            if i == frames.len() - 1 {
                assert_eq!(resp.done, Some(true));
                assert!(resp.keys.is_empty());
            } else {
                assert_eq!(
                    resp.done,
                    Some(false),
                    "non-terminator frames carry done=false"
                );
                total += resp.keys.len();
                if i < 3 {
                    assert_eq!(resp.keys.len(), LIST_CHUNK_SIZE);
                } else {
                    assert_eq!(resp.keys.len(), 1000 - 3 * LIST_CHUNK_SIZE);
                }
            }
        }
        assert_eq!(total, 1000);
    }

    #[tokio::test]
    async fn index_eq_first_frame_carries_partial_keys_for_old_clients() {
        // Backwards compat: a client that ignores `done` and reads
        // exactly one frame still observes a usable (partial)
        // result set.
        let mut keys = Vec::new();
        for i in 0..600u16 {
            keys.push(format!("k{i:04}").as_bytes().to_vec());
        }
        let ds = ScriptedIndexStore { keys };
        let frame = Frame::new(
            MessageCode::IndexReq.as_u8(),
            RpbIndexReq {
                bucket: b"u".to_vec(),
                index: b"x".to_vec(),
                qtype: INDEX_QUERY_TYPE_EQ,
                key: Some(b"v".to_vec()),
                ..RpbIndexReq::default()
            }
            .encode_to_vec(),
        );
        let mut stream =
            process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
                .await
                .expect("ok");
        let first = stream.next().await.expect("first").expect("frame");
        assert_eq!(first.code, MessageCode::IndexResp.as_u8());
        let parsed = RpbIndexResp::decode(first.body.as_slice()).expect("decode");
        assert_eq!(parsed.keys.len(), LIST_CHUNK_SIZE);
        assert_eq!(parsed.done, Some(false));
    }

    #[tokio::test]
    async fn index_empty_yields_single_terminator() {
        let ds = ScriptedIndexStore { keys: Vec::new() };
        let frame = Frame::new(
            MessageCode::IndexReq.as_u8(),
            RpbIndexReq {
                bucket: b"u".to_vec(),
                index: b"x".to_vec(),
                qtype: INDEX_QUERY_TYPE_EQ,
                key: Some(b"v".to_vec()),
                ..RpbIndexReq::default()
            }
            .encode_to_vec(),
        );
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        assert_eq!(frames.len(), 1);
        let resp = RpbIndexResp::decode(frames[0].body.as_slice()).expect("decode");
        assert_eq!(resp.done, Some(true));
        assert!(resp.keys.is_empty());
    }

    #[tokio::test]
    async fn index_eq_max_results_caps_total_streamed_keys() {
        // 1000 raw keys, max_results=300 -> should observe 300
        // total keys across the stream (chunk + partial chunk +
        // terminator).
        let mut keys = Vec::new();
        for i in 0..1000u16 {
            keys.push(format!("k{i:04}").as_bytes().to_vec());
        }
        let ds = ScriptedIndexStore { keys };
        let frame = Frame::new(
            MessageCode::IndexReq.as_u8(),
            RpbIndexReq {
                bucket: b"u".to_vec(),
                index: b"x".to_vec(),
                qtype: INDEX_QUERY_TYPE_EQ,
                key: Some(b"v".to_vec()),
                max_results: Some(300),
                ..RpbIndexReq::default()
            }
            .encode_to_vec(),
        );
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        let mut total = 0usize;
        for f in &frames {
            let resp = RpbIndexResp::decode(f.body.as_slice()).expect("decode");
            total += resp.keys.len();
        }
        assert_eq!(total, 300);
        let last = frames.last().expect("last");
        let last_resp = RpbIndexResp::decode(last.body.as_slice()).expect("decode");
        assert_eq!(last_resp.done, Some(true));
    }

    #[tokio::test]
    async fn index_unsupported_datastore_yields_error_frame() {
        // The Noop store does not override riak_index_*; the
        // default returns Unsupported, which surfaces as a single
        // RpbErrorResp frame.
        struct Noop;
        impl Datastore for Noop {
            fn protocol(&self) -> dynomite::embed::hooks::Protocol {
                dynomite::embed::hooks::Protocol::Custom
            }
            fn dispatch(
                &self,
                req: Msg,
            ) -> dynomite::embed::hooks::BoxFuture<
                '_,
                Result<Msg, dynomite::embed::hooks::DatastoreError>,
            > {
                Box::pin(async move {
                    let mut rsp = Msg::new(req.id(), MsgType::Unknown, false);
                    rsp.set_parent_id(req.id());
                    Ok(rsp)
                })
            }
        }
        let ds = Noop;
        let frame = Frame::new(
            MessageCode::IndexReq.as_u8(),
            RpbIndexReq {
                bucket: b"u".to_vec(),
                index: b"x".to_vec(),
                qtype: INDEX_QUERY_TYPE_EQ,
                key: Some(b"v".to_vec()),
                ..RpbIndexReq::default()
            }
            .encode_to_vec(),
        );
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].code, MessageCode::ErrorResp.as_u8());
    }

    // ---- streaming map-reduce ----

    /// Build the JSON body for a `RpbMapRedReq` describing a
    /// two-phase map+reduce job over inline `KeyData` inputs.
    fn mapred_req_two_phase(values: &[i64]) -> Vec<u8> {
        let inputs: Vec<serde_json::Value> = values
            .iter()
            .enumerate()
            .map(|(i, v)| {
                serde_json::json!({
                    "bucket": "b",
                    "key": format!("k{i}"),
                    "value": *v,
                })
            })
            .collect();
        let job = serde_json::json!({
            "inputs": inputs,
            "query": [
                { "map": { "language": "erlang",
                           "name": "map_object_value",
                           "keep": true } },
                { "reduce": { "language": "erlang",
                              "name": "reduce_sum",
                              "keep": true } },
            ]
        });
        let req = RpbMapRedReq {
            request: serde_json::to_vec(&job).expect("job json"),
            content_type: b"application/json".to_vec(),
        };
        req.encode_to_vec()
    }

    #[tokio::test]
    async fn process_frame_streams_mapreduce_response_with_per_phase_frames() {
        // Two kept phases (map + reduce) over three inputs. The
        // streaming handler must emit one body-carrying response
        // per phase batch, all with `done = false`, then a
        // terminator with `done = true`.
        let body = mapred_req_two_phase(&[1, 2, 3]);
        let frame = Frame::new(MessageCode::MapRedReq.as_u8(), body);
        let ds = MemoryDatastore::new();
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        assert_eq!(
            frames.len(),
            3,
            "expected two per-phase frames plus one terminator",
        );
        for f in &frames {
            assert_eq!(f.code, MessageCode::MapRedResp.as_u8());
        }
        let p0 = RpbMapRedResp::decode(frames[0].body.as_slice()).expect("decode 0");
        assert_eq!(p0.phase, Some(0));
        assert_eq!(p0.done, Some(false));
        let p0_body = p0.response.as_ref().expect("phase 0 body");
        let p0_json: serde_json::Value = serde_json::from_slice(p0_body).expect("phase 0 json");
        assert_eq!(p0_json[0]["phase"], 0);
        assert_eq!(p0_json[0]["data"].as_array().unwrap().len(), 3);

        let p1 = RpbMapRedResp::decode(frames[1].body.as_slice()).expect("decode 1");
        assert_eq!(p1.phase, Some(1));
        assert_eq!(p1.done, Some(false));
        let p1_body = p1.response.as_ref().expect("phase 1 body");
        let p1_json: serde_json::Value = serde_json::from_slice(p1_body).expect("phase 1 json");
        assert_eq!(p1_json[0]["phase"], 1);
        assert_eq!(p1_json[0]["data"], serde_json::json!([6]));
    }

    #[tokio::test]
    async fn process_frame_emits_terminator_frame_with_done_true() {
        let body = mapred_req_two_phase(&[10, 20]);
        let frame = Frame::new(MessageCode::MapRedReq.as_u8(), body);
        let ds = MemoryDatastore::new();
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        let term = frames.last().expect("at least one frame");
        assert_eq!(term.code, MessageCode::MapRedResp.as_u8());
        let parsed = RpbMapRedResp::decode(term.body.as_slice()).expect("decode terminator");
        assert_eq!(parsed.done, Some(true));
        assert_eq!(parsed.phase, None);
        assert!(parsed.response.is_none());
    }

    #[tokio::test]
    async fn mapreduce_first_frame_is_a_partial_phase_zero_answer() {
        // Backwards-compatibility check: a one-frame consumer that
        // reads only the first response observes phase-0 data.
        // This mirrors the legacy single-frame contract and lets a
        // pre-streaming PBC client continue to extract a useful
        // result.
        let body = mapred_req_two_phase(&[5, 6, 7, 8]);
        let frame = Frame::new(MessageCode::MapRedReq.as_u8(), body);
        let ds = MemoryDatastore::new();
        let mut stream =
            process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
                .await
                .expect("ok");
        let first = stream.next().await.expect("first").expect("frame");
        assert_eq!(first.code, MessageCode::MapRedResp.as_u8());
        let parsed = RpbMapRedResp::decode(first.body.as_slice()).expect("decode");
        assert_eq!(parsed.phase, Some(0));
        assert_eq!(parsed.done, Some(false));
        let body = parsed.response.expect("first frame carries body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(json[0]["phase"], 0);
        assert_eq!(json[0]["data"].as_array().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn mapreduce_unknown_function_emits_single_error_frame() {
        let job = serde_json::json!({
            "inputs": [{"bucket": "b", "key": "k"}],
            "query": [
                { "map": { "language": "erlang",
                           "name": "no_such_function",
                           "keep": true } }
            ]
        });
        let req = RpbMapRedReq {
            request: serde_json::to_vec(&job).expect("job json"),
            content_type: b"application/json".to_vec(),
        };
        let frame = Frame::new(MessageCode::MapRedReq.as_u8(), req.encode_to_vec());
        let ds = MemoryDatastore::new();
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        // Error stream: one error frame, no terminator.
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].code, MessageCode::ErrorResp.as_u8());
        let parsed = RpbErrorResp::decode(frames[0].body.as_slice()).expect("decode");
        let msg = String::from_utf8_lossy(&parsed.errmsg);
        assert!(
            msg.contains("no_such_function") || msg.contains("unknown"),
            "errmsg: {msg}",
        );
    }

    #[tokio::test]
    async fn mapreduce_unsupported_content_type_yields_error_frame() {
        let req = RpbMapRedReq {
            request: b"<xml/>".to_vec(),
            content_type: b"application/xml".to_vec(),
        };
        let frame = Frame::new(MessageCode::MapRedReq.as_u8(), req.encode_to_vec());
        let ds = MemoryDatastore::new();
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].code, MessageCode::ErrorResp.as_u8());
    }
    #[tokio::test]
    async fn aae_status_default_provider_returns_empty_snapshot() {
        let ds = MemoryDatastore::new();
        let frame = Frame::new(
            MessageCode::DynAaeStatusReq.as_u8(),
            DynRpbAaeStatusReq::default().encode_to_vec(),
        );
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &NoopAaeStatusProvider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].code, MessageCode::DynAaeStatusResp.as_u8());
        let resp = DynRpbAaeStatusResp::decode(frames[0].body.as_slice()).expect("decode");
        assert!(resp.peers.is_empty());
        assert_eq!(resp.snapshot_save_total, 0);
    }

    #[tokio::test]
    async fn aae_status_custom_provider_returns_live_snapshot() {
        struct Provider;
        impl crate::aae::status::AaeStatusProvider for Provider {
            fn current_status(&self) -> crate::aae::status::AaeStatusSnapshot {
                crate::aae::status::AaeStatusSnapshot {
                    peers: vec![crate::aae::status::AaePeerStatus {
                        peer_idx: 7,
                        dc: "dc1".into(),
                        rack: "rA".into(),
                        last_exchange_unix: 1_700_000_000,
                        divergent_keys_since_last_full_sweep: 4,
                        repair_dispatched_total: 3,
                    }],
                    snapshot_path: "/var/lib/dynomite/aae/tree.snapshot".into(),
                    snapshot_last_save_unix: 1_700_000_300,
                    snapshot_last_load_unix: 1_700_000_100,
                    snapshot_save_total: 5,
                    snapshot_load_total: 1,
                    snapshot_corruption_total: 0,
                    tree_n_time_buckets: 24,
                    tree_n_segments: 1024,
                    tree_time_window_seconds: 3600,
                    tree_memory_estimate_bytes: 8192,
                }
            }
        }
        let ds = MemoryDatastore::new();
        let frame = Frame::new(
            MessageCode::DynAaeStatusReq.as_u8(),
            DynRpbAaeStatusReq::default().encode_to_vec(),
        );
        let stream = process_frame(&frame, &ds, &NoopClusterAdmin, None, &Provider)
            .await
            .expect("ok");
        let frames = collect_frames(stream).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].code, MessageCode::DynAaeStatusResp.as_u8());
        let resp = DynRpbAaeStatusResp::decode(frames[0].body.as_slice()).expect("decode");
        assert_eq!(resp.peers.len(), 1);
        assert_eq!(resp.peers[0].peer_idx, 7);
        assert_eq!(resp.peers[0].dc, b"dc1".to_vec());
        assert_eq!(resp.snapshot_save_total, 5);
        assert_eq!(resp.tree_n_time_buckets, 24);
        assert_eq!(
            resp.snapshot_path,
            b"/var/lib/dynomite/aae/tree.snapshot".to_vec()
        );
    }
}
