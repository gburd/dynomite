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
    let (mut reader, mut writer) = tokio::io::split(stream);
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
fn pbc_content_from_storage(stored: &[u8]) -> RpbContent {
    match HttpObject::from_storage_bytes(stored) {
        Ok(obj) => RpbContent {
            value: obj.value,
            content_type: obj.content_type.map(String::into_bytes),
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
        },
        Err(_) => RpbContent {
            value: stored.to_vec(),
            ..RpbContent::default()
        },
    }
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

async fn handle_get(
    body: &[u8],
    datastore: &dyn Datastore,
    hooks: Option<&RoutingHooks>,
) -> Result<Frame, RiakError> {
    let req = RpbGetReq::decode(body)?;
    if let Some(hooks) = hooks {
        let bucket_type = req.r#type.as_deref().unwrap_or(b"");
        let decision = match hooks.router.try_route(bucket_type, &req.bucket, &req.key) {
            Ok(d) => d,
            Err(e) => return Ok(error_frame(format!("riak get: {e}"))),
        };
        for replica in decision.replica_list() {
            hooks
                .outbound
                .dispatch(
                    replica.peer_idx,
                    PeerOp::Get {
                        bucket_type: decision.bucket_type.clone(),
                        bucket: req.bucket.clone(),
                        key: req.key.clone(),
                    },
                )
                .await;
        }
    }
    // Trampoline through the substrate so dispatch counts tick.
    let routing = Msg::new(0, MsgType::Unknown, true);
    datastore.dispatch(routing).await?;
    // For datastores that implement the Riak K/V layer (today:
    // `NoxuDatastore`), fetch the object and emit it as a
    // single-content `RpbGetResp`. The `RpbContent` carries the
    // object value, its content-type, its 2i entries, and its links
    // (mapped from the stored `HttpObject`), so a PBC client reads
    // links natively. Datastores that report `Unsupported` fall back
    // to the empty response so the `MemoryDatastore` trampoline
    // continues to behave identically.
    let resp = match datastore.riak_get(&req.bucket, &req.key).await {
        Ok(Some(v)) => RpbGetResp {
            content: vec![pbc_content_from_storage(&v)],
            ..RpbGetResp::default()
        },
        Ok(None) | Err(DatastoreError::Unsupported(_)) => RpbGetResp::default(),
        Err(e) => {
            return Ok(error_frame(format!("riak get: {e}")));
        }
    };
    Ok(Frame::new(
        MessageCode::GetResp.as_u8(),
        resp.encode_to_vec(),
    ))
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
    if let Some(hooks) = hooks {
        let bucket_type = req.r#type.as_deref().unwrap_or(b"");
        let decision = match hooks.router.try_route(bucket_type, &req.bucket, &key) {
            Ok(d) => d,
            Err(e) => return Ok(error_frame(format!("riak put: {e}"))),
        };
        for replica in decision.replica_list() {
            hooks
                .outbound
                .dispatch(
                    replica.peer_idx,
                    PeerOp::Put {
                        bucket_type: decision.bucket_type.clone(),
                        bucket: req.bucket.clone(),
                        key: key.clone(),
                        value: content.value.clone(),
                    },
                )
                .await;
        }
    }
    let indexes: Vec<(Vec<u8>, Vec<u8>)> = content
        .indexes
        .iter()
        .filter_map(|p| p.value.as_ref().map(|v| (p.key.clone(), v.clone())))
        .collect();
    // Persist the PBC value in the same canonical `HttpObject`
    // storage form the HTTP gateway writes, so a put over one
    // transport is readable over the other. The links carried in
    // `RpbContent.links` are mapped onto `HttpObject.links`, so a
    // PBC put attaches links natively and an HTTP read re-emits them
    // as `Link:` headers. Index pairs and the content-type are
    // mirrored onto the envelope so an HTTP read echoes them, matching
    // the HTTP put path.
    let envelope = HttpObject {
        value: content.value.clone(),
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
    };
    let storage = envelope.to_storage_bytes();
    match datastore
        .riak_put(&req.bucket, &key, &storage, &indexes)
        .await
    {
        Ok(()) | Err(DatastoreError::Unsupported(_)) => Ok(Frame::new(
            MessageCode::PutResp.as_u8(),
            RpbPutResp::default().encode_to_vec(),
        )),
        Err(e) => Ok(error_frame(format!("riak put: {e}"))),
    }
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
            "riak dt_update: unsupported or empty op (counter/set only)".into(),
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
    use crate::datatypes::{TAG_COUNTER, TAG_SET};
    use crate::proto::pb::{DtFetchReq, DtFetchResp, DtValue, DATA_TYPE_COUNTER, DATA_TYPE_SET};

    let req = DtFetchReq::decode(body)?;
    // The bucket type selects the projection: `sets` -> OR-set,
    // anything else -> counter (Riak's `counters` default).
    let (tag, dtype) = if req.r#type == b"sets" {
        (TAG_SET, DATA_TYPE_SET)
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
    match value {
        CrdtValue::Counter(n) => {
            resp.value = Some(DtValue {
                counter_value: Some(n),
                ..DtValue::default()
            });
        }
        CrdtValue::Set(elems) => {
            resp.value = Some(DtValue {
                set_value: elems,
                ..DtValue::default()
            });
        }
        CrdtValue::Missing => {}
    }
    Ok(Frame::new(
        MessageCode::DtFetchResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

/// Translate a PBC [`DtOp`] into the internal
/// [`crate::crdt_store::CrdtOp`], attributing it to `actor`. Returns
/// `None` for an empty or unsupported op (only counter and set are
/// wired).
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
    None
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
            allow_mult: Some(false),
            last_write_wins: Some(false),
            chash_keyfun: Some(resolved.effective_keyfun().to_wire()),
            chash_keyfun_module: resolved
                .effective_keyfun()
                .custom_module()
                .map(|s| s.as_bytes().to_vec()),
            replication_strategy: Some(resolved.effective_strategy().to_wire()),
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
