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
//! Redis/Memcached/custom protocols; it does not yet carry Riak-
//! specific bucket/key semantics. For the v0.0.1 slice the handler
//! synthesizes a routing [`dynomite::msg::Msg`] per request and
//! interprets the dispatch result as "the substrate accepted this
//! request". A follow-up slice introduces a richer Riak K/V trait
//! that the server uses end-to-end.
//!
//! In practice this means:
//!
//! * `RpbPing` -- never reaches the datastore; replied with
//!   `RpbPingResp` directly.
//! * `RpbGetReq` -- routed through [`dynomite::embed::Datastore::dispatch`]
//!   so the substrate's accounting fires; reply is an empty
//!   `RpbGetResp` (zero content, no vclock) until the K/V trait
//!   lands.
//! * `RpbPutReq` -- same shape; reply is an empty `RpbPutResp`
//!   (no echoed body, no server-assigned key).
//! * `RpbDelReq` -- same shape; reply is the body-less
//!   `RpbDelResp` frame.
//! * `RpbServerInfoReq` -- replied with the crate name and version
//!   directly; no datastore interaction.
//! * `RpbGetBucketReq` -- replied with a default [`RpbBucketProps`]
//!   carrying conservative defaults (`n_val = 3`, `allow_mult =
//!   false`). Per-bucket persistence is deferred to the follow-up
//!   slice that wires bucket-property storage into the substrate.
//! * `RpbSetBucketReq` -- accepted and acknowledged with an empty
//!   `RpbSetBucketResp`. The supplied properties are not yet
//!   persisted; same follow-up as above.
//! * `RpbListBucketsReq`, `RpbListKeysReq`, `RpbIndexReq` --
//!   replied with [`RpbErrorResp`] carrying a `"not implemented for
//!   this datastore"` message. The follow-up slice that lands the
//!   richer K/V trait wires these against the `NoxuDatastore`
//!   storage engine.

use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use futures_util::StreamExt;
use prost::Message as _;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use dynomite::embed::hooks::{DatastoreByteStream, DatastoreError};
use dynomite::embed::Datastore;
use dynomite::msg::{Msg, MsgType};

use crate::error::RiakError;
use crate::proto::pb::framer::{read_frame, write_frame, Frame};
use crate::proto::pb::mapreduce::{RpbMapRedReq, RpbMapRedResp};
use crate::proto::pb::messages::{
    MessageCode, RpbBucketProps, RpbDelReq, RpbErrorResp, RpbGetBucketReq, RpbGetBucketResp,
    RpbGetReq, RpbGetResp, RpbGetServerInfoResp, RpbIndexReq, RpbIndexResp, RpbListBucketsReq,
    RpbListBucketsResp, RpbListKeysReq, RpbListKeysResp, RpbPingReq, RpbPingResp, RpbPutReq,
    RpbPutResp, RpbServerInfoReq, RpbSetBucketReq, RpbSetBucketResp, INDEX_QUERY_TYPE_EQ,
    INDEX_QUERY_TYPE_RANGE,
};

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
/// use dyn_riak::serve_pbc;
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
    serve_pbc_inner(listener, datastore, None).await
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
/// use dyn_riak::serve_pbc_tls;
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
    serve_pbc_inner(listener, datastore, Some(acceptor)).await
}

async fn serve_pbc_inner(
    listener: TcpListener,
    datastore: Arc<dyn Datastore>,
    acceptor: Option<TlsAcceptor>,
) -> Result<(), RiakError> {
    loop {
        let (sock, peer) = listener.accept().await?;
        let datastore = Arc::clone(&datastore);
        match acceptor.as_ref() {
            Some(acc) => {
                let acc = acc.clone();
                tokio::spawn(async move {
                    match acc.accept(sock).await {
                        Ok(tls) => {
                            if let Err(e) = handle_conn(tls, datastore).await {
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
                    if let Err(e) = handle_conn(sock, datastore).await {
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
    let (mut reader, mut writer) = tokio::io::split(stream);
    loop {
        let frame = match read_frame(&mut reader).await {
            Ok(f) => f,
            Err(RiakError::UnexpectedEof { .. }) => {
                // Peer closed at (or near) a frame boundary. We do
                // not distinguish a clean close from a mid-frame
                // close: in either case the right thing for a PBC
                // listener is to drop the connection silently.
                return Ok(());
            }
            Err(other) => return Err(other),
        };

        let mut response = process_frame(&frame, datastore.as_ref()).await?;
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
async fn process_frame(frame: &Frame, datastore: &dyn Datastore) -> Result<FrameStream, RiakError> {
    let code = MessageCode::from_u8(frame.code).map_err(RiakError::UnknownMessageCode)?;
    let stream: FrameStream = match code {
        MessageCode::PingReq => single_frame(handle_ping(&frame.body)?),
        MessageCode::ServerInfoReq => single_frame(handle_server_info(&frame.body)?),
        MessageCode::GetReq => single_frame(handle_get(&frame.body, datastore).await?),
        MessageCode::PutReq => single_frame(handle_put(&frame.body, datastore).await?),
        MessageCode::DelReq => single_frame(handle_del(&frame.body, datastore).await?),
        MessageCode::GetBucketReq => single_frame(handle_get_bucket(&frame.body)?),
        MessageCode::SetBucketReq => single_frame(handle_set_bucket(&frame.body)?),
        MessageCode::ListBucketsReq => handle_list_buckets(&frame.body, datastore)?,
        MessageCode::ListKeysReq => handle_list_keys(&frame.body, datastore)?,
        MessageCode::IndexReq => single_frame(handle_index(&frame.body, datastore).await?),
        MessageCode::MapRedReq => single_frame(handle_mapred(&frame.body).await?),
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
        | MessageCode::IndexResp
        | MessageCode::MapRedResp => {
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
        node: Some(b"dyn-riak".to_vec()),
        server_version: Some(format!("dyn-riak {}", env!("CARGO_PKG_VERSION")).into_bytes()),
    };
    Ok(Frame::new(
        MessageCode::GetServerInfoResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

async fn handle_get(body: &[u8], datastore: &dyn Datastore) -> Result<Frame, RiakError> {
    let req = RpbGetReq::decode(body)?;
    // Trampoline through the substrate so dispatch counts tick.
    let routing = Msg::new(0, MsgType::Unknown, true);
    datastore.dispatch(routing).await?;
    // For datastores that implement the Riak K/V layer (today:
    // `NoxuDatastore`), fetch the object and emit it as a
    // single-content `RpbGetResp`. Datastores that report
    // `Unsupported` fall back to the legacy empty response so
    // the `MemoryDatastore` trampoline continues to behave
    // identically.
    let resp = match datastore.riak_get(&req.bucket, &req.key).await {
        Ok(Some(v)) => RpbGetResp {
            content: vec![v],
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

async fn handle_put(body: &[u8], datastore: &dyn Datastore) -> Result<Frame, RiakError> {
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
    let indexes: Vec<(Vec<u8>, Vec<u8>)> = req
        .indexes
        .iter()
        .filter_map(|p| p.value.as_ref().map(|v| (p.key.clone(), v.clone())))
        .collect();
    match datastore
        .riak_put(&req.bucket, &key, &req.value, &indexes)
        .await
    {
        Ok(()) | Err(DatastoreError::Unsupported(_)) => Ok(Frame::new(
            MessageCode::PutResp.as_u8(),
            RpbPutResp::default().encode_to_vec(),
        )),
        Err(e) => Ok(error_frame(format!("riak put: {e}"))),
    }
}

async fn handle_del(body: &[u8], datastore: &dyn Datastore) -> Result<Frame, RiakError> {
    let req = RpbDelReq::decode(body)?;
    let routing = Msg::new(0, MsgType::Unknown, true);
    datastore.dispatch(routing).await?;
    match datastore.riak_delete(&req.bucket, &req.key).await {
        Ok(_) | Err(DatastoreError::Unsupported(_)) => {
            Ok(Frame::new(MessageCode::DelResp.as_u8(), Vec::new()))
        }
        Err(e) => Ok(error_frame(format!("riak del: {e}"))),
    }
}

/// Run a 2i secondary-index query against the datastore.
///
/// Both equality (`qtype = 0`) and range (`qtype = 1`) queries
/// are honoured. Streaming responses are not implemented in
/// this slice; the entire result set is packed into a single
/// [`RpbIndexResp`] frame with `done = Some(true)`. The
/// `pagination_sort`, `term_regex`, `continuation`, and
/// `cover_context` fields on the request are accepted but not
/// acted on; supporting them is tracked in the journal entry
/// for this slice.
///
/// 2i queries do NOT trampoline through [`Datastore::dispatch`]
/// because the existing list / property paths (the only other
/// non-KV ops) also bypass it; the substrate's dispatch counter
/// is reserved for K/V operations.
async fn handle_index(body: &[u8], datastore: &dyn Datastore) -> Result<Frame, RiakError> {
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
            return Ok(error_frame(format!(
                "riak index: unsupported qtype {other}; expected 0 (eq) or 1 (range)"
            )));
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
            let resp = RpbIndexResp {
                keys,
                results: Vec::new(),
                continuation: None,
                done: Some(true),
            };
            Ok(Frame::new(
                MessageCode::IndexResp.as_u8(),
                resp.encode_to_vec(),
            ))
        }
        Err(DatastoreError::Unsupported(_)) => Ok(error_frame(
            "secondary-index queries not implemented for this datastore".into(),
        )),
        Err(e) => Ok(error_frame(format!("riak index: {e}"))),
    }
}

/// Build an `RpbErrorResp` frame from a human-readable message.
fn error_frame(message: String) -> Frame {
    let resp = RpbErrorResp {
        errmsg: message.into_bytes(),
        errcode: 1,
    };
    Frame::new(MessageCode::ErrorResp.as_u8(), resp.encode_to_vec())
}

fn handle_get_bucket(body: &[u8]) -> Result<Frame, RiakError> {
    let _req = RpbGetBucketReq::decode(body)?;
    // Until per-bucket persistence lands, the server reports
    // conservative cluster defaults. These match Riak's
    // out-of-the-box defaults for the `default` bucket type.
    let resp = RpbGetBucketResp {
        props: Some(RpbBucketProps {
            n_val: Some(3),
            allow_mult: Some(false),
            last_write_wins: Some(false),
            ..RpbBucketProps::default()
        }),
    };
    Ok(Frame::new(
        MessageCode::GetBucketResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

fn handle_set_bucket(body: &[u8]) -> Result<Frame, RiakError> {
    let _req = RpbSetBucketReq::decode(body)?;
    // The supplied properties are not yet persisted; the response
    // is an empty acknowledgement so a conforming client treats
    // the call as successful.
    let resp = RpbSetBucketResp::default();
    Ok(Frame::new(
        MessageCode::SetBucketResp.as_u8(),
        resp.encode_to_vec(),
    ))
}

/// Run a MapReduce job submitted via PBC.
///
/// The single-response variant: the executor runs the entire job to
/// completion and the server emits one [`RpbMapRedResp`] carrying
/// the captured outputs as JSON, with `done = true`. Streaming
/// (one frame per phase plus a body-less terminator) is documented
/// in the journal as deferred.
async fn handle_mapred(body: &[u8]) -> Result<Frame, RiakError> {
    use std::sync::Arc;

    use crate::mapreduce::{builtins::default_registry, run_job, MapReduceJob};

    let req = RpbMapRedReq::decode(body)?;
    if req.content_type != b"application/json" {
        let resp = RpbErrorResp {
            errmsg: format!(
                "unsupported MapReduce content-type: {}",
                String::from_utf8_lossy(&req.content_type)
            )
            .into_bytes(),
            errcode: 1,
        };
        return Ok(Frame::new(
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
            return Ok(Frame::new(
                MessageCode::ErrorResp.as_u8(),
                resp.encode_to_vec(),
            ));
        }
    };

    let registry = Arc::new(default_registry());
    match run_job(job, registry).await {
        Ok(outputs) => {
            let body = serde_json::to_vec(&outputs).unwrap_or_else(|_| b"[]".to_vec());
            let resp = RpbMapRedResp {
                phase: Some(0),
                response: Some(body),
                done: Some(true),
            };
            Ok(Frame::new(
                MessageCode::MapRedResp.as_u8(),
                resp.encode_to_vec(),
            ))
        }
        Err(e) => {
            let resp = RpbErrorResp {
                errmsg: format!("MapReduce execution: {e}").into_bytes(),
                errcode: 1,
            };
            Ok(Frame::new(
                MessageCode::ErrorResp.as_u8(),
                resp.encode_to_vec(),
            ))
        }
    }
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
        let Err(err) = process_frame(&frame, &ds).await else {
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
        let stream = process_frame(&frame, &ds).await.expect("ok");
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
        let Err(err) = process_frame(&frame, &ds).await else {
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
                value: b"v".to_vec(),
                ..RpbPutReq::default()
            }
            .encode_to_vec(),
        );
        let _ = process_frame(&frame, ds.as_ref()).await.expect("ok");
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
        let stream = process_frame(&frame, &ds).await.expect("ok");
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
        let stream = process_frame(&frame, &ds).await.expect("ok");
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
        let stream = process_frame(&frame, &ds).await.expect("ok");
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
        let stream = process_frame(&frame, &ds).await.expect("ok");
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
}
