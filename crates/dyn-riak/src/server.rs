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

use std::sync::Arc;

use prost::Message as _;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;

use dynomite::embed::Datastore;
use dynomite::msg::{Msg, MsgType};

use crate::error::RiakError;
use crate::proto::pb::framer::{read_frame, write_frame, Frame};
use crate::proto::pb::messages::{
    MessageCode, RpbDelReq, RpbErrorResp, RpbGetReq, RpbGetResp, RpbPingReq, RpbPingResp,
    RpbPutReq, RpbPutResp,
};

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
    loop {
        let (sock, peer) = listener.accept().await?;
        let datastore = Arc::clone(&datastore);
        tokio::spawn(async move {
            if let Err(e) = handle_conn(sock, datastore).await {
                tracing::warn!(%peer, error = %e, "riak pbc connection ended with error");
            }
        });
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

        let response = process_frame(&frame, datastore.as_ref()).await?;
        write_frame(&mut writer, &response).await?;
    }
}

/// Per-frame dispatch. Pure function aside from the datastore call.
async fn process_frame(frame: &Frame, datastore: &dyn Datastore) -> Result<Frame, RiakError> {
    let code = MessageCode::from_u8(frame.code).map_err(RiakError::UnknownMessageCode)?;
    match code {
        MessageCode::PingReq => {
            // Body must decode (it is empty in conforming clients)
            // but we permit padding-tolerant clients by ignoring
            // unknown trailing bytes; `prost` already does this.
            let _ = RpbPingReq::decode(frame.body.as_slice())?;
            let resp = RpbPingResp::default();
            Ok(Frame::new(
                MessageCode::PingResp.as_u8(),
                resp.encode_to_vec(),
            ))
        }
        MessageCode::GetReq => {
            let _req = RpbGetReq::decode(frame.body.as_slice())?;
            // Trampoline through the substrate so dispatch counts
            // tick. The K/V semantics will move to a richer trait
            // in the follow-up slice.
            let routing = Msg::new(0, MsgType::Unknown, true);
            datastore.dispatch(routing).await?;
            let resp = RpbGetResp::default();
            Ok(Frame::new(
                MessageCode::GetResp.as_u8(),
                resp.encode_to_vec(),
            ))
        }
        MessageCode::PutReq => {
            let _req = RpbPutReq::decode(frame.body.as_slice())?;
            let routing = Msg::new(0, MsgType::Unknown, true);
            datastore.dispatch(routing).await?;
            let resp = RpbPutResp::default();
            Ok(Frame::new(
                MessageCode::PutResp.as_u8(),
                resp.encode_to_vec(),
            ))
        }
        MessageCode::DelReq => {
            let _req = RpbDelReq::decode(frame.body.as_slice())?;
            let routing = Msg::new(0, MsgType::Unknown, true);
            datastore.dispatch(routing).await?;
            // RpbDelResp is body-less.
            Ok(Frame::new(MessageCode::DelResp.as_u8(), Vec::new()))
        }
        // Response codes are illegal inbound.
        MessageCode::ErrorResp
        | MessageCode::PingResp
        | MessageCode::GetResp
        | MessageCode::PutResp
        | MessageCode::DelResp => {
            let body = RpbErrorResp {
                errmsg: format!("unsupported inbound message code: {}", frame.code).into_bytes(),
                errcode: 0,
            }
            .encode_to_vec();
            Ok(Frame::new(MessageCode::ErrorResp.as_u8(), body))
        }
    }
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
        let frame = Frame::new(7, Vec::new());
        let ds = MemoryDatastore::new();
        let err = process_frame(&frame, &ds).await.expect_err("unknown");
        assert!(matches!(err, RiakError::UnknownMessageCode(7)));
    }

    #[tokio::test]
    async fn response_codes_inbound_yield_error_resp() {
        // GetResp inbound is illegal but must not crash the server;
        // we reply with an RpbErrorResp instead.
        let frame = Frame::new(MessageCode::GetResp.as_u8(), Vec::new());
        let ds = MemoryDatastore::new();
        let resp = process_frame(&frame, &ds).await.expect("ok");
        assert_eq!(resp.code, MessageCode::ErrorResp.as_u8());
        let parsed = RpbErrorResp::decode(resp.body.as_slice()).expect("decode");
        assert!(!parsed.errmsg.is_empty());
    }

    #[tokio::test]
    async fn malformed_body_reports_decode_error() {
        // GetReq with a truncated length-delimited string field.
        let frame = Frame::new(MessageCode::GetReq.as_u8(), vec![0x0a, 0xff]);
        let ds = MemoryDatastore::new();
        let err = process_frame(&frame, &ds).await.expect_err("decode");
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
}
