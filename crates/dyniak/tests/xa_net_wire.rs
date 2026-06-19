//! Wire-level error paths for the dnode XA transport and peer loop.
//!
//! These tests drive the [`DnodeXaTransport`] / `serve_xa_peer`
//! framing seam directly with hand-built dnode frames, so the error
//! arms that only fire on malformed or unexpected wire traffic --
//! a connect failure, a per-phase timeout, a reply of the wrong
//! dnode type, the peer closing the connection, and an unexpected
//! inbound dnode type on the peer plane -- are all exercised without
//! relying on a fault-injecting mock transport.
//!
//! Gated on the `noxu` feature; without it the file compiles to an
//! empty module.

#![cfg(feature = "noxu")]

use std::sync::Arc;
use std::time::Duration;

use dyniak::datastore::xa_net::{
    serve_xa_peer, DnodeXaTransport, XaPeer, XaTransport, XaTransportError,
};
use dyniak::datastore::xa_wire::{WireXid, XaWriteOp};
use dyniak::datastore::XaParticipant;
use dynomite::io::mbuf::MbufPool;
use dynomite::proto::dnode::{dmsg_write, DmsgType};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

const FORMAT_ID: i32 = 0x6479_6e6b;

fn scratch_dir() -> TempDir {
    let base = std::path::Path::new("/scratch");
    if base.is_dir() {
        TempDir::new_in(base).expect("tempdir in /scratch")
    } else {
        TempDir::new().expect("tempdir")
    }
}

fn open_participant(dir: &TempDir, name: &[u8]) -> XaParticipant {
    XaParticipant::open(dir.path(), name.to_vec()).expect("open participant")
}

fn wire(bqual: &[u8]) -> WireXid {
    WireXid {
        format_id: FORMAT_ID,
        gtrid: 1u64.to_be_bytes().to_vec(),
        bqual: bqual.to_vec(),
    }
}

/// Build one dnode frame (header + payload) the same way the
/// transport does, for hand-feeding a fake peer's replies.
fn frame(ty: DmsgType, payload: &[u8]) -> Vec<u8> {
    let pool = MbufPool::default();
    let mut header = pool.get();
    let plen = u32::try_from(payload.len()).unwrap();
    dmsg_write(&mut header, 1, ty, 0, true, None, plen).expect("header");
    let mut out = header.readable().to_vec();
    out.extend_from_slice(payload);
    out
}

// ------------------------------------------------------------------
// 1. Connect failure: the transport cannot reach the peer at all.
//    Exercises round_trip's connect error mapping.
// ------------------------------------------------------------------

#[tokio::test]
async fn connect_failure_is_transport_error() {
    // 127.0.0.1:1 is reserved/unbound; a connect there fails fast.
    let addr = "127.0.0.1:1".parse().unwrap();
    let transport = DnodeXaTransport::new(addr).with_timeout(Duration::from_millis(200));
    let res = transport.commit(&wire(b"west"), b"west").await;
    assert!(matches!(res, Err(XaTransportError::Transport(_))));
}

// ------------------------------------------------------------------
// 2. Per-phase timeout: the peer accepts but never replies, so the
//    exchange times out and the connection is dropped.
// ------------------------------------------------------------------

#[tokio::test]
async fn silent_peer_times_out_the_phase() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Accept and hold the connection open without ever replying.
    let _server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        // Drain the request but never write a reply.
        let mut buf = [0u8; 256];
        let _ = s.read(&mut buf).await;
        // Hold the socket so the client waits out its deadline.
        tokio::time::sleep(Duration::from_secs(5)).await;
        drop(s);
    });
    let transport = DnodeXaTransport::new(addr).with_timeout(Duration::from_millis(150));
    let res = transport.commit(&wire(b"west"), b"west").await;
    assert!(matches!(res, Err(XaTransportError::Timeout)), "{res:?}");
}

// ------------------------------------------------------------------
// 3. Wrong reply type for prepare: peer answers with an XaAck where
//    the transport expects an XaVote.
// ------------------------------------------------------------------

#[tokio::test]
async fn prepare_wrong_reply_type_is_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 512];
        let _ = s.read(&mut buf).await;
        // Reply with an XaAck (type 21) instead of an XaVote.
        let reply = frame(DmsgType::XaAck, &[1u8]);
        s.write_all(&reply).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let transport = DnodeXaTransport::new(addr).with_timeout(Duration::from_secs(2));
    let writes = vec![XaWriteOp::Put {
        bucket: b"u".to_vec(),
        key: b"alice".to_vec(),
        value: b"a".to_vec(),
        indexes: vec![],
    }];
    let res = transport.prepare(&wire(b"west"), b"west", &writes).await;
    match res {
        Err(XaTransportError::Transport(msg)) => {
            assert!(msg.contains("expected XaVote"), "{msg}");
        }
        other => panic!("expected wrong-reply transport error, got {other:?}"),
    }
}

// ------------------------------------------------------------------
// 4. Wrong reply type for commit: peer answers with an XaVote where
//    the transport expects an XaAck.
// ------------------------------------------------------------------

#[tokio::test]
async fn commit_wrong_reply_type_is_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 512];
        let _ = s.read(&mut buf).await;
        // Reply with an XaVote (type 18) instead of an XaAck.
        let reply = frame(DmsgType::XaVote, &[0u8]);
        s.write_all(&reply).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let transport = DnodeXaTransport::new(addr).with_timeout(Duration::from_secs(2));
    let res = transport.commit(&wire(b"west"), b"west").await;
    match res {
        Err(XaTransportError::Transport(msg)) => {
            assert!(msg.contains("expected XaAck"), "{msg}");
        }
        other => panic!("expected wrong-reply transport error, got {other:?}"),
    }
}

// ------------------------------------------------------------------
// 5. Peer reports an unresolved branch (ack ok=false) on commit.
// ------------------------------------------------------------------

#[tokio::test]
async fn commit_ack_false_is_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 512];
        let _ = s.read(&mut buf).await;
        // XaAck with ok=false (byte 0).
        let reply = frame(DmsgType::XaAck, &[0u8]);
        s.write_all(&reply).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let transport = DnodeXaTransport::new(addr).with_timeout(Duration::from_secs(2));
    let res = transport.commit(&wire(b"west"), b"west").await;
    match res {
        Err(XaTransportError::Transport(msg)) => {
            assert!(msg.contains("unresolved"), "{msg}");
        }
        other => panic!("expected unresolved transport error, got {other:?}"),
    }
}

// ------------------------------------------------------------------
// 6. Peer closes mid-read: the transport surfaces a transport error
//    ("peer closed"), exercising the n == 0 arm of read_frame.
// ------------------------------------------------------------------

#[tokio::test]
async fn peer_closing_connection_is_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 512];
        let _ = s.read(&mut buf).await;
        // Close immediately without sending a reply frame.
        s.shutdown().await.unwrap();
        drop(s);
    });
    let transport = DnodeXaTransport::new(addr).with_timeout(Duration::from_secs(2));
    let res = transport.commit(&wire(b"west"), b"west").await;
    assert!(
        matches!(res, Err(XaTransportError::Transport(_))),
        "{res:?}"
    );
}

// ------------------------------------------------------------------
// 7. serve_xa_conn rejects an unexpected dnode type on the peer
//    plane. We send a non-XA dnode frame; the server tears the
//    connection down with an error (logged), which the client sees
//    as a closed/failed exchange.
// ------------------------------------------------------------------

#[tokio::test]
async fn peer_plane_rejects_unexpected_dnode_type() {
    let d = scratch_dir();
    let peer = Arc::new(XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d, b"west"),
    )]));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = tokio::spawn(async move {
        let _ = serve_xa_peer(listener, peer).await;
    });

    // Connect raw and send a GossipSyn frame the peer plane never
    // expects. The server's serve_xa_conn returns an error, closing
    // the connection; our read then sees EOF.
    let mut conn = TcpStream::connect(addr).await.unwrap();
    let bad = frame(DmsgType::GossipSyn, b"hello");
    conn.write_all(&bad).await.unwrap();
    let mut buf = [0u8; 64];
    let n = conn.read(&mut buf).await.unwrap_or(0);
    assert_eq!(n, 0, "peer closed the connection after an unexpected type");
}

// ------------------------------------------------------------------
// 8. Round-trip happy path over the real peer plane, then a second
//    phase on the SAME transport: proves the persistent connection
//    is reused (the lazy-connect branch is taken once).
// ------------------------------------------------------------------

#[tokio::test]
async fn persistent_connection_is_reused_across_phases() {
    let d = scratch_dir();
    let peer = Arc::new(XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d, b"west"),
    )]));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = tokio::spawn(async move {
        let _ = serve_xa_peer(listener, peer).await;
    });

    let transport = DnodeXaTransport::new(addr).with_timeout(Duration::from_secs(2));
    let writes = vec![XaWriteOp::Put {
        bucket: b"u".to_vec(),
        key: b"alice".to_vec(),
        value: b"a".to_vec(),
        indexes: vec![],
    }];
    // Prepare then commit on the same transport (connection reused).
    let vote = transport
        .prepare(&wire(b"west"), b"west", &writes)
        .await
        .expect("prepare");
    assert_eq!(vote, dyniak::datastore::xa_wire::XaVote::Ok);
    transport
        .commit(&wire(b"west"), b"west")
        .await
        .expect("commit");
}

// ------------------------------------------------------------------
// 9. read_frame reassembles a frame split across two reads: the peer
//    plane sends the header first, then the payload after a delay, so
//    the parser hits the "header parsed, payload not yet here" reset
//    branch and then completes on the next read.
// ------------------------------------------------------------------

#[tokio::test]
async fn split_frame_is_reassembled_across_reads() {
    let d = scratch_dir();
    let peer = Arc::new(XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d, b"west"),
    )]));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = tokio::spawn(async move {
        let _ = serve_xa_peer(listener, peer).await;
    });

    // Build a full XaCommit frame, then dribble it: header bytes
    // first, the payload after a short pause, so the server's
    // read_frame parses the header before the payload arrives.
    let payload = dyniak::datastore::xa_wire::XaResolveMsg {
        xid: wire(b"west"),
        env: b"west".to_vec(),
    }
    .encode();
    let full = frame(DmsgType::XaCommit, &payload);
    // The header ends where the magic + fields finish; split a few
    // bytes before the payload tail to guarantee a mid-frame read.
    let split_at = full.len() - payload.len().min(full.len() / 2).max(1);
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.write_all(&full[..split_at]).await.unwrap();
    conn.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    conn.write_all(&full[split_at..]).await.unwrap();
    // The server reassembles and replies with an XaAck.
    let mut buf = [0u8; 64];
    let n = conn.read(&mut buf).await.unwrap();
    assert!(n > 0, "server replied after reassembling the split frame");
}

// ------------------------------------------------------------------
// 10. A garbage header (not the dnode magic) makes read_frame return
//     a parse error; serve_xa_conn tears the connection down.
// ------------------------------------------------------------------

#[tokio::test]
async fn garbage_header_is_a_parse_error() {
    let d = scratch_dir();
    let peer = Arc::new(XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d, b"west"),
    )]));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = tokio::spawn(async move {
        let _ = serve_xa_peer(listener, peer).await;
    });

    let mut conn = TcpStream::connect(addr).await.unwrap();
    // Not the dnode magic ($2014$): the parser errors immediately.
    conn.write_all(b"GARBAGE!").await.unwrap();
    let mut buf = [0u8; 64];
    let n = conn.read(&mut buf).await.unwrap_or(0);
    assert_eq!(n, 0, "peer closed the connection after a parse error");
}
