//! End-to-end integration tests for the gossip-driven peer-state
//! authority.
//!
//! Each test wires two in-process peers (a `GossipHandler` over a
//! `ServerPool`) and exchanges gossip frames over a `tokio::io::duplex`
//! pair to verify:
//!
//! 1. A `GossipSyn` frame written by one side feeds the other
//!    side's failure detector and flips the remote peer's
//!    `PeerState` from `Down` to `Normal`.
//! 2. A `GossipShutdown` frame transitions the sender back to
//!    `Down` immediately.
//! 3. With no inbound traffic for `~threshold * gossip_interval`
//!    the `evaluate` tick transitions the peer to `Down`.
//!
//! The wire format follows the runtime's convention: the gossip
//! payload is the sender peer's `host:port` (ASCII).

use std::sync::Arc;
use std::time::{Duration, Instant};

use dynomite::cluster::gossip::GossipHandler;
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::hashkit::DynToken;
use dynomite::io::mbuf::MbufPool;
use dynomite::proto::dnode::{dmsg_write, DmsgType, DnodeParser, ParseStep};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn pool(local_pname: &str, remote_pname: &str) -> Arc<ServerPool> {
    let cfg = PoolConfig {
        dc: "dc1".into(),
        rack: "r1".into(),
        enable_gossip: true,
        ..PoolConfig::default()
    };
    let (lh, lp) = split_pname(local_pname);
    let (rh, rp) = split_pname(remote_pname);
    let local = Peer::new(
        0,
        PeerEndpoint::tcp(lh, lp),
        "r1".into(),
        "dc1".into(),
        vec![DynToken::from_u32(1)],
        true,
        true,
        false,
    );
    let remote = Peer::new(
        1,
        PeerEndpoint::tcp(rh, rp),
        "r1".into(),
        "dc1".into(),
        vec![DynToken::from_u32(2_147_483_648)],
        false,
        true,
        false,
    );
    Arc::new(ServerPool::new(cfg, vec![local, remote]))
}

fn split_pname(s: &str) -> (String, u16) {
    let mut it = s.rsplitn(2, ':');
    let port: u16 = it.next().unwrap().parse().unwrap();
    let host = it.next().unwrap().to_string();
    (host, port)
}

fn remote_state(pool: &ServerPool) -> PeerState {
    pool.peers()
        .read()
        .iter()
        .find(|p| !p.is_local())
        .map_or(PeerState::Unknown, Peer::state)
}

/// Build a single gossip frame the way the runtime does: dnode
/// header + ASCII pname payload.
fn build_gossip_frame(ty: DmsgType, msg_id: u64, payload: &[u8]) -> Vec<u8> {
    let pool = MbufPool::default();
    let mut header_buf = pool.get();
    dmsg_write(
        &mut header_buf,
        msg_id,
        ty,
        0,
        true,
        None,
        u32::try_from(payload.len()).unwrap(),
    )
    .unwrap();
    let mut out = header_buf.readable().to_vec();
    out.extend_from_slice(payload);
    out
}

/// Drain an inbound dnode stream; for each gossip header observed,
/// invoke `on_frame(ty, payload)`. Returns when the stream is
/// closed or `frames_expected` frames have been observed.
async fn drive_inbound<R, F>(mut reader: R, frames_expected: usize, mut on_frame: F)
where
    R: tokio::io::AsyncRead + Unpin,
    F: FnMut(DmsgType, &[u8]),
{
    let mut accumulated: Vec<u8> = Vec::new();
    let mut buf = vec![0u8; 4096];
    let mut parser = DnodeParser::new();
    let mut count = 0usize;
    loop {
        if count >= frames_expected {
            return;
        }
        let Ok(n) = reader.read(&mut buf).await else {
            return;
        };
        if n == 0 {
            return;
        }
        accumulated.extend_from_slice(&buf[..n]);
        loop {
            match parser.step(&accumulated) {
                ParseStep::HeaderDone { consumed } => {
                    let dmsg = parser.take_dmsg();
                    let plen = dmsg.plen as usize;
                    let total = consumed + plen;
                    if accumulated.len() < total {
                        parser.reset();
                        break;
                    }
                    let payload = accumulated[consumed..total].to_vec();
                    accumulated.drain(0..total);
                    parser.reset();
                    on_frame(dmsg.ty, &payload);
                    count += 1;
                    if count >= frames_expected {
                        return;
                    }
                }
                ParseStep::NeedMore { .. } => break,
                ParseStep::Error { .. } => return,
            }
        }
    }
}

/// Single-direction gossip wire: a frame written into `tx` must
/// flip the receiver's `PeerState` from `Down` to `Normal` once
/// the gossip handler observes it.
#[tokio::test(flavor = "multi_thread")]
async fn gossip_syn_promotes_remote_to_normal() {
    // The receiving peer has the sender registered as
    // `127.0.0.1:18101`; the sender will use that as its payload.
    let recv_pool = pool("127.0.0.1:18102", "127.0.0.1:18101");
    let handler = Arc::new(GossipHandler::new(recv_pool.clone()));

    assert_eq!(remote_state(&recv_pool), PeerState::Down);

    // In-memory full-duplex pipe. Sender writes one GossipSyn
    // frame; receiver task feeds it to the gossip handler.
    let (mut tx, rx) = tokio::io::duplex(8192);
    let frame = build_gossip_frame(DmsgType::GossipSyn, 1, b"127.0.0.1:18101");

    let writer = tokio::spawn(async move {
        tx.write_all(&frame).await.unwrap();
        tx.flush().await.unwrap();
    });

    let handler_clone = handler.clone();
    let reader = tokio::spawn(async move {
        drive_inbound(rx, 1, |ty, payload| {
            assert_eq!(ty, DmsgType::GossipSyn);
            let pname = std::str::from_utf8(payload).unwrap();
            handler_clone.record_heartbeat_pname(pname, Instant::now());
        })
        .await;
    });

    writer.await.unwrap();
    reader.await.unwrap();

    assert_eq!(remote_state(&recv_pool), PeerState::Normal);
}

#[tokio::test(flavor = "multi_thread")]
async fn gossip_shutdown_marks_sender_down() {
    let recv_pool = pool("127.0.0.1:28102", "127.0.0.1:28101");
    let handler = Arc::new(GossipHandler::new(recv_pool.clone()));

    // First a GossipSyn to flip Normal, then a GossipShutdown to
    // flip back to Down without waiting for phi.
    handler.record_heartbeat_pname("127.0.0.1:28101", Instant::now());
    assert_eq!(remote_state(&recv_pool), PeerState::Normal);

    let (mut tx, rx) = tokio::io::duplex(8192);
    let frame = build_gossip_frame(DmsgType::GossipShutdown, 2, b"127.0.0.1:28101");

    let writer = tokio::spawn(async move {
        tx.write_all(&frame).await.unwrap();
        tx.flush().await.unwrap();
    });

    let handler_clone = handler.clone();
    let reader = tokio::spawn(async move {
        drive_inbound(rx, 1, |ty, payload| {
            assert_eq!(ty, DmsgType::GossipShutdown);
            let pname = std::str::from_utf8(payload).unwrap();
            handler_clone.mark_down_pname(pname);
        })
        .await;
    });

    writer.await.unwrap();
    reader.await.unwrap();

    assert_eq!(remote_state(&recv_pool), PeerState::Down);
}

#[tokio::test(flavor = "multi_thread")]
async fn gossip_silence_flips_peer_to_down_via_evaluate() {
    // Drive 5 heartbeats at 1Hz, stop, then jump 60s into the
    // future and call `evaluate`; the peer must transition back
    // to `Down` because phi exceeds the default threshold of 8.0.
    let recv_pool = pool("127.0.0.1:38102", "127.0.0.1:38101");
    let handler = Arc::new(GossipHandler::new(recv_pool.clone()));

    let (mut tx, rx) = tokio::io::duplex(8192);
    let writer = tokio::spawn(async move {
        for i in 0..5u64 {
            let frame = build_gossip_frame(DmsgType::GossipSyn, i + 1, b"127.0.0.1:38101");
            tx.write_all(&frame).await.unwrap();
            tx.flush().await.unwrap();
        }
    });

    let t0 = Instant::now();
    let handler_clone = handler.clone();
    let reader = tokio::spawn(async move {
        let mut counter = 0u64;
        drive_inbound(rx, 5, |ty, payload| {
            assert_eq!(ty, DmsgType::GossipSyn);
            let pname = std::str::from_utf8(payload).unwrap();
            // Synthetic 1s spacing.
            handler_clone.record_heartbeat_pname(pname, t0 + Duration::from_secs(counter));
            counter += 1;
        })
        .await;
    });

    writer.await.unwrap();
    reader.await.unwrap();

    assert_eq!(remote_state(&recv_pool), PeerState::Normal);

    // Jump 60s into the future with no new heartbeats. Phi
    // crosses the threshold; `evaluate` flips the peer to Down.
    let later = t0 + Duration::from_mins(1);
    let transitions = handler.evaluate(later);
    assert_eq!(transitions, vec![(1, PeerState::Down)]);
    assert_eq!(remote_state(&recv_pool), PeerState::Down);
}

#[tokio::test(flavor = "multi_thread")]
async fn bidirectional_gossip_transcript() {
    // Two handlers, two pools, two duplex pipes (one per
    // direction). Each side sends a GossipSyn through its own
    // pipe; both sides should converge on `PeerState::Normal`.
    let pool_a = pool("127.0.0.1:48101", "127.0.0.1:48102");
    let pool_b = pool("127.0.0.1:48102", "127.0.0.1:48101");
    let handler_a = Arc::new(GossipHandler::new(pool_a.clone()));
    let handler_b = Arc::new(GossipHandler::new(pool_b.clone()));
    assert_eq!(remote_state(&pool_a), PeerState::Down);
    assert_eq!(remote_state(&pool_b), PeerState::Down);

    let (mut a_tx, b_rx) = tokio::io::duplex(8192);
    let (mut b_tx, a_rx) = tokio::io::duplex(8192);

    let frame_from_a = build_gossip_frame(DmsgType::GossipSyn, 1, b"127.0.0.1:48101");
    let frame_from_b = build_gossip_frame(DmsgType::GossipSyn, 1, b"127.0.0.1:48102");

    let a_writer = tokio::spawn(async move {
        a_tx.write_all(&frame_from_a).await.unwrap();
        a_tx.flush().await.unwrap();
    });
    let b_writer = tokio::spawn(async move {
        b_tx.write_all(&frame_from_b).await.unwrap();
        b_tx.flush().await.unwrap();
    });

    let h_b = handler_b.clone();
    let b_reader = tokio::spawn(async move {
        drive_inbound(b_rx, 1, |_ty, payload| {
            let pname = std::str::from_utf8(payload).unwrap();
            h_b.record_heartbeat_pname(pname, Instant::now());
        })
        .await;
    });
    let h_a = handler_a.clone();
    let a_reader = tokio::spawn(async move {
        drive_inbound(a_rx, 1, |_ty, payload| {
            let pname = std::str::from_utf8(payload).unwrap();
            h_a.record_heartbeat_pname(pname, Instant::now());
        })
        .await;
    });

    a_writer.await.unwrap();
    b_writer.await.unwrap();
    a_reader.await.unwrap();
    b_reader.await.unwrap();

    assert_eq!(remote_state(&pool_a), PeerState::Normal);
    assert_eq!(remote_state(&pool_b), PeerState::Normal);
}
