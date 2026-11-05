//! A request to a datastore or peer that stops answering fails once its
//! deadline passes: the whole connection closes, every request still
//! outstanding on it (written or queued) is answered with the
//! connection error, and the supervisor reconnects so later requests to
//! a healthy endpoint succeed.

use std::net::SocketAddr;
use std::time::Duration;

use dynomite::net::server::OutboundRequest;
use dynomite::net::OutboundEnvelope;
use dynomite::proto::dnode::{dmsg_write, DmsgType, DnodeParser, ParseStep};
use dynomited::server::{spawn_backend_supervisor_for_testing, spawn_peer_supervisor_for_testing};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::Instant;

const GET_K: &[u8] = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
const PEER_REFUSED: &[u8] = b"-ERR Peer: Peer Node refused connection\r\n";
const PEER_NOT_CONNECTED: &[u8] = b"-ERR Peer: Peer Node is not connected\r\n";
const STORAGE_REFUSED: &[u8] = b"-ERR Storage: Datastore refused connection\r\n";

/// Hold the socket open and swallow every byte: a frozen endpoint.
async fn black_hole(mut s: TcpStream) {
    let mut buf = [0u8; 4096];
    while matches!(s.read(&mut buf).await, Ok(n) if n > 0) {}
}

/// Redis backend: the first connection never answers, later ones answer
/// every command with `+OK`.
async fn fake_backend(listener: TcpListener) {
    let (first, _) = listener.accept().await.expect("accept");
    tokio::spawn(black_hole(first));
    loop {
        let Ok((mut s, _)) = listener.accept().await else {
            return;
        };
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            let mut acc = Vec::new();
            loop {
                match s.read(&mut buf).await {
                    Ok(n) if n > 0 => acc.extend_from_slice(&buf[..n]),
                    _ => return,
                }
                // Every test request is the same `GET k` frame.
                while acc.len() >= GET_K.len() {
                    acc.drain(..GET_K.len());
                    if s.write_all(b"+OK\r\n").await.is_err() {
                        return;
                    }
                }
            }
        });
    }
}

/// Dnode peer: the first connection never answers, later ones answer
/// every request frame with `+OK`.
async fn fake_peer(listener: TcpListener) {
    let (first, _) = listener.accept().await.expect("accept");
    tokio::spawn(black_hole(first));
    loop {
        let Ok((mut s, _)) = listener.accept().await else {
            return;
        };
        tokio::spawn(async move {
            let mut acc = Vec::new();
            let mut parser = DnodeParser::new();
            let mut buf = [0u8; 4096];
            let pool = dynomite::io::mbuf::MbufPool::default();
            loop {
                let n = match s.read(&mut buf).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };
                acc.extend_from_slice(&buf[..n]);
                while let ParseStep::HeaderDone { consumed } = parser.step(&acc) {
                    let dmsg = parser.take_dmsg();
                    let total = consumed + dmsg.plen as usize;
                    if acc.len() < total {
                        parser.reset();
                        break;
                    }
                    acc.drain(0..total);
                    parser.reset();
                    let mut hb = pool.get();
                    dmsg_write(&mut hb, dmsg.id, DmsgType::Res, 0, false, None, 5).expect("hdr");
                    if s.write_all(hb.readable()).await.is_err()
                        || s.write_all(b"+OK\r\n").await.is_err()
                    {
                        return;
                    }
                }
            }
        });
    }
}

fn request(
    id: u64,
    ty: DmsgType,
    timeout: Duration,
) -> (OutboundRequest, mpsc::Receiver<OutboundEnvelope>) {
    let (tx, rx) = mpsc::channel(1);
    let req = OutboundRequest {
        bytes: GET_K.to_vec(),
        req_id: id,
        responder: tx,
        span: tracing::Span::none(),
        ty,
        target_peer_idx: Some(1),
        deadline: Some(Instant::now() + timeout),
    };
    (req, rx)
}

async fn reply(rx: &mut mpsc::Receiver<OutboundEnvelope>, within: Duration) -> Vec<u8> {
    let env = tokio::time::timeout(within, rx.recv())
        .await
        .expect("reply in time")
        .expect("responder open");
    env.rsp
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect()
}

/// Send requests until one is answered with `+OK` or `within` passes.
async fn eventually_ok(tx: &mpsc::Sender<OutboundRequest>, ty: DmsgType, within: Duration) {
    let end = Instant::now() + within;
    let mut id = 100;
    while Instant::now() < end {
        let (req, mut rx) = request(id, ty, Duration::from_secs(5));
        tx.send(req).await.expect("channel");
        if reply(&mut rx, Duration::from_secs(6)).await == b"+OK\r\n" {
            return;
        }
        id += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no successful reply after reconnect within {within:?}");
}

async fn listener() -> (TcpListener, SocketAddr) {
    let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let a = l.local_addr().expect("addr");
    (l, a)
}

#[tokio::test]
async fn regression_backend_request_times_out_and_fails_outstanding() {
    let (l, addr) = listener().await;
    tokio::spawn(fake_backend(l));
    let (tx, rx) = mpsc::channel(16);
    let sup = spawn_backend_supervisor_for_testing(
        addr,
        rx,
        dynomite::conf::DataStore::Valkey,
        None,
        10_000,
    );

    let start = Instant::now();
    let (r1, mut rx1) = request(1, DmsgType::Req, Duration::from_millis(300));
    let (r2, mut rx2) = request(2, DmsgType::Req, Duration::from_secs(30));
    tx.send(r1).await.expect("send");
    tx.send(r2).await.expect("send");

    assert_eq!(
        reply(&mut rx1, Duration::from_secs(5)).await,
        STORAGE_REFUSED
    );
    let waited = start.elapsed();
    assert!(
        waited >= Duration::from_millis(290),
        "failed early: {waited:?}"
    );
    assert!(waited < Duration::from_secs(2), "failed late: {waited:?}");
    // The other request on the closed connection is answered at once,
    // not when its own (much later) deadline passes.
    assert_eq!(
        reply(&mut rx2, Duration::from_secs(1)).await,
        STORAGE_REFUSED
    );

    eventually_ok(&tx, DmsgType::Req, Duration::from_secs(10)).await;
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(5), sup).await;
}

#[tokio::test]
async fn regression_backend_request_without_deadline_never_times_out() {
    let (l, addr) = listener().await;
    tokio::spawn(fake_backend(l));
    let (tx, rx) = mpsc::channel(16);
    let _sup = spawn_backend_supervisor_for_testing(
        addr,
        rx,
        dynomite::conf::DataStore::Valkey,
        None,
        10_000,
    );
    let (mut r1, mut rx1) = request(1, DmsgType::Req, Duration::ZERO);
    r1.deadline = None;
    tx.send(r1).await.expect("send");
    assert!(
        tokio::time::timeout(Duration::from_millis(800), rx1.recv())
            .await
            .is_err(),
        "a request without a deadline must keep waiting"
    );
}

#[tokio::test]
async fn regression_peer_request_times_out_and_fails_outstanding() {
    let (l, addr) = listener().await;
    tokio::spawn(fake_peer(l));
    let (tx, rx) = mpsc::channel(16);
    let _sup = spawn_peer_supervisor_for_testing(addr, 1, rx, "dc1".into());

    let start = Instant::now();
    let (r1, mut rx1) = request(1, DmsgType::ReqForward, Duration::from_millis(300));
    let (r2, mut rx2) = request(2, DmsgType::ReqForward, Duration::from_secs(30));
    tx.send(r1).await.expect("send");
    tx.send(r2).await.expect("send");

    assert_eq!(reply(&mut rx1, Duration::from_secs(5)).await, PEER_REFUSED);
    let waited = start.elapsed();
    assert!(
        waited >= Duration::from_millis(290),
        "failed early: {waited:?}"
    );
    assert!(waited < Duration::from_secs(2), "failed late: {waited:?}");
    assert_eq!(reply(&mut rx2, Duration::from_secs(1)).await, PEER_REFUSED);

    // Until the reconnect wait passes the peer is not connected, and a
    // request is refused at once.
    let (r3, mut rx3) = request(3, DmsgType::ReqForward, Duration::from_secs(30));
    tx.send(r3).await.expect("send");
    assert_eq!(
        reply(&mut rx3, Duration::from_millis(200)).await,
        PEER_NOT_CONNECTED
    );

    eventually_ok(&tx, DmsgType::ReqForward, Duration::from_secs(10)).await;
}
