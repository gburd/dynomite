//! Integration tests for Unix-domain-socket datastore backends.
//!
//! The backend supervisor dials a RESP datastore either over TCP
//! (`host:port`) or over a Unix-domain socket (a filesystem path
//! configured as a `servers:` entry). These tests exercise the
//! Unix path end-to-end at the supervisor level:
//!
//! * `unix_backend_round_trip` - a fake `UnixListener` accepts one
//!   connection, records the request bytes it receives, and replies
//!   `+OK\r\n`. The supervisor dials the socket, forwards a request
//!   pushed onto its channel, and the test asserts both that the
//!   request reached the backend over the Unix socket and that the
//!   `+OK` reply came back through the responder.
//!
//! * `unix_backend_auth_round_trip` - the same shape with
//!   `redis_requirepass` configured. The fake backend first reads
//!   and answers the `AUTH` handshake with `+OK\r\n`, then handles
//!   the data request. The test asserts the AUTH command crossed
//!   the Unix transport before any data request.
//!
//! Unix sockets do not exist on Windows, so the whole module is
//! `#[cfg(unix)]`. Socket paths live under `/scratch` (short, to
//! stay inside the ~108-byte `sun_path` limit) with a tempdir
//! fallback.

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::{mpsc, Mutex};

use dynomite::net::dispatcher::OutboundEnvelope;
use dynomite::net::server::OutboundRequest;
use dynomite::proto::dnode::DmsgType;
use dynomited::server::spawn_backend_supervisor_unix_for_testing;

/// Reconstruct the wire bytes carried by an [`OutboundEnvelope`]'s
/// response message from its mbuf queue.
fn envelope_bytes(env: &OutboundEnvelope) -> Vec<u8> {
    let mut out = Vec::new();
    for buf in env.rsp.mbufs() {
        out.extend_from_slice(buf.readable());
    }
    out
}

/// Allocate a short Unix-socket path under `/scratch` (preferred,
/// to keep well inside the `sun_path` length limit) or the system
/// temp dir as a fallback. The caller owns removal; we pre-remove
/// any stale entry so `bind` does not fail with `EADDRINUSE`.
fn scratch_sock_path(tag: &str) -> PathBuf {
    let base = if std::path::Path::new("/scratch").is_dir() {
        PathBuf::from("/scratch")
    } else {
        std::env::temp_dir()
    };
    let path = base.join(format!("dyn-{tag}-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

/// Read from `sock` into `buf`, appending every byte to `recorded`,
/// until at least `min_crlf` CRLF pairs have accumulated or the
/// peer goes quiet. Bounded so a stuck peer cannot hang the test.
async fn read_frame(
    sock: &mut tokio::net::UnixStream,
    buf: &mut [u8],
    recorded: &Arc<Mutex<Vec<u8>>>,
    min_crlf: usize,
) {
    for _ in 0..32 {
        match tokio::time::timeout(Duration::from_secs(1), sock.read(buf)).await {
            Ok(Ok(n)) if n > 0 => {
                let mut g = recorded.lock().await;
                g.extend_from_slice(&buf[..n]);
                if g.windows(2).filter(|w| *w == b"\r\n").count() >= min_crlf {
                    return;
                }
            }
            _ => return,
        }
    }
}

/// Spawn a fake RESP backend on `path` that:
///   1. accepts a single connection,
///   2. optionally reads + acks an `AUTH` handshake (`auth = true`),
///   3. reads one data request, recording every byte it sees,
///   4. replies `+OK\r\n`.
///
/// Returns the shared buffer of recorded request bytes and the
/// accept-loop join handle.
fn fake_unix_backend(
    listener: UnixListener,
    auth: bool,
) -> (Arc<Mutex<Vec<u8>>>, tokio::task::JoinHandle<()>) {
    let recorded = Arc::new(Mutex::new(Vec::<u8>::new()));
    let recorded_inner = Arc::clone(&recorded);
    let handle = tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = [0u8; 1024];

        // Read until at least one CRLF-terminated frame arrives.
        // RESP commands end with a CRLF; the AUTH handshake is a
        // full array so it ends with several. We read once per
        // logical step and bound the loop so a stuck peer cannot
        // hang the test.
        if auth {
            // AUTH array `*2\r\n$4\r\nAUTH\r\n$N\r\n<pw>\r\n` carries
            // four CRLFs.
            read_frame(&mut sock, &mut buf, &recorded_inner, 4).await;
            let _ = sock.write_all(b"+OK\r\n").await;
        }

        // One data request, then a single `+OK` reply.
        let min_crlf = if auth { 8 } else { 2 };
        read_frame(&mut sock, &mut buf, &recorded_inner, min_crlf).await;
        let _ = sock.write_all(b"+OK\r\n").await;

        // Hold the socket briefly so the client can drain the reply.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = sock.shutdown().await;
    });
    (recorded, handle)
}

#[tokio::test(flavor = "multi_thread")]
async fn unix_backend_round_trip() {
    let path = scratch_sock_path("rt");
    let listener = UnixListener::bind(&path).expect("bind unix listener");
    let (recorded, backend) = fake_unix_backend(listener, false);

    let (tx, rx) = mpsc::channel::<OutboundRequest>(4);
    let supervisor = spawn_backend_supervisor_unix_for_testing(
        path.clone(),
        rx,
        dynomite::conf::DataStore::Valkey,
        None,
    );

    // Push a SET request and await its reply on a per-request
    // responder channel.
    let (rsp_tx, mut rsp_rx) = mpsc::channel::<OutboundEnvelope>(1);
    let req = OutboundRequest {
        bytes: b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n".to_vec(),
        req_id: 1,
        responder: rsp_tx,
        span: tracing::Span::none(),
        ty: DmsgType::Req,
        target_peer_idx: None,
    };
    tx.send(req).await.expect("queue request");

    let env = tokio::time::timeout(Duration::from_secs(5), rsp_rx.recv())
        .await
        .expect("response did not arrive within 5s")
        .expect("responder channel closed without a reply");

    assert_eq!(env.req_id, 1, "reply tagged with the wrong request id");
    assert_eq!(
        envelope_bytes(&env),
        b"+OK\r\n",
        "unexpected reply payload over the unix transport",
    );

    // The fake backend must have observed the SET command bytes
    // arriving over the Unix socket.
    let seen = recorded.lock().await.clone();
    let seen_str = String::from_utf8_lossy(&seen);
    assert!(
        seen_str.contains("SET"),
        "backend did not receive SET over unix socket: {seen_str:?}",
    );

    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(5), supervisor).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), backend).await;
    let _ = std::fs::remove_file(&path);
}

#[tokio::test(flavor = "multi_thread")]
async fn unix_backend_auth_round_trip() {
    let path = scratch_sock_path("auth");
    let listener = UnixListener::bind(&path).expect("bind unix listener");
    let (recorded, backend) = fake_unix_backend(listener, true);

    let (tx, rx) = mpsc::channel::<OutboundRequest>(4);
    let supervisor = spawn_backend_supervisor_unix_for_testing(
        path.clone(),
        rx,
        dynomite::conf::DataStore::Valkey,
        Some("hunter2".to_string()),
    );

    let (rsp_tx, mut rsp_rx) = mpsc::channel::<OutboundEnvelope>(1);
    let req = OutboundRequest {
        bytes: b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n".to_vec(),
        req_id: 7,
        responder: rsp_tx,
        span: tracing::Span::none(),
        ty: DmsgType::Req,
        target_peer_idx: None,
    };
    tx.send(req).await.expect("queue request");

    let env = tokio::time::timeout(Duration::from_secs(5), rsp_rx.recv())
        .await
        .expect("response did not arrive within 5s")
        .expect("responder channel closed without a reply");

    assert_eq!(env.req_id, 7);
    assert_eq!(envelope_bytes(&env), b"+OK\r\n");

    // The AUTH handshake must have crossed the unix transport
    // before (and as a prefix of) the GET request bytes.
    let seen = recorded.lock().await.clone();
    let seen_str = String::from_utf8_lossy(&seen);
    assert!(
        seen_str.contains("AUTH") && seen_str.contains("hunter2"),
        "backend did not receive AUTH over unix socket: {seen_str:?}",
    );
    let auth_pos = seen_str.find("AUTH").expect("AUTH present");
    let get_pos = seen_str.find("GET").expect("GET present");
    assert!(
        auth_pos < get_pos,
        "AUTH must precede the data request over unix: {seen_str:?}",
    );

    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(5), supervisor).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), backend).await;
    let _ = std::fs::remove_file(&path);
}

/// A pool whose single `servers:` entry is a Unix-socket path must
/// parse cleanly and surface `is_unix()`. The binary no longer
/// rejects such a config with a `BadConfig` error; the supervisor
/// dials the path over a `UnixStream`.
#[test]
fn unix_datastore_config_parses() {
    let yaml = "\
p:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  stats_listen: 127.0.0.1:22222
  tokens: '101134286'
  servers:
  - /scratch/r.sock:1
  data_store: 0
";
    let cfg = dynomite::conf::Config::parse_str(yaml).expect("unix-socket pool should parse");
    let servers = cfg.pool().servers.as_ref().expect("servers present");
    let first = servers.entries().first().expect("one server entry");
    assert!(
        first.is_unix(),
        "server entry should be classified as a unix socket",
    );
    assert_eq!(first.host(), "/scratch/r.sock");
    assert_eq!(first.port(), 0);
}
