//! The backend driver runs a repair-script request on its datastore
//! connection against a real `valkey-server` and returns the script's
//! reply, with the timestamp metadata the repair scripts keep in
//! `._add-set` / `._rem-set`.
//!
//! Skips when `valkey-server` is not on `PATH`.

use std::net::{SocketAddr, TcpListener as StdListener};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use dynomite::msg::{Msg, MsgParseResult, MsgType};
use dynomite::net::server::OutboundRequest;
use dynomite::net::OutboundEnvelope;
use dynomite::proto::dnode::DmsgType;
use dynomite::proto::redis::redis_parse_req;
use dynomite::proto::redis::repair::request::{encode_command, ScriptRequest};
use dynomited::server::spawn_backend_supervisor_for_testing;
use tokio::sync::mpsc;

struct Valkey(Child, SocketAddr);

impl Drop for Valkey {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn valkey() -> Option<Valkey> {
    let port = StdListener::bind("127.0.0.1:0")
        .ok()?
        .local_addr()
        .ok()?
        .port();
    let child = Command::new("valkey-server")
        .args([
            "--port",
            &port.to_string(),
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().ok()?;
    for _ in 0..100 {
        if std::net::TcpStream::connect(addr).is_ok() {
            return Some(Valkey(child, addr));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

fn wire(cmd: &str) -> Vec<u8> {
    encode_command(&cmd.split_whitespace().collect::<Vec<_>>())
}

fn script(cmd: &str, ts: u64) -> Vec<u8> {
    let mut m = Msg::new(1, MsgType::Unknown, true);
    assert_eq!(redis_parse_req(&mut m, &wire(cmd)), MsgParseResult::Ok);
    ScriptRequest::from_request(&m, ts)
        .expect("repairable")
        .encode()
}

async fn send(tx: &mpsc::Sender<OutboundRequest>, id: u64, bytes: Vec<u8>) -> Vec<u8> {
    let (rtx, mut rrx) = mpsc::channel::<OutboundEnvelope>(1);
    tx.send(OutboundRequest {
        bytes,
        req_id: id,
        responder: rtx,
        span: tracing::Span::none(),
        ty: DmsgType::Req,
        target_peer_idx: Some(7),
        deadline: None,
    })
    .await
    .expect("backend channel");
    let env = tokio::time::timeout(Duration::from_secs(5), rrx.recv())
        .await
        .expect("reply in time")
        .expect("reply");
    assert_eq!(env.req_id, id);
    assert_eq!(env.source_peer_idx, Some(7));
    env.rsp
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect()
}

#[tokio::test]
async fn repair_scripts_run_on_the_datastore_connection() {
    let Some(vk) = valkey() else {
        eprintln!("valkey-server not on PATH; skipping");
        return;
    };
    let (tx, rx) = mpsc::channel(16);
    let _sup = spawn_backend_supervisor_for_testing(
        vk.1,
        rx,
        dynomite::conf::DataStore::Valkey,
        None,
        10_000,
    );
    // Plain request, then a script: the script waits for the pipelined reply.
    assert_eq!(send(&tx, 1, wire("SET plain p")).await, b"+OK\r\n");
    assert_eq!(send(&tx, 2, script("SET k v1", 10)).await, b"+OK\r\n");
    assert_eq!(
        send(&tx, 3, wire("ZSCORE ._add-set k")).await,
        b"$2\r\n10\r\n"
    );
    // Stale write is refused with -1.
    assert_eq!(send(&tx, 4, script("SET k old", 5)).await, b":-1\r\n");
    assert_eq!(
        send(&tx, 5, script("GET k", 0)).await,
        b"*3\r\n$1\r\nE\r\n$2\r\n10\r\n$2\r\nv1\r\n"
    );
    // Delete records a tombstone; the read reports it.
    assert_eq!(send(&tx, 6, script("DEL k", 20)).await, b":1\r\n");
    assert_eq!(
        send(&tx, 7, script("GET k", 0)).await,
        b"*3\r\n$1\r\nR\r\n$2\r\n20\r\n$-1\r\n"
    );
    // Field level: HSET then HGET carries the field timestamp.
    assert_eq!(send(&tx, 8, script("HSET h f v", 30)).await, b":1\r\n");
    assert_eq!(
        send(&tx, 9, script("HGET h f", 0)).await,
        b"*3\r\n$1\r\nE\r\n$2\r\n30\r\n$1\r\nv\r\n"
    );
    // Missing key.
    assert_eq!(
        send(&tx, 10, script("GET nokey", 0)).await,
        b"*3\r\n$1\r\nX\r\n:0\r\n$-1\r\n"
    );
    // An error inside the script aborts it and is the reply.
    assert_eq!(send(&tx, 11, wire("SET str s")).await, b"+OK\r\n");
    let r = send(&tx, 12, script("HSET str f v", 40)).await;
    assert!(
        r.starts_with(b"-WRONGTYPE"),
        "{}",
        String::from_utf8_lossy(&r)
    );
    // The connection is still usable afterwards.
    assert_eq!(send(&tx, 13, wire("GET plain")).await, b"$1\r\np\r\n");
}
