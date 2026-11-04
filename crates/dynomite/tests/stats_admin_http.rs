//! Admin command surface of the stats HTTP server: every command's
//! reply body and its effect on the pool, the log level and the
//! read-repair flag. Expected bodies were captured from the reference
//! engine.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dynomite::cluster::dispatch::{ClusterDispatcher, DispatchPlan};
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{DynState, PoolConfig, ServerPool};
use dynomite::core::log::current_level;
use dynomite::hashkit::DynToken;
use dynomite::msg::{ConsistencyLevel, Msg, MsgType};
use dynomite::stats::{Snapshot, StatsServer};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const HELP: &[u8] = b"/info\n/help\n/ping\n/cluster_describe\n/setloglevel/<0-11>\n\
/loglevelup\n/logleveldown\n/historeset\n/get_consistency\n\
/set_consistency/<read|write>/<dc_one|dc_quorum|dc_safe_quorum>\n\
/get_timeout_factor\n/set_timeout_factor/<1-10>\n/peer/<up|down|reset>\n\
/state/<get_state|standby|writes_only|normal|resuming>\n\n";

fn peer(idx: u32, host: &str, rack: &str, tok: u32, local: bool) -> Peer {
    let mut p = Peer::new(
        idx,
        PeerEndpoint::tcp(host.into(), 18101),
        rack.into(),
        "dc1".into(),
        vec![DynToken::from_u32(tok)],
        local,
        true,
        false,
    );
    p.set_state(PeerState::Normal, 0);
    p
}

fn pool() -> Arc<ServerPool> {
    let cfg = PoolConfig {
        dc: "dc1".into(),
        rack: "rack1".into(),
        timeout_ms: 100,
        ..PoolConfig::default()
    };
    Arc::new(ServerPool::new(
        cfg,
        vec![
            peer(0, "127.0.0.1", "rack1", 437_425_602, true),
            peer(1, "127.0.0.2", "rack1", 100, false),
            peer(2, "127.0.0.3", "rack2", 200, false),
        ],
    ))
}

async fn raw(addr: SocketAddr, req: &[u8]) -> (String, Vec<u8>) {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    s.write_all(req).await.expect("write");
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut buf))
        .await
        .expect("reply in time")
        .expect("read");
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("header terminator");
    let head = String::from_utf8(buf[..split].to_vec()).expect("ascii header");
    (head, buf[split + 4..].to_vec())
}

async fn get(addr: SocketAddr, path: &str) -> Vec<u8> {
    let (head, body) = raw(
        addr,
        format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes(),
    )
    .await;
    assert!(head.starts_with("HTTP/1.1 200 OK"), "{path}: {head}");
    let len: usize = head
        .lines()
        .find_map(|l| l.strip_prefix("Content-Length: "))
        .and_then(|v| v.trim().parse().ok())
        .expect("content-length");
    assert_eq!(len, body.len(), "{path}: content-length");
    body
}

fn plan_consistency(pool: &Arc<ServerPool>, ty: MsgType) -> ConsistencyLevel {
    let disp = ClusterDispatcher::new(pool.clone());
    let mut req = Msg::new(1, ty, true);
    req.flags_mut().is_read = matches!(ty, MsgType::ReqRedisGet);
    match disp.plan(&req, b"key") {
        DispatchPlan::Replicas { consistency, .. } => consistency,
        other => panic!("expected Replicas, got {other:?}"),
    }
}

fn start() -> (SocketAddr, Arc<ServerPool>, Arc<AtomicUsize>) {
    let pool = pool();
    let resets = Arc::new(AtomicUsize::new(0));
    let r = resets.clone();
    let server = StatsServer::bind(
        "127.0.0.1:0".parse().expect("addr"),
        Arc::new(Mutex::new(Snapshot::default())),
    )
    .expect("bind")
    .with_admin_pool(pool.clone())
    .with_histo_reset(Arc::new(move || {
        r.fetch_add(1, Ordering::Relaxed);
    }));
    let addr = server.local_addr().expect("local addr");
    tokio::spawn(server.run());
    (addr, pool, resets)
}

#[tokio::test]
async fn snapshot_help_ping_and_unknown_commands() {
    let (addr, _, _) = start();
    // Snapshot views.
    for p in ["/", "/info"] {
        assert!(get(addr, p).await.starts_with(b"{"), "{p}");
    }
    assert_eq!(get(addr, "/help").await, HELP);
    for p in [
        "/ping",
        "/nothing",
        "/info/extra",
        "/stateful",
        "/peer/bogus",
    ] {
        assert_eq!(get(addr, p).await, b"OK\r\n", "{p}");
    }
}

#[tokio::test]
async fn log_level_commands() {
    let (addr, _, _) = start();
    // Log level.
    assert_eq!(get(addr, "/setloglevel/7").await, b"OK\r\n");
    assert_eq!(current_level(), 7);
    assert_eq!(get(addr, "/loglevelup").await, b"OK\r\n");
    assert_eq!(current_level(), 8);
    assert_eq!(get(addr, "/logleveldown").await, b"OK\r\n");
    assert_eq!(get(addr, "/logleveldown").await, b"OK\r\n");
    assert_eq!(current_level(), 6);
    assert_eq!(get(addr, "/setloglevel/99").await, b"OK\r\n");
    assert_eq!(current_level(), 11);
    assert_eq!(get(addr, "/setloglevel/-3").await, b"OK\r\n");
    assert_eq!(current_level(), 0);
    // No `/` after the command name: unknown, level untouched.
    for p in ["/setloglevel", "/setloglevelx"] {
        assert_eq!(get(addr, p).await, b"OK\r\n");
        assert_eq!(current_level(), 0, "{p}");
    }
    get(addr, "/setloglevel/5").await;
}

#[tokio::test]
async fn histogram_reset_invokes_the_hook() {
    let (addr, _, resets) = start();
    // Histogram reset.
    assert_eq!(get(addr, "/historeset").await, b"OK\r\n");
    assert_eq!(resets.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn consistency_and_read_repair_commands() {
    let (addr, pool, _) = start();
    // Consistency.
    assert_eq!(
        get(addr, "/get_consistency").await,
        b"Read Consistency: DC_ONE\r\nWrite Consistency: DC_ONE\r\n"
    );
    assert_eq!(
        get(addr, "/set_consistency/read/DC_SAFE_QUORUM").await,
        b"OK\r\n"
    );
    assert_eq!(
        get(addr, "/set_consistency/write/dc_quorum").await,
        b"OK\r\n"
    );
    for p in [
        "/set_consistency/read/bogus",
        "/set_consistency/x",
        "/set_consistency/write/dc_each_safe_quorum",
    ] {
        assert_eq!(get(addr, p).await, b"OK\r\n", "{p}");
    }
    assert_eq!(
        get(addr, "/get_consistency").await,
        b"Read Consistency: DC_SAFE_QUORUM\r\nWrite Consistency: DC_QUORUM\r\n"
    );
    assert_eq!(
        plan_consistency(&pool, MsgType::ReqRedisGet),
        ConsistencyLevel::DcSafeQuorum
    );
    assert_eq!(
        plan_consistency(&pool, MsgType::ReqRedisSet),
        ConsistencyLevel::DcQuorum
    );

    // Read repairs: the flag follows the command; the effective gate
    // also needs both consistencies above DC_ONE.
    assert_eq!(
        get(addr, "/read_repairs/enable").await,
        b"Read Repairs: ENABLED\r\n"
    );
    assert!(dynomite::msg::is_read_repairs_enabled());
    assert!(pool.is_read_repairs_enabled());
    get(addr, "/set_consistency/read/dc_one").await;
    assert!(!pool.is_read_repairs_enabled());
    get(addr, "/set_consistency/read/dc_quorum").await;
    assert!(pool.is_read_repairs_enabled());
    assert_eq!(get(addr, "/read_repairs/x").await, b"OK\r\n");
    assert!(dynomite::msg::is_read_repairs_enabled());
    assert_eq!(
        get(addr, "/read_repairs/disable").await,
        b"Read Repairs: DISABLED\r\n"
    );
    assert!(!pool.is_read_repairs_enabled());
}

#[tokio::test]
async fn timeout_factor_is_clamped() {
    let (addr, pool, _) = start();
    // Timeout factor: clamped to 1..=10, `%d` style parsing.
    assert_eq!(
        get(addr, "/get_timeout_factor").await,
        b"Timeout factor: 1\n"
    );
    for (arg, want) in [
        ("99", 10u8),
        ("0", 1),
        ("3abc", 3),
        ("abc", 1),
        ("7", 7),
        ("200", 1),
        ("266", 10),
    ] {
        let p = format!("/set_timeout_factor/{arg}");
        assert_eq!(get(addr, &p).await, b"OK\r\n", "{p}");
        assert_eq!(pool.timeout_factor(), want, "{p}");
        assert_eq!(
            get(addr, "/get_timeout_factor").await,
            format!("Timeout factor: {want}\n").as_bytes()
        );
    }
    assert_eq!(pool.request_timeout_ms(), 1_000);
    assert_eq!(get(addr, "/set_timeout_factor").await, b"OK\r\n");
    assert_eq!(pool.timeout_factor(), 10);
}

#[tokio::test]
async fn peer_state_commands() {
    let (addr, pool, _) = start();
    // Peer state.
    let state_of = |i: usize| pool.peers().read()[i].state();
    assert_eq!(get(addr, "/peer/down/127.0.0.2").await, b"OK\r\n");
    assert_eq!(state_of(1), PeerState::Down);
    assert_eq!(get(addr, "/peer/up/127.0.0.2").await, b"OK\r\n");
    assert_eq!(state_of(1), PeerState::Normal);
    assert_eq!(get(addr, "/peer/reset/127.0.0.3").await, b"OK\r\n");
    assert_eq!(state_of(2), PeerState::Reset);
    assert_eq!(get(addr, "/peer/down/nobody").await, b"OK\r\n");
    assert_eq!(state_of(1), PeerState::Normal);
    // `all` resets every peer, whatever the requested state.
    assert_eq!(get(addr, "/peer/up/all").await, b"OK\r\n");
    for i in 0..3 {
        assert_eq!(state_of(i), PeerState::Reset, "peer {i}");
    }
}

#[tokio::test]
async fn local_state_commands() {
    let (addr, pool, _) = start();
    let state_of = |i: usize| pool.peers().read()[i].state();
    // Local node state.
    assert_eq!(get(addr, "/state/get_state").await, b"State: NORMAL\n");
    for (cmd, want, name) in [
        ("standby", DynState::Standby, "STANDBY"),
        ("writes_only", DynState::WritesOnly, "WRITES_ONLY"),
        ("resuming", DynState::Resuming, "RESUMING"),
        ("normal", DynState::Normal, "NORMAL"),
    ] {
        assert_eq!(get(addr, &format!("/state/{cmd}")).await, b"OK\r\n");
        assert_eq!(pool.local_state(), want);
        assert_eq!(
            get(addr, "/state/get_state").await,
            format!("State: {name}\n").as_bytes()
        );
    }
    assert_eq!(state_of(0), PeerState::Normal);
    assert_eq!(get(addr, "/state/bogus").await, b"OK\r\n");
    assert_eq!(pool.local_state(), DynState::Normal);
}

#[tokio::test]
async fn cluster_describe_and_bad_request_line() {
    let (addr, _, _) = start();
    // Cluster description: every DC, rack and node in the reference
    // JSON layout.
    assert_eq!(
        get(addr, "/cluster_describe").await,
        b"{\"dcs\": [{\"name\":\"dc1\",\"racks\": [{\"name\":\"rack1\",\"servers\": \
[{\"name\":\"127.0.0.1\",\"host\":\"127.0.0.1\",\"port\":18101,\n\"token\":437425602\n},\
{\"name\":\"127.0.0.2\",\"host\":\"127.0.0.2\",\"port\":18101,\n\"token\":100\n}]},\
{\"name\":\"rack2\",\"servers\": [{\"name\":\"127.0.0.3\",\"host\":\"127.0.0.3\",\
\"port\":18101,\n\"token\":200\n}]}]}]}\n"
            .as_slice()
    );

    // A request line that is not `GET <path> HTTP/1.x` is refused.
    let (head, body) = raw(addr, b"GET /info HTTP/2.0\r\n\r\n").await;
    assert!(head.starts_with("HTTP/1.1 400 Bad Request"), "{head}");
    assert_eq!(body, b"OK\r\n");
}

#[tokio::test]
async fn admin_commands_without_a_pool_are_unavailable() {
    let server = StatsServer::bind(
        "127.0.0.1:0".parse().expect("addr"),
        Arc::new(Mutex::new(Snapshot::default())),
    )
    .expect("bind");
    let addr = server.local_addr().expect("local addr");
    tokio::spawn(server.run());
    let (head, _) = raw(addr, b"GET /get_consistency HTTP/1.1\r\n\r\n").await;
    assert!(head.starts_with("HTTP/1.1 503"), "{head}");
    assert_eq!(get(addr, "/ping").await, b"OK\r\n");
}
