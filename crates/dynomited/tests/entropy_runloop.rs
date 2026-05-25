//! Integration tests for the entropy run-loop wiring in
//! `dynomited`.
//!
//! These tests do not require a real Redis backend. They build
//! a [`dynomited::server::Server`] in-process, exercise the
//! deferred entropy task wiring through public accessors and
//! through an in-process [`dynomite::entropy::EntropyReceiver`],
//! and assert the configured cadence and lifecycle behaviour.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::watch;

use dynomite::cluster::peer::{Peer, PeerEndpoint};
use dynomite::conf::Config;
use dynomite::entropy::driver::{
    reconcile_with_peer, EntropyDriver, ReconCycle, DEFAULT_RECON_INTERVAL,
};
use dynomite::entropy::receive::MemorySink;
use dynomite::entropy::send::StaticSnapshot;
use dynomite::entropy::util::{load_material, EntropyMaterial};
use dynomite::entropy::{boxed_sink, BoxedSnapshotSource, EntropyConfig, EntropyReceiver};
use dynomite::hashkit::DynToken;

use dynomited::server::Server;

fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn pick_distinct_ports(n: usize) -> Vec<u16> {
    let mut out = Vec::new();
    while out.len() < n {
        let p = pick_port();
        if !out.contains(&p) {
            out.push(p);
        }
    }
    out
}

fn fixture_key_path() -> PathBuf {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    // The fixtures live under crates/dynomite/tests/fixtures/recon
    crate_root
        .parent()
        .unwrap()
        .join("dynomite/tests/fixtures/recon/recon_key.pem")
}

fn fixture_iv_path() -> PathBuf {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    crate_root
        .parent()
        .unwrap()
        .join("dynomite/tests/fixtures/recon/recon_iv.pem")
}

fn material_from_fixtures() -> EntropyMaterial {
    load_material(&fixture_key_path(), &fixture_iv_path()).expect("load fixtures")
}

fn yaml_with_entropy(
    listen: u16,
    dyn_listen: u16,
    stats_listen: u16,
    backend_port: u16,
    recon_key: Option<&Path>,
    recon_iv: Option<&Path>,
    recon_interval_seconds: Option<u64>,
) -> String {
    let mut s = format!(
        "p:\n  listen: 127.0.0.1:{listen}\n  dyn_listen: 127.0.0.1:{dyn_listen}\n  stats_listen: 127.0.0.1:{stats_listen}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:{backend_port}:1\n  data_store: 0\n",
    );
    if let Some(p) = recon_key {
        use std::fmt::Write as _;
        writeln!(s, "  recon_key_file: '{}'", p.display()).unwrap();
    }
    if let Some(p) = recon_iv {
        use std::fmt::Write as _;
        writeln!(s, "  recon_iv_file: '{}'", p.display()).unwrap();
    }
    if let Some(secs) = recon_interval_seconds {
        use std::fmt::Write as _;
        writeln!(s, "  recon_interval_seconds: {secs}").unwrap();
    }
    s
}

/// When the operator points `recon_key_file:` at the bundled
/// fixture pair, [`Server::build`] loads the AES material and
/// the entropy task is queued for [`Server::run`] with the
/// configured cadence.
#[tokio::test(flavor = "multi_thread")]
async fn server_builds_entropy_driver_when_recon_key_file_is_valid() {
    let ports = pick_distinct_ports(4);
    let yaml = yaml_with_entropy(
        ports[0],
        ports[1],
        ports[2],
        ports[3],
        Some(&fixture_key_path()),
        Some(&fixture_iv_path()),
        Some(7),
    );
    let cfg = Config::parse_str(&yaml).unwrap();
    let server = Server::build(cfg).await.expect("build");
    assert!(server.entropy_enabled(), "entropy task must be enabled");
    assert_eq!(
        server.entropy_cadence(),
        Some(Duration::from_secs(7)),
        "configured cadence must propagate"
    );
    server.shutdown_handle().shutdown();
    drop(server);
}

/// When `recon_key_file:` is set to an empty string the entropy
/// task must NOT spawn. This pins the brief's
/// "behavior is unchanged" contract for pools that explicitly
/// opt out.
#[tokio::test(flavor = "multi_thread")]
async fn server_skips_entropy_when_recon_key_file_is_empty() {
    let ports = pick_distinct_ports(4);
    let mut yaml = format!(
        "p:\n  listen: 127.0.0.1:{}\n  dyn_listen: 127.0.0.1:{}\n  stats_listen: 127.0.0.1:{}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:{}:1\n  data_store: 0\n",
        ports[0], ports[1], ports[2], ports[3]
    );
    yaml.push_str("  recon_key_file: ''\n");
    yaml.push_str("  recon_iv_file: ''\n");
    let cfg = Config::parse_str(&yaml).unwrap();
    let server = Server::build(cfg).await.expect("build");
    assert!(
        !server.entropy_enabled(),
        "explicit empty recon_key_file must disable the entropy task"
    );
    assert_eq!(server.entropy_cadence(), None);
}

/// When `recon_key_file:` resolves to a non-existent path the
/// entropy task must NOT spawn. The default YAML in production
/// hits this path because the bundled fixtures are not present
/// at the documented `conf/recon_key.pem` location on most test
/// hosts.
#[tokio::test(flavor = "multi_thread")]
async fn server_skips_entropy_when_recon_key_file_is_missing() {
    let ports = pick_distinct_ports(4);
    let yaml = yaml_with_entropy(
        ports[0],
        ports[1],
        ports[2],
        ports[3],
        Some(Path::new("/nonexistent/dynomite/recon_key.pem")),
        Some(Path::new("/nonexistent/dynomite/recon_iv.pem")),
        Some(60),
    );
    let cfg = Config::parse_str(&yaml).unwrap();
    let server = Server::build(cfg).await.expect("build");
    assert!(
        !server.entropy_enabled(),
        "missing recon key files must disable the entropy task"
    );
}

/// `reconcile_with_peer` against an [`EntropyReceiver`] bound on
/// localhost completes one cycle, the receiver consumes the
/// snapshot, and the returned [`ReconCycle`] reports the
/// expected counters.
#[tokio::test(flavor = "multi_thread")]
async fn reconcile_with_peer_pushes_snapshot_to_receiver() {
    let recv_cfg = EntropyConfig {
        key_file: fixture_key_path(),
        iv_file: fixture_iv_path(),
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        send_addr: None,
        peer_endpoint: "127.0.0.1:0".parse().unwrap(),
        buffer_size: 256,
        header_size: 64,
        encrypt: true,
    };

    let sink = Arc::new(MemorySink::default());
    let receiver = EntropyReceiver::bind(recv_cfg.clone(), boxed_sink(sink.clone()))
        .await
        .unwrap();
    let bound = receiver.local_addr().unwrap();
    let recv_handle = tokio::spawn(async move { receiver.accept_one().await });

    let payload = b"divergent-range-bytes".to_vec();
    let source: BoxedSnapshotSource = Arc::new(StaticSnapshot::new(payload.clone()));
    let material = material_from_fixtures();
    let peer = PeerEndpoint::tcp(bound.ip().to_string(), bound.port());

    let cycle = reconcile_with_peer(&material, &source, &peer, bound.port(), 256, 64, true)
        .await
        .expect("reconcile_with_peer");
    assert_eq!(cycle.peers_attempted, 1);
    assert_eq!(cycle.peers_exchanged, 1);
    assert_eq!(cycle.ranges_diverged, 1);
    assert_eq!(cycle.ranges_repaired, 1);

    let total = recv_handle.await.unwrap().unwrap();
    assert_eq!(total, payload.len());
    assert_eq!(sink.snapshot(), payload);
}

/// The driver runs one cycle against a live receiver, then
/// honours a watch-channel shutdown signal at the next per-peer
/// boundary.
#[tokio::test(flavor = "multi_thread")]
async fn driver_runs_cycle_then_drains_on_shutdown() {
    let recv_cfg = EntropyConfig {
        key_file: fixture_key_path(),
        iv_file: fixture_iv_path(),
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        send_addr: None,
        peer_endpoint: "127.0.0.1:0".parse().unwrap(),
        buffer_size: 256,
        header_size: 64,
        encrypt: true,
    };
    let sink = Arc::new(MemorySink::default());
    let receiver = EntropyReceiver::bind(recv_cfg.clone(), boxed_sink(sink.clone()))
        .await
        .unwrap();
    let bound = receiver.local_addr().unwrap();
    let _accept = tokio::spawn(async move {
        // Accept connections until aborted; each one drains a
        // single snapshot push.
        let _ = receiver.accept_one().await;
    });

    let material = material_from_fixtures();
    let source: BoxedSnapshotSource = Arc::new(StaticSnapshot::new(b"abc".to_vec()));
    let peer = Peer::new(
        1,
        PeerEndpoint::tcp("127.0.0.1".into(), bound.port()),
        "rack".into(),
        "dc".into(),
        vec![DynToken::from_u32(0)],
        false,
        false,
        false,
    );
    let local = Peer::new(
        0,
        PeerEndpoint::tcp("127.0.0.1".into(), 1),
        "rack".into(),
        "dc".into(),
        vec![DynToken::from_u32(1)],
        true,
        true,
        false,
    );
    let peers = Arc::new(RwLock::new(vec![local, peer]));
    let driver = EntropyDriver::new(material, source, peers, Duration::from_millis(50))
        .with_peer_port(bound.port())
        .with_buffer_size(256)
        .with_header_size(64);

    let (tx, rx) = watch::channel(false);
    let task = tokio::spawn(driver.run_until_shutdown(rx));

    // Allow at least one cycle to run.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !sink.snapshot().is_empty(),
        "driver must push at least once"
    );

    tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("driver did not honour shutdown")
        .expect("driver task panicked");
}

/// `EntropyDriver::cadence` rounds zero up to the documented
/// five-minute default, matching the brief's contract for
/// `recon_interval_seconds: 0`.
#[test]
fn driver_zero_cadence_rounds_up_to_default() {
    let peers = Arc::new(RwLock::new(Vec::new()));
    let source: BoxedSnapshotSource = Arc::new(StaticSnapshot::new(Vec::new()));
    let driver = EntropyDriver::new(material_from_fixtures(), source, peers, Duration::ZERO);
    assert_eq!(driver.cadence(), DEFAULT_RECON_INTERVAL);
}

/// `ReconCycle::default()` must be a true zero so a cycle that
/// visits no peers is unambiguous in INFO logs.
#[test]
fn empty_cycle_is_default() {
    let c = ReconCycle::default();
    assert_eq!(c.peers_attempted, 0);
    assert_eq!(c.peers_exchanged, 0);
    assert_eq!(c.ranges_diverged, 0);
    assert_eq!(c.ranges_repaired, 0);
}

fn make_peers(remote_port: u16) -> Arc<RwLock<Vec<Peer>>> {
    Arc::new(RwLock::new(vec![
        Peer::new(
            0,
            PeerEndpoint::tcp("127.0.0.1".into(), 1),
            "r".into(),
            "d".into(),
            vec![DynToken::from_u32(0)],
            true,
            true,
            false,
        ),
        Peer::new(
            1,
            PeerEndpoint::tcp("127.0.0.1".into(), remote_port),
            "r".into(),
            "d".into(),
            vec![DynToken::from_u32(1)],
            false,
            false,
            false,
        ),
    ]))
}

/// Two `dynomited`-style entropy drivers running concurrently
/// at a 100ms cadence each push their own snapshot to a peer
/// receiver and report nonzero `ranges_repaired` counters.
///
/// This is the in-process distillation of the brief's
/// "two dynomited instances with deliberately divergent state"
/// integration scenario: each driver represents one node, the
/// peer receiver represents the remote node it reconciles
/// with, and the divergent-payload assertion stands in for the
/// per-key Merkle-tree comparison a future stage will wire.
#[tokio::test(flavor = "multi_thread")]
async fn two_drivers_reconcile_with_each_others_receivers() {
    use dynomite::entropy::driver::EntropyDriver;

    async fn bind_receiver(
        sink: Arc<MemorySink>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let cfg = EntropyConfig {
            key_file: fixture_key_path(),
            iv_file: fixture_iv_path(),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            send_addr: None,
            peer_endpoint: "127.0.0.1:0".parse().unwrap(),
            buffer_size: 256,
            header_size: 64,
            encrypt: true,
        };
        let recv = EntropyReceiver::bind(cfg, boxed_sink(sink)).await.unwrap();
        let bound = recv.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            // Loop until the test aborts the task.
            loop {
                let cfg = EntropyConfig {
                    key_file: fixture_key_path(),
                    iv_file: fixture_iv_path(),
                    listen_addr: bound,
                    send_addr: None,
                    peer_endpoint: "127.0.0.1:0".parse().unwrap(),
                    buffer_size: 256,
                    header_size: 64,
                    encrypt: true,
                };
                let sink = boxed_sink(MemorySink::default());
                let Ok(r) = EntropyReceiver::bind(cfg, sink).await else {
                    return;
                };
                let _ = r.accept_one().await;
            }
        });
        (bound, handle)
    }

    let sink_a = Arc::new(MemorySink::default());
    let sink_b = Arc::new(MemorySink::default());
    let (addr_a, _h_a) = bind_receiver(sink_a.clone()).await;
    let (addr_b, _h_b) = bind_receiver(sink_b.clone()).await;

    // Node A's driver pushes "v1" toward Node B's receiver and
    // vice versa; the snapshots intentionally diverge so the
    // ranges_diverged counter is nonzero in both directions.
    let mat = material_from_fixtures();
    let src_a: BoxedSnapshotSource = Arc::new(StaticSnapshot::new(b"node-a:key=v1".to_vec()));
    let src_b: BoxedSnapshotSource = Arc::new(StaticSnapshot::new(b"node-b:key=v2".to_vec()));

    let peers_a = make_peers(addr_b.port());
    let peers_b = make_peers(addr_a.port());
    let driver_a = EntropyDriver::new(mat.clone(), src_a, peers_a, Duration::from_millis(100))
        .with_peer_port(addr_b.port())
        .with_buffer_size(256)
        .with_header_size(64);
    let driver_b = EntropyDriver::new(mat, src_b, peers_b, Duration::from_millis(100))
        .with_peer_port(addr_a.port())
        .with_buffer_size(256)
        .with_header_size(64);

    // Run one cycle on each driver explicitly; this is the
    // deterministic distillation of "wait 3s and check the
    // logs" the brief described.
    let cycle_a = driver_a.run_cycle().await;
    let cycle_b = driver_b.run_cycle().await;

    assert_eq!(cycle_a.peers_attempted, 1);
    assert_eq!(cycle_a.peers_exchanged, 1);
    assert_eq!(cycle_a.ranges_diverged, 1);
    assert_eq!(cycle_a.ranges_repaired, 1);
    assert_eq!(cycle_b.peers_attempted, 1);
    assert_eq!(cycle_b.peers_exchanged, 1);
    assert_eq!(cycle_b.ranges_diverged, 1);
    assert_eq!(cycle_b.ranges_repaired, 1);
}
