//! Integration tests that exercise the engine end-to-end.

use std::io::{BufRead, BufReader};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use rand::rngs::SmallRng;
use rand::SeedableRng;

use dyniak_bench::config::{
    Config, DriverConfig, DriverKind, KeyGenConfig, OpsConfig, RateConfig, RpsTable, RunConfig,
    ValGenConfig,
};
use dyniak_bench::engine::Engine;
use dyniak_bench::keygen::KeyGen;
use dyniak_bench::valgen::ValGen;

/// Find a free TCP port on the loopback by binding to `:0` and
/// reading the port back from the bound socket.
fn pick_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn redis_in_path() -> bool {
    Command::new("valkey-server")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

struct RedisServer {
    child: Child,
    port: u16,
}

impl RedisServer {
    fn spawn() -> Option<Self> {
        if !redis_in_path() {
            return None;
        }
        let port = pick_port();
        let child = Command::new("valkey-server")
            .args([
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
                "--bind",
                "127.0.0.1",
                "--protected-mode",
                "no",
                "--daemonize",
                "no",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        // Wait for the port to accept connections.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if Instant::now() > deadline {
                let mut me = Self { child, port };
                me.kill();
                return None;
            }
            if TcpStream::connect_timeout(
                &format!("127.0.0.1:{port}").parse().unwrap(),
                Duration::from_millis(200),
            )
            .is_ok()
            {
                return Some(Self { child, port });
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        self.kill();
    }
}

fn make_redis_config(port: u16, duration: &str, ops: OpsConfig, out_dir: &str) -> Config {
    Config {
        run: RunConfig {
            duration: duration.into(),
            concurrent: 4,
            rate: RateConfig::Max,
            out_dir: out_dir.into(),
            report_interval: "200ms".into(),
        },
        driver: DriverConfig {
            kind: DriverKind::Redis,
            host: "127.0.0.1".into(),
            port,
            timeout_ms: 1000,
            bucket: "bench".into(),
            encoding: dyniak_bench::config::HttpEncoding::Json,
        },
        ops,
        keygen: KeyGenConfig {
            kind: "uniform".into(),
            max: 10_000,
            shape: 1.5,
            mean: 0.0,
            stddev: 0.0,
            key: String::new(),
            prefix: "k_".into(),
        },
        valgen: ValGenConfig {
            kind: "fixed".into(),
            size: 32,
            min: 16,
            max: 128,
            mean: 32,
        },
    }
}

#[test]
fn redis_smoke() {
    let Some(server) = RedisServer::spawn() else {
        eprintln!("skipping redis_smoke: no valkey-server in PATH");
        return;
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = tmp.path().join("smoke");

    let mut ops = OpsConfig::default();
    ops.map.insert("set".into(), 1);
    ops.map.insert("get".into(), 1);

    let cfg = make_redis_config(server.port, "1500ms", ops, out.to_str().unwrap());
    let outcome = Engine::new(cfg).run().expect("run failed");
    drop(server);

    // Read summary.csv back and assert at least a couple of rows.
    let summary = out.join("summary.csv");
    assert!(summary.exists(), "summary.csv missing");
    let f = std::fs::File::open(&summary).unwrap();
    let lines: Vec<String> = BufReader::new(f).lines().map(|r| r.unwrap()).collect();
    assert!(
        lines.len() >= 4,
        "expected >= 4 rows in summary.csv (got {})",
        lines.len()
    );

    let svg = out.join("summary.svg");
    assert!(svg.exists(), "summary.svg missing");
    assert!(
        std::fs::metadata(&svg).unwrap().len() > 100,
        "SVG too small"
    );

    // Per-op SVGs land for whichever ops the workers touched.
    assert!(outcome.ok_count > 0, "no successful ops");
}

#[test]
fn riak_txn_example_config_parses() {
    // The shipped transaction-workload example must stay loadable and
    // valid: a `riak_http` driver with a `txn` op weight.
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("riak-txn.toml");
    let cfg = Config::from_path(&path).expect("riak-txn.toml must parse and validate");
    assert_eq!(cfg.driver.kind, DriverKind::RiakHttp);
    let weights = cfg.ops.weighted();
    assert!(
        weights.iter().any(|(op, w)| op == "txn" && *w > 0),
        "riak-txn.toml must weight the txn op: {weights:?}"
    );
}

#[test]
fn riak_search_example_config_parses() {
    // The shipped search-workload example must stay loadable and
    // valid: a `riak_http` driver weighting the search op classes.
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("riak-search.toml");
    let cfg = Config::from_path(&path).expect("riak-search.toml must parse and validate");
    assert_eq!(cfg.driver.kind, DriverKind::RiakHttp);
    let weights = cfg.ops.weighted();
    for op in ["index_put", "search_text", "search_regex", "search_vector"] {
        assert!(
            weights.iter().any(|(name, w)| name == op && *w > 0),
            "riak-search.toml must weight the {op} op: {weights:?}"
        );
    }
}

#[test]
fn keygen_uniform_distributes() {
    let mut g = KeyGen::UniformInt {
        max: 64,
        prefix: "k_".into(),
    };
    let mut r = SmallRng::seed_from_u64(0x00C0_FFEE);
    let mut counts = [0u32; 64];
    for _ in 0..32_768 {
        let s = g.next(&mut r);
        let n: u64 = s.trim_start_matches("k_").parse().unwrap();
        assert!(n < 64);
        counts[n as usize] += 1;
    }
    // Each bucket should see ~512 hits in 32K samples; allow
    // wide tolerance.
    for c in counts {
        assert!(c >= 380, "cold bucket {c}");
        assert!(c <= 700, "hot bucket {c}");
    }
}

#[test]
fn valgen_size_distribution() {
    let g = ValGen::Uniform { min: 8, max: 128 };
    let mut r = SmallRng::seed_from_u64(99);
    let mut total = 0usize;
    for _ in 0..1024 {
        let v = g.next(&mut r);
        assert!(v.len() >= 8 && v.len() <= 128);
        total += v.len();
    }
    let mean = total as f64 / 1024.0;
    // Symmetric distribution centred around 68; allow a wide
    // band so the test is not flaky.
    assert!(mean > 50.0 && mean < 90.0, "mean {mean}");
}

#[test]
fn rate_limit_holds() {
    let Some(server) = RedisServer::spawn() else {
        eprintln!("skipping rate_limit_holds: no valkey-server in PATH");
        return;
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = tmp.path().join("rate");

    let mut ops = OpsConfig::default();
    ops.map.insert("set".into(), 1);

    let mut cfg = make_redis_config(server.port, "2s", ops, out.to_str().unwrap());
    cfg.run.rate = RateConfig::Rps(RpsTable { rps: 1000 });
    cfg.run.report_interval = "500ms".into();

    let outcome = Engine::new(cfg).run().expect("run failed");
    drop(server);

    // Aggregate window count must be bounded by rps * (count of
    // 500ms windows) + slack. For 2s at 1000 rps that is <= ~2200
    // total ops; per-window is <= ~600.
    let max_per_window: u64 = outcome
        .snapshots
        .iter()
        .map(|s| s.ok_count + s.err_count)
        .max()
        .unwrap_or(0);
    assert!(
        max_per_window <= 700,
        "rate limit broke: {max_per_window} ops in one 500ms window"
    );
    let total: u64 = outcome
        .snapshots
        .iter()
        .map(|s| s.ok_count + s.err_count)
        .sum();
    assert!(total <= 2400, "rate limit broke: {total} total ops");
}

#[test]
fn error_classification_eof() {
    use dyniak_bench::error::{classify_driver_error, DriverErrorClass};

    // The driver surfaces EOF / closed errors with messages that
    // contain phrases like "peer closed", "unexpected eof", or
    // ECONNRESET. The classifier folds all of those into the
    // Closed bucket.
    let cases = [
        "peer closed mid-reply",
        "unexpected EOF reading bulk",
        "io error: connection reset by peer",
        "broken pipe",
    ];
    for msg in cases {
        assert_eq!(
            classify_driver_error(msg),
            DriverErrorClass::Closed,
            "msg `{msg}` not classified as Closed"
        );
    }
}

#[test]
fn csv_round_trip() {
    use dyniak_bench::error::DriverErrorClass;
    use dyniak_bench::report::{read_op_latencies, read_summary, ReportWriter};
    use dyniak_bench::stats::{OpWindow, WindowSnapshot};

    let dir = tempfile::tempdir().unwrap();
    let mut w = ReportWriter::new(dir.path()).unwrap();

    let snaps: Vec<WindowSnapshot> = (1..=10)
        .map(|i| WindowSnapshot {
            elapsed_s: i as f64,
            ok_count: 100,
            err_count: 1,
            p50_us: 500 * i as u64,
            p95_us: 1000 * i as u64,
            p99_us: 2000 * i as u64,
            p99_9_us: 4000 * i as u64,
            max_us: 8000 * i as u64,
            p50_total_us: 500 * i as u64,
            p99_total_us: 2000 * i as u64,
            per_op: vec![OpWindow {
                op: "get".into(),
                count: 100,
                p50_us: 400,
                p95_us: 800,
                p99_us: 1600,
                p99_9_us: 3200,
                max_us: 6400,
                mean_us: 500,
            }],
            errors: vec![("get".into(), DriverErrorClass::Timeout, 1)],
        })
        .collect();
    for s in &snaps {
        w.write_snapshot(s).unwrap();
    }
    w.finish().unwrap();

    let summary_rows = read_summary(&dir.path().join("summary.csv")).unwrap();
    assert_eq!(summary_rows.len(), 10);
    for (i, r) in summary_rows.iter().enumerate() {
        assert_eq!(r.ok_count, 100);
        assert_eq!(r.err_count, 1);
        let expected_elapsed = (i + 1) as f64;
        assert!(
            (r.elapsed_s - expected_elapsed).abs() < 1e-3,
            "row {i} elapsed {} vs expected {}",
            r.elapsed_s,
            expected_elapsed
        );
    }
    let op_rows = read_op_latencies(&dir.path().join("get_latencies.csv")).unwrap();
    assert_eq!(op_rows.len(), 10);
}
