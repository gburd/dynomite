//! Shared scenario plumbing for the operational test binaries.

#![allow(dead_code, missing_docs)]

use std::time::Duration;

use optest::check::{check, Guarantee, Report};
use optest::cluster::Cluster;
use optest::history::{Event, History, Op, Outcome};
use optest::resp::{RespError, Value};

/// Run a scenario body inside a fresh namespace sandbox, or skip.
#[macro_export]
macro_rules! sandboxed {
    ($name:expr) => {
        match optest::ns::in_sandbox($name) {
            optest::ns::Sandbox::Inside => {}
            optest::ns::Sandbox::Passed => return,
            optest::ns::Sandbox::Skipped(why) => {
                eprintln!("[optest] skip {}: {why}", $name);
                return;
            }
        }
    };
}

/// Start a cluster, or skip the scenario when a tool is missing (unless
/// `OPTEST_REQUIRE=1`).
pub fn start_or_skip(spec: optest::ClusterSpec) -> Option<Cluster> {
    match Cluster::start(spec) {
        Ok(c) => Some(c),
        Err(optest::Error::MissingTool(t)) if !optest::Tools::required() => {
            eprintln!("[optest] skip: missing {t}");
            None
        }
        Err(e) => panic!("cluster start failed: {e}"),
    }
}

/// Deterministic seed for this run: `OPTEST_SEED` or a fixed default,
/// printed so a failure can be replayed.
pub fn seed() -> u64 {
    let s = std::env::var("OPTEST_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x5eed_d1a0);
    eprintln!("[optest] seed {s}");
    s
}

/// One write-once register operation through node `node`, recorded.
pub fn op(
    c: &Cluster,
    h: &mut History,
    process: usize,
    node: usize,
    op: Op,
    timeout: Duration,
) -> Outcome {
    let t0 = h.now();
    let res = c.nodes[node].client(timeout).and_then(|mut cl| match &op {
        Op::Write { key, value } => cl.cmd(&["SET", key.as_str(), value.as_str()]),
        Op::Read { key, .. } => cl.cmd(&["GET", key.as_str()]),
    });
    let (outcome, op) = classify(op, res);
    h.record(Event {
        process,
        op,
        outcome,
        invoke_ns: t0,
        complete_ns: h.now(),
    });
    outcome
}

fn classify(op: Op, res: Result<Value, RespError>) -> (Outcome, Op) {
    match (op, res) {
        (Op::Write { key, value }, Ok(v)) if v.is_ok() => (Outcome::Ok, Op::Write { key, value }),
        (Op::Write { key, value }, Ok(_)) => (Outcome::Fail, Op::Write { key, value }),
        (Op::Read { key, .. }, Ok(Value::Bulk(b))) => (
            Outcome::Ok,
            Op::Read {
                key,
                value: b.map(|b| String::from_utf8_lossy(&b).into_owned()),
            },
        ),
        (Op::Read { key, .. }, Ok(_)) => (Outcome::Fail, Op::Read { key, value: None }),
        // A dropped or timed-out request may still have been applied.
        (op, Err(_)) => (Outcome::Info, op),
    }
}

/// Check `h` against `g`, writing it to `target/optest/<name>.jsonl` and
/// panicking with the anomaly list on failure.
pub fn assert_history(name: &str, h: &History, g: Guarantee) -> Report {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("histories");
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join(format!("{name}.jsonl"));
    let _ = h.write_jsonl(&path);
    let rep = check(h.events(), g);
    eprintln!(
        "[optest] {name}: {} writes ok, {} reads ok, {} failed, {} indeterminate, {} anomalies (history {})",
        rep.writes_ok,
        rep.reads_ok,
        rep.failed,
        rep.indeterminate,
        rep.anomalies.len(),
        path.display()
    );
    assert!(rep.valid(), "{name}: anomalies: {:#?}", rep.anomalies);
    rep
}

/// Poll `f` until it returns `Some`, or panic after `timeout`.
pub fn eventually<T>(what: &str, timeout: Duration, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
