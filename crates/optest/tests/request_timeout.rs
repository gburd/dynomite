//! Requests the datastore or a peer never answers fail after the request
//! timeout instead of hanging.
//!
//! Each scenario runs against the C reference first (when its binary is
//! present) and then against `dynomited`, which must reply the same.

#[macro_use]
mod common;

use std::time::{Duration, Instant};

use common::start_or_skip;
use optest::cluster::Engine;
use optest::resp::Value;
use optest::ClusterSpec;

const T: Duration = Duration::from_secs(15);
const REFUSED: &str = "ERR Storage: Datastore refused connection";

fn engines() -> Vec<Engine> {
    let mut e = Vec::new();
    let only = std::env::var("OPTEST_ENGINES").unwrap_or_default();
    if optest::Tools::discover().c_dynomite.is_some() && only != "rust" {
        e.push(Engine::C);
    }
    if only != "c" {
        e.push(Engine::Rust);
    }
    e
}

/// A key held by node `i`'s datastore, written through node 0.
fn key_on(c: &optest::Cluster, i: usize) -> String {
    let mut cl = c.nodes[0].client(T).expect("connect");
    for n in 0..200 {
        let k = format!("k{n}");
        assert!(cl.cmd(&["SET", &k, "v0"]).expect("set").is_ok());
        let mut s = c.nodes[i].store_client().expect("datastore");
        if s.cmd(&["EXISTS", &k]).expect("exists") == Value::Int(1) {
            return k;
        }
    }
    panic!("no key landed on node {i}");
}

fn timed(c: &optest::Cluster, node: usize, args: &[&str]) -> (Value, Duration) {
    let mut cl = c.nodes[node].client(T).expect("connect");
    let started = Instant::now();
    let v = cl.cmd(args).expect("reply");
    (v, started.elapsed())
}

/// One rack of three nodes, `timeout: 1000`. With a node's datastore
/// frozen, a read of a key it owns fails with the storage error after one
/// timeout, whether it enters at that node or is relayed by a peer; the
/// runtime timeout factor scales the wait; after the datastore resumes the
/// key is served again.
#[test]
fn frozen_datastore_fails_after_the_request_timeout() {
    sandboxed!("frozen_datastore_fails_after_the_request_timeout");
    for engine in engines() {
        let spec = ClusterSpec::grid(1, 1, 3)
            .engine(engine)
            .with("timeout", "1000");
        let Some(c) = start_or_skip(spec) else {
            return;
        };
        let k = key_on(&c, 1);
        c.nodes[1].pause_store().expect("freeze datastore");

        let (v, took) = timed(&c, 1, &["GET", &k]);
        assert_eq!(v, Value::Error(REFUSED.into()), "{engine:?} local");
        assert!(
            (Duration::from_millis(900)..Duration::from_secs(3)).contains(&took),
            "{engine:?} local read answered after {took:?}"
        );

        // Relayed through node 0: node 1's own datastore answer (a
        // timeout, or the refusal while it reconnects) comes back first.
        let (v, took) = timed(&c, 0, &["GET", &k]);
        assert_eq!(v, Value::Error(REFUSED.into()), "{engine:?} relayed");
        assert!(took < Duration::from_secs(3), "{engine:?} relayed {took:?}");

        // Factor 3: a read waits three timeouts. Each timeout closes the
        // datastore connection and requests fail at once until it is
        // re-established, so look for the first read that waits.
        assert!(c.nodes[1]
            .stats("/set_timeout_factor/3")
            .expect("factor")
            .contains("OK"));
        let took = common::eventually("a timed read", Duration::from_secs(30), || {
            let (v, took) = timed(&c, 1, &["GET", &k]);
            assert_eq!(v, Value::Error(REFUSED.into()), "{engine:?} factor 3");
            (took > Duration::from_millis(500)).then_some(took)
        });
        assert!(
            (Duration::from_millis(2800)..Duration::from_secs(6)).contains(&took),
            "{engine:?} factor-3 read answered after {took:?}"
        );

        c.nodes[1].resume_store().expect("resume datastore");
        common::eventually("datastore back", Duration::from_secs(15), || {
            let mut cl = c.nodes[1].client(T).ok()?;
            (cl.cmd(&["GET", &k]).ok()? == Value::Bulk(Some(b"v0".to_vec()))).then_some(())
        });
        eprintln!("[{engine:?}] datastore timeout matches");
    }
}

/// The value of counter `name` in a `/info` body (the first occurrence:
/// pool counters are unique, server counters sit in the server block).
fn counter(info: &str, name: &str) -> i64 {
    let key = format!("\"{name}\":");
    let at = info
        .find(&key)
        .unwrap_or_else(|| panic!("no {name} in /info"));
    info[at + key.len()..]
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .unwrap_or_else(|_| panic!("{name} is not a number"))
}

/// A request timeout is counted where the request timed out: a datastore
/// timeout bumps `server_timedout`, `server_dropped_requests` and
/// `server_ejects` on the node that owns the datastore; a timeout towards
/// a frozen same-DC peer bumps `peer_timedout` and `peer_timedout_requests`
/// on the node that forwarded it. Counters are read from `/info` after a
/// stats interval (1 s here) has aggregated them.
#[test]
fn timeouts_are_counted_in_info() {
    sandboxed!("timeouts_are_counted_in_info");
    for engine in engines() {
        let spec = ClusterSpec::grid(1, 1, 3)
            .engine(engine)
            .with("timeout", "1000")
            .with("stats_interval", "1000");
        let Some(c) = start_or_skip(spec) else {
            return;
        };
        let k = key_on(&c, 1);
        let info = |n: usize| c.nodes[n].stats("/info").expect("info");
        let settle = || std::thread::sleep(Duration::from_millis(2500));

        settle();
        let before1 = info(1);
        c.nodes[1].pause_store().expect("freeze datastore");
        let (v, _) = timed(&c, 1, &["GET", &k]);
        assert_eq!(v, Value::Error(REFUSED.into()), "{engine:?}");
        c.nodes[1].resume_store().expect("resume datastore");
        settle();
        let after1 = info(1);
        for name in [
            "server_timedout",
            "server_dropped_requests",
            "server_ejects",
        ] {
            assert_eq!(
                counter(&after1, name) - counter(&before1, name),
                1,
                "{engine:?} node 1 {name}"
            );
        }

        let before0 = info(0);
        c.nodes[1].pause().expect("freeze node 1");
        let (v, _) = timed(&c, 0, &["GET", &k]);
        assert_eq!(
            v,
            Value::Error("ERR Peer: Peer Node refused connection".into()),
            "{engine:?}"
        );
        c.nodes[1].resume().expect("resume node 1");
        settle();
        let after0 = info(0);
        for name in ["peer_timedout", "peer_timedout_requests"] {
            assert_eq!(
                counter(&after0, name) - counter(&before0, name),
                1,
                "{engine:?} node 0 {name}"
            );
        }
        eprintln!("[{engine:?}] timeout counters match");
    }
}
