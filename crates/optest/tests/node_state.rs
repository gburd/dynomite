//! The node state set through the admin endpoint (`/state/<state>`)
//! gates what a node serves from its own datastore.
//!
//! Each scenario runs against the C reference first (when its binary is
//! present) and then against `dynomited`, which must reply the same.

#[macro_use]
mod common;

use std::time::Duration;

use common::start_or_skip;
use optest::cluster::Engine;
use optest::resp::Value;
use optest::ClusterSpec;

const T: Duration = Duration::from_secs(5);
const INVALID_STATE: &str = "ERR Dynomite: Dynomite's current state does not allow this request";

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

/// One rack of three nodes. Node 0 is set to each state in turn; reads
/// and writes for a key node 0 owns, a key node 1 owns, PING, and a read
/// relayed through node 1 for node 0's key.
#[test]
fn admin_state_gates_the_local_datastore() {
    sandboxed!("admin_state_gates_the_local_datastore");
    for engine in engines() {
        let Some(c) = start_or_skip(ClusterSpec::grid(1, 1, 3).engine(engine)) else {
            return;
        };
        let mine = key_on(&c, 0);
        let theirs = key_on(&c, 1);
        let reply = |node: usize, args: &[&str]| {
            c.nodes[node]
                .client(T)
                .expect("connect")
                .cmd(args)
                .expect("reply")
        };
        let err = |v: Value| match v {
            Value::Error(e) => e,
            other => panic!("{engine:?}: expected an error, got {other:?}"),
        };

        // STANDBY: every request for node 0's own datastore is refused.
        assert!(c.nodes[0].stats("/state/standby").expect("standby").contains("OK"));
        assert_eq!(err(reply(0, &["GET", &mine])), INVALID_STATE, "{engine:?}");
        assert_eq!(err(reply(0, &["SET", &mine, "v1"])), INVALID_STATE, "{engine:?}");
        assert_eq!(err(reply(0, &["PING"])), INVALID_STATE, "{engine:?}");
        assert_eq!(err(reply(1, &["GET", &mine])), INVALID_STATE, "{engine:?} relayed");
        // Keys another node owns are served as usual.
        assert_eq!(reply(0, &["GET", &theirs]), Value::Bulk(Some(b"v0".to_vec())));
        assert!(reply(0, &["SET", &theirs, "v1"]).is_ok(), "{engine:?}");

        // WRITES_ONLY and RESUMING refuse reads.
        for state in ["writes_only", "resuming"] {
            assert!(c.nodes[0]
                .stats(&format!("/state/{state}"))
                .expect("state")
                .contains("OK"));
            assert_eq!(
                err(reply(0, &["GET", &mine])),
                INVALID_STATE,
                "{engine:?} {state}"
            );
            assert_eq!(reply(0, &["GET", &theirs]), Value::Bulk(Some(b"v1".to_vec())));
        }
        // RESUMING sends writes.
        assert!(reply(0, &["SET", &mine, "v2"]).is_ok(), "{engine:?} resuming write");

        // NORMAL serves everything.
        assert!(c.nodes[0].stats("/state/normal").expect("normal").contains("OK"));
        assert_eq!(
            reply(0, &["GET", &mine]),
            Value::Bulk(Some(b"v2".to_vec())),
            "{engine:?} normal"
        );
        assert_eq!(reply(0, &["PING"]), Value::Simple("PONG".into()));
        eprintln!("[{engine:?}] state gate matches");
    }
}
