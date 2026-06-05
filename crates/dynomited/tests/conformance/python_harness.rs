//! Rust adaptation of the functional test scenarios from
//! `_/dynomite/test/func_test.py` and friends. We chose to
//! adapt the scenarios to Rust rather than keep the Python
//! harness so the suite has one runner (cargo nextest), one
//! set of cleanup guarantees (the Drop impl on `Cluster`), and
//! one source of truth for assertions. The Python files in the
//! C tree remain unchanged in `_/dynomite/test/` for
//! archaeology.
//!
//! Mapped scenarios:
//!
//! * `run_key_value_tests` -> [`run_key_value_workload`]
//! * `run_multikey_test`   -> [`run_multikey_workload`]
//! * `run_hash_tests`      -> [`run_hash_workload`]
//!
//! The C harness calls `dual_run` to compare a stand-alone
//! Redis against the Dynomite cluster. Here we drive the same
//! workload through the dynomited proxy and assert that the
//! response shape conforms to the canonical Redis semantics,
//! eliminating the need for a second backend process.

use std::time::Duration;

use crate::helpers::{redis_server_available, Cluster, NodeSpec, RespClient, RespValue};

fn skip(name: &str) -> bool {
    if !redis_server_available() {
        eprintln!("[conformance::python_harness::{name}] valkey-server not on PATH; skipping");
        return true;
    }
    false
}

fn launch() -> Cluster {
    let spec = NodeSpec::simple("ph0", "127.0.0.1", 0, "437425602");
    Cluster::launch(vec![spec], "dyn_o_mite").expect("launch")
}

async fn open(cluster: &Cluster) -> RespClient {
    let n = &cluster.nodes[0];
    let mut c = RespClient::connect(&n.spec.host, n.spec.listen_port)
        .await
        .expect("connect");
    c.set_timeout(Duration::from_secs(5));
    c
}

fn make_key(test: &str, id: usize) -> Vec<u8> {
    format!("{test}_{id}").into_bytes()
}

#[tokio::test]
async fn run_key_value_workload() {
    if skip("run_key_value_workload") {
        return;
    }
    let cluster = launch();
    let mut c = open(&cluster).await;
    let test = "KEY_VALUE";
    let max_keys: usize = 32; // shrunk vs the Python 1000 to keep CI fast.
    let payload = b"sample-value-payload";
    for i in 0..max_keys {
        let key = make_key(test, i);
        let v = c.cmd::<&[u8]>(&[b"SET", &key, payload]).await.expect("SET");
        assert_eq!(v.as_simple(), Some("OK"));
    }
    for i in 0..max_keys {
        let key = make_key(test, i);
        let v = c.cmd::<&[u8]>(&[b"GET", &key]).await.expect("GET");
        assert_eq!(v.as_bulk(), Some(&payload[..]));
    }
    // append + verify
    let key = make_key(test, 0);
    let v = c
        .cmd::<&[u8]>(&[b"APPEND", &key, b"-tail"])
        .await
        .expect("APPEND");
    assert!(matches!(v, RespValue::Integer(_)));
    let v = c.cmd::<&[u8]>(&[b"GET", &key]).await.expect("GET");
    let got = v.as_bulk().expect("bulk").to_vec();
    assert!(got.ends_with(b"-tail"), "expected suffix in {got:?}");
    // expire / exists
    let v = c
        .cmd::<&[u8]>(&[b"EXPIRE", &key, b"60"])
        .await
        .expect("EXPIRE");
    assert_eq!(v.as_integer(), Some(1));
    let v = c.cmd::<&[u8]>(&[b"EXISTS", &key]).await.expect("EXISTS");
    assert_eq!(v.as_integer(), Some(1));
}

#[tokio::test]
async fn run_multikey_workload() {
    if skip("run_multikey_workload") {
        return;
    }
    let cluster = launch();
    let mut c = open(&cluster).await;
    let test = "MULTIKEY";
    // MSET 4 pairs, MGET them.
    let k1 = make_key(test, 1);
    let k2 = make_key(test, 2);
    let k3 = make_key(test, 3);
    let k4 = make_key(test, 4);
    let v = c
        .cmd::<&[u8]>(&[b"MSET", &k1, b"a", &k2, b"b", &k3, b"c", &k4, b"d"])
        .await
        .expect("MSET");
    assert_eq!(v.as_simple(), Some("OK"));
    let v = c
        .cmd::<&[u8]>(&[b"MGET", &k1, &k2, &k3, &k4])
        .await
        .expect("MGET");
    let RespValue::Array(Some(items)) = v else {
        panic!("MGET expected array");
    };
    assert_eq!(items.len(), 4);
    assert_eq!(items[0].as_bulk(), Some(&b"a"[..]));
    assert_eq!(items[1].as_bulk(), Some(&b"b"[..]));
    assert_eq!(items[2].as_bulk(), Some(&b"c"[..]));
    assert_eq!(items[3].as_bulk(), Some(&b"d"[..]));
}

#[tokio::test]
async fn run_hash_workload() {
    if skip("run_hash_workload") {
        return;
    }
    let cluster = launch();
    let mut c = open(&cluster).await;
    let test = "HASH_MAP";
    let key = make_key(test, 1);
    // HSET / HGET
    for i in 0..8u32 {
        let f = format!("field{i}").into_bytes();
        let val = format!("v{i}").into_bytes();
        let v = c
            .cmd::<&[u8]>(&[b"HSET", &key, &f, &val])
            .await
            .expect("HSET");
        assert!(matches!(v, RespValue::Integer(_)));
        let v = c.cmd::<&[u8]>(&[b"HGET", &key, &f]).await.expect("HGET");
        assert_eq!(v.as_bulk(), Some(val.as_slice()));
    }
    // HLEN
    let v = c.cmd::<&[u8]>(&[b"HLEN", &key]).await.expect("HLEN");
    assert_eq!(v.as_integer(), Some(8));
    // HDEL
    let f0 = b"field0".to_vec();
    let v = c.cmd::<&[u8]>(&[b"HDEL", &key, &f0]).await.expect("HDEL");
    assert_eq!(v.as_integer(), Some(1));
    let v = c.cmd::<&[u8]>(&[b"HLEN", &key]).await.expect("HLEN2");
    assert_eq!(v.as_integer(), Some(7));
}
