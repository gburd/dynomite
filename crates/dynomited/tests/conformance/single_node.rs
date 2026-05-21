//! Single-node conformance: spawn one `dynomited` against one
//! `redis-server` and exercise the basic Redis command surface
//! end to end.
//!
//! Each `#[tokio::test]` skips with a notice when
//! `redis-server` is not on `PATH`. Tests assert response shape
//! against the known Redis semantics, mirroring the workload of
//! `_/dynomite/test/func_test.py::run_key_value_tests` and
//! `run_hash_tests`.
//!
//! Successful execution exercises the proxy listener, the
//! Redis parser (request + response), the cluster dispatcher
//! (single-replica plan), and the client connection FSM.

use std::time::Duration;

use crate::helpers::{redis_server_available, Cluster, NodeSpec, RespClient, RespError, RespValue};

const SINGLE_NODE_TOKEN: &str = "437425602";

fn skip_if_no_redis(name: &str) -> bool {
    if !redis_server_available() {
        eprintln!("[conformance::{name}] redis-server not on PATH; skipping");
        return true;
    }
    false
}

fn launch_single() -> Cluster {
    let spec = NodeSpec::simple("n0", "127.0.0.1", 0, SINGLE_NODE_TOKEN);
    Cluster::launch(vec![spec], "dyn_o_mite").expect("launch single-node cluster")
}

async fn open_client(cluster: &Cluster) -> RespClient {
    let node = &cluster.nodes[0];
    let mut c = RespClient::connect(&node.spec.host, node.spec.listen_port)
        .await
        .expect("connect");
    c.set_timeout(Duration::from_secs(5));
    c
}

#[tokio::test]
async fn set_get_round_trip() {
    if skip_if_no_redis("set_get_round_trip") {
        return;
    }
    let cluster = launch_single();
    let mut c = open_client(&cluster).await;
    let v = c
        .cmd::<&[u8]>(&[b"SET", b"k", b"v"])
        .await
        .expect("SET reply");
    assert_eq!(v.as_simple(), Some("OK"));
    let v = c.cmd::<&[u8]>(&[b"GET", b"k"]).await.expect("GET reply");
    assert_eq!(v.as_bulk(), Some(&b"v"[..]));
}

#[tokio::test]
async fn del_and_exists() {
    if skip_if_no_redis("del_and_exists") {
        return;
    }
    let cluster = launch_single();
    let mut c = open_client(&cluster).await;
    let _ = c.cmd::<&[u8]>(&[b"SET", b"a", b"1"]).await.unwrap();
    let v = c.cmd::<&[u8]>(&[b"EXISTS", b"a"]).await.unwrap();
    assert_eq!(v.as_integer(), Some(1));
    let v = c.cmd::<&[u8]>(&[b"DEL", b"a"]).await.unwrap();
    assert_eq!(v.as_integer(), Some(1));
    let v = c.cmd::<&[u8]>(&[b"EXISTS", b"a"]).await.unwrap();
    assert_eq!(v.as_integer(), Some(0));
}

#[tokio::test]
async fn mset_mget_round_trip() {
    if skip_if_no_redis("mset_mget_round_trip") {
        return;
    }
    let cluster = launch_single();
    let mut c = open_client(&cluster).await;
    let v = c
        .cmd::<&[u8]>(&[b"MSET", b"x", b"1", b"y", b"2", b"z", b"3"])
        .await
        .unwrap();
    assert_eq!(v.as_simple(), Some("OK"));
    let v = c.cmd::<&[u8]>(&[b"MGET", b"x", b"y", b"z"]).await.unwrap();
    let RespValue::Array(Some(items)) = v else {
        panic!("MGET expected array");
    };
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].as_bulk(), Some(&b"1"[..]));
    assert_eq!(items[1].as_bulk(), Some(&b"2"[..]));
    assert_eq!(items[2].as_bulk(), Some(&b"3"[..]));
}

#[tokio::test]
async fn incr_decr_arithmetic() {
    if skip_if_no_redis("incr_decr_arithmetic") {
        return;
    }
    let cluster = launch_single();
    let mut c = open_client(&cluster).await;
    let _ = c.cmd::<&[u8]>(&[b"SET", b"n", b"10"]).await.unwrap();
    let v = c.cmd::<&[u8]>(&[b"INCR", b"n"]).await.unwrap();
    assert_eq!(v.as_integer(), Some(11));
    let v = c.cmd::<&[u8]>(&[b"DECR", b"n"]).await.unwrap();
    assert_eq!(v.as_integer(), Some(10));
    let v = c.cmd::<&[u8]>(&[b"DECRBY", b"n", b"4"]).await.unwrap();
    assert_eq!(v.as_integer(), Some(6));
}

#[tokio::test]
async fn hash_round_trip() {
    if skip_if_no_redis("hash_round_trip") {
        return;
    }
    let cluster = launch_single();
    let mut c = open_client(&cluster).await;
    let v = c
        .cmd::<&[u8]>(&[b"HSET", b"h", b"f1", b"v1"])
        .await
        .unwrap();
    assert_eq!(v.as_integer(), Some(1));
    let v = c.cmd::<&[u8]>(&[b"HGET", b"h", b"f1"]).await.unwrap();
    assert_eq!(v.as_bulk(), Some(&b"v1"[..]));
    let v = c.cmd::<&[u8]>(&[b"HDEL", b"h", b"f1"]).await.unwrap();
    assert_eq!(v.as_integer(), Some(1));
    let v = c.cmd::<&[u8]>(&[b"HGET", b"h", b"f1"]).await.unwrap();
    assert_eq!(v, RespValue::Bulk(None));
}

#[tokio::test]
async fn set_round_trip() {
    if skip_if_no_redis("set_round_trip") {
        return;
    }
    let cluster = launch_single();
    let mut c = open_client(&cluster).await;
    let _ = c
        .cmd::<&[u8]>(&[b"SADD", b"s", b"a", b"b", b"c"])
        .await
        .unwrap();
    let v = c.cmd::<&[u8]>(&[b"SMEMBERS", b"s"]).await.unwrap();
    let RespValue::Array(Some(mut items)) = v else {
        panic!("SMEMBERS expected array");
    };
    let mut got: Vec<Vec<u8>> = items
        .drain(..)
        .filter_map(|v| match v {
            RespValue::Bulk(Some(b)) => Some(b),
            _ => None,
        })
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        "SMEMBERS ordering normalised"
    );
}

#[tokio::test]
async fn zset_round_trip() {
    if skip_if_no_redis("zset_round_trip") {
        return;
    }
    let cluster = launch_single();
    let mut c = open_client(&cluster).await;
    let _ = c
        .cmd::<&[u8]>(&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"])
        .await
        .unwrap();
    let v = c
        .cmd::<&[u8]>(&[b"ZRANGE", b"z", b"0", b"-1"])
        .await
        .unwrap();
    let RespValue::Array(Some(items)) = v else {
        panic!("ZRANGE expected array");
    };
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].as_bulk(), Some(&b"a"[..]));
    assert_eq!(items[1].as_bulk(), Some(&b"b"[..]));
    assert_eq!(items[2].as_bulk(), Some(&b"c"[..]));
}

#[tokio::test]
async fn expire_and_ttl() {
    if skip_if_no_redis("expire_and_ttl") {
        return;
    }
    let cluster = launch_single();
    let mut c = open_client(&cluster).await;
    let _ = c.cmd::<&[u8]>(&[b"SET", b"e", b"x"]).await.unwrap();
    let v = c.cmd::<&[u8]>(&[b"EXPIRE", b"e", b"60"]).await.unwrap();
    assert_eq!(v.as_integer(), Some(1));
    let v = c.cmd::<&[u8]>(&[b"TTL", b"e"]).await.unwrap();
    let ttl = v.as_integer().expect("TTL int");
    assert!((1..=60).contains(&ttl), "TTL was {ttl}");
}

#[tokio::test]
async fn quit_closes_socket() {
    if skip_if_no_redis("quit_closes_socket") {
        return;
    }
    let cluster = launch_single();
    let mut c = open_client(&cluster).await;
    // Either +OK\r\n or an immediate close is acceptable.
    let res = c.cmd::<&[u8]>(&[b"QUIT"]).await;
    match res {
        Ok(v) => assert_eq!(v.as_simple(), Some("OK"), "QUIT reply"),
        Err(RespError::Eof) => {} // server closed without reply
        Err(other) => panic!("unexpected QUIT error: {other:?}"),
    }
}

#[tokio::test]
async fn append_and_strlen() {
    // Mirrors `func_test.py::run_key_value_tests` append +
    // verification.
    if skip_if_no_redis("append_and_strlen") {
        return;
    }
    let cluster = launch_single();
    let mut c = open_client(&cluster).await;
    let _ = c.cmd::<&[u8]>(&[b"SET", b"s", b"hello"]).await.unwrap();
    let v = c.cmd::<&[u8]>(&[b"APPEND", b"s", b"-world"]).await.unwrap();
    assert_eq!(v.as_integer(), Some(11));
    let v = c.cmd::<&[u8]>(&[b"GET", b"s"]).await.unwrap();
    assert_eq!(v.as_bulk(), Some(&b"hello-world"[..]));
}
