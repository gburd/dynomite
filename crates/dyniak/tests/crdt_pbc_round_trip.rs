//! End-to-end served CRDT (data-type) integration tests.
//!
//! Gated on the `noxu` feature; without it the file compiles to
//! nothing (the served CRDT path is backed by `NoxuDatastore`).
//!
//! Starts a real `serve_pbc` listener backed by [`NoxuDatastore`],
//! drives `DtUpdateReq` / `DtFetchReq` over TCP, and asserts the served
//! CRDT path converges to the arithmetically-expected value:
//!
//! * a counter incremented N times over the wire reads back N;
//! * an OR-set with adds reads back the union of adds;
//! * a replicated op applied through [`ReplicaApplier`] on a second
//!   node's store merges into that node's state (convergence across
//!   replicas), and applying the same op twice is idempotent (no
//!   double-count) -- the exact behaviour that keeps single-key CRDT
//!   updates always-available and convergent under partition.

#![cfg(feature = "noxu")]

use std::sync::Arc;

use prost::Message as _;
use tokio::net::{TcpListener, TcpStream};

use dyniak::crdt_store::{CrdtOp, CrdtStore};
use dyniak::datastore::NoxuDatastore;
use dyniak::datatypes::{ActorId, TAG_COUNTER};
use dyniak::proto::pb::{
    read_frame, write_frame, CounterOp, DtFetchReq, DtFetchResp, DtOp, DtUpdateReq, DtUpdateResp,
    Frame, MessageCode, SetOp,
};
use dyniak::replica_apply::ReplicaApplier;
use dyniak::serve_pbc;
use dynomite::embed::Datastore;
use dynomite::net::ReplicaApplySink as _;
use tempfile::TempDir;

async fn spawn_server() -> (TempDir, std::net::SocketAddr) {
    let dir = TempDir::new().expect("tempdir");
    let noxu = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    let ds: Arc<dyn Datastore> = Arc::new(noxu);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = serve_pbc(listener, ds).await;
    });
    (dir, addr)
}

async fn dt_update_counter(stream: &mut TcpStream, bucket: &[u8], key: &[u8], delta: i64) -> i64 {
    let req = DtUpdateReq {
        bucket: bucket.to_vec(),
        key: Some(key.to_vec()),
        r#type: b"counters".to_vec(),
        op: Some(DtOp {
            counter_op: Some(CounterOp {
                increment: Some(delta),
            }),
            ..DtOp::default()
        }),
        ..DtUpdateReq::default()
    };
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(
        &mut w,
        &Frame::new(MessageCode::DtUpdateReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("send dt_update");
    let resp = read_frame(&mut r).await.expect("recv dt_update");
    assert_eq!(
        resp.code,
        MessageCode::DtUpdateResp.as_u8(),
        "dt_update must return DtUpdateResp, got code {}",
        resp.code
    );
    DtUpdateResp::decode(resp.body.as_slice())
        .expect("decode")
        .counter_value
        .unwrap_or(0)
}

async fn dt_fetch_counter(stream: &mut TcpStream, bucket: &[u8], key: &[u8]) -> Option<i64> {
    let req = DtFetchReq {
        bucket: bucket.to_vec(),
        key: key.to_vec(),
        r#type: b"counters".to_vec(),
        ..DtFetchReq::default()
    };
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(
        &mut w,
        &Frame::new(MessageCode::DtFetchReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("send dt_fetch");
    let resp = read_frame(&mut r).await.expect("recv dt_fetch");
    assert_eq!(resp.code, MessageCode::DtFetchResp.as_u8());
    DtFetchResp::decode(resp.body.as_slice())
        .expect("decode")
        .value
        .and_then(|v| v.counter_value)
}

#[tokio::test]
async fn counter_increments_read_back_as_sum() {
    let (_dir, addr) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.expect("connect");
    for i in 1..=5 {
        let v = dt_update_counter(&mut s, b"chaos", b"c", 1).await;
        assert_eq!(v, i, "counter must reflect the running sum after each op");
    }
    let fetched = dt_fetch_counter(&mut s, b"chaos", b"c").await;
    assert_eq!(fetched, Some(5), "fetch must read back the total");
}

#[tokio::test]
async fn set_adds_read_back_as_union() {
    let (_dir, addr) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.expect("connect");
    for e in [b"x".as_slice(), b"y", b"x"] {
        let req = DtUpdateReq {
            bucket: b"chaos".to_vec(),
            key: Some(b"s".to_vec()),
            r#type: b"sets".to_vec(),
            op: Some(DtOp {
                set_op: Some(SetOp {
                    adds: vec![e.to_vec()],
                    removes: Vec::new(),
                }),
                ..DtOp::default()
            }),
            ..DtUpdateReq::default()
        };
        let (mut r, mut w) = tokio::io::split(&mut s);
        write_frame(
            &mut w,
            &Frame::new(MessageCode::DtUpdateReq.as_u8(), req.encode_to_vec()),
        )
        .await
        .expect("send");
        let resp = read_frame(&mut r).await.expect("recv");
        assert_eq!(resp.code, MessageCode::DtUpdateResp.as_u8());
    }
    // Fetch and confirm the union {x, y}.
    let req = DtFetchReq {
        bucket: b"chaos".to_vec(),
        key: b"s".to_vec(),
        r#type: b"sets".to_vec(),
        ..DtFetchReq::default()
    };
    let (mut r, mut w) = tokio::io::split(&mut s);
    write_frame(
        &mut w,
        &Frame::new(MessageCode::DtFetchReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("send fetch");
    let resp = read_frame(&mut r).await.expect("recv fetch");
    let mut members = DtFetchResp::decode(resp.body.as_slice())
        .expect("decode")
        .value
        .map(|v| v.set_value)
        .unwrap_or_default();
    members.sort();
    assert_eq!(members, vec![b"x".to_vec(), b"y".to_vec()]);
}

#[tokio::test]
async fn replicated_op_converges_and_is_idempotent() {
    // Two independent stores stand in for two replicas. Node A takes a
    // local increment; its op is replicated to node B via the
    // ReplicaApplier. B must converge to the same value, and a
    // re-delivered op must not double-count.
    let dir_a = TempDir::new().expect("a");
    let dir_b = TempDir::new().expect("b");
    let ds_a: Arc<dyn Datastore> = Arc::new(NoxuDatastore::open_in(dir_a.path()).expect("a"));
    let ds_b: Arc<dyn Datastore> = Arc::new(NoxuDatastore::open_in(dir_b.path()).expect("b"));

    // Node A applies +1 attributed to actor A, and +1 attributed to
    // actor B (as if B's op also reached A) -- A converges to 2.
    let store_a = CrdtStore::new(Arc::clone(&ds_a));
    let op_a = CrdtOp::Counter {
        actor: ActorId::new("dc1", "A"),
        delta: 1,
    };
    let op_b = CrdtOp::Counter {
        actor: ActorId::new("dc1", "B"),
        delta: 1,
    };
    store_a
        .apply(b"chaos", b"c", &op_a)
        .await
        .expect("a apply a");
    store_a
        .apply(b"chaos", b"c", &op_b)
        .await
        .expect("a apply b");

    // Node B receives the coordinator's merged STATE for each update
    // through the real replica-apply wire path (a PeerOp::DtUpdate
    // whose payload is the serialized post-apply state), and B's own
    // op is delivered TWICE (a duplicate). Build the shipped states the
    // way the handler does: apply on a scratch store and capture the
    // post-state.
    // After A applied op_a then op_b, A's stored state IS the merged
    // counter {A:1,B:1}. Shipping that whole state and merging it on B
    // (even twice -- a duplicate) converges B to 2 without
    // double-counting, because state merge is element-wise max.
    let state_after_both = ds_a.riak_get(b"chaos", b"c").await.unwrap().unwrap();
    let applier_b = ReplicaApplier::new(Arc::clone(&ds_b));
    let wire = |state: &[u8]| {
        dyniak::proto::replica_wire::encode_peer_op(&dyniak::router::PeerOp::DtUpdate {
            bucket_type: b"counters".to_vec(),
            bucket: b"chaos".to_vec(),
            key: b"c".to_vec(),
            op: state.to_vec(),
        })
    };
    applier_b.apply(&wire(&state_after_both)).await;
    applier_b.apply(&wire(&state_after_both)).await; // duplicate

    let store_b = CrdtStore::new(Arc::clone(&ds_b));
    let a_val = store_a
        .fetch(b"chaos", b"c", TAG_COUNTER)
        .await
        .expect("a fetch");
    let b_val = store_b
        .fetch(b"chaos", b"c", TAG_COUNTER)
        .await
        .expect("b fetch");
    assert_eq!(
        a_val,
        dyniak::crdt_store::CrdtValue::Counter(2),
        "node A converged value"
    );
    assert_eq!(
        b_val,
        dyniak::crdt_store::CrdtValue::Counter(2),
        "node B converged to the same value despite a duplicate op"
    );
}
