//! Deterministic tests for cross-node object-replica routing.
//!
//! Three concerns are covered here, complementing the PBC-server
//! fan-out already exercised by `bucket_props_routing.rs`:
//!
//! 1. **Fan-out matches `plan_replicas`.** The [`BucketRouter`]'s
//!    replica list for a set of keys is asserted equal, peer for
//!    peer, to what [`plan_replicas`] independently computes over
//!    the same ring / n_val / strategy. This pins the router to the
//!    replication planner so a divergence in either surfaces here.
//!
//! 2. **Receive-apply against a real `NoxuDatastore`.** An encoded
//!    [`PeerOp::Put`] is fed to a [`ReplicaApplier`] backed by a
//!    real transactional Noxu environment; the value is then read
//!    back locally over the same datastore. The applier has no
//!    outbound path at all, so applying a replica op structurally
//!    cannot re-forward it (the no-fan-out guarantee is enforced by
//!    construction, not merely by convention).
//!
//! 3. **Outbound / receive pairing (mock transport).** The exact
//!    bytes [`PeerChannelOutbound`](dynomited) would ship
//!    (`encode_peer_op`) are decoded and applied by the receiver's
//!    [`ReplicaApplier`] (`decode_peer_op` + local apply), standing
//!    in for the dnode wire hop. This is a mock-transport pairing:
//!    it exercises the encode/decode/apply contract end to end
//!    without spinning up two full dnode server loops. The live
//!    two-node dnode delivery is validated by the multi-node
//!    conformance rig, not by this in-process unit.

#![cfg(feature = "noxu")]

use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;

use dynomite::embed::Datastore;
use dynomite::hashkit::HashType;
use dynomite::msg::ConsistencyLevel;
use dynomite::net::ReplicaApplySink;

use dyniak::bucket_props::{BucketProps, BucketPropsRegistry};
use dyniak::datastore::NoxuDatastore;
use dyniak::datatypes::keyfun::KeyFun;
use dyniak::proto::http::object::HttpObject;
use dyniak::proto::replica_wire::{decode_peer_op, encode_peer_op};
use dyniak::replication::{
    plan_replicas, ReplicationPlan, ReplicationStrategy, RingPoint, RingView,
};
use dyniak::router::{BucketRouter, PeerOp};
use dyniak::ReplicaApplier;

use dyniak::router::{PeerOutbound, RoutingHooks};
use dynomite::net::client::BoxFuture;
use std::sync::Mutex as StdMutex;

/// Test double: records every `(peer_idx, PeerOp)` dispatched, so a
/// test can assert the HTTP put / delete path fanned out to the
/// key's replicas.
#[derive(Debug, Default)]
struct CapturingOutbound {
    ops: StdMutex<Vec<(u32, PeerOp)>>,
}

impl PeerOutbound for CapturingOutbound {
    fn dispatch(&self, peer_idx: u32, op: PeerOp) -> BoxFuture<'_, ()> {
        self.ops.lock().expect("lock").push((peer_idx, op));
        Box::pin(async {})
    }
}

/// Scratch root per AGENTS.md: /scratch, not /tmp.
fn scratch_dir() -> TempDir {
    let base = Path::new("/scratch");
    if base.is_dir() {
        TempDir::new_in(base).expect("tempdir in /scratch")
    } else {
        TempDir::new().expect("tempdir")
    }
}

fn five_peer_ring() -> Arc<RingView> {
    let span = u64::from(u32::MAX);
    let pts: Vec<RingPoint> = (0..5u32)
        .map(|i| RingPoint::new(u64::from(i) * span / 5, i, "dc1", "r1"))
        .collect();
    Arc::new(RingView::new(pts))
}

#[test]
fn router_fan_out_matches_plan_replicas() {
    let ring = five_peer_ring();
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(
        b"default",
        b"users",
        BucketProps {
            keyfun: Some(KeyFun::Std),
            strategy: Some(ReplicationStrategy::Successors),
            n_val: Some(3),
            ..BucketProps::default()
        },
    );
    // 32-bit hash keeps the produced hash within the ring's u32
    // token range so the successor walk is deterministic.
    let router = BucketRouter::new(registry, Arc::clone(&ring), HashType::Murmur);

    for i in 0..64u32 {
        let key = format!("key-{i}");
        let decision = router.route(b"default", b"users", key.as_bytes());
        // Independently plan the replicas from the same inputs.
        let expected = plan_replicas(
            ring.as_ref(),
            decision.key_hash,
            3,
            ReplicationStrategy::Successors,
            ConsistencyLevel::DcOne,
        );
        let expected_list = match expected {
            ReplicationPlan::Successors { .. } => expected.into_replica_list(),
            other @ ReplicationPlan::Topology(_) => {
                panic!("expected successors plan, got {other:?}")
            }
        };
        let actual: Vec<u32> = decision.replica_list().iter().map(|t| t.peer_idx).collect();
        let want: Vec<u32> = expected_list.iter().map(|t| t.peer_idx).collect();
        assert_eq!(
            actual, want,
            "router replica set for {key} diverges from plan_replicas"
        );
        // n_val = 3 over a 5-peer ring yields 3 distinct peers.
        assert_eq!(actual.len(), 3, "n_val=3 yields 3 replicas for {key}");
        let mut sorted = actual.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 3, "replicas are distinct peers for {key}");
    }
}

#[tokio::test]
async fn receive_apply_put_lands_in_local_noxu() {
    let dir = scratch_dir();
    let noxu = NoxuDatastore::open_transactional(dir.path()).expect("open noxu");
    let ds: Arc<dyn Datastore> = Arc::new(noxu);
    let applier = ReplicaApplier::new(Arc::clone(&ds));

    let op = PeerOp::Put {
        bucket_type: b"default".to_vec(),
        bucket: b"users".to_vec(),
        key: b"alice".to_vec(),
        value: b"alice-replicated".to_vec(),
    };
    // The wire bytes are exactly what the outbound side ships.
    let wire = encode_peer_op(&op);
    // Sanity: the receiver decodes the same op.
    assert_eq!(decode_peer_op(&wire).expect("decode"), op);

    // Apply as the dnode receive loop would.
    applier.apply(&wire).await;

    // The value is now locally readable over the same datastore,
    // and applying the replica op did NOT re-forward it (the
    // applier holds no outbound channel: fan-out is impossible by
    // construction).
    let stored = ds
        .riak_get(b"users", b"alice")
        .await
        .expect("riak_get")
        .expect("object present after replica apply");
    let obj = HttpObject::from_storage_bytes(&stored).expect("decode envelope");
    assert_eq!(obj.value, b"alice-replicated");
}

#[tokio::test]
async fn receive_apply_del_removes_from_local_noxu() {
    let dir = scratch_dir();
    let noxu = NoxuDatastore::open_transactional(dir.path()).expect("open noxu");
    let ds: Arc<dyn Datastore> = Arc::new(noxu);
    let applier = ReplicaApplier::new(Arc::clone(&ds));

    // Seed a value, then replicate a delete.
    applier
        .apply(&encode_peer_op(&PeerOp::Put {
            bucket_type: b"default".to_vec(),
            bucket: b"users".to_vec(),
            key: b"bob".to_vec(),
            value: b"bob-value".to_vec(),
        }))
        .await;
    assert!(ds.riak_get(b"users", b"bob").await.expect("get").is_some());

    applier
        .apply(&encode_peer_op(&PeerOp::Del {
            bucket_type: b"default".to_vec(),
            bucket: b"users".to_vec(),
            key: b"bob".to_vec(),
        }))
        .await;
    assert!(
        ds.riak_get(b"users", b"bob").await.expect("get").is_none(),
        "replicated delete removed the object locally"
    );
}

#[tokio::test]
async fn outbound_receive_pairing_delivers_write_to_node_b() {
    // Node A routes a write; node B applies it. We stand in for
    // the dnode hop with encode_peer_op / decode_peer_op, matching
    // exactly the bytes PeerChannelOutbound ships and the receive
    // loop hands to the applier. (Mock transport: no live dnode
    // server loops; the encode/decode/apply contract is what this
    // asserts.)
    let dir_b = scratch_dir();
    let noxu_b = NoxuDatastore::open_transactional(dir_b.path()).expect("open noxu b");
    let ds_b: Arc<dyn Datastore> = Arc::new(noxu_b);
    let applier_b = ReplicaApplier::new(Arc::clone(&ds_b));

    // Node A: route the key and take the primary replica op the
    // router would fan out.
    let ring = five_peer_ring();
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(
        b"default",
        b"carts",
        BucketProps {
            keyfun: Some(KeyFun::Std),
            strategy: Some(ReplicationStrategy::Successors),
            n_val: Some(3),
            ..BucketProps::default()
        },
    );
    let router = BucketRouter::new(registry, ring, HashType::Murmur);
    let decision = router.route(b"default", b"carts", b"cart-9");
    assert!(
        !decision.replica_list().is_empty(),
        "successors plan yields at least the primary"
    );

    // The op node A ships to a replica peer.
    let op = PeerOp::Put {
        bucket_type: decision.bucket_type.clone(),
        bucket: b"carts".to_vec(),
        key: b"cart-9".to_vec(),
        value: b"two-items".to_vec(),
    };
    let wire = encode_peer_op(&op);

    // Node B receives and applies.
    applier_b.apply(&wire).await;

    let stored = ds_b
        .riak_get(b"carts", b"cart-9")
        .await
        .expect("riak_get")
        .expect("replica landed on node B");
    let obj = HttpObject::from_storage_bytes(&stored).expect("decode envelope");
    assert_eq!(obj.value, b"two-items");
}

/// The HTTP object `PUT` path fans out to the key's replicas via the
/// routing hooks, mirroring the PBC put path. Drives the real
/// `serve_http_with_routing` accept loop over a loopback listener,
/// sends one PUT, and asserts the capturing outbound recorded a
/// `PeerOp::Put` per replica on the key's preference list. This is
/// the regression for the wireup gap found on the EC2 cluster, where
/// HTTP writes were local-only (no fan-out) because routing was
/// wired to PBC alone.
#[tokio::test]
async fn http_put_fans_out_to_replicas() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = scratch_dir();
    let noxu = NoxuDatastore::open_transactional(dir.path()).expect("open noxu");
    let ds: Arc<dyn Datastore> = Arc::new(noxu);

    let ring = five_peer_ring();
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(
        b"default",
        b"users",
        BucketProps {
            keyfun: Some(KeyFun::Std),
            strategy: Some(ReplicationStrategy::Successors),
            n_val: Some(3),
            ..BucketProps::default()
        },
    );
    let router = Arc::new(BucketRouter::new(registry, ring, HashType::Murmur));
    let outbound = Arc::new(CapturingOutbound::default());
    let hooks = RoutingHooks {
        router,
        outbound: outbound.clone(),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let serve = tokio::spawn(dyniak::serve_http_with_routing(listener, ds, hooks));

    // Send one HTTP/1.1 PUT of an HttpObject envelope for users/alice.
    let body = br#"{"value":[104,105],"content_type":"text/plain","indexes":[],"links":[]}"#;
    let req = format!(
        "PUT /buckets/users/keys/alice HTTP/1.1\r\nHost: t\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut sock = tokio::net::TcpStream::connect(addr).await.expect("connect");
    sock.write_all(req.as_bytes()).await.expect("write head");
    sock.write_all(body).await.expect("write body");
    sock.flush().await.expect("flush");
    let mut resp = Vec::new();
    let _ = sock.read_to_end(&mut resp).await;
    let resp = String::from_utf8_lossy(&resp);
    assert!(
        resp.contains("204"),
        "PUT should be 204 No Content, got: {resp}"
    );

    serve.abort();

    // The put fanned out one PeerOp::Put per replica (n_val=3).
    let ops = outbound.ops.lock().expect("lock");
    assert_eq!(
        ops.len(),
        3,
        "n_val=3 yields 3 replica dispatches, got {}",
        ops.len()
    );
    for (_peer, op) in ops.iter() {
        match op {
            PeerOp::Put {
                bucket, key, value, ..
            } => {
                assert_eq!(bucket, b"users");
                assert_eq!(key, b"alice");
                assert_eq!(value, &vec![104u8, 105u8]);
            }
            other => panic!("expected PeerOp::Put, got {other:?}"),
        }
    }
    // Replicas are distinct peers.
    let mut peers: Vec<u32> = ops.iter().map(|(p, _)| *p).collect();
    peers.sort_unstable();
    peers.dedup();
    assert_eq!(peers.len(), 3, "replicas are distinct peers");
}
