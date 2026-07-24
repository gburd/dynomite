//! Read-coordination integration test for CRDT fetches.
//!
//! Proves the gap closed by the read-coordination path: a `DtFetch`
//! coordinated at a node that is NOT a replica of the key (or holds
//! only a partial state) fans the fetch to the key's replica set,
//! merges the returned states, and answers with the converged value.
//! This is the read analogue of the write-path replica fan.

#![cfg(feature = "noxu")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dyniak::bucket_props::BucketPropsRegistry;
use dyniak::crdt_store::{CrdtOp, CrdtStore};
use dyniak::datastore::NoxuDatastore;
use dyniak::datatypes::ActorId;
use dyniak::proto::pb::{DtFetchReq, DtFetchResp, MessageCode};
use dyniak::replication::{RingPoint, RingView};
use dyniak::router::{BucketRouter, PeerOp, PeerOutbound, RoutingHooks};
use dynomite::embed::hooks::BoxFuture;
use dynomite::embed::Datastore;
use dynomite::hashkit::HashType;
use prost::Message;

/// A `PeerOutbound` fixture backed by one in-memory datastore per
/// peer. `dispatch` applies writes; `request` answers a `DtFetch`
/// with the peer's local serialized state -- exactly what the real
/// `ReplicaApplier::apply_query` does over the wire.
struct InMemPeers {
    stores: HashMap<u32, Arc<NoxuDatastore>>,
    // Records fetch fan-out for assertions.
    fetches: Mutex<Vec<u32>>,
    // Keep the temp dirs alive for the test's lifetime.
    _dirs: Vec<tempfile::TempDir>,
}

impl std::fmt::Debug for InMemPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemPeers")
            .field("peers", &self.stores.len())
            .finish_non_exhaustive()
    }
}

impl InMemPeers {
    fn new(peers: &[u32]) -> Self {
        let mut stores = HashMap::new();
        let mut dirs = Vec::new();
        for &p in peers {
            let dir = tempfile::tempdir().expect("tempdir");
            let ds = NoxuDatastore::open_transactional(dir.path()).expect("open noxu");
            stores.insert(p, Arc::new(ds));
            dirs.push(dir);
        }
        Self {
            stores,
            fetches: Mutex::new(Vec::new()),
            _dirs: dirs,
        }
    }

    /// Seed peer `idx` with a single-actor counter contribution.
    async fn seed_counter(&self, idx: u32, actor: &ActorId, bucket: &[u8], key: &[u8], delta: i64) {
        let ds = self.stores.get(&idx).expect("seeded peer exists");
        let op = CrdtOp::Counter {
            actor: actor.clone(),
            delta,
        };
        CrdtStore::apply_borrowed_with_state(ds.as_ref(), bucket, key, &op)
            .await
            .expect("seed apply");
    }
}

impl PeerOutbound for InMemPeers {
    fn dispatch(&self, peer_idx: u32, op: PeerOp) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if let PeerOp::DtUpdate {
                bucket, key, op, ..
            } = op
            {
                if let Some(ds) = self.stores.get(&peer_idx) {
                    // The wire is discriminated; a state fan starts
                    // with DT_WIRE_STATE.
                    if let Some((_disc, body)) = op.split_first() {
                        let _ =
                            CrdtStore::merge_state_borrowed(ds.as_ref(), &bucket, &key, body).await;
                    }
                }
            }
        })
    }

    fn request(&self, peer_idx: u32, op: PeerOp) -> BoxFuture<'_, Option<Vec<u8>>> {
        Box::pin(async move {
            let PeerOp::DtFetch { bucket, key, .. } = op else {
                return None;
            };
            self.fetches.lock().expect("lock").push(peer_idx);
            let ds = self.stores.get(&peer_idx)?;
            match ds.riak_get(&bucket, &key).await {
                Ok(Some(s)) => Some(s),
                _ => Some(Vec::new()),
            }
        })
    }
}

/// Build routing hooks over a 4-peer ring with distinct tokens, with
/// the local peer at `local_idx`.
fn hooks_over(peers: &[u32], local_idx: u32, outbound: Arc<InMemPeers>) -> RoutingHooks {
    let span = u64::from(u32::MAX);
    let mut points = Vec::new();
    for (i, &p) in peers.iter().enumerate() {
        let token = (i as u64) * span / (peers.len() as u64);
        points.push(RingPoint::new(token, p, "dc1", format!("r{p}")));
    }
    let ring = RingView::new(points);
    let router = BucketRouter::new(
        Arc::new(BucketPropsRegistry::new_riak_defaults()),
        Arc::new(ring),
        HashType::Murmur,
    );
    RoutingHooks {
        router: Arc::new(router),
        outbound,
        local_actor: ActorId::new("dc1", format!("r{local_idx}")),
        local_peer_idx: local_idx,
    }
}

#[tokio::test]
async fn fetch_at_a_replica_merges_the_replica_set() {
    // 4 peers, n_val default 3. Pick a key, find its replica set, seed
    // each replica with a distinct actor's contribution, then fetch at
    // one replica and confirm the merge returns the sum.
    let peers = [0u32, 1, 2, 3];
    let outbound = Arc::new(InMemPeers::new(&peers));

    // Coordinator = peer 0. Build hooks to learn the replica set.
    let hooks = hooks_over(&peers, 0, Arc::clone(&outbound));
    let bucket = b"cart".to_vec();
    let key = b"widgets".to_vec();
    let btype = b"counters".to_vec();
    let decision = hooks
        .router
        .try_route(&btype, &bucket, &key)
        .expect("route");
    let replicas: Vec<u32> = decision.replica_list().iter().map(|r| r.peer_idx).collect();
    assert_eq!(replicas.len(), 3, "n_val=3 gives 3 replicas: {replicas:?}");

    // Seed each replica with a distinct actor contributing a distinct
    // amount. Sum = 5 + 7 + 11 = 23.
    let amounts = [5i64, 7, 11];
    for (i, &r) in replicas.iter().enumerate() {
        let actor = ActorId::new("dc1", format!("actor{r}"));
        outbound
            .seed_counter(r, &actor, &bucket, &key, amounts[i])
            .await;
    }

    // Coordinate the fetch at the FIRST replica. Its local store holds
    // only its own contribution; read coordination must fan to the
    // other two replicas and merge to 23.
    let coord = replicas[0];
    let hooks_c = hooks_over(&peers, coord, Arc::clone(&outbound));
    let local_ds = Arc::clone(outbound.stores.get(&coord).expect("coord store"));

    let req = DtFetchReq {
        r#type: btype.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        ..DtFetchReq::default()
    };
    let frame = dyniak::server::handle_dt_fetch_for_test(
        &req.encode_to_vec(),
        local_ds.as_ref(),
        Some(&hooks_c),
    )
    .await
    .expect("fetch");
    assert_eq!(frame.code, MessageCode::DtFetchResp.as_u8());
    let resp = DtFetchResp::decode(frame.body.as_slice()).expect("decode resp");
    let got = resp
        .value
        .and_then(|v| v.counter_value)
        .expect("counter value present");
    assert_eq!(
        got, 23,
        "read coordination must merge the replica set to the full sum"
    );

    // It must have fanned to the two OTHER replicas (not to itself).
    let fetched = outbound.fetches.lock().expect("lock").clone();
    assert!(!fetched.contains(&coord), "must not fetch from self");
    assert_eq!(
        fetched.len(),
        2,
        "fanned to the other 2 replicas: {fetched:?}"
    );
}
