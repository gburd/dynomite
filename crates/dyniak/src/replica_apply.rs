//! Local application of inbound cross-node object-replica ops.
//!
//! [`ReplicaApplier`] is the receive-side counterpart to the
//! request-time replica fan-out driven by
//! [`crate::router::RoutingHooks`]. It implements the engine's
//! [`dynomite::net::ReplicaApplySink`] trait: the dnode peer
//! receive loop hands it the payload of every
//! [`DmsgType::RiakReplica`](dynomite::proto::dnode::DmsgType::RiakReplica)
//! frame, and the applier decodes the [`PeerOp`] and applies it to
//! the LOCAL [`Datastore`].
//!
//! # Terminal, local-only
//!
//! Applying a replica op is terminal: the applier calls the
//! datastore's `riak_put` / `riak_delete` methods directly and
//! never routes the op back onto the ring, so a replica write fans
//! out exactly once (the originating node forwards to each replica;
//! each replica applies locally and stops). This matches the
//! `DmsgType::ReqForward` `LocalNodeOnly` contract the RESP path
//! uses for its own no-re-forward guarantee.
//!
//! A [`PeerOp::Get`] carries a read-repair / handoff read; write
//! replication does not need it, so the applier treats it as a
//! no-op. Get forwarding can be wired to a real read-repair path in
//! a later slice without changing the wire format.

use std::sync::Arc;

use dynomite::embed::hooks::DatastoreError;
use dynomite::embed::Datastore;
use dynomite::net::client::BoxFuture;
use dynomite::net::ReplicaApplySink;

use crate::proto::http::object::HttpObject;
use crate::proto::replica_wire::decode_peer_op;
use crate::router::{PeerOp, ACK_STORED, ACK_STORED_DURABLE};

/// Applies inbound replica ops to a local [`Datastore`].
///
/// Cheap to clone via the shared [`Arc`] datastore handle.
#[derive(Clone)]
pub struct ReplicaApplier {
    datastore: Arc<dyn Datastore>,
}

impl std::fmt::Debug for ReplicaApplier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaApplier").finish_non_exhaustive()
    }
}

impl ReplicaApplier {
    /// Wrap a local datastore as a replica-apply sink.
    #[must_use]
    pub fn new(datastore: Arc<dyn Datastore>) -> Self {
        Self { datastore }
    }

    /// Ack byte a successful [`PeerOp::RepairPut`] apply replies with:
    /// [`ACK_STORED_DURABLE`] when the local store can confirm the
    /// write reached durable storage, [`ACK_STORED`] otherwise. See
    /// [`crate::datastore::write_is_durable`] for which backends can
    /// make that call.
    fn durable_ack_byte(&self) -> u8 {
        if crate::datastore::write_is_durable(self.datastore.as_ref()) {
            ACK_STORED_DURABLE
        } else {
            ACK_STORED
        }
    }

    /// Decode and apply one replica op to the local store.
    ///
    /// Errors are logged and swallowed: an `Unsupported`
    /// datastore or a transient store error must not tear down
    /// the peer receive loop, and anti-entropy reconciles a
    /// dropped replica later.
    async fn apply_op(&self, payload: &[u8]) {
        let op = match decode_peer_op(payload) {
            Ok(op) => op,
            Err(e) => {
                tracing::warn!(error = %e, "riak replica: undecodable peer op dropped");
                return;
            }
        };
        match op {
            PeerOp::Put {
                bucket_type,
                bucket,
                key,
                value,
            } => {
                let sbucket = crate::router::composite_storage_bucket(&bucket_type, &bucket);
                // Persist in the canonical `HttpObject` storage form
                // the local put path writes, so a replica write is
                // readable over PBC / HTTP exactly like a native put.
                // The forwarded op carries only the value bytes; 2i
                // indexes and links live in the primary's stored
                // envelope and are reconciled by anti-entropy, so the
                // replica stores a bare-value envelope with no index
                // associations.
                let envelope = HttpObject {
                    value,
                    content_type: None,
                    indexes: Vec::new(),
                    links: Vec::new(),
                    context: Vec::new(),
                    written_at_unix: 0,
                };
                let storage = envelope.to_storage_bytes();
                match self.datastore.riak_put(&sbucket, &key, &storage, &[]).await {
                    Ok(()) | Err(DatastoreError::Unsupported(_)) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "riak replica: local put failed");
                    }
                }
            }
            PeerOp::Del {
                bucket_type,
                bucket,
                key,
            } => {
                let sbucket = crate::router::composite_storage_bucket(&bucket_type, &bucket);
                match self.datastore.riak_delete(&sbucket, &key).await {
                    Ok(_) | Err(DatastoreError::Unsupported(_)) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "riak replica: local delete failed");
                    }
                }
            }
            PeerOp::RepairPut {
                bucket_type,
                bucket,
                key,
                storage,
            } => {
                let sbucket = crate::router::composite_storage_bucket(&bucket_type, &bucket);
                // Store the coordinator's canonical SiblingSet bytes
                // verbatim: this is a byte-identical, causally-correct
                // copy (write fan-out or read-repair), not a bare value
                // to re-wrap.
                match self.datastore.riak_put(&sbucket, &key, &storage, &[]).await {
                    Ok(()) | Err(DatastoreError::Unsupported(_)) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "riak replica: repair put failed");
                    }
                }
            }
            // A Get is a read-repair / handoff read and a DtFetch is a
            // read-coordination QUERY answered by `apply_query`; neither
            // is a write, so the write-apply path is a no-op for both.
            PeerOp::Get { .. } | PeerOp::DtFetch { .. } => {}
            PeerOp::DtUpdate {
                bucket_type,
                bucket,
                key,
                op,
            } => {
                let sbucket = crate::router::composite_storage_bucket(&bucket_type, &bucket);
                // `op` is discriminated: a leading byte marks whether
                // the payload is a CRDT STATE (merge idempotently) or a
                // CRDT OP (apply/accumulate once). A replica fan ships
                // state; a coordinator forward ships an op.
                let store =
                    crate::crdt_store::CrdtStore::new(std::sync::Arc::clone(&self.datastore));
                let res = match op.split_first() {
                    Some((&crate::crdt_store::DT_WIRE_STATE, body)) => {
                        store.merge_state(&sbucket, &key, body).await
                    }
                    Some((&crate::crdt_store::DT_WIRE_OP, body)) => {
                        match crate::crdt_store::CrdtOp::from_bytes(body) {
                            Ok(parsed) => store.apply(&sbucket, &key, &parsed).await.map(|_| ()),
                            Err(e) => {
                                tracing::warn!(error = %e, "riak replica: undecodable dt op");
                                return;
                            }
                        }
                    }
                    _ => {
                        // Legacy / untagged payload: treat as state.
                        store.merge_state(&sbucket, &key, &op).await
                    }
                };
                if let Err(e) = res {
                    if !matches!(
                        e,
                        crate::crdt_store::CrdtStoreError::Datastore(DatastoreError::Unsupported(
                            _
                        ))
                    ) {
                        tracing::warn!(error = %e, "riak replica: local dt apply/merge failed");
                    }
                }
            }
        }
    }
}

impl ReplicaApplySink for ReplicaApplier {
    fn apply<'a>(&'a self, payload: &'a [u8]) -> BoxFuture<'a, ()> {
        Box::pin(async move { self.apply_op(payload).await })
    }

    fn apply_query<'a>(&'a self, payload: &'a [u8]) -> BoxFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let op = decode_peer_op(payload).ok()?;
            match op {
                // Read queries (DtFetch / Get) reply with the local
                // stored bytes so the coordinator can merge across the
                // replica set: a DtFetch returns the serialized CRDT
                // state, a Get the serialized SiblingSet. An absent key
                // replies empty.
                PeerOp::DtFetch {
                    bucket_type,
                    bucket,
                    key,
                    ..
                }
                | PeerOp::Get {
                    bucket_type,
                    bucket,
                    key,
                } => {
                    let sbucket = crate::router::composite_storage_bucket(&bucket_type, &bucket);
                    match self.datastore.riak_get(&sbucket, &key).await {
                        Ok(Some(bytes)) => Some(bytes),
                        Ok(None) => Some(Vec::new()),
                        Err(e) => {
                            tracing::debug!(error = %e, "riak replica: read-coordination read failed");
                            Some(Vec::new())
                        }
                    }
                }
                // A RepairPut stores the canonical SiblingSet verbatim
                // AND replies with an ack byte so the coordinator can
                // count it toward the write quorum W (and, when the
                // reply is `ACK_STORED_DURABLE`, the durable-write
                // quorum DW too). A store failure replies empty (not
                // counted).
                PeerOp::RepairPut {
                    bucket_type,
                    bucket,
                    key,
                    storage,
                } => {
                    let sbucket = crate::router::composite_storage_bucket(&bucket_type, &bucket);
                    match self.datastore.riak_put(&sbucket, &key, &storage, &[]).await {
                        Ok(()) | Err(DatastoreError::Unsupported(_)) => {
                            Some(vec![self.durable_ack_byte()])
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "riak replica: repair put (acked) failed");
                            Some(Vec::new())
                        }
                    }
                }
                // Other ops (Put/Del/DtUpdate) are fire-and-forget:
                // return None so the receive loop calls `apply`.
                _ => None,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::replica_wire::encode_peer_op;

    #[tokio::test]
    async fn undecodable_payload_is_dropped_without_panic() {
        // A garbage payload must not panic the receive loop; the
        // applier logs and returns. We use the in-memory default
        // datastore (riak_put unsupported) so no store is needed.
        let ds: Arc<dyn Datastore> = Arc::new(dynomite::embed::MemoryDatastore::new());
        let applier = ReplicaApplier::new(ds);
        applier.apply(&[0xFF, 0x00, 0x01]).await;
        // A well-formed Put against an Unsupported datastore is
        // swallowed cleanly.
        let op = PeerOp::Put {
            bucket_type: b"default".to_vec(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        applier.apply(&encode_peer_op(&op)).await;
    }

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    use dynomite::embed::hooks::Protocol;
    use dynomite::msg::{Msg, MsgType};
    use parking_lot::Mutex;

    use crate::crdt_store::{CrdtOp, CrdtStore, CrdtValue, DT_WIRE_OP, DT_WIRE_STATE};
    use crate::datatypes::{counter_to_bytes, ActorId, PnCounter, TAG_COUNTER};
    use crate::router::composite_storage_bucket;

    /// In-memory riak store double; `fail` turns every call into a
    /// backend error so the warn-and-swallow arms are exercised.
    type Keyspace = HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>;

    #[derive(Default)]
    struct Store {
        map: Mutex<Keyspace>,
        fail: AtomicBool,
    }

    impl Store {
        fn get(&self, bucket: &[u8], key: &[u8]) -> Option<Vec<u8>> {
            self.map
                .lock()
                .get(&(bucket.to_vec(), key.to_vec()))
                .cloned()
        }
        fn err(&self) -> Option<DatastoreError> {
            self.fail
                .load(Ordering::SeqCst)
                .then(|| DatastoreError::Backend("injected".into()))
        }
    }

    impl Datastore for Store {
        fn protocol(&self) -> Protocol {
            Protocol::Custom
        }
        fn dispatch(&self, _req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
            Box::pin(async { Err(DatastoreError::Unsupported(MsgType::Unknown)) })
        }
        fn riak_get<'a>(
            &'a self,
            bucket: &'a [u8],
            key: &'a [u8],
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, DatastoreError>> {
            let r = self.err().map_or_else(|| Ok(self.get(bucket, key)), Err);
            Box::pin(async move { r })
        }
        fn riak_put<'a>(
            &'a self,
            bucket: &'a [u8],
            key: &'a [u8],
            value: &'a [u8],
            _indexes: &'a [(Vec<u8>, Vec<u8>)],
        ) -> BoxFuture<'a, Result<(), DatastoreError>> {
            let r = self.err().map_or_else(
                || {
                    self.map
                        .lock()
                        .insert((bucket.to_vec(), key.to_vec()), value.to_vec());
                    Ok(())
                },
                Err,
            );
            Box::pin(async move { r })
        }
        fn riak_delete<'a>(
            &'a self,
            bucket: &'a [u8],
            key: &'a [u8],
        ) -> BoxFuture<'a, Result<bool, DatastoreError>> {
            let r = self.err().map_or_else(
                || {
                    Ok(self
                        .map
                        .lock()
                        .remove(&(bucket.to_vec(), key.to_vec()))
                        .is_some())
                },
                Err,
            );
            Box::pin(async move { r })
        }
    }

    fn setup() -> (Arc<Store>, ReplicaApplier) {
        let store = Arc::new(Store::default());
        let ds: Arc<dyn Datastore> = store.clone();
        (store, ReplicaApplier::new(ds))
    }

    fn sb() -> Vec<u8> {
        composite_storage_bucket(b"default", b"b")
    }

    fn repair(storage: &[u8]) -> Vec<u8> {
        encode_peer_op(&PeerOp::RepairPut {
            bucket_type: b"default".to_vec(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
            storage: storage.to_vec(),
        })
    }

    fn dt_update(op: Vec<u8>) -> Vec<u8> {
        encode_peer_op(&PeerOp::DtUpdate {
            bucket_type: b"default".to_vec(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
            op,
        })
    }

    fn counter_op(peer: &str, delta: i64) -> CrdtOp {
        CrdtOp::Counter {
            actor: ActorId::new("dc1", peer),
            delta,
        }
    }

    async fn counter_value(store: &Arc<Store>) -> CrdtValue {
        let ds: Arc<dyn Datastore> = store.clone();
        CrdtStore::new(ds)
            .fetch(&sb(), b"k", TAG_COUNTER)
            .await
            .unwrap()
    }

    #[test]
    fn debug_does_not_leak_the_datastore() {
        let (_, a) = setup();
        assert_eq!(format!("{a:?}"), "ReplicaApplier { .. }");
    }

    #[tokio::test]
    async fn put_stores_a_bare_value_envelope_and_del_removes_it() {
        let (store, a) = setup();
        let put = PeerOp::Put {
            bucket_type: Vec::new(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
            value: b"hello".to_vec(),
        };
        // A write op is not a query: the receive loop must call apply.
        assert_eq!(a.apply_query(&encode_peer_op(&put)).await, None);
        a.apply(&encode_peer_op(&put)).await;
        let stored = store.get(&sb(), b"k").expect("stored");
        let obj = HttpObject::from_storage_bytes(&stored).unwrap();
        assert_eq!(obj.value, b"hello");
        assert!(obj.indexes.is_empty() && obj.links.is_empty());
        assert_eq!(obj.content_type, None);

        let del = PeerOp::Del {
            bucket_type: b"default".to_vec(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
        };
        a.apply(&encode_peer_op(&del)).await;
        assert_eq!(store.get(&sb(), b"k"), None);
        // Deleting an absent key is fine too.
        a.apply(&encode_peer_op(&del)).await;
    }

    #[tokio::test]
    async fn repair_put_stores_bytes_verbatim_on_both_paths() {
        let (store, a) = setup();
        a.apply(&repair(b"siblings-1")).await;
        assert_eq!(store.get(&sb(), b"k").unwrap(), b"siblings-1");
        let ack = a.apply_query(&repair(b"siblings-2")).await;
        assert_eq!(ack, Some(vec![a.durable_ack_byte()]));
        assert!(matches!(ack.unwrap()[0], ACK_STORED | ACK_STORED_DURABLE));
        assert_eq!(store.get(&sb(), b"k").unwrap(), b"siblings-2");
    }

    #[tokio::test]
    async fn get_and_dt_fetch_are_write_no_ops_but_answer_queries() {
        let (store, a) = setup();
        let get = encode_peer_op(&PeerOp::Get {
            bucket_type: b"default".to_vec(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
        });
        let fetch = encode_peer_op(&PeerOp::DtFetch {
            bucket_type: b"default".to_vec(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
            tag: TAG_COUNTER,
        });
        a.apply(&get).await;
        a.apply(&fetch).await;
        assert!(store.map.lock().is_empty());
        // Absent key answers empty.
        assert_eq!(a.apply_query(&get).await, Some(Vec::new()));
        store
            .map
            .lock()
            .insert((sb(), b"k".to_vec()), b"state".to_vec());
        assert_eq!(a.apply_query(&get).await, Some(b"state".to_vec()));
        assert_eq!(a.apply_query(&fetch).await, Some(b"state".to_vec()));
        // Undecodable query payload is not answered.
        assert_eq!(a.apply_query(&[0xEE]).await, None);
    }

    #[tokio::test]
    async fn dt_update_op_accumulates_and_state_merges_idempotently() {
        let (store, a) = setup();
        // OP form applies once per delivery.
        a.apply(&dt_update(counter_op("a", 2).to_op_wire())).await;
        a.apply(&dt_update(counter_op("a", 3).to_op_wire())).await;
        assert_eq!(counter_value(&store).await, CrdtValue::Counter(5));
        // STATE form merges: re-delivery does not double-count.
        let mut remote = PnCounter::new();
        remote.increment(&ActorId::new("dc1", "b"), 4);
        let state = crate::crdt_store::to_state_wire(&counter_to_bytes(&remote));
        assert_eq!(state[0], DT_WIRE_STATE);
        a.apply(&dt_update(state.clone())).await;
        a.apply(&dt_update(state)).await;
        assert_eq!(counter_value(&store).await, CrdtValue::Counter(9));
        // An untagged payload whose first byte is neither discriminator
        // takes the legacy state path; a valid state blob can never get
        // there because its FORMAT_V1 version byte equals DT_WIRE_OP.
        let raw = counter_to_bytes(&remote);
        assert_eq!(raw[0], DT_WIRE_OP);
        a.apply(&dt_update(vec![7, 1, 1])).await;
        assert_eq!(counter_value(&store).await, CrdtValue::Counter(9));
    }

    #[tokio::test]
    async fn dt_update_bad_payloads_are_dropped_without_writing() {
        let (store, a) = setup();
        // Op discriminator followed by garbage.
        a.apply(&dt_update(vec![DT_WIRE_OP, 0xFF, 0xFF])).await;
        // Empty payload falls back to the state path and fails to parse.
        a.apply(&dt_update(Vec::new())).await;
        // State discriminator followed by a corrupt blob.
        a.apply(&dt_update(vec![DT_WIRE_STATE, 9, 9])).await;
        assert!(store.map.lock().is_empty());
    }

    #[tokio::test]
    async fn backend_failures_are_swallowed_and_queries_answer_empty() {
        let (store, a) = setup();
        store.fail.store(true, Ordering::SeqCst);
        let put = PeerOp::Put {
            bucket_type: b"default".to_vec(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let del = PeerOp::Del {
            bucket_type: b"default".to_vec(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
        };
        a.apply(&encode_peer_op(&put)).await;
        a.apply(&encode_peer_op(&del)).await;
        a.apply(&repair(b"s")).await;
        a.apply(&dt_update(counter_op("a", 1).to_op_wire())).await;
        // A failed acked repair replies empty: not counted toward W.
        assert_eq!(a.apply_query(&repair(b"s")).await, Some(Vec::new()));
        let get = encode_peer_op(&PeerOp::Get {
            bucket_type: b"default".to_vec(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
        });
        assert_eq!(a.apply_query(&get).await, Some(Vec::new()));
        store.fail.store(false, Ordering::SeqCst);
        assert!(store.map.lock().is_empty());
    }

    #[tokio::test]
    async fn unsupported_datastore_acks_repair_and_answers_reads_empty() {
        let ds: Arc<dyn Datastore> = Arc::new(dynomite::embed::MemoryDatastore::new());
        let a = ReplicaApplier::new(ds);
        // Unsupported riak_put still acks: the in-memory store has no
        // riak keyspace and anti-entropy owns the gap.
        assert_eq!(
            a.apply_query(&repair(b"s")).await,
            Some(vec![ACK_STORED_DURABLE])
        );
        a.apply(&repair(b"s")).await;
        let del = PeerOp::Del {
            bucket_type: b"default".to_vec(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
        };
        a.apply(&encode_peer_op(&del)).await;
        a.apply(&dt_update(counter_op("a", 1).to_op_wire())).await;
        let get = encode_peer_op(&PeerOp::Get {
            bucket_type: b"default".to_vec(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
        });
        assert_eq!(a.apply_query(&get).await, Some(Vec::new()));
    }
}
