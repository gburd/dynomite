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
use crate::router::PeerOp;

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
                bucket, key, value, ..
            } => {
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
                };
                let storage = envelope.to_storage_bytes();
                match self.datastore.riak_put(&bucket, &key, &storage, &[]).await {
                    Ok(()) | Err(DatastoreError::Unsupported(_)) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "riak replica: local put failed");
                    }
                }
            }
            PeerOp::Del { bucket, key, .. } => {
                match self.datastore.riak_delete(&bucket, &key).await {
                    Ok(_) | Err(DatastoreError::Unsupported(_)) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "riak replica: local delete failed");
                    }
                }
            }
            // Get replication is a read-repair / handoff read; write
            // replication does not need it. No-op for now.
            PeerOp::Get { .. } => {}
            PeerOp::DtUpdate {
                bucket, key, op, ..
            } => {
                // `op` is discriminated: a leading byte marks whether
                // the payload is a CRDT STATE (merge idempotently) or a
                // CRDT OP (apply/accumulate once). A replica fan ships
                // state; a coordinator forward ships an op.
                let store =
                    crate::crdt_store::CrdtStore::new(std::sync::Arc::clone(&self.datastore));
                let res = match op.split_first() {
                    Some((&crate::crdt_store::DT_WIRE_STATE, body)) => {
                        store.merge_state(&bucket, &key, body).await
                    }
                    Some((&crate::crdt_store::DT_WIRE_OP, body)) => {
                        match crate::crdt_store::CrdtOp::from_bytes(body) {
                            Ok(parsed) => store.apply(&bucket, &key, &parsed).await.map(|_| ()),
                            Err(e) => {
                                tracing::warn!(error = %e, "riak replica: undecodable dt op");
                                return;
                            }
                        }
                    }
                    _ => {
                        // Legacy / untagged payload: treat as state.
                        store.merge_state(&bucket, &key, &op).await
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
}
