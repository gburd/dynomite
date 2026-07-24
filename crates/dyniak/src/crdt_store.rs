//! Convergent CRDT apply against the local datastore.
//!
//! A CRDT-typed key stores its full state-based value (see
//! [`crate::datatypes::serial`]) under `(bucket, key)`. Both the
//! client-facing update path ([`crate::server`]) and the replica-apply
//! path ([`crate::replica_apply`]) converge through the same routine:
//! read the current stored state, merge the incoming operation as a
//! contribution attributed to the originating node's actor, and write
//! the merged state back. Because the merge is a join-semilattice
//! operation (commutative, associative, idempotent), applying an op on
//! any replica, in any order, even more than once, converges to the
//! same value -- which is why a single-key CRDT update is always
//! accepted locally without a quorum and survives partitions and ring
//! changes.
//!
//! A per-key async mutex serialises apply on one node so two concurrent
//! local updates to the same key do not lose an update between the read
//! and the write-back; cross-node concurrency needs no coordination
//! because the CRDT merge resolves it.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex as SyncMutex;
use tokio::sync::Mutex;

use crate::datatypes::{
    counter_from_bytes, counter_to_bytes, peek_tag, set_from_bytes, set_to_bytes, ActorId, Crdt,
    OrSet, PnCounter, TAG_COUNTER, TAG_SET,
};
use dynomite::embed::hooks::{Datastore, DatastoreError};

/// Replica-apply wire discriminator: the payload is a serialized CRDT
/// STATE to be merged idempotently (element-wise max).
pub const DT_WIRE_STATE: u8 = 0;
/// Replica-apply wire discriminator: the payload is a serialized CRDT
/// OP to be applied (accumulated) once by the receiving replica.
pub const DT_WIRE_OP: u8 = 1;

/// Wrap a serialized CRDT state for the replica-apply wire (merge path).
#[must_use]
pub fn to_state_wire(state_bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(state_bytes.len() + 1);
    out.push(DT_WIRE_STATE);
    out.extend_from_slice(state_bytes);
    out
}

/// A CRDT operation to apply to a key, carrying the originating
/// actor so each node's contribution is attributed distinctly (the
/// per-actor G-Counter columns are what let concurrent increments sum
/// rather than overwrite).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CrdtOp {
    /// PN-counter delta (signed).
    Counter {
        /// Actor that produced the delta.
        actor: ActorId,
        /// Signed increment/decrement.
        delta: i64,
    },
    /// OR-set add / remove batch.
    Set {
        /// Actor that produced the operation.
        actor: ActorId,
        /// Elements added.
        adds: Vec<Vec<u8>>,
        /// Elements removed (observed-remove).
        removes: Vec<Vec<u8>>,
    },
}

impl CrdtOp {
    /// The stored type tag this op applies to.
    #[must_use]
    pub fn type_tag(&self) -> u8 {
        match self {
            CrdtOp::Counter { .. } => TAG_COUNTER,
            CrdtOp::Set { .. } => TAG_SET,
        }
    }

    /// Wire form for FORWARDING this op to the primary replica so it
    /// APPLIES (accumulates) the op. A leading discriminator byte marks
    /// op-vs-state so the replica-apply path knows whether to accumulate
    /// (op) or merge idempotently (state).
    #[must_use]
    pub fn to_op_wire(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(48);
        out.push(DT_WIRE_OP);
        out.extend_from_slice(&self.to_bytes());
        out
    }

    /// Build the serialized single-contribution CRDT STATE for this op
    /// (the op applied to an empty CRDT), suitable for shipping to a
    /// replica that will merge it. Because merge is idempotent
    /// (element-wise max), a replica can merge this state any number of
    /// times, in any order, and converge -- so the coordinator can fan
    /// the same state to every replica whether or not it is itself a
    /// replica, and no single node needs to hold the authoritative base.
    #[must_use]
    pub fn to_state_bytes(&self) -> Vec<u8> {
        match self {
            CrdtOp::Counter { actor, delta } => {
                let mut c = PnCounter::new();
                c.apply(actor, *delta);
                counter_to_bytes(&c)
            }
            CrdtOp::Set {
                actor,
                adds,
                removes,
            } => {
                let mut s = OrSet::new();
                for e in adds {
                    s.add(actor, e.clone());
                }
                for e in removes {
                    s.remove(e);
                }
                set_to_bytes(&s)
            }
        }
    }

    /// Serialize the op for the replication wire (carried in
    /// `PeerOp::DtUpdate.op`). Length-prefixed, self-describing.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32);
        out.push(self.type_tag());
        match self {
            CrdtOp::Counter { actor, delta } => {
                put_lp(&mut out, actor.dc.as_bytes());
                put_lp(&mut out, actor.peer.as_bytes());
                out.extend_from_slice(&delta.to_be_bytes());
            }
            CrdtOp::Set {
                actor,
                adds,
                removes,
            } => {
                put_lp(&mut out, actor.dc.as_bytes());
                put_lp(&mut out, actor.peer.as_bytes());
                out.extend_from_slice(&(adds.len() as u64).to_be_bytes());
                for a in adds {
                    put_lp(&mut out, a);
                }
                out.extend_from_slice(&(removes.len() as u64).to_be_bytes());
                for r in removes {
                    put_lp(&mut out, r);
                }
            }
        }
        out
    }

    /// Parse an op from [`CrdtOp::to_bytes`].
    ///
    /// # Errors
    /// [`CrdtSerialError::Truncated`] on a short buffer,
    /// [`CrdtSerialError::UnknownTag`] on an unknown type tag.
    pub fn from_bytes(buf: &[u8]) -> Result<Self, crate::datatypes::CrdtSerialError> {
        use crate::datatypes::CrdtSerialError;
        let mut r = OpReader { buf, pos: 0 };
        let tag = r.u8()?;
        let dc = r.string()?;
        let peer = r.string()?;
        let actor = ActorId::new(dc, peer);
        match tag {
            TAG_COUNTER => {
                let delta = r.i64()?;
                Ok(CrdtOp::Counter { actor, delta })
            }
            TAG_SET => {
                let na = usize::try_from(r.u64()?)
                    .map_err(|_| crate::datatypes::CrdtSerialError::Truncated)?;
                let mut adds = Vec::with_capacity(na);
                for _ in 0..na {
                    adds.push(r.bytes()?);
                }
                let nr = usize::try_from(r.u64()?)
                    .map_err(|_| crate::datatypes::CrdtSerialError::Truncated)?;
                let mut removes = Vec::with_capacity(nr);
                for _ in 0..nr {
                    removes.push(r.bytes()?);
                }
                Ok(CrdtOp::Set {
                    actor,
                    adds,
                    removes,
                })
            }
            other => Err(CrdtSerialError::UnknownTag(other)),
        }
    }
}

fn put_lp(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u64).to_be_bytes());
    out.extend_from_slice(b);
}

struct OpReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl OpReader<'_> {
    fn u8(&mut self) -> Result<u8, crate::datatypes::CrdtSerialError> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or(crate::datatypes::CrdtSerialError::Truncated)?;
        self.pos += 1;
        Ok(b)
    }
    fn u64(&mut self) -> Result<u64, crate::datatypes::CrdtSerialError> {
        let end = self.pos + 8;
        let s = self
            .buf
            .get(self.pos..end)
            .ok_or(crate::datatypes::CrdtSerialError::Truncated)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(s);
        self.pos = end;
        Ok(u64::from_be_bytes(a))
    }
    fn i64(&mut self) -> Result<i64, crate::datatypes::CrdtSerialError> {
        Ok(i64::from_be_bytes(self.u64()?.to_be_bytes()))
    }
    fn bytes(&mut self) -> Result<Vec<u8>, crate::datatypes::CrdtSerialError> {
        let n = usize::try_from(self.u64()?)
            .map_err(|_| crate::datatypes::CrdtSerialError::Truncated)?;
        let end = self.pos + n;
        let s = self
            .buf
            .get(self.pos..end)
            .ok_or(crate::datatypes::CrdtSerialError::Truncated)?;
        self.pos = end;
        Ok(s.to_vec())
    }
    fn string(&mut self) -> Result<String, crate::datatypes::CrdtSerialError> {
        String::from_utf8(self.bytes()?).map_err(|_| crate::datatypes::CrdtSerialError::Truncated)
    }
}

/// The projected value of a CRDT after applying an op or on fetch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CrdtValue {
    /// Counter total.
    Counter(i64),
    /// Set members.
    Set(Vec<Vec<u8>>),
    /// The key does not exist yet.
    Missing,
}

/// Project a serialized CRDT state to its value without a datastore.
///
/// Used to report the value of a just-computed contribution when the
/// coordinating node is not itself a replica of the key (so it has no
/// local stored state to read). `tag` selects the projection.
///
/// # Errors
/// [`crate::datatypes::CrdtSerialError`] on a corrupt blob or a tag
/// that does not match the requested projection.
pub fn project_state(
    state: &[u8],
    tag: u8,
) -> Result<CrdtValue, crate::datatypes::CrdtSerialError> {
    match tag {
        TAG_COUNTER => Ok(CrdtValue::Counter(counter_from_bytes(state)?.value())),
        TAG_SET => Ok(CrdtValue::Set(
            set_from_bytes(state)?.value().into_iter().collect(),
        )),
        other => Err(crate::datatypes::CrdtSerialError::UnknownTag(other)),
    }
}

/// Error applying or fetching a CRDT.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CrdtStoreError {
    /// The datastore rejected a read or write.
    #[error("crdt store: datastore error: {0}")]
    Datastore(#[from] DatastoreError),
    /// The stored blob could not be decoded, or its type tag did not
    /// match the requested operation.
    #[error("crdt store: {0}")]
    Serial(#[from] crate::datatypes::CrdtSerialError),
}

/// Convergent CRDT store over a [`Datastore`].
///
/// Per-key async lock table: maps an encoded `(bucket, key)` to the
/// mutex that serialises local apply on that key.
type KeyLocks = Arc<SyncMutex<HashMap<Vec<u8>, Arc<Mutex<()>>>>>;

/// Cheap to clone; the per-key lock table is shared behind an `Arc`.
#[derive(Clone)]
pub struct CrdtStore {
    datastore: Arc<dyn Datastore>,
    locks: KeyLocks,
}

impl CrdtStore {
    /// Wrap a datastore.
    #[must_use]
    pub fn new(datastore: Arc<dyn Datastore>) -> Self {
        Self {
            datastore,
            locks: Arc::new(SyncMutex::new(HashMap::new())),
        }
    }

    /// Apply an op against a borrowed datastore without a per-key lock.
    ///
    /// The PBC handlers hold only a `&dyn Datastore`, so this borrows
    /// it for a single read-merge-write. Cross-request local ordering
    /// is left to the datastore's own per-key write atomicity; a rare
    /// read-then-write interleaving on one node self-corrects through
    /// replication and anti-entropy because the merge is idempotent and
    /// commutative.
    ///
    /// # Errors
    /// As [`CrdtStore::apply`].
    pub async fn apply_borrowed(
        datastore: &dyn Datastore,
        bucket: &[u8],
        key: &[u8],
        op: &CrdtOp,
    ) -> Result<CrdtValue, CrdtStoreError> {
        Ok(Self::apply_borrowed_with_state(datastore, bucket, key, op)
            .await?
            .0)
    }

    /// Like [`CrdtStore::apply_borrowed`] but also returns the
    /// serialized post-apply CRDT state.
    ///
    /// The client-facing handler ships this state (not the delta op)
    /// to replicas so replica apply is an idempotent state merge: a
    /// re-delivered or reordered update cannot double-count, because
    /// merging a state twice is a no-op (element-wise max).
    ///
    /// # Errors
    /// As [`CrdtStore::apply`].
    pub async fn apply_borrowed_with_state(
        datastore: &dyn Datastore,
        bucket: &[u8],
        key: &[u8],
        op: &CrdtOp,
    ) -> Result<(CrdtValue, Vec<u8>), CrdtStoreError> {
        let current = datastore.riak_get(bucket, key).await?;
        Self::apply_to(datastore, bucket, key, op, current).await
    }

    async fn apply_to(
        datastore: &dyn Datastore,
        bucket: &[u8],
        key: &[u8],
        op: &CrdtOp,
        current: Option<Vec<u8>>,
    ) -> Result<(CrdtValue, Vec<u8>), CrdtStoreError> {
        match op {
            CrdtOp::Counter { actor, delta } => {
                let mut c = match &current {
                    Some(bytes) => counter_from_bytes(bytes)?,
                    None => PnCounter::new(),
                };
                c.apply(actor, *delta);
                let bytes = counter_to_bytes(&c);
                datastore.riak_put(bucket, key, &bytes, &[]).await?;
                Ok((CrdtValue::Counter(c.value()), bytes))
            }
            CrdtOp::Set {
                actor,
                adds,
                removes,
            } => {
                let mut s = match &current {
                    Some(bytes) => set_from_bytes(bytes)?,
                    None => OrSet::new(),
                };
                for e in adds {
                    s.add(actor, e.clone());
                }
                for e in removes {
                    s.remove(e);
                }
                let bytes = set_to_bytes(&s);
                datastore.riak_put(bucket, key, &bytes, &[]).await?;
                Ok((CrdtValue::Set(s.value().into_iter().collect()), bytes))
            }
        }
    }

    /// Fetch a projected CRDT value against a borrowed datastore.
    ///
    /// # Errors
    /// As [`CrdtStore::fetch`].
    pub async fn fetch_borrowed(
        datastore: &dyn Datastore,
        bucket: &[u8],
        key: &[u8],
        expected_tag: u8,
    ) -> Result<CrdtValue, CrdtStoreError> {
        let current = datastore.riak_get(bucket, key).await?;
        let Some(bytes) = current else {
            return Ok(CrdtValue::Missing);
        };
        match expected_tag {
            TAG_COUNTER => Ok(CrdtValue::Counter(counter_from_bytes(&bytes)?.value())),
            TAG_SET => Ok(CrdtValue::Set(
                set_from_bytes(&bytes)?.value().into_iter().collect(),
            )),
            other => Err(crate::datatypes::CrdtSerialError::UnknownTag(other).into()),
        }
    }

    fn key_lock(&self, bucket: &[u8], key: &[u8]) -> Arc<Mutex<()>> {
        let mut lk = Vec::with_capacity(bucket.len() + key.len() + 1);
        lk.extend_from_slice(bucket);
        lk.push(0);
        lk.extend_from_slice(key);
        let mut table = self.locks.lock();
        Arc::clone(table.entry(lk).or_insert_with(|| Arc::new(Mutex::new(()))))
    }

    /// Apply `op` to `(bucket, key)`, returning the post-merge value.
    ///
    /// Reads the current stored state (an empty CRDT when the key is
    /// absent), merges the op as a contribution from its actor, and
    /// writes the merged state back. Serialised per key so concurrent
    /// local applies do not lose an update.
    ///
    /// # Errors
    /// [`CrdtStoreError::Datastore`] on a store failure,
    /// [`CrdtStoreError::Serial`] when the stored blob is corrupt or
    /// its type does not match `op`.
    pub async fn apply(
        &self,
        bucket: &[u8],
        key: &[u8],
        op: &CrdtOp,
    ) -> Result<CrdtValue, CrdtStoreError> {
        let lock = self.key_lock(bucket, key);
        let _guard = lock.lock().await;
        let current = self.datastore.riak_get(bucket, key).await?;
        match op {
            CrdtOp::Counter { actor, delta } => {
                let mut c = match &current {
                    Some(bytes) => counter_from_bytes(bytes)?,
                    None => PnCounter::new(),
                };
                c.apply(actor, *delta);
                let bytes = counter_to_bytes(&c);
                self.datastore.riak_put(bucket, key, &bytes, &[]).await?;
                Ok(CrdtValue::Counter(c.value()))
            }
            CrdtOp::Set {
                actor,
                adds,
                removes,
            } => {
                let mut s = match &current {
                    Some(bytes) => set_from_bytes(bytes)?,
                    None => OrSet::new(),
                };
                for e in adds {
                    s.add(actor, e.clone());
                }
                for e in removes {
                    s.remove(e);
                }
                let bytes = set_to_bytes(&s);
                self.datastore.riak_put(bucket, key, &bytes, &[]).await?;
                Ok(CrdtValue::Set(s.value().into_iter().collect()))
            }
        }
    }

    /// Merge a full serialized CRDT state (from a peer replica) into the
    /// local stored state. Used by anti-entropy / state-shipping paths
    /// that carry whole values rather than ops. The blob's type tag
    /// selects the merge.
    ///
    /// # Errors
    /// As [`CrdtStore::apply`].
    pub async fn merge_state(
        &self,
        bucket: &[u8],
        key: &[u8],
        state: &[u8],
    ) -> Result<(), CrdtStoreError> {
        Self::merge_state_borrowed(self.datastore.as_ref(), bucket, key, state).await
    }

    /// Merge a serialized CRDT state into `(bucket, key)` against a
    /// borrowed datastore (no per-key lock). The datastore's own
    /// per-key write atomicity orders concurrent local merges; a rare
    /// read-then-write interleaving self-corrects because merge is
    /// idempotent and commutative.
    ///
    /// # Errors
    /// As [`CrdtStore::merge_state`].
    pub async fn merge_state_borrowed(
        datastore: &dyn Datastore,
        bucket: &[u8],
        key: &[u8],
        state: &[u8],
    ) -> Result<(), CrdtStoreError> {
        let tag = peek_tag(state)?;
        let current = datastore.riak_get(bucket, key).await?;
        let merged = match tag {
            TAG_COUNTER => {
                let mut c = match &current {
                    Some(b) => counter_from_bytes(b)?,
                    None => PnCounter::new(),
                };
                c.merge(&counter_from_bytes(state)?);
                counter_to_bytes(&c)
            }
            TAG_SET => {
                let mut s = match &current {
                    Some(b) => set_from_bytes(b)?,
                    None => OrSet::new(),
                };
                s.merge(&set_from_bytes(state)?);
                set_to_bytes(&s)
            }
            other => return Err(crate::datatypes::CrdtSerialError::UnknownTag(other).into()),
        };
        datastore.riak_put(bucket, key, &merged, &[]).await?;
        Ok(())
    }

    /// Fetch the projected value of a CRDT-typed key.
    ///
    /// `expected_tag` selects the projection; a stored blob whose tag
    /// disagrees is a [`CrdtStoreError::Serial`] type-mismatch.
    ///
    /// # Errors
    /// As [`CrdtStore::apply`]. A missing key yields
    /// [`CrdtValue::Missing`], not an error.
    pub async fn fetch(
        &self,
        bucket: &[u8],
        key: &[u8],
        expected_tag: u8,
    ) -> Result<CrdtValue, CrdtStoreError> {
        let current = self.datastore.riak_get(bucket, key).await?;
        let Some(bytes) = current else {
            return Ok(CrdtValue::Missing);
        };
        match expected_tag {
            TAG_COUNTER => Ok(CrdtValue::Counter(counter_from_bytes(&bytes)?.value())),
            TAG_SET => Ok(CrdtValue::Set(
                set_from_bytes(&bytes)?.value().into_iter().collect(),
            )),
            other => Err(crate::datatypes::CrdtSerialError::UnknownTag(other).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynomite::embed::hooks::{BoxFuture, Protocol};
    use dynomite::msg::Msg;

    /// In-memory datastore double for the store tests.
    #[derive(Default)]
    struct MemStore {
        map: SyncMutex<HashMap<Vec<u8>, Vec<u8>>>,
    }

    fn ck(bucket: &[u8], key: &[u8]) -> Vec<u8> {
        let mut k = bucket.to_vec();
        k.push(b'/');
        k.extend_from_slice(key);
        k
    }

    impl Datastore for MemStore {
        fn protocol(&self) -> Protocol {
            Protocol::Custom
        }
        fn dispatch(&self, _req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
            Box::pin(async { Err(DatastoreError::Unsupported(dynomite::msg::MsgType::Unknown)) })
        }
        fn riak_get<'a>(
            &'a self,
            bucket: &'a [u8],
            key: &'a [u8],
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, DatastoreError>> {
            let v = self.map.lock().get(&ck(bucket, key)).cloned();
            Box::pin(async move { Ok(v) })
        }
        fn riak_put<'a>(
            &'a self,
            bucket: &'a [u8],
            key: &'a [u8],
            value: &'a [u8],
            _indexes: &'a [(Vec<u8>, Vec<u8>)],
        ) -> BoxFuture<'a, Result<(), DatastoreError>> {
            self.map.lock().insert(ck(bucket, key), value.to_vec());
            Box::pin(async { Ok(()) })
        }
    }

    fn store() -> CrdtStore {
        CrdtStore::new(Arc::new(MemStore::default()))
    }

    fn counter_op(peer: &str, delta: i64) -> CrdtOp {
        CrdtOp::Counter {
            actor: ActorId::new("dc1", peer),
            delta,
        }
    }

    #[tokio::test]
    async fn counter_apply_accumulates_per_actor() {
        let s = store();
        // Two distinct actors each +1 => value 2 (this is the exact
        // shape of two partitioned replicas each taking one increment).
        s.apply(b"chaos", b"k", &counter_op("a", 1)).await.unwrap();
        let v = s.apply(b"chaos", b"k", &counter_op("b", 1)).await.unwrap();
        assert_eq!(v, CrdtValue::Counter(2));
    }

    #[tokio::test]
    async fn counter_same_actor_increments_sum() {
        let s = store();
        s.apply(b"c", b"k", &counter_op("a", 1)).await.unwrap();
        s.apply(b"c", b"k", &counter_op("a", 1)).await.unwrap();
        let v = s.apply(b"c", b"k", &counter_op("a", 3)).await.unwrap();
        assert_eq!(v, CrdtValue::Counter(5));
    }

    #[tokio::test]
    async fn merge_state_is_idempotent_and_sums() {
        // Model a replica shipping its whole counter state twice.
        let s = store();
        s.apply(b"c", b"k", &counter_op("a", 1)).await.unwrap();
        let mut remote = PnCounter::new();
        remote.increment(&ActorId::new("dc1", "b"), 1);
        let blob = counter_to_bytes(&remote);
        s.merge_state(b"c", b"k", &blob).await.unwrap();
        s.merge_state(b"c", b"k", &blob).await.unwrap(); // duplicate
        let v = s.fetch(b"c", b"k", TAG_COUNTER).await.unwrap();
        assert_eq!(v, CrdtValue::Counter(2));
    }

    #[tokio::test]
    async fn set_add_then_fetch_union() {
        let s = store();
        let op = |peer: &str, e: &[u8]| CrdtOp::Set {
            actor: ActorId::new("dc1", peer),
            adds: vec![e.to_vec()],
            removes: vec![],
        };
        s.apply(b"c", b"k", &op("a", b"x")).await.unwrap();
        s.apply(b"c", b"k", &op("b", b"y")).await.unwrap();
        let v = s.fetch(b"c", b"k", TAG_SET).await.unwrap();
        match v {
            CrdtValue::Set(mut elems) => {
                elems.sort();
                assert_eq!(elems, vec![b"x".to_vec(), b"y".to_vec()]);
            }
            other => panic!("expected set, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_missing_key_is_missing() {
        let s = store();
        assert_eq!(
            s.fetch(b"c", b"nope", TAG_COUNTER).await.unwrap(),
            CrdtValue::Missing
        );
    }
}
