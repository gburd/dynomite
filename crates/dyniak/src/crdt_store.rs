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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use parking_lot::Mutex as SyncMutex;
use tokio::sync::Mutex;

use crate::datatypes::{
    counter_from_bytes, counter_to_bytes, flag_from_bytes, flag_to_bytes, hll_from_bytes,
    hll_to_bytes, map_from_bytes, map_to_bytes, peek_tag, register_from_bytes, register_to_bytes,
    set_from_bytes, set_to_bytes, ActorId, Crdt, EwFlag, FieldKey, FieldType, FieldValue,
    HyperLogLog, LwwRegister, Map, MapOp, NestedOp, OrSet, PnCounter, TAG_COUNTER, TAG_FLAG,
    TAG_HLL, TAG_MAP, TAG_REGISTER, TAG_SET,
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
    /// LWW-register assignment.
    Register {
        /// Actor that produced the assignment.
        actor: ActorId,
        /// New register value.
        value: Vec<u8>,
    },
    /// EW-flag toggle.
    Flag {
        /// Actor that produced the toggle.
        actor: ActorId,
        /// `true` enables the flag, `false` disables it.
        enable: bool,
    },
    /// Observed-remove map update or remove batch, recursively
    /// covering nested maps.
    Map {
        /// Actor that produced the batch. [`Map::apply`] mints
        /// every fresh OR-Set tag (including tags for nested maps)
        /// from this single actor.
        actor: ActorId,
        /// The field-level operations to apply, in order.
        ops: Vec<MapOp>,
    },
    /// HyperLogLog item batch.
    Hll {
        /// Actor that produced the batch. Carried for wire-format
        /// symmetry with the other variants; [`HyperLogLog::add`]
        /// does not attribute items to an actor.
        actor: ActorId,
        /// Items to fold into the register array.
        items: Vec<Vec<u8>>,
    },
}

impl CrdtOp {
    /// The stored type tag this op applies to.
    #[must_use]
    pub fn type_tag(&self) -> u8 {
        match self {
            CrdtOp::Counter { .. } => TAG_COUNTER,
            CrdtOp::Set { .. } => TAG_SET,
            CrdtOp::Register { .. } => TAG_REGISTER,
            CrdtOp::Flag { .. } => TAG_FLAG,
            CrdtOp::Map { .. } => TAG_MAP,
            CrdtOp::Hll { .. } => TAG_HLL,
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
            CrdtOp::Register { actor, value } => {
                let mut r = LwwRegister::new();
                r.assign_now(actor, value.clone());
                register_to_bytes(&r)
            }
            CrdtOp::Flag { actor, enable } => {
                let mut f = EwFlag::new();
                if *enable {
                    f.enable(actor);
                } else {
                    f.disable();
                }
                flag_to_bytes(&f)
            }
            CrdtOp::Map { actor, ops } => {
                let mut m = Map::new();
                for op in ops {
                    m.apply(actor, op);
                }
                map_to_bytes(&m)
            }
            CrdtOp::Hll { items, .. } => {
                let mut h = HyperLogLog::new();
                for item in items {
                    h.add(item);
                }
                hll_to_bytes(&h)
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
            CrdtOp::Register { actor, value } => {
                put_lp(&mut out, actor.dc.as_bytes());
                put_lp(&mut out, actor.peer.as_bytes());
                put_lp(&mut out, value);
            }
            CrdtOp::Flag { actor, enable } => {
                put_lp(&mut out, actor.dc.as_bytes());
                put_lp(&mut out, actor.peer.as_bytes());
                out.push(u8::from(*enable));
            }
            CrdtOp::Map { actor, ops } => {
                put_lp(&mut out, actor.dc.as_bytes());
                put_lp(&mut out, actor.peer.as_bytes());
                out.extend_from_slice(&(ops.len() as u64).to_be_bytes());
                for op in ops {
                    put_map_op(&mut out, op);
                }
            }
            CrdtOp::Hll { actor, items } => {
                put_lp(&mut out, actor.dc.as_bytes());
                put_lp(&mut out, actor.peer.as_bytes());
                out.extend_from_slice(&(items.len() as u64).to_be_bytes());
                for item in items {
                    put_lp(&mut out, item);
                }
            }
        }
        out
    }

    /// Parse an op from [`CrdtOp::to_bytes`].
    ///
    /// # Errors
    /// [`crate::datatypes::CrdtSerialError::Truncated`] on a short
    /// buffer,
    /// [`crate::datatypes::CrdtSerialError::UnknownTag`] on an unknown
    /// type tag.
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
            TAG_REGISTER => {
                let value = r.bytes()?;
                Ok(CrdtOp::Register { actor, value })
            }
            TAG_FLAG => {
                let enable = r.u8()? != 0;
                Ok(CrdtOp::Flag { actor, enable })
            }
            TAG_MAP => {
                let no = usize::try_from(r.u64()?)
                    .map_err(|_| crate::datatypes::CrdtSerialError::Truncated)?;
                let mut ops = Vec::with_capacity(no);
                for _ in 0..no {
                    ops.push(read_map_op(&mut r)?);
                }
                Ok(CrdtOp::Map { actor, ops })
            }
            TAG_HLL => {
                let ni = usize::try_from(r.u64()?)
                    .map_err(|_| crate::datatypes::CrdtSerialError::Truncated)?;
                let mut items = Vec::with_capacity(ni);
                for _ in 0..ni {
                    items.push(r.bytes()?);
                }
                Ok(CrdtOp::Hll { actor, items })
            }
            other => Err(CrdtSerialError::UnknownTag(other)),
        }
    }
}

/// Field-type wire byte for a [`FieldKey`] on the `CrdtOp` wire.
/// Matches [`FieldType::to_wire`] narrowed to a `u8`; every
/// discriminant fits in `1..=5`.
fn put_field_type(out: &mut Vec<u8>, t: FieldType) {
    out.push(u8::try_from(t.to_wire()).unwrap_or(0));
}

fn read_field_type(r: &mut OpReader<'_>) -> Result<FieldType, crate::datatypes::CrdtSerialError> {
    let code = i32::from(r.u8()?);
    FieldType::from_wire(code).ok_or(crate::datatypes::CrdtSerialError::UnknownTag(
        u8::try_from(code).unwrap_or(0),
    ))
}

fn put_field_key(out: &mut Vec<u8>, k: &FieldKey) {
    put_lp(out, &k.name);
    put_field_type(out, k.field_type);
}

fn read_field_key(r: &mut OpReader<'_>) -> Result<FieldKey, crate::datatypes::CrdtSerialError> {
    let name = r.bytes()?;
    let field_type = read_field_type(r)?;
    Ok(FieldKey::new(name, field_type))
}

/// Nested-op discriminants on the `CrdtOp` wire. Distinct from the
/// stored-state [`crate::datatypes::TAG_MAP`] family since a
/// [`NestedOp`] is an operation, not a value.
const NESTED_OP_COUNTER: u8 = 1;
const NESTED_OP_SET_ADD: u8 = 2;
const NESTED_OP_SET_REMOVE: u8 = 3;
const NESTED_OP_REGISTER_ASSIGN: u8 = 4;
const NESTED_OP_FLAG: u8 = 5;
const NESTED_OP_MAP: u8 = 6;

/// Top-level `MapOp` discriminants on the `CrdtOp` wire.
const MAP_OP_UPDATE: u8 = 1;
const MAP_OP_REMOVE: u8 = 2;

fn put_map_op(out: &mut Vec<u8>, op: &MapOp) {
    match op {
        MapOp::Update { field, op } => {
            out.push(MAP_OP_UPDATE);
            put_field_key(out, field);
            put_nested_op(out, op);
        }
        MapOp::Remove { field } => {
            out.push(MAP_OP_REMOVE);
            put_field_key(out, field);
        }
    }
}

fn read_map_op(r: &mut OpReader<'_>) -> Result<MapOp, crate::datatypes::CrdtSerialError> {
    use crate::datatypes::CrdtSerialError;
    match r.u8()? {
        MAP_OP_UPDATE => {
            let field = read_field_key(r)?;
            let op = read_nested_op(r)?;
            Ok(MapOp::Update { field, op })
        }
        MAP_OP_REMOVE => {
            let field = read_field_key(r)?;
            Ok(MapOp::Remove { field })
        }
        other => Err(CrdtSerialError::UnknownTag(other)),
    }
}

fn put_nested_op(out: &mut Vec<u8>, op: &NestedOp) {
    match op {
        NestedOp::Counter(delta) => {
            out.push(NESTED_OP_COUNTER);
            out.extend_from_slice(&delta.to_be_bytes());
        }
        NestedOp::SetAdd(elt) => {
            out.push(NESTED_OP_SET_ADD);
            put_lp(out, elt);
        }
        NestedOp::SetRemove(elt) => {
            out.push(NESTED_OP_SET_REMOVE);
            put_lp(out, elt);
        }
        NestedOp::RegisterAssign { value, ts_micros } => {
            out.push(NESTED_OP_REGISTER_ASSIGN);
            put_lp(out, value);
            out.extend_from_slice(&ts_micros.to_be_bytes());
        }
        NestedOp::Flag(enable) => {
            out.push(NESTED_OP_FLAG);
            out.push(u8::from(*enable));
        }
        NestedOp::Map(inner) => {
            out.push(NESTED_OP_MAP);
            put_map_op(out, inner);
        }
    }
}

fn read_nested_op(r: &mut OpReader<'_>) -> Result<NestedOp, crate::datatypes::CrdtSerialError> {
    use crate::datatypes::CrdtSerialError;
    match r.u8()? {
        NESTED_OP_COUNTER => Ok(NestedOp::Counter(r.i64()?)),
        NESTED_OP_SET_ADD => Ok(NestedOp::SetAdd(r.bytes()?)),
        NESTED_OP_SET_REMOVE => Ok(NestedOp::SetRemove(r.bytes()?)),
        NESTED_OP_REGISTER_ASSIGN => {
            let value = r.bytes()?;
            let ts_micros = r.u64()?;
            Ok(NestedOp::RegisterAssign { value, ts_micros })
        }
        NESTED_OP_FLAG => Ok(NestedOp::Flag(r.u8()? != 0)),
        NESTED_OP_MAP => Ok(NestedOp::Map(Box::new(read_map_op(r)?))),
        other => Err(CrdtSerialError::UnknownTag(other)),
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
    /// Register value.
    Register(Vec<u8>),
    /// Flag value.
    Flag(bool),
    /// Map field projection: present fields and their CRDT values.
    Map(BTreeMap<FieldKey, FieldValue>),
    /// HyperLogLog cardinality estimate.
    Hll(u64),
    /// The key does not exist yet.
    Missing,
}

/// Merge two serialized CRDT states of the same type into one
/// serialized state (element-wise max). Used by read coordination to
/// combine replica states without touching a datastore.
///
/// # Errors
/// [`crate::datatypes::CrdtSerialError`] if either blob is corrupt or
/// the two blobs are different CRDT types.
pub fn merge_two_states(a: &[u8], b: &[u8]) -> Result<Vec<u8>, crate::datatypes::CrdtSerialError> {
    if a.is_empty() {
        return Ok(b.to_vec());
    }
    if b.is_empty() {
        return Ok(a.to_vec());
    }
    let tag = peek_tag(a)?;
    match tag {
        TAG_COUNTER => {
            let mut c = counter_from_bytes(a)?;
            c.merge(&counter_from_bytes(b)?);
            Ok(counter_to_bytes(&c))
        }
        TAG_SET => {
            let mut s = set_from_bytes(a)?;
            s.merge(&set_from_bytes(b)?);
            Ok(set_to_bytes(&s))
        }
        TAG_REGISTER => {
            let mut r = register_from_bytes(a)?;
            r.merge(&register_from_bytes(b)?);
            Ok(register_to_bytes(&r))
        }
        TAG_FLAG => {
            let mut f = flag_from_bytes(a)?;
            f.merge(&flag_from_bytes(b)?);
            Ok(flag_to_bytes(&f))
        }
        TAG_MAP => {
            let mut m = map_from_bytes(a)?;
            m.merge(&map_from_bytes(b)?);
            Ok(map_to_bytes(&m))
        }
        TAG_HLL => {
            let mut h = hll_from_bytes(a)?;
            h.merge(&hll_from_bytes(b)?);
            Ok(hll_to_bytes(&h))
        }
        other => Err(crate::datatypes::CrdtSerialError::UnknownTag(other)),
    }
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
        TAG_REGISTER => Ok(CrdtValue::Register(register_from_bytes(state)?.value())),
        TAG_FLAG => Ok(CrdtValue::Flag(flag_from_bytes(state)?.value())),
        TAG_MAP => Ok(CrdtValue::Map(map_from_bytes(state)?.value())),
        TAG_HLL => Ok(CrdtValue::Hll(hll_from_bytes(state)?.value())),
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
            CrdtOp::Register { actor, value } => {
                let mut r = match &current {
                    Some(bytes) => register_from_bytes(bytes)?,
                    None => LwwRegister::new(),
                };
                r.assign_now(actor, value.clone());
                let bytes = register_to_bytes(&r);
                datastore.riak_put(bucket, key, &bytes, &[]).await?;
                Ok((CrdtValue::Register(r.value()), bytes))
            }
            CrdtOp::Flag { actor, enable } => {
                let mut f = match &current {
                    Some(bytes) => flag_from_bytes(bytes)?,
                    None => EwFlag::new(),
                };
                if *enable {
                    f.enable(actor);
                } else {
                    f.disable();
                }
                let bytes = flag_to_bytes(&f);
                datastore.riak_put(bucket, key, &bytes, &[]).await?;
                Ok((CrdtValue::Flag(f.value()), bytes))
            }
            CrdtOp::Map { actor, ops } => {
                let mut m = match &current {
                    Some(bytes) => map_from_bytes(bytes)?,
                    None => Map::new(),
                };
                for op in ops {
                    m.apply(actor, op);
                }
                let bytes = map_to_bytes(&m);
                datastore.riak_put(bucket, key, &bytes, &[]).await?;
                Ok((CrdtValue::Map(m.value()), bytes))
            }
            CrdtOp::Hll { items, .. } => {
                let mut h = match &current {
                    Some(bytes) => hll_from_bytes(bytes)?,
                    None => HyperLogLog::new(),
                };
                for item in items {
                    h.add(item);
                }
                let bytes = hll_to_bytes(&h);
                datastore.riak_put(bucket, key, &bytes, &[]).await?;
                Ok((CrdtValue::Hll(h.value()), bytes))
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
            TAG_REGISTER => Ok(CrdtValue::Register(register_from_bytes(&bytes)?.value())),
            TAG_FLAG => Ok(CrdtValue::Flag(flag_from_bytes(&bytes)?.value())),
            TAG_MAP => Ok(CrdtValue::Map(map_from_bytes(&bytes)?.value())),
            TAG_HLL => Ok(CrdtValue::Hll(hll_from_bytes(&bytes)?.value())),
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
            CrdtOp::Register { actor, value } => {
                let mut r = match &current {
                    Some(bytes) => register_from_bytes(bytes)?,
                    None => LwwRegister::new(),
                };
                r.assign_now(actor, value.clone());
                let bytes = register_to_bytes(&r);
                self.datastore.riak_put(bucket, key, &bytes, &[]).await?;
                Ok(CrdtValue::Register(r.value()))
            }
            CrdtOp::Flag { actor, enable } => {
                let mut f = match &current {
                    Some(bytes) => flag_from_bytes(bytes)?,
                    None => EwFlag::new(),
                };
                if *enable {
                    f.enable(actor);
                } else {
                    f.disable();
                }
                let bytes = flag_to_bytes(&f);
                self.datastore.riak_put(bucket, key, &bytes, &[]).await?;
                Ok(CrdtValue::Flag(f.value()))
            }
            CrdtOp::Map { actor, ops } => {
                let mut m = match &current {
                    Some(bytes) => map_from_bytes(bytes)?,
                    None => Map::new(),
                };
                for op in ops {
                    m.apply(actor, op);
                }
                let bytes = map_to_bytes(&m);
                self.datastore.riak_put(bucket, key, &bytes, &[]).await?;
                Ok(CrdtValue::Map(m.value()))
            }
            CrdtOp::Hll { items, .. } => {
                let mut h = match &current {
                    Some(bytes) => hll_from_bytes(bytes)?,
                    None => HyperLogLog::new(),
                };
                for item in items {
                    h.add(item);
                }
                let bytes = hll_to_bytes(&h);
                self.datastore.riak_put(bucket, key, &bytes, &[]).await?;
                Ok(CrdtValue::Hll(h.value()))
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
            TAG_REGISTER => {
                let mut r = match &current {
                    Some(b) => register_from_bytes(b)?,
                    None => LwwRegister::new(),
                };
                r.merge(&register_from_bytes(state)?);
                register_to_bytes(&r)
            }
            TAG_FLAG => {
                let mut f = match &current {
                    Some(b) => flag_from_bytes(b)?,
                    None => EwFlag::new(),
                };
                f.merge(&flag_from_bytes(state)?);
                flag_to_bytes(&f)
            }
            TAG_MAP => {
                let mut m = match &current {
                    Some(b) => map_from_bytes(b)?,
                    None => Map::new(),
                };
                m.merge(&map_from_bytes(state)?);
                map_to_bytes(&m)
            }
            TAG_HLL => {
                let mut h = match &current {
                    Some(b) => hll_from_bytes(b)?,
                    None => HyperLogLog::new(),
                };
                h.merge(&hll_from_bytes(state)?);
                hll_to_bytes(&h)
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
            TAG_REGISTER => Ok(CrdtValue::Register(register_from_bytes(&bytes)?.value())),
            TAG_FLAG => Ok(CrdtValue::Flag(flag_from_bytes(&bytes)?.value())),
            TAG_MAP => Ok(CrdtValue::Map(map_from_bytes(&bytes)?.value())),
            TAG_HLL => Ok(CrdtValue::Hll(hll_from_bytes(&bytes)?.value())),
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

    #[tokio::test]
    async fn register_assign_then_fetch_returns_value() {
        let s = store();
        let op = CrdtOp::Register {
            actor: ActorId::new("dc1", "a"),
            value: b"hello".to_vec(),
        };
        let v = s.apply(b"c", b"k", &op).await.unwrap();
        assert_eq!(v, CrdtValue::Register(b"hello".to_vec()));
        let fetched = s.fetch(b"c", b"k", TAG_REGISTER).await.unwrap();
        assert_eq!(fetched, CrdtValue::Register(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn register_reassign_overwrites_via_lww() {
        let s = store();
        let assign = |peer: &str, v: &[u8]| CrdtOp::Register {
            actor: ActorId::new("dc1", peer),
            value: v.to_vec(),
        };
        // assign_now stamps with the wall clock; use a lexically
        // greater second actor so the assertion holds even if both
        // calls land in the same microsecond (the LWW tie-break is
        // by actor id).
        s.apply(b"c", b"k", &assign("a", b"first")).await.unwrap();
        let v = s.apply(b"c", b"k", &assign("z", b"second")).await.unwrap();
        assert_eq!(v, CrdtValue::Register(b"second".to_vec()));
    }

    #[tokio::test]
    async fn flag_enable_then_fetch_returns_true() {
        let s = store();
        let op = CrdtOp::Flag {
            actor: ActorId::new("dc1", "a"),
            enable: true,
        };
        let v = s.apply(b"c", b"k", &op).await.unwrap();
        assert_eq!(v, CrdtValue::Flag(true));
        let fetched = s.fetch(b"c", b"k", TAG_FLAG).await.unwrap();
        assert_eq!(fetched, CrdtValue::Flag(true));
    }

    #[tokio::test]
    async fn flag_disable_after_enable_reads_false() {
        let s = store();
        let actor = ActorId::new("dc1", "a");
        s.apply(
            b"c",
            b"k",
            &CrdtOp::Flag {
                actor: actor.clone(),
                enable: true,
            },
        )
        .await
        .unwrap();
        let v = s
            .apply(
                b"c",
                b"k",
                &CrdtOp::Flag {
                    actor,
                    enable: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(v, CrdtValue::Flag(false));
    }

    #[test]
    fn merge_two_states_picks_lww_register_winner() {
        let mut early = LwwRegister::new();
        early.assign(&ActorId::new("dc1", "a"), 1, b"early".to_vec());
        let mut late = LwwRegister::new();
        late.assign(&ActorId::new("dc1", "b"), 2, b"late".to_vec());
        let merged =
            merge_two_states(&register_to_bytes(&early), &register_to_bytes(&late)).unwrap();
        assert_eq!(
            project_state(&merged, TAG_REGISTER).unwrap(),
            CrdtValue::Register(b"late".to_vec())
        );
    }

    #[test]
    fn merge_two_states_flag_is_enable_wins() {
        let a = ActorId::new("dc1", "a");
        let b = ActorId::new("dc1", "b");
        let mut shared = EwFlag::new();
        shared.enable(&a);
        let mut disabled = shared.clone();
        disabled.disable();
        let mut concurrent_enable = shared.clone();
        concurrent_enable.enable(&b);
        let merged = merge_two_states(
            &flag_to_bytes(&disabled),
            &flag_to_bytes(&concurrent_enable),
        )
        .unwrap();
        assert_eq!(
            project_state(&merged, TAG_FLAG).unwrap(),
            CrdtValue::Flag(true)
        );
    }

    #[test]
    fn project_state_covers_register_and_flag() {
        let mut r = LwwRegister::new();
        r.assign(&ActorId::new("dc1", "a"), 1, b"v".to_vec());
        assert_eq!(
            project_state(&register_to_bytes(&r), TAG_REGISTER).unwrap(),
            CrdtValue::Register(b"v".to_vec())
        );

        let mut f = EwFlag::new();
        f.enable(&ActorId::new("dc1", "a"));
        assert_eq!(
            project_state(&flag_to_bytes(&f), TAG_FLAG).unwrap(),
            CrdtValue::Flag(true)
        );
    }

    fn counter_field(name: &str) -> FieldKey {
        FieldKey::new(name.as_bytes(), FieldType::Counter)
    }

    fn register_field(name: &str) -> FieldKey {
        FieldKey::new(name.as_bytes(), FieldType::LwwRegister)
    }

    #[tokio::test]
    async fn map_update_adds_counter_and_register_fields_then_fetch() {
        let s = store();
        let actor = ActorId::new("dc1", "a");
        let ops = vec![
            MapOp::Update {
                field: counter_field("hits"),
                op: NestedOp::Counter(4),
            },
            MapOp::Update {
                field: register_field("name"),
                op: NestedOp::RegisterAssign {
                    value: b"alice".to_vec(),
                    ts_micros: 9,
                },
            },
        ];
        let applied = s
            .apply(
                b"c",
                b"k",
                &CrdtOp::Map {
                    actor: actor.clone(),
                    ops,
                },
            )
            .await
            .unwrap();
        let CrdtValue::Map(ref fields) = applied else {
            panic!("expected map value");
        };
        match fields.get(&counter_field("hits")) {
            Some(FieldValue::Counter(c)) => assert_eq!(c.value(), 4),
            other => panic!("expected counter field, got {other:?}"),
        }
        match fields.get(&register_field("name")) {
            Some(FieldValue::LwwRegister(r)) => assert_eq!(r.value(), b"alice".to_vec()),
            other => panic!("expected register field, got {other:?}"),
        }

        let fetched = s.fetch(b"c", b"k", TAG_MAP).await.unwrap();
        assert_eq!(fetched, applied);
    }

    #[tokio::test]
    async fn hll_add_then_fetch_returns_cardinality() {
        let s = store();
        let actor = ActorId::new("dc1", "a");
        let items: Vec<Vec<u8>> = (0u32..500).map(|i| i.to_be_bytes().to_vec()).collect();
        let applied = s
            .apply(b"c", b"k", &CrdtOp::Hll { actor, items })
            .await
            .unwrap();
        let CrdtValue::Hll(n) = applied else {
            panic!("expected hll value");
        };
        assert!((450..=550).contains(&n), "cardinality {n} not near 500");

        let fetched = s.fetch(b"c", b"k", TAG_HLL).await.unwrap();
        assert_eq!(fetched, CrdtValue::Hll(n));
    }

    #[test]
    fn merge_two_states_map_recursively_merges_fields() {
        let a = ActorId::new("dc1", "a");
        let b = ActorId::new("dc1", "b");
        let mut left = Map::new();
        left.apply(
            &a,
            &MapOp::Update {
                field: counter_field("c"),
                op: NestedOp::Counter(3),
            },
        );
        let mut right = Map::new();
        right.apply(
            &b,
            &MapOp::Update {
                field: counter_field("c"),
                op: NestedOp::Counter(5),
            },
        );
        let merged = merge_two_states(&map_to_bytes(&left), &map_to_bytes(&right)).unwrap();
        let CrdtValue::Map(fields) = project_state(&merged, TAG_MAP).unwrap() else {
            panic!("expected map value");
        };
        match fields.get(&counter_field("c")) {
            Some(FieldValue::Counter(c)) => assert_eq!(c.value(), 8),
            other => panic!("expected counter field, got {other:?}"),
        }
    }

    #[test]
    fn merge_two_states_hll_is_elementwise_max() {
        let mut a = HyperLogLog::new();
        for i in 0u32..200 {
            a.add(i.to_be_bytes());
        }
        let mut b = HyperLogLog::new();
        for i in 100u32..300 {
            b.add(i.to_be_bytes());
        }
        let merged = merge_two_states(&hll_to_bytes(&a), &hll_to_bytes(&b)).unwrap();
        let CrdtValue::Hll(n) = project_state(&merged, TAG_HLL).unwrap() else {
            panic!("expected hll value");
        };
        assert!(
            (250..=350).contains(&n),
            "merged cardinality {n} not near 300"
        );
    }

    #[test]
    fn project_state_covers_map_and_hll() {
        let mut m = Map::new();
        m.apply(
            &ActorId::new("dc1", "a"),
            &MapOp::Update {
                field: counter_field("c"),
                op: NestedOp::Counter(2),
            },
        );
        let CrdtValue::Map(fields) = project_state(&map_to_bytes(&m), TAG_MAP).unwrap() else {
            panic!("expected map value");
        };
        assert_eq!(fields.len(), 1);

        let mut h = HyperLogLog::new();
        h.add(b"x");
        assert_eq!(
            project_state(&hll_to_bytes(&h), TAG_HLL).unwrap(),
            CrdtValue::Hll(h.value())
        );
    }
}
