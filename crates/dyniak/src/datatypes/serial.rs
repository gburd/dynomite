//! Binary serialization for the stored CRDT state.
//!
//! A CRDT-typed key stores its full state-based (CvRDT) value under
//! `(bucket, key)` in the datastore. This module defines the on-disk
//! wire form: a one-byte format version, a one-byte type tag, then the
//! type-specific payload. The form is explicit and length-prefixed so
//! it is stable across releases (a version bump is required to change a
//! payload layout) and so a fetch can reject a blob whose type tag does
//! not match the requested data type.
//!
//! Convergence relies only on the decoded state being merged with the
//! type's `merge`; the byte form itself carries no ordering.

use std::collections::{BTreeMap, BTreeSet};

use crate::datatypes::map::{FieldKey, FieldType, FieldValue, Map};
use crate::datatypes::set::{OrSet, Tag};
use crate::datatypes::{ActorId, Crdt, EwFlag, HyperLogLog, LwwRegister, PnCounter};

/// Current serialization format version.
const FORMAT_V1: u8 = 1;

/// Type tag: PN-counter state.
pub const TAG_COUNTER: u8 = 1;
/// Type tag: OR-set state.
pub const TAG_SET: u8 = 2;
/// Type tag: LWW-register state.
pub const TAG_REGISTER: u8 = 3;
/// Type tag: EW-flag state.
pub const TAG_FLAG: u8 = 4;
/// Type tag: observed-remove map state.
pub const TAG_MAP: u8 = 5;
/// Type tag: HyperLogLog state.
pub const TAG_HLL: u8 = 6;

/// Error decoding stored CRDT state.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum CrdtSerialError {
    /// Buffer ended before a declared field was fully read.
    #[error("crdt serial: truncated payload")]
    Truncated,
    /// The format-version byte is not one this build understands.
    #[error("crdt serial: unsupported format version {0}")]
    BadVersion(u8),
    /// The type tag did not match the requested data type.
    #[error("crdt serial: type tag {found} does not match expected {expected}")]
    TypeMismatch {
        /// Tag read from the blob.
        found: u8,
        /// Tag the caller expected.
        expected: u8,
    },
    /// The type tag is not a known CRDT type.
    #[error("crdt serial: unknown type tag {0}")]
    UnknownTag(u8),
    /// Trailing bytes remained after decoding a complete value.
    #[error("crdt serial: {0} trailing bytes")]
    Trailing(usize),
}

// ---- primitive writers / reader -------------------------------------------

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_u64(out, b.len() as u64);
    out.extend_from_slice(b);
}

fn put_actor(out: &mut Vec<u8>, a: &ActorId) {
    put_bytes(out, a.dc.as_bytes());
    put_bytes(out, a.peer.as_bytes());
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn u8(&mut self) -> Result<u8, CrdtSerialError> {
        let b = *self.buf.get(self.pos).ok_or(CrdtSerialError::Truncated)?;
        self.pos += 1;
        Ok(b)
    }

    fn u64(&mut self) -> Result<u64, CrdtSerialError> {
        let end = self.pos + 8;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(CrdtSerialError::Truncated)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(slice);
        self.pos = end;
        Ok(u64::from_be_bytes(a))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, CrdtSerialError> {
        let n = usize::try_from(self.u64()?).map_err(|_| CrdtSerialError::Truncated)?;
        let end = self.pos + n;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(CrdtSerialError::Truncated)?;
        self.pos = end;
        Ok(slice.to_vec())
    }

    fn string(&mut self) -> Result<String, CrdtSerialError> {
        String::from_utf8(self.bytes()?).map_err(|_| CrdtSerialError::Truncated)
    }

    fn actor(&mut self) -> Result<ActorId, CrdtSerialError> {
        let dc = self.string()?;
        let peer = self.string()?;
        Ok(ActorId::new(dc, peer))
    }

    fn done(&self) -> Result<(), CrdtSerialError> {
        let rem = self.buf.len() - self.pos;
        if rem == 0 {
            Ok(())
        } else {
            Err(CrdtSerialError::Trailing(rem))
        }
    }
}

// ---- PnCounter ------------------------------------------------------------

/// Serialize a PN-counter to its stored form (version + tag + payload).
#[must_use]
pub fn counter_to_bytes(c: &PnCounter) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    out.push(FORMAT_V1);
    out.push(TAG_COUNTER);
    let (pos, neg) = c.columns();
    put_u64(&mut out, pos.len() as u64);
    for (actor, n) in pos {
        put_actor(&mut out, actor);
        put_u64(&mut out, *n);
    }
    put_u64(&mut out, neg.len() as u64);
    for (actor, n) in neg {
        put_actor(&mut out, actor);
        put_u64(&mut out, *n);
    }
    out
}

/// Decode a PN-counter from its stored form.
///
/// # Errors
/// Version / tag / truncation / trailing errors per [`CrdtSerialError`].
pub fn counter_from_bytes(buf: &[u8]) -> Result<PnCounter, CrdtSerialError> {
    let mut r = Reader::new(buf);
    check_header(&mut r, TAG_COUNTER)?;
    let mut pos = BTreeMap::new();
    let np = r.u64()?;
    for _ in 0..np {
        let a = r.actor()?;
        let n = r.u64()?;
        pos.insert(a, n);
    }
    let mut neg = BTreeMap::new();
    let nn = r.u64()?;
    for _ in 0..nn {
        let a = r.actor()?;
        let n = r.u64()?;
        neg.insert(a, n);
    }
    r.done()?;
    Ok(PnCounter::from_columns(pos, neg))
}

// ---- OrSet ----------------------------------------------------------------

/// Serialize an OR-set to its stored form.
#[must_use]
pub fn set_to_bytes(s: &OrSet) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(FORMAT_V1);
    out.push(TAG_SET);
    let elements = s.raw_elements();
    put_u64(&mut out, elements.len() as u64);
    for (element, (adds, removes)) in elements {
        put_bytes(&mut out, &element);
        put_tags(&mut out, &adds);
        put_tags(&mut out, &removes);
    }
    let counters = s.raw_actor_counters();
    put_u64(&mut out, counters.len() as u64);
    for (actor, n) in counters {
        put_actor(&mut out, &actor);
        put_u64(&mut out, n);
    }
    out
}

fn put_tags(out: &mut Vec<u8>, tags: &BTreeSet<Tag>) {
    put_u64(out, tags.len() as u64);
    for t in tags {
        put_actor(out, &t.actor);
        put_u64(out, t.counter);
    }
}

fn read_tags(r: &mut Reader<'_>) -> Result<BTreeSet<Tag>, CrdtSerialError> {
    let n = r.u64()?;
    let mut set = BTreeSet::new();
    for _ in 0..n {
        let actor = r.actor()?;
        let counter = r.u64()?;
        set.insert(Tag { actor, counter });
    }
    Ok(set)
}

/// Decode an OR-set from its stored form.
///
/// # Errors
/// Version / tag / truncation / trailing errors per [`CrdtSerialError`].
pub fn set_from_bytes(buf: &[u8]) -> Result<OrSet, CrdtSerialError> {
    let mut r = Reader::new(buf);
    check_header(&mut r, TAG_SET)?;
    let ne = r.u64()?;
    let mut elements: BTreeMap<Vec<u8>, (BTreeSet<Tag>, BTreeSet<Tag>)> = BTreeMap::new();
    for _ in 0..ne {
        let element = r.bytes()?;
        let adds = read_tags(&mut r)?;
        let removes = read_tags(&mut r)?;
        elements.insert(element, (adds, removes));
    }
    let nc = r.u64()?;
    let mut counters = BTreeMap::new();
    for _ in 0..nc {
        let actor = r.actor()?;
        let n = r.u64()?;
        counters.insert(actor, n);
    }
    r.done()?;
    Ok(OrSet::from_raw(elements, counters))
}

// ---- LwwRegister -----------------------------------------------------------

/// Serialize an LWW-register to its stored form.
#[must_use]
pub fn register_to_bytes(r: &LwwRegister) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    out.push(FORMAT_V1);
    out.push(TAG_REGISTER);
    put_bytes(&mut out, &r.value());
    put_u64(&mut out, r.timestamp());
    match r.actor() {
        Some(actor) => {
            out.push(1);
            put_actor(&mut out, actor);
        }
        None => out.push(0),
    }
    out
}

/// Decode an LWW-register from its stored form.
///
/// # Errors
/// Version / tag / truncation / trailing errors per [`CrdtSerialError`].
pub fn register_from_bytes(buf: &[u8]) -> Result<LwwRegister, CrdtSerialError> {
    let mut r = Reader::new(buf);
    check_header(&mut r, TAG_REGISTER)?;
    let value = r.bytes()?;
    let ts_micros = r.u64()?;
    let has_actor = r.u8()?;
    let actor = if has_actor == 0 {
        None
    } else {
        Some(r.actor()?)
    };
    r.done()?;
    Ok(LwwRegister::from_raw(value, ts_micros, actor))
}

// ---- EwFlag -----------------------------------------------------------------

/// Serialize an EW-flag to its stored form.
#[must_use]
pub fn flag_to_bytes(f: &EwFlag) -> Vec<u8> {
    let mut out = Vec::with_capacity(48);
    out.push(FORMAT_V1);
    out.push(TAG_FLAG);
    put_tags(&mut out, &f.raw_adds());
    put_tags(&mut out, &f.raw_removes());
    let counters = f.raw_actor_counters();
    put_u64(&mut out, counters.len() as u64);
    for (actor, n) in counters {
        put_actor(&mut out, &actor);
        put_u64(&mut out, n);
    }
    out
}

/// Decode an EW-flag from its stored form.
///
/// # Errors
/// Version / tag / truncation / trailing errors per [`CrdtSerialError`].
pub fn flag_from_bytes(buf: &[u8]) -> Result<EwFlag, CrdtSerialError> {
    let mut r = Reader::new(buf);
    check_header(&mut r, TAG_FLAG)?;
    let adds = read_tags(&mut r)?;
    let removes = read_tags(&mut r)?;
    let nc = r.u64()?;
    let mut counters = BTreeMap::new();
    for _ in 0..nc {
        let actor = r.actor()?;
        let n = r.u64()?;
        counters.insert(actor, n);
    }
    r.done()?;
    Ok(EwFlag::from_raw(adds, removes, counters))
}

// ---- Map --------------------------------------------------------------------

/// Serialize an observed-remove map to its stored form.
///
/// Field values recurse through the corresponding `*_to_bytes`
/// serializer for each scalar type, length-prefixed so a decoder
/// does not need to understand a field's payload shape to skip
/// past it; a [`FieldValue::NestedMap`] recurses into
/// `map_to_bytes` again, so a doubly (or deeper) nested map
/// serializes correctly.
#[must_use]
pub fn map_to_bytes(m: &Map) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(FORMAT_V1);
    out.push(TAG_MAP);
    let fields = m.raw_fields();
    put_u64(&mut out, fields.len() as u64);
    for (key, (adds, removes, value)) in fields {
        put_bytes(&mut out, &key.name);
        out.push(field_type_wire(key.field_type));
        put_tags(&mut out, &adds);
        put_tags(&mut out, &removes);
        put_bytes(&mut out, &field_value_to_bytes(&value));
    }
    let counters = m.raw_actor_counters();
    put_u64(&mut out, counters.len() as u64);
    for (actor, n) in counters {
        put_actor(&mut out, &actor);
        put_u64(&mut out, n);
    }
    out
}

fn field_type_wire(t: FieldType) -> u8 {
    match t {
        FieldType::Counter => TAG_COUNTER,
        FieldType::OrSet => TAG_SET,
        FieldType::LwwRegister => TAG_REGISTER,
        FieldType::EwFlag => TAG_FLAG,
        FieldType::NestedMap => TAG_MAP,
    }
}

fn field_type_from_wire(tag: u8) -> Result<FieldType, CrdtSerialError> {
    match tag {
        TAG_COUNTER => Ok(FieldType::Counter),
        TAG_SET => Ok(FieldType::OrSet),
        TAG_REGISTER => Ok(FieldType::LwwRegister),
        TAG_FLAG => Ok(FieldType::EwFlag),
        TAG_MAP => Ok(FieldType::NestedMap),
        other => Err(CrdtSerialError::UnknownTag(other)),
    }
}

fn field_value_to_bytes(v: &FieldValue) -> Vec<u8> {
    match v {
        FieldValue::Counter(c) => counter_to_bytes(c),
        FieldValue::OrSet(s) => set_to_bytes(s),
        FieldValue::LwwRegister(r) => register_to_bytes(r),
        FieldValue::EwFlag(f) => flag_to_bytes(f),
        FieldValue::NestedMap(m) => map_to_bytes(m),
    }
}

fn field_value_from_bytes(
    bytes: &[u8],
    field_type: FieldType,
) -> Result<FieldValue, CrdtSerialError> {
    match field_type {
        FieldType::Counter => Ok(FieldValue::Counter(counter_from_bytes(bytes)?)),
        FieldType::OrSet => Ok(FieldValue::OrSet(set_from_bytes(bytes)?)),
        FieldType::LwwRegister => Ok(FieldValue::LwwRegister(register_from_bytes(bytes)?)),
        FieldType::EwFlag => Ok(FieldValue::EwFlag(flag_from_bytes(bytes)?)),
        FieldType::NestedMap => Ok(FieldValue::NestedMap(Box::new(map_from_bytes(bytes)?))),
    }
}

/// Decode an observed-remove map from its stored form.
///
/// # Errors
/// Version / tag / truncation / trailing errors per [`CrdtSerialError`].
pub fn map_from_bytes(buf: &[u8]) -> Result<Map, CrdtSerialError> {
    let mut r = Reader::new(buf);
    check_header(&mut r, TAG_MAP)?;
    let nf = r.u64()?;
    let mut fields = BTreeMap::new();
    for _ in 0..nf {
        let name = r.bytes()?;
        let field_type = field_type_from_wire(r.u8()?)?;
        let adds = read_tags(&mut r)?;
        let removes = read_tags(&mut r)?;
        let value_bytes = r.bytes()?;
        let value = field_value_from_bytes(&value_bytes, field_type)?;
        fields.insert(FieldKey::new(name, field_type), (adds, removes, value));
    }
    let nc = r.u64()?;
    let mut counters = BTreeMap::new();
    for _ in 0..nc {
        let actor = r.actor()?;
        let n = r.u64()?;
        counters.insert(actor, n);
    }
    r.done()?;
    Ok(Map::from_raw(fields, counters))
}

// ---- HyperLogLog -------------------------------------------------------------

/// Serialize a HyperLogLog register array to its stored form.
#[must_use]
pub fn hll_to_bytes(h: &HyperLogLog) -> Vec<u8> {
    let mut out = Vec::with_capacity(h.registers().len() + 16);
    out.push(FORMAT_V1);
    out.push(TAG_HLL);
    put_bytes(&mut out, h.registers());
    out
}

/// Decode a HyperLogLog register array from its stored form.
///
/// # Errors
/// Version / tag / truncation / trailing errors per
/// [`CrdtSerialError`], plus [`CrdtSerialError::Truncated`] if the
/// decoded register array is not exactly
/// [`crate::datatypes::hll::REGISTER_COUNT`] bytes.
pub fn hll_from_bytes(buf: &[u8]) -> Result<HyperLogLog, CrdtSerialError> {
    let mut r = Reader::new(buf);
    check_header(&mut r, TAG_HLL)?;
    let registers = r.bytes()?;
    r.done()?;
    HyperLogLog::from_registers(registers).ok_or(CrdtSerialError::Truncated)
}

// ---- header ----------------------------------------------------------------

/// Peek the type tag of a stored CRDT blob without fully decoding it.
///
/// # Errors
/// [`CrdtSerialError::Truncated`] if the blob is shorter than the
/// two-byte header, [`CrdtSerialError::BadVersion`] on an unknown
/// format version.
pub fn peek_tag(buf: &[u8]) -> Result<u8, CrdtSerialError> {
    let mut r = Reader::new(buf);
    let version = r.u8()?;
    if version != FORMAT_V1 {
        return Err(CrdtSerialError::BadVersion(version));
    }
    r.u8()
}

fn check_header(r: &mut Reader<'_>, expected_tag: u8) -> Result<(), CrdtSerialError> {
    let version = r.u8()?;
    if version != FORMAT_V1 {
        return Err(CrdtSerialError::BadVersion(version));
    }
    let tag = r.u8()?;
    if tag == expected_tag {
        Ok(())
    } else if tag == TAG_COUNTER
        || tag == TAG_SET
        || tag == TAG_REGISTER
        || tag == TAG_FLAG
        || tag == TAG_MAP
        || tag == TAG_HLL
    {
        Err(CrdtSerialError::TypeMismatch {
            found: tag,
            expected: expected_tag,
        })
    } else {
        Err(CrdtSerialError::UnknownTag(tag))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::Crdt;

    fn aid(peer: &str) -> ActorId {
        ActorId::new("dc1", peer)
    }

    #[test]
    fn counter_round_trips() {
        let mut c = PnCounter::new();
        c.increment(&aid("a"), 5);
        c.increment(&aid("b"), 3);
        c.decrement(&aid("a"), 2);
        let bytes = counter_to_bytes(&c);
        assert_eq!(peek_tag(&bytes).unwrap(), TAG_COUNTER);
        let back = counter_from_bytes(&bytes).unwrap();
        assert_eq!(back, c);
        assert_eq!(back.value(), c.value());
    }

    #[test]
    fn counter_merge_after_round_trip_sums() {
        // Two replicas each increment through their own actor, then
        // serialize + deserialize + merge -- the value must be the sum.
        let mut a = PnCounter::new();
        a.increment(&aid("a"), 1);
        let mut b = PnCounter::new();
        b.increment(&aid("b"), 1);
        let mut a2 = counter_from_bytes(&counter_to_bytes(&a)).unwrap();
        let b2 = counter_from_bytes(&counter_to_bytes(&b)).unwrap();
        a2.merge(&b2);
        assert_eq!(a2.value(), 2);
        // Idempotent: merging the same blob again does not double.
        a2.merge(&b2);
        assert_eq!(a2.value(), 2);
    }

    #[test]
    fn set_round_trips_and_merges_to_union() {
        let mut s = OrSet::new();
        s.add(&aid("a"), b"x".to_vec());
        let mut t = OrSet::new();
        t.add(&aid("b"), b"y".to_vec());
        let mut s2 = set_from_bytes(&set_to_bytes(&s)).unwrap();
        let t2 = set_from_bytes(&set_to_bytes(&t)).unwrap();
        s2.merge(&t2);
        let v = s2.value();
        assert!(v.contains(b"x".as_slice()));
        assert!(v.contains(b"y".as_slice()));
    }

    #[test]
    fn type_mismatch_is_rejected() {
        let mut c = PnCounter::new();
        c.increment(&aid("a"), 1);
        let bytes = counter_to_bytes(&c);
        let err = set_from_bytes(&bytes).unwrap_err();
        assert!(matches!(
            err,
            CrdtSerialError::TypeMismatch {
                found: TAG_COUNTER,
                expected: TAG_SET
            }
        ));
    }

    #[test]
    fn truncated_is_rejected() {
        let mut c = PnCounter::new();
        c.increment(&aid("a"), 1);
        let bytes = counter_to_bytes(&c);
        assert!(counter_from_bytes(&bytes[..bytes.len() - 3]).is_err());
    }

    #[test]
    fn register_round_trips() {
        let mut r = LwwRegister::new();
        r.assign(&aid("a"), 5, b"hello".to_vec());
        let bytes = register_to_bytes(&r);
        assert_eq!(peek_tag(&bytes).unwrap(), TAG_REGISTER);
        let back = register_from_bytes(&bytes).unwrap();
        assert_eq!(back, r);
        assert_eq!(back.value(), r.value());
    }

    #[test]
    fn empty_register_round_trips() {
        let r = LwwRegister::new();
        let back = register_from_bytes(&register_to_bytes(&r)).unwrap();
        assert_eq!(back, r);
        assert!(back.actor().is_none());
    }

    #[test]
    fn register_merge_after_round_trip_picks_lww_winner() {
        let mut a = LwwRegister::new();
        a.assign(&aid("a"), 1, b"early".to_vec());
        let mut b = LwwRegister::new();
        b.assign(&aid("b"), 2, b"late".to_vec());
        let mut a2 = register_from_bytes(&register_to_bytes(&a)).unwrap();
        let b2 = register_from_bytes(&register_to_bytes(&b)).unwrap();
        a2.merge(&b2);
        assert_eq!(a2.value(), b"late".to_vec());
        // Idempotent: merging the same blob again is a no-op.
        a2.merge(&b2);
        assert_eq!(a2.value(), b"late".to_vec());
    }

    #[test]
    fn register_merge_through_serialization_is_commutative() {
        let mut a = LwwRegister::new();
        a.assign(&aid("a"), 7, b"x".to_vec());
        let mut b = LwwRegister::new();
        b.assign(&aid("b"), 9, b"y".to_vec());
        let a2 = register_from_bytes(&register_to_bytes(&a)).unwrap();
        let b2 = register_from_bytes(&register_to_bytes(&b)).unwrap();

        let mut left = a2.clone();
        left.merge(&b2);
        let mut right = b2.clone();
        right.merge(&a2);
        assert_eq!(left, right);
    }

    #[test]
    fn flag_round_trips() {
        let mut f = EwFlag::new();
        f.enable(&aid("a"));
        let bytes = flag_to_bytes(&f);
        assert_eq!(peek_tag(&bytes).unwrap(), TAG_FLAG);
        let back = flag_from_bytes(&bytes).unwrap();
        assert_eq!(back, f);
        assert_eq!(back.value(), f.value());
    }

    #[test]
    fn empty_flag_round_trips() {
        let f = EwFlag::new();
        let back = flag_from_bytes(&flag_to_bytes(&f)).unwrap();
        assert_eq!(back, f);
        assert!(!back.value());
    }

    #[test]
    fn flag_merge_after_round_trip_is_enable_wins() {
        // Concurrent enable + disable resolves to enabled, the same
        // enable-wins rule the in-memory type enforces, preserved
        // across a serialize/deserialize round trip.
        let a = aid("a");
        let b = aid("b");
        let mut shared = EwFlag::new();
        shared.enable(&a);

        let mut left = shared.clone();
        left.disable();
        let mut right = shared.clone();
        right.enable(&b);

        let mut left2 = flag_from_bytes(&flag_to_bytes(&left)).unwrap();
        let right2 = flag_from_bytes(&flag_to_bytes(&right)).unwrap();
        left2.merge(&right2);
        assert!(left2.value());
        // Idempotent: merging the same blob again does not change it.
        left2.merge(&right2);
        assert!(left2.value());
    }

    #[test]
    fn flag_merge_through_serialization_is_commutative() {
        let a = aid("a");
        let b = aid("b");
        let mut x = EwFlag::new();
        x.enable(&a);
        x.disable();
        let mut y = EwFlag::new();
        y.enable(&b);

        let x2 = flag_from_bytes(&flag_to_bytes(&x)).unwrap();
        let y2 = flag_from_bytes(&flag_to_bytes(&y)).unwrap();

        let mut left = x2.clone();
        left.merge(&y2);
        let mut right = y2.clone();
        right.merge(&x2);
        assert_eq!(left, right);
    }

    #[test]
    fn register_type_mismatch_is_rejected() {
        let mut c = PnCounter::new();
        c.increment(&aid("a"), 1);
        let bytes = counter_to_bytes(&c);
        let err = register_from_bytes(&bytes).unwrap_err();
        assert!(matches!(
            err,
            CrdtSerialError::TypeMismatch {
                found: TAG_COUNTER,
                expected: TAG_REGISTER
            }
        ));
    }

    #[test]
    fn flag_type_mismatch_is_rejected() {
        let mut s = OrSet::new();
        s.add(&aid("a"), b"x".to_vec());
        let bytes = set_to_bytes(&s);
        let err = flag_from_bytes(&bytes).unwrap_err();
        assert!(matches!(
            err,
            CrdtSerialError::TypeMismatch {
                found: TAG_SET,
                expected: TAG_FLAG
            }
        ));
    }

    fn counter_field(name: &str) -> FieldKey {
        FieldKey::new(name.as_bytes(), FieldType::Counter)
    }

    fn register_field(name: &str) -> FieldKey {
        FieldKey::new(name.as_bytes(), FieldType::LwwRegister)
    }

    fn flag_field(name: &str) -> FieldKey {
        FieldKey::new(name.as_bytes(), FieldType::EwFlag)
    }

    fn set_field(name: &str) -> FieldKey {
        FieldKey::new(name.as_bytes(), FieldType::OrSet)
    }

    fn map_field(name: &str) -> FieldKey {
        FieldKey::new(name.as_bytes(), FieldType::NestedMap)
    }

    #[test]
    fn map_round_trips_with_counter_register_flag_and_set_fields() {
        use crate::datatypes::map::{MapOp, NestedOp};

        let a = aid("a");
        let mut m = Map::new();
        m.apply(
            &a,
            &MapOp::Update {
                field: counter_field("hits"),
                op: NestedOp::Counter(7),
            },
        );
        m.apply(
            &a,
            &MapOp::Update {
                field: register_field("name"),
                op: NestedOp::RegisterAssign {
                    value: b"alice".to_vec(),
                    ts_micros: 5,
                },
            },
        );
        m.apply(
            &a,
            &MapOp::Update {
                field: flag_field("on"),
                op: NestedOp::Flag(true),
            },
        );
        m.apply(
            &a,
            &MapOp::Update {
                field: set_field("tags"),
                op: NestedOp::SetAdd(b"x".to_vec()),
            },
        );

        let bytes = map_to_bytes(&m);
        assert_eq!(peek_tag(&bytes).unwrap(), TAG_MAP);
        let back = map_from_bytes(&bytes).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.value(), m.value());
    }

    #[test]
    fn nested_map_round_trips() {
        use crate::datatypes::map::{MapOp, NestedOp};

        let a = aid("a");
        let mut m = Map::new();
        m.apply(
            &a,
            &MapOp::Update {
                field: map_field("inner"),
                op: NestedOp::Map(Box::new(MapOp::Update {
                    field: counter_field("hits"),
                    op: NestedOp::Counter(3),
                })),
            },
        );

        let bytes = map_to_bytes(&m);
        let back = map_from_bytes(&bytes).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.value(), m.value());
    }

    #[test]
    fn doubly_nested_map_round_trips() {
        use crate::datatypes::map::{MapOp, NestedOp};

        let a = aid("a");
        let mut m = Map::new();
        m.apply(
            &a,
            &MapOp::Update {
                field: map_field("outer"),
                op: NestedOp::Map(Box::new(MapOp::Update {
                    field: map_field("inner"),
                    op: NestedOp::Map(Box::new(MapOp::Update {
                        field: counter_field("hits"),
                        op: NestedOp::Counter(9),
                    })),
                })),
            },
        );

        let bytes = map_to_bytes(&m);
        let back = map_from_bytes(&bytes).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.value(), m.value());
    }

    #[test]
    fn empty_map_round_trips() {
        let m = Map::new();
        let back = map_from_bytes(&map_to_bytes(&m)).unwrap();
        assert_eq!(back, m);
        assert!(back.value().is_empty());
    }

    #[test]
    fn map_merge_through_serialization_is_commutative_and_idempotent() {
        use crate::datatypes::map::{MapOp, NestedOp};

        let a = aid("a");
        let b = aid("b");
        let mut x = Map::new();
        x.apply(
            &a,
            &MapOp::Update {
                field: counter_field("c"),
                op: NestedOp::Counter(3),
            },
        );
        let mut y = Map::new();
        y.apply(
            &b,
            &MapOp::Update {
                field: counter_field("c"),
                op: NestedOp::Counter(5),
            },
        );
        let mut x2 = map_from_bytes(&map_to_bytes(&x)).unwrap();
        let y2 = map_from_bytes(&map_to_bytes(&y)).unwrap();

        let mut left = x2.clone();
        left.merge(&y2);
        let mut right = y2.clone();
        right.merge(&x2);
        assert_eq!(left.value(), right.value());

        x2.merge(&y2);
        x2.merge(&y2);
        assert_eq!(x2.value(), left.value());
    }

    #[test]
    fn map_type_mismatch_is_rejected() {
        let mut c = PnCounter::new();
        c.increment(&aid("a"), 1);
        let bytes = counter_to_bytes(&c);
        let err = map_from_bytes(&bytes).unwrap_err();
        assert!(matches!(
            err,
            CrdtSerialError::TypeMismatch {
                found: TAG_COUNTER,
                expected: TAG_MAP
            }
        ));
    }

    #[test]
    fn hll_round_trips() {
        let mut h = HyperLogLog::new();
        for i in 0u32..500 {
            h.add(i.to_be_bytes());
        }
        let bytes = hll_to_bytes(&h);
        assert_eq!(peek_tag(&bytes).unwrap(), TAG_HLL);
        let back = hll_from_bytes(&bytes).unwrap();
        assert_eq!(back, h);
        assert_eq!(back.value(), h.value());
    }

    #[test]
    fn empty_hll_round_trips() {
        let h = HyperLogLog::new();
        let back = hll_from_bytes(&hll_to_bytes(&h)).unwrap();
        assert_eq!(back, h);
        assert_eq!(back.value(), 0);
    }

    #[test]
    fn hll_merge_through_serialization_is_commutative_and_idempotent() {
        let mut a = HyperLogLog::new();
        for i in 0u32..200 {
            a.add(i.to_be_bytes());
        }
        let mut b = HyperLogLog::new();
        for i in 100u32..300 {
            b.add(i.to_be_bytes());
        }
        let mut a2 = hll_from_bytes(&hll_to_bytes(&a)).unwrap();
        let b2 = hll_from_bytes(&hll_to_bytes(&b)).unwrap();

        let mut left = a2.clone();
        left.merge(&b2);
        let mut right = b2.clone();
        right.merge(&a2);
        assert_eq!(left, right);

        a2.merge(&b2);
        a2.merge(&b2);
        assert_eq!(a2, left);
    }

    #[test]
    fn hll_type_mismatch_is_rejected() {
        let mut f = EwFlag::new();
        f.enable(&aid("a"));
        let bytes = flag_to_bytes(&f);
        let err = hll_from_bytes(&bytes).unwrap_err();
        assert!(matches!(
            err,
            CrdtSerialError::TypeMismatch {
                found: TAG_FLAG,
                expected: TAG_HLL
            }
        ));
    }

    #[test]
    fn peek_tag_distinguishes_all_six_types() {
        let counter = counter_to_bytes(&PnCounter::new());
        let set = set_to_bytes(&OrSet::new());
        let register = register_to_bytes(&LwwRegister::new());
        let flag = flag_to_bytes(&EwFlag::new());
        let map = map_to_bytes(&Map::new());
        let hll = hll_to_bytes(&HyperLogLog::new());
        assert_eq!(peek_tag(&counter).unwrap(), TAG_COUNTER);
        assert_eq!(peek_tag(&set).unwrap(), TAG_SET);
        assert_eq!(peek_tag(&register).unwrap(), TAG_REGISTER);
        assert_eq!(peek_tag(&flag).unwrap(), TAG_FLAG);
        assert_eq!(peek_tag(&map).unwrap(), TAG_MAP);
        assert_eq!(peek_tag(&hll).unwrap(), TAG_HLL);
    }
}
