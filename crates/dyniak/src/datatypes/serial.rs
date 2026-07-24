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

use crate::datatypes::set::{OrSet, Tag};
use crate::datatypes::{ActorId, Crdt, EwFlag, LwwRegister, PnCounter};

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
    } else if tag == TAG_COUNTER || tag == TAG_SET || tag == TAG_REGISTER || tag == TAG_FLAG {
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
}
