//! Read-Atomic Multi-Partition (RAMP) transactions.
//!
//! RAMP gives multi-key *read-atomic isolation* without locks and
//! without a reader ever blocking on a writer. A transaction either
//! sees ALL of another transaction's writes or NONE of them: no
//! *fractured read* where a reader observes transaction `T`'s write to
//! key `a` but misses `T`'s write to key `b`. This is a lower-latency,
//! availability-native complement to the heavyweight cross-node XA /
//! 2PC path in [`crate::datastore::xa`] for the common
//! "atomic multi-key read/write without full serializability" case.
//!
//! This module implements **RAMP-Fast** (Bailis et al., SIGMOD 2014):
//!
//! * **Writes are two-phase but non-blocking.** A write transaction
//!   picks one monotonic timestamp `ts` for the whole batch. In the
//!   PREPARE phase every item is written as a *versioned, invisible*
//!   record keyed by `ts`, carrying metadata = the SET of sibling keys
//!   the same transaction wrote. In the COMMIT phase each key's
//!   *latest-visible pointer* is advanced to `ts`. A reader never waits
//!   on a writer: it reads whatever pointer is currently visible.
//!
//! * **Reads are one round plus a conditional second round.** Round 1
//!   fetches the latest-visible version and its metadata for every key.
//!   The reader then checks: does any returned version's metadata name
//!   a sibling key for which the reader saw an *older* version than the
//!   sibling's transaction wrote? If so the first-round snapshot is
//!   fractured; round 2 fetches exactly those missing versions by their
//!   timestamp (which PREPARE guarantees is present even before it is
//!   the visible one). The repaired snapshot is fracture-free. In the
//!   common (contention-free) case round 1 already returns a
//!   fracture-free snapshot and round 2 is skipped.
//!
//! The read-atomic decision logic -- what a reader keeps from round 1,
//! which siblings it must re-fetch in round 2, and how the repaired
//! snapshot is assembled -- lives in [`select`] as a pure,
//! side-effect-free core. The production coordinator
//! ([`RampCoordinator`]) and the deterministic simulation model
//! (`crates/model-tests/src/ramp.rs`) both drive that same core, so the
//! model gates the real decision logic and not a re-imagining of it.
//!
//! # Scope
//!
//! This slice implements RAMP-Fast for the **single-node, local
//! multi-key** case: a transaction's keys all live in one process's
//! store, and the coordinator fans PREPARE / COMMIT / read rounds
//! across them in-process. The multi-partition wire fan-out over the
//! dnode peer plane (a [`dynomite::proto::dnode::DmsgType::RampPrepare`]
//! message analogous to the XA legs) is the documented next step; the
//! isolation algorithm itself -- fractured-read prevention -- is fully
//! implemented and gated here and does not change when the fan-out
//! moves cross-node, because RAMP's atomicity is a property of the
//! per-item versioning + metadata, not of where the items live.
//!
//! # Examples
//!
//! ```
//! use dyniak::ramp::{RampItem, select};
//!
//! // Round 1 saw key `a` at ts=5 (which names sibling `b`) and key
//! // `b` at ts=2 (an older, unrelated version). The reader must
//! // re-fetch `b` at ts=5 in round 2 to avoid a fractured read.
//! let a = RampItem::new(b"a".to_vec(), 5, vec![b"b".to_vec()], b"va".to_vec());
//! let b_old = RampItem::new(b"b".to_vec(), 2, vec![], b"vb-old".to_vec());
//! let round1 = vec![a, b_old];
//! let missing = select(&round1);
//! assert_eq!(missing, vec![(b"b".to_vec(), 5)]);
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A monotonically increasing transaction timestamp.
///
/// RAMP requires timestamps to be unique per write transaction and
/// comparable across transactions; the low bits carry a per-coordinator
/// counter and the high bits a coordinator id so two coordinators never
/// mint the same value (see [`RampClock`]).
pub type Timestamp = u64;

/// One versioned item as returned by a read round.
///
/// A RAMP item is the value a single write transaction stored for one
/// key, tagged with that transaction's timestamp and the set of sibling
/// keys the same transaction wrote. The sibling set is the RAMP-Fast
/// metadata that drives the second-round repair.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RampItem {
    /// The key this item belongs to.
    pub key: Vec<u8>,
    /// The writing transaction's timestamp.
    pub ts: Timestamp,
    /// The other keys the same transaction wrote (RAMP-Fast metadata).
    /// Does not include `key` itself.
    pub siblings: Vec<Vec<u8>>,
    /// The stored value bytes.
    pub value: Vec<u8>,
}

impl RampItem {
    /// Build a versioned item.
    #[must_use]
    pub fn new(key: Vec<u8>, ts: Timestamp, siblings: Vec<Vec<u8>>, value: Vec<u8>) -> Self {
        Self {
            key,
            ts,
            siblings,
            value,
        }
    }
}

/// The pure RAMP-Fast read-atomic core.
///
/// Given the versions a reader observed in round 1 (one [`RampItem`]
/// per key it is reading, the latest *visible* version of each), decide
/// which `(key, timestamp)` pairs the reader must fetch in round 2 to
/// guarantee a fracture-free snapshot.
///
/// The algorithm is exactly RAMP-Fast's: for every item `i` in the
/// round-1 set and every sibling `s` that `i`'s metadata names, if the
/// reader's current version of `s` has a timestamp *older* than `i.ts`,
/// then `i`'s transaction also wrote `s` at `i.ts` and the reader is
/// currently missing that write -- a fractured read. The reader must
/// upgrade `s` to `i.ts`. When several observed items name the same
/// sibling, the reader upgrades to the *highest* required timestamp
/// (the freshest transaction that the reader has already partially
/// observed), which subsumes the lower requirements.
///
/// Returns the `(key, ts)` pairs to fetch in round 2, sorted for
/// determinism. An empty result means round 1 was already
/// fracture-free and round 2 is skipped (the common case).
///
/// # Examples
///
/// ```
/// use dyniak::ramp::{RampItem, select};
///
/// // Fracture-free: `a` names `b`, and `b` is already at `a`'s ts.
/// let a = RampItem::new(b"a".to_vec(), 7, vec![b"b".to_vec()], b"va".to_vec());
/// let b = RampItem::new(b"b".to_vec(), 7, vec![b"a".to_vec()], b"vb".to_vec());
/// assert!(select(&[a, b]).is_empty());
/// ```
#[must_use]
pub fn select(round1: &[RampItem]) -> Vec<(Vec<u8>, Timestamp)> {
    // The reader's current timestamp per key (from round 1).
    let mut current: BTreeMap<&[u8], Timestamp> = BTreeMap::new();
    for item in round1 {
        // If a key appears twice in the round-1 set (should not happen
        // for a well-formed read set) keep the highest ts.
        let e = current.entry(item.key.as_slice()).or_insert(item.ts);
        if item.ts > *e {
            *e = item.ts;
        }
    }

    // For each key, the highest ts any observed sibling-metadata says
    // the reader ought to be seeing.
    let mut required: BTreeMap<Vec<u8>, Timestamp> = BTreeMap::new();
    for item in round1 {
        for sib in &item.siblings {
            // The reader currently sees `sib` at `have` (0 if it is not
            // in the read set at all -- it still must upgrade, because
            // the sibling's write at `item.ts` is part of the same
            // atomic transaction the reader already partially observed).
            let have = current.get(sib.as_slice()).copied().unwrap_or(0);
            if have < item.ts {
                let e = required.entry(sib.clone()).or_insert(item.ts);
                if item.ts > *e {
                    *e = item.ts;
                }
            }
        }
    }

    required.into_iter().collect()
}

/// A per-coordinator monotonic timestamp source for RAMP writes.
///
/// The high 16 bits are the coordinator id and the low 48 bits a
/// strictly increasing counter, so two coordinators never mint the same
/// timestamp and each coordinator's timestamps are monotonic. That is
/// all RAMP-Fast needs from the clock: uniqueness and per-writer
/// monotonicity (it does not require a global total order, which is why
/// RAMP is AP-native and lock-free).
#[derive(Debug)]
pub struct RampClock {
    id: u16,
    counter: u64,
}

impl RampClock {
    /// Create a clock for coordinator `id`.
    #[must_use]
    pub fn new(id: u16) -> Self {
        Self { id, counter: 0 }
    }

    /// Mint the next unique, monotonically increasing timestamp.
    pub fn mint(&mut self) -> Timestamp {
        self.counter += 1;
        (u64::from(self.id) << 48) | (self.counter & 0x0000_ffff_ffff_ffff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(k: &[u8], ts: Timestamp, sibs: &[&[u8]]) -> RampItem {
        RampItem::new(
            k.to_vec(),
            ts,
            sibs.iter().map(|s| s.to_vec()).collect(),
            format!("v@{ts}").into_bytes(),
        )
    }

    #[test]
    fn no_siblings_never_repairs() {
        let r = vec![item(b"a", 1, &[]), item(b"b", 2, &[])];
        assert!(select(&r).is_empty());
    }

    #[test]
    fn consistent_snapshot_is_fracture_free() {
        // Both written by the same txn at ts=5, each naming the other.
        let r = vec![item(b"a", 5, &[b"b"]), item(b"b", 5, &[b"a"])];
        assert!(select(&r).is_empty());
    }

    #[test]
    fn detects_and_repairs_a_fractured_read() {
        // `a` at ts=5 names `b`, but the reader saw `b` at ts=2: the
        // txn that wrote `a`@5 also wrote `b`@5, which the reader is
        // missing. Round 2 must fetch `b`@5.
        let r = vec![item(b"a", 5, &[b"b"]), item(b"b", 2, &[])];
        assert_eq!(select(&r), vec![(b"b".to_vec(), 5)]);
    }

    #[test]
    fn upgrades_to_highest_required_ts() {
        // Two observed items name `c`: `a`@8 and `b`@4. The reader saw
        // `c`@1; it must upgrade to the highest requirement, 8.
        let r = vec![
            item(b"a", 8, &[b"c"]),
            item(b"b", 4, &[b"c"]),
            item(b"c", 1, &[]),
        ];
        assert_eq!(select(&r), vec![(b"c".to_vec(), 8)]);
    }

    #[test]
    fn newer_local_version_needs_no_repair() {
        // `a`@3 names `b`, but the reader already saw `b`@6 (a newer
        // txn). No repair: the reader is not behind on `b`.
        let r = vec![item(b"a", 3, &[b"b"]), item(b"b", 6, &[])];
        assert!(select(&r).is_empty());
    }

    #[test]
    fn sibling_absent_from_read_set_still_upgrades() {
        // `a`@5 names `z`, which is not in the reader's round-1 set at
        // all (have=0 < 5). The reader must fetch `z`@5.
        let r = vec![item(b"a", 5, &[b"z"])];
        assert_eq!(select(&r), vec![(b"z".to_vec(), 5)]);
    }

    #[test]
    fn clock_is_unique_and_monotonic() {
        let mut c0 = RampClock::new(0);
        let mut c1 = RampClock::new(1);
        let a = c0.mint();
        let b = c0.mint();
        assert!(b > a, "monotonic per coordinator");
        let x = c1.mint();
        assert_ne!(a, x, "distinct coordinators never collide");
        assert_ne!(b, x);
    }
}
