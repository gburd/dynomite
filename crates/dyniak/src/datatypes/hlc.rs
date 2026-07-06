//! Hybrid Logical Clocks (HLC) -- a scalar, monotonic,
//! physically-close timestamp.
//!
//! The [`Hlc`] type implements the Hybrid Logical Clock described by
//! Kulkarni, Demirbas, Madappa, Avva, and Leone in "Logical Physical
//! Clocks and Consistent Snapshots in Globally Distributed Databases"
//! (OPODIS 2014). An HLC timestamp is a pair `(l, c)`:
//!
//! * `l` tracks the maximum physical time this node has ever
//!   observed -- either from its own physical clock or carried in a
//!   message received from another node.
//! * `c` is a bounded logical counter that breaks ties when physical
//!   time does not advance (a burst of events inside one clock tick,
//!   or a received `l` equal to the local `l`).
//!
//! HLC is *complementary* to the Interval Tree Clock in [`crate::datatypes::itc`].
//! ITC captures the causal *partial* order for conflict detection;
//! HLC gives a *scalar, totally ordered* stamp usable for snapshot
//! and version selection. The two coexist: a write can carry an ITC
//! stamp for conflict resolution and an HLC stamp for
//! bounded-staleness snapshot reads.
//!
//! # Guarantees
//!
//! Under the paper's update rules, HLC provides:
//!
//! 1. **Physical closeness.** `l` is always within the maximum
//!    inter-node physical-clock skew of the true physical time,
//!    so an HLC timestamp is never far from wall-clock time (unlike
//!    an unbounded Lamport clock).
//! 2. **Causality capture.** If event `e` happens-before `f`
//!    (`e -> f`, whether a local sequence on one node or a
//!    send/receive across nodes), then `hlc(e) < hlc(f)` strictly.
//!    HLC is a logical clock in the Lamport sense.
//! 3. **Monotonicity.** A node's HLC never goes backwards across its
//!    own successive events, regardless of physical-clock jitter.
//! 4. **Bounded counter.** `c` stays small; it only grows while
//!    physical time is stalled and resets to zero the moment `l`
//!    advances.
//!
//! # Deterministic physical time
//!
//! The core logic never reads the wall clock. Both event
//! constructors take a `physical_now: u64` argument (nanoseconds or
//! any monotonic unit; the type is unit-agnostic). This keeps the
//! type a pure, deterministic state machine so the model checker in
//! `crates/model-tests/src/hlc.rs` can drive physical time on a
//! scripted schedule (including skew and non-advancing time).
//! Production callers pass a real clock reading; see
//! [`Hlc::now_from_wall_clock`] for a convenience that does exactly
//! that at the edge.
//!
//! # Wire format
//!
//! [`Hlc::pack`] / [`Hlc::unpack`] fold the pair into a single `u64`
//! whose numeric order equals the HLC order: the top 48 bits are `l`,
//! the low 16 bits are `c`. This is the compact scalar a future MVCC
//! layer would stamp onto a stored version, and it is directly
//! comparable as an integer for the "latest version with
//! `hlc <= snapshot_ts`" snapshot-read predicate.
//! [`Hlc::encode`] / [`Hlc::decode`] round-trip the same value
//! through 8 big-endian bytes for on-the-wire carriage.
//!
//! # Examples
//!
//! ```
//! use dyniak::datatypes::Hlc;
//!
//! // A node advances its clock on a local event at physical time 10.
//! let mut node = Hlc::zero();
//! let t1 = node.tick(10);
//! assert_eq!(t1.logical(), 10);
//! assert_eq!(t1.counter(), 0);
//!
//! // Physical time has not advanced on the next event: the counter
//! // breaks the tie so the stamp still moves forward.
//! let t2 = node.tick(10);
//! assert!(t2 > t1);
//! assert_eq!(t2.counter(), 1);
//!
//! // A message arrives carrying a higher `l`; the receiver adopts it.
//! let remote = Hlc::from_parts(25, 3).unwrap();
//! let t3 = node.update(&remote, 12);
//! assert!(t3 > remote); // causality: receive is strictly after send
//! assert_eq!(t3.logical(), 25);
//! ```

use std::cmp::Ordering;
use std::fmt;

/// The maximum value the logical component `l` may hold: 48 bits.
///
/// With `l` in nanoseconds this covers roughly 3.2 million years,
/// which is far more than any deployment needs while leaving 16 bits
/// for the counter. Using 48 bits (rather than the full 64) is what
/// lets the packed `u64` scalar keep the counter inline while
/// preserving the numeric == HLC ordering.
pub const MAX_LOGICAL: u64 = (1u64 << 48) - 1;

/// The maximum value the counter component `c` may hold: 16 bits.
///
/// The paper proves the counter stays bounded under the update rules;
/// 16 bits (65535) is a generous ceiling. Reaching it means physical
/// time has failed to advance across 65536 successive events, which
/// signals a wedged or badly misconfigured clock. [`HlcError::CounterOverflow`]
/// surfaces that condition rather than silently wrapping.
pub const MAX_COUNTER: u16 = u16::MAX;

/// Error conditions from HLC construction and updates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HlcError {
    /// A logical component exceeded [`MAX_LOGICAL`]. Physical time
    /// this large is out of range for the 48-bit `l` field.
    LogicalOverflow,
    /// The counter would exceed [`MAX_COUNTER`]. Physical time has
    /// not advanced across 65536 successive events; the local clock
    /// is wedged or grossly misconfigured.
    CounterOverflow,
    /// [`Hlc::decode`] was given a slice that was not exactly 8 bytes.
    BadEncoding,
}

impl fmt::Display for HlcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HlcError::LogicalOverflow => {
                write!(f, "hlc logical component exceeds 48 bits")
            }
            HlcError::CounterOverflow => {
                write!(f, "hlc counter exceeds 16 bits (physical clock wedged)")
            }
            HlcError::BadEncoding => write!(f, "hlc encoding must be exactly 8 bytes"),
        }
    }
}

impl std::error::Error for HlcError {}

/// A Hybrid Logical Clock timestamp: the pair `(l, c)`.
///
/// `l` is the maximum physical time seen; `c` is the bounded tie
/// break counter. The derived [`Ord`] compares `l` first, then `c`,
/// which is exactly the HLC order and matches the numeric order of
/// the packed `u64` scalar from [`Hlc::pack`].
///
/// Construct the zero stamp with [`Hlc::zero`], advance it on a local
/// event with [`Hlc::tick`], and merge a received stamp with
/// [`Hlc::update`]. Both mutators return the new stamp and store it
/// back into `self`, so a single `Hlc` value serves as a node's clock.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Hlc {
    /// Physical-time component (max observed). At most [`MAX_LOGICAL`].
    l: u64,
    /// Logical tie break counter. At most [`MAX_COUNTER`].
    c: u16,
}

impl Default for Hlc {
    fn default() -> Self {
        Self::zero()
    }
}

impl fmt::Display for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "hlc(l={}, c={})", self.l, self.c)
    }
}

impl Hlc {
    /// The origin stamp `(0, 0)`, ordered below every other stamp.
    /// A fresh node starts here.
    #[must_use]
    pub const fn zero() -> Self {
        Self { l: 0, c: 0 }
    }

    /// Construct a stamp from an explicit `(l, c)` pair.
    ///
    /// # Errors
    ///
    /// [`HlcError::LogicalOverflow`] if `l > `[`MAX_LOGICAL`].
    pub const fn from_parts(l: u64, c: u16) -> Result<Self, HlcError> {
        if l > MAX_LOGICAL {
            return Err(HlcError::LogicalOverflow);
        }
        Ok(Self { l, c })
    }

    /// The physical-time (logical) component `l`.
    #[must_use]
    pub const fn logical(&self) -> u64 {
        self.l
    }

    /// The tie break counter component `c`.
    #[must_use]
    pub const fn counter(&self) -> u16 {
        self.c
    }

    /// Advance this clock for a *local* event (or a message send) at
    /// physical time `physical_now`, returning the new stamp.
    ///
    /// This is the paper's "Send or local event" rule:
    ///
    /// ```text
    /// l' = max(l, pt)
    /// c' = if l' == l { c + 1 } else { 0 }
    /// ```
    ///
    /// When the physical clock has advanced past the stored `l`, the
    /// stamp adopts physical time and the counter resets to zero.
    /// When physical time is behind or equal (clock jitter, or a
    /// burst of events inside one tick), `l` holds and the counter
    /// increments so the stamp still moves strictly forward -- this
    /// is the monotonicity guarantee.
    ///
    /// The returned stamp is also stored back into `self`, so the
    /// same value is the node's running clock.
    ///
    /// # Panics
    ///
    /// Panics if `physical_now` exceeds [`MAX_LOGICAL`] or the counter
    /// would exceed [`MAX_COUNTER`]. Callers that must not panic use
    /// [`Hlc::try_tick`]. In practice `physical_now` is a real clock
    /// reading in the type's unit, which is 48-bit-safe for millennia,
    /// and the counter only overflows on a wedged clock.
    #[must_use]
    pub fn tick(&mut self, physical_now: u64) -> Hlc {
        self.try_tick(physical_now)
            .expect("invariant: physical_now within 48 bits and counter not wedged")
    }

    /// Fallible form of [`Hlc::tick`].
    ///
    /// # Errors
    ///
    /// [`HlcError::LogicalOverflow`] if `physical_now > `[`MAX_LOGICAL`].
    /// [`HlcError::CounterOverflow`] if `l` did not advance and the
    /// counter is already at [`MAX_COUNTER`].
    pub fn try_tick(&mut self, physical_now: u64) -> Result<Hlc, HlcError> {
        if physical_now > MAX_LOGICAL {
            return Err(HlcError::LogicalOverflow);
        }
        let prev_l = self.l;
        let new_l = prev_l.max(physical_now);
        let new_c = if new_l == prev_l {
            self.c.checked_add(1).ok_or(HlcError::CounterOverflow)?
        } else {
            0
        };
        self.l = new_l;
        self.c = new_c;
        Ok(*self)
    }

    /// Advance this clock for a *receive* event that merged a stamp
    /// `received` carried on an incoming message, at physical time
    /// `physical_now`. Returns the new stamp.
    ///
    /// This is the paper's "Receive event" rule. Let `l_m` and `c_m`
    /// be the received stamp's components:
    ///
    /// ```text
    /// l' = max(l, l_m, pt)
    /// c' = if l' == l && l' == l_m { max(c, c_m) + 1 }
    ///      else if l' == l          { c + 1 }
    ///      else if l' == l_m        { c_m + 1 }
    ///      else                     { 0 }
    /// ```
    ///
    /// The receiver's `l` jumps to whichever of its own `l`, the
    /// message's `l_m`, or physical time is largest. The counter
    /// resets to zero when physical time won outright; otherwise it
    /// increments off whichever side (or the max of both, on a full
    /// tie) contributed the winning `l`. The `+ 1` on every non-reset
    /// branch is what makes a receive strictly later than its send:
    /// `hlc(receive) > received`, capturing the send -> receive edge.
    ///
    /// The returned stamp is stored back into `self`.
    ///
    /// # Panics
    ///
    /// Panics if `physical_now` or `received.l` exceeds
    /// [`MAX_LOGICAL`], or the counter would exceed [`MAX_COUNTER`].
    /// Use [`Hlc::try_update`] to handle those without panicking.
    #[must_use]
    pub fn update(&mut self, received: &Hlc, physical_now: u64) -> Hlc {
        self.try_update(received, physical_now)
            .expect("invariant: components within 48 bits and counter not wedged")
    }

    /// Fallible form of [`Hlc::update`].
    ///
    /// # Errors
    ///
    /// [`HlcError::LogicalOverflow`] if `physical_now` or `received.l`
    /// exceeds [`MAX_LOGICAL`].
    /// [`HlcError::CounterOverflow`] if the winning branch's counter
    /// is already at [`MAX_COUNTER`].
    pub fn try_update(&mut self, received: &Hlc, physical_now: u64) -> Result<Hlc, HlcError> {
        if physical_now > MAX_LOGICAL || received.l > MAX_LOGICAL {
            return Err(HlcError::LogicalOverflow);
        }
        let prev_l = self.l;
        let l_m = received.l;
        let new_l = prev_l.max(l_m).max(physical_now);
        let new_c = if new_l == prev_l && new_l == l_m {
            // Full tie between local and received: advance past both.
            self.c
                .max(received.c)
                .checked_add(1)
                .ok_or(HlcError::CounterOverflow)?
        } else if new_l == prev_l {
            self.c.checked_add(1).ok_or(HlcError::CounterOverflow)?
        } else if new_l == l_m {
            received.c.checked_add(1).ok_or(HlcError::CounterOverflow)?
        } else {
            // Physical time strictly dominates: reset the counter.
            0
        };
        self.l = new_l;
        self.c = new_c;
        Ok(*self)
    }

    /// Fold `(l, c)` into a single `u64` scalar whose numeric order
    /// equals the HLC order: `l` in the top 48 bits, `c` in the low
    /// 16 bits.
    ///
    /// This is the compact version stamp a future MVCC layer would
    /// store, and it is directly comparable as an integer for the
    /// snapshot-read predicate "latest version with `pack <= snapshot`".
    #[must_use]
    pub const fn pack(&self) -> u64 {
        (self.l << 16) | (self.c as u64)
    }

    /// Inverse of [`Hlc::pack`]. Total over all `u64` since the packed
    /// layout uses every bit and `l` occupies only 48 of them.
    #[must_use]
    pub const fn unpack(packed: u64) -> Self {
        Self {
            l: packed >> 16,
            c: (packed & 0xffff) as u16,
        }
    }

    /// Encode the stamp as 8 big-endian bytes (the packed scalar).
    /// The byte order preserves the HLC order under `memcmp`, so
    /// encoded stamps sort correctly as key suffixes.
    #[must_use]
    pub fn encode(&self) -> [u8; 8] {
        self.pack().to_be_bytes()
    }

    /// Decode an 8-byte big-endian stamp produced by [`Hlc::encode`].
    ///
    /// # Errors
    ///
    /// [`HlcError::BadEncoding`] if `bytes` is not exactly 8 bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, HlcError> {
        let arr: [u8; 8] = bytes.try_into().map_err(|_| HlcError::BadEncoding)?;
        Ok(Self::unpack(u64::from_be_bytes(arr)))
    }

    /// Convenience for production edges: advance the clock for a local
    /// event using the system wall clock as `physical_now`, in
    /// milliseconds since the Unix epoch.
    ///
    /// The core [`Hlc::tick`] takes physical time as a parameter for
    /// determinism; this wrapper reads the clock once at the boundary
    /// and delegates. A pre-epoch or out-of-range clock reading is
    /// clamped into range so the call cannot panic on a bad host clock.
    ///
    /// This is the only function in the module that touches real time;
    /// tests and the model checker use [`Hlc::tick`] directly.
    #[must_use]
    pub fn now_from_wall_clock(&mut self) -> Hlc {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        let pt = u64::try_from(millis)
            .unwrap_or(MAX_LOGICAL)
            .min(MAX_LOGICAL);
        self.tick(pt)
    }
}

/// Compare two HLC stamps. Equivalent to the derived [`Ord`]; exposed
/// as a free function to mirror the ITC surface and to read as an
/// explicit total order at call sites that select versions.
#[must_use]
pub fn hlc_cmp(a: &Hlc, b: &Hlc) -> Ordering {
    a.cmp(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_the_minimum() {
        let z = Hlc::zero();
        assert_eq!(z.logical(), 0);
        assert_eq!(z.counter(), 0);
        assert!(z <= Hlc::from_parts(0, 1).unwrap());
        assert!(z <= Hlc::from_parts(1, 0).unwrap());
    }

    #[test]
    fn tick_adopts_advancing_physical_time_and_resets_counter() {
        let mut c = Hlc::zero();
        let a = c.tick(10);
        assert_eq!((a.logical(), a.counter()), (10, 0));
        let b = c.tick(20);
        assert_eq!((b.logical(), b.counter()), (20, 0));
        assert!(b > a);
    }

    #[test]
    fn tick_stalled_physical_time_increments_counter() {
        let mut c = Hlc::zero();
        let a = c.tick(10);
        let b = c.tick(10);
        let d = c.tick(5); // clock went backwards
        assert_eq!((a.logical(), a.counter()), (10, 0));
        assert_eq!((b.logical(), b.counter()), (10, 1));
        assert_eq!((d.logical(), d.counter()), (10, 2));
        assert!(a < b && b < d);
    }

    #[test]
    fn update_adopts_higher_received_logical() {
        let mut c = Hlc::zero();
        let _ = c.tick(10);
        let remote = Hlc::from_parts(25, 3).unwrap();
        let r = c.update(&remote, 12);
        // l_m dominates -> l' = 25, c' = c_m + 1 = 4.
        assert_eq!((r.logical(), r.counter()), (25, 4));
        assert!(r > remote);
    }

    #[test]
    fn update_full_tie_takes_max_counter_plus_one() {
        let mut c = Hlc::from_parts(30, 2).unwrap();
        let remote = Hlc::from_parts(30, 5).unwrap();
        let r = c.update(&remote, 30);
        assert_eq!((r.logical(), r.counter()), (30, 6));
    }

    #[test]
    fn update_physical_time_dominates_resets_counter() {
        let mut c = Hlc::from_parts(30, 9).unwrap();
        let remote = Hlc::from_parts(20, 4).unwrap();
        let r = c.update(&remote, 40);
        assert_eq!((r.logical(), r.counter()), (40, 0));
    }

    #[test]
    fn receive_is_strictly_after_send_when_local_stalled() {
        // Local l equals received l, physical time behind: local
        // branch, c increments off self.c.
        let mut c = Hlc::from_parts(50, 1).unwrap();
        let remote = Hlc::from_parts(40, 7).unwrap();
        let r = c.update(&remote, 10);
        assert_eq!((r.logical(), r.counter()), (50, 2));
        assert!(r > remote);
    }

    #[test]
    fn pack_unpack_round_trip_and_orders() {
        let a = Hlc::from_parts(10, 5).unwrap();
        let b = Hlc::from_parts(10, 6).unwrap();
        let d = Hlc::from_parts(11, 0).unwrap();
        assert_eq!(Hlc::unpack(a.pack()), a);
        assert!(a.pack() < b.pack());
        assert!(b.pack() < d.pack());
        // numeric order equals HLC order
        assert_eq!(a.pack().cmp(&d.pack()), a.cmp(&d));
    }

    #[test]
    fn encode_decode_round_trip() {
        let a = Hlc::from_parts(123_456, 789).unwrap();
        let bytes = a.encode();
        assert_eq!(Hlc::decode(&bytes).unwrap(), a);
        assert_eq!(Hlc::decode(&[0u8; 4]), Err(HlcError::BadEncoding));
        assert_eq!(Hlc::decode(&[0u8; 9]), Err(HlcError::BadEncoding));
    }

    #[test]
    fn from_parts_rejects_oversized_logical() {
        assert_eq!(
            Hlc::from_parts(MAX_LOGICAL + 1, 0),
            Err(HlcError::LogicalOverflow)
        );
        assert!(Hlc::from_parts(MAX_LOGICAL, MAX_COUNTER).is_ok());
    }

    #[test]
    fn try_tick_reports_counter_overflow_on_wedged_clock() {
        let mut c = Hlc::from_parts(10, MAX_COUNTER).unwrap();
        // Physical time not advancing past l -> counter would overflow.
        assert_eq!(c.try_tick(10), Err(HlcError::CounterOverflow));
        assert_eq!(c.try_tick(5), Err(HlcError::CounterOverflow));
        // Advancing physical time resets the counter, no overflow.
        assert!(c.try_tick(11).is_ok());
    }

    #[test]
    fn try_update_reports_overflow_and_logical_range() {
        let mut c = Hlc::from_parts(10, MAX_COUNTER).unwrap();
        let remote = Hlc::from_parts(10, 3).unwrap();
        assert_eq!(c.try_update(&remote, 10), Err(HlcError::CounterOverflow));
        let mut d = Hlc::zero();
        let big = Hlc {
            l: MAX_LOGICAL + 1,
            c: 0,
        };
        assert_eq!(d.try_update(&big, 0), Err(HlcError::LogicalOverflow));
        assert_eq!(d.try_tick(MAX_LOGICAL + 1), Err(HlcError::LogicalOverflow));
    }

    #[test]
    fn monotone_across_arbitrary_local_schedule() {
        let mut c = Hlc::zero();
        let mut prev = Hlc::zero();
        for pt in [5u64, 5, 5, 3, 10, 10, 9, 100, 1] {
            let t = c.tick(pt);
            assert!(t > prev, "{t} !> {prev} at pt={pt}");
            prev = t;
        }
    }

    #[test]
    fn hlc_cmp_matches_ord() {
        let a = Hlc::from_parts(1, 2).unwrap();
        let b = Hlc::from_parts(1, 3).unwrap();
        assert_eq!(hlc_cmp(&a, &b), Ordering::Less);
        assert_eq!(hlc_cmp(&b, &a), Ordering::Greater);
        assert_eq!(hlc_cmp(&a, &a), Ordering::Equal);
    }
}
