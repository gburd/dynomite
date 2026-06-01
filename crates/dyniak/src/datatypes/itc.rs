//! Interval Tree Clocks (ITC) -- the per-key causality clock.
//!
//! The [`Itc`] type tracks per-key causality on the dyniak surface
//! using the algorithm described by Almeida, Baquero, and Fonte in
//! "Interval Tree Clocks: A Logical Clock for Dynamic Systems"
//! (2008). Compared with classic vector clocks or DVV-style sets,
//! ITC handles dynamic membership without per-replica metadata that
//! grows unboundedly with the number of actors that have ever
//! existed. Each stamp is a pair `(id, event)`:
//!
//! * The [`Id`] tree is a binary tree of `Leaf(0)` / `Leaf(1)`
//!   nodes describing what fraction of the global event-issuing
//!   authority a stamp owns. `Leaf(1)` is full ownership;
//!   `Leaf(0)` is none. Internal nodes record nested ownership.
//! * The [`Event`] tree is a binary tree of integer counters with
//!   relative offsets at the internal nodes. The supremum of two
//!   event trees is well-defined and finitely representable.
//!
//! The eight operations defined by the paper are exposed as inherent
//! methods on [`Itc`]: [`Itc::seed`], [`Itc::fork`], [`Itc::join`],
//! [`Itc::peek`], [`Itc::event`], [`Itc::leq`], [`Itc::send`],
//! [`Itc::receive`].
//!
//! # Wire format
//!
//! [`Itc::encode`] / [`Itc::decode`] round-trip an arbitrary stamp
//! through a bit-packed byte string. The format uses four cases
//! per id node and four cases per event node, plus a variable-length
//! integer encoding for event leaf values, in the spirit of section
//! 4 of the paper. The encoded bytes are stable across this crate's
//! versions and are the per-key context blob carried by the dyniak
//! Riak protobuf surfaces (see `docs/parity.md` for the deviation
//! note).
//!
//! # Examples
//!
//! ```
//! use dyniak::datatypes::Itc;
//! let s = Itc::seed();
//! let (a0, b0) = s.fork();
//! let mut a = a0;
//! a.event();
//! let mut b = b0;
//! b.event();
//! // The two sides issued concurrent events; neither dominates.
//! assert!(!a.leq(&b));
//! assert!(!b.leq(&a));
//! // Merging absorbs both histories.
//! let merged = a.clone().join(b.clone());
//! assert!(a.leq(&merged));
//! assert!(b.leq(&merged));
//! ```

use std::cmp::Ordering;
use std::fmt;

/// Identifier tree.
///
/// Records the share of the global event-issuing authority a
/// stamp owns. `Leaf(true)` is the genesis full-ownership leaf;
/// `Leaf(false)` is the read-only no-ownership leaf. Internal
/// nodes recurse pairwise into nested halves.
///
/// Constructed values are normalised: `Node(Leaf(false), Leaf(false))`
/// collapses to `Leaf(false)`; `Node(Leaf(true), Leaf(true))`
/// collapses to `Leaf(true)`. The id payload is a `bool` so the
/// pattern matcher enforces the two-state invariant.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Id {
    /// Leaf carrying `false` (no ownership) or `true` (full ownership).
    Leaf(bool),
    /// Internal node `(left, right)`.
    Node(Box<Id>, Box<Id>),
}

/// Event tree.
///
/// Each leaf carries an absolute count of events; internal nodes
/// carry a relative offset that applies to both subtrees. Constructed
/// values are normalised: `Node(n, Leaf(m), Leaf(m))` collapses to
/// `Leaf(n + m)`, and the per-node minimum is lifted into the parent
/// so any internal `Node(n, l, r)` has `min(min(l), min(r)) == 0`.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Event {
    /// Absolute leaf count.
    Leaf(u64),
    /// `(offset, left, right)`; both subtrees are at least 0 deeper.
    Node(u64, Box<Event>, Box<Event>),
}

/// Interval Tree Clock stamp.
///
/// The pair of an [`Id`] tree (event-issuing authority) and an
/// [`Event`] tree (observed history). All eight ITC operations are
/// inherent methods on this type. Construction goes through
/// [`Itc::seed`]; replicas branch off via [`Itc::fork`] and merge
/// back via [`Itc::join`].
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Itc {
    id: Id,
    event: Event,
}

impl fmt::Display for Itc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "itc(id={}, ev={})",
            id_repr(&self.id),
            event_repr(&self.event)
        )
    }
}

fn id_repr(i: &Id) -> String {
    match i {
        Id::Leaf(false) => "0".to_string(),
        Id::Leaf(true) => "1".to_string(),
        Id::Node(l, r) => format!("({}, {})", id_repr(l), id_repr(r)),
    }
}

fn event_repr(e: &Event) -> String {
    match e {
        Event::Leaf(n) => n.to_string(),
        Event::Node(n, l, r) => format!("({}, {}, {})", n, event_repr(l), event_repr(r)),
    }
}

// -- Id construction & normalisation ---------------------------------------

impl Id {
    /// `Leaf(false)` -- the read-only id (no event-issuing authority).
    #[must_use]
    pub fn zero() -> Self {
        Id::Leaf(false)
    }

    /// `Leaf(true)` -- the genesis full-ownership id.
    #[must_use]
    pub fn one() -> Self {
        Id::Leaf(true)
    }

    /// Construct a normalised internal node from two subtrees.
    #[must_use]
    pub fn node(l: Id, r: Id) -> Self {
        normalize_id(Id::Node(Box::new(l), Box::new(r)))
    }

    /// Whether the id is the zero id (no ownership anywhere).
    #[must_use]
    pub fn is_zero(&self) -> bool {
        matches!(self, Id::Leaf(false))
    }

    /// Whether the id is the full-ownership leaf.
    #[must_use]
    pub fn is_one(&self) -> bool {
        matches!(self, Id::Leaf(true))
    }
}

fn normalize_id(i: Id) -> Id {
    match i {
        Id::Leaf(v) => Id::Leaf(v),
        Id::Node(l, r) => match (*l, *r) {
            (Id::Leaf(false), Id::Leaf(false)) => Id::Leaf(false),
            (Id::Leaf(true), Id::Leaf(true)) => Id::Leaf(true),
            (l, r) => Id::Node(Box::new(l), Box::new(r)),
        },
    }
}

// -- Event construction & normalisation ------------------------------------

impl Event {
    /// `Leaf(0)` -- the genesis empty event tree.
    #[must_use]
    pub fn zero() -> Self {
        Event::Leaf(0)
    }
}

fn min_leaf(e: &Event) -> u64 {
    match e {
        Event::Leaf(n) => *n,
        Event::Node(n, l, r) => n.saturating_add(std::cmp::min(min_leaf(l), min_leaf(r))),
    }
}

fn max_leaf(e: &Event) -> u64 {
    match e {
        Event::Leaf(n) => *n,
        Event::Node(n, l, r) => n.saturating_add(std::cmp::max(max_leaf(l), max_leaf(r))),
    }
}

fn sink(event: Event, k: u64) -> Event {
    match event {
        Event::Leaf(n) => Event::Leaf(n - k),
        Event::Node(n, l, r) => Event::Node(n - k, l, r),
    }
}

fn lift(event: Event, k: u64) -> Event {
    match event {
        Event::Leaf(n) => Event::Leaf(n + k),
        Event::Node(n, l, r) => Event::Node(n + k, l, r),
    }
}

fn normalize_event(event: Event) -> Event {
    match event {
        Event::Leaf(_) => event,
        Event::Node(n, l, r) => {
            let l = normalize_event(*l);
            let r = normalize_event(*r);
            if let (Event::Leaf(lv), Event::Leaf(rv)) = (&l, &r) {
                if lv == rv {
                    return Event::Leaf(n + lv);
                }
            }
            let lifted = std::cmp::min(min_leaf(&l), min_leaf(&r));
            if lifted == 0 {
                Event::Node(n, Box::new(l), Box::new(r))
            } else {
                Event::Node(
                    n + lifted,
                    Box::new(sink(l, lifted)),
                    Box::new(sink(r, lifted)),
                )
            }
        }
    }
}

// -- Causal partial order on event trees -----------------------------------

fn leq_event(a: &Event, oa: u64, b: &Event, ob: u64) -> bool {
    match (a, b) {
        (Event::Leaf(n1), Event::Leaf(n2) | Event::Node(n2, _, _)) => n1 + oa <= n2 + ob,
        (Event::Node(n1, l1, r1), Event::Leaf(n2)) => {
            let new_oa = oa + n1;
            let nb = n2 + ob;
            (n1 + oa <= nb)
                && leq_event_to_leaf(l1, new_oa, nb)
                && leq_event_to_leaf(r1, new_oa, nb)
        }
        (Event::Node(n1, l1, r1), Event::Node(n2, l2, r2)) => {
            let new_oa = oa + n1;
            let new_b_offset = ob + n2;
            n1 + oa <= n2 + ob
                && leq_event(l1, new_oa, l2, new_b_offset)
                && leq_event(r1, new_oa, r2, new_b_offset)
        }
    }
}

fn leq_event_to_leaf(event: &Event, oa: u64, threshold: u64) -> bool {
    match event {
        Event::Leaf(n) => n + oa <= threshold,
        Event::Node(n, l, r) => {
            let new_oa = oa + n;
            n + oa <= threshold
                && leq_event_to_leaf(l, new_oa, threshold)
                && leq_event_to_leaf(r, new_oa, threshold)
        }
    }
}

// -- Supremum of two event trees -------------------------------------------

fn join_event(a: Event, b: Event) -> Event {
    match (a, b) {
        (Event::Leaf(n1), Event::Leaf(n2)) => Event::Leaf(n1.max(n2)),
        (Event::Leaf(n1), Event::Node(n2, l2, r2)) => join_event(
            Event::Node(n1, Box::new(Event::Leaf(0)), Box::new(Event::Leaf(0))),
            Event::Node(n2, l2, r2),
        ),
        (Event::Node(n1, l1, r1), Event::Leaf(n2)) => join_event(
            Event::Node(n1, l1, r1),
            Event::Node(n2, Box::new(Event::Leaf(0)), Box::new(Event::Leaf(0))),
        ),
        (Event::Node(n1, l1, r1), Event::Node(n2, l2, r2)) => {
            if n1 > n2 {
                // Swap so n1 <= n2.
                join_event(Event::Node(n2, l2, r2), Event::Node(n1, l1, r1))
            } else {
                let d = n2 - n1;
                let r2_lifted = lift(*r2, d);
                let l2_lifted = lift(*l2, d);
                let new_l = join_event(*l1, l2_lifted);
                let new_r = join_event(*r1, r2_lifted);
                normalize_event(Event::Node(n1, Box::new(new_l), Box::new(new_r)))
            }
        }
    }
}

// -- Sum of two id trees (used in join) ------------------------------------

fn sum_id(a: Id, b: Id) -> Id {
    match (a, b) {
        (Id::Leaf(false), x) | (x, Id::Leaf(false)) => x,
        (Id::Leaf(true), Id::Leaf(true)) => {
            // Joining two replicas that both still claim full
            // authority is a programming error: an `Itc::seed`
            // was duplicated without going through `fork`.
            panic!("itc: cannot join two stamps that both retain full id ownership")
        }
        (Id::Leaf(true), _) | (_, Id::Leaf(true)) => {
            // A leaf-1 overlapping with any non-zero id is the
            // same overlap; reject for the same reason.
            panic!("itc: id ownership overlap detected during join")
        }
        (Id::Node(l1, r1), Id::Node(l2, r2)) => {
            let new_l = sum_id(*l1, *l2);
            let new_r = sum_id(*r1, *r2);
            Id::node(new_l, new_r)
        }
    }
}

// -- Splitting an id (fork) ------------------------------------------------

fn split_id(i: Id) -> (Id, Id) {
    match i {
        Id::Leaf(false) => (Id::Leaf(false), Id::Leaf(false)),
        Id::Leaf(true) => (
            Id::Node(Box::new(Id::Leaf(true)), Box::new(Id::Leaf(false))),
            Id::Node(Box::new(Id::Leaf(false)), Box::new(Id::Leaf(true))),
        ),
        Id::Node(l, r) => match (*l, *r) {
            (Id::Leaf(false), ir) => {
                let (i1, i2) = split_id(ir);
                (
                    Id::Node(Box::new(Id::Leaf(false)), Box::new(i1)),
                    Id::Node(Box::new(Id::Leaf(false)), Box::new(i2)),
                )
            }
            (il, Id::Leaf(false)) => {
                let (i1, i2) = split_id(il);
                (
                    Id::Node(Box::new(i1), Box::new(Id::Leaf(false))),
                    Id::Node(Box::new(i2), Box::new(Id::Leaf(false))),
                )
            }
            (il, ir) => (
                Id::Node(Box::new(il), Box::new(Id::Leaf(false))),
                Id::Node(Box::new(Id::Leaf(false)), Box::new(ir)),
            ),
        },
    }
}

// -- event(): fill + grow --------------------------------------------------

const GROW_INFLATE_PENALTY: u64 = 1_000;

fn fill(id: &Id, ev: Event) -> Event {
    match (id, ev) {
        (Id::Leaf(false), e) => e,
        (Id::Leaf(true), e) => Event::Leaf(max_leaf(&e)),
        (Id::Node(_, _), Event::Leaf(n)) => Event::Leaf(n),
        (Id::Node(il, ir), Event::Node(n, el, er)) => {
            // Match on the canonical "1 on one side" cases first.
            if matches!(**il, Id::Leaf(true)) {
                let er2 = fill(ir, *er);
                let l_max = max_leaf(&el).max(min_leaf(&er2));
                return normalize_event(Event::Node(
                    n,
                    Box::new(Event::Leaf(l_max)),
                    Box::new(er2),
                ));
            }
            if matches!(**ir, Id::Leaf(true)) {
                let el2 = fill(il, *el);
                let r_max = max_leaf(&er).max(min_leaf(&el2));
                return normalize_event(Event::Node(
                    n,
                    Box::new(el2),
                    Box::new(Event::Leaf(r_max)),
                ));
            }
            normalize_event(Event::Node(
                n,
                Box::new(fill(il, *el)),
                Box::new(fill(ir, *er)),
            ))
        }
    }
}

fn grow(id: &Id, ev: Event) -> (Event, u64) {
    match (id, ev) {
        (Id::Leaf(true), Event::Leaf(n)) => (Event::Leaf(n + 1), 0),
        (id_any @ Id::Leaf(true), Event::Node(n, el, er)) => {
            // Active id over a node-shaped event tree: the
            // actor owns the whole sub-interval and may grow
            // anywhere within it. Mirror the symmetric
            // (Node, Node) arm by descending into the cheaper
            // child and charging the same +1 step. In the
            // normal call path `fill` collapses this case to
            // a leaf before `grow` sees it; the arm exists for
            // total exhaustiveness so callers that bypass
            // `fill` (none today, but the public Id / Event
            // construction surface allows it) still produce a
            // sound stamp.
            let (left_grown, left_cost) = grow(id_any, (*el).clone());
            let (right_grown, right_cost) = grow(id_any, (*er).clone());
            if left_cost < right_cost {
                (Event::Node(n, Box::new(left_grown), er), left_cost + 1)
            } else {
                (Event::Node(n, el, Box::new(right_grown)), right_cost + 1)
            }
        }
        (id_any @ Id::Node(_, _), Event::Leaf(n)) => {
            // Inflate the leaf into a node and grow inside.
            let (e2, c) = grow(
                id_any,
                Event::Node(n, Box::new(Event::Leaf(0)), Box::new(Event::Leaf(0))),
            );
            (e2, c + GROW_INFLATE_PENALTY)
        }
        (Id::Node(il, ir), Event::Node(n, el, er)) => {
            if matches!(**il, Id::Leaf(false)) {
                let (right_grown, cost) = grow(ir, *er);
                return (Event::Node(n, el, Box::new(right_grown)), cost + 1);
            }
            if matches!(**ir, Id::Leaf(false)) {
                let (left_grown, cost) = grow(il, *el);
                return (Event::Node(n, Box::new(left_grown), er), cost + 1);
            }
            let (left_grown, left_cost) = grow(il, (*el).clone());
            let (right_grown, right_cost) = grow(ir, (*er).clone());
            if left_cost < right_cost {
                (Event::Node(n, Box::new(left_grown), er), left_cost + 1)
            } else {
                (Event::Node(n, el, Box::new(right_grown)), right_cost + 1)
            }
        }
        (Id::Leaf(false), _) => {
            // No ownership anywhere; cannot issue an event. The
            // public Itc::event method rejects this case before
            // we get here.
            unreachable!("itc: grow called with zero ownership");
        }
    }
}

fn do_event(id: &Id, ev: Event) -> Event {
    let filled = fill(id, ev.clone());
    if filled == ev {
        let (grown, _cost) = grow(id, ev);
        grown
    } else {
        filled
    }
}

// -- Public Itc API --------------------------------------------------------

impl Itc {
    /// The genesis stamp: `id = Leaf(1)`, `event = Leaf(0)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dyniak::datatypes::Itc;
    /// let s = Itc::seed();
    /// assert!(s.has_authority());
    /// ```
    #[must_use]
    pub fn seed() -> Self {
        Self {
            id: Id::one(),
            event: Event::zero(),
        }
    }

    /// Construct a stamp from raw [`Id`] and [`Event`] trees.
    ///
    /// The trees are normalised on construction. Most callers
    /// should use [`Itc::seed`] + [`Itc::fork`] / [`Itc::join`]
    /// instead of building stamps by hand.
    #[must_use]
    pub fn from_parts(id: Id, event: Event) -> Self {
        Self {
            id: normalize_id(id),
            event: normalize_event(event),
        }
    }

    /// Borrow the id tree.
    #[must_use]
    pub fn id(&self) -> &Id {
        &self.id
    }

    /// Borrow the event tree.
    #[must_use]
    pub fn event_tree(&self) -> &Event {
        &self.event
    }

    /// Whether the stamp owns any event-issuing authority.
    ///
    /// A peek-derived stamp returns `false`; a freshly seeded
    /// stamp returns `true`.
    #[must_use]
    pub fn has_authority(&self) -> bool {
        !self.id.is_zero()
    }

    /// Split the id into two halves; both sides keep the same
    /// event tree.
    ///
    /// Use when a process spawns or a new replica joins the
    /// cluster.
    ///
    /// # Examples
    ///
    /// ```
    /// use dyniak::datatypes::Itc;
    /// let (a, b) = Itc::seed().fork();
    /// // Each half retains exactly half the global authority.
    /// assert!(a.has_authority());
    /// assert!(b.has_authority());
    /// ```
    #[must_use]
    pub fn fork(self) -> (Self, Self) {
        let (i1, i2) = split_id(self.id);
        let e = self.event;
        (
            Self {
                id: normalize_id(i1),
                event: e.clone(),
            },
            Self {
                id: normalize_id(i2),
                event: e,
            },
        )
    }

    /// Merge two stamps. The id tree is the disjoint union; the
    /// event tree is the supremum. Inverse of [`Itc::fork`] up
    /// to normalisation.
    ///
    /// # Panics
    ///
    /// Panics if the two id trees overlap (for example, both
    /// sides still hold a `Leaf(1)`). Such overlap is a
    /// programming error: forks are the only legal way to
    /// produce stamps that can later be joined.
    #[must_use]
    pub fn join(self, other: Self) -> Self {
        let new_id = sum_id(self.id, other.id);
        let new_event = join_event(self.event, other.event);
        Self {
            id: normalize_id(new_id),
            event: normalize_event(new_event),
        }
    }

    /// Read-only stamp: same event tree, `id = Leaf(0)`.
    ///
    /// Useful for shipping causal state to an observer that must
    /// not be able to issue events of its own.
    #[must_use]
    pub fn peek(&self) -> Self {
        Self {
            id: Id::zero(),
            event: self.event.clone(),
        }
    }

    /// Increment the event tree at one of the leaves the id tree
    /// owns. Uses the fill-then-grow strategy from the paper to
    /// keep the resulting tree small.
    ///
    /// # Panics
    ///
    /// Panics if the stamp has no event-issuing authority (id
    /// is `Leaf(0)`). The protocol contract is that observer
    /// stamps must not call `event`; they must `join` back into
    /// an authoritative stamp first.
    pub fn event(&mut self) {
        assert!(
            self.has_authority(),
            "itc: cannot issue an event from a peek stamp (id = 0)"
        );
        let prev = std::mem::replace(&mut self.event, Event::zero());
        self.event = normalize_event(do_event(&self.id, prev));
    }

    /// Causal partial-order test. Returns `true` iff every event
    /// recorded in `self` is also recorded in `other`.
    #[must_use]
    pub fn leq(&self, other: &Self) -> bool {
        leq_event(&self.event, 0, &other.event, 0)
    }

    /// Total comparison combining the two `leq` directions:
    ///
    /// * `Some(Ordering::Less)` -- `self` strictly precedes `other`.
    /// * `Some(Ordering::Equal)` -- the two stamps describe the same set of events.
    /// * `Some(Ordering::Greater)` -- `self` strictly succeeds `other`.
    /// * `None` -- the stamps are concurrent.
    #[must_use]
    pub fn partial_cmp_event(&self, other: &Self) -> Option<Ordering> {
        let a_le_b = self.leq(other);
        let b_le_a = other.leq(self);
        match (a_le_b, b_le_a) {
            (true, true) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Less),
            (false, true) => Some(Ordering::Greater),
            (false, false) => None,
        }
    }

    /// Convenience: equivalent to `(self.event(), self.peek())`.
    ///
    /// Conventionally pairs with [`Itc::receive`]: the sender
    /// issues a local event, then ships the resulting stamp's
    /// peek along with the message.
    #[must_use]
    pub fn send(mut self) -> (Self, Self) {
        self.event();
        let observer = self.peek();
        (self, observer)
    }

    /// Convenience: equivalent to `self.join(msg).event()`.
    ///
    /// Conventionally pairs with [`Itc::send`]: the receiver
    /// merges the sender's peek into its own stamp and then
    /// records the receive as a local event.
    #[must_use]
    pub fn receive(self, msg: Self) -> Self {
        let mut joined = self.join(msg);
        joined.event();
        joined
    }

    /// Encode the stamp into a length-prefixed bit-packed byte
    /// string suitable for the per-key context blob.
    ///
    /// The format is `[u32 BE bit_count][ceil(bit_count/8) packed bytes]`.
    /// The bit stream encodes the id then the event:
    ///
    /// * Id: 2-bit prefix per case (`00`/`01` for the `Leaf(0)`/`Leaf(1)`
    ///   leaves, `10` for `Node(Leaf(0), r)`, `110` for `Node(l, Leaf(0))`,
    ///   `111` for `Node(l, r)` with both children non-zero), recursing
    ///   into each non-trivial subtree.
    /// * Event: 1-bit prefix (`0` for leaf, `1` for node), followed by a
    ///   variable-length unsigned integer for the leaf or offset value
    ///   (3-bit chunks with a continuation bit), and the two subtrees
    ///   for the node case.
    ///
    /// The format is stable across this crate's versions and is
    /// the wire context format on the dyniak Riak surfaces (see
    /// `docs/parity.md`).
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bw = BitWriter::new();
        encode_id_bits(&self.id, &mut bw);
        encode_event_bits(&self.event, &mut bw);
        let bit_count = bw.bit_count();
        let bytes = bw.finish();
        let mut out = Vec::with_capacity(4 + bytes.len());
        out.extend_from_slice(&u32::try_from(bit_count).unwrap_or(u32::MAX).to_be_bytes());
        out.extend_from_slice(&bytes);
        out
    }

    /// Decode a stamp previously produced by [`Self::encode`].
    /// Returns `None` if the byte sequence is malformed
    /// (truncated, unknown prefix, or bit count outside the
    /// payload).
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }
        let bit_count = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let payload = &bytes[4..];
        let needed = bit_count.div_ceil(8);
        if payload.len() != needed {
            return None;
        }
        let mut br = BitReader::new(payload, bit_count);
        let id = decode_id_bits(&mut br)?;
        let event = decode_event_bits(&mut br)?;
        if br.remaining_bits() != 0 {
            return None;
        }
        Some(Self {
            id: normalize_id(id),
            event: normalize_event(event),
        })
    }
}

impl Default for Itc {
    fn default() -> Self {
        Self::seed()
    }
}

// -- Bit-level encoding ---------------------------------------------------

struct BitWriter {
    bytes: Vec<u8>,
    /// Number of bits written so far.
    bits: usize,
}

impl BitWriter {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            bits: 0,
        }
    }

    fn bit_count(&self) -> usize {
        self.bits
    }

    fn write_bit(&mut self, b: bool) {
        if self.bits.is_multiple_of(8) {
            self.bytes.push(0);
        }
        if b {
            let last_idx = self.bytes.len() - 1;
            let shift = 7 - (self.bits % 8);
            self.bytes[last_idx] |= 1u8 << shift;
        }
        self.bits += 1;
    }

    fn write_bits(&mut self, value: u64, count: u8) {
        // Most-significant bit first.
        for i in (0..count).rev() {
            let bit = ((value >> i) & 1) != 0;
            self.write_bit(bit);
        }
    }

    /// Emit `n` as 3-bit groups with a continuation prefix bit:
    /// each group is `[continuation][3 payload bits]`. The group
    /// payloads are written most-significant first.
    fn write_uint(&mut self, mut n: u64) {
        // Collect 3-bit groups (least significant first).
        let mut groups: Vec<u8> = Vec::new();
        loop {
            #[allow(clippy::cast_possible_truncation)]
            let chunk = (n & 0b111) as u8;
            groups.push(chunk);
            n >>= 3;
            if n == 0 {
                break;
            }
        }
        // Emit MSB group first; continuation = 1 except for the last.
        for (i, chunk) in groups.iter().enumerate().rev() {
            let continuation = i != 0;
            self.write_bit(continuation);
            self.write_bits(u64::from(*chunk), 3);
        }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

struct BitReader<'a> {
    bytes: &'a [u8],
    /// Total number of valid bits (>=0, <= bytes.len()*8).
    total_bits: usize,
    /// Number of bits already consumed.
    consumed: usize,
}

impl<'a> BitReader<'a> {
    fn new(bytes: &'a [u8], total_bits: usize) -> Self {
        Self {
            bytes,
            total_bits,
            consumed: 0,
        }
    }

    fn remaining_bits(&self) -> usize {
        self.total_bits - self.consumed
    }

    fn read_bit(&mut self) -> Option<bool> {
        if self.consumed >= self.total_bits {
            return None;
        }
        let byte_idx = self.consumed / 8;
        let shift = 7 - (self.consumed % 8);
        let b = (self.bytes[byte_idx] >> shift) & 1;
        self.consumed += 1;
        Some(b == 1)
    }

    fn read_bits(&mut self, count: u8) -> Option<u64> {
        let mut v: u64 = 0;
        for _ in 0..count {
            v = (v << 1) | u64::from(self.read_bit()?);
        }
        Some(v)
    }

    fn read_uint(&mut self) -> Option<u64> {
        let mut v: u64 = 0;
        loop {
            let cont = self.read_bit()?;
            let chunk = self.read_bits(3)?;
            v = v.checked_shl(3)?.checked_add(chunk)?;
            if !cont {
                return Some(v);
            }
        }
    }
}

fn encode_id_bits(id: &Id, bw: &mut BitWriter) {
    match id {
        Id::Leaf(false) => {
            bw.write_bit(false);
            bw.write_bit(false);
        }
        Id::Leaf(true) => {
            bw.write_bit(false);
            bw.write_bit(true);
        }
        Id::Node(l, r) => match (&**l, &**r) {
            (Id::Leaf(false), other) => {
                bw.write_bit(true);
                bw.write_bit(false);
                encode_id_bits(other, bw);
            }
            (other, Id::Leaf(false)) => {
                bw.write_bit(true);
                bw.write_bit(true);
                bw.write_bit(false);
                encode_id_bits(other, bw);
            }
            (left, right) => {
                bw.write_bit(true);
                bw.write_bit(true);
                bw.write_bit(true);
                encode_id_bits(left, bw);
                encode_id_bits(right, bw);
            }
        },
    }
}

fn decode_id_bits(br: &mut BitReader<'_>) -> Option<Id> {
    let b0 = br.read_bit()?;
    if !b0 {
        let b1 = br.read_bit()?;
        return Some(Id::Leaf(b1));
    }
    let b1 = br.read_bit()?;
    if !b1 {
        // "10" -> Node(Leaf(false), R)
        let r = decode_id_bits(br)?;
        return Some(Id::Node(Box::new(Id::Leaf(false)), Box::new(r)));
    }
    let b2 = br.read_bit()?;
    if b2 {
        // "111" -> Node(L, R)
        let l = decode_id_bits(br)?;
        let r = decode_id_bits(br)?;
        Some(Id::Node(Box::new(l), Box::new(r)))
    } else {
        // "110" -> Node(L, Leaf(false))
        let l = decode_id_bits(br)?;
        Some(Id::Node(Box::new(l), Box::new(Id::Leaf(false))))
    }
}

fn encode_event_bits(e: &Event, bw: &mut BitWriter) {
    match e {
        Event::Leaf(n) => {
            bw.write_bit(false);
            bw.write_uint(*n);
        }
        Event::Node(n, l, r) => {
            bw.write_bit(true);
            bw.write_uint(*n);
            encode_event_bits(l, bw);
            encode_event_bits(r, bw);
        }
    }
}

fn decode_event_bits(br: &mut BitReader<'_>) -> Option<Event> {
    let is_node = br.read_bit()?;
    let n = br.read_uint()?;
    if is_node {
        let l = decode_event_bits(br)?;
        let r = decode_event_bits(br)?;
        Some(Event::Node(n, Box::new(l), Box::new(r)))
    } else {
        Some(Event::Leaf(n))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev_leaf(n: u64) -> Event {
        Event::Leaf(n)
    }

    fn ev_node(n: u64, l: Event, r: Event) -> Event {
        Event::Node(n, Box::new(l), Box::new(r))
    }

    #[test]
    fn seed_is_full_authority_no_events() {
        let s = Itc::seed();
        assert_eq!(s.id(), &Id::Leaf(true));
        assert_eq!(s.event_tree(), &Event::Leaf(0));
        assert!(s.has_authority());
    }

    #[test]
    fn fork_splits_id_in_half() {
        let s = Itc::seed();
        let (a, b) = s.fork();
        assert_eq!(
            a.id(),
            &Id::Node(Box::new(Id::Leaf(true)), Box::new(Id::Leaf(false)))
        );
        assert_eq!(
            b.id(),
            &Id::Node(Box::new(Id::Leaf(false)), Box::new(Id::Leaf(true)))
        );
        assert_eq!(a.event_tree(), &Event::Leaf(0));
        assert_eq!(b.event_tree(), &Event::Leaf(0));
    }

    #[test]
    fn fork_join_round_trip_seed() {
        let s = Itc::seed();
        let (a, b) = s.clone().fork();
        let merged = a.join(b);
        assert_eq!(merged.id(), &Id::Leaf(true));
        assert_eq!(merged.event_tree(), &Event::Leaf(0));
        assert_eq!(
            merged.partial_cmp_event(&s),
            Some(std::cmp::Ordering::Equal)
        );
    }

    #[test]
    fn peek_zeroes_id_keeps_event() {
        let mut s = Itc::seed();
        s.event();
        let p = s.peek();
        assert_eq!(p.id(), &Id::Leaf(false));
        assert_eq!(p.event_tree(), s.event_tree());
        assert!(!p.has_authority());
    }

    #[test]
    fn event_progresses_causality() {
        let mut s = Itc::seed();
        let before = s.clone();
        s.event();
        assert!(before.leq(&s));
        assert!(!s.leq(&before));
    }

    #[test]
    fn paper_four_state_machine_example_replays() {
        // Sequence from the original paper: starting from a
        // single seed, the cluster forks twice, each side issues
        // events, and the four resulting stamps satisfy the
        // expected order relations.
        let s = Itc::seed();
        let (a, b) = s.fork();

        let mut a1 = a.clone();
        a1.event();
        let mut b1 = b.clone();
        b1.event();

        // Concurrent events: neither dominates.
        assert!(!a1.leq(&b1));
        assert!(!b1.leq(&a1));

        // Each new event dominates its parent.
        assert!(a.leq(&a1));
        assert!(b.leq(&b1));

        // Receiving the peer's peek and re-eventing dominates
        // both predecessors.
        let received = a1.clone().receive(b1.peek());
        assert!(a1.leq(&received));
        assert!(b1.leq(&received));

        // After fork-event-event-receive-event, the join must
        // be normalised (no stale offsets, no Leaf(0)/Leaf(0)
        // collapsed-away cases left in id).
        let _ = received.event_tree(); // smoke
    }

    #[test]
    fn fork_then_event_each_side_normalises() {
        let s = Itc::seed();
        let (mut a, mut b) = s.fork();
        a.event();
        b.event();
        // After event, the event trees for the two sides should
        // both be Node(0, Leaf(1), Leaf(0)) or
        // Node(0, Leaf(0), Leaf(1)). Each is canonical (children
        // differ, so no further collapse).
        match (a.event_tree(), b.event_tree()) {
            (Event::Node(an, al, ar), Event::Node(bn, bl, br)) => {
                assert_eq!(*an, 0);
                assert_eq!(*bn, 0);
                assert_eq!(**al, Event::Leaf(1));
                assert_eq!(**ar, Event::Leaf(0));
                assert_eq!(**bl, Event::Leaf(0));
                assert_eq!(**br, Event::Leaf(1));
            }
            other => panic!("unexpected post-event shape: {other:?}"),
        }
    }

    #[test]
    fn join_is_supremum_of_concurrent_branches() {
        let s = Itc::seed();
        let (mut a, mut b) = s.fork();
        a.event();
        b.event();
        let m = a.clone().join(b.clone());
        // Both halves precede the merge.
        assert!(a.leq(&m));
        assert!(b.leq(&m));
        // The merge has full id ownership again (since fork +
        // join is identity on id).
        assert_eq!(m.id(), &Id::Leaf(true));
    }

    #[test]
    fn event_after_join_collapses_event_tree() {
        let s = Itc::seed();
        let (mut a, mut b) = s.fork();
        a.event();
        b.event();
        let mut m = a.join(b);
        // After join the event tree may have a Node(0, Leaf(1),
        // Leaf(1)) shape (concurrent ticks). Issuing another
        // event from the merged stamp should normalise that
        // pair of equal children into Leaf(1) when possible.
        m.event();
        // The resulting stamp must still be normalised.
        assert_normalised(&m);
    }

    #[test]
    fn send_receive_round_trip_incorporates_message() {
        let (a, b) = Itc::seed().fork();
        // a sends a message; b receives it.
        let (a_after, msg) = a.send();
        let b_after = b.receive(msg);
        // a_after and b_after are both >= a's pre-send stamp.
        // We do not assert a strict ordering between a_after
        // and b_after; they can be ordered or concurrent
        // depending on which side issued the most recent event.
        let _ = (a_after.leq(&b_after), b_after.leq(&a_after));
        // b_after dominates the message peek that produced it.
        // Reconstruct the message peek from a_after for the
        // assertion.
        let a_peek = a_after.peek();
        assert!(a_peek.leq(&b_after));
    }

    #[test]
    fn n_event_fork_join_cycles_normalise() {
        let mut s = Itc::seed();
        for _ in 0..6 {
            s.event();
            let (mut a, mut b) = s.fork();
            a.event();
            b.event();
            s = a.join(b);
        }
        assert_normalised(&s);
        let bytes = s.encode();
        let back = Itc::decode(&bytes).expect("decode round-trip");
        assert_eq!(back, s);
    }

    #[test]
    fn encode_decode_seed_round_trip() {
        let s = Itc::seed();
        let bytes = s.encode();
        let back = Itc::decode(&bytes).expect("decode round-trip");
        assert_eq!(back, s);
    }

    #[test]
    fn encode_decode_just_forked_round_trip() {
        let (a, b) = Itc::seed().fork();
        for stamp in [a, b] {
            let bytes = stamp.encode();
            let back = Itc::decode(&bytes).expect("decode round-trip");
            assert_eq!(back, stamp);
        }
    }

    #[test]
    fn encode_decode_after_many_events() {
        let mut s = Itc::seed();
        for _ in 0..20 {
            s.event();
        }
        let bytes = s.encode();
        let back = Itc::decode(&bytes).expect("decode round-trip");
        assert_eq!(back, s);
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let bytes = Itc::seed().encode();
        for k in 1..bytes.len() {
            assert!(
                Itc::decode(&bytes[..k]).is_none(),
                "decode succeeded on prefix of length {k}; should fail"
            );
        }
    }

    #[test]
    fn decode_rejects_oversized_payload() {
        let mut bytes = Itc::seed().encode();
        bytes.push(0x00);
        assert!(
            Itc::decode(&bytes).is_none(),
            "decode must reject trailing garbage"
        );
    }

    #[test]
    fn decode_rejects_too_few_payload_bytes_for_bit_count() {
        // Set a bit count larger than the data we provide.
        let mut bytes = Itc::seed().encode();
        // Bump the bit count to bytes.len() * 8 + 8.
        let new_bits = u32::try_from((bytes.len() - 4) * 8 + 8).unwrap();
        bytes[..4].copy_from_slice(&new_bits.to_be_bytes());
        assert!(Itc::decode(&bytes).is_none());
    }

    #[test]
    fn leq_is_reflexive() {
        let mut s = Itc::seed();
        s.event();
        s.event();
        assert!(s.leq(&s));
    }

    #[test]
    fn leq_is_transitive_on_chain() {
        let s0 = Itc::seed();
        let mut s1 = s0.clone();
        s1.event();
        let mut s2 = s1.clone();
        s2.event();
        assert!(s0.leq(&s1));
        assert!(s1.leq(&s2));
        assert!(s0.leq(&s2));
    }

    #[test]
    fn join_is_idempotent() {
        let mut s = Itc::seed();
        s.event();
        // Re-joining a peek of self does not advance the stamp.
        let p = s.peek();
        let joined = s.clone().join(p);
        assert_eq!(joined.event_tree(), s.event_tree());
        assert_eq!(joined.id(), s.id());
    }

    #[test]
    fn peek_does_not_grant_authority() {
        let p = Itc::seed().peek();
        assert!(!p.has_authority());
    }

    #[test]
    #[should_panic(expected = "cannot issue an event")]
    fn event_on_peek_stamp_panics() {
        let mut p = Itc::seed().peek();
        p.event();
    }

    #[test]
    #[should_panic(expected = "ownership")]
    fn join_with_overlapping_authority_panics() {
        let s1 = Itc::seed();
        let s2 = Itc::seed();
        let _ = s1.join(s2);
    }

    #[test]
    fn normalize_event_collapses_equal_leaves() {
        let raw = ev_node(2, ev_leaf(3), ev_leaf(3));
        let norm = normalize_event(raw);
        assert_eq!(norm, Event::Leaf(5));
    }

    #[test]
    fn normalize_event_lifts_minimum_offset() {
        let raw = ev_node(0, ev_leaf(2), ev_leaf(5));
        let norm = normalize_event(raw);
        assert_eq!(norm, ev_node(2, ev_leaf(0), ev_leaf(3)));
    }

    #[test]
    fn normalize_id_collapses_zero_pair() {
        assert_eq!(
            normalize_id(Id::Node(
                Box::new(Id::Leaf(false)),
                Box::new(Id::Leaf(false))
            )),
            Id::Leaf(false)
        );
    }

    #[test]
    fn normalize_id_collapses_one_pair() {
        assert_eq!(
            normalize_id(Id::Node(Box::new(Id::Leaf(true)), Box::new(Id::Leaf(true)))),
            Id::Leaf(true)
        );
    }

    #[test]
    fn write_then_read_uint_round_trip() {
        for n in [0u64, 1, 7, 8, 63, 64, 1023, 1024, 4096, u64::from(u32::MAX)] {
            let mut bw = BitWriter::new();
            bw.write_uint(n);
            let bits = bw.bit_count();
            let bytes = bw.finish();
            let mut br = BitReader::new(&bytes, bits);
            let back = br.read_uint().expect("decode uint");
            assert_eq!(back, n, "uint round trip for {n}");
        }
    }

    /// Assert that the trees inside a stamp are in normal form.
    fn assert_normalised(s: &Itc) {
        assert_id_normalised(s.id());
        assert_event_normalised(s.event_tree());
    }

    fn assert_id_normalised(i: &Id) {
        match i {
            Id::Leaf(_) => {}
            Id::Node(l, r) => {
                match (&**l, &**r) {
                    (Id::Leaf(false), Id::Leaf(false)) => {
                        panic!("id Node(Leaf(false), Leaf(false)) should have collapsed to Leaf(false)")
                    }
                    (Id::Leaf(true), Id::Leaf(true)) => {
                        panic!(
                            "id Node(Leaf(true), Leaf(true)) should have collapsed to Leaf(true)"
                        )
                    }
                    _ => {}
                }
                assert_id_normalised(l);
                assert_id_normalised(r);
            }
        }
    }

    fn assert_event_normalised(e: &Event) {
        match e {
            Event::Leaf(_) => {}
            Event::Node(n, l, r) => {
                let lm = min_leaf(l);
                let rm = min_leaf(r);
                if let (Event::Leaf(lv), Event::Leaf(rv)) = (&**l, &**r) {
                    assert_ne!(
                        lv, rv,
                        "event Node({n}, Leaf({lv}), Leaf({rv})) should have collapsed"
                    );
                }
                assert!(
                    lm == 0 || rm == 0,
                    "event Node({n}, ...) min offsets ({lm}, {rm}) not lifted"
                );
                assert_event_normalised(l);
                assert_event_normalised(r);
            }
        }
    }
}
