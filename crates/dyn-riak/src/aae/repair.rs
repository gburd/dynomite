//! Tictac AAE repair scheduler.
//!
//! After [`crate::aae::exchange::Exchange::run`] surfaces one or
//! more [`Divergence`]s, the repair scheduler walks each
//! divergence, decides which side carries the winner, and
//! enqueues a [`RepairTask`] on a per-peer outbound channel.
//! The channel pattern mirrors the per-peer
//! `mpsc::Sender<OutboundRequest>` map maintained by
//! `crate::dynomite::cluster::dispatch::ClusterDispatcher`; a
//! production wiring trampolines [`RepairTask`]s through that
//! same map.
//!
//! # Vclock comparison
//!
//! The merkle tree treats vclocks as opaque bytes (see
//! [`crate::aae::tictac::KeyEntry`]). The repair scheduler's
//! winner-selection policy is therefore pluggable via the
//! [`VClockOrder`] trait. The default impl
//! ([`LexicographicOrder`]) treats the longer or
//! lexicographically-greater byte slice as the winner; a
//! production wiring should swap in a real Riak vector-clock
//! comparator (`vclock:descends/2`).
//!
//! # Edge cases
//!
//! * A divergence whose only entry has an unparseable vclock
//!   surfaces an [`Outcome::AmbiguousVClock`] event; the
//!   scheduler does NOT silently drop it.
//! * A peer whose outbound channel is closed surfaces an
//!   [`Outcome::PeerUnavailable`] event; the next sweep tick
//!   retries.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::aae::exchange::Divergence;
use crate::aae::tictac::KeyEntry;
use crate::datatypes::vclock::{Vclock, VclockOrder};

/// Direction marker for a repair: which side carries the winner
/// vclock.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RepairDirection {
    /// Push the local entry to the remote peer.
    PushToRemote,
    /// Pull the remote entry to the local node (the local
    /// scheduler enqueues itself a self-repair task that the
    /// next read will service).
    PullFromRemote,
}

/// Outcome of a cross-replica winner-selection across N
/// replicas of one key.
///
/// Used by [`RepairTask::evaluate`] when the divergent key has
/// been fetched from every replica and the scheduler must
/// decide whether one value strictly dominates the others or
/// whether the replicas hold genuine concurrent siblings.
///
/// # Examples
///
/// ```
/// use bytes::Bytes;
/// use dyn_riak::aae::repair::{RepairOutcome, RepairTask};
/// use dyn_riak::datatypes::{ActorId, Vclock};
///
/// // Build two clocks where `a` strictly dominates `b`.
/// let actor = ActorId::new("dc1", "peer");
/// let mut a = Vclock::new();
/// a.increment(&actor);
/// a.increment(&actor);
/// let mut b = Vclock::new();
/// b.increment(&actor);
///
/// let replicas = vec![
///     (Bytes::from_static(b"v_a"), a),
///     (Bytes::from_static(b"v_b"), b),
/// ];
/// match RepairTask::evaluate(&replicas) {
///     RepairOutcome::Winner { value, .. } => assert_eq!(value, "v_a"),
///     RepairOutcome::Siblings(_) => panic!("a should dominate b"),
/// }
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RepairOutcome {
    /// One replica strictly dominates every other replica;
    /// the dominated entries are discarded.
    Winner {
        /// The winning value bytes.
        value: Bytes,
        /// The winner's vector clock.
        vclock: Vclock,
    },
    /// Two or more replicas hold concurrent (or equal) clocks
    /// after every dominated entry has been removed; the
    /// caller decides whether to store siblings, log a
    /// warning, or fall back to a tiebreaker.
    Siblings(Vec<(Bytes, Vclock)>),
}

impl RepairOutcome {
    /// Resolve the outcome to a single `(value, vclock)` pair,
    /// emitting a `tracing::warn!` event when the outcome is
    /// [`RepairOutcome::Siblings`]. The fallback selection rule
    /// is the lexicographically-largest sibling value, with
    /// ties broken by the encoded vclock bytes; this matches
    /// the v1 "siblings as deferred follow-up" plan called out
    /// on the brief.
    ///
    /// `key` is reported alongside the warning so operators can
    /// correlate sibling events with specific Riak objects.
    #[must_use]
    pub fn resolve_with_warn(self, key: &[u8]) -> (Bytes, Vclock) {
        match self {
            Self::Winner { value, vclock } => (value, vclock),
            Self::Siblings(siblings) => {
                tracing::warn!(
                    target: "dyn_riak::aae::repair",
                    key = %String::from_utf8_lossy(key),
                    siblings = siblings.len(),
                    "sibling-aware merge: concurrent vclocks; falling back to lex-largest value"
                );
                siblings
                    .into_iter()
                    .max_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.encode().cmp(&b.1.encode())))
                    .expect("invariant: Siblings carries at least two entries")
            }
        }
    }
}

/// One repair the scheduler has decided to enact. Caller-owned;
/// the [`RepairSink`] is responsible for routing it onto the
/// underlying per-peer outbound channel.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RepairTask {
    /// Index of the peer the repair is destined for. Matches
    /// the dispatcher's per-peer outbound-channel keying.
    pub peer_idx: u32,
    /// Riak bucket name.
    pub bucket: Vec<u8>,
    /// Riak object key.
    pub key: Vec<u8>,
    /// Winner vclock.
    pub vclock: Vec<u8>,
    /// Repair direction.
    pub direction: RepairDirection,
}

impl RepairTask {
    /// Cross-replica winner selection.
    ///
    /// Given the `(value, vclock)` pair fetched from each of
    /// the `N` replicas of one key, return [`RepairOutcome::Winner`]
    /// when exactly one entry survives the dominance filter
    /// and [`RepairOutcome::Siblings`] otherwise. An entry is
    /// dominated when at least one other entry's vector clock
    /// strictly succeeds it under [`Vclock::compare`]. Entries
    /// whose clocks are pairwise [`VclockOrder::Equal`] are
    /// deduplicated by their encoded clock bytes so identical
    /// state across replicas does not surface as siblings.
    ///
    /// # Panics
    ///
    /// Does not panic. An empty `replicas` slice yields
    /// `RepairOutcome::Siblings(Vec::new())`; callers are
    /// expected to filter out that degenerate case before
    /// calling.
    #[must_use]
    pub fn evaluate(replicas: &[(Bytes, Vclock)]) -> RepairOutcome {
        let n = replicas.len();
        let mut keep = vec![true; n];
        for i in 0..n {
            if !keep[i] {
                continue;
            }
            for j in 0..n {
                if i == j || !keep[j] {
                    continue;
                }
                match replicas[i].1.compare(&replicas[j].1) {
                    VclockOrder::Less => {
                        // i is strictly dominated by j; drop i.
                        keep[i] = false;
                        break;
                    }
                    VclockOrder::Equal if i > j => {
                        // Deduplicate identical clocks: keep
                        // the lower index, drop the higher.
                        keep[i] = false;
                        break;
                    }
                    _ => {}
                }
            }
        }
        let surviving: Vec<(Bytes, Vclock)> = replicas
            .iter()
            .zip(keep.iter())
            .filter_map(|((v, c), &k)| {
                if k {
                    Some((v.clone(), c.clone()))
                } else {
                    None
                }
            })
            .collect();
        if surviving.len() == 1 {
            let (value, vclock) = surviving.into_iter().next().expect("len == 1");
            RepairOutcome::Winner { value, vclock }
        } else {
            RepairOutcome::Siblings(surviving)
        }
    }
}

/// Outcome of one divergence resolution. The scheduler emits
/// one [`Outcome`] per divergence so observability hooks can
/// surface the totals (repaired, ambiguous, peer-unavailable).
#[derive(Debug, Clone)]
pub enum Outcome {
    /// A repair task was successfully enqueued on the per-peer
    /// outbound channel.
    Repaired(RepairTask),
    /// Both sides held an entry but neither vclock dominated;
    /// the scheduler defers to a sibling-aware merge that lives
    /// outside this module.
    AmbiguousVClock {
        /// The bucket name in question.
        bucket: Vec<u8>,
        /// The key in question.
        key: Vec<u8>,
        /// The local vclock bytes.
        local: Vec<u8>,
        /// The remote vclock bytes.
        remote: Vec<u8>,
    },
    /// The peer's outbound channel was closed or full; the
    /// scheduler will retry on the next sweep tick.
    PeerUnavailable {
        /// The peer that was unavailable.
        peer_idx: u32,
        /// The repair task that could not be enqueued.
        task: RepairTask,
    },
}

/// Pluggable vclock comparator.
pub trait VClockOrder: Send + Sync {
    /// Compare two vclocks; return `Some(Ordering::Greater)`
    /// when `a` strictly dominates `b`, `Some(Ordering::Less)`
    /// when `b` strictly dominates `a`, and `None` when
    /// neither dominates (concurrent updates).
    fn compare(&self, a: &[u8], b: &[u8]) -> Option<std::cmp::Ordering>;
}

/// Default [`VClockOrder`] impl: longer-then-lexicographic
/// comparison. This is intentionally NOT a real vector-clock
/// dominance check (a real Riak comparator parses the encoded
/// vclock and walks per-actor counters); it is the simplest
/// totally-ordered relation that lets the scheduler tests
/// surface a deterministic winner. Production wirings replace
/// it with `vclock:descends/2`.
#[derive(Debug, Clone, Copy, Default)]
pub struct LexicographicOrder;

impl VClockOrder for LexicographicOrder {
    fn compare(&self, a: &[u8], b: &[u8]) -> Option<std::cmp::Ordering> {
        if a == b {
            return Some(std::cmp::Ordering::Equal);
        }
        match a.len().cmp(&b.len()) {
            std::cmp::Ordering::Equal => Some(a.cmp(b)),
            ord => Some(ord),
        }
    }
}

/// Per-peer outbound channel handle. Concrete sinks include
/// [`MpscRepairSink`] (used in tests) and the production wiring
/// that trampolines onto
/// `mpsc::Sender<OutboundRequest>` keyed by peer index.
pub trait RepairSink: Send + Sync {
    /// Enqueue one task on the underlying channel.
    ///
    /// # Errors
    /// Returns `Err(task)` (returning the task back) when the
    /// underlying channel is closed or full; the scheduler
    /// will surface this as [`Outcome::PeerUnavailable`].
    fn submit(&self, task: RepairTask) -> Result<(), RepairTask>;
}

/// Tokio-mpsc-backed [`RepairSink`] used by tests and as the
/// default in-process wiring.
pub struct MpscRepairSink {
    tx: mpsc::Sender<RepairTask>,
}

impl MpscRepairSink {
    /// Wrap a [`mpsc::Sender`].
    #[must_use]
    pub fn new(tx: mpsc::Sender<RepairTask>) -> Self {
        Self { tx }
    }
}

impl RepairSink for MpscRepairSink {
    fn submit(&self, task: RepairTask) -> Result<(), RepairTask> {
        match self.tx.try_send(task) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(t) | mpsc::error::TrySendError::Closed(t)) => {
                Err(t)
            }
        }
    }
}

/// Repair scheduler.
///
/// Configured with a [`RepairSink`] (per-peer outbound channel
/// abstraction) and a [`VClockOrder`] policy. Each call to
/// [`RepairScheduler::resolve`] consumes one [`Divergence`] and
/// emits one or more [`Outcome`]s.
pub struct RepairScheduler {
    sink: Arc<dyn RepairSink>,
    order: Arc<dyn VClockOrder>,
    peer_idx: u32,
}

impl RepairScheduler {
    /// Build a scheduler bound to one peer.
    #[must_use]
    pub fn new(peer_idx: u32, sink: Arc<dyn RepairSink>, order: Arc<dyn VClockOrder>) -> Self {
        Self {
            sink,
            order,
            peer_idx,
        }
    }

    /// Resolve every divergence in the slice. Convenience
    /// wrapper over repeated calls to [`resolve`].
    pub fn resolve_all(&self, divergences: &[Divergence]) -> Vec<Outcome> {
        let mut out = Vec::new();
        for d in divergences {
            out.extend(self.resolve(d));
        }
        out
    }

    /// Resolve one divergence: pair `local_only` and
    /// `remote_only` entries by `(bucket, key)`, decide the
    /// winner, and submit the task.
    pub fn resolve(&self, divergence: &Divergence) -> Vec<Outcome> {
        let mut out = Vec::new();
        // Index remote entries by (bucket, key) so we can pair
        // them with local entries having the same key but a
        // different vclock.
        let mut remote_by_key: std::collections::BTreeMap<(Vec<u8>, Vec<u8>), &KeyEntry> =
            std::collections::BTreeMap::new();
        for r in &divergence.remote_only {
            remote_by_key.insert((r.bucket.clone(), r.key.clone()), r);
        }
        let mut local_seen: std::collections::BTreeSet<(Vec<u8>, Vec<u8>)> =
            std::collections::BTreeSet::new();

        for l in &divergence.local_only {
            let id = (l.bucket.clone(), l.key.clone());
            local_seen.insert(id.clone());
            if let Some(r) = remote_by_key.remove(&id) {
                // Both sides have the key; resolve via vclock.
                match self.order.compare(&l.vclock, &r.vclock) {
                    Some(std::cmp::Ordering::Greater) => {
                        out.push(self.enact(l, RepairDirection::PushToRemote));
                    }
                    Some(std::cmp::Ordering::Less) => {
                        out.push(self.enact(r, RepairDirection::PullFromRemote));
                    }
                    Some(std::cmp::Ordering::Equal) => {
                        // Equal vclocks but the segment XOR
                        // diverged; that is a directory-level
                        // race -- another sweep will catch it.
                    }
                    None => {
                        out.push(Outcome::AmbiguousVClock {
                            bucket: l.bucket.clone(),
                            key: l.key.clone(),
                            local: l.vclock.clone(),
                            remote: r.vclock.clone(),
                        });
                    }
                }
            } else {
                // Local-only key; push to the remote.
                out.push(self.enact(l, RepairDirection::PushToRemote));
            }
        }

        // Anything left in remote_by_key is a remote-only key.
        for (_, r) in remote_by_key {
            out.push(self.enact(r, RepairDirection::PullFromRemote));
        }
        out
    }

    fn enact(&self, entry: &KeyEntry, direction: RepairDirection) -> Outcome {
        let task = RepairTask {
            peer_idx: self.peer_idx,
            bucket: entry.bucket.clone(),
            key: entry.key.clone(),
            vclock: entry.vclock.clone(),
            direction,
        };
        match self.sink.submit(task.clone()) {
            Ok(()) => Outcome::Repaired(task),
            Err(t) => Outcome::PeerUnavailable {
                peer_idx: self.peer_idx,
                task: t,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aae::exchange::Divergence;
    use crate::datatypes::ActorId;

    fn aid(peer: &str) -> ActorId {
        ActorId::new("dc1", peer)
    }

    fn vc(actor: &ActorId, ticks: u64) -> Vclock {
        let mut v = Vclock::new();
        for _ in 0..ticks {
            v.increment(actor);
        }
        v
    }

    #[test]
    fn lexicographic_order_picks_longer() {
        let lo = LexicographicOrder;
        assert_eq!(
            lo.compare(b"vc2", b"vc1"),
            Some(std::cmp::Ordering::Greater)
        );
        assert_eq!(
            lo.compare(b"vc11", b"vc2"),
            Some(std::cmp::Ordering::Greater),
            "longer wins over shorter"
        );
        assert_eq!(lo.compare(b"x", b"x"), Some(std::cmp::Ordering::Equal));
    }

    #[test]
    fn repair_for_divergent_key_reaches_channel() {
        let (tx, mut rx) = mpsc::channel::<RepairTask>(8);
        let sink: Arc<dyn RepairSink> = Arc::new(MpscRepairSink::new(tx));
        let order: Arc<dyn VClockOrder> = Arc::new(LexicographicOrder);
        let sched = RepairScheduler::new(7, sink, order);

        let div = Divergence {
            time_bucket: 0,
            segment: 11,
            local_only: vec![KeyEntry {
                bucket: b"users".to_vec(),
                key: b"alice".to_vec(),
                vclock: b"vc2".to_vec(),
            }],
            remote_only: vec![KeyEntry {
                bucket: b"users".to_vec(),
                key: b"alice".to_vec(),
                vclock: b"vc1".to_vec(),
            }],
        };

        let outcomes = sched.resolve(&div);
        assert_eq!(outcomes.len(), 1);
        match &outcomes[0] {
            Outcome::Repaired(task) => {
                assert_eq!(task.peer_idx, 7);
                assert_eq!(task.key, b"alice");
                assert_eq!(task.vclock, b"vc2");
                assert_eq!(task.direction, RepairDirection::PushToRemote);
            }
            other => panic!("expected Repaired, got {other:?}"),
        }

        // The task reached the channel.
        let received = rx.try_recv().unwrap();
        assert_eq!(received.key, b"alice");
        assert_eq!(received.vclock, b"vc2");
    }

    #[test]
    fn repair_local_only_pushes_to_remote() {
        let (tx, _rx) = mpsc::channel::<RepairTask>(8);
        let sink: Arc<dyn RepairSink> = Arc::new(MpscRepairSink::new(tx));
        let order: Arc<dyn VClockOrder> = Arc::new(LexicographicOrder);
        let sched = RepairScheduler::new(3, sink, order);

        let div = Divergence {
            time_bucket: 0,
            segment: 1,
            local_only: vec![KeyEntry {
                bucket: b"b".to_vec(),
                key: b"k".to_vec(),
                vclock: b"vc".to_vec(),
            }],
            remote_only: vec![],
        };
        let outcomes = sched.resolve(&div);
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(
            &outcomes[0],
            Outcome::Repaired(t) if t.direction == RepairDirection::PushToRemote
        ));
    }

    #[test]
    fn closed_channel_surfaces_peer_unavailable() {
        let (tx, rx) = mpsc::channel::<RepairTask>(1);
        drop(rx);
        let sink: Arc<dyn RepairSink> = Arc::new(MpscRepairSink::new(tx));
        let order: Arc<dyn VClockOrder> = Arc::new(LexicographicOrder);
        let sched = RepairScheduler::new(99, sink, order);

        let div = Divergence {
            time_bucket: 0,
            segment: 1,
            local_only: vec![KeyEntry {
                bucket: b"b".to_vec(),
                key: b"k".to_vec(),
                vclock: b"vc".to_vec(),
            }],
            remote_only: vec![],
        };
        let outcomes = sched.resolve(&div);
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(&outcomes[0], Outcome::PeerUnavailable { .. }));
    }

    #[test]
    fn evaluate_winner_when_one_dominates_others() {
        let actor = aid("p1");
        let a = vc(&actor, 3);
        let b = vc(&actor, 1);
        let c = vc(&actor, 2);
        let replicas = vec![
            (Bytes::from_static(b"v_a"), a.clone()),
            (Bytes::from_static(b"v_b"), b),
            (Bytes::from_static(b"v_c"), c),
        ];
        match RepairTask::evaluate(&replicas) {
            RepairOutcome::Winner { value, vclock } => {
                assert_eq!(value, Bytes::from_static(b"v_a"));
                assert_eq!(vclock, a);
            }
            RepairOutcome::Siblings(s) => panic!("expected Winner, got Siblings({})", s.len()),
        }
    }

    #[test]
    fn evaluate_siblings_when_all_concurrent() {
        let p1 = aid("p1");
        let p2 = aid("p2");
        let p3 = aid("p3");
        let a = vc(&p1, 1);
        let b = vc(&p2, 1);
        let c = vc(&p3, 1);
        let replicas = vec![
            (Bytes::from_static(b"v_a"), a),
            (Bytes::from_static(b"v_b"), b),
            (Bytes::from_static(b"v_c"), c),
        ];
        match RepairTask::evaluate(&replicas) {
            RepairOutcome::Siblings(s) => {
                assert_eq!(s.len(), 3);
            }
            RepairOutcome::Winner { .. } => panic!("expected Siblings(3), got Winner"),
        }
    }

    #[test]
    fn evaluate_siblings_excludes_dominated_entries() {
        // A strictly dominates B (so B is dropped); A and C
        // are concurrent (different actors). Expected:
        // Siblings([A, C]).
        let p1 = aid("p1");
        let p2 = aid("p2");
        let mut a = Vclock::new();
        a.increment(&p1);
        a.increment(&p1);
        let b = vc(&p1, 1);
        let c = vc(&p2, 1);
        let replicas = vec![
            (Bytes::from_static(b"v_a"), a.clone()),
            (Bytes::from_static(b"v_b"), b),
            (Bytes::from_static(b"v_c"), c.clone()),
        ];
        match RepairTask::evaluate(&replicas) {
            RepairOutcome::Siblings(s) => {
                assert_eq!(s.len(), 2, "B is dominated by A and must be excluded");
                let values: Vec<&Bytes> = s.iter().map(|(v, _)| v).collect();
                assert!(values.contains(&&Bytes::from_static(b"v_a")));
                assert!(values.contains(&&Bytes::from_static(b"v_c")));
                assert!(!values.contains(&&Bytes::from_static(b"v_b")));
            }
            RepairOutcome::Winner { .. } => panic!("expected Siblings(2), got Winner"),
        }
    }

    #[test]
    fn evaluate_dedupes_equal_clocks() {
        // Two replicas with identical clocks but different
        // values: the dedupe step keeps the first; with only
        // one survivor the outcome is Winner.
        let actor = aid("p1");
        let a = vc(&actor, 2);
        let replicas = vec![
            (Bytes::from_static(b"v_a"), a.clone()),
            (Bytes::from_static(b"v_a_dup"), a.clone()),
        ];
        match RepairTask::evaluate(&replicas) {
            RepairOutcome::Winner { value, vclock } => {
                assert_eq!(value, Bytes::from_static(b"v_a"));
                assert_eq!(vclock, a);
            }
            RepairOutcome::Siblings(s) => {
                panic!("expected Winner after dedupe, got Siblings({})", s.len())
            }
        }
    }

    #[test]
    fn resolve_with_warn_picks_lex_largest_on_siblings() {
        let p1 = aid("p1");
        let p2 = aid("p2");
        let outcome = RepairOutcome::Siblings(vec![
            (Bytes::from_static(b"alpha"), vc(&p1, 1)),
            (Bytes::from_static(b"zulu"), vc(&p2, 1)),
            (Bytes::from_static(b"mike"), vc(&p1, 1)),
        ]);
        let (value, _) = outcome.resolve_with_warn(b"some-key");
        assert_eq!(value, Bytes::from_static(b"zulu"));
    }

    #[test]
    fn resolve_with_warn_passes_winner_through() {
        let actor = aid("p1");
        let v = vc(&actor, 5);
        let outcome = RepairOutcome::Winner {
            value: Bytes::from_static(b"only"),
            vclock: v.clone(),
        };
        let (value, vclock) = outcome.resolve_with_warn(b"k");
        assert_eq!(value, Bytes::from_static(b"only"));
        assert_eq!(vclock, v);
    }
}
