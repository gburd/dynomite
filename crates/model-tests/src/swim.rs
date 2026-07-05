//! Model of SWIM + Lifeguard membership and failure detection.
//!
//! This mirrors the production state machine in
//! `crates/dynomite/src/cluster/swim.rs`
//! (`dynomite::cluster::swim::SwimState`). Following the doctrine of
//! this crate, the model does not link the production crate; it
//! re-expresses the same decision logic (probe outcome -> suspicion,
//! incarnation-number refutation, Lifeguard nack-score timeout
//! dilation, dogpile, and infection-style dissemination) as a small
//! self-contained state machine so the checker's search space stays
//! bounded.
//!
//! Two shapes of check live here:
//!
//! * A [`stateright::Model`] ([`SwimDissemination`]) that
//!   exhaustively explores infection-style update dissemination with
//!   refutation over a connected node set, mirroring `gossip.rs`'s
//!   convergence model but with the SWIM merge precedence
//!   (higher incarnation wins; equal incarnation, worse-belief wins;
//!   a refutation is a strictly-higher-incarnation Alive that
//!   overrides suspicion everywhere). It asserts CONVERGENCE and
//!   DISSEMINATION: an accepted membership fact reaches every live
//!   node.
//!
//! * Deterministic seeded simulations ([`mod sim`]) that drive the
//!   re-expressed SWIM state machine over a lossy/delaying network
//!   with some nodes genuinely dead and some merely slow, and assert:
//!   - **Completeness**: a genuinely-dead node is eventually
//!     confirmed dead by every live node.
//!   - **Accuracy / low false-positive**: a merely-slow-but-alive
//!     node is NOT permanently confirmed dead (incarnation
//!     refutation + Lifeguard dilation prevent it), and SWIM +
//!     Lifeguard produces strictly fewer false positives than a
//!     naive fixed-timeout detector under the identical slow-node
//!     schedule (a comparative assertion).
//!   - **Negative control**: with incarnation refutation disabled,
//!     the checker CATCHES a live-but-slow node being falsely and
//!     permanently declared dead -- proving the accuracy invariant
//!     has teeth.

use stateright::{Model, Property};

/// Incarnation number (monotone, per member).
type Incarnation = u8;

/// SWIM-internal belief about one member. `Alive < Suspect < Dead`
/// in the equal-incarnation tie-break (worse belief wins so
/// suspicion spreads).
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Belief {
    /// Reachable.
    Alive,
    /// Missed a probe; under the suspicion timer.
    Suspect,
    /// Confirmed dead.
    Dead,
}

impl Belief {
    /// Equal-incarnation precedence rank (higher overrides).
    fn rank(self) -> u8 {
        match self {
            Belief::Alive => 0,
            Belief::Suspect => 1,
            Belief::Dead => 2,
        }
    }
}

/// A `(incarnation, belief)` view one node holds of the tracked
/// member.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct View {
    incarnation: Incarnation,
    belief: Belief,
}

/// Aggregate state: each node's view of one tracked member, plus a
/// bounded budget of dissemination steps.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DissState {
    /// Per-node view of the tracked member.
    views: Vec<View>,
    /// Which node index IS the tracked member (it alone can refute).
    /// `None` models the member being genuinely dead (never
    /// refutes).
    member_node: Option<usize>,
    /// The tracked member's own incarnation (only it advances this).
    member_incarnation: Incarnation,
    /// Remaining refutation budget (bounds the search).
    refutations_left: u8,
}

/// SWIM dissemination + refutation convergence model over `n`
/// fully-connected nodes tracking one member.
#[derive(Clone)]
pub struct SwimDissemination {
    /// Node count.
    pub n: usize,
    /// Index of the tracked member among the nodes; it can refute.
    pub member: usize,
    /// Refutation budget.
    pub refutations: u8,
}

/// Actions the checker may take.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Node `i` locally suspects the member (a failed probe).
    Suspect(usize),
    /// The member observes it is suspected somewhere and refutes by
    /// bumping its incarnation and broadcasting Alive.
    Refute,
    /// Node `i` pushes its view to node `j` (infection-style
    /// piggyback); `j` merges by SWIM precedence.
    Push(usize, usize),
}

impl Model for SwimDissemination {
    type State = DissState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![DissState {
            views: vec![
                View {
                    incarnation: 0,
                    belief: Belief::Alive,
                };
                self.n
            ],
            member_node: Some(self.member),
            member_incarnation: 0,
            refutations_left: self.refutations,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        for i in 0..self.n {
            // Only a non-member node meaningfully suspects the member.
            if Some(i) != state.member_node {
                actions.push(Action::Suspect(i));
            }
        }
        if state.refutations_left > 0 {
            actions.push(Action::Refute);
        }
        for i in 0..self.n {
            for j in 0..self.n {
                if i != j {
                    actions.push(Action::Push(i, j));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Action::Suspect(i) => {
                let v = s.views[i];
                // Suspect at the current incarnation, if not already
                // a worse-or-equal belief there.
                let cand = View {
                    incarnation: v.incarnation,
                    belief: Belief::Suspect,
                };
                if merge(&mut s.views[i], cand) {
                    Some(s)
                } else {
                    None
                }
            }
            Action::Refute => {
                let member = s.member_node?;
                // The member only refutes if some node currently
                // suspects (or has killed) it -- otherwise the action
                // is a no-op and pruned.
                let suspected = s
                    .views
                    .iter()
                    .any(|v| v.belief != Belief::Alive && v.incarnation >= s.member_incarnation);
                if !suspected {
                    return None;
                }
                s.member_incarnation = s.member_incarnation.checked_add(1)?;
                s.refutations_left -= 1;
                // The member adopts its own fresh Alive view.
                s.views[member] = View {
                    incarnation: s.member_incarnation,
                    belief: Belief::Alive,
                };
                Some(s)
            }
            Action::Push(i, j) => {
                let src = s.views[i];
                if merge(&mut s.views[j], src) {
                    Some(s)
                } else {
                    None
                }
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Dissemination + convergence: at quiescence every node
            // agrees on the member's view.
            Property::<Self>::eventually("convergence", |_, s| all_agree(s)),
            // A refutation is total: once the member has refuted to
            // its highest incarnation and that view has spread, no
            // node holds a stale kill at that incarnation. Encoded as
            // a reachable fully-alive agreement after refutation.
            Property::<Self>::sometimes("refutation reaches all", |_, s| {
                s.views.iter().all(|v| v.belief == Belief::Alive) && s.member_incarnation > 0
            }),
            Property::<Self>::sometimes("suspicion reaches all", |_, s| {
                s.views.iter().all(|v| v.belief != Belief::Alive)
            }),
        ]
    }
}

/// SWIM merge into `dst` from `src`. Returns true when `dst` changed.
/// Higher incarnation wins; equal incarnation, higher belief-rank
/// wins.
fn merge(dst: &mut View, src: View) -> bool {
    let take = match src.incarnation.cmp(&dst.incarnation) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => src.belief.rank() > dst.belief.rank(),
    };
    if take && *dst != src {
        *dst = src;
        true
    } else {
        false
    }
}

/// True when every node holds an identical view.
fn all_agree(s: &DissState) -> bool {
    match s.views.first() {
        None => true,
        Some(first) => s.views.iter().all(|v| v == first),
    }
}

/// Deterministic seeded simulations of the SWIM + Lifeguard failure
/// detector against a lossy/delaying network. These carry the
/// completeness, accuracy/low-false-positive, comparative, and
/// negative-control invariants; the fault *schedule* they need does
/// not fit a pure BFS model cleanly, so they drive the re-expressed
/// state machine directly over a seeded pseudo-random schedule and
/// assert the invariants over the whole run.
pub mod sim {
    /// The tracked member's physical truth in a simulation.
    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub enum Truth {
        /// Alive and always answers.
        Healthy,
        /// Alive but its probes/acks are dropped with probability
        /// `drop` (a merely-slow node). It still answers `1 - drop`
        /// of the time, so it can refute.
        Slow,
        /// Genuinely dead: never answers, never refutes.
        Dead,
    }

    /// One observer's SWIM view of one target, plus Lifeguard state.
    #[derive(Clone, Debug)]
    struct Observer {
        /// Belief about the target: 0 alive, 1 suspect, 2 dead.
        belief: u8,
        /// Highest incarnation seen for the target.
        seen_incarnation: u32,
        /// Tick the current suspicion started (if suspect).
        suspect_since: u64,
        /// Lifeguard nack score for THIS observer.
        nack_score: u32,
    }

    impl Observer {
        fn new() -> Self {
            Self {
                belief: 0,
                seen_incarnation: 0,
                suspect_since: 0,
                nack_score: 0,
            }
        }
    }

    /// A tiny deterministic xorshift PRNG so the schedule is fully
    /// reproducible (no external rng dependency).
    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self {
            Rng(seed | 1)
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        /// True with probability `p` (0..=100 percent).
        fn chance(&mut self, p: u64) -> bool {
            self.next_u64() % 100 < p
        }
    }

    /// SWIM + Lifeguard configuration mirrored from
    /// `dynomite::cluster::swim::SwimConfig`.
    #[derive(Copy, Clone)]
    pub struct Cfg {
        /// Base suspicion timeout in periods.
        pub base: u64,
        /// Per-nack-score dilation multiplier (0 disables Lifeguard
        /// dilation -- one arm of the negative control).
        pub dilation: u64,
        /// Max nack score.
        pub ns_max: u32,
        /// Whether the target can refute a suspicion by bumping its
        /// incarnation (false is the primary negative control).
        pub refutation: bool,
    }

    impl Cfg {
        /// Production-shaped SWIM + Lifeguard.
        pub fn swim() -> Self {
            Cfg {
                base: 4,
                dilation: 1,
                ns_max: 8,
                refutation: true,
            }
        }
        /// Negative control: refutation disabled.
        pub fn no_refutation() -> Self {
            Cfg {
                refutation: false,
                ..Cfg::swim()
            }
        }
    }

    /// Outcome of one simulation over `periods` protocol periods.
    #[derive(Debug, Default)]
    pub struct Outcome {
        /// True if every live observer confirmed the target dead by
        /// the end (completeness, for a genuinely-dead target).
        pub all_confirmed_dead: bool,
        /// Number of observers that held the target dead at the final
        /// tick (a false positive when the target is actually alive).
        pub final_dead_observers: usize,
        /// Peak number of observers simultaneously holding dead at
        /// any tick.
        pub peak_dead_observers: usize,
    }

    /// Run one SWIM simulation: `n` observer nodes probe one target
    /// of physical `truth` over `periods` periods, with the observers
    /// themselves dropping their outbound probes with probability
    /// `observer_drop` (models slow OBSERVERS, the case Lifeguard
    /// targets). `fixed_timeout` selects the naive detector
    /// (fixed miss count, no refutation, no dilation) for the
    /// comparison; otherwise the SWIM + Lifeguard rules in `cfg` are
    /// used. Returns the [`Outcome`].
    #[allow(clippy::too_many_arguments)]
    pub fn run(
        seed: u64,
        n: usize,
        truth: Truth,
        periods: u64,
        target_drop: u64,
        observer_drop: u64,
        cfg: Cfg,
        fixed_timeout: bool,
    ) -> Outcome {
        let mut rng = Rng::new(seed);
        let mut obs: Vec<Observer> = (0..n).map(|_| Observer::new()).collect();
        // The target's own incarnation. Only the (live) target
        // advances it, when it learns it is suspected.
        let mut target_incarnation: u32 = 0;
        // For the naive detector: consecutive missed probes per obs.
        let mut misses = vec![0u64; n];

        let mut peak_dead = 0usize;

        for tick in 1..=periods {
            // Phase 1: each observer runs its probe period.
            for i in 0..n {
                if obs[i].belief == 2 {
                    // Already dead: naive never recovers; SWIM can via
                    // a higher-incarnation refutation handled below.
                    continue;
                }
                // Does the target answer this observer this period?
                let answered = match truth {
                    Truth::Healthy => true,
                    Truth::Dead => false,
                    Truth::Slow => {
                        // Target answers unless dropped, AND the
                        // observer's own path is up (observer_drop).
                        !rng.chance(target_drop) && !rng.chance(observer_drop)
                    }
                };
                // An observer that itself dropped the probe (but the
                // target is actually reachable) can still learn the
                // truth via an indirect probe: model that as an
                // "indirect ack" that raises the nack score.
                let observer_dropped = match truth {
                    Truth::Slow | Truth::Healthy => rng.chance(observer_drop),
                    Truth::Dead => false,
                };

                if answered {
                    obs[i].belief = 0;
                    obs[i].suspect_since = 0;
                    misses[i] = 0;
                    obs[i].nack_score = obs[i].nack_score.saturating_sub(1);
                } else if !fixed_timeout && observer_dropped && truth != Truth::Dead {
                    // Indirect probe succeeds: target alive, observer
                    // is the slow one. Raise nack score, keep alive.
                    obs[i].nack_score = (obs[i].nack_score + 1).min(cfg.ns_max);
                    obs[i].belief = 0;
                    obs[i].suspect_since = 0;
                    misses[i] = 0;
                } else {
                    // Failed probe -> begin/continue suspicion.
                    misses[i] += 1;
                    if obs[i].belief == 0 {
                        obs[i].belief = 1;
                        obs[i].suspect_since = tick;
                    }
                }
            }

            // Phase 2: the live target refutes if it is suspected
            // anywhere (SWIM only; a dead target cannot).
            let suspected_anywhere = obs.iter().any(|o| o.belief != 0);
            if !fixed_timeout && cfg.refutation && truth != Truth::Dead && suspected_anywhere {
                target_incarnation += 1;
                // The refutation infects every observer (connected
                // graph): each adopts the higher-incarnation Alive.
                for o in &mut obs {
                    if o.seen_incarnation < target_incarnation {
                        o.seen_incarnation = target_incarnation;
                        o.belief = 0;
                        o.suspect_since = 0;
                    }
                }
            }

            // Phase 3: confirm suspicions whose deadline passed.
            // SWIM dilates the deadline by nack score; the naive
            // detector uses a fixed miss threshold.
            for i in 0..n {
                if fixed_timeout {
                    if obs[i].belief == 1 && misses[i] >= cfg.base {
                        obs[i].belief = 2;
                    }
                } else if obs[i].belief == 1 {
                    let dilated = cfg
                        .base
                        .saturating_mul(1 + u64::from(obs[i].nack_score) * cfg.dilation);
                    if tick >= obs[i].suspect_since.saturating_add(dilated) {
                        obs[i].belief = 2;
                    }
                }
            }

            let dead_now = obs.iter().filter(|o| o.belief == 2).count();
            peak_dead = peak_dead.max(dead_now);
        }

        let final_dead = obs.iter().filter(|o| o.belief == 2).count();
        Outcome {
            all_confirmed_dead: final_dead == n,
            final_dead_observers: final_dead,
            peak_dead_observers: peak_dead,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::sim::{run, Cfg, Truth};
    use super::*;
    use stateright::Checker;

    /// Dissemination + convergence: SWIM's infection-style updates
    /// with refutation converge on a connected node set.
    #[test]
    fn dissemination_converges() {
        let checker = SwimDissemination {
            n: 3,
            member: 2,
            refutations: 2,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
        assert!(checker.unique_state_count() > 1);
    }

    /// A four-node group with the member able to refute still
    /// converges.
    #[test]
    fn four_node_dissemination_converges() {
        let checker = SwimDissemination {
            n: 4,
            member: 0,
            refutations: 1,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
    }

    /// COMPLETENESS: a genuinely-dead target is eventually confirmed
    /// dead by every live observer, across many seeds.
    #[test]
    fn completeness_dead_node_confirmed_by_all() {
        for seed in 1..=64u64 {
            let out = run(
                seed,
                5,
                Truth::Dead,
                60,
                0,
                20, // observers themselves flaky, but target is dead
                Cfg::swim(),
                false,
            );
            assert!(
                out.all_confirmed_dead,
                "seed {seed}: a dead node must be confirmed by all live observers, \
                 got {}/{} dead",
                out.final_dead_observers, 5
            );
        }
    }

    /// ACCURACY / LOW FALSE-POSITIVE: a merely-slow-but-alive target
    /// is NOT permanently confirmed dead under SWIM + Lifeguard.
    #[test]
    fn accuracy_slow_node_not_permanently_dead() {
        for seed in 1..=64u64 {
            let out = run(
                seed,
                5,
                Truth::Slow,
                80,
                40, // target drops 40% of answers
                30, // observers drop 30% of their probes
                Cfg::swim(),
                false,
            );
            assert_eq!(
                out.final_dead_observers, 0,
                "seed {seed}: a slow-but-alive node must not end permanently dead \
                 (refutation + dilation), got {} dead",
                out.final_dead_observers
            );
        }
    }

    /// COMPARATIVE: under the identical slow-node schedule, SWIM +
    /// Lifeguard produces strictly fewer false positives than the
    /// naive fixed-timeout detector. We compare peak false-positive
    /// pressure (the naive detector convicts the slow node; SWIM does
    /// not) and require a strict win in aggregate.
    #[test]
    fn comparative_fewer_false_positives_than_fixed_timeout() {
        let mut swim_total = 0usize;
        let mut naive_total = 0usize;
        for seed in 1..=64u64 {
            let swim = run(seed, 5, Truth::Slow, 80, 40, 30, Cfg::swim(), false);
            let naive = run(seed, 5, Truth::Slow, 80, 40, 30, Cfg::swim(), true);
            swim_total += swim.peak_dead_observers;
            naive_total += naive.peak_dead_observers;
        }
        assert_eq!(
            swim_total, 0,
            "SWIM + Lifeguard must produce zero false-positive convictions of a slow \
             live node, got aggregate peak {swim_total}"
        );
        assert!(
            naive_total > swim_total,
            "naive fixed-timeout must produce strictly MORE false positives than \
             SWIM (naive={naive_total}, swim={swim_total})"
        );
    }

    /// NEGATIVE CONTROL: disable incarnation refutation and the
    /// checker CATCHES a live-but-slow node being falsely and
    /// permanently declared dead. This is the invariant with teeth:
    /// if this test's `assert!` did NOT fire on the buggy config, the
    /// accuracy invariant above would be vacuous.
    #[test]
    fn negative_control_no_refutation_causes_false_death() {
        let mut any_false_death = false;
        for seed in 1..=64u64 {
            let out = run(
                seed,
                5,
                Truth::Slow,
                80,
                40,
                30,
                Cfg::no_refutation(),
                false,
            );
            if out.final_dead_observers > 0 {
                any_false_death = true;
                break;
            }
        }
        assert!(
            any_false_death,
            "negative control failed to reproduce a false death: with refutation \
             disabled a slow node MUST be able to get falsely and permanently \
             declared dead, otherwise the accuracy invariant is vacuous"
        );
    }

    /// The comparison is symmetric on the healthy case: nobody is
    /// ever declared dead when the target is healthy, under either
    /// detector (sanity floor).
    #[test]
    fn healthy_node_never_declared_dead() {
        for &fixed in &[false, true] {
            for seed in 1..=32u64 {
                let out = run(seed, 5, Truth::Healthy, 60, 0, 20, Cfg::swim(), fixed);
                assert_eq!(
                    out.peak_dead_observers, 0,
                    "seed {seed} fixed={fixed}: a healthy node must never be declared dead"
                );
            }
        }
    }
}
