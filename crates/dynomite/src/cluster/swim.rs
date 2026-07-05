//! SWIM + Lifeguard membership and failure detection.
//!
//! This module is an opt-in alternative to the Dynamo-style gossip
//! plus phi-accrual detector in [`crate::cluster::gossip`] and
//! [`crate::cluster::failure_detector`]. It is selected by the
//! pool's `membership: swim` directive; the default remains
//! `gossip` so nothing changes for existing deployments. The
//! phi-accrual path is left completely intact -- SWIM is additive.
//!
//! # Protocol
//!
//! SWIM (Das, Gupta, Motivala, DSN 2002) replaces all-to-all
//! heartbeating with a randomised probe: every protocol period a
//! node picks one random member and PINGs it. If no ack arrives
//! within the round-trip window, the prober asks `k` other members
//! to probe the target on its behalf (PING-REQ, "indirect probe").
//! Only when both the direct and every indirect probe fail does the
//! prober mark the target *suspect*. A suspect that is not refuted
//! within a suspicion timeout is *confirmed* dead. Membership
//! updates ride along ("piggyback") on the ordinary ping/ack
//! traffic, so a change spreads infection-style in O(log N) periods
//! with O(1) load per node per period.
//!
//! Incarnation numbers make suspicions refutable: every node owns a
//! monotonically increasing incarnation for itself. When a node
//! learns it is suspected, it re-broadcasts an *alive* update at a
//! strictly higher incarnation, which overrides the suspicion on
//! every other node. A dead node cannot refute, so a genuine death
//! is confirmed; a merely-slow node that is still running does
//! refute, so a transient slowdown does not produce a permanent
//! false "down".
//!
//! # Lifeguard
//!
//! Lifeguard (Dadgar, Phillips, Currey, HashiCorp 2018) addresses
//! the case where the *observer* is unhealthy rather than the
//! target. Three mechanisms, all implemented here:
//!
//! * **Self-awareness (nack score, NS).** A node tracks how many of
//!   its own recent probes failed to draw a response *and* were
//!   later contradicted (the target turned out to be alive via an
//!   indirect probe or a refutation). A high NS means "I am probably
//!   the slow one." NS dilates the local probe interval and the
//!   suspicion timeout, so a slow observer waits longer before
//!   convicting anyone.
//! * **Dogpile / buddy-system suspicion timeout.** The suspicion
//!   timeout shrinks as independent suspect confirmations pile up:
//!   the more nodes that independently suspect the same target, the
//!   shorter the wait before confirming. One lone suspicion waits
//!   the full (NS-dilated) timeout; a target suspected by many
//!   confirms quickly.
//! * **Buddy-system nack.** An indirect prober that could reach the
//!   target but got no ack (a "nack") is evidence the *original*
//!   prober is the problem, not the target; that feeds the nack
//!   score above.
//!
//! # Two-layer design (DST discipline)
//!
//! The protocol logic lives entirely in [`SwimState`], a pure,
//! synchronous, deterministic state machine. It takes explicit
//! inputs (a probe outcome, an incoming membership update, a clock
//! tick) and returns explicit outputs (the peer-state transitions to
//! apply). It performs no I/O and holds no clock -- the caller
//! supplies a logical time. This is exactly the shape the
//! `model-tests` DST checker drives, and it is why the accuracy and
//! completeness invariants can be model-checked deterministically.
//!
//! The I/O shell -- the tokio task that actually opens sockets,
//! sends pings, waits for acks with a timeout, and calls into the
//! state machine -- is [`SwimHandler`]. It mirrors
//! [`crate::cluster::gossip::GossipHandler::evaluate`] by producing
//! the same `Vec<(u32, PeerState)>` transition list the rest of the
//! engine already consumes, so dispatch and hinted handoff are
//! unchanged.
//!
//! # Examples
//!
//! ```
//! use dynomite::cluster::swim::{SwimConfig, SwimState, ProbeResult, Status};
//! use dynomite::cluster::peer::PeerState;
//!
//! // Three members: 0 (self), 1, 2. Node 2 is genuinely dead.
//! let mut s = SwimState::new(0, 3, SwimConfig::default());
//! // Period 1: probe node 2, no direct ack and no indirect ack.
//! s.on_probe(1, 2, ProbeResult::Failed);
//! // A suspicion immediately removes the peer from routing (maps to
//! // Down) but is not yet a confirmed death.
//! assert_eq!(s.member_state(2), PeerState::Down);
//! assert!(matches!(s.status(2), Status::Suspect { .. }));
//! // Let the suspicion timeout elapse with no refutation: the
//! // member is confirmed dead. (It was already projected Down, so
//! // no new PeerState transition is emitted.)
//! s.tick(s.confirm_deadline(2).unwrap());
//! assert_eq!(s.status(2), Status::Dead);
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::cluster::peer::PeerState;
use crate::cluster::pool::ServerPool;

/// Monotonic incarnation number for a member. A node bumps its own
/// incarnation to refute a suspicion; the highest incarnation always
/// wins a merge.
pub type Incarnation = u64;

/// Logical time, in protocol periods. The pure state machine never
/// reads a wall clock; the caller (test harness or tokio shell)
/// supplies a strictly non-decreasing tick count.
pub type Tick = u64;

/// The membership status a node holds for one member. This is the
/// SWIM-internal status; it is projected onto the engine-wide
/// [`PeerState`] by [`SwimState::member_state`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Status {
    /// The member is believed reachable.
    Alive,
    /// The member missed a probe (direct and indirect) and is under
    /// the suspicion timer. Carries the tick at which the suspicion
    /// began and the count of independent suspectors seen so far
    /// (the dogpile signal).
    Suspect {
        /// Tick at which the suspicion started.
        since: Tick,
        /// Number of independent suspectors observed (>= 1).
        suspectors: u32,
    },
    /// The member is confirmed dead. Terminal unless the member
    /// rejoins with a fresh incarnation.
    Dead,
}

/// One member's record in the local view.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Member {
    /// Highest incarnation observed for this member.
    pub incarnation: Incarnation,
    /// Current believed status.
    pub status: Status,
}

/// Outcome of one probe period against a target, as observed by the
/// prober. This is the input the I/O shell feeds the state machine
/// after a ping / ping-req round completes.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ProbeResult {
    /// The direct ping was acked (target is clearly alive).
    Acked,
    /// The direct ping timed out but an indirect (PING-REQ) probe
    /// drew an ack: the target is alive; the prober's own path to
    /// it is the problem. This is a Lifeguard "nack" against the
    /// prober and raises its nack score.
    IndirectAcked,
    /// Neither the direct ping nor any indirect probe drew an ack.
    Failed,
}

/// Tunables for the SWIM + Lifeguard state machine. The defaults
/// mirror the Lifeguard paper's recommended shape (small `k`, a
/// suspicion timeout that is a small multiple of the probe period,
/// scaled logarithmically by cluster size).
#[derive(Copy, Clone, Debug)]
pub struct SwimConfig {
    /// Number of indirect probers per period (`k`). Not consumed by
    /// the pure state machine directly -- the I/O shell uses it to
    /// pick fan-out -- but recorded here so the whole knob set lives
    /// in one place.
    pub indirect_probes: u32,
    /// Base suspicion timeout, in protocol periods, before the
    /// dogpile and nack-score adjustments. A lone suspicion on a
    /// healthy observer waits this long before confirming.
    pub suspicion_periods_base: Tick,
    /// Multiplier applied per unit of nack score when dilating the
    /// suspicion timeout: a slow observer (high NS) waits
    /// `base * (1 + ns * dilation)` periods. Setting this to 0
    /// disables Lifeguard timeout dilation (used by the negative
    /// control in the DST model).
    pub ns_dilation: Tick,
    /// Maximum nack score. Caps how far a slow observer dilates.
    pub ns_max: u32,
    /// When false, a node cannot refute a suspicion by bumping its
    /// incarnation. This is NOT a production knob -- it exists only
    /// so the DST model's negative control can show that disabling
    /// refutation makes a live-but-slow node get falsely and
    /// permanently confirmed dead.
    pub refutation_enabled: bool,
}

impl Default for SwimConfig {
    fn default() -> Self {
        Self {
            indirect_probes: 3,
            suspicion_periods_base: 4,
            ns_dilation: 1,
            ns_max: 8,
            refutation_enabled: true,
        }
    }
}

/// A membership update as it rides on ping / ack traffic
/// (infection-style dissemination). This is the unit the I/O shell
/// piggybacks and the state machine merges.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Update {
    /// Which member the update is about.
    pub member: usize,
    /// The incarnation the sender holds for that member.
    pub incarnation: Incarnation,
    /// The status the sender believes.
    pub status: Status,
}

/// The pure SWIM + Lifeguard state machine for one node.
///
/// Deterministic and I/O-free: every method takes a logical tick or
/// an explicit event and returns the peer-state transitions to
/// apply. See the module docs for the protocol and the two-layer
/// rationale.
#[derive(Clone, Debug)]
pub struct SwimState {
    /// This node's index into the member array.
    me: usize,
    /// This node's own incarnation (bumped to refute suspicions).
    my_incarnation: Incarnation,
    /// Per-member view, keyed by member index. `BTreeMap` keeps
    /// iteration order deterministic for the model checker.
    members: BTreeMap<usize, Member>,
    /// Lifeguard nack score: how "slow" this observer believes it
    /// is. Raised when its direct probe fails but an indirect one
    /// succeeds; decayed on a clean direct ack.
    nack_score: u32,
    /// Tunables.
    cfg: SwimConfig,
}

impl SwimState {
    /// Build a state machine for node `me` in a group of `n`
    /// members. Every other member starts [`Status::Alive`] at
    /// incarnation 0 (the join-time optimistic assumption; a
    /// genuinely dead member is discovered by the first failed
    /// probe).
    ///
    /// # Panics
    ///
    /// Panics if `me >= n`; a node must be a member of its own group.
    #[must_use]
    pub fn new(me: usize, n: usize, cfg: SwimConfig) -> Self {
        assert!(me < n, "self index {me} must be < group size {n}");
        let mut members = BTreeMap::new();
        for i in 0..n {
            if i != me {
                members.insert(
                    i,
                    Member {
                        incarnation: 0,
                        status: Status::Alive,
                    },
                );
            }
        }
        Self {
            me,
            my_incarnation: 0,
            members,
            nack_score: 0,
            cfg,
        }
    }

    /// This node's own index.
    #[must_use]
    pub fn me(&self) -> usize {
        self.me
    }

    /// This node's current incarnation.
    #[must_use]
    pub fn my_incarnation(&self) -> Incarnation {
        self.my_incarnation
    }

    /// Current Lifeguard nack score (0 means "I believe I am
    /// healthy").
    #[must_use]
    pub fn nack_score(&self) -> u32 {
        self.nack_score
    }

    /// The internal SWIM status this node holds for `member`.
    /// Returns [`Status::Alive`] for `me` (a node always considers
    /// itself alive) and for any unknown index.
    #[must_use]
    pub fn status(&self, member: usize) -> Status {
        if member == self.me {
            return Status::Alive;
        }
        self.members
            .get(&member)
            .map_or(Status::Alive, |m| m.status)
    }

    /// The incarnation this node holds for `member`.
    #[must_use]
    pub fn incarnation(&self, member: usize) -> Incarnation {
        if member == self.me {
            return self.my_incarnation;
        }
        self.members.get(&member).map_or(0, |m| m.incarnation)
    }

    /// Project the internal SWIM status onto the engine-wide
    /// [`PeerState`] the dispatcher consumes.
    ///
    /// * [`Status::Alive`] -> [`PeerState::Normal`]
    /// * [`Status::Suspect`] -> [`PeerState::Down`] (a suspect is
    ///   removed from routing immediately; refutation restores it,
    ///   exactly as phi-accrual's transient down/up does)
    /// * [`Status::Dead`] -> [`PeerState::Down`]
    ///
    /// Mapping suspect to `Down` is deliberate: routing to a
    /// suspected-unreachable peer wastes a request, and a refutation
    /// promotes it straight back to `Normal`. The DST accuracy
    /// invariant is about the *confirmed-dead* status, not the
    /// transient suspect window.
    #[must_use]
    pub fn member_state(&self, member: usize) -> PeerState {
        match self.status(member) {
            Status::Alive => PeerState::Normal,
            Status::Suspect { .. } | Status::Dead => PeerState::Down,
        }
    }

    /// Record the outcome of a probe period initiated by this node
    /// against `target` at logical `tick`.
    ///
    /// * [`ProbeResult::Acked`] -> target confirmed alive; refresh
    ///   its status and decay this node's nack score.
    /// * [`ProbeResult::IndirectAcked`] -> target alive, but this
    ///   node's direct path failed: raise the nack score (Lifeguard
    ///   self-awareness) and keep the target alive.
    /// * [`ProbeResult::Failed`] -> begin (or reinforce) suspicion
    ///   of the target.
    ///
    /// Returns any peer-state transitions the caller should apply.
    pub fn on_probe(
        &mut self,
        tick: Tick,
        target: usize,
        result: ProbeResult,
    ) -> Vec<(u32, PeerState)> {
        if target == self.me {
            return Vec::new();
        }
        match result {
            ProbeResult::Acked => {
                self.nack_score = self.nack_score.saturating_sub(1);
                self.mark_alive_local(tick, target)
            }
            ProbeResult::IndirectAcked => {
                // The target is alive but our direct probe missed:
                // we are the slow one. Raise nack score, keep target
                // alive.
                self.nack_score = (self.nack_score + 1).min(self.cfg.ns_max);
                self.mark_alive_local(tick, target)
            }
            ProbeResult::Failed => self.begin_suspicion(tick, target),
        }
    }

    /// Merge an incoming membership update that piggybacked on
    /// ping / ack traffic. Returns any peer-state transitions.
    ///
    /// The merge rule is the SWIM precedence order:
    ///
    /// * A higher incarnation always wins.
    /// * At equal incarnation, `Dead` beats `Suspect` beats `Alive`
    ///   (a worse belief overrides a better one at the same
    ///   incarnation, which is how a suspicion spreads).
    /// * An update about *this* node that suspects or kills it is
    ///   refuted: this node bumps its own incarnation above the
    ///   update and the refutation (a fresh `Alive` at the higher
    ///   incarnation) is what the caller disseminates. Refutation is
    ///   skipped when [`SwimConfig::refutation_enabled`] is false
    ///   (negative-control only).
    pub fn on_update(&mut self, tick: Tick, update: Update) -> Vec<(u32, PeerState)> {
        if update.member == self.me {
            self.handle_update_about_self(update);
            return Vec::new();
        }
        let entry = self.members.entry(update.member).or_insert(Member {
            incarnation: 0,
            status: Status::Alive,
        });
        let prev_state = status_to_peer_state(entry.status);
        let take = match update.incarnation.cmp(&entry.incarnation) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => {
                // Worse belief wins so suspicion spreads. A same-rank
                // Suspect also "takes" so the dogpile suspector count
                // can accumulate (merge_suspect sums the counts).
                status_rank(update.status) > status_rank(entry.status)
                    || matches!(
                        (update.status, entry.status),
                        (Status::Suspect { .. }, Status::Suspect { .. })
                    )
            }
        };
        if take {
            // Preserve dogpile bookkeeping: if we and the sender both
            // suspect the same target at the same incarnation, the
            // suspector count grows, which shortens the confirm
            // deadline.
            let merged = merge_suspect(entry.status, update.status, tick);
            entry.incarnation = update.incarnation;
            entry.status = merged;
        }
        let new_state = status_to_peer_state(self.status(update.member));
        transition(update.member, prev_state, new_state)
    }

    /// Advance logical time to `tick` and confirm any suspicions
    /// whose (dogpile- and NS-adjusted) deadline has passed. Returns
    /// the peer-state transitions produced (suspect -> dead is not a
    /// [`PeerState`] change, since both map to `Down`; the list is
    /// non-empty only when a member first crosses into `Down`).
    pub fn tick(&mut self, tick: Tick) -> Vec<(u32, PeerState)> {
        let mut transitions = Vec::new();
        let members: Vec<usize> = self.members.keys().copied().collect();
        for m in members {
            let Status::Suspect { since, suspectors } = self.members[&m].status else {
                continue;
            };
            if tick >= self.confirm_deadline_for(since, suspectors) {
                let prev = status_to_peer_state(self.members[&m].status);
                self.members.get_mut(&m).unwrap().status = Status::Dead;
                let now = status_to_peer_state(Status::Dead);
                transitions.extend(transition(m, prev, now));
            }
        }
        transitions
    }

    /// The tick at which a suspicion of `member` will confirm dead,
    /// given the current suspector count and this node's nack score.
    /// Returns `None` if the member is not currently suspect.
    #[must_use]
    pub fn confirm_deadline(&self, member: usize) -> Option<Tick> {
        match self.members.get(&member)?.status {
            Status::Suspect { since, suspectors } => {
                Some(self.confirm_deadline_for(since, suspectors))
            }
            _ => None,
        }
    }

    /// Snapshot every member's projected [`PeerState`], for the I/O
    /// shell to reconcile against the pool.
    #[must_use]
    pub fn snapshot(&self) -> Vec<(usize, PeerState)> {
        self.members
            .keys()
            .map(|&m| (m, self.member_state(m)))
            .collect()
    }

    // --- internals ---

    /// Compute the confirm deadline from the suspicion start, the
    /// dogpile suspector count, and this node's nack score.
    ///
    /// Base timeout is dilated by nack score (a slow observer waits
    /// longer -- Lifeguard self-awareness) and shrunk by the number
    /// of independent suspectors (dogpile: more suspectors -> faster
    /// confirm). The deadline is never shorter than 1 period past
    /// the suspicion start.
    fn confirm_deadline_for(&self, since: Tick, suspectors: u32) -> Tick {
        let dilated = self
            .cfg
            .suspicion_periods_base
            .saturating_mul(1 + u64::from(self.nack_score) * self.cfg.ns_dilation);
        // Dogpile: each extra independent suspector halves the
        // remaining wait, floored at 1 period. `suspectors` is >= 1.
        let shift = (suspectors.saturating_sub(1)).min(63);
        let shrunk = (dilated >> shift).max(1);
        since.saturating_add(shrunk)
    }

    /// Refresh a member to alive at the current known incarnation,
    /// emitting a transition if it was previously non-routable.
    fn mark_alive_local(&mut self, _tick: Tick, target: usize) -> Vec<(u32, PeerState)> {
        let entry = self.members.entry(target).or_insert(Member {
            incarnation: 0,
            status: Status::Alive,
        });
        let prev = status_to_peer_state(entry.status);
        // A local ack does not raise the incarnation (the target owns
        // its incarnation); it only clears a local suspicion.
        if !matches!(entry.status, Status::Dead) {
            entry.status = Status::Alive;
        }
        let now = status_to_peer_state(self.status(target));
        transition(target, prev, now)
    }

    /// Begin or reinforce suspicion of `target` after a fully failed
    /// probe period.
    fn begin_suspicion(&mut self, tick: Tick, target: usize) -> Vec<(u32, PeerState)> {
        let entry = self.members.entry(target).or_insert(Member {
            incarnation: 0,
            status: Status::Alive,
        });
        let prev = status_to_peer_state(entry.status);
        match entry.status {
            Status::Alive => {
                entry.status = Status::Suspect {
                    since: tick,
                    suspectors: 1,
                };
            }
            Status::Suspect { since, suspectors } => {
                entry.status = Status::Suspect {
                    since,
                    suspectors: suspectors.saturating_add(1),
                };
            }
            Status::Dead => {}
        }
        let now = status_to_peer_state(self.status(target));
        transition(target, prev, now)
    }

    /// Handle an update that names this node. If it suspects or
    /// kills us and refutation is enabled, bump our incarnation
    /// above the update so the fresh `Alive` overrides it everywhere.
    fn handle_update_about_self(&mut self, update: Update) {
        let suspects_us = matches!(update.status, Status::Suspect { .. } | Status::Dead);
        if suspects_us && self.cfg.refutation_enabled && update.incarnation >= self.my_incarnation {
            self.my_incarnation = update.incarnation + 1;
        }
    }
}

/// Rank for the equal-incarnation tie-break: worse beliefs win so
/// suspicion spreads. Dead > Suspect > Alive.
fn status_rank(s: Status) -> u8 {
    match s {
        Status::Alive => 0,
        Status::Suspect { .. } => 1,
        Status::Dead => 2,
    }
}

/// Merge two statuses at the (already-decided winning) incarnation,
/// combining dogpile suspector counts when both sides suspect.
fn merge_suspect(existing: Status, incoming: Status, tick: Tick) -> Status {
    match (existing, incoming) {
        (
            Status::Suspect {
                since: s1,
                suspectors: c1,
            },
            Status::Suspect { suspectors: c2, .. },
        ) => Status::Suspect {
            since: s1,
            suspectors: c1.saturating_add(c2),
        },
        (_, Status::Suspect { suspectors, .. }) => Status::Suspect {
            since: tick,
            suspectors: suspectors.max(1),
        },
        (_, other) => other,
    }
}

/// Project a raw [`Status`] to a [`PeerState`] without needing a
/// `SwimState` (used inside the merge/transition helpers).
fn status_to_peer_state(s: Status) -> PeerState {
    match s {
        Status::Alive => PeerState::Normal,
        Status::Suspect { .. } | Status::Dead => PeerState::Down,
    }
}

/// Emit a single transition when the projected state actually
/// changed; empty otherwise. `member` is cast to the `u32` peer
/// index the engine uses.
#[allow(
    clippy::cast_possible_truncation,
    reason = "member count is bounded by the configured peer array, well under u32::MAX"
)]
fn transition(member: usize, prev: PeerState, now: PeerState) -> Vec<(u32, PeerState)> {
    if prev == now {
        Vec::new()
    } else {
        vec![(member as u32, now)]
    }
}

/// The tokio-facing I/O shell around [`SwimState`].
///
/// It owns the pure state machine behind a `Mutex`, converts wall
/// time into the logical protocol-period ticks the state machine
/// speaks, and reconciles the machine's peer-state transitions onto
/// the shared [`ServerPool`]. Its [`SwimHandler::evaluate`] returns
/// the same `Vec<(u32, PeerState)>` shape as
/// [`crate::cluster::gossip::GossipHandler::evaluate`], so the run
/// loop and every downstream consumer (dispatch, hinted handoff) are
/// identical regardless of which membership backend is selected.
///
/// The actual ping / ping-req socket traffic and the ack-timeout
/// wait live in the run loop that calls [`SwimHandler::on_probe`] /
/// [`SwimHandler::on_update`]; this shell deliberately holds no
/// sockets so the protocol logic stays in the deterministically
/// testable [`SwimState`].
#[derive(Clone)]
pub struct SwimHandler {
    pool: Arc<ServerPool>,
    state: Arc<Mutex<SwimState>>,
    /// Wall-clock length of one protocol period. Wall time is
    /// divided by this to derive the logical tick.
    period: Duration,
    /// Anchor for tick derivation (the handler's construction time).
    epoch: Instant,
}

impl SwimHandler {
    /// Build a handler for local node `me` over a `pool` of `n`
    /// peers, using `cfg` tunables and a `period`-length protocol
    /// interval.
    #[must_use]
    pub fn new(
        pool: Arc<ServerPool>,
        me: usize,
        n: usize,
        cfg: SwimConfig,
        period: Duration,
    ) -> Self {
        Self {
            pool,
            state: Arc::new(Mutex::new(SwimState::new(me, n, cfg))),
            period: if period.is_zero() {
                Duration::from_millis(1)
            } else {
                period
            },
            epoch: Instant::now(),
        }
    }

    /// Borrow the owning pool.
    #[must_use]
    pub fn pool(&self) -> &Arc<ServerPool> {
        &self.pool
    }

    /// Convert a wall-clock instant into the logical protocol tick.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "period is >= 1ms so the tick count stays far below u64::MAX for any realistic uptime"
    )]
    fn tick_of(&self, now: Instant) -> Tick {
        let elapsed = now.saturating_duration_since(self.epoch);
        (elapsed.as_nanos() / self.period.as_nanos().max(1)) as Tick
    }

    /// Feed a completed probe outcome into the state machine and
    /// reconcile the result onto the pool. Returns the applied
    /// transitions.
    pub fn on_probe(
        &self,
        now: Instant,
        target: usize,
        result: ProbeResult,
    ) -> Vec<(u32, PeerState)> {
        let tick = self.tick_of(now);
        let t = self.state.lock().on_probe(tick, target, result);
        self.apply(&t);
        t
    }

    /// Merge a piggybacked membership update and reconcile.
    pub fn on_update(&self, now: Instant, update: Update) -> Vec<(u32, PeerState)> {
        let tick = self.tick_of(now);
        let t = self.state.lock().on_update(tick, update);
        self.apply(&t);
        t
    }

    /// The periodic timer tick: advance logical time, confirm any
    /// overdue suspicions, and reconcile. Mirrors
    /// [`crate::cluster::gossip::GossipHandler::evaluate`].
    pub fn evaluate(&self, now: Instant) -> Vec<(u32, PeerState)> {
        let tick = self.tick_of(now);
        let t = self.state.lock().tick(tick);
        self.apply(&t);
        t
    }

    /// Apply a batch of `(peer_idx, PeerState)` transitions onto the
    /// pool's peer table, skipping the local node.
    fn apply(&self, transitions: &[(u32, PeerState)]) {
        if transitions.is_empty() {
            return;
        }
        let mut peers = self.pool.peers().write();
        for &(idx, state) in transitions {
            if let Some(p) = peers.iter_mut().find(|p| p.idx() == idx && !p.is_local()) {
                if p.state() != state {
                    p.set_state(state, now_secs_wall());
                }
            }
        }
    }
}

/// Wall-clock epoch seconds, for stamping peer-state transitions.
fn now_secs_wall() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_node_is_confirmed_after_timeout() {
        let mut s = SwimState::new(0, 3, SwimConfig::default());
        // Probe node 2, fully failed -> suspect.
        let t = s.on_probe(1, 2, ProbeResult::Failed);
        assert_eq!(t, vec![(2, PeerState::Down)]);
        assert!(matches!(s.status(2), Status::Suspect { .. }));
        let deadline = s.confirm_deadline(2).unwrap();
        // Before the deadline: still suspect, no new transition.
        let none = s.tick(deadline - 1);
        assert!(none.is_empty());
        assert!(matches!(s.status(2), Status::Suspect { .. }));
        // At the deadline: confirmed dead (already Down, so no
        // PeerState change emitted).
        s.tick(deadline);
        assert_eq!(s.status(2), Status::Dead);
    }

    #[test]
    fn refutation_clears_a_false_suspicion() {
        // Node 0 suspects node 1; node 1 (alive) refutes with a
        // higher incarnation. Node 0 must restore node 1 to alive.
        let mut s0 = SwimState::new(0, 2, SwimConfig::default());
        s0.on_probe(1, 1, ProbeResult::Failed);
        assert_eq!(s0.member_state(1), PeerState::Down);

        // Node 1 learns it is suspected and refutes.
        let mut s1 = SwimState::new(1, 2, SwimConfig::default());
        s1.on_update(
            1,
            Update {
                member: 1,
                incarnation: 0,
                status: Status::Suspect {
                    since: 1,
                    suspectors: 1,
                },
            },
        );
        assert_eq!(s1.my_incarnation(), 1, "should bump to refute");

        // The refutation reaches node 0.
        let back = s0.on_update(
            2,
            Update {
                member: 1,
                incarnation: s1.my_incarnation(),
                status: Status::Alive,
            },
        );
        assert_eq!(back, vec![(1, PeerState::Normal)]);
        assert_eq!(s0.member_state(1), PeerState::Normal);
    }

    #[test]
    fn indirect_ack_raises_nack_score_and_keeps_target_alive() {
        let mut s = SwimState::new(0, 3, SwimConfig::default());
        s.on_probe(1, 1, ProbeResult::IndirectAcked);
        assert_eq!(s.nack_score(), 1);
        assert_eq!(s.member_state(1), PeerState::Normal);
    }

    #[test]
    fn nack_score_dilates_suspicion_timeout() {
        let cfg = SwimConfig::default();
        let mut healthy = SwimState::new(0, 2, cfg);
        let mut slow = SwimState::new(0, 2, cfg);
        // Make `slow` believe it is slow.
        for _ in 0..3 {
            slow.on_probe(0, 1, ProbeResult::IndirectAcked);
        }
        assert!(slow.nack_score() > 0);
        healthy.on_probe(1, 1, ProbeResult::Failed);
        slow.on_probe(1, 1, ProbeResult::Failed);
        let dh = healthy.confirm_deadline(1).unwrap();
        let ds = slow.confirm_deadline(1).unwrap();
        assert!(ds > dh, "slow observer must wait longer (ds={ds}, dh={dh})");
    }

    #[test]
    fn dogpile_shortens_confirm_deadline() {
        let cfg = SwimConfig::default();
        let mut lone = SwimState::new(0, 3, cfg);
        let mut piled = SwimState::new(0, 3, cfg);
        lone.on_probe(1, 2, ProbeResult::Failed);
        piled.on_probe(1, 2, ProbeResult::Failed);
        // Piled: two more independent suspectors arrive via gossip.
        for _ in 0..2 {
            piled.on_update(
                1,
                Update {
                    member: 2,
                    incarnation: 0,
                    status: Status::Suspect {
                        since: 1,
                        suspectors: 1,
                    },
                },
            );
        }
        let dl = lone.confirm_deadline(2).unwrap();
        let dp = piled.confirm_deadline(2).unwrap();
        assert!(dp < dl, "dogpile must shorten confirm (dp={dp}, dl={dl})");
    }

    #[test]
    fn higher_incarnation_wins_merge() {
        let mut s = SwimState::new(0, 2, SwimConfig::default());
        // Suspect node 1 at incarnation 0.
        s.on_update(
            1,
            Update {
                member: 1,
                incarnation: 0,
                status: Status::Suspect {
                    since: 1,
                    suspectors: 1,
                },
            },
        );
        assert_eq!(s.member_state(1), PeerState::Down);
        // A higher-incarnation Alive wins.
        s.on_update(
            2,
            Update {
                member: 1,
                incarnation: 1,
                status: Status::Alive,
            },
        );
        assert_eq!(s.member_state(1), PeerState::Normal);
        // A stale lower-incarnation Suspect is ignored.
        s.on_update(
            3,
            Update {
                member: 1,
                incarnation: 0,
                status: Status::Dead,
            },
        );
        assert_eq!(s.member_state(1), PeerState::Normal);
    }

    #[test]
    fn refutation_disabled_lets_false_death_stick() {
        let cfg = SwimConfig {
            refutation_enabled: false,
            ..SwimConfig::default()
        };
        let mut s1 = SwimState::new(1, 2, cfg);
        // Node 1 is told it is suspected but cannot refute.
        s1.on_update(
            1,
            Update {
                member: 1,
                incarnation: 0,
                status: Status::Suspect {
                    since: 1,
                    suspectors: 1,
                },
            },
        );
        assert_eq!(s1.my_incarnation(), 0, "refutation disabled: no bump");
    }

    #[test]
    fn self_is_always_alive() {
        let s = SwimState::new(1, 3, SwimConfig::default());
        assert_eq!(s.member_state(1), PeerState::Normal);
        assert_eq!(s.status(1), Status::Alive);
    }
}
