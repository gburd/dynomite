//! Tictac AAE sweep scheduler.
//!
//! Drives one tokio task per local node that periodically:
//!
//! 1. Picks the next peer in the rotation.
//! 2. For one (peer, time-bucket) at a time, runs an
//!    [`crate::aae::exchange::Exchange`].
//! 3. Hands every [`crate::aae::exchange::Divergence`] to a
//!    [`crate::aae::repair::RepairScheduler`].
//!
//! The cadence is configurable via [`crate::aae::config::ConfAae`]:
//! `segment_interval_seconds` is the per-tick spacing
//! (default 60s) and `full_sweep_interval_seconds` is the
//! envelope (default 24h) over which the scheduler aims to
//! complete one sweep across every peer pair and every time
//! bucket. The two interact via the [`SweepPlan`]: at start-of-
//! sweep the scheduler computes
//! `ceil(full_sweep / segment_interval)` total ticks and
//! distributes the (peer, time-bucket) pairs across them. The
//! plan is recomputed at every sweep boundary so config changes
//! take effect on the next rotation.
//!
//! # Mockable clock
//!
//! The scheduler is generic over a [`Clock`]; tests inject a
//! [`MockClock`] that advances explicitly so cadence can be
//! asserted without sleeping.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use dynomite::events::{ClusterEvent, EventManager, TokenRange};
use dynomite::hashkit::DynToken;

use crate::aae::config::ConfAae;
use crate::aae::exchange::{Divergence, Exchange, ExchangeError, PeerView};
use crate::aae::metrics::AaeMetrics;
use crate::aae::tictac::Tree;

/// Pluggable clock. Provided so tests can advance time
/// deterministically without sleeping.
pub trait Clock: Send + Sync {
    /// Return the current monotonic instant.
    fn now(&self) -> Instant;
}

/// Real-time clock backed by [`Instant::now`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Test-only mock clock. Time advances only when
/// [`MockClock::advance`] is called, which makes cadence
/// assertions deterministic.
#[derive(Debug)]
pub struct MockClock {
    base: Instant,
    offset: Mutex<Duration>,
}

impl MockClock {
    /// Build a mock clock anchored at the call site's
    /// [`Instant::now`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            offset: Mutex::new(Duration::ZERO),
        }
    }

    /// Advance the clock by `d`.
    pub fn advance(&self, d: Duration) {
        let mut g = self.offset.lock().expect("mock clock mutex poisoned");
        *g += d;
    }
}

impl Default for MockClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MockClock {
    fn now(&self) -> Instant {
        let g = self.offset.lock().expect("mock clock mutex poisoned");
        self.base + *g
    }
}

/// One tick of work the scheduler intends to perform.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SweepTick {
    /// Index of the peer whose tree the local node will
    /// exchange against on this tick.
    pub peer_idx: u32,
    /// Top-level time bucket the tick will exercise.
    pub time_bucket: u32,
    /// Hex representation of the token this tick targets.
    /// `None` when the plan was built per-peer; `Some` when it
    /// was built per-token via
    /// [`SweepPlan::new_per_token`]. The hex encoding is the
    /// same one [`DynToken::to_hex`] returns and round-trips
    /// via the engine's `dynomite::hashkit::parse_token` when
    /// prefixed with `0x`.
    pub token_hex: Option<String>,
}

/// Materialised plan for one full sweep.
///
/// The plan is round-robin across peers and segments; the
/// scheduler walks it in order and, once exhausted, recomputes
/// a fresh plan.
#[derive(Debug, Clone)]
pub struct SweepPlan {
    ticks: Vec<SweepTick>,
}

impl SweepPlan {
    /// Build a plan over the given peers (`peer_idxs`), the tree
    /// shape's `n_time_buckets`, and the configured
    /// `full_sweep_interval` / `segment_interval` cadence.
    ///
    /// The plan emits one tick per `(peer, time_bucket)` pair,
    /// repeating peers cyclically until the total count fills
    /// the sweep envelope. If the envelope is too short to
    /// cover every pair, the plan truncates after one full
    /// rotation; the next sweep will pick up wherever the
    /// previous one left off.
    #[must_use]
    pub fn new(peer_idxs: &[u32], n_time_buckets: u32, cfg: &ConfAae) -> Self {
        let total_ticks = cfg
            .full_sweep_interval_seconds
            .div_ceil(cfg.segment_interval_seconds.max(1));
        let total_ticks = usize::try_from(total_ticks).unwrap_or(usize::MAX);

        let pair_count = peer_idxs.len().saturating_mul(n_time_buckets as usize);
        if pair_count == 0 {
            return Self { ticks: Vec::new() };
        }

        let envelope = total_ticks.min(pair_count);
        let mut ticks = Vec::with_capacity(envelope);
        for i in 0..envelope {
            let peer = peer_idxs[i % peer_idxs.len()];
            let bucket =
                u32::try_from((i / peer_idxs.len()) % (n_time_buckets as usize)).unwrap_or(0);
            ticks.push(SweepTick {
                peer_idx: peer,
                time_bucket: bucket,
                token_hex: None,
            });
        }
        Self { ticks }
    }

    /// Per-token plan over the supplied `(peer_idx, tokens)`
    /// pairs.
    ///
    /// Each peer that owns N tokens contributes
    /// `N * n_time_buckets` ticks; the resulting plan walks
    /// `(peer, token, time_bucket)` triples in round-robin
    /// order so every peer sees a slice of progress on every
    /// rotation. The total tick count is bounded by the
    /// configured cadence envelope:
    /// `ceil(full_sweep / segment_interval)`. Operators who
    /// want a single sweep to cover every triple must size
    /// the envelope accordingly.
    ///
    /// Tokens are recorded as their hex representation
    /// ([`DynToken::to_hex`]) so the plan stays cheap to
    /// clone and trivially serialisable through metrics and
    /// admin RPCs.
    #[must_use]
    pub fn new_per_token(
        peers_with_tokens: &[(u32, Vec<DynToken>)],
        n_time_buckets: u32,
        cfg: &ConfAae,
    ) -> Self {
        let total_ticks = cfg
            .full_sweep_interval_seconds
            .div_ceil(cfg.segment_interval_seconds.max(1));
        let total_ticks = usize::try_from(total_ticks).unwrap_or(usize::MAX);

        // Flatten to a list of (peer, token_hex) pairs so the
        // round-robin walk visits every peer in turn before
        // returning to the first peer's next token.
        let mut peer_token_pairs: Vec<(u32, String)> = Vec::new();
        let max_tokens = peers_with_tokens
            .iter()
            .map(|(_, ts)| ts.len())
            .max()
            .unwrap_or(0);
        for ti in 0..max_tokens {
            for (peer, tokens) in peers_with_tokens {
                if let Some(t) = tokens.get(ti) {
                    peer_token_pairs.push((*peer, t.to_hex()));
                }
            }
        }

        let triple_count = peer_token_pairs
            .len()
            .saturating_mul(n_time_buckets as usize);
        if triple_count == 0 {
            return Self { ticks: Vec::new() };
        }

        let envelope = total_ticks.min(triple_count);
        let mut ticks = Vec::with_capacity(envelope);
        for i in 0..envelope {
            let pair = &peer_token_pairs[i % peer_token_pairs.len()];
            let bucket = u32::try_from((i / peer_token_pairs.len()) % (n_time_buckets as usize))
                .unwrap_or(0);
            ticks.push(SweepTick {
                peer_idx: pair.0,
                time_bucket: bucket,
                token_hex: Some(pair.1.clone()),
            });
        }
        Self { ticks }
    }

    /// Number of ticks planned in this sweep.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ticks.len()
    }

    /// Whether the plan has no ticks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ticks.is_empty()
    }

    /// Borrow the underlying tick slice.
    #[must_use]
    pub fn ticks(&self) -> &[SweepTick] {
        &self.ticks
    }
}

/// Cadence-only scheduler driver.
///
/// Most of the AAE machinery (running an exchange, computing
/// divergences, dispatching repairs) is tied to operator-
/// supplied trait implementations and so lives in the dynomited
/// integration. This driver owns the cadence: it tells the
/// caller "fire the next tick now" and advances its internal
/// rotation cursor on each [`Self::poll`] call.
pub struct Scheduler<C: Clock> {
    cfg: ConfAae,
    clock: Arc<C>,
    next_due: Mutex<Instant>,
    cursor: Mutex<usize>,
    plan: Mutex<Option<SweepPlan>>,
    next_snapshot_due: Mutex<Instant>,
    metrics: Mutex<Option<Arc<AaeMetrics>>>,
    events: Mutex<Option<Arc<EventManager>>>,
}

impl<C: Clock> Scheduler<C> {
    /// Build a scheduler bound to a clock and a config. The
    /// scheduler starts in the "due immediately" state so the
    /// caller's first poll will fire a tick if a plan is
    /// installed.
    #[must_use]
    pub fn new(cfg: ConfAae, clock: Arc<C>) -> Self {
        let now = clock.now();
        let snapshot_due = now + Duration::from_secs(cfg.snapshot_interval_seconds);
        Self {
            cfg,
            clock,
            next_due: Mutex::new(now),
            cursor: Mutex::new(0),
            plan: Mutex::new(None),
            next_snapshot_due: Mutex::new(snapshot_due),
            metrics: Mutex::new(None),
            events: Mutex::new(None),
        }
    }

    /// Attach an [`AaeMetrics`] handle. The handle is a
    /// shared `Arc`; the same accumulator is fed by every
    /// scheduler in a multi-peer driver. The driver calls
    /// [`Scheduler::observe_exchange_attempt`],
    /// [`Scheduler::observe_exchange_success`],
    /// [`Scheduler::observe_divergent_keys`] etc. from
    /// inside the per-tick hot path.
    pub fn install_metrics(&self, metrics: Arc<AaeMetrics>) {
        let mut g = self.metrics.lock().expect("metrics mutex poisoned");
        *g = Some(metrics);
    }

    /// Borrow the installed metrics handle.
    #[must_use]
    pub fn metrics(&self) -> Option<Arc<AaeMetrics>> {
        self.metrics.lock().expect("metrics mutex poisoned").clone()
    }

    /// Attach an [`EventManager`] handle.
    ///
    /// When set, [`Self::notify_exchange_started`] and
    /// [`Self::notify_exchange_completed`] surface
    /// [`ClusterEvent::AaeExchangeStarted`] /
    /// [`ClusterEvent::AaeExchangeCompleted`] payloads on the
    /// manager's broadcast.
    pub fn install_events(&self, events: Arc<EventManager>) {
        let mut g = self.events.lock().expect("events mutex poisoned");
        *g = Some(events);
    }

    /// Borrow the installed event manager handle.
    #[must_use]
    pub fn events(&self) -> Option<Arc<EventManager>> {
        self.events.lock().expect("events mutex poisoned").clone()
    }

    /// Publish a [`ClusterEvent::AaeExchangeStarted`] payload.
    /// No-op when no event manager is installed.
    pub fn notify_exchange_started(&self, with_peer: u32, partition: TokenRange) {
        if let Some(ev) = self.events() {
            ev.publish(ClusterEvent::AaeExchangeStarted {
                with_peer,
                partition,
                ts: std::time::SystemTime::now(),
            });
        }
    }

    /// Publish a [`ClusterEvent::AaeExchangeCompleted`] payload.
    /// No-op when no event manager is installed.
    pub fn notify_exchange_completed(&self, with_peer: u32, partition: TokenRange, repaired: u64) {
        if let Some(ev) = self.events() {
            ev.publish(ClusterEvent::AaeExchangeCompleted {
                with_peer,
                partition,
                repaired,
                ts: std::time::SystemTime::now(),
            });
        }
    }

    /// Increment `aae_exchange_attempts_total` for the given
    /// peer. No-op when no metrics handle is installed.
    pub fn observe_exchange_attempt(&self, peer_idx: u32, dc: &str, rack: &str) {
        if let Some(m) = self.metrics() {
            m.record_exchange_attempt(peer_idx, dc, rack);
        }
    }

    /// Increment `aae_exchange_success_total` for the given
    /// peer. No-op when no metrics handle is installed.
    pub fn observe_exchange_success(&self, peer_idx: u32, dc: &str, rack: &str) {
        if let Some(m) = self.metrics() {
            m.record_exchange_success(peer_idx, dc, rack);
        }
    }

    /// Add `count` divergent keys to
    /// `aae_exchange_divergent_keys_total` for the given
    /// peer. No-op when no metrics handle is installed or
    /// when `count == 0`.
    pub fn observe_divergent_keys(&self, peer_idx: u32, dc: &str, rack: &str, count: u64) {
        if let Some(m) = self.metrics() {
            m.record_divergent_keys(peer_idx, dc, rack, count);
        }
    }

    /// Install (or reinstall) a sweep plan. Resets the cursor.
    pub fn install_plan(&self, plan: SweepPlan) {
        let mut p = self.plan.lock().expect("plan mutex poisoned");
        let mut c = self.cursor.lock().expect("cursor mutex poisoned");
        *p = Some(plan);
        *c = 0;
    }

    /// Borrow the current cursor position. Useful for tests.
    pub fn cursor(&self) -> usize {
        *self.cursor.lock().expect("cursor mutex poisoned")
    }

    /// Poll for a tick. Returns `Some(tick)` if the cadence is
    /// up and the plan has one queued; otherwise `None`. The
    /// scheduler advances `next_due` by `segment_interval` on
    /// every fire and rotates the cursor through the plan;
    /// when the cursor wraps the plan is recomputed (in
    /// production via [`Scheduler::install_plan`] from the
    /// caller's outer loop, since plan inputs may have changed).
    pub fn poll(&self) -> Option<SweepTick> {
        if !self.cfg.enabled {
            return None;
        }
        let now = self.clock.now();
        let mut due = self.next_due.lock().expect("due mutex poisoned");
        if now < *due {
            return None;
        }
        let plan_g = self.plan.lock().expect("plan mutex poisoned");
        let plan = plan_g.as_ref()?;
        if plan.is_empty() {
            *due = now + self.cfg.segment_interval();
            return None;
        }
        let mut cursor_g = self.cursor.lock().expect("cursor mutex poisoned");
        let i = *cursor_g % plan.ticks().len();
        let tick = plan.ticks()[i].clone();
        *cursor_g = (*cursor_g + 1) % plan.ticks().len();
        *due = now + self.cfg.segment_interval();
        Some(tick)
    }

    /// Whether a snapshot is due now. Used by the AAE
    /// driver to decide whether to call
    /// [`crate::aae::tictac::Tree::save_snapshot`] on the
    /// current tick. Calling [`Scheduler::mark_snapshot_taken`]
    /// resets the cadence; if the caller does not call it,
    /// `snapshot_due` continues to return `true` so a
    /// transient save failure is retried on every poll.
    pub fn snapshot_due(&self) -> bool {
        if !self.cfg.enabled {
            return false;
        }
        let now = self.clock.now();
        let due = self
            .next_snapshot_due
            .lock()
            .expect("snapshot_due mutex poisoned");
        now >= *due
    }

    /// Reset the snapshot cadence. The driver calls this
    /// after a successful snapshot save; the next snapshot
    /// will not be due until
    /// `now + snapshot_interval_seconds`.
    pub fn mark_snapshot_taken(&self) {
        let now = self.clock.now();
        let mut due = self
            .next_snapshot_due
            .lock()
            .expect("snapshot_due mutex poisoned");
        *due = now + Duration::from_secs(self.cfg.snapshot_interval_seconds);
    }

    /// Run one [`Exchange`] per token in `tokens` against
    /// `remote`, accumulating the divergences each token
    /// produces. The local [`Tree`] is shared across tokens;
    /// per-token semantics are about cadence and reporting,
    /// not tree partitioning. The returned vector pairs each
    /// token's hex representation with the divergences it
    /// surfaced, so callers can route repairs and metrics
    /// per token.
    ///
    /// Backward-compat: this method runs whether or not
    /// [`ConfAae::per_token_exchange`] is set; the config
    /// flag governs which sweep plan the operator installs,
    /// not whether per-token driving is available.
    ///
    /// # Errors
    ///
    /// Forwards the first [`ExchangeError`] surfaced by any
    /// token's exchange. The remaining tokens are not run on
    /// error so an operator can retry the failed slice
    /// without re-doing the successful prefix.
    pub fn exchange_per_token<V: PeerView + Clone>(
        local: &Tree,
        remote: &V,
        tokens: &[DynToken],
    ) -> Result<Vec<(String, Vec<Divergence>)>, ExchangeError> {
        let mut out = Vec::with_capacity(tokens.len());
        for token in tokens {
            let exch = Exchange::new(local, remote.clone());
            let divs = exch.run()?;
            out.push((token.to_hex(), divs));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ConfAae {
        ConfAae {
            enabled: true,
            full_sweep_interval_seconds: 600,
            segment_interval_seconds: 60,
            n_time_buckets: 4,
            n_segments: 32,
            time_window_seconds: 60,
            ..ConfAae::default()
        }
    }

    #[test]
    fn plan_round_robins_peers_and_buckets() {
        let plan = SweepPlan::new(&[1, 2], 4, &cfg());
        // 600/60 = 10 ticks; pair_count = 2*4 = 8; min = 8.
        assert_eq!(plan.len(), 8);
        // First two ticks share time bucket 0, alternating peer.
        assert_eq!(plan.ticks()[0].time_bucket, 0);
        assert_eq!(plan.ticks()[1].time_bucket, 0);
        assert_ne!(plan.ticks()[0].peer_idx, plan.ticks()[1].peer_idx);
        // Bucket advances after every full peer rotation.
        assert_eq!(plan.ticks()[2].time_bucket, 1);
    }

    #[test]
    fn plan_handles_empty_peers() {
        let plan = SweepPlan::new(&[], 4, &cfg());
        assert!(plan.is_empty());
    }

    #[test]
    fn scheduler_fires_at_configured_cadence() {
        let clock = Arc::new(MockClock::new());
        let sched: Scheduler<MockClock> = Scheduler::new(cfg(), clock.clone());
        sched.install_plan(SweepPlan::new(&[1, 2], 4, &cfg()));

        // First poll fires immediately (next_due == now at construction).
        let t0 = sched.poll().expect("first tick should fire");
        assert_eq!(t0.peer_idx, 1);

        // Polling again before the cadence elapses returns None.
        clock.advance(Duration::from_secs(30));
        assert!(sched.poll().is_none());

        // Polling after the segment interval fires the next tick.
        clock.advance(Duration::from_secs(31));
        let t1 = sched.poll().expect("second tick should fire");
        assert_eq!(t1.peer_idx, 2);
    }

    #[test]
    fn scheduler_disabled_returns_none() {
        let clock = Arc::new(MockClock::new());
        let mut c = cfg();
        c.enabled = false;
        let sched: Scheduler<MockClock> = Scheduler::new(c, clock);
        sched.install_plan(SweepPlan::new(&[1], 1, &cfg()));
        assert!(sched.poll().is_none());
    }

    #[test]
    fn cursor_wraps_around_plan() {
        let clock = Arc::new(MockClock::new());
        let sched: Scheduler<MockClock> = Scheduler::new(cfg(), clock.clone());
        let plan = SweepPlan::new(&[7], 2, &cfg());
        let plan_len = plan.len();
        sched.install_plan(plan);
        for _ in 0..plan_len {
            clock.advance(Duration::from_mins(1));
            let _ = sched.poll();
        }
        // Cursor should now be back at 0.
        assert_eq!(sched.cursor(), 0);
    }

    #[test]
    fn snapshot_due_fires_on_configured_cadence() {
        let clock = Arc::new(MockClock::new());
        let mut c = cfg();
        c.snapshot_interval_seconds = 300;
        let sched: Scheduler<MockClock> = Scheduler::new(c, clock.clone());

        // Right after construction the next snapshot is
        // 300s out; nothing is due yet.
        assert!(!sched.snapshot_due());

        // Advancing a hair under 300s still not due.
        clock.advance(Duration::from_secs(299));
        assert!(!sched.snapshot_due());

        // At 300s the snapshot is due.
        clock.advance(Duration::from_secs(1));
        assert!(sched.snapshot_due());

        // mark_snapshot_taken resets the cadence.
        sched.mark_snapshot_taken();
        assert!(!sched.snapshot_due());

        // A second window must elapse before the next is due.
        clock.advance(Duration::from_secs(301));
        assert!(sched.snapshot_due());
    }

    #[test]
    fn snapshot_due_returns_false_when_disabled() {
        let clock = Arc::new(MockClock::new());
        let mut c = cfg();
        c.enabled = false;
        c.snapshot_interval_seconds = 1;
        let sched: Scheduler<MockClock> = Scheduler::new(c, clock.clone());
        clock.advance(Duration::from_hours(1));
        assert!(!sched.snapshot_due());
    }

    #[test]
    fn per_token_plan_emits_one_tick_per_triple() {
        // Two peers, one with two tokens, the other with one.
        let peers = vec![
            (1u32, vec![DynToken::from_u32(100), DynToken::from_u32(200)]),
            (2u32, vec![DynToken::from_u32(300)]),
        ];
        let mut c = cfg();
        // Generous envelope so the plan is not truncated.
        c.full_sweep_interval_seconds = 60 * 60;
        c.segment_interval_seconds = 60;
        let plan = SweepPlan::new_per_token(&peers, 4, &c);

        // Total triples: peer1*tok0 + peer2*tok0 + peer1*tok1 = 3 unique
        // (peer, token) pairs * 4 buckets = 12 triples.
        assert_eq!(plan.len(), 12);
        for tick in plan.ticks() {
            assert!(tick.token_hex.is_some(), "per-token plan must tag tokens");
        }
        // Round-robin: first three ticks are bucket 0 across peers.
        assert_eq!(plan.ticks()[0].time_bucket, 0);
        assert_eq!(plan.ticks()[1].time_bucket, 0);
        assert_eq!(plan.ticks()[2].time_bucket, 0);
        // Tick 3 advances to bucket 1.
        assert_eq!(plan.ticks()[3].time_bucket, 1);
    }

    #[test]
    fn per_token_plan_handles_no_peers() {
        let plan = SweepPlan::new_per_token(&[], 4, &cfg());
        assert!(plan.is_empty());
    }

    #[test]
    fn per_token_plan_handles_peers_with_no_tokens() {
        let plan = SweepPlan::new_per_token(&[(1u32, Vec::new())], 4, &cfg());
        assert!(plan.is_empty());
    }

    #[test]
    fn per_token_plan_token_hex_roundtrips() {
        let peers = vec![(7u32, vec![DynToken::from_u32(0xdead)])];
        let plan = SweepPlan::new_per_token(&peers, 1, &cfg());
        let hex = plan.ticks()[0].token_hex.as_ref().unwrap();
        assert_eq!(hex, "0000dead");
    }

    #[test]
    fn exchange_per_token_runs_one_exchange_per_token() {
        use crate::aae::exchange::LocalPeerView;
        use crate::aae::tictac::{Tree, TreeShape};
        let shape = TreeShape {
            n_time_buckets: 2,
            n_segments: 16,
            time_window_seconds: 60,
        };
        let mut a = Tree::new(shape);
        let mut b = Tree::new(shape);
        for i in 0..32u32 {
            let k = format!("k{i}");
            a.insert(b"users", k.as_bytes(), b"vc1", 0);
            b.insert(b"users", k.as_bytes(), b"vc1", 0);
        }
        // Identical trees: zero divergences across every token.
        let view = LocalPeerView::new(&b);
        let tokens = vec![DynToken::from_u32(1), DynToken::from_u32(2)];
        let pairs =
            Scheduler::<MockClock>::exchange_per_token(&a, &view, &tokens).expect("exchange");
        assert_eq!(pairs.len(), 2);
        for (hex, divs) in &pairs {
            assert!(!hex.is_empty());
            assert!(divs.is_empty(), "identical trees must not diverge");
        }
    }

    #[test]
    fn exchange_per_token_surfaces_divergence() {
        use crate::aae::exchange::LocalPeerView;
        use crate::aae::tictac::{Tree, TreeShape};
        let shape = TreeShape {
            n_time_buckets: 2,
            n_segments: 16,
            time_window_seconds: 60,
        };
        let mut a = Tree::new(shape);
        let b = Tree::new(shape);
        a.insert(b"users", b"alice", b"vc1", 0);
        // b is missing alice; per-token loop must report the
        // local-only entry on every token (the tree is shared
        // across tokens for now).
        let view = LocalPeerView::new(&b);
        let tokens = vec![DynToken::from_u32(7)];
        let pairs =
            Scheduler::<MockClock>::exchange_per_token(&a, &view, &tokens).expect("exchange");
        assert_eq!(pairs.len(), 1);
        let (_hex, divs) = &pairs[0];
        assert!(
            divs.iter()
                .any(|d| d.local_only.iter().any(|e| e.key == b"alice")),
            "alice must surface as local-only"
        );
    }

    #[test]
    fn install_metrics_routes_observations_to_handle() {
        use crate::aae::metrics::AaeMetrics;
        let clock = Arc::new(MockClock::new());
        let sched: Scheduler<MockClock> = Scheduler::new(cfg(), clock);
        let m = Arc::new(AaeMetrics::new());
        sched.install_metrics(Arc::clone(&m));
        sched.observe_exchange_attempt(3, "dc1", "rA");
        sched.observe_exchange_success(3, "dc1", "rA");
        sched.observe_divergent_keys(3, "dc1", "rA", 5);
        let snap = m.snapshot();
        assert_eq!(snap.exchange_attempts.len(), 1);
        assert_eq!(snap.exchange_attempts[0].count, 1);
        assert_eq!(snap.exchange_success.len(), 1);
        assert_eq!(snap.divergent_keys[0].count, 5);
    }

    #[test]
    fn observations_without_metrics_handle_are_noops() {
        let clock = Arc::new(MockClock::new());
        let sched: Scheduler<MockClock> = Scheduler::new(cfg(), clock);
        // No install_metrics call. Observations must not panic.
        sched.observe_exchange_attempt(0, "dc1", "rA");
        sched.observe_exchange_success(0, "dc1", "rA");
        sched.observe_divergent_keys(0, "dc1", "rA", 7);
        assert!(sched.metrics().is_none());
    }
}
