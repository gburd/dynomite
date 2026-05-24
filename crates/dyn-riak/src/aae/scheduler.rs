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

use crate::aae::config::ConfAae;

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
/// rotation cursor on each [`Scheduler::tick`] call.
pub struct Scheduler<C: Clock> {
    cfg: ConfAae,
    clock: Arc<C>,
    next_due: Mutex<Instant>,
    cursor: Mutex<usize>,
    plan: Mutex<Option<SweepPlan>>,
}

impl<C: Clock> Scheduler<C> {
    /// Build a scheduler bound to a clock and a config. The
    /// scheduler starts in the "due immediately" state so the
    /// caller's first poll will fire a tick if a plan is
    /// installed.
    #[must_use]
    pub fn new(cfg: ConfAae, clock: Arc<C>) -> Self {
        let now = clock.now();
        Self {
            cfg,
            clock,
            next_due: Mutex::new(now),
            cursor: Mutex::new(0),
            plan: Mutex::new(None),
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
            clock.advance(Duration::from_secs(60));
            let _ = sched.poll();
        }
        // Cursor should now be back at 0.
        assert_eq!(sched.cursor(), 0);
    }
}
