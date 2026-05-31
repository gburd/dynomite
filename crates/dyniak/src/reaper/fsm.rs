//! Reaper coordinator FSM.
//!
//! Drives one (`bucket`, `partitions`) reap session through
//! three protocol states: [`State::Idle`] (resting between
//! cycles), [`State::Scanning`] (walking partitions and
//! collecting reap candidates), [`State::Reaping`] (asking the
//! datastore to delete each candidate). The FSM cycles forever:
//! every [`State::Reaping`] -> [`State::Idle`] transition
//! re-arms the [`ReaperConfig::reap_interval_seconds`] state
//! timer that will trigger the next [`Event::Tick`].
//!
//! # Why a state machine
//!
//! A naive periodic sweep can be expressed as a `tokio::spawn`
//! plus a `tokio::time::interval`. We model the loop as an FSM
//! anyway because the per-bucket policy mixes three concerns
//! that interact:
//!
//! * **Per-cycle budget**. The handler must cap the batch at
//!   [`ReaperConfig::reap_max_per_cycle`] across an arbitrary
//!   number of partitions. A state-functions handler makes that
//!   cap a property of the [`State::Scanning`] arm rather than
//!   a side condition scattered through async code.
//! * **Throttling**. The reap step takes a token from a
//!   [`Throttle`] bucket per key. With a state machine the
//!   admission gate sits next to the state that needs it.
//! * **Auditability**. Every cycle ends with an explicit
//!   [`ReaperCycleComplete`] record. Modeling the cycle as a
//!   state graph makes "did we emit exactly one audit event per
//!   cycle?" an invariant the tests can inspect rather than a
//!   shape-of-the-code property.
//!
//! # Idempotency
//!
//! See the module-level documentation in [`super`] for the
//! full idempotency story. In short: the FSM accepts surplus
//! [`Event::KeyReaped`] events without complaining, the batch
//! is rebuilt from scratch each cycle, and missing keys count
//! as reaped from the datastore's perspective.

use std::time::{Duration, Instant};

use dynomite::cluster::apl::{get_apl_ann, ClusterState, NodeRole, RingPoint};
use dynomite::embed::events::PeerId;
use dynomite::events::TokenRange;
use dynomite::hashkit::DynToken;
use gen_fsm::{Action, EventType, FsmHandler, TimeoutKind, Transition};
use throttle_core::{SystemClock, Throttle};

/// Default minimum tombstone age (in seconds) before a
/// tombstone becomes eligible for reaping. Mirrors Riak KV's
/// `delete_mode = keep` policy with a one-day grace window.
pub const DEFAULT_REAP_TOMBSTONES_AFTER_SECONDS: u64 = 86_400;

/// Default minimum sibling age (in seconds) before an orphaned
/// sibling is reaped. Conservative so a slow client that has
/// not finished resolving a conflict still has time to issue
/// a put with a non-stale vclock.
pub const DEFAULT_REAP_SIBLINGS_AFTER_SECONDS: u64 = 604_800;

/// Default per-cycle batch ceiling. Bounded so a freshly-
/// upgraded operator who turned the reaper on for an existing
/// bucket does not melt the cluster on the first sweep.
pub const DEFAULT_REAP_MAX_PER_CYCLE: u64 = 10_000;

/// Default wall-clock period between cycles, in seconds.
/// 5 minutes matches the upstream Riak KV default for
/// `reap_sweep_interval`.
pub const DEFAULT_REAP_INTERVAL_SECONDS: u64 = 300;

/// Default number of reap calls admitted per second. 100/s at
/// the default batch ceiling of 10k keys gives a worst-case
/// cycle length of ~100s, well below the 5 minute cycle
/// interval.
pub const DEFAULT_REAPS_PER_SEC: u64 = 100;

/// Per-bucket-type reaper policy.
///
/// Operators tune one [`ReaperConfig`] per bucket type. The
/// production wiring loads these from the bucket-properties
/// registry; tests construct them directly.
///
/// # Examples
///
/// ```
/// use dyniak::reaper::ReaperConfig;
///
/// let cfg = ReaperConfig::default();
/// assert!(cfg.reap_max_per_cycle > 0);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaperConfig {
    /// Minimum tombstone age (in seconds) before the FSM
    /// queues the key for reaping. A value of `0` disables
    /// the age check (every tombstone is eligible).
    pub reap_tombstones_after_seconds: u64,
    /// Minimum sibling age (in seconds) before the FSM
    /// queues the orphaned sibling for eviction. A value of
    /// `0` disables the age check.
    pub reap_siblings_after_seconds: u64,
    /// Per-cycle batch ceiling. The FSM stops queuing
    /// candidates once it has collected this many; surplus
    /// candidates are dropped on the floor and re-discovered
    /// next cycle.
    pub reap_max_per_cycle: u64,
    /// Wall-clock period between [`Event::Tick`] events. The
    /// FSM arms a state timeout of this length on entry to
    /// [`State::Idle`].
    pub reap_interval_seconds: u64,
    /// Sustained reap admission rate, in keys per second.
    /// The throttle is consulted by
    /// [`ReaperHandler::try_admit_reap`] before the
    /// orchestrator issues each `riak_delete`.
    pub reaps_per_sec: u64,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            reap_tombstones_after_seconds: DEFAULT_REAP_TOMBSTONES_AFTER_SECONDS,
            reap_siblings_after_seconds: DEFAULT_REAP_SIBLINGS_AFTER_SECONDS,
            reap_max_per_cycle: DEFAULT_REAP_MAX_PER_CYCLE,
            reap_interval_seconds: DEFAULT_REAP_INTERVAL_SECONDS,
            reaps_per_sec: DEFAULT_REAPS_PER_SEC,
        }
    }
}

/// Classification of a key emerging from the partition scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KeyKind {
    /// A live object. Never reaped, regardless of age.
    Live,
    /// A tombstone. Reaped when its age exceeds
    /// [`ReaperConfig::reap_tombstones_after_seconds`].
    Tombstone,
    /// An orphaned sibling. Reaped when its age exceeds
    /// [`ReaperConfig::reap_siblings_after_seconds`].
    Sibling,
}

/// One key surfaced by the orchestrator's partition scan.
///
/// The FSM consults the [`ScannedKey::kind`] and
/// [`ScannedKey::age`] fields to decide whether the key is a
/// reap candidate. The bucket and key bytes are stored opaquely
/// so the FSM can pass them back through [`ReaperHandler::take_batch`]
/// without owning a copy of the bucket-properties registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannedKey {
    /// Index into [`ReaperHandler::partitions`] this key was
    /// drawn from. Surfaced for diagnostics; the FSM does not
    /// branch on it.
    pub partition_idx: usize,
    /// The raw key bytes.
    pub key: Vec<u8>,
    /// Whether the key is a live object, a tombstone, or a
    /// dangling sibling.
    pub kind: KeyKind,
    /// Wall-clock age of the key. The orchestrator computes
    /// this from the storage engine's last-modified timestamp.
    pub age: Duration,
}

/// Audit record emitted at the end of every reap cycle.
///
/// The FSM stores the most recent record in
/// [`ReaperHandler::last_complete`]; the orchestrator drains it
/// via [`ReaperHandler::take_last_complete`] and re-publishes
/// onto the cluster-wide [`dynomite::events::EventManager`].
/// Tests assert on the record directly without spinning up an
/// event manager.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaperCycleComplete {
    /// Bucket the cycle ran against.
    pub bucket: Vec<u8>,
    /// Number of keys reaped during the cycle. Equal to the
    /// number of [`Event::KeyReaped`] events accepted while in
    /// [`State::Reaping`], capped by
    /// [`ReaperConfig::reap_max_per_cycle`].
    pub reaped: u64,
    /// Number of keys scanned during the cycle. Includes live
    /// keys, tombstones, and siblings (whether or not they were
    /// queued for reap).
    pub scanned: u64,
    /// Wall-clock duration from the [`Event::Tick`] that
    /// started the cycle to the [`Event::BatchAcked`] that
    /// ended it.
    pub duration: Duration,
}

/// Outcome reported when the FSM stops.
///
/// The reaper is a long-lived loop; in production the only way
/// to stop it is to drop the driver. This variant exists so
/// integration tests can drive the FSM through a single cycle
/// and then assert on a clean shutdown.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReaperOutcome {
    /// The orchestrator asked the FSM to shut down between
    /// cycles. No partial-cycle work outstanding.
    Stopped,
}

/// Protocol states for the reaper coordinator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum State {
    /// Resting between cycles. Entry arms the
    /// [`ReaperConfig::reap_interval_seconds`] state timer
    /// that fires the next [`Event::Tick`].
    Idle,
    /// Walking the partition list and collecting reap
    /// candidates. The handler tracks the current partition
    /// index and the per-cycle budget.
    Scanning,
    /// Draining the candidate batch through the datastore.
    /// Each [`Event::KeyReaped`] decrements the outstanding
    /// counter; [`Event::BatchAcked`] confirms the batch is
    /// drained and returns the FSM to [`State::Idle`].
    Reaping,
}

/// Events accepted by [`ReaperHandler::handle`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// Wall-clock period elapsed; start a new cycle. Accepted
    /// only in [`State::Idle`]; ignored elsewhere so a stray
    /// late tick cannot interleave with an in-flight cycle.
    Tick,
    /// One key emerged from the partition scan. The FSM
    /// classifies the key against [`ReaperConfig`] and (if
    /// eligible) appends it to the batch.
    KeyScanned(ScannedKey),
    /// Orchestrator finished one segment of the current
    /// partition. Bumps the partition cursor; if the cursor
    /// has reached the end of the partition list the FSM
    /// transitions to [`State::Reaping`].
    NextSegmentDone,
    /// Orchestrator successfully removed (or skipped, in the
    /// idempotent "already-gone" case) one key from the
    /// batch. Increments the reaped counter.
    KeyReaped,
    /// Orchestrator drained the entire batch. Transitions the
    /// FSM back to [`State::Idle`] and emits the per-cycle
    /// audit record.
    BatchAcked,
    /// Orchestrator hit a fatal error mid-cycle. The FSM
    /// emits a partial-cycle audit event and returns to
    /// [`State::Idle`]. The next [`Event::Tick`] starts a
    /// fresh cycle; the dropped tail will reappear in the
    /// next scan.
    CycleError(String),
    /// Orchestrator shutdown signal. Stops the FSM from any
    /// state. Used by integration tests; production wiring
    /// drops the driver instead.
    Shutdown,
}

/// Reaper coordinator FSM state.
///
/// Owns the per-bucket policy, the partition list this peer
/// is primary for, the current cycle's batch, and the rate-
/// limit throttle. The handler is constructed once per bucket
/// type and reused across cycles.
pub struct ReaperHandler {
    bucket: Vec<u8>,
    config: ReaperConfig,
    partitions: Vec<TokenRange>,
    throttle: Throttle<SystemClock>,
    /// Index into [`Self::partitions`] of the partition the
    /// scanner is currently walking. Reset to `0` on every
    /// [`Event::Tick`].
    partition_idx: usize,
    /// Reap candidates accumulated this cycle. Bounded by
    /// [`ReaperConfig::reap_max_per_cycle`].
    batch: Vec<ScannedKey>,
    /// Number of keys reaped this cycle so far. Reset on
    /// every [`Event::Tick`].
    reaped_this_cycle: u64,
    /// Number of keys scanned this cycle so far. Reset on
    /// every [`Event::Tick`].
    scanned_this_cycle: u64,
    /// Number of [`Event::KeyReaped`] events still expected
    /// before the batch is drained. Reset to `batch.len()` on
    /// entry to [`State::Reaping`].
    outstanding_reaps: u64,
    /// Wall-clock instant at which the current cycle started,
    /// or `None` when the FSM is in [`State::Idle`].
    cycle_started_at: Option<Instant>,
    /// Most recently completed cycle's audit record. Cleared
    /// by [`Self::take_last_complete`].
    last_complete: Option<ReaperCycleComplete>,
    /// Last reason recorded by [`Event::CycleError`]; cleared
    /// on [`Event::Tick`].
    last_error: Option<String>,
    last_state: State,
}

impl ReaperHandler {
    /// Build a reaper for `bucket` with default policy and an
    /// empty partition list. Call
    /// [`Self::with_partitions`] before driving the FSM or
    /// every cycle will scan zero keys.
    ///
    /// # Examples
    ///
    /// ```
    /// use dyniak::reaper::ReaperHandler;
    /// let h = ReaperHandler::new(b"users".to_vec());
    /// assert_eq!(h.bucket(), b"users");
    /// assert!(h.partitions().is_empty());
    /// ```
    #[must_use]
    pub fn new(bucket: Vec<u8>) -> Self {
        Self::with_config(bucket, ReaperConfig::default())
    }

    /// Build a reaper for `bucket` with explicit policy.
    ///
    /// The throttle is sized off [`ReaperConfig::reaps_per_sec`].
    /// A configured rate of zero is clamped up to one to keep
    /// the bucket arithmetic monotonic; operators who want the
    /// reaper off should drop the driver, not zero the rate.
    ///
    /// # Examples
    ///
    /// ```
    /// use dyniak::reaper::{ReaperConfig, ReaperHandler};
    /// let cfg = ReaperConfig {
    ///     reap_max_per_cycle: 5,
    ///     ..ReaperConfig::default()
    /// };
    /// let h = ReaperHandler::with_config(b"orders".to_vec(), cfg);
    /// assert_eq!(h.config().reap_max_per_cycle, 5);
    /// ```
    #[must_use]
    pub fn with_config(bucket: Vec<u8>, config: ReaperConfig) -> Self {
        let rate = config.reaps_per_sec.max(1);
        let throttle = Throttle::new(rate, rate);
        Self {
            bucket,
            config,
            partitions: Vec::new(),
            throttle,
            partition_idx: 0,
            batch: Vec::new(),
            reaped_this_cycle: 0,
            scanned_this_cycle: 0,
            outstanding_reaps: 0,
            cycle_started_at: None,
            last_complete: None,
            last_error: None,
            last_state: State::Idle,
        }
    }

    /// Replace the partition list. Typically called by the
    /// orchestrator on every [`State::Scanning`] entry, after
    /// re-deriving the primary-owned ranges from the live
    /// [`get_apl_ann`] view.
    #[must_use]
    pub fn with_partitions(mut self, partitions: Vec<TokenRange>) -> Self {
        self.partitions = partitions;
        self
    }

    /// Replace the partition list in place.
    pub fn set_partitions(&mut self, partitions: Vec<TokenRange>) {
        self.partitions = partitions;
    }

    /// Re-derive the partition list from the current cluster
    /// view, retaining only ring slices this peer is the
    /// primary owner for.
    ///
    /// `local_peer` is the peer id this reaper is running on;
    /// `n` is the bucket-type's `n_val`. The function calls
    /// [`get_apl_ann`] for the start token of each ring entry
    /// and keeps the slices whose first [`NodeRole::Primary`]
    /// slot is `local_peer`.
    ///
    /// The cluster ring stores tokens as `u64` while the
    /// engine-wide [`DynToken`] continuum is built from
    /// `u32`-wide words; values larger than [`u32::MAX`] are
    /// saturated when populating the [`TokenRange`] bounds.
    /// The reaper does not consult the bound values for
    /// routing (it passes them to the orchestrator's scanner
    /// opaquely), so the saturation is informational.
    pub fn refresh_partitions_from_cluster(
        &mut self,
        cluster: &ClusterState,
        local_peer: PeerId,
        n: usize,
    ) {
        let ring = cluster.ring();
        if ring.is_empty() {
            self.partitions.clear();
            return;
        }
        let mut out: Vec<TokenRange> = Vec::new();
        for (idx, point) in ring.iter().enumerate() {
            let apl = get_apl_ann(cluster, point.token, n);
            let is_primary = apl
                .iter()
                .any(|p| p.peer_id == local_peer && p.role == NodeRole::Primary);
            if !is_primary {
                continue;
            }
            let next = (idx + 1) % ring.len();
            let start = ring_token_to_dyntoken(point);
            let end = ring_token_to_dyntoken(&ring[next]);
            out.push(TokenRange::new(start, end));
        }
        self.partitions = out;
    }

    /// Bucket bytes this reaper runs against.
    #[must_use]
    pub fn bucket(&self) -> &[u8] {
        &self.bucket
    }

    /// Active per-bucket-type policy.
    #[must_use]
    pub const fn config(&self) -> &ReaperConfig {
        &self.config
    }

    /// Read-only view of the partition list.
    #[must_use]
    pub fn partitions(&self) -> &[TokenRange] {
        &self.partitions
    }

    /// Index of the partition the scanner is currently walking.
    #[must_use]
    pub const fn partition_idx(&self) -> usize {
        self.partition_idx
    }

    /// The token range the scanner is currently walking, or
    /// `None` when the scan is complete.
    #[must_use]
    pub fn current_partition(&self) -> Option<&TokenRange> {
        self.partitions.get(self.partition_idx)
    }

    /// Number of reap candidates collected so far this cycle.
    #[must_use]
    pub fn batch_len(&self) -> usize {
        self.batch.len()
    }

    /// Number of [`Event::KeyReaped`] events still expected.
    #[must_use]
    pub const fn outstanding_reaps(&self) -> u64 {
        self.outstanding_reaps
    }

    /// Number of keys reaped so far this cycle.
    #[must_use]
    pub const fn reaped_this_cycle(&self) -> u64 {
        self.reaped_this_cycle
    }

    /// Number of keys scanned so far this cycle.
    #[must_use]
    pub const fn scanned_this_cycle(&self) -> u64 {
        self.scanned_this_cycle
    }

    /// Last [`State`] the FSM was in. Mirrors the field of
    /// the same name on [`super::HandoffHandler`] in the
    /// handoff module; surfaced for diagnostics.
    #[must_use]
    pub const fn last_state(&self) -> State {
        self.last_state
    }

    /// Borrow the most recent cycle's audit record without
    /// consuming it.
    #[must_use]
    pub const fn last_complete(&self) -> Option<&ReaperCycleComplete> {
        self.last_complete.as_ref()
    }

    /// Take ownership of the most recent cycle's audit record,
    /// leaving `None` behind. Returns `None` if no cycle has
    /// completed since the last call.
    pub fn take_last_complete(&mut self) -> Option<ReaperCycleComplete> {
        self.last_complete.take()
    }

    /// Drain the candidate batch. Used by the orchestrator on
    /// entry to [`State::Reaping`] to pick up the keys it
    /// needs to delete.
    ///
    /// Returns an empty vector if the batch is already empty.
    pub fn take_batch(&mut self) -> Vec<ScannedKey> {
        std::mem::take(&mut self.batch)
    }

    /// Try to take one token from the rate-limit throttle.
    /// The orchestrator polls this before issuing each
    /// `riak_delete`; on `false` it should park and retry
    /// after a refill.
    pub fn try_admit_reap(&self) -> bool {
        self.throttle.try_acquire(1)
    }

    /// Wall-clock duration since the current cycle started,
    /// or [`Duration::ZERO`] when the FSM is in
    /// [`State::Idle`].
    #[must_use]
    pub fn cycle_elapsed(&self) -> Duration {
        match self.cycle_started_at {
            Some(t) => t.elapsed(),
            None => Duration::ZERO,
        }
    }

    /// Decide whether `key` is a reap candidate under the
    /// active policy.
    ///
    /// A key is a candidate when:
    ///
    /// * it is a [`KeyKind::Tombstone`] and its age exceeds
    ///   [`ReaperConfig::reap_tombstones_after_seconds`]; or
    /// * it is a [`KeyKind::Sibling`] and its age exceeds
    ///   [`ReaperConfig::reap_siblings_after_seconds`].
    ///
    /// Live keys are never candidates.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use dyniak::reaper::{KeyKind, ReaperConfig, ReaperHandler, ScannedKey};
    ///
    /// let mut cfg = ReaperConfig::default();
    /// cfg.reap_tombstones_after_seconds = 10;
    /// let h = ReaperHandler::with_config(b"b".to_vec(), cfg);
    /// let young = ScannedKey {
    ///     partition_idx: 0,
    ///     key: b"k".to_vec(),
    ///     kind: KeyKind::Tombstone,
    ///     age: Duration::from_secs(5),
    /// };
    /// let old = ScannedKey {
    ///     partition_idx: 0,
    ///     key: b"k".to_vec(),
    ///     kind: KeyKind::Tombstone,
    ///     age: Duration::from_secs(20),
    /// };
    /// assert!(!h.is_reap_candidate(&young));
    /// assert!(h.is_reap_candidate(&old));
    /// ```
    #[must_use]
    pub fn is_reap_candidate(&self, key: &ScannedKey) -> bool {
        match key.kind {
            KeyKind::Live => false,
            KeyKind::Tombstone => {
                key.age >= Duration::from_secs(self.config.reap_tombstones_after_seconds)
            }
            KeyKind::Sibling => {
                key.age >= Duration::from_secs(self.config.reap_siblings_after_seconds)
            }
        }
    }

    fn record_state(&mut self, state: State) {
        self.last_state = state;
    }

    fn idle_state_timeout(&self) -> Duration {
        Duration::from_secs(self.config.reap_interval_seconds.max(1))
    }

    /// Reset per-cycle counters. Called on every
    /// [`Event::Tick`].
    fn begin_cycle(&mut self) {
        self.partition_idx = 0;
        self.batch.clear();
        self.reaped_this_cycle = 0;
        self.scanned_this_cycle = 0;
        self.outstanding_reaps = 0;
        self.cycle_started_at = Some(Instant::now());
        self.last_error = None;
    }

    /// Build the audit record for the current cycle and stash
    /// it in [`Self::last_complete`].
    fn finish_cycle(&mut self) {
        let duration = self
            .cycle_started_at
            .map_or(Duration::ZERO, |t| t.elapsed());
        self.last_complete = Some(ReaperCycleComplete {
            bucket: self.bucket.clone(),
            reaped: self.reaped_this_cycle,
            scanned: self.scanned_this_cycle,
            duration,
        });
        self.cycle_started_at = None;
    }

    fn handle_idle(&mut self, ev: &Event) -> Transition<Self> {
        match ev {
            Event::Tick => {
                if self.partitions.is_empty() {
                    // Nothing to scan: emit an empty audit
                    // record and stay in Idle. Re-arm the
                    // tick timer so the next cycle can pick
                    // up new partitions if the orchestrator
                    // installs them.
                    self.begin_cycle();
                    self.finish_cycle();
                    return Transition::Keep(vec![Action::set_state_timeout(
                        self.idle_state_timeout(),
                    )]);
                }
                self.begin_cycle();
                Transition::Next(State::Scanning, vec![])
            }
            Event::Shutdown => Transition::Stop(ReaperOutcome::Stopped),
            // Stale events from a previous cycle are dropped
            // silently.
            _ => Transition::Keep(vec![]),
        }
    }

    fn handle_scanning(&mut self, ev: Event) -> Transition<Self> {
        match ev {
            Event::KeyScanned(key) => {
                self.scanned_this_cycle = self.scanned_this_cycle.saturating_add(1);
                if self.batch.len() as u64 >= self.config.reap_max_per_cycle {
                    // Per-cycle budget exhausted. Drop the
                    // candidate; it reappears next cycle.
                    return Transition::Keep(vec![]);
                }
                if self.is_reap_candidate(&key) {
                    self.batch.push(key);
                }
                Transition::Keep(vec![])
            }
            Event::NextSegmentDone => {
                self.partition_idx = self.partition_idx.saturating_add(1);
                if self.partition_idx >= self.partitions.len() {
                    // All partitions walked. Lock in the
                    // candidate set and advance to Reaping.
                    self.outstanding_reaps = self.batch.len() as u64;
                    if self.outstanding_reaps == 0 {
                        // Nothing to reap. Skip the Reaping
                        // state entirely and emit the audit
                        // record.
                        self.finish_cycle();
                        return Transition::Next(
                            State::Idle,
                            vec![Action::set_state_timeout(self.idle_state_timeout())],
                        );
                    }
                    return Transition::Next(State::Reaping, vec![]);
                }
                Transition::Keep(vec![])
            }
            Event::CycleError(reason) => {
                self.last_error = Some(reason);
                self.finish_cycle();
                Transition::Next(
                    State::Idle,
                    vec![Action::set_state_timeout(self.idle_state_timeout())],
                )
            }
            Event::Shutdown => Transition::Stop(ReaperOutcome::Stopped),
            // Stale Tick / KeyReaped / BatchAcked dropped.
            _ => Transition::Keep(vec![]),
        }
    }

    fn handle_reaping(&mut self, ev: Event) -> Transition<Self> {
        match ev {
            Event::KeyReaped => {
                self.reaped_this_cycle = self.reaped_this_cycle.saturating_add(1);
                self.outstanding_reaps = self.outstanding_reaps.saturating_sub(1);
                Transition::Keep(vec![])
            }
            Event::BatchAcked => {
                self.finish_cycle();
                Transition::Next(
                    State::Idle,
                    vec![Action::set_state_timeout(self.idle_state_timeout())],
                )
            }
            Event::CycleError(reason) => {
                self.last_error = Some(reason);
                self.finish_cycle();
                Transition::Next(
                    State::Idle,
                    vec![Action::set_state_timeout(self.idle_state_timeout())],
                )
            }
            Event::Shutdown => Transition::Stop(ReaperOutcome::Stopped),
            // Stale Tick / KeyScanned / NextSegmentDone
            // dropped.
            _ => Transition::Keep(vec![]),
        }
    }
}

/// Convert a ring-side `u64` token into a [`DynToken`] in the
/// engine-wide `u32` continuum. Values larger than
/// [`u32::MAX`] saturate; see
/// [`ReaperHandler::refresh_partitions_from_cluster`] for the
/// rationale.
fn ring_token_to_dyntoken(point: &RingPoint) -> DynToken {
    let narrow = u32::try_from(point.token).unwrap_or(u32::MAX);
    DynToken::from_u32(narrow)
}

impl FsmHandler for ReaperHandler {
    type State = State;
    type Event = Event;
    type Reply = ();
    type Stop = ReaperOutcome;

    fn initial(&self) -> State {
        State::Idle
    }

    fn on_enter(&mut self, state: State) -> Transition<Self> {
        self.record_state(state);
        match state {
            State::Idle => {
                Transition::Keep(vec![Action::set_state_timeout(self.idle_state_timeout())])
            }
            State::Scanning | State::Reaping => Transition::Keep(vec![]),
        }
    }

    fn handle(&mut self, state: State, _et: EventType, ev: Event) -> Transition<Self> {
        self.record_state(state);
        match state {
            State::Idle => self.handle_idle(&ev),
            State::Scanning => self.handle_scanning(ev),
            State::Reaping => self.handle_reaping(ev),
        }
    }

    fn on_timeout(&mut self, state: State, kind: TimeoutKind) -> Transition<Self> {
        self.record_state(state);
        // The only timer we arm is the Idle state timer; on
        // its expiry we synthesize an internal Tick.
        if state == State::Idle && matches!(kind, TimeoutKind::State) {
            return Transition::Keep(vec![Action::post_internal(Event::Tick)]);
        }
        Transition::Keep(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ReaperConfig {
        ReaperConfig {
            reap_tombstones_after_seconds: 10,
            reap_siblings_after_seconds: 100,
            reap_max_per_cycle: 4,
            reap_interval_seconds: 60,
            reaps_per_sec: 1_000_000,
        }
    }

    fn handler() -> ReaperHandler {
        ReaperHandler::with_config(b"users".to_vec(), cfg()).with_partitions(vec![
            TokenRange::new(DynToken::from_u32(0), DynToken::from_u32(100)),
            TokenRange::new(DynToken::from_u32(100), DynToken::from_u32(200)),
        ])
    }

    fn key(idx: usize, kind: KeyKind, age_secs: u64) -> ScannedKey {
        ScannedKey {
            partition_idx: idx,
            key: format!("k{idx}-{age_secs}").into_bytes(),
            kind,
            age: Duration::from_secs(age_secs),
        }
    }

    #[test]
    fn idle_entry_arms_state_timeout() {
        let mut h = handler();
        let t = h.on_enter(State::Idle);
        match t {
            Transition::Keep(actions) => {
                let secs = cfg().reap_interval_seconds;
                let found = actions.iter().any(
                    |a| matches!(a, Action::SetStateTimeout(d) if *d == Duration::from_secs(secs)),
                );
                assert!(found, "expected SetStateTimeout; got {actions:?}");
            }
            other => panic!("expected Keep, got {other:?}"),
        }
    }

    #[test]
    fn idle_tick_with_empty_partitions_stays_idle() {
        let mut h = ReaperHandler::with_config(b"empty".to_vec(), cfg());
        let t = h.handle(State::Idle, EventType::Cast, Event::Tick);
        match t {
            Transition::Keep(_) => {}
            other => panic!("expected Keep on empty partitions, got {other:?}"),
        }
        let rec = h
            .take_last_complete()
            .expect("empty cycle should still emit audit");
        assert_eq!(rec.bucket, b"empty");
        assert_eq!(rec.scanned, 0);
        assert_eq!(rec.reaped, 0);
    }

    #[test]
    fn scanning_old_tombstone_is_queued() {
        let mut h = handler();
        let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
        let _ = h.handle(
            State::Scanning,
            EventType::Cast,
            Event::KeyScanned(key(0, KeyKind::Tombstone, 60)),
        );
        assert_eq!(h.batch_len(), 1);
        assert_eq!(h.scanned_this_cycle(), 1);
    }

    #[test]
    fn scanning_live_key_is_never_queued() {
        let mut h = handler();
        let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
        let _ = h.handle(
            State::Scanning,
            EventType::Cast,
            Event::KeyScanned(key(0, KeyKind::Live, 1_000_000)),
        );
        assert_eq!(h.batch_len(), 0);
        assert_eq!(h.scanned_this_cycle(), 1);
    }

    #[test]
    fn batch_capped_at_reap_max_per_cycle() {
        let mut h = handler();
        let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
        for i in 0..10u64 {
            let mut k = key(0, KeyKind::Tombstone, 60);
            k.key = format!("k{i}").into_bytes();
            let _ = h.handle(State::Scanning, EventType::Cast, Event::KeyScanned(k));
        }
        assert_eq!(h.batch_len() as u64, cfg().reap_max_per_cycle);
        // Scan counter still ticks for every key seen.
        assert_eq!(h.scanned_this_cycle(), 10);
    }

    #[test]
    fn next_segment_done_advances_idx() {
        let mut h = handler();
        let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
        assert_eq!(h.partition_idx(), 0);
        let _ = h.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
        assert_eq!(h.partition_idx(), 1);
    }

    #[test]
    fn shutdown_stops_from_any_state() {
        for state in [State::Idle, State::Scanning, State::Reaping] {
            let mut h = handler();
            let t = h.handle(state, EventType::Cast, Event::Shutdown);
            match t {
                Transition::Stop(ReaperOutcome::Stopped) => {}
                other => panic!("expected Stop from {state:?}, got {other:?}"),
            }
        }
    }
}
