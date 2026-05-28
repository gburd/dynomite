//! Tictac AAE exchange protocol expressed as a `gen_fsm` state
//! machine.
//!
//! The legacy [`crate::aae::exchange::Exchange`] driver runs the
//! three-phase exchange synchronously: it pulls roots, segments,
//! and keys from the [`PeerView`] in one call to
//! [`crate::aae::exchange::Exchange::run`] and returns the
//! divergences in a single shot. That is fine for in-process
//! tests, but the production wire-driven path needs explicit
//! per-state timeouts, partial-failure handling, and an event
//! surface so the supervising scheduler can correlate started
//! and completed exchanges with the rest of the cluster
//! lifecycle.
//!
//! This module re-expresses the same protocol as a
//! [`gen_fsm::FsmHandler`] with five states:
//!
//! ```text
//! Init -- snapshot_taken --> Compare -- diffs_found --> Repair
//!                                                         |
//!                                                         v
//!                                                      Finalize -> stop ok
//!
//!   any state can fail to Failed (-> stop err) on:
//!   peer_error, state-timeout, repair_failed, finalize_timeout
//! ```
//!
//! # Wiring
//!
//! [`exchange_with_peer`] is the high-level entry point. It
//! constructs an [`ExchangeHandler`], spawns it on a
//! [`gen_fsm::FsmDriver`], and runs an I/O orchestrator that
//! services the FSM's per-state requests against an
//! `Arc<dyn PeerViewAsync>` (the async-friendly wrapper around
//! [`PeerView`]). When the FSM stops, the orchestrator joins
//! and returns the final [`ExchangeOutcome`].
//!
//! # Testability
//!
//! [`ExchangeHandler`] is exposed directly so unit tests can
//! drive transitions without standing up a runtime. The
//! [`gen_fsm::FsmHandler`] methods are the only protocol
//! surface; the I/O orchestrator is a thin adapter on top of
//! the same handler.

use std::sync::Arc;
use std::time::Duration;

use dynomite::events::{ClusterEvent, EventManager, PeerId, TokenRange};
use gen_fsm::{
    Action, DriverError, EventType, FsmDriver, FsmHandler, StopReason, TimeoutKind, Transition,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::aae::exchange::{Divergence, ExchangeError, PeerView};
use crate::aae::tictac::{KeyEntry, Tree};

/// Default per-state timeout for all four protocol states.
/// Mirrors the C reference's per-phase ceiling.
pub const DEFAULT_STATE_TIMEOUT: Duration = Duration::from_secs(30);

/// States the AAE exchange FSM can be in.
///
/// The state graph is:
///
/// ```text
/// Init -> Compare -> Repair -> Finalize  (-> stop ok)
///   \         \        \         \
///    \--->-----+------>-+------->-+--> Failed (-> stop err)
/// ```
///
/// Every non-terminal state arms a 30s state timeout on entry.
/// Any timeout transitions to [`State::Failed`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Snapshot the local tree's roots and request the peer's
    /// roots.
    Init,
    /// Compare local and peer roots, then enumerate the
    /// diverging time-bucket / segment pairs.
    Compare,
    /// Walk every divergence and apply the repairs.
    Repair,
    /// Surface metrics and the final outcome.
    Finalize,
    /// Terminal failure state. Entry stops the FSM with an
    /// error outcome.
    Failed,
}

/// Events the exchange FSM handles.
///
/// All five flavours are routed through
/// [`gen_fsm::FsmHandler::handle`] except for state entry
/// (handled by [`ExchangeHandler::on_enter`]) and timeouts
/// (handled by [`ExchangeHandler::on_timeout`]).
#[derive(Debug)]
pub enum Event {
    /// Peer responded with its `roots()` view. Triggers the
    /// transition from [`State::Init`] to either
    /// [`State::Compare`] or [`State::Finalize`] depending on
    /// whether any time bucket diverges.
    PeerRootsReceived(Vec<(u32, u64)>),
    /// The compare phase finished and produced the listed
    /// divergences. Empty vector advances directly to
    /// [`State::Finalize`].
    SegmentDiffsComputed(Vec<Divergence>),
    /// One repair landed; bumps the repaired counter by `0` or
    /// more. Repeated occurrences accumulate.
    RepairProgress(usize),
    /// Every divergence has been repaired. Advances to
    /// [`State::Finalize`].
    AllRepaired,
    /// Peer-side error or transport failure. Advances to
    /// [`State::Failed`] regardless of the current state.
    PeerError(String),
}

/// Final outcome returned by the FSM via
/// [`gen_fsm::FsmDriver::join`].
#[derive(Debug, Clone)]
pub enum ExchangeOutcome {
    /// The exchange completed cleanly. `repaired` is the
    /// number of repair acknowledgements observed in
    /// [`State::Repair`].
    Completed {
        /// Number of [`Event::RepairProgress`] hits that
        /// arrived before [`Event::AllRepaired`].
        repaired: usize,
        /// Number of divergences found in the compare phase.
        divergences: usize,
    },
    /// The exchange failed. `reason` is the textual cause
    /// recorded on entry to [`State::Failed`].
    Failed {
        /// Cause string. Either the peer-side error message
        /// or `"state timeout"` for a state-timer fire.
        reason: String,
        /// State the FSM was in when the failure happened.
        last_state: State,
    },
}

/// FSM-side handle that the I/O orchestrator uses to react to
/// state entries. Each variant identifies a piece of work the
/// FSM expects an external worker to perform on its behalf.
#[derive(Debug, Clone)]
pub enum FsmRequest {
    /// Fetch the peer's `roots()` view; reply with
    /// [`Event::PeerRootsReceived`] on success or
    /// [`Event::PeerError`] on failure.
    FetchPeerRoots,
    /// Compare local roots against the peer roots, then walk
    /// every diverging time bucket / segment to compute the
    /// divergences. Reply with [`Event::SegmentDiffsComputed`]
    /// or [`Event::PeerError`].
    ComputeDivergences {
        /// Peer roots already received in [`State::Init`].
        peer_roots: Vec<(u32, u64)>,
    },
    /// Apply repairs for every divergence. Stream
    /// [`Event::RepairProgress`] hits and end with
    /// [`Event::AllRepaired`] (or [`Event::PeerError`] on
    /// failure).
    ApplyRepairs {
        /// The divergences produced by the compare phase.
        divergences: Vec<Divergence>,
    },
    /// Finalize: write any metrics or events and end the run.
    Finalize,
}

/// FSM handler driving one (peer, partition) AAE exchange.
///
/// The handler owns the local [`Tree`] reference and the
/// per-exchange context. It does not perform I/O directly:
/// every state-entry posts an [`FsmRequest`] on
/// [`ExchangeHandler::request_tx`] and waits for the I/O
/// worker to reply with one or more [`Event`]s.
pub struct ExchangeHandler {
    /// Index of the peer the exchange is running against.
    peer_idx: PeerId,
    /// Token range the exchange covers.
    partition: TokenRange,
    /// Snapshot of the local tree's per-time-bucket roots.
    /// Filled in [`State::Init`]'s entry handler.
    local_roots: Vec<(u32, u64)>,
    /// Peer roots stashed on the way from [`State::Init`] to
    /// [`State::Compare`].
    peer_roots: Vec<(u32, u64)>,
    /// Divergences stashed on the way from [`State::Compare`]
    /// to [`State::Repair`].
    divergences: Vec<Divergence>,
    /// Number of repair acknowledgements observed so far.
    repaired: usize,
    /// Last error message received, if any. Surfaced through
    /// [`ExchangeOutcome::Failed::reason`].
    last_error: Option<String>,
    /// Per-state timeout. Configurable via
    /// [`ExchangeHandler::with_state_timeout`].
    state_timeout: Duration,
    /// Outbound request channel. Each on-enter handler posts
    /// one [`FsmRequest`] here for the I/O worker to service.
    request_tx: mpsc::UnboundedSender<FsmRequest>,
    /// Optional event manager. When set, the FSM publishes
    /// [`ClusterEvent::AaeExchangeStarted`] on Init entry and
    /// [`ClusterEvent::AaeExchangeCompleted`] on Finalize
    /// entry.
    events: Option<Arc<EventManager>>,
    /// Local Tictac tree handle used by the I/O worker. Held
    /// here so the worker can borrow it without an extra
    /// argument.
    local_tree: Arc<Tree>,
}

impl ExchangeHandler {
    /// Build a handler bound to a local tree, peer index, and
    /// partition. The handler arms the default 30s state
    /// timeout on every state entry; override via
    /// [`Self::with_state_timeout`].
    #[must_use]
    pub fn new(
        local_tree: Arc<Tree>,
        peer_idx: PeerId,
        partition: TokenRange,
        request_tx: mpsc::UnboundedSender<FsmRequest>,
    ) -> Self {
        Self {
            peer_idx,
            partition,
            local_roots: Vec::new(),
            peer_roots: Vec::new(),
            divergences: Vec::new(),
            repaired: 0,
            last_error: None,
            state_timeout: DEFAULT_STATE_TIMEOUT,
            request_tx,
            events: None,
            local_tree,
        }
    }

    /// Override the per-state timeout. Applies to every state
    /// entry from this point on.
    #[must_use]
    pub fn with_state_timeout(mut self, timeout: Duration) -> Self {
        self.state_timeout = timeout;
        self
    }

    /// Attach an event manager. When set, the handler
    /// publishes [`ClusterEvent::AaeExchangeStarted`] on
    /// [`State::Init`] entry and
    /// [`ClusterEvent::AaeExchangeCompleted`] on
    /// [`State::Finalize`] entry.
    #[must_use]
    pub fn with_events(mut self, events: Arc<EventManager>) -> Self {
        self.events = Some(events);
        self
    }

    /// The partition this exchange is running against.
    #[must_use]
    pub fn partition(&self) -> &TokenRange {
        &self.partition
    }

    /// The peer this exchange is running against.
    #[must_use]
    pub const fn peer_idx(&self) -> PeerId {
        self.peer_idx
    }

    /// The repaired-key counter so far.
    #[must_use]
    pub const fn repaired(&self) -> usize {
        self.repaired
    }

    /// The divergences captured in [`State::Compare`].
    #[must_use]
    pub fn divergences(&self) -> &[Divergence] {
        &self.divergences
    }

    /// Borrow of the local tree.
    #[must_use]
    pub fn local_tree(&self) -> &Tree {
        self.local_tree.as_ref()
    }

    /// Try to send `req` to the I/O worker. A closed channel
    /// surfaces as a [`Event::PeerError`]-like failure: the
    /// caller should transition to [`State::Failed`].
    fn dispatch(&mut self, req: FsmRequest) -> bool {
        if self.request_tx.send(req).is_ok() {
            true
        } else {
            self.last_error
                .get_or_insert_with(|| "io worker channel closed".to_string());
            false
        }
    }
}

impl FsmHandler for ExchangeHandler {
    type State = State;
    type Event = Event;
    type Reply = ();
    type Stop = ExchangeOutcome;

    fn initial(&self) -> State {
        State::Init
    }

    fn on_enter(&mut self, state: State) -> Transition<Self> {
        match state {
            State::Init => {
                self.local_roots = self.local_tree.roots();
                if let Some(ev) = self.events.as_ref() {
                    ev.publish(ClusterEvent::AaeExchangeStarted {
                        with_peer: self.peer_idx,
                        partition: self.partition.clone(),
                        ts: std::time::SystemTime::now(),
                    });
                }
                if !self.dispatch(FsmRequest::FetchPeerRoots) {
                    return Transition::Next(State::Failed, vec![]);
                }
                Transition::Keep(vec![Action::set_state_timeout(self.state_timeout)])
            }
            State::Compare => {
                if !self.dispatch(FsmRequest::ComputeDivergences {
                    peer_roots: self.peer_roots.clone(),
                }) {
                    return Transition::Next(State::Failed, vec![]);
                }
                Transition::Keep(vec![Action::set_state_timeout(self.state_timeout)])
            }
            State::Repair => {
                if self.divergences.is_empty() {
                    return Transition::Next(State::Finalize, vec![]);
                }
                if !self.dispatch(FsmRequest::ApplyRepairs {
                    divergences: self.divergences.clone(),
                }) {
                    return Transition::Next(State::Failed, vec![]);
                }
                Transition::Keep(vec![Action::set_state_timeout(self.state_timeout)])
            }
            State::Finalize => {
                if let Some(ev) = self.events.as_ref() {
                    let repaired = u64::try_from(self.repaired).unwrap_or(u64::MAX);
                    ev.publish(ClusterEvent::AaeExchangeCompleted {
                        with_peer: self.peer_idx,
                        partition: self.partition.clone(),
                        repaired,
                        ts: std::time::SystemTime::now(),
                    });
                }
                let _ = self.dispatch(FsmRequest::Finalize);
                Transition::Stop(ExchangeOutcome::Completed {
                    repaired: self.repaired,
                    divergences: self.divergences.len(),
                })
            }
            State::Failed => Transition::Stop(ExchangeOutcome::Failed {
                reason: self
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "exchange failed".to_string()),
                last_state: State::Failed,
            }),
        }
    }

    fn handle(&mut self, state: State, _et: EventType, ev: Event) -> Transition<Self> {
        // PeerError at any non-terminal state -> Failed.
        if let Event::PeerError(msg) = &ev {
            self.last_error = Some(msg.clone());
            return Transition::Next(State::Failed, vec![]);
        }
        match (state, ev) {
            (State::Init, Event::PeerRootsReceived(roots)) => {
                self.peer_roots = roots;
                if self.peer_roots == self.local_roots {
                    Transition::Next(State::Finalize, vec![])
                } else {
                    Transition::Next(State::Compare, vec![])
                }
            }
            (State::Compare, Event::SegmentDiffsComputed(diffs)) => {
                let empty = diffs.is_empty();
                self.divergences = diffs;
                if empty {
                    Transition::Next(State::Finalize, vec![])
                } else {
                    Transition::Next(State::Repair, vec![])
                }
            }
            (State::Repair, Event::RepairProgress(n)) => {
                self.repaired = self.repaired.saturating_add(n);
                Transition::Keep(vec![])
            }
            (State::Repair, Event::AllRepaired) => Transition::Next(State::Finalize, vec![]),
            // Stale or out-of-order events are silently dropped:
            // the state timeout will catch a genuinely stuck FSM.
            _ => Transition::Keep(vec![]),
        }
    }

    fn on_timeout(&mut self, state: State, kind: TimeoutKind) -> Transition<Self> {
        let _ = kind;
        self.last_error
            .get_or_insert_with(|| format!("state timeout in {state:?}"));
        Transition::Next(State::Failed, vec![])
    }
}

/// Async wrapper around [`PeerView`] used by the I/O worker.
///
/// The legacy [`PeerView`] trait is sync; the FSM driver runs
/// in tokio. [`PeerViewAsync`] decouples the two so that
/// production wirings can implement a fully async trait
/// (talking to a real peer over a tokio stream) while tests
/// continue to use the sync [`PeerView`] via
/// [`SyncPeerViewAdapter`].
pub trait PeerViewAsync: Send + Sync + 'static {
    /// Fetch the peer's per-time-bucket roots.
    ///
    /// # Errors
    /// Implementation-defined.
    fn roots(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<(u32, u64)>, ExchangeError>> + Send;

    /// Fetch the peer's per-segment vector for one time
    /// bucket.
    ///
    /// # Errors
    /// Implementation-defined.
    fn segments(
        &self,
        time_bucket: u32,
    ) -> impl std::future::Future<Output = Result<Vec<(u32, u64)>, ExchangeError>> + Send;

    /// Fetch the peer's keys in one (time bucket, segment).
    ///
    /// # Errors
    /// Implementation-defined.
    fn keys_in_segment(
        &self,
        time_bucket: u32,
        segment: u32,
    ) -> impl std::future::Future<Output = Result<Vec<KeyEntry>, ExchangeError>> + Send;
}

/// Adapter that wraps any sync [`PeerView`] (which must be
/// `Send + Sync + 'static`) into a [`PeerViewAsync`].
///
/// The adapter calls the sync method directly inside the async
/// `fn`. In production wirings the underlying view's I/O is
/// already async; this adapter exists for the in-process and
/// test paths where the view is backed by an in-memory
/// [`Tree`].
pub struct SyncPeerViewAdapter<V> {
    inner: V,
}

impl<V> SyncPeerViewAdapter<V> {
    /// Wrap a sync `PeerView`.
    #[must_use]
    pub const fn new(inner: V) -> Self {
        Self { inner }
    }
}

impl<V> PeerViewAsync for SyncPeerViewAdapter<V>
where
    V: PeerView + Send + Sync + 'static,
{
    async fn roots(&self) -> Result<Vec<(u32, u64)>, ExchangeError> {
        self.inner.roots()
    }

    async fn segments(&self, time_bucket: u32) -> Result<Vec<(u32, u64)>, ExchangeError> {
        self.inner.segments(time_bucket)
    }

    async fn keys_in_segment(
        &self,
        time_bucket: u32,
        segment: u32,
    ) -> Result<Vec<KeyEntry>, ExchangeError> {
        self.inner.keys_in_segment(time_bucket, segment)
    }
}

/// Spawn the exchange FSM and the I/O orchestrator. Returns
/// the [`FsmDriver`] plus a [`JoinHandle`] for the orchestrator
/// task. Callers typically use the higher-level
/// [`exchange_with_peer`] instead.
///
/// The orchestrator reads [`FsmRequest`]s from the FSM and
/// services them against `peer`. Each reply (or failure) is
/// posted back to the FSM as an [`Event`].
#[must_use]
pub fn spawn_exchange<P>(
    handler: ExchangeHandler,
    request_rx: mpsc::UnboundedReceiver<FsmRequest>,
    peer: Arc<P>,
) -> (FsmDriver<ExchangeHandler>, JoinHandle<()>)
where
    P: PeerViewAsync,
{
    let local_tree = Arc::clone(&handler.local_tree);
    let driver = FsmDriver::start(handler);
    let driver_for_io = driver.clone();
    let io = tokio::spawn(io_loop(driver_for_io, request_rx, peer, local_tree));
    (driver, io)
}

/// Run a full exchange against `peer` and wait for the
/// outcome. This is the high-level entry point that the
/// scheduler will call once the wire-driven path is wired up.
///
/// # Errors
/// Returns an [`ExchangeError`] if the FSM driver was dropped
/// before producing an outcome. The "exchange failed" path
/// surfaces as `Ok(ExchangeOutcome::Failed { .. })`, not
/// `Err`.
pub async fn exchange_with_peer<P>(
    local_tree: Arc<Tree>,
    peer: Arc<P>,
    peer_idx: PeerId,
    partition: TokenRange,
) -> Result<ExchangeOutcome, ExchangeError>
where
    P: PeerViewAsync,
{
    let (req_tx, req_rx) = mpsc::unbounded_channel::<FsmRequest>();
    let handler = ExchangeHandler::new(local_tree, peer_idx, partition, req_tx);
    let (driver, io) = spawn_exchange(handler, req_rx, peer);
    let stop = driver
        .join()
        .await
        .map_err(|e: DriverError| ExchangeError::BadPayload(format!("fsm driver: {e}")))?;
    io.abort();
    let _ = io.await;
    match stop {
        StopReason::Handler(outcome) => Ok(outcome),
        StopReason::Closed => Err(ExchangeError::BadPayload(
            "fsm driver closed before outcome".to_string(),
        )),
    }
}

async fn io_loop<P>(
    driver: FsmDriver<ExchangeHandler>,
    mut rx: mpsc::UnboundedReceiver<FsmRequest>,
    peer: Arc<P>,
    local_tree: Arc<Tree>,
) where
    P: PeerViewAsync,
{
    while let Some(req) = rx.recv().await {
        match req {
            FsmRequest::FetchPeerRoots => match peer.roots().await {
                Ok(roots) => driver.cast(Event::PeerRootsReceived(roots)).await,
                Err(e) => driver.cast(Event::PeerError(e.to_string())).await,
            },
            FsmRequest::ComputeDivergences { peer_roots } => {
                match compute_divergences(local_tree.as_ref(), peer.as_ref(), &peer_roots).await {
                    Ok(divs) => driver.cast(Event::SegmentDiffsComputed(divs)).await,
                    Err(e) => driver.cast(Event::PeerError(e.to_string())).await,
                }
            }
            FsmRequest::ApplyRepairs { divergences } => {
                // The repair handoff is owned by the
                // RepairScheduler; here we simply acknowledge
                // every divergence as one repair-progress
                // event. Production wirings replace this body
                // with a real per-divergence dispatch.
                let count = divergences.len();
                for _ in 0..count {
                    driver.cast(Event::RepairProgress(1)).await;
                }
                driver.cast(Event::AllRepaired).await;
            }
            FsmRequest::Finalize => {
                // Nothing for the worker to do beyond letting
                // the FSM stop on its own.
                break;
            }
        }
    }
}

async fn compute_divergences<P>(
    local: &Tree,
    peer: &P,
    peer_roots: &[(u32, u64)],
) -> Result<Vec<Divergence>, ExchangeError>
where
    P: PeerViewAsync,
{
    let local_roots = local.roots();
    let dr = Tree::diverging_time_buckets(&local_roots, peer_roots);
    let mut out = Vec::new();
    for tb in dr {
        let local_segs = local.segments(tb)?;
        let peer_segs = peer.segments(tb).await?;
        let ds = Tree::diverging_segments(&local_segs, &peer_segs);
        for seg in ds {
            let local_keys = local.keys_in_segment(tb, seg)?;
            let peer_keys = peer.keys_in_segment(tb, seg).await?;
            let (local_only, remote_only) = symmetric_difference(&local_keys, &peer_keys);
            if local_only.is_empty() && remote_only.is_empty() {
                continue;
            }
            out.push(Divergence {
                time_bucket: tb,
                segment: seg,
                local_only,
                remote_only,
            });
        }
    }
    Ok(out)
}

fn symmetric_difference(a: &[KeyEntry], b: &[KeyEntry]) -> (Vec<KeyEntry>, Vec<KeyEntry>) {
    let bset: std::collections::BTreeSet<&KeyEntry> = b.iter().collect();
    let aset: std::collections::BTreeSet<&KeyEntry> = a.iter().collect();
    let only_a = a.iter().filter(|e| !bset.contains(e)).cloned().collect();
    let only_b = b.iter().filter(|e| !aset.contains(e)).cloned().collect();
    (only_a, only_b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aae::exchange::LocalPeerView;
    use crate::aae::tictac::TreeShape;
    use dynomite::hashkit::DynToken;
    use std::time::Duration;

    fn shape() -> TreeShape {
        TreeShape {
            n_time_buckets: 4,
            n_segments: 32,
            time_window_seconds: 60,
        }
    }

    fn partition() -> TokenRange {
        TokenRange::new(DynToken::from_u32(0), DynToken::from_u32(1024))
    }

    fn handler(tree: Tree) -> (ExchangeHandler, mpsc::UnboundedReceiver<FsmRequest>) {
        let (tx, rx) = mpsc::unbounded_channel::<FsmRequest>();
        (ExchangeHandler::new(Arc::new(tree), 7, partition(), tx), rx)
    }

    fn assert_state_timeout(transition: &Transition<ExchangeHandler>, expected: Duration) {
        match transition {
            Transition::Keep(actions) | Transition::Next(_, actions) => {
                let found = actions
                    .iter()
                    .any(|a| matches!(a, Action::SetStateTimeout(d) if *d == expected));
                assert!(
                    found,
                    "expected SetStateTimeout({expected:?}); actions = {actions:?}"
                );
            }
            Transition::Stop(_) => panic!("expected Keep/Next, got Stop"),
        }
    }

    fn drain(rx: &mut mpsc::UnboundedReceiver<FsmRequest>) -> Vec<FsmRequest> {
        let mut out = Vec::new();
        while let Ok(req) = rx.try_recv() {
            out.push(req);
        }
        out
    }

    #[test]
    fn exchange_init_state_sets_30s_timeout() {
        let tree = Tree::new(shape());
        let (mut h, mut rx) = handler(tree);
        let t = h.on_enter(State::Init);
        assert_state_timeout(&t, DEFAULT_STATE_TIMEOUT);
        // The Init handler must also dispatch a FetchPeerRoots
        // request so the I/O worker can act.
        let reqs = drain(&mut rx);
        assert!(matches!(reqs.as_slice(), [FsmRequest::FetchPeerRoots]));
    }

    #[test]
    fn peer_root_match_skips_to_finalize() {
        let mut tree = Tree::new(shape());
        tree.insert(b"users", b"alice", b"vc1", 0);
        let (mut h, _rx) = handler(tree);
        // Run Init's on_enter to populate local_roots.
        let _ = h.on_enter(State::Init);
        let local = h.local_roots.clone();
        // Send the same roots back -> should skip Compare and
        // jump straight to Finalize.
        let t = h.handle(
            State::Init,
            EventType::Cast,
            Event::PeerRootsReceived(local),
        );
        match t {
            Transition::Next(State::Finalize, _) => {}
            other => panic!("expected Next(Finalize), got {other:?}"),
        }
    }

    #[test]
    fn peer_root_mismatch_advances_to_compare() {
        let mut tree = Tree::new(shape());
        tree.insert(b"users", b"alice", b"vc1", 0);
        let (mut h, _rx) = handler(tree);
        let _ = h.on_enter(State::Init);
        let mut diff = h.local_roots.clone();
        if diff.is_empty() {
            diff.push((0, 0xdead_beef));
        } else {
            diff[0].1 ^= 0x1;
        }
        let t = h.handle(State::Init, EventType::Cast, Event::PeerRootsReceived(diff));
        match t {
            Transition::Next(State::Compare, _) => {}
            other => panic!("expected Next(Compare), got {other:?}"),
        }
    }

    #[test]
    fn compare_timeout_transitions_to_failed() {
        let tree = Tree::new(shape());
        let (mut h, _rx) = handler(tree);
        let t = h.on_timeout(State::Compare, TimeoutKind::State);
        match t {
            Transition::Next(State::Failed, _) => {}
            other => panic!("expected Next(Failed), got {other:?}"),
        }
        // The Failed entry handler stops with a timeout-shaped
        // outcome.
        let stop = h.on_enter(State::Failed);
        match stop {
            Transition::Stop(ExchangeOutcome::Failed { reason, .. }) => {
                assert!(
                    reason.contains("timeout"),
                    "expected reason to mention timeout; got {reason:?}"
                );
            }
            other => panic!("expected Stop(Failed), got {other:?}"),
        }
    }

    #[test]
    fn repair_progress_increments_counter() {
        let tree = Tree::new(shape());
        let (mut h, _rx) = handler(tree);
        let _ = h.handle(State::Repair, EventType::Cast, Event::RepairProgress(1));
        let _ = h.handle(State::Repair, EventType::Cast, Event::RepairProgress(2));
        assert_eq!(h.repaired(), 3);
    }

    #[test]
    fn all_repaired_advances_to_finalize() {
        let tree = Tree::new(shape());
        let (mut h, _rx) = handler(tree);
        let t = h.handle(State::Repair, EventType::Cast, Event::AllRepaired);
        match t {
            Transition::Next(State::Finalize, _) => {}
            other => panic!("expected Next(Finalize), got {other:?}"),
        }
    }

    #[test]
    fn peer_error_at_any_state_transitions_to_failed() {
        for state in [State::Init, State::Compare, State::Repair, State::Finalize] {
            let tree = Tree::new(shape());
            let (mut h, _rx) = handler(tree);
            let t = h.handle(
                state,
                EventType::Cast,
                Event::PeerError(format!("boom in {state:?}")),
            );
            match t {
                Transition::Next(State::Failed, _) => {}
                other => panic!("expected Next(Failed) from {state:?}, got {other:?}"),
            }
            let stop = h.on_enter(State::Failed);
            match stop {
                Transition::Stop(ExchangeOutcome::Failed { reason, .. }) => {
                    assert!(reason.contains("boom"));
                }
                other => panic!("expected Stop(Failed), got {other:?}"),
            }
        }
    }

    #[test]
    fn empty_diffs_skip_repair_and_go_to_finalize() {
        let tree = Tree::new(shape());
        let (mut h, _rx) = handler(tree);
        let t = h.handle(
            State::Compare,
            EventType::Cast,
            Event::SegmentDiffsComputed(vec![]),
        );
        match t {
            Transition::Next(State::Finalize, _) => {}
            other => panic!("expected Next(Finalize), got {other:?}"),
        }
    }

    #[test]
    fn diffs_found_advances_to_repair_and_dispatches_request() {
        let tree = Tree::new(shape());
        let (mut h, mut rx) = handler(tree);
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
        let t = h.handle(
            State::Compare,
            EventType::Cast,
            Event::SegmentDiffsComputed(vec![div.clone()]),
        );
        match t {
            Transition::Next(State::Repair, _) => {}
            other => panic!("expected Next(Repair), got {other:?}"),
        }
        // Now run on_enter(Repair) and confirm an
        // ApplyRepairs request is dispatched.
        let t2 = h.on_enter(State::Repair);
        assert_state_timeout(&t2, DEFAULT_STATE_TIMEOUT);
        let reqs = drain(&mut rx);
        assert_eq!(reqs.len(), 1, "expected one ApplyRepairs request");
        assert!(matches!(
            &reqs[0],
            FsmRequest::ApplyRepairs { divergences } if divergences == &vec![div.clone()]
        ));
    }

    #[test]
    fn finalize_entry_stops_with_completed_outcome() {
        let tree = Tree::new(shape());
        let (mut h, _rx) = handler(tree);
        h.repaired = 4;
        let _ = h.handle(
            State::Compare,
            EventType::Cast,
            Event::SegmentDiffsComputed(vec![Divergence {
                time_bucket: 0,
                segment: 1,
                local_only: vec![],
                remote_only: vec![],
            }]),
        );
        let stop = h.on_enter(State::Finalize);
        match stop {
            Transition::Stop(ExchangeOutcome::Completed {
                repaired,
                divergences,
            }) => {
                assert_eq!(repaired, 4);
                assert_eq!(divergences, 1);
            }
            other => panic!("expected Stop(Completed), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn end_to_end_exchange_via_fsm_finds_diff() {
        // Mirror the in-memory test in `exchange.rs`: build
        // two trees, mutate one key's vclock on `b`, and run
        // the FSM-based exchange. The completed outcome must
        // report at least one divergence and one repair.
        let mut a = Tree::new(shape());
        let mut b = Tree::new(shape());
        for i in 0..50u32 {
            let k = format!("k{i}");
            a.insert(b"users", k.as_bytes(), b"vc1", 0);
            b.insert(b"users", k.as_bytes(), b"vc1", 0);
        }
        b.update(b"users", b"k7", b"vc1", b"vc2", 0, 0);

        // Hold `b` behind an Arc so the adapter can borrow it
        // for the duration of the run.
        let b = Arc::new(b);
        let view = SyncPeerViewAdapter::new(BorrowedPeerView {
            tree: Arc::clone(&b),
        });
        let peer = Arc::new(view);
        let local = Arc::new(a);
        let outcome = exchange_with_peer(local, peer, 1, partition())
            .await
            .unwrap();
        match outcome {
            ExchangeOutcome::Completed {
                repaired,
                divergences,
            } => {
                assert!(divergences >= 1, "expected at least one divergence");
                assert_eq!(repaired, divergences, "io_loop posts one progress per div");
            }
            ExchangeOutcome::Failed { reason, last_state } => {
                panic!("unexpected Failed({last_state:?}, {reason})");
            }
        }
    }

    /// In-memory peer view backed by an `Arc<Tree>` so tests
    /// can hand it off to the io_loop without lifetime fights.
    struct BorrowedPeerView {
        tree: Arc<Tree>,
    }

    impl PeerView for BorrowedPeerView {
        fn roots(&self) -> Result<Vec<(u32, u64)>, ExchangeError> {
            Ok(self.tree.roots())
        }
        fn segments(&self, time_bucket: u32) -> Result<Vec<(u32, u64)>, ExchangeError> {
            self.tree.segments(time_bucket).map_err(ExchangeError::from)
        }
        fn keys_in_segment(
            &self,
            time_bucket: u32,
            segment: u32,
        ) -> Result<Vec<KeyEntry>, ExchangeError> {
            self.tree
                .keys_in_segment(time_bucket, segment)
                .map_err(ExchangeError::from)
        }
    }

    #[tokio::test]
    async fn end_to_end_with_failing_peer_yields_failed_outcome() {
        struct Boom;
        impl PeerView for Boom {
            fn roots(&self) -> Result<Vec<(u32, u64)>, ExchangeError> {
                Err(ExchangeError::BadPayload("synthetic peer down".into()))
            }
            fn segments(&self, _: u32) -> Result<Vec<(u32, u64)>, ExchangeError> {
                Err(ExchangeError::BadPayload("synthetic peer down".into()))
            }
            fn keys_in_segment(&self, _: u32, _: u32) -> Result<Vec<KeyEntry>, ExchangeError> {
                Err(ExchangeError::BadPayload("synthetic peer down".into()))
            }
        }

        let local = Arc::new(Tree::new(shape()));
        let peer = Arc::new(SyncPeerViewAdapter::new(Boom));
        let outcome = exchange_with_peer(local, peer, 9, partition())
            .await
            .unwrap();
        match outcome {
            ExchangeOutcome::Failed { reason, .. } => {
                assert!(reason.contains("synthetic peer down"));
            }
            ExchangeOutcome::Completed { .. } => panic!("expected Failed"),
        }
    }

    fn takes_peer_view(_: &impl PeerView) {}

    // Quick sanity that LocalPeerView still satisfies the
    // sync trait shape used by the adapter at compile time.
    #[test]
    fn local_peer_view_is_peer_view() {
        let t = Tree::new(shape());
        takes_peer_view(&LocalPeerView::new(&t));
    }
}
