//! Supervisor runtime.
//!
//! Public type [`Supervisor`] plus its split-handle [`SupervisorHandle`].
//! The runtime is built around a single mpsc event channel into which
//! every observable child transition lands; the supervisor task reacts
//! to those events according to the configured strategy.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::AbortHandle;

use crate::atomics::{BackoffState, ChildIdAllocator};
use crate::error::SupError;
use crate::types::{
    ChildExit, ChildId, ChildSpec, RestartPolicy, RestartStrategy, SupExit, Supervised,
};

/// Default upper bound on how long
/// [`Supervisor::shutdown`]/[`SupervisorHandle::shutdown`] will wait
/// for children to drain. Override with
/// [`Supervisor::with_shutdown_timeout`].
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type Factory = Box<dyn FnMut() -> BoxFuture<Result<(), SupError>> + Send>;

/// Internal per-child registration record.
struct ChildEntry {
    id: ChildId,
    name: String,
    factory: Factory,
    restart: RestartPolicy,
    /// Backoff bookkeeping: spec, consecutive-failure counter, and
    /// per-child PRNG. Atomic-backed so the same primitive can be
    /// model-checked under loom.
    backoff: BackoffState,
    /// Monotonically increasing per-spawn generation. Used to discard
    /// stale exit events (e.g. from a child we already aborted as part
    /// of a OneForAll cascade).
    generation: u64,
    abort: Option<AbortHandle>,
    state: ChildState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildState {
    /// Not running; not waiting to run. Children in this state are
    /// terminal unless [`RestartPolicy::Permanent`] forces a respawn.
    Idle,
    /// A tokio task is alive for this child.
    Running,
    /// A backoff timer is running; the child will respawn when it
    /// fires.
    BackingOff,
    /// The child has been removed (Temporary policy after exit, or a
    /// Transient child that exited cleanly). It will not run again.
    Removed,
}

/// Internal helper enum that flattens the three outcomes of an
/// event-channel poll into a single match.
enum NextEvent {
    Event(SupEvent),
    Drained,
    Timeout,
}

/// Internal supervisor event.
enum SupEvent {
    ChildExited {
        id: ChildId,
        generation: u64,
        exit: ChildExit,
    },
    RestartReady {
        id: ChildId,
        generation: u64,
    },
    Shutdown,
}

/// Shared shutdown signal. Cloned into the [`SupervisorHandle`] and
/// consulted by the supervisor's run loop. We use a watch channel
/// rather than a [`tokio::sync::Notify`] so that a shutdown request
/// fired before the supervisor's bridge task has subscribed is still
/// observed (the watch's current value seeds every new subscriber).
struct ShutdownState {
    tx: watch::Sender<bool>,
    /// Set by the supervisor task once it has finished and detached
    /// the channel. After this, [`SupervisorHandle::shutdown`] returns
    /// [`SupError::NotRunning`].
    finished: AtomicBool,
}

impl ShutdownState {
    fn new() -> Self {
        let (tx, _) = watch::channel(false);
        Self {
            tx,
            finished: AtomicBool::new(false),
        }
    }
}

/// A clone-able handle for steering a [`Supervisor`] from outside the
/// task that owns its [`Supervisor::run`] future.
///
/// Obtained by [`Supervisor::handle`] before consuming the supervisor
/// with `run`.
#[derive(Clone)]
pub struct SupervisorHandle {
    shutdown: Arc<ShutdownState>,
}

impl SupervisorHandle {
    /// Request graceful shutdown. The currently running supervisor
    /// task will abort all of its children and return
    /// [`SupExit::Shutdown`] (or [`SupExit::ShutdownTimeout`] if a
    /// child fails to drain in time).
    ///
    /// Returns [`SupError::NotRunning`] if the supervisor task has
    /// already finished. Calling `shutdown` more than once on the
    /// same handle is harmless: subsequent calls succeed without
    /// changing state.
    pub async fn shutdown(&self) -> Result<(), SupError> {
        if self.shutdown.finished.load(Ordering::Acquire) {
            return Err(SupError::NotRunning);
        }
        // `send` only fails if there are no receivers. The supervisor
        // task itself holds a receiver until it terminates, and the
        // sender always retains the latest value, so a second
        // `shutdown` call after the supervisor has dropped its
        // receiver still reflects the request in the sender's stored
        // value (which we treat as advisory).
        let _ = self.shutdown.tx.send(true);
        // Yield so callers cannot starve the supervisor task by
        // chaining shutdowns inside a tight non-yielding loop.
        tokio::task::yield_now().await;
        Ok(())
    }

    /// `true` once the supervised task has returned from `run`.
    pub fn is_finished(&self) -> bool {
        self.shutdown.finished.load(Ordering::Acquire)
    }
}

/// An OTP-style supervisor of tokio tasks.
///
/// Build with [`Supervisor::new`], register children with
/// [`Supervisor::add_child`], retrieve a [`SupervisorHandle`] with
/// [`Supervisor::handle`], then drive the supervisor by awaiting (or
/// spawning) [`Supervisor::run`].
pub struct Supervisor {
    strategy: RestartStrategy,
    children: Vec<ChildEntry>,
    next_id: ChildIdAllocator,
    shutdown: Arc<ShutdownState>,
    shutdown_timeout: Duration,
    seed_source: u64,
}

impl Supervisor {
    /// Create an empty supervisor that will use `strategy` when its
    /// children exit.
    pub fn new(strategy: RestartStrategy) -> Self {
        Self {
            strategy,
            children: Vec::new(),
            next_id: ChildIdAllocator::new(),
            shutdown: Arc::new(ShutdownState::new()),
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            seed_source: seed_prng(),
        }
    }

    /// Override the shutdown drain timeout.
    #[must_use]
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Get a [`SupervisorHandle`] that can outlive the call to `run`.
    pub fn handle(&self) -> SupervisorHandle {
        SupervisorHandle {
            shutdown: self.shutdown.clone(),
        }
    }

    /// Register a child. Returns the [`ChildId`] for use in logs and
    /// test assertions.
    ///
    /// # Panics
    ///
    /// Panics if the configured strategy is
    /// [`RestartStrategy::SimpleOneForOne`] and adding this child
    /// would exceed `max_children`. Use
    /// [`Self::try_add_child`] for a fallible variant.
    pub fn add_child<S: Supervised>(&mut self, spec: ChildSpec<S>) -> ChildId {
        match self.try_add_child(spec) {
            Ok(id) => id,
            Err(SupError::ChildLimitReached) => {
                panic!("supervisor child limit reached")
            }
            Err(e) => panic!("supervisor add_child failed: {e}"),
        }
    }

    /// Fallible variant of [`Self::add_child`].
    pub fn try_add_child<S: Supervised>(
        &mut self,
        spec: ChildSpec<S>,
    ) -> Result<ChildId, SupError> {
        if let RestartStrategy::SimpleOneForOne { max_children } = self.strategy {
            if self.children.len() >= max_children {
                return Err(SupError::ChildLimitReached);
            }
        }
        let id = ChildId(self.next_id.next());
        let ChildSpec {
            spec,
            restart,
            backoff,
        } = spec;
        let name = spec.name().to_string();
        let factory = make_factory(spec);
        let backoff_seed = mix_seed(self.seed_source, id.0);
        self.children.push(ChildEntry {
            id,
            name,
            factory,
            restart,
            backoff: BackoffState::new(backoff, backoff_seed),
            generation: 0,
            abort: None,
            state: ChildState::Idle,
        });
        Ok(id)
    }

    /// Equivalent to `self.handle().shutdown()`. Provided for parity
    /// with the documented API; you must hold a reference to `self`,
    /// so prefer [`Self::handle`] when the supervisor will be moved
    /// into a spawned task.
    pub async fn shutdown(&self) -> Result<(), SupError> {
        if self.shutdown.finished.load(Ordering::Acquire) {
            return Err(SupError::NotRunning);
        }
        let _ = self.shutdown.tx.send(true);
        tokio::task::yield_now().await;
        Ok(())
    }

    /// Drive the supervisor until it terminates.
    ///
    /// Returns [`SupExit::Shutdown`] if shutdown was requested,
    /// [`SupExit::AllChildrenStopped`] if every child reached a
    /// terminal state without external request, and
    /// [`SupExit::ShutdownTimeout`] if shutdown was requested but a
    /// child did not drain in time.
    pub async fn run(mut self) -> SupExit {
        let (tx, mut rx) = mpsc::unbounded_channel::<SupEvent>();
        let shutdown_bridge = self.spawn_shutdown_bridge(tx.clone());

        // Initial start of every registered child.
        for idx in 0..self.children.len() {
            self.spawn_child(idx, &tx);
        }

        let mut shutting_down = false;
        let mut exit_reason = SupExit::AllChildrenStopped;

        loop {
            if !shutting_down && self.all_terminal() {
                tracing::debug!(target: "sup", "all children terminal, stopping supervisor");
                exit_reason = SupExit::AllChildrenStopped;
                break;
            }
            if shutting_down && self.alive_count() == 0 {
                exit_reason = SupExit::Shutdown;
                break;
            }

            let event = match self.next_event(&mut rx, shutting_down).await {
                NextEvent::Event(e) => e,
                NextEvent::Drained => break,
                NextEvent::Timeout => {
                    let remaining = self.alive_count();
                    tracing::warn!(target: "sup", remaining, "shutdown drain timed out");
                    exit_reason = SupExit::ShutdownTimeout { remaining };
                    self.force_abort_remaining();
                    break;
                }
            };

            match event {
                SupEvent::Shutdown => {
                    if shutting_down {
                        continue;
                    }
                    shutting_down = true;
                    self.handle_shutdown_event();
                }
                SupEvent::ChildExited {
                    id,
                    generation,
                    exit,
                } => {
                    self.handle_child_exited(id, generation, &exit, shutting_down, &tx);
                }
                SupEvent::RestartReady { id, generation } => {
                    self.handle_restart_ready(id, generation, shutting_down, &tx);
                }
            }
        }

        self.shutdown.finished.store(true, Ordering::Release);
        shutdown_bridge.abort();
        exit_reason
    }

    fn spawn_shutdown_bridge(
        &mut self,
        tx: mpsc::UnboundedSender<SupEvent>,
    ) -> tokio::task::JoinHandle<()> {
        let mut shutdown_rx = self.shutdown.tx.subscribe();
        tokio::spawn(async move {
            if *shutdown_rx.borrow() {
                let _ = tx.send(SupEvent::Shutdown);
                return;
            }
            while shutdown_rx.changed().await.is_ok() {
                if *shutdown_rx.borrow() {
                    let _ = tx.send(SupEvent::Shutdown);
                    return;
                }
            }
        })
    }

    async fn next_event(
        &mut self,
        rx: &mut mpsc::UnboundedReceiver<SupEvent>,
        shutting_down: bool,
    ) -> NextEvent {
        if shutting_down {
            match tokio::time::timeout(self.shutdown_timeout, rx.recv()).await {
                Ok(Some(e)) => NextEvent::Event(e),
                Ok(None) => NextEvent::Drained,
                Err(_) => NextEvent::Timeout,
            }
        } else {
            match rx.recv().await {
                Some(e) => NextEvent::Event(e),
                None => NextEvent::Drained,
            }
        }
    }

    fn force_abort_remaining(&mut self) {
        for child in &mut self.children {
            if let Some(handle) = child.abort.take() {
                handle.abort();
            }
            child.state = ChildState::Removed;
        }
    }

    fn handle_shutdown_event(&mut self) {
        tracing::info!(target: "sup", "shutdown requested");
        for child in &mut self.children {
            if let Some(abort) = child.abort.take() {
                abort.abort();
            }
            match child.state {
                ChildState::Running => child.state = ChildState::Idle,
                ChildState::BackingOff => child.state = ChildState::Removed,
                _ => {}
            }
        }
    }

    fn handle_child_exited(
        &mut self,
        id: ChildId,
        generation: u64,
        exit: &ChildExit,
        shutting_down: bool,
        tx: &mpsc::UnboundedSender<SupEvent>,
    ) {
        let Some(idx) = self.find_index(id) else {
            return;
        };
        if self.children[idx].generation != generation {
            // Stale exit from a previously-cancelled generation.
            return;
        }
        self.children[idx].abort = None;
        self.children[idx].state = ChildState::Idle;
        let was_abnormal = exit.is_abnormal();
        let name = self.children[idx].name.clone();
        tracing::info!(
            target: "sup",
            child = %name,
            id = %id,
            ?exit,
            "child exited",
        );
        if shutting_down {
            self.children[idx].state = ChildState::Removed;
            return;
        }
        if was_abnormal {
            self.children[idx].backoff.observe_failure();
        } else {
            self.children[idx].backoff.observe_success();
        }
        let restart_self = match self.children[idx].restart {
            RestartPolicy::Permanent => true,
            RestartPolicy::Transient => was_abnormal,
            RestartPolicy::Temporary => false,
        };
        if !restart_self {
            self.children[idx].state = ChildState::Removed;
        }
        self.cascade(idx, restart_self, was_abnormal, tx);
    }

    fn handle_restart_ready(
        &mut self,
        id: ChildId,
        generation: u64,
        shutting_down: bool,
        tx: &mpsc::UnboundedSender<SupEvent>,
    ) {
        let Some(idx) = self.find_index(id) else {
            return;
        };
        if self.children[idx].generation != generation {
            return;
        }
        if shutting_down {
            return;
        }
        if self.children[idx].state != ChildState::BackingOff {
            return;
        }
        tracing::debug!(
            target: "sup",
            child = %self.children[idx].name,
            id = %id,
            "backoff elapsed, restarting child",
        );
        self.spawn_child(idx, tx);
    }

    fn cascade(
        &mut self,
        failed_idx: usize,
        restart_failed: bool,
        was_abnormal: bool,
        tx: &mpsc::UnboundedSender<SupEvent>,
    ) {
        match self.strategy {
            RestartStrategy::OneForOne | RestartStrategy::SimpleOneForOne { .. } => {
                if restart_failed {
                    if was_abnormal {
                        self.schedule_restart(failed_idx, tx);
                    } else {
                        self.spawn_child(failed_idx, tx);
                    }
                }
            }
            RestartStrategy::OneForAll => {
                self.terminate_others(failed_idx);
                let order: Vec<usize> = (0..self.children.len()).collect();
                self.respawn_set(failed_idx, restart_failed, was_abnormal, &order, tx);
            }
            RestartStrategy::RestForOne => {
                self.terminate_after(failed_idx);
                let order: Vec<usize> = (failed_idx..self.children.len()).collect();
                self.respawn_set(failed_idx, restart_failed, was_abnormal, &order, tx);
            }
        }
    }

    /// Abort and idle every other child in preparation for a
    /// OneForAll restart.
    fn terminate_others(&mut self, failed_idx: usize) {
        for (i, child) in self.children.iter_mut().enumerate() {
            if i == failed_idx {
                continue;
            }
            if let Some(abort) = child.abort.take() {
                abort.abort();
            }
            // Bump generation so any in-flight Cancelled event is
            // discarded by the run loop.
            child.generation = child.generation.wrapping_add(1);
            child.state = ChildState::Idle;
        }
    }

    /// Abort and idle every child registered after `failed_idx`.
    fn terminate_after(&mut self, failed_idx: usize) {
        for (i, child) in self.children.iter_mut().enumerate() {
            if i <= failed_idx {
                continue;
            }
            if let Some(abort) = child.abort.take() {
                abort.abort();
            }
            child.generation = child.generation.wrapping_add(1);
            child.state = ChildState::Idle;
        }
    }

    fn respawn_set(
        &mut self,
        failed_idx: usize,
        restart_failed: bool,
        was_abnormal: bool,
        order: &[usize],
        tx: &mpsc::UnboundedSender<SupEvent>,
    ) {
        for &i in order {
            if i == failed_idx {
                if restart_failed {
                    if was_abnormal {
                        self.schedule_restart(i, tx);
                    } else {
                        self.spawn_child(i, tx);
                    }
                }
                continue;
            }
            // Sibling restart: only restart siblings whose policy
            // permits it. Their consecutive_failures stay at zero
            // (they did not themselves fail).
            let policy = self.children[i].restart;
            let respawn = match policy {
                RestartPolicy::Permanent | RestartPolicy::Transient => true,
                RestartPolicy::Temporary => false,
            };
            if respawn {
                self.spawn_child(i, tx);
            } else {
                self.children[i].state = ChildState::Removed;
            }
        }
    }

    fn schedule_restart(&mut self, idx: usize, tx: &mpsc::UnboundedSender<SupEvent>) {
        let child = &mut self.children[idx];
        let delay = child.backoff.next_delay();
        let failures = child.backoff.failures();
        child.generation = child.generation.wrapping_add(1);
        let generation = child.generation;
        let id = child.id;
        child.state = ChildState::BackingOff;
        let tx = tx.clone();
        tracing::debug!(
            target: "sup",
            child = %child.name,
            id = %id,
            ?delay,
            failures,
            "scheduling restart with backoff",
        );
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(SupEvent::RestartReady { id, generation });
        });
    }

    fn spawn_child(&mut self, idx: usize, tx: &mpsc::UnboundedSender<SupEvent>) {
        let child = &mut self.children[idx];
        child.generation = child.generation.wrapping_add(1);
        let generation = child.generation;
        let id = child.id;
        let name = child.name.clone();
        let fut = (child.factory)();
        let join = tokio::spawn(fut);
        let abort = join.abort_handle();
        child.abort = Some(abort);
        child.state = ChildState::Running;
        let name_for_event = name.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let join_result = join.await;
            let exit = classify_exit(&name_for_event, join_result);
            let _ = tx.send(SupEvent::ChildExited {
                id,
                generation,
                exit,
            });
        });
        tracing::debug!(target: "sup", child = %name, id = %id, "spawned child");
    }

    fn find_index(&self, id: ChildId) -> Option<usize> {
        self.children.iter().position(|c| c.id == id)
    }

    fn all_terminal(&self) -> bool {
        self.children
            .iter()
            .all(|c| matches!(c.state, ChildState::Removed))
    }

    fn alive_count(&self) -> usize {
        self.children
            .iter()
            .filter(|c| matches!(c.state, ChildState::Running | ChildState::BackingOff))
            .count()
    }
}

/// Type-erase a [`Supervised`] into a `FnMut`-style factory that
/// produces a fresh `'static + Send` future each time it is invoked.
fn make_factory<S: Supervised>(spec: S) -> Factory {
    let shared = Arc::new(Mutex::new(spec));
    Box::new(move || {
        let s = shared.clone();
        Box::pin(async move {
            let mut guard = s.lock().await;
            match guard.run().await {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }
        })
    })
}

fn classify_exit(
    name: &str,
    join_result: Result<Result<(), SupError>, tokio::task::JoinError>,
) -> ChildExit {
    match join_result {
        Ok(Ok(())) => ChildExit::Ok,
        Ok(Err(e)) => ChildExit::Err(format!("{e}")),
        Err(je) if je.is_cancelled() => ChildExit::Cancelled,
        Err(je) => match je.try_into_panic() {
            Ok(payload) => ChildExit::Panic(panic_message(name, payload.as_ref())),
            Err(_) => ChildExit::Cancelled,
        },
    }
}

fn panic_message(name: &str, payload: &(dyn Any + Send + 'static)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        format!("<panic in '{name}'>")
    }
}

fn seed_prng() -> u64 {
    let nanos =
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0xA5A5_A5A5_A5A5_A5A5_u64, |d| {
                // u128 -> u64 fold: xor the high half into the low half.
                // The exact value is irrelevant to correctness; this is a
                // PRNG seed only.
                let n = d.as_nanos();
                let hi = u64::try_from(n >> 64).unwrap_or(0);
                let lo = u64::try_from(n & u128::from(u64::MAX)).unwrap_or(0);
                lo ^ hi
            });
    nanos ^ 0xC0FF_EEDE_CAFB_AD55_u64
}

/// Combine the supervisor's seed source with a child id to derive a
/// per-child PRNG seed. Uses the SplitMix64 finaliser so adjacent
/// child ids produce well-separated seeds even if the seed source
/// has poor entropy in its low bits.
fn mix_seed(source: u64, id: u64) -> u64 {
    let mut z = source.wrapping_add(id).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
