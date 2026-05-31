//! Public types: strategies, policies, child specifications, and the
//! [`Supervised`] trait.

use std::future::Future;

use crate::backoff::BackoffSpec;
use crate::error::SupError;

/// A unit of work managed by a [`Supervisor`](crate::Supervisor).
///
/// An implementor describes its identity (`name`) and how to run a
/// single life-cycle (`run`). The supervisor calls `run` once per
/// life-cycle: a `Supervised` value is a *factory* of futures, not the
/// future itself. After `run` completes (with `Ok`, `Err`, or by
/// panicking), the supervisor decides whether to call `run` again
/// based on the child's [`RestartPolicy`] and the supervisor's
/// [`RestartStrategy`].
///
/// # Example
///
/// ```
/// use sup::{SupError, Supervised};
///
/// struct Heartbeat;
///
/// impl Supervised for Heartbeat {
///     type Output = ();
///     fn name(&self) -> &str {
///         "heartbeat"
///     }
///     async fn run(&mut self) -> Result<Self::Output, SupError> {
///         // ... emit a heartbeat, sleep, repeat ...
///         Ok(())
///     }
/// }
/// ```
pub trait Supervised: Send + 'static {
    /// The successful output of one run cycle. The supervisor does not
    /// inspect this value; it exists so a `Supervised` impl can carry
    /// its own meaningful return type for direct (non-supervised)
    /// callers.
    type Output: Send + 'static;
    /// A short, stable identifier used in logs and the
    /// [`SupError::Child`](crate::SupError::Child) variant. The
    /// supervisor reads this once at registration time.
    fn name(&self) -> &str;
    /// Run one life-cycle. Returning `Ok` means "this run completed
    /// successfully"; returning `Err` means "this run failed". A panic
    /// inside the future is also classed as a failure and is captured
    /// by the supervisor without crashing the supervisor itself.
    fn run(&mut self) -> impl Future<Output = Result<Self::Output, SupError>> + Send;
}

/// How the supervisor reacts when one of its children exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartStrategy {
    /// Restart only the failed child. Other children are unaffected.
    OneForOne,
    /// On any child failure, terminate every child and restart the
    /// whole set in registration order.
    OneForAll,
    /// On a child failure, terminate that child and every child added
    /// after it, then restart the resulting suffix.
    RestForOne,
    /// Dynamic-children mode: children are added (and may exit) at
    /// runtime, restart semantics are per-child like
    /// [`OneForOne`](Self::OneForOne).
    SimpleOneForOne {
        /// The maximum number of children that may be alive at once.
        /// Adding past this cap returns
        /// [`SupError::ChildLimitReached`](crate::SupError::ChildLimitReached).
        max_children: usize,
    },
}

/// Whether a child is restarted after exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartPolicy {
    /// Always restart, no matter how the run ended.
    Permanent,
    /// Restart only on abnormal exit (panic or `Err`).
    Transient,
    /// Never restart; remove the child on any exit.
    Temporary,
}

/// Registration record for a single child.
///
/// Created by the embedder and consumed by
/// [`Supervisor::add_child`](crate::Supervisor::add_child).
pub struct ChildSpec<S: Supervised> {
    /// The implementor of [`Supervised`].
    pub spec: S,
    /// How this child is restarted after exit.
    pub restart: RestartPolicy,
    /// Backoff parameters for this child's restart timing.
    pub backoff: BackoffSpec,
}

/// Stable identifier returned by
/// [`Supervisor::add_child`](crate::Supervisor::add_child). Use this
/// to refer to a child in logs and in test assertions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChildId(pub(crate) u64);

impl std::fmt::Display for ChildId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "child#{}", self.0)
    }
}

/// Outcome of one run of a child, as observed by the supervisor.
#[derive(Debug, Clone)]
pub enum ChildExit {
    /// Run returned `Ok(_)`.
    Ok,
    /// Run returned `Err(_)`. The wrapped value is the displayed
    /// error.
    Err(String),
    /// Run panicked. The wrapped value is the displayed payload.
    Panic(String),
    /// Run was cancelled by the supervisor (during shutdown or a
    /// strategy-driven termination).
    Cancelled,
}

impl ChildExit {
    /// `true` for [`Self::Err`], [`Self::Panic`].
    /// `false` for [`Self::Ok`] and [`Self::Cancelled`].
    pub fn is_abnormal(&self) -> bool {
        matches!(self, Self::Err(_) | Self::Panic(_))
    }
}

/// Why the supervisor returned from [`Supervisor::run`](crate::Supervisor::run).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupExit {
    /// Shutdown was requested via
    /// [`SupervisorHandle::shutdown`](crate::SupervisorHandle::shutdown)
    /// or [`Supervisor::shutdown`](crate::Supervisor::shutdown).
    Shutdown,
    /// Every child has exited and none remain to be restarted.
    AllChildrenStopped,
    /// Shutdown was requested but at least one child did not
    /// terminate within the configured shutdown timeout.
    ShutdownTimeout {
        /// Number of children still alive when the timeout fired.
        remaining: usize,
    },
}
