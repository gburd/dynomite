//! Error type for the supervisor.

/// Errors that escape a supervisor or one of its children.
///
/// Children return `Result<S::Output, SupError>` from their
/// [`Supervised::run`](crate::Supervised::run) method. The supervisor
/// itself returns `SupError` from [`Supervisor::shutdown`] and uses
/// `SupError::Child` internally to record an abnormal exit.
///
/// [`Supervisor::shutdown`]: crate::Supervisor::shutdown
#[derive(Debug, thiserror::Error)]
pub enum SupError {
    /// A child returned an error from `run`. The wrapped string is the
    /// child's name plus the message produced by the child's own
    /// `Display` for its underlying error.
    #[error("child '{name}' failed: {message}")]
    Child {
        /// The child's reported name.
        name: String,
        /// A human-readable description of what the child reported.
        message: String,
    },
    /// A child panicked. The wrapped string is whatever payload could
    /// be extracted from the panic (the `&str` or `String` payload, or
    /// `<panic>` if the payload was of an unknown type).
    #[error("child '{name}' panicked: {payload}")]
    Panic {
        /// The child's reported name.
        name: String,
        /// The downcast text of the panic payload.
        payload: String,
    },
    /// `Supervisor::shutdown` could not signal the running supervisor.
    /// This happens only when the supervisor task has already exited.
    #[error("supervisor not running")]
    NotRunning,
    /// A child could not be added because the strategy's child limit
    /// would be exceeded (currently emitted only for
    /// [`SimpleOneForOne`](crate::RestartStrategy::SimpleOneForOne)).
    #[error("supervisor child limit reached")]
    ChildLimitReached,
    /// The supervisor was asked to shut down but at least one child
    /// failed to terminate within the configured shutdown timeout.
    #[error("shutdown timed out with {remaining} child(ren) still running")]
    ShutdownTimeout {
        /// Number of children still alive when the timeout fired.
        remaining: usize,
    },
}
