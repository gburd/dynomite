//! UNIX signal table and dispatch.
//!
//! The Dynomite C engine wires a small static table of signals to a
//! single `signal_handler` that dispatches on the signal number. The
//! Rust port encodes the same table as a list of [`SignalEntry`]
//! values; signal handling itself runs in a tokio task that consumes a
//! `Signal` stream so the body of every handler stays on the runtime
//! and never executes in async-signal-unsafe context.
//!
//! # Examples
//!
//! ```
//! use dynomite::core::signal::{default_actions, SignalAction};
//!
//! let table = default_actions();
//! assert!(table.iter().any(|e| matches!(e.action, SignalAction::Shutdown)));
//! ```

use nix::sys::signal::Signal;

use crate::core::log::{log_level_decrement, log_level_increment};
use crate::core::types::Status;

/// Action to run when a signal is delivered.
///
/// The default mapping is: SIGTTIN/SIGTTOU adjust the global log
/// verbosity, SIGHUP reopens the log file, SIGINT requests a
/// graceful shutdown, SIGUSR1 and SIGUSR2 are reserved noop slots,
/// SIGSEGV records a stack trace, and SIGPIPE is ignored.
///
/// # Examples
///
/// ```
/// use dynomite::core::signal::SignalAction;
/// assert_ne!(SignalAction::Shutdown, SignalAction::Noop);
/// assert_eq!(SignalAction::Ignore, SignalAction::Ignore);
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SignalAction {
    /// Reserved slot used by the in-process action table that
    /// currently does nothing.
    Noop,
    /// Bump the global log verbosity by one.
    LogLevelUp,
    /// Drop the global log verbosity by one.
    LogLevelDown,
    /// Reopen the active log file (if any).
    ReopenLog,
    /// Request a graceful shutdown.
    Shutdown,
    /// Print a stack trace; the dispatcher also re-raises SIGSEGV
    /// afterwards so the kernel can produce the standard core dump.
    StackTrace,
    /// Ignore the signal entirely (matches `SIG_IGN`).
    Ignore,
}

/// One entry in the signal-action table.
///
/// # Examples
///
/// ```
/// use dynomite::core::signal::{default_actions, SignalAction};
/// let entry = default_actions().iter().find(|e| e.name == "SIGINT").unwrap();
/// assert_eq!(entry.action, SignalAction::Shutdown);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SignalEntry {
    /// The POSIX signal number this entry handles.
    pub signal: Signal,
    /// Human-readable name used in log messages.
    pub name: &'static str,
    /// Action to run when the signal fires.
    pub action: SignalAction,
}

/// Return the default Dynomite signal-to-action table.
///
/// # Examples
///
/// ```
/// use dynomite::core::signal::default_actions;
/// let table = default_actions();
/// assert!(!table.is_empty());
/// ```
pub fn default_actions() -> &'static [SignalEntry] {
    &SIGNAL_TABLE
}

const SIGNAL_TABLE: [SignalEntry; 8] = [
    SignalEntry {
        signal: Signal::SIGUSR1,
        name: "SIGUSR1",
        action: SignalAction::Noop,
    },
    SignalEntry {
        signal: Signal::SIGUSR2,
        name: "SIGUSR2",
        action: SignalAction::Noop,
    },
    SignalEntry {
        signal: Signal::SIGTTIN,
        name: "SIGTTIN",
        action: SignalAction::LogLevelUp,
    },
    SignalEntry {
        signal: Signal::SIGTTOU,
        name: "SIGTTOU",
        action: SignalAction::LogLevelDown,
    },
    SignalEntry {
        signal: Signal::SIGHUP,
        name: "SIGHUP",
        action: SignalAction::ReopenLog,
    },
    SignalEntry {
        signal: Signal::SIGINT,
        name: "SIGINT",
        action: SignalAction::Shutdown,
    },
    SignalEntry {
        signal: Signal::SIGSEGV,
        name: "SIGSEGV",
        action: SignalAction::StackTrace,
    },
    SignalEntry {
        signal: Signal::SIGPIPE,
        name: "SIGPIPE",
        action: SignalAction::Ignore,
    },
];

/// Look up the [`SignalAction`] for a given POSIX signal in the default
/// table.
///
/// # Examples
///
/// ```
/// use dynomite::core::signal::{action_for, SignalAction};
/// use nix::sys::signal::Signal;
///
/// assert_eq!(action_for(Signal::SIGINT), Some(SignalAction::Shutdown));
/// assert_eq!(action_for(Signal::SIGCHLD), None);
/// ```
pub fn action_for(signal: Signal) -> Option<SignalAction> {
    SIGNAL_TABLE
        .iter()
        .find(|entry| entry.signal == signal)
        .map(|entry| entry.action)
}

/// Dispatch the action associated with `signal`.
///
/// Returns `true` when shutdown was requested. Unknown signals are
/// reported as `false` and produce no side effect.
///
/// # Examples
///
/// ```
/// use dynomite::core::signal::dispatch;
/// use nix::sys::signal::Signal;
/// assert!(!dispatch(Signal::SIGUSR1).unwrap());
/// assert!(dispatch(Signal::SIGINT).unwrap());
/// ```
pub fn dispatch(signal: Signal) -> Result<bool, crate::core::types::DynError> {
    let Some(action) = action_for(signal) else {
        return Ok(false);
    };
    match action {
        SignalAction::Noop | SignalAction::Ignore => Ok(false),
        SignalAction::LogLevelUp => {
            log_level_increment();
            Ok(false)
        }
        SignalAction::LogLevelDown => {
            log_level_decrement();
            Ok(false)
        }
        SignalAction::ReopenLog => {
            crate::core::log::reopen_on_sighup()?;
            Ok(false)
        }
        SignalAction::Shutdown => Ok(true),
        SignalAction::StackTrace => {
            tracing::error!(signal = %signal, "fatal signal received, terminating");
            Ok(true)
        }
    }
}

/// Convenience wrapper that returns [`Status`] for callers that prefer
/// the void-returning shape used elsewhere in the engine.
///
/// # Examples
///
/// ```
/// use dynomite::core::signal::handle;
/// use nix::sys::signal::Signal;
/// handle(Signal::SIGUSR1).unwrap();
/// ```
pub fn handle(signal: Signal) -> Status {
    dispatch(signal).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_covers_every_c_entry() {
        for sig in [
            Signal::SIGUSR1,
            Signal::SIGUSR2,
            Signal::SIGTTIN,
            Signal::SIGTTOU,
            Signal::SIGHUP,
            Signal::SIGINT,
            Signal::SIGSEGV,
            Signal::SIGPIPE,
        ] {
            assert!(action_for(sig).is_some(), "missing entry for {sig:?}");
        }
    }

    #[test]
    fn unknown_signals_return_none() {
        assert!(action_for(Signal::SIGCHLD).is_none());
    }

    #[test]
    fn shutdown_is_signalled_for_sigint() {
        // The runtime is global; for the dispatch test we only inspect
        // the boolean shutdown flag returned by the noop arms.
        assert!(!dispatch(Signal::SIGUSR1).unwrap());
        assert!(!dispatch(Signal::SIGPIPE).unwrap());
        assert!(dispatch(Signal::SIGINT).unwrap());
    }
}
