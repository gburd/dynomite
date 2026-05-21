//! Async signal handling for `dynomited`.
//!
//! Installs a tokio-driven [`SignalSet`] that maps `SIGINT`,
//! `SIGTERM`, `SIGHUP`, `SIGTTIN`, `SIGTTOU`, `SIGUSR1`,
//! `SIGUSR2`, and `SIGSEGV` to a single async stream of
//! [`SignalEvent`] values. The run loop selects on this
//! stream and dispatches each event to the appropriate
//! handler (graceful shutdown, log reopen, log-level toggle).
//!
//! Stage 12b only consumes the four signals the run loop must
//! react to:
//!
//! * `SIGINT` and `SIGTERM` -> graceful shutdown.
//! * `SIGHUP` -> reopen the log file (delegates to
//!   [`dynomite::core::log::reopen_on_sighup`]).
//! * `SIGPIPE` is ignored implicitly by tokio (writes return
//!   `EPIPE`); we do not attach a stream for it.
//!
//! `SIGTTIN` / `SIGTTOU` (log-level up / down) and `SIGUSR1` /
//! `SIGUSR2` are deferred until the embed API exposes the
//! corresponding control hooks; they are listed in
//! `docs/parity.md` as deferred rows.

use std::io;

use tokio::signal::unix::{signal, Signal as UnixSignal, SignalKind};

/// One of the signals [`SignalSet`] can deliver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SignalEvent {
    /// `SIGINT` (Ctrl-C). The C handler logs and exits.
    Interrupt,
    /// `SIGTERM`. Treated as a graceful-shutdown request.
    Terminate,
    /// `SIGHUP`. The C handler reopens the log file.
    Hangup,
}

/// Bundle of tokio `signal` streams the run loop selects on.
///
/// Construct via [`SignalSet::install`]; each call installs one
/// handler per signal kind. The streams remain installed for the
/// lifetime of the [`SignalSet`].
///
/// # Examples
///
/// ```no_run
/// use dynomited::signals::SignalSet;
/// # async fn _example() -> std::io::Result<()> {
/// let mut signals = SignalSet::install()?;
/// let _ = signals.recv().await;
/// # Ok(()) }
/// ```
pub struct SignalSet {
    sigint: UnixSignal,
    sigterm: UnixSignal,
    sighup: UnixSignal,
}

impl SignalSet {
    /// Install handlers for `SIGINT`, `SIGTERM`, and `SIGHUP`.
    ///
    /// # Errors
    /// Forwarded from `tokio::signal::unix::signal`. The function
    /// fails on platforms that lack the underlying signal kind or
    /// when the kernel rejects the registration (e.g. inside a
    /// constrained sandbox).
    pub fn install() -> io::Result<Self> {
        Ok(Self {
            sigint: signal(SignalKind::interrupt())?,
            sigterm: signal(SignalKind::terminate())?,
            sighup: signal(SignalKind::hangup())?,
        })
    }

    /// Wait for the next signal in the set.
    ///
    /// Returns `None` only if every underlying signal stream has
    /// been closed by the runtime; in normal operation the future
    /// always resolves to `Some(SignalEvent::*)`.
    pub async fn recv(&mut self) -> Option<SignalEvent> {
        tokio::select! {
            biased;
            v = self.sigterm.recv() => v.map(|()| SignalEvent::Terminate),
            v = self.sigint.recv()  => v.map(|()| SignalEvent::Interrupt),
            v = self.sighup.recv()  => v.map(|()| SignalEvent::Hangup),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn install_signal_set() {
        // The set must install without error inside a tokio
        // runtime. We do not raise a signal: nextest forks tests
        // off the same process group and a self-sent SIGTERM
        // would terminate the entire test runner.
        let _set = SignalSet::install().unwrap();
    }

    #[test]
    fn signal_event_is_copy() {
        let a = SignalEvent::Interrupt;
        let b = a;
        assert_eq!(a, b);
        assert_ne!(SignalEvent::Hangup, SignalEvent::Terminate);
    }
}
