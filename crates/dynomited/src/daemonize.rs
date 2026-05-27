//! Process daemonization for `dynomited`.
//!
//! [`daemonize`] reproduces the reference engine's `dn_daemonize`
//! flow: fork once to escape the controlling terminal, `setsid` to
//! become a session leader, fork a second time so the daemon can
//! never re-acquire a controlling tty, optionally `chdir("/")`,
//! `umask(0)`, and redirect `stdin` / `stdout` / `stderr` to
//! `/dev/null`.
//!
//! # Safety invariants
//!
//! `nix::unistd::fork` is `unsafe` because the post-fork child is
//! restricted to async-signal-safe operations. We honour that
//! contract by:
//!
//! * Calling [`daemonize`] **before** the tokio runtime starts,
//!   before any signal handlers are installed, before any logger
//!   sink is opened, and before any worker threads are spawned.
//!   The Rust process at fork time owns at most one OS thread (the
//!   main thread) plus the platform default signal mask.
//! * Performing only `setsid`, `fork`, `chdir`, `umask`, `open`,
//!   `dup2`, and `close` between the fork and the eventual return
//!   to `main`. None of those allocate; none take a Rust mutex.
//! * Returning `DaemonizeOutcome::Parent` from the parent so the
//!   caller can `exit(0)` immediately. Only the second-generation
//!   child returns `DaemonizeOutcome::Child` and continues into
//!   `Server::run`.
//!
//! The `unsafe` block is therefore fenced: the only `unsafe` call
//! is the `fork()` invocation itself, justified by the invariants
//! above.

#![allow(unsafe_code)]

use std::io;
use std::path::Path;

use nix::sys::stat::{umask, Mode};
use nix::unistd::{chdir, dup2_stderr, dup2_stdin, dup2_stdout, fork, setsid, ForkResult};

/// Outcome of [`daemonize`].
#[derive(Debug, Eq, PartialEq)]
pub enum DaemonizeOutcome {
    /// First or second parent process. The caller must exit
    /// immediately with status 0; staying alive would leave a
    /// stray process in the original session.
    Parent,
    /// Final child process. The caller continues into the runtime
    /// loop.
    Child,
}

/// Daemonize the current process.
///
/// `dump_core` mirrors the reference engine's parameter: when
/// `false`, the process changes its working directory to `/` so
/// any core dump lands at the root rather than under the user's
/// `cwd`. Pass `true` to preserve the working directory (matching
/// the reference engine, which calls `dn_daemonize(1)`).
///
/// # Errors
/// Forwarded from any of the underlying syscalls.
pub fn daemonize(dump_core: bool) -> io::Result<DaemonizeOutcome> {
    // Stage 1: fork() to escape the foreground process group.
    //
    // # Safety
    //
    // See module docs. The function is called from `main` before
    // tokio, before signal handlers, before any allocation that
    // depends on lazily-initialised globals.
    let stage1 = unsafe { fork() }.map_err(io_err)?;
    if matches!(stage1, ForkResult::Parent { .. }) {
        return Ok(DaemonizeOutcome::Parent);
    }

    // First child: become a session leader.
    setsid().map_err(io_err)?;

    // Stage 2: fork() once more so the session leader exits and
    // the final child can never reacquire a controlling tty.
    //
    // # Safety
    //
    // Same justification as stage 1. We have not yet started any
    // tokio worker, opened any file, or registered any signal
    // handler since stage 1.
    let stage2 = unsafe { fork() }.map_err(io_err)?;
    if matches!(stage2, ForkResult::Parent { .. }) {
        return Ok(DaemonizeOutcome::Parent);
    }

    if !dump_core {
        chdir(Path::new("/")).map_err(io_err)?;
    }

    // Clear the file mode creation mask so config-driven
    // permissions on later `open` calls land verbatim.
    umask(Mode::empty());

    redirect_stdio_to_devnull()?;

    Ok(DaemonizeOutcome::Child)
}

fn redirect_stdio_to_devnull() -> io::Result<()> {
    // OpenOptions::open is not available for /dev/null with both
    // read and write semantics in one fd, but std::fs::File::open
    // only opens read-only. Use OpenOptions instead.
    let dev_null = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")?;
    dup2_stdin(&dev_null).map_err(io_err)?;
    dup2_stdout(&dev_null).map_err(io_err)?;
    dup2_stderr(&dev_null).map_err(io_err)?;
    Ok(())
}

fn io_err(e: nix::errno::Errno) -> io::Error {
    io::Error::from_raw_os_error(e as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    // We deliberately do not call [`daemonize`] from a unit test:
    // forking a test binary leaves orphan processes and confuses
    // nextest. The behaviour is exercised by an integration test
    // that wraps the real binary.
    #[test]
    fn outcome_equality() {
        assert_eq!(DaemonizeOutcome::Parent, DaemonizeOutcome::Parent);
        assert_ne!(DaemonizeOutcome::Parent, DaemonizeOutcome::Child);
    }
}
