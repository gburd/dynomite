//! Leveled logger built on `tracing` + `tracing-subscriber`.
//!
//! The Dynomite C engine uses an integer log level on the `-v` command
//! line (`LOG_EMERG = 0` ... `LOG_PVERB = 11`). The Rust port preserves
//! the numeric scale exactly: callers pass the same 0..11 verbosity and
//! [`tracing_level_for`] maps it onto the underlying `tracing` level
//! filter.
//!
//! On startup, [`log_init`] installs a global [`tracing_subscriber`]
//! that writes to either standard error or to a configurable log file.
//! Sending `SIGHUP` reopens the log file at the stored path; the helper
//! [`reopen_on_sighup`] is what the signal handler invokes.
//!
//! # Examples
//!
//! ```
//! use dynomite::core::log::{log_init, tracing_level_for, LOG_NOTICE};
//! use tracing::Level;
//!
//! assert_eq!(tracing_level_for(LOG_NOTICE), Level::INFO);
//! // `log_init` is process-global; this is illustrative only.
//! let _ = log_init(LOG_NOTICE, None);
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::OnceLock;

use parking_lot::Mutex;
use tracing::Level;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::EnvFilter;

use crate::core::types::{DynError, Status};

/// System is unusable.
pub const LOG_EMERG: u8 = 0;
/// Action must be taken immediately.
pub const LOG_ALERT: u8 = 1;
/// Critical conditions.
pub const LOG_CRIT: u8 = 2;
/// Error conditions.
pub const LOG_ERR: u8 = 3;
/// Warning conditions.
pub const LOG_WARN: u8 = 4;
/// Normal but significant condition (default).
pub const LOG_NOTICE: u8 = 5;
/// Informational.
pub const LOG_INFO: u8 = 6;
/// Debug messages.
pub const LOG_DEBUG: u8 = 7;
/// Verbose messages.
pub const LOG_VERB: u8 = 8;
/// Verbose messages, second tier.
pub const LOG_VVERB: u8 = 9;
/// Verbose messages, third tier.
pub const LOG_VVVERB: u8 = 10;
/// Periodic verbose messages.
pub const LOG_PVERB: u8 = 11;

/// Largest accepted verbosity level.
pub const LOG_LEVEL_MAX: u8 = LOG_PVERB;

/// Map a Dynomite numeric verbosity to a `tracing::Level`.
///
/// Levels 0..=4 are mapped to `ERROR`, 5..=6 to `INFO`, 7 to `DEBUG`,
/// and 8..=11 to `TRACE`. Values above [`LOG_LEVEL_MAX`] saturate to
/// `TRACE`.
///
/// # Examples
///
/// ```
/// use dynomite::core::log::{tracing_level_for, LOG_DEBUG, LOG_PVERB, LOG_WARN};
/// use tracing::Level;
///
/// assert_eq!(tracing_level_for(LOG_WARN), Level::ERROR);
/// assert_eq!(tracing_level_for(LOG_DEBUG), Level::DEBUG);
/// assert_eq!(tracing_level_for(LOG_PVERB), Level::TRACE);
/// ```
pub fn tracing_level_for(level: u8) -> Level {
    match level {
        0..=4 => Level::ERROR,
        5..=6 => Level::INFO,
        7 => Level::DEBUG,
        _ => Level::TRACE,
    }
}

/// Clamp the supplied verbosity to the inclusive `[0, LOG_LEVEL_MAX]`
/// window the C engine uses.
pub fn clamp_level(level: u8) -> u8 {
    level.min(LOG_LEVEL_MAX)
}

struct State {
    path: Mutex<Option<PathBuf>>,
    sink: Mutex<Box<dyn Write + Send>>,
    nerror: Mutex<u64>,
}

static STATE: OnceLock<State> = OnceLock::new();
static CURRENT_LEVEL: AtomicU8 = AtomicU8::new(LOG_NOTICE);

#[derive(Clone)]
struct LoggerWriter;

impl Write for LoggerWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let Some(state) = STATE.get() else {
            return io::stderr().write(buf);
        };
        let mut sink = state.sink.lock();
        match sink.write_all(buf) {
            Ok(()) => Ok(buf.len()),
            Err(err) => {
                *state.nerror.lock() += 1;
                Err(err)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        let Some(state) = STATE.get() else {
            return io::stderr().flush();
        };
        let mut sink = state.sink.lock();
        sink.flush()
    }
}

impl<'a> MakeWriter<'a> for LoggerWriter {
    type Writer = LoggerWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LoggerWriter
    }
}

fn open_log_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .append(true)
        .create(true)
        .mode_for_append()
        .open(path)
}

trait OpenOptionsExt {
    fn mode_for_append(&mut self) -> &mut Self;
}

impl OpenOptionsExt for OpenOptions {
    #[cfg(unix)]
    fn mode_for_append(&mut self) -> &mut Self {
        use std::os::unix::fs::OpenOptionsExt as _;
        self.mode(0o644)
    }
    #[cfg(not(unix))]
    fn mode_for_append(&mut self) -> &mut Self {
        self
    }
}

/// Install the global tracing subscriber.
///
/// `level` is the C-style numeric verbosity in `0..=LOG_LEVEL_MAX`.
/// Values above the maximum saturate. When `path` is `Some`, log
/// records are appended to that file (created if missing); when `None`,
/// records are written to standard error.
///
/// `log_init` may be called only once per process; subsequent calls
/// return [`DynError::Generic`].
///
/// # Examples
///
/// ```no_run
/// use dynomite::core::log::{log_init, LOG_NOTICE};
/// log_init(LOG_NOTICE, None).expect("install logger");
/// ```
pub fn log_init(level: u8, path: Option<&Path>) -> Status {
    let sink: Box<dyn Write + Send> = match path {
        Some(p) => Box::new(open_log_file(p).map_err(DynError::Io)?),
        None => Box::new(io::stderr()),
    };
    let stored_path = path.map(PathBuf::from);

    let state = State {
        path: Mutex::new(stored_path),
        sink: Mutex::new(sink),
        nerror: Mutex::new(0),
    };
    STATE
        .set(state)
        .map_err(|_| DynError::generic("log_init: subscriber already installed"))?;

    let level_filter = LevelFilter::from_level(tracing_level_for(clamp_level(level)));
    CURRENT_LEVEL.store(clamp_level(level), Ordering::Relaxed);
    let env = EnvFilter::builder()
        .with_default_directive(level_filter.into())
        .from_env_lossy();

    tracing_subscriber::fmt()
        .with_env_filter(env)
        .with_writer(LoggerWriter)
        .with_target(true)
        .try_init()
        .map_err(|e| DynError::generic(format!("log_init: {e}")))?;

    Ok(())
}

/// Reopen the log file at the path remembered by [`log_init`].
///
/// Intended to be invoked from the SIGHUP handler. When the active sink
/// is standard error (no path was set), this is a no-op and returns
/// [`Ok`]. When the file cannot be reopened, the previous sink is left
/// in place and the error is returned.
///
/// # Examples
///
/// ```no_run
/// use dynomite::core::log::reopen_on_sighup;
/// reopen_on_sighup().expect("reopen log");
/// ```
pub fn reopen_on_sighup() -> Status {
    let state = STATE
        .get()
        .ok_or_else(|| DynError::generic("reopen_on_sighup: log not initialised"))?;
    let path_guard = state.path.lock();
    let Some(path) = path_guard.as_ref() else {
        return Ok(());
    };
    let new_file = open_log_file(path).map_err(DynError::Io)?;
    *state.sink.lock() = Box::new(new_file);
    Ok(())
}

/// Number of write errors observed by the underlying sink.
///
/// Exposes the equivalent of the C `logger.nerror` counter.
pub fn write_error_count() -> u64 {
    STATE.get().map_or(0, |s| *s.nerror.lock())
}

/// Return the current numeric verbosity stored by [`log_init`].
pub fn current_level() -> u8 {
    CURRENT_LEVEL.load(Ordering::Relaxed)
}

/// Bump the stored verbosity by one, saturating at [`LOG_LEVEL_MAX`].
///
/// The actual `tracing` filter is set once at [`log_init`] time; this
/// updates a numeric counter that downstream subsystems read to gate
/// periodic-verbose output.
pub fn log_level_increment() -> u8 {
    let prev = CURRENT_LEVEL.load(Ordering::Relaxed);
    let next = clamp_level(prev.saturating_add(1));
    CURRENT_LEVEL.store(next, Ordering::Relaxed);
    next
}

/// Drop the stored verbosity by one, saturating at zero.
pub fn log_level_decrement() -> u8 {
    let prev = CURRENT_LEVEL.load(Ordering::Relaxed);
    let next = prev.saturating_sub(1);
    CURRENT_LEVEL.store(next, Ordering::Relaxed);
    next
}

/// Set the stored verbosity directly, clamped to `[0, LOG_LEVEL_MAX]`.
pub fn log_level_set(level: u8) {
    CURRENT_LEVEL.store(clamp_level(level), Ordering::Relaxed);
}

/// Return whether a given numeric level is loud enough to be logged.
///
/// A message at `level` is loggable iff `level <= current_level()`.
pub fn log_loggable(level: u8) -> bool {
    level <= current_level()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_mapping_is_monotone_in_verbosity() {
        // Map both directions through a monotonic integer scale to be
        // robust against the orientation of `tracing::Level`'s `Ord`
        // impl, which has flipped between releases.
        let severity = |l: Level| -> u8 {
            match l {
                Level::ERROR => 0,
                Level::WARN => 1,
                Level::INFO => 2,
                Level::DEBUG => 3,
                Level::TRACE => 4,
            }
        };
        let mut prev = severity(tracing_level_for(0));
        for lvl in 1..=LOG_LEVEL_MAX {
            let cur = severity(tracing_level_for(lvl));
            assert!(cur >= prev, "level {lvl}: severity {cur} not >= {prev}");
            prev = cur;
        }
    }

    #[test]
    fn clamp_saturates() {
        assert_eq!(clamp_level(0), 0);
        assert_eq!(clamp_level(LOG_LEVEL_MAX), LOG_LEVEL_MAX);
        assert_eq!(clamp_level(LOG_LEVEL_MAX + 5), LOG_LEVEL_MAX);
        assert_eq!(clamp_level(255), LOG_LEVEL_MAX);
    }

    #[test]
    fn level_constants_match_c() {
        assert_eq!(LOG_EMERG, 0);
        assert_eq!(LOG_ALERT, 1);
        assert_eq!(LOG_CRIT, 2);
        assert_eq!(LOG_ERR, 3);
        assert_eq!(LOG_WARN, 4);
        assert_eq!(LOG_NOTICE, 5);
        assert_eq!(LOG_INFO, 6);
        assert_eq!(LOG_DEBUG, 7);
        assert_eq!(LOG_VERB, 8);
        assert_eq!(LOG_VVERB, 9);
        assert_eq!(LOG_VVVERB, 10);
        assert_eq!(LOG_PVERB, 11);
    }

    #[test]
    fn level_increment_and_decrement_saturate() {
        log_level_set(0);
        assert_eq!(log_level_decrement(), 0);
        for _ in 0..(u32::from(LOG_LEVEL_MAX) + 5) {
            log_level_increment();
        }
        assert_eq!(current_level(), LOG_LEVEL_MAX);
        log_level_set(5);
        assert!(log_loggable(0));
        assert!(log_loggable(5));
        assert!(!log_loggable(6));
    }
}
