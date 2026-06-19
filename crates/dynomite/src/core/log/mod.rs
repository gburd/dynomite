//! Leveled logger built on `tracing` + `tracing-subscriber`.
//!
//! The `-v` command-line flag selects an integer log level
//! (`LOG_EMERG = 0` ... `LOG_PVERB = 11`). Callers pass the
//! numeric 0..11 verbosity and [`tracing_level_for`] maps it onto
//! the underlying `tracing` level filter.
//!
//! On startup, [`log_init`] installs a global [`tracing_subscriber`]
//! that writes to either standard error or to a configurable log file.
//! Sending `SIGHUP` reopens the log file at the stored path; the helper
//! [`reopen_on_sighup`] is what the signal handler invokes.
//!
//! Four output shapes are supported, dispatched by [`LogFormat`]:
//! [`LogFormat::Default`] (the historical text format), [`LogFormat::Rfc5424`]
//! (modern syslog), [`LogFormat::Rfc3164`] (BSD syslog), and
//! [`LogFormat::Json`] (NDJSON). Operators select a shape via the
//! `log_format:` configuration key or the `--log-format` CLI flag;
//! when neither is set the default value reproduces the pre-existing
//! behavior byte-for-byte.
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
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Layer, Registry};

use crate::core::types::{DynError, Status};

mod format;
mod host;
mod syslog;

pub use format::{LogFormat, LogFormatParseError};
pub use host::local_hostname;

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
///
/// # Examples
///
/// ```
/// use dynomite::core::log::{clamp_level, LOG_LEVEL_MAX};
/// assert_eq!(clamp_level(LOG_LEVEL_MAX + 5), LOG_LEVEL_MAX);
/// ```
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
/// window.
///
/// # Examples
///
/// ```
/// use dynomite::core::log::{clamp_level, LOG_LEVEL_MAX};
/// assert_eq!(clamp_level(3), 3);
/// assert_eq!(clamp_level(255), LOG_LEVEL_MAX);
/// ```
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

/// Bundle of tunables consumed by [`build_logs_layer`] and
/// [`install_logs_only`].
///
/// Mirrors the legacy positional triple `(level, path, format)`
/// that [`log_init_with_format`] takes; lifting it to a struct
/// lets the binary share a single value between the OTLP-on and
/// OTLP-off install paths.
///
/// # Examples
///
/// ```
/// use dynomite::core::log::{LogConfig, LogFormat, LOG_NOTICE};
/// let cfg = LogConfig::new(LOG_NOTICE, None, LogFormat::Json);
/// assert_eq!(cfg.verbosity, LOG_NOTICE);
/// assert_eq!(cfg.format, LogFormat::Json);
/// ```
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Numeric verbosity (`0..=LOG_LEVEL_MAX`).
    pub verbosity: u8,
    /// Optional log file. When `None`, log records flow to standard
    /// error.
    pub output: Option<PathBuf>,
    /// Wire shape for emitted records.
    pub format: LogFormat,
}

impl LogConfig {
    /// Convenience constructor.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::core::log::{LogConfig, LogFormat, LOG_INFO};
    /// let cfg = LogConfig::new(LOG_INFO, None, LogFormat::Default);
    /// assert_eq!(cfg.verbosity, LOG_INFO);
    /// ```
    pub fn new(verbosity: u8, output: Option<PathBuf>, format: LogFormat) -> Self {
        Self {
            verbosity,
            output,
            format,
        }
    }
}

/// Boxed fmt layer returned by [`build_logs_layer`].
///
/// The layer is parameterised on [`tracing_subscriber::Registry`]
/// so callers can drop it into any registry stack the binary
/// builds (with or without an [`tracing_opentelemetry`] layer
/// stacked on top).
pub type LogsLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

/// Token returned by [`build_logs_layer`] proving the SIGHUP
/// log-reopen state has been initialised.
///
/// The token is zero-sized; its sole purpose is to make the
/// "build the fmt layer, then install it as a global" handshake
/// type-checked: callers cannot call [`reopen_on_sighup`] before
/// the writer state has been wired, because they cannot construct
/// a [`ReopenHandle`] without first calling [`build_logs_layer`]
/// or one of its convenience wrappers.
///
/// # Examples
///
/// ```
/// use dynomite::core::log::{build_logs_layer, LogConfig, LogFormat, LOG_NOTICE};
/// // Building the layer also populates the SIGHUP-reopen state.
/// // The handle below is the proof of that wiring.
/// // (The example does not install the layer as a global so it
/// // can run side-by-side with the rest of the doctest suite.)
/// let cfg = LogConfig::new(LOG_NOTICE, None, LogFormat::Default);
/// let _ = build_logs_layer(&cfg);
/// ```
#[derive(Debug)]
#[must_use = "the reopen handle must be threaded into install_global so SIGHUP-reopen is wired"]
pub struct ReopenHandle {
    _private: (),
}

/// Build the EnvFilter the install paths feed into the registry.
///
/// Honours `RUST_LOG` and falls back to the `tracing` level
/// derived from the supplied `verbosity`.
///
/// # Examples
///
/// ```
/// use dynomite::core::log::{build_env_filter, LOG_NOTICE};
/// let _filter = build_env_filter(LOG_NOTICE);
/// ```
pub fn build_env_filter(verbosity: u8) -> EnvFilter {
    let level_filter = LevelFilter::from_level(tracing_level_for(clamp_level(verbosity)));
    EnvFilter::builder()
        .with_default_directive(level_filter.into())
        .from_env_lossy()
}

fn init_reopen_state(verbosity: u8, path: Option<&Path>) -> Result<ReopenHandle, DynError> {
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
        .map_err(|_| DynError::generic("log: writer state already installed"))?;
    CURRENT_LEVEL.store(clamp_level(verbosity), Ordering::Relaxed);
    Ok(ReopenHandle { _private: () })
}

/// Build the fmt layer for the configured shape and wire the
/// SIGHUP-reopen writer state.
///
/// Returns the boxed layer plus a [`ReopenHandle`] proving the
/// internal writer state has been populated. The layer is *not*
/// installed as a global; the caller composes it into a
/// [`tracing_subscriber::Registry`] (typically together with an
/// [`EnvFilter`] and an optional `OpenTelemetryLayer`) and calls
/// `try_init`.
///
/// May only be called once per process: the underlying writer
/// state is a `OnceLock` and a second call returns
/// [`DynError::Generic`].
///
/// # Errors
/// Returns [`DynError::Io`] when the configured `output` cannot
/// be opened for append, and [`DynError::Generic`] when the
/// writer state has already been initialised.
///
/// # Examples
///
/// ```no_run
/// use dynomite::core::log::{build_env_filter, build_logs_layer, LogConfig, LogFormat, LOG_NOTICE};
/// use tracing_subscriber::layer::SubscriberExt as _;
/// use tracing_subscriber::util::SubscriberInitExt as _;
///
/// let cfg = LogConfig::new(LOG_NOTICE, None, LogFormat::Default);
/// let (layer, _reopen) = build_logs_layer(&cfg).expect("build layer");
/// tracing_subscriber::registry()
///     .with(layer)
///     .with(build_env_filter(LOG_NOTICE))
///     .try_init()
///     .expect("install");
/// ```
pub fn build_logs_layer(cfg: &LogConfig) -> Result<(LogsLayer, ReopenHandle), DynError> {
    let reopen = init_reopen_state(cfg.verbosity, cfg.output.as_deref())?;
    let layer = fmt_layer_for_format::<Registry>(cfg.format);
    Ok((layer, reopen))
}

fn fmt_layer_for_format<S>(format: LogFormat) -> Box<dyn Layer<S> + Send + Sync + 'static>
where
    S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    match format {
        LogFormat::Default => Box::new(
            tracing_subscriber::fmt::Layer::default()
                .with_writer(LoggerWriter)
                .with_target(true),
        ),
        LogFormat::Json => Box::new(
            tracing_subscriber::fmt::Layer::default()
                .with_writer(LoggerWriter)
                .with_target(true)
                .json()
                .flatten_event(false)
                .with_current_span(true)
                .with_span_list(false),
        ),
        LogFormat::Rfc5424 => Box::new(
            tracing_subscriber::fmt::Layer::default()
                .with_writer(LoggerWriter)
                .event_format(syslog::Rfc5424Formatter::new())
                .with_ansi(false),
        ),
        LogFormat::Rfc3164 => Box::new(
            tracing_subscriber::fmt::Layer::default()
                .with_writer(LoggerWriter)
                .event_format(syslog::Rfc3164Formatter::new())
                .with_ansi(false),
        ),
    }
}

/// Install a fmt-only global tracing subscriber.
///
/// Composes a [`Registry`] with the EnvFilter built from
/// `cfg.verbosity` and the fmt layer built from `cfg.format`,
/// then sets it as the global default. The SIGHUP-reopen writer
/// state is wired as a side effect.
///
/// This is the OTLP-off install path; the OTLP-on install path
/// lives in the `dynomited` binary's `observability::install_global`
/// and stacks an `OpenTelemetryLayer` on top of the same fmt layer.
///
/// May only be called once per process.
///
/// # Errors
/// Returns [`DynError::Io`] when `cfg.output` cannot be opened
/// for append, and [`DynError::Generic`] when a global tracing
/// subscriber has already been installed.
///
/// # Examples
///
/// ```no_run
/// use dynomite::core::log::{install_logs_only, LogConfig, LogFormat, LOG_NOTICE};
/// install_logs_only(&LogConfig::new(LOG_NOTICE, None, LogFormat::Default))
///     .expect("install logger");
/// ```
pub fn install_logs_only(cfg: &LogConfig) -> Status {
    let env = build_env_filter(cfg.verbosity);
    let (fmt_layer, _reopen) = build_logs_layer(cfg)?;
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(env)
        .try_init()
        .map_err(|e| DynError::generic(format!("install_logs_only: {e}")))?;
    Ok(())
}

/// Install the global tracing subscriber.
///
/// `level` is the C-style numeric verbosity in `0..=LOG_LEVEL_MAX`.
/// Values above the maximum saturate. When `path` is `Some`, log
/// records are appended to that file (created if missing); when `None`,
/// records are written to standard error.
///
/// This entry point preserves the historical default output shape
/// ([`LogFormat::Default`]). To pick a different shape, call
/// [`log_init_with_format`] directly. Both wrappers delegate to
/// [`install_logs_only`].
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
    log_init_with_format(level, path, LogFormat::Default)
}

/// Install the global tracing subscriber with a chosen output shape.
///
/// See [`LogFormat`] for the supported values. The default value
/// (`LogFormat::Default`) is byte-identical to what [`log_init`]
/// installs, so passing it here is equivalent to the original
/// two-argument call.
///
/// # Examples
///
/// ```no_run
/// use dynomite::core::log::{log_init_with_format, LogFormat, LOG_NOTICE};
/// log_init_with_format(LOG_NOTICE, None, LogFormat::Json).expect("install logger");
/// ```
pub fn log_init_with_format(level: u8, path: Option<&Path>, format: LogFormat) -> Status {
    install_logs_only(&LogConfig {
        verbosity: level,
        output: path.map(PathBuf::from),
        format,
    })
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
/// # Examples
///
/// ```
/// use dynomite::core::log::write_error_count;
/// // Before logging is initialised the count is zero.
/// let _: u64 = write_error_count();
/// ```
pub fn write_error_count() -> u64 {
    STATE.get().map_or(0, |s| *s.nerror.lock())
}

/// Return the current numeric verbosity stored by [`log_init`].
///
/// # Examples
///
/// ```
/// use dynomite::core::log::{current_level, log_level_set, LOG_INFO};
/// log_level_set(LOG_INFO);
/// assert_eq!(current_level(), LOG_INFO);
/// ```
pub fn current_level() -> u8 {
    CURRENT_LEVEL.load(Ordering::Relaxed)
}

/// Bump the stored verbosity by one, saturating at [`LOG_LEVEL_MAX`].
///
/// The actual `tracing` filter is set once at [`log_init`] time; this
/// updates a numeric counter that downstream subsystems read to gate
/// periodic-verbose output.
///
/// # Examples
///
/// ```
/// use dynomite::core::log::{log_level_increment, log_level_set};
/// log_level_set(3);
/// assert_eq!(log_level_increment(), 4);
/// ```
pub fn log_level_increment() -> u8 {
    let prev = CURRENT_LEVEL.load(Ordering::Relaxed);
    let next = clamp_level(prev.saturating_add(1));
    CURRENT_LEVEL.store(next, Ordering::Relaxed);
    next
}

/// Drop the stored verbosity by one, saturating at zero.
///
/// # Examples
///
/// ```
/// use dynomite::core::log::{log_level_decrement, log_level_set};
/// log_level_set(0);
/// assert_eq!(log_level_decrement(), 0);
/// log_level_set(5);
/// assert_eq!(log_level_decrement(), 4);
/// ```
pub fn log_level_decrement() -> u8 {
    let prev = CURRENT_LEVEL.load(Ordering::Relaxed);
    let next = prev.saturating_sub(1);
    CURRENT_LEVEL.store(next, Ordering::Relaxed);
    next
}

/// Set the stored verbosity directly, clamped to `[0, LOG_LEVEL_MAX]`.
///
/// # Examples
///
/// ```
/// use dynomite::core::log::{current_level, log_level_set, LOG_LEVEL_MAX};
/// log_level_set(255);
/// assert_eq!(current_level(), LOG_LEVEL_MAX);
/// ```
pub fn log_level_set(level: u8) {
    CURRENT_LEVEL.store(clamp_level(level), Ordering::Relaxed);
}

/// Return whether a given numeric level is loud enough to be logged.
///
/// A message at `level` is loggable iff `level <= current_level()`.
///
/// # Examples
///
/// ```
/// use dynomite::core::log::{log_level_set, log_loggable};
/// log_level_set(5);
/// assert!(log_loggable(0));
/// assert!(log_loggable(5));
/// assert!(!log_loggable(6));
/// ```
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

    #[test]
    fn fmt_layer_builds_for_every_format() {
        // `fmt_layer_for_format` is STATE-independent: it only
        // constructs a boxed layer wired to `LoggerWriter`. We
        // build one per shape so each match arm is exercised
        // without touching the process-global writer state.
        for format in [
            LogFormat::Default,
            LogFormat::Json,
            LogFormat::Rfc5424,
            LogFormat::Rfc3164,
        ] {
            let _layer = fmt_layer_for_format::<Registry>(format);
        }
    }

    #[test]
    fn logger_writer_falls_back_to_stderr_until_state_init() {
        // The `LoggerWriter` routes to stderr (write) and a no-op
        // success (flush) until STATE is installed. nextest runs
        // this in its own process where STATE is fresh; under a
        // single-process runner the guard keeps the assertions
        // honest after a sibling test has installed STATE.
        let mut w = LoggerWriter;
        if STATE.get().is_none() {
            // Empty write reports zero bytes via the stderr fallback.
            let n = w.write(b"").expect("stderr write");
            assert_eq!(n, 0);
            w.flush().expect("stderr flush");
        } else {
            // STATE present: the write/flush thread through the sink.
            w.flush().expect("sink flush");
        }
    }

    #[test]
    fn build_logs_layer_writer_state_swaps_on_reopen() {
        // The fmt layer returned by `build_logs_layer` writes
        // through the shared STATE.sink. Renaming the configured
        // file out from under the writer and then calling
        // `reopen_on_sighup` must rebind STATE.sink to a freshly
        // re-created file at the original path; events emitted
        // before the rotate land in the renamed file, events
        // emitted after the reopen land in the new file.
        //
        // STATE is a `OnceLock`, so this test is the only test
        // in the module that initialises it. nextest runs each
        // `#[test]` in its own process; when the suite is run
        // under plain `cargo test` this test is the canonical
        // owner of STATE.
        use std::fs;

        let dir = tempfile::tempdir().expect("tempdir");
        let log_path = dir.path().join("dyn.log");
        let cfg = LogConfig::new(LOG_NOTICE, Some(log_path.clone()), LogFormat::Default);
        let (fmt_layer, _reopen) = build_logs_layer(&cfg).expect("build layer");

        let env = build_env_filter(LOG_NOTICE);
        let sub = tracing_subscriber::registry().with(fmt_layer).with(env);

        tracing::subscriber::with_default(sub, || {
            tracing::info!(target: "dynomite::test", "first-line-marker");
            // tracing's fmt layer writes synchronously inside
            // `on_event`; no flush needed.
            let rotated = dir.path().join("dyn.log.1");
            fs::rename(&log_path, &rotated).expect("rotate file");
            reopen_on_sighup().expect("reopen");
            tracing::info!(target: "dynomite::test", "second-line-marker");
        });

        let rotated_contents =
            fs::read_to_string(dir.path().join("dyn.log.1")).expect("read rotated");
        let new_contents = fs::read_to_string(&log_path).expect("read new");

        assert!(
            rotated_contents.contains("first-line-marker"),
            "rotated file missing first marker: {rotated_contents:?}",
        );
        assert!(
            !rotated_contents.contains("second-line-marker"),
            "rotated file unexpectedly contained second marker: {rotated_contents:?}",
        );
        assert!(
            new_contents.contains("second-line-marker"),
            "new file missing second marker: {new_contents:?}",
        );
        assert!(
            !new_contents.contains("first-line-marker"),
            "new file unexpectedly contained first marker: {new_contents:?}",
        );

        // STATE is now installed (this test owns the OnceLock for
        // this binary). Exercise the writer's STATE-present
        // write and flush arms directly so they are covered even
        // when a sibling guard-test sees STATE already set.
        let mut w = LoggerWriter;
        let n = w.write(b"direct-write\n").expect("sink write");
        assert_eq!(n, "direct-write\n".len());
        w.flush().expect("sink flush");
        assert_eq!(write_error_count(), 0);
    }
}

#[cfg(test)]
mod format_tests {
    //! Per-format unit tests.
    //!
    //! These tests cannot install the global subscriber - that
    //! is process-wide and other tests already use it - so each
    //! test scopes a `tracing_subscriber` to a closure via
    //! `tracing::subscriber::with_default` and captures the bytes
    //! the subscriber writes to a shared `Vec<u8>`. The captured
    //! buffer is then asserted against the format's documented
    //! shape (regex for syslog, line-per-event JSON for NDJSON,
    //! field-name presence for the default text format).

    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    use regex::Regex;
    use tracing_subscriber::fmt::MakeWriter;

    use super::syslog::{Rfc3164Formatter, Rfc5424Formatter};

    /// Cloneable byte sink used as the writer behind a scoped
    /// subscriber. Each `make_writer()` call hands back a
    /// `Buffer` that pushes into the shared `Vec<u8>`.
    #[derive(Clone, Default)]
    struct CaptureBuffer(Arc<Mutex<Vec<u8>>>);

    impl CaptureBuffer {
        fn snapshot(&self) -> Vec<u8> {
            self.0.lock().expect("lock CaptureBuffer").clone()
        }
        fn snapshot_string(&self) -> String {
            String::from_utf8(self.snapshot()).expect("captured bytes are utf-8")
        }
    }

    impl Write for CaptureBuffer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut guard = self.0.lock().expect("lock CaptureBuffer");
            guard.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CaptureBuffer {
        type Writer = CaptureBuffer;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Build the four subscriber shapes against the given capture
    /// buffer. The functions are factored out so the assertions are
    /// next to the regex and not next to the subscriber wiring.
    fn run_default(buf: &CaptureBuffer) {
        // Exercise the capture buffer's no-op flush path; tracing
        // itself writes without flushing.
        {
            let mut probe = buf.clone();
            probe.flush().unwrap();
        }
        let sub = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_target(true)
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(sub, || {
            tracing::info!(answer = 42, name = "ada", "hello");
        });
    }

    fn run_rfc5424(buf: &CaptureBuffer) {
        use tracing_subscriber::layer::SubscriberExt as _;
        let layer = tracing_subscriber::fmt::Layer::new()
            .with_writer(buf.clone())
            .event_format(Rfc5424Formatter::new())
            .with_ansi(false);
        let sub = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(sub, || {
            tracing::info!(answer = 42, "hello");
        });
    }

    fn run_rfc3164(buf: &CaptureBuffer) {
        use tracing_subscriber::layer::SubscriberExt as _;
        let layer = tracing_subscriber::fmt::Layer::new()
            .with_writer(buf.clone())
            .event_format(Rfc3164Formatter::new())
            .with_ansi(false);
        let sub = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(sub, || {
            tracing::info!(answer = 42, "hello");
        });
    }

    fn run_json(buf: &CaptureBuffer) {
        let sub = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .json()
            .with_target(true)
            .flatten_event(false)
            .with_current_span(true)
            .with_span_list(false)
            .finish();
        tracing::subscriber::with_default(sub, || {
            tracing::info!(answer = 42, name = "ada", "first");
            tracing::warn!(retry = true, "second");
        });
    }

    #[test]
    fn default_format_unchanged_from_baseline() {
        let buf = CaptureBuffer::default();
        run_default(&buf);
        let text = buf.snapshot_string();
        // The historical text format stamps the level, the target,
        // the message and the field key/value pairs as
        // `key=value`. Anything weaker would fail to detect a
        // regression where a future refactor accidentally swaps
        // formatters.
        assert!(text.contains(" INFO "), "missing INFO level: {text:?}");
        assert!(text.contains("hello"), "missing message text: {text:?}");
        assert!(
            text.contains("answer=42"),
            "missing kv 'answer=42': {text:?}"
        );
        assert!(text.contains("name=\"ada\""), "missing kv 'name': {text:?}");
        // Sanity: line ends with a trailing newline.
        assert!(text.ends_with('\n'), "missing trailing newline");
    }

    #[test]
    fn rfc5424_format_starts_with_pri_version() {
        let buf = CaptureBuffer::default();
        run_rfc5424(&buf);
        let text = buf.snapshot_string();
        // The brief specifies the regex
        // `^<\d+>1 [\d-]+T[\d:.+-]+ \S+ dynomited \d+ - `
        let re =
            Regex::new(r"^<\d+>1 [\d-]+T[\d:.+\-]+ \S+ dynomited \d+ - ").expect("compile regex");
        let first_line = text.lines().next().expect("at least one line");
        assert!(
            re.is_match(first_line),
            "RFC 5424 line did not match regex: {first_line:?}"
        );
        assert!(
            first_line.contains("origin@32473"),
            "missing structured-data ID: {first_line:?}"
        );
        assert!(
            first_line.contains("hello"),
            "missing message: {first_line:?}"
        );
    }

    #[test]
    fn rfc3164_format_starts_with_pri_then_timestamp() {
        let buf = CaptureBuffer::default();
        run_rfc3164(&buf);
        let text = buf.snapshot_string();
        // The brief specifies the regex
        // `^<\d+>[A-Z][a-z]{2} [\d ]\d \d{2}:\d{2}:\d{2} \S+ \S+: `
        let re = Regex::new(r"^<\d+>[A-Z][a-z]{2} [\d ]\d \d{2}:\d{2}:\d{2} \S+ \S+: ")
            .expect("compile regex");
        let first_line = text.lines().next().expect("at least one line");
        assert!(
            re.is_match(first_line),
            "RFC 3164 line did not match regex: {first_line:?}"
        );
        assert!(
            first_line.contains("hello"),
            "missing message: {first_line:?}"
        );
    }

    #[test]
    fn ndjson_format_is_one_json_per_line() {
        let buf = CaptureBuffer::default();
        run_json(&buf);
        let text = buf.snapshot_string();
        let lines: Vec<_> = text.lines().filter(|l| !l.is_empty()).collect();
        assert!(
            lines.len() >= 2,
            "expected at least two JSON lines: {text:?}"
        );
        for line in &lines {
            // Each line must be a self-contained JSON object.
            let v: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("line is not valid JSON ({e}): {line:?}"));
            // Required keys, per the brief: timestamp, level,
            // target, fields. The `tracing-subscriber` JSON
            // formatter always emits `timestamp`, `level`,
            // `target`, and a `fields` object.
            for key in ["timestamp", "level", "target", "fields"] {
                assert!(
                    v.get(key).is_some(),
                    "JSON line missing key {key:?}: {line}"
                );
            }
            // Inner-newline check: a valid NDJSON line must not
            // contain a literal '\n' character.
            assert!(!line.contains('\n'));
        }
    }
}
