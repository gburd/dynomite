//! Error types used across the benchmarking engine.

use std::io;

use thiserror::Error;

/// Top-level benchmark error. Used by the CLI for `main` exit codes.
#[derive(Debug, Error)]
pub enum BenchError {
    /// Failed to read or parse the configuration file.
    #[error("config error: {0}")]
    Config(String),

    /// Filesystem / IO failure during setup or while writing reports.
    #[error("io: {0}")]
    Io(#[from] io::Error),

    /// CSV serialization failure.
    #[error("csv: {0}")]
    Csv(String),

    /// PNG / SVG plot rendering failure.
    #[error("plot: {0}")]
    Plot(String),

    /// Driver setup or inter-driver protocol failure during a run.
    #[error("driver: {0}")]
    Driver(String),

    /// Engine-level invariant violation that aborts the run.
    #[error("engine: {0}")]
    Engine(String),
}

/// Categories used to aggregate driver-level errors. These are written
/// into `errors.csv` so post-run tooling can correlate failure spikes
/// with specific causes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DriverErrorClass {
    /// Connection closed mid-reply or peer reset.
    Closed,
    /// Read or connect timed out.
    Timeout,
    /// Server reported "no replicas" / "no quorum" /
    /// `RpbErrorResp{NoTargets}`.
    NoTargets,
    /// Anything else (protocol, unknown reply codes, ...).
    Unknown,
}

impl DriverErrorClass {
    /// Return the canonical short name, used in CSV output.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "Closed",
            Self::Timeout => "Timeout",
            Self::NoTargets => "NoTargets",
            Self::Unknown => "Unknown",
        }
    }
}

/// Classify a driver error string into a [`DriverErrorClass`]. The
/// classifier looks at the textual representation produced by the
/// driver and matches a small set of substrings; anything that does
/// not match is reported as [`DriverErrorClass::Unknown`].
#[must_use]
pub fn classify_driver_error(msg: &str) -> DriverErrorClass {
    let lower = msg.to_ascii_lowercase();
    if lower.contains("notargets") || lower.contains("no quorum") || lower.contains("no replicas") {
        DriverErrorClass::NoTargets
    } else if lower.contains("timeout") || lower.contains("timed out") {
        DriverErrorClass::Timeout
    } else if lower.contains("closed")
        || lower.contains("reset")
        || lower.contains("eof")
        || lower.contains("broken pipe")
    {
        DriverErrorClass::Closed
    } else {
        DriverErrorClass::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_closed_on_eof() {
        assert_eq!(
            classify_driver_error("peer closed mid-reply"),
            DriverErrorClass::Closed
        );
        assert_eq!(
            classify_driver_error("connection reset"),
            DriverErrorClass::Closed
        );
        assert_eq!(
            classify_driver_error("unexpected EOF"),
            DriverErrorClass::Closed
        );
    }

    #[test]
    fn classifies_timeout() {
        assert_eq!(
            classify_driver_error("read timed out"),
            DriverErrorClass::Timeout
        );
        assert_eq!(
            classify_driver_error("Timeout: 5s"),
            DriverErrorClass::Timeout
        );
    }

    #[test]
    fn classifies_no_targets() {
        assert_eq!(
            classify_driver_error("DYNOMITE NoTargets"),
            DriverErrorClass::NoTargets
        );
        assert_eq!(
            classify_driver_error("no quorum"),
            DriverErrorClass::NoTargets
        );
    }

    #[test]
    fn classifies_unknown() {
        assert_eq!(
            classify_driver_error("WRONGTYPE Operation"),
            DriverErrorClass::Unknown
        );
    }
}
