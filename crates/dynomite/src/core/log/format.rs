//! Output-format selector for the global tracing subscriber.
//!
//! `dynomited` ships with four selectable wire shapes for log records:
//! the existing human-readable text format ([`LogFormat::Default`]),
//! IETF syslog ([`LogFormat::Rfc5424`]), BSD syslog
//! ([`LogFormat::Rfc3164`]), and newline-delimited JSON
//! ([`LogFormat::Json`], also accepting the alias `ndjson`). The choice
//! is exposed both through the YAML configuration (`log_format:` on the
//! pool) and through a CLI override (`--log-format`).
//!
//! When neither knob is set, the default value reproduces the
//! pre-existing behavior byte-for-byte: a `tracing_subscriber::fmt()`
//! line with target enabled. This module is intentionally
//! cheap to embed - all formats are dispatched at install time so the
//! per-event hot path costs the same as the original implementation.
//!
//! # Examples
//!
//! ```
//! use dynomite::core::log::LogFormat;
//!
//! assert_eq!(LogFormat::parse("default").unwrap(), LogFormat::Default);
//! assert_eq!(LogFormat::parse("RFC5424").unwrap(), LogFormat::Rfc5424);
//! assert_eq!(LogFormat::parse("rfc3164").unwrap(), LogFormat::Rfc3164);
//! assert_eq!(LogFormat::parse("ndjson").unwrap(), LogFormat::Json);
//! assert!(LogFormat::parse("yaml").is_err());
//! ```

use std::fmt;

/// Selectable on-disk / on-stderr shape for emitted tracing events.
///
/// # Examples
///
/// ```
/// use dynomite::core::log::LogFormat;
/// assert_eq!(LogFormat::default(), LogFormat::Default);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum LogFormat {
    /// Human-readable text via `tracing_subscriber::fmt()`.
    /// This is the historical default and is what `dynomited` emits
    /// when neither the configuration nor the CLI request another
    /// shape.
    #[default]
    Default,
    /// Modern structured syslog per RFC 5424.
    Rfc5424,
    /// BSD-style syslog per RFC 3164.
    ///
    /// The user-facing brief originally said "RFC 3124" - that is "The
    /// Congestion Manager" and is unrelated to logging. We treat it as
    /// a typo for RFC 3164 and implement the BSD syslog shape here.
    Rfc3164,
    /// Newline-delimited JSON, one event per line. Selected by either
    /// `json` or `ndjson`.
    Json,
}

impl LogFormat {
    /// Parse a configuration / CLI value into a `LogFormat`.
    ///
    /// The match is case-insensitive. Empty input maps to
    /// [`LogFormat::Default`] so a YAML value of `""` (or no value at
    /// all) selects the default. The aliases `json` and `ndjson` both
    /// map to [`LogFormat::Json`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::core::log::LogFormat;
    /// assert_eq!(LogFormat::parse("").unwrap(), LogFormat::Default);
    /// assert_eq!(LogFormat::parse("json").unwrap(), LogFormat::Json);
    /// assert_eq!(LogFormat::parse("ndjson").unwrap(), LogFormat::Json);
    /// assert!(LogFormat::parse("xml").is_err());
    /// ```
    pub fn parse(s: &str) -> Result<Self, LogFormatParseError> {
        let lower = s.trim().to_ascii_lowercase();
        match lower.as_str() {
            "" | "default" => Ok(Self::Default),
            "rfc5424" => Ok(Self::Rfc5424),
            "rfc3164" => Ok(Self::Rfc3164),
            "json" | "ndjson" => Ok(Self::Json),
            _ => Err(LogFormatParseError {
                input: s.to_string(),
            }),
        }
    }

    /// Stable canonical name used by the CLI / config / docs.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::core::log::LogFormat;
    /// assert_eq!(LogFormat::Json.as_str(), "json");
    /// assert_eq!(LogFormat::Rfc5424.as_str(), "rfc5424");
    /// ```
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Rfc5424 => "rfc5424",
            Self::Rfc3164 => "rfc3164",
            Self::Json => "json",
        }
    }
}

impl fmt::Display for LogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Returned by [`LogFormat::parse`] for a value that does not match any
/// of the four supported names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFormatParseError {
    /// The unrecognised input as supplied by the operator.
    pub input: String,
}

impl fmt::Display for LogFormatParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown log_format '{}': expected one of default, rfc5424, rfc3164, json, ndjson",
            self.input
        )
    }
}

impl std::error::Error for LogFormatParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_known_values() {
        for (input, expected) in [
            ("default", LogFormat::Default),
            ("DEFAULT", LogFormat::Default),
            ("", LogFormat::Default),
            ("  rfc5424  ", LogFormat::Rfc5424),
            ("RFC3164", LogFormat::Rfc3164),
            ("json", LogFormat::Json),
            ("ndjson", LogFormat::Json),
        ] {
            assert_eq!(LogFormat::parse(input).unwrap(), expected, "input: {input}");
        }
    }

    #[test]
    fn parse_unknown_rejected() {
        let err = LogFormat::parse("yaml").unwrap_err();
        assert_eq!(err.input, "yaml");
        assert!(err.to_string().contains("yaml"));
    }

    #[test]
    fn default_trait_matches_default_variant() {
        assert_eq!(LogFormat::default(), LogFormat::Default);
    }

    #[test]
    fn display_and_as_str_match() {
        for variant in [
            LogFormat::Default,
            LogFormat::Rfc5424,
            LogFormat::Rfc3164,
            LogFormat::Json,
        ] {
            assert_eq!(variant.to_string(), variant.as_str());
            assert_eq!(LogFormat::parse(variant.as_str()).unwrap(), variant);
        }
    }
}
