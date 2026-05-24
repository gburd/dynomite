//! Output helpers shared by the subcommands.
//!
//! Two modes:
//!
//! * Human: a short list of `field: value` pairs followed by a blank
//!   line. Useful for shell sessions and `riak-admin`-style logs.
//! * JSON: a single `serde_json::Value` per record. Useful for
//!   automation; downstream tools `jq` directly off the binary's
//!   stdout.
//!
//! Both shapes are versioned in `docs/book/src/operations/admin.md`.

use serde::Serialize;
use std::io::{self, Write};

/// Output format selected by the caller's `--json` flag.
///
/// # Examples
///
/// ```
/// use dyn_admin::output::OutputFormat;
/// assert_eq!(OutputFormat::Human, OutputFormat::default());
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OutputFormat {
    /// Human-readable `key: value` listing. The default.
    #[default]
    Human,
    /// Pretty-printed JSON.
    Json,
}

impl OutputFormat {
    /// Pick a format from the CLI flag.
    #[must_use]
    pub fn from_flag(json: bool) -> Self {
        if json {
            Self::Json
        } else {
            Self::Human
        }
    }
}

/// Render `value` as JSON to `out`.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] from `out` or a JSON
/// serialisation failure.
pub fn write_json<W: Write, T: Serialize>(out: &mut W, value: &T) -> io::Result<()> {
    let s = serde_json::to_string_pretty(value)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    writeln!(out, "{s}")
}

/// Render a list of (label, value) pairs as `label: value` lines
/// followed by a blank trailing line.
///
/// # Examples
///
/// ```
/// use dyn_admin::output::write_human_pairs;
/// let mut buf = Vec::new();
/// write_human_pairs(&mut buf, &[("node", "n1"), ("version", "0.0.1")])
///     .unwrap();
/// let s = String::from_utf8(buf).unwrap();
/// assert!(s.contains("node: n1"));
/// assert!(s.contains("version: 0.0.1"));
/// ```
///
/// # Errors
///
/// Returns the underlying [`io::Error`] from `out`.
pub fn write_human_pairs<W: Write>(out: &mut W, pairs: &[(&str, &str)]) -> io::Result<()> {
    for (k, v) in pairs {
        writeln!(out, "{k}: {v}")?;
    }
    writeln!(out)
}

/// Render a section header followed by a blank line.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] from `out`.
pub fn write_section<W: Write>(out: &mut W, title: &str) -> io::Result<()> {
    writeln!(out, "== {title} ==")
}
