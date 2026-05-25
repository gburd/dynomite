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

// -----------------------------------------------------------------
// Cluster admin shared types -- v0.0.4 admin slice. Used by the
// `cluster-join`, `cluster-leave`, and `cluster-plan` subcommands.
// -----------------------------------------------------------------

/// Description of a peer to add. Mirrors
/// `dyn_riak::proto::pb::DynRpbPeerInfo` but in a shape that
/// serialises naturally for both human and JSON output.
#[derive(Clone, Debug, Serialize)]
pub struct PeerSpec {
    /// Hostname or IP.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Datacenter name.
    pub dc: String,
    /// Rack name.
    pub rack: String,
    /// Tokens, rendered as decimal strings.
    pub tokens: Vec<String>,
    /// True when the peer expects an encrypted dnode link.
    pub is_secure: bool,
}

/// Pending cluster-membership change. Same shape as the
/// substrate's `ClusterChange` but with the inner peer rendered
/// for output.
#[derive(Clone, Debug, Serialize)]
pub struct StagedChange {
    /// Direction: "add" or "remove".
    pub kind: String,
    /// Peer index targeted by a remove.
    pub peer_idx: Option<u32>,
    /// Peer description carried by an add.
    pub peer: Option<PeerSpec>,
}

/// Render a [`StagedChange`] as indented `key: value` lines.
///
/// `prefix` is prepended to every line; this lets the caller
/// indent the change inside a wider report (`"  "` for a
/// single-change report, `"    "` for a list).
///
/// # Errors
///
/// Returns the underlying [`io::Error`] from `out`.
pub fn render_change_human<W: Write>(
    change: &StagedChange,
    prefix: &str,
    out: &mut W,
) -> io::Result<()> {
    writeln!(out, "{prefix}kind: {}", change.kind)?;
    if let Some(idx) = change.peer_idx {
        writeln!(out, "{prefix}peer_idx: {idx}")?;
    }
    if let Some(peer) = &change.peer {
        writeln!(out, "{prefix}peer:")?;
        writeln!(out, "{prefix}  host: {}", peer.host)?;
        writeln!(out, "{prefix}  port: {}", peer.port)?;
        writeln!(out, "{prefix}  dc: {}", peer.dc)?;
        writeln!(out, "{prefix}  rack: {}", peer.rack)?;
        writeln!(out, "{prefix}  tokens: {}", peer.tokens.join(","))?;
        if peer.is_secure {
            writeln!(out, "{prefix}  is_secure: true")?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod cluster_admin_output_tests {
    use super::*;

    #[test]
    fn render_change_human_emits_add_block() {
        let c = StagedChange {
            kind: "add".into(),
            peer_idx: None,
            peer: Some(PeerSpec {
                host: "10.0.0.1".into(),
                port: 8101,
                dc: "dc1".into(),
                rack: "r1".into(),
                tokens: vec!["1".into(), "2".into()],
                is_secure: true,
            }),
        };
        let mut buf = Vec::new();
        render_change_human(&c, "  ", &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("  kind: add"));
        assert!(s.contains("  peer:"));
        assert!(s.contains("    host: 10.0.0.1"));
        assert!(s.contains("    tokens: 1,2"));
        assert!(s.contains("    is_secure: true"));
    }

    #[test]
    fn render_change_human_emits_remove_block() {
        let c = StagedChange {
            kind: "remove".into(),
            peer_idx: Some(7),
            peer: None,
        };
        let mut buf = Vec::new();
        render_change_human(&c, "  ", &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("  kind: remove"));
        assert!(s.contains("  peer_idx: 7"));
        assert!(!s.contains("  peer:"));
    }
}
