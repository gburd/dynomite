//! `dyn-admin ring-status` -- best-effort topology view.
//!
//! The dynomite v0 substrate does not yet expose a multi-peer ring
//! map over PBC. Until it does, this subcommand combines what is
//! available:
//!
//! * `RpbGetServerInfoResp` from the configured node's PBC port:
//!   carries the node identity and version string.
//! * (optional) `/stats` JSON from the node's HTTP port: carries
//!   the datacenter and rack identifiers.
//!
//! The resulting view is a single-row "ring" listing the local node
//! plus its DC/rack and a `state: up` marker, mirroring the column
//! layout that `riak-admin ring-status` prints. Tokens and per-vnode
//! state are reported as `<unset>` because they are not yet exposed
//! by the substrate; the journal entry tracks the deferred
//! follow-up.

use std::io::Write;

use dyn_riak::proto::pb::{MessageCode, RpbGetServerInfoResp, RpbServerInfoReq};
use serde::Serialize;

use crate::client::{http_get, PbcClient};
use crate::error::AdminError;
use crate::output::{write_json, OutputFormat};

/// One ring entry as rendered by `ring-status`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RingEntry {
    /// Node name (`RpbGetServerInfoResp.node`, falling back to the
    /// stats `source`).
    pub node: String,
    /// Datacenter; `<unset>` when the stats endpoint did not supply
    /// one.
    pub dc: String,
    /// Rack; `<unset>` when the stats endpoint did not supply one.
    pub rack: String,
    /// Server-reported version string.
    pub version: String,
    /// Token range start (hex); `<unset>` until the substrate
    /// surfaces ring tokens through PBC.
    pub token: String,
    /// Per-node liveness flag; always `up` for a node that answered
    /// PBC ping.
    pub state: String,
}

/// Aggregate ring view.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RingView {
    /// `host:port` queried.
    pub queried: String,
    /// Ring entries, one per visible peer.
    pub entries: Vec<RingEntry>,
    /// Optional human-readable note explaining any partial state.
    pub note: Option<String>,
}

/// Run the ring-status subcommand. `stats_addr` is optional and
/// supplies DC/rack labels when reachable.
pub async fn run<W: Write>(
    node: &str,
    stats_addr: Option<&str>,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    let info = fetch_info(node).await?;
    let mut entry = RingEntry {
        node: info.node.as_ref().map_or_else(
            || node.to_string(),
            |b| String::from_utf8_lossy(b).into_owned(),
        ),
        dc: "<unset>".into(),
        rack: "<unset>".into(),
        version: info.server_version.as_ref().map_or_else(
            || "<unset>".into(),
            |b| String::from_utf8_lossy(b).into_owned(),
        ),
        token: "<unset>".into(),
        state: "up".into(),
    };
    let mut warning: Option<String> = None;
    if let Some(addr) = stats_addr {
        match http_get(addr, "/stats").await {
            Ok(body) => {
                let v: serde_json::Value = serde_json::from_str(&body)?;
                if let Some(s) = v.get("source").and_then(|s| s.as_str()) {
                    entry.node = s.to_string();
                }
                if let Some(s) = v.get("dc").and_then(|s| s.as_str()) {
                    entry.dc = s.to_string();
                }
                if let Some(s) = v.get("rack").and_then(|s| s.as_str()) {
                    entry.rack = s.to_string();
                }
            }
            Err(e) => warning = Some(format!("stats fetch failed: {e}")),
        }
    }
    let view = RingView {
        queried: node.to_string(),
        entries: vec![entry],
        note: warning,
    };
    render(&view, fmt, out)?;
    Ok(())
}

async fn fetch_info(node: &str) -> Result<RpbGetServerInfoResp, AdminError> {
    let mut client = PbcClient::connect(node).await?;
    let resp: RpbGetServerInfoResp = client
        .call(
            MessageCode::ServerInfoReq,
            MessageCode::GetServerInfoResp,
            &RpbServerInfoReq::default(),
        )
        .await?;
    Ok(resp)
}

fn render<W: Write>(view: &RingView, fmt: OutputFormat, out: &mut W) -> Result<(), AdminError> {
    match fmt {
        OutputFormat::Json => {
            write_json(out, view)?;
        }
        OutputFormat::Human => {
            writeln!(out, "Ring status (queried {})", view.queried)?;
            writeln!(out)?;
            writeln!(
                out,
                "{:<28}  {:<10}  {:<10}  {:<6}  {:<20}  token",
                "node", "dc", "rack", "state", "version",
            )?;
            writeln!(
                out,
                "{:<28}  {:<10}  {:<10}  {:<6}  {:<20}  {}",
                "-".repeat(28),
                "-".repeat(10),
                "-".repeat(10),
                "-".repeat(6),
                "-".repeat(20),
                "-".repeat(8),
            )?;
            for e in &view.entries {
                writeln!(
                    out,
                    "{:<28}  {:<10}  {:<10}  {:<6}  {:<20}  {}",
                    e.node, e.dc, e.rack, e.state, e.version, e.token
                )?;
            }
            if let Some(note) = &view.note {
                writeln!(out)?;
                writeln!(out, "note: {note}")?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> RingView {
        RingView {
            queried: "127.0.0.1:8087".into(),
            entries: vec![RingEntry {
                node: "node-a".into(),
                dc: "dc1".into(),
                rack: "r1".into(),
                version: "dyn-riak 0.0.1".into(),
                token: "<unset>".into(),
                state: "up".into(),
            }],
            note: None,
        }
    }

    #[test]
    fn human_render_emits_header_and_row() {
        let v = fixture();
        let mut buf = Vec::new();
        render(&v, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("Ring status"));
        assert!(s.contains("node-a"));
        assert!(s.contains("dc1"));
        assert!(s.contains("r1"));
        assert!(s.contains("up"));
    }

    #[test]
    fn human_render_includes_note_when_present() {
        let mut v = fixture();
        v.note = Some("stats fetch failed".into());
        let mut buf = Vec::new();
        render(&v, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("note: stats fetch failed"));
    }

    #[test]
    fn json_render_serialises_entries() {
        let v = fixture();
        let mut buf = Vec::new();
        render(&v, OutputFormat::Json, &mut buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(parsed["entries"][0]["node"], "node-a");
        assert_eq!(parsed["entries"][0]["state"], "up");
    }
}
