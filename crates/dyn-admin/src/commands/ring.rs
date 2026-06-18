//! `dyn-admin ring-status` -- multi-peer topology view.
//!
//! The view combines two sources:
//!
//! * `RpbGetServerInfoResp` from the configured node's PBC port:
//!   carries the queried node's identity and version string.
//! * The node's HTTP `/ring` endpoint: a JSON document listing
//!   every peer the engine knows about, with each peer's tokens,
//!   datacenter, rack, and lifecycle state.
//!
//! When the `/ring` endpoint is reachable, `ring-status` emits one
//! row per peer with real tokens and state, mirroring the column
//! layout that `riak-admin ring-status` prints. When the HTTP
//! endpoint is unavailable (no stats listener, or an older node
//! that does not serve `/ring`), it falls back to a single
//! best-effort row built from the PBC server-info response, with
//! the missing fields reported as `<unset>`.

use std::io::Write;

use dyniak::proto::pb::{MessageCode, RpbGetServerInfoResp, RpbServerInfoReq};
use serde::{Deserialize, Serialize};

use crate::client::{http_get, PbcClient};
use crate::error::AdminError;
use crate::output::{write_json, OutputFormat};

/// One ring entry as rendered by `ring-status`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RingEntry {
    /// Node name (`host:port` from the ring payload, or the
    /// `RpbGetServerInfoResp.node` for the fallback row).
    pub node: String,
    /// Datacenter; `<unset>` when no source supplied one.
    pub dc: String,
    /// Rack; `<unset>` when no source supplied one.
    pub rack: String,
    /// Server-reported version string (only populated for the
    /// queried node; `<unset>` for remote peers).
    pub version: String,
    /// Comma-separated token list, or `<unset>` when the peer has
    /// no tokens.
    pub token: String,
    /// Per-node liveness state (`up`, `NORMAL`, `DOWN`, ...).
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

/// Shape of one peer in the engine's `/ring` JSON document.
#[derive(Clone, Debug, Deserialize)]
struct RingPeerJson {
    node: String,
    dc: String,
    rack: String,
    #[serde(default)]
    tokens: Vec<u32>,
    state: String,
    #[serde(default)]
    is_local: bool,
}

/// Shape of the engine's `/ring` JSON document.
#[derive(Clone, Debug, Deserialize)]
struct RingJson {
    #[serde(default)]
    peers: Vec<RingPeerJson>,
}

/// Run the ring-status subcommand. `stats_addr` is optional and
/// supplies the structured per-peer ring view when reachable.
pub async fn run<W: Write>(
    node: &str,
    stats_addr: Option<&str>,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    let info = fetch_info(node).await?;
    let local_version = info.server_version.as_ref().map_or_else(
        || "<unset>".to_string(),
        |b| String::from_utf8_lossy(b).into_owned(),
    );
    let local_node = info.node.as_ref().map_or_else(
        || node.to_string(),
        |b| String::from_utf8_lossy(b).into_owned(),
    );

    let mut warning: Option<String> = None;
    let entries = match stats_addr {
        Some(addr) => match fetch_ring(addr).await {
            Ok(peers) if !peers.is_empty() => entries_from_ring(&peers, &local_version),
            Ok(_) => {
                warning = Some("ring endpoint returned no peers".into());
                vec![fallback_entry(&local_node, &local_version)]
            }
            Err(e) => {
                warning = Some(format!("ring fetch failed: {e}"));
                vec![fallback_entry(&local_node, &local_version)]
            }
        },
        None => vec![fallback_entry(&local_node, &local_version)],
    };

    let view = RingView {
        queried: node.to_string(),
        entries,
        note: warning,
    };
    render(&view, fmt, out)?;
    Ok(())
}

/// Build one [`RingEntry`] per peer from the structured ring
/// payload. The queried (local) peer carries the PBC-reported
/// version; remote peers report `<unset>` because the version is
/// only known for the node we issued the PBC call to.
fn entries_from_ring(peers: &[RingPeerJson], local_version: &str) -> Vec<RingEntry> {
    peers
        .iter()
        .map(|p| RingEntry {
            node: p.node.clone(),
            dc: blank_to_unset(&p.dc),
            rack: blank_to_unset(&p.rack),
            version: if p.is_local {
                local_version.to_string()
            } else {
                "<unset>".to_string()
            },
            token: render_tokens(&p.tokens),
            state: p.state.clone(),
        })
        .collect()
}

/// Single best-effort row used when the structured ring view is
/// unavailable.
fn fallback_entry(node: &str, version: &str) -> RingEntry {
    RingEntry {
        node: node.to_string(),
        dc: "<unset>".into(),
        rack: "<unset>".into(),
        version: version.to_string(),
        token: "<unset>".into(),
        state: "up".into(),
    }
}

fn render_tokens(tokens: &[u32]) -> String {
    if tokens.is_empty() {
        return "<unset>".into();
    }
    tokens
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn blank_to_unset(s: &str) -> String {
    if s.is_empty() {
        "<unset>".into()
    } else {
        s.to_string()
    }
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

async fn fetch_ring(addr: &str) -> Result<Vec<RingPeerJson>, AdminError> {
    let body = http_get(addr, "/ring").await?;
    let parsed: RingJson = serde_json::from_str(&body)?;
    Ok(parsed.peers)
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
                "{:<28}  {:<10}  {:<10}  {:<8}  {:<20}  token",
                "node", "dc", "rack", "state", "version",
            )?;
            writeln!(
                out,
                "{:<28}  {:<10}  {:<10}  {:<8}  {:<20}  {}",
                "-".repeat(28),
                "-".repeat(10),
                "-".repeat(10),
                "-".repeat(8),
                "-".repeat(20),
                "-".repeat(8),
            )?;
            for e in &view.entries {
                writeln!(
                    out,
                    "{:<28}  {:<10}  {:<10}  {:<8}  {:<20}  {}",
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

    fn three_peer_ring() -> Vec<RingPeerJson> {
        vec![
            RingPeerJson {
                node: "10.0.0.1:8101".into(),
                dc: "dc1".into(),
                rack: "r1".into(),
                tokens: vec![0],
                state: "NORMAL".into(),
                is_local: true,
            },
            RingPeerJson {
                node: "10.0.0.2:8101".into(),
                dc: "dc1".into(),
                rack: "r2".into(),
                tokens: vec![1_431_655_765],
                state: "NORMAL".into(),
                is_local: false,
            },
            RingPeerJson {
                node: "10.0.0.3:8101".into(),
                dc: "dc2".into(),
                rack: "r1".into(),
                tokens: vec![2_863_311_530, 4_000_000_000],
                state: "DOWN".into(),
                is_local: false,
            },
        ]
    }

    #[test]
    fn ring_payload_yields_one_row_per_peer() {
        let entries = entries_from_ring(&three_peer_ring(), "dyniak 0.0.1");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].node, "10.0.0.1:8101");
        assert_eq!(entries[0].token, "0");
        assert_eq!(entries[0].version, "dyniak 0.0.1");
        // Remote peers do not carry a version.
        assert_eq!(entries[1].version, "<unset>");
        assert_eq!(entries[1].token, "1431655765");
        assert_eq!(entries[2].state, "DOWN");
        assert_eq!(entries[2].dc, "dc2");
        assert_eq!(entries[2].token, "2863311530,4000000000");
    }

    #[test]
    fn ring_json_deserialises_and_round_trips_through_view() {
        let body = r#"{"peers":[
            {"node":"10.0.0.1:8101","dc":"dc1","rack":"r1","tokens":[0],"state":"NORMAL","is_local":true},
            {"node":"10.0.0.2:8101","dc":"dc1","rack":"r2","tokens":[100],"state":"DOWN","is_local":false}
        ]}"#;
        let parsed: RingJson = serde_json::from_str(body).expect("parse");
        let entries = entries_from_ring(&parsed.peers, "v1");
        let view = RingView {
            queried: "127.0.0.1:8087".into(),
            entries,
            note: None,
        };
        let mut buf = Vec::new();
        render(&view, OutputFormat::Json, &mut buf).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(json["entries"][0]["node"], "10.0.0.1:8101");
        assert_eq!(json["entries"][0]["state"], "NORMAL");
        assert_eq!(json["entries"][1]["state"], "DOWN");
        assert_eq!(json["entries"][1]["token"], "100");
    }

    #[test]
    fn human_render_emits_three_rows_with_tokens_and_state() {
        let entries = entries_from_ring(&three_peer_ring(), "v1");
        let view = RingView {
            queried: "127.0.0.1:8087".into(),
            entries,
            note: None,
        };
        let mut buf = Vec::new();
        render(&view, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("Ring status"));
        assert!(s.contains("10.0.0.1:8101"));
        assert!(s.contains("10.0.0.2:8101"));
        assert!(s.contains("10.0.0.3:8101"));
        assert!(s.contains("DOWN"));
        assert!(s.contains("1431655765"));
        assert!(s.contains("2863311530,4000000000"));
    }

    #[test]
    fn fallback_entry_is_single_best_effort_row() {
        let e = fallback_entry("node-a", "dyniak 0.0.1");
        assert_eq!(e.node, "node-a");
        assert_eq!(e.dc, "<unset>");
        assert_eq!(e.token, "<unset>");
        assert_eq!(e.state, "up");
        let view = RingView {
            queried: "127.0.0.1:8087".into(),
            entries: vec![e],
            note: Some("ring fetch failed".into()),
        };
        let mut buf = Vec::new();
        render(&view, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("node-a"));
        assert!(s.contains("note: ring fetch failed"));
    }

    #[test]
    fn empty_tokens_render_as_unset() {
        assert_eq!(render_tokens(&[]), "<unset>");
        assert_eq!(render_tokens(&[7]), "7");
        assert_eq!(render_tokens(&[1, 2, 3]), "1,2,3");
    }
}
