//! `dyn-admin cluster-list` -- discover cluster peers reachable from
//! the configured seed.
//!
//! The dynomite v0 substrate does not yet expose a peer-list message
//! over PBC. Until it does, this subcommand reports what the seed
//! knows about itself: the `RpbGetServerInfoResp` triple plus a
//! single derived `state: up` row. The journal entry that ships with
//! this binary records the deferred follow-up; once the substrate
//! gossips a peer table over PBC, the loop here grows naturally to
//! enumerate every peer.

use std::io::Write;

use dyn_riak::proto::pb::{MessageCode, RpbGetServerInfoResp, RpbServerInfoReq};
use serde::Serialize;

use crate::client::PbcClient;
use crate::error::AdminError;
use crate::output::{write_json, OutputFormat};

/// One peer's view from the seed.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ClusterPeer {
    /// PBC address `host:port`.
    pub addr: String,
    /// Server-reported node name; falls back to `addr` when the
    /// peer does not advertise one.
    pub node: String,
    /// Server-reported version.
    pub version: String,
    /// State string. `up` for reachable peers, `down` when the
    /// connection failed and the seed surfaced an error.
    pub state: String,
}

/// Aggregate output: which seed was queried and what it returned.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ClusterListing {
    /// `host:port` of the seed.
    pub seed: String,
    /// Peers known to the seed.
    pub peers: Vec<ClusterPeer>,
    /// Optional note explaining partial state.
    pub note: Option<String>,
}

/// Run the cluster-list subcommand.
pub async fn run<W: Write>(seed: &str, fmt: OutputFormat, out: &mut W) -> Result<(), AdminError> {
    let info = fetch_info(seed).await?;
    let peer = ClusterPeer {
        addr: seed.to_string(),
        node: info.node.as_ref().map_or_else(
            || seed.to_string(),
            |b| String::from_utf8_lossy(b).into_owned(),
        ),
        version: info.server_version.as_ref().map_or_else(
            || "<unset>".into(),
            |b| String::from_utf8_lossy(b).into_owned(),
        ),
        state: "up".into(),
    };
    let listing = ClusterListing {
        seed: seed.to_string(),
        peers: vec![peer],
        note: Some(
            "multi-peer discovery deferred: substrate does not yet expose a peer-list \
             message over PBC; reporting only the contacted seed"
                .into(),
        ),
    };
    render(&listing, fmt, out)?;
    Ok(())
}

async fn fetch_info(seed: &str) -> Result<RpbGetServerInfoResp, AdminError> {
    let mut client = PbcClient::connect(seed).await?;
    let resp: RpbGetServerInfoResp = client
        .call(
            MessageCode::ServerInfoReq,
            MessageCode::GetServerInfoResp,
            &RpbServerInfoReq::default(),
        )
        .await?;
    Ok(resp)
}

fn render<W: Write>(
    listing: &ClusterListing,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    match fmt {
        OutputFormat::Json => {
            write_json(out, listing)?;
        }
        OutputFormat::Human => {
            writeln!(out, "Cluster (seed {})", listing.seed)?;
            writeln!(out)?;
            writeln!(
                out,
                "{:<24}  {:<28}  {:<6}  version",
                "addr", "node", "state",
            )?;
            writeln!(
                out,
                "{:<24}  {:<28}  {:<6}  {}",
                "-".repeat(24),
                "-".repeat(28),
                "-".repeat(6),
                "-".repeat(20),
            )?;
            for p in &listing.peers {
                writeln!(
                    out,
                    "{:<24}  {:<28}  {:<6}  {}",
                    p.addr, p.node, p.state, p.version,
                )?;
            }
            if let Some(note) = &listing.note {
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

    fn fixture() -> ClusterListing {
        ClusterListing {
            seed: "127.0.0.1:8087".into(),
            peers: vec![ClusterPeer {
                addr: "127.0.0.1:8087".into(),
                node: "node-a".into(),
                version: "dyn-riak 0.0.1".into(),
                state: "up".into(),
            }],
            note: Some("multi-peer discovery deferred".into()),
        }
    }

    #[test]
    fn human_render_emits_table() {
        let l = fixture();
        let mut buf = Vec::new();
        render(&l, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("Cluster (seed 127.0.0.1:8087)"));
        assert!(s.contains("node-a"));
        assert!(s.contains("up"));
        assert!(s.contains("note: multi-peer discovery deferred"));
    }

    #[test]
    fn json_render_round_trips() {
        let l = fixture();
        let mut buf = Vec::new();
        render(&l, OutputFormat::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["peers"][0]["state"], "up");
        assert_eq!(v["peers"][0]["node"], "node-a");
        assert!(v["note"].as_str().unwrap().contains("deferred"));
    }
}
