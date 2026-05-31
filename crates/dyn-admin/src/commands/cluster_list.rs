//! `dyn-admin cluster-list` -- discover cluster peers reachable
//! from the configured seed via the cluster admin RPC.
//!
//! The seed is contacted over PBC; the server responds with a
//! `DynRpbListPeersResp` enumerating every peer in the gossip
//! table. Each peer is rendered as one row in human mode or one
//! object in the JSON variant.

use std::io::Write;

use dyniak::proto::pb::{DynRpbListPeersReq, DynRpbListPeersResp, MessageCode};
use serde::Serialize;

use crate::client::PbcClient;
use crate::error::AdminError;
use crate::output::{write_json, OutputFormat};

/// One peer's view from the seed.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ClusterPeer {
    /// Peer index (zero is the local node by convention).
    pub idx: u32,
    /// Datacenter name.
    pub dc: String,
    /// Rack name.
    pub rack: String,
    /// Hostname or IP.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Tokens, rendered as decimal strings.
    pub tokens: Vec<String>,
    /// Lifecycle state (`UNKNOWN`, `JOINING`, `NORMAL`, ...).
    pub state: String,
    /// True for the local peer.
    pub is_local: bool,
}

/// Aggregate output: which seed was queried and what it returned.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ClusterListing {
    /// `host:port` of the seed.
    pub seed: String,
    /// Peers known to the seed.
    pub peers: Vec<ClusterPeer>,
}

/// Run the cluster-list subcommand.
///
/// # Errors
///
/// Surfaces every wire-level or server-side failure as an
/// [`AdminError`] for the binary's `main` to render.
pub async fn run<W: Write>(seed: &str, fmt: OutputFormat, out: &mut W) -> Result<(), AdminError> {
    let listing = fetch_listing(seed).await?;
    render(&listing, fmt, out)?;
    Ok(())
}

async fn fetch_listing(seed: &str) -> Result<ClusterListing, AdminError> {
    let mut client = PbcClient::connect(seed).await?;
    let resp: DynRpbListPeersResp = client
        .call(
            MessageCode::DynListPeersReq,
            MessageCode::DynListPeersResp,
            &DynRpbListPeersReq::default(),
        )
        .await?;
    let peers = resp
        .peers
        .into_iter()
        .map(|p| ClusterPeer {
            idx: p.idx,
            dc: String::from_utf8_lossy(&p.dc).into_owned(),
            rack: String::from_utf8_lossy(&p.rack).into_owned(),
            host: String::from_utf8_lossy(&p.host).into_owned(),
            port: u16::try_from(p.port).unwrap_or(u16::MAX),
            tokens: p
                .tokens
                .iter()
                .map(|t| String::from_utf8_lossy(t).into_owned())
                .collect(),
            state: String::from_utf8_lossy(&p.state).into_owned(),
            is_local: p.is_local,
        })
        .collect();
    Ok(ClusterListing {
        seed: seed.to_string(),
        peers,
    })
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
                "{:>4}  {:<24}  {:<10}  {:<10}  {:<7}  {:<8}  tokens",
                "idx", "endpoint", "dc", "rack", "state", "local",
            )?;
            writeln!(
                out,
                "{:>4}  {:<24}  {:<10}  {:<10}  {:<7}  {:<8}  {}",
                "----",
                "-".repeat(24),
                "-".repeat(10),
                "-".repeat(10),
                "-".repeat(7),
                "-".repeat(8),
                "-".repeat(20),
            )?;
            for p in &listing.peers {
                let endpoint = format!("{}:{}", p.host, p.port);
                let local = if p.is_local { "yes" } else { "no" };
                writeln!(
                    out,
                    "{:>4}  {:<24}  {:<10}  {:<10}  {:<7}  {:<8}  {}",
                    p.idx,
                    endpoint,
                    p.dc,
                    p.rack,
                    p.state,
                    local,
                    p.tokens.join(","),
                )?;
            }
            writeln!(out)?;
            writeln!(out, "{} peer(s)", listing.peers.len())?;
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
            peers: vec![
                ClusterPeer {
                    idx: 0,
                    dc: "dc1".into(),
                    rack: "r1".into(),
                    host: "127.0.0.1".into(),
                    port: 8101,
                    tokens: vec!["0".into()],
                    state: "JOINING".into(),
                    is_local: true,
                },
                ClusterPeer {
                    idx: 1,
                    dc: "dc1".into(),
                    rack: "r1".into(),
                    host: "127.0.0.1".into(),
                    port: 8102,
                    tokens: vec!["2147483648".into()],
                    state: "DOWN".into(),
                    is_local: false,
                },
            ],
        }
    }

    #[test]
    fn human_render_emits_one_row_per_peer() {
        let l = fixture();
        let mut buf = Vec::new();
        render(&l, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("Cluster (seed 127.0.0.1:8087)"));
        assert!(s.contains("127.0.0.1:8101"));
        assert!(s.contains("127.0.0.1:8102"));
        assert!(s.contains("JOINING"));
        assert!(s.contains("DOWN"));
        assert!(s.contains("2 peer(s)"));
    }

    #[test]
    fn json_render_round_trips() {
        let l = fixture();
        let mut buf = Vec::new();
        render(&l, OutputFormat::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["seed"], "127.0.0.1:8087");
        assert_eq!(v["peers"][0]["idx"], 0);
        assert_eq!(v["peers"][0]["is_local"], true);
        assert_eq!(v["peers"][1]["state"], "DOWN");
        assert_eq!(v["peers"][1]["tokens"][0], "2147483648");
    }

    #[test]
    fn empty_listing_renders_zero_peers() {
        let l = ClusterListing {
            seed: "h:1".into(),
            peers: Vec::new(),
        };
        let mut buf = Vec::new();
        render(&l, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("0 peer(s)"));
    }
}
