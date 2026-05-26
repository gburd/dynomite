//! `dyn-admin aae-status` -- query a node's AAE worker for its
//! live snapshot.
//!
//! Sends a `DynRpbAaeStatusReq` (PBC code 220) and renders the
//! `DynRpbAaeStatusResp` (code 221) reply. The default human
//! formatter renders a compact per-peer table plus the
//! snapshot-file metadata; `--json` returns the same fields as
//! a stable JSON object suitable for `jq` filters and dashboard
//! pipelines.

use std::io::Write;

use dyn_riak::proto::pb::{DynRpbAaeStatusReq, DynRpbAaeStatusResp, MessageCode};
use serde::Serialize;

use crate::client::PbcClient;
use crate::error::AdminError;
use crate::output::{write_json, OutputFormat};

/// One peer's row in the AAE status snapshot.
#[derive(Clone, Debug, Default, Serialize)]
pub struct AaePeerRow {
    /// Peer index.
    pub peer_idx: u32,
    /// Datacenter of the peer.
    pub dc: String,
    /// Rack of the peer.
    pub rack: String,
    /// Wall-clock seconds (UNIX epoch) when this peer's most
    /// recent exchange completed. Zero means "never".
    pub last_exchange_unix: u64,
    /// Cumulative count of divergent keys observed since the
    /// last full sweep finished.
    pub divergent_keys_since_last_full_sweep: u64,
    /// Cumulative count of repair tasks dispatched against
    /// this peer.
    pub repair_dispatched_total: u64,
}

/// Top-level snapshot returned by `dyn-admin aae-status`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct AaeStatusReport {
    /// Address of the node the snapshot was queried from.
    pub node: String,
    /// One row per peer.
    pub peers: Vec<AaePeerRow>,
    /// Path of the local snapshot file. Empty when the
    /// embedding has not configured a snapshot path.
    pub snapshot_path: String,
    /// Wall-clock seconds (UNIX epoch) of the most recent
    /// successful snapshot save. Zero means "never".
    pub snapshot_last_save_unix: u64,
    /// Wall-clock seconds (UNIX epoch) of the most recent
    /// successful snapshot load. Zero means "never".
    pub snapshot_last_load_unix: u64,
    /// Cumulative count of snapshot writes.
    pub snapshot_save_total: u64,
    /// Cumulative count of snapshot loads.
    pub snapshot_load_total: u64,
    /// Cumulative count of corrupted-snapshot rejections.
    pub snapshot_corruption_total: u64,
    /// Number of top-level time buckets in the local tree.
    pub tree_n_time_buckets: u32,
    /// Number of bottom-level segments per time bucket.
    pub tree_n_segments: u32,
    /// Width of one time bucket, in seconds.
    pub tree_time_window_seconds: u64,
    /// Rough estimate of the local tree's resident memory.
    pub tree_memory_estimate_bytes: u64,
}

/// Run the `aae-status` subcommand.
///
/// # Errors
///
/// Surfaces every wire-level or server-side failure as an
/// [`AdminError`].
pub async fn run<W: Write>(node: &str, fmt: OutputFormat, out: &mut W) -> Result<(), AdminError> {
    let report = fetch(node).await?;
    render(&report, fmt, out)?;
    Ok(())
}

async fn fetch(node: &str) -> Result<AaeStatusReport, AdminError> {
    let mut client = PbcClient::connect(node).await?;
    let resp: DynRpbAaeStatusResp = client
        .call(
            MessageCode::DynAaeStatusReq,
            MessageCode::DynAaeStatusResp,
            &DynRpbAaeStatusReq::default(),
        )
        .await?;
    Ok(report_from_pb(node, resp))
}

fn report_from_pb(node: &str, resp: DynRpbAaeStatusResp) -> AaeStatusReport {
    AaeStatusReport {
        node: node.to_string(),
        peers: resp
            .peers
            .into_iter()
            .map(|p| AaePeerRow {
                peer_idx: p.peer_idx,
                dc: String::from_utf8_lossy(&p.dc).into_owned(),
                rack: String::from_utf8_lossy(&p.rack).into_owned(),
                last_exchange_unix: p.last_exchange_unix,
                divergent_keys_since_last_full_sweep: p.divergent_keys_since_last_full_sweep,
                repair_dispatched_total: p.repair_dispatched_total,
            })
            .collect(),
        snapshot_path: String::from_utf8_lossy(&resp.snapshot_path).into_owned(),
        snapshot_last_save_unix: resp.snapshot_last_save_unix,
        snapshot_last_load_unix: resp.snapshot_last_load_unix,
        snapshot_save_total: resp.snapshot_save_total,
        snapshot_load_total: resp.snapshot_load_total,
        snapshot_corruption_total: resp.snapshot_corruption_total,
        tree_n_time_buckets: resp.tree_n_time_buckets,
        tree_n_segments: resp.tree_n_segments,
        tree_time_window_seconds: resp.tree_time_window_seconds,
        tree_memory_estimate_bytes: resp.tree_memory_estimate_bytes,
    }
}

fn render<W: Write>(
    report: &AaeStatusReport,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    match fmt {
        OutputFormat::Json => {
            write_json(out, report)?;
        }
        OutputFormat::Human => {
            writeln!(out, "AAE status (node {})", report.node)?;
            writeln!(out)?;
            writeln!(
                out,
                "{:>5}  {:<10}  {:<10}  {:>16}  {:>11}  {:>10}",
                "peer", "dc", "rack", "last_exchange", "divergent", "repaired",
            )?;
            writeln!(
                out,
                "{:>5}  {:<10}  {:<10}  {:>16}  {:>11}  {:>10}",
                "-".repeat(5),
                "-".repeat(10),
                "-".repeat(10),
                "-".repeat(16),
                "-".repeat(11),
                "-".repeat(10),
            )?;
            for p in &report.peers {
                writeln!(
                    out,
                    "{:>5}  {:<10}  {:<10}  {:>16}  {:>11}  {:>10}",
                    p.peer_idx,
                    p.dc,
                    p.rack,
                    p.last_exchange_unix,
                    p.divergent_keys_since_last_full_sweep,
                    p.repair_dispatched_total,
                )?;
            }
            writeln!(out)?;
            writeln!(out, "snapshot_path: {}", report.snapshot_path)?;
            writeln!(
                out,
                "snapshot_last_save_unix: {}",
                report.snapshot_last_save_unix
            )?;
            writeln!(
                out,
                "snapshot_last_load_unix: {}",
                report.snapshot_last_load_unix
            )?;
            writeln!(out, "snapshot_save_total: {}", report.snapshot_save_total)?;
            writeln!(out, "snapshot_load_total: {}", report.snapshot_load_total)?;
            writeln!(
                out,
                "snapshot_corruption_total: {}",
                report.snapshot_corruption_total
            )?;
            writeln!(
                out,
                "tree: {} time-buckets * {} segments, window {}s, ~{} bytes",
                report.tree_n_time_buckets,
                report.tree_n_segments,
                report.tree_time_window_seconds,
                report.tree_memory_estimate_bytes,
            )?;
            writeln!(out, "{} peer(s)", report.peers.len())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> AaeStatusReport {
        AaeStatusReport {
            node: "127.0.0.1:8087".into(),
            peers: vec![
                AaePeerRow {
                    peer_idx: 0,
                    dc: "dc1".into(),
                    rack: "rA".into(),
                    last_exchange_unix: 1_700_000_000,
                    divergent_keys_since_last_full_sweep: 12,
                    repair_dispatched_total: 9,
                },
                AaePeerRow {
                    peer_idx: 1,
                    dc: "dc1".into(),
                    rack: "rB".into(),
                    last_exchange_unix: 0,
                    divergent_keys_since_last_full_sweep: 0,
                    repair_dispatched_total: 0,
                },
            ],
            snapshot_path: "/var/lib/dynomite/aae/tree.snapshot".into(),
            snapshot_last_save_unix: 1_700_000_300,
            snapshot_last_load_unix: 1_700_000_100,
            snapshot_save_total: 5,
            snapshot_load_total: 1,
            snapshot_corruption_total: 0,
            tree_n_time_buckets: 24,
            tree_n_segments: 1024,
            tree_time_window_seconds: 3600,
            tree_memory_estimate_bytes: 4096,
        }
    }

    #[test]
    fn human_render_emits_per_peer_rows_and_metadata() {
        let r = fixture();
        let mut buf = Vec::new();
        render(&r, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("AAE status (node 127.0.0.1:8087)"));
        assert!(s.contains("dc1"));
        assert!(s.contains("rA"));
        assert!(s.contains("snapshot_path: /var/lib/dynomite/aae/tree.snapshot"));
        assert!(s.contains("snapshot_save_total: 5"));
        assert!(s.contains("24 time-buckets"));
        assert!(s.contains("2 peer(s)"));
    }

    #[test]
    fn json_render_round_trips_every_field() {
        let r = fixture();
        let mut buf = Vec::new();
        render(&r, OutputFormat::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["node"], "127.0.0.1:8087");
        assert_eq!(v["peers"][0]["peer_idx"], 0);
        assert_eq!(v["peers"][0]["divergent_keys_since_last_full_sweep"], 12);
        assert_eq!(v["snapshot_save_total"], 5);
        assert_eq!(v["tree_n_time_buckets"], 24);
        assert_eq!(v["snapshot_path"], "/var/lib/dynomite/aae/tree.snapshot");
    }

    #[test]
    fn empty_report_renders_zero_peers() {
        let r = AaeStatusReport {
            node: "127.0.0.1:8087".into(),
            ..AaeStatusReport::default()
        };
        let mut buf = Vec::new();
        render(&r, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("0 peer(s)"));
    }

    #[test]
    fn report_from_pb_maps_every_field() {
        let pb = DynRpbAaeStatusResp {
            peers: vec![dyn_riak::proto::pb::DynRpbAaePeerStatus {
                peer_idx: 3,
                dc: b"dc2".to_vec(),
                rack: b"rZ".to_vec(),
                last_exchange_unix: 7,
                divergent_keys_since_last_full_sweep: 1,
                repair_dispatched_total: 1,
            }],
            snapshot_path: b"/x".to_vec(),
            snapshot_last_save_unix: 11,
            snapshot_last_load_unix: 12,
            snapshot_save_total: 2,
            snapshot_load_total: 1,
            snapshot_corruption_total: 1,
            tree_n_time_buckets: 8,
            tree_n_segments: 16,
            tree_time_window_seconds: 60,
            tree_memory_estimate_bytes: 1024,
        };
        let r = report_from_pb("127.0.0.1:8087", pb);
        assert_eq!(r.peers.len(), 1);
        assert_eq!(r.peers[0].dc, "dc2");
        assert_eq!(r.snapshot_path, "/x");
        assert_eq!(r.snapshot_corruption_total, 1);
        assert_eq!(r.tree_n_segments, 16);
    }
}
