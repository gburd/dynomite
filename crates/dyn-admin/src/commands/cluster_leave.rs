//! `dyn-admin cluster-leave` -- stage a peer-leave over the
//! cluster admin RPC.

use std::io::Write;

use dyniak::proto::pb::{DynRpbClusterLeaveReq, DynRpbClusterLeaveResp, MessageCode};
use serde::Serialize;

use crate::client::PbcClient;
use crate::commands::cluster_join::change_from_pb;
use crate::error::AdminError;
use crate::output::{render_change_human, write_json, OutputFormat, StagedChange};

/// Result envelope emitted by `dyn-admin cluster-leave`.
#[derive(Clone, Debug, Serialize)]
pub struct LeaveReport {
    /// Node `host:port` the request was sent to.
    pub node: String,
    /// Peer index the operator asked to remove.
    pub peer_idx: u32,
    /// Plan returned by the server.
    pub plan: StagedChange,
}

/// Run the cluster-leave subcommand.
///
/// # Errors
///
/// Surfaces every wire-level or server-side failure as an
/// [`AdminError`] for the binary's `main` to render.
pub async fn run<W: Write>(
    node: &str,
    peer_idx: u32,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    let mut client = PbcClient::connect(node).await?;
    let req = DynRpbClusterLeaveReq { peer_idx };
    let resp: DynRpbClusterLeaveResp = client
        .call(
            MessageCode::DynClusterLeaveReq,
            MessageCode::DynClusterLeaveResp,
            &req,
        )
        .await?;
    let change_pb = resp
        .change
        .ok_or_else(|| AdminError::Protocol("cluster-leave response missing change".into()))?;
    let change = change_from_pb(&change_pb)?;
    let report = LeaveReport {
        node: node.to_string(),
        peer_idx,
        plan: change,
    };
    render(&report, fmt, out)
}

fn render<W: Write>(
    report: &LeaveReport,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    match fmt {
        OutputFormat::Json => write_json(out, report)?,
        OutputFormat::Human => {
            writeln!(out, "Staged cluster-leave via {}", report.node)?;
            writeln!(out, "  peer_idx: {}", report.peer_idx)?;
            render_change_human(&report.plan, "  ", out)?;
            writeln!(out)?;
            writeln!(out, "Run `dyn-admin cluster-commit` to apply.")?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> LeaveReport {
        LeaveReport {
            node: "127.0.0.1:8087".into(),
            peer_idx: 5,
            plan: StagedChange {
                kind: "remove".into(),
                peer_idx: Some(5),
                peer: None,
            },
        }
    }

    #[test]
    fn human_render_includes_idx_and_kind() {
        let r = report();
        let mut buf = Vec::new();
        render(&r, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Staged cluster-leave via 127.0.0.1:8087"));
        assert!(s.contains("peer_idx: 5"));
        assert!(s.contains("kind: remove"));
        assert!(s.contains("cluster-commit"));
    }

    #[test]
    fn json_render_round_trips() {
        let r = report();
        let mut buf = Vec::new();
        render(&r, OutputFormat::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["node"], "127.0.0.1:8087");
        assert_eq!(v["peer_idx"], 5);
        assert_eq!(v["plan"]["kind"], "remove");
        assert_eq!(v["plan"]["peer_idx"], 5);
    }
}
