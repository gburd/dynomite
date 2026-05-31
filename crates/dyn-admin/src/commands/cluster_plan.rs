//! `dyn-admin cluster-plan` -- list staged-but-uncommitted
//! cluster changes via the cluster admin RPC.

use std::io::Write;

use dyniak::proto::pb::{DynRpbClusterPlanReq, DynRpbClusterPlanResp, MessageCode};
use serde::Serialize;

use crate::client::PbcClient;
use crate::commands::cluster_join::change_from_pb;
use crate::error::AdminError;
use crate::output::{render_change_human, write_json, OutputFormat, StagedChange};

/// Result envelope emitted by `dyn-admin cluster-plan`.
#[derive(Clone, Debug, Serialize)]
pub struct PlanReport {
    /// Node `host:port` the request was sent to.
    pub node: String,
    /// Pending staged changes.
    pub changes: Vec<StagedChange>,
}

/// Run the cluster-plan subcommand.
///
/// # Errors
///
/// Surfaces every wire-level or server-side failure as an
/// [`AdminError`] for the binary's `main` to render.
pub async fn run<W: Write>(node: &str, fmt: OutputFormat, out: &mut W) -> Result<(), AdminError> {
    let mut client = PbcClient::connect(node).await?;
    let resp: DynRpbClusterPlanResp = client
        .call(
            MessageCode::DynClusterPlanReq,
            MessageCode::DynClusterPlanResp,
            &DynRpbClusterPlanReq::default(),
        )
        .await?;
    let mut changes = Vec::with_capacity(resp.changes.len());
    for pb in &resp.changes {
        changes.push(change_from_pb(pb)?);
    }
    let report = PlanReport {
        node: node.to_string(),
        changes,
    };
    render(&report, fmt, out)
}

fn render<W: Write>(report: &PlanReport, fmt: OutputFormat, out: &mut W) -> Result<(), AdminError> {
    match fmt {
        OutputFormat::Json => write_json(out, report)?,
        OutputFormat::Human => {
            writeln!(out, "Pending staged changes on {}", report.node)?;
            if report.changes.is_empty() {
                writeln!(out)?;
                writeln!(out, "  (no staged changes)")?;
                return Ok(());
            }
            for (i, c) in report.changes.iter().enumerate() {
                writeln!(out)?;
                writeln!(out, "  [{i}]")?;
                render_change_human(c, "    ", out)?;
            }
            writeln!(out)?;
            writeln!(out, "{} change(s) staged.", report.changes.len())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::PeerSpec;

    fn report() -> PlanReport {
        PlanReport {
            node: "127.0.0.1:8087".into(),
            changes: vec![
                StagedChange {
                    kind: "add".into(),
                    peer_idx: None,
                    peer: Some(PeerSpec {
                        host: "10.0.0.1".into(),
                        port: 8101,
                        dc: "dc1".into(),
                        rack: "r1".into(),
                        tokens: vec!["123".into()],
                        is_secure: false,
                    }),
                },
                StagedChange {
                    kind: "remove".into(),
                    peer_idx: Some(2),
                    peer: None,
                },
            ],
        }
    }

    #[test]
    fn human_render_lists_each_change() {
        let r = report();
        let mut buf = Vec::new();
        render(&r, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Pending staged changes on 127.0.0.1:8087"));
        assert!(s.contains("[0]"));
        assert!(s.contains("[1]"));
        assert!(s.contains("kind: add"));
        assert!(s.contains("kind: remove"));
        assert!(s.contains("2 change(s) staged"));
    }

    #[test]
    fn empty_plan_says_so() {
        let r = PlanReport {
            node: "n".into(),
            changes: Vec::new(),
        };
        let mut buf = Vec::new();
        render(&r, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("(no staged changes)"));
    }

    #[test]
    fn json_render_round_trips() {
        let r = report();
        let mut buf = Vec::new();
        render(&r, OutputFormat::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["changes"][0]["kind"], "add");
        assert_eq!(v["changes"][1]["kind"], "remove");
        assert_eq!(v["changes"][1]["peer_idx"], 2);
    }
}
