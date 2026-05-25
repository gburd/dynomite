//! `dyn-admin cluster-commit` -- commit every staged cluster
//! change via the cluster admin RPC.

use std::io::Write;

use dyn_riak::proto::pb::{DynRpbClusterCommitReq, DynRpbClusterCommitResp, MessageCode};
use serde::Serialize;

use crate::client::PbcClient;
use crate::error::AdminError;
use crate::output::{write_json, OutputFormat};

/// Result envelope emitted by `dyn-admin cluster-commit`.
#[derive(Clone, Debug, Serialize)]
pub struct CommitReport {
    /// Node `host:port` the request was sent to.
    pub node: String,
    /// Number of staged changes the server applied.
    pub applied: u32,
}

/// Run the cluster-commit subcommand.
///
/// # Errors
///
/// Surfaces every wire-level or server-side failure as an
/// [`AdminError`] for the binary's `main` to render.
pub async fn run<W: Write>(node: &str, fmt: OutputFormat, out: &mut W) -> Result<(), AdminError> {
    let mut client = PbcClient::connect(node).await?;
    let resp: DynRpbClusterCommitResp = client
        .call(
            MessageCode::DynClusterCommitReq,
            MessageCode::DynClusterCommitResp,
            &DynRpbClusterCommitReq::default(),
        )
        .await?;
    let report = CommitReport {
        node: node.to_string(),
        applied: resp.applied,
    };
    render(&report, fmt, out)
}

fn render<W: Write>(
    report: &CommitReport,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    match fmt {
        OutputFormat::Json => write_json(out, report)?,
        OutputFormat::Human => {
            writeln!(
                out,
                "Committed {} staged change(s) on {}",
                report.applied, report.node
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> CommitReport {
        CommitReport {
            node: "127.0.0.1:8087".into(),
            applied: 3,
        }
    }

    #[test]
    fn human_render_includes_applied_count() {
        let r = report();
        let mut buf = Vec::new();
        render(&r, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Committed 3 staged change(s) on 127.0.0.1:8087"));
    }

    #[test]
    fn json_render_round_trips() {
        let r = report();
        let mut buf = Vec::new();
        render(&r, OutputFormat::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["node"], "127.0.0.1:8087");
        assert_eq!(v["applied"], 3);
    }
}
