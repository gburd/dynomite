//! `dyn-admin cluster-join` -- stage a peer-join over the cluster
//! admin RPC.
//!
//! The plan returned by the seed is staged but not applied; the
//! operator runs `dyn-admin cluster-commit` afterwards to commit
//! every staged change.

use std::io::Write;

use dyn_riak::proto::pb::{
    DynRpbClusterJoinReq, DynRpbClusterJoinResp, MessageCode, DYN_STAGED_CHANGE_ADD,
    DYN_STAGED_CHANGE_REMOVE,
};
use serde::Serialize;

use crate::client::PbcClient;
use crate::error::AdminError;
use crate::output::{render_change_human, write_json, OutputFormat, StagedChange};

/// Result envelope emitted by `dyn-admin cluster-join`.
#[derive(Clone, Debug, Serialize)]
pub struct JoinReport {
    /// Node `host:port` the request was sent to.
    pub node: String,
    /// Target `host:port` the operator asked to add.
    pub target: String,
    /// Plan returned by the server.
    pub plan: StagedChange,
}

/// Run the cluster-join subcommand.
///
/// # Errors
///
/// Surfaces every wire-level or server-side failure as an
/// [`AdminError`] for the binary's `main` to render.
pub async fn run<W: Write>(
    node: &str,
    target: &str,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    let mut client = PbcClient::connect(node).await?;
    let req = DynRpbClusterJoinReq {
        target: target.as_bytes().to_vec(),
    };
    let resp: DynRpbClusterJoinResp = client
        .call(
            MessageCode::DynClusterJoinReq,
            MessageCode::DynClusterJoinResp,
            &req,
        )
        .await?;
    let change_pb = resp
        .change
        .ok_or_else(|| AdminError::Protocol("cluster-join response missing change".into()))?;
    let change = change_from_pb(&change_pb)?;
    let report = JoinReport {
        node: node.to_string(),
        target: target.to_string(),
        plan: change,
    };
    render(&report, fmt, out)
}

fn render<W: Write>(report: &JoinReport, fmt: OutputFormat, out: &mut W) -> Result<(), AdminError> {
    match fmt {
        OutputFormat::Json => write_json(out, report)?,
        OutputFormat::Human => {
            writeln!(out, "Staged cluster-join via {}", report.node)?;
            writeln!(out, "  target: {}", report.target)?;
            render_change_human(&report.plan, "  ", out)?;
            writeln!(out)?;
            writeln!(out, "Run `dyn-admin cluster-commit` to apply.")?;
        }
    }
    Ok(())
}

pub(crate) fn change_from_pb(
    pb: &dyn_riak::proto::pb::DynRpbStagedChange,
) -> Result<StagedChange, AdminError> {
    let kind = match pb.kind {
        DYN_STAGED_CHANGE_ADD => "add".into(),
        DYN_STAGED_CHANGE_REMOVE => "remove".into(),
        other => {
            return Err(AdminError::Protocol(format!(
                "unknown staged-change kind: {other}"
            )));
        }
    };
    let peer = pb.peer.as_ref().map(|p| crate::output::PeerSpec {
        host: String::from_utf8_lossy(&p.host).into_owned(),
        port: u16::try_from(p.port).unwrap_or(u16::MAX),
        dc: String::from_utf8_lossy(&p.dc).into_owned(),
        rack: String::from_utf8_lossy(&p.rack).into_owned(),
        tokens: p
            .tokens
            .iter()
            .map(|t| String::from_utf8_lossy(t).into_owned())
            .collect(),
        is_secure: p.is_secure.unwrap_or(false),
    });
    Ok(StagedChange {
        kind,
        peer_idx: pb.peer_idx,
        peer,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::PeerSpec;

    fn report() -> JoinReport {
        JoinReport {
            node: "127.0.0.1:8087".into(),
            target: "127.0.0.1:8103".into(),
            plan: StagedChange {
                kind: "add".into(),
                peer_idx: None,
                peer: Some(PeerSpec {
                    host: "127.0.0.1".into(),
                    port: 8103,
                    dc: "dc1".into(),
                    rack: "r1".into(),
                    tokens: vec!["123".into()],
                    is_secure: false,
                }),
            },
        }
    }

    #[test]
    fn human_render_includes_target_and_kind() {
        let r = report();
        let mut buf = Vec::new();
        render(&r, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Staged cluster-join via 127.0.0.1:8087"));
        assert!(s.contains("target: 127.0.0.1:8103"));
        assert!(s.contains("kind: add"));
        assert!(s.contains("cluster-commit"));
    }

    #[test]
    fn json_render_round_trips() {
        let r = report();
        let mut buf = Vec::new();
        render(&r, OutputFormat::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["node"], "127.0.0.1:8087");
        assert_eq!(v["target"], "127.0.0.1:8103");
        assert_eq!(v["plan"]["kind"], "add");
        assert_eq!(v["plan"]["peer"]["port"], 8103);
    }
}
