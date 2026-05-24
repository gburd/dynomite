//! `dyn-admin status` -- fetch [`RpbGetServerInfoResp`] from the
//! configured node's PBC port and combine it with a small slice of
//! the `/stats` HTTP endpoint when reachable.
//!
//! The HTTP query is best-effort: a stats endpoint may not be
//! configured for every node, in which case the subcommand still
//! prints the PBC-derived fields and notes the stats fetch failure
//! in the human-formatted output. The JSON variant carries an
//! explicit `stats: null` when the fetch fails so consumers can
//! distinguish "unreachable" from "empty".

use std::io::Write;

use dyn_riak::proto::pb::{MessageCode, RpbGetServerInfoResp, RpbServerInfoReq};
use serde::Serialize;

use crate::client::{http_get, PbcClient};
use crate::error::AdminError;
use crate::output::{write_human_pairs, write_json, OutputFormat};

/// Subset of the `/stats` JSON snapshot the `status` command surfaces.
#[derive(Clone, Debug, Default, Serialize)]
pub struct StatusStats {
    /// `info.source` -- engine identity.
    pub source: Option<String>,
    /// `info.version` -- engine version string.
    pub version: Option<String>,
    /// `info.dc` / `info.rack`.
    pub dc: Option<String>,
    /// As above.
    pub rack: Option<String>,
    /// `uptime` in seconds.
    pub uptime: Option<i64>,
    /// `pool.name`.
    pub pool: Option<String>,
}

/// Output payload for the `status` subcommand.
#[derive(Clone, Debug, Serialize)]
pub struct Status {
    /// `host:port` of the PBC peer.
    pub node: String,
    /// `RpbGetServerInfoResp.node`.
    pub server_node: Option<String>,
    /// `RpbGetServerInfoResp.server_version`.
    pub server_version: Option<String>,
    /// Stats fetch outcome. `None` when the stats endpoint was not
    /// configured by the caller; `Some(Err(...))` when the fetch
    /// failed; `Some(Ok(...))` on success.
    pub stats: Option<StatusStats>,
    /// Human-readable note about the stats fetch failure when one
    /// occurred.
    pub stats_error: Option<String>,
}

/// Run the status subcommand. `stats_addr` is optional: when `None`
/// only PBC fields are queried.
pub async fn run<W: Write>(
    node: &str,
    stats_addr: Option<&str>,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    let info = fetch_server_info(node).await?;
    let mut status = Status {
        node: node.to_string(),
        server_node: info.node.map(|b| String::from_utf8_lossy(&b).into_owned()),
        server_version: info
            .server_version
            .map(|b| String::from_utf8_lossy(&b).into_owned()),
        stats: None,
        stats_error: None,
    };
    if let Some(addr) = stats_addr {
        match http_get(addr, "/stats").await {
            Ok(body) => match parse_stats(&body) {
                Ok(s) => status.stats = Some(s),
                Err(e) => status.stats_error = Some(format!("parse: {e}")),
            },
            Err(e) => status.stats_error = Some(format!("fetch: {e}")),
        }
    }
    render(&status, fmt, out)?;
    Ok(())
}

async fn fetch_server_info(node: &str) -> Result<RpbGetServerInfoResp, AdminError> {
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

fn parse_stats(body: &str) -> Result<StatusStats, AdminError> {
    let v: serde_json::Value = serde_json::from_str(body)?;
    // The dynomite snapshot uses flat top-level keys (`source`,
    // `version`, `dc`, `rack`, `uptime`, `service`) plus a
    // dynamically-named pool sub-object whose key is the YAML pool
    // name. We pick the pool by ignoring the well-known scalar
    // top-level keys and keeping the first remaining object value.
    let pool_name = first_pool_name(&v);
    Ok(StatusStats {
        source: v.get("source").and_then(|s| s.as_str().map(String::from)),
        version: v.get("version").and_then(|s| s.as_str().map(String::from)),
        dc: v.get("dc").and_then(|s| s.as_str().map(String::from)),
        rack: v.get("rack").and_then(|s| s.as_str().map(String::from)),
        uptime: v.get("uptime").and_then(serde_json::Value::as_i64),
        pool: pool_name,
    })
}

/// Locate the dynamically-named pool key inside a stats snapshot.
/// Returns the first key whose value is a JSON object; the snapshot
/// emits exactly one such key (the pool).
fn first_pool_name(v: &serde_json::Value) -> Option<String> {
    let obj = v.as_object()?;
    obj.iter().find_map(|(k, val)| {
        if val.is_object() {
            Some(k.clone())
        } else {
            None
        }
    })
}

fn render<W: Write>(s: &Status, fmt: OutputFormat, out: &mut W) -> Result<(), AdminError> {
    match fmt {
        OutputFormat::Json => {
            write_json(out, s)?;
        }
        OutputFormat::Human => {
            let mut pairs: Vec<(&str, String)> = Vec::new();
            pairs.push(("node", s.node.clone()));
            pairs.push((
                "server_node",
                s.server_node.clone().unwrap_or_else(|| "<unset>".into()),
            ));
            pairs.push((
                "server_version",
                s.server_version.clone().unwrap_or_else(|| "<unset>".into()),
            ));
            if let Some(stats) = &s.stats {
                pairs.push((
                    "engine_source",
                    stats.source.clone().unwrap_or_else(|| "<unset>".into()),
                ));
                pairs.push((
                    "engine_version",
                    stats.version.clone().unwrap_or_else(|| "<unset>".into()),
                ));
                pairs.push((
                    "datacenter",
                    stats.dc.clone().unwrap_or_else(|| "<unset>".into()),
                ));
                pairs.push((
                    "rack",
                    stats.rack.clone().unwrap_or_else(|| "<unset>".into()),
                ));
                if let Some(u) = stats.uptime {
                    pairs.push(("uptime_seconds", u.to_string()));
                }
                if let Some(p) = &stats.pool {
                    pairs.push(("pool", p.clone()));
                }
            } else if let Some(err) = &s.stats_error {
                pairs.push(("stats", format!("unavailable ({err})")));
            }
            // Re-borrow to satisfy `write_human_pairs`'s `&[(&str, &str)]`.
            let view: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
            write_human_pairs(out, &view)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_stats_pulls_known_fields() {
        let body = r#"{
            "service": "dynomite",
            "source": "node-a",
            "version": "0.0.1",
            "dc": "dc1",
            "rack": "r1",
            "uptime": 42,
            "dyn_o_mite": {"client_eof": 0}
        }"#;
        let s = parse_stats(body).expect("parse");
        assert_eq!(s.source.as_deref(), Some("node-a"));
        assert_eq!(s.version.as_deref(), Some("0.0.1"));
        assert_eq!(s.dc.as_deref(), Some("dc1"));
        assert_eq!(s.rack.as_deref(), Some("r1"));
        assert_eq!(s.uptime, Some(42));
        assert_eq!(s.pool.as_deref(), Some("dyn_o_mite"));
    }

    #[test]
    fn parse_stats_tolerates_missing_fields() {
        let body = "{}";
        let s = parse_stats(body).expect("parse");
        assert!(s.source.is_none());
        assert!(s.uptime.is_none());
    }

    #[test]
    fn human_render_includes_pbc_fields() {
        let status = Status {
            node: "127.0.0.1:8087".into(),
            server_node: Some("dyn-riak".into()),
            server_version: Some("dyn-riak 0.0.1".into()),
            stats: None,
            stats_error: None,
        };
        let mut buf = Vec::new();
        render(&status, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("server_node: dyn-riak"));
        assert!(s.contains("server_version: dyn-riak 0.0.1"));
    }

    #[test]
    fn human_render_marks_stats_unavailable() {
        let status = Status {
            node: "n".into(),
            server_node: None,
            server_version: None,
            stats: None,
            stats_error: Some("connection refused".into()),
        };
        let mut buf = Vec::new();
        render(&status, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("stats: unavailable"));
    }

    #[test]
    fn json_render_includes_stats_block() {
        let status = Status {
            node: "n".into(),
            server_node: Some("n".into()),
            server_version: Some("v".into()),
            stats: Some(StatusStats {
                source: Some("node-a".into()),
                version: Some("0.0.1".into()),
                dc: Some("dc".into()),
                rack: Some("r".into()),
                uptime: Some(7),
                pool: Some("p".into()),
            }),
            stats_error: None,
        };
        let mut buf = Vec::new();
        render(&status, OutputFormat::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["stats"]["source"], "node-a");
        assert_eq!(v["stats"]["uptime"], 7);
    }
}
