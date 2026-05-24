//! `dyn-admin stats` -- query the `/stats` HTTP endpoint and pretty-
//! print a hand-picked slice of metrics.

use std::io::Write;

use serde::Serialize;

use crate::client::http_get;
use crate::error::AdminError;
use crate::output::{write_human_pairs, write_json, OutputFormat};

/// Hand-picked summary the human formatter emits. Includes the
/// fields operators look at first when triaging.
#[derive(Clone, Debug, Default, Serialize)]
pub struct StatsSummary {
    /// Engine identity and version.
    pub source: Option<String>,
    /// Engine identity and version.
    pub version: Option<String>,
    /// Datacenter name from `info.dc`.
    pub dc: Option<String>,
    /// Rack name from `info.rack`.
    pub rack: Option<String>,
    /// `uptime` seconds.
    pub uptime: Option<i64>,
    /// `pool.name`.
    pub pool: Option<String>,
    /// Backing-server name `server.name`.
    pub server: Option<String>,
    /// Latency p99 in microseconds.
    pub latency_p99: Option<u64>,
    /// Latency max in microseconds.
    pub latency_max: Option<u64>,
    /// Allocated message structs.
    pub alloc_msgs: Option<i64>,
    /// Free message structs.
    pub free_msgs: Option<i64>,
    /// Allocated mbufs.
    pub alloc_mbufs: Option<i64>,
    /// Free mbufs.
    pub free_mbufs: Option<i64>,
    /// Total bytes attributed to the engine.
    pub dyn_memory: Option<i64>,
}

/// Run the stats subcommand. The full JSON snapshot is reproduced
/// verbatim under `--json`; the human formatter prints
/// [`StatsSummary`].
pub async fn run<W: Write>(node: &str, fmt: OutputFormat, out: &mut W) -> Result<(), AdminError> {
    let body = http_get(node, "/stats").await?;
    match fmt {
        OutputFormat::Json => {
            // Echo the raw body so downstream `jq` filters keep
            // working against fields we haven't surfaced in the
            // typed summary.
            out.write_all(body.as_bytes())?;
            if !body.ends_with('\n') {
                writeln!(out)?;
            }
        }
        OutputFormat::Human => {
            let summary = summarise(&body)?;
            render_human(&summary, out)?;
        }
    }
    Ok(())
}

/// Decode `body` (a `/stats` snapshot) into a [`StatsSummary`].
pub fn summarise(body: &str) -> Result<StatsSummary, AdminError> {
    let v: serde_json::Value = serde_json::from_str(body)?;
    // Flat top-level shape produced by `Snapshot::to_json`. Pool and
    // server names are dynamic; pick the first object-valued key for
    // the pool, then the first object-valued key inside it for the
    // backing server.
    let pool_name = v.as_object().and_then(|m| {
        m.iter()
            .find_map(|(k, val)| val.is_object().then(|| k.clone()))
    });
    let server_name = pool_name
        .as_deref()
        .and_then(|p| v.get(p))
        .and_then(|p| p.as_object())
        .and_then(|m| {
            m.iter()
                .find_map(|(k, val)| val.is_object().then(|| k.clone()))
        });
    Ok(StatsSummary {
        source: v.get("source").and_then(|s| s.as_str().map(String::from)),
        version: v.get("version").and_then(|s| s.as_str().map(String::from)),
        dc: v.get("dc").and_then(|s| s.as_str().map(String::from)),
        rack: v.get("rack").and_then(|s| s.as_str().map(String::from)),
        uptime: v.get("uptime").and_then(serde_json::Value::as_i64),
        pool: pool_name,
        server: server_name,
        latency_p99: v.get("latency_99th").and_then(serde_json::Value::as_u64),
        latency_max: v.get("latency_max").and_then(serde_json::Value::as_u64),
        alloc_msgs: v.get("alloc_msgs").and_then(serde_json::Value::as_i64),
        free_msgs: v.get("free_msgs").and_then(serde_json::Value::as_i64),
        alloc_mbufs: v.get("alloc_mbufs").and_then(serde_json::Value::as_i64),
        free_mbufs: v.get("free_mbufs").and_then(serde_json::Value::as_i64),
        dyn_memory: v.get("dyn_memory").and_then(serde_json::Value::as_i64),
    })
}

fn render_human<W: Write>(s: &StatsSummary, out: &mut W) -> Result<(), AdminError> {
    let mut pairs: Vec<(&str, String)> = Vec::new();
    if let Some(v) = &s.source {
        pairs.push(("engine_source", v.clone()));
    }
    if let Some(v) = &s.version {
        pairs.push(("engine_version", v.clone()));
    }
    if let Some(v) = &s.dc {
        pairs.push(("datacenter", v.clone()));
    }
    if let Some(v) = &s.rack {
        pairs.push(("rack", v.clone()));
    }
    if let Some(v) = s.uptime {
        pairs.push(("uptime_seconds", v.to_string()));
    }
    if let Some(v) = &s.pool {
        pairs.push(("pool", v.clone()));
    }
    if let Some(v) = &s.server {
        pairs.push(("server", v.clone()));
    }
    if let Some(v) = s.latency_p99 {
        pairs.push(("latency_p99_us", v.to_string()));
    }
    if let Some(v) = s.latency_max {
        pairs.push(("latency_max_us", v.to_string()));
    }
    if let Some(v) = s.alloc_msgs {
        pairs.push(("alloc_msgs", v.to_string()));
    }
    if let Some(v) = s.free_msgs {
        pairs.push(("free_msgs", v.to_string()));
    }
    if let Some(v) = s.alloc_mbufs {
        pairs.push(("alloc_mbufs", v.to_string()));
    }
    if let Some(v) = s.free_mbufs {
        pairs.push(("free_mbufs", v.to_string()));
    }
    if let Some(v) = s.dyn_memory {
        pairs.push(("dyn_memory_bytes", v.to_string()));
    }
    let view: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    write_human_pairs(out, &view)?;
    Ok(())
}

/// Re-export so the `--json` raw passthrough has a name to call.
pub fn passthrough_json<W: Write>(body: &str, out: &mut W) -> Result<(), AdminError> {
    if body.is_empty() {
        write_json(out, &serde_json::Value::Null)?;
    } else {
        let v: serde_json::Value = serde_json::from_str(body)?;
        write_json(out, &v)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
      "service": "dynomite",
      "source": "node-a",
      "version": "0.0.1",
      "dc": "dc1",
      "rack": "r1",
      "uptime": 7,
      "latency_max": 2345,
      "latency_99th": 1234,
      "alloc_msgs": 10,
      "free_msgs": 5,
      "alloc_mbufs": 8,
      "free_mbufs": 2,
      "dyn_memory": 4096,
      "dyn_o_mite": {
        "client_eof": 0,
        "redis": {"read_requests": 0}
      }
    }"#;

    #[test]
    fn summarise_extracts_known_fields() {
        let s = summarise(SAMPLE).expect("summary");
        assert_eq!(s.source.as_deref(), Some("node-a"));
        assert_eq!(s.dc.as_deref(), Some("dc1"));
        assert_eq!(s.uptime, Some(7));
        assert_eq!(s.pool.as_deref(), Some("dyn_o_mite"));
        assert_eq!(s.latency_p99, Some(1234));
        assert_eq!(s.latency_max, Some(2345));
        assert_eq!(s.alloc_msgs, Some(10));
        assert_eq!(s.dyn_memory, Some(4096));
    }

    #[test]
    fn human_render_emits_lines_for_present_fields() {
        let s = summarise(SAMPLE).unwrap();
        let mut buf = Vec::new();
        render_human(&s, &mut buf).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("engine_source: node-a"));
        assert!(out.contains("uptime_seconds: 7"));
        assert!(out.contains("latency_p99_us: 1234"));
        assert!(out.contains("dyn_memory_bytes: 4096"));
    }

    #[test]
    fn passthrough_round_trips() {
        let mut buf = Vec::new();
        passthrough_json("{\"x\": 1}", &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["x"], 1);
    }

    #[test]
    fn summarise_tolerates_missing_fields() {
        let s = summarise("{}").unwrap();
        assert!(s.source.is_none());
        assert!(s.alloc_msgs.is_none());
    }
}
