//! `dyn-admin ping` -- send `RpbPingReq` and time the reply.

use std::io::Write;
use std::time::{Duration, Instant};

use dyn_riak::proto::pb::{MessageCode, RpbPingReq, RpbPingResp};

use crate::client::PbcClient;
use crate::error::AdminError;
use crate::output::{write_json, OutputFormat};

/// Result of a ping round-trip. Public so the `--json` formatter and
/// integration tests can introspect it.
#[derive(Clone, Debug, serde::Serialize)]
pub struct PingResult {
    /// `host:port` the client targeted.
    pub node: String,
    /// Reply text. Always `"PONG"` on success.
    pub reply: String,
    /// Round-trip time in microseconds.
    pub rtt_us: u64,
}

/// Run the ping subcommand against `node` and write the outcome to
/// `out`. Returns `Ok(())` on success; a server-side error or wire
/// failure surfaces as an [`AdminError`].
pub async fn run<W: Write>(node: &str, fmt: OutputFormat, out: &mut W) -> Result<(), AdminError> {
    let mut client = PbcClient::connect(node).await?;
    let start = Instant::now();
    let _resp: RpbPingResp = client
        .call(
            MessageCode::PingReq,
            MessageCode::PingResp,
            &RpbPingReq::default(),
        )
        .await?;
    let rtt = start.elapsed();
    let result = PingResult {
        node: node.to_string(),
        reply: "PONG".into(),
        rtt_us: u64::try_from(rtt.as_micros()).unwrap_or(u64::MAX),
    };
    render(&result, fmt, out)?;
    Ok(())
}

fn render<W: Write>(r: &PingResult, fmt: OutputFormat, out: &mut W) -> Result<(), AdminError> {
    match fmt {
        OutputFormat::Json => {
            write_json(out, r)?;
        }
        OutputFormat::Human => {
            let rtt = format_rtt(Duration::from_micros(r.rtt_us));
            writeln!(out, "{} from {} ({})", r.reply, r.node, rtt)?;
        }
    }
    Ok(())
}

fn format_rtt(d: Duration) -> String {
    if d.as_millis() >= 1 {
        format!("{:.3} ms", d.as_secs_f64() * 1000.0)
    } else {
        format!("{} us", d.as_micros())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_render_contains_pong_and_node() {
        let r = PingResult {
            node: "127.0.0.1:8087".into(),
            reply: "PONG".into(),
            rtt_us: 1234,
        };
        let mut buf = Vec::new();
        render(&r, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("PONG"));
        assert!(s.contains("127.0.0.1:8087"));
        assert!(s.contains("1.234 ms"));
    }

    #[test]
    fn human_render_uses_us_for_sub_ms() {
        let r = PingResult {
            node: "127.0.0.1:8087".into(),
            reply: "PONG".into(),
            rtt_us: 250,
        };
        let mut buf = Vec::new();
        render(&r, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("250 us"), "expected microseconds: {s}");
    }

    #[test]
    fn json_render_emits_object() {
        let r = PingResult {
            node: "127.0.0.1:8087".into(),
            reply: "PONG".into(),
            rtt_us: 1234,
        };
        let mut buf = Vec::new();
        render(&r, OutputFormat::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["reply"], "PONG");
        assert_eq!(v["rtt_us"], 1234);
        assert_eq!(v["node"], "127.0.0.1:8087");
    }

    #[test]
    fn write_human_pairs_emits_lines() {
        use crate::output::write_human_pairs;
        let mut buf = Vec::new();
        write_human_pairs(&mut buf, &[("a", "1"), ("b", "two")]).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("a: 1\n"));
        assert!(s.contains("b: two"));
    }
}
