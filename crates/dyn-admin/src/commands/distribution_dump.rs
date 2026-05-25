//! `dyn-admin distribution-dump` -- pretty-print the
//! configured distribution for the targeted node.
//!
//! The substrate does not (yet) expose the per-rack slice table
//! over PBC, so this subcommand consults the HTTP `/stats`
//! endpoint for the distribution mode and the
//! `distribution_shadow_disagreement_total` Prometheus counter.
//! Operators use it to verify that a YAML edit took effect
//! without scraping the metrics endpoint by hand.
//!
//! When the targeted node does not advertise a distribution
//! mode (older binaries, or custom builds that skip the
//! shadow-counter wiring), the subcommand reports `<unknown>`
//! rather than failing.

use std::io::Write;

use serde::Serialize;

use crate::client::http_get;
use crate::error::AdminError;
use crate::output::{write_json, OutputFormat};

/// Per-node distribution view.
#[derive(Clone, Debug, Default, Serialize)]
pub struct DistributionView {
    /// Stats endpoint queried.
    pub queried: String,
    /// Configured live distribution.
    pub distribution: String,
    /// Configured shadow distribution, if any.
    pub shadow: Option<String>,
    /// Cumulative shadow-disagreement counter.
    pub shadow_disagreement_total: u64,
    /// Free-form note for partial state.
    pub note: Option<String>,
}

/// Run the subcommand. `stats_addr` is the `host:port` of the
/// node's HTTP stats endpoint.
pub async fn run<W: Write>(
    stats_addr: &str,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    let mut view = DistributionView {
        queried: stats_addr.to_string(),
        distribution: "<unknown>".into(),
        shadow: None,
        shadow_disagreement_total: 0,
        note: None,
    };
    match http_get(stats_addr, "/stats").await {
        Ok(body) => {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                if let Some(s) = v.get("distribution").and_then(|s| s.as_str()) {
                    view.distribution = s.to_string();
                }
                if let Some(s) = v.get("distribution_shadow").and_then(|s| s.as_str()) {
                    view.shadow = Some(s.to_string());
                }
                if let Some(n) = v
                    .get("distribution_shadow_disagreement_total")
                    .and_then(serde_json::Value::as_u64)
                {
                    view.shadow_disagreement_total = n;
                }
            } else {
                view.note = Some("stats body did not parse as JSON".into());
            }
        }
        Err(e) => view.note = Some(format!("stats fetch failed: {e}")),
    }
    render(&view, fmt, out)?;
    Ok(())
}

fn render<W: Write>(
    view: &DistributionView,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    match fmt {
        OutputFormat::Json => write_json(out, view)?,
        OutputFormat::Human => {
            writeln!(out, "Distribution dump (queried {})", view.queried)?;
            writeln!(out)?;
            writeln!(out, "  live distribution:        {}", view.distribution)?;
            writeln!(
                out,
                "  shadow distribution:      {}",
                view.shadow.as_deref().unwrap_or("<unset>")
            )?;
            writeln!(
                out,
                "  shadow disagreement count: {}",
                view.shadow_disagreement_total
            )?;
            if let Some(note) = &view.note {
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

    #[test]
    fn human_render_emits_summary_block() {
        let v = DistributionView {
            queried: "127.0.0.1:22222".into(),
            distribution: "random_slicing".into(),
            shadow: Some("vnode".into()),
            shadow_disagreement_total: 42,
            note: None,
        };
        let mut buf = Vec::new();
        render(&v, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Distribution dump"));
        assert!(s.contains("random_slicing"));
        assert!(s.contains("vnode"));
        assert!(s.contains("42"));
    }

    #[test]
    fn human_render_includes_note_when_present() {
        let v = DistributionView {
            queried: "127.0.0.1:22222".into(),
            distribution: "<unknown>".into(),
            shadow: None,
            shadow_disagreement_total: 0,
            note: Some("stats fetch failed".into()),
        };
        let mut buf = Vec::new();
        render(&v, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("note: stats fetch failed"));
    }

    #[test]
    fn json_render_serialises_fields() {
        let v = DistributionView {
            queried: "127.0.0.1:22222".into(),
            distribution: "vnode".into(),
            shadow: Some("random_slicing".into()),
            shadow_disagreement_total: 7,
            note: None,
        };
        let mut buf = Vec::new();
        render(&v, OutputFormat::Json, &mut buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(parsed["distribution"], "vnode");
        assert_eq!(parsed["shadow"], "random_slicing");
        assert_eq!(parsed["shadow_disagreement_total"], 7);
    }
}
