//! Smoke test: render an SVG plot from a synthetic stats stream
//! and assert it lands on disk with non-zero size.

use std::path::PathBuf;

use dyniak_bench::plot::{render_op_latencies_svg, render_summary_svg};
use dyniak_bench::report::{OpLatencyRow, SummaryRow};

fn make_summary(n: usize) -> Vec<SummaryRow> {
    (0..n)
        .map(|i| SummaryRow {
            elapsed_s: i as f64,
            window_count: 1000 + (i as u64 % 80),
            ok_count: 990 + (i as u64 % 50),
            err_count: 10 + (i as u64 % 4),
            p50_ms: 0.4 + (i as f64) * 0.01,
            p95_ms: 0.8 + (i as f64) * 0.02,
            p99_ms: 1.4 + (i as f64) * 0.05,
            p99_9_ms: 2.4 + (i as f64) * 0.1,
            max_ms: 5.0 + (i as f64) * 0.2,
            p50_total_ms: 0.5,
            p99_total_ms: 1.6,
        })
        .collect()
}

fn make_op(n: usize) -> Vec<OpLatencyRow> {
    (0..n)
        .map(|i| OpLatencyRow {
            elapsed_s: i as f64,
            count: 1024,
            p50_us: 500 + (i as u64) * 5,
            p95_us: 800 + (i as u64) * 10,
            p99_us: 1600 + (i as u64) * 30,
            p99_9_us: 3200 + (i as u64) * 70,
            max_us: 8000 + (i as u64) * 200,
            mean_us: 700,
        })
        .collect()
}

#[test]
fn synthetic_summary_svg() {
    let dir = tempfile::tempdir().unwrap();
    let out: PathBuf = dir.path().join("summary.svg");
    render_summary_svg(&make_summary(20), 1.0, &out).unwrap();
    let md = std::fs::metadata(&out).unwrap();
    assert!(md.len() > 200, "SVG too small ({} bytes)", md.len());
    let body = std::fs::read_to_string(&out).unwrap();
    assert!(body.contains("<svg"), "no <svg> tag in output");
    assert!(body.contains("</svg>"), "<svg> not closed");
}

#[test]
fn synthetic_op_latencies_svg() {
    let dir = tempfile::tempdir().unwrap();
    let out: PathBuf = dir.path().join("get_latencies.svg");
    render_op_latencies_svg(&make_op(15), "get", &out).unwrap();
    let md = std::fs::metadata(&out).unwrap();
    assert!(md.len() > 200, "SVG too small ({} bytes)", md.len());
}
