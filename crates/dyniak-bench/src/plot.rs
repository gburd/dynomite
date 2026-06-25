//! Graph generation. Uses [`plotters`] with the `svg_backend`
//! feature so the binary stays portable and does not require
//! system fontconfig / freetype. The output is an SVG file per
//! chart; the `<text>` elements use the viewer's font stack.
//!
//! SVG is preferred over PNG here because PNG rendering in
//! plotters requires a TrueType font implementation (`ttf` or
//! `ab_glyph`) which would pin a system dependency this crate
//! does not otherwise need. SVG opens in any browser, scales
//! cleanly, and round-trips through the test suite without
//! pulling fontconfig.

use std::path::Path;

use plotters::prelude::*;

use crate::error::BenchError;
use crate::report::{OpLatencyRow, SummaryRow};
use crate::stats::WindowSnapshot;

/// Default plot dimensions (width x height in pixels).
pub const PLOT_W: u32 = 1200;
/// Default plot height in pixels.
pub const PLOT_H: u32 = 800;

/// Render the summary plot to an SVG file: ops/sec on the left
/// axis, error rate (%) on the right axis, both as a function of
/// elapsed seconds.
pub fn render_summary_svg(
    rows: &[SummaryRow],
    interval_s: f64,
    out_path: &Path,
) -> Result<(), BenchError> {
    if rows.is_empty() {
        return Err(BenchError::Plot("no summary rows to plot".into()));
    }
    let max_x = rows
        .iter()
        .map(|r| r.elapsed_s)
        .fold(0.0f64, f64::max)
        .max(1.0);
    let max_ops = rows
        .iter()
        .map(|r| (r.window_count as f64) / interval_s.max(1e-3))
        .fold(0.0f64, f64::max)
        .max(1.0);
    let max_err_pct = rows
        .iter()
        .map(|r| {
            let total = r.window_count.max(1) as f64;
            (r.err_count as f64 / total) * 100.0
        })
        .fold(0.0f64, f64::max)
        .max(1.0);

    let root = SVGBackend::new(out_path, (PLOT_W, PLOT_H)).into_drawing_area();
    root.fill(&WHITE)
        .map_err(|e| BenchError::Plot(format!("fill: {e}")))?;

    let mut chart = ChartBuilder::on(&root)
        .caption("dyniak-bench summary", ("sans-serif", 30))
        .margin(20)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .right_y_label_area_size(60)
        .build_cartesian_2d(0.0_f64..max_x, 0.0_f64..max_ops * 1.10)
        .map_err(|e| BenchError::Plot(format!("build cartesian: {e}")))?
        .set_secondary_coord(0.0_f64..max_x, 0.0_f64..max_err_pct.max(1.0) * 1.10);

    chart
        .configure_mesh()
        .x_desc("elapsed (s)")
        .y_desc("ops / sec")
        .axis_desc_style(("sans-serif", 16))
        .draw()
        .map_err(|e| BenchError::Plot(format!("mesh: {e}")))?;

    chart
        .configure_secondary_axes()
        .y_desc("error rate (%)")
        .draw()
        .map_err(|e| BenchError::Plot(format!("secondary axes: {e}")))?;

    let inv = 1.0 / interval_s.max(1e-3);
    let ops_series: Vec<(f64, f64)> = rows
        .iter()
        .map(|r| (r.elapsed_s, r.window_count as f64 * inv))
        .collect();
    chart
        .draw_series(LineSeries::new(ops_series.iter().copied(), BLUE))
        .map_err(|e| BenchError::Plot(format!("ops series: {e}")))?
        .label("ops/sec")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], BLUE));

    let err_series: Vec<(f64, f64)> = rows
        .iter()
        .map(|r| {
            let total = r.window_count.max(1) as f64;
            (r.elapsed_s, (r.err_count as f64 / total) * 100.0)
        })
        .collect();
    chart
        .draw_secondary_series(LineSeries::new(err_series.iter().copied(), RED))
        .map_err(|e| BenchError::Plot(format!("error series: {e}")))?
        .label("err %")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], RED));

    chart
        .configure_series_labels()
        .border_style(BLACK)
        .background_style(WHITE.mix(0.8))
        .draw()
        .map_err(|e| BenchError::Plot(format!("legend: {e}")))?;

    root.present()
        .map_err(|e| BenchError::Plot(format!("present: {e}")))?;
    Ok(())
}

/// Render a per-op latency SVG: x = elapsed seconds, y = latency
/// in milliseconds, with three lines (p50, p99, p99.9).
pub fn render_op_latencies_svg(
    rows: &[OpLatencyRow],
    op: &str,
    out_path: &Path,
) -> Result<(), BenchError> {
    if rows.is_empty() {
        return Err(BenchError::Plot(format!("no latency rows for op `{op}`")));
    }
    let max_x = rows
        .iter()
        .map(|r| r.elapsed_s)
        .fold(0.0f64, f64::max)
        .max(1.0);
    let max_y_ms = rows
        .iter()
        .map(|r| r.p99_9_us as f64 / 1000.0)
        .fold(0.0f64, f64::max)
        .max(0.5);

    let root = SVGBackend::new(out_path, (PLOT_W, PLOT_H)).into_drawing_area();
    root.fill(&WHITE)
        .map_err(|e| BenchError::Plot(format!("fill: {e}")))?;

    let title = format!("{op} latency (p50 / p99 / p99.9)");
    let mut chart = ChartBuilder::on(&root)
        .caption(title.as_str(), ("sans-serif", 28))
        .margin(20)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(0.0_f64..max_x, 0.0_f64..max_y_ms * 1.10)
        .map_err(|e| BenchError::Plot(format!("build cartesian: {e}")))?;

    chart
        .configure_mesh()
        .x_desc("elapsed (s)")
        .y_desc("latency (ms)")
        .axis_desc_style(("sans-serif", 16))
        .draw()
        .map_err(|e| BenchError::Plot(format!("mesh: {e}")))?;

    let p50: Vec<(f64, f64)> = rows
        .iter()
        .map(|r| (r.elapsed_s, r.p50_us as f64 / 1000.0))
        .collect();
    let p99: Vec<(f64, f64)> = rows
        .iter()
        .map(|r| (r.elapsed_s, r.p99_us as f64 / 1000.0))
        .collect();
    let p999: Vec<(f64, f64)> = rows
        .iter()
        .map(|r| (r.elapsed_s, r.p99_9_us as f64 / 1000.0))
        .collect();

    chart
        .draw_series(LineSeries::new(p50.iter().copied(), BLUE))
        .map_err(|e| BenchError::Plot(format!("p50: {e}")))?
        .label("p50")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], BLUE));
    chart
        .draw_series(LineSeries::new(p99.iter().copied(), MAGENTA))
        .map_err(|e| BenchError::Plot(format!("p99: {e}")))?
        .label("p99")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], MAGENTA));
    chart
        .draw_series(LineSeries::new(p999.iter().copied(), RED))
        .map_err(|e| BenchError::Plot(format!("p999: {e}")))?
        .label("p99.9")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], RED));

    chart
        .configure_series_labels()
        .border_style(BLACK)
        .background_style(WHITE.mix(0.8))
        .draw()
        .map_err(|e| BenchError::Plot(format!("legend: {e}")))?;

    root.present()
        .map_err(|e| BenchError::Plot(format!("present: {e}")))?;
    Ok(())
}

/// Render a histogram SVG for the cumulative latency distribution
/// of one op, derived from the live snapshot. The x axis is
/// log-microseconds to keep the long tail readable.
pub fn render_op_histogram_svg(
    snapshot: &hdrhistogram::Histogram<u64>,
    op: &str,
    out_path: &Path,
) -> Result<(), BenchError> {
    if snapshot.is_empty() {
        return Err(BenchError::Plot(format!(
            "histogram is empty for op `{op}`"
        )));
    }
    // Build log-spaced buckets between 1us and the observed max.
    let max_us = snapshot.max().max(1);
    let n_buckets: usize = 64;
    let log_max = (max_us as f64).ln().max(1e-6);
    let mut bucket_counts = vec![0u64; n_buckets];
    for v in snapshot.iter_recorded() {
        let raw = v.value_iterated_to().max(1) as f64;
        let frac = (raw.ln() / log_max).clamp(0.0, 1.0);
        let idx = ((frac * (n_buckets as f64 - 1.0)).floor() as usize).min(n_buckets - 1);
        bucket_counts[idx] += v.count_at_value();
    }
    let max_count = *bucket_counts.iter().max().unwrap_or(&1);
    let max_count_f = (max_count as f64).max(1.0);

    let root = SVGBackend::new(out_path, (PLOT_W, PLOT_H)).into_drawing_area();
    root.fill(&WHITE)
        .map_err(|e| BenchError::Plot(format!("fill: {e}")))?;

    let title = format!("{op} latency histogram");
    let mut chart = ChartBuilder::on(&root)
        .caption(title.as_str(), ("sans-serif", 28))
        .margin(20)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(0.0_f64..n_buckets as f64, 0.0_f64..max_count_f * 1.10)
        .map_err(|e| BenchError::Plot(format!("build cartesian: {e}")))?;

    chart
        .configure_mesh()
        .x_desc("log-bucket (1us .. max)")
        .y_desc("count")
        .axis_desc_style(("sans-serif", 16))
        .draw()
        .map_err(|e| BenchError::Plot(format!("mesh: {e}")))?;

    // Render as a piecewise-linear envelope of the bucket counts.
    let series: Vec<(f64, f64)> = bucket_counts
        .iter()
        .enumerate()
        .map(|(i, c)| (i as f64, *c as f64))
        .collect();
    chart
        .draw_series(LineSeries::new(series, BLUE))
        .map_err(|e| BenchError::Plot(format!("series: {e}")))?
        .label("count")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], BLUE));

    chart
        .configure_series_labels()
        .border_style(BLACK)
        .background_style(WHITE.mix(0.8))
        .draw()
        .map_err(|e| BenchError::Plot(format!("legend: {e}")))?;

    root.present()
        .map_err(|e| BenchError::Plot(format!("present: {e}")))?;
    Ok(())
}

/// Build a synthetic series of [`SummaryRow`]s from a vector of
/// in-memory [`WindowSnapshot`]s. Used by the smoke tests.
#[must_use]
pub fn snapshots_to_summary_rows(snaps: &[WindowSnapshot]) -> Vec<SummaryRow> {
    snaps
        .iter()
        .map(|s| SummaryRow {
            elapsed_s: s.elapsed_s,
            window_count: s.ok_count + s.err_count,
            ok_count: s.ok_count,
            err_count: s.err_count,
            p50_ms: (s.p50_us as f64) / 1000.0,
            p95_ms: (s.p95_us as f64) / 1000.0,
            p99_ms: (s.p99_us as f64) / 1000.0,
            p99_9_ms: (s.p99_9_us as f64) / 1000.0,
            max_ms: (s.max_us as f64) / 1000.0,
            p50_total_ms: (s.p50_total_us as f64) / 1000.0,
            p99_total_ms: (s.p99_total_us as f64) / 1000.0,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn rows(n: usize) -> Vec<SummaryRow> {
        (0..n)
            .map(|i| SummaryRow {
                elapsed_s: i as f64,
                window_count: 1000 + (i as u64 % 50),
                ok_count: 990 + (i as u64 % 40),
                err_count: 10 + (i as u64 % 5),
                p50_ms: 0.5,
                p95_ms: 1.2,
                p99_ms: 2.4,
                p99_9_ms: 4.8,
                max_ms: 9.6,
                p50_total_ms: 0.6,
                p99_total_ms: 2.5,
            })
            .collect()
    }

    fn op_rows(n: usize) -> Vec<OpLatencyRow> {
        (0..n)
            .map(|i| OpLatencyRow {
                elapsed_s: i as f64,
                count: 1000,
                p50_us: 500,
                p95_us: 1200,
                p99_us: 2400,
                p99_9_us: 4800,
                max_us: 9600,
                mean_us: 600,
            })
            .collect()
    }

    #[test]
    fn summary_svg_renders_nonzero_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("summary.svg");
        let r = rows(10);
        render_summary_svg(&r, 1.0, &path).unwrap();
        let md = std::fs::metadata(&path).unwrap();
        assert!(md.len() > 0, "SVG should be non-empty");
    }

    #[test]
    fn op_latencies_svg_renders() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("get_latencies.svg");
        let r = op_rows(8);
        render_op_latencies_svg(&r, "get", &path).unwrap();
        let md = std::fs::metadata(&path).unwrap();
        assert!(md.len() > 0, "SVG should be non-empty");
    }

    #[test]
    fn empty_summary_errors() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("summary.svg");
        let r = render_summary_svg(&[], 1.0, &p);
        assert!(r.is_err());
    }

    #[test]
    fn histogram_renders() {
        let mut h = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        for v in 1..=1000 {
            h.record(v).unwrap();
        }
        let dir = tempdir().unwrap();
        let p = dir.path().join("get_histogram.svg");
        render_op_histogram_svg(&h, "get", &p).unwrap();
        let md = std::fs::metadata(&p).unwrap();
        assert!(md.len() > 0);
    }
}
