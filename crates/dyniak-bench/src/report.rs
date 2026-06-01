//! CSV report writer.
//!
//! One `summary.csv`, one `errors.csv`, and one
//! `<op>_latencies.csv` per observed op. Each file is opened on
//! first use and held open until the run ends.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::error::BenchError;
use crate::stats::WindowSnapshot;

/// File-set used by the engine to emit per-window rows.
pub struct ReportWriter {
    out_dir: PathBuf,
    summary: BufWriter<File>,
    errors: BufWriter<File>,
    per_op: HashMap<String, BufWriter<File>>,
}

impl ReportWriter {
    /// Create the output directory (if needed) and open
    /// `summary.csv` and `errors.csv`. Per-op latency files are
    /// opened lazily on first observation.
    pub fn new(out_dir: &Path) -> Result<Self, BenchError> {
        std::fs::create_dir_all(out_dir)?;
        let summary_path = out_dir.join("summary.csv");
        let mut summary = BufWriter::new(open_truncate(&summary_path)?);
        writeln!(
            summary,
            "elapsed_s,window_count,ok_count,err_count,p50_ms,p95_ms,p99_ms,p99_9_ms,max_ms,p50_total_ms,p99_total_ms"
        )
        .map_err(|e| BenchError::Csv(e.to_string()))?;

        let errors_path = out_dir.join("errors.csv");
        let mut errors = BufWriter::new(open_truncate(&errors_path)?);
        writeln!(errors, "elapsed_s,op,class,count").map_err(|e| BenchError::Csv(e.to_string()))?;

        Ok(Self {
            out_dir: out_dir.to_path_buf(),
            summary,
            errors,
            per_op: HashMap::new(),
        })
    }

    /// Write one snapshot row to every relevant file.
    pub fn write_snapshot(&mut self, snap: &WindowSnapshot) -> Result<(), BenchError> {
        let window_count = snap.ok_count + snap.err_count;
        writeln!(
            self.summary,
            "{:.3},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
            snap.elapsed_s,
            window_count,
            snap.ok_count,
            snap.err_count,
            us_to_ms(snap.p50_us),
            us_to_ms(snap.p95_us),
            us_to_ms(snap.p99_us),
            us_to_ms(snap.p99_9_us),
            us_to_ms(snap.max_us),
            us_to_ms(snap.p50_total_us),
            us_to_ms(snap.p99_total_us),
        )
        .map_err(|e| BenchError::Csv(e.to_string()))?;
        self.summary.flush().ok();

        for op in &snap.per_op {
            if !self.per_op.contains_key(&op.op) {
                let path = self.out_dir.join(format!("{}_latencies.csv", op.op));
                let mut f = BufWriter::new(open_truncate(&path)?);
                writeln!(
                    f,
                    "elapsed_s,count,p50_us,p95_us,p99_us,p99_9_us,max_us,mean_us"
                )
                .map_err(|e| BenchError::Csv(e.to_string()))?;
                self.per_op.insert(op.op.clone(), f);
            }
            let entry = self
                .per_op
                .get_mut(&op.op)
                .expect("inserted above; lookup is total");
            writeln!(
                entry,
                "{:.3},{},{},{},{},{},{},{}",
                snap.elapsed_s,
                op.count,
                op.p50_us,
                op.p95_us,
                op.p99_us,
                op.p99_9_us,
                op.max_us,
                op.mean_us,
            )
            .map_err(|e| BenchError::Csv(e.to_string()))?;
            entry.flush().ok();
        }

        for (op, cls, n) in &snap.errors {
            writeln!(
                self.errors,
                "{:.3},{},{},{}",
                snap.elapsed_s,
                op,
                cls.as_str(),
                n
            )
            .map_err(|e| BenchError::Csv(e.to_string()))?;
        }
        self.errors.flush().ok();
        Ok(())
    }

    /// Flush every file. Called at the end of a run before plot
    /// rendering so the plotters code can re-read the data from
    /// disk (round-trip integration test depends on this).
    pub fn finish(mut self) -> Result<(), BenchError> {
        self.summary.flush().ok();
        self.errors.flush().ok();
        for (_, mut w) in self.per_op.drain() {
            w.flush().ok();
        }
        Ok(())
    }
}

fn open_truncate(p: &Path) -> Result<File, BenchError> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(p)
        .map_err(BenchError::Io)
}

fn us_to_ms(us: u64) -> f64 {
    (us as f64) / 1000.0
}

/// Parse a previously-written `summary.csv` back into snapshots.
/// Used by the `csv_round_trip` integration test (and by the
/// `plot::summary` code as a fallback when the engine does not
/// keep the in-memory copy around).
pub fn read_summary(path: &Path) -> Result<Vec<SummaryRow>, BenchError> {
    let text = std::fs::read_to_string(path)?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if i == 0 {
            continue; // header
        }
        let cells: Vec<&str> = line.split(',').collect();
        if cells.len() != 11 {
            return Err(BenchError::Csv(format!(
                "summary.csv line {}: expected 11 cells, got {}",
                i + 1,
                cells.len()
            )));
        }
        let parse_f64 = |s: &str| -> Result<f64, BenchError> {
            s.parse()
                .map_err(|e| BenchError::Csv(format!("parse f64 `{s}`: {e}")))
        };
        let parse_u64 = |s: &str| -> Result<u64, BenchError> {
            s.parse()
                .map_err(|e| BenchError::Csv(format!("parse u64 `{s}`: {e}")))
        };
        out.push(SummaryRow {
            elapsed_s: parse_f64(cells[0])?,
            window_count: parse_u64(cells[1])?,
            ok_count: parse_u64(cells[2])?,
            err_count: parse_u64(cells[3])?,
            p50_ms: parse_f64(cells[4])?,
            p95_ms: parse_f64(cells[5])?,
            p99_ms: parse_f64(cells[6])?,
            p99_9_ms: parse_f64(cells[7])?,
            max_ms: parse_f64(cells[8])?,
            p50_total_ms: parse_f64(cells[9])?,
            p99_total_ms: parse_f64(cells[10])?,
        });
    }
    Ok(out)
}

/// Round-tripped row of `summary.csv`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SummaryRow {
    /// Seconds since the run started.
    pub elapsed_s: f64,
    /// Total ops in the window (ok + err).
    pub window_count: u64,
    /// Successful op count in the window.
    pub ok_count: u64,
    /// Failed op count in the window.
    pub err_count: u64,
    /// Window p50 latency, milliseconds.
    pub p50_ms: f64,
    /// Window p95 latency, milliseconds.
    pub p95_ms: f64,
    /// Window p99 latency, milliseconds.
    pub p99_ms: f64,
    /// Window p99.9 latency, milliseconds.
    pub p99_9_ms: f64,
    /// Window max latency, milliseconds.
    pub max_ms: f64,
    /// Cumulative p50 latency, milliseconds.
    pub p50_total_ms: f64,
    /// Cumulative p99 latency, milliseconds.
    pub p99_total_ms: f64,
}

/// Round-tripped row of `<op>_latencies.csv`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpLatencyRow {
    /// Seconds since the run started.
    pub elapsed_s: f64,
    /// Sample count contributing to the window.
    pub count: u64,
    /// Window p50, microseconds.
    pub p50_us: u64,
    /// Window p95, microseconds.
    pub p95_us: u64,
    /// Window p99, microseconds.
    pub p99_us: u64,
    /// Window p99.9, microseconds.
    pub p99_9_us: u64,
    /// Window max, microseconds.
    pub max_us: u64,
    /// Window mean, microseconds.
    pub mean_us: u64,
}

/// Parse a previously-written `<op>_latencies.csv` back into rows.
pub fn read_op_latencies(path: &Path) -> Result<Vec<OpLatencyRow>, BenchError> {
    let text = std::fs::read_to_string(path)?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if i == 0 {
            continue;
        }
        let cells: Vec<&str> = line.split(',').collect();
        if cells.len() != 8 {
            return Err(BenchError::Csv(format!(
                "{} line {}: expected 8 cells, got {}",
                path.display(),
                i + 1,
                cells.len()
            )));
        }
        let parse_f64 = |s: &str| -> Result<f64, BenchError> {
            s.parse()
                .map_err(|e| BenchError::Csv(format!("parse f64 `{s}`: {e}")))
        };
        let parse_u64 = |s: &str| -> Result<u64, BenchError> {
            s.parse()
                .map_err(|e| BenchError::Csv(format!("parse u64 `{s}`: {e}")))
        };
        out.push(OpLatencyRow {
            elapsed_s: parse_f64(cells[0])?,
            count: parse_u64(cells[1])?,
            p50_us: parse_u64(cells[2])?,
            p95_us: parse_u64(cells[3])?,
            p99_us: parse_u64(cells[4])?,
            p99_9_us: parse_u64(cells[5])?,
            max_us: parse_u64(cells[6])?,
            mean_us: parse_u64(cells[7])?,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DriverErrorClass;
    use crate::stats::OpWindow;
    use tempfile::tempdir;

    fn snap(elapsed: f64, ok: u64) -> WindowSnapshot {
        WindowSnapshot {
            elapsed_s: elapsed,
            ok_count: ok,
            err_count: 0,
            p50_us: 1000,
            p95_us: 2000,
            p99_us: 4000,
            p99_9_us: 8000,
            max_us: 16000,
            p50_total_us: 1100,
            p99_total_us: 4100,
            per_op: vec![OpWindow {
                op: "get".into(),
                count: ok,
                p50_us: 900,
                p95_us: 1800,
                p99_us: 3600,
                p99_9_us: 7200,
                max_us: 14_400,
                mean_us: 1100,
            }],
            errors: vec![("get".into(), DriverErrorClass::Closed, 1)],
        }
    }

    #[test]
    fn write_and_read_summary() {
        let dir = tempdir().unwrap();
        let mut w = ReportWriter::new(dir.path()).unwrap();
        w.write_snapshot(&snap(1.0, 100)).unwrap();
        w.write_snapshot(&snap(2.0, 200)).unwrap();
        w.finish().unwrap();

        let rows = read_summary(&dir.path().join("summary.csv")).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ok_count, 100);
        assert!((rows[0].p50_ms - 1.0).abs() < 1e-9);

        let op = read_op_latencies(&dir.path().join("get_latencies.csv")).unwrap();
        assert_eq!(op.len(), 2);
        assert_eq!(op[0].count, 100);
        assert_eq!(op[0].p99_us, 3600);
    }

    #[test]
    fn round_trip_matches_input_shape() {
        let dir = tempdir().unwrap();
        let mut w = ReportWriter::new(dir.path()).unwrap();
        for i in 1..=5 {
            w.write_snapshot(&snap(i as f64, 50)).unwrap();
        }
        w.finish().unwrap();
        let rows = read_summary(&dir.path().join("summary.csv")).unwrap();
        assert_eq!(rows.len(), 5);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.ok_count, 50);
            assert_eq!(row.elapsed_s as u64, (i + 1) as u64);
        }
    }
}
