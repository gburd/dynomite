//! HdrHistogram-backed stats aggregation.
//!
//! Per-worker per-op histograms are kept inside the worker tasks
//! and merged into a global view every `report_interval`. After
//! each merge the per-worker histograms reset; the global cumulative
//! histograms are kept around so we can report a "lifetime"
//! percentile alongside the per-window numbers.

use std::collections::HashMap;
use std::sync::Arc;

use hdrhistogram::Histogram;
use parking_lot::Mutex;

use crate::error::DriverErrorClass;

/// Per-op latency window summary. All times are in microseconds.
#[derive(Debug, Clone)]
pub struct OpWindow {
    /// Op name.
    pub op: String,
    /// Number of successful ops merged into this window.
    pub count: u64,
    /// 50th percentile latency, microseconds.
    pub p50_us: u64,
    /// 95th percentile latency, microseconds.
    pub p95_us: u64,
    /// 99th percentile latency, microseconds.
    pub p99_us: u64,
    /// 99.9th percentile latency, microseconds.
    pub p99_9_us: u64,
    /// Maximum latency observed, microseconds.
    pub max_us: u64,
    /// Mean latency, microseconds.
    pub mean_us: u64,
}

/// Aggregate snapshot for a single reporting window. Includes a
/// summary across every successful op plus per-op detail.
#[derive(Debug, Clone)]
pub struct WindowSnapshot {
    /// Wall-clock time elapsed since the run started, in seconds.
    pub elapsed_s: f64,
    /// Successful op count merged into this window (sum across all ops).
    pub ok_count: u64,
    /// Failure count merged into this window.
    pub err_count: u64,
    /// "All-ops" percentiles for the window (microseconds).
    pub p50_us: u64,
    /// 95th percentile (window).
    pub p95_us: u64,
    /// 99th percentile (window).
    pub p99_us: u64,
    /// 99.9th percentile (window).
    pub p99_9_us: u64,
    /// Maximum latency in the window, microseconds.
    pub max_us: u64,
    /// "All-ops" cumulative 50th percentile, microseconds.
    pub p50_total_us: u64,
    /// "All-ops" cumulative 99th percentile, microseconds.
    pub p99_total_us: u64,
    /// Per-op windows (sorted alphabetically by op name).
    pub per_op: Vec<OpWindow>,
    /// Per-class error counts in this window (op -> class -> count).
    pub errors: Vec<(String, DriverErrorClass, u64)>,
}

/// Per-worker mutable view.
struct WorkerSlot {
    per_op: HashMap<String, Histogram<u64>>,
    err_counts: HashMap<(String, DriverErrorClass), u64>,
    ok_count: u64,
    err_count: u64,
}

impl WorkerSlot {
    fn new() -> Self {
        Self {
            per_op: HashMap::new(),
            err_counts: HashMap::new(),
            ok_count: 0,
            err_count: 0,
        }
    }
}

/// Process-wide stats aggregator.
///
/// Thread model: each worker calls [`WorkerHandle::record_ok`] /
/// [`WorkerHandle::record_err`] on its own slot through a
/// [`WorkerHandle`]. The reporter thread calls [`Self::flush`]
/// from a separate task; the slot mutex protects
/// the merge itself but the typical (record) path is uncontended
/// because each worker uses its own slot.
pub struct StatsAggregator {
    slots: Vec<Arc<Mutex<WorkerSlot>>>,
    cumulative: Mutex<Histogram<u64>>,
    cumulative_per_op: Mutex<HashMap<String, Histogram<u64>>>,
    cumulative_ok: parking_lot::Mutex<u64>,
    cumulative_err: parking_lot::Mutex<u64>,
}

impl StatsAggregator {
    /// Build an aggregator with `n_workers` per-worker slots.
    #[must_use]
    pub fn new(n_workers: usize) -> Self {
        let mut slots = Vec::with_capacity(n_workers);
        for _ in 0..n_workers {
            slots.push(Arc::new(Mutex::new(WorkerSlot::new())));
        }
        Self {
            slots,
            cumulative: Mutex::new(new_hist()),
            cumulative_per_op: Mutex::new(HashMap::new()),
            cumulative_ok: parking_lot::Mutex::new(0),
            cumulative_err: parking_lot::Mutex::new(0),
        }
    }

    /// Return a [`WorkerHandle`] for worker `idx`.
    #[must_use]
    pub fn worker(&self, idx: usize) -> WorkerHandle {
        WorkerHandle {
            slot: self.slots[idx].clone(),
        }
    }

    /// Merge every worker slot into a fresh window snapshot, then
    /// reset the per-worker histograms. Cumulative state survives
    /// across windows.
    pub fn flush(&self, elapsed_s: f64) -> WindowSnapshot {
        let mut window_total = new_hist();
        let mut per_op: HashMap<String, Histogram<u64>> = HashMap::new();
        let mut err_counts: HashMap<(String, DriverErrorClass), u64> = HashMap::new();
        let mut ok_count = 0u64;
        let mut err_count = 0u64;

        for slot in &self.slots {
            let mut s = slot.lock();
            ok_count += s.ok_count;
            err_count += s.err_count;
            s.ok_count = 0;
            s.err_count = 0;

            let drained: Vec<(String, Histogram<u64>)> = s.per_op.drain().collect();
            for (op, h) in drained {
                if h.is_empty() {
                    continue;
                }
                let _ = window_total.add(&h);
                per_op
                    .entry(op.clone())
                    .or_insert_with(new_hist)
                    .add(&h)
                    .ok();
            }
            for ((op, cls), n) in s.err_counts.drain() {
                *err_counts.entry((op, cls)).or_insert(0) += n;
            }
        }

        // Update cumulative state.
        {
            let mut c = self.cumulative.lock();
            let _ = c.add(&window_total);
        }
        {
            let mut cper = self.cumulative_per_op.lock();
            for (op, h) in &per_op {
                cper.entry(op.clone()).or_insert_with(new_hist).add(h).ok();
            }
        }
        *self.cumulative_ok.lock() += ok_count;
        *self.cumulative_err.lock() += err_count;

        let mut sorted_ops: Vec<String> = per_op.keys().cloned().collect();
        sorted_ops.sort();
        let per_op_snap: Vec<OpWindow> = sorted_ops
            .into_iter()
            .map(|op| {
                let h = &per_op[&op];
                OpWindow {
                    op,
                    count: h.len(),
                    p50_us: h.value_at_quantile(0.50),
                    p95_us: h.value_at_quantile(0.95),
                    p99_us: h.value_at_quantile(0.99),
                    p99_9_us: h.value_at_quantile(0.999),
                    max_us: h.max(),
                    mean_us: h.mean() as u64,
                }
            })
            .collect();

        let cumulative_total = self.cumulative.lock();
        let mut errors: Vec<(String, DriverErrorClass, u64)> = err_counts
            .into_iter()
            .map(|((op, cls), n)| (op, cls, n))
            .collect();
        errors.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.as_str().cmp(b.1.as_str())));

        WindowSnapshot {
            elapsed_s,
            ok_count,
            err_count,
            p50_us: window_total.value_at_quantile(0.50),
            p95_us: window_total.value_at_quantile(0.95),
            p99_us: window_total.value_at_quantile(0.99),
            p99_9_us: window_total.value_at_quantile(0.999),
            max_us: window_total.max(),
            p50_total_us: cumulative_total.value_at_quantile(0.50),
            p99_total_us: cumulative_total.value_at_quantile(0.99),
            per_op: per_op_snap,
            errors,
        }
    }

    /// Return the cumulative per-op histogram for the named op, if any.
    #[must_use]
    pub fn cumulative_for(&self, op: &str) -> Option<Histogram<u64>> {
        let c = self.cumulative_per_op.lock();
        c.get(op).cloned()
    }

    /// Return all op names that have at least one observation in
    /// the cumulative store.
    #[must_use]
    pub fn cumulative_op_names(&self) -> Vec<String> {
        let c = self.cumulative_per_op.lock();
        let mut names: Vec<String> = c.keys().cloned().collect();
        names.sort();
        names
    }
}

/// Per-worker handle. Cheap to clone (it just wraps an `Arc`).
#[derive(Clone)]
pub struct WorkerHandle {
    slot: Arc<Mutex<WorkerSlot>>,
}

impl WorkerHandle {
    /// Record one successful op of duration `latency_ns` against
    /// op `name`.
    pub fn record_ok(&self, name: &str, latency_ns: u64) {
        let us = latency_ns / 1000;
        let us = us.max(1);
        let mut s = self.slot.lock();
        s.ok_count += 1;
        let h = s.per_op.entry(name.to_string()).or_insert_with(new_hist);
        // Hist max is 60s in microseconds; clamp to keep
        // `record` total.
        let clamped = us.min(h.high());
        let _ = h.record(clamped);
    }

    /// Record one failed op against op `name` with class `cls`.
    pub fn record_err(&self, name: &str, cls: DriverErrorClass) {
        let mut s = self.slot.lock();
        s.err_count += 1;
        let key = (name.to_string(), cls);
        *s.err_counts.entry(key).or_insert(0) += 1;
    }
}

fn new_hist() -> Histogram<u64> {
    // 1us .. 60s, three significant digits. `.expect` is justified
    // because the inputs are all fixed constants vetted by the
    // hdrhistogram crate at compile time.
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
        .expect("hdrhistogram bounds (1us..60s, 3-digit precision) are always valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flush_resets_per_worker_state() {
        let agg = StatsAggregator::new(2);
        let w0 = agg.worker(0);
        let w1 = agg.worker(1);
        for i in 0..100 {
            w0.record_ok("get", (i * 1000) as u64);
            w1.record_ok("set", ((i + 1) * 2000) as u64);
        }
        let snap = agg.flush(1.0);
        assert_eq!(snap.ok_count, 200);
        assert!(snap.p50_us > 0);
        assert_eq!(snap.per_op.len(), 2);
        // After flush, a second flush with no new data must show
        // zero counts and zero per-op windows.
        let snap2 = agg.flush(2.0);
        assert_eq!(snap2.ok_count, 0);
        assert!(snap2.per_op.is_empty());
    }

    #[test]
    fn cumulative_carries_across_windows() {
        let agg = StatsAggregator::new(1);
        let w = agg.worker(0);
        for _ in 0..10 {
            w.record_ok("get", 1000);
        }
        let _ = agg.flush(1.0);
        let names = agg.cumulative_op_names();
        assert_eq!(names, vec!["get".to_string()]);
        let h = agg.cumulative_for("get").unwrap();
        assert_eq!(h.len(), 10);
    }

    #[test]
    fn errors_classified_into_window() {
        let agg = StatsAggregator::new(1);
        let w = agg.worker(0);
        w.record_err("get", DriverErrorClass::Closed);
        w.record_err("get", DriverErrorClass::Closed);
        w.record_err("set", DriverErrorClass::Timeout);
        let snap = agg.flush(1.0);
        assert_eq!(snap.err_count, 3);
        assert_eq!(snap.errors.len(), 2);
    }
}
