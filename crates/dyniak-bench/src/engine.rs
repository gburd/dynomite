//! The engine: worker pool, scheduler, rate control.
//!
//! Each worker is a `tokio::task::spawn_blocking` task because the
//! drivers are blocking by design (the drivers own a TCP socket
//! and use blocking IO to keep the byte path under tight control).
//! A multi-thread tokio runtime pools the blocking workers and a
//! lightweight async reporter task flushes histograms every
//! `report_interval`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use tracing::{debug, error, info, warn};

use crate::config::{Config, RateConfig};
use crate::driver::{make_driver, DriverOutcome};
use crate::error::{classify_driver_error, BenchError, DriverErrorClass};
use crate::keygen::KeyGen;
use crate::plot;
use crate::report::{read_op_latencies, read_summary, ReportWriter};
use crate::stats::{StatsAggregator, WindowSnapshot};
use crate::valgen::ValGen;

/// Outcome of a completed run.
#[derive(Debug)]
pub struct RunOutcome {
    /// The directory where reports + plots were written.
    pub out_dir: PathBuf,
    /// Total successful ops.
    pub ok_count: u64,
    /// Total failed ops.
    pub err_count: u64,
    /// Wall-clock duration the run actually took.
    pub elapsed: Duration,
    /// In-memory copy of every window snapshot. Useful for tests.
    pub snapshots: Vec<WindowSnapshot>,
}

/// Engine configuration.
pub struct Engine {
    cfg: Config,
}

impl Engine {
    /// Build an engine from a fully-validated config.
    #[must_use]
    pub fn new(cfg: Config) -> Self {
        Self { cfg }
    }

    /// Run the benchmark to completion.
    pub fn run(self) -> Result<RunOutcome, BenchError> {
        let cfg = self.cfg;
        let n_workers = cfg.run.concurrent;
        let duration = cfg.duration()?;
        let interval = cfg.report_interval()?;
        let interval_s = interval.as_secs_f64();

        let out_dir = cfg.resolve_out_dir();
        std::fs::create_dir_all(&out_dir)?;
        info!("dyniak-bench: out_dir = {}", out_dir.display());

        // Pre-validate the ops table against the driver vocabulary
        // by constructing one driver up front.
        let probe = make_driver(&cfg.driver)?;
        let supported = probe.supported_ops();
        for (op, _) in cfg.ops.weighted() {
            if !supported.contains(&op.as_str()) {
                return Err(BenchError::Config(format!(
                    "op `{op}` not supported by driver `{}`; supported = {supported:?}",
                    cfg.driver.kind.label()
                )));
            }
        }
        drop(probe);

        let weights: Vec<(String, u32)> = cfg.ops.weighted();
        let total_weight: u64 = weights.iter().map(|(_, w)| u64::from(*w)).sum();
        if total_weight == 0 {
            return Err(BenchError::Config("zero total op weight".into()));
        }

        let agg = Arc::new(StatsAggregator::new(n_workers));
        let stop = Arc::new(AtomicBool::new(false));
        let started = Instant::now();
        let snapshots = Arc::new(Mutex::new(Vec::<WindowSnapshot>::new()));

        // Per-worker rate-bucket: `target_ops_per_sec / concurrent`,
        // refilled per second of monotonic clock. `0` means
        // saturate.
        let per_worker_rps: f64 = match cfg.run.rate {
            RateConfig::Max => 0.0,
            RateConfig::Rps(ref t) => (t.rps as f64) / (n_workers.max(1) as f64),
        };

        let cfg_arc = Arc::new(cfg);
        let mut handles = Vec::with_capacity(n_workers);
        let stop_workers = stop.clone();
        for worker_idx in 0..n_workers {
            let agg = agg.clone();
            let cfg = cfg_arc.clone();
            let weights = weights.clone();
            let stop = stop_workers.clone();
            let h = std::thread::Builder::new()
                .name(format!("dyniak-bench-w{worker_idx}"))
                .spawn(move || {
                    worker_loop(worker_idx, cfg, weights, agg, stop, per_worker_rps);
                })
                .map_err(|e| BenchError::Engine(format!("spawn worker {worker_idx}: {e}")))?;
            handles.push(h);
        }

        // Reporter loop: every `interval`, snapshot + write CSVs.
        let mut writer = ReportWriter::new(&out_dir)?;
        let report_thread = {
            let agg = agg.clone();
            let stop = stop.clone();
            let snapshots_w = snapshots.clone();
            std::thread::Builder::new()
                .name("dyniak-bench-reporter".into())
                .spawn(move || -> Result<ReportWriter, BenchError> {
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(interval);
                        let elapsed = started.elapsed().as_secs_f64();
                        let snap = agg.flush(elapsed);
                        if snap.ok_count == 0 && snap.err_count == 0 && snap.per_op.is_empty() {
                            continue;
                        }
                        if let Err(e) = writer.write_snapshot(&snap) {
                            error!("write_snapshot: {e}");
                        }
                        snapshots_w.lock().push(snap);
                    }
                    // One final flush so any straggler ops land in
                    // the CSVs.
                    let elapsed = started.elapsed().as_secs_f64();
                    let snap = agg.flush(elapsed);
                    if snap.ok_count > 0 || snap.err_count > 0 || !snap.per_op.is_empty() {
                        if let Err(e) = writer.write_snapshot(&snap) {
                            error!("final write_snapshot: {e}");
                        }
                        snapshots_w.lock().push(snap);
                    }
                    Ok(writer)
                })
                .map_err(|e| BenchError::Engine(format!("spawn reporter: {e}")))?
        };

        // Wait out the run, then signal stop.
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);

        // Join workers and reporter.
        for h in handles {
            if let Err(e) = h.join() {
                warn!("worker join error: {e:?}");
            }
        }
        let writer = report_thread
            .join()
            .map_err(|e| BenchError::Engine(format!("reporter join: {e:?}")))??;
        writer.finish()?;

        // Summarize.
        let snapshots_owned: Vec<WindowSnapshot> = std::mem::take(&mut *snapshots.lock());
        let ok_count: u64 = snapshots_owned.iter().map(|s| s.ok_count).sum();
        let err_count: u64 = snapshots_owned.iter().map(|s| s.err_count).sum();
        let elapsed = started.elapsed();

        // Render plots.
        if let Err(e) = render_all_plots(&out_dir, interval_s, agg.clone()) {
            warn!("plot rendering failed: {e}");
        }

        // Symlink tests/last -> out_dir if possible.
        if cfg_arc.run.out_dir == "auto" {
            let _ = relink_last(&out_dir);
        }

        Ok(RunOutcome {
            out_dir,
            ok_count,
            err_count,
            elapsed,
            snapshots: snapshots_owned,
        })
    }
}

fn render_all_plots(
    out_dir: &std::path::Path,
    interval_s: f64,
    agg: Arc<StatsAggregator>,
) -> Result<(), BenchError> {
    let summary_csv = out_dir.join("summary.csv");
    if summary_csv.exists() {
        let rows = read_summary(&summary_csv)?;
        if !rows.is_empty() {
            plot::render_summary_svg(&rows, interval_s, &out_dir.join("summary.svg"))?;
        }
    }

    for op in agg.cumulative_op_names() {
        let lat_csv = out_dir.join(format!("{op}_latencies.csv"));
        if lat_csv.exists() {
            let rows = read_op_latencies(&lat_csv)?;
            if !rows.is_empty() {
                let svg = out_dir.join(format!("{op}_latencies.svg"));
                plot::render_op_latencies_svg(&rows, &op, &svg)?;
            }
        }
        if let Some(h) = agg.cumulative_for(&op) {
            let svg = out_dir.join(format!("{op}_histogram.svg"));
            plot::render_op_histogram_svg(&h, &op, &svg)?;
        }
    }
    Ok(())
}

fn relink_last(out_dir: &std::path::Path) -> std::io::Result<()> {
    let last = std::path::Path::new("tests/last");
    if last.exists() {
        let _ = std::fs::remove_file(last);
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(out_dir, last)
    }
    #[cfg(not(unix))]
    {
        // Windows / unsupported: best-effort copy of the path
        // into a marker file.
        std::fs::write(last, out_dir.to_string_lossy().as_bytes())
    }
}

/// Cap on per-class error log lines a single worker emits to
/// stderr before silencing the rest. Picked by analogy with the
/// way `basho_bench` rate-limits driver chatter.
const MAX_LOGS_PER_CLASS: u32 = 5;

fn worker_loop(
    idx: usize,
    cfg: Arc<Config>,
    weights: Vec<(String, u32)>,
    agg: Arc<StatsAggregator>,
    stop: Arc<AtomicBool>,
    per_worker_rps: f64,
) {
    let handle = agg.worker(idx);
    let mut rng = SmallRng::seed_from_u64(0x9E37_79B9_7F4A_7C15u64.wrapping_add(idx as u64));
    let mut keygen = match KeyGen::from_config(&cfg.keygen) {
        Ok(k) => k,
        Err(e) => {
            error!("worker {idx}: keygen init failed: {e}");
            return;
        }
    };
    let valgen = match ValGen::from_config(&cfg.valgen) {
        Ok(v) => v,
        Err(e) => {
            error!("worker {idx}: valgen init failed: {e}");
            return;
        }
    };
    let mut driver = match make_driver(&cfg.driver) {
        Ok(d) => d,
        Err(e) => {
            error!("worker {idx}: driver init failed: {e}");
            return;
        }
    };

    let total_weight: u64 = weights.iter().map(|(_, w)| u64::from(*w)).sum();
    let mut error_log_budget: HashMap<DriverErrorClass, u32> = HashMap::new();
    let bucket = TokenBucket::new(per_worker_rps);

    while !stop.load(Ordering::Relaxed) {
        bucket.wait();
        if stop.load(Ordering::Relaxed) {
            break;
        }

        let op = pick_op(&weights, total_weight, &mut rng);
        let op_name: &str = op;
        let started = Instant::now();
        let outcome = driver.run(op_name, &mut keygen, &valgen, &mut rng);
        let latency = started.elapsed();
        let nanos = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);

        match outcome {
            DriverOutcome::Ok => handle.record_ok(op_name, nanos),
            DriverOutcome::Err(msg) => {
                let class = classify_driver_error(&msg);
                handle.record_err(op_name, class);
                let count = error_log_budget.entry(class).or_insert(0);
                if *count < MAX_LOGS_PER_CLASS {
                    eprintln!("worker {idx} op {op_name} {}: {msg}", class.as_str());
                    *count += 1;
                    if *count == MAX_LOGS_PER_CLASS {
                        eprintln!(
                            "worker {idx}: silencing further `{}` errors",
                            class.as_str()
                        );
                    }
                } else {
                    debug!(?class, %msg, "worker {idx}: error suppressed");
                }
            }
        }
    }
}

fn pick_op<'a>(weights: &'a [(String, u32)], total: u64, rng: &mut SmallRng) -> &'a str {
    let r: u64 = {
        use rand::Rng;
        rng.random_range(0..total)
    };
    let mut acc = 0u64;
    for (op, w) in weights {
        acc += u64::from(*w);
        if r < acc {
            return op.as_str();
        }
    }
    // Should never happen because `total > 0` and `r < total`.
    weights
        .last()
        .map(|(op, _)| op.as_str())
        .expect("invariant: weights non-empty (validated upstream)")
}

/// Smooth per-worker rate limiter: targets a fixed inter-op
/// interval (`1 / rps`) and sleeps the worker between ops to
/// hold the rate. When `rps <= 0.0` the limiter is disabled
/// (saturate mode).
struct TokenBucket {
    interval_ns: u64,
    next_send: Mutex<Option<Instant>>,
}

impl TokenBucket {
    fn new(rps: f64) -> Self {
        let interval_ns = if rps > 0.0 {
            (1_000_000_000.0 / rps).max(1.0) as u64
        } else {
            0
        };
        Self {
            interval_ns,
            next_send: Mutex::new(None),
        }
    }

    /// Block until the worker is allowed to send the next op.
    /// In saturate mode this is a no-op; in rate-limited mode
    /// it sleeps the calling thread for whatever residue remains
    /// of the inter-op interval.
    fn wait(&self) {
        if self.interval_ns == 0 {
            return;
        }
        let mut slot = self.next_send.lock();
        let target = match *slot {
            Some(t) => t,
            None => Instant::now(),
        };
        let now = Instant::now();
        if target > now {
            // Cap the sleep to keep the stop signal responsive.
            let dur = (target - now).min(Duration::from_millis(50));
            drop(slot);
            std::thread::sleep(dur);
            slot = self.next_send.lock();
        }
        let advance = Duration::from_nanos(self.interval_ns);
        *slot = Some(target.max(Instant::now()) + advance);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn pick_op_distributes_by_weight() {
        let weights = vec![("get".to_string(), 4), ("set".to_string(), 1)];
        let total = 5u64;
        let mut rng = SmallRng::seed_from_u64(123);
        let mut g = 0u32;
        let mut s = 0u32;
        for _ in 0..10_000 {
            match pick_op(&weights, total, &mut rng) {
                "get" => g += 1,
                "set" => s += 1,
                _ => unreachable!(),
            }
        }
        // Expected ratio g:s = 4:1; tolerate +/- 10%.
        let total_iters = (g + s) as f64;
        let g_frac = g as f64 / total_iters;
        assert!(g_frac > 0.72 && g_frac < 0.88, "g_frac = {g_frac}");
    }

    #[test]
    fn token_bucket_saturate_is_noop() {
        let b = TokenBucket::new(0.0);
        let start = Instant::now();
        for _ in 0..1000 {
            b.wait();
        }
        assert!(start.elapsed() < Duration::from_millis(20));
    }

    #[test]
    fn token_bucket_paces() {
        // 1000 rps target -> 1ms inter-op interval.
        let b = TokenBucket::new(1000.0);
        let start = Instant::now();
        for _ in 0..50 {
            b.wait();
        }
        let elapsed = start.elapsed();
        // 50 ops at 1000 rps == 50ms. Tolerate +/- 30ms slack.
        assert!(
            elapsed >= Duration::from_millis(40),
            "too fast: {elapsed:?}"
        );
        assert!(
            elapsed <= Duration::from_millis(120),
            "too slow: {elapsed:?}"
        );
    }

    #[test]
    fn token_bucket_new_interval_math() {
        // rps <= 0 disables the limiter (interval 0).
        assert_eq!(TokenBucket::new(0.0).interval_ns, 0);
        assert_eq!(TokenBucket::new(-5.0).interval_ns, 0);
        // 1000 rps == 1ms == 1_000_000 ns inter-op interval.
        assert_eq!(TokenBucket::new(1000.0).interval_ns, 1_000_000);
        // An absurdly high rps still floors at 1ns rather than 0.
        assert_eq!(TokenBucket::new(1e12).interval_ns, 1);
    }

    #[test]
    fn pick_op_single_weight_always_returns_it() {
        let weights = vec![("only".to_string(), 7)];
        let mut rng = SmallRng::seed_from_u64(99);
        for _ in 0..100 {
            assert_eq!(pick_op(&weights, 7, &mut rng), "only");
        }
    }
}
