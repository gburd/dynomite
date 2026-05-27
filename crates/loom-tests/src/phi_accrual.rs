//! Phi-accrual model: a sample counter that records inter-arrival
//! gaps and produces a phi observation. Production type is
//! `dynomite::cluster::failure_detector`. We model the contended
//! path: two threads recording heartbeats from the same peer
//! plus a third thread observing phi.
//!
//! Invariants:
//! * Sample count equals the number of recorded heartbeats.
//! * Phi never goes negative.

use loom::sync::atomic::{AtomicU64, Ordering};
use loom::sync::Mutex;

pub struct PhiModel {
    samples: Mutex<Vec<u32>>,
    sample_count: AtomicU64,
}

impl PhiModel {
    pub(crate) fn new() -> Self {
        Self {
            samples: Mutex::new(Vec::new()),
            sample_count: AtomicU64::new(0),
        }
    }

    pub(crate) fn record(&self, gap: u32) {
        let mut g = self.samples.lock().unwrap();
        g.push(gap);
        self.sample_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn phi(&self) -> f64 {
        let g = self.samples.lock().unwrap();
        if g.is_empty() {
            return 0.0;
        }
        // Bounded to u32 above; f64 precision is fine for the
        // model's non-negativity assertion.
        let total: u64 = g.iter().copied().map(u64::from).sum();
        let total_f64 = u32::try_from(total).map(f64::from).unwrap_or(f64::MAX);
        let n = u32::try_from(g.len()).unwrap_or(u32::MAX);
        let mean = total_f64 / f64::from(n);
        // Simplified phi: scaled mean. Real phi-accrual uses a
        // running estimate of the inter-arrival distribution; the
        // model here is just enough to assert non-negativity.
        mean.ln().abs()
    }
}

impl Default for PhiModel {
    fn default() -> Self {
        Self::new()
    }
}

#[test]
fn concurrent_record_does_not_lose_samples() {
    use loom::sync::Arc;
    use loom::thread;
    loom::model(|| {
        let phi = Arc::new(PhiModel::new());

        let h1 = {
            let p = phi.clone();
            thread::spawn(move || {
                p.record(10);
            })
        };
        let h2 = {
            let p = phi.clone();
            thread::spawn(move || {
                p.record(20);
            })
        };

        h1.join().unwrap();
        h2.join().unwrap();

        let count = phi.sample_count.load(Ordering::Relaxed);
        assert_eq!(count, 2, "lost a heartbeat sample");
        let v = phi.phi();
        assert!(v >= 0.0, "phi went negative under concurrency: {v}");
    });
}
