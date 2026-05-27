//! Lazy registration of the runtime back-pressure metric families.
//!
//! The two families are registered against the process-wide
//! [`prometheus::default_registry`] on first use. Subsequent calls
//! return the same handle, so labelled child metrics stay aggregated
//! across all sidejobs and throttles in the binary.

use std::sync::OnceLock;

use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts};

/// Histogram bucket boundaries for `throttle_wait_seconds`. Spans
/// 1 ms through 10 s in roughly half-decade steps; matches the
/// shape used by the existing Dynomite latency histograms.
const THROTTLE_WAIT_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

static SIDEJOB_OVERLOAD: OnceLock<IntCounterVec> = OnceLock::new();
static THROTTLE_WAIT: OnceLock<HistogramVec> = OnceLock::new();

/// Returns the global `sidejob_overload_total` counter family,
/// registering it against the default registry on the first call.
pub(super) fn sidejob_overload() -> &'static IntCounterVec {
    SIDEJOB_OVERLOAD.get_or_init(|| {
        let opts = Opts::new(
            "sidejob_overload_total",
            "Number of submits rejected because the sidejob mailbox was full",
        );
        let cv = IntCounterVec::new(opts, &["name"])
            .expect("invariant: sidejob_overload_total opts are valid");
        // A previous test in the same process may have registered an
        // identically-named family; treat that as success and reuse
        // the locally-built handle. The `OnceLock` guarantees we only
        // ever build one handle per process.
        let _ = prometheus::default_registry().register(Box::new(cv.clone()));
        cv
    })
}

/// Returns the global `throttle_wait_seconds` histogram family,
/// registering it against the default registry on the first call.
pub(super) fn throttle_wait() -> &'static HistogramVec {
    THROTTLE_WAIT.get_or_init(|| {
        let opts = HistogramOpts::new(
            "throttle_wait_seconds",
            "Seconds spent blocked in Throttle::acquire waiting for tokens",
        )
        .buckets(THROTTLE_WAIT_BUCKETS.to_vec());
        let hv = HistogramVec::new(opts, &["queue"])
            .expect("invariant: throttle_wait_seconds opts are valid");
        let _ = prometheus::default_registry().register(Box::new(hv.clone()));
        hv
    })
}
