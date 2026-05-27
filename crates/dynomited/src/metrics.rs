//! Lazy-registered Prometheus metric families owned by the
//! `dynomited` binary.
//!
//! The engine exports its own metric families through
//! [`dynomite::runtime::metrics`]; this module is reserved for
//! counters and histograms that only make sense at the binary
//! layer (currently: backend reconnect attempts emitted from the
//! supervisor in [`crate::server`]).
//!
//! # Examples
//!
//! ```
//! use dynomited::metrics::backend_reconnect;
//! // Touching the family registers it against the default
//! // Prometheus registry exactly once.
//! backend_reconnect()
//!     .with_label_values(&["127.0.0.1:6379", "parse"])
//!     .inc();
//! ```
use std::sync::OnceLock;

use prometheus::{IntCounterVec, Opts};

static BACKEND_RECONNECT: OnceLock<IntCounterVec> = OnceLock::new();

/// Counter family `backend_reconnect_total{backend, reason}`.
///
/// Incremented every time the backend supervisor tears down the
/// current outbound connection and schedules a reconnect. The
/// `reason` label is one of:
///
/// * `connect_refused` - TCP `connect(2)` returned an error.
/// * `connect_timeout` - the 5 s connect timeout fired.
/// * `auth_failed` - the optional Redis `AUTH` handshake was
///   rejected.
/// * `parse` - the running connection's parser returned an
///   irrecoverable error (the pass-6 chaos symptom).
/// * `io` - a read or write returned an `io::Error` mid-flight.
/// * `closed` - peer closed the connection cleanly.
/// * `other` - any other [`dynomite::net::NetError`] variant.
///
/// The family is registered against
/// [`prometheus::default_registry`] on the first call and reused
/// across the process lifetime.
#[must_use]
pub fn backend_reconnect() -> &'static IntCounterVec {
    BACKEND_RECONNECT.get_or_init(|| {
        let opts = Opts::new(
            "backend_reconnect_total",
            "Number of backend reconnect attempts the supervisor scheduled, by reason",
        );
        let cv = IntCounterVec::new(opts, &["backend", "reason"])
            .expect("invariant: backend_reconnect_total opts are valid");
        // A previous test in the same process may have registered
        // an identically-named family; treat that as success and
        // reuse the locally-built handle. The OnceLock guarantees
        // we only ever build one handle per process.
        let _ = prometheus::default_registry().register(Box::new(cv.clone()));
        cv
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_is_stable_across_calls() {
        let a = backend_reconnect();
        let b = backend_reconnect();
        assert!(std::ptr::eq(a, b));
    }

    #[test]
    fn increment_is_observed() {
        let cv = backend_reconnect();
        let before = cv.with_label_values(&["test:0", "parse"]).get();
        cv.with_label_values(&["test:0", "parse"]).inc();
        let after = cv.with_label_values(&["test:0", "parse"]).get();
        assert_eq!(after, before + 1);
    }
}
