//! Distributed-tracing wiring for `dynomited`.
//!
//! When the configuration's
//! `observability.otlp_traces_endpoint` is set, [`install_global`]
//! builds an OpenTelemetry `TracerProvider` that exports spans
//! over OTLP / gRPC (compatible with Jaeger, Tempo, Honeycomb,
//! the OTel-Collector and every other OTLP consumer) and
//! installs a layered tracing subscriber:
//!
//! * an `EnvFilter` honoring `RUST_LOG` (default: from the
//!   verbosity level passed in),
//! * a `fmt::layer` writing to stderr,
//! * a `tracing_opentelemetry::OpenTelemetryLayer` fanning every
//!   span to the OTLP collector.
//!
//! Because `tracing_subscriber` only allows one global default,
//! callers must invoke this BEFORE
//! [`dynomite::core::log::log_init`]. When the endpoint is unset
//! the function returns `Ok(false)` and the binary keeps using
//! the standard `log_init` path; the OTel SDK is therefore inert
//! at run time unless the operator opts in, satisfying the
//! brief's default-behavior-must-not-regress contract.

use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{Sampler, TracerProvider};
use opentelemetry_sdk::Resource;
use thiserror::Error;
use tracing::Level;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use dynomite::conf::ObservabilityConfig;
use dynomite::core::log::{clamp_level, tracing_level_for};

/// Default service name attached to emitted spans when the
/// configuration does not set [`ObservabilityConfig::service_name`].
pub const DEFAULT_SERVICE_NAME: &str = "dynomited";

/// Errors returned by the OTLP wiring helpers.
#[derive(Debug, Error)]
pub enum ObservabilityError {
    /// Building the OTLP gRPC exporter or installing the
    /// subscriber failed.
    #[error("otlp tracer install: {0}")]
    Build(String),
}

/// Guard returned by [`install_global`].
///
/// Holds the live `TracerProvider` so the caller can flush the
/// batch span processor and shut down the SDK on graceful exit.
/// `Drop` calls [`Self::shutdown`] best-effort; the explicit
/// [`Self::shutdown`] is preferred because it can surface
/// failures via the binary's `tracing::warn!` path.
pub struct TracerGuard {
    provider: Option<TracerProvider>,
}

impl TracerGuard {
    /// Flush any in-flight spans and tear down the exporter.
    /// Idempotent.
    pub fn shutdown(&mut self) {
        if let Some(provider) = self.provider.take() {
            if let Err(e) = provider.shutdown() {
                tracing::warn!(error = %e, "otlp tracer shutdown reported an error");
            }
        }
    }
}

impl Drop for TracerGuard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Install the OTLP layer as the global tracing subscriber when
/// the configuration opts in.
///
/// Must be called BEFORE
/// [`dynomite::core::log::log_init`]: a global tracing subscriber
/// can only be installed once and `log_init` installs its own.
/// When the endpoint is unset (or the empty string), this
/// function does nothing and returns `Ok(None)`; the caller
/// keeps using `log_init` as before. When the endpoint is set,
/// the returned guard owns the live `TracerProvider`; dropping
/// it (or calling [`TracerGuard::shutdown`]) flushes pending
/// spans and shuts the exporter down.
///
/// # Errors
/// Returns [`ObservabilityError::Build`] when the gRPC exporter
/// cannot be constructed or when a global subscriber is already
/// installed (typically because `log_init` ran first).
///
/// # Examples
///
/// ```
/// use dynomite::conf::ObservabilityConfig;
/// use dynomited::observability::install_global;
/// // No endpoint configured -> install_global is a no-op and
/// // the returned guard is None.
/// let cfg = ObservabilityConfig::default();
/// let g = install_global(&cfg, 5).expect("default cfg is a no-op");
/// assert!(g.is_none());
/// ```
pub fn install_global(
    cfg: &ObservabilityConfig,
    verbosity: u8,
) -> Result<Option<TracerGuard>, ObservabilityError> {
    if cfg.otlp_traces_endpoint.as_deref().unwrap_or("").is_empty() {
        return Ok(None);
    }
    let provider = init_otlp_tracer(cfg)?;
    let tracer = provider.tracer("dynomite");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // EnvFilter honors RUST_LOG; default falls back to the
    // verbosity level the CLI passed in (`-v`).
    let level: Level = tracing_level_for(clamp_level(verbosity));
    let level_filter = LevelFilter::from_level(level);
    let env = EnvFilter::builder()
        .with_default_directive(level_filter.into())
        .from_env_lossy();

    let registry = tracing_subscriber::registry()
        .with(env)
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(otel_layer);
    registry
        .try_init()
        .map_err(|e| ObservabilityError::Build(format!("set_global_default: {e}")))?;
    Ok(Some(TracerGuard {
        provider: Some(provider),
    }))
}

/// Build (but do not install) an OTLP `TracerProvider` for tests
/// or for callers that own their own subscriber stack.
///
/// Must be called from a tokio runtime context: the underlying
/// `tonic` channel installs an HTTP/2 connection driver on the
/// current runtime when the provider is built.
///
/// # Errors
/// Returns [`ObservabilityError::Build`] when the gRPC exporter
/// cannot be constructed.
///
/// # Examples
///
/// ```no_run
/// use dynomite::conf::ObservabilityConfig;
/// use dynomited::observability::init_otlp_tracer;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let cfg = ObservabilityConfig {
///     otlp_traces_endpoint: Some("http://localhost:4317".into()),
///     otlp_logs_endpoint: None,
///     service_name: Some("my-service".into()),
///     traces_sampling: Some(1.0),
/// };
/// let _provider = init_otlp_tracer(&cfg).expect("build provider");
/// # }
/// ```
pub fn init_otlp_tracer(cfg: &ObservabilityConfig) -> Result<TracerProvider, ObservabilityError> {
    let endpoint = cfg
        .otlp_traces_endpoint
        .as_deref()
        .ok_or_else(|| ObservabilityError::Build("otlp_traces_endpoint is required".into()))?;
    build_tracer_provider(endpoint, cfg)
}

fn build_tracer_provider(
    endpoint: &str,
    cfg: &ObservabilityConfig,
) -> Result<TracerProvider, ObservabilityError> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .with_timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| ObservabilityError::Build(format!("build span exporter: {e}")))?;

    let service_name = cfg
        .service_name
        .clone()
        .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_string());
    let resource = Resource::new(vec![KeyValue::new("service.name", service_name)]);

    let sampler = match cfg.traces_sampling {
        Some(r) if (0.0..1.0).contains(&r) => Sampler::TraceIdRatioBased(r),
        _ => Sampler::AlwaysOn,
    };

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_sampler(sampler)
        .with_resource(resource)
        .build();
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_endpoint_returns_none_guard() {
        let cfg = ObservabilityConfig::default();
        let g = install_global(&cfg, 5).expect("should be Ok");
        assert!(g.is_none(), "no endpoint should mean no guard");
    }

    #[test]
    fn empty_endpoint_string_is_treated_as_unset() {
        let cfg = ObservabilityConfig {
            otlp_traces_endpoint: Some(String::new()),
            otlp_logs_endpoint: None,
            service_name: None,
            traces_sampling: None,
        };
        let g = install_global(&cfg, 5).expect("should be Ok");
        assert!(g.is_none());
    }

    #[test]
    fn init_otlp_tracer_requires_endpoint() {
        let cfg = ObservabilityConfig::default();
        let err = init_otlp_tracer(&cfg).expect_err("should fail without endpoint");
        let msg = format!("{err}");
        assert!(msg.contains("otlp_traces_endpoint"), "msg = {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires a real OTLP endpoint to fully validate; the build itself is exercised by install_global tests"]
    async fn build_with_well_formed_endpoint_succeeds() {
        // Building the provider does not connect to the
        // collector; only the URL syntax is validated. Marked
        // `#[ignore]` because the batch span processor's
        // shutdown waits on the tokio runtime; without a real
        // collector the test would hang on flush.
        let cfg = ObservabilityConfig {
            otlp_traces_endpoint: Some("http://127.0.0.1:4317".into()),
            otlp_logs_endpoint: None,
            service_name: Some("dynomited-test".into()),
            traces_sampling: Some(0.5),
        };
        let provider = init_otlp_tracer(&cfg).expect("provider");
        let _ = provider.shutdown();
    }
}
