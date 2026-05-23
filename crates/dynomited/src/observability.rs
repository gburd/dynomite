//! Distributed-tracing wiring for `dynomited`.
//!
//! When the configuration's
//! `observability.otlp_traces_endpoint` is set, [`install_global`]
//! builds an OpenTelemetry `TracerProvider` that exports spans
//! over OTLP / gRPC (compatible with Jaeger, Tempo, Honeycomb,
//! the OTel-Collector and every other OTLP consumer) and
//! installs a layered tracing subscriber:
//!
//! * the caller-provided fmt layer (built via
//!   [`dynomite::core::log::build_logs_layer`]),
//! * an `EnvFilter` honoring `RUST_LOG` (default: from the
//!   verbosity level passed in),
//! * a `tracing_opentelemetry::OpenTelemetryLayer` fanning every
//!   span to the OTLP collector.
//!
//! Because `tracing_subscriber` only allows one global default,
//! callers must invoke this exactly once. The fmt layer (and
//! its SIGHUP-reopen wiring) is part of the global subscriber
//! whether OTLP is on or off; the [`dynomite::core::log`] module
//! exposes [`dynomite::core::log::install_logs_only`] for the
//! OTLP-off path. When the endpoint is unset the binary picks
//! that path; when the endpoint is set this function picks the
//! layered (fmt + EnvFilter + OTel) path while keeping the same
//! fmt layer in the stack.

use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{Sampler, TracerProvider};
use opentelemetry_sdk::Resource;
use thiserror::Error;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use dynomite::conf::ObservabilityConfig;
use dynomite::core::log::{build_env_filter, LogsLayer, ReopenHandle};

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

/// Returns whether the OTLP traces exporter is enabled by the
/// supplied configuration.
///
/// The check is intentionally small: the trace exporter is
/// considered "on" iff `otlp_traces_endpoint` is `Some` and not
/// the empty string. Callers use it to dispatch between the
/// fmt-only install path
/// ([`dynomite::core::log::install_logs_only`]) and the
/// fmt-plus-OTel path ([`install_global`]).
///
/// # Examples
///
/// ```
/// use dynomite::conf::ObservabilityConfig;
/// use dynomited::observability::otlp_traces_enabled;
///
/// assert!(!otlp_traces_enabled(&ObservabilityConfig::default()));
/// ```
pub fn otlp_traces_enabled(cfg: &ObservabilityConfig) -> bool {
    cfg.otlp_traces_endpoint
        .as_deref()
        .is_some_and(|s| !s.is_empty())
}

/// Install the layered fmt + EnvFilter + OTel global tracing
/// subscriber.
///
/// `fmt_layer` is the boxed fmt layer built by
/// [`dynomite::core::log::build_logs_layer`]; the `reopen` token
/// proves the SIGHUP-reopen writer state has been initialised.
/// Both are consumed: the layer is moved into the registry and
/// the token is dropped once the subscriber is installed.
///
/// Callers are expected to gate this on
/// [`otlp_traces_enabled`] and fall back to
/// [`dynomite::core::log::install_logs_only`] when the endpoint
/// is unset.
///
/// # Errors
/// Returns [`ObservabilityError::Build`] when the gRPC exporter
/// cannot be constructed, when the configured endpoint is
/// missing, or when a global subscriber is already installed.
///
/// # Examples
///
/// ```no_run
/// use dynomite::conf::ObservabilityConfig;
/// use dynomite::core::log::{build_logs_layer, LogConfig, LogFormat, LOG_NOTICE};
/// use dynomited::observability::install_global;
///
/// let log_cfg = LogConfig::new(LOG_NOTICE, None, LogFormat::Default);
/// let (fmt_layer, reopen) = build_logs_layer(&log_cfg).expect("layer");
/// let obs = ObservabilityConfig {
///     otlp_traces_endpoint: Some("http://localhost:4317".into()),
///     otlp_logs_endpoint: None,
///     service_name: Some("my-service".into()),
///     traces_sampling: Some(1.0),
/// };
/// let _guard = install_global(&obs, LOG_NOTICE, fmt_layer, reopen)
///     .expect("install");
/// ```
pub fn install_global(
    cfg: &ObservabilityConfig,
    verbosity: u8,
    fmt_layer: LogsLayer,
    _reopen: ReopenHandle,
) -> Result<TracerGuard, ObservabilityError> {
    if !otlp_traces_enabled(cfg) {
        return Err(ObservabilityError::Build(
            "otlp_traces_endpoint is required".into(),
        ));
    }
    let provider = init_otlp_tracer(cfg)?;
    let tracer = provider.tracer("dynomite");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let env = build_env_filter(verbosity);

    let registry = tracing_subscriber::registry()
        .with(fmt_layer)
        .with(otel_layer)
        .with(env);
    registry
        .try_init()
        .map_err(|e| ObservabilityError::Build(format!("set_global_default: {e}")))?;
    Ok(TracerGuard {
        provider: Some(provider),
    })
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
    fn otlp_traces_enabled_returns_false_for_default_config() {
        let cfg = ObservabilityConfig::default();
        assert!(!otlp_traces_enabled(&cfg));
    }

    #[test]
    fn otlp_traces_enabled_returns_false_for_empty_endpoint_string() {
        let cfg = ObservabilityConfig {
            otlp_traces_endpoint: Some(String::new()),
            otlp_logs_endpoint: None,
            service_name: None,
            traces_sampling: None,
        };
        assert!(!otlp_traces_enabled(&cfg));
    }

    #[test]
    fn otlp_traces_enabled_returns_true_for_set_endpoint() {
        let cfg = ObservabilityConfig {
            otlp_traces_endpoint: Some("http://localhost:4317".into()),
            otlp_logs_endpoint: None,
            service_name: None,
            traces_sampling: None,
        };
        assert!(otlp_traces_enabled(&cfg));
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
