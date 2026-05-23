//! OTLP exporter wiring for `dynomited`.
//!
//! Two pillars share this module:
//!
//! * **Distributed tracing** - when
//!   `observability.otlp_traces_endpoint` is set, an
//!   OpenTelemetry `TracerProvider` exports spans over OTLP/gRPC
//!   and a [`tracing_opentelemetry::OpenTelemetryLayer`] is
//!   stacked onto the global subscriber.
//! * **Log appender** - when
//!   `observability.otlp_logs_endpoint` is set, an OpenTelemetry
//!   `LoggerProvider` exports `tracing` events as OTLP log
//!   records and an
//!   [`opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge`]
//!   is stacked onto the same subscriber. The fmt layer keeps
//!   writing to stderr / the configured file in parallel, so
//!   operators see local logs and the collector receives the
//!   structured-log stream.
//!
//! Either or both pillars can be enabled. When at least one is,
//! [`install_global`] composes the layered subscriber:
//!
//! * the caller-provided fmt layer (built via
//!   [`dynomite::core::log::build_logs_layer`]),
//! * an `EnvFilter` honoring `RUST_LOG` (default: from the
//!   verbosity level passed in),
//! * a `tracing_opentelemetry::OpenTelemetryLayer` if traces
//!   are enabled,
//! * an `OpenTelemetryTracingBridge` if logs are enabled.
//!
//! Because `tracing_subscriber` only allows one global default,
//! callers must invoke this exactly once. The fmt layer (and
//! its SIGHUP-reopen wiring) is part of the global subscriber
//! whether OTLP is on or off; the [`dynomite::core::log`] module
//! exposes [`dynomite::core::log::install_logs_only`] for the
//! OTLP-off path. When neither endpoint is set the binary picks
//! that path; otherwise this function picks the layered path
//! while keeping the same fmt layer in the stack.

use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::logs::LoggerProvider;
use opentelemetry_sdk::trace::{Sampler, TracerProvider};
use opentelemetry_sdk::Resource;
use thiserror::Error;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use dynomite::conf::ObservabilityConfig;
use dynomite::core::log::{build_env_filter, LogsLayer, ReopenHandle};

/// Default service name attached to emitted spans and log
/// records when the configuration does not set
/// [`ObservabilityConfig::service_name`].
pub const DEFAULT_SERVICE_NAME: &str = "dynomited";

/// Errors returned by the OTLP wiring helpers.
#[derive(Debug, Error)]
pub enum ObservabilityError {
    /// Building the OTLP gRPC exporter or installing the
    /// subscriber failed.
    #[error("otlp install: {0}")]
    Build(String),
}

/// Guard returned by [`install_global`].
///
/// Holds the live `TracerProvider` and (optionally) the live
/// `LoggerProvider` so the caller can flush their batch
/// processors and shut down the SDK on graceful exit. `Drop`
/// calls [`Self::shutdown`] best-effort; the explicit
/// [`Self::shutdown`] is preferred because it can surface
/// failures via the binary's `tracing::warn!` path.
pub struct ObservabilityGuard {
    tracer: Option<TracerProvider>,
    logger: Option<LoggerProvider>,
}

impl ObservabilityGuard {
    /// Flush any in-flight spans and log records, then tear down
    /// both exporters. Idempotent.
    pub fn shutdown(&mut self) {
        if let Some(provider) = self.tracer.take() {
            if let Err(e) = provider.shutdown() {
                tracing::warn!(error = %e, "otlp tracer shutdown reported an error");
            }
        }
        if let Some(provider) = self.logger.take() {
            if let Err(e) = provider.shutdown() {
                tracing::warn!(error = %e, "otlp logger shutdown reported an error");
            }
        }
    }
}

impl Drop for ObservabilityGuard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Backwards-compatible alias for [`ObservabilityGuard`].
///
/// The original tracing-only milestone exposed `TracerGuard`;
/// the four-pillar refactor merged the trace and log shutdown
/// surfaces into a single guard. The alias keeps any
/// out-of-tree caller building.
pub type TracerGuard = ObservabilityGuard;

/// Returns whether the OTLP traces exporter is enabled by the
/// supplied configuration.
///
/// The check is intentionally small: the trace exporter is
/// considered "on" iff `otlp_traces_endpoint` is `Some` and not
/// the empty string.
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

/// Returns whether the OTLP log appender is enabled by the
/// supplied configuration.
///
/// Mirrors [`otlp_traces_enabled`]: the log appender is "on"
/// iff `otlp_logs_endpoint` is `Some` and not the empty string.
///
/// # Examples
///
/// ```
/// use dynomite::conf::ObservabilityConfig;
/// use dynomited::observability::otlp_logs_enabled;
///
/// assert!(!otlp_logs_enabled(&ObservabilityConfig::default()));
/// ```
pub fn otlp_logs_enabled(cfg: &ObservabilityConfig) -> bool {
    cfg.otlp_logs_endpoint
        .as_deref()
        .is_some_and(|s| !s.is_empty())
}

/// Returns whether at least one OTLP pillar is enabled.
///
/// Callers use it to dispatch between the fmt-only install path
/// ([`dynomite::core::log::install_logs_only`]) and the
/// fmt-plus-OTel path ([`install_global`]).
///
/// # Examples
///
/// ```
/// use dynomite::conf::ObservabilityConfig;
/// use dynomited::observability::otlp_any_enabled;
///
/// assert!(!otlp_any_enabled(&ObservabilityConfig::default()));
/// ```
pub fn otlp_any_enabled(cfg: &ObservabilityConfig) -> bool {
    otlp_traces_enabled(cfg) || otlp_logs_enabled(cfg)
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
/// The function inspects `cfg` and installs:
///
/// * a [`tracing_opentelemetry::OpenTelemetryLayer`] when
///   [`otlp_traces_enabled`] is true,
/// * an
///   [`opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge`]
///   when [`otlp_logs_enabled`] is true.
///
/// Callers are expected to gate this on [`otlp_any_enabled`] and
/// fall back to [`dynomite::core::log::install_logs_only`] when
/// neither endpoint is set.
///
/// # Errors
/// Returns [`ObservabilityError::Build`] when the gRPC exporter
/// for either pillar cannot be constructed, when neither
/// endpoint is set, or when a global subscriber is already
/// installed.
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
///     otlp_logs_endpoint: Some("http://localhost:4317".into()),
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
) -> Result<ObservabilityGuard, ObservabilityError> {
    if !otlp_any_enabled(cfg) {
        return Err(ObservabilityError::Build(
            "at least one of otlp_traces_endpoint / otlp_logs_endpoint is required".into(),
        ));
    }

    let tracer_provider = if otlp_traces_enabled(cfg) {
        Some(init_otlp_tracer(cfg)?)
    } else {
        None
    };
    let logger_provider = init_otlp_logger(cfg)?;

    let env = build_env_filter(verbosity);

    // Collect every active layer behind the same boxed-trait
    // shape. A `Vec<Box<dyn Layer<Registry>>>` itself implements
    // `Layer<Registry>`, so we add it as a single composite
    // layer on top of the empty registry. This sidesteps the
    // trait-object stacking limit (a `Box<dyn Layer<Registry>>`
    // cannot be stacked on top of another layer because the
    // underlying subscriber type changes from `Registry` to
    // `Layered<...>`).
    let mut layers: Vec<LogsLayer> = Vec::with_capacity(3);
    layers.push(fmt_layer);
    if let Some(provider) = tracer_provider.as_ref() {
        let tracer = provider.tracer("dynomite");
        layers.push(Box::new(tracing_opentelemetry::layer().with_tracer(tracer)));
    }
    if let Some(provider) = logger_provider.as_ref() {
        layers.push(Box::new(OpenTelemetryTracingBridge::new(provider)));
    }

    tracing_subscriber::registry()
        .with(layers)
        .with(env)
        .try_init()
        .map_err(|e| ObservabilityError::Build(format!("set_global_default: {e}")))?;

    Ok(ObservabilityGuard {
        tracer: tracer_provider,
        logger: logger_provider,
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
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ObservabilityError::Build("otlp_traces_endpoint is required".into()))?;
    build_tracer_provider(endpoint, cfg)
}

/// Build (but do not install) an OTLP `LoggerProvider` for tests
/// or for callers that own their own subscriber stack.
///
/// Returns `Ok(None)` when `otlp_logs_endpoint` is unset or
/// empty, mirroring the "feature off by default" semantics of
/// the rest of the observability config.
///
/// Must be called from a tokio runtime context for the same
/// reason [`init_otlp_tracer`] is.
///
/// # Errors
/// Returns [`ObservabilityError::Build`] when the gRPC log
/// exporter cannot be constructed.
///
/// # Examples
///
/// ```
/// use dynomite::conf::ObservabilityConfig;
/// use dynomited::observability::init_otlp_logger;
///
/// // Default config disables the log appender.
/// assert!(init_otlp_logger(&ObservabilityConfig::default())
///     .expect("ok")
///     .is_none());
/// ```
pub fn init_otlp_logger(
    cfg: &ObservabilityConfig,
) -> Result<Option<LoggerProvider>, ObservabilityError> {
    let Some(endpoint) = cfg.otlp_logs_endpoint.as_deref().filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    build_logger_provider(endpoint, cfg).map(Some)
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

    let resource = build_resource(cfg);

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

fn build_logger_provider(
    endpoint: &str,
    cfg: &ObservabilityConfig,
) -> Result<LoggerProvider, ObservabilityError> {
    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .with_timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| ObservabilityError::Build(format!("build log exporter: {e}")))?;

    let resource = build_resource(cfg);

    let provider = LoggerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(resource)
        .build();
    Ok(provider)
}

fn build_resource(cfg: &ObservabilityConfig) -> Resource {
    let service_name = cfg
        .service_name
        .clone()
        .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_string());
    Resource::new(vec![KeyValue::new("service.name", service_name)])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn otlp_traces_enabled_returns_false_for_default_config() {
        let cfg = ObservabilityConfig::default();
        assert!(!otlp_traces_enabled(&cfg));
        assert!(!otlp_logs_enabled(&cfg));
        assert!(!otlp_any_enabled(&cfg));
    }

    #[test]
    fn otlp_traces_enabled_returns_false_for_empty_endpoint_string() {
        let cfg = ObservabilityConfig {
            otlp_traces_endpoint: Some(String::new()),
            otlp_logs_endpoint: Some(String::new()),
            service_name: None,
            traces_sampling: None,
        };
        assert!(!otlp_traces_enabled(&cfg));
        assert!(!otlp_logs_enabled(&cfg));
        assert!(!otlp_any_enabled(&cfg));
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
        assert!(!otlp_logs_enabled(&cfg));
        assert!(otlp_any_enabled(&cfg));
    }

    #[test]
    fn otlp_logs_enabled_returns_true_for_set_endpoint() {
        let cfg = ObservabilityConfig {
            otlp_traces_endpoint: None,
            otlp_logs_endpoint: Some("http://localhost:4317".into()),
            service_name: None,
            traces_sampling: None,
        };
        assert!(!otlp_traces_enabled(&cfg));
        assert!(otlp_logs_enabled(&cfg));
        assert!(otlp_any_enabled(&cfg));
    }

    #[test]
    fn init_otlp_tracer_requires_endpoint() {
        let cfg = ObservabilityConfig::default();
        let err = init_otlp_tracer(&cfg).expect_err("should fail without endpoint");
        let msg = format!("{err}");
        assert!(msg.contains("otlp_traces_endpoint"), "msg = {msg}");
    }

    #[test]
    fn init_otlp_logger_with_no_endpoint_returns_none() {
        let cfg = ObservabilityConfig::default();
        let provider = init_otlp_logger(&cfg).expect("ok");
        assert!(provider.is_none());
    }

    #[test]
    fn init_otlp_logger_with_empty_endpoint_returns_none() {
        let cfg = ObservabilityConfig {
            otlp_traces_endpoint: None,
            otlp_logs_endpoint: Some(String::new()),
            service_name: None,
            traces_sampling: None,
        };
        let provider = init_otlp_logger(&cfg).expect("ok");
        assert!(provider.is_none());
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
            otlp_logs_endpoint: Some("http://127.0.0.1:4317".into()),
            service_name: Some("dynomited-test".into()),
            traces_sampling: Some(0.5),
        };
        let provider = init_otlp_tracer(&cfg).expect("provider");
        let _ = provider.shutdown();
        let log_provider = init_otlp_logger(&cfg).expect("ok").expect("provider");
        let _ = log_provider.shutdown();
    }
}
