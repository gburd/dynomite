//! End-to-end tests for the OTLP exporter wiring.
//!
//! These tests cover three things:
//!
//! 1. The `otlp_traces_enabled` predicate that drives the
//!    OTLP-on / OTLP-off dispatch in `dynomited::main::run_server`.
//! 2. The `init_otlp_tracer` provider builder against a
//!    well-formed endpoint (does not connect).
//! 3. The full `install_global` path co-existing with a
//!    JSON-formatted fmt layer and a working SIGHUP-reopen
//!    handler. The brief explicitly asks for "JSON-shaped log
//!    lines AND the SIGHUP-reopen state is populated"; the
//!    third test rotates a log file under a running global
//!    subscriber and confirms each side of the rotate received
//!    a JSON-formatted line.
//!
//! The "bytes flow to a real OTLP collector" test is `#[ignore]`d
//! because the BatchSpanProcessor's flush path can stall outside
//! CI.

use std::time::Duration;

use dynomite::conf::ObservabilityConfig;
use dynomite::core::log::{build_logs_layer, reopen_on_sighup, LogConfig, LogFormat, LOG_NOTICE};
use dynomited::observability::{init_otlp_tracer, install_global, otlp_traces_enabled};
use opentelemetry::trace::{Tracer, TracerProvider as _};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

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

#[tokio::test(flavor = "multi_thread")]
async fn init_otlp_tracer_builds_provider_for_well_formed_endpoint() {
    let cfg = ObservabilityConfig {
        otlp_traces_endpoint: Some("http://127.0.0.1:4317".into()),
        otlp_logs_endpoint: None,
        service_name: Some("dynomited-test".into()),
        traces_sampling: Some(0.5),
    };
    let provider = init_otlp_tracer(&cfg).expect("provider builds");
    let tracer = provider.tracer("integration");
    {
        let _span = tracer.start("ping");
    }
    // Drain inside the runtime so the batch processor's task
    // gets a chance to run; the test does not require the
    // shutdown to succeed (no real collector is listening).
    let _ = provider.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn install_global_with_otlp_and_json_emits_json_and_sighup_reopens() {
    // This test exercises the previously-impossible combination:
    // OTLP traces enabled AND a JSON fmt layer AND working
    // SIGHUP-driven log rotation. It uses a free TCP port as a
    // place-holder OTLP endpoint - the SDK only validates the URL
    // shape at provider-build time and does not require a live
    // collector when the sampling ratio is 0.0 (no spans get
    // exported through the gRPC channel). The `tracing::info!`
    // events still flow through the fmt layer and land in the
    // configured log file.
    use std::fs;

    let dir = tempfile::tempdir().expect("tempdir");
    let log_path = dir.path().join("dyn.log");

    // A real listener so the OTLP gRPC client has something to
    // connect to even though no spans get exported. Held alive
    // for the duration of the test.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let _accept = tokio::spawn(async move {
        // Best-effort accept loop; we drain anything the SDK
        // sends over so the kernel buffer never blocks.
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                while let Ok(n) = sock.read(&mut buf).await {
                    if n == 0 {
                        return;
                    }
                }
            });
        }
    });

    let log_cfg = LogConfig::new(LOG_NOTICE, Some(log_path.clone()), LogFormat::Json);
    let (fmt_layer, reopen) = build_logs_layer(&log_cfg).expect("build logs layer");

    let obs = ObservabilityConfig {
        otlp_traces_endpoint: Some(format!("http://{addr}")),
        otlp_logs_endpoint: None,
        service_name: Some("dynomited-test".into()),
        // Sampling ratio of 0.0 means TraceIdRatioBased(0.0): the
        // OTel SDK records nothing and the gRPC channel is dormant.
        traces_sampling: Some(0.0),
    };

    let mut guard =
        install_global(&obs, LOG_NOTICE, fmt_layer, reopen).expect("install global subscriber");

    tracing::info!(stage = "before-rotate", "first-line-marker");

    let rotated = dir.path().join("dyn.log.1");
    fs::rename(&log_path, &rotated).expect("rotate file");
    reopen_on_sighup().expect("reopen log");

    tracing::info!(stage = "after-rotate", "second-line-marker");

    // Best-effort flush: tracing's fmt layer writes synchronously
    // inside `on_event`, so the events are already on disk. A
    // small timeout absorbs any last-mile scheduling jitter on
    // overloaded CI hosts.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let rotated_contents = fs::read_to_string(&rotated).expect("read rotated");
    let new_contents = fs::read_to_string(&log_path).expect("read new");

    // 1. Each non-empty line is a self-contained JSON object.
    for (label, contents) in [("rotated", &rotated_contents), ("new", &new_contents)] {
        for line in contents.lines().filter(|l| !l.is_empty()) {
            let v: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("non-JSON line in {label} ({e}): {line:?}"));
            for key in ["timestamp", "level", "target", "fields"] {
                assert!(
                    v.get(key).is_some(),
                    "JSON line missing key {key:?} in {label}: {line}",
                );
            }
        }
    }

    // 2. Each marker landed in exactly one of the files.
    assert!(
        rotated_contents.contains("first-line-marker"),
        "rotated file missing first marker: {rotated_contents:?}",
    );
    assert!(
        !rotated_contents.contains("second-line-marker"),
        "rotated file unexpectedly contained second marker: {rotated_contents:?}",
    );
    assert!(
        new_contents.contains("second-line-marker"),
        "new file missing second marker: {new_contents:?}",
    );
    assert!(
        !new_contents.contains("first-line-marker"),
        "new file unexpectedly contained first marker: {new_contents:?}",
    );

    guard.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow / flaky outside CI: relies on the SDK exporter task draining within 3s"]
async fn otlp_grpc_bytes_reach_mock_listener() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let listener_handle = tokio::spawn(async move {
        let mut total = 0usize;
        let accept = tokio::time::timeout(Duration::from_secs(3), listener.accept()).await;
        let Ok(Ok((mut sock, _))) = accept else {
            return total;
        };
        let mut buf = [0u8; 8192];
        let read_until = async {
            loop {
                match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => total += n,
                }
            }
        };
        let _ = tokio::time::timeout(Duration::from_secs(3), read_until).await;
        total
    });

    let cfg = ObservabilityConfig {
        otlp_traces_endpoint: Some(format!("http://{addr}")),
        otlp_logs_endpoint: None,
        service_name: Some("dynomited-test".into()),
        traces_sampling: Some(1.0),
    };
    let provider = init_otlp_tracer(&cfg).expect("provider");
    let tracer = provider.tracer("integration");
    {
        let _span = tracer.start("integration.test_span");
    }
    // Best-effort flush + tear-down so the batch processor sends
    // through the gRPC channel before the listener exits.
    let _ = provider.shutdown();

    let bytes = listener_handle.await.expect("listener task panicked");
    assert!(
        bytes > 0,
        "expected the OTLP exporter to write bytes to the mock collector; saw {bytes}"
    );
}
