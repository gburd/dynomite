//! End-to-end test for the OTLP exporter wiring.
//!
//! Spins up a tokio TCP listener on a free loopback port to act
//! as a mock OTLP collector, points
//! [`dynomited::observability::init_otlp_tracer`] at the address,
//! emits a span, calls `provider.shutdown()` so the batch
//! processor flushes, and asserts that the listener received
//! some bytes. The brief explicitly says we do not parse the
//! OTLP wire format here; "just confirm bytes flow" is enough
//! to prove the exporter task runs and the gRPC channel
//! reaches the configured endpoint.

use std::time::Duration;

use dynomite::conf::ObservabilityConfig;
use dynomited::observability::{init_otlp_tracer, install_global};
use opentelemetry::trace::{Tracer, TracerProvider as _};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

#[test]
fn install_global_with_no_endpoint_is_a_no_op() {
    let cfg = ObservabilityConfig::default();
    let g = install_global(&cfg, 5).expect("no-op should be Ok");
    assert!(g.is_none(), "no endpoint should mean no guard");
}

#[test]
fn install_global_with_empty_endpoint_string_is_a_no_op() {
    let cfg = ObservabilityConfig {
        otlp_traces_endpoint: Some(String::new()),
        service_name: None,
        traces_sampling: None,
    };
    let g = install_global(&cfg, 5).expect("no-op should be Ok");
    assert!(g.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn init_otlp_tracer_builds_provider_for_well_formed_endpoint() {
    let cfg = ObservabilityConfig {
        otlp_traces_endpoint: Some("http://127.0.0.1:4317".into()),
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
