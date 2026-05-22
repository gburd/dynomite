//! Distributed-tracing integration tests for the cluster
//! dispatcher.
//!
//! Drives one request through `ClusterDispatcher::dispatch` with
//! a recording subscriber installed via
//! `tracing::subscriber::with_default` and asserts the expected
//! span names appear in the captured stream. The OTLP exporter
//! itself is exercised in `crates/dynomited/tests/observability.rs`
//! to keep the dependency footprint of this crate's test target
//! minimal.

use std::sync::{Arc, Mutex};

use dynomite::cluster::dispatch::ClusterDispatcher;
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::conf::{DataStore, HashType};
use dynomite::hashkit::DynToken;
use dynomite::msg::{ConsistencyLevel, Msg, MsgType};
use dynomite::net::{Dispatcher, OutboundEnvelope};
use tokio::sync::mpsc;
use tracing::span::{Attributes, Id};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{LookupSpan, Registry};
use tracing_subscriber::Layer;

/// Recording subscriber: collects every span name a child layer
/// observed, in creation order.
#[derive(Default, Clone)]
struct RecordingLayer {
    spans: Arc<Mutex<Vec<String>>>,
}

impl<S> Layer<S> for RecordingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        let mut g = self.spans.lock().unwrap();
        g.push(attrs.metadata().name().to_string());
    }

    fn on_event(&self, _event: &Event<'_>, _ctx: Context<'_, S>) {}

    fn enabled(&self, _metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        true
    }
}

fn pool_config(read: ConsistencyLevel, write: ConsistencyLevel) -> PoolConfig {
    PoolConfig {
        name: "p".into(),
        dc: "dc1".into(),
        rack: "rA".into(),
        data_store: DataStore::Redis,
        hash: HashType::Murmur,
        read_consistency: read,
        write_consistency: write,
        timeout_ms: 5_000,
        server_retry_timeout_ms: 30_000,
        server_failure_limit: 2,
        auto_eject_hosts: false,
        enable_gossip: false,
    }
}

fn local_peer() -> Peer {
    let mut p = Peer::new(
        0,
        PeerEndpoint::tcp("h".into(), 8101),
        "rA".into(),
        "dc1".into(),
        vec![DynToken::from_u32(0)],
        true,
        true,
        false,
    );
    p.set_state(PeerState::Normal, 0);
    p
}

#[test]
fn dispatch_emits_dispatch_plan_span() {
    use tracing_subscriber::layer::SubscriberExt;

    let layer = RecordingLayer::default();
    let captured = layer.spans.clone();
    let subscriber = Registry::default().with(layer);

    tracing::subscriber::with_default(subscriber, || {
        let pool = Arc::new(ServerPool::new(
            pool_config(ConsistencyLevel::DcOne, ConsistencyLevel::DcOne),
            vec![local_peer()],
        ));
        pool.preselect_remote_racks();
        let disp = ClusterDispatcher::new(pool);
        let req = Msg::new(42, MsgType::ReqRedisGet, true);
        let (tx, _rx) = mpsc::channel::<OutboundEnvelope>(8);
        let _ = disp.dispatch(req, tx);
    });

    let names = captured.lock().unwrap().clone();
    assert!(
        names.iter().any(|n| n == "dispatch.plan"),
        "expected dispatch.plan in captured spans, saw: {names:?}"
    );
}

#[test]
fn outbound_request_carries_span_field() {
    // The OutboundRequest envelope must carry a tracing::Span so
    // the receiver task can re-enter the originating client
    // request span.
    let (tx, _rx) = mpsc::channel::<OutboundEnvelope>(1);
    let req = dynomite::net::server::OutboundRequest {
        bytes: b"*1\r\n$4\r\nPING\r\n".to_vec(),
        req_id: 7,
        responder: tx,
        span: tracing::Span::none(),
    };
    assert_eq!(req.req_id, 7);
    let _ = format!("{:?}", req.span);
}

#[test]
fn outbound_envelope_carries_span_field() {
    let env = OutboundEnvelope {
        req_id: 9,
        rsp: Msg::new(9, MsgType::RspRedisStatus, false),
        span: tracing::Span::none(),
    };
    assert_eq!(env.req_id, 9);
    let _ = format!("{:?}", env.span);
}
