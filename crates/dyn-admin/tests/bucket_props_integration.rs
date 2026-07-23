//! End-to-end integration tests for the dyn-admin `bucket-props`
//! subcommands.
//!
//! Each test spins up a real `dyniak::serve_pbc_with_routing`
//! listener bound to a random localhost port with a populated
//! `BucketPropsRegistry`, drives the relevant `dyn-admin` subcommand
//! via `assert_cmd`, and asserts both the exit status and the
//! post-call registry state.

use std::sync::Arc;
use std::time::Duration;

use assert_cmd::Command;
use predicates::prelude::*;
use tokio::net::TcpListener;

use dyniak::datatypes::keyfun::KeyFun;
use dyniak::replication::{RingPoint, RingView};
use dyniak::router::BucketRouter;
use dyniak::{
    serve_pbc_with_routing, BucketProps, BucketPropsRegistry, PeerOp, PeerOutbound,
    ReplicationStrategy, RoutingHooks,
};
use dynomite::cluster::admin_rpc::{ClusterAdmin, NoopClusterAdmin};
use dynomite::embed::hooks::BoxFuture;
use dynomite::embed::{Datastore, MemoryDatastore};
use dynomite::hashkit::HashType;

/// A `PeerOutbound` that drops every dispatch on the floor. The
/// `bucket-props` command path never touches it (the property
/// handlers do not fan out to peers), so the `BucketPropsRegistry`
/// is the only routing hook the test cares about.
#[derive(Debug, Default)]
struct NullOutbound;

impl PeerOutbound for NullOutbound {
    fn dispatch(&self, _peer_idx: u32, _op: PeerOp) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

fn make_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("tokio multi-thread runtime")
}

fn make_routing_hooks(registry: Arc<BucketPropsRegistry>) -> RoutingHooks {
    // A two-point ring is enough for the bucket-props handler;
    // the points only matter for `route(...)` calls, which the
    // bucket-props path does not make.
    let pts = vec![
        RingPoint::new(0, 0, "dc1", "r1"),
        RingPoint::new(u64::MAX / 2, 1, "dc1", "r1"),
    ];
    let ring = Arc::new(RingView::new(pts));
    let router = Arc::new(BucketRouter::new(registry, ring, HashType::Murmur3X64_64));
    RoutingHooks {
        router,
        outbound: Arc::new(NullOutbound),
        local_actor: dyniak::datatypes::ActorId::new("dc1", "local"),
    }
}

fn spawn_server(
    runtime: &tokio::runtime::Runtime,
    registry: Arc<BucketPropsRegistry>,
) -> (String, tokio::task::JoinHandle<()>) {
    runtime.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let admin: Arc<dyn ClusterAdmin> = Arc::new(NoopClusterAdmin);
        let hooks = make_routing_hooks(registry);
        let task = tokio::spawn(async move {
            let _ = serve_pbc_with_routing(listener, ds, admin, hooks).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (format!("127.0.0.1:{}", addr.port()), task)
    })
}

#[test]
fn bucket_props_get_unknown_bucket_returns_defaults() {
    let runtime = make_runtime();
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    let (node, server) = spawn_server(&runtime, registry);

    let assertion = Command::cargo_bin("dyn-admin")
        .expect("binary")
        .args([
            "bucket-props",
            "get",
            "unknown_bucket",
            "--node",
            &node,
            "--json",
        ])
        .assert()
        .success();
    let stdout = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(v["node"], node);
    assert_eq!(v["bucket"], "unknown_bucket");
    // A bucket the registry has never seen reports the Riak-mode
    // defaults: n_val 3, keyfun std, replication-strategy
    // successors.
    assert_eq!(v["props"]["n_val"], 3);
    assert_eq!(v["props"]["keyfun"], "std");
    assert_eq!(v["props"]["replication_strategy"], "successors");

    server.abort();
}

#[test]
fn bucket_props_get_returns_registered_overrides() {
    let runtime = make_runtime();
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(
        b"default",
        b"users",
        BucketProps {
            n_val: Some(5),
            keyfun: Some(KeyFun::BucketOnly),
            strategy: Some(ReplicationStrategy::Successors),
            ..BucketProps::default()
        },
    );
    let (node, server) = spawn_server(&runtime, registry);

    let assertion = Command::cargo_bin("dyn-admin")
        .expect("binary")
        .args(["bucket-props", "get", "users", "--node", &node, "--json"])
        .assert()
        .success();
    let stdout = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(v["props"]["n_val"], 5);
    assert_eq!(v["props"]["keyfun"], "bucketonly");
    assert_eq!(v["props"]["replication_strategy"], "successors");

    server.abort();
}

#[test]
fn bucket_props_set_then_get_round_trips_n_val() {
    let runtime = make_runtime();
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    let (node, server) = spawn_server(&runtime, Arc::clone(&registry));

    // Set n_val to 5.
    Command::cargo_bin("dyn-admin")
        .expect("binary")
        .args([
            "bucket-props",
            "set",
            "users",
            "--n-val",
            "5",
            "--node",
            &node,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Updated bucket properties for users",
        ))
        .stdout(predicate::str::contains("n_val: 5"));

    // The registry reflects the new value (the server-side handler
    // wrote through to it).
    let resolved = registry.resolve(b"default", b"users");
    assert_eq!(resolved.effective_n_val(), 5);

    // A subsequent get reports the new value.
    let assertion = Command::cargo_bin("dyn-admin")
        .expect("binary")
        .args(["bucket-props", "get", "users", "--node", &node, "--json"])
        .assert()
        .success();
    let stdout = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(v["props"]["n_val"], 5);

    server.abort();
}

#[test]
fn bucket_props_set_with_no_overrides_fails() {
    // No flags supplied -- the client refuses to send a no-op
    // update. The error is rendered on stderr; the binary exits
    // non-zero without ever opening a socket.
    Command::cargo_bin("dyn-admin")
        .expect("binary")
        .args(["bucket-props", "set", "users", "--node", "127.0.0.1:1"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("requires at least one of"));
}

#[test]
fn bucket_props_set_keyfun_and_replication_strategy() {
    let runtime = make_runtime();
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    let (node, server) = spawn_server(&runtime, Arc::clone(&registry));

    Command::cargo_bin("dyn-admin")
        .expect("binary")
        .args([
            "bucket-props",
            "set",
            "users",
            "--keyfun",
            "bucketonly",
            "--replication-strategy",
            "successors",
            "--node",
            &node,
            "--json",
        ])
        .assert()
        .success();

    let resolved = registry.resolve(b"default", b"users");
    assert_eq!(resolved.effective_keyfun(), KeyFun::BucketOnly);
    assert_eq!(
        resolved.effective_strategy(),
        ReplicationStrategy::Successors
    );

    server.abort();
}
