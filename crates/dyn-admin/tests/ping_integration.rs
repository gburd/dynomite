//! End-to-end smoke test for `dyn-admin ping`.
//!
//! Spins up [`dyn_riak::serve_pbc`] over a `tokio::net::TcpListener`
//! bound to a random localhost port, drives a real `dyn-admin ping`
//! invocation via [`assert_cmd`] against the listener, and asserts
//! that the binary exits with status 0 and prints `PONG`.
//!
//! The test deliberately avoids any HTTP plumbing: `ping` is the
//! only subcommand that lives entirely on the PBC surface, so a
//! bare `MemoryDatastore` is enough.

use std::sync::Arc;
use std::time::Duration;

use assert_cmd::Command;
use predicates::prelude::*;
use tokio::net::TcpListener;

use dyn_riak::serve_pbc;
use dynomite::embed::{Datastore, MemoryDatastore};

fn make_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("tokio multi-thread runtime")
}

#[test]
fn ping_round_trip_against_real_pbc_listener() {
    let runtime = make_runtime();

    // Bind in the runtime so the listener uses the runtime's reactor.
    let (addr, server_task, _shutdown) = runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let task = tokio::spawn(async move {
            // serve_pbc runs forever; the test aborts the task on drop.
            let _ = serve_pbc(listener, ds).await;
        });
        // Yield once so the accept loop is parked on accept() before
        // the CLI tries to connect from a separate process.
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, task, ())
    });

    let node = format!("127.0.0.1:{}", addr.port());

    Command::cargo_bin("dyn-admin")
        .expect("locate dyn-admin binary")
        .args(["ping", "--node", &node])
        .assert()
        .success()
        .stdout(predicate::str::contains("PONG"))
        .stdout(predicate::str::contains(node.as_str()));

    server_task.abort();
}

#[test]
fn ping_json_output_round_trips() {
    let runtime = make_runtime();

    let (addr, server_task) = runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let task = tokio::spawn(async move {
            let _ = serve_pbc(listener, ds).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, task)
    });

    let node = format!("127.0.0.1:{}", addr.port());

    let assertion = Command::cargo_bin("dyn-admin")
        .expect("locate dyn-admin binary")
        .args(["ping", "--node", &node, "--json"])
        .assert()
        .success();
    let stdout = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("json parse");
    assert_eq!(parsed["reply"], "PONG");
    assert_eq!(parsed["node"], node);
    assert!(parsed["rtt_us"].is_number());

    server_task.abort();
}

#[test]
fn ping_unreachable_node_exits_nonzero() {
    // 127.0.0.1:1 is well-known unused; connect should be refused.
    Command::cargo_bin("dyn-admin")
        .expect("locate dyn-admin binary")
        .args(["ping", "--node", "127.0.0.1:1"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("dyn-admin:"));
}
