//! Headline end-to-end test: compile REAL Rust to
//! `wasm32-unknown-unknown` and run it as a live custom keyfun (and
//! as a MapReduce map phase) at runtime.
//!
//! No hand-written WAT here. A small operator-style fixture crate
//! is compiled with `cargo build --release --target
//! wasm32-unknown-unknown`, and the resulting `.wasm` is registered
//! into the keyfun / module store and exercised through the
//! production routing and MapReduce paths.
//!
//! The test is gated: if the `wasm32-unknown-unknown` target is not
//! installed, or `cargo` cannot build the fixture, the test prints
//! a skip notice and returns instead of failing. On the project's
//! pinned toolchain (`rust-toolchain.toml` lists
//! `targets = ["wasm32-unknown-unknown"]`) the build always
//! succeeds, so the test runs for real in CI.
//!
//! Build scratch goes under `/scratch` via `CARGO_TARGET_DIR` so
//! the fixture build never pollutes the workspace `target/` and the
//! fixture crate stays out of the main build graph (it is excluded
//! from the workspace and carries its own empty `[workspace]`
//! table).

#![cfg(feature = "wasm")]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use dyniak::datatypes::keyfun::KeyFun;
use dyniak::datatypes::keyfun_wasm::WasmKeyfunStore;
use dyniak::mapreduce::{
    builtins::default_registry, run_job_with_wasm, Inputs, KeyDatum, MapReduceJob, Phase, WasmHook,
    WasmModuleStore,
};
use dyniak::replication::{ReplicationStrategy, RingPoint, RingView};
use dyniak::router::BucketRouter;
use dyniak::{BucketProps, BucketPropsRegistry};
use dynomite::hashkit::{hash64, HashType};

/// Workspace-relative path to a fixture crate's manifest.
fn fixture_manifest(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
        .join("Cargo.toml")
}

/// Build a fixture crate to `wasm32-unknown-unknown` and return the
/// path to the produced `.wasm`, or `None` if the build cannot run
/// (target missing / cargo failure) so the caller can skip.
///
/// `crate_lib` is the crate name with `-` mapped to `_`, which is
/// the `.wasm` file stem cargo emits for a `cdylib`.
fn build_fixture(name: &str, crate_lib: &str) -> Option<PathBuf> {
    let manifest = fixture_manifest(name);
    if !manifest.exists() {
        eprintln!("SKIP: fixture manifest missing at {}", manifest.display());
        return None;
    }
    // Quick capability probe: is the wasm target installed?
    let probe = Command::new(env!("CARGO"))
        .args(["build", "--release", "--target", "wasm32-unknown-unknown"])
        .arg("--manifest-path")
        .arg(&manifest)
        .env("CARGO_TARGET_DIR", scratch_target(name))
        .output();
    let out = match probe {
        Ok(o) => o,
        Err(e) => {
            eprintln!("SKIP: could not invoke cargo to build {name}: {e}");
            return None;
        }
    };
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("wasm32-unknown-unknown")
            && (stderr.contains("target may not be installed")
                || stderr.contains("is not installed")
                || stderr.contains("can't find crate for `core`"))
        {
            eprintln!("SKIP: wasm32-unknown-unknown target unavailable:\n{stderr}");
            return None;
        }
        panic!("fixture build failed for {name}:\n{stderr}");
    }
    let wasm = scratch_target(name)
        .join("wasm32-unknown-unknown/release")
        .join(format!("{crate_lib}.wasm"));
    if !wasm.exists() {
        eprintln!("SKIP: built ok but no .wasm at {}", wasm.display());
        return None;
    }
    Some(wasm)
}

/// Per-fixture scratch target dir under `/scratch` (falling back to
/// the system temp dir if `/scratch` is absent).
fn scratch_target(name: &str) -> PathBuf {
    let base = if Path::new("/scratch").is_dir() {
        PathBuf::from("/scratch")
    } else {
        std::env::temp_dir()
    };
    base.join("dyniak-wasm-fixtures").join(name)
}

/// Build a 5-peer ring spanning the u32 token space.
fn five_peer_ring() -> Arc<RingView> {
    let span = u64::from(u32::MAX);
    let pts: Vec<RingPoint> = (0..5u32)
        .map(|i| RingPoint::new(u64::from(i) * span / 5, i, "dc1", "r1"))
        .collect();
    Arc::new(RingView::new(pts))
}

#[test]
fn real_rust_wasm_custom_keyfun_routes_live_keys() {
    let Some(wasm) = build_fixture("keyfun-reverse", "keyfun_reverse") else {
        return;
    };
    let bytes = std::fs::read(&wasm).expect("read compiled keyfun wasm");

    // Register the compiled-from-Rust module into the keyfun store.
    let store = WasmKeyfunStore::new().expect("keyfun store");
    store.register("reverse", &bytes).expect("register wasm");
    assert!(store.contains("reverse"));

    // A bucket selecting CUSTOM + the registered module, plus a
    // plain Std bucket, in the same registry.
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(
        b"default",
        b"shaped",
        BucketProps {
            keyfun: Some(KeyFun::Custom("reverse".to_string())),
            strategy: Some(ReplicationStrategy::Successors),
            n_val: Some(1),
            custom_keyfun_module: Some("reverse".to_string()),
        },
    );
    registry.set(
        b"default",
        b"plain",
        BucketProps {
            keyfun: Some(KeyFun::Std),
            strategy: Some(ReplicationStrategy::Successors),
            n_val: Some(1),
            ..BucketProps::default()
        },
    );

    let hash = HashType::Murmur;
    let router = BucketRouter::new(registry, five_peer_ring(), hash).with_keyfun_store(store);

    // The fixture rule is `<bucket>:<reversed key>`. Assert the
    // route bytes exactly for several keys, and that the chosen
    // peer matches an independent hash of those exact bytes.
    for (bucket, key, expected) in [
        (&b"shaped"[..], &b"alice"[..], &b"shaped:ecila"[..]),
        (&b"shaped"[..], &b"bob"[..], &b"shaped:bob"[..]),
        (&b"shaped"[..], &b"12345"[..], &b"shaped:54321"[..]),
    ] {
        let decision = router
            .try_route(b"default", bucket, key)
            .expect("custom keyfun routes");
        assert_eq!(
            decision.route_bytes, expected,
            "fixture rule <bucket>:<reversed key> for key {key:?}"
        );
        // The decision's primary peer is the ring lookup of the
        // hash of those exact route bytes.
        let want_hash = hash64(hash, expected);
        assert_eq!(decision.key_hash, want_hash, "hash of route bytes");
    }

    // Prove a Std bucket routes DIFFERENTLY for the same key: its
    // route bytes are `<bucket>/<key>`, not the custom shape, so
    // the resulting hash (and almost surely the peer) differs.
    let custom = router.try_route(b"default", b"shaped", b"alice").unwrap();
    let std_dec = router.try_route(b"default", b"plain", b"alice").unwrap();
    assert_eq!(custom.route_bytes, b"shaped:ecila");
    assert_eq!(std_dec.route_bytes, b"plain/alice");
    assert_ne!(
        custom.key_hash, std_dec.key_hash,
        "custom and std keyfuns must shape different hash inputs"
    );
}

#[test]
fn real_rust_wasm_custom_keyfun_negative_paths() {
    let Some(wasm) = build_fixture("keyfun-reverse", "keyfun_reverse") else {
        return;
    };
    let bytes = std::fs::read(&wasm).expect("read wasm");
    let store = WasmKeyfunStore::new().expect("store");
    store.register("reverse", &bytes).expect("register");

    // Unregistered module id -> clean ModuleNotFound, no panic.
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(
        b"default",
        b"bad",
        BucketProps {
            keyfun: Some(KeyFun::Custom("missing".to_string())),
            strategy: Some(ReplicationStrategy::Successors),
            n_val: Some(1),
            custom_keyfun_module: Some("missing".to_string()),
        },
    );
    let router =
        BucketRouter::new(registry, five_peer_ring(), HashType::Murmur).with_keyfun_store(store);
    let err = router
        .try_route(b"default", b"bad", b"k")
        .expect_err("unregistered module errors cleanly");
    assert!(
        matches!(err, dyniak::datatypes::keyfun::KeyFunError::ModuleNotFound(ref s) if s == "missing"),
        "got {err:?}"
    );
}

#[tokio::test]
async fn real_rust_wasm_mapreduce_phase_doubles_values() {
    let Some(wasm) = build_fixture("mapreduce-double", "mapreduce_double") else {
        return;
    };
    let bytes = std::fs::read(&wasm).expect("read compiled mapreduce wasm");

    let store = WasmModuleStore::new().expect("module store");
    store.register("double", &bytes).expect("register wasm");
    let hook: Arc<dyn WasmHook> = Arc::new(store);

    let job = MapReduceJob {
        inputs: Inputs::KeyData(vec![
            KeyDatum::with_value("b", "k1", serde_json::json!(10)),
            KeyDatum::with_value("b", "k2", serde_json::json!(21)),
            KeyDatum::with_value("b", "k3", serde_json::json!(0)),
        ]),
        phases: vec![Phase::WasmModule {
            module_id: "double".into(),
            fn_name: "apply".into(),
            arg: None,
            keep: true,
        }],
        timeout_ms: None,
    };
    let out = run_job_with_wasm(job, Arc::new(default_registry()), Some(hook))
        .await
        .expect("mapreduce job ok");
    let mut got: Vec<i64> = out
        .into_iter()
        .map(|o| o.value.as_i64().expect("doubled number"))
        .collect();
    got.sort_unstable();
    assert_eq!(got, vec![0, 20, 42], "value * 2 for each input");
}
