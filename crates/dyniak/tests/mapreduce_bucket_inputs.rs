//! Integration tests for whole-bucket MapReduce inputs against a
//! real [`NoxuDatastore`].
//!
//! These tests put N objects into a bucket through the Noxu K/V
//! layer, then run a MapReduce job whose inputs are the bucket name.
//! The executor enumerates the bucket's keys through
//! [`dynomite::embed::Datastore::list_keys_stream`] and seeds the
//! pipeline with one `(bucket, key)` datum per key.
//!
//! Gated on the `noxu` feature; without it the file compiles to an
//! empty module.

#![cfg(feature = "noxu")]

use std::sync::Arc;

use dyniak::datastore::NoxuDatastore;
use dyniak::mapreduce::{
    builtins::default_registry, run_job_full, Inputs, MapReduceJob, Phase, PhaseRegistry,
};
use dynomite::embed::Datastore;
use tempfile::TempDir;

fn registry() -> Arc<PhaseRegistry> {
    Arc::new(default_registry())
}

fn open_store() -> (TempDir, Arc<dyn Datastore>) {
    let dir = TempDir::new().expect("tempdir");
    let noxu = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    let ds: Arc<dyn Datastore> = Arc::new(noxu);
    (dir, ds)
}

#[tokio::test]
async fn bucket_inputs_see_all_keys_and_reduce_aggregates() {
    const N: u64 = 17;
    let dir = TempDir::new().expect("tempdir");
    let store = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    for i in 0..N {
        store
            .put_object(b"docs", format!("k{i}").as_bytes(), b"payload", &[])
            .expect("put");
    }
    let ds: Arc<dyn Datastore> = Arc::new(store);

    let job = MapReduceJob {
        inputs: Inputs::Bucket("docs".into()),
        phases: vec![
            Phase::Map {
                fn_name: "map_identity".into(),
                arg: None,
                keep: false,
            },
            Phase::Reduce {
                fn_name: "reduce_count".into(),
                arg: None,
                keep: true,
            },
        ],
        timeout_ms: None,
    };
    let out = run_job_full(job, registry(), None, Some(ds))
        .await
        .expect("ok");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].value, serde_json::json!(N));
    drop(dir);
}

#[tokio::test]
async fn bucket_inputs_carry_routing_metadata_per_key() {
    let dir = TempDir::new().expect("tempdir");
    let store = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    store
        .put_object(b"users", b"alice", b"v", &[])
        .expect("put");
    store.put_object(b"users", b"bob", b"v", &[]).expect("put");
    // A key in a different bucket must not leak into the scan.
    store.put_object(b"orders", b"o1", b"v", &[]).expect("put");
    let ds: Arc<dyn Datastore> = Arc::new(store);

    let job = MapReduceJob {
        inputs: Inputs::Bucket("users".into()),
        phases: vec![],
        timeout_ms: None,
    };
    let out = run_job_full(job, registry(), None, Some(ds))
        .await
        .expect("ok");
    assert_eq!(out.len(), 2);
    let mut keys: Vec<String> = out
        .iter()
        .map(|o| o.value["key"].as_str().expect("key string").to_string())
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["alice".to_string(), "bob".to_string()]);
    for o in &out {
        assert_eq!(o.value["bucket"], "users");
        assert!(o.value["value"].is_null());
    }
    drop(dir);
}

#[tokio::test]
async fn bucket_inputs_over_empty_bucket_is_not_an_error() {
    let (dir, ds) = open_store();
    let job = MapReduceJob {
        inputs: Inputs::Bucket("never-written".into()),
        phases: vec![Phase::Reduce {
            fn_name: "reduce_count".into(),
            arg: None,
            keep: true,
        }],
        timeout_ms: None,
    };
    let out = run_job_full(job, registry(), None, Some(ds))
        .await
        .expect("empty bucket is ok, not error");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].value, serde_json::json!(0u64));
    drop(dir);
}
