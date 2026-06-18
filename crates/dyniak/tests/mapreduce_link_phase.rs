//! Integration tests for the MapReduce link phase against a real
//! [`NoxuDatastore`].
//!
//! These tests seed a bucket with objects whose stored
//! [`HttpObject`] envelopes carry links, then run a job with a
//! [`Phase::Link`] over a known source object and assert the phase
//! emits exactly the matching `(bucket, key)` targets, honouring the
//! `{bucket, tag}` filter and the `keep` flag.
//!
//! Gated on the `noxu` feature; without it the file compiles to an
//! empty module.

#![cfg(feature = "noxu")]

use std::sync::Arc;

use dyniak::datastore::NoxuDatastore;
use dyniak::mapreduce::{
    builtins::default_registry, run_job_full, Inputs, KeyDatum, MapReduceJob, Phase, PhaseRegistry,
};
use dyniak::proto::http::object::{HttpLink, HttpObject};
use dynomite::embed::Datastore;
use tempfile::TempDir;

fn registry() -> Arc<PhaseRegistry> {
    Arc::new(default_registry())
}

/// Store an object whose envelope carries `links`.
fn put_with_links(store: &NoxuDatastore, bucket: &[u8], key: &[u8], links: Vec<HttpLink>) {
    let obj = HttpObject {
        value: b"src".to_vec(),
        content_type: None,
        indexes: Vec::new(),
        links,
    };
    store
        .put_object(bucket, key, &obj.to_storage_bytes(), &[])
        .expect("put");
}

fn link(bucket: &str, key: &str, tag: &str) -> HttpLink {
    HttpLink {
        bucket: bucket.to_string(),
        key: key.to_string(),
        tag: tag.to_string(),
    }
}

/// Collect the `(bucket, key)` pairs from a link phase's output.
fn targets(out: &[dyniak::mapreduce::PhaseOutput]) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = out
        .iter()
        .map(|o| {
            (
                o.value["bucket"].as_str().expect("bucket").to_string(),
                o.value["key"].as_str().expect("key").to_string(),
            )
        })
        .collect();
    v.sort();
    v
}

/// Seed object A in bucket `people` with links to B and C tagged
/// "friend" and to D tagged "other".
fn seed(store: &NoxuDatastore) {
    put_with_links(
        store,
        b"people",
        b"a",
        vec![
            link("people", "b", "friend"),
            link("people", "c", "friend"),
            link("people", "d", "other"),
        ],
    );
}

#[tokio::test]
async fn link_phase_filters_by_tag() {
    let dir = TempDir::new().expect("tempdir");
    let store = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    seed(&store);
    let ds: Arc<dyn Datastore> = Arc::new(store);

    let job = MapReduceJob {
        inputs: Inputs::KeyData(vec![KeyDatum::pair("people", "a")]),
        phases: vec![Phase::Link {
            bucket: Some("people".into()),
            tag: Some("friend".into()),
            keep: true,
        }],
        timeout_ms: None,
    };
    let out = run_job_full(job, registry(), None, Some(ds))
        .await
        .expect("ok");
    assert_eq!(
        targets(&out),
        vec![
            ("people".to_string(), "b".to_string()),
            ("people".to_string(), "c".to_string()),
        ],
        "tag=friend matches exactly B and C"
    );
}

#[tokio::test]
async fn link_phase_wildcard_tag_includes_all() {
    let dir = TempDir::new().expect("tempdir");
    let store = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    seed(&store);
    let ds: Arc<dyn Datastore> = Arc::new(store);

    let job = MapReduceJob {
        inputs: Inputs::KeyData(vec![KeyDatum::pair("people", "a")]),
        phases: vec![Phase::Link {
            bucket: Some("people".into()),
            tag: None,
            keep: true,
        }],
        timeout_ms: None,
    };
    let out = run_job_full(job, registry(), None, Some(ds))
        .await
        .expect("ok");
    assert_eq!(
        targets(&out),
        vec![
            ("people".to_string(), "b".to_string()),
            ("people".to_string(), "c".to_string()),
            ("people".to_string(), "d".to_string()),
        ],
        "tag=None (wildcard) includes the 'other'-tagged D"
    );
}

#[tokio::test]
async fn link_phase_filters_by_bucket() {
    let dir = TempDir::new().expect("tempdir");
    let store = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    put_with_links(
        &store,
        b"people",
        b"a",
        vec![link("people", "b", "x"), link("work", "acme", "x")],
    );
    let ds: Arc<dyn Datastore> = Arc::new(store);

    let job = MapReduceJob {
        inputs: Inputs::KeyData(vec![KeyDatum::pair("people", "a")]),
        phases: vec![Phase::Link {
            bucket: Some("work".into()),
            tag: None,
            keep: true,
        }],
        timeout_ms: None,
    };
    let out = run_job_full(job, registry(), None, Some(ds))
        .await
        .expect("ok");
    assert_eq!(
        targets(&out),
        vec![("work".to_string(), "acme".to_string())],
        "bucket=work selects only the work link"
    );
}

#[tokio::test]
async fn link_phase_over_missing_object_yields_empty() {
    let dir = TempDir::new().expect("tempdir");
    let store = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    let ds: Arc<dyn Datastore> = Arc::new(store);

    let job = MapReduceJob {
        inputs: Inputs::KeyData(vec![KeyDatum::pair("people", "ghost")]),
        phases: vec![Phase::Link {
            bucket: None,
            tag: None,
            keep: true,
        }],
        timeout_ms: None,
    };
    let out = run_job_full(job, registry(), None, Some(ds))
        .await
        .expect("missing object is not an error");
    assert!(out.is_empty(), "a missing object contributes no links");
}

#[tokio::test]
async fn link_phase_feeds_following_reduce() {
    // Prove the link output threads into a later phase: a reduce that
    // counts its inputs sees exactly the matched links.
    let dir = TempDir::new().expect("tempdir");
    let store = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    seed(&store);
    let ds: Arc<dyn Datastore> = Arc::new(store);

    let job = MapReduceJob {
        inputs: Inputs::KeyData(vec![KeyDatum::pair("people", "a")]),
        phases: vec![
            Phase::Link {
                bucket: None,
                tag: Some("friend".into()),
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
    assert_eq!(out.len(), 1, "only the reduce phase keeps");
    assert_eq!(
        out[0].value,
        serde_json::json!(2u64),
        "two friend links flowed into the reduce"
    );
}

#[tokio::test]
async fn link_phase_keep_is_honored() {
    let dir = TempDir::new().expect("tempdir");
    let store = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    seed(&store);
    let ds: Arc<dyn Datastore> = Arc::new(store);

    // keep=false on a non-final link phase: its output is not
    // captured, only the final reduce's.
    let job = MapReduceJob {
        inputs: Inputs::KeyData(vec![KeyDatum::pair("people", "a")]),
        phases: vec![
            Phase::Link {
                bucket: None,
                tag: Some("friend".into()),
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
    assert!(
        out.iter().all(|o| o.phase == 1),
        "link phase (0) output suppressed by keep=false"
    );

    // keep=true makes the link phase output appear alongside the
    // final phase.
    let dir2 = TempDir::new().expect("tempdir");
    let store2 = NoxuDatastore::open_in(dir2.path()).expect("open noxu");
    seed(&store2);
    let ds2: Arc<dyn Datastore> = Arc::new(store2);
    let job2 = MapReduceJob {
        inputs: Inputs::KeyData(vec![KeyDatum::pair("people", "a")]),
        phases: vec![
            Phase::Link {
                bucket: None,
                tag: Some("friend".into()),
                keep: true,
            },
            Phase::Reduce {
                fn_name: "reduce_count".into(),
                arg: None,
                keep: true,
            },
        ],
        timeout_ms: None,
    };
    let out2 = run_job_full(job2, registry(), None, Some(ds2))
        .await
        .expect("ok");
    assert!(
        out2.iter().any(|o| o.phase == 0),
        "link phase output kept with keep=true"
    );
}
