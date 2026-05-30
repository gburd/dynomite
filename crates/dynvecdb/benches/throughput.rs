//! Throughput benchmarks for the vector engine.
//!
//! Two suites:
//!
//! * `insert_throughput` -- inserts N=10_000 random 64-dim
//!   vectors into a single-table store. Reports inserts/sec.
//! * `search_throughput` -- runs single-vector top-10 searches
//!   against a pre-populated 10k-vector index. Reports
//!   searches/sec at p50/p95/p99 via Criterion's built-in
//!   reporters.
//!
//! Run with `cargo bench -p dynvecdb --bench throughput`.

// Bench harness; criterion's `criterion_group!` expands to an
// undocumented function which lints as `missing_docs` under the
// workspace-wide `warn` setting. Disable the lint for this file
// only; benches are not part of the public surface.
#![allow(missing_docs)]

use std::collections::HashMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use dynvecdb::distance::Distance;
use dynvecdb::encoding::Codec;
use dynvecdb::index::HnswParams;
use dynvecdb::storage::{TableSchema, VectorStore};

fn rand_vec(seed: u64, dim: usize) -> Vec<f32> {
    let mut x = seed;
    let mut v = Vec::with_capacity(dim);
    for _ in 0..dim {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let bits = (x >> 11) & ((1_u64 << 53) - 1);
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            reason = "bench fixture: deterministic PRNG narrowed to f32"
        )]
        let r = (((bits as f64) / ((1_u64 << 53) as f64)) * 2.0 - 1.0) as f32;
        v.push(r);
    }
    v
}

fn schema(name: &str, dim: u16) -> TableSchema {
    TableSchema {
        name: name.to_string(),
        dim,
        codec: Codec::Int8Quantized,
        distance: Distance::Euclidean,
        hnsw: HnswParams::default(),
    }
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    group.throughput(Throughput::Elements(1));
    group.bench_function("upsert_64d", |b| {
        let store = VectorStore::in_memory();
        store.create_table(schema("t", 64)).unwrap();
        let mut i = 0_u64;
        b.iter(|| {
            let v = rand_vec(i, 64);
            let key = format!("k{i}").into_bytes();
            store.upsert("t", key, &v, HashMap::new()).unwrap();
            i += 1;
            black_box(i);
        });
    });
    group.finish();
}

fn bench_search(c: &mut Criterion) {
    // Pre-populate a 10k-vector index.
    let store = VectorStore::in_memory();
    store.create_table(schema("t", 64)).unwrap();
    for i in 0..10_000_u64 {
        let v = rand_vec(i.wrapping_mul(0x9E37_79B9_7F4A_7C15), 64);
        let key = format!("k{i}").into_bytes();
        store.upsert("t", key, &v, HashMap::new()).unwrap();
    }

    let mut group = c.benchmark_group("search");
    group.throughput(Throughput::Elements(1));
    group.bench_function("topk10_64d_10k", |b| {
        let mut q = 0_u64;
        b.iter(|| {
            let qv = rand_vec(q.wrapping_mul(0x517C_C1B7_2722_0A95), 64);
            let hits = store.search("t", &qv, 10, None).unwrap();
            q += 1;
            black_box(hits);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_insert, bench_search);
criterion_main!(benches);
