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
//! Run with `cargo bench -p dynvec --bench throughput`.

// Bench harness; criterion's `criterion_group!` expands to an
// undocumented function which lints as `missing_docs` under the
// workspace-wide `warn` setting. Disable the lint for this file
// only; benches are not part of the public surface.
#![allow(missing_docs)]

use std::collections::HashMap;

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use dynvec::distance::Distance;
use dynvec::encoding::Codec;
use dynvec::index::HnswParams;
use dynvec::storage::{IndexAlgorithm, TableSchema, VectorStore};

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
        algorithm: IndexAlgorithm::Hnsw,
    }
}

fn schema_turbovec(name: &str, dim: u16) -> TableSchema {
    TableSchema {
        name: name.to_string(),
        dim,
        codec: Codec::Turbovec4Bit,
        // Turbovec's SIMD scoring is inner-product-style; the
        // table layer normalises queries and stored vectors
        // when the metric is Cosine, which is the production
        // default for embedding workloads.
        distance: Distance::Cosine,
        hnsw: HnswParams::default(),
        // Brute-force baseline: the SIMD scan that this
        // benchmark group is paired against the HNSW variants
        // below.
        algorithm: IndexAlgorithm::Flat,
    }
}

fn schema_turbo_hnsw(name: &str, dim: u16, codec: Codec) -> TableSchema {
    TableSchema {
        name: name.to_string(),
        dim,
        codec,
        distance: Distance::Cosine,
        hnsw: HnswParams::default(),
        algorithm: IndexAlgorithm::Hnsw,
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

/// Turbovec-backed search benchmark. Same fixture as
/// [`bench_search`] but the table's codec is `Turbovec4Bit`,
/// so the SIMD search kernel and 4-bit packed code layout are
/// on the hot path. Compare the two `topk10_64d_10k` rows to
/// quantify the SIMD speedup over the HNSW + scalar `f32`
/// distance baseline.
fn bench_search_turbovec(c: &mut Criterion) {
    let store = VectorStore::in_memory();
    store.create_table(schema_turbovec("t", 64)).unwrap();
    for i in 0..10_000_u64 {
        let v = rand_vec(i.wrapping_mul(0x9E37_79B9_7F4A_7C15), 64);
        let key = format!("k{i}").into_bytes();
        store.upsert("t", key, &v, HashMap::new()).unwrap();
    }

    let mut group = c.benchmark_group("search_turbovec_4bit");
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

/// Turbo-HNSW search benchmark for the 4-bit codec. Pairs
/// HNSW topology with the turbovec packed-code distance
/// kernel; this is the path the workspace falls back to when
/// the corpus is large enough that the brute scan above caps
/// p99.
fn bench_search_turbo_hnsw_4bit(c: &mut Criterion) {
    let store = VectorStore::in_memory();
    store
        .create_table(schema_turbo_hnsw("t", 64, Codec::Turbovec4Bit))
        .unwrap();
    for i in 0..10_000_u64 {
        let v = rand_vec(i.wrapping_mul(0x9E37_79B9_7F4A_7C15), 64);
        let key = format!("k{i}").into_bytes();
        store.upsert("t", key, &v, HashMap::new()).unwrap();
    }

    let mut group = c.benchmark_group("search_turbo_hnsw_4bit");
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

/// Turbo-HNSW search benchmark at 2-bit. The packed code
/// halves vs 4-bit, the codec error doubles; HNSW topology
/// holds recall together via the bigger ef_construction.
fn bench_search_turbo_hnsw_2bit(c: &mut Criterion) {
    let store = VectorStore::in_memory();
    store
        .create_table(schema_turbo_hnsw("t", 64, Codec::Turbovec2Bit))
        .unwrap();
    for i in 0..10_000_u64 {
        let v = rand_vec(i.wrapping_mul(0x9E37_79B9_7F4A_7C15), 64);
        let key = format!("k{i}").into_bytes();
        store.upsert("t", key, &v, HashMap::new()).unwrap();
    }

    let mut group = c.benchmark_group("search_turbo_hnsw_2bit");
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

/// Turbo-HNSW at 4-bit on a 100k corpus. The full ft-search
/// rig in `crates/dynomite/benches/ft_search.rs` is the
/// authoritative end-to-end reference; this is the smaller
/// `dynomite-vec` companion that lets a contributor see the
/// per-codec asymptote without rebuilding the rest of the
/// engine.
fn bench_search_turbo_hnsw_4bit_100k(c: &mut Criterion) {
    let store = VectorStore::in_memory();
    store
        .create_table(schema_turbo_hnsw("t", 64, Codec::Turbovec4Bit))
        .unwrap();
    for i in 0..100_000_u64 {
        let v = rand_vec(i.wrapping_mul(0x9E37_79B9_7F4A_7C15), 64);
        let key = format!("k{i}").into_bytes();
        store.upsert("t", key, &v, HashMap::new()).unwrap();
    }

    let mut group = c.benchmark_group("search_turbo_hnsw_4bit");
    group.throughput(Throughput::Elements(1));
    group.bench_function("topk10_64d_100k", |b| {
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

/// Turbo-HNSW at 2-bit on a 100k corpus. This is the path
/// the stage's perf goal targets.
fn bench_search_turbo_hnsw_2bit_100k(c: &mut Criterion) {
    let store = VectorStore::in_memory();
    store
        .create_table(schema_turbo_hnsw("t", 64, Codec::Turbovec2Bit))
        .unwrap();
    for i in 0..100_000_u64 {
        let v = rand_vec(i.wrapping_mul(0x9E37_79B9_7F4A_7C15), 64);
        let key = format!("k{i}").into_bytes();
        store.upsert("t", key, &v, HashMap::new()).unwrap();
    }

    let mut group = c.benchmark_group("search_turbo_hnsw_2bit");
    group.throughput(Throughput::Elements(1));
    group.bench_function("topk10_64d_100k", |b| {
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

criterion_group!(
    benches,
    bench_insert,
    bench_search,
    bench_search_turbovec,
    bench_search_turbo_hnsw_4bit,
    bench_search_turbo_hnsw_2bit,
    bench_search_turbo_hnsw_4bit_100k,
    bench_search_turbo_hnsw_2bit_100k,
);
criterion_main!(benches);
