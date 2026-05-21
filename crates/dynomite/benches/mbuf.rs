//! Mbuf alloc/free, pool recycling, split, and copy hot-path benches.
//!
//! Run with `cargo bench --bench mbuf`. The harness exercises:
//!
//! * fresh-allocation latency through an empty pool
//! * pool-recycle latency (`get` after `put`)
//! * `split_off` at the midpoint
//! * `copy_from_slice` over four payload sizes
//!
//! The corresponding baseline JSON file lives at
//! `crates/dynomite/benches/baseline/mbuf.json`.

#![allow(missing_docs)]

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use dynomite::io::mbuf::MbufPool;

const PAYLOAD_SIZES: [usize; 4] = [16, 64, 1024, 8192];

fn alloc_free_cold(c: &mut Criterion) {
    c.bench_function("mbuf_alloc_free_cold", |b| {
        b.iter_batched(
            || MbufPool::new(16384, 0),
            |pool| {
                let mb = pool.get();
                pool.put(mb);
            },
            BatchSize::SmallInput,
        );
    });
}

fn alloc_free_recycled(c: &mut Criterion) {
    let pool = MbufPool::new(16384, 1024);
    // Pre-warm: allocate and return a buffer so the pool's free list
    // is non-empty by the time the timed region begins.
    pool.put(pool.get());
    c.bench_function("mbuf_alloc_free_recycled", |b| {
        b.iter(|| {
            let mb = pool.get();
            pool.put(mb);
        });
    });
}

fn split_off_midpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("mbuf_split_off");
    for &p in &PAYLOAD_SIZES {
        group.throughput(Throughput::Bytes(p as u64));
        group.bench_with_input(BenchmarkId::from_parameter(p), &p, |b, &p| {
            let pool = MbufPool::new(16384, 1024);
            let payload = vec![b'a'; p];
            b.iter_batched(
                || {
                    let mut mb = pool.get();
                    mb.copy_from_slice(&payload);
                    mb
                },
                |mut mb| {
                    let _ = mb.split_off(p / 2, &pool);
                    pool.put(mb);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn copy_from_slice(c: &mut Criterion) {
    let mut group = c.benchmark_group("mbuf_copy_from_slice");
    for &p in &PAYLOAD_SIZES {
        group.throughput(Throughput::Bytes(p as u64));
        group.bench_with_input(BenchmarkId::from_parameter(p), &p, |b, &p| {
            let pool = MbufPool::new(16384, 1024);
            let payload = vec![b'a'; p];
            b.iter_batched(
                || pool.get(),
                |mut mb| {
                    mb.copy_from_slice(black_box(&payload));
                    pool.put(mb);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    alloc_free_cold,
    alloc_free_recycled,
    split_off_midpoint,
    copy_from_slice,
);
criterion_main!(benches);
