//! Per-algorithm hashkit throughput benches.
//!
//! Run with `cargo bench --bench hashkit`. The harness exercises
//! every algorithm exposed by [`HashType::all`] over four key
//! sizes (16 / 64 / 256 / 1024 bytes). Each iteration computes one
//! hash of a fresh key.
//!
//! The corresponding baseline JSON file lives at
//! `crates/dynomite/benches/baseline/hashkit.json`.

#![allow(missing_docs)]

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dynomite::hashkit::{hash, HashType};

const KEY_SIZES: [usize; 4] = [16, 64, 256, 1024];

fn run(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashkit");
    for &ty in HashType::all() {
        for &k in &KEY_SIZES {
            let key: Vec<u8> = (0..k)
                .map(|i| u8::try_from(i & 0xff).unwrap_or(0))
                .collect();
            let id = BenchmarkId::new(ty.as_str(), k);
            group.throughput(Throughput::Bytes(k as u64));
            group.bench_with_input(id, &key, |b, key| {
                b.iter(|| black_box(hash(ty, black_box(key))));
            });
        }
    }
    group.finish();
}

criterion_group!(benches, run);
criterion_main!(benches);
