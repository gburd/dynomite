//! Token arithmetic and ring-lookup benches.
//!
//! Run with `cargo bench --bench tokens`. The harness exercises:
//!
//! * `set_int_dyn_token` (the `DynToken::from_u32` constructor)
//! * `dyn_token_cmp` (the `cmp` impl on the comparable token type)
//! * ring lookup (`vnode_dispatch` over a sorted continuum)
//!
//! Continuum sizes sweep over 100 / 1000 / 10000 servers to expose
//! the binary-search scaling.
//!
//! The corresponding baseline JSON file lives at
//! `crates/dynomite/benches/baseline/tokens.json`.

#![allow(missing_docs)]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use dynomite::cluster::datacenter::Continuum;
use dynomite::cluster::vnode::dispatch;
use dynomite::hashkit::DynToken;

const RING_SIZES: [usize; 3] = [100, 1000, 10000];

fn build_ring(n: usize) -> Vec<Continuum> {
    let step = u32::MAX / u32::try_from(n).unwrap_or(1).max(1);
    let mut v: Vec<Continuum> = (0..n)
        .map(|i| {
            let tok = DynToken::from_u32(step.wrapping_mul(u32::try_from(i).unwrap_or(0)));
            Continuum::new(tok, u32::try_from(i).unwrap_or(0))
        })
        .collect();
    v.sort_by(|a, b| a.token.cmp(&b.token));
    v
}

fn set_int(c: &mut Criterion) {
    c.bench_function("dyn_token_set_int", |b| {
        b.iter(|| {
            let t = DynToken::from_u32(black_box(0xdead_beef));
            black_box(t);
        });
    });
}

fn token_cmp(c: &mut Criterion) {
    let a = DynToken::from_u32(0x1234_5678);
    let bb = DynToken::from_u32(0x8765_4321);
    c.bench_function("dyn_token_cmp", |b| {
        b.iter(|| black_box(a.cmp(black_box(&bb))));
    });
}

fn ring_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_lookup");
    for &n in &RING_SIZES {
        let ring = build_ring(n);
        // Pick a query token that is unlikely to be a ring point;
        // the dispatch routine then performs the worst-case search.
        let query = DynToken::from_u32(0x1234_5678);
        group.bench_with_input(BenchmarkId::from_parameter(n), &ring, |b, ring| {
            b.iter(|| black_box(dispatch(black_box(ring), black_box(&query))));
        });
    }
    group.finish();
}

criterion_group!(benches, set_int, token_cmp, ring_lookup);
criterion_main!(benches);
