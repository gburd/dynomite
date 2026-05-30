//! Criterion benches for the random-slicing distribution.
//!
//! Compares `RandomSlices::index_for` against `vnode::dispatch`
//! over the same per-rack peer count. The bench is wired into
//! the Cargo bench list in `crates/dynomite/Cargo.toml`; run it
//! with `cargo bench --bench random_slicing`. It accepts the
//! standard criterion CLI flags, including `--quick` for a CI
//! smoke pass.

#![allow(missing_docs)]

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dynomite::cluster::datacenter::Continuum;
use dynomite::cluster::vnode;
use dynomite::hashkit::random_slicing::RandomSlices;
use dynomite::hashkit::DynToken;

const PEER_COUNTS: [usize; 4] = [16, 64, 256, 1024];

fn bench_random_slicing(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_slicing");
    for &n in &PEER_COUNTS {
        let names: Vec<String> = (0..n).map(|i| format!("peer-{i:08x}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let slices = RandomSlices::from_uniform(&refs).unwrap();
        let id = BenchmarkId::new("index_for", n);
        group.bench_function(id, |b| {
            let mut h: u64 = 0x9e37_79b9_7f4a_7c15;
            b.iter(|| {
                h = h.wrapping_mul(0x6a4a_7935_bd1e_995d).wrapping_add(7);
                black_box(slices.index_for(black_box(h)));
            });
        });
    }
    group.finish();
}

fn bench_vnode(c: &mut Criterion) {
    let mut group = c.benchmark_group("vnode_for_compare");
    for &n in &PEER_COUNTS {
        let mut continua: Vec<Continuum> = (0..n)
            .map(|i| {
                let token_val: u32 = (((i as u64).wrapping_mul(0x9e37_79b9)) & 0xffff_ffff) as u32;
                let peer_idx = u32::try_from(i).unwrap_or(0);
                Continuum::new(DynToken::from_u32(token_val), peer_idx)
            })
            .collect();
        continua.sort_by(|a, b| a.token.cmp(&b.token));
        let id = BenchmarkId::new("dispatch", n);
        group.bench_function(id, |b| {
            let mut h: u64 = 0x9e37_79b9_7f4a_7c15;
            b.iter(|| {
                h = h.wrapping_mul(0x6a4a_7935_bd1e_995d).wrapping_add(7);
                let tok = DynToken::from_u32((h & 0xffff_ffff) as u32);
                black_box(vnode::dispatch(&continua, &tok));
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_random_slicing, bench_vnode);
criterion_main!(benches);
