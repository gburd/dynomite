//! Throughput benchmarks for the text index.
//!
//! Two suites:
//!
//! * `insert_10k_docs` -- inserts 10,000 random 256-byte text
//!   strings into a single index. Reports inserts/sec.
//! * `search_query` -- runs single-query substring searches
//!   against a pre-populated 10k-doc index. Reports
//!   queries/sec at p50/p95/p99 via Criterion's built-in
//!   reporters.
//!
//! Run with `cargo bench -p dyntext --bench index_throughput`.

// Bench harness; criterion's `criterion_group!` expands to an
// undocumented function which lints as `missing_docs` under the
// workspace-wide `warn` setting. Disable for this file only;
// benches are not part of the public surface.
#![allow(missing_docs)]

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use dyntext::TextIndex;

/// Tiny xorshift PRNG so the bench has a deterministic data
/// stream without pulling in the rand crate.
struct Xorshift(u64);
impl Xorshift {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "bench-only narrowing of a u64 PRNG output to a single byte"
)]
fn rand_text(rng: &mut Xorshift, len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(len);
    for _ in 0..len {
        // Restrict to lowercase ASCII so trigrams collide
        // realistically and the postings are non-trivial.
        let byte = b'a' + ((rng.next() % 26) as u8);
        v.push(byte);
    }
    v
}

fn build_corpus(n: usize, doc_len: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut rng = Xorshift::new(seed);
    (0..n).map(|_| rand_text(&mut rng, doc_len)).collect()
}

fn bench_insert(c: &mut Criterion) {
    const N: usize = 10_000;
    const DOC_LEN: usize = 256;
    let corpus = build_corpus(N, DOC_LEN, 0x00C0_FFEE);

    let mut group = c.benchmark_group("dyntext_insert");
    group.throughput(Throughput::Elements(N as u64));
    group.sample_size(10);
    group.bench_function("insert_10k_docs_256B", |b| {
        b.iter(|| {
            let mut idx = TextIndex::new();
            for doc in &corpus {
                idx.insert(black_box(doc.clone()));
            }
            black_box(idx);
        });
    });
    group.finish();
}

fn bench_search(c: &mut Criterion) {
    const N: usize = 10_000;
    const DOC_LEN: usize = 256;
    let corpus = build_corpus(N, DOC_LEN, 0x00C0_FFEE);

    let mut idx = TextIndex::new();
    for doc in &corpus {
        idx.insert(doc.clone());
    }

    // Pick a handful of queries; some will hit, some will miss.
    let queries: Vec<Vec<u8>> = vec![
        corpus[0][0..6].to_vec(),
        corpus[100][16..22].to_vec(),
        corpus[5_000][50..56].to_vec(),
        b"zzzzzz".to_vec(),
        b"qqqqqq".to_vec(),
    ];

    let mut group = c.benchmark_group("dyntext_search");
    group.throughput(Throughput::Elements(queries.len() as u64));
    group.sample_size(20);
    group.bench_function("search_query_10k_docs", |b| {
        b.iter(|| {
            for q in &queries {
                let hits = idx.search_substring(black_box(q));
                black_box(hits);
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_insert, bench_search);
criterion_main!(benches);
