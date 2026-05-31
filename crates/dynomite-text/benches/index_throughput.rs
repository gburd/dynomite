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

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

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

/// Regex-search throughput across the K = 0, 1, 2 regimes
/// against several corpus sizes. Mirrors the parameter space
/// of the workspace-level `ft_search` bench but stays inside
/// the `dynomite-text` crate so we can iterate on the matcher
/// without rebuilding the engine.
#[allow(
    clippy::cast_possible_truncation,
    reason = "bench-only narrowing of a u64 PRNG output to a single byte"
)]
fn bench_regex(c: &mut Criterion) {
    const ALNUM: &[u8; 37] = b"abcdefghijklmnopqrstuvwxyz0123456789 ";
    const STRING_LEN: usize = 256;
    const CORPUS_SIZES: &[usize] = &[1_000, 10_000];

    fn rand_alnum(state: &mut Xorshift) -> u8 {
        ALNUM[(state.next() % ALNUM.len() as u64) as usize]
    }
    fn rand_string(state: &mut Xorshift, len: usize) -> Vec<u8> {
        (0..len).map(|_| rand_alnum(state)).collect()
    }

    fn build_index(n: usize) -> TextIndex {
        let mut idx = TextIndex::new();
        let mut state = Xorshift::new(0xDEAD_BEEF_CAFE_F00D ^ n as u64);
        for _ in 0..n {
            idx.insert(rand_string(&mut state, STRING_LEN));
        }
        idx
    }

    fn regex_escape(s: &[u8]) -> String {
        let mut out = String::with_capacity(s.len());
        for &b in s {
            let c = b as char;
            if matches!(
                c,
                '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
            ) {
                out.push('\\');
            }
            out.push(c);
        }
        out
    }

    fn make_queries(idx: &TextIndex, n: usize, seed: u64) -> Vec<String> {
        let docs = idx.docs();
        let dn = docs.len();
        let mut state = Xorshift::new(seed);
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let want_hit = i % 2 == 0 && dn > 0;
            let pat = if want_hit {
                let pick = (state.next() % dn as u64) as usize;
                let txt = docs.iter().nth(pick).map(|(_, d)| d.text.clone()).unwrap();
                if txt.len() > 8 {
                    let off = (state.next() % (txt.len() - 8) as u64) as usize;
                    let head = &txt[off..off + 5];
                    let tail = &txt[off + 5..off + 8];
                    format!("{}\\w+{}", regex_escape(head), regex_escape(tail))
                } else {
                    let head: String = (0..5).map(|_| rand_alnum(&mut state) as char).collect();
                    let tail: String = (0..3).map(|_| rand_alnum(&mut state) as char).collect();
                    format!("{head}\\w+{tail}")
                }
            } else {
                let head: String = (0..5).map(|_| rand_alnum(&mut state) as char).collect();
                let tail: String = (0..3).map(|_| rand_alnum(&mut state) as char).collect();
                format!("{head}\\w+{tail}")
            };
            out.push(pat);
        }
        out
    }

    let mut group = c.benchmark_group("dyntext_regex_search");
    group.sample_size(20);
    for &corpus in CORPUS_SIZES {
        let idx = build_index(corpus);
        let queries = make_queries(&idx, 32, 0xCAFE_F00D ^ corpus as u64);

        group.bench_with_input(
            BenchmarkId::new("k0", corpus),
            &(&idx, &queries),
            |b, (idx, queries)| {
                let mut q_idx = 0_usize;
                b.iter(|| {
                    let q = &queries[q_idx % queries.len()];
                    q_idx = q_idx.wrapping_add(1);
                    black_box(idx.search_regex(q).expect("pattern compiles"));
                });
            },
        );
        for &k in &[1_u16, 2_u16] {
            let label = if k == 1 { "k1" } else { "k2" };
            group.bench_with_input(
                BenchmarkId::new(label, corpus),
                &(&idx, &queries),
                |b, (idx, queries)| {
                    let mut q_idx = 0_usize;
                    b.iter(|| {
                        let q = &queries[q_idx % queries.len()];
                        q_idx = q_idx.wrapping_add(1);
                        black_box(idx.search_regex_approx(q, k).expect("pattern compiles"));
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_insert, bench_search, bench_regex);
criterion_main!(benches);
