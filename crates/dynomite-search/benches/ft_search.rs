//! End-to-end FT.SEARCH latency benchmarks (p50 / p95 / p99).
//!
//! Three benchmark groups capture the per-query wall-clock
//! latency a single client sees against a fully built corpus.
//! The corpus for each case is built once, outside the timed
//! loop, via [`Criterion::bench_with_input`]. The timed region
//! is the search call only.
//!
//! 1. `vector_knn_latency` -- 128-dim float32 vectors at 1k /
//!    10k / 100k corpus sizes, KNN-10 queries. The codec varies
//!    over `Fp16`, `Int8Quantized`, `Turbovec4Bit`, and
//!    `Turbovec2Bit` to expose the per-codec cost on the same
//!    workload. The brief asks for `Fp32` as well; the engine
//!    layer maps the wire-level `FLOAT32` schema to the `Fp16`
//!    codec on disk (see `dynomite_search::schema`), so `Fp16`
//!    is the canonical representative for the float path. The
//!    extra row substitutes the most aggressive `Turbovec2Bit`
//!    codec to round out the four-codec sweep.
//!
//! 2. `text_substring_latency` -- 256-byte random-alphanumeric
//!    strings at 1k / 10k / 100k corpus sizes; queries mix
//!    3-byte, 5-byte, and 10-byte substrings, with a 50 / 50
//!    split between guaranteed hits (substrings sampled from a
//!    random doc) and random misses. Variants: `bloom_on` (the
//!    default tier-3 funnel that ships in `dyntext::TextIndex`)
//!    and `bloom_off` (the same trigram + recheck pipeline with
//!    the per-doc bloom filter step skipped). The delta
//!    between the two rows is the bloom filter's contribution
//!    to query latency.
//!
//! 3. `regex_search_latency` -- same corpus shape; queries mix
//!    literal anchors with `\\w+` placeholders. K=0 uses
//!    [`TextIndex::search_regex`] (the trigram + bloom funnel
//!    plus a `regex::bytes::Regex` recheck). K=1 and K=2 use
//!    [`TextIndex::search_regex_approx`], the TRE-backed
//!    approximate-regex path with `max_errors` set to 1 and 2
//!    respectively. Note that `search_regex_approx` is a
//!    full-scan recheck today; the per-query latency
//!    accordingly grows linearly in the corpus size and is
//!    expected to dominate the K=0 row by a wide margin.
//!
//! # Methodology
//!
//! Criterion's default measurement (mean + standard deviation
//! over a sampling distribution) does not report tail
//! percentiles directly. The bench therefore:
//!
//! * Registers each (group, corpus, variant) tuple as a normal
//!   `bench_with_input` so the criterion HTML / JSON output
//!   under `target/criterion/` has the usual statistics, and
//! * Right after each `bench_with_input` call, runs an explicit
//!   1000-iteration timing pass with a fresh `Instant` per
//!   query. The collected `Vec<Duration>` is sorted and
//!   reduced to p50 / p95 / p99. The result is appended to a
//!   process-wide registry.
//!
//! After every group has run, [`main`] writes:
//!
//! * One JSON sidecar per group at
//!   `target/criterion/ft_search/<group>/percentiles.json` so a
//!   follow-up CI step can diff successive runs, and
//! * One markdown summary at `docs/dynvec/bench-results.md`
//!   containing the latest percentile table for every group.
//!
//! # Hardware / environment
//!
//! The bench runs entirely in-process: no network, no GPU, no
//! sudo. Build the index, query the index, measure. The
//! deterministic xorshift64* PRNG below seeds every random
//! input so successive runs see identical corpora.
//!
//! # Recommended invocation
//!
//! ```text
//! cargo bench -p dynomite --bench ft_search
//! # full run is roughly 10 minutes on a desktop-class CPU.
//! # Smoke pass at reduced sample count:
//! cargo bench -p dynomite --bench ft_search -- --quick
//! # Compile-only iteration (criterion test mode):
//! cargo bench -p dynomite --bench ft_search -- --test
//! ```
//!
//! Override the per-case percentile-pass iteration count with
//! `FT_SEARCH_QUERY_COUNT=N`. The default is 1000; values below
//! 100 produce noisy percentiles. Set to 0 to skip the
//! percentile pass entirely (criterion measurement still runs).
//! Set `FT_SEARCH_QUICK=1` to behave as if `--quick` was passed,
//! useful for ad-hoc shell sessions that already pass `--`
//! to the bench harness.
//!
//! # Corpus-size policy
//!
//! * Test mode (`--test` / `--list`): a single 256-element
//!   corpus per group, the percentile pass disabled. The full
//!   smoke-test pass runs in well under a minute even on a
//!   cold target tree.
//! * Quick mode (`--quick` or `FT_SEARCH_QUICK=1`): 1k and 10k
//!   corpora, 100 queries per percentile pass.
//! * Full mode: 1k, 10k, and 100k corpora, 1000 queries per
//!   percentile pass.

#![allow(missing_docs)]

use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use criterion::{criterion_group, BenchmarkId, Criterion};
use dyntext::tre::{TreCompiledPattern, TreMatchOpts};
use dyntext::trigram;
use dyntext::TextIndex;
use dynvec::distance::Distance;
use dynvec::encoding::Codec;
use dynvec::index::HnswParams;
use dynvec::storage::{IndexAlgorithm, TableSchema, VectorStore};

// --- Tunables -------------------------------------------------

const VECTOR_DIM_USIZE: usize = 128;
const VECTOR_DIM: u16 = 128;
const STRING_LEN: usize = 256;
const DEFAULT_QUERY_COUNT: usize = 1000;
const QUICK_QUERY_COUNT: usize = 100;
const FULL_CORPUS_SIZES: [usize; 3] = [1_000, 10_000, 100_000];
const QUICK_CORPUS_SIZES: [usize; 2] = [1_000, 10_000];
const TEST_CORPUS_SIZES: [usize; 1] = [256];

fn corpus_sizes() -> &'static [usize] {
    if is_test_mode() {
        &TEST_CORPUS_SIZES
    } else if args_contain("--quick") || std::env::var("FT_SEARCH_QUICK").is_ok() {
        &QUICK_CORPUS_SIZES
    } else {
        &FULL_CORPUS_SIZES
    }
}

// --- PRNG -----------------------------------------------------

/// xorshift64* PRNG. Deterministic for a given seed.
fn next_u64(state: &mut u64) -> u64 {
    let mut x = *state;
    if x == 0 {
        x = 0x9E37_79B9_7F4A_7C15;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

fn rand_unit_f32(state: &mut u64) -> f32 {
    // Top 53 bits make a fraction in `[0, 1)`; rescale to
    // `[-1, 1)`.
    let bits = next_u64(state) >> 11;
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        reason = "bench fixture: deterministic PRNG narrowed to f32"
    )]
    let r = (((bits as f64) / ((1_u64 << 53) as f64)) * 2.0 - 1.0) as f32;
    r
}

fn rand_vec(state: &mut u64) -> Vec<f32> {
    (0..VECTOR_DIM_USIZE)
        .map(|_| rand_unit_f32(state))
        .collect()
}

const ALNUM: &[u8; 37] = b"abcdefghijklmnopqrstuvwxyz0123456789 ";

fn rand_alnum_byte(state: &mut u64) -> u8 {
    #[allow(clippy::cast_possible_truncation, reason = "modulo 37 fits in u8")]
    let i = (next_u64(state) % ALNUM.len() as u64) as usize;
    ALNUM[i]
}

fn rand_string(state: &mut u64, len: usize) -> Vec<u8> {
    (0..len).map(|_| rand_alnum_byte(state)).collect()
}

// --- Percentile registry --------------------------------------

#[derive(Clone)]
struct PercentileRecord {
    group: String,
    corpus: usize,
    variant: String,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    samples: usize,
}

static REGISTRY: Mutex<Vec<PercentileRecord>> = Mutex::new(Vec::new());

fn percentile(durations: &[Duration], q: f64) -> Duration {
    debug_assert!(!durations.is_empty());
    debug_assert!((0.0..=1.0).contains(&q));
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "bench fixture: durations.len() < 2^32"
    )]
    let idx = ((durations.len() as f64) * q) as usize;
    durations[idx.min(durations.len() - 1)]
}

fn record(group: &str, corpus: usize, variant: &str, durations: &mut [Duration]) {
    if durations.is_empty() {
        return;
    }
    durations.sort_unstable();
    let rec = PercentileRecord {
        group: group.to_string(),
        corpus,
        variant: variant.to_string(),
        p50: percentile(durations, 0.50),
        p95: percentile(durations, 0.95),
        p99: percentile(durations, 0.99),
        samples: durations.len(),
    };
    eprintln!(
        "ft_search: {} corpus={} variant={} samples={} p50={:?} p95={:?} p99={:?}",
        rec.group, rec.corpus, rec.variant, rec.samples, rec.p50, rec.p95, rec.p99
    );
    REGISTRY
        .lock()
        .expect("invariant: REGISTRY mutex never poisoned in benches")
        .push(rec);
}

// --- Mode detection ------------------------------------------

fn args_contain(needle: &str) -> bool {
    std::env::args().any(|a| a == needle)
}

fn is_test_mode() -> bool {
    args_contain("--test") || args_contain("--list")
}

fn query_count() -> usize {
    if is_test_mode() {
        return 0;
    }
    if let Ok(s) = std::env::var("FT_SEARCH_QUERY_COUNT") {
        if let Ok(n) = s.parse::<usize>() {
            return n;
        }
    }
    if args_contain("--quick") {
        QUICK_QUERY_COUNT
    } else {
        DEFAULT_QUERY_COUNT
    }
}

// --- Corpus builders ------------------------------------------

fn vector_schema(name: &str, codec: Codec, algorithm: IndexAlgorithm) -> TableSchema {
    TableSchema {
        name: name.to_string(),
        dim: VECTOR_DIM,
        codec,
        // Turbovec is inner-product based and the table layer
        // normalises queries when the metric is Cosine. Keep
        // the metric uniform across codecs so the row sweep
        // compares like-for-like.
        distance: Distance::Cosine,
        hnsw: HnswParams::default(),
        algorithm,
    }
}

fn build_vector_store(corpus: usize, codec: Codec, algorithm: IndexAlgorithm) -> VectorStore {
    let store = VectorStore::in_memory();
    store
        .create_table(vector_schema("t", codec, algorithm))
        .expect("invariant: fresh store accepts the schema");
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15 ^ codec.name().len() as u64;
    for i in 0..corpus {
        let v = rand_vec(&mut state);
        let key = format!("k{i}").into_bytes();
        store
            .upsert("t", key, &v, HashMap::new())
            .expect("invariant: vector dimension matches schema");
    }
    store
}

fn build_text_index(corpus: usize) -> TextIndex {
    let mut idx = TextIndex::new();
    let mut state: u64 = 0xDEAD_BEEF_CAFE_F00D ^ corpus as u64;
    for _ in 0..corpus {
        idx.insert(rand_string(&mut state, STRING_LEN));
    }
    idx
}

// --- Query generators ----------------------------------------

fn vector_queries(n: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut state: u64 = seed;
    (0..n).map(|_| rand_vec(&mut state)).collect()
}

/// A mix of 3 / 5 / 10 byte substring queries. Half are
/// guaranteed hits sampled from random docs in the index; the
/// other half are random byte sequences that may or may not
/// hit by chance.
fn text_substring_queries(idx: &TextIndex, n: usize, seed: u64) -> Vec<Vec<u8>> {
    const LENS: [usize; 3] = [3, 5, 10];
    let docs = idx.docs();
    let doc_count = docs.len();
    let mut state: u64 = seed;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let len = LENS[i % LENS.len()];
        let want_hit = (i % 2) == 0 && doc_count > 0;
        if want_hit {
            // Sample a doc, then a slice within it.
            #[allow(
                clippy::cast_possible_truncation,
                reason = "doc_count fits in usize on the host"
            )]
            let pick = (next_u64(&mut state) % doc_count as u64) as u32;
            let doc = docs.iter().nth(pick as usize).map(|(_, d)| d);
            if let Some(doc) = doc {
                let text = &doc.text;
                if text.len() > len {
                    #[allow(clippy::cast_possible_truncation, reason = "text.len() < usize::MAX")]
                    let off = (next_u64(&mut state) % (text.len() - len) as u64) as usize;
                    out.push(text[off..off + len].to_vec());
                    continue;
                }
            }
        }
        out.push((0..len).map(|_| rand_alnum_byte(&mut state)).collect());
    }
    out
}

/// A mix of regex patterns that combine literal anchors with
/// `\w+` placeholders. The literals come from the same
/// alphanumeric alphabet as the corpus so the patterns hit
/// realistically.
fn regex_queries(idx: &TextIndex, n: usize, seed: u64) -> Vec<String> {
    let mut state: u64 = seed;
    let mut out = Vec::with_capacity(n);
    let docs = idx.docs();
    let doc_count = docs.len();
    for i in 0..n {
        // 50 / 50 between hit-flavoured and random-flavoured.
        let flavour = i % 4;
        let pat = if doc_count > 0 && (flavour == 0 || flavour == 1) {
            #[allow(clippy::cast_possible_truncation, reason = "doc_count < u32::MAX")]
            let pick = (next_u64(&mut state) % doc_count as u64) as u32;
            let doc_text = docs.iter().nth(pick as usize).map(|(_, d)| d.text.clone());
            match doc_text {
                Some(text) => sampled_pattern(&text, &mut state, flavour),
                None => random_pattern(&mut state),
            }
        } else {
            random_pattern(&mut state)
        };
        out.push(pat);
    }
    out
}

fn sample_substring(text: &[u8], state: &mut u64, len: usize) -> Vec<u8> {
    if text.len() <= len {
        return text.to_vec();
    }
    #[allow(clippy::cast_possible_truncation, reason = "text.len() < usize::MAX")]
    let off = (next_u64(state) % (text.len() - len) as u64) as usize;
    text[off..off + len].to_vec()
}

fn sampled_pattern(text: &[u8], state: &mut u64, flavour: usize) -> String {
    let s = sample_substring(text, state, 5);
    let middle = sample_substring(text, state, 3);
    let s_str = String::from_utf8_lossy(&s).to_string();
    let m_str = String::from_utf8_lossy(&middle).to_string();
    if flavour == 0 {
        format!("{}\\w+{}", regex_escape(&s_str), regex_escape(&m_str))
    } else {
        format!("^{}\\w+{}", regex_escape(&s_str), regex_escape(&m_str))
    }
}

fn random_pattern(state: &mut u64) -> String {
    let head = (0..3)
        .map(|_| rand_alnum_byte(state) as char)
        .collect::<String>();
    let tail = (0..3)
        .map(|_| rand_alnum_byte(state) as char)
        .collect::<String>();
    format!("{}\\w+{}", regex_escape(&head), regex_escape(&tail))
}

fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        // Escape the metacharacters we might emit. The query
        // alphabet is alnum + space, so the only risk is the
        // space (treated as a literal) and the rare zero byte;
        // play it safe and escape the standard set anyway.
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

// --- Bloom-off baseline for the text bench --------------------

/// `dyntext::TextIndex::search_substring` minus the per-doc
/// bloom-filter recheck. Re-uses the public `postings` and
/// `docs` accessors so the bench stays out of `dyntext`'s
/// internals.
fn search_substring_without_bloom(idx: &TextIndex, query: &[u8]) -> Vec<u32> {
    if query.is_empty() {
        return idx.docs().keys().copied().collect();
    }
    if query.len() < dyntext::MIN_TRIGRAM_QUERY_LEN {
        let mut out = Vec::new();
        for (id, doc) in idx.docs() {
            if doc.text.windows(query.len()).any(|w| w == query) {
                out.push(*id);
            }
        }
        return out;
    }
    let qtris = trigram::extract_query_trigram_set(query);
    if qtris.is_empty() {
        let mut out = Vec::new();
        for (id, doc) in idx.docs() {
            if doc.text.windows(query.len()).any(|w| w == query) {
                out.push(*id);
            }
        }
        return out;
    }
    let candidates = idx.postings().intersect(&qtris);
    if candidates.is_empty() {
        return Vec::new();
    }
    let mut hits: Vec<u32> = Vec::new();
    for doc_id in &candidates {
        if let Some(doc) = idx.docs().get(&doc_id) {
            if doc.text.windows(query.len()).any(|w| w == query) {
                hits.push(doc_id);
            }
        }
    }
    hits.sort_unstable();
    hits
}

// --- Bench groups --------------------------------------------

const VECTOR_CODECS: &[(Codec, &str, IndexAlgorithm)] = &[
    (Codec::Fp16, "fp16", IndexAlgorithm::Hnsw),
    (Codec::Int8Quantized, "int8", IndexAlgorithm::Hnsw),
    (Codec::Turbovec4Bit, "tv4b", IndexAlgorithm::Flat),
    (Codec::Turbovec2Bit, "tv2b", IndexAlgorithm::Flat),
    // HNSW topology over turbovec packed codes; the speedup
    // over the brute SIMD scan kicks in once the corpus
    // crosses ~50k vectors. Below that, the brute scan's
    // SIMD throughput beats HNSW's traversal overhead.
    (Codec::Turbovec4Bit, "tv4b_hnsw", IndexAlgorithm::Hnsw),
    (Codec::Turbovec2Bit, "tv2b_hnsw", IndexAlgorithm::Hnsw),
];

fn bench_vector_knn(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_knn_latency");
    let n_pct = query_count();
    for &corpus in corpus_sizes() {
        for &(codec, label, algorithm) in VECTOR_CODECS {
            let store = build_vector_store(corpus, codec, algorithm);
            let queries = vector_queries(256, 0x00C0_FFEE_u64.wrapping_add(corpus as u64));

            let id = BenchmarkId::new(label, corpus);
            group.bench_with_input(id, &(store, queries), |b, (store, queries)| {
                let mut q_idx: usize = 0;
                b.iter(|| {
                    let q = &queries[q_idx % queries.len()];
                    q_idx = q_idx.wrapping_add(1);
                    let hits = store
                        .search("t", q, 10, None)
                        .expect("invariant: fixed schema");
                    black_box(hits);
                });
            });

            if n_pct > 0 {
                let store = build_vector_store(corpus, codec, algorithm);
                let queries = vector_queries(n_pct, 0xBEEF_u64.wrapping_add(corpus as u64));
                let mut durations = Vec::with_capacity(n_pct);
                for q in &queries {
                    let t0 = Instant::now();
                    let hits = store
                        .search("t", q, 10, None)
                        .expect("invariant: fixed schema");
                    durations.push(t0.elapsed());
                    black_box(hits);
                }
                record("vector_knn_latency", corpus, label, &mut durations);
            }
        }
    }
    group.finish();
}

fn bench_text_substring(c: &mut Criterion) {
    let mut group = c.benchmark_group("text_substring_latency");
    let n_pct = query_count();
    for &corpus in corpus_sizes() {
        let idx = build_text_index(corpus);
        let queries = text_substring_queries(&idx, 256, 0xFEED_u64.wrapping_add(corpus as u64));

        // bloom_on: stock TextIndex::search_substring path.
        {
            let id = BenchmarkId::new("bloom_on", corpus);
            group.bench_with_input(id, &(&idx, &queries), |b, (idx, queries)| {
                let mut q_idx = 0_usize;
                b.iter(|| {
                    let q = &queries[q_idx % queries.len()];
                    q_idx = q_idx.wrapping_add(1);
                    black_box(idx.search_substring(q));
                });
            });

            if n_pct > 0 {
                let pct_queries =
                    text_substring_queries(&idx, n_pct, 0xFACE_u64.wrapping_add(corpus as u64));
                let mut durations = Vec::with_capacity(n_pct);
                for q in &pct_queries {
                    let t0 = Instant::now();
                    let hits = idx.search_substring(q);
                    durations.push(t0.elapsed());
                    black_box(hits);
                }
                record("text_substring_latency", corpus, "bloom_on", &mut durations);
            }
        }

        // bloom_off: the same trigram + recheck pipeline, with
        // the per-doc bloom filter step skipped.
        {
            let id = BenchmarkId::new("bloom_off", corpus);
            group.bench_with_input(id, &(&idx, &queries), |b, (idx, queries)| {
                let mut q_idx = 0_usize;
                b.iter(|| {
                    let q = &queries[q_idx % queries.len()];
                    q_idx = q_idx.wrapping_add(1);
                    black_box(search_substring_without_bloom(idx, q));
                });
            });

            if n_pct > 0 {
                let pct_queries =
                    text_substring_queries(&idx, n_pct, 0xBAD0_u64.wrapping_add(corpus as u64));
                let mut durations = Vec::with_capacity(n_pct);
                for q in &pct_queries {
                    let t0 = Instant::now();
                    let hits = search_substring_without_bloom(&idx, q);
                    durations.push(t0.elapsed());
                    black_box(hits);
                }
                record(
                    "text_substring_latency",
                    corpus,
                    "bloom_off",
                    &mut durations,
                );
            }
        }
    }
    group.finish();
}

fn bench_regex_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("regex_search_latency");
    let n_pct = query_count();
    for &corpus in corpus_sizes() {
        let idx = build_text_index(corpus);
        let queries = regex_queries(&idx, 64, 0xCAFE_u64.wrapping_add(corpus as u64));

        // K=0: exact regex via TextIndex::search_regex.
        {
            let id = BenchmarkId::new("k0", corpus);
            group.bench_with_input(id, &(&idx, &queries), |b, (idx, queries)| {
                let mut q_idx = 0_usize;
                b.iter(|| {
                    let q = &queries[q_idx % queries.len()];
                    q_idx = q_idx.wrapping_add(1);
                    let hits = idx
                        .search_regex(q)
                        .expect("invariant: well-formed pattern alphabet");
                    black_box(hits);
                });
            });

            if n_pct > 0 {
                let pct_queries =
                    regex_queries(&idx, n_pct, 0xDEAD_u64.wrapping_add(corpus as u64));
                let mut durations = Vec::with_capacity(n_pct);
                for q in &pct_queries {
                    let t0 = Instant::now();
                    let hits = idx.search_regex(q).expect("invariant: well-formed pattern");
                    durations.push(t0.elapsed());
                    black_box(hits);
                }
                record("regex_search_latency", corpus, "k0", &mut durations);
            }
        }

        // K=1, K=2: TRE approximate match. The current
        // search_regex_approx path is a full scan; cap the
        // percentile pass at 100 queries so the 100k corpus
        // case stays inside the bench budget.
        for &k in &[1_u16, 2_u16] {
            let label = if k == 1 { "k1" } else { "k2" };
            let id = BenchmarkId::new(label, corpus);
            group.bench_with_input(id, &(&idx, &queries), |b, (idx, queries)| {
                let mut q_idx = 0_usize;
                b.iter(|| {
                    let q = &queries[q_idx % queries.len()];
                    q_idx = q_idx.wrapping_add(1);
                    let hits = idx
                        .search_regex_approx(q, k)
                        .expect("invariant: well-formed pattern alphabet");
                    black_box(hits);
                });
            });

            if n_pct > 0 {
                let cap = approx_cap(n_pct, corpus);
                let pct_queries = regex_queries(
                    &idx,
                    cap,
                    0xC0DE_u64.wrapping_add(corpus as u64) ^ u64::from(k),
                );
                let mut durations = Vec::with_capacity(cap);
                for q in &pct_queries {
                    let t0 = Instant::now();
                    let hits = idx.search_regex_approx(q, k).expect("invariant: pattern");
                    durations.push(t0.elapsed());
                    black_box(hits);
                }
                record("regex_search_latency", corpus, label, &mut durations);
            }
        }

        // Sanity-check: a TRE pattern compiles. This catches a
        // build-time linkage regression early in the bench
        // run; it runs once per corpus size and is not timed.
        let _smoke = TreCompiledPattern::compile(b"^abc.*def", TreMatchOpts::default())
            .expect("invariant: TRE links and compiles a fixed pattern");
    }
    group.finish();
}

/// Tighten the percentile-pass iteration count for the
/// expensive TRE full-scan path so the 100k corpus does not
/// blow the bench budget.
fn approx_cap(n_pct: usize, corpus: usize) -> usize {
    if corpus >= 100_000 {
        n_pct.min(50)
    } else if corpus >= 10_000 {
        n_pct.min(200)
    } else {
        n_pct
    }
}

// --- Output emission -----------------------------------------

fn target_dir() -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let manifest = std::env::var("CARGO_MANIFEST_DIR").ok();
    let candidate_a = cwd.join("target");
    if let Some(dir) = manifest {
        let p = PathBuf::from(dir).join("..").join("..").join("target");
        if p.exists() {
            return p;
        }
    }
    candidate_a
}

fn duration_us(d: Duration) -> f64 {
    #[allow(clippy::cast_precision_loss, reason = "bench fixture: nanos < 2^53")]
    let us = (d.as_nanos() as f64) / 1000.0;
    us
}

fn write_outputs() {
    let records = REGISTRY
        .lock()
        .expect("invariant: REGISTRY mutex never poisoned in benches")
        .clone();
    if records.is_empty() {
        return;
    }
    write_json_sidecars(&records);
    if let Err(e) = write_markdown(&records) {
        eprintln!("ft_search: failed to write markdown summary: {e}");
    }
}

fn write_json_sidecars(records: &[PercentileRecord]) {
    let base = target_dir().join("criterion").join("ft_search");
    let mut by_group: HashMap<&str, Vec<&PercentileRecord>> = HashMap::new();
    for rec in records {
        by_group.entry(rec.group.as_str()).or_default().push(rec);
    }
    for (group, recs) in by_group {
        let dir = base.join(group);
        if let Err(e) = fs::create_dir_all(&dir) {
            eprintln!("ft_search: cannot create {}: {e}", dir.display());
            continue;
        }
        let path = dir.join("percentiles.json");
        let mut s = String::new();
        s.push_str("{\n  \"records\": [\n");
        for (i, r) in recs.iter().enumerate() {
            s.push_str("    {");
            let _ = write!(
                s,
                "\"corpus\": {}, \"variant\": \"{}\", \"samples\": {}, ",
                r.corpus, r.variant, r.samples
            );
            let _ = write!(
                s,
                "\"p50_us\": {:.3}, \"p95_us\": {:.3}, \"p99_us\": {:.3}",
                duration_us(r.p50),
                duration_us(r.p95),
                duration_us(r.p99)
            );
            s.push('}');
            if i + 1 < recs.len() {
                s.push(',');
            }
            s.push('\n');
        }
        s.push_str("  ]\n}\n");
        if let Err(e) = fs::write(&path, s) {
            eprintln!("ft_search: cannot write {}: {e}", path.display());
        } else {
            eprintln!("ft_search: wrote {}", path.display());
        }
    }
}

fn corpus_label(n: usize) -> String {
    if n.is_multiple_of(1_000_000) {
        format!("{}m", n / 1_000_000)
    } else if n.is_multiple_of(1_000) {
        format!("{}k", n / 1_000)
    } else {
        n.to_string()
    }
}

fn fmt_us(d: Duration) -> String {
    let us = duration_us(d);
    if us >= 1000.0 {
        format!("{:.2} ms", us / 1000.0)
    } else if us >= 10.0 {
        format!("{us:.1} us")
    } else {
        format!("{us:.2} us")
    }
}

fn write_markdown(records: &[PercentileRecord]) -> std::io::Result<()> {
    // The path is repository-relative and reachable from the
    // `cargo bench` working directory (the workspace root).
    let path = PathBuf::from("docs/dynvec/bench-results.md");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut s = String::new();
    s.push_str("# FT.SEARCH end-to-end latency\n\n");
    s.push_str(
        "Generated by `cargo bench -p dynomite --bench ft_search`. \
Each row is the per-query wall-clock latency observed by a single \
client against a fully built corpus. Build cost is excluded from \
the measurement; only the search call is timed.\n\n",
    );
    let mut by_group: HashMap<&str, Vec<&PercentileRecord>> = HashMap::new();
    for rec in records {
        by_group.entry(rec.group.as_str()).or_default().push(rec);
    }
    let order = [
        "vector_knn_latency",
        "text_substring_latency",
        "regex_search_latency",
    ];
    for group in order {
        let Some(recs) = by_group.get(group) else {
            continue;
        };
        let _ = writeln!(s, "## {group}\n");
        s.push_str("| corpus | variant | p50 | p95 | p99 | samples |\n");
        s.push_str("| ------ | ------- | --- | --- | --- | ------- |\n");
        let mut sorted: Vec<&PercentileRecord> = (*recs).clone();
        sorted.sort_by(|a, b| a.corpus.cmp(&b.corpus).then(a.variant.cmp(&b.variant)));
        for r in sorted {
            let _ = writeln!(
                s,
                "| {} | {} | {} | {} | {} | {} |",
                corpus_label(r.corpus),
                r.variant,
                fmt_us(r.p50),
                fmt_us(r.p95),
                fmt_us(r.p99),
                r.samples
            );
        }
        s.push('\n');
    }
    fs::write(&path, s)?;
    eprintln!("ft_search: wrote {}", path.display());
    Ok(())
}

// --- main ----------------------------------------------------

fn finalise() {
    if !is_test_mode() {
        write_outputs();
    }
}

criterion_group!(
    benches,
    bench_vector_knn,
    bench_text_substring,
    bench_regex_search,
);

fn main() {
    benches();
    finalise();
    Criterion::default().configure_from_args().final_summary();
}
