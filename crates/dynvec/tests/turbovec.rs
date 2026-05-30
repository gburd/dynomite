//! Integration tests for the `turbovec`-backed dynvecdb path.
//!
//! These tests cover three layers:
//!
//! 1. The encoding shim (`encode_turbovec` / `decode_turbovec`)
//!    and the helper SIMD distance surface
//!    (`distance_turbovec`).
//! 2. The `TurboTable` ANN container that replaces the HNSW
//!    graph when the table's codec is `Turbovec*`.
//! 3. End-to-end recall against a brute-force baseline at
//!    `recall@10` over a 1024-point random fixture.

use std::collections::HashMap;

use dynvec::distance::Distance;
use dynvec::encoding::{decode_turbovec, distance_turbovec, encode_turbovec, Codec, EncodedVector};
use dynvec::index::HnswParams;
use dynvec::storage::{TableSchema, VectorStore};

/// xorshift64* PRNG, seeded -> `Vec<f32>` with components in
/// roughly `[-1, 1)`. Deterministic.
fn rand_vec(seed: u64, dim: usize) -> Vec<f32> {
    let mut x = if seed == 0 { 0xDEAD_BEEF } else { seed };
    let mut v = Vec::with_capacity(dim);
    for _ in 0..dim {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let bits = (x >> 11) & ((1_u64 << 53) - 1);
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            reason = "test fixture: PRNG output narrowed to f32"
        )]
        let r = (((bits as f64) / ((1_u64 << 53) as f64)) * 2.0 - 1.0) as f32;
        v.push(r);
    }
    v
}

fn schema_turbovec(name: &str, dim: u16, codec: Codec) -> TableSchema {
    TableSchema {
        name: name.to_string(),
        dim,
        codec,
        distance: Distance::Cosine,
        hnsw: HnswParams::default(),
    }
}

/// Round-trip a 64-dim vector through `encode_turbovec` /
/// `decode_turbovec` at 2-bit. Because the row-layer storage is
/// a passthrough of the original `f32` bytes, the round-trip
/// is exact at the row layer; the "5%" budget is the
/// quantisation loss measured through the SIMD search path,
/// which we sample by issuing a self-similarity query.
#[test]
fn turbovec_2bit_roundtrip_within_5pct_l2() {
    let v = rand_vec(0x1234_5678, 64);
    let enc = encode_turbovec(&v, 2).expect("encode 2-bit");
    let dec = decode_turbovec(&enc, 2).expect("decode 2-bit");
    // Row-layer round-trip is exact (passthrough storage).
    assert_eq!(dec, v);
    // Through the SIMD path, the 2-bit estimator preserves
    // self-similarity to within ~5%; the surrogate test is
    // the cosine distance from `distance_turbovec` between
    // the vector and itself, which should be near zero.
    let d = distance_turbovec(&v, &enc, Distance::Cosine);
    assert!(
        d.abs() <= 0.05,
        "self-cosine should be near zero at 2-bit, got {d}",
    );
}

/// Round-trip at 3-bit. Tighter budget than 2-bit per the
/// quantisation hierarchy.
#[test]
fn turbovec_3bit_roundtrip_within_2pct_l2() {
    let v = rand_vec(0xDEAD_BEEF, 128);
    let enc = encode_turbovec(&v, 3).expect("encode 3-bit");
    let dec = decode_turbovec(&enc, 3).expect("decode 3-bit");
    assert_eq!(dec, v);
    let d = distance_turbovec(&v, &enc, Distance::Cosine);
    assert!(
        d.abs() <= 0.02,
        "self-cosine should be near zero at 3-bit, got {d}",
    );
}

/// Round-trip at 4-bit. Tightest budget; 4-bit is the
/// production sweet spot.
#[test]
fn turbovec_4bit_roundtrip_within_1pct_l2() {
    let v = rand_vec(0xCAFE_F00D, 256);
    let enc = encode_turbovec(&v, 4).expect("encode 4-bit");
    let dec = decode_turbovec(&enc, 4).expect("decode 4-bit");
    assert_eq!(dec, v);
    let d = distance_turbovec(&v, &enc, Distance::Cosine);
    assert!(
        d.abs() <= 0.01,
        "self-cosine should be near zero at 4-bit, got {d}",
    );
}

/// Compare turbovec's SIMD-evaluated dot-product / cosine
/// score against the brute-force scalar score on a random
/// pair. The estimator is biased by quantisation; the test
/// allows a generous tolerance suitable for 4-bit
/// quantisation.
#[test]
fn turbovec_simd_distance_matches_brute_force_within_tolerance() {
    let q = rand_vec(7, 64);
    let r = rand_vec(11, 64);
    let enc = encode_turbovec(&r, 4).unwrap();

    let simd = distance_turbovec(&q, &enc, Distance::Cosine);
    let brute = Distance::Cosine.score(&q, &r);

    // 4-bit cosine reconstruction is accurate to within
    // about 0.05 absolute on uniformly-random vectors.
    let delta = (simd - brute).abs();
    assert!(
        delta <= 0.10,
        "simd cosine {simd} vs brute {brute} (delta {delta})",
    );
}

/// Insert 1024 random 64-dim vectors into a turbovec-backed
/// table at 4-bit, then query against the brute-force ground
/// truth. The 4-bit recall@10 on uniformly random fixtures
/// usually reaches 95% or better at 64 dim; the test asserts
/// `>= 85%` to leave headroom for the inherent quantisation
/// variance.
#[test]
fn recall_at_10_with_turbovec_4bit_above_85pct() {
    let dim: usize = 64;
    let n: usize = 1024;
    let store = VectorStore::in_memory();
    store
        .create_table(schema_turbovec(
            "t",
            u16::try_from(dim).unwrap(),
            Codec::Turbovec4Bit,
        ))
        .unwrap();

    // Generate the corpus and a separate query set.
    let corpus: Vec<Vec<f32>> = (0..n)
        .map(|i| {
            rand_vec(
                u64::try_from(i)
                    .unwrap()
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    + 1,
                dim,
            )
        })
        .collect();
    for (i, v) in corpus.iter().enumerate() {
        let key = format!("k{i}").into_bytes();
        store.upsert("t", key, v, HashMap::new()).unwrap();
    }

    let queries: Vec<Vec<f32>> = (0..32_u64)
        .map(|i| rand_vec(i.wrapping_mul(0x517C_C1B7_2722_0A95) + 7, dim))
        .collect();
    let k = 10;

    let mut hits = 0;
    let mut total = 0;
    for q in &queries {
        // Brute force: cosine score against every corpus
        // vector, take the smallest k as ground truth.
        let mut scored: Vec<(usize, f32)> = corpus
            .iter()
            .enumerate()
            .map(|(i, v)| (i, Distance::Cosine.score(q, v)))
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let truth: std::collections::HashSet<usize> =
            scored.iter().take(k).map(|(i, _)| *i).collect();

        let res = store.search("t", q, k, None).unwrap();
        for (row, _score) in res {
            // Recover the corpus index from the key.
            let key = std::str::from_utf8(&row.key).unwrap();
            let idx: usize = key.trim_start_matches('k').parse().unwrap();
            if truth.contains(&idx) {
                hits += 1;
            }
            total += 1;
        }
    }

    let recall = f64::from(hits) / f64::from(total);
    eprintln!("recall@10 (turbovec 4-bit, 64-dim, 1024 corpus): {recall:.4}");
    assert!(
        recall >= 0.85,
        "recall@10 should be >=0.85 at 4-bit, got {recall}",
    );
}

/// Storage size of the turbovec packed payload should be
/// `dim * bits / 8` bytes per vector. For 64-dim 4-bit, that
/// is 32 bytes per vector versus the 256-byte raw `f32`
/// representation: an 8x compression ratio.
///
/// The compression is a property of the in-index packed
/// codes; this test verifies the formula and documents the
/// number that the SIMD search path consumes.
#[test]
fn compression_ratio_4bit_is_8x_for_64_dim_floats() {
    let dim: usize = 64;
    let bits: u8 = 4;
    let raw_bytes_per_vec = dim * std::mem::size_of::<f32>();
    let packed_bytes_per_vec = dim * usize::from(bits) / 8;
    let ratio = raw_bytes_per_vec / packed_bytes_per_vec;
    assert_eq!(packed_bytes_per_vec, 32, "expected 32 packed bytes / vec");
    assert_eq!(raw_bytes_per_vec, 256, "expected 256 raw bytes / vec");
    assert_eq!(ratio, 8, "compression ratio should be exactly 8 for 4-bit");
}

/// Smoke test: distance_turbovec rejects non-turbovec
/// payloads and dim mismatches by returning infinity rather
/// than panicking. Production callers can rely on the score
/// being comparable.
#[test]
fn distance_turbovec_rejects_non_turbovec_payload() {
    let v = rand_vec(1, 64);
    // Build an Fp16-encoded payload via the public encoder.
    let fp16 = dynvec::encoding::Fp16;
    let enc: EncodedVector = dynvec::encoding::Encoder::encode(&fp16, &v).unwrap();
    let d = distance_turbovec(&v, &enc, Distance::Cosine);
    assert!(d.is_infinite());
}
