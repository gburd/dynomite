//! HNSW recall test.
//!
//! Inserts N=1000 random 64-dim vectors, runs 50 top-10 ANN
//! queries, and compares against brute-force ground truth.
//! Asserts recall@10 > 0.85.

use dynvec::distance::Distance;
use dynvec::index::{HnswIndex, HnswParams, NodeId};

const N: u64 = 1000;
const DIM: usize = 64;
const QUERIES: u64 = 50;
const K: usize = 10;
const RECALL_TARGET: f32 = 0.85;

fn rand_vec(seed: u64, dim: usize) -> Vec<f32> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    let mut v = Vec::with_capacity(dim);
    for _ in 0..dim {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let bits = (x >> 11) & ((1_u64 << 53) - 1);
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            reason = "test fixture: deterministic PRNG narrowed to f32"
        )]
        let r = (((bits as f64) / ((1_u64 << 53) as f64)) * 2.0 - 1.0) as f32;
        v.push(r);
    }
    v
}

fn brute_force_topk(
    distance: Distance,
    points: &[(NodeId, Vec<f32>)],
    query: &[f32],
    k: usize,
) -> Vec<NodeId> {
    let mut scored: Vec<(NodeId, f32)> = points
        .iter()
        .map(|(id, v)| (*id, distance.score(query, v)))
        .collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.into_iter().take(k).map(|(id, _)| id).collect()
}

#[test]
fn recall_at_10_is_above_85_percent_euclidean() {
    let points: Vec<(NodeId, Vec<f32>)> = (0..N).map(|i| (i, rand_vec(i, DIM))).collect();
    let mut idx = HnswIndex::new(Distance::Euclidean, HnswParams::default());
    for (id, v) in &points {
        idx.insert(*id, v.clone()).unwrap();
    }

    let mut total_hits = 0_usize;
    let mut total_expected = 0_usize;
    for q in 0..QUERIES {
        let query = rand_vec(N + 1 + q, DIM);
        let truth: std::collections::HashSet<NodeId> =
            brute_force_topk(Distance::Euclidean, &points, &query, K)
                .into_iter()
                .collect();
        let ann: std::collections::HashSet<NodeId> = idx
            .search(&query, K, Some(100))
            .unwrap()
            .into_iter()
            .map(|r| r.id)
            .collect();
        total_hits += truth.intersection(&ann).count();
        total_expected += K;
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "test counts are small (50*10=500); f32 conversion is exact"
    )]
    let recall = total_hits as f32 / total_expected as f32;
    eprintln!("euclidean recall@{K} = {recall:.3} (target > {RECALL_TARGET})");
    assert!(recall > RECALL_TARGET, "recall {recall} <= {RECALL_TARGET}");
}

#[test]
fn recall_at_10_is_above_85_percent_cosine() {
    let points: Vec<(NodeId, Vec<f32>)> = (0..N).map(|i| (i, rand_vec(i, DIM))).collect();
    let mut idx = HnswIndex::new(Distance::Cosine, HnswParams::default());
    for (id, v) in &points {
        idx.insert(*id, v.clone()).unwrap();
    }

    let mut total_hits = 0_usize;
    let mut total_expected = 0_usize;
    for q in 0..QUERIES {
        let query = rand_vec(N + 1 + q, DIM);
        let truth: std::collections::HashSet<NodeId> =
            brute_force_topk(Distance::Cosine, &points, &query, K)
                .into_iter()
                .collect();
        let ann: std::collections::HashSet<NodeId> = idx
            .search(&query, K, Some(100))
            .unwrap()
            .into_iter()
            .map(|r| r.id)
            .collect();
        total_hits += truth.intersection(&ann).count();
        total_expected += K;
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "test counts are small (50*10=500); f32 conversion is exact"
    )]
    let recall = total_hits as f32 / total_expected as f32;
    eprintln!("cosine recall@{K} = {recall:.3} (target > {RECALL_TARGET})");
    assert!(recall > RECALL_TARGET, "recall {recall} <= {RECALL_TARGET}");
}

#[test]
fn delete_after_insert_round_trip() {
    let mut idx = HnswIndex::new(Distance::Euclidean, HnswParams::default());
    for i in 0..200_u64 {
        idx.insert(i, rand_vec(i, 8)).unwrap();
    }
    assert_eq!(idx.len(), 200);
    for i in 0..50_u64 {
        assert!(idx.delete(i));
    }
    assert_eq!(idx.len(), 150);
    let q = rand_vec(0, 8);
    let res = idx.search(&q, 10, None).unwrap();
    for r in res {
        assert!(r.id >= 50);
    }
}
