//! Wire-protocol tests for cluster-coordinated FT.SEARCH.
//!
//! These tests exercise the distributed FT.SEARCH path end to
//! end without standing up a real network: each "peer" is a
//! [`dynomite::vector::registry::VectorRegistry`] handle held
//! in-process. The simulated probe round-trips the request and
//! reply through the on-the-wire codec
//! [`dynomite::vector::wire`] so the tests cover both the
//! merge logic and the byte format the production peer plane
//! will exchange.
//!
//! The pattern is the in-process-cluster pattern used by
//! `crates/dynomite/tests/read_repair.rs`: build N registries,
//! seed each with a disjoint slice of documents, and then
//! drive [`dynomite::vector::query_fsm::broadcast`] with a
//! probe that dispatches to the right registry.
//!
//! Test list (matching the brief):
//!
//! 1. `ft_search_routes_to_local_peer_when_no_remote_peers`
//!    -- single-peer cluster.
//! 2. `ft_search_broadcasts_to_two_peers_and_merges_results`.
//! 3. `ft_search_merge_returns_global_top_k`.
//! 4. `ft_search_one_peer_timeout_returns_partial_with_warning`.
//! 5. `ft_search_all_peers_timeout_returns_empty_with_warning`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dynomite::embed::events::PeerId;
use dynomite::vector::query_fsm::{
    broadcast, AsyncPeerProbe, BroadcastRequest, HitWithScore, MergeOrder, PeerReply,
    SerializedQuery,
};
use dynomite::vector::registry::VectorRegistry;
use dynomite::vector::schema::{
    DistanceMetric, IndexAlgorithm, MetadataField, MetadataFieldType, VectorSchema, VectorType,
};
use dynomite::vector::wire::{decode_reply, decode_request, encode_reply, encode_request};
use serde_json::Value;

const TABLE: &str = "ix";
const VECTOR_FIELD: &str = "v";
const META_FIELD: &str = "title";
const DIM: u16 = 4;

/// Build the per-peer registry with a single 4-dim cosine
/// HNSW index. Each peer hosts the same schema so the
/// cluster-wide table looks consistent end to end.
fn fresh_registry() -> Arc<VectorRegistry> {
    let registry = Arc::new(VectorRegistry::new());
    let schema = VectorSchema {
        vector_field: VECTOR_FIELD.into(),
        vector_type: VectorType::Float32,
        dim: DIM,
        distance: DistanceMetric::Cosine,
        algorithm: IndexAlgorithm::Hnsw,
        prefixes: vec![b"doc:".to_vec()],
        metadata_fields: vec![MetadataField {
            name: META_FIELD.into(),
            field_type: MetadataFieldType::Text,
        }],
    };
    registry.create(TABLE.into(), schema).unwrap();
    registry
}

/// Insert one document into the named index on `registry`.
fn seed_doc(registry: &VectorRegistry, key: &[u8], vector: [f32; 4], title: &str) {
    let table = registry.get(TABLE).expect("index registered");
    let mut metadata: HashMap<String, Value> = HashMap::new();
    metadata.insert(META_FIELD.into(), Value::String(title.into()));
    table
        .engine
        .upsert(key.to_vec(), &vector, metadata)
        .expect("upsert ok");
    table.record_indexed_key(key.to_vec());
}

/// Encode a 4-element f32 vector to little-endian bytes (the
/// wire shape produced by the FT.SEARCH parser).
fn vec_to_le(values: [f32; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

/// Build a [`BroadcastRequest`] for a k-NN over `vector`.
fn knn_request(top_k: u32, vector: [f32; 4]) -> BroadcastRequest {
    BroadcastRequest {
        table: TABLE.into(),
        query: SerializedQuery::Knn {
            vector_field: VECTOR_FIELD.into(),
            vector_bytes: vec_to_le(vector),
            ef: None,
        },
        top_k,
    }
}

/// Run a [`BroadcastRequest`] against a single registry and
/// return the per-peer top-K hit list, exactly as a peer would
/// return it. The conversion mirrors what
/// `proto::redis::ft::execute_search` does at the local-only
/// path, but expressed in terms of [`HitWithScore`] (the
/// cluster-coordinator hit shape).
fn run_local_search(registry: &VectorRegistry, req: &BroadcastRequest) -> Vec<HitWithScore> {
    let table = registry.get(&req.table).expect("table present");
    match &req.query {
        SerializedQuery::Knn { vector_bytes, .. } => {
            assert!(
                vector_bytes.len() == usize::from(table.schema.dim) * 4,
                "test rig uses fixed dim={}",
                table.schema.dim
            );
            let mut query = Vec::with_capacity(usize::from(table.schema.dim));
            for chunk in vector_bytes.chunks_exact(4) {
                let arr = [chunk[0], chunk[1], chunk[2], chunk[3]];
                query.push(f32::from_le_bytes(arr));
            }
            let hits = table
                .engine
                .search(&query, req.top_k as usize, None)
                .expect("local search ok");
            hits.into_iter()
                .map(|(row, score)| HitWithScore {
                    doc_id: row.key,
                    score,
                })
                .collect()
        }
        SerializedQuery::Text { field, query } => table
            .search_text_substring(field, query)
            .unwrap_or_default()
            .into_iter()
            .map(|(key, _text)| HitWithScore {
                doc_id: key,
                score: 0.0,
            })
            .collect(),
        SerializedQuery::Regex {
            field,
            pattern,
            max_errors,
        } => {
            if *max_errors == 0 {
                table
                    .search_text_regex(field, pattern)
                    .and_then(Result::ok)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(key, _text)| HitWithScore {
                        doc_id: key,
                        score: 0.0,
                    })
                    .collect()
            } else {
                table
                    .search_text_regex_approx(field, pattern, *max_errors)
                    .and_then(Result::ok)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(key, _text)| HitWithScore {
                        doc_id: key,
                        score: 0.0,
                    })
                    .collect()
            }
        }
    }
}

/// Build an [`AsyncPeerProbe`] that simulates the production
/// peer plane:
///
/// 1. Encode the request via the wire codec.
/// 2. Decode it on the "peer side" (assert the round-trip is
///    exact).
/// 3. Run the local search against the per-peer registry.
/// 4. Encode the reply via the wire codec.
/// 5. Decode it back on the "coordinator side" (asserting the
///    reply round-trip is exact too) and return the hits.
///
/// The optional `latency` map artificially slows specific peers
/// so the tests can exercise the per-peer-timeout path.
fn make_probe(
    registries: HashMap<PeerId, Arc<VectorRegistry>>,
    latency: HashMap<PeerId, Duration>,
) -> AsyncPeerProbe {
    Arc::new(move |peer, req| {
        let registry = registries.get(&peer).cloned();
        let delay = latency.get(&peer).copied();
        Box::pin(async move {
            if let Some(d) = delay {
                tokio::time::sleep(d).await;
            }
            let registry = registry.ok_or_else(|| format!("unknown peer {peer}"))?;
            // 1+2: wire round-trip on the request side.
            let encoded = encode_request(&req);
            let decoded = decode_request(&encoded).map_err(|e| format!("decode req: {e:?}"))?;
            assert_eq!(decoded, req, "request round-trip must be exact");
            // 3: local search on the peer's registry.
            let hits = run_local_search(&registry, &decoded);
            // 4+5: wire round-trip on the reply side.
            let reply = PeerReply {
                hits: hits.clone(),
                timed_out: false,
            };
            let bytes = encode_reply(&reply);
            let back = decode_reply(&bytes).map_err(|e| format!("decode rep: {e:?}"))?;
            assert_eq!(back, reply, "reply round-trip must be exact");
            Ok(back.hits)
        })
    })
}

// ---- 1. single-peer ---------------------------------------------------

#[tokio::test]
async fn ft_search_routes_to_local_peer_when_no_remote_peers() {
    let r0 = fresh_registry();
    seed_doc(&r0, b"doc:1", [1.0, 0.0, 0.0, 0.0], "alpha");
    seed_doc(&r0, b"doc:2", [0.9, 0.1, 0.0, 0.0], "bravo");
    seed_doc(&r0, b"doc:3", [0.0, 1.0, 0.0, 0.0], "charlie");

    let mut registries: HashMap<PeerId, Arc<VectorRegistry>> = HashMap::new();
    registries.insert(0, Arc::clone(&r0));
    let probe = make_probe(registries, HashMap::new());

    let resp = broadcast(
        knn_request(2, [1.0, 0.0, 0.0, 0.0]),
        vec![0],
        probe,
        Duration::from_millis(500),
        MergeOrder::ScoreAscending,
    )
    .await
    .unwrap();
    assert_eq!(resp.peers_consulted, 1);
    assert_eq!(resp.peers_timed_out, 0);
    assert!(!resp.partial);
    assert_eq!(resp.hits.len(), 2);
    // The closest-cosine match is doc:1 (parallel to query).
    assert_eq!(resp.hits[0].doc_id, b"doc:1");
    assert_eq!(resp.hits[1].doc_id, b"doc:2");
}

// ---- 2. broadcast to two peers and merges ------------------------------

#[tokio::test]
async fn ft_search_broadcasts_to_two_peers_and_merges_results() {
    let r0 = fresh_registry();
    let r1 = fresh_registry();
    // Disjoint corpus per peer (typical Dynomite shard layout).
    seed_doc(&r0, b"doc:1", [1.0, 0.0, 0.0, 0.0], "alpha");
    seed_doc(&r0, b"doc:2", [0.9, 0.1, 0.0, 0.0], "bravo");
    seed_doc(&r1, b"doc:3", [0.95, 0.05, 0.0, 0.0], "charlie");
    seed_doc(&r1, b"doc:4", [0.0, 1.0, 0.0, 0.0], "delta");

    let mut registries: HashMap<PeerId, Arc<VectorRegistry>> = HashMap::new();
    registries.insert(0, Arc::clone(&r0));
    registries.insert(1, Arc::clone(&r1));
    let probe = make_probe(registries, HashMap::new());

    let resp = broadcast(
        knn_request(3, [1.0, 0.0, 0.0, 0.0]),
        vec![0, 1],
        probe,
        Duration::from_millis(500),
        MergeOrder::ScoreAscending,
    )
    .await
    .unwrap();
    assert_eq!(resp.peers_consulted, 2);
    assert_eq!(resp.peers_timed_out, 0);
    assert!(!resp.partial);
    assert_eq!(resp.hits.len(), 3);
    // The merge picks the smallest-score (closest) hits across
    // both peers: doc:1 (peer 0) is exactly parallel, doc:3
    // (peer 1) is next-closest, doc:2 (peer 0) follows.
    let ids: Vec<&[u8]> = resp.hits.iter().map(|h| h.doc_id.as_slice()).collect();
    assert_eq!(ids, vec![&b"doc:1"[..], &b"doc:3"[..], &b"doc:2"[..]]);
    // Scores must be monotonic non-decreasing.
    for w in resp.hits.windows(2) {
        assert!(w[0].score <= w[1].score, "scores must be non-decreasing");
    }
}

// ---- 3. global top-K cap -----------------------------------------------

#[tokio::test]
async fn ft_search_merge_returns_global_top_k() {
    // Each of three peers holds 4 docs; the cluster has 12 in
    // total. We ask for top-3 and assert we get exactly 3 back,
    // regardless of how many each peer contributes.
    let r0 = fresh_registry();
    let r1 = fresh_registry();
    let r2 = fresh_registry();
    // Place "good" matches on different peers to force the
    // merge to actually do work (no single peer dominates).
    seed_doc(&r0, b"doc:01", [1.00, 0.00, 0.00, 0.00], "p0a");
    seed_doc(&r0, b"doc:02", [0.10, 0.99, 0.00, 0.00], "p0b");
    seed_doc(&r0, b"doc:03", [0.10, 0.99, 0.00, 0.00], "p0c");
    seed_doc(&r0, b"doc:04", [0.10, 0.99, 0.00, 0.00], "p0d");
    seed_doc(&r1, b"doc:05", [0.99, 0.01, 0.00, 0.00], "p1a");
    seed_doc(&r1, b"doc:06", [0.10, 0.99, 0.00, 0.00], "p1b");
    seed_doc(&r1, b"doc:07", [0.10, 0.99, 0.00, 0.00], "p1c");
    seed_doc(&r1, b"doc:08", [0.10, 0.99, 0.00, 0.00], "p1d");
    seed_doc(&r2, b"doc:09", [0.95, 0.05, 0.00, 0.00], "p2a");
    seed_doc(&r2, b"doc:10", [0.10, 0.99, 0.00, 0.00], "p2b");
    seed_doc(&r2, b"doc:11", [0.10, 0.99, 0.00, 0.00], "p2c");
    seed_doc(&r2, b"doc:12", [0.10, 0.99, 0.00, 0.00], "p2d");

    let mut registries: HashMap<PeerId, Arc<VectorRegistry>> = HashMap::new();
    registries.insert(10, Arc::clone(&r0));
    registries.insert(11, Arc::clone(&r1));
    registries.insert(12, Arc::clone(&r2));
    let probe = make_probe(registries, HashMap::new());

    let resp = broadcast(
        knn_request(3, [1.0, 0.0, 0.0, 0.0]),
        vec![10, 11, 12],
        probe,
        Duration::from_millis(500),
        MergeOrder::ScoreAscending,
    )
    .await
    .unwrap();
    assert_eq!(resp.peers_consulted, 3);
    assert_eq!(resp.hits.len(), 3, "global top-K cap is K");
    // The three closest-to-the-query matches are doc:01,
    // doc:05, doc:09 (one per peer). Order is by score.
    let mut ids: Vec<Vec<u8>> = resp.hits.iter().map(|h| h.doc_id.clone()).collect();
    ids.sort();
    assert_eq!(
        ids,
        vec![b"doc:01".to_vec(), b"doc:05".to_vec(), b"doc:09".to_vec()],
    );
    // Doc ids must be unique across the merged result.
    let mut deduped = ids.clone();
    deduped.dedup();
    assert_eq!(deduped, ids);
}

// ---- 4. one peer timeout -> partial result -----------------------------

#[tokio::test]
async fn ft_search_one_peer_timeout_returns_partial_with_warning() {
    let r0 = fresh_registry();
    let r1 = fresh_registry();
    seed_doc(&r0, b"doc:1", [1.0, 0.0, 0.0, 0.0], "alpha");
    seed_doc(&r1, b"doc:2", [0.95, 0.05, 0.0, 0.0], "bravo");

    let mut registries: HashMap<PeerId, Arc<VectorRegistry>> = HashMap::new();
    registries.insert(0, Arc::clone(&r0));
    registries.insert(1, Arc::clone(&r1));
    // Peer 1 is slow and will miss the per-peer deadline.
    let mut latency: HashMap<PeerId, Duration> = HashMap::new();
    latency.insert(1, Duration::from_millis(500));
    let probe = make_probe(registries, latency);

    let resp = broadcast(
        knn_request(3, [1.0, 0.0, 0.0, 0.0]),
        vec![0, 1],
        probe,
        Duration::from_millis(50),
        MergeOrder::ScoreAscending,
    )
    .await
    .unwrap();
    assert_eq!(resp.peers_consulted, 2, "every peer reports back");
    assert_eq!(resp.peers_timed_out, 1, "one peer timed out");
    assert!(resp.partial, "partial flag set when any peer times out");
    // Only the fast peer's hits survived.
    assert_eq!(resp.hits.len(), 1);
    assert_eq!(resp.hits[0].doc_id, b"doc:1");
}

// ---- 5. all peers timeout -> empty + partial ---------------------------

#[tokio::test]
async fn ft_search_all_peers_timeout_returns_empty_with_warning() {
    let r0 = fresh_registry();
    let r1 = fresh_registry();
    let r2 = fresh_registry();
    seed_doc(&r0, b"doc:1", [1.0, 0.0, 0.0, 0.0], "alpha");
    seed_doc(&r1, b"doc:2", [0.95, 0.05, 0.0, 0.0], "bravo");
    seed_doc(&r2, b"doc:3", [0.90, 0.10, 0.0, 0.0], "charlie");

    let mut registries: HashMap<PeerId, Arc<VectorRegistry>> = HashMap::new();
    registries.insert(0, Arc::clone(&r0));
    registries.insert(1, Arc::clone(&r1));
    registries.insert(2, Arc::clone(&r2));
    // Every peer is slower than the per-peer deadline.
    let mut latency: HashMap<PeerId, Duration> = HashMap::new();
    latency.insert(0, Duration::from_millis(500));
    latency.insert(1, Duration::from_millis(500));
    latency.insert(2, Duration::from_millis(500));
    let probe = make_probe(registries, latency);

    let resp = broadcast(
        knn_request(5, [1.0, 0.0, 0.0, 0.0]),
        vec![0, 1, 2],
        probe,
        Duration::from_millis(40),
        MergeOrder::ScoreAscending,
    )
    .await
    .unwrap();
    assert_eq!(resp.peers_consulted, 3);
    assert_eq!(resp.peers_timed_out, 3);
    assert!(resp.partial, "partial when every peer times out");
    assert!(resp.hits.is_empty(), "no peer produced hits in time");
}
