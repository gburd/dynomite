//! Quickstart demo: bring up a single-node `dynvec` HTTP
//! server, populate it with a handful of vectors, and run a
//! search.
//!
//! Run with:
//!
//! ```bash
//! cargo run -p dynvec --example quickstart --features http
//! ```
//!
//! The example listens on `127.0.0.1:21900` by default; set
//! `DYNVEC_LISTEN` to override.

use std::collections::HashMap;
use std::sync::Arc;

use dynvec::api::serve;
use dynvec::distance::Distance;
use dynvec::encoding::Codec;
use dynvec::index::HnswParams;
use dynvec::storage::{IndexAlgorithm, TableSchema, VectorStore};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(VectorStore::in_memory());
    store.create_table(TableSchema {
        name: "demo".to_string(),
        dim: 3,
        codec: Codec::Int8Quantized,
        distance: Distance::Cosine,
        hnsw: HnswParams::default(),
        algorithm: IndexAlgorithm::Hnsw,
    })?;

    // Seed a few vectors so the demo has something to search.
    let seed: &[(&[u8], [f32; 3], &str)] = &[
        (b"unit_x", [1.0, 0.0, 0.0], "x-axis"),
        (b"unit_y", [0.0, 1.0, 0.0], "y-axis"),
        (b"unit_z", [0.0, 0.0, 1.0], "z-axis"),
        (b"diag", [0.577, 0.577, 0.577], "diagonal"),
    ];
    for (key, vec, label) in seed {
        let mut md = HashMap::new();
        md.insert("label".to_string(), serde_json::json!(*label));
        store.upsert("demo", key.to_vec(), vec, md)?;
    }

    let addr = std::env::var("DYNVEC_LISTEN").unwrap_or_else(|_| "127.0.0.1:21900".to_string());
    let listener = TcpListener::bind(&addr).await?;
    eprintln!("dynvec listening on http://{addr}");
    eprintln!("try: curl -s {addr}/tables");
    serve(listener, store).await?;
    Ok(())
}
