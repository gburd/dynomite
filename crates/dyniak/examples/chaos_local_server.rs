//! Local Dyniak PBC server for chaos-driver validation (dev harness).
use dyniak::datastore::NoxuDatastore;
use dyniak::serve_pbc;
use dynomite::embed::Datastore;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args()
        .nth(1)
        .map_or_else(|| PathBuf::from("/tmp/chaos-noxu-validate"), PathBuf::from);
    std::fs::create_dir_all(&dir)?;
    let noxu = NoxuDatastore::open_in(&dir)?;
    let ds: Arc<dyn Datastore> = Arc::new(noxu);
    let listener = TcpListener::bind("127.0.0.1:8087").await?;
    eprintln!(
        "chaos_local_server: PBC on 127.0.0.1:8087, noxu at {}",
        dir.display()
    );
    serve_pbc(listener, ds).await?;
    Ok(())
}
