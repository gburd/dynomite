//! `embedded_single_node` - the simplest embedding sketch.
//!
//! Builds a one-node Dynomite engine in front of an existing
//! Redis at 127.0.0.1:6379 and waits for Ctrl-C. No peers, no
//! gossip, no custom hooks; every default applies.
//!
//! Run with `cargo run --example embedded_single_node`.

use std::time::Duration;

use dynomite::conf::{ConfServer, DataStore};
use dynomite::embed::{Server, ServerBuilder};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server: Server = ServerBuilder::new("dyn_o_mite")
        .listen("127.0.0.1:18102".parse()?)
        .dyn_listen("127.0.0.1:18101".parse()?)
        .data_store(DataStore::Redis)
        .servers(vec![ConfServer::parse("127.0.0.1:6379:1 backend")?])
        .datacenter("dc-local")
        .rack("rack-local")
        .tokens_str("0")
        .timeout(Duration::from_secs(5))
        .enable_gossip(false)
        .build()?;

    let handle = server.start().await?;
    eprintln!(
        "embedded dynomite up; client listen={:?} dnode listen={:?}",
        handle.listen_addr(),
        handle.dyn_listen_addr()
    );

    // Demonstrate the API rather than blocking on Ctrl-C: emit a
    // single stats snapshot and shut down cleanly.
    let snap = handle.stats();
    eprintln!("snapshot pool={} uptime={}s", snap.pool.name, snap.uptime);

    handle.shutdown().await?;
    Ok(())
}
