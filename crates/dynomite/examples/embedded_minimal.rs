//! `embedded_minimal` - the smallest runnable embedded Dynomite.
//!
//! Five-call build chain, plus the start / shutdown handshake.
//! No external Redis, no peers, no gossip; the in-crate
//! [`MemoryDatastore`](dynomite::embed::MemoryDatastore) stands
//! in for the backing store. The point is the API shape: the
//! cookbook in `docs/book/src/embedding/cookbook.md` reproduces
//! this exact chain as the canonical \"smallest possible embedded
//! server\".
//!
//! Run with `cargo run --example embedded_minimal`.
//!
//! Expected output:
//!
//! ```text
//! embedded dynomite up; client listen=Some(127.0.0.1:NN) dnode listen=Some(127.0.0.1:NN)
//! shutdown ok
//! ```

use dynomite::embed::{Server, ServerBuilder};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The five-call chain. Pool name, two listeners, build, start.
    let handle = Server::start_with(
        ServerBuilder::new("dyn_o_mite")
            .listen("127.0.0.1:0".parse()?)
            .dyn_listen("127.0.0.1:0".parse()?),
    )
    .await?;

    eprintln!(
        "embedded dynomite up; client listen={:?} dnode listen={:?}",
        handle.listen_addr(),
        handle.dyn_listen_addr()
    );

    handle.shutdown().await?;
    eprintln!("shutdown ok");
    Ok(())
}
