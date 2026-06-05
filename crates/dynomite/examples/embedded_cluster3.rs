//! `embedded_cluster3` - in-process 3-node cluster.
//!
//! Builds three `Server` instances in one process, registers a
//! shared in-memory datastore behind all three, and drives a
//! handful of requests through `inject_request`. Useful as a
//! test scaffold for hook implementations.
//!
//! Run with `cargo run --example embedded_cluster3`.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use dynomite::conf::{ConfDynSeed, ConfServer, ConsistencyLevel, DataStore};
use dynomite::embed::hooks::{BoxFuture, Datastore, DatastoreError, Protocol};
use dynomite::embed::{Server, ServerBuilder, ServerHandle};
use dynomite::msg::{Msg, MsgType};

#[derive(Default, Clone)]
struct SharedKv {
    inner: Arc<Mutex<std::collections::HashMap<u64, MsgType>>>,
}

impl Datastore for SharedKv {
    fn protocol(&self) -> Protocol {
        Protocol::Custom
    }

    fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut g = inner.lock();
            if matches!(req.ty(), MsgType::ReqRedisSet) {
                g.insert(req.id(), MsgType::RspRedisStatus);
            }
            let stored = g.get(&req.id()).copied();
            drop(g);
            let mut rsp = Msg::new(req.id(), stored.unwrap_or(MsgType::RspRedisStatus), false);
            rsp.set_parent_id(req.id());
            Ok(rsp)
        })
    }
}

async fn spawn_node(
    rack: &str,
    listen: &str,
    dyn_listen: &str,
    tokens: &str,
    seeds: Vec<ConfDynSeed>,
    kv: SharedKv,
) -> ServerHandle {
    let server: Server = ServerBuilder::new("p")
        .listen(listen.parse().unwrap())
        .dyn_listen(dyn_listen.parse().unwrap())
        .data_store(DataStore::Valkey)
        .servers(vec![ConfServer::parse("127.0.0.1:6379:1").unwrap()])
        .datacenter("dc-local")
        .rack(rack)
        .tokens_str(tokens)
        .read_consistency(ConsistencyLevel::DcOne)
        .write_consistency(ConsistencyLevel::DcOne)
        .gossip_interval(Duration::from_millis(100))
        .enable_gossip(false)
        .dyn_seeds(seeds)
        .datastore(Box::new(kv))
        .build()
        .unwrap();
    server.start().await.unwrap()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kv = SharedKv::default();
    let n0 = spawn_node(
        "rA",
        "127.0.0.1:19001",
        "127.0.0.1:19101",
        "101134286",
        Vec::new(),
        kv.clone(),
    )
    .await;
    let n1 = spawn_node(
        "rB",
        "127.0.0.1:19002",
        "127.0.0.1:19102",
        "1431655765",
        Vec::new(),
        kv.clone(),
    )
    .await;
    let n2 = spawn_node(
        "rC",
        "127.0.0.1:19003",
        "127.0.0.1:19103",
        "2863311530",
        Vec::new(),
        kv.clone(),
    )
    .await;

    // Drive a write through node 0 and a read through nodes 1 and 2.
    let mut w = Msg::new(7, MsgType::ReqRedisSet, true);
    w.set_parent_id(0);
    let _ = n0.inject_request(w).await?;

    for (label, h) in [("n1", &n1), ("n2", &n2)] {
        let req = Msg::new(7, MsgType::ReqRedisGet, true);
        let rsp = h.inject_request(req).await?;
        eprintln!("{label}: rsp ty={:?} parent={}", rsp.ty(), rsp.parent_id());
    }

    n0.shutdown().await?;
    n1.shutdown().await?;
    n2.shutdown().await?;
    Ok(())
}
