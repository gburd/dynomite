# Your First Embedded Engine

This chapter is the library counterpart to
[Your First Cluster](./first-cluster.md). Instead of running the
`dynomited` server, you embed the same distribution layer directly in a
Rust program through the `dynomite` crate. You supply a backend by
implementing one trait; Dynomite supplies the ring, gossip, quorum,
hinted handoff, and repair.

We start with the smallest engine that runs, then grow it in two steps:
a real Valkey backend, then a second in-process peer. Every snippet here
mirrors a complete program under
[`crates/dynomite/examples/`](../examples/index.md); the inline code
is the teaching version, the examples are the runnable version.

```admonish tip title="Prerequisites"
Run inside `nix develop`. Add the crate to your own project with
`dynomite = "..."` (published as `dynomite-engine` on crates.io). The
API surface is governed by SemVer once 0.1 is cut; see the
[Server Lifecycle](../embedding/server.md) SemVer policy.
```

## The smallest engine that runs

The minimal embed is a five-call build chain plus a start/shutdown
handshake. No external store, no peers, no gossip: the in-crate
`MemoryDatastore` stands in for the backing store, so this compiles and
runs with nothing else installed.

```rust,no_run
use dynomite::embed::{Server, ServerBuilder};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    Ok(())
}
```

That is the whole program. It is
[`examples/embedded_minimal.rs`](../examples/embedded_minimal.md)
verbatim. Run it with `cargo run --example embedded_minimal`.

### The chain, call by call

<dl class="dyn-facts">
<dt>ServerBuilder::new("dyn_o_mite")</dt>
<dd>Start a builder for a pool named <code>dyn_o_mite</code>. The pool
name is the same one you would put as the top-level key in a YAML config;
it labels the pool in stats and events.</dd>
<dt>.listen("127.0.0.1:0")</dt>
<dd>The client plane -- where RESP/Memcache clients connect. Port
<code>0</code> asks the OS for an ephemeral port; you read the assigned
port back from the handle. Bind a fixed port in production.</dd>
<dt>.dyn_listen("127.0.0.1:0")</dt>
<dd>The peer plane -- where other nodes connect for gossip and
replication. Also ephemeral here.</dd>
<dt>Server::start_with(builder)</dt>
<dd>A convenience that calls <code>.build()</code> then
<code>.start()</code> in one step. It spawns the background tasks (stats,
metrics, and -- if enabled -- gossip and the accept loops) on the current
tokio runtime and returns a <code>ServerHandle</code>.</dd>
</dl>

`Server::start_with(builder)` is exactly `builder.build()?.start().await`
folded into one call. When you need to inspect or hold the configured
`Server` before starting it, split them:

```rust,no_run
# use dynomite::embed::{Server, ServerBuilder};
# async fn f() -> Result<(), Box<dyn std::error::Error>> {
let server: Server = ServerBuilder::new("dyn_o_mite")
    .listen("127.0.0.1:0".parse()?)
    .dyn_listen("127.0.0.1:0".parse()?)
    .build()?;                 // validate config, no tasks yet
let handle = server.start().await?;   // spawn background tasks
# handle.shutdown().await?;
# Ok(())
# }
```

`.build()` validates the configuration and can fail with an
`EmbedError`; nothing binds or spawns until `.start()`. That split is the
in-process equivalent of `dynomited --test-conf` followed by launch.

### The handle

`ServerHandle` is the control surface. It is `Clone + Send + Sync`, so
several parts of your program can hold it. The calls you meet first:

* `handle.listen_addr()` / `handle.dyn_listen_addr()` -- the bound
  addresses (with the real port resolved when you asked for `:0`).
* `handle.stats()` -- a `Snapshot` of the current counters.
* `handle.shutdown().await?` -- graceful shutdown: cancel every
  background task, deregister, drain, return. It is idempotent.

```admonish warning title="Dropping the handle does not stop the server"
The background tasks keep running until you call
<code>shutdown().await</code> (or <code>join().await</code> resolves).
Dropping the last <code>ServerHandle</code> does <em>not</em> shut the
server down. In a long-running service, keep a handle and drive shutdown
from your signal handler.
```

## Step up: a real Valkey backend

The minimal engine used the in-memory stand-in. To front a real Valkey,
add the backend and topology to the chain. This mirrors
[`examples/embedded_single_node.rs`](../examples/embedded_single_node.md):

```rust,no_run
use std::time::Duration;

use dynomite::conf::{ConfServer, DataStore};
use dynomite::embed::{Server, ServerBuilder};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server: Server = ServerBuilder::new("dyn_o_mite")
        .listen("127.0.0.1:18102".parse()?)
        .dyn_listen("127.0.0.1:18101".parse()?)
        .data_store(DataStore::Valkey)
        .servers(vec![ConfServer::parse("127.0.0.1:6379:1 backend")?])
        .datacenter("dc-local")
        .rack("rack-local")
        .tokens_str("0")
        .timeout(Duration::from_secs(5))
        .enable_gossip(false)
        .build()?;

    let handle = server.start().await?;
    eprintln!(
        "up; client={:?} dnode={:?}",
        handle.listen_addr(),
        handle.dyn_listen_addr()
    );

    let snap = handle.stats();
    eprintln!("pool={} uptime={}s", snap.pool.name, snap.uptime);

    handle.shutdown().await?;
    Ok(())
}
```

The new calls, in order:

<dl class="dyn-facts">
<dt>.data_store(DataStore::Valkey)</dt>
<dd>Select the backend protocol. The parallel to <code>data_store: 0</code>
in YAML. <code>DataStore::Memcache</code> and <code>DataStore::Dyniak</code>
are the other two.</dd>
<dt>.servers(vec![ConfServer::parse("127.0.0.1:6379:1 backend")?])</dt>
<dd>The backend endpoints, parsed from the same
<code>host:port:weight [name]</code> string the YAML <code>servers:</code>
list uses. Here one Valkey on <code>6379</code>, weight <code>1</code>,
named <code>backend</code>.</dd>
<dt>.datacenter(...) / .rack(...)</dt>
<dd>This node's place in the hierarchy, exactly as in
<a href="./concepts.md">Concepts</a>.</dd>
<dt>.tokens_str("0")</dt>
<dd>This node's ring position(s), parsed from the same string the YAML
<code>tokens:</code> key takes. One node owning token <code>0</code> owns
the whole ring.</dd>
<dt>.timeout(Duration::from_secs(5))</dt>
<dd>Backend request timeout.</dd>
<dt>.enable_gossip(false)</dt>
<dd>Single node, no peers to gossip with, so gossip is off. Turn it on
once you add peers.</dd>
</dl>

Point a plain client at the bound `listen` address and it talks RESP to
your embedded engine:

```console
$ valkey-cli -p 18102 set k v
OK
$ valkey-cli -p 18102 get k
"v"
```

A connection accepted on the `listen` socket is served end to end --
parsed, routed through the dispatcher, and answered by the backend --
the same path the standalone `dynomited` proxy uses. See
[Server Lifecycle](../embedding/server.md) for the client-plane and
in-process traffic contract.

## Bring your own backend: the Datastore hook

You are not limited to Valkey and Memcache. Implement the `Datastore`
trait and register it with `.datastore(...)`, and Dynomite routes
requests to your code instead of an external store. This is the single
most important extension point; the full trait set is in
[Hooks and Traits](../embedding/hooks.md).

A tiny in-memory `Datastore` (the shape used by
[`examples/embedded_cluster3.rs`](../examples/embedded_cluster3.md)):

```rust,no_run
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use dynomite::embed::hooks::{BoxFuture, Datastore, DatastoreError, Protocol};
use dynomite::msg::{Msg, MsgType};

#[derive(Default, Clone)]
struct SharedKv {
    inner: Arc<Mutex<HashMap<u64, MsgType>>>,
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
            let mut rsp =
                Msg::new(req.id(), stored.unwrap_or(MsgType::RspRedisStatus), false);
            rsp.set_parent_id(req.id());
            Ok(rsp)
        })
    }
}
```

`dispatch` is the whole contract: take a parsed request `Msg`, return a
response `Msg` (or a `DatastoreError`). `protocol()` tells the engine how
to frame replies. Register it on the builder:

```rust,no_run
# use dynomite::embed::ServerBuilder;
# use dynomite::conf::DataStore;
# #[derive(Default)] struct SharedKv;
# impl dynomite::embed::hooks::Datastore for SharedKv {
#   fn protocol(&self) -> dynomite::embed::hooks::Protocol { dynomite::embed::hooks::Protocol::Custom }
#   fn dispatch(&self, req: dynomite::msg::Msg) -> dynomite::embed::hooks::BoxFuture<'_, Result<dynomite::msg::Msg, dynomite::embed::hooks::DatastoreError>> {
#     Box::pin(async move { Ok(req) })
#   }
# }
# fn f() -> Result<(), Box<dyn std::error::Error>> {
let builder = ServerBuilder::new("p")
    .listen("127.0.0.1:0".parse()?)
    .dyn_listen("127.0.0.1:0".parse()?)
    .data_store(DataStore::Valkey)
    .tokens_str("0")
    .datastore(Box::new(SharedKv::default()));   // your backend
# let _ = builder;
# Ok(())
# }
```

`Datastore` is one of several hooks. The others -- `SeedsProvider`
(where peer addresses come from), `CryptoProvider` (peer-plane
encryption), and `MetricsSink` (where counters flush) -- each ship a
default and are documented, with examples, in
[Hooks and Traits](../embedding/hooks.md).

## Step up again: a second peer

To make it a cluster, build more than one `Server` and give each the
other's peer address as a seed. The shape below follows
[`examples/embedded_cluster3.rs`](../examples/embedded_cluster3.md),
trimmed to two nodes:

```rust,no_run
# use std::time::Duration;
# use dynomite::conf::{ConfDynSeed, ConfServer, ConsistencyLevel, DataStore};
# use dynomite::embed::{Server, ServerBuilder, ServerHandle};
async fn spawn_node(
    rack: &str,
    listen: &str,
    dyn_listen: &str,
    tokens: &str,
    seeds: Vec<ConfDynSeed>,
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
        .dyn_seeds(seeds)
        .build()
        .unwrap();
    server.start().await.unwrap()
}
```

Each node takes a distinct rack, distinct listen/peer ports, its own
token, and the other node(s) as `dyn_seeds`. The consistency-level calls
map one-to-one to the `read_consistency` / `write_consistency` YAML keys
and take the `ConsistencyLevel` variants from
[Concepts](./concepts.md): `DcOne`, `DcQuorum`, `DcSafeQuorum`,
`DcEachSafeQuorum`.

For in-process clusters you can drive requests without a socket hop via
`handle.inject_request(msg).await` -- the dispatcher computes a routing
plan and, if it targets a co-located peer, forwards through the
in-process registry. That is how the cluster example verifies a write on
one node reads back through another entirely in one process.

```admonish note title="Road not taken"
Embedded multi-node forwarding uses an in-process registry, not a real
socket hop between the in-process nodes. Cross-process peer serving --
the real DNODE wire path -- is provided by the <code>dynomited</code>
binary. The embedded harness keeps tests off the network while
preserving the production routing semantics (compute plan, deliver to
target peers). See
<a href="../embedding/server.md">Server Lifecycle</a>.
```

## Start-then-park for a long-running service

The examples shut down immediately to keep them short. A real embedder
starts the engine, then parks until a signal arrives. Use `join()`,
which waits for the tasks without requesting cancellation, and drive
`shutdown()` from a separate task:

```rust,no_run
# use dynomite::embed::ServerBuilder;
# use dynomite::conf::DataStore;
# async fn f() -> Result<(), Box<dyn std::error::Error>> {
let handle = ServerBuilder::new("p")
    .listen("127.0.0.1:0".parse()?)
    .dyn_listen("127.0.0.1:0".parse()?)
    .data_store(DataStore::Valkey)
    .servers(vec![dynomite::conf::ConfServer::parse("127.0.0.1:6379:1")?])
    .tokens_str("0")
    .build()?
    .start()
    .await?;

let shutdown = handle.clone();
tokio::spawn(async move {
    tokio::signal::ctrl_c().await.ok();
    shutdown.shutdown().await.ok();
});

handle.join().await;   // parks until shutdown() drains the tasks
# Ok(())
# }
```

## Where to next

* [Embedding API Overview](../embedding/index.md) -- the two-tier API
  and the component diagram.
* [Server Lifecycle](../embedding/server.md) -- the full `ServerBuilder`
  chain, `ServerHandle` control surface, events, snapshots, and the
  SemVer policy.
* [Hooks and Traits](../embedding/hooks.md) -- `Datastore`,
  `SeedsProvider`, `CryptoProvider`, `MetricsSink`, each with a default
  impl and an example.
* [Cookbook](../embedding/cookbook.md) -- task-oriented recipes.
* The runnable examples, each with a walk-through:
  [embedded_minimal](../examples/embedded_minimal.md),
  [embedded_single_node](../examples/embedded_single_node.md),
  [embedded_cluster3](../examples/embedded_cluster3.md).
