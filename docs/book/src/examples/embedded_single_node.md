# `embedded_single_node`

<div class="dyn-hero">
A one-node engine fronting a real Valkey, with every configuration knob
spelled out explicitly instead of defaulted.
</div>

<p class="dyn-srclink">Source:
<code>crates/dynomite/examples/embedded_single_node.rs</code> --
run with <code>cargo run -p dynomite --example embedded_single_node</code>
(requires a Valkey/Redis on 127.0.0.1:6379)</p>

## What it demonstrates

The full `ServerBuilder` chain for a realistic single node: a named
pool, both listeners, a Valkey backend, datacenter and rack identity, a
token, a request timeout, and gossip explicitly disabled.

```rust,no_run
use std::time::Duration;
use dynomite::conf::{ConfServer, DataStore};
use dynomite::embed::{Server, ServerBuilder};

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
```

Note the two-step build/start here, in contrast to
[`embedded_minimal`](./embedded_minimal.md)'s `start_with`. The `Server`
value exists before it starts, which is the seam you use to wire up
observability.

The example then reads a stats snapshot and shuts down, rather than
blocking on Ctrl-C, so it terminates cleanly under `cargo run`:

```rust,no_run
let snap = handle.stats();
eprintln!("snapshot pool={} uptime={}s", snap.pool.name, snap.uptime);
handle.shutdown().await?;
```

## The configuration, knob by knob

<dl class="dyn-facts">
<dt><code>data_store(DataStore::Valkey)</code></dt>
<dd>Selects the RESP/Valkey backend protocol. See
<a href="../protocols/redis.md">Redis / Valkey</a>.</dd>
<dt><code>servers(["127.0.0.1:6379:1 backend"])</code></dt>
<dd>The backing store endpoint and weight; parsed by
<code>ConfServer::parse</code>. The trailing name is a label.</dd>
<dt><code>datacenter</code> / <code>rack</code></dt>
<dd>This node's placement in the topology. Even a single node has a DC
and rack because the consistency and replication logic is defined in
those terms; see <a href="../architecture/consistency.md">Replication and
Consistency</a>.</dd>
<dt><code>tokens_str("0")</code></dt>
<dd>The token this node owns on the ring. A lone node owns the whole
ring, so any single token works; see
<a href="../architecture/ring.md">The Ring</a>.</dd>
<dt><code>enable_gossip(false)</code></dt>
<dd>There are no peers, so gossip has nothing to do. Turning it off keeps
startup instant and the log quiet.</dd>
</dl>

## Design decisions and trade-offs

```admonish note title="Road not taken: implicit topology"
The builder does not infer datacenter, rack, or token. Dynomite makes
placement explicit because getting it wrong silently changes where data
lands -- an implicit default would hide a decision that has to be
deliberate in any real cluster. The cost is a little more ceremony for a
single node; the benefit is that the single-node config reads the same
as a production one.
```

Fronting an *existing* Valkey (rather than the in-memory default) is the
point of this example: it shows that the engine's job is orchestration,
not storage. The backing store stays a plain Valkey that knows nothing
about the ring.

## When to use this pattern

As the starting point for any single-node embedding that fronts a real
store, and as the template you extend by adding peers (see
[`embedded_cluster3`](./embedded_cluster3.md)) and hooks (see
[Hooks and Traits](../embedding/hooks.md)).

## Where to go next

* [`embedded_cluster3`](./embedded_cluster3.md) turns this into three
  cooperating nodes with a custom `Datastore`.
* [Configuration](../configuration.md) is the full reference for every
  knob shown here and many more.
