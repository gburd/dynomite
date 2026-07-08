# `embedded_minimal`

<div class="dyn-hero">
The smallest runnable embedded Dynomite: build, start, shut down. No
external backend, no peers, no gossip, no hooks.
</div>

<p class="dyn-srclink">Source:
<code>crates/dynomite/examples/embedded_minimal.rs</code> --
run with <code>cargo run -p dynomite --example embedded_minimal</code></p>

## What it demonstrates

The entire lifecycle of an embedded engine in the fewest possible calls:
name a pool, bind two listeners, start, and shut down.

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

Two listeners are the irreducible minimum: `listen` is the client-facing
port (where a Redis or Memcache client connects), and `dyn_listen` is the
DNODE peer plane (where other nodes connect). Binding to port `0` lets
the OS pick a free port, which you then read back with `listen_addr()`
and `dyn_listen_addr()` -- handy in tests.

`Server::start_with` is a convenience that builds and starts in one call.
The longer form -- `ServerBuilder::...build()?` then
`server.start().await?` -- is what
[`embedded_single_node`](./embedded_single_node.md) uses, and it is what
you want when you need the `Server` value before it starts (for example,
to subscribe to its event stream).

## Design decisions and trade-offs

<dl class="dyn-facts">
<dt>In-memory default backend</dt>
<dd>With no <code>datastore</code> hook and no <code>servers</code>, the
engine uses the in-crate
<a href="../embedding/hooks.md">MemoryDatastore</a>. That makes the
example self-contained -- no Valkey to start -- at the cost of realism.
The moment you want to front a real store you add one call; see the next
example.</dd>
<dt>Gossip off by omission</dt>
<dd>With no peers configured there is nothing to gossip with, so the
example is effectively a single node. This keeps the output deterministic
and the startup instant.</dd>
<dt>current_thread runtime</dt>
<dd>A single-threaded tokio runtime is enough for one node with no
concurrent peer traffic and keeps the example's resource use trivial.
Production embeddings use the multi-thread flavor.</dd>
</dl>

```admonish note title="Road not taken: a builder that starts implicitly"
`ServerBuilder` deliberately does *not* start the server when it is
built. Build and start are separate so that a caller can inspect or
register against the `Server` (events, stats handle) before any socket is
bound. `start_with` exists only to collapse the common case into one
line. See [Server Lifecycle](../embedding/server.md).
```

## When to use this pattern

As a smoke test that the engine links and starts in your process, and as
the skeleton you paste and then grow. It is not a useful deployment on
its own -- it stores nothing durable and talks to no peers.

## Where to go next

* [`embedded_single_node`](./embedded_single_node.md) adds a real Valkey
  backend and spells out the configuration.
* [Your First Embedded Engine](../getting-started/first-embed.md) walks
  this chain call by call.
