# `embedded_cluster3`

<div class="dyn-hero">
Three Dynomite nodes in one process, sharing one custom
<code>Datastore</code>, driven directly through the in-process request
injector. A test scaffold for hook implementations.
</div>

<p class="dyn-srclink">Source:
<code>crates/dynomite/examples/embedded_cluster3.rs</code> --
run with <code>cargo run -p dynomite --example embedded_cluster3</code></p>

## What it demonstrates

Three things at once: building more than one node in a process, supplying
your own `Datastore` hook, and driving requests without a socket via
`inject_request`.

The custom datastore is a shared in-memory map that all three nodes write
through:

```rust,no_run
#[derive(Default, Clone)]
struct SharedKv {
    inner: Arc<Mutex<std::collections::HashMap<u64, MsgType>>>,
}

impl Datastore for SharedKv {
    fn protocol(&self) -> Protocol { Protocol::Custom }

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
```

Each node is built with a distinct rack and token so the three of them
tile the ring, then a write is injected at node 0 and reads at nodes 1
and 2:

```rust,no_run
let mut w = Msg::new(7, MsgType::ReqRedisSet, true);
w.set_parent_id(0);
let _ = n0.inject_request(w).await?;

for (label, h) in [("n1", &n1), ("n2", &n2)] {
    let req = Msg::new(7, MsgType::ReqRedisGet, true);
    let rsp = h.inject_request(req).await?;
    eprintln!("{label}: rsp ty={:?} parent={}", rsp.ty(), rsp.parent_id());
}
```

## Design decisions and trade-offs

<dl class="dyn-facts">
<dt>Shared in-memory <code>Datastore</code></dt>
<dd>One <code>SharedKv</code> behind all three nodes lets the example
show routing and response flow without three real backends. It is a test
double, not a replication story -- the nodes are not actually replicating
to independent stores.</dd>
<dt><code>inject_request</code> over sockets</dt>
<dd>Requests go straight into the engine, skipping the client listener.
That makes the example deterministic and fast and is exactly how you
would unit-test a hook. Real clients still connect over the socket.</dd>
<dt>Gossip disabled, seeds empty</dt>
<dd>The topology is wired statically by giving each node its rack and
token directly. This keeps the example reproducible; a live cluster would
enable gossip and share seeds.</dd>
<dt><code>multi_thread</code> runtime</dt>
<dd>Three nodes with concurrent injected traffic want real parallelism,
unlike the single-node examples.</dd>
</dl>

```admonish note title="Road not taken: three processes"
A truly independent three-node cluster runs three processes with three
backends and real gossip -- that is what the integration and conformance
suites do. This example collapses it into one process on purpose: it
exists to exercise the *hook surface* under multi-node routing, where a
process boundary would add noise without teaching anything new.
```

```admonish warning title="This is a scaffold, not a deployment"
Because the three nodes share one in-memory map and use
<code>inject_request</code>, do not read this example as a template for a
production cluster. For that, see
<a href="../getting-started/first-cluster.md">Your First Cluster</a>,
which runs separate <code>dynomited</code> processes.
```

## When to use this pattern

When you are implementing a `Datastore`, `MetricsSink`, or other hook and
want to test it under multi-node routing in a single fast, deterministic
process.

## Where to go next

* [Hooks and Traits](../embedding/hooks.md) for the full trait surface
  the `SharedKv` here implements.
* [Your First Cluster](../getting-started/first-cluster.md) for a real
  multi-process cluster.
