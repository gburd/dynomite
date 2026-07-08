# `random_slicing`

<div class="dyn-hero">
A four-peer in-memory pool using the random-slicing distribution mode,
driven with 10,000 synthetic keys to report per-peer ownership. A window
into the routing internals.
</div>

<p class="dyn-srclink">Source:
<code>crates/dynomite/examples/random_slicing.rs</code> --
run with <code>cargo run -p dynomite --example random_slicing</code></p>

## What it demonstrates

How the dispatcher turns a key into an owning peer, and how the
random-slicing [distribution mode](../operations/distribution.md)
spreads keys across peers. It builds the ring by hand -- four peers, one
rack, one datacenter -- and then asks the dispatcher to plan 10,000
requests, counting where each lands.

```rust,no_run
let cfg = PoolConfig {
    dc: "dc1".into(),
    rack: "r1".into(),
    hash: HashType::Murmur3X64_64,
    distribution: Distribution::RandomSlicing,
    ..PoolConfig::default()
};
// ... build four Peers, mark them Normal, rebuild the ring ...
let disp = ClusterDispatcher::new(pool);

for i in 0..10_000 {
    let req = Msg::new(i as u64, MsgType::ReqRedisGet, true);
    let key = format!("key-{i:08x}");
    match disp.plan(&req, key.as_bytes()) {
        DispatchPlan::Replicas { targets, .. } => counts[targets[0].peer_idx as usize] += 1,
        DispatchPlan::LocalDatastore => counts[0] += 1,
        DispatchPlan::NoTargets | DispatchPlan::Drop => {}
    }
}
```

The output is a histogram of ownership -- the same information the
operator command `dyn-admin distribution-dump` reports for a live
cluster.

## Design decisions and trade-offs

<dl class="dyn-facts">
<dt>Hand-built pool, no server</dt>
<dd>This example constructs <code>ServerPool</code> and
<code>ClusterDispatcher</code> directly rather than through
<code>ServerBuilder</code>. It is studying the routing layer in
isolation, so it skips listeners, gossip, and backends entirely.</dd>
<dt><code>DispatchPlan</code> exposed</dt>
<dd>Matching on <code>DispatchPlan::{Replicas, LocalDatastore, NoTargets,
Drop}</code> shows the four outcomes the planner can produce for a key.
Only the first replica is counted here, which measures primary
ownership.</dd>
<dt>10,000 synthetic keys</dt>
<dd>Enough to see the distribution shape without being slow. A perfectly
even split would be 2,500 per peer; the spread from that is the point of
comparing distribution modes.</dd>
</dl>

```admonish note title="Road not taken: only consistent hashing"
Dynomite ships more than one distribution strategy. Random slicing
trades the strict monotonicity of a plain token ring for more even
rebalancing when peers are added or removed. This example exists so you
can *measure* that trade-off rather than take it on faith; see
<a href="../operations/distribution.md">Distribution Modes</a> for the
full comparison and when each is appropriate.
```

## When to use this pattern

When you want to reason about or test routing and ownership without
standing up a cluster: comparing distribution modes, checking that a
token assignment balances, or unit-testing changes to the dispatcher.

## Where to go next

* [The Ring and the Token Space](../architecture/ring.md) explains the
  token math this example exercises.
* [Distribution Modes](../operations/distribution.md) compares random
  slicing against the alternatives.
