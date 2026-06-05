# Riak mode

`dynomited` ships an optional Riak-compatible protocol surface
through the [`dyniak`](https://crates.io/crates/dyniak)
crate. The surface is gated behind the `riak` Cargo feature so
operators who do not run Riak workloads pay nothing for the
extra dependencies and listeners.

## Building

```sh
cargo build -p dynomited --features riak
```

Without the feature, `dynomited` builds and runs identically to
a Redis / Memcache deployment. The YAML `riak:` block is parsed
and validated either way; under the default build it is a no-op
at run time.

## Listeners

Two independent listeners are available:

* **PBC (Protocol Buffers Client)** -- Riak's binary wire format.
  Hand-rolled `prost`-derived messages plus the standard
  `[4-byte BE length][1-byte msg-code][prost body]` framing. The
  v0.0.1 surface covers `RpbPing`, `RpbGetReq`/`Resp`,
  `RpbPutReq`/`Resp`, `RpbDelReq`, `RpbServerInfoReq`/`Resp`,
  `RpbGetBucket`, `RpbSetBucket`, plus error responses for
  `RpbListBuckets`, `RpbListKeys`, `RpbIndex` (which return a
  `not implemented for this datastore` error pending the
  follow-up slice that wires the richer K/V trait against the
  storage engine).
* **HTTP gateway** -- the same operations exposed over
  `application/x-protobuf`, `application/json`, or
  `application/cbor` via the
  [`dyn-encoding`](https://crates.io/crates/dyn-encoding)
  registry. The `GET /ping` endpoint is the simplest liveness
  probe.

Either listener can be enabled on its own; both can run
side-by-side. They share a single `Datastore` so request
accounting accumulates in one place.

## YAML configuration

```yaml
my_pool:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '101134286'
  servers:
  - 127.0.0.1:6379:1
  data_store: 0
  riak:
    pbc_listen: 127.0.0.1:8087
    http_listen: 127.0.0.1:8098
    aae_enabled: true
    aae_full_sweep_interval_seconds: 86400
    aae_segment_interval_seconds: 60
```

| Key | Type | Meaning |
| --- | --- | --- |
| `pbc_listen` | `host:port` | PBC listener bind address. Omit to disable. |
| `http_listen` | `host:port` | HTTP gateway bind address. Omit to disable. |
| `aae_enabled` | bool | When `true`, the active anti-entropy scheduler is spawned. Default `false`. |
| `aae_full_sweep_interval_seconds` | u64 | Cadence over which one full sweep across every peer pair completes. Defaults to 86400 (24 hours). |
| `aae_segment_interval_seconds` | u64 | Cadence of one (peer, time-bucket) exchange tick. Defaults to 60 (one minute). |

`aae_segment_interval_seconds` must be `<= aae_full_sweep_interval_seconds`.
The validator surfaces a `BadServer` error when either constraint
is violated; `dynomited -t -c <file>` reports the same diagnostic.

## CLI overrides

Three flags override the YAML at startup; each requires the
binary to be compiled with `--features riak`:

```
--riak-pbc-listen HOST:PORT     override riak.pbc_listen
--riak-http-listen HOST:PORT    override riak.http_listen
--riak-aae-enabled              force-enable the AAE scheduler
```

The flags compose with the rest of the CLI: an existing YAML
without a `riak:` block can have one materialised purely from
the command line.

## PBC vs HTTP gateway

Both listeners execute against the same underlying
`Datastore`. Choice between them is operational rather than
correctness-driven:

* **PBC** is the lower-overhead binary protocol and matches the
  Erlang Riak client libraries' default. Use this for
  production workloads where every microsecond counts.
* **HTTP** is human-debuggable, traverses any L7 proxy, and
  supports content-type negotiation
  (protobuf / JSON / CBOR) for clients that prefer text
  formats. Use this for ops tooling, smoke tests, and any
  environment where HTTP middleware is already in place.

## Active anti-entropy (AAE)

When `aae_enabled: true`, a background scheduler ticks at
`aae_segment_interval_seconds` and walks the configured peer
rotation. A future slice will wire the per-peer Tictac tree
exchange and surface `RepairTask`s to the per-peer outbound
channels (the same `mpsc::Sender<OutboundRequest>` map used by
gossip and hinted handoff). For now the task is observable via
`tracing::debug!` events under the
`dynomite::riak::aae` target so operators can confirm that the
cadence is firing as configured.

The repair sink wiring is materialised today as a
[`PeerChannelRepairSink`] adapter living next to the
scheduler; once the exchange protocol lands, only the body of
the scheduler tick has to change.

[`PeerChannelRepairSink`]: ../api/dynomited/riak/struct.PeerChannelRepairSink.html

## Noxu as the backing datastore

The Riak protocol surface speaks to a `Datastore` implementation;
operators select which one via the pool's `data_store:` knob.
With `--features riak`, `dynomited` accepts a third value
alongside the historical `valkey` (`0`, also accepted as the
back-compat alias `redis`) and `memcache` (`1`):

```yaml
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '0'
  data_store: dyniak        # or '2', mirroring the integer form
  noxu_path: /var/lib/dynomite/noxu
  servers:
  - 127.0.0.1:6379:1        # placeholder, ignored under dyniak
  riak:
    pbc_listen: 127.0.0.1:8087
    http_listen: 127.0.0.1:8098
```

When `data_store: dyniak` is set:

* The pool opens an in-process Noxu environment in transactional
  mode at `noxu_path:` and serves the dyniak Riak PBC / HTTP
  surface directly against it.
* The pool does NOT run a RESP client proxy and does NOT dial an
  external backend: there is no RESP backend supervisor and the
  `listen:` address is not bound. All traffic enters through the
  Riak PBC / HTTP listeners.
* A 2i index entry written via `RpbPutReq` is visible to
  subsequent `RpbIndexReq` queries against the same environment.
* The `servers:` list is preserved for schema compatibility but
  is not contacted; the placeholder `127.0.0.1:6379:1` is
  conventional.

If `dynomited` is built without `--features riak`, selecting
`data_store: dyniak` is rejected at configuration validation time
with `dyniak data_store requires dynomited built with --features
riak`.

## Secondary indexes (2i)

The Noxu-backed `Datastore` implements the Riak 2i extension
trait methods used by the PBC `RpbIndexReq` handler. Two query
types are supported:

* **Equality** (`qtype: 0`): scan keys whose index value
  matches exactly.
* **Range** (`qtype: 1`): scan keys whose index value falls
  inside `[range_min, range_max]` (inclusive bounds).

Index entries are attached to an object at put time. The
`RpbPutReq.indexes` field carries one `RpbPair` per entry where
`pair.key` names the index (suffixed with `_int` for integer
indexes, `_bin` for binary indexes) and `pair.value` carries the
value bytes.

Example (Python, using `riak` PBC client):

```python
client = riak.RiakClient(host='127.0.0.1', pb_port=8087)
b = client.bucket('users')
o = b.new('alice', data='profile')
o.add_index('age_int', '42')
o.add_index('city_bin', 'seattle')
o.store()

# Equality query:
hits = b.get_index('age_int', '42').results
# Range query:
hits = b.get_index('age_int', '10', '50').results
```

Storage layout (deviation from upstream Riak's
`2i_partition_table` schema): index entries are stored as plain
records inside the same Noxu environment as the primary KV
data, under three reserved prefixes:

* Primary: `K\\0{bucket}\\0{key}` -> value
* Forward 2i: `I\\0{bucket}\\0{name}\\0<u32-be vlen>{value}{key}`
  -> empty
* Reverse 2i: `R\\0{bucket}\\0{key}` -> length-prefixed
  `(name, value)` list, used to clean stale forward entries on
  delete / overwrite.

The fixed-width length prefix on the value keeps prefix scans
unambiguous when value bytes contain the structural separator;
see `docs/journal/2026-05-24-noxu-firstclass-and-2i.md` for the
schema rationale.

## Streaming response and follow-ups

The current `RpbIndexResp` handler emits a single response frame
with `done = Some(true)`. Streaming (one frame per chunk plus a
body-less terminator) is scoped as a follow-up. The `$key`
reserved internal index for primary-key range queries is also
deferred.

## Default distribution

`--features riak` builds default the pool's `distribution:` to
`random_slicing` whenever a Riak listener is configured
(`riak.pbc_listen` or `riak.http_listen`). The choice mirrors
classic Riak behaviour: Riak-shaped deployments inherit a
gap-free partition table by default, so a 3-of-4 host topology
cannot silently leave a quarter of the ring unowned.

Operators who want the legacy `vnode` behaviour can still set
`distribution: vnode` explicitly in the YAML; the override is
respected. See [Distribution modes](./distribution.md) for the
full reference and migration playbook.

## Causality tracking

The Riak surface tracks per-key causality with an Interval Tree
Clock (ITC). ITC is the default for every Riak listener;
operators do not normally need to think about it.

What it does, in one paragraph: each per-key context blob the
server returns to a client is a small encoded clock that
records the causal history of the key as a pair of small trees
(an id tree describing event-issuing authority shared between
live actors and an event tree describing observed history).
Clients echo the blob back on the next update so the server can
make a correct merge / sibling decision. Compared with classic
vector clocks, ITC scales with the population of currently-live
actors instead of the population of every actor that has ever
participated -- retired actors leave no residual cost in the
clock. The on-the-wire shape of the context blob is opaque to
clients (you round-trip the bytes verbatim), so a client that
treats the context as opaque continues to work without
modification. Clients that crack the blob and parse it as a
Riak DVV need to switch decoders; the byte shape is documented
as a deviation under `docs/parity.md` D4 and the "Causality
clock divergence" ambiguity entry.

Operator-visible behaviour is unchanged from a typical client's
perspective: the same `R / W` quorum semantics, the same
sibling presentation, the same `return_body` shape on
`DtUpdateResp`.

References:

* Almeida, Baquero, Fonte, "Interval Tree Clocks: A Logical
  Clock for Dynamic Systems" (2008).
* The implementation lives in
  `crates/dyniak/src/datatypes/itc.rs`; the deviation is
  recorded in `docs/parity.md` D4 and the migration notes in
  `docs/journal/2026-06-01-dvv-to-itc.md`.
## Bucket properties

Two operator-confirmed bucket-property knobs let Riak deployments
match the upstream behaviour byte-for-byte without forcing every
deployment onto the same defaults. See
[the `dyn-admin bucket-props` reference](admin.md#bucket-props)
for the operator-facing CLI that fetches and edits these knobs over
PBC.

### `chash_keyfun`: bucket-only hashing

By default Dynomite hashes `<bucket>/<key>` to choose a
partition; this is Riak's `chash_std_keyfun`. Some deployments
want every key in a bucket to land on the same partition (so the
bucket is effectively a single shard); for that case the per-
bucket-property `chash_keyfun` selector accepts `BUCKETONLY`,
which hashes only `<bucket>`. The wire-level enum is:

| Value | Meaning                                                          |
|-------|------------------------------------------------------------------|
| 0     | `STD` -- hash `<bucket>/<key>` (default).                        |
| 1     | `BUCKETONLY` -- hash `<bucket>` only.                            |
| 99    | `CUSTOM` -- reserved. Not implemented; rejected at decode time.  |

The selector is stored in
`RpbBucketProps.chash_keyfun` (Dynomite extension at tag 30).
Set it through the standard `RpbSetBucketReq` admin path; the
in-memory enum is `dyniak::datatypes::keyfun::KeyFun`. The
shaping happens before the cluster's hash function: the
distribution layer (vnode or random-slicing) keeps consuming the
already-hashed bytes verbatim.

### `replication_strategy`: walk-N-successors

Dynomite's classic replication fans a write across datacenters
and racks per the configured consistency level; Riak instead
replicates a key to the primary partition plus the next
`n_val - 1` peers reached by walking forward on the ring,
deduplicating peers with multiple ring slots. Both models are
now available behind a per-bucket-property selector:

| Value | Meaning                                                           |
|-------|-------------------------------------------------------------------|
| 0     | `TOPOLOGY` -- per-DC, per-rack quorum fan-out (Dynomite default). |
| 1     | `SUCCESSORS` -- walk-N-successors (Riak default).                 |

The default is mode-aware:

* Non-Riak pools always run `TOPOLOGY`; the knob is not
  exposed to operators.
* Riak-mode pools default to `SUCCESSORS` for newly-created
  bucket-types. Operators override per-bucket-type by
  `RpbSetBucketReq`.

Edge cases honoured by the planner:

* Fewer peers than `n_val`: the plan returns whatever peers are
  available and the operator sees a `tracing::warn!` at config-
  validation time.
* Peers in `Down` state: NOT skipped during planning (they are
  returned as targets and the runtime
  [`is_routable()`](https://docs.rs/dynomite) filter handles
  the actual exclusion). This matches the topology mode's
  behaviour.

The selector is stored in
`RpbBucketProps.replication_strategy` (Dynomite extension at
tag 31). The in-memory enum is
`dyniak::replication::ReplicationStrategy`; the planning
function is `dyniak::replication::plan_replicas`.
