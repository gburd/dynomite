# Riak mode

`dynomited` ships an optional Riak-compatible protocol surface
through the [`dyn-riak`](https://crates.io/crates/dyn-riak)
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
alongside the historical `redis` (`0`) and `memcache` (`1`):

```yaml
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '0'
  data_store: noxu          # or '2', mirroring the integer form
  noxu_path: /var/lib/dynomite/noxu
  servers:
  - 127.0.0.1:6379:1        # placeholder, ignored under noxu
  riak:
    pbc_listen: 127.0.0.1:8087
    http_listen: 127.0.0.1:8098
```

When `data_store: noxu` is set:

* The Redis-fronting proxy still parses RESP requests on
  `listen:`. Each `GET` / `SET` / `DEL` / `PING` is delivered to
  an in-process supervisor that reads / writes against the Noxu
  environment at `noxu_path:`.
* The Riak PBC and HTTP listeners (when configured) bind to the
  same Noxu environment, so a key written via RESP is visible via
  Riak's `RpbGetReq`, and a 2i index entry written via
  `RpbPutReq` is visible to subsequent `RpbIndexReq` queries.
* The `servers:` list is preserved for schema compatibility but
  is not contacted; the placeholder `127.0.0.1:6379:1` is
  conventional.

If `dynomited` is built without `--features riak`, selecting
`data_store: noxu` is rejected at configuration validation time
with `noxu data_store requires dynomited built with --features
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
