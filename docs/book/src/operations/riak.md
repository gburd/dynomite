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
