# Configuration

This page covers configuration knobs that go beyond the basic
pool stanza. Refer to the inline rustdoc on
[`dynomite::conf::ConfPool`] for the exhaustive field list; the
pages here describe the operator-facing surface in more detail.

## Bucket types

A *bucket type* is a named bundle of routing properties that
applies to every key whose on-the-wire form starts with the
bucket prefix. Operators use bucket types to give different key
classes different SLAs from inside a single pool: cache-style
keys can sit on `DC_ONE` while transactional keys ride
`DC_EACH_SAFE_QUORUM`, and the dispatcher swaps in the right
settings on a per-request basis without needing more pools, more
listeners, or client-side routing.

The wire convention is intentionally simple. The bucket name is
the byte sequence before the first `/` in the key; everything
else (including the slash itself) is the user-visible key body.
A key with no `/` has no bucket, and the dispatcher falls back
to the pool defaults (or to `default_bucket_type`, when one is
named).

```yaml
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '101134286'
  servers:
  - 127.0.0.1:22122:1
  data_store: 0
  read_consistency: DC_ONE
  write_consistency: DC_ONE
  bucket_types:
  - name: sessions
    read_consistency: DC_QUORUM
    write_consistency: DC_EACH_SAFE_QUORUM
    n_val: 3
  - name: cache
    read_consistency: DC_ONE
    write_consistency: DC_ONE
    n_val: 1
  default_bucket_type: cache
```

With this stanza:

* `GET sessions/abc123` is planned with `DC_QUORUM`; writes use
  `DC_EACH_SAFE_QUORUM` and may fan out across DCs.
* `GET cache/u/9000` uses `DC_ONE` reads and writes.
* `GET plain-key` (no slash) falls through to
  `default_bucket_type: cache` and inherits its `DC_ONE`
  routing.
* Removing `default_bucket_type` makes the slashless and
  unknown-prefix cases inherit the pool-level defaults.

`n_val` caps the number of replicas a single request fans out
to. `0` means "no cap" (the default; the consistency level
alone decides fan-out). A positive `n_val` truncates the plan
to its first `n_val` targets, where rack-local replicas are
already ordered first.

Validation enforces unique bucket-type names, valid consistency
strings, and that `default_bucket_type` (when set) names an
entry in `bucket_types`. Invalid stanzas are caught by
`dynomited --test-conf` before the daemon starts.

## Hinted handoff

Hinted handoff is the data-availability tier that lets writes
succeed even when one of their target peers is unreachable. The
dispatcher records the on-the-wire request bytes plus the
intended peer index in a node-local *hint store*; a background
drainer periodically replays the hints once the peer returns to
the gossip-defined `Normal` state.

The feature is **off by default**. Enable it per-pool in YAML:

```yaml
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '101134286'
  servers:
  - 127.0.0.1:22122:1
  data_store: 0
  read_consistency: DC_QUORUM
  write_consistency: DC_QUORUM
  enable_hinted_handoff: true
  hint_ttl_seconds: 86400
  hint_store_max_bytes: 67108864
  hint_drain_interval_ms: 30000
```

Knobs:

* `enable_hinted_handoff` (bool, default `false`): master
  switch. When `false` the dispatcher's behaviour is unchanged
  and a Down or unreachable target is silently skipped.
* `hint_ttl_seconds` (uint, default `86400` / 24 hours):
  per-hint expiry. Hints older than this are dropped during the
  next drainer sweep so the in-memory store stays bounded.
* `hint_store_max_bytes` (uint, default `67108864` / 64 MiB):
  upper bound on the cumulative payload bytes of pending hints.
  Once the store is full the dispatcher falls back to its
  non-handoff error path
  (`DynomiteNoQuorumAchieved`) and the next drainer sweep
  reclaims space when peers come back online.
* `hint_drain_interval_ms` (uint, default `30000`): cadence of
  the drainer sweep. Every tick (a) drops expired hints and
  (b) replays the queued hints for every peer that has
  transitioned to `Normal`.

Operational notes:

* Hinted handoff applies only to **writes**. Reads against a
  Down peer remain a no-data-to-hint situation and continue to
  follow the legacy "skip the target and fall through to the
  consistency check" path.
* The hint store is RAM-only in this release. An on-disk
  variant (one segment file per peer, replayed at startup) is
  on the roadmap; until then a node restart drops every
  pending hint. Operators that need cross-restart durability
  should size `hint_store_max_bytes` and the drainer cadence
  conservatively.
* The synthesised reply that the dispatcher feeds the
  coalescer on a hinted target's behalf is `+OK\r\n`. This is
  the correct shape for `SET`-style writes (the dominant
  case) but may show up as a "divergent target" for
  request types whose reply is an integer or a multibulk
  (e.g. `DEL`). The hint is still recorded and replayed; the
  surviving real replies determine the coalesced answer the
  client sees, and read-repair (which only fires on `GET`)
  is not affected.
* The `hint_drain_interval_ms` value should be small enough to
  notice peer recoveries promptly but large enough that the
  drainer does not dominate the per-tick scheduling cost when
  many peers come back at once. The default 30 seconds matches
  the gossip period.

## Riak mode

The optional Riak protocol surface (`crates/dyn-riak`) is
wired into `dynomited` behind the `riak` Cargo feature. When
the binary is built with `--features riak`, the `riak:` block
of the pool body controls the PBC listener, the HTTP gateway,
and the optional active-anti-entropy (AAE) scheduler. The
block is parsed and validated even under the default build so
YAML files authored against the Riak-enabled binary still
validate without the feature flag; under the default build the
fields are inert at run time.

```yaml
my_pool:
  ...
  riak:
    pbc_listen: 127.0.0.1:8087
    http_listen: 127.0.0.1:8098
    aae_enabled: true
    aae_full_sweep_interval_seconds: 86400
    aae_segment_interval_seconds: 60
```

Every field is optional. Setting only `pbc_listen` enables the
PBC listener with no HTTP gateway; setting only `http_listen`
runs HTTP without PBC. The two listeners share a single
backing `Datastore` so request accounting accumulates in one
place.

| Key | Type | Meaning |
| --- | --- | --- |
| `pbc_listen` | `host:port` | Riak PBC listener bind address. |
| `http_listen` | `host:port` | Riak HTTP gateway bind address. |
| `aae_enabled` | bool | Spawn the AAE scheduler. Default `false`. |
| `aae_full_sweep_interval_seconds` | u64 | Cadence over which one full sweep across every peer pair completes. Default 86400. |
| `aae_segment_interval_seconds` | u64 | Cadence of one (peer, time-bucket) exchange tick. Default 60. Must be <= `aae_full_sweep_interval_seconds`. |

The CLI offers three matching overrides for the same knobs:
`--riak-pbc-listen=HOST:PORT`, `--riak-http-listen=HOST:PORT`,
and `--riak-aae-enabled`. They are visible in
`dynomited --help` only when the binary was built with
`--features riak`. See [Riak mode](./operations/riak.md) for
operator-facing details.
