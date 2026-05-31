# 2026-05-24 -- dyniak CRDT data types (v0.0.3)

Branch: `stage/dyniak-crdts`
Commit base: `261ac6b` (main: "merge: stage/dyniak-aae into main")

## What landed

A first slice of Riak CRDT data types under
`crates/dyniak/src/datatypes/`, plus the wire-level
`DtFetchReq` / `DtFetchResp` / `DtUpdateReq` / `DtUpdateResp`
PBC messages and the supporting per-type op payloads.

Four datatypes ship end-to-end:

| Type | Module | Riak codename | State shape |
|---|---|---|---|
| PN-Counter | `datatypes::counter::PnCounter` | `riak_dt_pncounter` | per-actor pos and neg counters |
| OR-Set | `datatypes::set::OrSet` | `riak_dt_orswot` | per-element add and remove tag sets |
| LWW-Register | `datatypes::register::LwwRegister` | `riak_dt_lwwreg` | (value, ts-micros, actor) |
| EW-Flag | `datatypes::flag::EwFlag` | `riak_dt_od_flag` (enable-wins) | OR-Set restricted to a singleton |

Map and HLL are **deferred** to a follow-up slice. They are not
shipped, not stubbed; their PBC enum codes
(`DATA_TYPE_MAP = 3`, `DATA_TYPE_HLL = 4`) and reserved tag
numbers are published in `proto::pb::datatypes` so a client
probing for support sees the canonical schema, but the datatype
Rust modules do not exist yet.

The `Crdt` trait lives in `datatypes::mod.rs` with two methods:
`merge(&mut self, other: &Self)` and
`value(&self) -> Self::Value`. Every implementation satisfies the
three CRDT laws (associative, commutative, idempotent merge),
exercised by 12 `hegeltest` property tests at 256 cases each.

A `Vclock` keyed by `ActorId` lives in
`datatypes::vclock`. The four-way `compare` returns
`VclockOrder::{Less, Equal, Greater, Concurrent}`. Encoding is
length-prefixed so a clock round-trips through the opaque
`bytes` context field on `DtFetchResp` / `DtUpdateReq`.

## Design choices

### Actor-id mapping

Riak keys CRDT metadata by an Erlang `vnode_id` tuple. The
substrate this crate embeds in does not have vnodes in the Riak
sense; it has `(Datacenter, Peer)` pairs that gossip publishes
through `dynomite::embed::TopologySnapshot`. `ActorId` is
therefore a simple `(dc: String, peer: String)` value type with
derived `Ord` so OR-Set tag comparisons and LWW tiebreakers
are deterministic across replicas.

Two consequences:

* Tags carry the full `(dc, peer)` pair, which means a tag is
  ~16 bytes plus actor-name lengths. That is heavier than the
  Erlang `binary()` `vnode_id`, but the OR-Set tag domain is
  still small enough that we eat the cost rather than introduce
  a separate compact-actor-id table at this stage. If profiling
  shows tag storage dominating, we can intern the actor names.
* No tag is comparable across clusters with different
  datacenter or peer naming. That matches Riak's behaviour:
  CRDT replicas are scoped to a single cluster.

### OR-Set tag-generation policy

Tags are `(actor, counter)` pairs. Each `OrSet` holds a
per-actor `BTreeMap<ActorId, u64>` of counters. Every `add`
increments the actor's counter and stores the resulting tag
in the element's `adds` set. `remove(element)` copies every
currently-observed tag for the element into the `removes` set,
which acts as a tombstone -- a concurrent add from a different
replica produces a fresh tag the tombstone has not observed,
so the element survives the merge. This is the standard Shapiro
"Set OR-Set" design, and it is what Riak's `riak_dt_orswot`
implements.

The "add wins on tie" property is exercised by a regression
test (`orset_remove_concurrent_with_add_keeps_element`) that
mirrors the brief: replica A removes X concurrent with B's add
of X; merge yields X present.

### LWW timestamp policy

`LwwRegister` keys assignments by `(ts_micros, actor)`. The
timestamp is microseconds since the Unix epoch, matching
Riak's `riak_dt_lwwreg` units. The actor is the
[`ActorId`] above. Ties on the timestamp are broken by actor
ordering -- Riak's tiebreaker in the reference implementation
is the bigger Erlang term; we use the lex order of the
`(dc, peer)` pair. Both are deterministic and total; the choice
of order matters only for the rare case where two replicas
assign at the same microsecond.

`assign_now` reads the system clock; tests that need
determinism use `assign(actor, ts, value)` with an explicit
timestamp so the deterministic path is exercised in CI.

### Enable-wins flag

`EwFlag` is the OR-Set restricted to a singleton "this flag is
enabled" element, with a tag set instead of an element-keyed
map. Concurrent enable + disable resolves to enabled because
the enable mints a tag the concurrent disable has not observed,
so the merged tombstone set does not cover it. This is Riak's
default for new flags (`enable_wins`); the alternate
`disable_wins` policy is a separate datatype not shipped here.

### PBC envelope

The PBC structs live in `crates/dyniak/src/proto/pb/datatypes.rs`.
Riak's published schema places nested messages (`MapOp`,
`MapEntry`, `MapField`, `MapUpdate`) inside `DtOp` and
`DtValue`. Those nested types are not modelled here; their tag
numbers (3 in `DtOp`, 3 in `DtValue`, 5 in `DtUpdateResp`) are
left as gaps so the wire format stays stable when the map slice
lands. `prost` skips unknown tags on decode, so a client that
sends a map op against this server gets a clean
`UnknownMessageCode`-style error rather than a parse failure.

The `DtFetchResp.type` field is an enum on the wire
(`COUNTER=1, SET=2, MAP=3, HLL=4, GSET=5`). It is modelled as
`int32` with `pub const DATA_TYPE_*` constants, mirroring the
existing approach for `INDEX_QUERY_TYPE_*`. Modelling it as a
prost-derived Rust enum would inflate the surface and require
an explicit `from_i32` round-trip for one byte of wire savings.

## Testing summary

Counts (relative to `main` at base commit):

| Suite | Before | After |
|---|---:|---:|
| `cargo nextest run --workspace` | 861 | 941 |
| ... thereof unit tests in `dyniak` | 105 | 173 |
| ... thereof integration tests | 0 | 14 |
| ... thereof property tests | 0 | 12 |

Total new tests: **80** (28 unit + 14 integration + 12 property
+ 26 inside `proto::pb::datatypes::tests` + 12 inside the new
`datatypes::*::tests`).

The four scenarios called out in the brief each get a dedicated
integration test in
`crates/dyniak/tests/datatypes_round_trip.rs`:

* `pncounter_distinct_actors_sum_on_merge`
* `orset_remove_concurrent_with_add_keeps_element`
* `lww_register_higher_timestamp_wins_on_merge`
* `ewflag_enable_wins_concurrent_disable`

The four `VclockOrder` cases each get a dedicated test
(`vclock_compare_{equal, less, greater, concurrent}`).

Property-test counts: 12 properties, 256 cases each, run by
`hegeltest`. Properties cover associativity, commutativity, and
idempotence on `PnCounter`, `OrSet`, `LwwRegister`, and `EwFlag`.

## Files touched

```
crates/dyniak/Cargo.toml                              [+1 line]
crates/dyniak/src/lib.rs                              [+1 line]
crates/dyniak/src/proto/pb/mod.rs                     [+8 lines]
crates/dyniak/src/datatypes/mod.rs                    [new]
crates/dyniak/src/datatypes/vclock.rs                 [new]
crates/dyniak/src/datatypes/counter.rs                [new]
crates/dyniak/src/datatypes/set.rs                    [new]
crates/dyniak/src/datatypes/register.rs               [new]
crates/dyniak/src/datatypes/flag.rs                   [new]
crates/dyniak/src/proto/pb/datatypes.rs               [new]
crates/dyniak/tests/datatypes_round_trip.rs           [new]
crates/dyniak/tests/datatypes_properties.rs           [new]
docs/journal/2026-05-24-dyniak-crdts.md               [this file]
```

`crates/dynomite/`, `crates/dynomited/`, `crates/dyn-encoding/`,
`crates/dyn-hash-tool/`, `crates/dyniak/src/proto/http/`, and
`crates/dyniak/src/aae/` are untouched. `lib.rs` and
`proto/pb/mod.rs` are append-only edits.

## Verification

```
cargo build --workspace --all-targets --locked            -- clean
cargo fmt -p dynomite -p dynomited -p dyn-hash-tool \
         -p dyn-encoding -p dyniak -- --check           -- clean
cargo clippy --workspace --all-targets --all-features \
         -- -D warnings                                   -- clean
cargo nextest run --workspace                             -- 941 passed, 4 skipped
cargo test --doc --workspace                              -- 15 passed
bash scripts/check_no_todos.sh                            -- clean
bash scripts/check_no_port_comments.sh                    -- clean
bash scripts/check_ascii.sh                               -- clean
```

## Deferred

* `Map` (riak_dt_map) -- nested CRDT keying flags, registers,
  counters, sets, sub-maps. Schema gaps (`DtOp.map_op` tag 3,
  `DtValue.map_value` tag 3, `DtUpdateResp.map_value` tag 5)
  reserve the wire space.
* `HyperLogLog` (riak_dt_hll) -- 14-bit precision sparse/dense
  register array. `HllOp` and `DtValue.hll_value` are already
  on the wire; the datatype module is not yet shipped.
* Dispatcher integration -- the new PBC ops are not yet wired
  into `server::handle_conn`. Adding them requires a small
  extension to the dispatch match and a corresponding
  `Datastore` hook for CRDT operations. Out of scope for this
  slice; the wire types and the in-memory datatypes are
  independent and sufficient for client testing.

## Open questions

None at this slice. The dispatcher integration is the natural
next step; it does not require any new design work beyond
extending the message-code match in `server::handle_conn` and
deciding whether `MemoryDatastore` should grow CRDT-typed
storage (probably yes, gated by a feature flag) or whether the
CRDT path goes straight to `NoxuDatastore`.
