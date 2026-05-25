# 2026-05-25 -- DVVSet as default causality clock

Branch: `stage/dvv-default`
Commit base: `429fea8` (main: "merge: stage/random-slicing into main")

## What landed

`crates/dyn-riak/src/datatypes/dvv.rs`, a self-contained Dotted
Version Vector Set (DVVSet) implementation, plus the migration
of the AAE repair winner-selection path off the legacy classic
[`Vclock`] type. The default for the Riak `DtFetchResp.context`
opaque blob is now a [`DvvSet`]-encoded clock; the wire shape of
`context` is opaque to clients, so existing client drivers
continue to round-trip whatever the server hands back without
needing to parse the bytes.

* `crates/dyn-riak/src/datatypes/dvv.rs` -- the new module.
  Exposes [`DvvSet`], [`DvvOrder`], `update`, `sync`, `merge`,
  `compare`, `encode`, `decode`, plus inspection helpers
  (`max_seq`, `contains_event`, `vc_iter`, `dots`, `len`,
  `is_empty`).
* `crates/dyn-riak/src/datatypes/mod.rs` -- module-level docs
  point readers at DVV first; the `Vclock` re-export sits behind
  a single `#[allow(deprecated)]` (logged in
  `docs/journal/allowances.md`).
* `crates/dyn-riak/src/datatypes/vclock.rs` -- the `Vclock` and
  `VclockOrder` types now carry `#[deprecated(since = "0.0.2",
  note = "use DvvSet")]`. The module-level `#![allow(deprecated)]`
  keeps the kept-for-archaeology unit tests on; the file's
  internal doc points at DVV first.
* `crates/dyn-riak/src/aae/repair.rs` -- `RepairTask::evaluate`
  takes `&[(Bytes, DvvSet)]` and the `RepairOutcome::Winner`
  variant now carries a `dvv: DvvSet` field (renamed from
  `vclock: Vclock`). Comparison logic is unchanged in shape; it
  just routes through `DvvOrder` instead of `VclockOrder`.
* `crates/dyn-riak/src/proto/pb/datatypes.rs` -- the
  `DtFetchResp.context` rustdoc now documents the
  Riak-mode default of a `DvvSet`-encoded blob; three new
  round-trip tests cover the protobuf-context-protobuf path on
  both the fetch and update sides, plus the empty-clock case.
* `crates/dyn-riak/tests/dvv_properties.rs` -- nine
  `#[hegel::test]` properties: `single_actor_sequential_writes_dominate_each_other`,
  `cross_actor_concurrent_writes_remain_concurrent`,
  `merge_commutative`, `merge_associative`, `merge_idempotent`,
  `sync_against_self_is_identity`, `dots_eventually_absorbed`,
  `encode_decode_round_trip`, and
  `compare_is_total_under_canonical_form`.
* `crates/dyn-riak/tests/datatypes_round_trip.rs` -- file-level
  `#![allow(deprecated)]` so the kept-for-archaeology
  `Vclock`-direct integration tests continue to compile under
  `RUSTFLAGS="-D warnings"`.

## Why DVVSet

Classic vector clocks track the count of events emitted by each
actor and compare two clocks under the standard happens-before
relation. They are sound when each actor only ever increments its
own slot; they cannot represent gaps in a per-actor history. In
practice, classic VVs surface false-concurrency artefacts when
the substrate has to merge histories that were observed
non-contiguously -- for example after a sloppy-quorum write
arrives at a non-coordinator before the coordinator's
contiguous prefix has propagated.

DVVSet ([Almeida, Baquero, Goncalves, Preguica, Fonte, "Dotted
Version Vectors: Logical Clocks for Optimistic Replication"
(2010)] and the follow-up ["Scalable and Accurate Causality
Tracking for Eventually Consistent Stores" (2014)]) fixes the
gap by pairing the contiguous vector clock with an explicit
list of "dots" -- per-actor non-contiguous events whose
predecessors have not yet been observed. Each dot is absorbed
back into the vc the moment its predecessor arrives via sync,
so the canonical form drifts back to a tight VV under steady
state. Until then, the dot list is the receipt that
distinguishes "I saw events 1..=3 and the singleton 5" from "I
saw events 1..=5".

## Algorithm summary (matches the in-repo implementation)

`DvvSet` carries:

* `vc: BTreeMap<ActorId, u64>` -- the largest contiguous
  sequence number observed for each actor.
* `dots: Vec<(ActorId, u64)>` -- a sorted list of non-contiguous
  events `(a, n)` where `n > vc[a] + 1`.

Operations:

* `update(actor)` -- the new event's number is
  `max(vc[actor], max_dot[actor]) + 1`. If contiguous with
  `vc[actor]`, fold into `vc`; otherwise push to dots. Then
  canonicalise.
* `sync(other)` (and the pure `merge`) -- pointwise max on vc;
  union of dots minus any covered by either side's vc; then
  canonicalise.
* `compare(self, other)` -- happens-before; `Equal` iff vc and
  dot lists match exactly; `Less` iff every event in self is
  in other AND other carries at least one self does not;
  `Concurrent` otherwise.
* `encode` / `decode` -- length-prefixed
  `(vc_count, vc-entries..., dot_count, dot-entries...)` with
  big-endian u32 lengths and u64 counts. Opaque to clients.

Canonicalisation pass:

1. Drop dots covered by vc (`n <= vc[a]`).
2. For each actor, repeatedly absorb `(a, vc[a] + 1)` into vc.

Both `sync` and `update` end with a canonicalisation pass so
public-API states are always in canonical form.

## False-concurrency property

The brief lists two properties:

1. *single-actor sequential writes do not appear concurrent* --
   one actor performing N updates against its own clock
   produces a strictly ordered chain. This holds for both
   classic VV and DVVSet, but the DVVSet variant is robust
   under the dot-bearing case (the sequence number stays in vc
   if any prior dot for the same actor has been absorbed first;
   if not, it lands in dots, but the per-actor max-seq is still
   monotone so each subsequent clock dominates the previous).
2. *cross-actor concurrent writes remain concurrent* -- two
   actors that both branch from a shared prior state and each
   perform a local update produce concurrent clocks. This holds
   identically for both VV and DVVSet.

Hegeltest covers both as `single_actor_sequential_writes_dominate_each_other`
and `cross_actor_concurrent_writes_remain_concurrent`.

## Why the repair-side migration is mechanical

`RepairTask::evaluate` consumes `(Bytes, clock)` replicas and
returns either `Winner` or `Siblings`. The dominance filter
walks pairwise and drops entries strictly preceded by another
entry; the `Equal` arm dedupes identical clocks by index. The
shape is identical for `DvvOrder` and `VclockOrder`, so the
migration was a typed-replacement plus a field rename
(`vclock` -> `dvv` on `RepairOutcome::Winner`).

## Wire shape: the deprecated and the new format side-by-side

For our internal log only -- both formats are opaque to clients.

`Vclock::encode` (deprecated):

```
u32 vc_count
for vc_count entries:
    u32 dc_len, dc bytes,
    u32 peer_len, peer bytes,
    u64 count
```

`DvvSet::encode` (default):

```
u32 vc_count
for vc_count entries:
    u32 dc_len, dc bytes,
    u32 peer_len, peer bytes,
    u64 count
u32 dot_count
for dot_count entries:
    u32 dc_len, dc bytes,
    u32 peer_len, peer bytes,
    u64 dot_seq
```

The vc half is byte-for-byte identical, so an old
`Vclock`-encoded blob from a v0.0.1 server differs from a
`DvvSet`-encoded blob from a v0.0.2 server by exactly the
trailing `u32 0` for `dot_count` -- nothing more. Old clients
that round-trip the blob verbatim see the new shape as more
bytes; new clients that try to decode an old blob with
`DvvSet::decode` would fail (no trailing dot section), which is
why the bytes stay opaque. The server-side caller
(`RepairTask::evaluate`) only sees `DvvSet` already-decoded
state; the protobuf-level `context` field is `Option<Vec<u8>>`
either way.

## What is deferred

`crates/dyn-riak/src/aae/exchange.rs` carries
`KeyEntry.vclock: Vec<u8>` on the AAE wire. Per the brief, the
exchange protocol's wire bytes stay as opaque blobs and don't
need DVV-aware decoding for the v1: each peer interprets its
own context decoding internally and the merkle-tree XOR
aggregate sees the bytes as opaque hash inputs. A future v2
slice can promote the bytes to `DvvSet` parsing on receipt if
we want the repair scheduler to do same-pass dominance instead
of routing through `RepairTask::evaluate`.

`crates/dyn-riak/src/datastore/` already round-trips
`context: Option<Vec<u8>>` verbatim across put / get; no change
required.

The `Vclock` API is kept for archaeology and direct
in-test comparisons:

* `crates/dyn-riak/src/datatypes/vclock.rs` continues to ship
  `Vclock`, `VclockOrder`, encode / decode, plus the original
  unit tests.
* `crates/dyn-riak/tests/datatypes_round_trip.rs` keeps its
  `vclock_compare_*` tests as the kept-for-archaeology coverage
  of the legacy clock.
* The `Vclock` and `VclockOrder` types carry
  `#[deprecated(since = "0.0.2", ...)]`. With
  `RUSTFLAGS="-D warnings"`, the deprecation lint becomes a
  hard error at use sites; the three call sites that still
  reference `Vclock` (the module-level docstring on
  `vclock.rs`, the re-export in `mod.rs`, and the integration
  test file) gain a `#[allow(deprecated)]` and are recorded in
  `docs/journal/allowances.md`.

## Tests

* Workspace baseline (`main`): 1273 nextest tests.
* This branch: 1307 nextest tests (+34).
  * 22 new unit tests in `dyn_riak::datatypes::dvv::tests`
    (constructors, update behaviour, sync semantics, dot
    absorption, compare arms, encode/decode + malformed
    inputs).
  * 3 new context-blob round-trip tests in
    `dyn_riak::proto::pb::datatypes::tests::dvvset_*`.
  * 9 new hegeltest properties in
    `dyn_riak::dvv_properties` (sequential dominance,
    cross-actor concurrency, merge laws, sync identity, dot
    absorption, encode round-trip, compare totality).
* `cargo test --doc --workspace` clean (15 doctests).
* `cargo clippy --workspace --all-targets --all-features --
  -D warnings` clean.
* Hygiene scripts (`check_no_todos.sh`,
  `check_no_port_comments.sh`, `check_ascii.sh`) all clean.

## Open questions

None. The two-paper citation matrix is captured in this journal
entry and in `docs/parity.md` D4.

## Files touched

```
M  crates/dyn-riak/src/aae/repair.rs
A  crates/dyn-riak/src/datatypes/dvv.rs
M  crates/dyn-riak/src/datatypes/mod.rs
M  crates/dyn-riak/src/datatypes/vclock.rs
M  crates/dyn-riak/src/proto/pb/datatypes.rs
M  crates/dyn-riak/tests/datatypes_round_trip.rs
A  crates/dyn-riak/tests/dvv_properties.rs
M  docs/book/src/operations/riak.md
M  docs/journal/allowances.md
A  docs/journal/2026-05-25-dvv-default.md
M  docs/parity.md
```

## References

* Almeida, Baquero, Goncalves, Preguica, Fonte, "Dotted Version
  Vectors: Logical Clocks for Optimistic Replication", 2010.
* Goncalves, Almeida, Baquero, Fonte, "Scalable and Accurate
  Causality Tracking for Eventually Consistent Stores", 2014.
* The original Riak vector-clock semantics
  (`vclock:descends/2`) are described in Klophaus, "Riak Core",
  2010, and in the Riak documentation. The Riak codebase moved
  to a DVV-style scheme in 2.0 for the same false-concurrency
  reason captured here.
