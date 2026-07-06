# HLC prototype (Hybrid Logical Clocks) -- 2026-07-05

Prototype qualification of Hybrid Logical Clocks (Kulkarni, Demirbas,
Madappa, Avva, Leone, "Logical Physical Clocks and Consistent Snapshots
in Globally Distributed Databases", OPODIS 2014) as a dyniak causality /
snapshot-timestamp primitive. One of the five Tier 1/2 ideas dispatched
for qualification against the full release gate (build + clippy + fmt +
tests + the DST + Elle merge gate, AGENTS.md Section 6.5).

Branch: `proto/hlc` (off `main` @ `acdecc9`, the delta-CRDT merge).

## What HLC is and why

An HLC stamp is a pair `(l, c)`: `l` tracks the maximum physical time
the node has observed (its own clock or any received stamp), and `c` is
a bounded tie-break counter used when physical time does not advance
between events. The update rules for local events (`tick`) and receive
events (`update`) guarantee three things:

1. **Monotonicity** -- a node's stamp never goes backward.
2. **Causality capture** -- if event `e` happens-before `f` (program
   order on one node, or a send -> receive edge across nodes) then
   `hlc(e) < hlc(f)` strictly, exactly like a logical clock.
3. **Bounded drift** -- `l` stays within the maximum inter-node
   physical skew of real physical time, so the scalar is physically
   meaningful and usable for a "read as of timestamp T" snapshot.

HLC complements the existing per-key ITC (Interval Tree Clock): ITC
tracks the causal partial order for conflict detection; HLC gives a
scalar, monotone, physically-close timestamp for version / snapshot
selection. It is the enabling primitive a RAMP-SI / Percolator-style
snapshot-isolation path would build on (see the RAMP prototype,
`2026-07-05-ramp-prototype.md`).

## Implementation

* `crates/dyniak/src/datatypes/hlc.rs` -- the `Hlc { l: u64, c: u16 }`
  type. Physical time is *injected* (`tick(physical_now)`,
  `update(received, physical_now)`) rather than read from the wall
  clock inside the core logic, so the DST model and property tests
  drive it deterministically. `now_from_wall_clock` is the one method
  that reads the real clock, for production callers. `l` is capped at
  `MAX_LOGICAL = 2^48 - 1` and `c` at `u16::MAX`; the fallible
  `try_tick` / `try_update` report `CounterOverflow` /
  `LogicalOverflow` rather than wrapping. `pack`/`unpack` give a single
  `u64` (48-bit `l`, 16-bit `c`) that sorts in HLC order; `encode`/
  `decode` give the 8-byte big-endian form for a storage key suffix.
* `crates/dyniak/tests/hlc_snapshot_demo.rs` -- the intended use without
  reworking storage: HLC-stamped versions written to noxu under a
  lexicographically ordered key, and a "latest version with HLC <=
  snapshot_ts" read predicate. Proves HLC enables consistent snapshot
  selection; a full MVCC rework is explicitly out of scope.

## Gates

### DST (`crates/model-tests/src/hlc.rs`, wired into `scripts/model.sh`)

A stateright model of N nodes generating local events and exchanging
messages under a deterministic physical-clock schedule with per-node
skew and stalls. Asserts:

* **monotonicity** -- every node's successive event stamps are
  non-decreasing;
* **causality capture** -- every happens-before edge (program order and
  send -> receive) is strictly increasing in HLC;
* **bounded drift** -- `l` stays within the maximum skew of the global
  physical time.

Negative control `broken_receive_rule_inverts_causality`: the broken
receive rule omits the counter advance, so a send -> receive edge
becomes non-strict when the logical times tie; the checker finds the
causality-capture counterexample. The test asserts
`checker.discovery("causality capture").is_some()`, so a toothless model
fails the test.

**Bounding note.** The model state carries an append-only event/edge
history for the happens-before checks, so two paths reaching the same
logical clock configuration via different interleavings are distinct
states and are not deduplicated by BFS. The reachable-state count is
therefore the number of distinct schedules, which grows
super-exponentially in the event budget. The initial prototype used
`nodes = 3, budget = 6`, which allocated multiple GiB and aborted under
a memory cap (and would OOM a CI runner). The model configs are pinned
to `nodes = 2, budget = 3`, which keeps the search exhaustive, fast
(~1.5s for both tests) and under a few MB of RSS, while still exercising
a send -> receive causal edge under clock skew, a stalled clock, and the
broken-rule inversion. Do not raise these bounds for the CI gate; a
deeper schedule belongs in a soak run, not `scripts/model.sh`. A comment
records this in the test module.

### Property tests (`crates/dyniak/tests/hlc_properties.rs`, 256 cases each)

Monotonicity of `tick`; monotonicity of a `tick`/`update` mix;
`pack`/`unpack` and `encode`/`decode` round-trips; decode rejects a
wrong-length buffer; packed numeric order equals HLC order; encoded
bytes sort in HLC order; counter overflow is reported not wrapped; and
causality capture over a random DAG of events across 2-4 nodes with
per-node skew and stalling physical clocks.

### Elle / consistency harness

HLC is a clock primitive, not a store operation, so the list-append
Elle-style checker in `scripts/consistency/` does not directly apply
(there is no total order to linearize and no dependency-cycle anomaly
class over a clock). Per Section 6.5's framing (as the delta-CRDT and
SWIM prototypes documented for their non-list-append semantics), the DST
model plus the property tests carry the gate. The snapshot-read demo is
the closest thing to a recorded-history check and asserts the snapshot
predicate directly.

## Gate results

* `cargo build -p dyniak --features noxu --locked` -- ok
* `cargo build -p model-tests --locked` -- ok
* dyniak hlc property tests -- 9 passed
* dyniak hlc snapshot demo -- 2 passed
* dyniak lib hlc unit tests -- passed
* `cargo test -p model-tests` (hlc) -- 2 passed, negative control fires
* `scripts/model.sh` -- all models green, ~1.3s, 38 MB peak
* clippy `-D warnings` under `--features noxu` and `noxu,wasm,search`
  -- clean (fixed a `map(..).unwrap_or(..)` -> `map_or` in
  `now_from_wall_clock`)
* `cargo fmt -- --check` -- clean

## Limitations (production rollout still needs)

1. **MVCC rework is out of scope.** The snapshot demo proves HLC enables
   consistent snapshot selection; wiring HLC-stamped versions into the
   real write path, a version-keyed storage layout, and a snapshot-read
   API is the next slice.
2. **Wall-clock feed.** `now_from_wall_clock` reads `SystemTime`; a
   production deployment wants a monotonic clock source and a
   configurable maximum-drift alarm (the paper's bound becomes an
   operational SLO).
3. **No cross-node HLC exchange is wired.** HLC stamps are not yet
   piggybacked on the dnode plane; the RAMP-SI path that would consume
   them is itself single-node for now.
4. **Counter width.** `c` is a `u16`; a pathological burst of > 65535
   events at one non-advancing physical millisecond reports
   `CounterOverflow` rather than wrapping. That is the correct failure
   mode, but production tuning should size the counter and the physical
   granularity together.
