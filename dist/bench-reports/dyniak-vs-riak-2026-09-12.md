# Dyniak vs Apache Riak KV: single-node head-to-head benchmark

Date: 2026-09-12
Region: AWS us-east-1 (`lava` profile)
Base commit: `d3fc627`
Run tag: `dyn-run=dynriak-20260912-160820` (torn down, verified clean)

## Summary

Real Apache Riak KV 3.x-lineage (2.2.3, the last version Basho
published a maintained Docker image for) was successfully installed
and run on AL2023 via Docker. Both systems were benchmarked with the
same `dyniak-bench` binary over the Riak PBC wire protocol, same
workload shapes, same instance type, same region, same duration.

**Headline result: dyniak's Riak-PBC surface currently trails Riak by
roughly an order of magnitude in this benchmark, and the entire gap
traces to one specific, fixable defect in the response-write path
(`crates/dyniak/src/proto/pb/framer.rs::write_frame`), not to noxu, the
storage engine, or CPU/network capacity.** See "Root-cause finding"
below. The "similar or better than Riak" claim is **not substantiated
by this run** as measured, but the measurement is dominated by a
one-line-class bug rather than a structural ceiling. A re-run after
that fix is required before the claim can be evaluated honestly.

Two additional defects were found and are documented rather than
silently worked around (see "Defects found").

## Environment

| | |
|---|---|
| Region | us-east-1 |
| Instance type (both nodes) | `m6id.xlarge` (4 vCPU, 16 GiB RAM, 1x NVMe instance store) |
| AMI | `ami-0de568ccf3b0080d9` (Amazon Linux 2023, x86_64) |
| Dyniak binary | `dynomited` built on-node from this tree's HEAD worktree (with the `dyniak-bench` fix below), `cargo build --release -p dynomited --no-default-features --features riak` |
| Dyniak storage | noxu (transactional), data on the mounted instance-store NVMe at `/mnt/data/noxu` |
| Riak | `basho/riak-kv:latest` Docker image, tag reports Riak `2.2.3`, default `bitcask` backend, data on the mounted instance-store NVMe at `/mnt/data/riak-bitcask` |
| Load driver | `dyniak-bench` (this tree, `crates/dyniak-bench`), built on-node, `riak_pbc` driver, `--features riak` |
| Network | loopback (both dyniak-bench and the server under test ran on the same node; there was no cross-node traffic in the timed runs) |
| Security | allowlist-only SG, controller `/32` only, ports 22/8087/8098/8101/8102/22222; no `0.0.0.0/0` anywhere |

Riak was not trivial to stand up on AL2023: there is no native AL2023
package, and building from source pulls an old pinned Erlang/OTP
toolchain that is a multi-hour yak-shave. The `basho/riak-kv` Docker
image (Erlang bundled, Ubuntu 14.04 base layer) was the pragmatic
path and produced a real, unmodified Riak 2.2.3 node with PBC, HTTP,
bitcask, and the CRDT bucket-type machinery all working as documented
upstream. Two Docker-specific snags, both resolved and documented in
`scripts/ec2-dist/dyniak-vs-riak-bench.sh`:

1. The container's entrypoint (`riak-cluster.sh`) times out at 15s by
   default waiting for the Erlang VM; `WAIT_FOR_ERLANG=60` fixes this
   under Docker's default cgroup/network setup.
2. Riak's `riak` process drops privileges to a `riak` user (uid 102,
   gid 105) before touching `/var/lib/riak`; a root-owned bind mount
   (Docker's default when the host directory does not pre-exist with
   the right owner) makes every write fail with `permission denied`.
   `chown 102:105` on the host-side bind-mount directories before
   `docker run` fixes it.

## Methodology

* Same `dyniak-bench` binary (AL2023-native build, not a
  cross-compiled artifact) on both nodes.
* Same workload TOML on both systems, `bucket`/`bucket_type` renamed
  only where the CRDT identity model differs (see "CRDT identity"
  caveat below); ops/keygen/valgen unchanged.
* 90 second measurement window per workload per system, `report_interval
  = 1s`, `rate = "max"` (uncapped, closed-loop, `concurrent` workers
  each issuing one op at a time -- the standard basho_bench/dyniak-bench
  model).
* A 15s warm-up run (discarded) preceded the timed PBC run on each
  system to page in the binary and warm TCP/connection state.
* No A/B alternation across systems was needed (this is not a
  micro-architectural drift concern the way repeated CPU benchmarking
  is); each system got exactly one warm-up + one timed run per
  workload, run once, not batched-and-averaged. A single-shot 90s
  run at this variance level is standard for this kind of workload
  comparison but is a smaller n than an ideal report would use --
  flagged as a caveat.
* Workload TOMLs are committed alongside this report:
  `wl-pbc.toml`, `wl-crdt-counter.toml`, `wl-crdt-set.toml`,
  `wl-crdt-map.toml`.

## Workloads

### `wl-pbc.toml` -- balanced PBC ping/get/put/del

90s, 32 concurrent workers, uncapped rate. Pareto keyspace (100K keys,
shape 1.5), exponential-256B values. Mirrors
`crates/dyniak-bench/examples/riak-pbc.toml` with `duration = 90s`.

### `wl-crdt-counter.toml`, `wl-crdt-set.toml`, `wl-crdt-map.toml` -- CRDT updates

90s, 16 concurrent workers, uncapped rate. Uniform 10K keyspace,
uniform 8-64B values. Each runs a single CRDT op class in its own
bucket (`crdts_counter`, `crdts_set`, `crdts_map`) against the
matching Riak bucket-type (`counters`, `sets`, `maps`), all created
and activated on the Riak node before the run. Splitting into
separate per-type buckets (rather than the single mixed `crdts`
bucket in the shipped `riak-crdts.toml` example) is a deliberate
deviation -- see "Defects found #2" below.

## Results

All throughput figures are `ok_count` summed over the run divided by
wall-clock elapsed seconds (from `summary.csv`); latency figures are
the median of each window's reported percentile (from `summary.csv`
for CRDT ops, from the per-op `*_latencies.csv` for the mixed PBC
workload).

### Aggregate throughput and tail latency, by workload

| System | Workload | Ops (ok) | Errors | Elapsed (s) | Throughput (ops/s) | p50 (ms) | p99 (ms) | p99.9 (ms) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| dyniak | PBC mixed (ping/get/put/del) | 55,109 | 0 | 90.3 | 610 | 50.0 | 60.1 | 60.2 |
| Riak 2.2.3 | PBC mixed (ping/get/put/del) | 389,376 | 1 | 90.4 | 4,305 | 2.4 | 29.3 | 32.8 |
| dyniak | counter_inc | 27,922 | 0 | 90.0 | 310 | 50.0 | 60.1 | 60.1 |
| Riak 2.2.3 | counter_inc | 269,265 | 0 | 90.1 | 2,990 | 5.3 | 8.6 | 10.2 |
| dyniak | set_add | 27,967 | 0 | 90.0 | 311 | 50.0 | 60.1 | 60.1 |
| Riak 2.2.3 | set_add | 237,133 | 0 | 90.1 | 2,633 | 6.0 | 9.9 | 11.7 |
| Riak 2.2.3 | map_update | 244,566 | 0 | 90.1 | 2,715 | 5.8 | 9.4 | 11.3 |

dyniak's `map_update` is intentionally omitted from the comparison
table -- see "Defects found #2".

### Per-op breakdown, PBC mixed workload

| System | Op | Ops (ok) | Throughput (ops/s) | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) |
|---|---|---:|---:|---:|---:|---:|---:|
| dyniak | ping | 16,638 | 184 | 50.0 | 60.0 | 60.1 | 60.1 |
| dyniak | get | 16,472 | 183 | 50.0 | 60.1 | 60.1 | 60.2 |
| dyniak | put | 16,457 | 182 | 50.0 | 60.1 | 60.1 | 60.1 |
| dyniak | del | 5,542 | 61 | 50.0 | 60.1 | 60.1 | 60.1 |
| Riak 2.2.3 | ping | 117,235 | 1,296 | 0.2 | 0.4 | 0.7 | 1.5 |
| Riak 2.2.3 | get | 116,644 | 1,290 | 11.1 | 15.0 | 16.6 | 18.0 |
| Riak 2.2.3 | put | 116,579 | 1,289 | 13.3 | 17.7 | 19.6 | 21.2 |
| Riak 2.2.3 | del | 38,918 | 430 | 22.7 | 30.5 | 32.7 | 34.5 |

Raw CSVs for every row above are under `csv/dyniak/` and `csv/riak/`
next to this report.

## Root-cause finding: the ~50ms/op floor on dyniak's PBC path is a bug, not a ceiling

Every dyniak PBC op above -- `ping`, `get`, `put`, `del`,
`counter_inc`, `set_add` -- clusters at almost exactly `p50 = 50.0ms,
p99 = 60.1ms` regardless of op type or concurrency. That signature
(a fixed cost, independent of what the op actually does) is the
signal that something structural, not the workload, is dominating.
Investigation traced it to a specific defect:

* `crates/dyniak/src/proto/pb/framer.rs::write_frame` issues **three
  separate `write_all` calls** per response (4-byte length prefix,
  1-byte message code, then the body), followed by one `flush()`.
* The PBC accept loops (`serve_pbc_full` and `serve_pbc_quic_inner`
  in `crates/dyniak/src/server.rs`) **never call
  `TcpStream::set_nodelay(true)`** on the accepted socket, so Nagle's
  algorithm stays enabled server-side.
* With Nagle enabled and a multi-write response, the OS holds the
  first small segment waiting either for more application data (none
  is coming -- `write_frame` is done) or a delayed ACK from the
  peer, which on Linux defaults to firing after tens of
  milliseconds. The client observed `ato:40` (40ms ACK timer) on a
  live connection via `ss -tin`, consistent with delayed-ACK/Nagle
  interaction stalling every response.
* Confirmed directly: a hand-rolled Python PBC client issuing a bare
  `ping` on a **fresh** connection each time gets sub-0.1ms round
  trips; the same client **reusing one connection** for repeated
  pings gets ~50ms per ping after the first -- exactly the
  Nagle/delayed-ACK signature, and exactly what `dyniak-bench`'s
  driver does (it keeps one connection per worker for the whole
  run).
* The arithmetic closes the loop: `concurrency (32) / latency
  (~50ms)` is ~640 ops/sec, which is within a few percent of the
  measured 610-611 ops/sec aggregate PBC throughput. The bottleneck
  is not CPU, not noxu, not the network -- it is this fixed per-op
  stall.

**This is a real, previously undocumented performance defect in
dyniak's Riak PBC server**, not a benchmark artifact and not a
storage-engine tradeoff. It is very likely fixable with a small,
targeted change (either coalesce `write_frame`'s three writes into
one buffer + one `write_all`, or call `set_nodelay(true)` on accept,
or both) and, if fixed, would be expected to remove the dominant
bottleneck this report measured. This falls under `crates/dyniak/src`,
which is out of scope for this benchmarking task to touch; it is
reported here for the owning team to pick up, along with a
regression-test suggestion: a PBC round-trip latency test asserting
p50 stays in the low single-digit milliseconds on loopback, which
would have caught this immediately.

**Consequence for this report**: the throughput and latency numbers
above are real, honestly measured, and reproducible -- but they
measure dyniak with this defect present, not dyniak's underlying
capacity. Until the fix lands and the benchmark is re-run, no
"comparable to Riak" or "N% of Riak's throughput" claim should be
made from this data in either direction.

## Defects found (and fixed, where in scope)

### 1. `dyniak-bench`'s CRDT PBC encoder used the wrong wire layout (fixed)

`crates/dyniak-bench/src/driver/riak.rs`'s `encode_counter_inc`,
`encode_set_add`, and `encode_map_update` built the `RpbDtUpdateReq`
body with the operation payload at protobuf field 4. Per the
upstream `riak_dt.proto` (`https://github.com/basho/riak_pb`), field
4 of `DtUpdateReq` is `context`; the operation is field 5. Every
CRDT op therefore landed as an empty/malformed update and was
rejected by both dyniak (`"unsupported or empty op"`) and, when
checked against real Riak, by Riak's own protobuf decoder. This was
a bug in the benchmark tool itself, not a dyniak-specific problem;
it would have broken any user trying to drive CRDT load with this
driver against any Riak-protocol server. `encode_map_update`
additionally wrapped the per-field operation in a nonexistent
`ScalarOp` message and used the wrong `MapOp` field numbers.

Fixed in `crates/dyniak-bench/src/driver/riak.rs` (this task's scope
allowed touching `dyniak-bench`; `dyniak`/`dynomite`/`dynomited` were
not touched). Verified against real Riak 2.2.3's actual wire decoder
(the fix eliminated the `riak_dt_pb:decode` badarg crashes visible in
Riak's own error responses) and against dyniak. Unit tests in that
file continue to pass (`cargo test -p dyniak-bench --features riak
--lib`, 93 passed).

### 2. dyniak's Map CRDT wire schema does not match real Riak's protocol (found, not fixed -- out of scope)

While validating the fix above against real Riak, `map_update`
against dyniak failed with `RpbErrorResp: unsupported or empty op`
even after emitting a spec-correct request. Reading dyniak's
`crates/dyniak/src/proto/pb/datatypes.rs` shows two divergences from
the published `riak_dt.proto`:

* dyniak's `MapOp` has `updates` at field 1 and `removes` at field
  2; upstream has `removes` at field 1 and `updates` at field 2
  (swapped).
* dyniak's `MapUpdate` wraps the per-field operation in a
  `ScalarOp` sub-message (field 2) that does not exist in the
  published protocol at all; upstream's `MapUpdate` has flat fields
  (`field=1, counter_op=2, set_op=3, register_op=4 [raw bytes, not
  wrapped], flag_op=5, map_op=6`).

This means dyniak's Map-CRDT PBC surface is not wire-compatible with
any standard Riak PBC client today, only with itself. This is a
parity gap in `crates/dyniak/src`, out of scope for this task to fix;
it should be added to `docs/parity.md` and picked up by the
dyniak-owning team. `dyniak-bench`'s `map_update` encoder was still
corrected to match the real upstream protocol (benefiting anyone
using the tool against real Riak or a future spec-correct dyniak),
which is why `map_update` numbers exist for Riak but not dyniak in
this report.

Separately, and unrelated to the wire format: `dyniak`'s
`Datastore::riak_get`/`riak_put` trait methods
(`crates/dynomite/src/embed/hooks.rs`) key storage by `(bucket, key)`
only, dropping `bucket_type`. Real Riak's CRDT/object identity is
`(bucket_type, bucket, key)`. The shipped
`crates/dyniak-bench/examples/riak-crdts.toml` example puts
counter/set/map ops all in one bucket (`crdts`) differentiated only
by `bucket_type`, which works against real Riak but causes
"type tag mismatch" errors against dyniak once two CRDT types land
on the same `(bucket, key)`. Worked around in this report's
workloads by giving each CRDT type its own bucket name (a realistic
client-side workaround, not a hidden skip); documented here as a
storage-key gap for the dyniak-owning team.

## Storage-engine asymmetry caveat

Dyniak uses noxu (a transactional embedded engine with WAL-based
`CommitSync` durability by default) on the mounted NVMe; Riak uses
bitcask (an append-only log + in-memory hash-table index) on the same
NVMe. This is the expected, intended configuration for both systems
(noxu is dyniak's only storage backend; bitcask is Riak's default),
not a benchmark misconfiguration, but it means throughput numbers on
the write path are not a clean engine-vs-engine comparison even
setting the framer bug aside -- bitcask's index is entirely in RAM and
its writes are pure appends, while noxu's transactional path does
WAL commit + B-tree maintenance. Per the task brief's guidance,
latency percentiles are the more defensible comparison axis here,
though in this run the framer defect dominates dyniak's latency
numbers too, so even the percentile comparison is not yet reading
the storage engines against each other -- it is reading the framer
bug.

## Verdict: is dyniak "similar to or better than Riak"?

**Not established by this run.** As measured, dyniak is roughly
7-10x slower in throughput and 5-20x worse in p50/p99 latency across
every workload. But every dyniak workload in this report shows the
identical ~50ms fixed-cost signature traced above to a specific,
narrow defect in the PBC response-write path, not to noxu, not to
CPU, not to the network. This is not "dyniak's real performance
looked mediocre and the storage asymmetry explains part of it" --
it is "dyniak's real performance was not measured, because a
framing/Nagle bug added a ~50ms floor to every single PBC round
trip." Per-dimension:

* **Throughput**: worse in this run (by ~7-10x), not credible as a
  ceiling given the root cause above. No verdict possible until
  re-measured post-fix.
* **Latency (p50/p99/p99.9)**: worse in this run, same caveat.
* **Correctness/feature surface** (ping/get/put/del, counter, set all
  work end-to-end against dyniak with zero errors at load; map
  currently does not, per the parity gap above): dyniak's core PBC
  surface is functionally solid where it is wired up; the CRDT map
  type needs the parity fix before it is usable from any standards-
  compliant Riak client.

**Recommendation**: fix the `write_frame` multi-write / missing
`set_nodelay` defect in `crates/dyniak/src/proto/pb/framer.rs` and
`crates/dyniak/src/server.rs`, then re-run this exact benchmark
(`scripts/ec2-dist/dyniak-vs-riak-bench.sh`) before making or
retracting any "comparable to Riak" claim. Given the raw NVMe fsync
latency measured on this hardware (~0.1-0.2ms) and Riak's own
15-20ms p50 on `get`/`put` (dominated by bitcask + Erlang VM
overhead, not raw disk), there is no evidence in this data that
dyniak's real ceiling is below Riak's once the fix lands -- but that
is a hypothesis for the next run to confirm, not a claim this run
supports.

## Reproduction

```
RUN_ID=dynriak-$(date -u +%Y%m%d-%H%M%S) SRC_DIR=/home/gburd/ws/dynomite \
  scripts/ec2-dist/dyniak-vs-riak-bench.sh up
scripts/ec2-dist/dyniak-vs-riak-bench.sh run
scripts/ec2-dist/dyniak-vs-riak-bench.sh down
```

Results land under `/tmp/${RUN_ID}/results/{dyniak,riak}/{pbc,counter,set,map}/`.

## Files in this report directory

```
wl-pbc.toml               PBC mixed ping/get/put/del workload
wl-crdt-counter.toml      CRDT counter_inc workload
wl-crdt-set.toml          CRDT set_add workload
wl-crdt-map.toml          CRDT map_update workload (Riak-only, see above)
csv/dyniak/pbc/           dyniak PBC mixed run: summary.csv + per-op *_latencies.csv + errors.csv
csv/dyniak/counter/       dyniak counter_inc run
csv/dyniak/set/           dyniak set_add run
csv/riak/pbc/             Riak PBC mixed run
csv/riak/counter/         Riak counter_inc run
csv/riak/set/             Riak set_add run
csv/riak/map/             Riak map_update run
```

## EC2 teardown verification

All resources tagged `dyn-run=dynriak-20260912-160820` were
terminated/deleted and verified absent (0 instances, 0 security
groups, 0 key pairs, 0 managed prefix lists, 0 EBS volumes) across
`us-east-1`, `us-east-2`, `us-west-2`, and `eu-central-1` after
teardown. An unrelated pre-existing instance in `us-east-1`
(`xtc-pmbench-20260912-101412`, a different tag namespace) was left
untouched throughout, as it belonged to a different task.
