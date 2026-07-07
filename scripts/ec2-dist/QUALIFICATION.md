# Full-qualification orchestrator

`run-full-qualification.sh` drives the entire multi-region,
mixed-architecture qualification to completion **unattended** and tears
everything down at the end. It is meant to run detached on a controller
or bastion host that has the AWS CLI (`--profile numa`), SSH access, and
this repository, so a disconnect never interrupts the run.

## What it does

A phase state machine (checkpointed to `$STATE_DIR/phase`, so a
re-invocation resumes from the last completed phase):

1. **provision** -- 5 regions x 9 nodes (3 racks x 3 nodes per region;
   each rack a full replica, the 3 nodes in a rack partition the token
   ring so `n_val = 3`). Three Intel regions (`m6id.xlarge`, x86_64) are
   the "old" fleet; two Graviton regions (`m8gd.xlarge`, arm64) are the
   "new" fleet. Local NVMe instance-store; moderate 4 vCPU / 16 GiB.
   (`deploy-mixed.sh up`.)
2. **build** -- Rust `dynomited` (`--no-default-features --features
   riak`) and the C Netflix `dynomite` reference, for BOTH
   architectures, on one node of each arch, in parallel.
   (`qual-build.sh`.)
3. **distribute** -- push the arch-correct Rust + C binaries and the
   differential driver to all 45 nodes (throttled 8 at a time).
4. **mount** -- format + mount the NVMe instance store at `/mnt/data`
   and point noxu data there; nodes without a second NVMe fall back to
   the home directory (non-fatal).
5. **matrix** -- the exhaustive C-vs-Rust differential. For each
   consistency level (`DC_ONE`, `DC_QUORUM`) the whole cluster is
   relaunched with C on `8101/8102` (backend `6379`) and Rust on
   `9101/9102` (backend `6380`) -- SEPARATE backends so the two rings
   never share a store. The differential driver runs from an entry node
   in every region (both archs), several seeds each, and a cell passes
   only at >= 99.5% agreement (a transient read-after-write timing race
   in the eventual-consistency window is tolerated; a real divergence
   is not). Results in `$STATE_DIR/results/matrix-<CONS>.txt`.
6. **migrate** -- under a constant 60/40 read/write load, grow to a 3rd
   Graviton region (`eu-west-1`) and then drain + remove the three Intel
   regions one at a time. Gossip re-routes and replication keeps
   `n_val` satisfied on the surviving Graviton racks. The load records a
   history; a lost-write audit runs at the end.
   (`deploy-mixed.sh add-region`.)
7. **jepsen** -- deploy Jepsen on a surviving Graviton node and run a
   register-linearizability + partition-nemesis test against the final
   3-region Graviton cluster's RESP plane. (`qual-jepsen.sh`.)
8. **teardown** -- terminate every instance, delete every security group
   and key pair, across all regions including the migration target.
   Runs even on a phase failure unless `KEEP_ON_FAIL=1`.

## Running it

```
cd scripts/ec2-dist
RUN_ID=dyn-qual-$(date -u +%Y%m%d-%H%M%S) \
  setsid bash run-full-qualification.sh > /tmp/$RUN_ID.orch 2>&1 < /dev/null &
# follow along:
tail -f /tmp/$RUN_ID/orchestrator.log
```

Environment knobs:

* `RUN_ID` -- unique tag for all AWS resources (default: timestamped).
* `SRC_DIR` -- the dynomite source tree (default: this repo).
* `STATE_DIR` -- scratch + results (default: `/tmp/$RUN_ID`).
* `KEEP_ON_FAIL=1` -- leave the cluster up on a phase failure for
  inspection (default: tear down regardless).
* `PROFILE` -- AWS CLI profile (default: `numa`).

Results land in `$STATE_DIR/results/` (per-phase files) and the running
narrative in `$STATE_DIR/orchestrator.log`.

## Resumption

Each completed phase appends its name to `$STATE_DIR/phase`. Re-running
the orchestrator with the same `RUN_ID`/`STATE_DIR` skips completed
phases. To force a full re-run, use a fresh `RUN_ID`.

## Security discipline

Security groups are allowlist-only: the controller `/32` plus every
node `/32`, on consolidated port ranges (`8087-9102`, `22222-22223`,
`22`) to stay under the 60-rule/SG quota. Never `0.0.0.0/0`. All
resources are tagged `dyn-run=<RUN_ID>` and swept by that tag at
teardown.

## Honest limitations

* The migration load generator records raw op batches, not a strictly
  ordered list-append history; the lost-write audit is therefore a
  best-effort convergence check rather than a full Elle cycle analysis.
  A stronger audit would drive `scripts/consistency/txn_history_
  workload.py` and feed `elle_check.py`.
* The Jepsen test uses jepsen's library form with a no-op DB (the
  cluster is already running) and a last-write register model over the
  RESP `SET`/`GET` the proxy exposes; it exercises linearizability under
  a partition nemesis. It is not the full `jepsen.dynomite` project.
* First run compiles Rust from scratch on both a x86 and an arm node
  (~3-5 min each) and builds the C reference; the whole pipeline is
  multi-hour. Provisioning 45 global nodes is sequential and takes
  ~30 min.
