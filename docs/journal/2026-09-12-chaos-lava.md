# 2026-09-12 multi-region chaos on `lava` (noxu 7.10.1 + full feature set)

Re-ran the multi-region adversarial Dyniak chaos test on the `lava`
burner account (769093516156) after the dependency refresh (noxu
7.10.1, hegeltest 0.44) and the parity feature work (all six CRDTs,
sibling retention, KV read coordination + read-repair, runtime TTL
reaper, precommit hooks, R/W quorum enforcement).

## Topology + faults

* 6 Dyniak nodes (m6id.xlarge, local NVMe at /mnt/data/noxu) across
  us-east-1 / us-west-2 / eu-central-1 (2 per region, each its own
  rack/DC), n_val=3, distinct ring tokens.
* 3 topology-aware load generators (t3.medium), 240s counter workload,
  12331 total ops, 200 keys.
* Faults during load: 2 cross-region partitions (45s each) + 2 node
  churns (kill/restart, 30-40s each). Both churned nodes needed one
  restart retry to rebind PBC (the known near-simultaneous-restart
  bind race) and rejoined.

## Verdict

Availability (always-available, bounded tail through faults):

```
gen1  ok=4797  err=0  avail=100.0%    p99=50ms    p999=60ms
gen2  ok=2749  err=1  avail=99.964%   p99=160ms   p999=171ms  (its node churned)
gen3  ok=4785  err=0  avail=100.0%    p99=59ms    p999=80ms
```

Convergence:

```
keys_expected=200  total_ops=12331
lost_count=0  overcount_count=0  all_converged=true
CHAOS-VERDICT converged=True worst_avail=99.964% worst_p99=160.02ms
```

Every key converged on its replica set with zero lost updates and zero
over-counts, always-available with bounded p99, through two partitions
and two node churns -- on noxu 7.10.1 with the full parity feature set.
This is the same clean result as the 2026-07-24 run, confirming the
dependency bump and the recent features did not regress the
distributed path.

EC2 fully torn down and verified clean across all three regions (0
instances / SGs / prefix-lists) after the run.

## Caveat carried forward

This exercises CRDT counter convergence, availability, and the failover
client -- it does NOT exercise the background AAE exchange/repair
pipeline (which is not yet wired into dynomited; see the doc-accuracy
pass of 2026-09-12). Object read-repair on the read path IS exercised
implicitly by the topology-aware reads. The chaos harness itself is not
a committed CI-reproducible test; it is an operator-run scale
validation.
