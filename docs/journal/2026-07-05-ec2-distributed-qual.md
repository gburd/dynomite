# Distributed EC2 qualification of dynomite / noxu 7.3.0 (2026-07-05)

Real multi-AZ / multi-region EC2 deployment to validate dynomite's
distributed functionality at scale, after migrating dyniak to the
noxu 7.3.0 point-operation API (commit 44506a2).

## Deployment

- 6 nodes, 2 regions (= 2 datacenters), 3 AZs each:
  - us-east-2 (dc-use2): nodes in AZs a/b/c
  - us-west-2 (dc-usw2): nodes in AZs a/b/c
- t3.small (2 vCPU, 2 GB) on AL2023, root grown to 20 GB.
- The `dynomited` binary was built natively on an AL2023 node from
  the noxu-7.3.0 migration commit (riak feature, no search), then
  distributed to all six nodes.
- data_store: dyniak (in-process transactional noxu per node),
  gossip enabled, evenly-spaced u32 token ring, each node seeded
  with the other five by public IP on the dnode port.
- Security groups: allowlist-only /32 rules (controller IP + the six
  node IPs); NO 0.0.0.0/0 (see the account-termination note below).

## Results

### PASS -- the noxu 7.3.0 migration builds and runs in a real deployment

The migrated binary compiled cleanly on AL2023 (native, not the nix
dev shell) and ran on all six nodes.

### PASS -- cross-region gossip forms the ring

Every node discovered all five peers by their public IPs and
established dnode-plane connections across AZs and across regions.
After a brief startup race (peers not yet listening -> transient
"connection refused" retries), the cluster converged: zero
peer-connect failures within ~2 minutes of launch, dozens of
gossip / peer-up events per node, all listeners
(client/dnode/stats/pbc/http) up. This exercised real WAN peering
between us-east-2 and us-west-2.

### PASS -- dyniak transactional writes/reads work per node under 7.3.0

A `POST /transactions` write committed on the receiving node and was
read back correctly via HTTP GET (the storage-format + txn-abort
fixes from 1.1.1 hold on 7.3.0; no process abort under the write).

### FINDING (significant) -- dyniak cross-ring replication is not wired into `dynomited`

A write committed via us-east-2 node 1 was readable ONLY on that
node. The same-DC-different-AZ peer and both us-west-2 nodes returned
404. Root cause (confirmed in source, not inferred): the dyniak
per-replica fan-out lives behind `serve_pbc_with_routing` +
`RoutingHooks` / `BucketRouter` in the `dyniak` crate, but
`dynomited/src/riak.rs` calls the plain `serve_pbc` / `serve_http`
(and search/wasm) entry points -- it never constructs a
`BucketRouter` or passes `RoutingHooks`. So each dyniak node serves
its own local noxu environment and dyniak object writes do not
replicate across the ring.

The distributed machinery EXISTS (router, ReplicationStrategy,
per-replica dispatch, the dnode peer plane, gossip) and the ring
forms correctly, but the shipped server binary does not instantiate
the dyniak routing hooks, so dyniak data is node-local in a real
deployment. This is a library-capability-vs-wired-binary gap, only
observable by driving a real multi-node cluster (unit/integration
tests wire the hooks themselves). It is the headline finding of this
qualification and the clear next piece of work: wire
`serve_pbc_with_routing` / a routing-enabled HTTP serve path into
`dynomited` for `data_store: dyniak` pools, with the ring's
n_val / consistency honored.

Scope note: the RESP (valkey/memcache) proxy path DOES route through
`ClusterDispatcher` across the ring; this gap is specific to the
dyniak object/transaction API.

### FINDING (environmental, not a bug) -- noxu free-disk reserve vs small root disks

noxu 7.3.0 requires 5 GiB of free disk to remain available
(`free_disk` reserve). The initial 8 GB t3.small root (3.7 GB free
after OS + toolchain + binary) fell below the reserve, and noxu
correctly refused writes with "disk limit exceeded: used=0,
limit=5368709120". The error text is misleadingly labeled (it is a
free-disk-reserve check, not a used-bytes cap). Resolved by growing
the root volumes to 20 GB. A production dyniak node must be
provisioned with headroom above the 5 GiB reserve; worth surfacing
the reserve as a documented sizing requirement.

## Operational note -- prior account termination

The first attempt used security groups that opened the ssh +
dynomite ports to 0.0.0.0/0 (for cross-region peer reachability).
That is a world-open-instance policy violation and the AWS account
was terminated. The deploy script was rewritten to an allowlist-only
model (controller /32 + node /32s, never 0.0.0.0/0; commit 4c4ce42)
and the re-run on the replacement account used only tight rules,
verified live (zero 0.0.0.0/0 rules in either region's SG).

## Teardown

All EC2 resources (6 instances, 2 security groups, 2 key pairs
across both regions) were terminated and deleted after the run; the
teardown enumerates by the `dyn-run=<RUN_ID>` tag.
