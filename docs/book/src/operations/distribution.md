# Distribution modes

This page is the operator's reference for the two
first-class distribution algorithms the engine supports:
`vnode` (the historical default) and `random_slicing` (new).
The engine-level reference, including the C-to-Rust mapping,
lives in `docs/design/random-slicing-integration.md`; the
configuration syntax is in
[Configuration](../configuration.md#distribution-modes); this
page covers operator workflows.

## When to pick which

* **`vnode`** is the historical algorithm: each peer publishes a list of
  tokens and the dispatcher walks a per-rack continuum to find the
  owning peer. Pick `vnode` when you have an existing operator-managed
  token plan you want to preserve byte-identically, or when you depend
  on the exact vnode-to-peer mapping for an external system (e.g. a
  backup pipeline that walks the per-peer token list).
* **`random_slicing`** is the recommended mode for new
  deployments. Coverage is gap-free by construction: the chaos
  pass-3 failure mode (a 3-of-4 host topology silently leaving
  a quarter of the ring unowned) is structurally impossible.
  The operator-facing knob shrinks from "list of magic 32-bit
  integers per peer" to "one float per peer" (or simply nothing
  for a uniform partition).

In `--features riak` builds, `random_slicing` is the default
when a Riak listener is configured.

## Migration playbook

### 1. Run shadow mode

Set `distribution_shadow:` in the YAML (or pass
`--distribution-shadow=random_slicing` on the command line).
Every routing decision then computes both the live `vnode`
plan and the shadow `random_slicing` plan; the dispatcher
routes by `distribution:` (the live mode) and bumps
`distribution_shadow_disagreement_total` whenever the two
disagree.

```sh
dynomited --conf-file /etc/dynomite.yml \
          --distribution-shadow=random_slicing
```

Run shadow mode for a working day. Watch the counter:

```sh
dyn-admin distribution-dump --node 127.0.0.1:22222
# or, for the raw counter,
curl -s 127.0.0.1:22222/metrics | grep distribution_shadow
```

The counter is a u64 that grows monotonically; a stable value
means no recent disagreements (which on a fresh cluster will
never happen because the algorithms produce independent
partitions).

### 2. Cut over

Edit the pool YAML:

```yaml
dyn_o_mite:
  ...
  distribution: random_slicing       # was 'vnode'
  distribution_shadow: vnode         # keep the old mode as
                                     # the shadow for safety
```

Issue `kill -HUP $(pgrep dynomited)` on every node. The
SIGHUP-reload pipeline rebuilds the rack ring atomically; no
restart is required. The first request after the reload that
lands on a peer that does not yet hold the key locally returns
a miss (memcache) or kicks off a read-repair (Redis dyn-mode);
the cluster converges through the usual entropy / repair
machinery.

### 3. Drop the shadow

Once the cluster is happy and the disagreement counter has
stopped growing on every node, remove `distribution_shadow:`
from the YAML and SIGHUP again. The shadow path is now fully
dormant.

## Rollback

If shadow mode disagreed but the operator cut over anyway and
is now unhappy, revert the YAML and SIGHUP. The `vnode` ring
rebuilds deterministically from the unchanged `tokens:` lists.
Keys written under `random_slicing` are returned by their
original `vnode` owner once that owner runs read-repair against
the new primary.

## Peer-state interaction

In v1, the random-slicing slice table is built once per
`rebuild_ring` and includes every peer in the rack regardless
of state. Down peers are filtered out by the dispatcher's
existing `is_routable()` filter on top of the slice lookup;
a Down peer is invisible to a per-key route just like it is
under `vnode`. Removing or replacing a peer requires a
configuration reload (the gossip path that already does this
for `vnode` is unchanged).

This v1 limitation is documented in `docs/parity.md` under the
random-slicing Deviation entry.
