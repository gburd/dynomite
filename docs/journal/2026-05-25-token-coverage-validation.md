# 2026-05-25: token-coverage validation and pass-3 root-cause re-analysis

## Scope

Deliver `crates/dynomite/src/cluster/coverage.rs`: a config-time
validator that catches the class of token-ring misconfigurations
that pass-3 chaos surfaced. Wire-in to `ConfPool::validate()` is
out of scope for this task.

## Implementation

`validate_token_coverage(peers: &[Peer]) -> Result<(),
TokenCoverageError>` groups the supplied peers by `(dc, rack)`,
collects every `(token, peer_idx)` pair the rack contributes,
sorts numerically, and reports the first fault encountered.

Two error variants, both reachable:

* `EmptyRack { dc, rack }` - the rack has no peers, or every peer
  in the rack has an empty token list. The dispatcher silently
  produces zero candidates for any key hashed against this rack.
* `Overlap { dc, rack, token, peer_a, peer_b }` - two peers in
  the same rack claim the exact same token point. The dispatcher
  picks whichever sorts first, which is non-deterministic across
  reloads or peer-array reshuffles.

A `Gap` variant is **not** included. See the next section.

## Why `Gap` is structurally impossible

`crates/dynomite/src/cluster/vnode.rs::dispatch` (the project's
sole token-to-peer resolver) implements wrap-around vnode
semantics:

```text
n = continuums.len()
if n == 0:                      return None
if last.token < search:         return first.peer_idx     # wrap high
if first.token >= search:       return first.peer_idx     # wrap low / boundary
otherwise: binary-search smallest entry with token >= search
```

The two early-return branches together guarantee that for any
non-empty continuum, *every* `u32` token resolves to some peer:

* Tokens above `last.token` wrap to the first peer.
* Tokens at or below `first.token` resolve to the first peer.
* Tokens in between are bracketed by two adjacent continuum
  entries and resolve to the upper bracket's owner.

There is no `(lo, hi)` window in `[0, u32::MAX]` that fails to
resolve. The only way to produce "no candidates" at the per-rack
layer is to leave the rack with zero continuum entries, which is
exactly `EmptyRack`. There is therefore no point in implementing
gap detection: the validator would have nothing to detect.

## Pass-3 root-cause re-analysis

The brief framed pass-3's 14% workload error rate as "1/4 of the
ring unowned" because the coordinator hardcoded a 4-way token
split (`0`, `1G`, `2G`, `3G`) for four hosts but only spawned
three. Reading the dispatcher carefully, that framing is
**incorrect**: pass-3's failure was *not* a token-coverage gap
under Dynomite's vnode semantics.

Two scenarios are possible, depending on how the coordinator
generates the live conf file:

### Scenario A: conf lists all four peers; one never starts

The live `dynomited` process loads a conf with four peer entries.
The fourth peer (token `3G`) is never reachable. After gossip
times out, peer 3's state transitions to `PeerState::Down`. The
continuum still has all four entries, so
`vnode::dispatch(token_in (2G, 3G])` returns `peer_idx = 3`
deterministically.

Then in `cluster::dispatch::collect_routable`:

```rust
if let Some(peer_idx) = vnode::dispatch(...) {
    if let Some(peer) = peers.get(peer_idx as usize) {
        let state = peer.state();
        let accept = state.is_routable() || (include_down && ...);
        if accept { routable.push(...); }
    }
}
```

Peer 3 is `Down`, so `state.is_routable() == false`, so it's
skipped. With only one rack (the typical pass-3 shape), the
routable set is empty for keys hashing into `(2G, 3G]` and the
plan is `NoTargets`.

This is a **runtime liveness** issue, not a config-time gap.
`validate_token_coverage` is run at config-load time and sees
all four peers with valid, non-overlapping tokens; it correctly
returns `Ok(())` because the static config *is* valid. No
config-time validator can catch this without also reaching out
to the network to confirm liveness, which is out of scope for
config validation.

### Scenario B: conf lists only the three running peers

The coordinator omits the fourth host from the conf. The live
process sees three peers with tokens `0`, `1G`, `2G`. By the
wrap-around rule, peer 0 owns `(2G, u32::MAX]` plus `[0, 0]`,
peer 1 owns `(0, 1G]`, peer 2 owns `(1G, 2G]`. **Full coverage.**
No `NoTargets` is ever emitted at the dispatch layer.

`validate_token_coverage` returns `Ok(())` here too, and that's
correct: the config is valid. If pass-3 saw a 14% drop with this
shape, the cause is somewhere else entirely.

### Hypotheses for the actual 14% drop

Ranked by likelihood given the chaos profile:

1. **Down-peer transient gap (Scenario A)**. Most likely. The
   conf had four peers, one was forcibly killed by the chaos
   injector, and gossip's failure-detection settling time
   produced a short window during which the dispatcher had not
   yet reclassified the missing peer. During that window, keys
   hashing into the missing peer's arc generated `NoTargets`.
   The 14% number is consistent with `1/4` of keys * a fraction
   of the run time before phi-accrual flagged the peer (not the
   whole run, since once reclassification happens the routable
   set still does not include peer 3 for reads, but for *writes*
   with hinted handoff active it would).

2. **Hinted-handoff disabled or hint store unwired**. Even after
   gossip flags peer 3 as Down, reads still produce `NoTargets`
   for that arc (handoff only affects writes). If the chaos
   workload was read-heavy, this looks like a sustained 25%
   read-error rate, scaled down to 14% if some reads land on
   already-down-peer arcs but get retried on a different DC by
   client logic.

3. **Single-rack topology**. Pass-3 likely ran one rack per DC.
   Multi-rack topologies replicate the ring per rack, so a
   single Down peer in one rack is masked by a healthy peer in
   another rack. Single-rack means no replication; one Down
   peer maps directly to one ring slice with no candidates.

4. **`make_error` path producing visible error** (the one
   already shipped). Before that fix, the same situation
   manifested as a request-timeout hang on the client side,
   which the workload probably retried into eventual success;
   after the fix it surfaces as an immediate `NoTargets` error
   that the workload counts as a failure. The 14% drop may
   therefore be largely *unchanged underlying behaviour* with
   improved client-visible failure semantics.

### Recommended follow-ups (out of scope here)

* Wire `validate_token_coverage` into `ConfPool::validate()`.
  Catches the operator-error class even though it does not
  catch pass-3's specific situation.
* Audit pass-3's chaos coordinator to confirm it was Scenario A
  vs B; correlate the dispatch's `NoTargets` count with the
  failure-detector transition timestamps.
* If hypothesis 1 is confirmed, consider lowering phi-accrual's
  detection floor so the Down transition fires faster, or pre-
  blacklist conf-listed peers that fail the initial `connect()`
  for longer than `gos_node_timeout_ms`.
* Make hinted-handoff cover read replays for the single-rack
  topology case (currently writes-only).

## Files touched

* `crates/dynomite/src/cluster/coverage.rs` (new, 12 tests).
* `crates/dynomite/src/cluster/mod.rs` (added `pub mod coverage;`).
* `docs/journal/2026-05-25-token-coverage-validation.md` (this
  entry).

## Test summary

* `cargo nextest run --workspace`: 1216 -> 1228 (+12 new
  coverage tests).
* `cargo test --doc -p dynomite cluster::coverage`: 2 doctests,
  pass.
* `cargo clippy --workspace --all-targets --all-features --
  -D warnings`: clean.

## Variants implemented

`{ EmptyRack, Overlap }`. `Gap` deliberately omitted because
Dynomite's wrap-around vnode dispatch makes ring gaps
structurally unreachable; emitting it would be dead code.
