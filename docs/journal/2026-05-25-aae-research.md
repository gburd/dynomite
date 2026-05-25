# 2026-05-25 - AAE comparison vs Riak generations 1 and 2

## Background

Operator request: evaluate our shipped Tictac AAE
implementation against Riak's two generations of Active
Anti-Entropy and identify gaps.

## What happened

Two sub-agent dispatches for this research (an Explore-typed
agent, then a general-purpose retry) both completed without
producing a deliverable.  The Explore type is read-only and
cannot commit; the general-purpose retry was cleaned up
without leaving a branch.  The lead wrote the analysis
directly from working memory.

Result: `docs/design/aae-comparison.md` (~13 KB).

## Summary

* What we shipped is structurally aligned with Riak gen-2
  (the NHS-Digital / Martin Sumner redesign): rolling
  time-bucket merkle, three-phase exchange, sibling-aware
  merge with DVV semantics.
* Storage-agnostic tree is a deliberate trade-off: works
  against any `Datastore` impl, but pays for it with a
  full rebuild on every reconcile.  Gen-2's tight coupling
  to `leveled` buys "fold on demand" without a maintained
  tree.
* Wire format is Rust-self-consistent, not Erlang ETF.
  Re-affirmed as non-goal in 2026-05-25 discussion.
* DVV-aware repair landed in commit `efd3511` (DVVSet
  default for Riak causality), matching what gen-2
  ships post-2018.

## Recommended follow-ups (~9 days total)

* R1: persist tree across restart (~2 days)
* R2: native-fold path for `NoxuDatastore` (~3 days)
* R3: per-token (not per-peer) exchange (~2 days)
* R4: operator metrics for AAE health (~1 day)
* R5: `dyn-admin aae-status` command (~1 day)

None change the wire format or add dependencies.

## Caveats

The analysis is based on lead's working memory rather than
fresh research.  Specific benchmark numbers and citations
should be verified against:

* `basho/riak_kv/src/riak_kv_index_hashtree.erl` (gen-1)
* `martinsumner/kv_index_tictactree` (gen-2 library)
* `nhs-riak/riak_kv/src/riak_kv_tictacaae_handler.erl` (gen-2 integration)
* Martin Sumner's blog posts and Riak users mailing list
  archives circa 2018-2020

A future research pass with confirmed web access could
improve the citations.
