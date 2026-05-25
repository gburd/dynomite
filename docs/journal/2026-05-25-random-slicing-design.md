# 2026-05-25: random-slicing integration design recorded

Stage: design only
Branch: design/random-slicing
Author: design sub-agent

## Summary

A design document evaluating the integration of "random slicing"
(an alternative to consistent hashing with vnodes) into Dynomite is
landed at `docs/design/random-slicing-integration.md`. The document
is design-only and awaiting operator approval before any
implementation work begins.

## Trigger

Two converging inputs:

1. The InfoQ article
   `https://www.infoq.com/articles/dynamo-riak-random-slicing/` from
   the operator's friend, plus a faithful C reference at
   `hash_demo.c` in the repository root.
2. The chaos pass-3 redis-mode finding (recorded under
   "Pass-3 follow-ups" in `docs/post-chaos-queue.md`): a hand-rolled
   4-way vnode token plan running on 3 hosts left 25% of the ring
   unowned and produced a 14-point success-rate drop. Random slicing
   makes that class of bug structurally impossible.

## Key recommendation

**Yes, ship as opt-in.** Add `Distribution::RandomSlicing` alongside
the current vnode behaviour; keep vnode the default; gate cutover
behind a `--distribution-shadow` mode that logs disagreement
between the live and would-be distributions for at least one
working day.

Effort estimate: ~6 days of implementation + review. The
implementation factors into five small, independently-reviewable
branches sketched at the end of the design doc.

## What the design covers

* Section 1 - operator-friendly summary of random slicing.
* Section 2 - side-by-side comparison with vnode along coverage,
  add/remove cost, weighted distribution, lookup cost, and
  configuration ergonomics.
* Section 3 - line-by-line review of `hash_demo.c`, with five bugs
  / limitations called out (fixed `MAX_NAME_LEN`; no thread safety;
  zero-sized-interval acceptance; weight-rounding divide-by-zero;
  unchecked allocation).
* Section 4 - concrete wiring proposal: a new `Distribution` enum
  in `conf::pool`, a new `hashkit::random_slicing` module exposing
  `RandomSlices` with `from_uniform` / `from_weights` /
  `from_sizes` builders, a `RackRing` enum on `Rack`, and a single
  `match` in `dispatch::collect_routable`.
* Section 5 - migration path with shadow-mode pre-validation,
  SIGHUP cutover, and rollback.
* Section 6 - effort breakdown.
* Section 7 - six open questions for operator decision (hash width,
  hash family, bucket-types interaction, default/deprecation,
  heterogeneous racks, atomic publish).
* Section 8 - recommendation with a five-branch ship sequence.

## Files touched

* `docs/design/random-slicing-integration.md` - new, design only.
* `docs/journal/2026-05-25-random-slicing-design.md` - this file.

No source code, tests, scripts, or schemas were modified. The
deprecated `distribution: Option<String>` field in
`crates/dynomite/src/conf/pool.rs` remains as-is; the design
proposes promoting it to a typed sibling but does not do so.

## Awaiting operator decision

Six open questions are listed in Section 7 of the design doc. The
two that most affect implementation cost are:

1. Hash width: stay at 32-bit (truncate, current `DynToken` shape)
   or widen to 64-bit (matches the article and the C reference).
   Design recommends **widen**.
2. Default: keep vnode as default and random-slicing opt-in for
   the foreseeable future, or schedule a deprecation? Design
   recommends **keep both, opt-in only, defer deprecation
   discussion until six months of telemetry are in**.

Once an operator signs off on the recommendation and Section 7's
answers, implementation can start on the first branch
(`feat/distribution-enum`).

## Verification

```
bash scripts/check_ascii.sh
bash scripts/check_no_port_comments.sh
bash scripts/check_no_todos.sh
git diff --stat   # two new docs files
```

All four pass on this branch.
