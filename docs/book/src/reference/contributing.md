# Contributing and the Parity Discipline

This manual is for users of Dynomite. Contributors -- human or automated
-- have two additional documents that govern how the project is built:

* [`AGENTS.md`](https://codeberg.org/gregburd/dynomite/src/branch/main/AGENTS.md)
  -- the operating manual: coding standards, the testing strategy, the
  git workflow, and the review discipline.
* [`PLAN.md`](https://codeberg.org/gregburd/dynomite/src/branch/main/PLAN.md)
  -- the staged roadmap.

This page summarizes the two ideas a user-turned-contributor most needs
to understand.

## Parity with the C original

Dynomite (Rust) aims to be functionally identical to
[Netflix Dynomite](https://github.com/Netflix/dynomite) (C). That aim is
not aspirational prose -- it is tracked, symbol by symbol, in
[`docs/parity.md`](https://codeberg.org/gregburd/dynomite/src/branch/main/docs/parity.md),
which maps each C function to its Rust home and records every deliberate
divergence as a **deviation** with a rationale.

```admonish note title="Why parity, and where the Rust code is idiomatic"
Parity governs observable behavior, not code shape. The Rust code reads
as first-class Rust -- traits, enums, async, typed errors -- and does not
carry "ported from" comments. Where a C shape does not map cleanly to
Rust, the project uses a two-layer pattern: an idiomatic inner layer and
a thin parity shim that mirrors the C function's semantics. Both are
tested to produce identical behavior.
```

If you change behavior, you update `docs/parity.md`. If you find the C
reference ambiguous, you record the ambiguity, choose the interpretation
most consistent with the surrounding code, and pin it with a regression
test.

## The testing discipline for distributed changes

Anything that touches the distributed path -- routing, replication,
gossip, quorum, anti-entropy, causality, cross-node transactions, CRDT
convergence, or the peer plane -- must pass two gates beyond the ordinary
unit, property, and fuzz tests, and both are merge blockers:

1. A **deterministic simulation model** (no real sockets, no wall clock,
   seeded RNG) that asserts the safety and liveness invariants the change
   must never violate, and that includes a **negative control** -- a
   deliberately broken variant the checker is shown to catch, so the
   model is proven to have teeth.

2. An **Elle-style consistency check** that drives the real built binary
   with a history-recording workload and finds zero anomalies of the
   classes it covers.

```admonish warning title="Model the failure first"
When a real-world distributed failure is found, the discipline is to
reproduce it in a deterministic model *before* fixing it. A green unit
suite is explicitly not sufficient evidence of distributed correctness --
several real defects in this port passed the entire unit suite. See
[Design Decisions](./roads-not-taken.md#property-dst-and-elle-testing-not-unit-tests-alone).
```

## The everyday gate

Every commit must pass
[`scripts/check.sh`](https://codeberg.org/gregburd/dynomite/src/branch/main/scripts/check.sh),
which is what CI runs on both GitHub Actions and Codeberg Forgejo Actions.
It covers formatting, clippy (pedantic, warnings denied), the full test
matrix, doctests, the documentation link checker, dependency audits, the
mdBook build, and the project hygiene checks (no `todo!()`, ASCII only, no
port-acknowledgement comments). Run it before you propose a change.

## Getting the environment

Everything runs inside the pinned Nix dev shell:

```sh
nix develop
scripts/check.sh
```

The flake pins the Rust toolchain and every auxiliary tool -- including
the mdBook, mermaid, admonish, linkcheck, and lychee binaries this manual
is built with -- so a fresh checkout reproduces CI exactly.
