# Curated chaos-test reports

The Stage 16 chaos test
(`crates/dynomite/tests/stage_16_chaos.rs`) emits its run
artifact at `target/chaos/<run-id>/report.md`. Per AGENTS.md
Section 14c, those raw runs are gitignored; only the report
for the v0.1.0 release tag (and any future releases) is
curated and committed under this directory:

```
dist/chaos-reports/v0.1.0/report.md
```

The lead copies the production-mode (1-hour) run report into
this tree as part of the pre-tag procedure; see
`docs/book/src/operations/release.md`.

## Multi-host rig

The Stage 16 in-process test is complemented by the multi-host
rig under `scripts/chaos-multi-host/`, which drives a real
4-DC dynomite cluster across `floki`, `arnold`, `nuc`, and
`meh` for hours at a time. Curated reports from those passes
are also committed under `v0.1.0/`:

* `multi-host-pass-1.md` - first 2-hour pass, redis backend,
  3-DC topology with identical tokens.
* `multi-host-pass-2.md` - second 2-hour pass, redis backend,
  3-DC topology with per-DC distinct tokens (cross-DC routing
  exercised).

Follow-up passes that exercise the four-host topology and the
memcache backend land here as `multi-host-pass-N.md`. The
operator's manual for that rig (modes, hosts, smoke procedure,
topology, token assignments) lives in
`docs/operations/chaos.md`.
