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
