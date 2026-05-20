# Blockers / Cross-cutting follow-ups

Items requiring a human decision are top-level. Cross-cutting cleanups
that are not blockers but need to be addressed at a planned point are
listed below.

## Cross-cutting follow-ups

### Doctest coverage gap (project-wide)

AGENTS.md Section 5 requires every public item to carry a rustdoc
`# Examples` section that compiles as a doctest. Stage 1 - Stage 5
shipped public items without `# Examples` blocks at the per-item
level (typically ~1 doctest per module instead, except Stage 5
which now has 68 doctests). This is a project-wide gap that pre-
dates any one stage.

Resolution: a cross-cutting cleanup pass after Stages 1 - 5 merge to
main, adding `# Examples` doctests to every `pub` item across the
`dynomite` crate. Owner: lead, scheduled to run between the
foundation merge and the dispatch of Stage 7.

### `conf_parse` cargo-fuzz target

AGENTS.md Section 6.4 lists `conf_parse` as a mandatory fuzz target.
Deferred: the fuzz target lands in the same crate
(`crates/fuzz/`) as `proto_redis_parse`, `proto_memcache_parse`,
`dnode_parse`, and `crypto_aes_decrypt`. Those parsers are the
high-risk fuzz targets and arrive in Stages 7 and 8. The fuzz crate
is created at that point with all five targets; until then, Stage
4 has property tests covering parser totality on arbitrary YAML
strings via `proptest`.

### Stage 5 follow-up nits (post-merge cleanup)

The Stage 5 re-review verdict APPROVE_WITH_NITS landed two
non-blocking polish items deferred to a later cleanup pass:

1. Strengthen the `Latency` / `QueueWait` / `QueueGauge` enum
   doctests in `crates/dynomite/src/stats/mod.rs` from
   `let _ = Latency::Request;` to assertions that exercise behavior
   (e.g. `assert_ne!(Latency::Request, Latency::Server);`).
2. Add an end-to-end regression test that overflows a histogram via
   `Stats::record_latency` and asserts the resulting
   `Snapshot.latency.{max,p999,p99,p95,mean}` are all `0`. Existing
   tests cover `HistogramSummary::from_histogram` and
   `Histogram::*` separately but not the JSON-output layer
   end-to-end on overflow.

Owner: lead, scheduled for the doctest cleanup pass mentioned above.

### Stage 3 follow-up: inline allowance comments

The Stage 3 re-review noted two `#[allow]` directives whose inline
comment cites the C origin
(`crates/dyn-hash-tool/src/main.rs:56`, `crates/dynomite/src/hashkit/ketama.rs:11-13`).
The text passes the literal hygiene grep but duplicates the
rationale already documented in `docs/journal/allowances.md`. To be
addressed by the doctest cleanup pass: rewrite each inline comment
to describe the lint without mentioning the C reference, leaving the
detailed justification in `allowances.md`.
