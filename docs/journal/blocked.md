# Blockers

Items requiring a human decision. Empty when work is unblocked.

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
