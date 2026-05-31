# dyntext Phase 2: regex AST + prefix extraction

Date: 2026-05-30
Branch: `stage/dyntext-phase2`
Worktree: `/home/gburd/ws/wt-dyntext-phase2`

## Scope

Implement the Phase 2 pipeline outlined in
`docs/dyntext/design.md`: parse a regex pattern into a small
internal AST, extract the trigrams that any matching string
must contain, and wire those trigrams through the postings
intersection so the index can prune candidates before the
recheck step.

## Files added

* `crates/dyntext/src/regex_ast.rs`: internal AST plus a
  `parse(pattern)` entry point that drives `regex_syntax`'s HIR
  and projects it into the small AST the extractor walks.
  Detects named-capture groups and reports them as
  `RegexError::PrefixUnsupported` so the caller can fall back.
* `crates/dyntext/src/prefix_extract.rs`: linearizing extractor
  that walks the AST while maintaining a literal-byte run, then
  flushes every length-3 window into a deduplicated set on each
  break point. Implements the simplified Russ-Cox propagation
  from the algorithm in the design doc.
* `crates/dyntext/tests/regex_ast.rs`: 14 integration tests
  covering literal / alternation / grouping / anchor / class /
  named-capture / lookaround / backreference / invalid-pattern.
* `crates/dyntext/tests/prefix_extract.rs`: 12 integration
  tests covering empty / literal / dot / anchor /
  required-and-optional groups / alternation intersection /
  realistic anchored regex / dedup-and-sort invariant.
* `crates/dyntext/tests/regex_search.rs`: 10 end-to-end
  integration tests plus one Hegel property test (256 cases)
  that compares `TextIndex::search_regex` against
  `regex::bytes::Regex::is_match` on a randomly generated
  corpus and pattern over the supported subset.

## Files changed

* `crates/dyntext/Cargo.toml`: added `regex-syntax = "0.8"`
  (parser for the AST projection) and `regex` from the
  workspace (used for the Tier-4 recheck).
* `crates/dyntext/src/lib.rs`: re-exports for `regex_ast`,
  `prefix_extract`, `RegexError`, and the extraction helpers.
  Updated module-level docs to describe Phase 2 scope.
* `crates/dyntext/src/index.rs`: added
  `TextIndex::search_regex` which compiles the matcher,
  extracts required trigrams, intersects the postings if
  trigrams were extracted (otherwise full-scans), then runs
  the matcher against survivors.

## Algorithm notes

The extractor diverges from the C reference (`pg_tre`'s
`src/query/extract.c`) in one place: anchors and word
boundaries are treated as *run-breaks* rather than no-ops. The
C version keeps the run intact across an anchor, which leads to
a soundness bug on pathological patterns like `abc^def` (where
the bytes `bcd` are not actually adjacent in any match). The
Rust implementation flushes the run at every `Look` node, which
costs us almost nothing on realistic patterns (anchors usually
sit at pattern boundaries, where the run is empty anyway) and
guarantees we never required-include a trigram that is not
actually required. The brief's example
`^errno: \w+ refused$` extracts trigrams of `"errno: "` and
`" refused"` under both interpretations.

Repetitions with `min >= 1` are inlined up to twice (matching
the C cap), so trigrams that span the boundary between two
iterations of `(ab){2,}` (`aba`, `bab`) become required.

## Testing

* `cargo build -p dyntext --all-targets`: clean.
* `cargo nextest run -p dyntext`: 120 tests pass (was 79
  before).
* `cargo test --doc -p dyntext`: 9 doctests pass.
* `cargo clippy -p dyntext --all-targets -- -D warnings`:
  clean.
* `cargo fmt --all -- --check`: clean.
* `scripts/check_ascii.sh`, `check_no_todos.sh`,
  `check_no_port_comments.sh`: all clean.

## Open questions

None. The follow-up work is Phase 3 (TRE FFI for
approximate-regex match), which is out of scope here.
