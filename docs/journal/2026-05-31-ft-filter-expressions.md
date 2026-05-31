# 2026-05-31: FT.SEARCH filter expressions

Worker: stage/ft-filter-expressions

## Summary

Extends `FT.SEARCH` to accept a real query expression on the
LHS of the optional `=>[KNN ...]` operator. The previous
parser only accepted `*` (match-all) on the LHS; this branch
introduces a full filter DSL covering numeric ranges, tag
sets, text substrings, and the boolean combinators (AND, OR,
NOT, grouping). It also extends `FT.CREATE` to recognise
`<field> NUMERIC SORTABLE` and `<field> TAG SEPARATOR <c>`
(plus the noise modifiers `NOSTEM`, `NOINDEX`, `WEIGHT <n>`,
`PHONETIC <m>`, `UNF`, `CASESENSITIVE`).

## Files touched

* `crates/dynomite/src/vector/schema.rs`: `MetadataField`
  gains an optional `tag_separator` byte (`#[serde(default)]`
  preserves backward compatibility for any persisted schema).
* `crates/dynomite/src/proto/redis/ft_filter.rs` (new): the
  filter expression AST (`FilterExpr`, `NumericBound`), a
  recursive-descent parser (`parse_expr`), and a set-based
  evaluator (`evaluate`).
* `crates/dynomite/src/proto/redis/ft.rs`: `parse_search`
  now splits at `=>[KNN ...]`, parses the LHS as a filter
  expression (or `*`), and dispatches to either
  `FtCommand::Search` (with optional `filter`),
  `FtCommand::SearchText` (legacy single-field substring
  fast-path), or `FtCommand::SearchFilter` (filter-only).
  `execute_search` post-filters HNSW candidates against the
  filter set; `execute_search_filter` walks the indexed key
  set and projects each surviving row into a `SearchHit`.
* `crates/dynomite/src/proto/redis/mod.rs`: register the new
  module.
* `crates/dynomited/tests/ft_filter_wire.rs` (new): 13 wire
  tests that drive RESP traffic through `dynomited`.
* Test files referencing `MetadataField`: updated to set the
  new `tag_separator: None` field.

## Tests

* `cargo nextest run -p dynomite --test ft_redis`: 17/17 pass
  (all pre-existing).
* `cargo nextest run -p dynomite --lib`: 678/678 pass
  (includes 17 new unit tests in `ft_filter::tests`).
* `cargo nextest run -p dynomited --features integration
  --test ft_filter_wire`: 13/13 pass (new).
* `cargo nextest run -p dynomited --features integration
  --test ft_text_wire --test ft_extensions_wire`: 17/17
  pass (regression coverage for the legacy text-substring
  and projection-clause paths).
* `cargo nextest run --workspace --features riak`:
  1941/1941 pass.
* `cargo test --doc -p dynomite`: 673/673 pass (one new
  doctest in `ft_filter`).
* `cargo clippy --workspace --all-targets --features riak
  -- -D warnings`: clean.
* `cargo fmt -p dynomite -p dynomited -- --check`: clean.

## Design notes

* **Two layers, one interpreter.** The filter expression is
  parsed once (`parse_expr`) into an AST and then evaluated
  against the registry (`evaluate`). The KNN executor calls
  `evaluate` to compute the surviving candidate set, then
  oversamples HNSW (`max(k, candidate_count)`) and trims to
  the surviving keys before applying `req.k`. This keeps the
  HNSW path untouched at the cost of a brute-force candidate
  scan when a filter is present; the brief explicitly accepts
  this trade-off for the supported scale.
* **Backward compatibility for the legacy text path.** The
  pre-existing `@field:word` (no boolean operators, no
  brackets) shape is detected by `try_parse_simple_text_field_query`
  and routed through the existing `SearchTextRequest`
  trigram path. Anything richer (numeric ranges, tag sets,
  AND/OR/NOT, grouping) lands on the new `SearchFilter`
  path. This preserves the wire-test patterns in
  `ft_text_wire.rs` byte-for-byte.
* **Text substring inside a filter expression** uses the
  trigram + bloom index when one is provisioned for the
  field; otherwise it falls back to a direct substring scan
  on the row's metadata bytes. The two paths are functionally
  equivalent.
* **TAG separator.** Stored as `Option<u8>` on
  `MetadataField`; `None` means the RediSearch default of
  `,`. The parser rejects multi-byte separators with a
  syntax error (RediSearch documents the separator as a
  single character).
* **Numeric bounds.** `f64` internally; the wire is allowed
  to send `+inf` / `-inf` (mapped to `NumericBound::PosInf`
  / `NumericBound::NegInf`) and `(<n>` (mapped to
  `NumericBound::Exclusive`). The applying-`(`-to-infinity
  combination is rejected as a syntax error to match
  RediSearch.
* **Negation** is computed against the index's universe
  (the observed indexed-key set), so `-@status:{stale}`
  returns "every doc this index has indexed minus the stale
  ones". Documents that have never been HSET-into are
  invisible, which is consistent with the rest of the FT.*
  surface.
* **Out-of-scope shapes** surface as
  `FtError::Unsupported`: trailing tokens inside a numeric
  range (`@loc:[lon lat radius unit]`, the geo filter
  shape) and quoted phrases (`@body:"hello world"`). Both
  have wire tests.

## Out of scope (per brief)

* `NUMFIELD <field> WEIGHT <n>` per-field weighting.
* `GEOFILTER <field> <lon> <lat> <radius> <unit>` -
  rejected with `not supported`.
* Phrase queries with `"..."` quoting - rejected with
  `not supported`.
* Stemming / stop-words - documented; dyntext does not
  ship them.

## Open questions

None.
