# 2026-05-30: dyntext Phase 1 (trigram + postings + bloom + exact substring)

## Stage

`stage/dyntext-phase1` -- Phase 1 of the pg_tre port. The
algorithmic core only: pure-Rust trigram extraction, an
inverted-index keyed on trigram hash, a per-document bloom
filter, and an exact-substring search path that funnels
candidates through tier-2 postings intersection -> tier-3 bloom
recheck -> tier-4 substring recheck.

Phases 2-5 (regex AST, TRE FFI, Redis FT.* integration, Noxu
persistence) are explicit follow-ups and are NOT touched here.

## Files added

```
crates/dyntext/Cargo.toml
crates/dyntext/src/lib.rs              60 LOC
crates/dyntext/src/trigram.rs         267 LOC  (5 fns, 12 unit tests)
crates/dyntext/src/postings.rs        266 LOC  (8 methods, 11 unit tests)
crates/dyntext/src/bloom.rs           372 LOC  (4 methods, 7 unit tests)
crates/dyntext/src/index.rs           386 LOC  (TextIndex; 12 unit tests)
crates/dyntext/tests/trigram.rs        42 LOC  (5 integration tests)
crates/dyntext/tests/postings.rs       60 LOC  (5 integration tests)
crates/dyntext/tests/bloom.rs          42 LOC  (3 integration tests)
crates/dyntext/tests/index.rs         100 LOC  (8 integration tests)
crates/dyntext/tests/property.rs      172 LOC  (3 hegel properties)
crates/dyntext/benches/index_throughput.rs  117 LOC
docs/journal/2026-05-30-dyntext-roaring.md
docs/journal/2026-05-30-dyntext-phase1.md  (this file)
```

Workspace-level changes:

* `Cargo.toml`: added `crates/dyntext` to `members`.
* `docs/journal/allowances.md`: added two rows for the
  per-cast helpers in `bloom.rs` and the criterion
  `missing_docs` allow in the bench.

No changes to any other crate (the parallel worker on
`dynvec` FT.* is untouched, per the brief).

## Public API surface

```text
dyntext::trigram::PAD_LEFT, PAD_RIGHT, TRIGRAM_LEN
dyntext::trigram::hash_trigram(&[u8]) -> u64
dyntext::trigram::extract_trigrams(&[u8]) -> Vec<u64>           // padded, for docs
dyntext::trigram::extract_trigram_set(&[u8]) -> Vec<u64>        // padded, dedup
dyntext::trigram::extract_query_trigrams(&[u8]) -> Vec<u64>     // unpadded, for queries
dyntext::trigram::extract_query_trigram_set(&[u8]) -> Vec<u64>  // unpadded, dedup
dyntext::postings::Postings { new, len, is_empty, insert, remove,
                              lookup, intersect, union }
dyntext::bloom::BloomFilter   { with_size_and_fp_rate, n_bits,
                                hash_count, insert, contains,
                                false_positive_rate }
dyntext::index::TextIndex     { new, doc_count, postings, docs,
                                insert, remove, search_substring }
dyntext::index::IndexedDoc    { text, bloom }
dyntext::index::MIN_TRIGRAM_QUERY_LEN
```

`dyntext` re-exports the major types at the crate root.

## Design notes

### Padded vs unpadded trigrams

The brief showed `extract_trigrams` padded with
`\x01\x01<text>\x03\x03`. Padding gives the document's first
and last bytes full coverage. However, when applied to a
substring query the padding bytes do not appear adjacent to
the query inside the doc: `"hello"` inside `"hello world"`
sits next to a space, not next to `\x03`.

The fix (added in this stage; the brief's pseudocode would
otherwise miss true positives) is a query-time
`extract_query_trigrams` that returns the UNPADDED windows.
A property test ([`property::substring_search_no_false_negatives_under_arbitrary_corpus`])
catches the regression: any doc that truly contains the query
must surface in the result set.

### Filter funnel

`TextIndex::search_substring` runs the four-tier funnel:

1. Trigram extraction from the query (unpadded).
2. Tier 2: `postings.intersect(&trigrams)` -- intersect the
   postings lists. Any candidate must contain every query
   trigram. Reduction order is by ascending posting-list size
   so the working set shrinks fastest.
3. Tier 3: per-doc `BloomFilter::contains` for each query
   trigram. In the in-memory case this is logically redundant
   with tier 2 (the postings already proved containment), but
   the brief specifies it as defence-in-depth and as a model
   for the future on-disk path where the postings live in a
   separate segment.
4. Tier 4: byte-level substring match against the stored doc
   text.

Short queries (< 3 bytes) and queries that produce no
trigrams fall back to a full scan over `docs.values()`.
Empty queries are treated specially: every doc matches.

### Doc-id ordering

`docs` is a `BTreeMap<u32, IndexedDoc>` and `next_doc_id` is
monotonic, so `docs.keys()` iterates in insertion order. The
tier-2 `RoaringBitmap` also iterates ascending. The search
function additionally sorts the result vector to make the
ordering contract explicit. Removed doc ids are NOT recycled.

### Roaring vs Vec<u32>

Documented in `2026-05-30-dyntext-roaring.md`: pure-Rust
`roaring 0.10`, MIT/Apache-2.0, used for postings list
compression and SIMD-friendly intersection. Pinned in
`crates/dyntext/Cargo.toml` only; not promoted to a workspace
dep.

## Tests

```
unit (in src/):                42  (trigram 12, postings 11, bloom 7, index 12)
integration (tests/):          21  (trigram 5, postings 5, bloom 3, index 8)
property (tests/property.rs):   3  (#[hegel::test(test_cases = 256)])
doctests:                       6  (lib.rs, trigram x4, bloom x1)
                              ----
total:                         72
```

`cargo nextest run -p dyntext` -- 66 passed, 0 skipped (5.7s
incl. property tests).
`cargo test --doc -p dyntext` -- 6 passed.
`cargo clippy -p dyntext --all-targets -- -D warnings` -- clean.
`cargo fmt --all -- --check` -- clean.

## Bench

`cargo bench -p dyntext --bench index_throughput`:

| Bench | Median |
|---|---|
| `insert_10k_docs_256B` (10000 docs of 256 random ASCII bytes) | ~4.9 s/iter -> ~2k docs/sec |
| `search_query_10k_docs` (5 queries against 10k-doc index) | ~644 us / 5q -> ~128 us/query (p50), ~170 us (p95) |

Numbers will improve with future work (sketches, prefix
extraction, batched intersect) but are well within the brief's
"runs in under 60s on a laptop" budget.

## Lints

The crate is workspace-default (`clippy::pedantic` warn,
`forbid(unsafe_code)`, `missing_docs` warn). Two new
allowances were added to `docs/journal/allowances.md`:

1. `crates/dyntext/src/bloom.rs` per-helper casts for the
   textbook bloom dimension formula (m and k).
2. `crates/dyntext/benches/index_throughput.rs` file-level
   `missing_docs` allow for criterion's macro-generated public
   symbols (same rationale as `crates/dynomite/benches/*`).

## Open questions / follow-ups

* Phase 2: regex AST + prefix extraction; will use the same
  `Postings`/`BloomFilter` facilities.
* The bloom-filter check at tier 3 is logically subsumed by
  the postings intersection in the in-memory configuration.
  When persistence lands (Phase 5) the tiers become
  storage-medium boundaries and the bloom regains its purpose
  as a "skip-this-on-disk-page" filter.
* `roaring` may benefit from the `simd` feature on x86; left
  off for now for portability.
* The property test corpus alphabet is `{a, b, c, d}`; this
  was chosen so collisions and overlaps are common. Wider
  alphabets would not exercise the data path as effectively.

## Status

`READY_FOR_REVIEW`.
