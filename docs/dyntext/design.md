# dyntext: pg_tre port for dynomite Redis text-search

## Decision

Port the trigram + bloom-filter index from
[`pg_tre`](https://codeberg.org/gregburd/pg_tre) into a new Rust
crate `crates/dyntext/`, expose it via Redis FT.* commands
(extending the dynvec fold's RediSearch surface), and add a
`FT.REGEX` command for approximate-regex search.

## Why pg_tre

pg_tre is a PostgreSQL 18+ index access method for approximate
regex matching. Its three-tier filter funnel:

```
Query: SELECT id FROM docs WHERE body %~~ tre_pattern('(error){~1}.*(42[0-9]){~0}', 1);

Tier 1: range bloom         -- cheap; skips whole pages
Tier 2: trigram postings    -- per-trigram inverted lists
Tier 3: per-tuple bloom     -- cheap recheck before heap
   |
   v
TRE library                 -- approximate regex match against
                               recovered text (heap recheck)
```

Each tier is cheaper than the next; the executor never reads the
heap for rows the earlier tiers eliminate.

For dynomite-as-Redis-text-search this maps cleanly:
- Tier 1 (range bloom) -> per-segment filter at the data store
- Tier 2 (trigram postings) -> Redis-compatible inverted-index
  data structure (we already have HSET / SETs / SADDs on the
  Redis side; the postings list is just a SET of doc ids per
  trigram)
- Tier 3 (per-tuple bloom) -> per-key bloom in the metadata
- TRE recheck -> link to libtre (C library) via FFI for the
  approximate-regex final check

## Why not just use a Rust regex crate

`regex` is for exact (not approximate) match. `fancy-regex`
similar. `regex-automata` similar. The "approximate regex"
problem (find text that matches a pattern with up to k errors)
is what TRE solves; no Rust crate exists today that does the
same thing as well as TRE.

Options:
1. Port TRE to Rust (multi-month project; TRE is a complex 12k
   line C library implementing TNFA matching with edit distance).
2. Use TRE via FFI from Rust.
3. Skip approximate regex; only support exact regex via the
   `regex` crate.

Recommendation: option 2 (FFI to TRE) for the approximate-regex
tier. Trigram + bloom tiers are pure Rust ports.

## Network API

Extend the dynvec-fold's RediSearch FT.* command surface with:

```text
FT.CREATE idx_text
    ON HASH
    PREFIX 1 docs:
    SCHEMA
        body TEXT             # NEW: TEXT type using trigram + bloom
```
=> Creates a text-indexed schema. Each insert into a doc with
   the indexed prefix builds the trigram index incrementally.

```text
FT.SEARCH idx_text "@body:hello"
```
=> Substring match via trigram lookup.

```text
FT.REGEX idx_text "(error){~1}.*(42[0-9]){~0}" K=1 LIMIT 0 10
```
=> NEW custom command (not standard RediSearch; documented as
   a Dynomite extension). Does the three-tier filter funnel
   plus TRE recheck.

```text
FT.AGGREGATE idx_text "@body:error" GROUPBY 1 @category REDUCE COUNT 0 AS n
```
=> RediSearch aggregation; approximate counts via trigram
   intersection.

## Crate layout

```
crates/dyntext/
    Cargo.toml
    src/
        lib.rs              # public surface
        trigram.rs          # 3-gram extraction
        postings.rs         # inverted-index data structure
        bloom.rs            # range + per-tuple bloom filters
        regex_ast.rs        # parsed regex AST (for query planning)
        prefix_extract.rs   # extract trigrams the regex MUST contain
        index.rs            # the TextIndex type (write + query path)
        ffi/
            mod.rs
            tre.rs          # TRE C library FFI (feature-gated)
        encoding.rs         # serialize/deserialize the index for
                            # persistence
    tests/
        trigram.rs
        postings.rs
        bloom.rs
        index.rs
        property.rs         # hegeltest properties
    benches/
        index_throughput.rs
```

## Phases

1. **Phase 1 (this brief)**: trigram + postings + bloom in pure
   Rust. EXACT-substring search only. No regex, no TRE.
   Estimated: 4-6 hours.
2. **Phase 2**: add the regex AST + prefix extraction (compute
   which trigrams a regex MUST contain). Pure Rust.
   Estimated: 4-6 hours.
3. **Phase 3**: TRE FFI binding for the approximate-regex
   recheck. Estimated: 8-12 hours (cargo-build the C library;
   write safe Rust wrappers).
4. **Phase 4**: Redis FT.SEARCH / FT.REGEX command parser
   integration. Depends on the dynvec fold landing first.
   Estimated: 6-8 hours.
5. **Phase 5**: persistence (serialize the index to Noxu).
   Estimated: 4 hours.

Total: about 1 week of focused work for a full port.

## What this brief covers

Phase 1 only. The crate skeleton + trigram + postings + bloom +
exact-substring search. Phases 2-5 are documented as follow-ups.

## Tests for Phase 1

- `trigram_extracts_three_grams_from_simple_string`
- `trigram_handles_unicode_via_byte_level_three_gram`
- `postings_insert_and_lookup_returns_doc_ids`
- `postings_intersection_of_two_trigrams`
- `bloom_no_false_negatives_on_inserted_keys`
- `bloom_false_positive_rate_under_5pct`
- `index_substring_search_finds_all_matches`
- `index_substring_search_no_false_negatives`
- Property test (hegeltest): for any text corpus + any
  substring query, every result returned is a true positive
  (actually contains the substring after final-tier check).

## Compression / read-mostly story

Trigram postings lists are highly compressible (Roaring Bitmap
encoding, FOR delta encoding, etc.). For Phase 5 we'll use
`roaring` crate (already a workspace dep candidate; pure Rust,
MIT) to compress postings lists. Sample compression: 1M doc
ids in a postings list -> ~120KB Roaring vs 4MB plain u32 vec.
For mostly-read workloads this is critical; the index sits in
memory and queries do bitmap intersections without touching
disk.

## Trigram extraction

Pad the input with padding bytes (`SOH SOH text ETX ETX` style)
so the first and last few characters get trigrams, matching
pg_tre's behaviour:

- Input: `"hello"` (with padding: `\x01\x01hello\x03\x03`)
- Trigrams: `\x01\x01h`, `\x01he`, `hel`, `ell`, `llo`,
  `lo\x03`, `o\x03\x03`
- Each trigram is 3 bytes; treated as a u32 key (zero-padded).

Hashing: blake3 of the 3 bytes -> u64. Postings keyed by the u64.

## Indexed write path

```
text -> trigram set -> for each trigram: postings[trigram].insert(doc_id)
                   -> per-doc bloom filter -> persist
```

## Query path (Phase 1, exact substring)

```
query substring -> trigram set
    -> trigram intersection across postings
    -> candidate doc id set
    -> per-doc bloom filter test  (tier 3)
    -> recheck: read doc, substring match
    -> result set
```

For Phase 2+ (regex), the trigram set comes from the regex
prefix extractor instead of the raw query bytes.

## Note on pg_tre code reuse

pg_tre is 383 C/H files. We are NOT vendoring all of it. We are
porting the algorithmic core (trigram extraction, postings,
bloom filters, regex AST) to Rust. The TRE C library itself
gets used via FFI (Phase 3). pg_tre's PostgreSQL-specific glue
code (heap access, page format, buffer manager integration,
WAL, vacuum) does NOT come over.

## Dependencies on other in-flight work

- dynvec-fold-redis-path must land before Phase 4 so we have
  the Redis parser hooks ready for FT.* commands.
- Phase 1 (this brief) is independent; can land any time.
