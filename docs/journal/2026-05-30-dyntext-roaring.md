# 2026-05-30: dyntext crate adds `roaring` 0.10

## What

The new `crates/dyntext` (Phase 1 of the pg_tre port) adds
`roaring = "0.10"` as a direct dependency. The crate is the
algorithmic core of the dynomite text-search surface (trigram
extraction + bloom filter + postings inverted index for
exact-substring search; future phases will layer regex + TRE
FFI on top).

## Why this dep

The trigram inverted index is dominated by set operations
(intersection of postings lists at query time, union at
write-time updates). The two production-grade implementations
of this exact data structure -- pg_tre and Apache Lucene --
both use Roaring Bitmap encoding because:

1. Postings lists vary wildly in cardinality. Popular trigrams
   like `the` may appear in millions of docs; rare ones in a
   handful. A single representation that compresses well in
   both regimes is required, and Roaring's hybrid container
   layout (run-length / array / dense bitmap, chosen
   per-65k-bucket) is the de facto state of the art.
2. Intersection is the hot path. Roaring's container
   intersection is SIMD-friendly and asymptotically
   `O(min(|a|, |b|))` instead of `O(|a| + |b|)`.
3. For 1M doc ids in a single trigram's postings list,
   Roaring is ~120KB on disk vs ~4MB for a plain `Vec<u32>`.
   The index is read-mostly and lives in memory, so the
   compression ratio directly improves cache behaviour.

## Crate provenance

* Repo: https://github.com/RoaringBitmap/roaring-rs
* License: Apache-2.0 / MIT (compatible with the dynomite
  license allowlist in `deny.toml`).
* Pure Rust (no `unsafe` in the dependency graph at the API
  surface; internal `unsafe` is used for SIMD intersection,
  reviewed upstream).
* Active maintenance; the crates.io page shows weekly
  downloads in the millions and a 0.10 line that has been
  stable since 2024.

## Scope

The dependency is pinned in `crates/dyntext/Cargo.toml` only,
not added to the workspace `[workspace.dependencies]` table.
Future phases (persistence, redis FT.* integration in
`dynvec`) may want to bump it to a workspace dep; that bump
will be a separate journal entry.

## Audit trail

* `cargo deny check` -- expected clean (license: MIT/Apache-2.0).
* `cargo audit` -- expected clean.
* No unsafe in the user-visible API of the crate.

## Alternatives considered

* `bit-set` / `fixedbitset`: dense bitsets only, no
  compression, would blow up memory for sparse postings.
* Hand-rolled run-length encoded `Vec<u32>`: would replicate
  Roaring's algorithm at a fraction of the test coverage and
  battle-hardness.
* `croaring` (FFI to the C library): adds an `unsafe` surface
  and a build-time `cmake` step; pure-Rust crate avoids both.

The pure-Rust `roaring` crate is the right shape for the
project's "no unsafe at the application boundary, all
dependencies pure Rust where feasible" stance.
