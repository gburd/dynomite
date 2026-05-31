//! Trigram + bloom-filter text index.
//!
//! `dyntext` is the algorithmic core of the dynomite text-search
//! surface. It ports the inverted-index pipeline from
//! [pg_tre](https://codeberg.org/gregburd/pg_tre) -- a
//! PostgreSQL access method for approximate-regex matching --
//! into pure Rust, so the same trigram + bloom funnel can sit
//! behind dynomite's Redis FT.* command surface.
//!
//! # Phase 1 scope
//!
//! This crate currently implements:
//!
//! * Three-byte n-gram extraction with padding so the input's
//!   boundary bytes get full coverage (see [`trigram`]).
//! * A roaring-bitmap-backed inverted index keyed by trigram
//!   hash (see [`postings::Postings`]).
//! * A standard bloom filter with configurable bit count and
//!   hash count (see [`bloom::BloomFilter`]).
//! * A combined [`index::TextIndex`] that ties the three
//!   together and serves exact-substring queries through the
//!   four-tier filter funnel from the design doc.
//!
//! # Phase 2 + 3 scope
//!
//! Phase 2 adds regex-driven search on top of the existing
//! exact-substring path:
//!
//! * A small internal regex AST built from
//!   [`regex_syntax`]'s HIR (see [`regex_ast`]).
//! * A required-trigram extractor that walks the AST and
//!   computes the trigrams every matching string must contain
//!   (see [`prefix_extract`]).
//! * [`index::TextIndex::search_regex`], which uses the
//!   extractor to prune the postings lists before running the
//!   actual matcher (currently [`regex::bytes::Regex`]).
//!
//! Phase 3 adds the approximate-regex recheck:
//!
//! * Safe FFI wrapper around the TRE C library for
//!   approximate-regex matching with up to k typos
//!   (see [`tre`]). The wrapper is the optional recheck step;
//!   the trigram + bloom funnel is reused unchanged.
//! * Phase 4: Redis FT.SEARCH / FT.REGEX command parser
//!   integration on top of the dynvec fold.
//!
//! # Optional features
//!
//! * `noxu` -- enables the [`persist`] module that serialises
//!   a [`TextIndex`] to an embedded Noxu DB environment so
//!   the trigram postings, per-doc bloom filters, and raw
//!   text survive a process restart. The feature pulls in
//!   `noxu-db` and `bincode` as workspace path dependencies.
//!
//! # Quick start
//!
//! ```
//! use dyntext::index::TextIndex;
//!
//! let mut idx = TextIndex::new();
//! let id_a = idx.insert(b"the quick brown fox".to_vec());
//! let id_b = idx.insert(b"jumped over a lazy dog".to_vec());
//! let id_c = idx.insert(b"another brown fox here".to_vec());
//!
//! let hits = idx.search_substring(b"brown fox");
//! assert!(hits.contains(&id_a));
//! assert!(hits.contains(&id_c));
//! assert!(!hits.contains(&id_b));
//! ```

pub mod bloom;
pub mod index;
#[cfg(feature = "noxu")]
pub mod persist;
pub mod postings;
pub mod prefix_extract;
pub mod regex_ast;
pub mod tiling;
pub mod tre;
pub mod trigram;

pub use bloom::BloomFilter;
pub use index::{IndexedDoc, TextIndex, MIN_TRIGRAM_QUERY_LEN};
pub use postings::Postings;
pub use prefix_extract::{
    anchored_prefix, extract_literal_runs, has_top_level_start_anchor, required_trigram_hashes,
    required_trigrams,
};
pub use regex_ast::{parse as parse_regex, Ast as RegexAst, RegexError};
pub use tiling::ApproxFilter;
pub use tre::{TreCompiledPattern, TreError, TreMatch, TreMatchOpts};
pub use trigram::{
    extract_query_trigram_set, extract_query_trigrams, extract_trigram_set, extract_trigrams,
    hash_trigram,
};
