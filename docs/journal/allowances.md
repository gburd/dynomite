# Allowances

Each `#[allow(...)]` or `#![allow(...)]` in the codebase requires a row
here, citing the exact lint name, the file/line, the upstream
issue/limitation, and the date.

| Date | Crate | File | Lint | Reason |
|---|---|---|---|---|
| 2026-05-20 | dynomite | crates/dynomite/src/stats/numeric.rs | clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss | The `floor_p_times_u64` helper must match the reference percentile expression `floor((double)scale * percentile)` exactly. The function performs the multiplication in IEEE 754 `f64` and floors the product. Any alternate arithmetic that avoids the casts diverges from the reference at percentile cutoffs the histogram actually uses (for example `p=0.95` over `scale=1000` yields 950 in the reference and 949 with rational arithmetic). The casts are protected by explicit range and finiteness checks before the conversion. |
| 2026-05-20 | dynomite | crates/dynomite/src/stats/numeric.rs | clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss (test) | The accompanying unit test `floor_matches_f64_reference_over_known_pairs` reproduces the reference expression literally. |
| 2026-05-19 | dynomite | `crates/dynomite/src/util/time.rs` | `clippy::cast_possible_truncation` | The `u128 -> u64` step in `usec_now` runs only after a guard rejects values larger than `u64::MAX`, so truncation is unreachable. |
| 2026-05-19 | dynomite | crates/dynomite/src/hashkit/mod.rs | `non_camel_case_types` | The C reference exports the algorithm names `fnv1_64`, `fnv1a_64`, `fnv1_32`, `fnv1a_32` and the configuration parser keys on those exact strings. Renaming the variants to UpperCamelCase would either obscure the parity with the spec or require a separate name table, both of which lose information. The lint is allowed only on the `HashType` enum. |
| 2026-05-19 | workspace | Cargo.toml | `clippy::cast_possible_truncation`, `cast_possible_wrap`, `cast_sign_loss`, `cast_precision_loss` | The hashkit and DNODE codecs intentionally truncate, wrap, and reinterpret integer types to reproduce the bit-exact arithmetic of the C reference. Per AGENTS.md section 6 these casts are part of the algorithmic contract. Allowed at workspace scope so the bit-mixing code reads as in the original. |
| 2026-05-19 | workspace | Cargo.toml | `clippy::many_single_char_names`, `similar_names` | RFC 1321 (MD5) and the lookup3 (Jenkins) reference code name their working registers `a`, `b`, `c`, `d`. Renaming them would make the code harder to verify against the spec. |
| 2026-05-19 | workspace | Cargo.toml | `clippy::doc_markdown` | Trade names `MurmurHash`, `SuperFastHash`, `MD5` etc. trigger this lint. Adding backticks would make doc text read as code in rustdoc. |
| 2026-05-19 | workspace | Cargo.toml | `clippy::too_many_arguments`, `too_many_lines` | AGENTS.md section 5 explicitly permits long single functions for state-machine parsers and round-mixing primitives. The MD5 round and the Redis parser both rely on this latitude. |
