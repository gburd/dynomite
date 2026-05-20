# Allowances

Each `#[allow(...)]` or `#![allow(...)]` in the codebase requires a row
here, citing the exact lint name, the file/line, the upstream
issue/limitation, and the date.

| Date | Crate | File | Lint | Reason |
|---|---|---|---|---|
| 2026-05-20 | dynomite | crates/dynomite/src/stats/numeric.rs | clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss | The `floor_p_times_u64` helper must match the reference percentile expression `floor((double)scale * percentile)` exactly. The function performs the multiplication in IEEE 754 `f64` and floors the product. Any alternate arithmetic that avoids the casts diverges from the reference at percentile cutoffs the histogram actually uses (for example `p=0.95` over `scale=1000` yields 950 in the reference and 949 with rational arithmetic). The casts are protected by explicit range and finiteness checks before the conversion. |
| 2026-05-20 | dynomite | crates/dynomite/src/stats/numeric.rs | clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss (test) | The accompanying unit test `floor_matches_f64_reference_over_known_pairs` reproduces the reference expression literally. |
| 2026-05-19 | dynomite | `crates/dynomite/src/util/time.rs` | `clippy::cast_possible_truncation` | The `u128 -> u64` step in `usec_now` runs only after a guard rejects values larger than `u64::MAX`, so truncation is unreachable. |
