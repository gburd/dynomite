//! Quorum resolution for Riak-style tunable consistency.
//!
//! Riak lets a read or write specify how many replicas must respond:
//! `R`/`PR` for reads, `W`/`PW`/`DW` for writes. The value is either a
//! literal count or one of the symbolic values `one`, `quorum`, `all`,
//! `default`, carried on the wire as reserved `u32` magic values. This
//! module resolves a knob against the replica count `N` into a concrete
//! required count in `[1, N]`.

/// Symbolic quorum wire value: `one` (a single replica).
pub const QUORUM_ONE: u32 = u32::MAX;
/// Symbolic quorum wire value: `quorum` (a strict majority of N).
pub const QUORUM_QUORUM: u32 = u32::MAX - 1;
/// Symbolic quorum wire value: `all` (every replica).
pub const QUORUM_ALL: u32 = u32::MAX - 2;
/// Symbolic quorum wire value: `default` (defer to the bucket default).
pub const QUORUM_DEFAULT: u32 = u32::MAX - 3;

/// Resolve a quorum knob against the replica count `n_val`.
///
/// * `None` or [`QUORUM_DEFAULT`] resolves to the bucket default, which
///   itself defaults to `quorum` (a strict majority).
/// * [`QUORUM_ONE`] -> 1, [`QUORUM_QUORUM`] -> `n/2 + 1`,
///   [`QUORUM_ALL`] -> `n`.
/// * A literal count is clamped to `[1, n]`.
///
/// `n_val` is clamped to at least 1 so the result is always a usable
/// count.
#[must_use]
pub fn resolve(knob: Option<u32>, n_val: u8, bucket_default: Option<u32>) -> u32 {
    let n = u32::from(n_val.max(1));
    match knob {
        None | Some(QUORUM_DEFAULT) => resolve_default(bucket_default, n),
        Some(QUORUM_ONE) => 1,
        Some(QUORUM_QUORUM) => n / 2 + 1,
        Some(QUORUM_ALL) => n,
        Some(k) => k.clamp(1, n),
    }
}

/// Resolve the bucket default (used when a request omits the knob or
/// asks for `default`). An unset bucket default is `quorum`.
fn resolve_default(bucket_default: Option<u32>, n: u32) -> u32 {
    match bucket_default {
        None | Some(QUORUM_DEFAULT | QUORUM_QUORUM) => n / 2 + 1,
        Some(QUORUM_ONE) => 1,
        Some(QUORUM_ALL) => n,
        Some(k) => k.clamp(1, n),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbolic_values_resolve_against_n() {
        // N = 3: quorum = 2, all = 3, one = 1.
        assert_eq!(resolve(Some(QUORUM_QUORUM), 3, None), 2);
        assert_eq!(resolve(Some(QUORUM_ALL), 3, None), 3);
        assert_eq!(resolve(Some(QUORUM_ONE), 3, None), 1);
        // N = 5: quorum = 3.
        assert_eq!(resolve(Some(QUORUM_QUORUM), 5, None), 3);
    }

    #[test]
    fn unset_and_default_fall_back_to_bucket_default_then_quorum() {
        // No knob, no bucket default -> quorum (2 of 3).
        assert_eq!(resolve(None, 3, None), 2);
        // default-magic, no bucket default -> quorum.
        assert_eq!(resolve(Some(QUORUM_DEFAULT), 3, None), 2);
        // No knob, bucket default = all -> all.
        assert_eq!(resolve(None, 3, Some(QUORUM_ALL)), 3);
        // No knob, bucket default = one -> one.
        assert_eq!(resolve(None, 3, Some(QUORUM_ONE)), 1);
        // default-magic request over a bucket default of all -> all.
        assert_eq!(resolve(Some(QUORUM_DEFAULT), 3, Some(QUORUM_ALL)), 3);
    }

    #[test]
    fn literal_counts_are_clamped_to_the_replica_range() {
        assert_eq!(resolve(Some(2), 3, None), 2);
        // 0 clamps up to 1.
        assert_eq!(resolve(Some(0), 3, None), 1);
        // Above N clamps down to N.
        assert_eq!(resolve(Some(9), 3, None), 3);
    }

    #[test]
    fn per_request_overrides_the_bucket_default() {
        // Bucket default = all, but the request asks for one -> one.
        assert_eq!(resolve(Some(QUORUM_ONE), 3, Some(QUORUM_ALL)), 1);
        // Request literal 2 over a bucket default of all -> 2.
        assert_eq!(resolve(Some(2), 3, Some(QUORUM_ALL)), 2);
    }

    #[test]
    fn n_val_of_one_always_resolves_to_one() {
        assert_eq!(resolve(Some(QUORUM_QUORUM), 1, None), 1);
        assert_eq!(resolve(Some(QUORUM_ALL), 1, None), 1);
        assert_eq!(resolve(None, 1, None), 1);
    }
}
