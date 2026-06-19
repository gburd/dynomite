//! Token-ring coverage validation.
//!
//! Operators sometimes ship configs where one or more peers'
//! token ranges leave a rack effectively empty, or where two
//! peers in the same rack claim the same token point. Both
//! shapes are silent footguns: the dispatcher will either skip
//! the affected rack (empty continuum) or non-deterministically
//! prefer one of the colliding peers (duplicate tokens), and
//! the resulting routing surprises only show up under load.
//! With the `make_error` fix shipped, the affected requests now
//! surface as real `NoTargets` errors to clients (previously
//! they were request-timeout hangs); this validator catches the
//! offending configs at config-load time before they ever reach
//! the dispatcher.
//!
//! # Ring semantics and why `Gap` is structurally impossible
//!
//! [`crate::cluster::vnode::dispatch`] uses wrap-around vnode
//! semantics: for any non-empty continuum, the first peer owns
//! `(last.token, u32::MAX]` and `[0, first.token]`, and each
//! subsequent peer at index `i` owns `(prev.token, current.token]`.
//! As a consequence, every `u32` token resolves to *some* peer
//! whenever the rack has at least one continuum entry. There is
//! no token configuration that leaves a hole in `[0, u32::MAX]`
//! short of an entirely empty rack. The two failure modes this
//! validator catches are therefore:
//!
//! * [`TokenCoverageError::EmptyRack`] - a `(dc, rack)` pair has
//!   peers but none contribute tokens, or has no peers at all.
//! * [`TokenCoverageError::Overlap`] - two peers in the same rack
//!   claim the exact same token value, leaving the choice of
//!   primary peer for that point dependent on iteration order.
//!
//! A `Gap` variant is intentionally *not* included: gaps are not
//! reachable under the current Dynomite vnode model, and keeping
//! the enum to the variants we actually emit avoids dead code.
//!
//! # Examples
//!
//! ```
//! use dynomite::cluster::coverage::validate_token_coverage;
//! use dynomite::cluster::peer::{Peer, PeerEndpoint};
//! use dynomite::hashkit::DynToken;
//!
//! let local = Peer::new(
//!     0,
//!     PeerEndpoint::tcp("h0".into(), 8101),
//!     "r1".into(),
//!     "dc1".into(),
//!     vec![DynToken::from_u32(0)],
//!     true,
//!     true,
//!     false,
//! );
//! assert!(validate_token_coverage(&[local]).is_ok());
//! ```

use std::collections::BTreeMap;

use thiserror::Error;

use crate::cluster::peer::Peer;

/// Error returned by [`validate_token_coverage`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TokenCoverageError {
    /// A `(dc, rack)` pair contributes zero tokens to the ring.
    /// Either the rack has no peers, or every peer in the rack
    /// has an empty token list.
    #[error("rack {dc}/{rack} has no tokens")]
    EmptyRack {
        /// Datacenter name.
        dc: String,
        /// Rack name.
        rack: String,
    },
    /// Two peers in the same rack claim the same token value.
    /// The dispatcher's binary search will pick whichever peer
    /// happened to sort first, which is non-deterministic across
    /// reloads if peer indices change.
    #[error(
        "rack {dc}/{rack} has overlapping token {token} claimed by peer {peer_a} and peer {peer_b}"
    )]
    Overlap {
        /// Datacenter name.
        dc: String,
        /// Rack name.
        rack: String,
        /// The token value claimed by both peers.
        token: u32,
        /// Index of the first claimant peer.
        peer_a: u32,
        /// Index of the second claimant peer.
        peer_b: u32,
    },
}

/// Validate that every `(dc, rack)` in `peers` produces a usable
/// ring under [`crate::cluster::vnode::dispatch`] semantics.
///
/// The check is per-rack: a fault in DC2 still rejects even if
/// DC1 is fully populated, because the dispatcher walks each
/// rack independently when planning (the dispatcher's internal
/// `collect_routable` step pushes one entry per `(dc, rack)`).
///
/// Returns the *first* fault encountered. The traversal order is
/// the natural `BTreeMap` order over `(dc, rack)` strings, which
/// is stable across runs given identical input.
///
/// # Errors
///
/// * [`TokenCoverageError::EmptyRack`] if any `(dc, rack)`
///   contributes zero tokens.
/// * [`TokenCoverageError::Overlap`] if two peers in the same
///   rack claim the same token value.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::coverage::{validate_token_coverage, TokenCoverageError};
/// use dynomite::cluster::peer::{Peer, PeerEndpoint};
/// use dynomite::hashkit::DynToken;
///
/// let a = Peer::new(
///     0, PeerEndpoint::tcp("a".into(), 8101),
///     "r".into(), "d".into(),
///     vec![DynToken::from_u32(7)], true, true, false,
/// );
/// let b = Peer::new(
///     1, PeerEndpoint::tcp("b".into(), 8101),
///     "r".into(), "d".into(),
///     vec![DynToken::from_u32(7)], false, true, false,
/// );
/// assert!(matches!(
///     validate_token_coverage(&[a, b]),
///     Err(TokenCoverageError::Overlap { .. })
/// ));
/// ```
pub fn validate_token_coverage(peers: &[Peer]) -> Result<(), TokenCoverageError> {
    let mut by_rack: BTreeMap<(&str, &str), Vec<&Peer>> = BTreeMap::new();
    for p in peers {
        by_rack.entry((p.dc(), p.rack())).or_default().push(p);
    }
    for ((dc, rack), rack_peers) in &by_rack {
        let mut tokens: Vec<(u32, u32)> = Vec::new();
        for p in rack_peers {
            for t in p.tokens() {
                tokens.push((t.get_int(), p.idx()));
            }
        }
        if tokens.is_empty() {
            return Err(TokenCoverageError::EmptyRack {
                dc: (*dc).to_string(),
                rack: (*rack).to_string(),
            });
        }
        tokens.sort_by_key(|&(t, _)| t);
        for w in tokens.windows(2) {
            if w[0].0 == w[1].0 {
                return Err(TokenCoverageError::Overlap {
                    dc: (*dc).to_string(),
                    rack: (*rack).to_string(),
                    token: w[0].0,
                    peer_a: w[0].1,
                    peer_b: w[1].1,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::peer::PeerEndpoint;
    use crate::hashkit::DynToken;

    fn mk(idx: u32, dc: &str, rack: &str, tokens: &[u32]) -> Peer {
        Peer::new(
            idx,
            PeerEndpoint::tcp("127.0.0.1".into(), 8101 + u16::try_from(idx).unwrap_or(0)),
            rack.into(),
            dc.into(),
            tokens.iter().copied().map(DynToken::from_u32).collect(),
            idx == 0,
            true,
            false,
        )
    }

    #[test]
    fn empty_input_is_ok() {
        assert!(validate_token_coverage(&[]).is_ok());
    }

    #[test]
    fn empty_rack_rejected() {
        // Single peer with no tokens at all: the rack
        // contributes nothing to the continuum, so the
        // dispatcher would silently produce zero candidates for
        // every key hashed against this rack.
        let p = mk(0, "dc1", "r1", &[]);
        let err = validate_token_coverage(&[p]).unwrap_err();
        assert_eq!(
            err,
            TokenCoverageError::EmptyRack {
                dc: "dc1".into(),
                rack: "r1".into(),
            }
        );
    }

    #[test]
    fn duplicate_token_rejected() {
        // Two peers in the same rack with identical tokens:
        // dispatch would non-deterministically pick whichever
        // sorted first, producing reload-dependent routing.
        let a = mk(0, "dc1", "r1", &[1_234_567]);
        let b = mk(1, "dc1", "r1", &[1_234_567]);
        let err = validate_token_coverage(&[a, b]).unwrap_err();
        match err {
            TokenCoverageError::Overlap {
                dc,
                rack,
                token,
                peer_a,
                peer_b,
            } => {
                assert_eq!(dc, "dc1");
                assert_eq!(rack, "r1");
                assert_eq!(token, 1_234_567);
                let mut got = [peer_a, peer_b];
                got.sort_unstable();
                assert_eq!(got, [0, 1]);
            }
            TokenCoverageError::EmptyRack { .. } => panic!("expected Overlap, got EmptyRack"),
        }
    }

    #[test]
    fn duplicate_across_racks_is_fine() {
        // Same token in different racks is the standard vnode
        // pattern (each rack is an independent replica of the
        // ring) and must not be rejected.
        let a = mk(0, "dc1", "r1", &[42]);
        let b = mk(1, "dc1", "r2", &[42]);
        assert!(validate_token_coverage(&[a, b]).is_ok());
    }

    #[test]
    fn duplicate_across_dcs_is_fine() {
        // Same token in different DCs is also normal: each DC
        // walks its own continuum.
        let a = mk(0, "dc1", "r1", &[42]);
        let b = mk(1, "dc2", "r1", &[42]);
        assert!(validate_token_coverage(&[a, b]).is_ok());
    }

    #[test]
    fn valid_3_peer_ring() {
        // Even token spacing for a 3-way split.
        let a = mk(0, "dc1", "r1", &[0]);
        let b = mk(1, "dc1", "r1", &[1_431_655_765]);
        let c = mk(2, "dc1", "r1", &[2_863_311_530]);
        assert!(validate_token_coverage(&[a, b, c]).is_ok());
    }

    #[test]
    fn valid_4_peer_ring() {
        // Even token spacing for a 4-way split (the shape pass-3
        // intended).
        let a = mk(0, "dc1", "r1", &[0]);
        let b = mk(1, "dc1", "r1", &[1_073_741_824]);
        let c = mk(2, "dc1", "r1", &[2_147_483_648]);
        let d = mk(3, "dc1", "r1", &[3_221_225_472]);
        assert!(validate_token_coverage(&[a, b, c, d]).is_ok());
    }

    #[test]
    fn pass3_3_peer_subset_is_valid_config() {
        // Pass-3 ran with only the first three of the four
        // intended peers: floki/0, arnold/1G, nuc/2G. By the
        // vnode wrap-around semantics this is a fully-covered
        // ring (peer 0 owns the wrap segment from 2G+1 through
        // u32::MAX plus the singleton at 0), so the validator
        // must accept it. The 14% workload error rate observed
        // in the chaos run was therefore *not* a token-coverage
        // gap; see docs/journal/2026-05-25-token-coverage-validation.md
        // for the alternative root-cause hypotheses.
        let a = mk(0, "dc1", "r1", &[0]);
        let b = mk(1, "dc1", "r1", &[1_073_741_824]);
        let c = mk(2, "dc1", "r1", &[2_147_483_648]);
        assert!(validate_token_coverage(&[a, b, c]).is_ok());
    }

    #[test]
    fn multi_token_per_peer_is_fine() {
        // A single peer holding several tokens (vnode "virtual
        // node" pattern) is a valid configuration.
        let a = mk(0, "dc1", "r1", &[0, 1_073_741_824]);
        let b = mk(1, "dc1", "r1", &[2_147_483_648, 3_221_225_472]);
        assert!(validate_token_coverage(&[a, b]).is_ok());
    }

    #[test]
    fn duplicate_within_single_peer_token_list_rejected() {
        // A peer whose own token list contains the same token
        // twice is also a fault: the rack-level continuum will
        // contain two entries with the same key and the same
        // peer_idx, breaking the strictly-increasing precondition
        // the dispatcher relies on.
        let a = mk(0, "dc1", "r1", &[5, 5]);
        let err = validate_token_coverage(&[a]).unwrap_err();
        assert!(matches!(err, TokenCoverageError::Overlap { token: 5, .. }));
    }

    #[test]
    fn fault_in_second_dc_still_rejects() {
        // First DC is fine; second DC has a duplicate. The
        // validator must still reject because the dispatcher
        // walks each rack independently.
        let a = mk(0, "dc1", "r1", &[10]);
        let b = mk(1, "dc1", "r1", &[20]);
        let c = mk(2, "dc2", "r1", &[100]);
        let d = mk(3, "dc2", "r1", &[100]);
        let err = validate_token_coverage(&[a, b, c, d]).unwrap_err();
        match err {
            TokenCoverageError::Overlap { dc, .. } => assert_eq!(dc, "dc2"),
            TokenCoverageError::EmptyRack { .. } => {
                panic!("expected Overlap in dc2, got EmptyRack")
            }
        }
    }

    #[test]
    fn empty_rack_in_second_dc_still_rejects() {
        // A peer present in dc2/r1 but with no tokens leaves the
        // second DC empty even though the first is healthy.
        let a = mk(0, "dc1", "r1", &[10]);
        let b = mk(1, "dc2", "r1", &[]);
        let err = validate_token_coverage(&[a, b]).unwrap_err();
        assert_eq!(
            err,
            TokenCoverageError::EmptyRack {
                dc: "dc2".into(),
                rack: "r1".into(),
            }
        );
    }
}
