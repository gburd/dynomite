//! Token ring math: building and querying per-rack continuums.
//!
//! The reference engine's `vnode_update` walks the pool's peer list,
//! pushes each peer's tokens onto the owning rack's `continuum`
//! array, then sorts the array per rack. The dispatcher then calls
//! `vnode_dispatch(continuums, ncontinuum, token)` to find the peer
//! that owns a key. The function uses a left-leaning binary search:
//!
//! * if the search token falls outside the ring (less than the first
//!   token or strictly greater than the last), wrap to the first
//!   continuum point;
//! * otherwise, find the smallest continuum entry whose token is
//!   greater than or equal to the search token (mirrors
//!   `(a, b]` semantics from the reference).
//!
//! Both behaviours are reproduced verbatim by [`dispatch`] below.
//!
//! # Examples
//!
//! ```
//! use dynomite::cluster::vnode::dispatch;
//! use dynomite::cluster::datacenter::{Continuum, Rack};
//! use dynomite::hashkit::DynToken;
//!
//! let mut r = Rack::new("r".into(), "d".into());
//! r.add_peer_tokens(0, &[DynToken::from_u32(10)]);
//! r.add_peer_tokens(1, &[DynToken::from_u32(20)]);
//! r.add_peer_tokens(2, &[DynToken::from_u32(30)]);
//! r.sort_continuums();
//! assert_eq!(dispatch(r.continuums(), &DynToken::from_u32(15)), Some(1));
//! assert_eq!(dispatch(r.continuums(), &DynToken::from_u32(35)), Some(0));
//! ```

use std::cmp::Ordering;

use crate::cluster::datacenter::Continuum;
use crate::hashkit::DynToken;

/// Run the reference engine's `vnode_dispatch` over `continuums`.
///
/// Returns the peer index for the continuum point that owns
/// `token`, or `None` when the slice is empty.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::vnode::dispatch;
/// use dynomite::cluster::datacenter::Continuum;
/// use dynomite::hashkit::DynToken;
/// let cs: [Continuum; 0] = [];
/// assert_eq!(dispatch(&cs, &DynToken::from_u32(0)), None);
/// ```
#[must_use]
pub fn dispatch(continuums: &[Continuum], token: &DynToken) -> Option<u32> {
    let n = continuums.len();
    if n == 0 {
        return None;
    }
    let first = &continuums[0];
    let last = &continuums[n - 1];

    // Wraparound: token greater than the largest continuum token, or
    // less than or equal to the first one. Reference returns
    // `left->index` in either case.
    if last.token.cmp(token) == Ordering::Less {
        return Some(first.peer_idx);
    }
    if first.token.cmp(token) != Ordering::Less {
        return Some(first.peer_idx);
    }

    // Binary search for the smallest continuum entry with token >=
    // search token. Mirrors the reference engine's `vnode_dispatch`.
    let mut left = 0usize;
    let mut right = n - 1;
    while left < right {
        let middle = left + (right - left) / 2;
        match continuums[middle].token.cmp(token) {
            Ordering::Equal => return Some(continuums[middle].peer_idx),
            Ordering::Less => left = middle + 1,
            Ordering::Greater => right = middle,
        }
    }
    Some(continuums[right].peer_idx)
}

/// Per-peer token-list shape consumed by the rebuild pass.
///
/// `peer_idx` is the index into the pool's peer array; `tokens`
/// is the token list for that peer. Mirrors the data shape
/// `vnode_update` walks but decoupled from the live pool so the
/// rebuild can be unit-tested.
#[derive(Clone, Debug)]
pub struct PeerTokens<'a> {
    /// Peer index in the pool's peer array.
    pub peer_idx: u32,
    /// Datacenter name.
    pub dc: &'a str,
    /// Rack name.
    pub rack: &'a str,
    /// Peer's token list.
    pub tokens: &'a [DynToken],
}

/// Walk a list of [`PeerTokens`] and append continuum entries to
/// the matching rack inside `dcs`.
///
/// Caller is responsible for invoking
/// [`crate::cluster::datacenter::Rack::sort_continuums`] on each
/// touched rack once the rebuild is complete (this matches the
/// reference engine's `vnode_rack_verify_continuum`).
///
/// Returns the count of peers actually applied (a peer whose
/// `(dc, rack)` is missing from `dcs` is skipped, which mirrors
/// the reference engine's behaviour of populating dc / rack tables
/// before calling `vnode_update`).
///
/// # Examples
///
/// ```
/// use dynomite::cluster::datacenter::Datacenter;
/// use dynomite::cluster::vnode::{rebuild_continuums, PeerTokens};
/// use dynomite::hashkit::DynToken;
///
/// let mut dc = Datacenter::new("d".into());
/// dc.upsert_rack("r".into());
/// let toks = [DynToken::from_u32(7)];
/// let count = rebuild_continuums(
///     &mut [dc],
///     &[PeerTokens { peer_idx: 0, dc: "d", rack: "r", tokens: &toks }],
/// );
/// assert_eq!(count, 1);
/// ```
pub fn rebuild_continuums(
    dcs: &mut [crate::cluster::datacenter::Datacenter],
    peers: &[PeerTokens<'_>],
) -> usize {
    // First, clear every rack's continuum so the walk produces a
    // deterministic result on each call.
    for dc in dcs.iter_mut() {
        for rack in dc.racks_mut().iter_mut() {
            rack.clear_continuums();
        }
    }
    let mut applied = 0usize;
    let mut touched: Vec<(usize, usize)> = Vec::new();
    for peer in peers {
        let Some(dc_idx) = dcs.iter().position(|d| d.name() == peer.dc) else {
            continue;
        };
        let Some(rack_idx) = dcs[dc_idx].rack_idx(peer.rack) else {
            continue;
        };
        let dc = &mut dcs[dc_idx];
        let rack = &mut dc.racks_mut()[rack_idx];
        rack.add_peer_tokens(peer.peer_idx, peer.tokens);
        applied += 1;
        if !touched.contains(&(dc_idx, rack_idx)) {
            touched.push((dc_idx, rack_idx));
        }
    }
    for (di, ri) in touched {
        dcs[di].racks_mut()[ri].sort_continuums();
    }
    applied
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::datacenter::Datacenter;

    fn ring(pairs: &[(u32, u32)]) -> Vec<Continuum> {
        pairs
            .iter()
            .map(|&(idx, tok)| Continuum::new(DynToken::from_u32(tok), idx))
            .collect()
    }

    #[test]
    fn empty_ring_returns_none() {
        let cs: [Continuum; 0] = [];
        assert_eq!(dispatch(&cs, &DynToken::from_u32(0)), None);
    }

    #[test]
    fn single_token_always_resolves() {
        let cs = ring(&[(7, 100)]);
        assert_eq!(dispatch(&cs, &DynToken::from_u32(0)), Some(7));
        assert_eq!(dispatch(&cs, &DynToken::from_u32(100)), Some(7));
        assert_eq!(dispatch(&cs, &DynToken::from_u32(101)), Some(7));
    }

    #[test]
    fn dispatch_wraps_on_overflow() {
        let cs = ring(&[(0, 10), (1, 20), (2, 30)]);
        assert_eq!(dispatch(&cs, &DynToken::from_u32(35)), Some(0));
        assert_eq!(dispatch(&cs, &DynToken::from_u32(0)), Some(0));
    }

    #[test]
    fn dispatch_finds_upper_bound() {
        let cs = ring(&[(0, 10), (1, 20), (2, 30)]);
        assert_eq!(dispatch(&cs, &DynToken::from_u32(11)), Some(1));
        assert_eq!(dispatch(&cs, &DynToken::from_u32(20)), Some(1));
        assert_eq!(dispatch(&cs, &DynToken::from_u32(21)), Some(2));
        assert_eq!(dispatch(&cs, &DynToken::from_u32(30)), Some(2));
    }

    #[test]
    fn rebuild_skips_unknown_dc() {
        let mut dc = Datacenter::new("d".into());
        dc.upsert_rack("r".into());
        let mut dcs = vec![dc];
        let toks = [DynToken::from_u32(1)];
        let known = PeerTokens {
            peer_idx: 0,
            dc: "d",
            rack: "r",
            tokens: &toks,
        };
        let unknown = PeerTokens {
            peer_idx: 1,
            dc: "ghost",
            rack: "r",
            tokens: &toks,
        };
        assert_eq!(rebuild_continuums(&mut dcs, &[known, unknown]), 1);
    }

    #[test]
    fn rebuild_clears_before_repopulating() {
        let mut dc = Datacenter::new("d".into());
        dc.upsert_rack("r".into());
        let mut dcs = vec![dc];
        let toks = [DynToken::from_u32(1)];
        let p = PeerTokens {
            peer_idx: 0,
            dc: "d",
            rack: "r",
            tokens: &toks,
        };
        rebuild_continuums(&mut dcs, std::slice::from_ref(&p));
        rebuild_continuums(&mut dcs, &[p]);
        assert_eq!(dcs[0].racks()[0].ncontinuum(), 1);
    }
}
