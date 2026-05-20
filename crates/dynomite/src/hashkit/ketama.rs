//! Ketama consistent-hashing continuum.
//!
//! For every live server we generate `KETAMA_POINTS_PER_SERVER` (160)
//! continuum points proportional to the server's weight. Each set of 4
//! points is computed from a single MD5 digest of `"<name>-<idx>"`,
//! pulling 32-bit slices out of the digest at offsets 0, 4, 8, and 12.
//! Lookups walk the sorted continuum and pick the first point with a
//! token strictly greater than the requested hash, wrapping back to the
//! beginning when the lookup falls past the end.

// Weighted-fraction arithmetic widens u32 weights into f64 in the
// continuum builder; precision loss at u32::MAX is negligible for ring-
// point counts. See docs/journal/allowances.md.
#![allow(clippy::cast_precision_loss)]

use std::cmp::Ordering;

use crate::core::types::DynError;
use crate::hashkit::md5_signature;
use crate::hashkit::token::DynToken;

/// 160 points per server. Mirrors `KETAMA_POINTS_PER_SERVER` in C.
pub const POINTS_PER_SERVER: u32 = 160;
/// Each MD5 digest yields 4 continuum points.
pub const POINTS_PER_HASH: u32 = 4;
/// Maximum length of `"<name>-<idx>"` used to seed each digest.
pub const MAX_HOSTLEN: usize = 86;

/// Specification for one server in the continuum.
#[derive(Clone, Debug)]
pub struct ServerSpec {
    /// Stable, unique identifier (used to derive the continuum points).
    pub name: String,
    /// Relative weight; higher weights map to more continuum points.
    pub weight: u32,
}

/// One entry on the continuum: a token and the index of the server that
/// owns it.
#[derive(Clone, Debug)]
pub struct ContinuumPoint {
    /// Sorted-by-token coordinate.
    pub token: DynToken,
    /// Index back into the original server list.
    pub server: usize,
}

/// Sorted continuum, ready for `dispatch`.
#[derive(Clone, Debug, Default)]
pub struct Continuum {
    points: Vec<ContinuumPoint>,
}

impl Continuum {
    /// Build the continuum for the supplied servers.
    ///
    /// # Errors
    ///
    /// Returns `DynError::Generic` when a server's `name + index` would
    /// overflow the 86-byte buffer that the C reference allocates.
    pub fn build(servers: &[ServerSpec]) -> Result<Self, DynError> {
        if servers.is_empty() {
            return Ok(Self::default());
        }
        let total_weight: u64 = servers.iter().map(|s| u64::from(s.weight)).sum();
        if total_weight == 0 {
            return Ok(Self::default());
        }
        let nlive = servers.len() as u64;
        let mut points: Vec<ContinuumPoint> = Vec::new();

        for (server_idx, server) in servers.iter().enumerate() {
            let pct = f64::from(server.weight) / total_weight as f64;
            let raw = pct * f64::from(POINTS_PER_SERVER) / f64::from(POINTS_PER_HASH)
                * (nlive as f64)
                + 0.000_000_000_1;
            let pointer_per_server = raw.floor() as u32 * POINTS_PER_HASH;
            let groups = pointer_per_server / POINTS_PER_HASH;

            for pointer_index in 1..=groups {
                let host = format!("{}-{}", server.name, pointer_index - 1);
                if host.len() >= MAX_HOSTLEN {
                    return Err(DynError::Generic(format!(
                        "ketama host string {host:?} exceeds {MAX_HOSTLEN}"
                    )));
                }
                let digest = md5_signature(host.as_bytes());
                for x in 0..POINTS_PER_HASH {
                    let off = (x as usize) * 4;
                    let value = (u32::from(digest[3 + off]) << 24)
                        | (u32::from(digest[2 + off]) << 16)
                        | (u32::from(digest[1 + off]) << 8)
                        | u32::from(digest[off]);
                    points.push(ContinuumPoint {
                        token: DynToken::from_u32(value),
                        server: server_idx,
                    });
                }
            }
        }

        points.sort_by(|a, b| a.token.cmp(&b.token));
        Ok(Self { points })
    }

    /// Number of continuum points.
    #[must_use]
    pub fn len(&self) -> usize {
        self.points.len()
    }

    /// Whether the continuum is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// Read-only view of the continuum points, in sorted order.
    #[must_use]
    pub fn points(&self) -> &[ContinuumPoint] {
        &self.points
    }

    /// Map a hash value to the owning server index.
    ///
    /// Walks the continuum with a binary search and wraps around when
    /// the requested token sorts after the last point.
    ///
    /// # Errors
    ///
    /// Returns an error if the continuum is empty.
    pub fn dispatch(&self, hash: &DynToken) -> Result<usize, DynError> {
        if self.points.is_empty() {
            return Err(DynError::Generic("empty ketama continuum".into()));
        }
        // Lower bound: first point with token >= hash.
        let mut left = 0usize;
        let mut right = self.points.len();
        while left < right {
            let mid = left + (right - left) / 2;
            match self.points[mid].token.cmp(hash) {
                Ordering::Less => left = mid + 1,
                _ => right = mid,
            }
        }
        let pos = if right == self.points.len() { 0 } else { right };
        Ok(self.points[pos].server)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn equal_servers(n: usize) -> Vec<ServerSpec> {
        (0..n)
            .map(|i| ServerSpec {
                name: format!("server-{i}"),
                weight: 1,
            })
            .collect()
    }

    #[test]
    fn empty_input_yields_empty_continuum() {
        let c = Continuum::build(&[]).unwrap();
        assert!(c.is_empty());
        assert!(c.dispatch(&DynToken::from_u32(123)).is_err());
    }

    #[test]
    fn equal_weight_balanced() {
        let c = Continuum::build(&equal_servers(4)).unwrap();
        // Each server should contribute the same number of points.
        let mut counts = [0usize; 4];
        for p in c.points() {
            counts[p.server] += 1;
        }
        let expected = counts[0];
        for c in &counts {
            assert_eq!(*c, expected);
        }
    }

    #[test]
    fn dispatch_is_deterministic() {
        let c = Continuum::build(&equal_servers(3)).unwrap();
        for k in [123u32, 1, 0xdead_beef, 0x8000_0000, u32::MAX] {
            let a = c.dispatch(&DynToken::from_u32(k)).unwrap();
            let b = c.dispatch(&DynToken::from_u32(k)).unwrap();
            assert_eq!(a, b);
        }
    }

    #[test]
    fn dispatch_wraps_past_last_point() {
        let c = Continuum::build(&equal_servers(2)).unwrap();
        let last = c.points().last().unwrap().token.clone();
        let beyond = DynToken::from_u32(last.get_int().wrapping_add(1));
        let s = c.dispatch(&beyond).unwrap();
        assert_eq!(s, c.points()[0].server);
    }

    #[test]
    fn weight_changes_share() {
        let servers = vec![
            ServerSpec {
                name: "s0".into(),
                weight: 1,
            },
            ServerSpec {
                name: "s1".into(),
                weight: 2,
            },
        ];
        let c = Continuum::build(&servers).unwrap();
        let mut counts = [0usize; 2];
        for p in c.points() {
            counts[p.server] += 1;
        }
        assert!(counts[1] > counts[0]);
    }
}
