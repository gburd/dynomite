//! Node snitch: environment-driven address resolution and rack
//! proximity helpers.
//!
//! The reference engine's `dyn_node_snitch.{c,h}` is intentionally
//! small: it caches the local node's broadcast address, public
//! hostname, public IPv4, and private IPv4, looked up from
//! environment variables (`EC2_*` in the AWS environment, plain
//! `PUBLIC_*`/`LOCAL_IPV4` otherwise) with a fallback to the first
//! peer's name. The proximity ordering used by the dispatcher
//! (`pick_target_rack`, `rack_distance`) is not in `dyn_node_snitch.c`
//! at all; the reference engine's only DC/rack proximity decision
//! lives in `preselect_remote_rack_for_replication`
//! (`dyn_dnode_peer.c`). Per AGENTS.md non-negotiable #6 we honor the
//! C source: this module ports the env-var/hostname helpers and adds
//! a small set of pure rack-distance utilities used by
//! [`crate::cluster::dispatch`]. The proximity helpers are flagged as
//! a Stage-10 deviation in `docs/parity.md` because the brief asked
//! for them.
//!
//! # Examples
//!
//! ```
//! use dynomite::cluster::snitch::{rack_distance, RackDistance};
//! assert_eq!(rack_distance("dc1", "r1", "dc1", "r1"), RackDistance::Same);
//! assert_eq!(rack_distance("dc1", "r1", "dc1", "r2"), RackDistance::SameDc);
//! assert_eq!(rack_distance("dc1", "r1", "dc2", "r1"), RackDistance::Remote);
//! ```

use std::env;

/// Default environment string the reference engine treats as
/// "non-AWS" (mirrors `CONF_DEFAULT_ENV`).
pub const DEFAULT_ENV: &str = "aws";

/// Coarse-grained proximity classification used by the dispatcher
/// to order replica candidates.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum RackDistance {
    /// Same datacenter, same rack.
    Same,
    /// Same datacenter, different rack.
    SameDc,
    /// Different datacenter.
    Remote,
}

impl RackDistance {
    /// Numeric distance in `0..=2` for sorting.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::snitch::RackDistance;
    /// assert!(RackDistance::Same.cost() < RackDistance::SameDc.cost());
    /// ```
    #[must_use]
    pub fn cost(self) -> u8 {
        match self {
            RackDistance::Same => 0,
            RackDistance::SameDc => 1,
            RackDistance::Remote => 2,
        }
    }
}

/// Compute the [`RackDistance`] between `(self_dc, self_rack)` and
/// `(other_dc, other_rack)`.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::snitch::{rack_distance, RackDistance};
/// assert_eq!(rack_distance("a", "1", "a", "1"), RackDistance::Same);
/// ```
#[must_use]
pub fn rack_distance(
    self_dc: &str,
    self_rack: &str,
    other_dc: &str,
    other_rack: &str,
) -> RackDistance {
    if self_dc != other_dc {
        RackDistance::Remote
    } else if self_rack != other_rack {
        RackDistance::SameDc
    } else {
        RackDistance::Same
    }
}

/// Pick a rack name from `candidates` that is closest to
/// `(self_dc, self_rack)`.
///
/// Returns the first candidate at the smallest distance. `None` if
/// the candidate list is empty.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::snitch::pick_target_rack;
/// let cands = [("dc1", "r1"), ("dc1", "r2"), ("dc2", "r1")];
/// assert_eq!(pick_target_rack("dc1", "r2", &cands), Some(("dc1", "r2")));
/// ```
#[must_use]
pub fn pick_target_rack<'a>(
    self_dc: &str,
    self_rack: &str,
    candidates: &'a [(&'a str, &'a str)],
) -> Option<(&'a str, &'a str)> {
    let mut best: Option<(RackDistance, (&str, &str))> = None;
    for &(dc, rack) in candidates {
        let d = rack_distance(self_dc, self_rack, dc, rack);
        match best {
            Some((bd, _)) if bd.cost() <= d.cost() => {}
            _ => best = Some((d, (dc, rack))),
        }
    }
    best.map(|(_, p)| p)
}

/// Whether the supplied environment label equals
/// [`DEFAULT_ENV`].
///
/// # Examples
///
/// ```
/// use dynomite::cluster::snitch::{is_aws_env, DEFAULT_ENV};
/// assert!(is_aws_env(DEFAULT_ENV));
/// assert!(!is_aws_env("baremetal"));
/// ```
#[must_use]
pub fn is_aws_env(env_label: &str) -> bool {
    env_label.starts_with(DEFAULT_ENV)
}

/// Look up the broadcast address from environment variables, then
/// fall back to `peer_name_fallback` (the first peer's name in the
/// reference engine).
///
/// Mirrors `get_broadcast_address`.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::snitch::broadcast_address;
/// // With no env vars set, falls back to the supplied peer name.
/// assert_eq!(
///     broadcast_address("baremetal", "127.0.0.1", &mut |_| None),
///     "127.0.0.1",
/// );
/// ```
pub fn broadcast_address(
    env_label: &str,
    peer_name_fallback: &str,
    lookup_env: &mut dyn FnMut(&str) -> Option<String>,
) -> String {
    let key = if is_aws_env(env_label) {
        "EC2_PUBLIC_HOSTNAME"
    } else {
        "PUBLIC_HOSTNAME"
    };
    if let Some(v) = lookup_env(key) {
        return v;
    }
    peer_name_fallback.to_string()
}

/// Look up the public hostname; mirrors `get_public_hostname`. The
/// fallback is the peer's `name` field if it does not begin with a
/// digit.
pub fn public_hostname(
    env_label: &str,
    peer_name_fallback: &str,
    lookup_env: &mut dyn FnMut(&str) -> Option<String>,
) -> Option<String> {
    let key = if is_aws_env(env_label) {
        "EC2_PUBLIC_HOSTNAME"
    } else {
        "PUBLIC_HOSTNAME"
    };
    if let Some(v) = lookup_env(key) {
        return Some(v);
    }
    let first = peer_name_fallback.bytes().next()?;
    if first.is_ascii_digit() {
        None
    } else {
        Some(peer_name_fallback.to_string())
    }
}

/// Look up the public IPv4 address; mirrors `get_public_ip4`. The
/// fallback is the peer's `name` if it begins with a digit.
pub fn public_ip4(
    env_label: &str,
    peer_name_fallback: &str,
    lookup_env: &mut dyn FnMut(&str) -> Option<String>,
) -> Option<String> {
    let key = if is_aws_env(env_label) {
        "EC2_PUBLIC_IPV4"
    } else {
        "PUBLIC_IPV4"
    };
    if let Some(v) = lookup_env(key) {
        return Some(v);
    }
    let first = peer_name_fallback.bytes().next()?;
    if first.is_ascii_digit() {
        Some(peer_name_fallback.to_string())
    } else {
        None
    }
}

/// Look up the private IPv4 address; mirrors `get_private_ip4`.
/// Returns `None` when neither environment variable is set (the
/// reference engine returns `NULL` in that case).
pub fn private_ip4(
    env_label: &str,
    lookup_env: &mut dyn FnMut(&str) -> Option<String>,
) -> Option<String> {
    let key = if is_aws_env(env_label) {
        "EC2_LOCAL_IPV4"
    } else {
        "LOCAL_IPV4"
    };
    lookup_env(key)
}

/// Convenience that reads from the real process environment via
/// [`std::env::var`].
///
/// # Examples
///
/// ```
/// use dynomite::cluster::snitch::process_env_lookup;
/// // The closure is `FnMut` and reads the live environment.
/// let mut f = process_env_lookup();
/// // PATH is almost always set; if not, the closure simply returns None.
/// let _ = f("PATH");
/// ```
#[must_use]
pub fn process_env_lookup() -> impl FnMut(&str) -> Option<String> {
    |key: &str| env::var(key).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_orders_correctly() {
        assert_eq!(rack_distance("dc", "r", "dc", "r"), RackDistance::Same);
        assert_eq!(rack_distance("dc", "r", "dc", "x"), RackDistance::SameDc);
        assert_eq!(rack_distance("dc", "r", "dx", "r"), RackDistance::Remote);
    }

    #[test]
    fn pick_target_rack_prefers_local_rack() {
        let cands = [("dc", "r"), ("dc", "x"), ("d2", "r")];
        let pick = pick_target_rack("dc", "r", &cands);
        assert_eq!(pick, Some(("dc", "r")));
    }

    #[test]
    fn pick_target_rack_falls_back_to_same_dc() {
        let cands = [("dc", "x"), ("d2", "r")];
        let pick = pick_target_rack("dc", "r", &cands);
        assert_eq!(pick, Some(("dc", "x")));
    }

    #[test]
    fn pick_target_rack_falls_back_to_remote() {
        let cands = [("d2", "r")];
        let pick = pick_target_rack("dc", "r", &cands);
        assert_eq!(pick, Some(("d2", "r")));
    }

    #[test]
    fn pick_target_rack_empty() {
        let cands: [(&str, &str); 0] = [];
        let pick = pick_target_rack("dc", "r", &cands);
        assert!(pick.is_none());
    }

    #[test]
    fn broadcast_uses_env_first() {
        let mut envs = |k: &str| {
            if k == "EC2_PUBLIC_HOSTNAME" {
                Some("ec2-host".into())
            } else {
                None
            }
        };
        assert_eq!(broadcast_address("aws", "fb", &mut envs), "ec2-host");
    }

    #[test]
    fn broadcast_falls_back_to_peer_name() {
        let mut envs = |_: &str| None;
        assert_eq!(broadcast_address("aws", "127.0.0.1", &mut envs), "127.0.0.1");
    }

    #[test]
    fn public_hostname_skips_numeric_fallback() {
        let mut envs = |_: &str| None;
        assert!(public_hostname("baremetal", "1.2.3.4", &mut envs).is_none());
        assert_eq!(
            public_hostname("baremetal", "host.dns", &mut envs).as_deref(),
            Some("host.dns"),
        );
    }

    #[test]
    fn public_ip4_skips_dns_fallback() {
        let mut envs = |_: &str| None;
        assert!(public_ip4("aws", "host.dns", &mut envs).is_none());
        assert_eq!(
            public_ip4("aws", "10.0.0.1", &mut envs).as_deref(),
            Some("10.0.0.1"),
        );
    }
}
