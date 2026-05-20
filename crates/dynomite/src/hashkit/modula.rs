//! Modula distribution: each live server contributes a number of
//! continuum slots equal to its weight, and dispatch is `hash %
//! ncontinuum`.
//!
//! # Examples
//!
//! ```
//! use dynomite::hashkit::modula::{Continuum, ServerSpec};
//! let c = Continuum::build(&[
//!     ServerSpec { name: "a".into(), weight: 1 },
//!     ServerSpec { name: "b".into(), weight: 1 },
//! ]).unwrap();
//! assert_eq!(c.dispatch(0).unwrap(), 0);
//! assert_eq!(c.dispatch(1).unwrap(), 1);
//! ```

use crate::core::types::DynError;

/// One continuum slot: weight unit -> server index.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::modula::Slot;
/// let s = Slot { server: 0 };
/// assert_eq!(s.server, 0);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Slot {
    /// Index back into the original server list.
    pub server: usize,
}

/// Sorted-by-server-order continuum.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::modula::{Continuum, ServerSpec};
/// let c = Continuum::build(&[ServerSpec { name: "a".into(), weight: 2 }]).unwrap();
/// assert_eq!(c.len(), 2);
/// ```
#[derive(Clone, Debug, Default)]
pub struct Continuum {
    slots: Vec<Slot>,
}

/// Specification for one server in modula mode.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::modula::ServerSpec;
/// let s = ServerSpec { name: "redis-a".into(), weight: 3 };
/// assert_eq!(s.weight, 3);
/// ```
#[derive(Clone, Debug)]
pub struct ServerSpec {
    /// Stable, unique identifier.
    pub name: String,
    /// Number of slots this server occupies on the continuum.
    pub weight: u32,
}

impl Continuum {
    /// Build the continuum from `servers`. Every server contributes
    /// `weight` consecutive slots in declaration order.
    ///
    /// # Errors
    ///
    /// Currently never fails; the signature returns `Result` so the
    /// distribution interface stays consistent with `ketama`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::modula::{Continuum, ServerSpec};
    /// let c = Continuum::build(&[
    ///     ServerSpec { name: "a".into(), weight: 3 },
    ///     ServerSpec { name: "b".into(), weight: 1 },
    /// ]).unwrap();
    /// assert_eq!(c.len(), 4);
    /// ```
    pub fn build(servers: &[ServerSpec]) -> Result<Self, DynError> {
        let total: usize = servers.iter().map(|s| s.weight as usize).sum();
        let mut slots = Vec::with_capacity(total);
        for (idx, server) in servers.iter().enumerate() {
            for _ in 0..server.weight {
                slots.push(Slot { server: idx });
            }
        }
        Ok(Self { slots })
    }

    /// Read-only view of the slots in their canonical order.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::modula::{Continuum, ServerSpec};
    /// let c = Continuum::build(&[ServerSpec { name: "a".into(), weight: 2 }]).unwrap();
    /// assert_eq!(c.slots().len(), 2);
    /// ```
    #[must_use]
    pub fn slots(&self) -> &[Slot] {
        &self.slots
    }

    /// Number of slots.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::modula::Continuum;
    /// assert_eq!(Continuum::default().len(), 0);
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether the continuum is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::modula::Continuum;
    /// assert!(Continuum::default().is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Map a 32-bit hash to a server index using `hash % len`.
    ///
    /// # Errors
    ///
    /// Returns an error when the continuum is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::modula::{Continuum, ServerSpec};
    /// let c = Continuum::build(&[
    ///     ServerSpec { name: "a".into(), weight: 1 },
    ///     ServerSpec { name: "b".into(), weight: 1 },
    /// ]).unwrap();
    /// assert_eq!(c.dispatch(2).unwrap(), 0);
    /// assert_eq!(c.dispatch(3).unwrap(), 1);
    /// ```
    pub fn dispatch(&self, hash: u32) -> Result<usize, DynError> {
        if self.slots.is_empty() {
            return Err(DynError::Generic("empty modula continuum".into()));
        }
        let i = (hash as usize) % self.slots.len();
        Ok(self.slots[i].server)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn equal_servers(n: usize) -> Vec<ServerSpec> {
        (0..n)
            .map(|i| ServerSpec {
                name: format!("s-{i}"),
                weight: 1,
            })
            .collect()
    }

    #[test]
    fn empty_input_yields_empty_continuum() {
        let c = Continuum::build(&[]).unwrap();
        assert!(c.is_empty());
        assert!(c.dispatch(0).is_err());
    }

    #[test]
    fn equal_weight_dispatches_modulo() {
        let c = Continuum::build(&equal_servers(4)).unwrap();
        for h in 0u32..32 {
            assert_eq!(c.dispatch(h).unwrap(), (h as usize) % 4);
        }
    }

    #[test]
    fn weights_expand_slots() {
        let servers = vec![
            ServerSpec {
                name: "a".into(),
                weight: 3,
            },
            ServerSpec {
                name: "b".into(),
                weight: 1,
            },
        ];
        let c = Continuum::build(&servers).unwrap();
        assert_eq!(c.len(), 4);
        assert_eq!(c.dispatch(0).unwrap(), 0);
        assert_eq!(c.dispatch(1).unwrap(), 0);
        assert_eq!(c.dispatch(2).unwrap(), 0);
        assert_eq!(c.dispatch(3).unwrap(), 1);
        assert_eq!(c.dispatch(4).unwrap(), 0);
    }

    #[test]
    fn dispatch_is_deterministic() {
        let c = Continuum::build(&equal_servers(3)).unwrap();
        for h in [0xdead_beef_u32, 1, 0, u32::MAX] {
            assert_eq!(c.dispatch(h).unwrap(), c.dispatch(h).unwrap());
        }
    }
}
