//! DNS-backed seeds provider.
//!
//! The reference engine issues a `T_TXT` query (or `T_A` when
//! `DYNOMITE_DNS_TYPE=A`) against `_dynomite.<host>` and returns
//! each TXT record's contents (or one synthesised seed per A
//! record). The Rust port abstracts the resolver behind the
//! [`Resolver`] trait so the unit test can drive a deterministic
//! in-memory resolver. The caller is expected to wire
//! `tokio::net::lookup_host` (or a similar resolver) when
//! building the production provider.
//!
//! # Examples
//!
//! ```
//! use dynomite::seeds::dns::{DnsSeedsProvider, Resolver, ResolvedSeeds};
//! use dynomite::seeds::SeedsProvider;
//!
//! struct StaticResolver;
//! impl Resolver for StaticResolver {
//!     fn resolve(&self, _name: &str)
//!         -> Result<ResolvedSeeds, dynomite::seeds::SeedsError>
//!     {
//!         Ok(ResolvedSeeds::Txt(vec![
//!             "h1:8101:rA:dc1:1".into(),
//!             "h2:8101:rA:dc1:2".into(),
//!         ]))
//!     }
//! }
//! let p = DnsSeedsProvider::new("_dynomite.example".into(), Box::new(StaticResolver));
//! assert_eq!(p.get_seeds().unwrap().len(), 2);
//! ```

use std::sync::Arc;

use crate::conf::ConfDynSeed;
use crate::seeds::{SeedsError, SeedsProvider};

/// Resolver result.
#[derive(Debug, Clone)]
pub enum ResolvedSeeds {
    /// One TXT record per element. Each TXT body must be a
    /// `host:port:rack:dc:tokens` seed (mirrors the reference
    /// engine's `dns_get_seeds` TXT branch).
    Txt(Vec<String>),
    /// One A record per element, returned as `host:port` strings.
    /// The provider attaches the supplied default rack/dc/tokens
    /// when building the seed (mirrors the reference's `T_A`
    /// branch where every result shares the same rack / dc).
    A {
        /// Resolved IP literals.
        ips: Vec<String>,
        /// Default port to attach.
        port: u16,
        /// Default rack name.
        rack: String,
        /// Default dc name.
        dc: String,
        /// Default token list.
        tokens: String,
    },
}

/// Trait used by [`DnsSeedsProvider`] to look up a name. Tests
/// inject a deterministic implementation; the production binary
/// wires `tokio::net::lookup_host` plus a TXT lookup helper.
pub trait Resolver: Send + Sync {
    /// Resolve `name` and return the [`ResolvedSeeds`].
    fn resolve(&self, name: &str) -> Result<ResolvedSeeds, SeedsError>;
}

impl<T: Resolver + ?Sized> Resolver for Arc<T> {
    fn resolve(&self, name: &str) -> Result<ResolvedSeeds, SeedsError> {
        (**self).resolve(name)
    }
}

/// DNS-backed provider.
pub struct DnsSeedsProvider {
    name: String,
    resolver: Box<dyn Resolver>,
}

impl DnsSeedsProvider {
    /// Build a provider that queries `name` via `resolver`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::seeds::dns::{DnsSeedsProvider, Resolver, ResolvedSeeds};
    /// struct R;
    /// impl Resolver for R {
    ///     fn resolve(&self, _: &str)
    ///         -> Result<ResolvedSeeds, dynomite::seeds::SeedsError>
    ///     {
    ///         Ok(ResolvedSeeds::Txt(Vec::new()))
    ///     }
    /// }
    /// let p = DnsSeedsProvider::new("n".into(), Box::new(R));
    /// assert_eq!(p.name(), "n");
    /// ```
    #[must_use]
    pub fn new(name: String, resolver: Box<dyn Resolver>) -> Self {
        Self { name, resolver }
    }

    /// DNS query name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Debug for DnsSeedsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsSeedsProvider")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl SeedsProvider for DnsSeedsProvider {
    fn get_seeds(&self) -> Result<Vec<ConfDynSeed>, SeedsError> {
        let resolved = self.resolver.resolve(&self.name)?;
        match resolved {
            ResolvedSeeds::Txt(entries) => {
                let mut out = Vec::with_capacity(entries.len());
                for raw in entries {
                    let seed =
                        ConfDynSeed::parse(&raw).map_err(|e| SeedsError::Parse(e.to_string()))?;
                    out.push(seed);
                }
                Ok(out)
            }
            ResolvedSeeds::A {
                ips,
                port,
                rack,
                dc,
                tokens,
            } => {
                let mut out = Vec::with_capacity(ips.len());
                for ip in ips {
                    let raw = format!("{ip}:{port}:{rack}:{dc}:{tokens}");
                    let seed =
                        ConfDynSeed::parse(&raw).map_err(|e| SeedsError::Parse(e.to_string()))?;
                    out.push(seed);
                }
                Ok(out)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StaticResolver(ResolvedSeeds);
    impl Resolver for StaticResolver {
        fn resolve(&self, _: &str) -> Result<ResolvedSeeds, SeedsError> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn txt_branch() {
        let r = StaticResolver(ResolvedSeeds::Txt(vec![
            "127.0.0.1:8101:rA:dc1:1".into(),
            "127.0.0.2:8101:rA:dc1:2".into(),
        ]));
        let p = DnsSeedsProvider::new("n".into(), Box::new(r));
        let v = p.get_seeds().unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].host(), "127.0.0.1");
    }

    #[test]
    fn a_branch_synthesises_seed_format() {
        let r = StaticResolver(ResolvedSeeds::A {
            ips: vec!["10.0.0.1".into(), "10.0.0.2".into()],
            port: 8101,
            rack: "rA".into(),
            dc: "dc1".into(),
            tokens: "1".into(),
        });
        let p = DnsSeedsProvider::new("n".into(), Box::new(r));
        let v = p.get_seeds().unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].port(), 8101);
        assert_eq!(v[0].dc(), "dc1");
    }

    #[test]
    fn parse_error_propagates() {
        let r = StaticResolver(ResolvedSeeds::Txt(vec!["invalid-seed".into()]));
        let p = DnsSeedsProvider::new("n".into(), Box::new(r));
        assert!(matches!(p.get_seeds(), Err(SeedsError::Parse(_))));
    }
}
