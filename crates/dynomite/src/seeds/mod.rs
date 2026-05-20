//! Pluggable seeds providers.
//!
//! A seeds provider hands the gossip task an up-to-date list of
//! peers in the canonical
//! `host:port:rack:dc:tokens|host:port:rack:dc:tokens` format.
//! Three implementations ship with the engine:
//!
//! * [`simple::SimpleSeedsProvider`] - returns the seeds parsed
//!   from the YAML config.
//! * [`dns::DnsSeedsProvider`] - resolves a configured DNS
//!   hostname to a list of IPs (mirroring the reference
//!   `dns_get_seeds`, with the resolver factored behind a trait so
//!   the unit test can substitute a deterministic implementation).
//! * [`florida::FloridaSeedsProvider`] - HTTP GET to a Florida
//!   service, parses the body. Hand-rolled HTTP/1.0 client over
//!   `tokio::net::TcpStream` to stay within the locked dependency
//!   set.
//!
//! The trait shape is the seam Stage 13 will expose through the
//! embedding API; this stage locks the surface so the embed
//! wrapper only needs to forward.
//!
//! # Examples
//!
//! ```
//! use dynomite::seeds::{SeedsProvider, simple::SimpleSeedsProvider};
//! use dynomite::conf::ConfDynSeed;
//! let seeds = vec![ConfDynSeed::parse("h1:8101:rA:dc1:1").unwrap()];
//! let p = SimpleSeedsProvider::new(seeds);
//! let got = p.get_seeds().unwrap();
//! assert_eq!(got.len(), 1);
//! ```

pub mod dns;
pub mod florida;
pub mod simple;

use std::io;

use thiserror::Error;

use crate::conf::ConfDynSeed;

/// Error type for seeds providers.
#[derive(Debug, Error)]
pub enum SeedsError {
    /// The provider has no fresh data: the gossip task should
    /// retry on the next interval. Mirrors the reference engine's
    /// `DN_NOOPS` return.
    #[error("no fresh seeds")]
    NoFreshSeeds,
    /// I/O error.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// Parse error.
    #[error("parse error: {0}")]
    Parse(String),
    /// Endpoint returned an HTTP error.
    #[error("http error: {0}")]
    Http(String),
}

/// Pluggable seeds provider.
///
/// Implementations may block; the gossip task calls them from a
/// dedicated tokio task with timeouts wrapped at the call site.
/// `SeedsProvider` is an async trait emulated via an associated
/// future type so it can be implemented for both blocking
/// (`SimpleSeedsProvider`) and async (`FloridaSeedsProvider`)
/// shapes.
pub trait SeedsProvider: Send + Sync {
    /// Return the current list of seeds, or an error explaining
    /// why no fresh data is available.
    ///
    /// Blocking implementations do their work synchronously and
    /// return immediately; async implementations should run on a
    /// blocking task spawned by the caller. Stage 12 binary
    /// wiring picks the right runtime path; the trait stays sync
    /// to keep the surface small for embedders.
    fn get_seeds(&self) -> Result<Vec<ConfDynSeed>, SeedsError>;
}

/// Marker trait used by Stage 13 to register custom seeds
/// providers through the embedding API. Implementing
/// [`SeedsProvider`] is sufficient.
impl<T> SeedsProvider for std::sync::Arc<T>
where
    T: SeedsProvider + ?Sized,
{
    fn get_seeds(&self) -> Result<Vec<ConfDynSeed>, SeedsError> {
        (**self).get_seeds()
    }
}
