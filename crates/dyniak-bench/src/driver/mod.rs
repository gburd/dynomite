//! Driver trait and implementations.

pub mod redis;
#[cfg(feature = "riak")]
pub mod riak;
#[cfg(feature = "http")]
pub mod riak_http;

use std::fmt;

use rand::rngs::SmallRng;

use crate::config::{DriverConfig, DriverKind};
use crate::error::BenchError;
use crate::keygen::KeyGen;
use crate::valgen::ValGen;

/// Outcome of a single op invocation.
#[derive(Debug)]
pub enum DriverOutcome {
    /// Op succeeded; the latency was already captured by the
    /// caller.
    Ok,
    /// Op failed; the inner string carries the original error message
    /// (for classification + stderr logging).
    Err(String),
}

impl fmt::Display for DriverOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => f.write_str("ok"),
            Self::Err(e) => write!(f, "err({e})"),
        }
    }
}

/// A driver runs a single named operation against a backend.
///
/// Drivers are owned by a worker task; they need not be `Sync`.
/// They are constructed via [`make_driver`] which dispatches on
/// the configured driver kind.
pub trait Driver: Send {
    /// List of op names this driver supports. Used to validate the
    /// `[ops]` table at startup.
    fn supported_ops(&self) -> &'static [&'static str];

    /// Execute one named op. Returns [`DriverOutcome::Ok`] on
    /// success, or [`DriverOutcome::Err`] with the raw error
    /// message on failure.
    fn run(
        &mut self,
        op: &str,
        keygen: &mut KeyGen,
        valgen: &ValGen,
        rng: &mut SmallRng,
    ) -> DriverOutcome;
}

/// Build a driver from a [`DriverConfig`]. Each call constructs a
/// fresh driver instance owning its own connection state.
pub fn make_driver(cfg: &DriverConfig) -> Result<Box<dyn Driver>, BenchError> {
    match cfg.kind {
        DriverKind::Redis => Ok(Box::new(redis::RedisDriver::new(cfg)?)),
        DriverKind::RiakPbc => {
            #[cfg(feature = "riak")]
            {
                Ok(Box::new(riak::RiakPbcDriver::new(cfg)?))
            }
            #[cfg(not(feature = "riak"))]
            Err(BenchError::Config(
                "riak_pbc driver requires the `riak` feature; rebuild with --features riak".into(),
            ))
        }
        DriverKind::RiakHttp => {
            #[cfg(feature = "http")]
            {
                Ok(Box::new(riak_http::RiakHttpDriver::new(cfg)?))
            }
            #[cfg(not(feature = "http"))]
            Err(BenchError::Config(
                "riak_http driver requires the `http` feature; rebuild with --features http".into(),
            ))
        }
    }
}
