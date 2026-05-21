//! Public embedding API for the Dynomite engine.
//!
//! `dynomite::embed` is the surface an embedding program uses to
//! construct, run, observe, and shut down a Dynomite engine
//! in-process. The same surface drives the [`dynomited`] binary;
//! the binary is a thin wrapper around this module rather than a
//! parallel code path.
//!
//! # Two-tier API
//!
//! 1. [`Server::builder`] returns a [`ServerBuilder`] - a typed,
//!    fluent, validated-at-`build` builder that mirrors every
//!    YAML-visible [`crate::conf::ConfPool`] field plus typed
//!    setters for hook traits with no YAML form.
//! 2. [`Server::start`] returns immediately; the tokio runtime
//!    owns the background work. The caller drives the running
//!    server through a [`ServerHandle`].
//!
//! # Hooks
//!
//! Five public traits expose every cross-cutting concern an
//! embedder may want to override:
//!
//! * [`hooks::Datastore`] - backing store (Redis / Memcache /
//!   custom).
//! * [`hooks::SeedsProvider`] - service discovery integration.
//! * [`hooks::CryptoProvider`] - HSM / KMS integration.
//! * [`hooks::MetricsSink`] - exporter integration.
//! * [`crate::io::reactor::Transport`] - already exposed by
//!   `io::reactor`; re-exported here as
//!   [`Transport`](crate::embed::Transport) for discoverability.
//!
//! Every trait has at least one default implementation shipped
//! in-crate, and the crate-root re-exports below mean a typical
//! embedder writes
//! `use dynomite::embed::{Server, ServerBuilder, ServerHandle};`
//! without reaching into the submodules.
//!
//! # Examples
//!
//! ```no_run
//! use dynomite::embed::{Server, ServerBuilder};
//! use dynomite::conf::DataStore;
//! # tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
//! let server: Server = ServerBuilder::new("dyn_o_mite")
//!     .listen("127.0.0.1:0".parse().unwrap())
//!     .dyn_listen("127.0.0.1:0".parse().unwrap())
//!     .data_store(DataStore::Redis)
//!     .servers(vec![dynomite::conf::ConfServer::parse("127.0.0.1:6379:1").unwrap()])
//!     .tokens_str("0")
//!     .build()
//!     .unwrap();
//! let handle = server.start().await.unwrap();
//! assert_eq!(handle.peers().len(), 1);
//! handle.shutdown().await.unwrap();
//! # });
//! ```
//!
//! [`dynomited`]: ../../dynomited/index.html

pub mod builder;
pub mod error;
pub mod events;
pub mod hooks;
pub mod server;
pub mod snapshots;

pub use crate::io::reactor::{ConnRole, TcpTransport, Transport};

pub use self::builder::ServerBuilder;
pub use self::error::EmbedError;
pub use self::events::{
    CloseReason, ConnId, ConnRoleTag, EventStream, PeerDownReason, PeerId, ServerEvent,
};
pub use self::hooks::{
    BoxFuture, CryptoProvider, CryptoProviderError, Datastore, DatastoreError, DnsSeedsProvider,
    FloridaSeedsProvider, LegacySeedsAdapter, LoggingMetricsSink, MemcacheDatastore,
    MemoryDatastore, MetricsError, MetricsSink, Protocol, RedisDatastore, RustCryptoProvider,
    SeedsProvider, SimpleSeedsProvider,
};
pub use self::server::{Server, ServerHandle};
pub use self::snapshots::{DatacenterSnapshot, PeerSnapshot, RackSnapshot, RingSnapshot};

impl Server {
    /// Convenience entry point.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::Server;
    /// let _b = Server::builder("dyn_o_mite");
    /// ```
    #[must_use]
    pub fn builder(pool_name: impl Into<String>) -> ServerBuilder {
        ServerBuilder::new(pool_name)
    }
}
