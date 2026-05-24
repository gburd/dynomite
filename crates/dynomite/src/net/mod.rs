//! Connection state machines and transport listeners.
//!
//! This module wires together the I/O substrate from
//! [`crate::io`], the message model from [`crate::msg`], and the
//! protocol codecs in [`crate::proto`] into a runtime that:
//!
//! * accepts client connections through a [`proxy::Proxy`] listener,
//! * accepts inbound peer connections through a
//!   [`dnode_proxy::DnodeProxy`] listener,
//! * fans out to backend datastores through a per-pool
//!   [`pool::ConnPool`] of outbound [`server::ServerConn`]s,
//! * fans out to peer nodes through a
//!   [`dnode_server::DnodeServerConn`] outbound connection,
//! * tracks consecutive-failure auto-eject decisions via
//!   [`auto_eject::AutoEject`].
//!
//! Every connection holds a boxed
//! [`Transport`](crate::io::reactor::Transport) so the same FSM
//! drives TCP today and QUIC behind the `quic` cargo feature.
//!
//! Cluster routing decisions (request forwarding across racks /
//! DCs, peer selection, gossip) live in Stage 10's `cluster::*`
//! module and are dispatched into via the [`Dispatcher`] trait
//! below.
//!
//! # Examples
//!
//! ```no_run
//! use dynomite::net::{ConnPool, ConnPoolConfig};
//! # use dynomite::io::reactor::ConnRole;
//! # use dynomite::io::reactor::TcpTransport;
//! # use std::time::Duration;
//! # async fn demo() -> std::io::Result<()> {
//! let cfg = ConnPoolConfig {
//!     max_connections: 4,
//!     server_failure_limit: 3,
//!     server_retry_timeout_ms: 1_000,
//!     auto_eject: true,
//! };
//! let pool: ConnPool<TcpTransport> = ConnPool::new(cfg);
//! assert_eq!(pool.config().max_connections, 4);
//! # Ok(())
//! # }
//! ```

pub mod auto_eject;
pub mod client;
pub mod conn;
pub mod dispatcher;
pub mod dnode_client;
pub mod dnode_proxy;
pub mod dnode_server;
pub mod listener;
pub mod pool;
pub mod proxy;
pub mod server;
pub mod tls;

#[cfg(feature = "quic")]
pub mod quic;

use std::io;

use thiserror::Error;

pub use self::auto_eject::{AutoEject, AutoEjectState};
pub use self::client::{ClientHandler, ClientLoopOutcome};
pub use self::conn::{Conn, ConnHandle, ConnStats};
pub use self::dispatcher::{
    DispatchOutcome, Dispatcher, NoopDispatcher, OutboundEnvelope, ServerSink,
};
pub use self::dnode_client::DnodeClientHandler;
pub use self::dnode_proxy::DnodeProxy;
pub use self::dnode_server::DnodeServerConn;
pub use self::listener::{bind_dual_stack, BindOptions};
pub use self::pool::{ConnFactory, ConnHandle as PoolHandle, ConnPool, ConnPoolConfig};
pub use self::proxy::Proxy;
pub use self::server::ServerConn;
pub use self::tls::{
    acceptor_from, connector_from, load_client_config, load_server_config, server_name_owned,
    TlsClientTransport, TlsError, TlsServerTransport,
};

/// Top-level error type returned by net::* operations.
#[derive(Debug, Error)]
pub enum NetError {
    /// I/O error from the transport layer.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// Protocol parsing error reported by a datastore codec.
    #[error("protocol parse error: {0}")]
    Parse(String),
    /// DNODE codec error.
    #[error("dnode error: {0}")]
    Dnode(String),
    /// The pool exhausted its retry budget for a server.
    #[error("server has been auto-ejected")]
    Ejected,
    /// The pool reached its maximum connection count and there is
    /// no idle handle to return.
    #[error("connection pool exhausted")]
    PoolExhausted,
    /// The pool was shut down while a caller was waiting for a
    /// handle.
    #[error("connection pool shut down")]
    PoolShutdown,
    /// Connection closed while operation was in flight.
    #[error("connection closed")]
    Closed,
    /// TLS-handshake or load-time error.
    #[error("tls error: {0}")]
    Tls(String),
}

impl From<crate::net::tls::TlsError> for NetError {
    fn from(value: crate::net::tls::TlsError) -> Self {
        NetError::Tls(value.to_string())
    }
}

impl From<crate::proto::dnode::DnodeError> for NetError {
    fn from(value: crate::proto::dnode::DnodeError) -> Self {
        NetError::Dnode(format!("{value:?}"))
    }
}
