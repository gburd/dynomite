//! RediSearch FT.* command surface for the
//! [Dynomite](dynomite) cluster engine.
//!
//! `dynomite-search` is the layered search surface that sits
//! on top of `dynomite-engine`. It owns:
//!
//! * the per-server [vector index registry](registry),
//! * the [schema types](schema) that compile FT.CREATE
//!   payloads into engine-level shapes,
//! * the [FT.* dispatch layer](ft) plus the
//!   [filter-expression grammar](ft_filter),
//! * the cluster-coordinated k-NN [broadcast FSM](query_fsm),
//! * the on-the-wire [codec](wire) the engine's DNODE plane
//!   uses to fan a query out to every primary peer.
//!
//! The crate is designed to be wired into a Dynomite
//! [`ServerBuilder`](dynomite::embed::ServerBuilder) via the
//! [`CommandExtension`](dynomite::embed::CommandExtension)
//! hook. The [`install`] helper does this in one call;
//! [`SearchExtension`] is the underlying impl for embedders
//! who want finer control.
//!
//! # Quickstart
//!
//! ```no_run
//! use dynomite::embed::ServerBuilder;
//! use dynomite::conf::DataStore;
//! # tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
//! let mut builder = ServerBuilder::new("dyn_o_mite")
//!     .listen("127.0.0.1:0".parse().unwrap())
//!     .dyn_listen("127.0.0.1:0".parse().unwrap())
//!     .data_store(DataStore::Redis)
//!     .servers(vec![dynomite::conf::ConfServer::parse("127.0.0.1:6379:1").unwrap()])
//!     .tokens_str("0");
//! let registry = dynomite_search::install(&mut builder);
//! let handle = builder.build().unwrap().start().await.unwrap();
//! let _ = registry; // hand off to admin tools, tests, ...
//! handle.shutdown().await.unwrap();
//! # });
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod ft;
pub mod ft_filter;
pub mod query_fsm;
pub mod registry;
pub mod schema;
pub mod wire;

use std::sync::Arc;

use dynomite::embed::{CommandExtension, HsetOutcome, ServerBuilder};
use dynomite::msg::MsgType;

pub use crate::registry::{
    RegistryError, TextFieldIndex, TextHit, TextRegexApproxResult, TextRegexResult, VectorRegistry,
    VectorTable, VectorTableInfo,
};
pub use crate::schema::{
    DistanceMetric, IndexAlgorithm, MetadataField, MetadataFieldType, VectorSchema, VectorType,
};

/// [`CommandExtension`] implementation that routes FT.*
/// commands and the HSET interception path through a shared
/// [`VectorRegistry`].
///
/// Every cloneable handle to a `SearchExtension` references
/// the same registry; embedders who want to inspect the live
/// FT.* surface (admin paths, tests) can clone the registry
/// out via [`SearchExtension::registry`].
#[derive(Clone, Debug)]
pub struct SearchExtension {
    registry: Arc<VectorRegistry>,
}

impl SearchExtension {
    /// Wrap an existing registry in a [`SearchExtension`].
    #[must_use]
    pub fn new(registry: Arc<VectorRegistry>) -> Self {
        Self { registry }
    }

    /// Borrow the wrapped registry.
    #[must_use]
    pub fn registry(&self) -> &Arc<VectorRegistry> {
        &self.registry
    }
}

impl Default for SearchExtension {
    fn default() -> Self {
        Self {
            registry: Arc::new(VectorRegistry::new()),
        }
    }
}

impl CommandExtension for SearchExtension {
    fn handles_msg_type(&self, ty: MsgType) -> bool {
        matches!(
            ty,
            MsgType::ReqRedisFtCreate
                | MsgType::ReqRedisFtSearch
                | MsgType::ReqRedisFtInfo
                | MsgType::ReqRedisFtList
                | MsgType::ReqRedisFtDropindex
                | MsgType::ReqRedisFtRegex
                | MsgType::ReqRedisFtUnknown
        )
    }

    fn try_dispatch(&self, args: &[&[u8]]) -> Option<Vec<u8>> {
        Some(crate::ft::dispatch(&self.registry, args))
    }

    fn try_intercept_hset(&self, args: &[&[u8]]) -> HsetOutcome {
        match crate::ft::maybe_index_hset(&self.registry, args) {
            Ok(Some(_)) => HsetOutcome::Absorbed,
            Ok(None) => HsetOutcome::NotIndexed,
            Err(e) => HsetOutcome::Error(format!("{e}")),
        }
    }
}

/// Wire the FT.* command surface into `builder` via the
/// [`CommandExtension`] hook. Returns an [`Arc`] handle to the
/// shared [`VectorRegistry`] so the caller can hold a cloneable
/// reference for admin paths / tests.
///
/// Equivalent to constructing a fresh [`SearchExtension`],
/// installing it on the builder, and returning the registry
/// handle:
///
/// ```no_run
/// use std::sync::Arc;
/// use dynomite::embed::ServerBuilder;
/// use dynomite_search::{SearchExtension, VectorRegistry};
/// let mut b = ServerBuilder::new("p");
/// let registry = Arc::new(VectorRegistry::new());
/// let ext = SearchExtension::new(registry.clone());
/// b = b.with_command_extension(Arc::new(ext));
/// ```
pub fn install(builder: &mut ServerBuilder) -> Arc<VectorRegistry> {
    let ext = SearchExtension::default();
    let registry = Arc::clone(ext.registry());
    builder.set_command_extension(Arc::new(ext));
    registry
}

/// Take a [`ServerBuilder`] by value, install the FT.*
/// extension, and return the wired builder plus the shared
/// registry. Useful when the caller prefers to own the
/// builder by value (the chained-call form):
///
/// ```no_run
/// use dynomite::embed::ServerBuilder;
/// let builder = ServerBuilder::new("p");
/// let (builder, registry) = dynomite_search::install_owned(builder);
/// let _ = (builder, registry);
/// ```
#[must_use]
pub fn install_owned(builder: ServerBuilder) -> (ServerBuilder, Arc<VectorRegistry>) {
    let ext = SearchExtension::default();
    let registry = Arc::clone(ext.registry());
    let builder = builder.with_command_extension(Arc::new(ext));
    (builder, registry)
}
