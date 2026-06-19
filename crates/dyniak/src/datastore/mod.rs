//! Storage bridges for the Riak protocol layer.
//!
//! Today this module hosts one bridge: [`NoxuDatastore`], gated behind
//! the `noxu` Cargo feature, which wires the Dynomite engine to the
//! in-process Noxu DB storage engine. The bridge satisfies
//! [`dynomite::embed::Datastore`], so an embedder can drop a
//! `NoxuDatastore` into [`dynomite::embed::Server`]. A richer
//! Riak-aware K/V trait spanning the protocol layer end-to-end is
//! not yet defined; the protocol layer uses the `embed::Datastore`
//! surface directly.

#[cfg(feature = "noxu")]
pub mod noxu;

// Cross-node X/Open XA two-phase commit. The local single-process
// coordinator lives in `xa`; the network leg (transport seam, remote
// branches, receiver-side peer handler, durable in-doubt log, and the
// cross-node async coordinator) lives in `xa_net`, with its wire codec
// in `xa_wire`.
#[cfg(feature = "noxu")]
pub mod xa;

#[cfg(feature = "noxu")]
pub mod xa_net;

#[cfg(feature = "noxu")]
pub mod xa_wire;

#[cfg(feature = "noxu")]
pub use crate::datastore::noxu::{NoxuDatastore, NoxuDatastoreError};

#[cfg(feature = "noxu")]
pub use crate::datastore::xa::{XaCoordinator, XaParticipant};

#[cfg(feature = "noxu")]
pub use crate::datastore::xa_net::{
    serve_xa_peer, CrossNodeCoordinator, DnodeXaTransport, InDoubtLog, RemoteXaBranch, XaBranch,
    XaPeer, XaTransport, XaTransportError,
};
