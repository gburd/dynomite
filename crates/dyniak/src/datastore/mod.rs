//! Storage bridges for the Riak protocol layer.
//!
//! Today this module hosts one bridge: [`NoxuDatastore`], gated behind
//! the `noxu` Cargo feature, which wires the Dynomite engine to the
//! in-process Noxu DB storage engine. The follow-up slice will add a
//! richer Riak-aware K/V trait that the protocol layer uses end-to-
//! end; for now the bridge satisfies
//! [`dynomite::embed::Datastore`] so an embedder can drop a
//! `NoxuDatastore` into [`dynomite::embed::Server`] today.

#[cfg(feature = "noxu")]
pub mod noxu;

// Cross-node X/Open XA two-phase commit. Local-only realisation
// today: the coordinator and its participants all live in one
// process (see the module docs for the multi-node boundary).
#[cfg(feature = "noxu")]
pub mod xa;

#[cfg(feature = "noxu")]
pub use crate::datastore::noxu::{NoxuDatastore, NoxuDatastoreError};

#[cfg(feature = "noxu")]
pub use crate::datastore::xa::{XaCoordinator, XaParticipant};
