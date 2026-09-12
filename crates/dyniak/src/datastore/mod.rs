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

/// True when a completed [`dynomite::embed::Datastore::riak_put`] on
/// `datastore` is known to have reached durable (synced / committed)
/// storage.
///
/// [`NoxuDatastore`] can make an exact call here (probed via
/// [`dynomite::embed::Datastore::as_any`], the seam the HTTP layer
/// already uses to reach the transactional store): its committed
/// auto-commit writes are durable exactly when the environment's sync
/// policy says so (see [`NoxuDatastore::commits_durably`]). For every
/// other backend -- a custom [`dynomite::embed::Datastore`] impl, or
/// any backend when this crate is built without the `noxu` feature --
/// there is no durability signal to inspect. The honest default in
/// that case is `true`, not `false`: a [`dynomite::embed::Datastore`]
/// implementation contracts that `riak_put` either fully applies the
/// write or returns an `Err`, so a successful return is the same
/// signal a `commit()` returning `Ok` would be; treating "cannot
/// inspect" as "assume it did not durably commit" would make the
/// durable-write quorum DW (whose bucket default is `quorum`, exactly
/// like R/W, unlike PR/PW's `0`) spuriously fail every write against
/// every backend this crate does not special-case, which is not an
/// honest counting path either -- it is a different kind of
/// dishonesty. Shared by the DW counting on the coordinator side
/// ([`crate::server`]) and the ack-byte choice on the replica-apply
/// side ([`crate::replica_apply::ReplicaApplier`]).
#[cfg(feature = "noxu")]
#[must_use]
pub fn write_is_durable(datastore: &dyn dynomite::embed::Datastore) -> bool {
    datastore
        .as_any()
        .and_then(|any| any.downcast_ref::<NoxuDatastore>())
        .is_none_or(NoxuDatastore::commits_durably)
}

/// See the `noxu`-feature [`write_is_durable`]: without the `noxu`
/// feature this crate has no backend that can override the default,
/// so every successful write counts as durable.
#[cfg(not(feature = "noxu"))]
#[must_use]
pub fn write_is_durable(_datastore: &dyn dynomite::embed::Datastore) -> bool {
    true
}
