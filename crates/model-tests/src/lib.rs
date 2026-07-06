//! Stateright explicit-state model checks for the Dynomite distributed
//! protocols.
//!
//! Each module here is an *abstract* model: a small state machine that
//! reproduces the decision logic of a production protocol and asserts the
//! safety and liveness invariants the real code must satisfy. The models
//! do not link the production crates; they re-express the protocol so the
//! checker's reachable state space stays small enough for an exhaustive
//! bounded search.
//!
//! The modules and the production code they model:
//!
//! * [`xa`] models the cross-node XA two-phase commit implemented in
//!   `crates/dyniak/src/datastore/xa.rs` and
//!   `crates/dyniak/src/datastore/xa_net.rs` (presumed abort,
//!   forward-commit recovery, durable in-doubt log, idempotent
//!   commit/rollback, cold-restart recovery scan).
//! * [`ramp`] models RAMP-Fast read-atomic multi-partition
//!   transactions implemented in `crates/dyniak/src/ramp.rs`
//!   (`select`, the read-atomic decision core) and
//!   `crates/dyniak/src/ramp_store.rs` (`RampCoordinator` write/read
//!   rounds), asserting no fractured read and non-blocking reads.
//! * [`quorum`] models the per-DC quorum decision implemented in
//!   `crates/dynomite/src/msg/response_mgr.rs` and the consistency
//!   fan-out in `crates/dynomite/src/cluster/dispatch.rs` /
//!   `pool.rs`.
//! * [`ring`] models the token-ring routing implemented in
//!   `crates/dynomite/src/cluster/vnode.rs` (primary + successor
//!   preference list, determinism, coverage, bounded disruption on a
//!   single membership change).
//! * [`gossip`] models the peer-state dissemination implemented in
//!   `crates/dynomite/src/cluster/gossip.rs`
//!   (`GossipState::add_or_update`, last-writer-wins on the per-token
//!   timestamp), asserting eventual convergence and a stable fixpoint.
//!   the checker catches.
//! * [`swim`] models the opt-in SWIM + Lifeguard membership /
//!   failure detector implemented in
//!   `crates/dynomite/src/cluster/swim.rs` (`SwimState`), asserting
//!   completeness (a dead node is confirmed by all), accuracy / low
//!   false-positive (a slow-but-alive node is never permanently
//!   confirmed dead, and SWIM beats a naive fixed-timeout detector),
//!   dissemination (infection-style convergence with refutation), and
//!   a negative control (disabling refutation reproduces a false
//!   permanent death).
//! * [`delta_crdt`] models the delta-state CRDT convergence
//!   implemented in `crates/dyniak/src/datatypes/delta_set.rs`
//!   (the `DeltaOrSet` type) and the delta-shipping AAE hook in
//!   `crates/dyniak/src/aae/delta_ship.rs`, asserting strong
//!   eventual consistency over a lossy/reordering/duplicating delta
//!   channel plus the lattice laws, with a non-join-irreducible
//!   mutator as the negative control.
//! * [`aae`] models the divergence-proportional anti-entropy
//!   reconcile implemented in
//!   `crates/dyniak/src/aae/mst_reconcile.rs` (`reconcile_pull`) over
//!   the Merkle Search Tree diff in `crates/dyn-hashtree/src/mst.rs`
//!   (the `Mst` diff walk), asserting replica convergence, the
//!   divergence-proportional diff bound, and a negative control that
//!   the checker catches.
//! * [`hlc`] models the Hybrid Logical Clock implemented in
//!   `crates/dyniak/src/datatypes/hlc.rs` (the `Hlc` type),
//!   asserting monotonicity, causality capture over program-order and
//!   send/receive happens-before edges, and bounded drift within the
//!   max inter-node physical skew, with a broken receive rule (no
//!   counter advance) as the negative control.
//!
//! # Running
//!
//! The checks are ordinary `#[test]`s, gated out of the default fast
//! test pass by living in a separate, non-default workspace member.
//! Run them with:
//!
//! ```sh
//! cargo test -p model-tests
//! ```
//!
//! or via `scripts/model.sh`, which bounds the search depth for CI and
//! documents a deeper soak invocation.

#![forbid(unsafe_code)]

pub mod aae;
pub mod delta_crdt;
pub mod gossip;
pub mod hlc;
pub mod quorum;
pub mod ramp;
pub mod ring;
pub mod swim;
pub mod xa;
