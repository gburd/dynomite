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

pub mod gossip;
pub mod quorum;
pub mod ring;
pub mod xa;
