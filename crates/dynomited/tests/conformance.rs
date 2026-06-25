//! Integration test crate entry for the conformance suite.
//!
//! The harness layout is:
//!
//! * `conformance/mod.rs` - shared cluster spawner, redis
//!   backend spawner, and RESP client (loaded as a `helpers`
//!   submodule via `#[path]`).
//! * `conformance/single_node.rs` - 1-node cluster scenarios.
//! * `conformance/three_node_single_dc.rs` - 3-node single-DC
//!   cluster + gossip + per-node routing parity.
//! * `conformance/multi_dc.rs` - 2 DCs * 2 racks * 2 nodes;
//!   exercises every consistency level.
//! * `conformance/quic_transport.rs` - QUIC peer transport
//!   smoke (gated on `feature = "quic"`).
//! * `conformance/python_harness.rs` - Rust adaptation of the
//!   functional test scenarios from the upstream Netflix
//!   Dynomite Python test suite.
//!
//! All scenarios are gated behind `feature = "integration"` and
//! a runtime check for `valkey-server` on `PATH`. When Redis is
//! missing the tests print a skip notice and exit successfully
//! rather than failing.
//!
//! # Running
//!
//! ```bash
//! cargo nextest run -p dynomited --features integration
//! cargo nextest run -p dynomited --features integration,quic
//! cargo nextest run --profile conformance --features integration
//! ```
//!
//! The `conformance` nextest profile (`.config/nextest.toml`)
//! emits JUnit XML at `target/nextest/conformance/junit.xml`;
//! `scripts/check.sh` mirrors that file to
//! `target/junit/conformance.xml` for CI consumption.

#![cfg(feature = "integration")]
#![allow(missing_docs)]

#[path = "conformance/mod.rs"]
pub mod helpers;

#[path = "conformance/single_node.rs"]
mod single_node;

#[path = "conformance/three_node_single_dc.rs"]
mod three_node_single_dc;

#[path = "conformance/multi_dc.rs"]
mod multi_dc;

#[cfg(feature = "quic")]
#[path = "conformance/quic_transport.rs"]
mod quic_transport;

#[cfg(feature = "quic")]
#[path = "conformance/quic_three_node.rs"]
mod quic_three_node;

#[path = "conformance/python_harness.rs"]
mod python_harness;
