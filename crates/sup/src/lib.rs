//! OTP-style supervisor tree for tokio.
//!
//! A [`Supervisor`] owns a set of supervised tasks (its *children*),
//! watches each one for panics, errors, or normal exit, and restarts
//! them according to a configured [`RestartStrategy`] and the per-child
//! [`RestartPolicy`]. The design mirrors the supervisor behaviour
//! shipped with Erlang/OTP: a single point in the process tree whose
//! only job is to keep its children alive (or to take them down
//! together when one fails badly).
//!
//! # When to reach for a supervisor
//!
//! Use a supervisor whenever you have a long-running tokio task whose
//! liveness matters and whose death you want to *notice*. The class of
//! bug a supervisor catches is "a peer-supervisor task panics inside
//! `tokio::spawn` and dies silently because no one ever joined the
//! handle". With a supervisor, the panic is captured, logged, and
//! either acted upon (by restarting) or propagated to the supervisor's
//! own owner.
//!
//! # The four strategies
//!
//! * [`RestartStrategy::OneForOne`] - if a child dies, restart only
//!   that child. The other children are unaffected. Use when children
//!   are independent.
//! * [`RestartStrategy::OneForAll`] - if any child dies, terminate all
//!   children and restart the whole set. Use when the children share
//!   in-memory state that becomes inconsistent if any one of them
//!   restarts on its own.
//! * [`RestartStrategy::RestForOne`] - if a child dies, terminate it
//!   and every child registered after it (in `add_child` order), then
//!   restart that suffix. Use when children form a startup chain where
//!   each one depends on its predecessor.
//! * [`RestartStrategy::SimpleOneForOne`] - dynamic-children mode:
//!   children are added at runtime (and may exit at runtime), each
//!   restart is independent. The same restart semantics as
//!   `OneForOne`, with a hard cap on the number of children.
//!
//! # The three policies
//!
//! [`RestartPolicy::Permanent`] children are always restarted. This is
//! the default for "this task is the system; if it stops, restart
//! it". [`RestartPolicy::Transient`] children are restarted only if
//! they exit abnormally (panic or `Err`); a normal `Ok` exit removes
//! them. [`RestartPolicy::Temporary`] children are never restarted;
//! when they exit, for any reason, they are removed.
//!
//! # Backoff
//!
//! Restarts are delayed by an exponential backoff with jitter, defined
//! per-child by [`BackoffSpec`]. The backoff resets to the start delay
//! after a successful run (a child that completed `Ok`, or for
//! permanent children a child that ran for at least the start delay
//! without failing). This matches OTP's `intensity`/`period` ideas,
//! restated in a form more familiar to network-service programmers.
//!
//! # Example
//!
//! ```no_run
//! use std::time::Duration;
//! use sup::{
//!     BackoffSpec, ChildSpec, RestartPolicy, RestartStrategy, SupError, Supervised,
//!     Supervisor,
//! };
//!
//! struct Worker {
//!     name: String,
//! }
//!
//! impl Supervised for Worker {
//!     type Output = ();
//!     fn name(&self) -> &str {
//!         &self.name
//!     }
//!     async fn run(&mut self) -> Result<Self::Output, SupError> {
//!         // ... do real work ...
//!         Ok(())
//!     }
//! }
//!
//! # async fn entry() {
//! let mut sup = Supervisor::new(RestartStrategy::OneForOne);
//! sup.add_child(ChildSpec {
//!     spec: Worker { name: "ingester".into() },
//!     restart: RestartPolicy::Permanent,
//!     backoff: BackoffSpec::default(),
//! });
//! let handle = sup.handle();
//! let runner = tokio::spawn(sup.run());
//! // ... later ...
//! handle.shutdown().await.unwrap();
//! let _exit = runner.await.unwrap();
//! # }
//! ```
//!
//! # Tracing
//!
//! The supervisor emits structured `tracing` events on every child
//! transition. Subscribe to the `sup` target to see the start, stop,
//! restart, and backoff decisions; the events carry the child name,
//! the [`ChildId`], and the exit classification.

#![doc(html_root_url = "https://docs.rs/sup/0.0.1")]
#![forbid(unsafe_code)]

mod backoff;
mod error;
mod supervisor;
mod types;

pub use backoff::BackoffSpec;
pub use error::SupError;
pub use supervisor::{Supervisor, SupervisorHandle};
pub use types::{
    ChildExit, ChildId, ChildSpec, RestartPolicy, RestartStrategy, SupExit, Supervised,
};
