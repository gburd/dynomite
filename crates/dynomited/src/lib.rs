//! Library facade for the `dynomited` server binary.
//!
//! The binary itself lives in `src/main.rs`; everything that wants a
//! doctest or an integration test ports here so the surface is
//! reachable without spawning a process.
//!
//! `unsafe_code` is denied (not forbidden) at this crate level so the
//! [`daemonize`] module can isolate the two `unsafe { fork() }` calls
//! the reference engine's `dn_daemonize` requires. Every other module
//! is `#[deny(unsafe_code)]` by default; the allowance is local to
//! `daemonize.rs` and documented in `docs/journal/allowances.md`.

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod asciilogo;
pub mod cli;
pub mod daemonize;
pub mod observability;
pub mod pidfile;
pub mod server;
pub mod signals;
