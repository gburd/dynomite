//! Library facade for the `dynomited` server binary.
//!
//! The binary itself lives in `src/main.rs`; everything that wants a
//! doctest or an integration test ports here so the surface is
//! reachable without spawning a process.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod asciilogo;
pub mod cli;
