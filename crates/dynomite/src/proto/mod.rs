//! Wire-protocol codecs for Dynomite's external surfaces.
//!
//! This module exposes:
//!
//! * [`dnode`] - the DNODE peer-to-peer codec (Stage 7).
//! * [`memcache`] - the Memcached text-protocol parser, helpers,
//!   and repair stubs (Stage 8).
//! * [`redis`] - the Redis (RESP) parser, helpers, and repair
//!   plumbing (Stage 8).

pub mod dnode;
pub mod memcache;
pub mod redis;
