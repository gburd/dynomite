//! Wire-protocol codecs for Dynomite's external surfaces.
//!
//! This module exposes:
//!
//! * [`dnode`] - the DNODE peer-to-peer codec.
//! * [`memcache`] - the Memcached text-protocol parser, helpers,
//!   and repair surface.
//! * [`redis`] - the RESP parser, helpers, and repair
//!   plumbing.

pub mod dnode;
pub mod memcache;
pub mod redis;
