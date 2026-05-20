//! Wire-protocol codecs for Dynomite's external surfaces.
//!
//! This module currently houses the DNODE peer-to-peer codec
//! ([`dnode`]). Stage 8 will add the Redis and Memcached protocol
//! parsers as `proto::redis` and `proto::memcache`.

pub mod dnode;
