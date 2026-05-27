//! Operator-facing diagnostic surfaces.
//!
//! Today the only module here is [`cluster_info`]: a structured
//! plaintext dump of the local node's state intended for postmortems.
//! It is the spiritual port of `riak_kv`'s
//! `cluster_info:dump_local_node` helper.

pub mod cluster_info;
