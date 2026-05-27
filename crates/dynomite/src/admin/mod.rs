//! Operator-facing diagnostic surfaces.
//!
//! Today the only module here is [`cluster_info`]: a structured
//! plaintext dump of the local node's state intended for postmortems.

pub mod cluster_info;
