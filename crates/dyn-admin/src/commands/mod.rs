//! Subcommand implementations for the `dyn-admin` CLI.
//!
//! Each module owns one subcommand. All of them return
//! [`crate::AdminError`] so a single match in `main` renders failures
//! uniformly.

pub mod aae_status;
pub mod bucket_props;
pub mod cluster_commit;
pub mod cluster_join;
pub mod cluster_leave;
pub mod cluster_list;
pub mod cluster_plan;
pub mod distribution_dump;
pub mod metrics;
pub mod ping;
pub mod ring;
pub mod stats;
pub mod status;
