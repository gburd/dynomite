//! `dyn-admin` -- cluster admin CLI for the Dynomite Rust port.
//!
//! See the crate-level docs in `lib.rs` for an architectural
//! overview and `README.md` for usage examples.

use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use dyn_admin::commands::{
    aae_status, bucket_props, cluster_commit, cluster_info, cluster_join, cluster_leave,
    cluster_list, cluster_plan, distribution_dump, metrics, ping, ring, stats, status,
};
use dyn_admin::output::OutputFormat;

/// Default address operators expect when no `--node` flag is passed.
const DEFAULT_PBC_NODE: &str = "127.0.0.1:8087";

/// Default address for the stats / metrics HTTP endpoint.
const DEFAULT_STATS_NODE: &str = "127.0.0.1:22222";

/// Top-level CLI parser.
#[derive(Debug, Parser)]
#[command(
    name = "dyn-admin",
    bin_name = "dyn-admin",
    version,
    about = "Cluster admin CLI for the Dynomite Rust port (riak-admin equivalent)",
    long_about = None,
)]
struct Cli {
    /// Selected subcommand.
    #[command(subcommand)]
    command: Cmd,
}

/// Subcommand catalogue. Mirrors the `riak-admin` columns documented
/// in `docs/riak-compat-plan.md` Section 5. The mutating commands
/// (`cluster-join`, `cluster-leave`, `cluster-plan`,
/// `cluster-commit`) drive the cluster admin RPC family added in
/// the v0.0.4 dyn-admin slice.
#[derive(Debug, Subcommand)]
enum Cmd {
    /// Print the node identity (PBC) plus a stats summary (HTTP).
    Status {
        /// PBC `host:port`.
        #[arg(long = "node", default_value = DEFAULT_PBC_NODE)]
        node: String,
        /// Stats HTTP `host:port`.
        #[arg(long = "stats-node", default_value = DEFAULT_STATS_NODE)]
        stats_node: String,
        /// Skip the stats fetch.
        #[arg(long = "no-stats")]
        no_stats: bool,
        /// Emit JSON instead of human text.
        #[arg(long = "json")]
        json: bool,
    },
    /// Print the ring/topology view.
    RingStatus {
        /// PBC `host:port`.
        #[arg(long = "node", default_value = DEFAULT_PBC_NODE)]
        node: String,
        /// Stats HTTP `host:port`.
        #[arg(long = "stats-node", default_value = DEFAULT_STATS_NODE)]
        stats_node: String,
        /// Skip the stats fetch.
        #[arg(long = "no-stats")]
        no_stats: bool,
        /// Emit JSON instead of human text.
        #[arg(long = "json")]
        json: bool,
    },
    /// Pretty-print key metrics from the `/stats` endpoint.
    Stats {
        /// Stats HTTP `host:port`.
        #[arg(long = "node", default_value = DEFAULT_STATS_NODE)]
        node: String,
        /// Emit the raw JSON snapshot instead of the typed summary.
        #[arg(long = "json")]
        json: bool,
    },
    /// Print the Prometheus text endpoint verbatim.
    Metrics {
        /// Stats HTTP `host:port`.
        #[arg(long = "node", default_value = DEFAULT_STATS_NODE)]
        node: String,
    },
    /// Send a PBC ping and report RTT.
    Ping {
        /// PBC `host:port`.
        #[arg(long = "node", default_value = DEFAULT_PBC_NODE)]
        node: String,
        /// Emit JSON instead of human text.
        #[arg(long = "json")]
        json: bool,
    },
    /// List cluster peers reachable through the seed.
    ClusterList {
        /// PBC `host:port` of the seed node.
        #[arg(long = "seed", default_value = DEFAULT_PBC_NODE)]
        seed: String,
        /// Emit JSON instead of human text.
        #[arg(long = "json")]
        json: bool,
    },
    /// Stage a peer-join.
    ClusterJoin {
        /// Target peer to add, in `host:port` form.
        target: String,
        /// PBC `host:port` of the node to send the request to.
        #[arg(long = "node", default_value = DEFAULT_PBC_NODE)]
        node: String,
        /// Emit JSON instead of human text.
        #[arg(long = "json")]
        json: bool,
    },
    /// Stage a peer-leave.
    ClusterLeave {
        /// Index of the peer to remove.
        peer_idx: u32,
        /// PBC `host:port` of the node to send the request to.
        #[arg(long = "node", default_value = DEFAULT_PBC_NODE)]
        node: String,
        /// Emit JSON instead of human text.
        #[arg(long = "json")]
        json: bool,
    },
    /// Show staged-but-uncommitted cluster changes.
    ClusterPlan {
        /// PBC `host:port` of the node to send the request to.
        #[arg(long = "node", default_value = DEFAULT_PBC_NODE)]
        node: String,
        /// Emit JSON instead of human text.
        #[arg(long = "json")]
        json: bool,
    },
    /// Commit every staged cluster change.
    ClusterCommit {
        /// PBC `host:port` of the node to send the request to.
        #[arg(long = "node", default_value = DEFAULT_PBC_NODE)]
        node: String,
        /// Emit JSON instead of human text.
        #[arg(long = "json")]
        json: bool,
    },
    /// Pretty-print the configured distribution / shadow
    /// distribution and the cumulative shadow-disagreement
    /// counter for the targeted node.
    DistributionDump {
        /// Stats HTTP `host:port` of the node.
        #[arg(long = "node", default_value = DEFAULT_STATS_NODE)]
        node: String,
        /// Emit JSON instead of human text.
        #[arg(long = "json")]
        json: bool,
    },
    /// Inspect or update a bucket's `RpbBucketProps`.
    BucketProps {
        /// `get` or `set` subcommand.
        #[command(subcommand)]
        action: BucketPropsCmd,
    },
    /// Print a snapshot of the AAE worker's state on the
    /// targeted node.
    AaeStatus {
        /// PBC `host:port`.
        #[arg(long = "node", default_value = DEFAULT_PBC_NODE)]
        node: String,
        /// Emit JSON instead of human text.
        #[arg(long = "json")]
        json: bool,
    },
    /// Fetch the structured plaintext diagnostic dump.
    ///
    /// Mirrors `riak-admin cluster_info`. The node serves the
    /// dump on `GET /cluster-info.txt`; this subcommand writes
    /// the response to stdout, or to a file when `--output` is
    /// supplied.
    ClusterInfo {
        /// Stats HTTP `host:port` of the node.
        #[arg(long = "node", default_value = DEFAULT_STATS_NODE)]
        node: String,
        /// Optional output path. When unset, the dump is
        /// written to stdout.
        #[arg(long = "output", short = 'o')]
        output: Option<PathBuf>,
    },
}

/// `bucket-props` subcommand parser.
#[derive(Debug, Subcommand)]
enum BucketPropsCmd {
    /// Fetch and print the bucket's current properties.
    Get {
        /// Bucket name.
        bucket: String,
        /// PBC `host:port`.
        #[arg(long = "node", default_value = DEFAULT_PBC_NODE)]
        node: String,
        /// Emit JSON instead of human text.
        #[arg(long = "json")]
        json: bool,
    },
    /// Update one or more of the bucket's properties.
    ///
    /// Unspecified fields are read first and re-sent unchanged so a
    /// partial update never clobbers a property the operator did not
    /// name on the command line.
    Set {
        /// Bucket name.
        bucket: String,
        /// New replication factor.
        #[arg(long = "n-val")]
        n_val: Option<u32>,
        /// New default read consistency: `one`, `quorum`, `all`,
        /// `default`, or a literal integer.
        #[arg(long = "read-consistency")]
        read_consistency: Option<bucket_props::ConsistencyArg>,
        /// New default write consistency: same shape as
        /// `--read-consistency`.
        #[arg(long = "write-consistency")]
        write_consistency: Option<bucket_props::ConsistencyArg>,
        /// Hash-key function: `std` or `bucketonly`.
        #[arg(long = "keyfun")]
        keyfun: Option<bucket_props::KeyFunArg>,
        /// Replication strategy: `topology` or `successors`.
        #[arg(long = "replication-strategy")]
        replication_strategy: Option<bucket_props::ReplicationStrategyArg>,
        /// PBC `host:port`.
        #[arg(long = "node", default_value = DEFAULT_PBC_NODE)]
        node: String,
        /// Emit JSON instead of human text.
        #[arg(long = "json")]
        json: bool,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = writeln!(io::stderr(), "dyn-admin: tokio runtime: {e}");
            return ExitCode::from(2);
        }
    };
    let result = runtime.block_on(dispatch(cli));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            let _ = writeln!(io::stderr(), "dyn-admin: {e}");
            ExitCode::from(1)
        }
    }
}

async fn dispatch(cli: Cli) -> Result<(), dyn_admin::AdminError> {
    let mut stdout = io::stdout().lock();
    match cli.command {
        Cmd::Status {
            node,
            stats_node,
            no_stats,
            json,
        } => {
            let stats_addr = if no_stats {
                None
            } else {
                Some(stats_node.as_str())
            };
            status::run(
                &node,
                stats_addr,
                OutputFormat::from_flag(json),
                &mut stdout,
            )
            .await
        }
        Cmd::RingStatus {
            node,
            stats_node,
            no_stats,
            json,
        } => {
            let stats_addr = if no_stats {
                None
            } else {
                Some(stats_node.as_str())
            };
            ring::run(
                &node,
                stats_addr,
                OutputFormat::from_flag(json),
                &mut stdout,
            )
            .await
        }
        Cmd::Stats { node, json } => {
            stats::run(&node, OutputFormat::from_flag(json), &mut stdout).await
        }
        Cmd::Metrics { node } => metrics::run(&node, &mut stdout).await,
        Cmd::Ping { node, json } => {
            ping::run(&node, OutputFormat::from_flag(json), &mut stdout).await
        }
        Cmd::ClusterList { seed, json } => {
            cluster_list::run(&seed, OutputFormat::from_flag(json), &mut stdout).await
        }
        Cmd::ClusterJoin { target, node, json } => {
            cluster_join::run(&node, &target, OutputFormat::from_flag(json), &mut stdout).await
        }
        Cmd::ClusterLeave {
            peer_idx,
            node,
            json,
        } => cluster_leave::run(&node, peer_idx, OutputFormat::from_flag(json), &mut stdout).await,
        Cmd::ClusterPlan { node, json } => {
            cluster_plan::run(&node, OutputFormat::from_flag(json), &mut stdout).await
        }
        Cmd::ClusterCommit { node, json } => {
            cluster_commit::run(&node, OutputFormat::from_flag(json), &mut stdout).await
        }
        Cmd::DistributionDump { node, json } => {
            distribution_dump::run(&node, OutputFormat::from_flag(json), &mut stdout).await
        }
        Cmd::BucketProps { action } => dispatch_bucket_props(action, &mut stdout).await,
        Cmd::AaeStatus { node, json } => {
            aae_status::run(&node, OutputFormat::from_flag(json), &mut stdout).await
        }
        Cmd::ClusterInfo { node, output } => {
            cluster_info::run(&node, output.as_deref(), &mut stdout).await
        }
    }
}

/// Run a `bucket-props` subcommand. Pulled out of [`dispatch`] to
/// keep the top-level match arm short.
async fn dispatch_bucket_props(
    action: BucketPropsCmd,
    stdout: &mut impl Write,
) -> Result<(), dyn_admin::AdminError> {
    match action {
        BucketPropsCmd::Get { bucket, node, json } => {
            bucket_props::run_get(&node, &bucket, OutputFormat::from_flag(json), stdout).await
        }
        BucketPropsCmd::Set {
            bucket,
            n_val,
            read_consistency,
            write_consistency,
            keyfun,
            replication_strategy,
            node,
            json,
        } => {
            let opts = bucket_props::SetOptions {
                n_val,
                read: read_consistency,
                write: write_consistency,
                keyfun,
                replication_strategy,
            };
            bucket_props::run_set(&node, &bucket, &opts, OutputFormat::from_flag(json), stdout)
                .await
        }
    }
}
