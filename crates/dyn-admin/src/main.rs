//! `dyn-admin` -- cluster admin CLI for the Dynomite Rust port.
//!
//! See the crate-level docs in `lib.rs` for an architectural
//! overview and `README.md` for usage examples.

use std::io::{self, Write};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use dyn_admin::commands::{cluster_list, metrics, ping, ring, stats, status};
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
/// in `docs/riak-compat-plan.md` Section 5; mutating commands
/// (`cluster-join`, `cluster-leave`, `cluster-plan`, `cluster-commit`)
/// are deferred and intentionally absent in v0.
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
    }
}
