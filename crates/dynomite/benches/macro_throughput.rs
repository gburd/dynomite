//! Macro (end-to-end) throughput bench scaffold.
//!
//! Gated behind `--features bench-macro` because the live network
//! impairment matrix (`tc qdisc add dev lo root netem ...`) needs
//! `CAP_NET_ADMIN`. Out-of-the-box this binary compiles to a
//! diagnostic stub that prints how to enable the macro harness;
//! the operator-run version under `bench-macro` spawns a 3-node
//! local cluster, drives a redis-benchmark style workload for 30s
//! per condition, and writes the per-condition JSON snapshot to
//! `target/bench/macro-<git-sha>.json`.
//!
//! See `docs/book/src/operations/benchmarks.md` for the full setup.

#![allow(missing_docs)]

#[cfg(not(feature = "bench-macro"))]
fn main() {
    eprintln!(
        "macro_throughput is gated behind --features bench-macro.\n\
         The macro harness drives a live three-node cluster and \n\
         needs CAP_NET_ADMIN to install netem qdiscs on `lo`.\n\
         See docs/book/src/operations/benchmarks.md for setup."
    );
}

#[cfg(feature = "bench-macro")]
mod harness {
    use std::fs;
    use std::path::PathBuf;
    use std::process::Command;
    use std::time::{Duration, Instant};

    /// Single tc/netem condition driven by the macro harness.
    pub struct NetemCondition {
        /// Human-readable label used in the JSON output.
        pub name: &'static str,
        /// Arguments passed to `tc qdisc add dev lo root netem ...`.
        /// An empty slice means "no netem; baseline run".
        pub netem_args: &'static [&'static str],
    }

    pub const CONDITIONS: &[NetemCondition] = &[
        NetemCondition {
            name: "baseline",
            netem_args: &[],
        },
        NetemCondition {
            name: "delay_5ms",
            netem_args: &["delay", "5ms"],
        },
        NetemCondition {
            name: "loss_1pct",
            netem_args: &["loss", "1%"],
        },
        NetemCondition {
            name: "loss_5pct",
            netem_args: &["loss", "5%"],
        },
        NetemCondition {
            name: "loss_10pct",
            netem_args: &["loss", "10%"],
        },
        NetemCondition {
            name: "corrupt_0_1pct",
            netem_args: &["corrupt", "0.1%"],
        },
        NetemCondition {
            name: "reorder_25_50",
            netem_args: &["reorder", "25%", "50%"],
        },
    ];

    /// Apply a netem condition to the loopback interface. Returns
    /// `true` on success; logs and returns `false` on failure (the
    /// caller treats failure as "skip this condition").
    pub fn apply_netem(args: &[&str]) -> bool {
        if args.is_empty() {
            return true;
        }
        let mut cmd = Command::new("tc");
        cmd.args(["qdisc", "add", "dev", "lo", "root", "netem"]);
        cmd.args(args);
        match cmd.status() {
            Ok(s) if s.success() => true,
            Ok(s) => {
                eprintln!("tc qdisc add failed (exit {s})");
                false
            }
            Err(err) => {
                eprintln!("tc qdisc add: {err}");
                false
            }
        }
    }

    /// Best-effort cleanup: removes any qdisc this harness installed.
    pub fn clear_netem() {
        let _ = Command::new("tc")
            .args(["qdisc", "del", "dev", "lo", "root", "netem"])
            .status();
    }

    /// Run one condition. Placeholder: spawns three [`Server`](dynomite::Server)
    /// instances, drives `redis-benchmark`, and reports tail latency.
    /// The full implementation lives in the operator harness; this
    /// stub captures the wall time so the gate compiles end-to-end.
    pub fn run_condition(name: &str) -> serde_json::Value {
        let started = Instant::now();
        // The real harness spawns a 3-node cluster on localhost and
        // drives `redis-benchmark` for 30 seconds; the stub returns
        // an explicit `not-implemented-in-ci` marker so consumers do
        // not mistake the wall-clock for measured throughput.
        let elapsed = started.elapsed().min(Duration::from_secs(30));
        serde_json::json!({
            "condition": name,
            "ops_per_sec": serde_json::Value::Null,
            "latency_p50_us": serde_json::Value::Null,
            "latency_p99_us": serde_json::Value::Null,
            "latency_p999_us": serde_json::Value::Null,
            "latency_p9999_us": serde_json::Value::Null,
            "wall_seconds": elapsed.as_secs_f64(),
            "status": "scaffold",
        })
    }

    pub fn run() {
        let git_sha = Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map_or_else(|| "unknown".into(), |s| s.trim().to_string());

        let out_dir = PathBuf::from("target").join("bench");
        let _ = fs::create_dir_all(&out_dir);
        let mut all = Vec::new();
        for cond in CONDITIONS {
            if !apply_netem(cond.netem_args) {
                continue;
            }
            let result = run_condition(cond.name);
            clear_netem();
            all.push(result);
        }
        let path = out_dir.join(format!("macro-{git_sha}.json"));
        let body = serde_json::to_string_pretty(&all).unwrap();
        let _ = fs::write(&path, body);
        eprintln!("macro report: {}", path.display());
    }
}

#[cfg(feature = "bench-macro")]
fn main() {
    harness::run();
}
