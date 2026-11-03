//! Operational test harness for multi-node clusters on a single host.
//!
//! Every scenario runs inside a private network namespace (unprivileged:
//! `unshare --user --map-root-user --net`), so it owns its loopback
//! device, its `tc` qdiscs, and every process it starts. No root, no
//! `CAP_NET_ADMIN` on the host, nothing left behind: when the scenario
//! exits the namespace and every qdisc in it disappear.
//!
//! Inside the namespace each node listens on its own loopback address
//! (`127.0.1.N`), so a fault between two specific nodes is a `tc` filter
//! on that source/destination pair. The fault layers are:
//!
//! | Layer | Mechanism | Module |
//! |---|---|---|
//! | network | `tc` netem: delay, jitter, loss, duplicate, reorder, corrupt, rate, partition | [`net`] |
//! | process | `SIGSTOP`/`SIGCONT` pause, `SIGKILL`, restart | [`cluster`] |
//! | I/O | libfiu POSIX preload, failure points toggled at run time over its control FIFO | [`fault`] |
//! | memory | libumem `UMEM_DEBUG=mtbf=N` and FAINT one-allocation-at-a-time injection | [`fault`] |
//! | page cache | libdirectio `O_DIRECT` preload, so storage paths hit the device | [`fault`] |
//! | clock | libfaketime offsets per node | [`fault`] |
//!
//! Faults are deterministic: netem takes an explicit `seed`, libfiu takes
//! `FIU_PRNG_SEED`, and the scenario's workload draws from a seeded RNG,
//! so a failing run is replayed by re-running with the seed it printed.
//!
//! Tools are discovered at run time ([`Tools::discover`]). A scenario that
//! needs a tool the host lacks reports a skip naming the tool instead of
//! failing, so the suite builds and runs everywhere and becomes a hard
//! gate on hosts that carry the tools (`OPTEST_REQUIRE=1` turns every skip
//! into a failure; CI and the chef test host set it).

pub mod check;
pub mod cluster;
pub mod fault;
pub mod history;
pub mod net;
pub mod ns;
pub mod resp;
pub mod tools;

pub use cluster::{Cluster, ClusterSpec, NodeSpec};
pub use ns::in_sandbox;
pub use tools::Tools;

/// Errors raised by the harness itself (not by the system under test).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error while spawning, writing configs, or talking to a node.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// A required external tool is missing.
    #[error("missing tool: {0}")]
    MissingTool(&'static str),
    /// A command the harness ran (tc, ip, a datastore) failed.
    #[error("command `{cmd}` failed: {stderr}")]
    Command {
        /// The command line.
        cmd: String,
        /// Its standard error.
        stderr: String,
    },
    /// A node or datastore did not become ready in time.
    #[error("timeout: {0}")]
    Timeout(String),
    /// A protocol-level error talking to a node.
    #[error("protocol: {0}")]
    Protocol(String),
}

/// Result alias for harness operations.
pub type Result<T> = std::result::Result<T, Error>;
