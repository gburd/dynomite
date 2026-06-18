//! Plaintext, postmortem-shaped dump of one node's state.
//!
//! The output mirrors what an operator wants to attach to a bug
//! report: build info, the sanitized config, the ring view, the
//! peer table, queue depths, gossip churn, recent events, and a
//! coarse memory / file-descriptor accounting block. The shape is
//! deliberately ASCII-only and grep-friendly: each section opens
//! with a `=== <name> ===` header followed by `key=value` lines or
//! per-row entries; no JSON, no YAML, no smart quotes.
//!
//! [`ClusterInfoSnapshot`] is the value type: every field is owned
//! and `Clone`. [`format_text`] renders a snapshot to any
//! [`std::io::Write`].
//!
//! Two helpers exist to obtain a snapshot:
//!
//! * [`ClusterInfoSnapshot::synthetic`] - hand-built for tests.
//! * [`gather_from_handle`] (re-exported below) - assembles a
//!   snapshot from a live [`crate::embed::ServerHandle`] plus a
//!   [`RecentEvents`] ring buffer; this is what dynomited wires
//!   up at boot.
//!
//! The snapshot deliberately does not include any secret fields:
//! Redis `requirepass` values, AUTH passwords, and the contents
//! of TLS / reconciliation key files are omitted. Paths to those
//! files are included so an operator can see whether the node is
//! configured for secure mode without leaking the material.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use serde::Serialize;

use crate::cluster::peer::PeerState;
use crate::cluster::pool::ServerPool;
use crate::conf::ConfPool;
use crate::embed::ServerHandle;
use crate::hashkit::DynToken;
use crate::stats::PoolField;

/// Maximum number of recent events retained in the ring buffer.
///
/// # Examples
///
/// ```
/// assert_eq!(dynomite::admin::cluster_info::MAX_RECENT_EVENTS, 50);
/// ```
pub const MAX_RECENT_EVENTS: usize = 50;

/// One section of the dump expressed as a name plus an ordered
/// list of `(key, value)` pairs. Sections always render the same
/// way, so authors can append a new field without touching the
/// formatter.
///
/// # Examples
///
/// ```
/// use dynomite::admin::cluster_info::Section;
/// let s = Section::with_pairs("build", &[("version", "0.0.1")]);
/// assert_eq!(s.name(), "build");
/// assert_eq!(s.pairs().len(), 1);
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Section {
    name: String,
    pairs: Vec<(String, String)>,
}

impl Section {
    /// Build a section with the supplied name and list of
    /// `(key, value)` pairs. Both keys and values are copied
    /// into owned strings so the section may outlive its inputs.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::admin::cluster_info::Section;
    /// let s = Section::with_pairs("hello", &[("k", "v")]);
    /// assert_eq!(s.pairs()[0].1, "v");
    /// ```
    #[must_use]
    pub fn with_pairs(name: &str, pairs: &[(&str, &str)]) -> Self {
        Self {
            name: name.to_string(),
            pairs: pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }
    }

    /// Section name (without the framing `===` markers).
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Borrow the ordered pair list.
    #[must_use]
    pub fn pairs(&self) -> &[(String, String)] {
        &self.pairs
    }

    /// Append a `(key, value)` pair to the section.
    pub fn push<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) {
        self.pairs.push((key.into(), value.into()));
    }
}

/// One free-form row inside the `peers` or `recent_events`
/// section. The text has already been rendered to an ASCII line.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RowSection {
    name: String,
    rows: Vec<String>,
}

impl RowSection {
    /// Build an empty row-section.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            rows: Vec::new(),
        }
    }

    /// Section name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Borrow the row list.
    #[must_use]
    pub fn rows(&self) -> &[String] {
        &self.rows
    }

    /// Append a row.
    pub fn push<S: Into<String>>(&mut self, row: S) {
        self.rows.push(row.into());
    }
}

/// One entry in the recent-events ring buffer.
///
/// Stored as a flat tuple so the producer (the gossip / lifecycle
/// taps) does not have to depend on the cluster-info module to
/// emit events.
///
/// # Examples
///
/// ```
/// use dynomite::admin::cluster_info::RecentEvent;
/// let e = RecentEvent::new(1_700_000_000, "peer_up", "arnold");
/// assert_eq!(e.kind, "peer_up");
/// assert_eq!(e.detail, "arnold");
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecentEvent {
    /// Wall-clock time (seconds since the unix epoch).
    pub ts_secs: u64,
    /// Short event kind: `peer_up`, `peer_down`, `gossip_round`,
    /// `config_reloaded`, `restart`. Free-form ASCII; consumers
    /// match by exact string.
    pub kind: String,
    /// Optional payload. ASCII; spaces are allowed.
    pub detail: String,
}

impl RecentEvent {
    /// Build a new event.
    #[must_use]
    pub fn new<K: Into<String>, D: Into<String>>(ts_secs: u64, kind: K, detail: D) -> Self {
        Self {
            ts_secs,
            kind: kind.into(),
            detail: detail.into(),
        }
    }
}

/// Bounded ring buffer of recent lifecycle events.
///
/// The ring is held inside an [`Arc<Mutex<...>>`] so the gossip
/// task and the dump endpoint can share it without contention on
/// the steady-state path. New entries push to the back; once the
/// length reaches [`MAX_RECENT_EVENTS`] the oldest entry is
/// dropped.
///
/// # Examples
///
/// ```
/// use dynomite::admin::cluster_info::{RecentEvent, RecentEvents};
/// let log = RecentEvents::new();
/// log.push(RecentEvent::new(1, "restart", ""));
/// assert_eq!(log.snapshot().len(), 1);
/// ```
#[derive(Clone, Debug, Default)]
pub struct RecentEvents {
    inner: Arc<Mutex<VecDeque<RecentEvent>>>,
}

impl RecentEvents {
    /// Construct a fresh, empty ring.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_RECENT_EVENTS))),
        }
    }

    /// Append `event`, dropping the oldest entry when the ring
    /// is full.
    pub fn push(&self, event: RecentEvent) {
        let mut guard = self.inner.lock();
        if guard.len() == MAX_RECENT_EVENTS {
            guard.pop_front();
        }
        guard.push_back(event);
    }

    /// Cloned snapshot of the ring contents (oldest first).
    #[must_use]
    pub fn snapshot(&self) -> Vec<RecentEvent> {
        self.inner.lock().iter().cloned().collect()
    }

    /// Number of events currently buffered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// True when the ring has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

/// Owned snapshot ready to be rendered.
///
/// Sections are stored in the canonical render order:
/// `build`, `config`, `ring`, `peers`, `queues`, `gossip`,
/// `recent_events`, `memory`, `fds`. Producers should fill every
/// section even when the underlying data is unavailable; the
/// formatter happily emits empty sections so the resulting text
/// has a stable shape across releases.
///
/// # Examples
///
/// ```
/// use dynomite::admin::cluster_info::ClusterInfoSnapshot;
/// let snap = ClusterInfoSnapshot::synthetic();
/// assert_eq!(snap.build.name(), "build");
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterInfoSnapshot {
    /// Build identity: version, git sha, build profile.
    pub build: Section,
    /// Sanitized configuration view.
    pub config: Section,
    /// Ring distribution / vnode counts.
    pub ring: Section,
    /// Per-peer rows.
    pub peers: RowSection,
    /// Queue-depth gauges.
    pub queues: Section,
    /// Gossip churn metrics.
    pub gossip: Section,
    /// Bounded ring of lifecycle events.
    pub recent_events: RowSection,
    /// Memory accounting block.
    pub memory: Section,
    /// Open file-descriptor accounting block.
    pub fds: Section,
}

impl ClusterInfoSnapshot {
    /// Hand-built snapshot useful for unit tests and the doc
    /// example. Every section is populated with a single
    /// representative entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::admin::cluster_info::{format_text, ClusterInfoSnapshot};
    /// let snap = ClusterInfoSnapshot::synthetic();
    /// let mut out = Vec::new();
    /// format_text(&snap, &mut out).unwrap();
    /// let text = String::from_utf8(out).unwrap();
    /// assert!(text.contains("=== build ==="));
    /// ```
    #[must_use]
    pub fn synthetic() -> Self {
        let mut snap = Self {
            build: Section::with_pairs(
                "build",
                &[
                    ("version", "0.0.1"),
                    ("git_sha", "unknown"),
                    ("build_profile", "debug"),
                ],
            ),
            config: Section::with_pairs(
                "config",
                &[
                    ("data_store", "redis"),
                    ("listen", "0.0.0.0:8102"),
                    ("dyn_listen", "0.0.0.0:8101"),
                    ("rack", "rack-1"),
                    ("dc", "dc-1"),
                ],
            ),
            ring: Section::with_pairs(
                "ring",
                &[
                    ("distribution", "vnode"),
                    ("vnodes", "1"),
                    ("tokens_per_node", "1"),
                ],
            ),
            peers: RowSection::new("peers"),
            queues: Section::with_pairs(
                "queues",
                &[
                    ("dispatcher_inflight", "0"),
                    ("backend_supervisor_pending", "0"),
                    ("hint_store_size", "0"),
                ],
            ),
            gossip: Section::with_pairs(
                "gossip",
                &[
                    ("churn_total", "0"),
                    ("phi_alarms_total", "0"),
                    ("heartbeats_sent", "0"),
                    ("heartbeats_received", "0"),
                ],
            ),
            recent_events: RowSection::new("recent_events"),
            memory: Section::with_pairs("memory", &[("rss_bytes", "unavailable")]),
            fds: Section::with_pairs("fds", &[("total", "unavailable")]),
        };
        snap.peers.push(
            "peer_id=local dc=dc-1 rack=rack-1 status=Normal phi=0.00 last_seen_ms=0 tokens=1",
        );
        snap.recent_events
            .push("1970-01-01T00:00:00Z restart local-bootstrap");
        snap
    }
}

/// Render `snap` to `w` as ASCII text. Each section is preceded
/// by a `=== <name> ===` header line, then either `key=value`
/// pairs (one per line) or free-form rows. Sections render in
/// the canonical order documented on
/// [`ClusterInfoSnapshot`]; each section ends with a single
/// trailing blank line so consumers can split on `\n\n`.
///
/// # Errors
///
/// Returns the underlying [`std::io::Error`] when the writer
/// rejects a write.
///
/// # Examples
///
/// ```
/// use dynomite::admin::cluster_info::{format_text, ClusterInfoSnapshot};
/// let snap = ClusterInfoSnapshot::synthetic();
/// let mut out = Vec::new();
/// format_text(&snap, &mut out).unwrap();
/// let text = String::from_utf8(out).unwrap();
/// for header in [
///     "=== build ===",
///     "=== config ===",
///     "=== ring ===",
///     "=== peers ===",
///     "=== queues ===",
///     "=== gossip ===",
///     "=== recent_events ===",
///     "=== memory ===",
///     "=== fds ===",
/// ] {
///     assert!(text.contains(header), "missing {header}");
/// }
/// ```
pub fn format_text<W: Write>(snap: &ClusterInfoSnapshot, w: &mut W) -> io::Result<()> {
    write_pair_section(w, &snap.build)?;
    write_pair_section(w, &snap.config)?;
    write_pair_section(w, &snap.ring)?;
    write_row_section(w, &snap.peers)?;
    write_pair_section(w, &snap.queues)?;
    write_pair_section(w, &snap.gossip)?;
    write_row_section(w, &snap.recent_events)?;
    write_pair_section(w, &snap.memory)?;
    write_pair_section(w, &snap.fds)?;
    Ok(())
}

fn write_pair_section<W: Write>(w: &mut W, s: &Section) -> io::Result<()> {
    writeln!(w, "=== {} ===", s.name())?;
    for (k, v) in &s.pairs {
        writeln!(w, "{k}={v}")?;
    }
    writeln!(w)
}

fn write_row_section<W: Write>(w: &mut W, s: &RowSection) -> io::Result<()> {
    writeln!(w, "=== {} ===", s.name())?;
    for r in &s.rows {
        writeln!(w, "{r}")?;
    }
    writeln!(w)
}

// ----- snapshot builders -------------------------------------------------

/// Compile-time crate version.
#[must_use]
pub fn build_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Best-effort short git SHA. Looks at the `DYNOMITE_GIT_SHA`
/// environment variable at compile time and falls back to
/// `unknown` when unset. The dynomited build script (if any) is
/// expected to populate the variable; the engine library does
/// not require it.
#[must_use]
pub fn build_git_sha() -> &'static str {
    option_env!("DYNOMITE_GIT_SHA").unwrap_or("unknown")
}

/// Returns `release` when compiled with optimisations, `debug`
/// otherwise.
#[must_use]
pub fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

/// Assemble the `build` section.
#[must_use]
pub fn build_section() -> Section {
    Section::with_pairs(
        "build",
        &[
            ("version", build_version()),
            ("git_sha", build_git_sha()),
            ("build_profile", build_profile()),
        ],
    )
}

/// Set of YAML field names whose values must never appear in the
/// dump. Paths to key files are fine; the file contents and
/// passwords are not.
const SECRET_FIELDS: &[&str] = &["redis_requirepass"];

/// True when `key` names a sensitive configuration field.
#[must_use]
pub fn is_secret_config_field(key: &str) -> bool {
    SECRET_FIELDS.contains(&key)
}

/// Build the sanitized `config` section from a [`ConfPool`].
///
/// Only fields useful in postmortems are included; secret
/// material (Redis AUTH password) is masked as `redacted`. The
/// emission order is fixed so the rendered text is reproducible.
///
/// # Examples
///
/// ```
/// use dynomite::admin::cluster_info::config_section;
/// use dynomite::conf::ConfPool;
/// let mut p = ConfPool::default();
/// p.apply_defaults();
/// let s = config_section(&p);
/// assert_eq!(s.name(), "config");
/// // No secret value ever leaks.
/// for (_, v) in s.pairs() {
///     assert!(!v.contains("super-secret-password"));
/// }
/// ```
#[must_use]
pub fn config_section(pool: &ConfPool) -> Section {
    let mut s = Section::with_pairs("config", &[]);
    push_config_listeners(&mut s, pool);
    push_config_topology(&mut s, pool);
    push_config_security(&mut s, pool);
    push_config_runtime(&mut s, pool);
    s
}

fn push_config_listeners(s: &mut Section, pool: &ConfPool) {
    let listen = pool
        .listen
        .as_ref()
        .map_or_else(|| "unset".to_string(), endpoint_to_string);
    let dyn_listen = pool
        .dyn_listen
        .as_ref()
        .map_or_else(|| "unset".to_string(), endpoint_to_string);
    let stats_listen = pool
        .stats_listen
        .as_ref()
        .map_or_else(|| "unset".to_string(), endpoint_to_string);
    let data_store = match pool.data_store {
        Some(0) => "redis".to_string(),
        Some(1) => "memcache".to_string(),
        Some(2) => "noxu".to_string(),
        Some(n) => format!("unknown({n})"),
        None => "unset".to_string(),
    };
    s.push("data_store", data_store);
    s.push("listen", listen);
    s.push("dyn_listen", dyn_listen);
    s.push("stats_listen", stats_listen);
}

fn push_config_topology(s: &mut Section, pool: &ConfPool) {
    s.push("rack", pool.rack.clone().unwrap_or_else(|| "unset".into()));
    s.push(
        "dc",
        pool.datacenter.clone().unwrap_or_else(|| "unset".into()),
    );
    s.push("env", pool.env.clone().unwrap_or_else(|| "unset".into()));
    s.push(
        "distribution",
        pool.resolved_distribution().as_str().to_string(),
    );
    s.push(
        "hash",
        pool.hash
            .map_or_else(|| "unset".to_string(), |h| h.as_str().to_string()),
    );
}

fn push_config_security(s: &mut Section, pool: &ConfPool) {
    s.push(
        "secure_server_option",
        pool.secure_server_option
            .clone()
            .unwrap_or_else(|| "unset".into()),
    );
    s.push(
        "pem_key_file",
        pool.pem_key_file.clone().unwrap_or_else(|| "unset".into()),
    );
    s.push(
        "recon_key_file",
        pool.recon_key_file
            .clone()
            .unwrap_or_else(|| "unset".into()),
    );
    s.push(
        "recon_iv_file",
        pool.recon_iv_file.clone().unwrap_or_else(|| "unset".into()),
    );
    s.push(
        "redis_requirepass",
        if pool.redis_requirepass.is_some() {
            "redacted".to_string()
        } else {
            "unset".to_string()
        },
    );
}

fn push_config_runtime(s: &mut Section, pool: &ConfPool) {
    s.push(
        "timeout_ms",
        pool.timeout
            .map_or_else(|| "unset".to_string(), |n| n.to_string()),
    );
    s.push(
        "preconnect",
        pool.preconnect
            .map_or_else(|| "unset".to_string(), |b| b.to_string()),
    );
    s.push(
        "auto_eject_hosts",
        pool.auto_eject_hosts
            .map_or_else(|| "unset".to_string(), |b| b.to_string()),
    );
    s.push(
        "enable_hinted_handoff",
        pool.enable_hinted_handoff
            .map_or_else(|| "unset".to_string(), |b| b.to_string()),
    );
    s.push(
        "enable_gossip",
        pool.enable_gossip
            .map_or_else(|| "unset".to_string(), |b| b.to_string()),
    );
    s.push(
        "read_consistency",
        pool.read_consistency
            .clone()
            .unwrap_or_else(|| "unset".into()),
    );
    s.push(
        "write_consistency",
        pool.write_consistency
            .clone()
            .unwrap_or_else(|| "unset".into()),
    );
}

fn endpoint_to_string(l: &crate::conf::ConfListen) -> String {
    format!("{}:{}", l.name(), l.port())
}

/// Build the `ring` section from the resolved distribution and
/// the live ring snapshot.
#[must_use]
pub fn ring_section(distribution: &str, ring_entries: usize, tokens_per_node: usize) -> Section {
    let mut s = Section::with_pairs("ring", &[]);
    s.push("distribution", distribution);
    s.push("vnodes", ring_entries.to_string());
    s.push("tokens_per_node", tokens_per_node.to_string());
    s
}

/// Format a single peer row.
#[must_use]
pub fn peer_row(
    peer_id: &str,
    dc: &str,
    rack: &str,
    state: PeerState,
    phi: f64,
    last_seen_ms: i64,
    tokens: usize,
) -> String {
    format!(
        "peer_id={peer_id} dc={dc} rack={rack} status={status} phi={phi:.2} \
         last_seen_ms={last_seen_ms} tokens={tokens}",
        status = state.name(),
    )
}

/// One peer rendered as a typed, machine-parseable ring row.
///
/// This is the JSON-friendly counterpart to the free-form
/// [`RowSection`] used by the plaintext dump: every field is a
/// named value so a consumer (such as `dyn-admin ring-status`)
/// can deserialize the whole ring without scraping text.
///
/// # Examples
///
/// ```
/// use dynomite::admin::cluster_info::RingPeer;
/// let p = RingPeer {
///     node: "10.0.0.1:8101".into(),
///     dc: "dc1".into(),
///     rack: "r1".into(),
///     tokens: vec![0, 2_147_483_648],
///     state: "NORMAL".into(),
///     is_local: true,
/// };
/// assert_eq!(p.tokens.len(), 2);
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RingPeer {
    /// Node label: `host:port` for a remote peer, or the local
    /// node's endpoint.
    pub node: String,
    /// Datacenter name.
    pub dc: String,
    /// Rack name.
    pub rack: String,
    /// Token list as the `u32` integers the engine uses for ring
    /// lookups.
    pub tokens: Vec<u32>,
    /// Lifecycle state name (`JOINING`, `NORMAL`, `DOWN`, ...).
    pub state: String,
    /// True for the local peer.
    pub is_local: bool,
}

/// Typed, JSON-serializable view of the whole ring.
///
/// Produced by [`gather_ring_from_pool`] and served verbatim on
/// the stats server's `/ring` route.
///
/// # Examples
///
/// ```
/// use dynomite::admin::cluster_info::RingSnapshot;
/// let snap = RingSnapshot { peers: Vec::new() };
/// assert!(snap.peers.is_empty());
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct RingSnapshot {
    /// One entry per peer the pool knows about.
    pub peers: Vec<RingPeer>,
}

/// Build a [`RingSnapshot`] from a live [`ServerPool`]: every
/// peer's endpoint, dc, rack, tokens, and lifecycle state, read
/// under the pool's existing peer `RwLock`.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use dynomite::admin::cluster_info::gather_ring_from_pool;
/// use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
/// use dynomite::cluster::pool::{PoolConfig, ServerPool};
/// use dynomite::hashkit::DynToken;
///
/// let local = Peer::new(
///     0, PeerEndpoint::tcp("127.0.0.1".into(), 8101), "r1".into(),
///     "dc1".into(), vec![DynToken::from_u32(0)], true, true, false,
/// );
/// let pool = ServerPool::new(PoolConfig::default(), vec![local]);
/// let ring = gather_ring_from_pool(&pool);
/// assert_eq!(ring.peers.len(), 1);
/// assert!(ring.peers[0].is_local);
/// ```
#[must_use]
pub fn gather_ring_from_pool(pool: &ServerPool) -> RingSnapshot {
    let peers = pool.peers().read();
    let rows = peers
        .iter()
        .map(|p| RingPeer {
            node: format!("{}:{}", p.endpoint().host(), p.endpoint().port()),
            dc: p.dc().to_string(),
            rack: p.rack().to_string(),
            tokens: p.tokens().iter().map(DynToken::get_int).collect(),
            state: p.state().name().to_string(),
            is_local: p.is_local(),
        })
        .collect();
    RingSnapshot { peers: rows }
}

/// Build the `queues` section from a stats snapshot.
#[must_use]
pub fn queues_section(snap: &crate::stats::Snapshot) -> Section {
    let mut s = Section::with_pairs("queues", &[]);
    let dispatcher = snap.pool.metrics[PoolField::DnodeClientInQueue.index()]
        + snap.pool.metrics[PoolField::DnodeClientOutQueue.index()];
    let backend = snap.server.metrics[crate::stats::ServerField::InQueue.index()]
        + snap.server.metrics[crate::stats::ServerField::OutQueue.index()];
    let hint_store = snap.pool.metrics[PoolField::PeerInQueue.index()]
        + snap.pool.metrics[PoolField::RemotePeerInQueue.index()];
    s.push("dispatcher_inflight", dispatcher.to_string());
    s.push("backend_supervisor_pending", backend.to_string());
    s.push("hint_store_size", hint_store.to_string());
    s
}

/// Build the `gossip` section from a stats snapshot. Only the
/// counters tracked today are surfaced; missing fields are
/// emitted as `unavailable`.
#[must_use]
pub fn gossip_section(snap: &crate::stats::Snapshot) -> Section {
    let mut s = Section::with_pairs("gossip", &[]);
    let churn = snap.pool.metrics[PoolField::StatsCount.index()];
    // Phi >= 8.0 is the conviction threshold defined in
    // failure_detector::DEFAULT_THRESHOLD; we surface a count
    // of peers currently above it as the alarm gauge.
    let alarms: i64 = snap
        .failure
        .peer_phi
        .iter()
        .filter(|p| p.phi >= 8.0)
        .count()
        .try_into()
        .unwrap_or(i64::MAX);
    let transitions: i64 = snap
        .failure
        .peer_state_transitions
        .iter()
        .map(|t| i64::try_from(t.count).unwrap_or(i64::MAX))
        .sum();
    s.push("churn_total", transitions.to_string());
    s.push("phi_alarms_total", alarms.to_string());
    s.push("stats_count", churn.to_string());
    s.push("heartbeats_sent", "unavailable".to_string());
    s.push("heartbeats_received", "unavailable".to_string());
    s
}

/// Build the `memory` section by scraping `/proc/self/status` on
/// Linux. Other platforms emit `unavailable`.
#[must_use]
pub fn memory_section(dyn_memory: i64) -> Section {
    let mut s = Section::with_pairs("memory", &[]);
    let rss = read_proc_status_kb("VmRSS").map_or_else(
        || "unavailable".to_string(),
        |kb| (kb.saturating_mul(1024)).to_string(),
    );
    let vms = read_proc_status_kb("VmSize").map_or_else(
        || "unavailable".to_string(),
        |kb| (kb.saturating_mul(1024)).to_string(),
    );
    s.push("rss_bytes", rss);
    s.push("vms_bytes", vms);
    s.push("hint_store_bytes", "unavailable".to_string());
    s.push("mbuf_pool_bytes", dyn_memory.to_string());
    s
}

/// Build the `fds` section by counting `/proc/self/fd` on Linux.
/// Other platforms emit `unavailable`.
#[must_use]
pub fn fds_section() -> Section {
    let mut s = Section::with_pairs("fds", &[]);
    let total = match std::fs::read_dir("/proc/self/fd") {
        Ok(rd) => rd.count().to_string(),
        Err(_) => "unavailable".to_string(),
    };
    s.push("total", total);
    s.push("listening", "unavailable".to_string());
    s.push("peer_links", "unavailable".to_string());
    s
}

/// Read `/proc/self/status` on Linux and parse the value (in
/// kilobytes) of the named line. Returns `None` on any error
/// or when the field is absent.
fn read_proc_status_kb(field: &str) -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in s.lines() {
        let Some(rest) = line.strip_prefix(field) else {
            continue;
        };
        // Accept either `Field:\tNNN kB` or `Field: NNN kB`.
        let rest = rest.trim_start_matches(':').trim();
        let mut it = rest.split_whitespace();
        let n_str = it.next()?;
        return n_str.parse::<u64>().ok();
    }
    None
}

/// Render an iso-8601 ASCII UTC timestamp from `ts_secs`.
/// Tolerates pre-1970 inputs by emitting `1970-01-01T00:00:00Z`.
#[must_use]
pub fn render_timestamp(ts_secs: u64) -> String {
    let mut out = String::with_capacity(20);
    let (y, mo, d, h, mi, se) = secs_to_components(ts_secs);
    let _ = write!(out, "{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{se:02}Z");
    out
}

/// Decompose `ts_secs` into `(year, month, day, hour, minute,
/// second)` UTC components without a third-party calendar
/// dependency. Years through `9999` are representable; inputs
/// past that horizon saturate.
fn secs_to_components(ts_secs: u64) -> (u64, u64, u64, u64, u64, u64) {
    let secs_per_day: u64 = 86_400;
    let days = ts_secs / secs_per_day;
    let day_rem = ts_secs % secs_per_day;
    let hour = day_rem / 3_600;
    let minute = (day_rem % 3_600) / 60;
    let second = day_rem % 60;
    let (year, month, day) = days_to_civil(days);
    (year, month, day, hour, minute, second)
}

/// Howard Hinnant's `civil_from_days` adapted to non-negative
/// inputs and `u64` arithmetic. Returns a `(year, month, day)`
/// triple; the algorithm is documented at
/// <https://howardhinnant.github.io/date_algorithms.html>.
fn days_to_civil(days_since_epoch: u64) -> (u64, u64, u64) {
    let serial = days_since_epoch + 719_468;
    let era = serial / 146_097;
    let day_of_era = serial - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_phase = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_phase + 2) / 5 + 1;
    let month = if month_phase < 10 {
        month_phase + 3
    } else {
        month_phase.saturating_sub(9)
    };
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}

/// Render the `recent_events` section from the snapshot of a
/// [`RecentEvents`] ring.
#[must_use]
pub fn recent_events_section(events: &[RecentEvent]) -> RowSection {
    let mut sec = RowSection::new("recent_events");
    for e in events {
        let detail = if e.detail.is_empty() {
            String::new()
        } else {
            format!(" {}", e.detail)
        };
        sec.push(format!(
            "{} {}{}",
            render_timestamp(e.ts_secs),
            e.kind,
            detail
        ));
    }
    sec
}

/// Assemble a snapshot from a live [`ServerHandle`] plus a
/// shared [`RecentEvents`] ring.
///
/// This is the call dynomited makes when the operator hits
/// `GET /cluster-info.txt`: it folds together the build, config,
/// ring, peers, queues, gossip, recent-events, memory, and fds
/// sections so the response shape is independent of the binary
/// layer.
#[must_use]
pub fn gather_from_handle(
    handle: &ServerHandle,
    pool: &ConfPool,
    events: &RecentEvents,
) -> ClusterInfoSnapshot {
    let stats = handle.stats();
    let ring = handle.ring();
    let peers_view = handle.peers();
    let tokens_per_node = peers_view
        .iter()
        .find(|p| p.is_local)
        .map_or(0, |p| p.tokens.len());
    let dist = pool.resolved_distribution().as_str();
    let mut peer_rows = RowSection::new("peers");
    let _now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    for p in &peers_view {
        // PeerSnapshot doesn't carry the gossip last-seen
        // timestamp today; emit 0 so the field is present but
        // honestly empty. Updating PeerSnapshot to surface
        // `last_state_ts_secs()` is tracked as a follow-up.
        let last_seen_ms: i64 = 0;
        let row = peer_row(
            &peer_label(p),
            &p.dc,
            &p.rack,
            p.state,
            phi_for(&stats, p.idx),
            last_seen_ms,
            p.tokens.len(),
        );
        peer_rows.push(row);
    }
    ClusterInfoSnapshot {
        build: build_section(),
        config: config_section(pool),
        ring: ring_section(dist, ring.entries.len(), tokens_per_node),
        peers: peer_rows,
        queues: queues_section(&stats),
        gossip: gossip_section(&stats),
        recent_events: recent_events_section(&events.snapshot()),
        memory: memory_section(stats.dyn_memory),
        fds: fds_section(),
    }
}

fn peer_label(p: &crate::embed::PeerSnapshot) -> String {
    if p.is_local {
        "local".to_string()
    } else {
        format!("{}:{}", p.host, p.port)
    }
}

fn phi_for(stats: &crate::stats::Snapshot, peer_idx: u32) -> f64 {
    stats
        .failure
        .peer_phi
        .iter()
        .find(|p| p.peer_idx == peer_idx)
        .map_or(0.0, |p| p.phi)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_snapshot_renders_all_sections() {
        let snap = ClusterInfoSnapshot::synthetic();
        let mut buf = Vec::new();
        format_text(&snap, &mut buf).expect("format");
        let text = String::from_utf8(buf).expect("ascii");
        for header in [
            "=== build ===",
            "=== config ===",
            "=== ring ===",
            "=== peers ===",
            "=== queues ===",
            "=== gossip ===",
            "=== recent_events ===",
            "=== memory ===",
            "=== fds ===",
        ] {
            assert!(text.contains(header), "missing {header}");
        }
        assert!(text.is_ascii());
    }

    #[test]
    fn config_section_redacts_requirepass() {
        let mut pool = ConfPool::default();
        pool.apply_defaults();
        pool.redis_requirepass = Some("super-secret-password".into());
        let sec = config_section(&pool);
        let pair = sec
            .pairs()
            .iter()
            .find(|(k, _)| k == "redis_requirepass")
            .expect("present");
        assert_eq!(pair.1, "redacted");
        let mut buf = Vec::new();
        let snap = ClusterInfoSnapshot {
            config: sec,
            ..ClusterInfoSnapshot::synthetic()
        };
        format_text(&snap, &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(!text.contains("super-secret-password"));
    }

    #[test]
    fn ring_buffer_drops_oldest_when_full() {
        let log = RecentEvents::new();
        for i in 0..(MAX_RECENT_EVENTS + 5) {
            log.push(RecentEvent::new(i as u64, "tick", ""));
        }
        let snap = log.snapshot();
        assert_eq!(snap.len(), MAX_RECENT_EVENTS);
        assert_eq!(snap.first().unwrap().ts_secs, 5);
        assert_eq!(snap.last().unwrap().ts_secs, (MAX_RECENT_EVENTS + 4) as u64);
    }

    #[test]
    fn timestamp_renders_known_epoch() {
        assert_eq!(render_timestamp(0), "1970-01-01T00:00:00Z");
        // 2026-05-27T00:00:00Z
        assert_eq!(render_timestamp(1_779_840_000), "2026-05-27T00:00:00Z");
    }

    #[test]
    fn is_secret_reports_known_fields() {
        assert!(is_secret_config_field("redis_requirepass"));
        assert!(!is_secret_config_field("listen"));
    }

    #[test]
    fn peer_row_is_ascii_and_well_formed() {
        let row = peer_row(
            "arnold",
            "dc-arnold",
            "rack-1",
            PeerState::Normal,
            0.42,
            120,
            3,
        );
        assert!(row.is_ascii());
        assert!(row.contains("phi=0.42"));
        assert!(row.contains("status=NORMAL"));
        assert!(row.contains("tokens=3"));
    }

    #[test]
    fn build_profile_is_one_of_two_values() {
        let p = build_profile();
        assert!(p == "debug" || p == "release");
    }
}
