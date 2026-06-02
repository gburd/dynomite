//! Gossip state machine and seed-list bookkeeping.
//!
//! The reference engine runs a dedicated pthread that wakes on a
//! fixed interval, queries the seeds provider, parses returned
//! `host:port:rack:dc:tokens|...` blobs, and reconciles the
//! resulting nodes against the per-DC / per-rack tables. Nodes
//! are added when absent, replaced when their IP changes, and
//! gossip-updated when only the timestamp / state moves. Once
//! per round, the engine forwards either a `GOSSIP_SYN` (if
//! joining) or the local state digest (if normal) to a randomly
//! chosen peer.
//!
//! This module ports the data shape, the seed-list parser, and a
//! deterministic state machine that the dispatcher / a tokio
//! periodic task drives. The actual outbound dnode framing of
//! `GOSSIP_SYN` lives in [`crate::proto::dnode`]; the cluster
//! layer composes the two.
//!
//! # Examples
//!
//! ```
//! use dynomite::cluster::gossip::{parse_seed_node, SeedRecord};
//! let r = parse_seed_node("10.0.0.1:8101:rackA:dcX:1383429731").unwrap();
//! assert_eq!(r.host, "10.0.0.1");
//! assert_eq!(r.port, 8101);
//! assert_eq!(r.dc, "dcX");
//! assert_eq!(r.rack, "rackA");
//! assert_eq!(r.tokens.len(), 1);
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cluster::failure_detector::DEFAULT_THRESHOLD;
use crate::cluster::peer::PeerState;
use crate::cluster::pool::ServerPool;
use crate::events::{ClusterEvent, EventManager};
use crate::hashkit::{token::parse_token, DynToken};

/// Default gossip period (ms) - mirrors `CONF_DEFAULT_GOS_INTERVAL`
/// (1000 ms).
pub const DEFAULT_GOSSIP_INTERVAL_MS: u64 = 1_000;

/// Default seeds-check interval (`SEEDS_CHECK_INTERVAL`, 30s).
pub const DEFAULT_SEEDS_CHECK_INTERVAL_MS: u64 = 30_000;

/// Static configuration consumed by the gossip task.
#[derive(Clone, Debug)]
pub struct GossipConfig {
    /// Whether gossip is enabled.
    pub enabled: bool,
    /// Gossip period.
    pub interval: Duration,
    /// Seeds-check period (the C engine queries the seeds
    /// provider at most once per `SEEDS_CHECK_INTERVAL`).
    pub seeds_check_interval: Duration,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: Duration::from_millis(DEFAULT_GOSSIP_INTERVAL_MS),
            seeds_check_interval: Duration::from_millis(DEFAULT_SEEDS_CHECK_INTERVAL_MS),
        }
    }
}

/// Parsed view of one entry from a seeds-provider blob.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeedRecord {
    /// Hostname or IP.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Rack name.
    pub rack: String,
    /// Datacenter name.
    pub dc: String,
    /// Token list.
    pub tokens: Vec<DynToken>,
}

/// In-memory record of a node observed via gossip. Sits next to
/// [`crate::cluster::peer::Peer`]; the gossip task keeps a
/// dedicated table because the reference engine separates the two
/// (`gossip_node` vs `node`).
#[derive(Clone, Debug)]
pub struct GossipNode {
    /// Datacenter.
    pub dc: String,
    /// Rack.
    pub rack: String,
    /// Hostname or IP.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Token list.
    pub tokens: Vec<DynToken>,
    /// Lifecycle state.
    pub state: PeerState,
    /// Epoch-seconds timestamp of the last update.
    pub ts_secs: u64,
    /// True for the local node.
    pub is_local: bool,
}

/// Live gossip state.
///
/// A simple `HashMap` keyed on `(dc, rack, primary token bytes)`
/// reproduces the reference engine's per-rack `dict_token_nodes`
/// behaviour. A second map keyed on `(dc, rack, host)` reproduces
/// the per-rack `dict_name_nodes` lookup used to detect IP
/// replacement.
#[derive(Clone, Debug, Default)]
pub struct GossipState {
    by_token: HashMap<(String, String, String), GossipNode>,
    by_name: HashMap<(String, String, String), GossipNode>,
    node_count: usize,
}

impl GossipState {
    /// Empty state.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::gossip::GossipState;
    /// let s = GossipState::new();
    /// assert_eq!(s.node_count(), 0);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of distinct gossip nodes tracked.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.node_count
    }

    /// Step result of [`GossipState::add_or_update`].
    fn token_key(node: &GossipNode) -> (String, String, String) {
        let primary = node
            .tokens
            .first()
            .map(|t| format!("{}", t.get_int()))
            .unwrap_or_default();
        (node.dc.clone(), node.rack.clone(), primary)
    }

    fn name_key(node: &GossipNode) -> (String, String, String) {
        (node.dc.clone(), node.rack.clone(), node.host.clone())
    }

    /// Add or update a [`GossipNode`].
    ///
    /// Mirrors the reference engine's `gossip_add_node_if_absent`
    /// state machine:
    ///
    /// * brand-new (dc, rack, token) -> insert.
    /// * known token but new host -> replace IP and re-index.
    /// * known token + known host -> update timestamp / state if
    ///   the supplied `ts_secs` is newer than the stored value.
    ///
    /// Returns the [`GossipStep`] that classifies the change for
    /// the caller (handy in tests).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::gossip::{GossipNode, GossipState, GossipStep};
    /// use dynomite::cluster::peer::PeerState;
    /// use dynomite::hashkit::DynToken;
    /// let mut s = GossipState::new();
    /// let n = GossipNode {
    ///     dc: "d".into(), rack: "r".into(), host: "h".into(), port: 1,
    ///     tokens: vec![DynToken::from_u32(7)], state: PeerState::Normal,
    ///     ts_secs: 1, is_local: false,
    /// };
    /// assert_eq!(s.add_or_update(n.clone()), GossipStep::Added);
    /// assert_eq!(s.add_or_update(n), GossipStep::Unchanged);
    /// ```
    pub fn add_or_update(&mut self, node: GossipNode) -> GossipStep {
        let token_key = Self::token_key(&node);
        let name_key = Self::name_key(&node);
        if let Some(existing) = self.by_token.get_mut(&token_key) {
            if existing.host == node.host {
                if node.ts_secs > existing.ts_secs {
                    let changed = existing.state != node.state;
                    existing.state = node.state;
                    existing.ts_secs = node.ts_secs;
                    if changed {
                        return GossipStep::StateChanged;
                    }
                    return GossipStep::TimestampUpdated;
                }
                GossipStep::Unchanged
            } else {
                // Replace IP.
                let old_name_key = Self::name_key(existing);
                self.by_name.remove(&old_name_key);
                *existing = node.clone();
                self.by_name.insert(name_key, node);
                GossipStep::Replaced
            }
        } else {
            self.by_token.insert(token_key, node.clone());
            self.by_name.insert(name_key, node);
            self.node_count += 1;
            GossipStep::Added
        }
    }

    /// Iterate over the live gossip nodes.
    pub fn nodes(&self) -> impl Iterator<Item = &GossipNode> + '_ {
        self.by_token.values()
    }

    /// Apply the failure detector to every non-local node.
    ///
    /// Mirrors `gossip_failure_detector`: a node whose
    /// `now_secs - ts_secs` exceeds `(interval_ms / 1000) * 40`
    /// is marked [`PeerState::Down`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::gossip::{GossipNode, GossipState};
    /// use dynomite::cluster::peer::PeerState;
    /// use dynomite::hashkit::DynToken;
    /// let mut s = GossipState::new();
    /// s.add_or_update(GossipNode {
    ///     dc: "d".into(), rack: "r".into(), host: "h".into(), port: 1,
    ///     tokens: vec![DynToken::from_u32(7)], state: PeerState::Normal,
    ///     ts_secs: 0, is_local: false,
    /// });
    /// s.run_failure_detector(100, 1000);
    /// assert_eq!(s.nodes().next().unwrap().state, PeerState::Down);
    /// ```
    pub fn run_failure_detector(&mut self, now_secs: u64, interval_ms: u64) {
        let delta_secs = (interval_ms / 1000).saturating_mul(40);
        for node in self.by_token.values_mut() {
            if node.is_local {
                continue;
            }
            if now_secs.saturating_sub(node.ts_secs) > delta_secs {
                node.state = PeerState::Down;
            }
        }
        // Mirror by_name.
        for node in self.by_name.values_mut() {
            if node.is_local {
                continue;
            }
            if now_secs.saturating_sub(node.ts_secs) > delta_secs {
                node.state = PeerState::Down;
            }
        }
    }
}

/// Outcome of [`GossipState::add_or_update`].
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum GossipStep {
    /// Node is brand new.
    Added,
    /// Same node, same host, newer state arrived.
    StateChanged,
    /// Same node, same host, only the timestamp moved forward.
    TimestampUpdated,
    /// Same token but different host: IP replacement.
    Replaced,
    /// Stale or duplicate ack.
    Unchanged,
}

/// Parse one `host:port:rack:dc:tokens` seed string.
///
/// Mirrors the reference engine's `parse_seeds` routine. The token
/// list may be a single big-int or a comma-separated list.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::gossip::parse_seed_node;
/// assert!(parse_seed_node("h:1:r:d:1,2,3").is_ok());
/// assert!(parse_seed_node("h:1:r:d").is_err());
/// ```
pub fn parse_seed_node(raw: &str) -> Result<SeedRecord, String> {
    let parts: Vec<&str> = raw.splitn(5, ':').collect();
    if parts.len() != 5 {
        return Err(format!("malformed seed entry '{raw}'"));
    }
    // The reference engine splits from the right (strrchr), so
    // tokens get the rightmost field. To preserve that with hosts
    // that may contain colons (rare; typically IPv4), we instead
    // rsplit:
    let mut iter = raw.rsplitn(5, ':');
    let tokens_str = iter.next().ok_or("missing tokens")?;
    let dc = iter.next().ok_or("missing dc")?;
    let rack = iter.next().ok_or("missing rack")?;
    let port_str = iter.next().ok_or("missing port")?;
    let host = iter.next().ok_or("missing host")?;
    if host.is_empty() {
        return Err(format!("empty host in '{raw}'"));
    }
    if rack.is_empty() {
        return Err(format!("empty rack in '{raw}'"));
    }
    if dc.is_empty() {
        return Err(format!("empty dc in '{raw}'"));
    }
    let port: u16 = port_str
        .parse()
        .map_err(|e| format!("bad port '{port_str}': {e}"))?;
    if port == 0 {
        return Err(format!("zero port in '{raw}'"));
    }
    if tokens_str.is_empty() {
        return Err(format!("empty tokens in '{raw}'"));
    }
    let mut tokens = Vec::new();
    for t in tokens_str.split(',') {
        let parsed = parse_token(t.as_bytes()).map_err(|e| format!("bad token '{t}': {e}"))?;
        tokens.push(parsed);
    }
    Ok(SeedRecord {
        host: host.to_string(),
        port,
        rack: rack.to_string(),
        dc: dc.to_string(),
        tokens,
    })
}

/// Parse a multi-entry seeds blob (entries separated by `|`).
///
/// # Examples
///
/// ```
/// use dynomite::cluster::gossip::parse_seed_blob;
/// let v = parse_seed_blob("h1:8101:r:d:1|h2:8101:r:d:2").unwrap();
/// assert_eq!(v.len(), 2);
/// ```
pub fn parse_seed_blob(raw: &str) -> Result<Vec<SeedRecord>, String> {
    let mut out = Vec::new();
    for piece in raw.split('|') {
        if piece.is_empty() {
            continue;
        }
        out.push(parse_seed_node(piece)?);
    }
    Ok(out)
}

/// Authoritative owner of [`PeerState`] transitions for the
/// gossip plane.
///
/// The handler holds an `Arc<ServerPool>` and feeds the
/// per-peer phi-accrual failure detectors as gossip frames
/// arrive. A periodic tick re-evaluates phi for every non-local
/// peer and toggles `PeerState` between `Normal` and `Down` based
/// on the configured threshold:
///
/// * a peer is `Normal` once at least one heartbeat has been
///   recorded AND `phi(now) <= threshold`,
/// * a peer is `Down` when no heartbeat has ever been recorded
///   OR `phi(now) > threshold`.
///
/// The handler is the single place that mutates `peer.state`
/// once gossip is wired; the supervisor loop that owns the TCP
/// link no longer publishes peer-state transitions of its own.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use dynomite::cluster::gossip::GossipHandler;
/// use dynomite::cluster::peer::{Peer, PeerEndpoint};
/// use dynomite::cluster::pool::{PoolConfig, ServerPool};
/// use dynomite::hashkit::DynToken;
///
/// let cfg = PoolConfig::default();
/// let local = Peer::new(
///     0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
///     vec![DynToken::from_u32(0)], true, true, false,
/// );
/// let pool = Arc::new(ServerPool::new(cfg, vec![local]));
/// let handler = GossipHandler::new(pool);
/// assert!((handler.threshold() - 8.0).abs() < f64::EPSILON);
/// ```
#[derive(Debug)]
pub struct GossipHandler {
    pool: Arc<ServerPool>,
    threshold: f64,
    interval: Duration,
    /// Optional failure-cause metrics handle. When wired,
    /// every peer-state transition observed by
    /// [`Self::evaluate`] increments the matching
    /// `peer_state_transitions_total` counter and updates the
    /// `peer_state_current` and `gossip_phi_score` gauges.
    failure_metrics: Option<Arc<crate::stats::FailureMetrics>>,
    /// Optional structured-event publisher. When wired, every
    /// peer-state transition observed by [`Self::evaluate`] or
    /// [`Self::record_heartbeat_pname`] /
    /// [`Self::record_heartbeat_idx`] surfaces a
    /// [`ClusterEvent::PeerUp`] / [`ClusterEvent::PeerDown`]
    /// payload on the manager's broadcast.
    events: Option<Arc<EventManager>>,
}

impl GossipHandler {
    /// Build a fresh handler over `pool` using the default
    /// phi-accrual threshold ([`crate::cluster::failure_detector::DEFAULT_THRESHOLD`]).
    #[must_use]
    pub fn new(pool: Arc<ServerPool>) -> Self {
        Self {
            pool,
            threshold: DEFAULT_THRESHOLD,
            interval: Duration::from_millis(DEFAULT_GOSSIP_INTERVAL_MS),
            failure_metrics: None,
            events: None,
        }
    }

    /// Attach a [`crate::stats::FailureMetrics`] handle.
    ///
    /// When set, [`Self::evaluate`] emits a
    /// `peer_state_transitions_total` counter tick and a
    /// `peer_state_current` gauge update for every transition
    /// it applies, plus a `gossip_phi_score` gauge update for
    /// every non-local peer regardless of whether its state
    /// changed. Default behaviour is unchanged when no metrics
    /// handle is supplied.
    #[must_use]
    pub fn with_failure_metrics(mut self, metrics: Arc<crate::stats::FailureMetrics>) -> Self {
        self.failure_metrics = Some(metrics);
        self
    }

    /// Attach an [`EventManager`] handle.
    ///
    /// When set, every peer-state transition the handler
    /// applies surfaces a [`ClusterEvent::PeerUp`] or
    /// [`ClusterEvent::PeerDown`] payload on the manager's
    /// broadcast. Default behaviour is unchanged when no event
    /// manager is supplied.
    #[must_use]
    pub fn with_events(mut self, events: Arc<EventManager>) -> Self {
        self.events = Some(events);
        self
    }

    /// Borrow the installed event manager, if any.
    #[must_use]
    pub fn events(&self) -> Option<&Arc<EventManager>> {
        self.events.as_ref()
    }

    /// Override the phi threshold (default 8.0).
    #[must_use]
    pub fn with_threshold(mut self, threshold: f64) -> Self {
        self.threshold = threshold;
        self
    }

    /// Override the gossip interval used by the periodic tick
    /// when the handler is driven by the binary's run loop. The
    /// in-process tests do not depend on this value.
    #[must_use]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Phi threshold the handler is configured with.
    #[must_use]
    pub fn threshold(&self) -> f64 {
        self.threshold
    }

    /// Configured gossip interval.
    #[must_use]
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Borrow the underlying pool.
    #[must_use]
    pub fn pool(&self) -> &Arc<ServerPool> {
        &self.pool
    }

    /// Record an inbound gossip heartbeat from the peer
    /// identified by `pname` (a `host:port` string matching the
    /// peer's [`crate::cluster::peer::PeerEndpoint::pname`]).
    ///
    /// Mutates the peer's failure detector and immediately
    /// promotes the peer's state to [`PeerState::Normal`] when
    /// `phi(now)` is below the threshold; this gives gossip a
    /// snappy first-contact transition without waiting for the
    /// next periodic tick.
    ///
    /// Unknown pnames are ignored.
    pub fn record_heartbeat_pname(&self, pname: &str, now: Instant) {
        let mut peers = self.pool.peers().write();
        for p in peers.iter_mut() {
            if p.is_local() {
                continue;
            }
            if p.endpoint().pname() == pname {
                p.failure_detector_mut().record_heartbeat(now);
                if p.failure_detector().phi(now) <= self.threshold && p.state() != PeerState::Normal
                {
                    let prev = p.state();
                    p.set_state(PeerState::Normal, now_secs_wall());
                    if let Some(m) = self.failure_metrics.as_ref() {
                        m.record_peer_state_transition(
                            p.idx(),
                            p.dc(),
                            p.rack(),
                            prev,
                            PeerState::Normal,
                        );
                    }
                    if let Some(ev) = self.events.as_ref() {
                        ev.publish(ClusterEvent::PeerUp {
                            peer_id: p.idx(),
                            dc: p.dc().to_string(),
                            ts: std::time::SystemTime::now(),
                        });
                    }
                }
                return;
            }
        }
    }

    /// Record an inbound gossip heartbeat against a known peer
    /// index. Used by tests and by callers that already resolved
    /// the originating peer.
    pub fn record_heartbeat_idx(&self, peer_idx: u32, now: Instant) {
        let mut peers = self.pool.peers().write();
        if let Some(p) = peers.iter_mut().find(|p| p.idx() == peer_idx) {
            if p.is_local() {
                return;
            }
            p.failure_detector_mut().record_heartbeat(now);
            if p.failure_detector().phi(now) <= self.threshold && p.state() != PeerState::Normal {
                let prev = p.state();
                p.set_state(PeerState::Normal, now_secs_wall());
                if let Some(m) = self.failure_metrics.as_ref() {
                    m.record_peer_state_transition(
                        p.idx(),
                        p.dc(),
                        p.rack(),
                        prev,
                        PeerState::Normal,
                    );
                }
                if let Some(ev) = self.events.as_ref() {
                    ev.publish(ClusterEvent::PeerUp {
                        peer_id: p.idx(),
                        dc: p.dc().to_string(),
                        ts: std::time::SystemTime::now(),
                    });
                }
            }
        }
    }

    /// Walk every non-local peer and reconcile its `PeerState`
    /// with the failure detector's current view of `phi(now)`.
    /// Returns the list of `(peer_idx, new_state)` transitions
    /// the call applied (handy in tests).
    ///
    /// This is the failure-detector tick the binary runs on a
    /// periodic timer. Calling it never panics and it never
    /// blocks on I/O.
    pub fn evaluate(&self, now: Instant) -> Vec<(u32, PeerState)> {
        let mut peers = self.pool.peers().write();
        let mut transitions = Vec::new();
        for p in peers.iter_mut() {
            if p.is_local() {
                continue;
            }
            let phi = p.failure_detector().phi(now);
            if let Some(m) = self.failure_metrics.as_ref() {
                m.observe_phi(p.idx(), p.dc(), p.rack(), phi);
                m.observe_threshold(p.idx(), p.dc(), p.rack(), self.threshold);
            }
            let target = if p.failure_detector().last_heartbeat().is_some() && phi <= self.threshold
            {
                PeerState::Normal
            } else {
                PeerState::Down
            };
            let prev = p.state();
            if prev != target {
                p.set_state(target, now_secs_wall());
                transitions.push((p.idx(), target));
                if let Some(m) = self.failure_metrics.as_ref() {
                    m.record_peer_state_transition_at(p.idx(), p.dc(), p.rack(), prev, target, now);
                }
                if let Some(ev) = self.events.as_ref() {
                    let ts = std::time::SystemTime::now();
                    match target {
                        PeerState::Normal => ev.publish(ClusterEvent::PeerUp {
                            peer_id: p.idx(),
                            dc: p.dc().to_string(),
                            ts,
                        }),
                        PeerState::Down => ev.publish(ClusterEvent::PeerDown {
                            peer_id: p.idx(),
                            dc: p.dc().to_string(),
                            phi,
                            ts,
                        }),
                        _ => {}
                    }
                }
            } else if let Some(m) = self.failure_metrics.as_ref() {
                m.observe_peer_state(p.idx(), p.dc(), p.rack(), target);
            }
        }
        transitions
    }

    /// Mark the peer identified by `pname` as [`PeerState::Down`]
    /// without consulting the failure detector. Used by the
    /// gossip-shutdown path so the dispatcher can short-circuit
    /// routing to a peer that announced its own departure.
    pub fn mark_down_pname(&self, pname: &str) {
        let mut peers = self.pool.peers().write();
        for p in peers.iter_mut() {
            if p.is_local() {
                continue;
            }
            if p.endpoint().pname() == pname && p.state() != PeerState::Down {
                let prev = p.state();
                p.set_state(PeerState::Down, now_secs_wall());
                if let Some(m) = self.failure_metrics.as_ref() {
                    m.record_peer_state_transition(
                        p.idx(),
                        p.dc(),
                        p.rack(),
                        prev,
                        PeerState::Down,
                    );
                }
                if let Some(ev) = self.events.as_ref() {
                    ev.publish(ClusterEvent::PeerDown {
                        peer_id: p.idx(),
                        dc: p.dc().to_string(),
                        phi: p.failure_detector().phi(Instant::now()),
                        ts: std::time::SystemTime::now(),
                    });
                }
                return;
            }
        }
    }

    /// Reset the per-peer failure detector. Used when a peer is
    /// removed and re-added so historical jitter does not bias
    /// the new suspicion value.
    pub fn reset_detector(&self, peer_idx: u32) {
        let mut peers = self.pool.peers().write();
        if let Some(p) = peers.iter_mut().find(|p| p.idx() == peer_idx) {
            p.failure_detector_mut().reset();
        }
    }
}

fn now_secs_wall() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(dc: &str, rack: &str, host: &str, tok: u32, ts: u64) -> GossipNode {
        GossipNode {
            dc: dc.into(),
            rack: rack.into(),
            host: host.into(),
            port: 8101,
            tokens: vec![DynToken::from_u32(tok)],
            state: PeerState::Normal,
            ts_secs: ts,
            is_local: false,
        }
    }

    #[test]
    fn add_then_update_state() {
        let mut s = GossipState::new();
        assert_eq!(
            s.add_or_update(node("d", "r", "h", 7, 1)),
            GossipStep::Added
        );
        let mut n2 = node("d", "r", "h", 7, 2);
        n2.state = PeerState::Down;
        assert_eq!(s.add_or_update(n2), GossipStep::StateChanged);
    }

    #[test]
    fn ip_replacement() {
        let mut s = GossipState::new();
        s.add_or_update(node("d", "r", "h1", 7, 1));
        let n2 = node("d", "r", "h2", 7, 2);
        assert_eq!(s.add_or_update(n2), GossipStep::Replaced);
    }

    #[test]
    fn stale_update_ignored() {
        let mut s = GossipState::new();
        s.add_or_update(node("d", "r", "h", 7, 5));
        let stale = node("d", "r", "h", 7, 1);
        assert_eq!(s.add_or_update(stale), GossipStep::Unchanged);
    }

    #[test]
    fn parse_one_seed() {
        let r = parse_seed_node("10.0.0.1:8101:rA:dc1:1383429731").unwrap();
        assert_eq!(r.host, "10.0.0.1");
        assert_eq!(r.port, 8101);
        assert_eq!(r.rack, "rA");
        assert_eq!(r.dc, "dc1");
    }

    #[test]
    fn parse_multi_token_seed() {
        let r = parse_seed_node("h:1:r:d:1,2,3").unwrap();
        assert_eq!(r.tokens.len(), 3);
    }

    #[test]
    fn parse_blob_with_pipe() {
        let v = parse_seed_blob("h1:1:r:d:1|h2:2:r:d:2").unwrap();
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn parse_seed_rejects_short() {
        assert!(parse_seed_node("h:1:r:d").is_err());
    }

    #[test]
    fn failure_detector_ages_node_to_down() {
        let mut s = GossipState::new();
        s.add_or_update(node("d", "r", "h", 7, 0));
        s.run_failure_detector(1000, 1000); // delta = 40s, now > 40s
        assert_eq!(s.nodes().next().unwrap().state, PeerState::Down);
    }

    /// Construction helper for the `GossipHandler` test suite.
    /// The handler operates on a real `ServerPool`, so each test
    /// builds a small two-peer pool (one local, one remote).
    mod handler_helpers {
        use std::sync::Arc;

        use crate::cluster::peer::{Peer, PeerEndpoint};
        use crate::cluster::pool::{PoolConfig, ServerPool};
        use crate::hashkit::DynToken;

        pub fn pool() -> Arc<ServerPool> {
            let cfg = PoolConfig {
                dc: "dc1".into(),
                rack: "r1".into(),
                enable_gossip: true,
                ..PoolConfig::default()
            };
            let local = Peer::new(
                0,
                PeerEndpoint::tcp("127.0.0.1".into(), 8101),
                "r1".into(),
                "dc1".into(),
                vec![DynToken::from_u32(0)],
                true,
                true,
                false,
            );
            let remote = Peer::new(
                1,
                PeerEndpoint::tcp("127.0.0.1".into(), 8102),
                "r1".into(),
                "dc1".into(),
                vec![DynToken::from_u32(2_147_483_648)],
                false,
                true,
                false,
            );
            Arc::new(ServerPool::new(cfg, vec![local, remote]))
        }
    }

    fn remote_state(pool: &super::ServerPool) -> PeerState {
        pool.peers()
            .read()
            .iter()
            .find(|p| !p.is_local())
            .map_or(PeerState::Unknown, super::super::peer::Peer::state)
    }

    #[test]
    fn handler_first_heartbeat_promotes_to_normal() {
        let pool = handler_helpers::pool();
        let handler = GossipHandler::new(pool.clone());
        let t0 = std::time::Instant::now();
        assert_eq!(remote_state(&pool), PeerState::Down);
        handler.record_heartbeat_pname("127.0.0.1:8102", t0);
        // After the first received heartbeat the remote peer is
        // promoted out of the initial `Down` state.
        assert_eq!(remote_state(&pool), PeerState::Normal);
    }

    #[test]
    fn handler_steady_heartbeats_keep_peer_normal() {
        // Drive 100 heartbeats at 1s intervals; phi must stay
        // below 1.0 throughout and the peer must remain `Normal`.
        let pool = handler_helpers::pool();
        let handler = GossipHandler::new(pool.clone());
        let t0 = std::time::Instant::now();
        for i in 0..100 {
            let now = t0 + std::time::Duration::from_secs(i);
            handler.record_heartbeat_pname("127.0.0.1:8102", now);
            handler.evaluate(now);
        }
        let after_last =
            t0 + std::time::Duration::from_secs(99) + std::time::Duration::from_millis(10);
        let phi = pool
            .peers()
            .read()
            .iter()
            .find(|p| !p.is_local())
            .map_or(0.0, |p| p.failure_detector().phi(after_last));
        assert!(
            phi < 1.0,
            "phi should be < 1.0 right after a heartbeat, got {phi}"
        );
        assert_eq!(remote_state(&pool), PeerState::Normal);
    }

    #[test]
    fn handler_silence_transitions_peer_to_down() {
        // Stop heartbeats; advance the clock 60s; assert the
        // periodic evaluation transitions the peer to `Down`.
        let pool = handler_helpers::pool();
        let handler = GossipHandler::new(pool.clone());
        let t0 = std::time::Instant::now();
        for i in 0..100 {
            let now = t0 + std::time::Duration::from_secs(i);
            handler.record_heartbeat_pname("127.0.0.1:8102", now);
        }
        // Advance 60 seconds past the last heartbeat with no new
        // gossip; phi crosses the default threshold of 8.0.
        let later = t0 + std::time::Duration::from_secs(159);
        let transitions = handler.evaluate(later);
        assert_eq!(transitions, vec![(1, PeerState::Down)]);
        assert_eq!(remote_state(&pool), PeerState::Down);
    }

    #[test]
    fn handler_evaluate_no_data_keeps_peer_down() {
        // A peer we have never heard from stays `Down`.
        let pool = handler_helpers::pool();
        let handler = GossipHandler::new(pool.clone());
        let t0 = std::time::Instant::now();
        let transitions = handler.evaluate(t0);
        assert!(transitions.is_empty());
        assert_eq!(remote_state(&pool), PeerState::Down);
    }

    #[test]
    fn handler_unknown_pname_is_silent() {
        let pool = handler_helpers::pool();
        let handler = GossipHandler::new(pool.clone());
        let t0 = std::time::Instant::now();
        handler.record_heartbeat_pname("10.0.0.99:9999", t0);
        assert_eq!(remote_state(&pool), PeerState::Down);
    }

    #[test]
    fn handler_mark_down_overrides_normal() {
        let pool = handler_helpers::pool();
        let handler = GossipHandler::new(pool.clone());
        let t0 = std::time::Instant::now();
        handler.record_heartbeat_pname("127.0.0.1:8102", t0);
        assert_eq!(remote_state(&pool), PeerState::Normal);
        handler.mark_down_pname("127.0.0.1:8102");
        assert_eq!(remote_state(&pool), PeerState::Down);
    }

    /// `evaluate` toggles a peer Normal->Down once gossip
    /// quiesces. The wired `FailureMetrics` accumulator must
    /// see exactly one `(from=Normal, to=Down)` transition
    /// counter tick and the matching `peer_state_current`
    /// gauge entry.
    #[test]
    fn handler_evaluate_records_normal_to_down_transition() {
        let pool = handler_helpers::pool();
        let metrics = std::sync::Arc::new(crate::stats::FailureMetrics::new());
        let handler = GossipHandler::new(pool.clone()).with_failure_metrics(metrics.clone());
        let t0 = std::time::Instant::now();
        // Drive 100 heartbeats so the peer is firmly `Normal`.
        for i in 0..100 {
            let now = t0 + std::time::Duration::from_secs(i);
            handler.record_heartbeat_pname("127.0.0.1:8102", now);
            handler.evaluate(now);
        }
        let mid_snap = metrics.snapshot();
        let normal_count = mid_snap
            .peer_state_transitions
            .iter()
            .filter(|t| t.to == PeerState::Normal)
            .map(|t| t.count)
            .sum::<u64>();
        // There should be exactly one Down->Normal flip from
        // the very first heartbeat.
        assert_eq!(
            normal_count, 1,
            "got transitions: {:?}",
            mid_snap.peer_state_transitions
        );

        // Now stop heartbeats and skip 60 seconds of wall
        // time. evaluate should flip the peer to Down once.
        let later = t0 + std::time::Duration::from_secs(159);
        let transitions = handler.evaluate(later);
        assert_eq!(transitions, vec![(1, PeerState::Down)]);

        let snap = metrics.snapshot();
        let down_entry = snap
            .peer_state_transitions
            .iter()
            .find(|t| t.from == PeerState::Normal && t.to == PeerState::Down)
            .expect("normal->down transition should be recorded");
        assert_eq!(down_entry.count, 1);
        assert_eq!(down_entry.peer_idx, 1);

        // The current-state gauge follows the latest
        // observation.
        let current = snap
            .peer_state_current
            .iter()
            .find(|c| c.peer_idx == 1)
            .expect("peer_state_current entry should be present");
        assert_eq!(current.state, PeerState::Down);
        assert_eq!(current.dc, "dc1");
        assert_eq!(current.rack, "r1");

        // Phi gauge must be populated for the remote peer.
        let phi_entry = snap
            .peer_phi
            .iter()
            .find(|p| p.peer_idx == 1)
            .expect("gossip_phi_score gauge should be populated");
        assert!(
            phi_entry.phi >= 0.0,
            "phi should be non-negative; got {}",
            phi_entry.phi
        );
    }

    /// Simulate a peer flap (Normal -> Down -> Normal) and
    /// confirm:
    ///
    /// * the transitions counter records exactly one
    ///   Normal->Down and one Down->Normal entry,
    /// * the dwell histogram captures at least one observation
    ///   for both the Normal and Down state buckets,
    /// * the threshold gauge is populated alongside the phi
    ///   score so the operator can read both side by side.
    #[test]
    fn handler_flap_increments_transitions_and_records_dwell() {
        let pool = handler_helpers::pool();
        let metrics = std::sync::Arc::new(crate::stats::FailureMetrics::new());
        let handler = GossipHandler::new(pool.clone())
            .with_failure_metrics(metrics.clone())
            .with_threshold(8.0);
        let t0 = std::time::Instant::now();
        // Phase 1: 100 steady heartbeats establish Normal.
        for i in 0..100 {
            let now = t0 + std::time::Duration::from_secs(i);
            handler.record_heartbeat_pname("127.0.0.1:8102", now);
            handler.evaluate(now);
        }
        // Phase 2: stop heartbeats; the next evaluate flips to
        // Down.
        let down_at = t0 + std::time::Duration::from_secs(160);
        let trans1 = handler.evaluate(down_at);
        assert_eq!(trans1, vec![(1, PeerState::Down)]);
        // Phase 3: heartbeats resume; the next inbound message
        // promotes the peer back to Normal (the snappy-promote
        // path inside `record_heartbeat_pname`).
        let up_at = down_at + std::time::Duration::from_secs(5);
        handler.record_heartbeat_pname("127.0.0.1:8102", up_at);

        let snap = metrics.snapshot();
        // Exactly one Normal -> Down and one Down -> Normal
        // since the start of the flap.
        let n_to_d = snap
            .peer_state_transitions
            .iter()
            .find(|t| t.from == PeerState::Normal && t.to == PeerState::Down)
            .map_or(0, |t| t.count);
        let d_to_n = snap
            .peer_state_transitions
            .iter()
            .find(|t| t.from == PeerState::Down && t.to == PeerState::Normal)
            .map_or(0, |t| t.count);
        assert_eq!(
            n_to_d, 1,
            "expected exactly one Normal->Down transition, got: {:?}",
            snap.peer_state_transitions
        );
        assert_eq!(
            d_to_n, 2,
            "expected exactly two Down->Normal transitions (initial promote + flap recover), got: {:?}",
            snap.peer_state_transitions
        );

        // Dwell histogram must hold at least one observation
        // in both Normal and Down state rows.
        let normal_dwell = snap
            .peer_state_dwell
            .iter()
            .find(|e| e.state == PeerState::Normal)
            .expect("Normal dwell row missing");
        let down_dwell = snap
            .peer_state_dwell
            .iter()
            .find(|e| e.state == PeerState::Down)
            .expect("Down dwell row missing");
        assert!(
            normal_dwell.count >= 1,
            "Normal dwell row had no observations"
        );
        assert!(down_dwell.count >= 1, "Down dwell row had no observations");
        // The +Inf bucket equals the per-state observation
        // count for both rows.
        assert_eq!(
            *normal_dwell.bucket_counts.last().unwrap(),
            normal_dwell.count
        );
        assert_eq!(*down_dwell.bucket_counts.last().unwrap(), down_dwell.count);

        // Threshold gauge must be populated for the remote
        // peer.
        let thr_entry = snap
            .peer_threshold
            .iter()
            .find(|t| t.peer_idx == 1)
            .expect("gossip_phi_threshold_observed gauge should be populated");
        assert!(
            (thr_entry.threshold - 8.0).abs() < 1e-6,
            "threshold gauge should mirror handler config (got {})",
            thr_entry.threshold
        );
    }
}
