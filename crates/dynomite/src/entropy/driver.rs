//! Periodic entropy reconciliation driver.
//!
//! [`EntropyDriver`] is the long-running task that walks a
//! [`ServerPool`]'s peer list at a configured cadence and calls
//! [`reconcile_with_peer`] for every non-local entry. Each cycle
//! produces a [`ReconCycle`] summary that is logged at INFO level
//! so operators can verify the run loop is alive and observe
//! divergence / repair counters as the cluster's state settles.
//!
//! The driver uses the existing [`crate::entropy::send::EntropySender::push`]
//! primitive: each peer interaction is one snapshot push of the
//! configured [`crate::entropy::SnapshotSource`]. Embedders that
//! supply a richer source (e.g. one that carries per-range
//! Merkle digests) get the corresponding richer reconciliation
//! semantics for free; the default in-memory or RDB-backed
//! sources still drive a full snapshot push per cycle.
//!
//! # Shutdown
//!
//! [`EntropyDriver::run_until_shutdown`] honours a
//! `tokio::sync::watch::Receiver<bool>`: when the flag flips to
//! `true` the loop drains the in-flight cycle (the
//! per-peer reconciliations of the current tick complete) and
//! returns. The next tick is suppressed.
//!
//! [`ServerPool`]: crate::cluster::pool::ServerPool

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::watch;

use crate::cluster::peer::{Peer, PeerEndpoint};
use crate::entropy::send::EntropySender;
use crate::entropy::util::EntropyMaterial;
use crate::entropy::{
    BoxedSnapshotSource, EntropyConfig, EntropyError, EntropyResult, DEFAULT_BUFFER_SIZE,
    DEFAULT_HEADER_SIZE,
};

/// Default cadence for the entropy run loop (five minutes).
///
/// Mirrors the operator-visible default for the
/// `recon_interval_seconds:` YAML directive.
pub const DEFAULT_RECON_INTERVAL: Duration = Duration::from_mins(5);

/// Default TCP port the entropy receiver listens on.
///
/// The default port is `8105`. When operators want a different port
/// they can plug
/// their own [`EntropyDriver`] together via [`EntropyDriver::with_peer_port`].
pub const DEFAULT_ENTROPY_PORT: u16 = 8105;

/// Outcome of a single reconciliation pass.
///
/// All four counters are simple totals over the peers visited
/// during one cycle of [`EntropyDriver::run_cycle`].
///
/// # Examples
///
/// ```
/// use dynomite::entropy::driver::ReconCycle;
/// let mut c = ReconCycle::default();
/// c.record_attempted();
/// c.record_exchanged(128);
/// assert_eq!(c.peers_attempted, 1);
/// assert_eq!(c.peers_exchanged, 1);
/// assert_eq!(c.ranges_diverged, 1);
/// assert_eq!(c.ranges_repaired, 1);
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReconCycle {
    /// Peers the driver attempted to dial during the cycle.
    pub peers_attempted: u64,
    /// Peers the driver successfully exchanged a snapshot with.
    pub peers_exchanged: u64,
    /// Number of divergent ranges observed (one per peer when
    /// the snapshot was non-empty).
    pub ranges_diverged: u64,
    /// Number of divergent ranges actually repaired (currently
    /// equals [`Self::ranges_diverged`]: every range pushed is
    /// considered repaired once the receiver acknowledges by
    /// closing the socket).
    pub ranges_repaired: u64,
}

impl ReconCycle {
    /// Note that the driver dialled one more peer.
    pub fn record_attempted(&mut self) {
        self.peers_attempted = self.peers_attempted.saturating_add(1);
    }

    /// Note that one peer interaction completed successfully.
    /// `bytes` is the plaintext snapshot length the sender
    /// pushed; non-zero values are interpreted as one divergent
    /// range repaired.
    pub fn record_exchanged(&mut self, bytes: usize) {
        self.peers_exchanged = self.peers_exchanged.saturating_add(1);
        if bytes > 0 {
            self.ranges_diverged = self.ranges_diverged.saturating_add(1);
            self.ranges_repaired = self.ranges_repaired.saturating_add(1);
        }
    }

    /// Merge `other` into `self`.
    pub fn merge(&mut self, other: ReconCycle) {
        self.peers_attempted = self.peers_attempted.saturating_add(other.peers_attempted);
        self.peers_exchanged = self.peers_exchanged.saturating_add(other.peers_exchanged);
        self.ranges_diverged = self.ranges_diverged.saturating_add(other.ranges_diverged);
        self.ranges_repaired = self.ranges_repaired.saturating_add(other.ranges_repaired);
    }
}

/// Run one reconciliation pass against `peer`.
///
/// Dials `peer` on `peer_port`, performs the negotiation
/// handshake, and pushes one snapshot from `source`. The
/// returned [`ReconCycle`] always reports `peers_attempted = 1`;
/// `peers_exchanged` is `1` on success and `0` on failure, with
/// the error returned in `Err(_)`.
///
/// # Errors
/// [`EntropyError`] for resolution, dial, transport, or crypto
/// faults. Callers (typically [`EntropyDriver`]) are expected to
/// log and continue on `Err` rather than abort the cycle.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use dynomite::cluster::peer::PeerEndpoint;
/// use dynomite::entropy::driver::reconcile_with_peer;
/// use dynomite::entropy::send::StaticSnapshot;
/// use dynomite::entropy::util::{EntropyIv, EntropyKey, EntropyMaterial};
///
/// # async fn run() {
/// let mat = EntropyMaterial::new(
///     EntropyKey::from_bytes([0x10; 16]),
///     EntropyIv::from_bytes([0x42; 16]),
/// );
/// let source: dynomite::entropy::BoxedSnapshotSource =
///     Arc::new(StaticSnapshot::new(b"hello".to_vec()));
/// let peer = PeerEndpoint::tcp("127.0.0.1".into(), 9000);
/// let cycle = reconcile_with_peer(&mat, &source, &peer, 8105, 256, 64, true)
///     .await
///     .unwrap();
/// assert_eq!(cycle.peers_attempted, 1);
/// # }
/// ```
pub async fn reconcile_with_peer(
    material: &EntropyMaterial,
    source: &BoxedSnapshotSource,
    peer: &PeerEndpoint,
    peer_port: u16,
    buffer_size: usize,
    header_size: usize,
    encrypt: bool,
) -> EntropyResult<ReconCycle> {
    let endpoint = resolve_peer_endpoint(peer, peer_port)?;
    let cfg = EntropyConfig {
        // The on-disk paths are unused by `EntropySender::push`
        // when material is supplied via the in-memory shortcut
        // below, but the field is non-optional in the public
        // struct. Use placeholder paths; the sender does not
        // touch them because we override encryption with the
        // already-loaded material.
        key_file: std::path::PathBuf::new(),
        iv_file: std::path::PathBuf::new(),
        listen_addr: endpoint,
        send_addr: None,
        peer_endpoint: endpoint,
        buffer_size,
        header_size,
        encrypt,
    };
    let bytes =
        EntropySender::push_with_material(cfg, source.clone(), Some(material.clone())).await?;
    let mut cycle = ReconCycle::default();
    cycle.record_attempted();
    cycle.record_exchanged(bytes);
    Ok(cycle)
}

fn resolve_peer_endpoint(peer: &PeerEndpoint, port: u16) -> EntropyResult<SocketAddr> {
    if let Ok(ip) = peer.host().parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let mut iter = (peer.host(), port)
        .to_socket_addrs()
        .map_err(EntropyError::Io)?;
    iter.next().ok_or_else(|| {
        EntropyError::Config(format!("could not resolve peer host '{}'", peer.host()))
    })
}

/// Periodic reconciliation driver.
///
/// Constructed by the embedding binary once the entropy key /
/// IV material has been loaded; spawned as a tokio task with
/// [`EntropyDriver::run_until_shutdown`].
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
/// use parking_lot::RwLock;
/// use dynomite::entropy::driver::EntropyDriver;
/// use dynomite::entropy::send::StaticSnapshot;
/// use dynomite::entropy::util::{EntropyIv, EntropyKey, EntropyMaterial};
///
/// let mat = EntropyMaterial::new(
///     EntropyKey::from_bytes([0x10; 16]),
///     EntropyIv::from_bytes([0x42; 16]),
/// );
/// let source: dynomite::entropy::BoxedSnapshotSource =
///     Arc::new(StaticSnapshot::new(Vec::new()));
/// let peers = Arc::new(RwLock::new(Vec::new()));
/// let driver = EntropyDriver::new(mat, source, peers, Duration::from_secs(300));
/// assert_eq!(driver.cadence(), Duration::from_secs(300));
/// ```
pub struct EntropyDriver {
    material: EntropyMaterial,
    source: BoxedSnapshotSource,
    peers: Arc<RwLock<Vec<Peer>>>,
    cadence: Duration,
    peer_port: u16,
    buffer_size: usize,
    header_size: usize,
    encrypt: bool,
}

impl EntropyDriver {
    /// Build a driver with the default entropy port and chunk
    /// sizes.
    #[must_use]
    pub fn new(
        material: EntropyMaterial,
        source: BoxedSnapshotSource,
        peers: Arc<RwLock<Vec<Peer>>>,
        cadence: Duration,
    ) -> Self {
        Self {
            material,
            source,
            peers,
            cadence: if cadence.is_zero() {
                DEFAULT_RECON_INTERVAL
            } else {
                cadence
            },
            peer_port: DEFAULT_ENTROPY_PORT,
            buffer_size: DEFAULT_BUFFER_SIZE,
            header_size: DEFAULT_HEADER_SIZE,
            encrypt: true,
        }
    }

    /// Override the per-peer entropy receiver port.
    #[must_use]
    pub fn with_peer_port(mut self, port: u16) -> Self {
        self.peer_port = port;
        self
    }

    /// Override the per-chunk plaintext buffer size in bytes.
    #[must_use]
    pub fn with_buffer_size(mut self, bytes: usize) -> Self {
        self.buffer_size = bytes;
        self
    }

    /// Override the snapshot header size in bytes.
    #[must_use]
    pub fn with_header_size(mut self, bytes: usize) -> Self {
        self.header_size = bytes;
        self
    }

    /// Disable AES-128-CBC encryption of per-chunk payloads.
    /// Intended for tests; production deployments leave the
    /// encryption flag at its default of `true`.
    #[must_use]
    pub fn with_encrypt(mut self, on: bool) -> Self {
        self.encrypt = on;
        self
    }

    /// Reconciliation cadence.
    #[must_use]
    pub fn cadence(&self) -> Duration {
        self.cadence
    }

    /// Per-peer entropy receiver port the driver dials.
    #[must_use]
    pub fn peer_port(&self) -> u16 {
        self.peer_port
    }

    /// Run a single reconciliation cycle: visit every non-local
    /// peer in the pool, attempt one snapshot push each, and
    /// return the aggregated [`ReconCycle`].
    ///
    /// Per-peer failures are logged at WARN and recorded as
    /// `peers_attempted` (without bumping `peers_exchanged`).
    pub async fn run_cycle(&self) -> ReconCycle {
        // Snapshot the peer list to a local Vec so we do not
        // hold the RwLock across awaits. The peer list rarely
        // changes; copying a handful of `Peer` values per cycle
        // is cheap relative to the per-peer TCP exchange.
        let peer_list: Vec<Peer> = {
            let guard = self.peers.read();
            guard.iter().filter(|p| !p.is_local()).cloned().collect()
        };
        let mut total = ReconCycle::default();
        for peer in &peer_list {
            match reconcile_with_peer(
                &self.material,
                &self.source,
                peer.endpoint(),
                self.peer_port,
                self.buffer_size,
                self.header_size,
                self.encrypt,
            )
            .await
            {
                Ok(cycle) => total.merge(cycle),
                Err(e) => {
                    total.record_attempted();
                    tracing::warn!(
                        peer = %peer.endpoint().pname(),
                        error = %e,
                        "entropy reconciliation with peer failed"
                    );
                }
            }
        }
        total
    }

    /// Drive the periodic loop until `shutdown` is set.
    ///
    /// The first cycle runs immediately so the receiver synchronises
    /// eagerly on startup; subsequent cycles fire on `cadence`. A
    /// shutdown observed mid-cycle is honoured at the next per-peer
    /// boundary so the in-flight peer interaction completes
    /// (the driver does not abort the AES handshake mid-frame).
    pub async fn run_until_shutdown(self, mut shutdown: watch::Receiver<bool>) {
        if *shutdown.borrow() {
            return;
        }
        let mut tick = tokio::time::interval(self.cadence);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::info!("entropy driver shutting down");
                        return;
                    }
                }
                _ = tick.tick() => {
                    let cycle = self.run_cycle().await;
                    tracing::info!(
                        peers_attempted = cycle.peers_attempted,
                        peers_exchanged = cycle.peers_exchanged,
                        ranges_diverged = cycle.ranges_diverged,
                        ranges_repaired = cycle.ranges_repaired,
                        "entropy reconciliation cycle completed"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::peer::{Peer, PeerEndpoint};
    use crate::entropy::send::StaticSnapshot;
    use crate::entropy::util::{EntropyIv, EntropyKey};
    use crate::hashkit::DynToken;

    fn material() -> EntropyMaterial {
        EntropyMaterial::new(
            EntropyKey::from_bytes([0x10; 16]),
            EntropyIv::from_bytes([0x42; 16]),
        )
    }

    fn empty_source() -> BoxedSnapshotSource {
        Arc::new(StaticSnapshot::new(Vec::new()))
    }

    #[test]
    fn cadence_defaults_to_five_minutes_when_zero() {
        let peers = Arc::new(RwLock::new(Vec::new()));
        let driver = EntropyDriver::new(material(), empty_source(), peers, Duration::ZERO);
        assert_eq!(driver.cadence(), DEFAULT_RECON_INTERVAL);
    }

    #[test]
    fn cadence_passthrough_for_nonzero() {
        let peers = Arc::new(RwLock::new(Vec::new()));
        let driver = EntropyDriver::new(material(), empty_source(), peers, Duration::from_secs(1));
        assert_eq!(driver.cadence(), Duration::from_secs(1));
        assert_eq!(driver.peer_port(), DEFAULT_ENTROPY_PORT);
    }

    #[test]
    fn cycle_record_helpers() {
        let mut c = ReconCycle::default();
        c.record_attempted();
        c.record_attempted();
        c.record_exchanged(0);
        c.record_exchanged(128);
        assert_eq!(c.peers_attempted, 2);
        assert_eq!(c.peers_exchanged, 2);
        assert_eq!(c.ranges_diverged, 1);
        assert_eq!(c.ranges_repaired, 1);
    }

    #[test]
    fn cycle_merge_sums_fields() {
        let mut a = ReconCycle::default();
        a.record_attempted();
        a.record_exchanged(64);
        let mut b = ReconCycle::default();
        b.record_attempted();
        b.record_exchanged(0);
        a.merge(b);
        assert_eq!(a.peers_attempted, 2);
        assert_eq!(a.peers_exchanged, 2);
        assert_eq!(a.ranges_diverged, 1);
    }

    #[tokio::test]
    async fn driver_skips_local_peers_in_cycle() {
        // A pool with only the local peer must complete a
        // cycle in zero attempts.
        let local = Peer::new(
            0,
            PeerEndpoint::tcp("127.0.0.1".into(), 1),
            "r".into(),
            "d".into(),
            vec![DynToken::from_u32(0)],
            true,
            true,
            false,
        );
        let peers = Arc::new(RwLock::new(vec![local]));
        let driver = EntropyDriver::new(material(), empty_source(), peers, Duration::from_mins(1));
        let cycle = driver.run_cycle().await;
        assert_eq!(cycle.peers_attempted, 0);
        assert_eq!(cycle.peers_exchanged, 0);
    }

    #[tokio::test]
    async fn driver_returns_immediately_when_shutdown_already_set() {
        let peers = Arc::new(RwLock::new(Vec::new()));
        let driver = EntropyDriver::new(material(), empty_source(), peers, Duration::from_mins(1));
        let (tx, rx) = watch::channel(true);
        // The driver must observe the pre-set flag and return
        // without ticking; if it ticked it would block for the
        // full cadence.
        let res =
            tokio::time::timeout(Duration::from_millis(500), driver.run_until_shutdown(rx)).await;
        assert!(res.is_ok(), "driver did not honour pre-set shutdown");
        drop(tx);
    }
}
