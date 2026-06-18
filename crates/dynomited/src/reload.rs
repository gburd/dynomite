//! SIGHUP-driven configuration reload.
//!
//! Owns the runtime's reloadable-state cell and the reload
//! pipeline that the [`crate::server::Server`] run loop wires
//! into the SIGHUP handler.
//!
//! # Reload contract
//!
//! On `SIGHUP` the run loop:
//!
//! 1. Reopens the log file (existing
//!    [`dynomite::core::log::reopen_on_sighup`] path).
//! 2. Calls [`reload_from_path`] with the original startup YAML
//!    path, the live [`ReloadableState`], and the live
//!    [`dynomite::net::SharedTlsProfiles`].
//!
//! The pipeline is:
//!
//! * Re-read and parse the YAML at the same path used at startup.
//! * Validate. Either failure leaves every live structure
//!   untouched and surfaces a `tracing::error!` line; the
//!   process keeps running with the previous configuration.
//! * Diff the new pool block against the snapshot taken at
//!   `Server::build` time. Fields the run loop classifies as
//!   "value, not structure" (consistency, gossip cadence,
//!   entropy cadence, bucket-type bundle, etc.) are written
//!   into the [`ReloadableState`]. Fields that pin a listener
//!   socket, the peer topology, or the datastore choice are
//!   refused with a `tracing::warn!` line; the value already
//!   in memory wins.
//! * Rebuild the [`dynomite::net::tls::TlsProfileMap`] from the
//!   new pool's `peer_tls_*` fields and atomically swap it via
//!   [`dynomite::net::SharedTlsProfiles::replace`]. New
//!   handshakes pick up the new material; established sessions
//!   finish on their already-negotiated keys.
//!
//! The dispatcher, gossip task, and entropy driver hold a clone
//! of the [`ReloadableState`] handle; their hot loops re-read
//! through the cell on every cycle so reloads land without a
//! restart. Wiring that consumption is incremental: this slice
//! ships the publisher and the TLS swap; downstream consumers
//! land in follow-up stages without further changes here.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use dynomite::conf::{ConfPool, Config};
use dynomite::net::tls::{SharedTlsProfiles, TlsProfileMap, TlsProfileSpec};

/// Snapshot of every "value, not structure" pool knob the run
/// loop is willing to swap on SIGHUP.
///
/// Held inside an `Arc<RwLock<...>>` (see [`ReloadableState`]) so
/// hot loops can re-read on every cycle without locking out the
/// reload writer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReloadableSnapshot {
    /// Pool-level read consistency (string form, lower-case).
    pub read_consistency: Option<String>,
    /// Pool-level write consistency (string form, lower-case).
    pub write_consistency: Option<String>,
    /// Gossip cadence in milliseconds.
    pub gos_interval_ms: Option<i64>,
    /// Entropy reconciliation cadence in seconds.
    pub recon_interval_seconds: Option<u64>,
    /// Hint TTL in seconds.
    pub hint_ttl_seconds: Option<u64>,
    /// Hint store byte cap.
    pub hint_store_max_bytes: Option<u64>,
    /// Hint drainer cadence in milliseconds.
    pub hint_drain_interval_ms: Option<u64>,
    /// Inter-node read timeout in milliseconds.
    pub dyn_read_timeout_ms: Option<i64>,
    /// Inter-node write timeout in milliseconds.
    pub dyn_write_timeout_ms: Option<i64>,
    /// Request timeout in milliseconds.
    pub timeout_ms: Option<i64>,
    /// Per-bucket-type bundle (verbatim).
    pub bucket_types: Vec<dynomite::conf::ConfBucketType>,
    /// Default bucket type.
    pub default_bucket_type: Option<String>,
}

impl ReloadableSnapshot {
    /// Build a snapshot from a live [`ConfPool`].
    #[must_use]
    pub fn from_pool(pool: &ConfPool) -> Self {
        Self {
            read_consistency: pool.read_consistency.clone(),
            write_consistency: pool.write_consistency.clone(),
            gos_interval_ms: pool.gos_interval,
            recon_interval_seconds: pool.recon_interval_seconds,
            hint_ttl_seconds: pool.hint_ttl_seconds,
            hint_store_max_bytes: pool.hint_store_max_bytes,
            hint_drain_interval_ms: pool.hint_drain_interval_ms,
            dyn_read_timeout_ms: pool.dyn_read_timeout,
            dyn_write_timeout_ms: pool.dyn_write_timeout,
            timeout_ms: pool.timeout,
            bucket_types: pool.bucket_types.clone(),
            default_bucket_type: pool.default_bucket_type.clone(),
        }
    }
}

/// Shared, reloadable view of the pool's "value, not structure"
/// knobs.
///
/// Cheap to clone (Arc under the hood). Consumers (dispatcher,
/// gossip task, entropy driver) read via [`Self::snapshot`] on
/// each cycle; the SIGHUP handler writes via [`Self::store`].
#[derive(Clone, Debug, Default)]
pub struct ReloadableState {
    inner: Arc<RwLock<ReloadableSnapshot>>,
}

impl ReloadableState {
    /// Wrap an initial snapshot in a shared cell.
    #[must_use]
    pub fn new(initial: ReloadableSnapshot) -> Self {
        Self {
            inner: Arc::new(RwLock::new(initial)),
        }
    }

    /// Cheap read-side snapshot. Clones the inner record; cheap
    /// because every contained type is small.
    #[must_use]
    pub fn snapshot(&self) -> ReloadableSnapshot {
        self.inner.read().clone()
    }

    /// Atomic swap of the inner record. Returns the previous
    /// value so callers (the SIGHUP handler) can compute a diff
    /// for the operator log.
    pub fn store(&self, next: ReloadableSnapshot) -> ReloadableSnapshot {
        let mut g = self.inner.write();
        std::mem::replace(&mut *g, next)
    }
}

/// One reload operation outcome.
///
/// Returned by [`reload_from_path`] for use by the run loop's
/// SIGHUP arm so it can synthesise a single info-level summary
/// line without re-walking the diff itself.
#[derive(Debug, Default)]
pub struct ReloadOutcome {
    /// Names of fields the reload swapped in (sorted).
    pub reloaded: Vec<&'static str>,
    /// Names of fields the operator changed but which the
    /// runtime cannot reload without a restart (sorted).
    pub non_reloadable: Vec<&'static str>,
    /// Whether the TLS profile map was rebuilt and swapped in.
    /// `false` when no `peer_tls_*` knob was touched OR when
    /// the peer plane is plaintext on both sides of the diff.
    pub tls_swapped: bool,
}

/// Errors raised by [`reload_from_path`].
#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    /// The YAML on disk could not be parsed.
    #[error("reload: parse {path}: {source}")]
    Parse {
        /// Path the run loop tried to re-read.
        path: PathBuf,
        /// Underlying parse failure.
        source: dynomite::conf::ConfError,
    },
    /// The parsed YAML failed validation.
    #[error("reload: validate {path}: {source}")]
    Validate {
        /// Path the run loop tried to re-read.
        path: PathBuf,
        /// Underlying validation failure.
        source: dynomite::conf::ConfError,
    },
    /// Rebuilding the TLS profile map failed (missing files,
    /// malformed PEM, rustls rejection).
    #[error("reload: tls: {0}")]
    Tls(#[from] dynomite::net::tls::TlsError),
    /// A field whose validator does not catch the case but the
    /// reload pipeline still rejects (e.g. the original config
    /// had `peer_tls_cert` set without `peer_tls_key`).
    #[error("reload: bad config: field '{field}' is invalid: {reason}")]
    BadConfig {
        /// Field name (matches the YAML key).
        field: &'static str,
        /// Human-readable reason.
        reason: String,
    },
}

/// Re-read, validate, and apply a configuration file at
/// `path`, swapping reloadable values into `state` and TLS
/// material into `tls`. Returns a [`ReloadOutcome`] summarising
/// the diff.
///
/// `original` is the [`ConfPool`] the server was started with;
/// it is the reference point for the structural diff (peer
/// topology, listener ports, datastore choice).
///
/// # Errors
///
/// [`ReloadError`] for parse, validate, or TLS-rebuild
/// failures. The function is total in the sense that
/// `state` and `tls` are mutated only when every error gate
/// passes; on error the live configuration stays untouched.
pub fn reload_from_path(
    path: &Path,
    original: &ConfPool,
    state: &ReloadableState,
    tls: &SharedTlsProfiles,
) -> Result<ReloadOutcome, ReloadError> {
    let mut cfg = Config::parse_file(path).map_err(|e| ReloadError::Parse {
        path: path.to_path_buf(),
        source: e,
    })?;
    cfg.finalize();
    cfg.validate().map_err(|e| ReloadError::Validate {
        path: path.to_path_buf(),
        source: e,
    })?;
    let new_pool = cfg.pool().clone();

    // Build the new TLS map BEFORE we touch the shared state, so
    // a malformed cert / key bundle aborts the reload cleanly
    // and leaves the live `state` and `tls` untouched.
    let (default_spec, per_dc_spec) = build_profile_specs(&new_pool)?;
    let new_map = TlsProfileMap::build(default_spec, per_dc_spec)?;

    let mut outcome = ReloadOutcome::default();

    // Classify changes. Every diff lives on `&new_pool` vs
    // `original`, so the order of comparisons matches the
    // operator-facing summary line below.
    classify_reloadable(original, &new_pool, &mut outcome);
    classify_non_reloadable(original, &new_pool, &mut outcome);
    outcome.reloaded.sort_unstable();
    outcome.non_reloadable.sort_unstable();

    // Apply: snapshot first so the diff line is consistent with
    // what consumers will read on the next cycle.
    let snapshot = ReloadableSnapshot::from_pool(&new_pool);
    let _previous = state.store(snapshot);

    // Swap TLS material. The same cell is read by the listener's
    // SNI resolver and by the per-peer outbound supervisors;
    // both pick up the new map on their next handshake.
    let was_empty = tls.is_empty();
    let new_empty = new_map.is_empty();
    tls.replace(new_map);
    outcome.tls_swapped = !was_empty || !new_empty;

    for field in &outcome.non_reloadable {
        tracing::warn!(
            field = %field,
            "config field '{field}' is not reloadable; ignored. Restart dynomited to apply."
        );
    }

    Ok(outcome)
}

/// Translate `peer_tls_*` knobs in `pool` into the loader's
/// expected `(default, per_dc)` argument shape.
fn build_profile_specs(
    pool: &ConfPool,
) -> Result<
    (
        Option<TlsProfileSpec>,
        std::collections::BTreeMap<String, TlsProfileSpec>,
    ),
    ReloadError,
> {
    let default_spec = match (pool.peer_tls_cert.as_deref(), pool.peer_tls_key.as_deref()) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some(TlsProfileSpec {
            cert: cert.to_path_buf(),
            key: key.to_path_buf(),
            ca: pool.peer_tls_ca.clone(),
        }),
        (Some(_), None) => {
            return Err(ReloadError::BadConfig {
                field: "peer_tls_key",
                reason: "peer_tls_cert is set but peer_tls_key is not".into(),
            });
        }
        (None, Some(_)) => {
            return Err(ReloadError::BadConfig {
                field: "peer_tls_cert",
                reason: "peer_tls_key is set but peer_tls_cert is not".into(),
            });
        }
    };

    let mut per_dc: std::collections::BTreeMap<String, TlsProfileSpec> =
        std::collections::BTreeMap::new();
    for (dc, profile) in &pool.peer_tls_profiles {
        match (profile.cert.as_deref(), profile.key.as_deref()) {
            (Some(cert), Some(key)) => {
                per_dc.insert(
                    dc.clone(),
                    TlsProfileSpec {
                        cert: cert.to_path_buf(),
                        key: key.to_path_buf(),
                        ca: profile.ca.clone(),
                    },
                );
            }
            (None, None) => {
                return Err(ReloadError::BadConfig {
                    field: "peer_tls_profiles",
                    reason: format!("peer_tls_profiles[{dc}] is empty"),
                });
            }
            (Some(_), None) => {
                return Err(ReloadError::BadConfig {
                    field: "peer_tls_profiles.key",
                    reason: format!("peer_tls_profiles[{dc}].cert is set but .key is not"),
                });
            }
            (None, Some(_)) => {
                return Err(ReloadError::BadConfig {
                    field: "peer_tls_profiles.cert",
                    reason: format!("peer_tls_profiles[{dc}].key is set but .cert is not"),
                });
            }
        }
    }
    Ok((default_spec, per_dc))
}

fn classify_reloadable(old: &ConfPool, new: &ConfPool, out: &mut ReloadOutcome) {
    if old.read_consistency != new.read_consistency {
        out.reloaded.push("read_consistency");
    }
    if old.write_consistency != new.write_consistency {
        out.reloaded.push("write_consistency");
    }
    if old.gos_interval != new.gos_interval {
        out.reloaded.push("gos_interval");
    }
    if old.recon_interval_seconds != new.recon_interval_seconds {
        out.reloaded.push("recon_interval_seconds");
    }
    if old.hint_ttl_seconds != new.hint_ttl_seconds {
        out.reloaded.push("hint_ttl_seconds");
    }
    if old.hint_store_max_bytes != new.hint_store_max_bytes {
        out.reloaded.push("hint_store_max_bytes");
    }
    if old.hint_drain_interval_ms != new.hint_drain_interval_ms {
        out.reloaded.push("hint_drain_interval_ms");
    }
    if old.dyn_read_timeout != new.dyn_read_timeout {
        out.reloaded.push("dyn_read_timeout");
    }
    if old.dyn_write_timeout != new.dyn_write_timeout {
        out.reloaded.push("dyn_write_timeout");
    }
    if old.timeout != new.timeout {
        out.reloaded.push("timeout");
    }
    if old.bucket_types != new.bucket_types {
        out.reloaded.push("bucket_types");
    }
    if old.default_bucket_type != new.default_bucket_type {
        out.reloaded.push("default_bucket_type");
    }
    // TLS material lives in `peer_tls_*`; we always rebuild the
    // map but only flag the diff as a "TLS reload" when the
    // underlying paths changed. Track it as a reloaded field for
    // the operator summary.
    if old.peer_tls_cert != new.peer_tls_cert
        || old.peer_tls_key != new.peer_tls_key
        || old.peer_tls_ca != new.peer_tls_ca
        || old.peer_tls_profiles != new.peer_tls_profiles
    {
        out.reloaded.push("peer_tls");
    }
}

fn classify_non_reloadable(old: &ConfPool, new: &ConfPool, out: &mut ReloadOutcome) {
    if old.listen != new.listen {
        out.non_reloadable.push("listen");
    }
    if old.dyn_listen != new.dyn_listen {
        out.non_reloadable.push("dyn_listen");
    }
    if old.stats_listen != new.stats_listen {
        out.non_reloadable.push("stats_listen");
    }
    if old.dyn_seeds != new.dyn_seeds {
        out.non_reloadable.push("dyn_seeds");
    }
    if old.tokens != new.tokens {
        out.non_reloadable.push("tokens");
    }
    if old.rack != new.rack {
        out.non_reloadable.push("rack");
    }
    if old.datacenter != new.datacenter {
        out.non_reloadable.push("datacenter");
    }
    if old.data_store != new.data_store {
        out.non_reloadable.push("data_store");
    }
    if old.servers != new.servers {
        out.non_reloadable.push("servers");
    }
    if old.noxu_path != new.noxu_path {
        out.non_reloadable.push("noxu_path");
    }
    if old.search_index_dir != new.search_index_dir {
        out.non_reloadable.push("search_index_dir");
    }
    if old.enable_gossip != new.enable_gossip {
        out.non_reloadable.push("enable_gossip");
    }
    if old.enable_hinted_handoff != new.enable_hinted_handoff {
        out.non_reloadable.push("enable_hinted_handoff");
    }
    if old.hash != new.hash {
        out.non_reloadable.push("hash");
    }
    if old.client_connections != new.client_connections {
        out.non_reloadable.push("client_connections");
    }
    if old.dyn_connections != new.dyn_connections {
        out.non_reloadable.push("dyn_connections");
    }
    if old.observability != new.observability {
        out.non_reloadable.push("observability");
    }
    if old.riak != new.riak {
        out.non_reloadable.push("riak");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_yaml(dir: &tempfile::TempDir, body: &str) -> PathBuf {
        let p = dir.path().join("dynomite.yml");
        std::fs::write(&p, body).unwrap();
        p
    }

    fn pool_with_consistency(
        listen: u16,
        dyn_port: u16,
        stats_port: u16,
        read: &str,
        write: &str,
    ) -> String {
        format!(
            "p:\n  listen: 127.0.0.1:{listen}\n  dyn_listen: 127.0.0.1:{dyn_port}\n  stats_listen: 127.0.0.1:{stats_port}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:22122:1\n  data_store: 0\n  read_consistency: {read}\n  write_consistency: {write}\n",
        )
    }

    #[test]
    fn snapshot_records_consistency_and_intervals() {
        let yaml = pool_with_consistency(8101, 8102, 8103, "DC_QUORUM", "DC_QUORUM");
        let mut cfg = Config::parse_str(&yaml).unwrap();
        cfg.finalize();
        let snap = ReloadableSnapshot::from_pool(cfg.pool());
        assert_eq!(snap.read_consistency.as_deref(), Some("DC_QUORUM"));
        assert_eq!(snap.write_consistency.as_deref(), Some("DC_QUORUM"));
    }

    #[test]
    fn store_returns_previous() {
        let state = ReloadableState::new(ReloadableSnapshot::default());
        let next = ReloadableSnapshot {
            read_consistency: Some("DC_ONE".into()),
            ..ReloadableSnapshot::default()
        };
        let prev = state.store(next.clone());
        assert_eq!(prev, ReloadableSnapshot::default());
        assert_eq!(state.snapshot(), next);
    }

    #[test]
    fn reload_swaps_consistency_and_returns_diff() {
        let dir = tempfile::tempdir().unwrap();
        let original_yaml = pool_with_consistency(8101, 8102, 8103, "DC_ONE", "DC_ONE");
        let path = write_yaml(&dir, &original_yaml);
        let mut cfg = Config::parse_file(&path).unwrap();
        cfg.finalize();
        cfg.validate().unwrap();
        let original_pool = cfg.pool().clone();
        let state = ReloadableState::new(ReloadableSnapshot::from_pool(&original_pool));
        let tls = SharedTlsProfiles::default();

        // Rewrite the file with a different read_consistency.
        let new_yaml = pool_with_consistency(8101, 8102, 8103, "DC_QUORUM", "DC_ONE");
        std::fs::write(&path, new_yaml).unwrap();

        let outcome = reload_from_path(&path, &original_pool, &state, &tls).unwrap();
        assert!(outcome.reloaded.contains(&"read_consistency"));
        assert!(outcome.non_reloadable.is_empty());
        assert_eq!(
            state.snapshot().read_consistency.as_deref(),
            Some("DC_QUORUM")
        );
    }

    #[test]
    fn reload_warns_on_listener_change() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_yaml(
            &dir,
            &pool_with_consistency(8101, 8102, 8103, "DC_ONE", "DC_ONE"),
        );
        let mut cfg = Config::parse_file(&path).unwrap();
        cfg.finalize();
        cfg.validate().unwrap();
        let original_pool = cfg.pool().clone();
        let state = ReloadableState::new(ReloadableSnapshot::from_pool(&original_pool));
        let tls = SharedTlsProfiles::default();

        // Change the listener port (non-reloadable).
        std::fs::write(
            &path,
            pool_with_consistency(9999, 8102, 8103, "DC_ONE", "DC_ONE"),
        )
        .unwrap();
        let outcome = reload_from_path(&path, &original_pool, &state, &tls).unwrap();
        assert!(outcome.non_reloadable.contains(&"listen"));
        assert!(outcome.reloaded.is_empty());
    }

    #[test]
    fn reload_returns_parse_error_and_leaves_state_intact() {
        let dir = tempfile::tempdir().unwrap();
        let original_yaml = pool_with_consistency(8101, 8102, 8103, "DC_ONE", "DC_ONE");
        let path = write_yaml(&dir, &original_yaml);
        let mut cfg = Config::parse_file(&path).unwrap();
        cfg.finalize();
        cfg.validate().unwrap();
        let original_pool = cfg.pool().clone();
        let original_snapshot = ReloadableSnapshot::from_pool(&original_pool);
        let state = ReloadableState::new(original_snapshot.clone());
        let tls = SharedTlsProfiles::default();

        // Corrupt the YAML.
        std::fs::write(&path, "not: valid: yaml: at: all: [").unwrap();
        let err = reload_from_path(&path, &original_pool, &state, &tls).unwrap_err();
        assert!(matches!(err, ReloadError::Parse { .. }));
        // State was not mutated.
        assert_eq!(state.snapshot(), original_snapshot);
    }
}
