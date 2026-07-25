//! Reaper orchestrator: the runtime loop that scans stored objects and
//! expires those past their bucket's TTL.
//!
//! The [`fsm`](super::fsm) is the pure decision core; this module is
//! the side-effecting driver that turns it into a running background
//! task. On each sweep it walks the primary key space, computes each
//! object's age from its stored write timestamp, resolves the object's
//! bucket TTL from the [`BucketPropsRegistry`], and deletes objects
//! whose age exceeds a non-zero TTL. A TTL of zero (the default)
//! disables expiry, so a bucket without a `ttl` property is never
//! touched.
//!
//! The sweep is bounded (a per-sweep cap) and best-effort: a decode
//! error on one record is skipped rather than aborting the sweep, and a
//! delete error is logged and skipped. The loop sleeps
//! `interval_seconds` between sweeps.

use std::sync::Arc;
use std::time::Duration;

use crate::bucket_props::BucketPropsRegistry;
use crate::datastore::NoxuDatastore;
use crate::proto::http::object::SiblingSet;

/// Configuration for one reaper orchestrator run loop.
#[derive(Clone, Debug)]
pub struct OrchestratorConfig {
    /// Seconds between sweeps.
    pub interval_seconds: u64,
    /// Maximum objects deleted per sweep (bounds a sweep's blast
    /// radius on a large expired backlog).
    pub max_per_sweep: usize,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            interval_seconds: super::DEFAULT_REAP_INTERVAL_SECONDS,
            max_per_sweep: 10_000,
        }
    }
}

/// One reaper orchestrator over a datastore and a bucket-props registry.
///
/// The registry supplies each bucket's `ttl_seconds`; the datastore is
/// walked and expired keys are deleted from it.
pub struct ReaperOrchestrator {
    datastore: Arc<NoxuDatastore>,
    registry: Arc<BucketPropsRegistry>,
    config: OrchestratorConfig,
}

impl std::fmt::Debug for ReaperOrchestrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReaperOrchestrator")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ReaperOrchestrator {
    /// Build an orchestrator.
    #[must_use]
    pub fn new(
        datastore: Arc<NoxuDatastore>,
        registry: Arc<BucketPropsRegistry>,
        config: OrchestratorConfig,
    ) -> Self {
        Self {
            datastore,
            registry,
            config,
        }
    }

    /// Run one sweep against the current wall clock, deleting expired
    /// objects. Returns the number of objects reaped.
    ///
    /// A live object is expired when its bucket's `ttl_seconds` is
    /// non-zero and `now - written_at_unix >= ttl_seconds`. An object
    /// with a zero write timestamp (a legacy record, or a replica copy
    /// that never carried one) has unknown age and is never expired.
    ///
    /// # Errors
    ///
    /// Surfaces a storage error from the initial scan; per-record
    /// decode and delete errors are logged and skipped so one bad
    /// record cannot stall the sweep.
    pub fn sweep_once(&self, now_unix: u64) -> Result<usize, crate::datastore::NoxuDatastoreError> {
        // First pass: collect the keys to expire. We cannot delete
        // while the read cursor is open, so gather then delete.
        let mut expired: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let registry = &self.registry;
        let max = self.config.max_per_sweep;
        self.datastore.fold_primary(|bucket, key, value| {
            if expired.len() >= max {
                return Ok(());
            }
            let ttl = registry.resolve(b"", bucket).effective_ttl_seconds();
            if ttl == 0 {
                return Ok(());
            }
            // The stored value is a SiblingSet; an object expires only
            // when EVERY sibling is past the TTL (a live sibling keeps
            // the key). An undecodable record is skipped.
            let Ok(set) = SiblingSet::from_storage_bytes(value) else {
                return Ok(());
            };
            if set.siblings.is_empty() {
                return Ok(());
            }
            let all_expired = set.siblings.iter().all(|o| {
                o.written_at_unix != 0 && now_unix.saturating_sub(o.written_at_unix) >= ttl
            });
            if all_expired {
                expired.push((bucket.to_vec(), key.to_vec()));
            }
            Ok(())
        })?;

        let mut reaped = 0usize;
        for (bucket, key) in expired {
            match self.datastore.delete_object(&bucket, &key) {
                Ok(_) => reaped += 1,
                Err(e) => {
                    tracing::warn!(error = %e, "reaper: delete of expired object failed");
                }
            }
        }
        Ok(reaped)
    }

    /// Run the sweep loop until `shutdown` resolves. Sleeps
    /// `interval_seconds` between sweeps. Intended to be spawned as a
    /// background task.
    pub async fn run(self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let interval = Duration::from_secs(self.config.interval_seconds.max(1));
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                () = tokio::time::sleep(interval) => {
                    let now = crate::server::now_unix();
                    match self.sweep_once(now) {
                        Ok(n) if n > 0 => {
                            tracing::info!(reaped = n, "reaper: sweep expired objects");
                        }
                        Ok(_) => {}
                        Err(e) => tracing::warn!(error = %e, "reaper: sweep failed"),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket_props::BucketProps;
    use crate::proto::http::object::HttpObject;

    fn store_object(ds: &NoxuDatastore, bucket: &[u8], key: &[u8], written_at: u64) {
        let obj = HttpObject {
            value: b"v".to_vec(),
            written_at_unix: written_at,
            ..HttpObject::default()
        };
        let storage = SiblingSet::single(obj).to_storage_bytes();
        ds.put_object(bucket, key, &storage, &[]).expect("put");
    }

    #[test]
    fn sweep_expires_only_objects_past_a_nonzero_ttl() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ds = Arc::new(NoxuDatastore::open_transactional(dir.path()).expect("noxu"));
        let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
        // bucket "cache" has ttl 100s; bucket "keep" has no ttl.
        registry.set(
            b"",
            b"cache",
            BucketProps {
                ttl_seconds: Some(100),
                ..BucketProps::default()
            },
        );

        let now = 1_000_000u64;
        // cache/old written 200s ago -> expired; cache/new 10s ago ->
        // kept; keep/any (no ttl) -> kept regardless of age.
        store_object(&ds, b"cache", b"old", now - 200);
        store_object(&ds, b"cache", b"new", now - 10);
        store_object(&ds, b"keep", b"ancient", now - 1_000_000);

        let orch = ReaperOrchestrator::new(
            ds.clone(),
            registry,
            OrchestratorConfig {
                interval_seconds: 60,
                max_per_sweep: 100,
            },
        );
        let reaped = orch.sweep_once(now).expect("sweep");
        assert_eq!(reaped, 1, "only cache/old is past its TTL");
        assert!(ds.get_object(b"cache", b"old").expect("get").is_none());
        assert!(ds.get_object(b"cache", b"new").expect("get").is_some());
        assert!(ds.get_object(b"keep", b"ancient").expect("get").is_some());
    }

    #[test]
    fn zero_write_timestamp_is_never_expired() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ds = Arc::new(NoxuDatastore::open_transactional(dir.path()).expect("noxu"));
        let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
        registry.set(
            b"",
            b"cache",
            BucketProps {
                ttl_seconds: Some(1),
                ..BucketProps::default()
            },
        );
        // written_at 0 = unknown age; even a 1s TTL must not expire it.
        store_object(&ds, b"cache", b"legacy", 0);
        let orch = ReaperOrchestrator::new(ds.clone(), registry, OrchestratorConfig::default());
        assert_eq!(orch.sweep_once(1_000_000).expect("sweep"), 0);
        assert!(ds.get_object(b"cache", b"legacy").expect("get").is_some());
    }

    #[test]
    fn max_per_sweep_bounds_the_batch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ds = Arc::new(NoxuDatastore::open_transactional(dir.path()).expect("noxu"));
        let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
        registry.set(
            b"",
            b"cache",
            BucketProps {
                ttl_seconds: Some(10),
                ..BucketProps::default()
            },
        );
        let now = 1_000_000u64;
        for i in 0..5u32 {
            store_object(&ds, b"cache", format!("k{i}").as_bytes(), now - 100);
        }
        let orch = ReaperOrchestrator::new(
            ds.clone(),
            registry,
            OrchestratorConfig {
                interval_seconds: 60,
                max_per_sweep: 3,
            },
        );
        assert_eq!(orch.sweep_once(now).expect("sweep"), 3, "capped at 3");
    }
}
