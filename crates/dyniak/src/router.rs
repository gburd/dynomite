//! Bucket-aware request router.
//!
//! [`BucketRouter`] is the seam the Riak request path uses to:
//!
//! 1. Resolve a bucket's effective [`BucketProps`] from the
//!    [`BucketPropsRegistry`].
//! 2. Compute the pre-hash bytes via the chosen
//!    [`crate::datatypes::keyfun::KeyFun`] (deliverable A).
//! 3. Choose the replica set via the chosen
//!    [`crate::replication::ReplicationStrategy`] (deliverable B).
//!
//! The resulting [`RouteDecision`] carries the strategy, the
//! list of replica peers, and the bytes that were fed to the
//! cluster's hash function. The dispatcher then delivers the
//! request to those peers' outbound channels (the topology path
//! still owns the existing
//! [`dynomite::cluster::dispatch::ClusterDispatcher`] code; this
//! module only computes the targets when `Successors` is in
//! force).
//!
//! # Wiring
//!
//! Tests wire a `BucketRouter` directly with a fixture
//! [`RingView`]; production code constructs one from the live
//! cluster server pool via [`BucketRouter::new`].
//!
//! # Examples
//!
//! ```
//! use std::sync::Arc;
//! use dyniak::{BucketProps, BucketPropsRegistry, ReplicationStrategy};
//! use dyniak::datatypes::keyfun::KeyFun;
//! use dyniak::replication::{RingPoint, RingView};
//! use dyniak::router::BucketRouter;
//! use dynomite::hashkit::HashType;
//!
//! let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
//! registry.set(
//!     b"default",
//!     b"users",
//!     BucketProps {
//!         keyfun: Some(KeyFun::BucketOnly),
//!         strategy: Some(ReplicationStrategy::Successors),
//!         n_val: Some(3),
//!         ..Default::default()
//!     },
//! );
//! let span = u64::from(u32::MAX);
//! let pts: Vec<RingPoint> = (0..5u32)
//!     .map(|i| RingPoint::new(u64::from(i) * span / 5, i, "dc1", "r1"))
//!     .collect();
//! let ring = Arc::new(RingView::new(pts));
//! let router = BucketRouter::new(registry, ring, HashType::Murmur3X64_64);
//!
//! let a = router.route(b"default", b"users", b"alice");
//! let b = router.route(b"default", b"users", b"bob");
//! // BucketOnly: every key in the same bucket maps to the same primary.
//! assert_eq!(a.primary_peer_idx(), b.primary_peer_idx());
//! ```

use std::sync::Arc;

use dynomite::cluster::ReplicaTarget;
use dynomite::embed::hooks::BoxFuture;
use dynomite::hashkit::{hash64, HashType};
use dynomite::msg::ConsistencyLevel;

use crate::bucket_props::{BucketProps, BucketPropsRegistry};
use crate::datatypes::keyfun::KeyFun;
use crate::replication::{plan_replicas, ReplicationPlan, ReplicationStrategy, RingView};

/// Routing decision for a single `(bucket-type, bucket, key)`
/// triple.
///
/// Carries every input that contributed to the choice so a
/// caller can audit the decision (tests and the request-tracing
/// span both consume this).
#[derive(Clone, Debug)]
pub struct RouteDecision {
    /// Bucket type the routing was performed against.
    pub bucket_type: Vec<u8>,
    /// Effective properties (defaults filled in).
    pub props: BucketProps,
    /// Bytes fed to the hash function (after [`KeyFun`]).
    pub route_bytes: Vec<u8>,
    /// 64-bit hash of [`Self::route_bytes`].
    pub key_hash: u64,
    /// Replica plan produced by [`plan_replicas`]. For
    /// [`ReplicationStrategy::Topology`] the plan carries an
    /// empty vector; the caller is expected to fall through to
    /// the existing topology dispatch in that case.
    pub plan: ReplicationPlan,
}

impl RouteDecision {
    /// Effective [`KeyFun`] applied to the request.
    #[must_use]
    pub fn keyfun(&self) -> KeyFun {
        self.props.effective_keyfun()
    }

    /// Effective [`ReplicationStrategy`].
    #[must_use]
    pub fn strategy(&self) -> ReplicationStrategy {
        self.props.effective_strategy()
    }

    /// Replica list the dispatcher should hand to the per-peer
    /// outbound channels. For `Topology` strategy this is empty
    /// (the existing topology pipeline is the source of truth);
    /// for `Successors` it is `[primary, succ1, succ2, ...]`.
    #[must_use]
    pub fn replica_list(&self) -> Vec<ReplicaTarget> {
        self.plan.clone().into_replica_list()
    }

    /// Convenience: peer index of the primary replica.
    /// Returns `None` when the plan is `Topology(empty)`.
    #[must_use]
    pub fn primary_peer_idx(&self) -> Option<u32> {
        match &self.plan {
            ReplicationPlan::Successors { primary, .. } => Some(primary.peer_idx),
            ReplicationPlan::Topology(targets) => targets.first().map(|t| t.peer_idx),
        }
    }
}

/// Bucket-aware request router.
///
/// Cheap to clone via [`Arc`].
#[derive(Clone, Debug)]
pub struct BucketRouter {
    registry: Arc<BucketPropsRegistry>,
    ring: Arc<RingView>,
    hash: HashType,
    /// Store of operator-supplied custom-keyfun WASM modules.
    /// `None` when no keyfun store is wired; a
    /// [`crate::datatypes::keyfun::KeyFun::Custom`] route then
    /// surfaces a clean [`crate::datatypes::keyfun::KeyFunError`]
    /// instead of routing. Present only with the `wasm` feature.
    #[cfg(feature = "wasm")]
    keyfun_store: Option<crate::datatypes::keyfun_wasm::WasmKeyfunStore>,
}

impl BucketRouter {
    /// Construct a router from its three inputs.
    #[must_use]
    pub fn new(registry: Arc<BucketPropsRegistry>, ring: Arc<RingView>, hash: HashType) -> Self {
        Self {
            registry,
            ring,
            hash,
            #[cfg(feature = "wasm")]
            keyfun_store: None,
        }
    }

    /// Attach a custom-keyfun WASM store to the router.
    ///
    /// After this call, a bucket whose `chash_keyfun` selects
    /// [`crate::datatypes::keyfun::KeyFun::Custom`] routes its keys
    /// through the named module in `store`. Consumes and returns
    /// `self` for builder-style construction.
    #[cfg(feature = "wasm")]
    #[must_use]
    pub fn with_keyfun_store(
        mut self,
        store: crate::datatypes::keyfun_wasm::WasmKeyfunStore,
    ) -> Self {
        self.keyfun_store = Some(store);
        self
    }

    /// Borrow the attached custom-keyfun WASM store, if any.
    #[cfg(feature = "wasm")]
    #[must_use]
    pub fn keyfun_store(&self) -> Option<&crate::datatypes::keyfun_wasm::WasmKeyfunStore> {
        self.keyfun_store.as_ref()
    }

    /// Borrow the bucket-properties registry. Useful for the
    /// PBC `RpbSetBucketReq` / `RpbGetBucketReq` handlers, which
    /// share the registry with the request-time router.
    #[must_use]
    pub fn registry(&self) -> &Arc<BucketPropsRegistry> {
        &self.registry
    }

    /// Borrow the ring view.
    #[must_use]
    pub fn ring(&self) -> &Arc<RingView> {
        &self.ring
    }

    /// Hash function the router applies to [`KeyFun`]-shaped bytes.
    #[must_use]
    pub fn hash_type(&self) -> HashType {
        self.hash
    }

    /// Compute a [`RouteDecision`] for `(bucket_type, bucket,
    /// key)`.
    ///
    /// `bucket_type` is the optional Riak bucket-type qualifier;
    /// pass an empty slice to mean "the `default` bucket type".
    ///
    /// # Examples
    ///
    /// See the module-level example.
    #[must_use]
    pub fn route(&self, bucket_type: &[u8], bucket: &[u8], key: &[u8]) -> RouteDecision {
        self.try_route(bucket_type, bucket, key).expect(
            "invariant: route called on a Custom keyfun without a keyfun store; use try_route",
        )
    }

    /// Fallible [`Self::route`].
    ///
    /// Behaves identically to [`Self::route`] for the built-in
    /// `Std` / `BucketOnly` keyfuns (it never errors for them), and
    /// resolves a [`crate::datatypes::keyfun::KeyFun::Custom`]
    /// keyfun by running its WASM module through the attached
    /// keyfun store. The route bytes the module returns are fed to
    /// the cluster hash exactly as the built-in keyfuns' bytes are.
    ///
    /// # Errors
    ///
    /// Returns a [`crate::datatypes::keyfun::KeyFunError`] when the
    /// bucket selects a custom keyfun and the module is missing,
    /// the store is not wired, or the module traps / times out /
    /// exceeds its memory cap. Routing never panics or hangs on a
    /// bad module; the caller surfaces the error cleanly (the PBC
    /// server emits an `RpbErrorResp`).
    pub fn try_route(
        &self,
        bucket_type: &[u8],
        bucket: &[u8],
        key: &[u8],
    ) -> Result<RouteDecision, crate::datatypes::keyfun::KeyFunError> {
        let props = self.registry.resolve(bucket_type, bucket);
        let kf = props.effective_keyfun();
        let strategy = props.effective_strategy();
        let n_val = props.effective_n_val();
        let route_bytes = self.resolve_route_bytes(&kf, bucket, key)?;
        let key_hash = hash64(self.hash, &route_bytes);
        let plan = plan_replicas(
            self.ring.as_ref(),
            key_hash,
            n_val,
            strategy,
            ConsistencyLevel::DcOne,
        );
        Ok(RouteDecision {
            bucket_type: if bucket_type.is_empty() {
                b"default".to_vec()
            } else {
                bucket_type.to_vec()
            },
            props,
            route_bytes,
            key_hash,
            plan,
        })
    }

    /// Compute the pre-hash route bytes for a resolved keyfun.
    ///
    /// `Std` / `BucketOnly` use the pure path; `Custom` runs the
    /// named WASM module through the attached keyfun store.
    fn resolve_route_bytes(
        &self,
        kf: &KeyFun,
        bucket: &[u8],
        key: &[u8],
    ) -> Result<Vec<u8>, crate::datatypes::keyfun::KeyFunError> {
        match kf {
            KeyFun::Std | KeyFun::BucketOnly => kf.try_route_bytes(bucket, key),
            KeyFun::Custom(module_id) => self.resolve_custom_route_bytes(module_id, bucket, key),
        }
    }

    #[cfg(feature = "wasm")]
    fn resolve_custom_route_bytes(
        &self,
        module_id: &str,
        bucket: &[u8],
        key: &[u8],
    ) -> Result<Vec<u8>, crate::datatypes::keyfun::KeyFunError> {
        match &self.keyfun_store {
            Some(store) => store.route_bytes(module_id, bucket, key),
            None => Err(crate::datatypes::keyfun::KeyFunError::ModuleNotFound(
                module_id.to_string(),
            )),
        }
    }

    #[cfg(not(feature = "wasm"))]
    fn resolve_custom_route_bytes(
        &self,
        module_id: &str,
        _bucket: &[u8],
        _key: &[u8],
    ) -> Result<Vec<u8>, crate::datatypes::keyfun::KeyFunError> {
        let _ = self;
        Err(crate::datatypes::keyfun::KeyFunError::ModuleNotFound(
            module_id.to_string(),
        ))
    }
}

/// One operation forwarded by [`PeerOutbound::dispatch`] to a
/// peer's outbound channel.
///
/// Carries enough metadata to let a test assert the put/get/del
/// arrived at the right peer; production wiring will replace
/// this with the wire-level dnode framing in a follow-up slice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerOp {
    /// Forward a `RpbPutReq`-shaped operation.
    Put {
        /// Bucket type (`default` when the original request did
        /// not carry one).
        bucket_type: Vec<u8>,
        /// Bucket name.
        bucket: Vec<u8>,
        /// Key supplied by the client.
        key: Vec<u8>,
        /// Value bytes.
        value: Vec<u8>,
    },
    /// Forward a `RpbGetReq`-shaped operation.
    Get {
        /// Bucket type.
        bucket_type: Vec<u8>,
        /// Bucket name.
        bucket: Vec<u8>,
        /// Key supplied by the client.
        key: Vec<u8>,
    },
    /// Forward a `RpbDelReq`-shaped operation.
    Del {
        /// Bucket type.
        bucket_type: Vec<u8>,
        /// Bucket name.
        bucket: Vec<u8>,
        /// Key supplied by the client.
        key: Vec<u8>,
    },
}

/// Receiver of replica-peer dispatches.
///
/// The Riak PBC server calls [`Self::dispatch`] once per peer
/// in a [`RouteDecision`]'s replica list (only when the
/// strategy is [`ReplicationStrategy::Successors`]; topology
/// mode falls through to the existing dispatcher pipeline).
/// Implementors route the [`PeerOp`] to the matching peer's
/// outbound channel.
///
/// Production wiring uses the per-peer [`tokio::sync::mpsc`]
/// channels held by
/// [`dynomite::cluster::dispatch::ClusterDispatcher`]; tests
/// implement this trait against a fixture that records calls.
pub trait PeerOutbound: Send + Sync + std::fmt::Debug {
    /// Dispatch `op` to the peer at `peer_idx`. Errors are the
    /// caller's responsibility to surface; the trait contract
    /// is fire-and-forget so an unreachable peer does not block
    /// the request handler.
    fn dispatch(&self, peer_idx: u32, op: PeerOp) -> BoxFuture<'_, ()>;
}

/// Routing-hook bundle handed to
/// [`crate::server::serve_pbc_with_routing`].
#[derive(Clone, Debug)]
pub struct RoutingHooks {
    /// Bucket-aware request router.
    pub router: Arc<BucketRouter>,
    /// Per-peer outbound dispatcher invoked once per replica.
    pub outbound: Arc<dyn PeerOutbound>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::bucket_props::BucketProps;

    fn five_peer_ring() -> Arc<RingView> {
        let span = u64::from(u32::MAX);
        let pts: Vec<RingPoint> = (0..5u32)
            .map(|i| RingPoint::new(u64::from(i) * span / 5, i, "dc1", "r1"))
            .collect();
        Arc::new(RingView::new(pts))
    }

    use crate::replication::RingPoint;

    fn router_with_bucket(props: BucketProps) -> BucketRouter {
        let reg = Arc::new(BucketPropsRegistry::new_riak_defaults());
        reg.set(b"default", b"users", props);
        // Use a 32-bit hash so the produced u64 hash stays within
        // the prompt-specified u32-token range; with a u64 hash the
        // ring's wrap slot would dominate the distribution.
        BucketRouter::new(reg, five_peer_ring(), HashType::Murmur)
    }

    #[test]
    fn bucketonly_keyfun_collapses_keys_to_one_partition() {
        let router = router_with_bucket(BucketProps {
            keyfun: Some(KeyFun::BucketOnly),
            strategy: Some(ReplicationStrategy::Successors),
            n_val: Some(3),
            ..BucketProps::default()
        });
        let mut buckets: HashMap<u32, usize> = HashMap::new();
        for i in 0..100u32 {
            let key = format!("key-{i}");
            let d = router.route(b"default", b"users", key.as_bytes());
            let primary = d.primary_peer_idx().expect("successors yields primary");
            *buckets.entry(primary).or_insert(0) += 1;
        }
        assert_eq!(
            buckets.len(),
            1,
            "BUCKETONLY routes every key to one peer; saw {buckets:?}"
        );
    }

    #[test]
    fn std_keyfun_distributes_within_5_percent_of_uniform() {
        let router = router_with_bucket(BucketProps {
            keyfun: Some(KeyFun::Std),
            strategy: Some(ReplicationStrategy::Successors),
            n_val: Some(1),
            ..BucketProps::default()
        });
        let mut buckets: HashMap<u32, usize> = HashMap::new();
        // 10_000 keys gives a low-variance check; std deviation
        // for a Bernoulli-trial estimator with 5 buckets is
        // sqrt(N * p * (1 - p)) ~= 40, so the 5% relative
        // tolerance (= 100 keys absolute) clears noise reliably.
        let total: u32 = 10_000;
        for i in 0..total {
            let key = format!("key-{i}");
            let d = router.route(b"default", b"users", key.as_bytes());
            let primary = d.primary_peer_idx().expect("successors yields primary");
            *buckets.entry(primary).or_insert(0) += 1;
        }
        // 5 peers in the ring; each should see ~20% of keys.
        let expected = f64::from(total) / 5.0;
        let tolerance = expected * 0.05;
        for peer in 0..5u32 {
            let observed = f64::from(u32::try_from(*buckets.get(&peer).unwrap_or(&0)).unwrap());
            let delta = (observed - expected).abs();
            assert!(
                delta < tolerance,
                "peer {peer}: observed {observed}, expected {expected:.0}, delta {delta:.1} >= tol {tolerance:.1}"
            );
        }
    }

    #[test]
    fn route_bytes_match_keyfun_shape() {
        let router = router_with_bucket(BucketProps {
            keyfun: Some(KeyFun::BucketOnly),
            ..BucketProps::default()
        });
        let d = router.route(b"default", b"users", b"alice");
        assert_eq!(d.route_bytes, b"users");
        let router = router_with_bucket(BucketProps {
            keyfun: Some(KeyFun::Std),
            ..BucketProps::default()
        });
        let d = router.route(b"default", b"users", b"alice");
        assert_eq!(d.route_bytes, b"users/alice");
    }

    #[test]
    fn topology_strategy_yields_empty_replica_list() {
        let router = router_with_bucket(BucketProps {
            strategy: Some(ReplicationStrategy::Topology),
            ..BucketProps::default()
        });
        let d = router.route(b"default", b"users", b"alice");
        assert!(d.replica_list().is_empty());
        assert!(matches!(d.plan, ReplicationPlan::Topology(_)));
    }

    #[test]
    fn empty_bucket_type_normalises_to_default() {
        let router = router_with_bucket(BucketProps {
            keyfun: Some(KeyFun::BucketOnly),
            ..BucketProps::default()
        });
        let d = router.route(b"", b"users", b"alice");
        assert_eq!(d.bucket_type, b"default");
        assert_eq!(d.keyfun(), KeyFun::BucketOnly);
    }
}
