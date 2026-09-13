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

    /// Count of PRIMARY-owner targets in the replica list (excludes
    /// any [`ReplicaTarget::is_fallback`] stand-in). PR / PW are
    /// enforced against this count, mirroring how R / W are enforced
    /// against [`Self::replica_list`]'s full length: a bucket's PR
    /// can never demand more acks than there are primaries to answer.
    #[must_use]
    pub fn primary_replica_count(&self) -> usize {
        self.replica_list()
            .iter()
            .filter(|t| !t.is_fallback)
            .count()
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
    /// Optional liveness source consulted when planning a
    /// [`ReplicationStrategy::Successors`] replica set. `None` (the
    /// default) means no liveness source is wired: [`Self::try_route`]
    /// falls back to [`crate::replication::plan_replicas`], which
    /// marks every planned replica a primary
    /// ([`dynomite::cluster::ReplicaTarget::is_fallback`] is always
    /// `false`) -- PR then behaves like R because there is nothing
    /// for it to distinguish. When set, [`Self::try_route`] instead
    /// calls [`crate::replication::plan_replicas_with_liveness`],
    /// which drops a down primary-window peer and backfills a
    /// fallback stand-in, so PR is enforced against the real
    /// primary-vs-fallback split.
    liveness: Option<Arc<dyn crate::replication::ReplicaLiveness>>,
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
            liveness: None,
            #[cfg(feature = "wasm")]
            keyfun_store: None,
        }
    }

    /// Attach a liveness source. After this call,
    /// [`Self::try_route`] plans replica sets with sloppy-quorum
    /// fallback substitution ([`crate::replication::plan_replicas_with_liveness`])
    /// instead of the no-liveness planner. Consumes and returns
    /// `self` for builder-style construction.
    #[must_use]
    pub fn with_liveness(mut self, liveness: Arc<dyn crate::replication::ReplicaLiveness>) -> Self {
        self.liveness = Some(liveness);
        self
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
        let plan = match (&self.liveness, strategy) {
            (Some(liveness), ReplicationStrategy::Successors) => {
                crate::replication::plan_replicas_with_liveness(
                    self.ring.as_ref(),
                    key_hash,
                    n_val,
                    liveness.as_ref(),
                )
            }
            _ => plan_replicas(
                self.ring.as_ref(),
                key_hash,
                n_val,
                strategy,
                ConsistencyLevel::DcOne,
            ),
        };
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

/// Ack byte a [`PeerOp::RepairPut`] reply carries when the write
/// landed but the replica's storage backend cannot confirm it reached
/// durable (synced / committed) storage. Counts toward the write
/// quorum `W` but not the durable-write quorum `DW`.
pub const ACK_STORED: u8 = 1;
/// Ack byte a [`PeerOp::RepairPut`] reply carries when the write both
/// landed AND the replica's storage backend confirms it reached
/// durable storage (a committed transaction). Counts toward both `W`
/// and `DW`.
pub const ACK_STORED_DURABLE: u8 = 2;

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
    /// Forward a CRDT data-type update operation. Carries the OP
    /// (not a merged value) plus the originating actor, so every
    /// replica applies it to its local CRDT state and converges by
    /// merge -- concurrent increments sum instead of overwriting.
    DtUpdate {
        /// Bucket type (`counters` / `sets`).
        bucket_type: Vec<u8>,
        /// Bucket name.
        bucket: Vec<u8>,
        /// Key supplied by the client.
        key: Vec<u8>,
        /// Serialized [`crate::crdt_store::CrdtOp`] payload.
        op: Vec<u8>,
    },
    /// Query a peer replica for its local CRDT state (read
    /// coordination). Unlike the other ops this expects a REPLY: the
    /// peer reads its local stored state for the key and returns the
    /// serialized CRDT state so the coordinator can merge the replica
    /// set and answer with the converged value.
    DtFetch {
        /// Bucket type (`counters` / `sets`).
        bucket_type: Vec<u8>,
        /// Bucket name.
        bucket: Vec<u8>,
        /// Key supplied by the client.
        key: Vec<u8>,
        /// CRDT type tag selecting the projection
        /// ([`crate::crdt_store::CrdtOp::type_tag`]).
        tag: u8,
    },
    /// Store a pre-serialized object `SiblingSet` verbatim on the
    /// replica. Unlike [`PeerOp::Put`] (which carries a bare value the
    /// receiver wraps in a fresh envelope with a seed context), this
    /// carries the coordinator's canonical storage bytes -- the full
    /// sibling set with per-object causal contexts -- so the replica
    /// holds a byte-identical, causally-correct copy. Used by the
    /// object write fan-out and by read-repair.
    ///
    /// A [`PeerOutbound::request`] reply for this op is one of
    /// [`ACK_STORED`] (landed, durability unconfirmed) or
    /// [`ACK_STORED_DURABLE`] (landed AND confirmed durable); any other
    /// non-empty byte is treated as [`ACK_STORED`] for backward
    /// compatibility with a transport that has not been upgraded, and
    /// an empty reply is not an ack at all.
    RepairPut {
        /// Bucket type (`default` when unset).
        bucket_type: Vec<u8>,
        /// Bucket name.
        bucket: Vec<u8>,
        /// Key supplied by the client.
        key: Vec<u8>,
        /// Canonical `SiblingSet` storage bytes, stored verbatim.
        storage: Vec<u8>,
    },
}

/// Compose the storage-layer bucket key from the bucket type and
/// bucket name.
///
/// Riak's object and CRDT identity is `(bucket_type, bucket, key)`,
/// but the [`crate::datastore`]-facing storage API keys by
/// `(bucket, key)` only. Folding the type into the bucket here keeps
/// distinct bucket types under the same bucket name in separate
/// keyspaces -- so a counter and a set under the same `(bucket,
/// key)` no longer collide -- without changing the public storage
/// trait. An empty or `default` type maps to a stable `default`
/// prefix, so objects written under the default type keep a single
/// canonical storage key. The separator byte `0x1f` (ASCII unit
/// separator) cannot appear in a Riak bucket-type name, so the
/// composition is unambiguous. Both the coordinator and every
/// replica compose identically (the [`PeerOp`] variants all carry
/// `bucket_type`), so replicas store byte-identical keys.
///
/// # Examples
///
/// ```
/// use dyniak::router::composite_storage_bucket;
/// // Distinct types under the same bucket name do not collide.
/// assert_ne!(
///     composite_storage_bucket(b"counters", b"crdts"),
///     composite_storage_bucket(b"sets", b"crdts"),
/// );
/// // Empty and "default" normalise to the same storage key.
/// assert_eq!(
///     composite_storage_bucket(b"", b"b"),
///     composite_storage_bucket(b"default", b"b"),
/// );
/// ```
#[must_use]
pub fn composite_storage_bucket(bucket_type: &[u8], bucket: &[u8]) -> Vec<u8> {
    let ty: &[u8] = if bucket_type.is_empty() || bucket_type == b"default" {
        b"default"
    } else {
        bucket_type
    };
    let mut out = Vec::with_capacity(ty.len() + 1 + bucket.len());
    out.extend_from_slice(ty);
    out.push(0x1f);
    out.extend_from_slice(bucket);
    out
}

/// Split a composite storage bucket key back into `(bucket_type,
/// bucket)`. The inverse of [`composite_storage_bucket`]: it splits on
/// the first `0x1f` separator. A key with no separator (a legacy
/// object written before the type fold) is returned as
/// `(b"default", whole)`, so pre-existing data resolves under the
/// default type.
///
/// # Examples
///
/// ```
/// use dyniak::router::{composite_storage_bucket, split_composite_storage_bucket};
/// let c = composite_storage_bucket(b"counters", b"crdts");
/// let (ty, b) = split_composite_storage_bucket(&c);
/// assert_eq!(ty, b"counters");
/// assert_eq!(b, b"crdts");
/// // A legacy (unfolded) key resolves under the default type.
/// let (ty, b) = split_composite_storage_bucket(b"plainbucket");
/// assert_eq!(ty, b"default");
/// assert_eq!(b, b"plainbucket");
/// ```
#[must_use]
pub fn split_composite_storage_bucket(composite: &[u8]) -> (&[u8], &[u8]) {
    match composite.iter().position(|&b| b == 0x1f) {
        Some(i) => (&composite[..i], &composite[i + 1..]),
        None => (b"default", composite),
    }
}
/// strategy is [`ReplicationStrategy::Successors`]; topology
/// Receiver of replica-peer dispatches.
///
/// The Riak PBC server calls `dispatch` once per peer in a
/// [`RouteDecision`]'s replica list (only when the multi-node
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

    /// Send `op` to the peer at `peer_idx` and await a reply, up to an
    /// implementation-defined timeout. Returns the reply payload, or
    /// `None` if the peer is unreachable, times out, or the transport
    /// does not support request/response. This is the read-
    /// coordination path (a `PeerOp::DtFetch` fans to the replica set
    /// and the coordinator merges the replies). The default returns
    /// `None` so a fire-and-forget-only transport needs no change.
    fn request(&self, peer_idx: u32, op: PeerOp) -> BoxFuture<'_, Option<Vec<u8>>> {
        let _ = (peer_idx, op);
        Box::pin(async { None })
    }
}

/// Routing-hook bundle handed to
/// [`crate::server::serve_pbc_with_routing`].
/// Runs a bucket's precommit hook over a write value before it
/// commits. Implemented by the WASM hook engine
/// ([`crate::precommit::PrecommitHooks`]); kept as a trait here so
/// [`RoutingHooks`] does not depend on the `wasm` feature.
pub trait PrecommitRunner: Send + Sync + std::fmt::Debug {
    /// Run the hook module `module_id` over `value`. Returns the
    /// value to store (possibly transformed), or an `Err(reason)` that
    /// vetoes the write, or a transport-level error string.
    fn run(&self, module_id: &str, value: &[u8]) -> Result<Vec<u8>, PrecommitVeto>;
}

/// Outcome of a rejected or failed precommit hook.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrecommitVeto {
    /// The hook vetoed the write with this reason.
    Rejected(String),
    /// The hook could not run (missing module, trap, timeout).
    Error(String),
}

/// Runs a bucket's postcommit hook after a write has committed.
/// Implemented by the WASM hook engine
/// ([`crate::precommit::PostcommitHooks`]); kept as a trait here so
/// [`RoutingHooks`] does not depend on the `wasm` feature.
///
/// Unlike [`PrecommitRunner`], the hook's outcome never changes the
/// write's result: it is a fire-and-forget notification run over the
/// already-committed value. A failing or vetoing hook is logged by the
/// implementation and otherwise ignored by the caller.
pub trait PostcommitRunner: Send + Sync + std::fmt::Debug {
    /// Run the hook module `module_id` over the committed `value`.
    /// The return value is informational only; callers do not act on
    /// it beyond logging.
    fn run(&self, module_id: &str, value: &[u8]);
}

/// Routing-hook bundle handed to
/// [`crate::server::serve_pbc_with_routing`].
#[derive(Clone, Debug)]
pub struct RoutingHooks {
    /// Bucket-aware request router.
    pub router: Arc<BucketRouter>,
    /// Per-peer outbound dispatcher invoked once per replica.
    pub outbound: Arc<dyn PeerOutbound>,
    /// This node's CRDT actor identity (datacenter + peer name).
    /// Every CRDT contribution this node coordinates is attributed
    /// to this actor, so per-actor counter columns sum correctly
    /// across replicas instead of overwriting.
    pub local_actor: crate::datatypes::ActorId,
    /// This node's peer index in the pool. Used to decide whether the
    /// coordinating node is itself a replica of a key: a CRDT write is
    /// applied locally only when this node is in the key's replica set,
    /// and is fanned to the OTHER replicas -- so data lands on replicas,
    /// not on whichever node the client happened to reach.
    pub local_peer_idx: u32,
    /// Optional precommit-hook runner. When a bucket names a
    /// `precommit_module`, an object write is run through it before it
    /// commits; the hook may transform the value or veto the write.
    pub precommit: Option<Arc<dyn PrecommitRunner>>,
    /// Optional postcommit-hook runner. When a bucket names a
    /// `postcommit_module`, a committed write is run through it as a
    /// fire-and-forget notification; the hook's outcome never affects
    /// the write's result.
    pub postcommit: Option<Arc<dyn PostcommitRunner>>,
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
