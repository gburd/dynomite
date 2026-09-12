//! Per-bucket-type, per-bucket property cache.
//!
//! [`BucketPropsRegistry`] is the in-memory store the Riak request
//! path consults to resolve `(bucket-type, bucket)` -> [`BucketProps`]
//! lookups. It backs the bucket-properties admin path
//! ([`crate::proto::pb::RpbGetBucketReq`] /
//! [`crate::proto::pb::RpbSetBucketReq`]) and the request-time route
//! decision in [`crate::router::BucketRouter`].
//!
//! Properties are sparse: only fields the operator actually set are
//! stored. Defaults (Riak-mode vs non-Riak-mode) live next to each
//! field accessor and are applied on read so a fresh registry returns
//! sensible values for any bucket.
//!
//! The store is intentionally narrow: it tracks the v0.0.x slice's
//! routing-relevant knobs ([`KeyFun`], [`ReplicationStrategy`],
//! `n_val`) and ignores the rest. The wire-level
//! [`crate::proto::pb::RpbBucketProps`] message keeps every
//! published field; the registry only holds what the dispatcher
//! needs.
//!
//! # Examples
//!
//! ```
//! use dyniak::{BucketProps, BucketPropsRegistry, ReplicationStrategy};
//! use dyniak::datatypes::keyfun::KeyFun;
//!
//! let mut reg = BucketPropsRegistry::new_riak_defaults();
//! reg.set(
//!     b"default",
//!     b"users",
//!     BucketProps {
//!         keyfun: Some(KeyFun::BucketOnly),
//!         strategy: Some(ReplicationStrategy::Successors),
//!         n_val: Some(3),
//!         ..Default::default()
//!     },
//! );
//! let props = reg.resolve(b"default", b"users");
//! assert_eq!(props.effective_keyfun(), KeyFun::BucketOnly);
//! assert_eq!(props.effective_strategy(), ReplicationStrategy::Successors);
//! ```

use std::collections::HashMap;
use std::sync::RwLock;

use crate::datatypes::keyfun::KeyFun;
use crate::replication::ReplicationStrategy;

/// Sparse per-bucket properties stored in [`BucketPropsRegistry`].
///
/// Unset fields fall through to the registry's mode-aware defaults
/// at lookup time; see [`BucketProps::effective_keyfun`] and
/// [`BucketProps::effective_strategy`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketProps {
    /// `chash_keyfun` selector. `None` means "use default".
    pub keyfun: Option<KeyFun>,
    /// `replication_strategy` selector. `None` means "use default".
    pub strategy: Option<ReplicationStrategy>,
    /// `n_val` replication factor. `None` means "use default" (3).
    pub n_val: Option<u8>,
    /// Module id of the operator-supplied keyfun WASM module,
    /// when [`Self::keyfun`] selects [`KeyFun::Custom`]. Riak's
    /// wire `chash_keyfun = CUSTOM` selector carries no module
    /// name, so dyniak takes it from this field. The bucket-
    /// property write path rejects a `CUSTOM` selection that
    /// names no module (or names an unregistered one), so a
    /// `Some` here is always a module the keyfun store knows.
    pub custom_keyfun_module: Option<String>,
    /// Whether the bucket keeps siblings on a concurrent write
    /// (Riak's `allow_mult`). `None` means the default (`false`:
    /// concurrent writes collapse to one value). `Some(true)` retains
    /// concurrent siblings so a read can surface them.
    pub allow_mult: Option<bool>,
    /// Id of the WASM precommit-hook module for this bucket, if any.
    /// A write is run through this hook before it commits; the hook may
    /// transform the value or veto the write. `None` means no hook.
    pub precommit_module: Option<String>,
    /// Id of the WASM postcommit-hook module for this bucket, if any.
    /// Run once a write has committed, over the committed value. The
    /// hook's outcome never affects the write's result (fire-and-
    /// forget notification); `None` means no hook.
    pub postcommit_module: Option<String>,
    /// Object time-to-live in seconds (Riak's `ttl` bucket
    /// property). `None` or `Some(0)` means no expiry: live
    /// objects are never reaped by age. A non-zero value is
    /// carried to the reaper as
    /// [`crate::reaper::ReaperConfig::object_ttl_seconds`].
    pub ttl_seconds: Option<u64>,
    /// Default read quorum (`r`). `None` means `quorum`. Symbolic
    /// values (`one`/`quorum`/`all`) use the reserved magic values in
    /// [`crate::quorum`]; a literal is a count.
    pub r: Option<u32>,
    /// Default write quorum (`w`). `None` means `quorum`.
    pub w: Option<u32>,
    /// Default primary-read quorum (`pr`). `None` means `0` (no primary
    /// requirement).
    pub pr: Option<u32>,
    /// Default primary-write quorum (`pw`). `None` means `0`.
    pub pw: Option<u32>,
    /// Default durable-write quorum (`dw`). `None` means `quorum`.
    pub dw: Option<u32>,
}

impl BucketProps {
    /// Resolve the effective [`KeyFun`], applying the supplied
    /// default when the field is unset.
    ///
    /// When the stored keyfun is [`KeyFun::Custom`] the returned
    /// variant carries the module id from
    /// [`Self::custom_keyfun_module`] (the wire selector alone
    /// does not name the module). If the field is empty the
    /// returned `Custom` id is empty too; the router treats an
    /// empty / unregistered id as a clean
    /// [`crate::datatypes::keyfun::KeyFunError::ModuleNotFound`].
    #[must_use]
    pub fn effective_keyfun_with(&self, default: KeyFun) -> KeyFun {
        match self.keyfun.clone() {
            Some(KeyFun::Custom(id)) => {
                let module = self.custom_keyfun_module.clone().unwrap_or_else(|| {
                    if id.is_empty() {
                        String::new()
                    } else {
                        id
                    }
                });
                KeyFun::Custom(module)
            }
            Some(other) => other,
            None => default,
        }
    }

    /// Resolve the effective [`ReplicationStrategy`], applying the
    /// supplied default when the field is unset.
    #[must_use]
    pub fn effective_strategy_with(&self, default: ReplicationStrategy) -> ReplicationStrategy {
        self.strategy.unwrap_or(default)
    }

    /// Resolve the effective `n_val`, applying the supplied
    /// default when the field is unset.
    #[must_use]
    pub fn effective_n_val_with(&self, default: u8) -> u8 {
        self.n_val.unwrap_or(default)
    }

    /// Resolve the effective object TTL in seconds. `0` means no
    /// expiry (the default): an unset `ttl_seconds` and an explicit
    /// `Some(0)` both disable object expiry.
    #[must_use]
    pub fn effective_ttl_seconds(&self) -> u64 {
        self.ttl_seconds.unwrap_or(0)
    }

    /// Resolve the effective `allow_mult`. Defaults to `false`
    /// (concurrent writes collapse to a single value).
    #[must_use]
    pub fn effective_allow_mult(&self) -> bool {
        self.allow_mult.unwrap_or(false)
    }

    /// The precommit-hook module id for this bucket, if any.
    #[must_use]
    pub fn precommit_module(&self) -> Option<&str> {
        self.precommit_module.as_deref()
    }

    /// The postcommit-hook module id for this bucket, if any.
    #[must_use]
    pub fn postcommit_module(&self) -> Option<&str> {
        self.postcommit_module.as_deref()
    }

    /// Resolve the effective read quorum `R` for `n_val`, applying a
    /// per-request override when supplied (Riak precedence: request >
    /// bucket default > `quorum`).
    #[must_use]
    pub fn effective_r(&self, n_val: u8, request: Option<u32>) -> u32 {
        crate::quorum::resolve(request, n_val, self.r)
    }

    /// Resolve the effective write quorum `W` for `n_val`.
    #[must_use]
    pub fn effective_w(&self, n_val: u8, request: Option<u32>) -> u32 {
        crate::quorum::resolve(request, n_val, self.w)
    }

    /// Resolve the effective primary-read quorum `PR` for `n_val`.
    /// Defaults to `0` (no primary requirement).
    #[must_use]
    pub fn effective_pr(&self, n_val: u8, request: Option<u32>) -> u32 {
        match request.or(self.pr) {
            None => 0,
            some => crate::quorum::resolve(some, n_val, self.pr),
        }
    }

    /// Resolve the effective primary-write quorum `PW` for `n_val`.
    /// Defaults to `0`.
    #[must_use]
    pub fn effective_pw(&self, n_val: u8, request: Option<u32>) -> u32 {
        match request.or(self.pw) {
            None => 0,
            some => crate::quorum::resolve(some, n_val, self.pw),
        }
    }

    /// Resolve the effective durable-write quorum `DW` for `n_val`.
    /// Unlike PR / PW (which default to `0`, no requirement), DW
    /// follows R / W precedence: request override, then bucket
    /// default, then `quorum`. This mirrors Riak, where DW's default
    /// is `quorum` rather than `0`.
    #[must_use]
    pub fn effective_dw(&self, n_val: u8, request: Option<u32>) -> u32 {
        crate::quorum::resolve(request, n_val, self.dw)
    }
    /// Convenience: effective [`KeyFun`] using
    /// [`KeyFun::default`] (`Std`) when unset.
    #[must_use]
    pub fn effective_keyfun(&self) -> KeyFun {
        self.effective_keyfun_with(KeyFun::default())
    }

    /// Convenience: effective [`ReplicationStrategy`] using
    /// [`ReplicationStrategy::default`] (`Topology`) when unset.
    #[must_use]
    pub fn effective_strategy(&self) -> ReplicationStrategy {
        self.effective_strategy_with(ReplicationStrategy::default())
    }

    /// Convenience: effective `n_val` using `3` when unset
    /// (matches Riak's documented default).
    #[must_use]
    pub fn effective_n_val(&self) -> u8 {
        self.effective_n_val_with(3)
    }
}

/// Cache of `(bucket-type, bucket-name)` -> [`BucketProps`].
///
/// The registry holds two layers of defaults: a per-registry
/// fallback (the "mode default" -- Riak-mode pools start with
/// [`ReplicationStrategy::Successors`], non-Riak-mode pools with
/// [`ReplicationStrategy::Topology`]), and a per-bucket override
/// recorded by the operator through
/// [`crate::proto::pb::RpbSetBucketReq`].
///
/// Lookups consult the per-bucket entry first, then fall through
/// to the registry default when a field is unset.
#[derive(Debug)]
pub struct BucketPropsRegistry {
    inner: RwLock<RegistryInner>,
}

#[derive(Debug)]
struct RegistryInner {
    /// `(bucket-type, bucket)` -> overrides. Bucket-type defaults
    /// to `default` when callers pass an empty slice.
    by_bucket: HashMap<(Vec<u8>, Vec<u8>), BucketProps>,
    /// Mode-aware default keyfun.
    default_keyfun: KeyFun,
    /// Mode-aware default replication strategy.
    default_strategy: ReplicationStrategy,
    /// Default replication factor (Riak's documented default is 3).
    default_n_val: u8,
}

impl BucketPropsRegistry {
    /// Build a registry with the non-Riak-mode defaults
    /// ([`KeyFun::Std`], [`ReplicationStrategy::Topology`],
    /// `n_val = 3`).
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(RegistryInner {
                by_bucket: HashMap::new(),
                default_keyfun: KeyFun::Std,
                default_strategy: ReplicationStrategy::Topology,
                default_n_val: 3,
            }),
        }
    }

    /// Build a registry with the Riak-mode defaults: [`KeyFun::Std`]
    /// (Riak's `chash_std_keyfun` is the canonical default),
    /// [`ReplicationStrategy::Successors`], `n_val = 3`. Operators
    /// override per-bucket-type via [`Self::set`].
    #[must_use]
    pub fn new_riak_defaults() -> Self {
        Self {
            inner: RwLock::new(RegistryInner {
                by_bucket: HashMap::new(),
                default_keyfun: KeyFun::Std,
                default_strategy: ReplicationStrategy::Successors,
                default_n_val: 3,
            }),
        }
    }

    /// Override a per-bucket entry. Subsequent
    /// [`Self::resolve`] calls return the supplied props
    /// (with mode defaults still applied to unset fields).
    pub fn set(&self, bucket_type: &[u8], bucket: &[u8], props: BucketProps) {
        let key = (Self::norm_type(bucket_type), bucket.to_vec());
        let mut inner = self.inner.write().expect("registry rwlock poisoned");
        inner.by_bucket.insert(key, props);
    }

    /// Resolve `(bucket-type, bucket)` to a fully populated
    /// [`BucketProps`]. Unset fields are filled in from the
    /// registry-level defaults so the returned value has every
    /// field present.
    #[must_use]
    pub fn resolve(&self, bucket_type: &[u8], bucket: &[u8]) -> BucketProps {
        let key = (Self::norm_type(bucket_type), bucket.to_vec());
        let inner = self.inner.read().expect("registry rwlock poisoned");
        let mut p = inner.by_bucket.get(&key).cloned().unwrap_or_default();
        if p.keyfun.is_none() {
            p.keyfun = Some(inner.default_keyfun.clone());
        }
        if p.strategy.is_none() {
            p.strategy = Some(inner.default_strategy);
        }
        if p.n_val.is_none() {
            p.n_val = Some(inner.default_n_val);
        }
        p
    }

    /// Read the mode-aware defaults. Used by the bucket-properties
    /// PBC handler when a bucket has no override on file.
    #[must_use]
    pub fn defaults(&self) -> BucketProps {
        let inner = self.inner.read().expect("registry rwlock poisoned");
        BucketProps {
            keyfun: Some(inner.default_keyfun.clone()),
            strategy: Some(inner.default_strategy),
            n_val: Some(inner.default_n_val),
            custom_keyfun_module: None,
            allow_mult: None,
            precommit_module: None,
            postcommit_module: None,
            ttl_seconds: None,
            r: None,
            w: None,
            pr: None,
            pw: None,
            dw: None,
        }
    }

    /// Replace the registry-level default replication strategy.
    /// Useful for the dynomited binary which decides the default
    /// at startup based on the pool's `data_store` value.
    pub fn set_default_strategy(&self, strategy: ReplicationStrategy) {
        let mut inner = self.inner.write().expect("registry rwlock poisoned");
        inner.default_strategy = strategy;
    }

    fn norm_type(bucket_type: &[u8]) -> Vec<u8> {
        if bucket_type.is_empty() {
            b"default".to_vec()
        } else {
            bucket_type.to_vec()
        }
    }
}

impl Default for BucketPropsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_registry_returns_mode_defaults() {
        let reg = BucketPropsRegistry::new();
        let p = reg.resolve(b"default", b"users");
        assert_eq!(p.effective_keyfun(), KeyFun::Std);
        assert_eq!(p.effective_strategy(), ReplicationStrategy::Topology);
        assert_eq!(p.effective_n_val(), 3);
    }

    #[test]
    fn riak_default_swaps_strategy_to_successors() {
        let reg = BucketPropsRegistry::new_riak_defaults();
        let p = reg.resolve(b"default", b"users");
        assert_eq!(p.effective_strategy(), ReplicationStrategy::Successors);
    }

    #[test]
    fn override_takes_precedence() {
        let reg = BucketPropsRegistry::new_riak_defaults();
        reg.set(
            b"default",
            b"users",
            BucketProps {
                keyfun: Some(KeyFun::BucketOnly),
                strategy: Some(ReplicationStrategy::Topology),
                n_val: Some(5),
                ..BucketProps::default()
            },
        );
        let p = reg.resolve(b"default", b"users");
        assert_eq!(p.effective_keyfun(), KeyFun::BucketOnly);
        assert_eq!(p.effective_strategy(), ReplicationStrategy::Topology);
        assert_eq!(p.effective_n_val(), 5);
    }

    #[test]
    fn ttl_defaults_to_zero_and_round_trips() {
        let reg = BucketPropsRegistry::new_riak_defaults();
        // Unset ttl resolves to 0 (no expiry).
        assert_eq!(reg.resolve(b"default", b"c").effective_ttl_seconds(), 0);
        // A stored ttl round-trips through resolve.
        reg.set(
            b"default",
            b"cache",
            BucketProps {
                ttl_seconds: Some(3600),
                ..BucketProps::default()
            },
        );
        assert_eq!(
            reg.resolve(b"default", b"cache").effective_ttl_seconds(),
            3600
        );
        // An explicit zero is also no-expiry.
        assert_eq!(
            BucketProps {
                ttl_seconds: Some(0),
                ..BucketProps::default()
            }
            .effective_ttl_seconds(),
            0
        );
    }

    #[test]
    fn quorum_resolves_request_over_bucket_default_over_quorum() {
        use crate::quorum::{QUORUM_ALL, QUORUM_ONE};
        // Bucket with no r/w defaults: R and W default to quorum (2 of 3).
        let p = BucketProps::default();
        assert_eq!(p.effective_r(3, None), 2);
        assert_eq!(p.effective_w(3, None), 2);
        // A per-request override wins.
        assert_eq!(p.effective_r(3, Some(QUORUM_ALL)), 3);
        assert_eq!(p.effective_w(3, Some(QUORUM_ONE)), 1);
        // A bucket default of all, no request -> all.
        let all = BucketProps {
            r: Some(QUORUM_ALL),
            w: Some(QUORUM_ONE),
            ..BucketProps::default()
        };
        assert_eq!(all.effective_r(3, None), 3);
        assert_eq!(all.effective_w(3, None), 1);
        // A request still overrides the bucket default.
        assert_eq!(all.effective_r(3, Some(QUORUM_ONE)), 1);
        // PR/PW default to 0 (no primary requirement).
        assert_eq!(p.effective_pr(3, None), 0);
        assert_eq!(p.effective_pw(3, None), 0);
        assert_eq!(p.effective_pr(3, Some(QUORUM_ALL)), 3);
        // DW defaults to quorum, like R/W (not 0 like PR/PW).
        assert_eq!(p.effective_dw(3, None), 2);
        assert_eq!(p.effective_dw(3, Some(QUORUM_ALL)), 3);
        let dw_all = BucketProps {
            dw: Some(QUORUM_ALL),
            ..BucketProps::default()
        };
        assert_eq!(dw_all.effective_dw(3, None), 3);
        assert_eq!(dw_all.effective_dw(3, Some(QUORUM_ONE)), 1);
    }

    #[test]
    fn empty_bucket_type_normalises_to_default() {
        let reg = BucketPropsRegistry::new();
        reg.set(
            b"",
            b"users",
            BucketProps {
                keyfun: Some(KeyFun::BucketOnly),
                ..BucketProps::default()
            },
        );
        // Both lookups hit the same entry.
        assert_eq!(
            reg.resolve(b"", b"users").effective_keyfun(),
            KeyFun::BucketOnly
        );
        assert_eq!(
            reg.resolve(b"default", b"users").effective_keyfun(),
            KeyFun::BucketOnly
        );
    }

    #[test]
    fn missing_buckets_fall_through_to_defaults() {
        let reg = BucketPropsRegistry::new_riak_defaults();
        let p = reg.resolve(b"default", b"never-set");
        assert_eq!(p.effective_strategy(), ReplicationStrategy::Successors);
    }

    #[test]
    fn custom_keyfun_takes_module_id_from_field() {
        // The explicit module field wins over the id embedded in
        // the variant.
        let p = BucketProps {
            keyfun: Some(KeyFun::Custom(String::new())),
            custom_keyfun_module: Some("reverse".to_string()),
            ..BucketProps::default()
        };
        assert_eq!(p.effective_keyfun(), KeyFun::Custom("reverse".to_string()));

        // With no module field the embedded id is used.
        let p2 = BucketProps {
            keyfun: Some(KeyFun::Custom("embedded".to_string())),
            ..BucketProps::default()
        };
        assert_eq!(
            p2.effective_keyfun(),
            KeyFun::Custom("embedded".to_string())
        );

        // Neither set: an empty Custom id, which the router treats
        // as ModuleNotFound.
        let p3 = BucketProps {
            keyfun: Some(KeyFun::Custom(String::new())),
            ..BucketProps::default()
        };
        assert_eq!(p3.effective_keyfun(), KeyFun::Custom(String::new()));
    }

    #[test]
    fn set_default_strategy_changes_unconfigured_buckets_only() {
        let reg = BucketPropsRegistry::new_riak_defaults();
        reg.set(
            b"default",
            b"explicit",
            BucketProps {
                strategy: Some(ReplicationStrategy::Topology),
                ..BucketProps::default()
            },
        );
        reg.set_default_strategy(ReplicationStrategy::Topology);
        assert_eq!(
            reg.resolve(b"default", b"never-set").effective_strategy(),
            ReplicationStrategy::Topology,
        );
        // Per-bucket override unchanged.
        assert_eq!(
            reg.resolve(b"default", b"explicit").effective_strategy(),
            ReplicationStrategy::Topology,
        );
    }
}
