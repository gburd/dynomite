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
}

impl BucketProps {
    /// Resolve the effective [`KeyFun`], applying the supplied
    /// default when the field is unset.
    #[must_use]
    pub fn effective_keyfun_with(&self, default: KeyFun) -> KeyFun {
        self.keyfun.unwrap_or(default)
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
            p.keyfun = Some(inner.default_keyfun);
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
            keyfun: Some(inner.default_keyfun),
            strategy: Some(inner.default_strategy),
            n_val: Some(inner.default_n_val),
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
            },
        );
        let p = reg.resolve(b"default", b"users");
        assert_eq!(p.effective_keyfun(), KeyFun::BucketOnly);
        assert_eq!(p.effective_strategy(), ReplicationStrategy::Topology);
        assert_eq!(p.effective_n_val(), 5);
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
