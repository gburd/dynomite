//! Pre-hash key shaping for Riak bucket routing.
//!
//! Riak exposes a per-bucket-property `chash_keyfun` knob that
//! determines which bytes are fed to the cluster's hash function:
//!
//! * [`KeyFun::Std`] -- the default. The hash input is
//!   `<bucket>/<key>`, so each key in the bucket is distributed
//!   across the ring independently.
//! * [`KeyFun::BucketOnly`] -- the hash input is `<bucket>`
//!   alone, so every key in the bucket maps to the same
//!   partition. Useful for bucket-as-shard layouts.
//! * [`KeyFun::Custom`] -- the hash input is whatever an
//!   operator-supplied WebAssembly module decides. The variant
//!   carries the module id; the actual byte-shaping happens in
//!   [`crate::router::BucketRouter`], which owns the keyfun WASM
//!   store. This is the dyniak realisation of Riak's
//!   user-defined `{chash_keyfun, {modfun, Mod, Fun}}` keyfun.
//!
//! This module models the in-memory choice and produces the byte
//! sequence the dispatcher feeds to the existing distribution
//! machinery (`vnode` or `random-slicing`). It does NOT touch the
//! distribution code itself; the distribution layer keeps
//! consuming the already-shaped bytes verbatim.
//!
//! # Wire mapping
//!
//! [`KeyFun::from_wire`] / [`KeyFun::to_wire`] convert between the
//! protobuf [`crate::proto::pb::RpbBucketProps::chash_keyfun`]
//! numeric selector and the in-memory enum. The `CUSTOM = 99`
//! selector carries no module id on the wire (Riak names the
//! module separately via `{modfun, Mod, Fun}`); dyniak therefore
//! decodes `99` to [`KeyFun::Custom`] with an EMPTY module id and
//! relies on the bucket-property field
//! [`crate::bucket_props::BucketProps::custom_keyfun_module`] to
//! name the registered WASM module. The bucket-property write path
//! ([`crate::server`]) fills that field in.
//!
//! # Examples
//!
//! ```
//! use dyniak::datatypes::keyfun::KeyFun;
//!
//! let std = KeyFun::Std;
//! assert_eq!(std.route_bytes(b"users", b"alice"), b"users/alice");
//!
//! let bo = KeyFun::BucketOnly;
//! assert_eq!(bo.route_bytes(b"users", b"alice"), b"users");
//! assert_eq!(bo.route_bytes(b"users", b"bob"), b"users");
//! ```

use crate::proto::pb::{CHASH_KEYFUN_BUCKETONLY, CHASH_KEYFUN_CUSTOM, CHASH_KEYFUN_STD};

/// Pre-hash strategy applied before the cluster's distribution
/// machinery sees the routing bytes.
///
/// The variants line up with Riak's `chash_keyfun` selectors.
/// Default is [`KeyFun::Std`].
///
/// [`KeyFun::Custom`] carries the id of the registered keyfun WASM
/// module that shapes the routing bytes. Because of that owned
/// `String` the enum is `Clone` rather than `Copy`; the `Std` and
/// `BucketOnly` variants stay cheap to clone (no allocation).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum KeyFun {
    /// Hash `<bucket>/<key>` (Riak's `chash_std_keyfun`).
    /// Every key is independently distributed across the ring.
    #[default]
    Std,
    /// Hash `<bucket>` only (Riak's `chash_bucketonly_keyfun`).
    /// Every key in the bucket maps to the same partition.
    BucketOnly,
    /// Hash whatever the named operator-supplied WASM module
    /// returns for `(bucket, key)` (Riak's user-defined
    /// `{chash_keyfun, {modfun, Mod, Fun}}`). The `String` is the
    /// module id registered with the keyfun WASM store.
    Custom(String),
}

/// Errors produced when decoding a wire `chash_keyfun` value or
/// running a [`KeyFun::Custom`] module.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum KeyFunError {
    /// [`Self::route_bytes`] was called on a [`KeyFun::Custom`]
    /// keyfun, which needs the WASM store. Callers must route a
    /// `Custom` keyfun through
    /// [`crate::router::BucketRouter`], which owns the store.
    #[error("chash_keyfun: CUSTOM keyfun {0:?} must be routed through the WASM keyfun store")]
    Custom(String),
    /// The bucket selected `CUSTOM` but named no module, or the
    /// named module is not registered with the keyfun store.
    #[error("chash_keyfun: CUSTOM keyfun module {0:?} is not registered")]
    ModuleNotFound(String),
    /// The WASM module trapped, ran out of fuel, or hit its
    /// wall-clock deadline while computing the route bytes.
    #[error("chash_keyfun: CUSTOM keyfun module {module:?} failed: {message}")]
    Runtime {
        /// Module id that failed.
        module: String,
        /// Human-readable failure reason.
        message: String,
    },
    /// The WASM module asked for more linear memory than the
    /// configured cap allows.
    #[error("chash_keyfun: CUSTOM keyfun module {0:?} exceeded its memory limit")]
    MemoryLimit(String),
    /// The wire value was outside the documented enum range.
    #[error("chash_keyfun: unknown selector {0}")]
    Unknown(u32),
}

impl KeyFun {
    /// Produce the byte sequence the cluster hash function consumes
    /// for `(bucket, key)` under this keyfun.
    ///
    /// `Std` returns the canonical `<bucket>/<key>` shape; the
    /// separator is the literal forward slash so the bytes match
    /// the request keys downstream code (notably
    /// [`dynomite::proto::redis::bucket_name`]) already produces.
    /// `BucketOnly` returns just the bucket name.
    ///
    /// # Panics
    ///
    /// Panics if called on [`KeyFun::Custom`]: a custom keyfun
    /// needs the WASM keyfun store, which this pure method does
    /// not have. The router resolves `Custom` keyfuns through
    /// [`crate::router::BucketRouter::route`] before this method
    /// is reached, so production code never trips this. Use
    /// [`Self::try_route_bytes`] when a `Custom` keyfun is
    /// possible and a panic is not acceptable.
    ///
    /// # Examples
    ///
    /// ```
    /// use dyniak::datatypes::keyfun::KeyFun;
    /// assert_eq!(KeyFun::Std.route_bytes(b"b", b"k"), b"b/k");
    /// assert_eq!(KeyFun::BucketOnly.route_bytes(b"b", b"k"), b"b");
    /// ```
    #[must_use]
    pub fn route_bytes(&self, bucket: &[u8], key: &[u8]) -> Vec<u8> {
        self.try_route_bytes(bucket, key)
            .expect("invariant: route_bytes called on KeyFun::Custom; route through BucketRouter")
    }

    /// Fallible analogue of [`Self::route_bytes`] for the pure
    /// (storeless) variants.
    ///
    /// Returns the shaped bytes for `Std` / `BucketOnly`, and
    /// [`KeyFunError::Custom`] for [`KeyFun::Custom`] (whose bytes
    /// can only be produced by running the WASM module via the
    /// router's keyfun store).
    ///
    /// # Errors
    ///
    /// [`KeyFunError::Custom`] when `self` is [`KeyFun::Custom`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dyniak::datatypes::keyfun::{KeyFun, KeyFunError};
    /// assert_eq!(KeyFun::Std.try_route_bytes(b"b", b"k").unwrap(), b"b/k");
    /// let id = "rev".to_string();
    /// assert_eq!(
    ///     KeyFun::Custom(id.clone()).try_route_bytes(b"b", b"k"),
    ///     Err(KeyFunError::Custom(id)),
    /// );
    /// ```
    pub fn try_route_bytes(&self, bucket: &[u8], key: &[u8]) -> Result<Vec<u8>, KeyFunError> {
        match self {
            Self::Std => {
                let mut out = Vec::with_capacity(bucket.len() + 1 + key.len());
                out.extend_from_slice(bucket);
                out.push(b'/');
                out.extend_from_slice(key);
                Ok(out)
            }
            Self::BucketOnly => Ok(bucket.to_vec()),
            Self::Custom(id) => Err(KeyFunError::Custom(id.clone())),
        }
    }

    /// Append the route-input bytes for `(bucket, key)` onto
    /// `buf`. The buffer is NOT cleared first; reuse across calls
    /// is left to the caller.
    ///
    /// # Panics
    ///
    /// Panics on [`KeyFun::Custom`] for the same reason as
    /// [`Self::route_bytes`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dyniak::datatypes::keyfun::KeyFun;
    /// let mut buf = Vec::new();
    /// KeyFun::Std.route_bytes_into(b"users", b"alice", &mut buf);
    /// assert_eq!(buf, b"users/alice");
    /// ```
    pub fn route_bytes_into(&self, bucket: &[u8], key: &[u8], buf: &mut Vec<u8>) {
        match self {
            Self::Std => {
                buf.extend_from_slice(bucket);
                buf.push(b'/');
                buf.extend_from_slice(key);
            }
            Self::BucketOnly => {
                buf.extend_from_slice(bucket);
            }
            Self::Custom(_) => {
                panic!(
                    "invariant: route_bytes_into called on KeyFun::Custom; route through BucketRouter"
                );
            }
        }
    }

    /// `true` when this keyfun needs the WASM keyfun store to
    /// produce its route bytes (i.e. it is [`KeyFun::Custom`]).
    #[must_use]
    pub fn is_custom(&self) -> bool {
        matches!(self, Self::Custom(_))
    }

    /// The custom keyfun module id, when `self` is
    /// [`KeyFun::Custom`].
    #[must_use]
    pub fn custom_module(&self) -> Option<&str> {
        match self {
            Self::Custom(id) => Some(id.as_str()),
            Self::Std | Self::BucketOnly => None,
        }
    }

    /// Length of the byte sequence [`Self::route_bytes`] would
    /// return for the pure variants. `None` for [`KeyFun::Custom`]
    /// (the length is only known after running the module).
    #[must_use]
    pub fn route_len(&self, bucket: &[u8], key: &[u8]) -> Option<usize> {
        match self {
            Self::Std => Some(bucket.len() + 1 + key.len()),
            Self::BucketOnly => Some(bucket.len()),
            Self::Custom(_) => None,
        }
    }

    /// Convert a wire `chash_keyfun` selector to the in-memory
    /// enum.
    ///
    /// The reserved `CUSTOM = 99` selector decodes to
    /// `KeyFun::Custom(String::new())` -- an empty module id. The
    /// wire selector alone carries no module name (Riak names it
    /// separately via `{modfun, Mod, Fun}`), so the caller MUST
    /// fill the id in from
    /// [`crate::bucket_props::BucketProps::custom_keyfun_module`].
    ///
    /// # Errors
    ///
    /// [`KeyFunError::Unknown`] when the wire value is outside the
    /// documented enum range.
    pub fn from_wire(value: u32) -> Result<Self, KeyFunError> {
        match value {
            CHASH_KEYFUN_STD => Ok(Self::Std),
            CHASH_KEYFUN_BUCKETONLY => Ok(Self::BucketOnly),
            CHASH_KEYFUN_CUSTOM => Ok(Self::Custom(String::new())),
            other => Err(KeyFunError::Unknown(other)),
        }
    }

    /// Convert the in-memory enum to a wire `chash_keyfun`
    /// selector ready to drop into
    /// [`crate::proto::pb::RpbBucketProps::chash_keyfun`].
    #[must_use]
    pub fn to_wire(&self) -> u32 {
        match self {
            Self::Std => CHASH_KEYFUN_STD,
            Self::BucketOnly => CHASH_KEYFUN_BUCKETONLY,
            Self::Custom(_) => CHASH_KEYFUN_CUSTOM,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn std_shapes_bucket_slash_key() {
        let bytes = KeyFun::Std.route_bytes(b"users", b"alice");
        assert_eq!(bytes, b"users/alice");
    }

    #[test]
    fn bucket_only_drops_key() {
        let a = KeyFun::BucketOnly.route_bytes(b"users", b"alice");
        let b = KeyFun::BucketOnly.route_bytes(b"users", b"bob");
        assert_eq!(a, b);
        assert_eq!(a, b"users");
    }

    #[test]
    fn route_bytes_into_matches_route_bytes() {
        for kf in [KeyFun::Std, KeyFun::BucketOnly] {
            let owned = kf.route_bytes(b"b", b"k");
            let mut buf = Vec::new();
            kf.route_bytes_into(b"b", b"k", &mut buf);
            assert_eq!(owned, buf, "kf = {kf:?}");
            assert_eq!(owned.len(), kf.route_len(b"b", b"k").unwrap());
        }
    }

    #[test]
    fn custom_try_route_bytes_is_error() {
        let kf = KeyFun::Custom("rev".to_string());
        assert_eq!(
            kf.try_route_bytes(b"b", b"k"),
            Err(KeyFunError::Custom("rev".to_string()))
        );
        assert!(kf.is_custom());
        assert_eq!(kf.custom_module(), Some("rev"));
        assert_eq!(kf.route_len(b"b", b"k"), None);
    }

    #[test]
    #[should_panic(expected = "KeyFun::Custom")]
    fn custom_route_bytes_panics() {
        let _ = KeyFun::Custom("rev".to_string()).route_bytes(b"b", b"k");
    }

    #[test]
    fn from_wire_round_trips() {
        for kf in [KeyFun::Std, KeyFun::BucketOnly] {
            let w = kf.to_wire();
            let back = KeyFun::from_wire(w).expect("known");
            assert_eq!(back, kf);
        }
    }

    #[test]
    fn from_wire_custom_yields_empty_module_id() {
        assert_eq!(KeyFun::from_wire(99), Ok(KeyFun::Custom(String::new())));
        assert_eq!(KeyFun::Custom("anything".to_string()).to_wire(), 99);
    }

    #[test]
    fn from_wire_rejects_unknown() {
        assert_eq!(KeyFun::from_wire(7), Err(KeyFunError::Unknown(7)));
    }

    #[test]
    fn default_is_std() {
        assert_eq!(KeyFun::default(), KeyFun::Std);
    }

    #[test]
    fn empty_inputs_are_total() {
        assert_eq!(KeyFun::Std.route_bytes(b"", b""), b"/");
        assert_eq!(KeyFun::BucketOnly.route_bytes(b"", b""), b"");
    }
}
