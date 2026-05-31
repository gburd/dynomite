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
//! numeric selector and the in-memory enum. A wire value the
//! crate does not yet honour (notably the reserved `CUSTOM = 99`
//! slot) decodes to `Err(KeyFunError::Custom)` so a future slice
//! that ships a user-defined keyfun can return the right enum
//! variant without breaking older deployments.
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
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum KeyFun {
    /// Hash `<bucket>/<key>` (Riak's `chash_std_keyfun`).
    /// Every key is independently distributed across the ring.
    #[default]
    Std,
    /// Hash `<bucket>` only (Riak's `chash_bucketonly_keyfun`).
    /// Every key in the bucket maps to the same partition.
    BucketOnly,
}

/// Errors produced when decoding a wire `chash_keyfun` value.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum KeyFunError {
    /// The wire value was the reserved `CUSTOM = 99` selector.
    /// User-defined keyfuns are not implemented in this slice.
    #[error("chash_keyfun: CUSTOM (user-defined) is reserved but not implemented")]
    Custom,
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
    /// `BucketOnly` returns just the bucket name. The function
    /// allocates a fresh `Vec<u8>`; callers that route many keys
    /// in a row should reuse the buffer via
    /// [`Self::route_bytes_into`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dyniak::datatypes::keyfun::KeyFun;
    /// assert_eq!(KeyFun::Std.route_bytes(b"b", b"k"), b"b/k");
    /// assert_eq!(KeyFun::BucketOnly.route_bytes(b"b", b"k"), b"b");
    /// ```
    #[must_use]
    pub fn route_bytes(self, bucket: &[u8], key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.route_len(bucket, key));
        self.route_bytes_into(bucket, key, &mut out);
        out
    }

    /// Append the route-input bytes for `(bucket, key)` onto
    /// `buf`. The buffer is NOT cleared first; reuse across calls
    /// is left to the caller.
    ///
    /// # Examples
    ///
    /// ```
    /// use dyniak::datatypes::keyfun::KeyFun;
    /// let mut buf = Vec::new();
    /// KeyFun::Std.route_bytes_into(b"users", b"alice", &mut buf);
    /// assert_eq!(buf, b"users/alice");
    /// ```
    pub fn route_bytes_into(self, bucket: &[u8], key: &[u8], buf: &mut Vec<u8>) {
        match self {
            Self::Std => {
                buf.extend_from_slice(bucket);
                buf.push(b'/');
                buf.extend_from_slice(key);
            }
            Self::BucketOnly => {
                buf.extend_from_slice(bucket);
            }
        }
    }

    /// Length of the byte sequence [`Self::route_bytes`] would
    /// return. Useful for pre-sizing a buffer when batching
    /// routing calls.
    #[must_use]
    pub fn route_len(self, bucket: &[u8], key: &[u8]) -> usize {
        match self {
            Self::Std => bucket.len() + 1 + key.len(),
            Self::BucketOnly => bucket.len(),
        }
    }

    /// Convert a wire `chash_keyfun` selector to the in-memory
    /// enum. `None` means the field is unset on the wire and
    /// the default applies; the caller decides what default to
    /// use (Riak-mode pools default to `Std` for keyfun).
    ///
    /// # Errors
    ///
    /// * [`KeyFunError::Custom`] when the wire value is the
    ///   reserved `99` selector.
    /// * [`KeyFunError::Unknown`] when the wire value is outside
    ///   the documented enum range.
    pub fn from_wire(value: u32) -> Result<Self, KeyFunError> {
        match value {
            CHASH_KEYFUN_STD => Ok(Self::Std),
            CHASH_KEYFUN_BUCKETONLY => Ok(Self::BucketOnly),
            CHASH_KEYFUN_CUSTOM => Err(KeyFunError::Custom),
            other => Err(KeyFunError::Unknown(other)),
        }
    }

    /// Convert the in-memory enum to a wire `chash_keyfun`
    /// selector ready to drop into
    /// [`crate::proto::pb::RpbBucketProps::chash_keyfun`].
    #[must_use]
    pub fn to_wire(self) -> u32 {
        match self {
            Self::Std => CHASH_KEYFUN_STD,
            Self::BucketOnly => CHASH_KEYFUN_BUCKETONLY,
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
            assert_eq!(owned.len(), kf.route_len(b"b", b"k"));
        }
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
    fn from_wire_rejects_custom_and_unknown() {
        assert_eq!(KeyFun::from_wire(99), Err(KeyFunError::Custom));
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
