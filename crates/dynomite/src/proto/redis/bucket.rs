//! Bucket-name extraction for per-key routing overrides.
//!
//! A "bucket" is the prefix of a key, separated from the rest by
//! the first `/` byte. The dispatcher uses the bucket name to
//! look up a [`crate::conf::ConfBucketType`] and apply per-bucket
//! consistency / fan-out overrides for the duration of one
//! request. Keys without a `/` use the pool's default bucket
//! type (if any) or, failing that, the pool-level defaults.
//!
//! The convention is intentionally simpler than Riak's
//! `bucket-type/bucket/key` triple: we treat the part before the
//! first `/` as the bucket name and the remainder (including the
//! slash itself) as the user-visible key. That keeps existing
//! single-key Redis commands working unchanged for callers that
//! never use a slash, and gives operators a one-line knob for
//! callers that do.

/// Extract the bucket-type name from a key.
///
/// The key is split at the first `/` byte. The portion before the
/// slash is the bucket name; everything after the slash (the
/// slash itself is consumed) is the user key. An empty bucket
/// (key starts with `/`) returns `None`.
///
/// Returns:
///
/// * `Some(name)` when the key contains at least one `/` and the
///   prefix before it is non-empty.
/// * `None` when the key has no `/`, or when the slash is the
///   first byte of the key.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::bucket_name;
/// assert_eq!(bucket_name(b"users/42"), Some(&b"users"[..]));
/// assert_eq!(bucket_name(b"plain-key"), None);
/// assert_eq!(bucket_name(b"/leading"), None);
/// assert_eq!(bucket_name(b"a/b/c"), Some(&b"a"[..]));
/// ```
#[must_use]
pub fn bucket_name(key: &[u8]) -> Option<&[u8]> {
    let slash = key.iter().position(|&b| b == b'/')?;
    if slash == 0 {
        return None;
    }
    Some(&key[..slash])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_slash_returns_none() {
        assert_eq!(bucket_name(b""), None);
        assert_eq!(bucket_name(b"plain"), None);
        assert_eq!(bucket_name(b"key-with-no-slash"), None);
    }

    #[test]
    fn single_slash_returns_prefix() {
        assert_eq!(bucket_name(b"users/42"), Some(&b"users"[..]));
        assert_eq!(bucket_name(b"a/b"), Some(&b"a"[..]));
        // The remainder of the key after the slash may itself be empty.
        assert_eq!(bucket_name(b"only-prefix/"), Some(&b"only-prefix"[..]));
    }

    #[test]
    fn multi_slash_returns_first_segment() {
        assert_eq!(bucket_name(b"a/b/c"), Some(&b"a"[..]));
        assert_eq!(
            bucket_name(b"sessions/2026/05/23/abc"),
            Some(&b"sessions"[..]),
        );
    }

    #[test]
    fn empty_prefix_returns_none() {
        assert_eq!(bucket_name(b"/"), None);
        assert_eq!(bucket_name(b"/leading-slash"), None);
        assert_eq!(bucket_name(b"//double"), None);
    }

    #[test]
    fn binary_safe_keys_round_trip() {
        let key = b"\xffbucket\xff/\xfekey";
        // First slash is at index 8; bucket is the eight bytes before it.
        assert_eq!(bucket_name(key), Some(&key[..8]));
    }
}
