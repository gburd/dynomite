//! Per-token position records collected by the protocol parsers.
//!
//! The Redis and Memcached parsers identify keys and other argument
//! tokens inside the wire bytes and surface them on [`Msg`] for the
//! cluster, fragmenter, and repair layers to consume. This module
//! defines the two record shapes those parsers emit.
//!
//! [`KeyPos`] carries a full key plus an inner sub-range that
//! identifies the routing tag (the portion between the optional
//! hash-tag delimiters). [`ArgPos`] carries a single non-key
//! argument or response token.
//!
//! [`Msg`]: crate::msg::Msg

use std::ops::Range;

/// Position record for one parsed key.
///
/// The full key bytes live in `key`. When a hash tag is configured
/// and the tag delimiters are present in the key, `tag` indexes the
/// range inside `key` that should be hashed for routing; otherwise
/// `tag` covers the whole key and is equal to `0..key.len()`.
///
/// # Examples
///
/// ```
/// use dynomite::msg::keypos::KeyPos;
///
/// let kp = KeyPos::without_tag(b"user:42".to_vec());
/// assert_eq!(kp.key(), b"user:42");
/// assert_eq!(kp.tag_bytes(), b"user:42");
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyPos {
    /// Full key bytes.
    key: Vec<u8>,
    /// Sub-range of `key` that holds the routing tag.
    tag: Range<usize>,
}

impl KeyPos {
    /// Build a [`KeyPos`] with an explicit tag sub-range.
    ///
    /// Panics if `tag.end > key.len()` or `tag.start > tag.end`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::keypos::KeyPos;
    /// let kp = KeyPos::new(b"{abc}xyz".to_vec(), 1..4);
    /// assert_eq!(kp.tag_bytes(), b"abc");
    /// ```
    #[must_use]
    pub fn new(key: Vec<u8>, tag: Range<usize>) -> Self {
        assert!(tag.start <= tag.end, "tag range must be valid");
        assert!(tag.end <= key.len(), "tag range must be within key");
        Self { key, tag }
    }

    /// Build a [`KeyPos`] whose tag covers the entire key.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::keypos::KeyPos;
    /// let kp = KeyPos::without_tag(b"hello".to_vec());
    /// assert_eq!(kp.tag_bytes(), b"hello");
    /// ```
    #[must_use]
    pub fn without_tag(key: Vec<u8>) -> Self {
        let len = key.len();
        Self { key, tag: 0..len }
    }

    /// Borrow the full key bytes.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Length of the full key in bytes.
    #[must_use]
    pub fn key_len(&self) -> usize {
        self.key.len()
    }

    /// Borrow the tag range.
    #[must_use]
    pub fn tag(&self) -> Range<usize> {
        self.tag.clone()
    }

    /// Borrow the tag bytes.
    #[must_use]
    pub fn tag_bytes(&self) -> &[u8] {
        &self.key[self.tag.clone()]
    }

    /// Length of the tag in bytes.
    #[must_use]
    pub fn tag_len(&self) -> usize {
        self.tag.end - self.tag.start
    }
}

/// Position record for one non-key argument or response token.
///
/// # Examples
///
/// ```
/// use dynomite::msg::keypos::ArgPos;
/// let arg = ArgPos::new(b"value".to_vec());
/// assert_eq!(arg.bytes(), b"value");
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArgPos {
    data: Vec<u8>,
}

impl ArgPos {
    /// Build an argument record from owned bytes.
    #[must_use]
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Borrow the argument bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.data
    }

    /// Length of the argument in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// True when the argument is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keypos_default_tag_is_full_key() {
        let kp = KeyPos::without_tag(b"abc".to_vec());
        assert_eq!(kp.tag_bytes(), b"abc");
        assert_eq!(kp.tag_len(), 3);
        assert_eq!(kp.key_len(), 3);
    }

    #[test]
    fn keypos_inner_tag_indexes_inside_key() {
        let kp = KeyPos::new(b"{ab}cd".to_vec(), 1..3);
        assert_eq!(kp.tag_bytes(), b"ab");
        assert_eq!(kp.key(), b"{ab}cd");
    }

    #[test]
    #[should_panic(expected = "tag range must be valid")]
    fn keypos_rejects_inverted_range() {
        let _ = KeyPos::new(b"abc".to_vec(), 2..1);
    }

    #[test]
    #[should_panic(expected = "tag range must be within key")]
    fn keypos_rejects_overrun_range() {
        let _ = KeyPos::new(b"abc".to_vec(), 0..4);
    }

    #[test]
    fn argpos_round_trips() {
        let a = ArgPos::new(vec![1, 2, 3]);
        assert_eq!(a.len(), 3);
        assert!(!a.is_empty());
        assert_eq!(a.bytes(), &[1, 2, 3]);
    }
}
