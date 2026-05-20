//! Byte-slice helpers that complement [`bytes::Bytes`] and [`String`].
//!
//! The C `struct string` is a length-tagged byte view (`uint8_t*` plus
//! `uint32_t`). The Rust port replaces it with [`bytes::Bytes`] for
//! shared ownership of message payloads and [`String`] for textual
//! data. This module collects the small handful of helpers downstream
//! stages reach for: a stripped-down `string_compare` (length-prefixed
//! byte ordering) and char-finding wrappers that operate on slices.

/// Compare two byte slices using the same rule as the C
/// `string_compare`: shorter slices sort first, otherwise sort
/// lexicographically.
///
/// # Examples
///
/// ```
/// use std::cmp::Ordering;
/// use dynomite::util::dyn_string::string_compare;
///
/// assert_eq!(string_compare(b"abc", b"abc"), Ordering::Equal);
/// assert_eq!(string_compare(b"ab", b"abc"), Ordering::Less);
/// assert_eq!(string_compare(b"abd", b"abc"), Ordering::Greater);
/// ```
pub fn string_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    if a.len() == b.len() {
        a.cmp(b)
    } else {
        a.len().cmp(&b.len())
    }
}

/// Return the byte index of the first occurrence of `needle` in
/// `haystack`, or [`None`] if absent. Mirrors `dn_strchr`.
///
/// # Examples
///
/// ```
/// use dynomite::util::dyn_string::strchr;
/// assert_eq!(strchr(b"abcde", b'c'), Some(2));
/// assert_eq!(strchr(b"abcde", b'z'), None);
/// ```
pub fn strchr(haystack: &[u8], needle: u8) -> Option<usize> {
    haystack.iter().position(|&b| b == needle)
}

/// Return the byte index of the last occurrence of `needle` in
/// `haystack`, or [`None`] if absent. Mirrors `dn_strrchr`.
///
/// # Examples
///
/// ```
/// use dynomite::util::dyn_string::strrchr;
/// assert_eq!(strrchr(b"abcabc", b'b'), Some(4));
/// assert_eq!(strrchr(b"abcabc", b'z'), None);
/// ```
pub fn strrchr(haystack: &[u8], needle: u8) -> Option<usize> {
    haystack.iter().rposition(|&b| b == needle)
}

/// Case-insensitive ASCII slice equality. Slices of different lengths
/// are unequal.
///
/// # Examples
///
/// ```
/// use dynomite::util::dyn_string::eq_ignore_ascii_case;
/// assert!(eq_ignore_ascii_case(b"GET", b"get"));
/// assert!(!eq_ignore_ascii_case(b"GET", b"GETS"));
/// ```
pub fn eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;

    #[test]
    fn shorter_sorts_first() {
        assert_eq!(string_compare(b"a", b"ab"), Ordering::Less);
        assert_eq!(string_compare(b"abc", b"ab"), Ordering::Greater);
    }

    #[test]
    fn equal_length_uses_lex_order() {
        assert_eq!(string_compare(b"abc", b"abd"), Ordering::Less);
        assert_eq!(string_compare(b"abc", b"abc"), Ordering::Equal);
    }

    #[test]
    fn strchr_and_strrchr() {
        assert_eq!(strchr(b"hello", b'l'), Some(2));
        assert_eq!(strrchr(b"hello", b'l'), Some(3));
        assert_eq!(strchr(b"", b'x'), None);
        assert_eq!(strrchr(b"", b'x'), None);
    }
}
