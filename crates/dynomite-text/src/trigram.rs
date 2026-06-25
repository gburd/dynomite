//! Three-byte n-gram extraction.
//!
//! Trigrams are the smallest unit of substring evidence used by
//! the index. Each trigram is a window of three contiguous bytes;
//! the window is hashed to a `u64` so the inverted index keys
//! are fixed size regardless of input alphabet.
//!
//! # Padding
//!
//! Inputs are padded with `\x01\x01<text>\x03\x03` so the first
//! and last few characters get full trigram coverage. Without
//! padding, a one-byte input would generate zero trigrams; with
//! padding it generates three (`\x01\x01x`, `\x01x\x03`,
//! `x\x03\x03`). The padding bytes are chosen from the C0
//! control range so they do not collide with printable ASCII or
//! UTF-8 continuation bytes.
//!
//! # Hashing
//!
//! The trigram is hashed via [BLAKE3] truncated to its first
//! eight little-endian bytes. BLAKE3 is the workspace standard
//! hash, and its avalanche behaviour over three-byte inputs is
//! comfortably within the bloom-filter design budget for the
//! per-document filter (see `bloom`).
//!
//! [BLAKE3]: https://github.com/BLAKE3-team/BLAKE3

/// First padding byte (Start of Heading control).
pub const PAD_LEFT: u8 = 0x01;

/// Trailing padding byte (End of Text control).
pub const PAD_RIGHT: u8 = 0x03;

/// Number of bytes per trigram window.
pub const TRIGRAM_LEN: usize = 3;

/// Hash a single three-byte trigram to a `u64`.
///
/// Takes any byte slice but only inspects the first three bytes;
/// callers should pass exactly a trigram. The function is
/// deterministic: equal trigrams hash to equal `u64`s on every
/// platform.
///
/// # Examples
///
/// ```
/// use dyntext::trigram::hash_trigram;
/// assert_eq!(hash_trigram(b"abc"), hash_trigram(b"abc"));
/// assert_ne!(hash_trigram(b"abc"), hash_trigram(b"abd"));
/// ```
#[must_use]
pub fn hash_trigram(trigram: &[u8]) -> u64 {
    let h = blake3::hash(trigram);
    let bytes = h.as_bytes();
    let mut head = [0_u8; 8];
    head.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(head)
}

/// Extract the trigram hash sequence from a byte slice.
///
/// The input is padded with two `PAD_LEFT` bytes on the front
/// and two `PAD_RIGHT` bytes on the back so the boundary
/// characters get the same coverage as interior bytes. The
/// returned vector preserves trigram order; duplicate trigrams
/// are NOT deduplicated here so callers can count occurrences if
/// they need to.
///
/// # Examples
///
/// ```
/// use dyntext::trigram::extract_trigrams;
/// let v = extract_trigrams(b"hi");
/// // padded: "\x01\x01hi\x03\x03" -> 4 trigrams.
/// assert_eq!(v.len(), 4);
/// ```
#[must_use]
pub fn extract_trigrams(text: &[u8]) -> Vec<u64> {
    let mut padded: Vec<u8> = Vec::with_capacity(text.len() + 4);
    padded.push(PAD_LEFT);
    padded.push(PAD_LEFT);
    padded.extend_from_slice(text);
    padded.push(PAD_RIGHT);
    padded.push(PAD_RIGHT);
    if padded.len() < TRIGRAM_LEN {
        return Vec::new();
    }
    padded.windows(TRIGRAM_LEN).map(hash_trigram).collect()
}

/// Extract a deduplicated trigram set from a byte slice.
///
/// Equivalent to `extract_trigrams(text)` followed by sort +
/// dedup. Useful at write time when the caller cares about
/// "does the trigram appear at all" rather than its multiplicity.
///
/// # Examples
///
/// ```
/// use dyntext::trigram::extract_trigram_set;
/// let v = extract_trigram_set(b"aaaa");
/// // Without dedup we would have 6 trigrams; with dedup the
/// // structurally distinct ones survive.
/// assert!(v.len() < 6);
/// ```
#[must_use]
pub fn extract_trigram_set(text: &[u8]) -> Vec<u64> {
    let mut v = extract_trigrams(text);
    v.sort_unstable();
    v.dedup();
    v
}

/// Extract the trigram hash sequence for a substring query.
///
/// Unlike [`extract_trigrams`], the input is NOT padded: a
/// substring query asks "do my exact bytes appear inside any
/// indexed doc?", and the boundary-padding bytes only show up
/// at document boundaries (which the query is not adjacent to).
/// Padding the query would cause it to miss interior matches
/// because the padding-bearing trigrams would not be in any
/// doc's interior.
///
/// Returns an empty vector for inputs shorter than
/// [`TRIGRAM_LEN`]: such queries cannot be served through the
/// trigram index and the caller should fall back to a full
/// scan.
///
/// # Examples
///
/// ```
/// use dyntext::trigram::extract_query_trigrams;
/// assert_eq!(extract_query_trigrams(b"abcdef").len(), 4);
/// assert!(extract_query_trigrams(b"ab").is_empty());
/// ```
#[must_use]
pub fn extract_query_trigrams(query: &[u8]) -> Vec<u64> {
    if query.len() < TRIGRAM_LEN {
        return Vec::new();
    }
    query.windows(TRIGRAM_LEN).map(hash_trigram).collect()
}

/// Deduplicated query trigram set.
///
/// Same as [`extract_query_trigrams`] followed by sort + dedup.
#[must_use]
pub fn extract_query_trigram_set(query: &[u8]) -> Vec<u64> {
    let mut v = extract_query_trigrams(query);
    v.sort_unstable();
    v.dedup();
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_trigrams_simple_string() {
        // "abc" with padding -> "\x01\x01abc\x03\x03" (7 bytes,
        // 5 trigrams).
        let v = extract_trigrams(b"abc");
        assert_eq!(v.len(), 5);
        // Distinct trigrams produce distinct hashes.
        let mut sorted = v.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 5);
    }

    #[test]
    fn extract_trigrams_short_input_one_byte() {
        // "x" -> "\x01\x01x\x03\x03" (5 bytes, 3 trigrams).
        let v = extract_trigrams(b"x");
        assert_eq!(v.len(), 3);
    }

    #[test]
    fn extract_trigrams_short_input_two_bytes() {
        // "xy" -> "\x01\x01xy\x03\x03" (6 bytes, 4 trigrams).
        let v = extract_trigrams(b"xy");
        assert_eq!(v.len(), 4);
    }

    #[test]
    fn extract_trigrams_empty_input() {
        // "" -> "\x01\x01\x03\x03" (4 bytes, 2 trigrams).
        let v = extract_trigrams(b"");
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn extract_trigrams_unicode_byte_level() {
        // The "e with acute" character is two UTF-8 bytes
        // (0xC3 0xA9). Trigrams operate on bytes, not
        // codepoints; the input "ae" (with the e-acute char) is
        // three bytes, padded to seven, giving five trigrams.
        let s: &[u8] = b"a\xc3\xa9";
        let v = extract_trigrams(s);
        assert_eq!(v.len(), 5);
    }

    #[test]
    fn hash_trigram_deterministic() {
        let a = hash_trigram(b"foo");
        let b = hash_trigram(b"foo");
        assert_eq!(a, b);
    }

    #[test]
    fn hash_trigram_distinct_for_different_inputs() {
        let a = hash_trigram(b"foo");
        let b = hash_trigram(b"foa");
        let c = hash_trigram(b"oof");
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }

    #[test]
    fn extract_trigram_set_dedups() {
        let raw = extract_trigrams(b"aaaa");
        let set = extract_trigram_set(b"aaaa");
        assert!(set.len() <= raw.len());
        // The set is sorted and unique.
        for w in set.windows(2) {
            assert!(w[0] < w[1]);
        }
    }

    #[test]
    fn extract_query_trigrams_unpadded() {
        // "abcdef" has 6 bytes -> 4 unpadded trigrams.
        let v = extract_query_trigrams(b"abcdef");
        assert_eq!(v.len(), 4);
        assert_eq!(v[0], hash_trigram(b"abc"));
        assert_eq!(v[1], hash_trigram(b"bcd"));
        assert_eq!(v[2], hash_trigram(b"cde"));
        assert_eq!(v[3], hash_trigram(b"def"));
    }

    #[test]
    fn extract_query_trigrams_short_input_is_empty() {
        assert!(extract_query_trigrams(b"").is_empty());
        assert!(extract_query_trigrams(b"a").is_empty());
        assert!(extract_query_trigrams(b"ab").is_empty());
    }

    #[test]
    fn extract_query_trigrams_three_byte_input_yields_one_trigram() {
        let v = extract_query_trigrams(b"abc");
        assert_eq!(v, vec![hash_trigram(b"abc")]);
    }

    #[test]
    fn query_trigrams_are_subset_of_padded_trigrams() {
        // A query that appears inside a doc must have its
        // unpadded query trigrams contained in the doc's
        // padded trigram set. ("hello" inside "hello world".)
        let doc_set = extract_trigram_set(b"hello world");
        let q_set = extract_query_trigram_set(b"hello");
        for q in &q_set {
            assert!(doc_set.contains(q), "query trigram {q:#x} not in doc set");
        }
    }
}
