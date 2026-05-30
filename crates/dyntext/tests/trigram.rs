//! Integration tests for `dyntext::trigram`.

use dyntext::trigram::{extract_trigram_set, extract_trigrams, hash_trigram};

#[test]
fn extract_trigrams_simple_string() {
    let v = extract_trigrams(b"abc");
    // padded "\x01\x01abc\x03\x03" is 7 bytes -> 5 trigrams.
    assert_eq!(v.len(), 5);
}

#[test]
fn extract_trigrams_short_input() {
    assert_eq!(extract_trigrams(b"x").len(), 3);
    assert_eq!(extract_trigrams(b"xy").len(), 4);
    // Empty input still emits two trigrams from the padding.
    assert_eq!(extract_trigrams(b"").len(), 2);
}

#[test]
fn extract_trigrams_unicode_byte_level() {
    // "cafe" with the e-acute character -> the e-acute is two
    // UTF-8 bytes (0xC3 0xA9). Trigram extraction works on
    // bytes, not codepoints.
    let s: &[u8] = b"caf\xc3\xa9";
    // 5 input bytes padded to 9 -> 7 trigrams.
    assert_eq!(extract_trigrams(s).len(), 7);
}

#[test]
fn hash_trigram_deterministic() {
    assert_eq!(hash_trigram(b"foo"), hash_trigram(b"foo"));
    assert_ne!(hash_trigram(b"foo"), hash_trigram(b"foa"));
}

#[test]
fn extract_trigram_set_is_sorted_unique() {
    let v = extract_trigram_set(b"abracadabra");
    for w in v.windows(2) {
        assert!(w[0] < w[1], "trigram set must be sorted unique");
    }
}
