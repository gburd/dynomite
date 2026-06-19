use crate::hashkit::token::DynToken;

const M: u32 = 0x5bd1_e995;
const R: u32 = 24;

/// MurmurHash 2 (32-bit).
pub(super) fn hash(key: &[u8]) -> DynToken {
    let length_full = key.len();
    let seed: u32 = 0xdead_beef_u32.wrapping_mul(length_full as u32);
    let mut h: u32 = seed ^ (length_full as u32);

    let mut remaining = key;
    while remaining.len() >= 4 {
        let mut k = u32::from_le_bytes([remaining[0], remaining[1], remaining[2], remaining[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        h = h.wrapping_mul(M);
        h ^= k;

        remaining = &remaining[4..];
    }

    match remaining.len() {
        3 => {
            h ^= u32::from(remaining[2]) << 16;
            h ^= u32::from(remaining[1]) << 8;
            h ^= u32::from(remaining[0]);
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= u32::from(remaining[1]) << 8;
            h ^= u32::from(remaining[0]);
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= u32::from(remaining[0]);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;

    DynToken::from_u32(h)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(key: &[u8]) -> u32 {
        hash(key).get_int()
    }

    #[test]
    fn empty_yields_known_value() {
        // length=0 => seed = 0, h = 0 ^ 0 = 0.
        // Final mix on h=0 is 0.
        assert_eq!(h(b""), 0);
    }

    #[test]
    fn determinism() {
        for k in [&b"key"[..], b"hello", b"123456789", b"abcdefghi"] {
            assert_eq!(h(k), h(k));
        }
    }

    #[test]
    fn distinguishes_short_inputs() {
        let mut seen = std::collections::HashSet::new();
        for k in [&b"a"[..], b"b", b"c", b"d", b"foo", b"bar", b"baz"] {
            assert!(seen.insert(h(k)));
        }
    }
}
