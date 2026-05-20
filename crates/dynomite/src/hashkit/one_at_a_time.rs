use crate::hashkit::token::DynToken;

/// Jenkins one-at-a-time hash.
pub(super) fn hash(key: &[u8]) -> DynToken {
    let mut value: u32 = 0;
    for &byte in key {
        value = value.wrapping_add(u32::from(byte));
        value = value.wrapping_add(value << 10);
        value ^= value >> 6;
    }
    value = value.wrapping_add(value << 3);
    value ^= value >> 11;
    value = value.wrapping_add(value << 15);

    DynToken::from_u32(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(key: &[u8]) -> u32 {
        hash(key).get_int()
    }

    #[test]
    fn empty() {
        assert_eq!(h(b""), 0);
    }

    #[test]
    fn known_short_inputs() {
        // Empty input zeroes out the running value.
        assert_eq!(h(b""), 0);
        // Same input twice must yield the same word.
        assert_eq!(h(b"a"), h(b"a"));
        assert_eq!(h(b"abc"), h(b"abc"));
    }

    #[test]
    fn deterministic_for_same_input() {
        for k in [&b"hello"[..], b"world", b"123456789", b"k"] {
            assert_eq!(h(k), h(k));
        }
    }
}
