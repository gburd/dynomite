use crate::hashkit::token::DynToken;

#[inline]
fn get16(d: &[u8]) -> u32 {
    u32::from(u16::from_le_bytes([d[0], d[1]]))
}

/// Paul Hsieh's "SuperFastHash". Reproduces the byte-by-byte
/// implementation that the C source falls back to on every platform
/// other than 32-bit GCC (the only path that takes the unaligned-read
/// shortcut). On a little-endian host the two paths produce the same
/// result.
pub(super) fn hash(key: &[u8]) -> DynToken {
    if key.is_empty() {
        return DynToken::from_u32(0);
    }

    let mut hash: u32 = 0;
    let rem = key.len() & 3;
    let blocks = key.len() >> 2;

    let mut cursor = 0usize;
    for _ in 0..blocks {
        let chunk = &key[cursor..cursor + 4];
        hash = hash.wrapping_add(get16(&chunk[0..2]));
        let tmp = (get16(&chunk[2..4]) << 11) ^ hash;
        hash = (hash << 16) ^ tmp;
        cursor += 4;
        hash = hash.wrapping_add(hash >> 11);
    }

    let tail = &key[cursor..];
    match rem {
        3 => {
            hash = hash.wrapping_add(get16(&tail[0..2]));
            hash ^= hash << 16;
            hash ^= u32::from(tail[2]) << 18;
            hash = hash.wrapping_add(hash >> 11);
        }
        2 => {
            hash = hash.wrapping_add(get16(&tail[0..2]));
            hash ^= hash << 11;
            hash = hash.wrapping_add(hash >> 17);
        }
        1 => {
            hash = hash.wrapping_add(u32::from(tail[0]));
            hash ^= hash << 10;
            hash = hash.wrapping_add(hash >> 1);
        }
        _ => {}
    }

    hash ^= hash << 3;
    hash = hash.wrapping_add(hash >> 5);
    hash ^= hash << 4;
    hash = hash.wrapping_add(hash >> 17);
    hash ^= hash << 25;
    hash = hash.wrapping_add(hash >> 6);

    DynToken::from_u32(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(key: &[u8]) -> u32 {
        hash(key).get_int()
    }

    #[test]
    fn empty_yields_zero() {
        assert_eq!(h(b""), 0);
    }

    #[test]
    fn determinism() {
        for k in [&b"a"[..], b"ab", b"abc", b"abcd", b"abcde"] {
            assert_eq!(h(k), h(k));
        }
    }

    #[test]
    fn distinguishes_short_inputs() {
        let mut seen = std::collections::HashSet::new();
        for k in [&b"a"[..], b"b", b"c", b"d", b"e", b"foo", b"bar"] {
            assert!(seen.insert(h(k)), "collision on {k:?}");
        }
    }
}
