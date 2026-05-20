use crate::hashkit::token::DynToken;

const MURMUR3_SEED: u32 = 0xc0a1_e5ce;

const C1: u32 = 0x239b_961b;
const C2: u32 = 0xab0e_9789;
const C3: u32 = 0x38b3_4ae5;
const C4: u32 = 0xa1e3_8b93;

#[inline]
fn rotl32(x: u32, r: u32) -> u32 {
    x.rotate_left(r)
}

#[inline]
fn fmix32(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h
}

/// MurmurHash3 x86 128-bit. Produces a four-word [`DynToken`] in the
/// same order as `MurmurHash3_x86_128` writes its output buffer.
pub(super) fn hash(key: &[u8]) -> DynToken {
    murmur3_x86_128(key, MURMUR3_SEED)
}

fn murmur3_x86_128(key: &[u8], seed: u32) -> DynToken {
    let len = key.len();
    let nblocks = len / 16;

    let mut h1: u32 = seed;
    let mut h2: u32 = seed;
    let mut h3: u32 = seed;
    let mut h4: u32 = seed;

    let body_end = nblocks * 16;
    let body = &key[..body_end];
    let tail = &key[body_end..];

    for i in 0..nblocks {
        let off = i * 16;
        let mut k1 = u32::from_le_bytes([body[off], body[off + 1], body[off + 2], body[off + 3]]);
        let mut k2 =
            u32::from_le_bytes([body[off + 4], body[off + 5], body[off + 6], body[off + 7]]);
        let mut k3 =
            u32::from_le_bytes([body[off + 8], body[off + 9], body[off + 10], body[off + 11]]);
        let mut k4 = u32::from_le_bytes([
            body[off + 12],
            body[off + 13],
            body[off + 14],
            body[off + 15],
        ]);

        k1 = k1.wrapping_mul(C1);
        k1 = rotl32(k1, 15);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;

        h1 = rotl32(h1, 19);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x561c_cd1b);

        k2 = k2.wrapping_mul(C2);
        k2 = rotl32(k2, 16);
        k2 = k2.wrapping_mul(C3);
        h2 ^= k2;

        h2 = rotl32(h2, 17);
        h2 = h2.wrapping_add(h3);
        h2 = h2.wrapping_mul(5).wrapping_add(0x0bca_a747);

        k3 = k3.wrapping_mul(C3);
        k3 = rotl32(k3, 17);
        k3 = k3.wrapping_mul(C4);
        h3 ^= k3;

        h3 = rotl32(h3, 15);
        h3 = h3.wrapping_add(h4);
        h3 = h3.wrapping_mul(5).wrapping_add(0x96cd_1c35);

        k4 = k4.wrapping_mul(C4);
        k4 = rotl32(k4, 18);
        k4 = k4.wrapping_mul(C1);
        h4 ^= k4;

        h4 = rotl32(h4, 13);
        h4 = h4.wrapping_add(h1);
        h4 = h4.wrapping_mul(5).wrapping_add(0x32ac_3b17);
    }

    let mut k1: u32 = 0;
    let mut k2: u32 = 0;
    let mut k3: u32 = 0;
    let mut k4: u32 = 0;

    let rem = len & 15;
    if rem >= 15 {
        k4 ^= u32::from(tail[14]) << 16;
    }
    if rem >= 14 {
        k4 ^= u32::from(tail[13]) << 8;
    }
    if rem >= 13 {
        k4 ^= u32::from(tail[12]);
        k4 = k4.wrapping_mul(C4);
        k4 = rotl32(k4, 18);
        k4 = k4.wrapping_mul(C1);
        h4 ^= k4;
    }
    if rem >= 12 {
        k3 ^= u32::from(tail[11]) << 24;
    }
    if rem >= 11 {
        k3 ^= u32::from(tail[10]) << 16;
    }
    if rem >= 10 {
        k3 ^= u32::from(tail[9]) << 8;
    }
    if rem >= 9 {
        k3 ^= u32::from(tail[8]);
        k3 = k3.wrapping_mul(C3);
        k3 = rotl32(k3, 17);
        k3 = k3.wrapping_mul(C4);
        h3 ^= k3;
    }
    if rem >= 8 {
        k2 ^= u32::from(tail[7]) << 24;
    }
    if rem >= 7 {
        k2 ^= u32::from(tail[6]) << 16;
    }
    if rem >= 6 {
        k2 ^= u32::from(tail[5]) << 8;
    }
    if rem >= 5 {
        k2 ^= u32::from(tail[4]);
        k2 = k2.wrapping_mul(C2);
        k2 = rotl32(k2, 16);
        k2 = k2.wrapping_mul(C3);
        h2 ^= k2;
    }
    if rem >= 4 {
        k1 ^= u32::from(tail[3]) << 24;
    }
    if rem >= 3 {
        k1 ^= u32::from(tail[2]) << 16;
    }
    if rem >= 2 {
        k1 ^= u32::from(tail[1]) << 8;
    }
    if rem >= 1 {
        k1 ^= u32::from(tail[0]);
        k1 = k1.wrapping_mul(C1);
        k1 = rotl32(k1, 15);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    let len32 = len as u32;
    h1 ^= len32;
    h2 ^= len32;
    h3 ^= len32;
    h4 ^= len32;

    h1 = h1.wrapping_add(h2);
    h1 = h1.wrapping_add(h3);
    h1 = h1.wrapping_add(h4);
    h2 = h2.wrapping_add(h1);
    h3 = h3.wrapping_add(h1);
    h4 = h4.wrapping_add(h1);

    h1 = fmix32(h1);
    h2 = fmix32(h2);
    h3 = fmix32(h3);
    h4 = fmix32(h4);

    h1 = h1.wrapping_add(h2);
    h1 = h1.wrapping_add(h3);
    h1 = h1.wrapping_add(h4);
    h2 = h2.wrapping_add(h1);
    h3 = h3.wrapping_add(h1);
    h4 = h4.wrapping_add(h1);

    let mut token = DynToken::default();
    token.size(4).expect("len 4 fits");
    let mag = token.mag_mut();
    mag[0] = h1;
    mag[1] = h2;
    mag[2] = h3;
    mag[3] = h4;
    token
}

/// Wrapper exposing arbitrary seeds for tests.
#[cfg(test)]
fn hash_with_seed(key: &[u8], seed: u32) -> DynToken {
    murmur3_x86_128(key, seed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(key: &[u8]) -> [u32; 4] {
        let t = hash(key);
        let m = t.mag();
        [m[0], m[1], m[2], m[3]]
    }

    #[test]
    fn empty_with_zero_seed_is_zero() {
        // Standard MurmurHash3 x86_128 of empty input with seed 0 is
        // [0, 0, 0, 0].
        let t = hash_with_seed(b"", 0);
        assert_eq!(t.mag(), &[0, 0, 0, 0]);
    }

    #[test]
    fn produces_four_words() {
        let t = hash(b"abc");
        assert_eq!(t.len(), 4);
    }

    #[test]
    fn determinism() {
        for k in [&b"a"[..], b"abcd", b"abcdefghijklmnopqrstuvwxyz"] {
            assert_eq!(h(k), h(k));
        }
    }

    #[test]
    fn distinguishes_short_inputs() {
        let mut seen = std::collections::HashSet::new();
        for k in [&b"a"[..], b"b", b"c", b"foo", b"bar", b"baz"] {
            assert!(seen.insert(h(k)));
        }
    }
}
