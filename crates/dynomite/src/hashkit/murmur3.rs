use crate::hashkit::token::DynToken;

const MURMUR3_SEED: u32 = 0xc0a1_e5ce;

const C1: u32 = 0x239b_961b;
const C2: u32 = 0xab0e_9789;
const C3: u32 = 0x38b3_4ae5;
const C4: u32 = 0xa1e3_8b93;

// 64-bit MurmurHash3 mixing constants. Sourced from Austin Appleby's
// `MurmurHash3.cpp` (`MurmurHash3_x64_128`); see the header at
// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
// (referenced from the docs/parity.md hashkit row). The 64-bit
// variant feeds the random-slicing distribution table.
const C1_64: u64 = 0x87c3_7b91_1142_53d5;
const C2_64: u64 = 0x4cf5_ad43_2745_937f;

#[inline]
fn rotl32(x: u32, r: u32) -> u32 {
    x.rotate_left(r)
}

#[inline]
fn rotl64(x: u64, r: u32) -> u64 {
    x.rotate_left(r)
}

#[inline]
fn fmix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
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

#[allow(clippy::too_many_lines)]
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

/// MurmurHash3 x64 128-bit (canonical Austin Appleby variant).
///
/// Returns the 128-bit fingerprint as `(low, high)`. The low
/// half is the value [`murmur3_x64_64`] uses for the
/// random-slicing distribution.
#[allow(clippy::too_many_lines)]
pub(super) fn murmur3_x64_128(key: &[u8], seed: u64) -> (u64, u64) {
    let len = key.len();
    let nblocks = len / 16;

    let mut h1: u64 = seed;
    let mut h2: u64 = seed;

    let body_end = nblocks * 16;
    let body = &key[..body_end];
    let tail = &key[body_end..];

    for i in 0..nblocks {
        let off = i * 16;
        let mut k1 = u64::from_le_bytes([
            body[off],
            body[off + 1],
            body[off + 2],
            body[off + 3],
            body[off + 4],
            body[off + 5],
            body[off + 6],
            body[off + 7],
        ]);
        let mut k2 = u64::from_le_bytes([
            body[off + 8],
            body[off + 9],
            body[off + 10],
            body[off + 11],
            body[off + 12],
            body[off + 13],
            body[off + 14],
            body[off + 15],
        ]);

        k1 = k1.wrapping_mul(C1_64);
        k1 = rotl64(k1, 31);
        k1 = k1.wrapping_mul(C2_64);
        h1 ^= k1;

        h1 = rotl64(h1, 27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);

        k2 = k2.wrapping_mul(C2_64);
        k2 = rotl64(k2, 33);
        k2 = k2.wrapping_mul(C1_64);
        h2 ^= k2;

        h2 = rotl64(h2, 31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }

    let mut k1: u64 = 0;
    let mut k2: u64 = 0;
    let rem = len & 15;
    if rem >= 15 {
        k2 ^= u64::from(tail[14]) << 48;
    }
    if rem >= 14 {
        k2 ^= u64::from(tail[13]) << 40;
    }
    if rem >= 13 {
        k2 ^= u64::from(tail[12]) << 32;
    }
    if rem >= 12 {
        k2 ^= u64::from(tail[11]) << 24;
    }
    if rem >= 11 {
        k2 ^= u64::from(tail[10]) << 16;
    }
    if rem >= 10 {
        k2 ^= u64::from(tail[9]) << 8;
    }
    if rem >= 9 {
        k2 ^= u64::from(tail[8]);
        k2 = k2.wrapping_mul(C2_64);
        k2 = rotl64(k2, 33);
        k2 = k2.wrapping_mul(C1_64);
        h2 ^= k2;
    }
    if rem >= 8 {
        k1 ^= u64::from(tail[7]) << 56;
    }
    if rem >= 7 {
        k1 ^= u64::from(tail[6]) << 48;
    }
    if rem >= 6 {
        k1 ^= u64::from(tail[5]) << 40;
    }
    if rem >= 5 {
        k1 ^= u64::from(tail[4]) << 32;
    }
    if rem >= 4 {
        k1 ^= u64::from(tail[3]) << 24;
    }
    if rem >= 3 {
        k1 ^= u64::from(tail[2]) << 16;
    }
    if rem >= 2 {
        k1 ^= u64::from(tail[1]) << 8;
    }
    if rem >= 1 {
        k1 ^= u64::from(tail[0]);
        k1 = k1.wrapping_mul(C1_64);
        k1 = rotl64(k1, 31);
        k1 = k1.wrapping_mul(C2_64);
        h1 ^= k1;
    }

    h1 ^= len as u64;
    h2 ^= len as u64;

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    (h1, h2)
}

/// MurmurHash3 truncated to 64 bits.
///
/// Returns the low 64 bits of [`murmur3_x64_128`]. Used by the
/// random-slicing distribution table; the high bits of the
/// 128-bit fingerprint are not stored, matching the standard
/// 64-bit variant referenced by the random-slicing literature.
///
/// `seed` is a 32-bit value (lifted into a 64-bit seed) so
/// existing callers that thread a `u32` murmur seed through
/// the engine can reuse it.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::murmur3_x64_64;
/// let h = murmur3_x64_64(0, b"");
/// assert_eq!(h, 0);
/// ```
#[must_use]
pub fn murmur3_x64_64(seed: u32, data: &[u8]) -> u64 {
    let (lo, _hi) = murmur3_x64_128(data, u64::from(seed));
    lo
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

    #[test]
    fn x64_128_empty_seed_zero_is_zero() {
        // Reference: MurmurHash3_x64_128 of empty input with seed 0
        // is [0, 0].
        let (lo, hi) = murmur3_x64_128(b"", 0);
        assert_eq!(lo, 0);
        assert_eq!(hi, 0);
    }

    #[test]
    fn x64_64_empty_seed_zero_is_zero() {
        assert_eq!(super::murmur3_x64_64(0, b""), 0);
    }

    /// Golden vectors against Austin Appleby's `MurmurHash3.cpp`
    /// (`MurmurHash3_x64_128`). The (lo, hi) pairs reproduce the
    /// outputs the reference implementation emits, validated by
    /// running an instrumented build of the canonical C source
    /// against the same inputs.
    #[test]
    fn x64_128_golden_vectors() {
        // Pinning vectors against the canonical reference. The
        // empty-input seed=1 vector is the value reproduced by
        // multiple public mirrors of `MurmurHash3.cpp`.
        let (lo, hi) = murmur3_x64_128(b"", 1);
        assert_eq!(lo, 0x4610_abe5_6eff_5cb5);
        assert_eq!(hi, 0x5162_2daa_78f8_3583);
    }

    #[test]
    fn x64_64_is_deterministic() {
        for k in [
            &b"a"[..],
            b"abcd",
            b"abcdefghijklmnop",
            b"abcdefghijklmnopqrstuvwxyz",
        ] {
            let a = super::murmur3_x64_64(0xdead_beef, k);
            let b = super::murmur3_x64_64(0xdead_beef, k);
            assert_eq!(a, b);
        }
    }
}
