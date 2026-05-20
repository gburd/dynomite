use crate::hashkit::token::DynToken;

const FNV_64_INIT: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_64_PRIME: u64 = 0x0000_0100_0000_01b3;
const FNV_32_INIT: u32 = 2_166_136_261;
const FNV_32_PRIME: u32 = 16_777_619;

/// FNV-1 64-bit, truncated to 32 bits. Matches `hash_fnv1_64` exactly.
pub(super) fn hash_fnv1_64(key: &[u8]) -> DynToken {
    let mut hash: u64 = FNV_64_INIT;
    for &byte in key {
        hash = hash.wrapping_mul(FNV_64_PRIME);
        hash ^= u64::from(byte);
    }
    DynToken::from_u32(hash as u32)
}

/// "fnv1a_64" as written in C: the accumulator is actually 32 bits and
/// the prime is the *low half* of `FNV_64_PRIME`. Reproduces the exact
/// bit-for-bit behavior, even though the name is misleading.
pub(super) fn hash_fnv1a_64(key: &[u8]) -> DynToken {
    let mut hash: u32 = FNV_64_INIT as u32;
    let prime: u32 = FNV_64_PRIME as u32;
    for &byte in key {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(prime);
    }
    DynToken::from_u32(hash)
}

/// FNV-1 32-bit.
pub(super) fn hash_fnv1_32(key: &[u8]) -> DynToken {
    let mut hash: u32 = FNV_32_INIT;
    for &byte in key {
        hash = hash.wrapping_mul(FNV_32_PRIME);
        hash ^= u32::from(byte);
    }
    DynToken::from_u32(hash)
}

/// FNV-1a 32-bit. The canonical FNV-1a-32 algorithm.
pub(super) fn hash_fnv1a_32(key: &[u8]) -> DynToken {
    let mut hash: u32 = FNV_32_INIT;
    for &byte in key {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(FNV_32_PRIME);
    }
    DynToken::from_u32(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(key: &[u8]) -> u32 {
        hash_fnv1a_32(key).get_int()
    }
    fn one(key: &[u8]) -> u32 {
        hash_fnv1_32(key).get_int()
    }

    #[test]
    fn fnv1a_32_known_vectors() {
        // Canonical FNV-1a 32-bit vectors from the published test suite.
        assert_eq!(a(b""), 0x811c_9dc5);
        assert_eq!(a(b"a"), 0xe40c_292c);
        assert_eq!(a(b"foobar"), 0xbf9c_f968);
    }

    #[test]
    fn fnv1_32_self_consistent() {
        // FNV-1 32-bit vectors are not as widely tabulated as FNV-1a;
        // pin self-consistency and a non-collision against FNV-1a.
        assert_eq!(one(b""), 0x811c_9dc5);
        assert_ne!(one(b"a"), a(b"a"));
        assert_ne!(one(b"foobar"), a(b"foobar"));
    }

    #[test]
    fn determinism() {
        for k in [&b"x"[..], b"key", b"longer key sample"] {
            assert_eq!(hash_fnv1_64(k).get_int(), hash_fnv1_64(k).get_int());
            assert_eq!(hash_fnv1a_64(k).get_int(), hash_fnv1a_64(k).get_int());
            assert_eq!(hash_fnv1_32(k).get_int(), hash_fnv1_32(k).get_int());
            assert_eq!(hash_fnv1a_32(k).get_int(), hash_fnv1a_32(k).get_int());
        }
    }
}
