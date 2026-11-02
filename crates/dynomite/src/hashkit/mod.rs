//! Hashing primitives, token arithmetic, and ring distributions.
//!
//! The [`HashType`] enum lists every hashing algorithm offered by the
//! engine. [`hash`] dispatches a key to the chosen algorithm and yields a
//! [`token::DynToken`] that can be compared and used as a key in a
//! continuum.
//!
//! # Examples
//!
//! ```
//! use dynomite::hashkit::{hash, HashType};
//!
//! let token = hash(HashType::Murmur3, b"dynomite");
//! assert_eq!(token.len(), 1);
//! ```

// Hashing primitives reinterpret bytes across signed/unsigned
// boundaries and truncate by design; the casts are part of the
// algorithmic contract and isolated to this subtree.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

mod crc16;
mod crc32;
mod fnv;
mod hsieh;
mod jenkins;
mod md5;
mod murmur;
mod murmur3;
mod one_at_a_time;
mod random;

pub mod ketama;
pub mod modula;
pub mod random_slicing;
pub mod token;

pub use crate::hashkit::murmur3::murmur3_x64_64;
pub use crate::hashkit::random::PseudoRng;
pub use crate::hashkit::random_slicing::{RandomSlices, RandomSlicesError};
pub use crate::hashkit::token::DynToken;

/// All hashing algorithms supported by the engine.
///
/// Variant order mirrors the on-disk `hash_type_t` integer codec:
/// configuration files and the `dyn-hash-tool` CLI rely on the integer
/// ordering of these variants when they round-trip through other
/// systems.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::HashType;
/// assert_eq!(HashType::all().len(), 14);
/// assert_eq!(HashType::Murmur3.as_str(), "murmur3");
/// ```
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum HashType {
    /// Jenkins one-at-a-time.
    OneAtATime,
    /// MD5; truncates the digest to its low 32 bits.
    Md5,
    /// CRC-16 (CCITT polynomial table).
    Crc16,
    /// libmemcached-compatible CRC-32.
    Crc32,
    /// Standards-compliant CRC-32.
    Crc32a,
    /// FNV-1 over 64-bit accumulator, truncated to 32 bits.
    Fnv1_64,
    /// FNV-1a operating in 32-bit space using the 64-bit prime.
    Fnv1a_64,
    /// FNV-1 in 32 bits.
    Fnv1_32,
    /// FNV-1a in 32 bits.
    Fnv1a_32,
    /// Paul Hsieh's "SuperFastHash".
    Hsieh,
    /// MurmurHash 2.
    Murmur,
    /// Bob Jenkins lookup3.
    Jenkins,
    /// MurmurHash3 x86 128-bit.
    Murmur3,
    /// MurmurHash3 truncated to 64 bits (used by the
    /// random-slicing distribution lookup).
    #[allow(non_camel_case_types)]
    Murmur3X64_64,
}

impl HashType {
    /// Lower-case algorithm name as it appears in YAML configurations.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::HashType;
    /// assert_eq!(HashType::Murmur3.as_str(), "murmur3");
    /// ```
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            HashType::OneAtATime => "one_at_a_time",
            HashType::Md5 => "md5",
            HashType::Crc16 => "crc16",
            HashType::Crc32 => "crc32",
            HashType::Crc32a => "crc32a",
            HashType::Fnv1_64 => "fnv1_64",
            HashType::Fnv1a_64 => "fnv1a_64",
            HashType::Fnv1_32 => "fnv1_32",
            HashType::Fnv1a_32 => "fnv1a_32",
            HashType::Hsieh => "hsieh",
            HashType::Murmur => "murmur",
            HashType::Jenkins => "jenkins",
            HashType::Murmur3 => "murmur3",
            HashType::Murmur3X64_64 => "murmur3_x64_64",
        }
    }

    /// All variants, in declaration order.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::HashType;
    /// assert!(HashType::all().contains(&HashType::Murmur3));
    /// ```
    #[must_use]
    pub const fn all() -> &'static [HashType] {
        &[
            HashType::OneAtATime,
            HashType::Md5,
            HashType::Crc16,
            HashType::Crc32,
            HashType::Crc32a,
            HashType::Fnv1_64,
            HashType::Fnv1a_64,
            HashType::Fnv1_32,
            HashType::Fnv1a_32,
            HashType::Hsieh,
            HashType::Murmur,
            HashType::Jenkins,
            HashType::Murmur3,
            HashType::Murmur3X64_64,
        ]
    }

    /// Parse a YAML-style algorithm name. Unknown names yield `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::HashType;
    /// assert_eq!(HashType::from_name("murmur3"), Some(HashType::Murmur3));
    /// assert_eq!(HashType::from_name("nope"), None);
    /// ```
    #[must_use]
    pub fn from_name(name: &str) -> Option<HashType> {
        Self::all().iter().copied().find(|h| h.as_str() == name)
    }
}

/// Hash `key` with the requested algorithm and return the resulting token.
///
/// The returned ring token has `len == 1` for every algorithm, the width
/// of a node token. Dispatch is total over [`HashType::all`].
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::{hash, HashType};
///
/// let t1 = hash(HashType::Crc32a, b"abc");
/// let t2 = hash(HashType::Crc32a, b"abc");
/// assert_eq!(t1, t2);
/// ```
#[must_use]
pub fn hash(ty: HashType, key: &[u8]) -> DynToken {
    match ty {
        HashType::OneAtATime => one_at_a_time::hash(key),
        HashType::Md5 => md5::hash(key),
        HashType::Crc16 => crc16::hash(key),
        HashType::Crc32 => crc32::hash_libmemcached(key),
        HashType::Crc32a => crc32::hash_standard(key),
        HashType::Fnv1_64 => fnv::hash_fnv1_64(key),
        HashType::Fnv1a_64 => fnv::hash_fnv1a_64(key),
        HashType::Fnv1_32 => fnv::hash_fnv1_32(key),
        HashType::Fnv1a_32 => fnv::hash_fnv1a_32(key),
        HashType::Hsieh => hsieh::hash(key),
        HashType::Murmur => murmur::hash(key),
        HashType::Jenkins => jenkins::hash(key),
        HashType::Murmur3 => murmur3::hash(key),
        // The vnode ring holds 32-bit node tokens, so the 64-bit hash is
        // folded to its high word; a wider token would sort above every
        // node and route every key to the first one.
        HashType::Murmur3X64_64 => {
            DynToken::from_u32((murmur3::murmur3_x64_64(0xc0a1_e5ce, key) >> 32) as u32)
        }
    }
}

/// The 128-bit `MurmurHash3_x86_128` fingerprint of `key` (seed
/// `0xc0a1e5ce`) as four words. A wide fingerprint for sketches such as
/// HyperLogLog; not a ring token (use [`hash`] for routing).
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::murmur3_x86_128;
/// assert_eq!(murmur3_x86_128(b"a").len(), 4);
/// ```
#[must_use]
pub fn murmur3_x86_128(key: &[u8]) -> DynToken {
    murmur3::hash_x86_128(key)
}

/// Hash `key` and return a 64-bit value used by the
/// random-slicing distribution table, which partitions the full 64-bit
/// space.
///
/// `Murmur3X64_64` returns its natural 64-bit value. Every other
/// algorithm yields a 32-bit ring token, which is placed in the high half
/// of the returned `u64` so it spans the whole table; left in the low half
/// it would always fall in the first slice.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::{hash, hash64, HashType};
///
/// let t = hash(HashType::Murmur, b"alice").get_int();
/// assert_eq!(hash64(HashType::Murmur, b"alice"), u64::from(t) << 32);
/// ```
#[must_use]
pub fn hash64(ty: HashType, key: &[u8]) -> u64 {
    if matches!(ty, HashType::Murmur3X64_64) {
        return murmur3::murmur3_x64_64(0xc0a1_e5ce, key);
    }
    u64::from(hash(ty, key).get_int()) << 32
}

/// Compute the raw 128-byte MD5 digest of `key`.
///
/// Exposed so the `ketama` continuum can build per-server points using the
/// same digest layout as the original implementation.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::md5_signature;
/// // The empty string has a known MD5 digest.
/// let d = md5_signature(b"");
/// assert_eq!(d[0], 0xd4);
/// assert_eq!(d.len(), 16);
/// ```
#[must_use]
pub fn md5_signature(key: &[u8]) -> [u8; 16] {
    md5::digest(key)
}

/// CRC-32 over a buffer, lower-cased before mixing.
///
/// Each byte is forced to lower case before being fed to the table; the
/// helper is used by the entropy reconciliation path.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::crc32_sz;
/// // Lower-casing means "ABC" and "abc" produce the same digest.
/// assert_eq!(crc32_sz(b"ABC", 0), crc32_sz(b"abc", 0));
/// ```
#[must_use]
pub fn crc32_sz(buf: &[u8], in_crc32: u32) -> u32 {
    crc32::crc32_sz(buf, in_crc32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip() {
        for ty in HashType::all().iter().copied() {
            assert_eq!(HashType::from_name(ty.as_str()), Some(ty));
        }
    }

    #[test]
    fn dispatch_is_deterministic() {
        let key = b"the quick brown fox";
        for ty in HashType::all().iter().copied() {
            let a = hash(ty, key);
            let b = hash(ty, key);
            assert_eq!(a, b, "algorithm {ty:?} not deterministic");
        }
    }

    #[test]
    fn every_ring_token_is_one_word() {
        // Node tokens are one word and token order compares length first,
        // so a wider key token would route every key to the first node.
        for ty in HashType::all().iter().copied() {
            assert_eq!(hash(ty, b"k").len(), 1, "{ty:?}");
        }
    }
}
