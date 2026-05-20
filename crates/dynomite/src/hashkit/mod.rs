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
//! assert_eq!(token.len(), 4);
//! ```

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
pub mod token;

pub use crate::hashkit::random::PseudoRng;
pub use crate::hashkit::token::DynToken;

/// All hashing algorithms supported by the engine.
///
/// Variant order mirrors the on-disk `hash_type_t` integer codec:
/// configuration files and the `dyn-hash-tool` CLI rely on the integer
/// ordering of these variants when they round-trip through other
/// systems.
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
        }
    }

    /// All variants, in declaration order.
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
/// The returned token has `len == 1` for every 32-bit algorithm and
/// `len == 4` for `Murmur3`. The contract is identical to the C
/// `hash_func_t` it replaces: dispatch is total over [`HashType::all`].
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
    }
}

/// Compute the raw 128-byte MD5 digest of `key`.
///
/// Exposed so the `ketama` continuum can build per-server points using the
/// same digest layout as the original implementation.
#[must_use]
pub fn md5_signature(key: &[u8]) -> [u8; 16] {
    md5::digest(key)
}

/// CRC-32 over a buffer, lower-cased before mixing.
///
/// Mirrors the C `crc32_sz` helper used by the entropy reconciliation
/// path: each byte is forced to lower case before being fed to the table.
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
    fn dispatch_lengths_match_c_codec() {
        for ty in HashType::all().iter().copied() {
            let token = hash(ty, b"k");
            let expected = if matches!(ty, HashType::Murmur3) {
                4
            } else {
                1
            };
            assert_eq!(
                token.len(),
                expected,
                "wrong token length for {ty:?}: got {}",
                token.len()
            );
        }
    }
}
