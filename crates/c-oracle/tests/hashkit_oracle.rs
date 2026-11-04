//! Oracle: every C hashkit function against its Rust transliteration.
//!
//! Each property feeds the same arbitrary key to the C function (compiled
//! from the C Dynomite reference) and to the Rust function, and requires
//! the resulting token to match word for word, including sign and length.

#![cfg(not(c_oracle_absent))]

use c_oracle::hashkit::{self, CHash, CToken};
use dynomite::hashkit::token::Sign;
use dynomite::hashkit::{self as rs, DynToken, HashType};
use hegel::generators as gs;
use hegel::TestCase;

fn rust_type(h: CHash) -> HashType {
    match h {
        CHash::OneAtATime => HashType::OneAtATime,
        CHash::Md5 => HashType::Md5,
        CHash::Crc16 => HashType::Crc16,
        CHash::Crc32 => HashType::Crc32,
        CHash::Crc32a => HashType::Crc32a,
        CHash::Fnv164 => HashType::Fnv1_64,
        CHash::Fnv1a64 => HashType::Fnv1a_64,
        CHash::Fnv132 => HashType::Fnv1_32,
        CHash::Fnv1a32 => HashType::Fnv1a_32,
        CHash::Hsieh => HashType::Hsieh,
        CHash::Murmur => HashType::Murmur,
        CHash::Jenkins => HashType::Jenkins,
        CHash::Murmur3 => HashType::Murmur3,
    }
}

/// The C signum convention: 0 for zero, 1 for positive, `-1` stored in a
/// `uint32_t` for negative.
fn c_signum(t: &DynToken) -> u32 {
    match t.sign() {
        Sign::Zero => 0,
        Sign::Positive => 1,
        Sign::Negative => u32::MAX,
    }
}

fn assert_same(h: CHash, key: &[u8], c: &CToken, r: &DynToken) {
    assert_eq!(
        (c.signum, c.words()),
        (c_signum(r), r.mag()),
        "{h:?} diverges on key {key:?}: C {c:?}, Rust {r:?}"
    );
}

/// Hash functions whose Rust behaviour is an owner-authorised deviation
/// from C, and so are excluded from the byte-for-byte comparison. Each is
/// pinned by its own test below.
///
/// * `Murmur3` (DEV-H1): C `hash_murmur3` has its `MurmurHash3_x86_128`
///   call commented out (since 2013-12-10, upstream 9eb54c1) and returns a
///   zero token for every key. Rust computes `MurmurHash3_x86_32`.
/// * `OneAtATime` (DEV-H2): C reads the key through `const char *`, so on
///   x86-64 bytes >= 0x80 are sign-extended. Rust reads bytes unsigned.
const DEVIATES: &[CHash] = &[CHash::Murmur3, CHash::OneAtATime];

fn decided() -> impl Iterator<Item = CHash> {
    CHash::ALL.into_iter().filter(|h| !DEVIATES.contains(h))
}

/// DEV-H2 scope: on ASCII-only keys (every byte below 0x80) sign extension
/// is a no-op, so C and Rust agree exactly.
#[hegel::test(test_cases = 2000)]
fn one_at_a_time_matches_c_on_ascii_keys(tc: TestCase) {
    let key: Vec<u8> = tc.draw(
        gs::vecs(gs::integers::<u8>().max_value(0x7f))
            .min_size(1)
            .max_size(512),
    );
    let c = hashkit::hash(CHash::OneAtATime, &key).expect("non-empty key");
    let r = rs::hash(HashType::OneAtATime, &key);
    assert_same(CHash::OneAtATime, &key, &c, &r);
}

/// Jenkins one-at-a-time over unsigned bytes: the authorised DEV-H2
/// behaviour, written out independently of the engine.
fn one_at_a_time_unsigned(key: &[u8]) -> u32 {
    let mut v: u32 = 0;
    for &b in key {
        v = v.wrapping_add(u32::from(b));
        v = v.wrapping_add(v << 10);
        v ^= v >> 6;
    }
    v = v.wrapping_add(v << 3);
    v ^= v >> 11;
    v.wrapping_add(v << 15)
}

/// DEV-H2: Rust reads every byte unsigned, including bytes >= 0x80.
#[hegel::test(test_cases = 2000)]
fn one_at_a_time_reads_bytes_unsigned(tc: TestCase) {
    let key: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(1).max_size(512));
    let r = rs::hash(HashType::OneAtATime, &key);
    assert_eq!(r.mag(), &[one_at_a_time_unsigned(&key)], "key {key:?}");
}

/// Pins the C side of DEV-H1 so the oracle notices if upstream ever
/// changes: C murmur3 returns the zero token for every key.
#[hegel::test(test_cases = 200)]
fn c_murmur3_is_constant_zero(tc: TestCase) {
    let key: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(1).max_size(256));
    let c = hashkit::hash(CHash::Murmur3, &key).expect("non-empty key");
    assert_eq!((c.signum, c.mag), (0, [0; 4]), "key {key:?}");
}

/// DEV-H1: the Rust `murmur3` ring token is the C contrib
/// `MurmurHash3_x86_32` (the reference implementation shipped in the C
/// tree) with the C `MURMUR3_SEED`, as a one-word token like every other
/// ring hash.
#[hegel::test(test_cases = 2000)]
fn murmur3_ring_token_is_c_contrib_x86_32(tc: TestCase) {
    let key: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>()).max_size(512));
    let want = hashkit::murmur3_x86_32(&key, 0xc0a1_e5ce);
    let r = rs::hash(HashType::Murmur3, &key);
    assert_eq!(r.len(), 1, "ring tokens are one word");
    assert_eq!(r.get_int(), want, "key {key:?}");
}

#[hegel::test(test_cases = 2000)]
fn every_hash_matches_c(tc: TestCase) {
    let key: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(1).max_size(512));
    for h in decided() {
        let c = hashkit::hash(h, &key).expect("non-empty key has a defined C hash");
        let r = rs::hash(rust_type(h), &key);
        assert_same(h, &key, &c, &r);
    }
}

#[test]
fn every_hash_matches_c_on_edge_keys() {
    // Lengths around every block boundary the C implementations branch on
    // (4, 8, 12, 16, 64), starting at one byte. The empty key has no defined
    // C hash; see `c_oracle::hashkit::hash`.
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for len in 1..=130usize {
        keys.push((0..len).map(|i| u8::try_from(i % 251).unwrap()).collect());
        keys.push(vec![0xff; len]);
    }
    for key in &keys {
        for h in decided() {
            let c = hashkit::hash(h, key).expect("non-empty key has a defined C hash");
            let r = rs::hash(rust_type(h), key);
            assert_same(h, key, &c, &r);
        }
    }
}

#[hegel::test(test_cases = 2000)]
fn md5_signature_matches_c(tc: TestCase) {
    let key: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>()).max_size(512));
    assert_eq!(hashkit::md5(&key), rs::md5_signature(&key), "key {key:?}");
}

#[hegel::test(test_cases = 2000)]
fn crc32_sz_matches_c(tc: TestCase) {
    let buf: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>()).max_size(512));
    let seed: u32 = tc.draw(gs::integers::<u32>());
    assert_eq!(
        hashkit::crc32_sz_c(&buf, seed),
        rs::crc32_sz(&buf, seed),
        "buf {buf:?} seed {seed:#x}"
    );
}

/// C `get_hash_type` / `get_hash_func`: every configuration name resolves to
/// the same position in the hash table as `HashType::from_name` /
/// `HashType::all`, the function at that position is the same hash, and an
/// unknown or case-changed name is rejected by both (C returns
/// `HASH_INVALID`, the table length).
#[test]
fn hash_name_lookup_matches_c() {
    let names: Vec<&str> = CHash::ALL.iter().map(|&h| rust_type(h).as_str()).collect();
    for (i, name) in names.iter().enumerate() {
        let c_index = hashkit::hash_type_index(name.as_bytes());
        assert_eq!(usize::try_from(c_index).unwrap(), i, "C index of {name}");
        let rust = HashType::from_name(name).expect("Rust knows every C name");
        assert_eq!(HashType::all()[i], rust, "Rust table position of {name}");
        for key in [&b"a"[..], b"hello", b"{tag}:key", &[0xff; 70][..]] {
            let c = hashkit::hash_by_index(c_index, key).expect("C function for a valid index");
            if DEVIATES.contains(&CHash::ALL[i]) {
                continue;
            }
            assert_same(CHash::ALL[i], key, &c, &rs::hash(rust, key));
        }
    }
    let invalid = i32::try_from(names.len()).unwrap();
    for bad in ["", "MURMUR3", "murmur3 ", "nope", "crc32b"] {
        assert_eq!(
            hashkit::hash_type_index(bad.as_bytes()),
            invalid,
            "C on {bad:?}"
        );
        assert_eq!(HashType::from_name(bad), None, "Rust on {bad:?}");
    }
    assert!(hashkit::hash_by_index(invalid, b"k").is_none());
    assert!(hashkit::hash_by_index(-1, b"k").is_none());
}
