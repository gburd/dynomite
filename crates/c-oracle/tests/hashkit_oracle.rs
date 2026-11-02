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

/// The hash functions whose C behaviour is pending an owner decision and is
/// therefore not yet a transliteration target. Each entry names its
/// `deviations.md` record. A function leaves this list only when the
/// decision lands and the Rust side matches it.
///
/// * `Murmur3`: the C `hash_murmur3` has its `MurmurHash3_x86_128` call
///   commented out (since 2013-12-10, upstream 9eb54c1), so it returns a
///   zero token for every key. See DEV-H1.
/// * `OneAtATime`: the C reads the key through `const char *`, so on
///   targets where `char` is signed (x86-64, where the C reference is built)
///   bytes >= 0x80 are sign-extended before being added. The Rust reads
///   `u8`. See DEV-H2.
const PENDING_DECISION: &[CHash] = &[CHash::Murmur3, CHash::OneAtATime];

fn decided() -> impl Iterator<Item = CHash> {
    CHash::ALL
        .into_iter()
        .filter(|h| !PENDING_DECISION.contains(h))
}

/// Pins the C behaviour behind DEV-H2: for ASCII-only keys (every byte below
/// 0x80) the sign extension is a no-op and C and Rust agree, so the
/// divergence is confined to keys with high bytes.
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

/// Pins the C behaviour behind DEV-H1 so the oracle notices if the C pin
/// ever changes: C murmur3 returns the zero token for every key.
#[hegel::test(test_cases = 200)]
fn c_murmur3_is_constant_zero(tc: TestCase) {
    let key: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(1).max_size(256));
    let c = hashkit::hash(CHash::Murmur3, &key).expect("non-empty key");
    assert_eq!((c.signum, c.mag), (0, [0; 4]), "key {key:?}");
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
