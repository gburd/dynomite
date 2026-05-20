//! Conformance test for the hashkit module.
//!
//! Reads a frozen fixture of `(algorithm, key, expected_token_hex)`
//! rows and confirms that the current implementation reproduces every
//! row exactly. The fixture combines well-known specification vectors
//! (RFC 1321 MD5 inputs, the FNV-1a 32-bit reference set) with values
//! produced by walking the C source line by line for the algorithm
//! specifics that have no canonical test vectors (CRC-16/CCITT,
//! libmemcached CRC-32, MurmurHash 2 with the C seed of `0xdeadbeef *
//! length`, Bob Jenkins lookup3 in byte-by-byte mode, and MurmurHash3
//! x86_128 with the seed `0xc0a1e5ce` chosen by the C source).
//!
//! Once committed, the fixture is the regression contract: any change
//! that breaks even one row blocks merge.

use std::fs;
use std::path::PathBuf;

use dynomite::hashkit::{hash, HashType};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Vector {
    algo: String,
    key_hex: String,
    token_hex: String,
}

fn fixture_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("fixtures");
    p.push("hashkit_vectors.json");
    p
}

fn parse_hex(s: &str) -> Vec<u8> {
    assert!(s.len() % 2 == 0, "odd-length hex {s:?}");
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hi = decode_nibble(bytes[i]);
        let lo = decode_nibble(bytes[i + 1]);
        out.push(hi * 16 + lo);
    }
    out
}

fn decode_nibble(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => panic!("not a hex byte: {b:#x}"),
    }
}

#[test]
fn fixture_has_at_least_100_rows() {
    let raw = fs::read_to_string(fixture_path()).expect("read fixture");
    let vectors: Vec<Vector> = serde_json::from_str(&raw).expect("parse fixture");
    assert!(
        vectors.len() >= 100,
        "fixture has only {} rows, need >= 100",
        vectors.len()
    );
}

#[test]
fn fixture_covers_every_algorithm() {
    let raw = fs::read_to_string(fixture_path()).expect("read fixture");
    let vectors: Vec<Vector> = serde_json::from_str(&raw).expect("parse fixture");
    for ty in HashType::all() {
        let n = vectors.iter().filter(|v| v.algo == ty.as_str()).count();
        assert!(n >= 5, "only {n} rows for {}, need >= 5", ty.as_str());
    }
}

#[test]
fn every_vector_matches_current_implementation() {
    let raw = fs::read_to_string(fixture_path()).expect("read fixture");
    let vectors: Vec<Vector> = serde_json::from_str(&raw).expect("parse fixture");

    let mut failures = Vec::new();
    for v in &vectors {
        let ty = HashType::from_name(&v.algo)
            .unwrap_or_else(|| panic!("unknown algo {} in fixture", v.algo));
        let key = parse_hex(&v.key_hex);
        let token = hash(ty, &key);
        let actual = token.to_hex();
        if actual != v.token_hex {
            failures.push(format!(
                "{}({}): expected {}, got {}",
                v.algo, v.key_hex, v.token_hex, actual
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn rfc_1321_md5_signatures_are_intact() {
    // Independent assertion that does not rely on the JSON corpus: if
    // the fixture is ever regenerated incorrectly, these RFC vectors
    // still anchor MD5.
    let cases: [(&[u8], &str); 2] = [
        // First 4 bytes of MD5("") = d4 1d 8c d9, read little-endian.
        (b"", "d98c1dd4"),
        // First 4 bytes of MD5("abc") = 90 01 50 98, read little-endian.
        (b"abc", "98500190"),
    ];
    for (key, expected) in cases {
        let token = hash(HashType::Md5, key);
        assert_eq!(token.to_hex(), expected, "md5({key:?}) drifted");
    }
}

#[test]
fn fnv1a_32_canonical_vectors() {
    // Vectors from the published FNV-1a 32-bit test suite.
    let cases = [
        (&b""[..], 0x811c_9dc5u32),
        (&b"a"[..], 0xe40c_292c),
        (&b"foobar"[..], 0xbf9c_f968),
    ];
    for (key, expected) in cases {
        let token = hash(HashType::Fnv1a_32, key);
        assert_eq!(token.get_int(), expected, "fnv1a_32({key:?}) drifted");
    }
}
