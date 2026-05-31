//! Generates the JSON conformance corpus consumed by
//! `crates/dynomite/tests/stage_03_hashkit.rs`.
//!
//! Run with `cargo run -p dynomited --example generate_hashkit_vectors`.
//! The output is meant to be redirected into
//! `crates/dynomite/tests/fixtures/hashkit_vectors.json` and reviewed
//! before committing.

#![allow(
    clippy::format_push_string,
    clippy::needless_continue,
    clippy::unnecessary_debug_formatting,
    clippy::match_same_arms,
    clippy::similar_names,
    clippy::cast_possible_truncation
)]
use dynomite::hashkit::{hash, HashType};

fn main() {
    let mut keys: Vec<Vec<u8>> = vec![
        b"".to_vec(),
        b"a".to_vec(),
        b"ab".to_vec(),
        b"abc".to_vec(),
        b"abcd".to_vec(),
        b"abcde".to_vec(),
        b"abcdef".to_vec(),
        b"abcdefg".to_vec(),
        b"the quick brown fox jumps over the lazy dog".to_vec(),
        b"123456789".to_vec(),
        b"netflix:dynomite:rs".to_vec(),
        b"x".repeat(64),
        b"k".repeat(63),
        b"y".repeat(65),
        vec![0u8; 16],
        vec![0xffu8; 16],
    ];
    for n in 0..50u8 {
        keys.push(vec![n; (n as usize) % 13 + 1]);
    }

    let mut rows: Vec<String> = Vec::new();
    for ty in HashType::all().iter().copied() {
        for key in &keys {
            // Skip jenkins on length 0: the C reference takes an early
            // return that leaves the token in an implementation-defined
            // state and would not be a useful regression anchor.
            if matches!(ty, HashType::Jenkins) && key.is_empty() {
                continue;
            }
            let token = hash(ty, key);
            let row = format!(
                "    {{\"algo\": {}, \"key_hex\": {}, \"token_hex\": {}}}",
                json_str(ty.as_str()),
                json_str(&hex_encode(key)),
                json_str(&token.to_hex()),
            );
            rows.push(row);
        }
    }

    println!("[");
    let n = rows.len();
    for (i, row) in rows.iter().enumerate() {
        if i + 1 == n {
            println!("{row}");
        } else {
            println!("{row},");
        }
    }
    println!("]");
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
