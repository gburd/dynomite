//! Crypto throughput benches: AES-128-CBC, RSA OAEP, and PEM load.
//!
//! Run with `cargo bench --bench crypto`. The harness exercises:
//!
//! * `aes::encrypt_to_vec` and `aes::decrypt_to_vec` over five
//!   payload sizes (16 / 64 / 256 / 1024 / 4096 bytes)
//! * `Crypto::rsa_encrypt` and `rsa_decrypt` (OAEP wrap / unwrap of
//!   a 32-byte AES key, the same shape the DNODE handshake uses)
//! * `Crypto::from_pem` (cold load of a 2048-bit RSA private key)
//!
//! The corresponding baseline JSON file lives at
//! `crates/dynomite/benches/baseline/crypto.json`.

#![allow(missing_docs)]

use std::path::PathBuf;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dynomite::crypto::aes::{decrypt_to_vec, encrypt_to_vec};
use dynomite::crypto::Crypto;

const PAYLOAD_SIZES: [usize; 5] = [16, 64, 256, 1024, 4096];

fn pem_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("crypto")
        .join("dynomite.pem")
}

fn aes_encrypt(c: &mut Criterion) {
    let key = Crypto::generate_aes_key().expect("rng must seed");
    let mut group = c.benchmark_group("aes_encrypt");
    for &p in &PAYLOAD_SIZES {
        let payload = vec![b'x'; p];
        group.throughput(Throughput::Bytes(p as u64));
        group.bench_with_input(BenchmarkId::from_parameter(p), &payload, |b, payload| {
            b.iter(|| {
                let _ = encrypt_to_vec(black_box(payload), black_box(&key));
            });
        });
    }
    group.finish();
}

fn aes_decrypt(c: &mut Criterion) {
    let key = Crypto::generate_aes_key().expect("rng must seed");
    let mut group = c.benchmark_group("aes_decrypt");
    for &p in &PAYLOAD_SIZES {
        let payload = vec![b'x'; p];
        let cipher = encrypt_to_vec(&payload, &key).unwrap();
        group.throughput(Throughput::Bytes(cipher.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(p), &cipher, |b, cipher| {
            b.iter(|| {
                let _ = decrypt_to_vec(black_box(cipher), black_box(&key));
            });
        });
    }
    group.finish();
}

fn rsa_wrap_unwrap(c: &mut Criterion) {
    let pem = pem_path();
    if !pem.exists() {
        // Fixture missing on this checkout; skip rather than panic.
        return;
    }
    let crypto = Crypto::from_pem(&pem).expect("test PEM must load");
    let key = Crypto::generate_aes_key().expect("rng must seed");
    let wrapped = crypto.rsa_encrypt(&key).expect("RSA wrap");

    c.bench_function("rsa_oaep_encrypt", |b| {
        b.iter(|| {
            let _ = crypto.rsa_encrypt(black_box(&key));
        });
    });
    c.bench_function("rsa_oaep_decrypt", |b| {
        b.iter(|| {
            let _ = crypto.rsa_decrypt(black_box(&wrapped));
        });
    });
}

fn pem_load(c: &mut Criterion) {
    let pem = pem_path();
    if !pem.exists() {
        return;
    }
    c.bench_function("pem_load_rsa_2048", |b| {
        b.iter(|| {
            let _ = Crypto::from_pem(black_box(&pem));
        });
    });
}

criterion_group!(benches, aes_encrypt, aes_decrypt, rsa_wrap_unwrap, pem_load);
criterion_main!(benches);
