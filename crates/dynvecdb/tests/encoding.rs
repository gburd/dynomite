//! Encoding round-trip and accuracy tests.

use dynvecdb::encoding::{Codec, EncodingError, Fp16, Int8Quantized};
use dynvecdb::Encoder;

fn rand_vec(seed: u64, dim: usize, span: f32) -> Vec<f32> {
    let mut x = seed;
    let mut v = Vec::with_capacity(dim);
    for _ in 0..dim {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let bits = (x >> 11) & ((1_u64 << 53) - 1);
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            reason = "test fixture: deterministic PRNG narrowed to f32"
        )]
        let r = (((bits as f64) / ((1_u64 << 53) as f64)) * 2.0 - 1.0) as f32;
        v.push(r * span);
    }
    v
}

#[test]
fn int8_round_trip_preserves_dim() {
    for dim in [1_usize, 16, 64, 512] {
        let v = rand_vec(0xFEED_BEEF + dim as u64, dim, 5.0);
        let enc = Int8Quantized.encode(&v).unwrap();
        assert_eq!(usize::from(enc.dim), dim);
        let dec = Int8Quantized.decode(&enc).unwrap();
        assert_eq!(dec.len(), dim);
    }
}

#[test]
fn int8_reconstruction_within_quantum() {
    // For a uniform [-2, 2] vector, range is at most 4.0, so the
    // per-component reconstruction error is at most 4.0 / 255 ~= 0.0157.
    let v = rand_vec(0x00C0_FFEE, 256, 2.0);
    let enc = Int8Quantized.encode(&v).unwrap();
    let dec = Int8Quantized.decode(&enc).unwrap();
    for (a, b) in v.iter().zip(&dec) {
        assert!((a - b).abs() <= 4.0 / 255.0 + 1e-5, "a={a}, b={b}");
    }
}

#[test]
fn fp16_round_trip_preserves_dim() {
    for dim in [1_usize, 16, 64, 512] {
        let v = rand_vec(0x000A_BCDE + dim as u64, dim, 1.0);
        let enc = Fp16.encode(&v).unwrap();
        assert_eq!(usize::from(enc.dim), dim);
        assert_eq!(enc.bytes.len(), dim * 2);
        let dec = Fp16.decode(&enc).unwrap();
        assert_eq!(dec.len(), dim);
    }
}

#[test]
fn fp16_relative_error_below_threshold() {
    let v = rand_vec(0xDEAD_BEEF, 256, 1.0);
    let enc = Fp16.encode(&v).unwrap();
    let dec = Fp16.decode(&enc).unwrap();
    for (a, b) in v.iter().zip(&dec) {
        // f16 has 10 mantissa bits => relative error ~ 2^-10 ~= 1e-3.
        let denom = a.abs().max(1e-6);
        assert!((a - b).abs() / denom < 2e-3, "a={a}, b={b}");
    }
}

#[test]
fn rejects_non_finite_components() {
    assert!(matches!(
        Int8Quantized.encode(&[1.0, f32::NAN]),
        Err(EncodingError::NonFinite)
    ));
    assert!(matches!(
        Fp16.encode(&[f32::NEG_INFINITY, 0.0]),
        Err(EncodingError::NonFinite)
    ));
}

#[test]
fn codec_identifier_round_trips() {
    let v = vec![0.1, 0.2, 0.3, 0.4];
    let enc1 = Codec::Int8Quantized.encoder().encode(&v).unwrap();
    assert_eq!(enc1.codec, Codec::Int8Quantized);
    let enc2 = Codec::Fp16.encoder().encode(&v).unwrap();
    assert_eq!(enc2.codec, Codec::Fp16);
}

#[test]
fn malformed_payload_detected() {
    let v = vec![0.1, 0.2, 0.3];
    let mut enc = Fp16.encode(&v).unwrap();
    enc.bytes.truncate(enc.bytes.len() - 1);
    assert!(matches!(
        Fp16.decode(&enc),
        Err(EncodingError::Malformed { .. })
    ));
}
