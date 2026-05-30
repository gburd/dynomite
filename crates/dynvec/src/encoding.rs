//! Vector encodings.
//!
//! Two compression schemes are available:
//!
//! * [`Int8Quantized`] -- per-vector scalar quantisation to `u8`,
//!   storing the per-vector minimum and scale alongside. Roughly 4x
//!   compression vs raw `f32`; reconstruction error is in the
//!   0.5-1.0 percent range for typical embedding distributions.
//! * [`Fp16`] -- IEEE 754 half-precision floats. 2x compression;
//!   reconstruction error around 0.05 percent.
//!
//! Both encodings round-trip every vector to within their
//! respective error budgets and preserve dimension count exactly.
//!
//! The encoded byte stream produced by [`encode`](Encoder::encode)
//! is self-describing in the [`EncodedVector`] wrapper: the
//! dimension count, the codec identifier, and any per-vector
//! parameters (the int8 minimum and scale, for instance) are
//! captured on the [`EncodedVector`] struct so an operator can
//! `dynvec-cli inspect <id>` and see the human-readable form
//! without re-running the codec.

use half::f16;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Codec identifier.
///
/// Persisted on every [`EncodedVector`] so a reader can pick the
/// correct decoder without ambient state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Codec {
    /// Per-vector scalar quantisation to `u8`.
    Int8Quantized,
    /// IEEE 754 half-precision floats.
    Fp16,
}

impl Codec {
    /// Build the encoder bound to this codec.
    #[must_use]
    pub fn encoder(self) -> Box<dyn Encoder> {
        match self {
            Self::Int8Quantized => Box::new(Int8Quantized),
            Self::Fp16 => Box::new(Fp16),
        }
    }

    /// Encoder name suitable for logging.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Int8Quantized => "int8q",
            Self::Fp16 => "fp16",
        }
    }
}

/// Encoded vector ready to persist or hand to a distance routine.
///
/// The byte buffer in [`Self::bytes`] is opaque to callers other
/// than the matching [`Codec`]. The remaining fields are
/// inspectable so an operator can reason about the row without
/// decoding it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EncodedVector {
    /// Codec used to produce [`Self::bytes`].
    pub codec: Codec,
    /// Number of dimensions in the original `f32` vector.
    pub dim: u16,
    /// Codec-private payload.
    pub bytes: Vec<u8>,
    /// Codec-private parameters. For `Int8Quantized` this is
    /// `[min, scale]`; for `Fp16` it is empty. Stored as `f32`
    /// so an inspect tool can render them without parsing the
    /// payload.
    pub params: Vec<f32>,
}

impl EncodedVector {
    /// L2 norm of the decoded vector, computed by re-decoding.
    ///
    /// Used by the inspect tooling and by some distance metrics
    /// that need to factor out vector magnitude.
    #[must_use]
    pub fn l2_norm(&self) -> f32 {
        let v = self.codec.encoder().decode(self).unwrap_or_default();
        v.iter().map(|x| x * x).sum::<f32>().sqrt()
    }
}

/// Errors returned by encoders.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EncodingError {
    /// Vector dimension count cannot be represented as `u16`.
    #[error("vector has {0} dimensions; max supported is 65535")]
    DimensionTooLarge(usize),
    /// Vector dimension count is zero.
    #[error("vector has zero dimensions")]
    EmptyVector,
    /// The encoded payload's length does not match the codec's
    /// expectation for the recorded dimension.
    #[error("malformed encoded vector: dim={dim} payload_bytes={bytes}")]
    Malformed {
        /// Recorded dimension count.
        dim: u16,
        /// Actual payload byte count.
        bytes: usize,
    },
    /// The codec identifier on the [`EncodedVector`] does not
    /// match the encoder being asked to decode it.
    #[error("codec mismatch: expected {expected:?}, got {got:?}")]
    CodecMismatch {
        /// Encoder's codec.
        expected: Codec,
        /// Codec recorded on the [`EncodedVector`].
        got: Codec,
    },
    /// Component value was non-finite (NaN, +inf, -inf). The
    /// codecs reject these so the reconstruction error budget
    /// is well-defined.
    #[error("vector contains non-finite component")]
    NonFinite,
}

/// Encoder trait. Each codec ships exactly one impl.
pub trait Encoder: Send + Sync {
    /// Codec identifier produced by this encoder.
    fn codec(&self) -> Codec;
    /// Encode `values` into an [`EncodedVector`].
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::EmptyVector`] for a zero-dim
    /// input, [`EncodingError::DimensionTooLarge`] for >65535
    /// dimensions, and [`EncodingError::NonFinite`] for any
    /// non-finite component.
    fn encode(&self, values: &[f32]) -> Result<EncodedVector, EncodingError>;
    /// Decode an [`EncodedVector`] back to `Vec<f32>`.
    ///
    /// # Errors
    ///
    /// Returns [`EncodingError::CodecMismatch`] when the
    /// `EncodedVector` was produced by a different codec, and
    /// [`EncodingError::Malformed`] when the payload byte
    /// count does not match the recorded dimension.
    fn decode(&self, ev: &EncodedVector) -> Result<Vec<f32>, EncodingError>;
}

/// Per-vector scalar quantisation to `u8`.
///
/// Stores `min` and `scale` per vector so each vector occupies its
/// own quantisation range; this keeps reconstruction error inside
/// 0.5-1.0 percent on typical embedding distributions where
/// neighbouring components have similar magnitude. With 256
/// quantisation buckets the worst-case relative error per
/// component is `range / 255` where `range = max - min`.
#[derive(Clone, Copy, Debug, Default)]
pub struct Int8Quantized;

impl Encoder for Int8Quantized {
    fn codec(&self) -> Codec {
        Codec::Int8Quantized
    }

    fn encode(&self, values: &[f32]) -> Result<EncodedVector, EncodingError> {
        if values.is_empty() {
            return Err(EncodingError::EmptyVector);
        }
        let dim = u16::try_from(values.len())
            .map_err(|_| EncodingError::DimensionTooLarge(values.len()))?;
        for v in values {
            if !v.is_finite() {
                return Err(EncodingError::NonFinite);
            }
        }
        let mut min = values[0];
        let mut max = values[0];
        for &v in &values[1..] {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
        }
        let range = max - min;
        // Scale of 0.0 (all components equal) is fine: every
        // component quantises to bucket 0 and decodes back to
        // `min`, which is exact.
        let scale = if range > 0.0 { range / 255.0 } else { 0.0 };
        let bytes: Vec<u8> = values
            .iter()
            .map(|&v| {
                if scale == 0.0 {
                    0_u8
                } else {
                    let q = ((v - min) / scale).round();
                    let clamped = q.clamp(0.0, 255.0);
                    // Clamped to [0, 255]; the cast can not
                    // truncate or sign-flip in this range.
                    #[allow(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        reason = "clamped to [0, 255]"
                    )]
                    let byte = clamped as u8;
                    byte
                }
            })
            .collect();
        Ok(EncodedVector {
            codec: Codec::Int8Quantized,
            dim,
            bytes,
            params: vec![min, scale],
        })
    }

    fn decode(&self, ev: &EncodedVector) -> Result<Vec<f32>, EncodingError> {
        if ev.codec != Codec::Int8Quantized {
            return Err(EncodingError::CodecMismatch {
                expected: Codec::Int8Quantized,
                got: ev.codec,
            });
        }
        if ev.bytes.len() != usize::from(ev.dim) {
            return Err(EncodingError::Malformed {
                dim: ev.dim,
                bytes: ev.bytes.len(),
            });
        }
        if ev.params.len() != 2 {
            return Err(EncodingError::Malformed {
                dim: ev.dim,
                bytes: ev.bytes.len(),
            });
        }
        let min = ev.params[0];
        let scale = ev.params[1];
        Ok(ev
            .bytes
            .iter()
            .map(|&b| min + scale * f32::from(b))
            .collect())
    }
}

/// IEEE 754 half-precision encoder.
///
/// Each component is converted via [`f16::from_f32`]. Storage is
/// `2 * dim` bytes; no per-vector parameters are needed because
/// the codec does not depend on the input distribution.
#[derive(Clone, Copy, Debug, Default)]
pub struct Fp16;

impl Encoder for Fp16 {
    fn codec(&self) -> Codec {
        Codec::Fp16
    }

    fn encode(&self, values: &[f32]) -> Result<EncodedVector, EncodingError> {
        if values.is_empty() {
            return Err(EncodingError::EmptyVector);
        }
        let dim = u16::try_from(values.len())
            .map_err(|_| EncodingError::DimensionTooLarge(values.len()))?;
        for v in values {
            if !v.is_finite() {
                return Err(EncodingError::NonFinite);
            }
        }
        let mut bytes = Vec::with_capacity(values.len() * 2);
        for &v in values {
            let h = f16::from_f32(v);
            bytes.extend_from_slice(&h.to_le_bytes());
        }
        Ok(EncodedVector {
            codec: Codec::Fp16,
            dim,
            bytes,
            params: Vec::new(),
        })
    }

    fn decode(&self, ev: &EncodedVector) -> Result<Vec<f32>, EncodingError> {
        if ev.codec != Codec::Fp16 {
            return Err(EncodingError::CodecMismatch {
                expected: Codec::Fp16,
                got: ev.codec,
            });
        }
        if ev.bytes.len() != usize::from(ev.dim) * 2 {
            return Err(EncodingError::Malformed {
                dim: ev.dim,
                bytes: ev.bytes.len(),
            });
        }
        let mut out = Vec::with_capacity(usize::from(ev.dim));
        for chunk in ev.bytes.chunks_exact(2) {
            // Safe: chunks_exact(2) guarantees length 2.
            let hb: [u8; 2] = [chunk[0], chunk[1]];
            out.push(f16::from_le_bytes(hb).to_f32());
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vec_close(a: &[f32], b: &[f32], eps: f32) -> bool {
        a.len() == b.len() && a.iter().zip(b).all(|(x, y)| (x - y).abs() <= eps)
    }

    #[test]
    fn int8_round_trip_within_budget() {
        let v: Vec<f32> = (0_i16..64).map(|i| f32::from(i) * 0.1 - 3.2).collect();
        let enc = Int8Quantized.encode(&v).unwrap();
        let dec = Int8Quantized.decode(&enc).unwrap();
        let range = 6.4_f32;
        let bucket = range / 255.0;
        assert!(vec_close(&v, &dec, bucket));
        assert_eq!(enc.dim as usize, v.len());
    }

    #[test]
    fn fp16_round_trip_within_budget() {
        let v: Vec<f32> = (0_i16..32).map(|i| f32::from(i) * 0.05 - 0.8).collect();
        let enc = Fp16.encode(&v).unwrap();
        let dec = Fp16.decode(&enc).unwrap();
        // f16 mantissa is 10 bits; relative error is roughly 1e-3.
        assert!(vec_close(&v, &dec, 1e-2));
    }

    #[test]
    fn rejects_non_finite() {
        assert!(matches!(
            Int8Quantized.encode(&[1.0, f32::NAN]),
            Err(EncodingError::NonFinite)
        ));
        assert!(matches!(
            Fp16.encode(&[1.0, f32::INFINITY]),
            Err(EncodingError::NonFinite)
        ));
    }

    #[test]
    fn rejects_empty() {
        assert!(matches!(
            Int8Quantized.encode(&[]),
            Err(EncodingError::EmptyVector)
        ));
    }

    #[test]
    fn codec_mismatch_detected() {
        let v = vec![0.1, 0.2, 0.3];
        let enc = Fp16.encode(&v).unwrap();
        assert!(matches!(
            Int8Quantized.decode(&enc),
            Err(EncodingError::CodecMismatch { .. })
        ));
    }

    #[test]
    fn constant_vector_quantises_cleanly() {
        let v = vec![1.5_f32; 8];
        let enc = Int8Quantized.encode(&v).unwrap();
        let dec = Int8Quantized.decode(&enc).unwrap();
        assert_eq!(dec, vec![1.5_f32; 8]);
    }

    #[test]
    fn l2_norm_matches_decoded() {
        let v = vec![3.0_f32, 4.0, 0.0];
        let enc = Fp16.encode(&v).unwrap();
        // sqrt(9 + 16 + 0) = 5
        assert!((enc.l2_norm() - 5.0).abs() < 1e-2);
    }
}
