//! Vector encodings.
//!
//! Three compression schemes are available:
//!
//! * [`Int8Quantized`] -- per-vector scalar quantisation to `u8`,
//!   storing the per-vector minimum and scale alongside. Roughly 4x
//!   compression vs raw `f32`; reconstruction error is in the
//!   0.5-1.0 percent range for typical embedding distributions.
//! * [`Fp16`] -- IEEE 754 half-precision floats. 2x compression;
//!   reconstruction error around 0.05 percent.
//! * [`Turbovec`] -- 2/3/4-bit data-oblivious quantisation backed
//!   by the `turbovec` crate's TurboQuant codec. The compressed
//!   payload is held in a per-table SIMD-friendly index; the
//!   per-row [`EncodedVector`] holds the original `f32` bytes so
//!   round-trip and rehydration paths remain exact while the
//!   in-memory ANN index uses the 8x to 16x compressed packed
//!   representation. See [`turbovec`] for the on-disk packed
//!   layout and the SIMD search kernels.
//!
//! The `Int8Quantized` and `Fp16` encodings round-trip every
//! vector to within their respective error budgets and preserve
//! dimension count exactly. `Turbovec` round-trips losslessly at
//! the row layer because the compressed representation lives in
//! the table's [`crate::turbo_index::TurboTable`] alongside the row
//! store, not in the row payload itself; quantisation loss is
//! exposed through the search-path scoring (see
//! [`distance_turbovec`]).
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

use crate::distance::Distance;

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
    /// `turbovec` 2-bit TurboQuant codec, 16x nominal compression.
    Turbovec2Bit,
    /// `turbovec` 3-bit TurboQuant codec, 10.6x nominal compression.
    Turbovec3Bit,
    /// `turbovec` 4-bit TurboQuant codec, 8x nominal compression.
    Turbovec4Bit,
}

impl Codec {
    /// Build the encoder bound to this codec.
    #[must_use]
    pub fn encoder(self) -> Box<dyn Encoder> {
        match self {
            Self::Int8Quantized => Box::new(Int8Quantized),
            Self::Fp16 => Box::new(Fp16),
            Self::Turbovec2Bit => Box::new(Turbovec::new(2)),
            Self::Turbovec3Bit => Box::new(Turbovec::new(3)),
            Self::Turbovec4Bit => Box::new(Turbovec::new(4)),
        }
    }

    /// Encoder name suitable for logging.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Int8Quantized => "int8q",
            Self::Fp16 => "fp16",
            Self::Turbovec2Bit => "turbovec2",
            Self::Turbovec3Bit => "turbovec3",
            Self::Turbovec4Bit => "turbovec4",
        }
    }

    /// `Some(bit_width)` for the turbovec codecs, `None`
    /// otherwise. Used by the storage layer to dispatch to the
    /// SIMD-backed table state.
    #[must_use]
    pub fn turbovec_bits(self) -> Option<u8> {
        match self {
            Self::Turbovec2Bit => Some(2),
            Self::Turbovec3Bit => Some(3),
            Self::Turbovec4Bit => Some(4),
            _ => None,
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
    /// `bit_width` parameter outside the supported range. The
    /// turbovec codec accepts only 2, 3, or 4.
    #[error("turbovec bit width must be 2, 3, or 4, got {0}")]
    UnsupportedBitWidth(u8),
    /// Vector dimension is incompatible with the requested
    /// codec. The turbovec codec requires `dim` to be a positive
    /// multiple of 8.
    #[error("turbovec requires dim to be a positive multiple of 8, got {0}")]
    UnsupportedDim(u16),
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
/// Each component is converted via `half::f16::from_f32`. Storage is
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

/// `turbovec` 2/3/4-bit TurboQuant encoder.
///
/// The on-row payload is the original vector's `f32`
/// little-endian bytes. Compression and SIMD scoring happen at
/// the per-table layer (see [`crate::turbo_index::TurboTable`]) where
/// a [`turbovec::TurboQuantIndex`] holds every vector in its
/// 2/3/4-bit packed form. Per-row storage stays at 4 * `dim`
/// bytes; the in-memory ANN index achieves the headline 8x
/// (4-bit) or 16x (2-bit) compression on the slot table.
///
/// The on-row passthrough is deliberate: turbovec's public API
/// does not expose per-vector reconstruction, so a faithful
/// `decode` implementation requires keeping the source bytes.
/// The trade-off (no row-layer compression) is documented in
/// `docs/dynvecdb/architecture.md`.
#[derive(Clone, Copy, Debug)]
pub struct Turbovec {
    /// Bit width: 2, 3, or 4.
    bits: u8,
}

impl Turbovec {
    /// Build a turbovec encoder for the given bit width.
    ///
    /// # Panics
    ///
    /// Panics on a bit width outside `{2, 3, 4}`. Use
    /// [`Codec::encoder`] in the dynamic-dispatch path to keep
    /// the panic out of caller code; the codec variants are
    /// pre-validated.
    #[must_use]
    pub fn new(bits: u8) -> Self {
        assert!(
            (2..=4).contains(&bits),
            "turbovec bit width must be 2, 3, or 4"
        );
        Self { bits }
    }

    /// Bit width this encoder was built with.
    #[must_use]
    pub fn bits(self) -> u8 {
        self.bits
    }

    fn codec_for(bits: u8) -> Codec {
        match bits {
            2 => Codec::Turbovec2Bit,
            3 => Codec::Turbovec3Bit,
            4 => Codec::Turbovec4Bit,
            _ => unreachable!("turbovec bits validated in Turbovec::new"),
        }
    }
}

impl Encoder for Turbovec {
    fn codec(&self) -> Codec {
        Self::codec_for(self.bits)
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
        if dim == 0 || !dim.is_multiple_of(8) {
            return Err(EncodingError::UnsupportedDim(dim));
        }
        let mut bytes = Vec::with_capacity(values.len() * 4);
        for &v in values {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        Ok(EncodedVector {
            codec: Self::codec_for(self.bits),
            dim,
            bytes,
            params: vec![f32::from(self.bits)],
        })
    }

    fn decode(&self, ev: &EncodedVector) -> Result<Vec<f32>, EncodingError> {
        let expected = Self::codec_for(self.bits);
        if ev.codec != expected {
            return Err(EncodingError::CodecMismatch {
                expected,
                got: ev.codec,
            });
        }
        if ev.bytes.len() != usize::from(ev.dim) * 4 {
            return Err(EncodingError::Malformed {
                dim: ev.dim,
                bytes: ev.bytes.len(),
            });
        }
        let mut out = Vec::with_capacity(usize::from(ev.dim));
        for chunk in ev.bytes.chunks_exact(4) {
            let arr: [u8; 4] = [chunk[0], chunk[1], chunk[2], chunk[3]];
            out.push(f32::from_le_bytes(arr));
        }
        Ok(out)
    }
}

/// Encode a single `f32` vector under the turbovec codec at
/// `bits` bit-width.
///
/// The returned [`EncodedVector`] holds the source vector's
/// `f32` little-endian bytes; the codec marker on the
/// [`EncodedVector`] indicates that the table-level SIMD index
/// owns the compressed representation. See module docs for the
/// rationale.
///
/// # Errors
///
/// [`EncodingError::UnsupportedBitWidth`] for `bits` outside
/// `{2, 3, 4}`; [`EncodingError::EmptyVector`] for a zero-dim
/// input; [`EncodingError::UnsupportedDim`] when `dim` is not a
/// positive multiple of 8; [`EncodingError::DimensionTooLarge`]
/// for >65535 dimensions; [`EncodingError::NonFinite`] for any
/// non-finite component.
pub fn encode_turbovec(vector: &[f32], bits: u8) -> Result<EncodedVector, EncodingError> {
    if !(2..=4).contains(&bits) {
        return Err(EncodingError::UnsupportedBitWidth(bits));
    }
    Turbovec::new(bits).encode(vector)
}

/// Decode a turbovec-encoded blob back to `Vec<f32>`.
///
/// Round-trips losslessly because the row-level payload holds
/// the source `f32` bytes; quantisation loss is a property of
/// the search-time scoring path, not of the row.
///
/// # Errors
///
/// [`EncodingError::UnsupportedBitWidth`] for `bits` outside
/// `{2, 3, 4}`; [`EncodingError::CodecMismatch`] when the
/// [`EncodedVector`] was produced under a different bit width;
/// [`EncodingError::Malformed`] when the payload size does not
/// match the recorded dimension.
pub fn decode_turbovec(ev: &EncodedVector, bits: u8) -> Result<Vec<f32>, EncodingError> {
    if !(2..=4).contains(&bits) {
        return Err(EncodingError::UnsupportedBitWidth(bits));
    }
    Turbovec::new(bits).decode(ev)
}

/// Score `query` against a single turbovec-stored vector at the
/// given [`Distance`] metric, using turbovec's SIMD-accelerated
/// search path against an ephemeral one-element index.
///
/// Returns `f32::INFINITY` if `stored` is not a turbovec
/// payload, if the stored vector cannot be added to the
/// ephemeral index (e.g. a coordinate magnitude trips
/// turbovec's input validation), or if the dimension is not a
/// supported turbovec dim. Smaller scores are closer; cosine
/// and dot-product are mapped from turbovec's similarity score
/// by negation, and euclidean is approximated through the
/// inner-product surrogate after L2 normalisation.
///
/// This is a single-pair surrogate; the bulk-search path used
/// by the table layer issues one batched call to
/// [`turbovec::TurboQuantIndex::search`] across all rows and
/// avoids the ephemeral-index cost.
#[must_use]
pub fn distance_turbovec(query: &[f32], stored: &EncodedVector, metric: Distance) -> f32 {
    let Some(bits) = stored.codec.turbovec_bits() else {
        return f32::INFINITY;
    };
    if usize::from(stored.dim) != query.len() {
        return f32::INFINITY;
    }
    let dim = usize::from(stored.dim);
    if dim == 0 || !dim.is_multiple_of(8) {
        return f32::INFINITY;
    }
    let Ok(stored_vec) = decode_turbovec(stored, bits) else {
        return f32::INFINITY;
    };
    let Ok(mut index) = turbovec::TurboQuantIndex::new(dim, usize::from(bits)) else {
        return f32::INFINITY;
    };
    // Cosine and Euclidean both decompose into an inner
    // product on the L2-normalised inputs; we hand turbovec
    // the normalised forms so its inner-product surrogate
    // produces the right ordering.
    let (q_input, s_input) = match metric {
        Distance::DotProduct => (query.to_vec(), stored_vec.clone()),
        Distance::Cosine | Distance::Euclidean => (l2_normalise(query), l2_normalise(&stored_vec)),
    };
    if index.add_2d(&s_input, dim).is_err() {
        return f32::INFINITY;
    }
    let results = index.search(&q_input, 1);
    if results.scores.is_empty() {
        return f32::INFINITY;
    }
    let similarity = results.scores[0];
    match metric {
        Distance::DotProduct => -similarity,
        // For the L2-normalised inputs, similarity is an
        // estimate of cos(theta); cosine distance is `1 - cos`.
        Distance::Cosine => 1.0 - similarity,
        // sqrt(2 - 2 cos(theta)) for unit vectors. Clamp the
        // inside of the sqrt at 0 to absorb rounding noise on
        // identical inputs.
        Distance::Euclidean => (2.0 - 2.0 * similarity).max(0.0).sqrt(),
    }
}

/// L2-normalise a vector. Zero-norm input is returned
/// unchanged so callers do not produce NaN.
fn l2_normalise(v: &[f32]) -> Vec<f32> {
    let n2: f32 = v.iter().map(|x| x * x).sum();
    let n = n2.sqrt();
    if n <= 0.0 {
        return v.to_vec();
    }
    v.iter().map(|x| x / n).collect()
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

    #[test]
    fn turbovec_encode_round_trips_at_row_layer() {
        let v: Vec<f32> = (0_i16..64).map(|i| f32::from(i) * 0.05 - 1.6).collect();
        let enc = encode_turbovec(&v, 4).unwrap();
        assert_eq!(enc.codec, Codec::Turbovec4Bit);
        assert_eq!(enc.dim as usize, v.len());
        let dec = decode_turbovec(&enc, 4).unwrap();
        // Row-layer round-trip is exact (passthrough).
        assert_eq!(dec, v);
    }

    #[test]
    fn turbovec_rejects_unsupported_bit_width() {
        let v = vec![0.1_f32; 8];
        assert!(matches!(
            encode_turbovec(&v, 5),
            Err(EncodingError::UnsupportedBitWidth(5))
        ));
    }

    #[test]
    fn turbovec_rejects_non_multiple_of_eight_dim() {
        let v = vec![0.1_f32; 7];
        assert!(matches!(
            encode_turbovec(&v, 4),
            Err(EncodingError::UnsupportedDim(7))
        ));
    }
}
