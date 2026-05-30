//! Distance metrics for vector search.
//!
//! Three metrics are supported: [`Distance::Euclidean`] (L2),
//! [`Distance::Cosine`], and [`Distance::DotProduct`]. All routines
//! operate on `f32` slices and run in scalar code; portable SIMD
//! is not used because the workspace forbids `unsafe_code` and
//! `std::simd` is still nightly-only.
//!
//! For ANN search, smaller scores mean closer. Cosine and
//! euclidean already have that orientation; dot product is
//! negated so the same comparator works across metrics.
//!
//! # Examples
//!
//! ```
//! use dynvec::distance::Distance;
//! let a = [1.0, 0.0, 0.0];
//! let b = [0.0, 1.0, 0.0];
//! assert!((Distance::Euclidean.score(&a, &b) - 2.0_f32.sqrt()).abs() < 1e-6);
//! assert!((Distance::Cosine.score(&a, &b) - 1.0).abs() < 1e-6);
//! ```

use serde::{Deserialize, Serialize};

/// A distance / similarity metric over `f32` vectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Distance {
    /// L2 distance: `sqrt(sum((a_i - b_i)^2))`.
    Euclidean,
    /// `1 - cos(theta)`, where `cos(theta) = (a . b) / (|a| |b|)`.
    /// Returns 0 for identical direction, 2 for opposite.
    Cosine,
    /// Negated dot product: `-(a . b)`. The negation makes the
    /// metric monotonically smaller-is-closer so a single
    /// comparator works across all three.
    DotProduct,
}

impl Distance {
    /// Score `a` against `b`. Smaller scores mean closer.
    ///
    /// # Panics
    ///
    /// Does not panic. Mismatched lengths return `f32::INFINITY`
    /// to signal "incomparable" without bringing the search
    /// down; production callers should validate dimensions
    /// upstream of this routine.
    #[must_use]
    pub fn score(self, a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return f32::INFINITY;
        }
        match self {
            Self::Euclidean => euclidean(a, b),
            Self::Cosine => cosine(a, b),
            Self::DotProduct => -dot(a, b),
        }
    }

    /// Human-readable name suitable for logging or JSON
    /// serialisation.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Euclidean => "euclidean",
            Self::Cosine => "cosine",
            Self::DotProduct => "dot",
        }
    }
}

/// L2 (euclidean) distance.
fn euclidean(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = 0.0_f32;
    for (x, y) in a.iter().zip(b) {
        let d = x - y;
        acc += d * d;
    }
    acc.sqrt()
}

/// `1 - cos(theta)`. Matches the orientation used by FAISS and
/// other ANN libraries: identical direction -> 0, orthogonal -> 1,
/// antiparallel -> 2.
///
/// Zero-norm vectors return 1.0 (orthogonal); this avoids
/// dividing by zero and matches the "no information" intuition.
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let mut dot_acc = 0.0_f32;
    let mut na = 0.0_f32;
    let mut nb = 0.0_f32;
    for (x, y) in a.iter().zip(b) {
        dot_acc += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = (na * nb).sqrt();
    if denom <= 0.0 {
        return 1.0;
    }
    1.0 - (dot_acc / denom)
}

/// Raw inner product `a . b`.
fn dot(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = 0.0_f32;
    for (x, y) in a.iter().zip(b) {
        acc += x * y;
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f32, b: f32) -> bool {
        (a - b).abs() < 1e-5
    }

    #[test]
    fn euclidean_known_values() {
        assert!(approx(
            Distance::Euclidean.score(&[0.0, 0.0], &[3.0, 4.0]),
            5.0
        ));
        assert!(approx(
            Distance::Euclidean.score(&[1.0, 1.0, 1.0], &[1.0, 1.0, 1.0]),
            0.0
        ));
    }

    #[test]
    fn cosine_known_values() {
        // Identical direction -> 0
        assert!(approx(
            Distance::Cosine.score(&[1.0, 0.0], &[2.0, 0.0]),
            0.0
        ));
        // Orthogonal -> 1
        assert!(approx(
            Distance::Cosine.score(&[1.0, 0.0], &[0.0, 1.0]),
            1.0
        ));
        // Antiparallel -> 2
        assert!(approx(
            Distance::Cosine.score(&[1.0, 0.0], &[-1.0, 0.0]),
            2.0
        ));
    }

    #[test]
    fn dot_product_known_values() {
        // dot([1,2,3], [4,5,6]) = 4 + 10 + 18 = 32 -> -32
        assert!(approx(
            Distance::DotProduct.score(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]),
            -32.0
        ));
    }

    #[test]
    fn mismatched_dim_is_infinity() {
        assert!(Distance::Euclidean
            .score(&[1.0, 2.0], &[1.0, 2.0, 3.0])
            .is_infinite());
    }

    #[test]
    fn cosine_zero_vector_is_orthogonal() {
        assert!(approx(
            Distance::Cosine.score(&[0.0, 0.0], &[1.0, 0.0]),
            1.0
        ));
    }
}
