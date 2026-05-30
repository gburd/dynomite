//! Distance metric correctness tests.

use dynvecdb::distance::Distance;

fn approx(a: f32, b: f32, eps: f32) -> bool {
    (a - b).abs() <= eps
}

#[test]
fn euclidean_canonical() {
    assert!(approx(
        Distance::Euclidean.score(&[0.0, 0.0, 0.0], &[3.0, 4.0, 0.0]),
        5.0,
        1e-5,
    ));
    assert!(approx(
        Distance::Euclidean.score(&[1.0, 1.0], &[4.0, 5.0]),
        5.0,
        1e-5,
    ));
}

#[test]
fn euclidean_self_is_zero() {
    let v = vec![1.0_f32, 2.0, 3.0, 4.0];
    assert!(approx(Distance::Euclidean.score(&v, &v), 0.0, 1e-6));
}

#[test]
fn cosine_orthogonal_is_one() {
    assert!(approx(
        Distance::Cosine.score(&[1.0, 0.0, 0.0], &[0.0, 1.0, 0.0]),
        1.0,
        1e-6,
    ));
}

#[test]
fn cosine_parallel_is_zero() {
    assert!(approx(
        Distance::Cosine.score(&[2.0, 0.0], &[5.0, 0.0]),
        0.0,
        1e-6,
    ));
}

#[test]
fn cosine_antiparallel_is_two() {
    assert!(approx(
        Distance::Cosine.score(&[1.0, 1.0], &[-1.0, -1.0]),
        2.0,
        1e-5,
    ));
}

#[test]
fn cosine_zero_vector_is_orthogonal() {
    assert!(approx(
        Distance::Cosine.score(&[0.0, 0.0, 0.0], &[1.0, 2.0, 3.0]),
        1.0,
        1e-6,
    ));
}

#[test]
fn dot_product_known_values() {
    // Dot product score is negated; smaller is closer.
    let s = Distance::DotProduct.score(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]);
    assert!(approx(s, -32.0, 1e-5));
}

#[test]
fn dot_product_orthogonal_is_zero() {
    let s = Distance::DotProduct.score(&[1.0, 0.0], &[0.0, 1.0]);
    assert!(approx(s, 0.0, 1e-6));
}

#[test]
fn mismatched_dim_returns_infinity() {
    assert!(Distance::Euclidean
        .score(&[1.0, 2.0], &[1.0, 2.0, 3.0])
        .is_infinite());
    assert!(Distance::Cosine.score(&[1.0], &[1.0, 2.0]).is_infinite());
    assert!(Distance::DotProduct
        .score(&[1.0, 2.0], &[1.0])
        .is_infinite());
}

#[test]
fn metric_names_are_unique() {
    let n1 = Distance::Euclidean.name();
    let n2 = Distance::Cosine.name();
    let n3 = Distance::DotProduct.name();
    assert_ne!(n1, n2);
    assert_ne!(n2, n3);
    assert_ne!(n1, n3);
}
