//! Key generators.
//!
//! Each `KeyGen` variant produces a textual key. The API is built
//! around a sample method that takes an explicit RNG so callers can
//! control determinism (and so unit tests can pin a seed).

use rand::rngs::SmallRng;
use rand::Rng;

use crate::config::KeyGenConfig;
use crate::error::BenchError;

/// Key-generation strategy.
#[derive(Debug, Clone)]
pub enum KeyGen {
    /// Uniformly distributed integer in `[0, max)`.
    UniformInt {
        /// Inclusive upper bound on the integer space.
        max: u64,
        /// Textual prefix prepended to the key.
        prefix: String,
    },
    /// Sequential rolling integer in `[0, max)`. Each call increments
    /// a per-generator counter modulo `max`.
    SequentialInt {
        /// Wrap-around modulus.
        max: u64,
        /// Internal monotonic counter.
        counter: u64,
        /// Textual prefix.
        prefix: String,
    },
    /// Pareto-distributed integer with shape `alpha`, capped at
    /// `max - 1`.
    ParetoInt {
        /// Hard cap on the sampled value.
        max: u64,
        /// Pareto exponent. Must be > 0.
        alpha: f64,
        /// Textual prefix.
        prefix: String,
    },
    /// Normally distributed integer with the given mean and standard
    /// deviation, clamped to `[0, max - 1]`.
    NormalInt {
        /// Hard cap on the sampled value.
        max: u64,
        /// Distribution mean.
        mean: f64,
        /// Distribution standard deviation.
        stddev: f64,
        /// Textual prefix.
        prefix: String,
    },
    /// A constant key.
    Fixed {
        /// The fixed key returned on every call.
        key: String,
    },
}

impl KeyGen {
    /// Build a [`KeyGen`] from a [`KeyGenConfig`].
    pub fn from_config(cfg: &KeyGenConfig) -> Result<Self, BenchError> {
        match cfg.kind.as_str() {
            "uniform" => Ok(Self::UniformInt {
                max: cfg.max.max(1),
                prefix: cfg.prefix.clone(),
            }),
            "sequential" => Ok(Self::SequentialInt {
                max: cfg.max.max(1),
                counter: 0,
                prefix: cfg.prefix.clone(),
            }),
            "pareto" => {
                if cfg.shape <= 0.0 {
                    return Err(BenchError::Config(
                        "keygen.shape must be > 0 for pareto".into(),
                    ));
                }
                Ok(Self::ParetoInt {
                    max: cfg.max.max(1),
                    alpha: cfg.shape,
                    prefix: cfg.prefix.clone(),
                })
            }
            "normal" => {
                if cfg.stddev < 0.0 {
                    return Err(BenchError::Config(
                        "keygen.stddev must be >= 0 for normal".into(),
                    ));
                }
                Ok(Self::NormalInt {
                    max: cfg.max.max(1),
                    mean: cfg.mean,
                    stddev: cfg.stddev,
                    prefix: cfg.prefix.clone(),
                })
            }
            "fixed" => {
                if cfg.key.is_empty() {
                    return Err(BenchError::Config(
                        "keygen.key must not be empty for fixed".into(),
                    ));
                }
                Ok(Self::Fixed {
                    key: cfg.key.clone(),
                })
            }
            other => Err(BenchError::Config(format!("unknown keygen kind `{other}`"))),
        }
    }

    /// Parse a CLI shorthand like `"uniform:1000000"`,
    /// `"pareto:1000000:1.5"`, or `"fixed:hello"`.
    pub fn parse_cli(spec: &str) -> Result<Self, BenchError> {
        let mut iter = spec.splitn(4, ':');
        let kind = iter
            .next()
            .ok_or_else(|| BenchError::Config("empty keygen spec".into()))?;
        match kind {
            "uniform" | "sequential" => {
                let max = iter
                    .next()
                    .ok_or_else(|| BenchError::Config("missing max for keygen".into()))?
                    .parse()
                    .map_err(|e| BenchError::Config(format!("bad max: {e}")))?;
                let prefix = iter.next().unwrap_or("k_").to_string();
                Ok(if kind == "uniform" {
                    Self::UniformInt { max, prefix }
                } else {
                    Self::SequentialInt {
                        max,
                        counter: 0,
                        prefix,
                    }
                })
            }
            "pareto" => {
                let max = iter
                    .next()
                    .ok_or_else(|| BenchError::Config("missing max for pareto".into()))?
                    .parse()
                    .map_err(|e| BenchError::Config(format!("bad max: {e}")))?;
                let alpha = iter
                    .next()
                    .ok_or_else(|| BenchError::Config("missing shape for pareto".into()))?
                    .parse()
                    .map_err(|e| BenchError::Config(format!("bad shape: {e}")))?;
                Ok(Self::ParetoInt {
                    max,
                    alpha,
                    prefix: "k_".into(),
                })
            }
            "fixed" => {
                let key = iter
                    .next()
                    .ok_or_else(|| BenchError::Config("missing key for fixed".into()))?;
                Ok(Self::Fixed {
                    key: key.to_string(),
                })
            }
            other => Err(BenchError::Config(format!(
                "unknown keygen `{other}`; want uniform|sequential|pareto|fixed"
            ))),
        }
    }

    /// Sample one key.
    pub fn next(&mut self, rng: &mut SmallRng) -> String {
        match self {
            Self::UniformInt { max, prefix } => {
                let v = rng.random_range(0..*max);
                format!("{prefix}{v}")
            }
            Self::SequentialInt {
                max,
                counter,
                prefix,
            } => {
                let v = *counter;
                *counter = (*counter + 1) % *max;
                format!("{prefix}{v}")
            }
            Self::ParetoInt { max, alpha, prefix } => {
                // Inverse-CDF sampling: F(x) = 1 - (1/x)^alpha.
                // x = (1 - U)^(-1/alpha), with U in (0, 1).
                let u: f64 = rng.random_range(f64::MIN_POSITIVE..1.0);
                let raw = (1.0 - u).powf(-1.0 / *alpha);
                let cap = *max as f64;
                let bounded = raw.min(cap).max(1.0);
                let v = bounded as u64;
                let v = v.saturating_sub(1).min(max.saturating_sub(1));
                format!("{prefix}{v}")
            }
            Self::NormalInt {
                max,
                mean,
                stddev,
                prefix,
            } => {
                // Box-Muller transform; we only need one of the two
                // normals each call so the second is discarded.
                let u1: f64 = rng.random_range(f64::MIN_POSITIVE..1.0);
                let u2: f64 = rng.random_range(0.0..1.0);
                let z = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
                let raw = (*mean + z * *stddev).round();
                let v = if raw < 0.0 {
                    0
                } else if raw >= *max as f64 {
                    max.saturating_sub(1)
                } else {
                    raw as u64
                };
                format!("{prefix}{v}")
            }
            Self::Fixed { key } => key.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;

    use super::*;

    fn rng() -> SmallRng {
        SmallRng::seed_from_u64(42)
    }

    #[test]
    fn uniform_distributes_values() {
        let mut g = KeyGen::UniformInt {
            max: 1024,
            prefix: "k_".into(),
        };
        let mut r = rng();
        let mut counts = [0u32; 16];
        for _ in 0..16_000 {
            let s = g.next(&mut r);
            let n: u64 = s.trim_start_matches("k_").parse().unwrap();
            assert!(n < 1024);
            counts[(n as usize) % 16] += 1;
        }
        // Every coarse bucket should see roughly 1000 hits;
        // tolerate a wide band so the test is not flaky.
        for c in counts {
            assert!(c >= 700, "bucket too cold: {c}");
            assert!(c <= 1300, "bucket too hot: {c}");
        }
    }

    #[test]
    fn sequential_wraps() {
        let mut g = KeyGen::SequentialInt {
            max: 5,
            counter: 0,
            prefix: "k_".into(),
        };
        let mut r = rng();
        let observed: Vec<String> = (0..12).map(|_| g.next(&mut r)).collect();
        assert_eq!(observed[0], "k_0");
        assert_eq!(observed[4], "k_4");
        assert_eq!(observed[5], "k_0");
        assert_eq!(observed[10], "k_0");
    }

    #[test]
    fn pareto_clamps_to_max() {
        let mut g = KeyGen::ParetoInt {
            max: 100,
            alpha: 1.2,
            prefix: "k_".into(),
        };
        let mut r = rng();
        for _ in 0..2_000 {
            let s = g.next(&mut r);
            let n: u64 = s.trim_start_matches("k_").parse().unwrap();
            assert!(n < 100);
        }
    }

    #[test]
    fn normal_clamps() {
        let mut g = KeyGen::NormalInt {
            max: 1000,
            mean: 500.0,
            stddev: 50.0,
            prefix: "k_".into(),
        };
        let mut r = rng();
        for _ in 0..2_000 {
            let s = g.next(&mut r);
            let n: u64 = s.trim_start_matches("k_").parse().unwrap();
            assert!(n < 1000);
        }
    }

    #[test]
    fn fixed_returns_constant() {
        let mut g = KeyGen::Fixed {
            key: "the-key".into(),
        };
        let mut r = rng();
        for _ in 0..3 {
            assert_eq!(g.next(&mut r), "the-key");
        }
    }

    #[test]
    fn parse_cli_uniform() {
        let g = KeyGen::parse_cli("uniform:100").unwrap();
        match g {
            KeyGen::UniformInt { max, .. } => assert_eq!(max, 100),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_cli_pareto() {
        let g = KeyGen::parse_cli("pareto:1000:1.5").unwrap();
        match g {
            KeyGen::ParetoInt { max, alpha, .. } => {
                assert_eq!(max, 1000);
                assert!((alpha - 1.5).abs() < 1e-9);
            }
            _ => panic!("wrong variant"),
        }
    }
}
