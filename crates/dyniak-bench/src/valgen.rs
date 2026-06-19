//! Value generators.

use rand::rngs::SmallRng;
use rand::Rng;

use crate::config::ValGenConfig;
use crate::error::BenchError;

/// Value-generation strategy. Generators always emit pseudo-random
/// ASCII bytes (printable lowercase letters and digits) so the output
/// is safe to log and diff.
#[derive(Debug, Clone)]
pub enum ValGen {
    /// Constant-size payload of `n` bytes.
    Fixed {
        /// Payload size.
        n: usize,
    },
    /// Uniformly distributed size in `[min, max]`.
    Uniform {
        /// Lower bound (inclusive) on payload size.
        min: usize,
        /// Upper bound (inclusive) on payload size.
        max: usize,
    },
    /// Exponentially distributed size with the given mean.
    Exponential {
        /// Distribution mean (in bytes).
        mean: f64,
    },
}

impl ValGen {
    /// Build a [`ValGen`] from a [`ValGenConfig`].
    pub fn from_config(cfg: &ValGenConfig) -> Result<Self, BenchError> {
        match cfg.kind.as_str() {
            "fixed" => Ok(Self::Fixed { n: cfg.size.max(1) }),
            "uniform" => {
                if cfg.min > cfg.max {
                    return Err(BenchError::Config(
                        "valgen.min must be <= valgen.max".into(),
                    ));
                }
                Ok(Self::Uniform {
                    min: cfg.min.max(1),
                    max: cfg.max.max(1),
                })
            }
            "exponential" => Ok(Self::Exponential {
                mean: cfg.mean.max(1) as f64,
            }),
            other => Err(BenchError::Config(format!("unknown valgen kind `{other}`"))),
        }
    }

    /// Parse a CLI shorthand like `"fixed:256"` or `"uniform:16:1024"`.
    pub fn parse_cli(spec: &str) -> Result<Self, BenchError> {
        let mut it = spec.splitn(3, ':');
        let kind = it
            .next()
            .ok_or_else(|| BenchError::Config("empty valgen spec".into()))?;
        match kind {
            "fixed" => {
                let n: usize = it
                    .next()
                    .ok_or_else(|| BenchError::Config("missing size".into()))?
                    .parse()
                    .map_err(|e| BenchError::Config(format!("bad size: {e}")))?;
                Ok(Self::Fixed { n })
            }
            "uniform" => {
                let min: usize = it
                    .next()
                    .ok_or_else(|| BenchError::Config("missing min".into()))?
                    .parse()
                    .map_err(|e| BenchError::Config(format!("bad min: {e}")))?;
                let max: usize = it
                    .next()
                    .ok_or_else(|| BenchError::Config("missing max".into()))?
                    .parse()
                    .map_err(|e| BenchError::Config(format!("bad max: {e}")))?;
                if min > max {
                    return Err(BenchError::Config("min > max".into()));
                }
                Ok(Self::Uniform { min, max })
            }
            "exponential" => {
                let mean: f64 = it
                    .next()
                    .ok_or_else(|| BenchError::Config("missing mean".into()))?
                    .parse()
                    .map_err(|e| BenchError::Config(format!("bad mean: {e}")))?;
                if mean <= 0.0 {
                    return Err(BenchError::Config("mean must be > 0".into()));
                }
                Ok(Self::Exponential { mean })
            }
            other => Err(BenchError::Config(format!(
                "unknown valgen `{other}`; want fixed|uniform|exponential"
            ))),
        }
    }

    /// Emit a fresh value.
    pub fn next(&self, rng: &mut SmallRng) -> Vec<u8> {
        let n = match *self {
            Self::Fixed { n } => n,
            Self::Uniform { min, max } => rng.random_range(min..=max),
            Self::Exponential { mean } => {
                let u: f64 = rng.random_range(f64::MIN_POSITIVE..1.0);
                let raw = -mean * u.ln();
                let bounded = raw.clamp(1.0, 1_048_576.0);
                bounded as usize
            }
        };
        random_bytes(rng, n)
    }
}

fn random_bytes(rng: &mut SmallRng, n: usize) -> Vec<u8> {
    const ALPHA: &[u8; 36] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let i = rng.random_range(0..ALPHA.len());
        out.push(ALPHA[i]);
    }
    out
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;

    use super::*;

    fn rng() -> SmallRng {
        SmallRng::seed_from_u64(7)
    }

    #[test]
    fn fixed_size_holds() {
        let g = ValGen::Fixed { n: 64 };
        let mut r = rng();
        for _ in 0..32 {
            assert_eq!(g.next(&mut r).len(), 64);
        }
    }

    #[test]
    fn uniform_size_within_bounds() {
        let g = ValGen::Uniform { min: 16, max: 32 };
        let mut r = rng();
        for _ in 0..256 {
            let v = g.next(&mut r);
            assert!(v.len() >= 16 && v.len() <= 32);
        }
    }

    #[test]
    fn exponential_size_finite() {
        let g = ValGen::Exponential { mean: 64.0 };
        let mut r = rng();
        let mut total = 0usize;
        for _ in 0..1024 {
            let v = g.next(&mut r);
            assert!(!v.is_empty());
            total += v.len();
        }
        let mean = total / 1024;
        // Loose sanity bound: mean over 1024 samples should land in
        // a generous band around the configured mean.
        assert!((16..=256).contains(&mean), "mean = {mean}");
    }

    #[test]
    fn parse_cli_fixed() {
        let g = ValGen::parse_cli("fixed:128").unwrap();
        assert!(matches!(g, ValGen::Fixed { n: 128 }));
    }

    #[test]
    fn parse_cli_uniform() {
        let g = ValGen::parse_cli("uniform:8:64").unwrap();
        match g {
            ValGen::Uniform { min, max } => {
                assert_eq!(min, 8);
                assert_eq!(max, 64);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_cli_rejects_min_gt_max() {
        assert!(ValGen::parse_cli("uniform:64:8").is_err());
    }

    // ---- from_config: each kind plus error branches ----

    fn val_cfg(kind: &str) -> ValGenConfig {
        ValGenConfig {
            kind: kind.to_string(),
            size: 256,
            min: 16,
            max: 1024,
            mean: 64,
        }
    }

    #[test]
    fn from_config_fixed_clamps_zero_to_one() {
        let mut cfg = val_cfg("fixed");
        cfg.size = 0;
        assert!(matches!(
            ValGen::from_config(&cfg).unwrap(),
            ValGen::Fixed { n: 1 }
        ));
    }

    #[test]
    fn from_config_uniform_ok_and_clamps() {
        let mut cfg = val_cfg("uniform");
        cfg.min = 0;
        cfg.max = 0;
        // Both bounds clamp up to 1.
        assert!(matches!(
            ValGen::from_config(&cfg).unwrap(),
            ValGen::Uniform { min: 1, max: 1 }
        ));
    }

    #[test]
    fn from_config_uniform_rejects_min_gt_max() {
        let mut cfg = val_cfg("uniform");
        cfg.min = 100;
        cfg.max = 10;
        assert!(ValGen::from_config(&cfg).is_err());
    }

    #[test]
    fn from_config_exponential_clamps_zero_mean() {
        let mut cfg = val_cfg("exponential");
        cfg.mean = 0;
        // mean.max(1) keeps the rate finite.
        match ValGen::from_config(&cfg).unwrap() {
            ValGen::Exponential { mean } => assert!((mean - 1.0).abs() < 1e-9),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn from_config_rejects_unknown_kind() {
        assert!(ValGen::from_config(&val_cfg("bogus")).is_err());
    }

    // ---- parse_cli: exponential and error branches ----

    #[test]
    fn parse_cli_exponential() {
        match ValGen::parse_cli("exponential:128").unwrap() {
            ValGen::Exponential { mean } => assert!((mean - 128.0).abs() < 1e-9),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_cli_error_branches() {
        // Missing / bad fixed size.
        assert!(ValGen::parse_cli("fixed").is_err());
        assert!(ValGen::parse_cli("fixed:notanumber").is_err());
        // Missing min / max for uniform.
        assert!(ValGen::parse_cli("uniform").is_err());
        assert!(ValGen::parse_cli("uniform:8").is_err());
        assert!(ValGen::parse_cli("uniform:8:bad").is_err());
        // Missing / non-positive mean for exponential.
        assert!(ValGen::parse_cli("exponential").is_err());
        assert!(ValGen::parse_cli("exponential:0").is_err());
        assert!(ValGen::parse_cli("exponential:bad").is_err());
        // Unknown kind.
        assert!(ValGen::parse_cli("weird:1").is_err());
    }

    #[test]
    fn exponential_size_capped_at_one_mib() {
        // A huge mean still clamps to the 1 MiB ceiling, so no
        // single value blows the heap.
        let g = ValGen::Exponential { mean: 1e18 };
        let mut r = rng();
        for _ in 0..4 {
            assert!(g.next(&mut r).len() <= 1_048_576);
        }
    }

    #[hegel::test(test_cases = 64)]
    fn uniform_value_size_respects_bounds(tc: hegel::TestCase) {
        // For any in-order [min, max] pair, every emitted value's
        // length lands inside the inclusive band.
        let min = tc.draw(
            hegel::generators::integers::<usize>()
                .min_value(1)
                .max_value(512),
        );
        let span = tc.draw(
            hegel::generators::integers::<usize>()
                .min_value(0)
                .max_value(512),
        );
        let max = min + span;
        let g = ValGen::Uniform { min, max };
        let mut r = rng();
        for _ in 0..32 {
            let len = g.next(&mut r).len();
            assert!(len >= min && len <= max, "len {len} outside [{min}, {max}]");
        }
    }
}
