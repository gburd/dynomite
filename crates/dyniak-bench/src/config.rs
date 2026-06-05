//! Configuration: TOML schema, parsing, and CLI override merging.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::BenchError;

/// Top-level configuration. Mirrors the TOML schema documented in the
/// crate `README.md`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Run-level knobs (duration, concurrency, rate, output).
    pub run: RunConfig,
    /// Driver-specific knobs (kind, host, port, ...).
    pub driver: DriverConfig,
    /// Operation weights. Each key is an op name; the value is a
    /// non-negative integer weight.
    pub ops: OpsConfig,
    /// Key generator configuration.
    pub keygen: KeyGenConfig,
    /// Value generator configuration.
    pub valgen: ValGenConfig,
}

/// Run-level configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RunConfig {
    /// Total run duration in textual form (e.g. `"10m"`, `"30s"`).
    pub duration: String,
    /// Number of concurrent worker tasks.
    pub concurrent: usize,
    /// Rate limit. Either `"max"` or a `RateConfig::Rps` table.
    #[serde(default = "default_rate")]
    pub rate: RateConfig,
    /// Output directory. `"auto"` mints a stamped path under
    /// `tests/`; any other value is used as-is (created if missing).
    #[serde(default = "default_out_dir")]
    pub out_dir: String,
    /// How often the engine flushes per-worker histograms into the
    /// global view and writes one summary row.
    #[serde(default = "default_report_interval")]
    pub report_interval: String,
}

fn default_rate() -> RateConfig {
    RateConfig::Max
}

fn default_out_dir() -> String {
    "tests/auto".to_string()
}

fn default_report_interval() -> String {
    "1s".to_string()
}

/// Rate-limit selector. Either saturate the driver (`Max`) or pace
/// the workers at a target ops/sec (`Rps { rps }`).
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum RateConfig {
    /// Numeric variant: `rate = 5000` sets aggregate rps directly.
    Rps(RpsTable),
    /// String variant: `rate = "max"` saturates.
    Max,
}

impl<'de> Deserialize<'de> for RateConfig {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Accept either:
        //   rate = "max"
        //   rate = { rps = N }
        //   rate = N             (shorthand: integer is treated as rps)
        use serde::de::{Error, MapAccess, Visitor};

        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = RateConfig;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("`\"max\"`, an integer rps, or a table { rps = N }")
            }

            fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
                if v.eq_ignore_ascii_case("max") {
                    Ok(RateConfig::Max)
                } else {
                    let n: u64 = v
                        .parse()
                        .map_err(|e| Error::custom(format!("bad rate `{v}`: {e}")))?;
                    Ok(RateConfig::Rps(RpsTable { rps: n }))
                }
            }

            fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(RateConfig::Rps(RpsTable { rps: v }))
            }

            fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
                if v < 0 {
                    Err(Error::custom("rate must be >= 0"))
                } else {
                    Ok(RateConfig::Rps(RpsTable { rps: v as u64 }))
                }
            }

            fn visit_map<M>(self, mut m: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut rps: Option<u64> = None;
                while let Some(key) = m.next_key::<String>()? {
                    match key.as_str() {
                        "rps" => rps = Some(m.next_value()?),
                        other => {
                            return Err(M::Error::custom(format!("unknown rate key `{other}`")));
                        }
                    }
                }
                let rps = rps.ok_or_else(|| M::Error::custom("missing `rps`"))?;
                Ok(RateConfig::Rps(RpsTable { rps }))
            }
        }

        d.deserialize_any(V)
    }
}

/// Table form of a rate limit: `rate = { rps = N }`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpsTable {
    /// Aggregate target ops per second across all workers.
    pub rps: u64,
}

/// Driver selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DriverKind {
    /// RESP-2 (Redis / dynomited).
    Redis,
    /// Riak Protocol Buffer Client.
    RiakPbc,
    /// Riak HTTP API (feature-gated; build with `--features http`).
    RiakHttp,
}

impl DriverKind {
    /// Return the kebab-case label used in directory names.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Redis => "redis",
            Self::RiakPbc => "riak-pbc",
            Self::RiakHttp => "riak-http",
        }
    }
}

/// Body and response encoding for the `riak_http` driver.
///
/// Selects the HTTP `Content-Type` of `PUT` bodies and the `Accept`
/// header of `GET` / `PUT` requests, so a run can measure each
/// codec the gateway negotiates.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpEncoding {
    /// `application/json` envelope (default).
    #[default]
    Json,
    /// `application/cbor` envelope.
    Cbor,
    /// `application/x-protobuf` envelope.
    Protobuf,
}

impl HttpEncoding {
    /// The HTTP media type string for this encoding.
    #[must_use]
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Cbor => "application/cbor",
            Self::Protobuf => "application/x-protobuf",
        }
    }
}

/// Driver-level configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DriverConfig {
    /// Driver kind.
    pub kind: DriverKind,
    /// Hostname / IP of the server.
    #[serde(default = "default_host")]
    pub host: String,
    /// TCP port (or HTTP port for `riak_http`).
    #[serde(default = "default_port")]
    pub port: u16,
    /// Timeout in milliseconds applied to connect / read / write.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Bucket name for Riak drivers.
    #[serde(default = "default_bucket")]
    pub bucket: String,
    /// Object encoding for the `riak_http` driver. Ignored by the
    /// other drivers.
    #[serde(default)]
    pub encoding: HttpEncoding,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    6379
}

fn default_timeout_ms() -> u64 {
    5000
}

fn default_bucket() -> String {
    "bench".to_string()
}

/// Op-weight table. Each entry is `name -> weight`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(transparent)]
pub struct OpsConfig {
    /// Map from op-name to weight.
    pub map: std::collections::BTreeMap<String, u32>,
}

impl OpsConfig {
    /// Return the (op, weight) pairs sorted by op name.
    #[must_use]
    pub fn weighted(&self) -> Vec<(String, u32)> {
        self.map
            .iter()
            .filter(|(_, &w)| w > 0)
            .map(|(k, &v)| (k.clone(), v))
            .collect()
    }
}

/// Key generator configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KeyGenConfig {
    /// Variant tag: `uniform` | `sequential` | `pareto` | `normal`
    /// | `fixed`.
    pub kind: String,
    /// Upper bound (exclusive) for integer-valued generators.
    #[serde(default = "default_max")]
    pub max: u64,
    /// Pareto exponent (>1.0). Used by the `pareto` variant.
    #[serde(default = "default_shape")]
    pub shape: f64,
    /// Mean (used by the `normal` variant).
    #[serde(default)]
    pub mean: f64,
    /// Standard deviation (used by the `normal` variant).
    #[serde(default)]
    pub stddev: f64,
    /// Fixed key (used by the `fixed` variant).
    #[serde(default)]
    pub key: String,
    /// Prefix prepended to the textual key. Default `"k_"`.
    #[serde(default = "default_key_prefix")]
    pub prefix: String,
}

fn default_max() -> u64 {
    1_000_000
}

fn default_shape() -> f64 {
    1.5
}

fn default_key_prefix() -> String {
    "k_".to_string()
}

/// Value generator configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ValGenConfig {
    /// Variant tag: `fixed` | `uniform` | `exponential`.
    pub kind: String,
    /// Size in bytes (fixed variant).
    #[serde(default = "default_val_size")]
    pub size: usize,
    /// Lower bound in bytes (uniform variant).
    #[serde(default = "default_val_min")]
    pub min: usize,
    /// Upper bound in bytes (uniform variant).
    #[serde(default = "default_val_max")]
    pub max: usize,
    /// Mean in bytes (exponential variant).
    #[serde(default = "default_val_size")]
    pub mean: usize,
}

fn default_val_size() -> usize {
    256
}

fn default_val_min() -> usize {
    16
}

fn default_val_max() -> usize {
    1024
}

impl Config {
    /// Load a config from a TOML file path.
    pub fn from_path(path: &std::path::Path) -> Result<Self, BenchError> {
        let raw = std::fs::read_to_string(path)?;
        let cfg: Self = toml::from_str(&raw).map_err(|e| BenchError::Config(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate cross-field invariants (positive concurrency, parsable
    /// duration string, ...).
    pub fn validate(&self) -> Result<(), BenchError> {
        if self.run.concurrent == 0 {
            return Err(BenchError::Config("run.concurrent must be > 0".to_string()));
        }
        let _ = parse_duration(&self.run.duration)
            .map_err(|e| BenchError::Config(format!("run.duration: {e}")))?;
        let _ = parse_duration(&self.run.report_interval)
            .map_err(|e| BenchError::Config(format!("run.report_interval: {e}")))?;
        if self.ops.weighted().is_empty() {
            return Err(BenchError::Config(
                "ops table must contain at least one positive-weight entry".to_string(),
            ));
        }
        Ok(())
    }

    /// Return the parsed run duration.
    pub fn duration(&self) -> Result<Duration, BenchError> {
        parse_duration(&self.run.duration).map_err(|e| BenchError::Config(e.to_string()))
    }

    /// Return the parsed reporting interval.
    pub fn report_interval(&self) -> Result<Duration, BenchError> {
        parse_duration(&self.run.report_interval).map_err(|e| BenchError::Config(e.to_string()))
    }

    /// Resolve the output directory: when set to `"auto"`, mint a
    /// stamped path; otherwise return the configured path.
    pub fn resolve_out_dir(&self) -> PathBuf {
        if self.run.out_dir == "auto" {
            let stamp = utc_stamp();
            let dur = self.run.duration.replace(' ', "");
            let kind = self.driver.kind.label();
            PathBuf::from(format!(
                "tests/{stamp}-{kind}-{dur}-{w}w",
                w = self.run.concurrent
            ))
        } else {
            PathBuf::from(&self.run.out_dir)
        }
    }
}

/// Parse a duration like `"10m"`, `"5s"`, `"500ms"`, `"2h"` into a
/// [`Duration`]. Bare numbers are interpreted as seconds.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err("empty duration".to_string());
    }
    let (num_part, unit) = split_unit(trimmed);
    let value: f64 = num_part
        .parse()
        .map_err(|e| format!("invalid number `{num_part}`: {e}"))?;
    if value < 0.0 {
        return Err(format!("duration must be >= 0, got `{trimmed}`"));
    }
    let nanos = match unit {
        "" | "s" => value * 1_000_000_000.0,
        "ms" => value * 1_000_000.0,
        "us" => value * 1_000.0,
        "ns" => value,
        "m" => value * 60.0 * 1_000_000_000.0,
        "h" => value * 3_600.0 * 1_000_000_000.0,
        other => return Err(format!("unknown duration unit `{other}`")),
    };
    if !nanos.is_finite() || nanos < 0.0 {
        return Err(format!("duration overflow: `{trimmed}`"));
    }
    // u128 nanos -> Duration via integer floor.
    let nanos = nanos as u128;
    let secs = u64::try_from(nanos / 1_000_000_000)
        .map_err(|_| format!("duration too large: `{trimmed}`"))?;
    let nanos_rem = u32::try_from(nanos % 1_000_000_000).unwrap_or(0);
    Ok(Duration::new(secs, nanos_rem))
}

fn split_unit(s: &str) -> (&str, &str) {
    let split_at = s.find(|c: char| c.is_alphabetic()).unwrap_or(s.len());
    (&s[..split_at], &s[split_at..])
}

/// Produce a UTC timestamp like `20260601T140530Z` without dragging
/// in `chrono`. Uses the system clock and carves the value into
/// year/month/day/hour/min/sec by hand.
fn utc_stamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (y, mo, d, h, mi, s) = epoch_to_civil(secs);
    format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z")
}

/// Convert seconds-since-Unix-epoch into civil UTC components.
/// Algorithm from Howard Hinnant's date library (public domain).
fn epoch_to_civil(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let days = i64::try_from(secs / 86400).unwrap_or(0);
    let secs_of_day = (secs % 86400) as u32;
    let h = secs_of_day / 3600;
    let mi = (secs_of_day / 60) % 60;
    let s = secs_of_day % 60;

    // Shift to a Mar-1 0000 epoch.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (
        u32::try_from(y).unwrap_or(0),
        u32::try_from(m).unwrap_or(0),
        u32::try_from(d).unwrap_or(0),
        h,
        mi,
        s,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_seconds() {
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("0.5s").unwrap(), Duration::from_millis(500));
    }

    #[test]
    fn parses_minutes_hours() {
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn parses_subsecond() {
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("100us").unwrap(), Duration::from_micros(100));
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("notanumber").is_err());
        assert!(parse_duration("10x").is_err());
        assert!(parse_duration("-5s").is_err());
    }

    #[test]
    fn config_basic_round_trip() {
        let toml_text = r#"
[run]
duration = "30s"
concurrent = 4
rate = "max"
out_dir = "auto"

[driver]
kind = "redis"
host = "127.0.0.1"
port = 6379

[ops]
get = 4
set = 1

[keygen]
kind = "uniform"
max = 1000

[valgen]
kind = "fixed"
size = 64
"#;
        let cfg: Config = toml::from_str(toml_text).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.driver.kind, DriverKind::Redis);
        assert_eq!(cfg.run.concurrent, 4);
        assert_eq!(cfg.duration().unwrap(), Duration::from_secs(30));
        let ops = cfg.ops.weighted();
        assert_eq!(ops.len(), 2);
    }

    #[test]
    fn rate_rps_table_parses() {
        let toml_text = r#"
[run]
duration = "10s"
concurrent = 2
rate = { rps = 1000 }

[driver]
kind = "redis"

[ops]
get = 1

[keygen]
kind = "uniform"
max = 100

[valgen]
kind = "fixed"
size = 16
"#;
        let cfg: Config = toml::from_str(toml_text).unwrap();
        match cfg.run.rate {
            RateConfig::Rps(ref r) => assert_eq!(r.rps, 1000),
            RateConfig::Max => panic!("expected Rps"),
        }
    }

    #[test]
    fn auto_out_dir_renders_kind_and_concurrency() {
        let cfg: Config = toml::from_str(
            r#"
[run]
duration = "10s"
concurrent = 8
rate = "max"
out_dir = "auto"

[driver]
kind = "redis"

[ops]
get = 1

[keygen]
kind = "uniform"
max = 100

[valgen]
kind = "fixed"
size = 16
"#,
        )
        .unwrap();
        let p = cfg.resolve_out_dir();
        let s = p.to_string_lossy();
        assert!(s.starts_with("tests/"), "got `{s}`");
        assert!(s.contains("redis"));
        assert!(s.ends_with("8w"));
    }

    #[test]
    fn epoch_civil_known_value() {
        // 2020-01-01 00:00:00 UTC = 1577836800
        let (y, m, d, h, mi, s) = epoch_to_civil(1_577_836_800);
        assert_eq!((y, m, d, h, mi, s), (2020, 1, 1, 0, 0, 0));
    }

    #[test]
    fn ops_weighted_skips_zero() {
        let mut o = OpsConfig::default();
        o.map.insert("get".into(), 0);
        o.map.insert("set".into(), 1);
        let w = o.weighted();
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].0, "set");
    }
}
