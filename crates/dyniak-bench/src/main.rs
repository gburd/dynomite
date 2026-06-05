//! `dyniak-bench` -- load-generation tool with first-class drivers
//! for the dyniak Riak surface and the Redis (RESP-2) /
//! RediSearch path.
//!
//! See `README.md` and the example TOMLs under `examples/` for
//! the CLI surface.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use tracing::{error, info};

use dyniak_bench::config::{
    Config, DriverConfig, DriverKind as CfgDriverKind, HttpEncoding, KeyGenConfig, OpsConfig,
    RateConfig, RpsTable, RunConfig, ValGenConfig,
};
use dyniak_bench::engine::Engine;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliDriverKind {
    Redis,
    RiakPbc,
    RiakQuic,
    RiakHttp,
}

impl From<CliDriverKind> for CfgDriverKind {
    fn from(c: CliDriverKind) -> Self {
        match c {
            CliDriverKind::Redis => Self::Redis,
            CliDriverKind::RiakPbc => Self::RiakPbc,
            CliDriverKind::RiakQuic => Self::RiakQuic,
            CliDriverKind::RiakHttp => Self::RiakHttp,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliEncoding {
    Json,
    Cbor,
    Protobuf,
}

impl From<CliEncoding> for HttpEncoding {
    fn from(c: CliEncoding) -> Self {
        match c {
            CliEncoding::Json => Self::Json,
            CliEncoding::Cbor => Self::Cbor,
            CliEncoding::Protobuf => Self::Protobuf,
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "dyniak-bench",
    version,
    about = "Load-generation and benchmarking for Redis (RESP-2) and Riak PBC backends",
    long_about = None,
)]
struct Cli {
    /// Path to a TOML config file. CLI flags below override
    /// individual fields.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Output directory. Use `auto` to mint a stamped path under
    /// `tests/`.
    #[arg(long)]
    out: Option<PathBuf>,

    /// Driver kind. Required when `--config` is not supplied.
    #[arg(long, value_enum)]
    driver: Option<CliDriverKind>,

    /// Server hostname / IP.
    #[arg(long)]
    host: Option<String>,

    /// Server port.
    #[arg(long)]
    port: Option<u16>,

    /// Bucket name (Riak drivers).
    #[arg(long)]
    bucket: Option<String>,

    /// Object encoding for the `riak_http` driver: `json`, `cbor`,
    /// or `protobuf`. Sets the request `Content-Type` and `Accept`.
    #[arg(long, value_enum)]
    encoding: Option<CliEncoding>,

    /// Op weights as `op:weight,op:weight,...`. Example
    /// `get:4,set:1`.
    #[arg(long)]
    ops: Option<String>,

    /// Key generator spec. Example: `uniform:1000000`,
    /// `pareto:1000000:1.5`, `fixed:hello`.
    #[arg(long)]
    keygen: Option<String>,

    /// Value generator spec. Example: `fixed:256`,
    /// `uniform:32:1024`, `exponential:512`.
    #[arg(long)]
    valgen: Option<String>,

    /// Aggregate rate target. `max` or a positive integer (rps).
    #[arg(long)]
    rate: Option<String>,

    /// Run duration. Example: `10s`, `5m`.
    #[arg(long)]
    duration: Option<String>,

    /// Number of concurrent worker threads.
    #[arg(long, default_value_t = 0)]
    concurrent: usize,

    /// Reporting interval (default `1s`).
    #[arg(long)]
    report_interval: Option<String>,

    /// Per-op timeout in milliseconds.
    #[arg(long)]
    timeout_ms: Option<u64>,

    /// Verbose tracing.
    #[arg(short, long)]
    verbose: bool,
}

fn parse_ops_arg(s: &str) -> Result<OpsConfig, String> {
    let mut map = BTreeMap::new();
    for chunk in s.split(',') {
        let mut it = chunk.splitn(2, ':');
        let name = it.next().ok_or_else(|| "empty op spec".to_string())?.trim();
        let weight: u32 = it
            .next()
            .ok_or_else(|| format!("op `{name}` missing weight"))?
            .trim()
            .parse()
            .map_err(|e| format!("op `{name}` bad weight: {e}"))?;
        if name.is_empty() {
            return Err("empty op name".into());
        }
        map.insert(name.to_string(), weight);
    }
    if map.is_empty() {
        return Err("ops spec must contain at least one entry".into());
    }
    Ok(OpsConfig { map })
}

fn parse_rate_arg(s: &str) -> Result<RateConfig, String> {
    if s.eq_ignore_ascii_case("max") {
        Ok(RateConfig::Max)
    } else {
        let v: u64 = s.parse().map_err(|e| format!("bad rate `{s}`: {e}"))?;
        Ok(RateConfig::Rps(RpsTable { rps: v }))
    }
}

fn build_config(cli: &Cli) -> Result<Config, String> {
    let mut cfg = if let Some(path) = &cli.config {
        Config::from_path(path).map_err(|e| format!("load config: {e}"))?
    } else {
        let driver = cli
            .driver
            .ok_or_else(|| "must provide --driver or --config".to_string())?;
        let ops = cli
            .ops
            .as_deref()
            .ok_or_else(|| "must provide --ops or --config".to_string())?;
        let keygen = cli
            .keygen
            .as_deref()
            .ok_or_else(|| "must provide --keygen or --config".to_string())?;
        let valgen = cli
            .valgen
            .as_deref()
            .ok_or_else(|| "must provide --valgen or --config".to_string())?;
        let duration = cli
            .duration
            .clone()
            .ok_or_else(|| "must provide --duration or --config".to_string())?;
        let concurrent = if cli.concurrent == 0 {
            8
        } else {
            cli.concurrent
        };
        Config {
            run: RunConfig {
                duration,
                concurrent,
                rate: cli
                    .rate
                    .as_deref()
                    .map(parse_rate_arg)
                    .transpose()?
                    .unwrap_or(RateConfig::Max),
                out_dir: "auto".to_string(),
                report_interval: cli
                    .report_interval
                    .clone()
                    .unwrap_or_else(|| "1s".to_string()),
            },
            driver: DriverConfig {
                kind: driver.into(),
                host: cli.host.clone().unwrap_or_else(|| "127.0.0.1".into()),
                port: cli.port.unwrap_or(match driver {
                    CliDriverKind::Redis => 6379,
                    CliDriverKind::RiakPbc => 8087,
                    CliDriverKind::RiakQuic => 8089,
                    CliDriverKind::RiakHttp => 8098,
                }),
                timeout_ms: cli.timeout_ms.unwrap_or(5000),
                bucket: cli.bucket.clone().unwrap_or_else(|| "bench".into()),
                encoding: cli.encoding.map(Into::into).unwrap_or_default(),
            },
            ops: parse_ops_arg(ops)?,
            keygen: keygen_from_cli(keygen)?,
            valgen: valgen_from_cli(valgen)?,
        }
    };

    // Apply CLI overrides on top of a loaded TOML.
    if let Some(d) = &cli.duration {
        cfg.run.duration.clone_from(d);
    }
    if cli.concurrent > 0 {
        cfg.run.concurrent = cli.concurrent;
    }
    if let Some(r) = &cli.rate {
        cfg.run.rate = parse_rate_arg(r)?;
    }
    if let Some(o) = &cli.out {
        cfg.run.out_dir = o.to_string_lossy().to_string();
    }
    if let Some(h) = &cli.host {
        cfg.driver.host.clone_from(h);
    }
    if let Some(p) = cli.port {
        cfg.driver.port = p;
    }
    if let Some(t) = cli.timeout_ms {
        cfg.driver.timeout_ms = t;
    }
    if let Some(b) = &cli.bucket {
        cfg.driver.bucket.clone_from(b);
    }
    if let Some(e) = cli.encoding {
        cfg.driver.encoding = e.into();
    }
    if let Some(d) = cli.driver {
        cfg.driver.kind = d.into();
    }
    if let Some(o) = &cli.ops {
        cfg.ops = parse_ops_arg(o)?;
    }
    if let Some(k) = &cli.keygen {
        cfg.keygen = keygen_from_cli(k)?;
    }
    if let Some(v) = &cli.valgen {
        cfg.valgen = valgen_from_cli(v)?;
    }
    if let Some(ri) = &cli.report_interval {
        cfg.run.report_interval.clone_from(ri);
    }
    cfg.validate().map_err(|e| format!("validate: {e}"))?;
    Ok(cfg)
}

fn keygen_from_cli(spec: &str) -> Result<KeyGenConfig, String> {
    let mut it = spec.splitn(4, ':');
    let kind = it.next().ok_or_else(|| "empty keygen".to_string())?;
    let mut cfg = KeyGenConfig {
        kind: kind.to_string(),
        max: 1_000_000,
        shape: 1.5,
        mean: 0.0,
        stddev: 0.0,
        key: String::new(),
        prefix: "k_".into(),
    };
    match kind {
        "uniform" | "sequential" => {
            let max = it
                .next()
                .ok_or_else(|| format!("{kind}: missing max"))?
                .parse::<u64>()
                .map_err(|e| format!("{kind} max: {e}"))?;
            cfg.max = max;
            if let Some(p) = it.next() {
                cfg.prefix = p.to_string();
            }
        }
        "pareto" => {
            let max = it
                .next()
                .ok_or_else(|| "pareto: missing max".to_string())?
                .parse::<u64>()
                .map_err(|e| format!("pareto max: {e}"))?;
            let shape = it
                .next()
                .ok_or_else(|| "pareto: missing shape".to_string())?
                .parse::<f64>()
                .map_err(|e| format!("pareto shape: {e}"))?;
            cfg.max = max;
            cfg.shape = shape;
        }
        "normal" => {
            let max = it
                .next()
                .ok_or_else(|| "normal: missing max".to_string())?
                .parse::<u64>()
                .map_err(|e| format!("normal max: {e}"))?;
            let mean = it
                .next()
                .ok_or_else(|| "normal: missing mean".to_string())?
                .parse::<f64>()
                .map_err(|e| format!("normal mean: {e}"))?;
            let stddev = it
                .next()
                .ok_or_else(|| "normal: missing stddev".to_string())?
                .parse::<f64>()
                .map_err(|e| format!("normal stddev: {e}"))?;
            cfg.max = max;
            cfg.mean = mean;
            cfg.stddev = stddev;
        }
        "fixed" => {
            cfg.key = it
                .next()
                .ok_or_else(|| "fixed: missing key".to_string())?
                .to_string();
        }
        other => return Err(format!("unknown keygen `{other}`")),
    }
    Ok(cfg)
}

fn valgen_from_cli(spec: &str) -> Result<ValGenConfig, String> {
    let mut it = spec.splitn(3, ':');
    let kind = it.next().ok_or_else(|| "empty valgen".to_string())?;
    let mut cfg = ValGenConfig {
        kind: kind.to_string(),
        size: 256,
        min: 16,
        max: 1024,
        mean: 256,
    };
    match kind {
        "fixed" => {
            cfg.size = it
                .next()
                .ok_or_else(|| "fixed: missing size".to_string())?
                .parse::<usize>()
                .map_err(|e| format!("fixed size: {e}"))?;
        }
        "uniform" => {
            cfg.min = it
                .next()
                .ok_or_else(|| "uniform: missing min".to_string())?
                .parse::<usize>()
                .map_err(|e| format!("uniform min: {e}"))?;
            cfg.max = it
                .next()
                .ok_or_else(|| "uniform: missing max".to_string())?
                .parse::<usize>()
                .map_err(|e| format!("uniform max: {e}"))?;
        }
        "exponential" => {
            cfg.mean = it
                .next()
                .ok_or_else(|| "exponential: missing mean".to_string())?
                .parse::<usize>()
                .map_err(|e| format!("exponential mean: {e}"))?;
        }
        other => return Err(format!("unknown valgen `{other}`")),
    }
    Ok(cfg)
}

fn init_tracing(verbose: bool) {
    let level = if verbose { "debug" } else { "info" };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    let cfg = match build_config(&cli) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config error: {e}");
            return ExitCode::from(2);
        }
    };

    info!(
        "starting run: driver={} duration={} concurrent={}",
        cfg.driver.kind.label(),
        cfg.run.duration,
        cfg.run.concurrent
    );

    let engine = Engine::new(cfg);
    match engine.run() {
        Ok(out) => {
            info!(
                "run complete: ok={} err={} elapsed={:.2}s out_dir={}",
                out.ok_count,
                out.err_count,
                out.elapsed.as_secs_f64(),
                out.out_dir.display()
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            error!("run failed: {e}");
            eprintln!("dyniak-bench: {e}");
            ExitCode::from(1)
        }
    }
}
