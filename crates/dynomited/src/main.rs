//! `dynomited` - the Dynomite server binary.
//!
//! Mirrors the reference engine's `main()` flow: parse options,
//! optionally print version/help/describe-stats, optionally
//! validate the YAML config (`-t`), otherwise drop into the
//! [`Server`] run loop. Logging, daemonization, and pid-file
//! management are all wired here; the run loop itself lives in
//! [`dynomited::server`].

use std::process::ExitCode;

use clap::Parser;

use dynomite::conf::Config;
use dynomite::core::log::{build_logs_layer, install_logs_only, LogConfig, LogFormat};
use dynomite::stats::describe_stats;
use dynomited::asciilogo::ASCII_LOGO;
use dynomited::cli::{print_usage, print_version, Cli};
use dynomited::daemonize::{daemonize, DaemonizeOutcome};
use dynomited::observability::{install_global, otlp_any_enabled, ObservabilityGuard};
use dynomited::pidfile::PidFile;
use dynomited::server::Server;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() -> ExitCode {
    // Toggle the engine's `data_store: dyniak` build-time gate on
    // when this binary was compiled with `--features riak`. The
    // gate's default is off; when off, the configuration
    // validator rejects `data_store: dyniak` with a documented
    // error message. The flag is process-wide and read by
    // `dynomite::conf::ConfPool::validate`.
    #[cfg(feature = "riak")]
    dynomite::conf::set_dyniak_supported(true);

    let cli = match Cli::try_parse() {
        Ok(c) => c,
        Err(err) => {
            err.print().ok();
            print_usage();
            return ExitCode::from(1);
        }
    };

    if cli.show_help {
        print_version(VERSION);
        print_usage();
        return ExitCode::SUCCESS;
    }
    if cli.describe_stats {
        print_version(VERSION);
        print!("{}", describe_stats());
        return ExitCode::SUCCESS;
    }
    if cli.show_version {
        print_version(VERSION);
        return ExitCode::SUCCESS;
    }

    if cli.test_conf {
        return run_test_conf(&cli);
    }

    run_server(&cli)
}

fn run_test_conf(cli: &Cli) -> ExitCode {
    if cli.daemonize {
        eprintln!("dynomite: --test-conf and --daemonize are mutually exclusive");
        return ExitCode::from(1);
    }
    let path = &cli.conf_file;
    let parsed = match Config::parse_file(path) {
        Ok(mut cfg) => {
            cfg.finalize();
            match cfg.validate() {
                Ok(()) => Ok(cfg),
                Err(e) => Err(format!("validation: {e}")),
            }
        }
        Err(e) => Err(format!("parse: {e}")),
    };

    match parsed {
        Ok(_) => {
            eprintln!(
                "dynomite: configuration file '{}' syntax is valid",
                path.display()
            );
            ExitCode::SUCCESS
        }
        Err(reason) => {
            eprintln!(
                "dynomite: configuration file '{}' syntax is invalid: {reason}",
                path.display()
            );
            ExitCode::from(1)
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "main's run_server threads CLI parsing, log/format selection, OTLP install, daemonize, runtime build, server build, server run, and tracer-guard shutdown into a single linear flow whose order matters; splitting hides the construction-vs-shutdown ordering invariants"
)]
fn run_server(cli: &Cli) -> ExitCode {
    // Parse and validate the configuration before any side effect
    // so a malformed YAML never leaves a daemonized child or a
    // dangling pid file behind.
    let mut cfg = match Config::parse_file(&cli.conf_file) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "dynomite: configuration file '{}' syntax is invalid: parse: {e}",
                cli.conf_file.display()
            );
            return ExitCode::from(1);
        }
    };
    cfg.finalize();
    if let Err(e) = cfg.validate() {
        eprintln!(
            "dynomite: configuration file '{}' syntax is invalid: validation: {e}",
            cli.conf_file.display()
        );
        return ExitCode::from(1);
    }

    if cli.daemonize {
        match daemonize(true) {
            Ok(DaemonizeOutcome::Parent) => return ExitCode::SUCCESS,
            Ok(DaemonizeOutcome::Child) => {}
            Err(e) => {
                eprintln!("dynomite: daemonize failed: {e}");
                return ExitCode::from(1);
            }
        }
    }

    let log_format = match resolve_log_format(cli, &cfg) {
        Ok(f) => f,
        Err(reason) => {
            eprintln!("dynomite: {reason}");
            return ExitCode::from(1);
        }
    };

    // Distributed-tracing OTLP exporter, OTLP log appender,
    // and the standard log pipeline now share the same global
    // tracing subscriber. The fmt layer (and its SIGHUP-reopen
    // wiring) is built exactly once via `build_logs_layer`; the
    // registry is assembled either with `install_logs_only`
    // (every OTLP knob off, the default-behavior path) or by
    // `install_global` which adds an `OpenTelemetryLayer` and /
    // or an `OpenTelemetryTracingBridge` log appender on top of
    // the same fmt layer. The observability guard must outlive
    // `runtime.block_on(...)` so the batch processors flush on
    // shutdown.
    let log_cfg = LogConfig::new(
        cli.verbosity,
        cli.output.as_deref().map(std::path::PathBuf::from),
        log_format,
    );
    let otlp_obs = cfg
        .pool()
        .observability
        .as_ref()
        .filter(|obs| otlp_any_enabled(obs));
    let obs_guard: Option<ObservabilityGuard> = if let Some(obs) = otlp_obs {
        let (fmt_layer, reopen) = match build_logs_layer(&log_cfg) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("dynomite: log_init failed: {e}");
                return ExitCode::from(1);
            }
        };
        match install_global(obs, cli.verbosity, fmt_layer, reopen) {
            Ok(g) => Some(g),
            Err(e) => {
                eprintln!("dynomite: OTLP exporter install failed: {e}");
                return ExitCode::from(1);
            }
        }
    } else {
        if let Err(e) = install_logs_only(&log_cfg) {
            eprintln!("dynomite: log_init failed: {e}");
            return ExitCode::from(1);
        }
        None
    };

    let _pid = match cli.pid_file.as_deref() {
        Some(path) => match PidFile::create(path) {
            Ok(g) => Some(g),
            Err(e) => {
                tracing::error!(error = %e, path = %path.display(), "create pid file");
                return ExitCode::from(1);
            }
        },
        None => None,
    };

    tracing::info!(
        version = VERSION,
        pid = std::process::id(),
        "dynomited starting"
    );
    tracing::info!("\n{ASCII_LOGO}");

    // The CLI's `--gossip` flag mirrors the reference engine's
    // `-g` knob: it force-enables gossip regardless of the
    // configuration. Apply it before `Server::build` so the
    // resulting `ConfPool` carries the override.
    if cli.gossip {
        cfg.pool_mut().enable_gossip = Some(true);
    }

    if let Some(s) = cli.distribution_shadow.as_deref() {
        match dynomite::conf::Distribution::parse(s) {
            Ok(d) => {
                cfg.pool_mut().distribution_shadow = Some(d);
                tracing::info!(
                    shadow = d.as_str(),
                    "shadow distribution mode enabled via --distribution-shadow"
                );
            }
            Err(e) => {
                tracing::error!(error = %e, "--distribution-shadow value rejected");
                return ExitCode::from(1);
            }
        }
    }

    // CLI overrides for the optional Riak surface. The flags
    // are only present when the binary was built with the
    // `riak` Cargo feature; without the feature this whole
    // block is compiled away and the YAML's `riak:` block (if
    // any) is used verbatim.
    #[cfg(feature = "riak")]
    apply_riak_overrides(cli, &mut cfg);

    #[cfg(feature = "riak")]
    apply_riak_distribution_default(&mut cfg);

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "build tokio runtime");
            return ExitCode::from(1);
        }
    };

    // Capture the YAML configuration path so the run loop can
    // re-read it on `SIGHUP` for the configuration-reload
    // pipeline.
    let conf_path = cli.conf_file.clone();

    let exit = runtime.block_on(async move {
        let server = match Server::build(cfg).await {
            Ok(s) => s.with_conf_path(conf_path),
            Err(e) => {
                tracing::error!(error = %e, "server build failed");
                return ExitCode::from(1);
            }
        };
        match server.run().await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                tracing::error!(error = %e, "server run loop failed");
                ExitCode::from(1)
            }
        }
    });
    // Drop the observability guard inside the runtime so the
    // batch span and log processors' flush tasks have a runtime
    // to run on.
    if let Some(mut g) = obs_guard {
        runtime.block_on(async move { g.shutdown() });
    }
    exit
}

/// Resolve the effective log format for this invocation.
///
/// Precedence: explicit `--log-format` CLI flag > YAML `log_format:`
/// pool field > built-in default ([`LogFormat::Default`]).
fn resolve_log_format(cli: &Cli, cfg: &Config) -> Result<LogFormat, String> {
    if let Some(s) = cli.log_format.as_deref() {
        return LogFormat::parse(s).map_err(|e| format!("--log-format: {e}"));
    }
    if let Some(s) = cfg.pool().log_format.as_deref() {
        return LogFormat::parse(s).map_err(|e| format!("log_format: {e}"));
    }
    Ok(LogFormat::Default)
}

#[cfg(feature = "riak")]
fn apply_riak_overrides(cli: &Cli, cfg: &mut Config) {
    let pool = cfg.pool_mut();
    let any_override =
        cli.riak_pbc_listen.is_some() || cli.riak_http_listen.is_some() || cli.riak_aae_enabled;
    if !any_override {
        return;
    }
    let mut block = pool.riak.clone().unwrap_or_default();
    if let Some(addr) = cli.riak_pbc_listen.clone() {
        block.pbc_listen = Some(addr);
    }
    if let Some(addr) = cli.riak_http_listen.clone() {
        block.http_listen = Some(addr);
    }
    if cli.riak_aae_enabled {
        block.aae_enabled = Some(true);
    }
    pool.riak = Some(block);
}

/// When the Riak listener is configured AND the operator did
/// not explicitly select a distribution, default to
/// [`dynomite::conf::Distribution::RandomSlicing`]. Riak-shaped
/// deployments inherit Riak's full-coverage partition
/// invariant by default; operators who want the legacy vnode
/// behaviour can still set `distribution: vnode` in the YAML.
#[cfg(feature = "riak")]
fn apply_riak_distribution_default(cfg: &mut Config) {
    let pool = cfg.pool_mut();
    let riak_listening = pool
        .riak
        .as_ref()
        .is_some_and(|r| r.pbc_listen.is_some() || r.http_listen.is_some());
    if !riak_listening {
        return;
    }
    if pool.distribution.is_none() {
        pool.distribution = Some(dynomite::conf::Distribution::RandomSlicing);
        tracing::info!(
            distribution = "random_slicing",
            "Riak mode default distribution: random_slicing"
        );
    }
}
