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
use dynomite::core::log::log_init;
use dynomite::stats::describe_stats;
use dynomited::asciilogo::ASCII_LOGO;
use dynomited::cli::{print_usage, print_version, Cli};
use dynomited::daemonize::{daemonize, DaemonizeOutcome};
use dynomited::observability::{install_global, TracerGuard};
use dynomited::pidfile::PidFile;
use dynomited::server::Server;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() -> ExitCode {
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

    // Distributed-tracing OTLP exporter must be wired BEFORE
    // `log_init` because both helpers install a global
    // `tracing` subscriber and only one global default may be
    // set per process. When `observability.otlp_traces_endpoint`
    // is unset (the default-behavior path), install_global
    // returns Ok(None) and we fall through to `log_init` as
    // before. When the operator opts in, install_global wires a
    // layered Registry+EnvFilter+fmt+OTel subscriber and we skip
    // log_init's subscriber install (the fmt layer takes its
    // place). The guard must outlive `runtime.block_on(...)` so
    // the batch span processor flushes on shutdown.
    let tracer_guard: Option<TracerGuard> = match cfg.pool().observability.as_ref() {
        Some(obs) => match install_global(obs, cli.verbosity) {
            Ok(g) => g,
            Err(e) => {
                eprintln!(
                    "dynomite: OTLP exporter install failed: {e}; continuing without distributed tracing"
                );
                None
            }
        },
        None => None,
    };

    if tracer_guard.is_none() {
        if let Err(e) = log_init(cli.verbosity, cli.output.as_deref()) {
            eprintln!("dynomite: log_init failed: {e}");
            return ExitCode::from(1);
        }
    } else {
        // OTLP path installed a fmt+otel layered subscriber
        // already; the standalone log_init STATE (used by SIGHUP
        // log-reopen) is intentionally unset. SIGHUP log-reopen
        // is unavailable in OTLP mode; operators that need both
        // can run with otlp_traces_endpoint unset.
        tracing::debug!("OTLP exporter installed; skipping log_init STATE setup");
    }

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

    let exit = runtime.block_on(async move {
        let server = match Server::build(cfg).await {
            Ok(s) => s,
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
    // Drop the tracer guard inside the runtime so the batch span
    // processor's flush task has a runtime to run on.
    if let Some(mut g) = tracer_guard {
        runtime.block_on(async move { g.shutdown() });
    }
    exit
}
