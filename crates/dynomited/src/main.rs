//! `dynomited` - the Dynomite server binary.
//!
//! The binary mirrors the reference engine's `main()` flow:
//! parse options, optionally print version/help/describe-stats,
//! optionally validate the YAML config (`-t`), otherwise drop into
//! the runtime loop. Logging, daemonization, and pid-file
//! management are wired here; the runtime loop itself lands in a
//! later commit in this stage.

use std::process::ExitCode;

use clap::Parser;

use dynomite::conf::Config;
use dynomite::core::log::log_init;
use dynomite::stats::describe_stats;
use dynomited::asciilogo::ASCII_LOGO;
use dynomited::cli::{print_usage, print_version, Cli};
use dynomited::daemonize::{daemonize, DaemonizeOutcome};
use dynomited::pidfile::PidFile;

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

    if let Err(e) = log_init(cli.verbosity, cli.output.as_deref()) {
        eprintln!("dynomite: log_init failed: {e}");
        return ExitCode::from(1);
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

    // The runtime loop arrives in the next commit in this stage.
    // For now, exiting with 0 keeps the daemonize + pid-file
    // smoke tests green.
    tracing::info!("dynomited: runtime loop not yet wired; exiting cleanly");
    ExitCode::SUCCESS
}
