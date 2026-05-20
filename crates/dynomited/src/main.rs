//! `dynomited` - the Dynomite server binary.
//!
//! The binary mirrors the reference engine's `main()` flow:
//! parse options, optionally print version/help/describe-stats,
//! optionally validate the YAML config (`-t`), otherwise drop into
//! the runtime loop. The runtime loop and signal handling land in
//! later commits in this stage.

use std::process::ExitCode;

use clap::Parser;

use dynomite::conf::Config;
use dynomite::stats::describe_stats;
use dynomited::asciilogo::ASCII_LOGO;
use dynomited::cli::{print_usage, print_version, Cli};

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(c) => c,
        Err(err) => {
            // clap renders the underlying error (e.g. "invalid value
            // for `-v`"); we then append the C-style usage banner so
            // the operator sees the same text dn_show_usage would
            // produce on a parse failure.
            err.print().ok();
            print_usage();
            return ExitCode::from(1);
        }
    };

    // -h prints version then usage, then exits 0. -D prints version
    // then the stats description, then exits 0. -V on its own prints
    // the version banner only. This mirrors the reference engine's
    // `if (show_version) { ... exit(0); }` block at the bottom of
    // dn_get_options' caller.
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

    // The startup banner lands in stderr regardless of the eventual
    // log sink, matching `dn_print_run`'s `loga(..)` calls. Wiring
    // the runtime loop up is the job of later commits in this stage.
    eprintln!("{ASCII_LOGO}");
    eprintln!(
        "dynomited {VERSION}: server runtime is not yet wired in this binary; \
         use --test-conf to validate configuration."
    );
    ExitCode::from(1)
}

fn run_test_conf(cli: &Cli) -> ExitCode {
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
