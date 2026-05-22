//! Command-line front end for the `dynomited` server.
//!
//! The flag set mirrors the reference engine's `getopt_long` table in
//! `_/dynomite/src/dynomite.c`. The help and version banners are
//! reproduced byte-identical to the C reference (`dn_show_usage` and
//! the `"This is dynomite-%s"` print) so operators, init-script
//! authors, and downstream tooling can rely on the same surface.
//!
//! # Examples
//!
//! ```
//! use clap::Parser;
//! use dynomited::cli::Cli;
//! let parsed = Cli::try_parse_from(["dynomited", "-V"]).unwrap();
//! assert!(parsed.show_version);
//! ```

use std::fmt::Write as _;
use std::path::PathBuf;

use clap::Parser;

/// Default path to the YAML configuration file.
///
/// Mirrors `DN_CONF_PATH` in the reference engine.
pub const DEFAULT_CONF_PATH: &str = "conf/dynomite.yml";

/// Default verbosity used when `-v` is not passed.
///
/// Matches `DN_LOG_DEFAULT` (`LOG_NOTICE`, value `5`).
pub const DEFAULT_VERBOSITY: u8 = 5;

/// Smallest accepted `--verbosity` value (matches `DN_LOG_MIN`).
pub const MIN_VERBOSITY: u8 = 0;

/// Largest accepted `--verbosity` value (matches `DN_LOG_MAX`).
pub const MAX_VERBOSITY: u8 = 11;

/// Parsed command-line invocation.
///
/// The struct deliberately disables clap's auto-generated help and
/// version flags: the reference engine's `dn_show_usage` output is
/// reproduced verbatim by [`print_usage`] so we cannot let clap emit
/// its own help template.
///
/// # Examples
///
/// ```
/// use clap::Parser;
/// use dynomited::cli::Cli;
/// let cli = Cli::try_parse_from(["dynomited", "-c", "x.yml"]).unwrap();
/// assert_eq!(cli.conf_file.to_str(), Some("x.yml"));
/// ```
#[derive(Debug, Parser)]
#[command(
    name = "dynomited",
    bin_name = "dynomited",
    disable_help_flag = true,
    disable_version_flag = true,
    override_usage = "dynomited [-?hVdDt] [-v verbosity level] [-o output file]\r\n                  [-c conf file] [-p pid file] [-m mbuf size (deprecated)]\r\n                  [-M max alloc messages (deprecated)]"
)]
#[allow(clippy::struct_excessive_bools)]
pub struct Cli {
    /// Show the help banner and exit (sets [`Self::show_version`]).
    #[arg(short = 'h', long = "help")]
    pub show_help: bool,

    /// Show the version banner and exit.
    #[arg(short = 'V', long = "version")]
    pub show_version: bool,

    /// Validate the configuration file and exit.
    #[arg(short = 't', long = "test-conf")]
    pub test_conf: bool,

    /// Daemonize: fork twice, become a session leader, redirect
    /// stdio to /dev/null.
    #[arg(short = 'd', long = "daemonize")]
    pub daemonize: bool,

    /// Print the stats description and exit.
    #[arg(short = 'D', long = "describe-stats")]
    pub describe_stats: bool,

    /// Numeric verbosity. Values outside `0..=11` are rejected.
    #[arg(
        short = 'v',
        long = "verbosity",
        value_name = "N",
        default_value_t = DEFAULT_VERBOSITY,
        value_parser = clap::value_parser!(u8).range(i64::from(MIN_VERBOSITY)..=i64::from(MAX_VERBOSITY)),
    )]
    pub verbosity: u8,

    /// Path to the log output file. `None` keeps logs on standard error.
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    pub output: Option<PathBuf>,

    /// Path to the YAML configuration file.
    #[arg(
        short = 'c',
        long = "conf-file",
        value_name = "FILE",
        default_value = DEFAULT_CONF_PATH,
    )]
    pub conf_file: PathBuf,

    /// Path to the pid file. `None` disables the pid file.
    #[arg(short = 'p', long = "pid-file", value_name = "FILE")]
    pub pid_file: Option<PathBuf>,

    /// Admin operation flag. `0` means "no admin override".
    #[arg(
        short = 'x',
        long = "admin-operation",
        value_name = "N",
        default_value_t = 0
    )]
    pub admin_operation: u32,

    /// Deprecated mbuf chunk size override.
    #[arg(short = 'm', long = "mbuf-size", value_name = "N")]
    pub mbuf_size: Option<u32>,

    /// Deprecated maximum allocated messages override.
    #[arg(short = 'M', long = "max-msgs", value_name = "N")]
    pub max_msgs: Option<u32>,

    /// Force-enable gossip regardless of the YAML setting.
    #[arg(short = 'g', long = "gossip")]
    pub gossip: bool,

    /// Override the YAML `log_format:` knob. Accepts `default`,
    /// `rfc5424`, `rfc3164`, `json`, or `ndjson`.
    #[arg(long = "log-format", value_name = "FMT")]
    pub log_format: Option<String>,
}

/// The version banner the reference engine writes via
/// `log_stderr("This is dynomite-%s" CRLF, VERSION)`.
///
/// `CRLF` is `\r\n` and `log_stderr` appends a trailing `\n`, so the
/// overall byte sequence ends in `"\r\n\n"`.
///
/// # Examples
///
/// ```
/// let s = dynomited::cli::format_version_banner("0.0.1");
/// assert_eq!(s, "This is dynomite-0.0.1\r\n\n");
/// ```
#[must_use]
pub fn format_version_banner(version: &str) -> String {
    format!("This is dynomite-{version}\r\n\n")
}

/// The usage banner the reference engine writes via three sequential
/// `log_stderr` calls in `dn_show_usage`. Each call produces one line
/// terminated by `\n`; lines internal to each call are separated by
/// `\r\n`. The defaults shown reflect the reference engine's
/// compile-time constants.
///
/// # Examples
///
/// ```
/// let s = dynomited::cli::format_usage();
/// assert!(s.starts_with("Usage: dynomite ["));
/// assert!(s.contains("default: 5, min: 0, max: 11"));
/// assert!(s.contains("conf/dynomite.yml"));
/// ```
#[must_use]
pub fn format_usage() -> String {
    let mut out = String::new();
    out.push_str(
        "Usage: dynomite [-?hVdDt] [-v verbosity level] [-o output file]\r\n\
         \x20                 [-c conf file] [-p pid file] [-m mbuf size (deprecated)]\r\n\
         \x20                 [-M max alloc messages (deprecated)]\r\n\n",
    );
    out.push_str(
        "Options:\r\n\
         \x20 -h, --help             : this help\r\n\
         \x20 -V, --version          : show version and exit\r\n\
         \x20 -t, --test-conf        : test configuration for syntax errors and exit\r\n\
         \x20 -d, --daemonize        : run as a daemon\r\n\
         \x20 -D, --describe-stats   : print stats description and exit\n",
    );
    let _ = write!(
        out,
        "  -v, --verbosity=N            : set logging level (default: {DEFAULT_VERBOSITY}, min: {MIN_VERBOSITY}, max: {MAX_VERBOSITY})\r\n\
         \x20 -o, --output=S               : set logging file (default: stderr)\r\n\
         \x20 -c, --conf-file=S            : set configuration file (default: {DEFAULT_CONF_PATH})\r\n\
         \x20 -p, --pid-file=S             : set pid file (default: off)\r\n\
         \x20 -m, --mbuf-size=N            : set size of mbuf chunk in bytes (default: 0 bytes)\r\n\n",
    );
    out
}

/// Print [`format_usage`] to standard error.
pub fn print_usage() {
    eprint!("{}", format_usage());
}

/// Print [`format_version_banner`] to standard error.
pub fn print_version(version: &str) {
    eprint!("{}", format_version_banner(version));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_c_reference() {
        let cli = Cli::try_parse_from(["dynomited"]).unwrap();
        assert_eq!(cli.verbosity, DEFAULT_VERBOSITY);
        assert_eq!(cli.conf_file, PathBuf::from(DEFAULT_CONF_PATH));
        assert_eq!(cli.admin_operation, 0);
        assert!(!cli.daemonize);
        assert!(!cli.gossip);
    }

    #[test]
    fn verbosity_range_enforced() {
        assert!(Cli::try_parse_from(["dynomited", "-v", "12"]).is_err());
        assert!(Cli::try_parse_from(["dynomited", "-v", "0"]).is_ok());
        assert!(Cli::try_parse_from(["dynomited", "-v", "11"]).is_ok());
    }

    #[test]
    fn long_flags_parse() {
        let cli = Cli::try_parse_from([
            "dynomited",
            "--test-conf",
            "--conf-file",
            "x.yml",
            "--pid-file",
            "p.pid",
            "--gossip",
        ])
        .unwrap();
        assert!(cli.test_conf);
        assert_eq!(cli.conf_file.to_str(), Some("x.yml"));
        assert_eq!(
            cli.pid_file.as_deref().and_then(|p| p.to_str()),
            Some("p.pid")
        );
        assert!(cli.gossip);
    }

    #[test]
    fn usage_text_terminators() {
        let s = format_usage();
        assert!(s.contains("\r\n"));
        assert!(s.ends_with('\n'));
        assert!(s.is_ascii());
    }

    #[test]
    fn log_format_flag_parses() {
        let cli = Cli::try_parse_from(["dynomited", "--log-format", "json"]).unwrap();
        assert_eq!(cli.log_format.as_deref(), Some("json"));
        let cli = Cli::try_parse_from(["dynomited"]).unwrap();
        assert!(cli.log_format.is_none());
    }

    #[test]
    fn version_banner_format() {
        let s = format_version_banner("9.9.9");
        assert_eq!(s, "This is dynomite-9.9.9\r\n\n");
    }
}
