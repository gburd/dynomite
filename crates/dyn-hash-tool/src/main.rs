//! `dyn-hash-tool` is a small command-line wrapper around
//! [`dynomite::hashkit`].
//!
//! # Default mode
//!
//! ```text
//! dyn-hash-tool -H <algorithm> --key <key> [--key <key> ...]
//! dyn-hash-tool -H <algorithm> --stdin
//! ```
//!
//! For each input key the tool prints one line of the form
//!
//! ```text
//! <algorithm>:<key>:<token-hex>
//! ```
//!
//! where `<token-hex>` is the magnitude bytes of the resulting token in
//! big-endian order (eight hex digits per word). Example:
//!
//! ```text
//! one_at_a_time:hello:01b78c00
//! murmur3:hello:6a85af6f1bd6efee9b4c9bf2c2eaa2ce
//! ```
//!
//! # C-compatible mode (`--c-compat`)
//!
//! Reads keys one per line from `--input/-i` (default stdin, `-` also
//! means stdin) and writes one decimal token per line to `--output/-o`
//! (default stdout, `-` also means stdout). Only `murmur` is supported
//! in this mode (the C `dyn_hash_tool` only supported murmur); using any
//! other algorithm with `--c-compat` is rejected with an explicit error.
//!
//! When `-k` is also passed, each token line is preceded by a
//! `KEY:<key>` line, matching the C tool's `-k`/`--outputkey` flag:
//!
//! ```text
//! KEY:hello
//! 12345678
//! KEY:world
//! 87654321
//! ```
//!
//! Note: the short flag `-k` is overloaded for parity with the C tool;
//! to pass an inline key on the command line, use the long flag
//! `--key`.

use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use dynomite::hashkit::{hash, HashType};

/// Hash one or more keys with a Dynomite hashkit algorithm.
#[allow(clippy::struct_excessive_bools)] // CLI surface mirrors the C tool's flag set
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Algorithm name (e.g. `one_at_a_time`, `md5`, `crc32a`,
    /// `fnv1a_64`, `murmur3`). Required unless `--list` is set; in
    /// `--c-compat` mode defaults to `murmur` if omitted.
    #[arg(short = 'H', long = "hash")]
    hash: Option<String>,

    /// One or more keys to hash on the command line (default mode
    /// only). Repeatable.
    #[arg(long = "key")]
    keys: Vec<String>,

    /// Read additional keys (one per line) from standard input
    /// (default mode only; `--c-compat` reads from `--input` instead).
    #[arg(long = "stdin", default_value_t = false)]
    stdin: bool,

    /// List supported algorithms and exit.
    #[arg(long = "list", default_value_t = false)]
    list: bool,

    /// Emit output in the format produced by the C `dyn_hash_tool`:
    /// one decimal token per line, optionally preceded by a
    /// `KEY:<key>` line when `-k` is set. Requires `murmur`.
    #[arg(long = "c-compat", default_value_t = false)]
    c_compat: bool,

    /// In `--c-compat` mode, prefix each token line with a `KEY:<key>`
    /// line (mirrors the C `-k`/`--outputkey` flag).
    #[arg(short = 'k', default_value_t = false)]
    c_outputkey: bool,

    /// Input file (`--c-compat` mode). Use `-` or omit for stdin.
    #[arg(short = 'i', long = "input")]
    input: Option<PathBuf>,

    /// Output file (`--c-compat` mode). Use `-` or omit for stdout.
    #[arg(short = 'o', long = "output")]
    output: Option<PathBuf>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.list {
        let mut out = io::stdout().lock();
        for ty in HashType::all() {
            writeln!(out, "{}", ty.as_str())?;
        }
        return Ok(());
    }

    if cli.c_compat {
        run_c_compat(&cli)
    } else {
        run_default(&cli)
    }
}

fn run_default(cli: &Cli) -> Result<()> {
    let ty_name = cli
        .hash
        .as_deref()
        .context("missing required --hash argument")?;
    let ty = HashType::from_name(ty_name)
        .with_context(|| format!("unknown hash algorithm: {ty_name}"))?;

    if cli.keys.is_empty() && !cli.stdin {
        anyhow::bail!("provide at least one --key or pass --stdin");
    }

    let mut out = io::stdout().lock();
    for key in &cli.keys {
        write_default_line(&mut out, ty, key)?;
    }

    if cli.stdin {
        let stdin = io::stdin();
        let lock = stdin.lock();
        for line in lock.lines() {
            let line = line.context("reading key from stdin")?;
            write_default_line(&mut out, ty, &line)?;
        }
    }
    Ok(())
}

fn run_c_compat(cli: &Cli) -> Result<()> {
    // The C dyn_hash_tool only supports murmur. Default the algorithm
    // when omitted and reject any other choice explicitly.
    let ty_name = cli.hash.as_deref().unwrap_or("murmur");
    let ty = HashType::from_name(ty_name)
        .with_context(|| format!("unknown hash algorithm: {ty_name}"))?;
    if !matches!(ty, HashType::Murmur) {
        anyhow::bail!(
            "--c-compat only supports the murmur algorithm (got {})",
            ty.as_str()
        );
    }

    if !cli.keys.is_empty() {
        anyhow::bail!("--c-compat reads keys from --input, not --key");
    }
    if cli.stdin {
        anyhow::bail!("--c-compat reads keys from --input; --stdin is not allowed");
    }

    let input: Box<dyn Read> = match cli.input.as_deref() {
        None => Box::new(io::stdin().lock()),
        Some(p) if p.as_os_str() == "-" => Box::new(io::stdin().lock()),
        Some(p) => Box::new(
            File::open(p).with_context(|| format!("could not open input {}", p.display()))?,
        ),
    };
    let output: Box<dyn Write> = match cli.output.as_deref() {
        None => Box::new(io::stdout().lock()),
        Some(p) if p.as_os_str() == "-" => Box::new(io::stdout().lock()),
        Some(p) => Box::new(
            File::create(p).with_context(|| format!("could not open output {}", p.display()))?,
        ),
    };

    let mut reader = BufReader::new(input);
    let mut writer = BufWriter::new(output);
    let mut buf = String::new();
    loop {
        buf.clear();
        let n = reader
            .read_line(&mut buf)
            .context("reading key from input")?;
        if n == 0 {
            break;
        }
        let key = buf.trim_end_matches('\n').trim_end_matches('\r');
        let token = hash(ty, key.as_bytes());
        let value = token.get_int();
        if cli.c_outputkey {
            writeln!(writer, "KEY:{key}")?;
        }
        writeln!(writer, "{value}")?;
    }
    writer.flush().context("flushing output")?;
    Ok(())
}

fn write_default_line<W: Write>(w: &mut W, ty: HashType, key: &str) -> Result<()> {
    let token = hash(ty, key.as_bytes());
    writeln!(w, "{}:{}:{}", ty.as_str(), key, token.to_hex())?;
    Ok(())
}
