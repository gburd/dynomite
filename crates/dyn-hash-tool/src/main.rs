//! `dyn-hash-tool` is a small command-line wrapper around
//! [`dynomite::hashkit`].
//!
//! Usage:
//!
//! ```text
//! dyn-hash-tool -h <algorithm> -k <key> [-k <key> ...]
//! dyn-hash-tool -h <algorithm> --stdin
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

use std::io::{self, BufRead, Write};

use anyhow::{Context, Result};
use clap::Parser;
use dynomite::hashkit::{hash, HashType};

/// Hash one or more keys with a Dynomite hashkit algorithm.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Algorithm name (e.g. `one_at_a_time`, `md5`, `crc32a`,
    /// `fnv1a_64`, `murmur3`).
    #[arg(short = 'H', long = "hash", required_unless_present = "list")]
    hash: Option<String>,

    /// One or more keys to hash. Repeatable.
    #[arg(short = 'k', long = "key")]
    keys: Vec<String>,

    /// Read additional keys (one per line) from standard input.
    #[arg(long = "stdin", default_value_t = false)]
    stdin: bool,

    /// List supported algorithms and exit.
    #[arg(long = "list", default_value_t = false)]
    list: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut out = io::stdout().lock();

    if cli.list {
        for ty in HashType::all() {
            writeln!(out, "{}", ty.as_str())?;
        }
        return Ok(());
    }

    let ty_name = cli
        .hash
        .as_deref()
        .context("missing required --hash argument")?;
    let ty = HashType::from_name(ty_name)
        .with_context(|| format!("unknown hash algorithm: {ty_name}"))?;

    if cli.keys.is_empty() && !cli.stdin {
        anyhow::bail!("provide at least one -k/--key or pass --stdin");
    }

    for key in &cli.keys {
        write_line(&mut out, ty, key)?;
    }

    if cli.stdin {
        let stdin = io::stdin();
        let lock = stdin.lock();
        for line in lock.lines() {
            let line = line.context("reading key from stdin")?;
            write_line(&mut out, ty, &line)?;
        }
    }
    Ok(())
}

fn write_line<W: Write>(w: &mut W, ty: HashType, key: &str) -> Result<()> {
    let token = hash(ty, key.as_bytes());
    writeln!(w, "{}:{}:{}", ty.as_str(), key, token.to_hex())?;
    Ok(())
}
