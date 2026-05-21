//! Manpage generator for `dynomited`.
//!
//! Run via `cargo run -p dynomited --bin gen-man` to regenerate
//! `crates/dynomited/man/dynomited.8`. The bin is intentionally
//! manual: regenerating at build time would force every checkout
//! to depend on `clap_mangen` and would make the manpage diffable
//! across CI runs even when nothing else changed. The runbook is
//! recorded in `docs/journal/2026-05-19-stage-12b-runloop.md`.
//!
//! The OPTIONS table is augmented with a free-form preamble that
//! mirrors the wording of the reference manpage at
//! `_/dynomite/man/dynomite.8` so existing operator-facing
//! documentation still applies. Anything clap can derive
//! automatically (the option list, defaults) flows through
//! `clap_mangen`.

use std::path::PathBuf;

use clap::CommandFactory;
use dynomited::cli::Cli;

const HEADER: &str = ".TH DYNOMITED 8 \"2026-05-19\" \"v0.0.1\"
.SH NAME
dynomited - a generic dynamo implementation for different key/value storage engines (Rust port).
.SH DESCRIPTION
.B Dynomited
is a thin, distributed Dynamo layer for different storage engines and protocols. Dynomite provides sharding and multi-data center replication. It has a shared nothing architecture with no single point of failure (SPOF) that delivers high availability (HA) even when a server, rack or entire data center goes offline.
.PP
Redis is currently the primary backend and protocol supported by Dynomite, while support for Memcached is partially implemented.
.PP
Dynomite provides the following functionality:
.IP \\[bu]
Linear scalability
.IP \\[bu]
High availability (HA)
.IP \\[bu]
Shared nothing architecture with symmetric nodes
.IP \\[bu]
Multi-data center (DC) replication
.IP \\[bu]
Data replication and sharding
.IP \\[bu]
Support for any Redis client plus a specialized Dyno client for Java
.IP \\[bu]
Reduced connections to and lower connection overhead on backend storage engines via persistent connections
.IP \\[bu]
Observability via easily accessible statistics
";

const FOOTER: &str = ".SH SEE ALSO
.BR memcached (8),
.BR redis-server (1)
.br
.SH AUTHOR
Greg Burd <greg@burd.me> and contributors.
Project home: https://github.com/gburd/dynomite
";

fn main() -> std::io::Result<()> {
    let mut cmd = Cli::command();
    // Force the binary name regardless of how the generator is
    // invoked so the manpage always refers to the deployed name.
    cmd = cmd.name("dynomited").bin_name("dynomited");

    let mut buf: Vec<u8> = Vec::new();
    let man = clap_mangen::Man::new(cmd).section("8");
    // Use a custom title to keep the manpage in section 8 and
    // splice our descriptive header in front of clap's OPTIONS
    // section.
    buf.extend_from_slice(HEADER.as_bytes());
    man.render_synopsis_section(&mut buf)?;
    buf.extend_from_slice(b"\n");
    man.render_options_section(&mut buf)?;
    buf.extend_from_slice(b"\n");
    buf.extend_from_slice(FOOTER.as_bytes());

    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let out_dir = crate_dir.join("man");
    std::fs::create_dir_all(&out_dir)?;
    let out_path = out_dir.join("dynomited.8");
    std::fs::write(&out_path, &buf)?;
    eprintln!("wrote {}", out_path.display());
    Ok(())
}
