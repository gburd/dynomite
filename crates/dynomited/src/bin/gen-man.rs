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
//! describes the server for operators. Anything clap can derive
//! automatically (the option list, defaults) flows through
//! `clap_mangen`.
//!
//! The version string is read from `CARGO_PKG_VERSION` at build
//! time so the manpage cannot drift from the crate version.

use std::path::PathBuf;

use clap::CommandFactory;
use dynomited::cli::Cli;

const HEADER: &str = concat!(
    ".TH DYNOMITED 8 \"2026-06-18\" \"v",
    env!("CARGO_PKG_VERSION"),
    "\"
.SH NAME
dynomited - a generic Dynamo implementation for different key/value storage engines (Rust port).
.SH DESCRIPTION
.B dynomited
is a distributed, Dynamo-style replication layer that fronts a per-node data store. It provides sharding and multi-data-center replication on a shared-nothing architecture with no single point of failure (SPOF), so the cluster stays available even when a server, rack, or entire data center goes offline.
.PP
The data store backing each node is selected by the
.B data_store
configuration key and may be one of three first-class backends:
.IP \\[bu]
.B valkey
(alias
.BR redis ):
the Valkey / RESP wire protocol, vendor neutral.
.IP \\[bu]
.B memcache :
the Memcached text protocol.
.IP \\[bu]
.B dyniak :
the built-in Riak-compatible store (Riak PBC and HTTP surfaces, backed by an embedded noxu engine).
.PP
Client and inter-node traffic run over TCP or QUIC, on IPv4 or IPv6.
.PP
dynomited provides the following functionality:
.IP \\[bu]
Multi-data-center (DC) replication
.IP \\[bu]
Gossip-based cluster membership and topology discovery
.IP \\[bu]
Tunable quorum reads and writes
.IP \\[bu]
Hinted handoff for writes to temporarily unavailable peers
.IP \\[bu]
Read repair on divergent replicas
.IP \\[bu]
Active anti-entropy (Merkle-tree) reconciliation
.IP \\[bu]
Shared-nothing architecture with symmetric nodes and no SPOF
.IP \\[bu]
Data replication and consistent-hash sharding
.IP \\[bu]
Observability via easily accessible statistics
"
);

const FOOTER: &str = ".SH SEE ALSO
.BR memcached (8),
.BR valkey-server (1)
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
