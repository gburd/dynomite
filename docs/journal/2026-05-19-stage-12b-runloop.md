# 2026-05-19 Stage 12b: dynomited run loop, integration test, manpage

## Status

READY_FOR_REVIEW. Stage 12b delivers the three commits the
original Stage 12 worker stalled on:

1. `feat(dynomited): wire async run loop and signal handlers`
2. `test(dynomited): add Redis-backed end-to-end integration test`
3. `docs(dynomited): add manpage generator and committed dynomited.8`
4. `docs(stage-12b): close the dynomite.c parity table and journal`

After Stage 12b lands, every C symbol in `_/dynomite/src/dynomite.c`
is mapped to a Rust home or recorded as an explicit deviation in
`docs/parity.md`.

## Files touched (this stage)

```
crates/dynomited/Cargo.toml                        (modified)
crates/dynomited/src/lib.rs                        (modified)
crates/dynomited/src/main.rs                       (modified)
crates/dynomited/src/server.rs                     (new)
crates/dynomited/src/signals.rs                    (new)
crates/dynomited/src/bin/gen-man.rs                (new)
crates/dynomited/man/dynomited.8                   (new)
crates/dynomited/tests/cli.rs                      (modified - pidfile lifecycle)
crates/dynomited/tests/integration.rs              (new, behind feature `integration`)
docs/parity.md                                     (modified - dynomite.c table closed)
docs/journal/2026-05-19-stage-12b-runloop.md       (this file)
Cargo.lock                                         (modified)
```

## Commit 1: async run loop + signal handling + Server struct

`crates/dynomited/src/server.rs` introduces `Server`, the
top-level orchestrator constructed from a validated
`dynomite::conf::Config`. `Server::build` mirrors the C
`dn_pre_run`-into-`dn_run` order:

1. `Config::finalize` + `Config::validate` (idempotent against
   an already-finalised config).
2. Build a `dynomite::cluster::ServerPool` from
   `PoolConfig::from_conf` plus the local-node `Peer` and the
   `dyn_seeds` peers.
3. Wrap the pool in a `dynomite::cluster::ClusterDispatcher`.
4. Bind the client-facing `dynomite::net::Proxy` on
   `pool.listen` (eager bind so failures surface from `build`,
   not from the run loop).
5. Bind the peer-facing `dynomite::net::DnodeProxy` on
   `pool.dyn_listen` when configured. Defaults are applied by
   `apply_defaults` so `dyn_listen` is effectively always
   present; the conditional remains for symmetry with
   `secure_server_option == "none"`.
6. Bind the `dynomite::stats::StatsServer` when `stats_listen`
   is configured.
7. Allocate the `tokio::sync::watch::<bool>` shutdown channel.

`Server::run` is the supervisor:

* Spawns the proxy, dnode_proxy, and stats listener tasks.
* Installs the `SignalSet` (SIGINT, SIGTERM, SIGHUP).
* `tokio::select!`s on the shutdown watch, signal events, and
  unexpected listener completion.
* SIGINT and SIGTERM trigger a graceful shutdown via the watch
  channel; `cancel_future` (a watch-receiver future) resolves,
  the listeners drain their accept loops, and every join handle
  is awaited before `run` returns.
* SIGHUP delegates to `dynomite::core::log::reopen_on_sighup`
  per the brief; config reload is deferred to the embed API in
  Stage 13.

`Server::shutdown` and the cheap-clonable `ShutdownHandle` are
the programmatic equivalent of the signal arms.

`crates/dynomited/src/signals.rs` provides `SignalSet`, a
`tokio::signal::unix` wrapper that exposes a typed `SignalEvent`
enum to the run loop.

`crates/dynomited/src/main.rs` now builds a multi-thread tokio
runtime, applies the `-g` / `--gossip` override to the parsed
`ConfPool`, and drives `Server::build(...).run()`.

The CLI smoke test `pidfile_is_written_and_removed` was
rewritten (it relied on the placeholder runtime exiting
immediately): it now spawns the binary, polls for the pid file
to materialise, sends SIGTERM, and asserts a clean exit.

Two unit tests in `server::tests` exercise `Server::build` and
`Server::run` against ephemeral ports plus the
`ShutdownHandle::shutdown` path.

### Deferrals from the brief

* The brief mentions a `dynomite::cluster::gossip::GossipTask`
  (Stage 10). That type does not exist in the workspace; Stage
  10 ships gossip as `GossipState` data-shape only. The run
  loop emits a `tracing::warn!` when `enable_gossip: true` is
  configured. Recorded in `docs/parity.md` as a deferred row.
* The brief mentions wiring `EntropyReceiver` / `EntropySender`
  when the entropy fields are configured. The YAML schema (Stage
  4) does not yet surface the listen / peer addresses or the
  encrypt flag the entropy modules need; only `recon_key_file`
  / `recon_iv_file` are exposed. Wiring the receiver against
  defaults would require an opinionated set of magic addresses,
  so the run loop instead emits a `tracing::warn!` when
  `recon_key_file` is set and defers the wiring to Stage 13.

### Token saturation

`token_component_to_dyn` saturates `TokenComponent` digits above
`u32::MAX` to `u32::MAX`. The C engine carries arbitrary-precision
tokens; the existing `dynomite::hashkit::DynToken` is a four-byte
big-endian integer. The default config `tokens: '101134286'` is
well below the limit. A full big-int port of `DynToken` is tracked
under the `hashkit/token.c` parity row.

## Commit 2: Redis-backed integration test

`crates/dynomited/tests/integration.rs` is gated behind the
`integration` Cargo feature (added to `crates/dynomited/Cargo.toml`).
The test:

1. Picks ephemeral ports via three `TcpListener::bind("127.0.0.1:0")`
   calls and drops the listeners.
2. Spawns `redis-server` with `--save '' --appendonly no --protected-mode no`
   on the ephemeral backend port. Skips with a notice (no
   failure) when `redis-server` is not on `PATH`.
3. Writes a temporary YAML pointing the dynomite pool at the
   ephemeral redis backend.
4. Spawns `dynomited` against the temp config via
   `assert_cmd::cargo::cargo_bin`.
5. Opens a raw `tokio::net::TcpStream` to the dynomited proxy
   port, writes `*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n`,
   reads `+OK\r\n`, writes `*2\r\n$3\r\nGET\r\n$1\r\nk\r\n`,
   reads back the value `$1\r\nv\r\n`, then writes
   `*1\r\n$4\r\nQUIT\r\n` and drains.
6. Sends `SIGTERM` to dynomited via `nix::sys::signal::kill`,
   waits up to 5s for a clean exit.
7. Tears down `redis-server` with `SIGTERM` and a 5s grace.

The Nix flake provides `redis`, so the dev shell exercises the
gate end-to-end. Inside this Stage 12b worktree (`pi` agent
sandbox), `redis-server` is not on `PATH`, so the test logs
`"redis-server not in PATH; skipping integration test"` and
returns. That matches the brief's "skipped, not failed"
contract.

## Commit 3: Manpage generator

`crates/dynomited/src/bin/gen-man.rs` is a small bin that
consumes the existing `Cli` struct via `clap::CommandFactory`
and emits a section-8 manpage to
`crates/dynomited/man/dynomited.8`. The bin is gated behind a
new `gen-man` Cargo feature so a default `cargo build` does not
pull `clap_mangen` / `roff` into the dependency graph.

### Runbook

To regenerate the manpage:

```
cargo run -p dynomited --features gen-man --bin gen-man
```

The bin overwrites `crates/dynomited/man/dynomited.8` and prints
the path. The brief originally directed
`cargo run -p dynomited --bin gen-man`; we deviated by adding the
`--features gen-man` requirement because a `src/bin/` target
cannot reference dev-only dependencies. Without the feature
gate, every production build would link `clap_mangen` into the
dependency closure, which the brief explicitly tries to avoid by
keeping `clap_mangen` a dev-only concern.

### Wording verification

Compared the generated manpage against `_/dynomite/man/dynomite.8`:

* `.SH NAME`: matches "a generic dynamo implementation for
  different key/value storage engines" with a "(Rust port)"
  suffix to distinguish the Rust binary.
* `.SH DESCRIPTION`: ports the eight-bullet feature list
  verbatim (linear scalability, HA, shared-nothing, multi-DC,
  data replication, Redis client compatibility, persistent
  connections, observability).
* `.SH OPTIONS`: clap_mangen renders the existing `Cli` arg
  attributes; the option list matches the C reference 1:1 for
  the flags the binary keeps (`-h`, `-V`, `-t`, `-d`, `-D`,
  `-v`, `-o`, `-c`, `-p`, `-x`, `-m`, `-M`, `-g`). The C
  reference lists `-s` (stats-port), `-a` (stats-addr), and
  `-i` (stats-interval) in the SYNOPSIS - those flags are
  absent from the C `getopt_long` table (operators set them via
  YAML), so the Rust port leaves them out as well, matching
  Stage 12 commit 1's CLI parity decision.
* `.SH SEE ALSO`: matches `memcached(8)`, `redis-server(1)`.
* `.SH AUTHOR`: rewritten to credit the Rust port and link to
  both upstream repos, matching the project convention from
  `README.md`.

## Commit 4: Parity rows + this journal entry

`docs/parity.md` now closes the `dynomite.c` table:

* `dn_pre_run` -> `dynomited::main::run_server` +
  `Server::build` (the log_init -> daemonize -> pidfile -> bind
  order is preserved).
* `dn_run` -> `Server::run` (tokio multi-thread runtime,
  listener fan-out, watch-channel shutdown).
* `dn_post_run` -> the graceful-drop path inside `Server::run`
  plus `Drop for PidFile`.
* `dn_signal_handlers` (the `signals[]` table in
  `_/dynomite/src/dyn_signal.c`) -> `SignalSet` and the SIGHUP
  arm of `Server::run`. SIGUSR1 / SIGUSR2 / SIGTTIN / SIGTTOU /
  SIGSEGV / SIGPIPE remain deferred (recorded under
  Ambiguities; the kernel default action is the most useful
  fallback for the deferred subset).
* `dn_coredump_init` -> deviation: deferred to operator init
  scripts (the embed API does not own a global `setrlimit`).

A new "Stage 12b additions" subsection lists `Server`,
`ShutdownHandle`, `ServerError`, `SignalSet`, `SignalEvent`,
and the `gen-man` integration of the existing `Cli`.

## Test count

Workspace nextest:

* Before Stage 12b: 584 (Stage 12 partial baseline).
* After Stage 12b commits 1-3: **590** (`cargo nextest run --workspace`).
* After Stage 12b with `--all-features`: **595**
  (`cargo nextest run --workspace --all-features`). The five
  extra tests are the integration test (skipped when
  redis-server is missing) and the four `--features integration`
  / `--features gen-man` builds that cargo enumerates.

Doctests: **536** (527 + 9 from the new `dynomited::server` /
`dynomited::signals` rustdoc).

The `cargo nextest run -p dynomited --features integration`
gate runs the integration test as a single test; in this
sandbox it skipped (no `redis-server`).

## Verification gates run

```
cargo fmt --all -- --check                                       PASS
cargo clippy --workspace --all-targets -- -D warnings            PASS
cargo clippy --workspace --all-targets --all-features -- -D warnings  PASS
cargo build --workspace --all-targets --locked                   PASS
cargo build --workspace --all-targets --all-features --locked    PASS
cargo nextest run --workspace                                    590 passed
cargo nextest run --workspace --all-features                     595 passed
cargo test --doc --workspace                                     536 passed
cargo run -p dynomited -- -h                                     prints help
cargo run -p dynomited -- -V                                     "This is dynomite-0.0.1"
cargo run -p dynomited -- -t -c crates/dynomited/conf/dynomite.yml  "syntax is valid"
cargo nextest run -p dynomited --features integration            skipped (redis-server not in PATH)
```

## C-verification checks performed

* Read `_/dynomite/src/dynomite.c` lines 480-630 in full, with
  particular attention to `dn_pre_run` (480-528), `dn_post_run`
  (530-547), and `dn_run` (549-575). The Rust port preserves
  the `log_init -> daemonize -> pid -> signals -> bind ->
  loop -> teardown` order, with the only reordering being that
  signal installation happens inside `Server::run` (see the
  Ambiguities row in `docs/parity.md`).
* Read `_/dynomite/src/dyn_signal.c` in full. Mapped each row
  of the C `signals[]` table to its Rust counterpart:
  - SIGUSR1, SIGUSR2: deferred (no embed control hook yet).
  - SIGTTIN, SIGTTOU: deferred (log-level up/down hooks live in
    `dynomite::core::log` but are not yet wired through the
    embed API).
  - SIGHUP: live in `Server::run` -> `reopen_on_sighup`.
  - SIGINT: live in `Server::run` -> shutdown.
  - SIGSEGV: deferred; kernel default (core dump) is the most
    useful fallback until we own a stack-trace facility.
  - SIGPIPE: tokio writes return `EPIPE` directly; the C
    `SIG_IGN` is the platform default for our runtime.
* Read `_/dynomite/man/dynomite.8` in full and walked the
  generated manpage section by section.

## Ambiguities and deviations

* SIGSEGV stack trace: the C engine calls `dn_stacktrace(1)`
  before `raise(SIGSEGV)`. The Rust port relies on the kernel
  default; embedded users can opt in to a panic hook (Stage 13).
* `dn_coredump_init`: omitted (deviation noted in `docs/parity.md`).
* Token saturation above `u32::MAX`: documented above and in
  `docs/parity.md`.
* Manpage section number: the reference manpage uses section 1;
  the Rust port uses section 8 because daemons conventionally
  install under `/usr/share/man/man8/`. This matches modern
  packaging guidance (Debian Policy Manual section 12.1) and
  systemd's own `man8/`.

## Next steps

* Stage 13 (embed API) consumes `Server` directly via
  `Server::build` / `ShutdownHandle`; no further refactor is
  needed in this crate.
* Gossip wiring (`GossipTask` over `cluster::gossip::GossipState`
  + `tokio::time::interval`) is queued for the gossip-runtime
  follow-up after Stage 13 lands the embed events stream.
* Entropy YAML surface (the listen / peer / buffer fields) is
  queued alongside Stage 13.
