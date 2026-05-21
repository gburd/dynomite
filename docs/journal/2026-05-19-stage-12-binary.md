# 2026-05-19 Stage 12 (partial): dynomited binary - CLI + daemonize + pidfile

## Status

PARTIAL: 2 of 5 commits from the dispatch brief landed cleanly.
Commits 1 and 2 (CLI parsing, version banner, ASCII logo,
`--test-conf`, `--describe-stats`, log_init wiring, daemonize,
pid-file management) are complete; commits 3-5 (the async run
loop, manpage generator, full parity table) are deferred to
Stage 12b.

The original Stage 12 worker (pi agent `b629ec25-ce17-4c1`)
delivered the two commits then wedged at 18,170 seconds of
runtime with no tool-use progress for over an hour. Steering
the wedged worker did not produce a response. The lead
recovered the two committed commits via cherry-pick onto a
fresh `stage/12-binary-recovered` branch, validated all gates,
and is merging the partial work as Stage 12 (partial) so
downstream stages can proceed.

## What landed

### Commit 1: `feat(dynomited): land CLI parser, version banner, ASCII logo, and -t`

Files:
- `crates/dynomited/src/cli.rs` (new): clap derive structures
  for every C `getopt_long` flag (`-h`, `-V`, `-t`, `-d`,
  `-D`, `-v`, `-o`, `-c`, `-p`, `-x`, `-m`, `-M`, `-g`).
- `crates/dynomited/src/asciilogo.rs` (new): the C
  `dyn_asciilogo.h` ASCII art ported verbatim as a const
  `&str`. ASCII-only verified.
- `crates/dynomited/src/lib.rs` (new): re-exports for the
  binary's modules so they are testable from
  `tests/cli.rs`.
- `crates/dynomited/src/main.rs`: rewritten from the Stage 0
  placeholder.
- `crates/dynomited/tests/cli.rs` (new): 14 `assert_cmd`
  smoke tests covering `--help`, `--version`,
  `--test-conf` against every YAML in
  `crates/dynomited/conf/`, `--describe-stats`, verbosity
  range checks, mutually-exclusive flag combinations.

The help text and version banner match the C reference's
`print_help` and `print_version` output line for line; the
test-conf success message reproduces the C
`dn_test_conf` text.

### Commit 2: `feat(dynomited): wire log_init, daemonize, and pid-file management`

Files:
- `crates/dynomited/src/daemonize.rs` (new): `daemonize()`
  function performing fork + setsid + stdin/stdout/stderr
  redirection via `nix::unistd::{fork, setsid, dup2}` and
  `nix::sys::stat::umask`. No `unsafe` blocks; `nix` wraps
  the primitives as safe `Result`-returning fns.
- `crates/dynomited/src/pidfile.rs` (new): `PidFile` RAII
  wrapper. `PidFile::create(path)` opens the path
  O_CREAT|O_RDWR, takes an exclusive `nix::fcntl::Flock`,
  writes the current PID, returns. `Drop` removes the file
  and releases the lock.
- `crates/dynomited/src/main.rs`: invokes log init, then
  daemonize (when `-d` is set), then pid-file creation
  (when `-p` is set), in that order. The pid file lives
  for the lifetime of the process; SIGINT/SIGTERM handlers
  trigger Drop.
- `crates/dynomited/tests/cli.rs`: 5 additional assert_cmd
  tests covering pid-file creation, exclusive locking
  (second instance fails), and pid-file cleanup on
  graceful shutdown.

## What did NOT land (deferred to Stage 12b)

The Stage 12 dispatch brief listed five commits; only the
first two delivered before the worker wedged. The remaining
work is captured in a fresh dispatch as Stage 12b:

* **Commit 3**: async run loop. tokio::main entry, build a
  `Server` struct from `Config` + Stage 9 transports +
  Stage 10 ClusterDispatcher + optional Stage 11 entropy
  worker. tokio::select! on SIGINT/SIGTERM/SIGHUP. End-to-
  end Redis-backed integration test that sends SET / GET /
  QUIT through real tokio TCP and asserts byte-equal
  responses.
* **Commit 4**: manpage generator via `clap_mangen` (already
  in workspace dev-deps). Output to
  `crates/dynomited/man/dynomited.8`.
* **Commit 5**: full parity table for `dynomite.c` (the
  partial table in `docs/parity.md` covers Commits 1 + 2
  plus deferral entries for Commits 3 + 4 + 5).

## Test count

Before this stage: 565 nextest, 569 with --all-features,
527 doctests.
After: 584 nextest (+19 from the new assert_cmd tests),
569 with --all-features, 527 doctests.

## C-verification checks performed

The wedged worker's tool log shows it read
`_/dynomite/src/dynomite.c` lines 1-630 fully and
`_/dynomite/src/dyn_asciilogo.h` before stalling. The
recovered commits cite the exact C lines for each
`print_help`/`print_version`/`set_default_options`/
`dynomite_daemonize` parity claim in the inline `cli.rs`
rustdoc.

## Worker stall diagnosis

`b629ec25-ce17-4c1` ran on the default Sonnet model, not
opus-4-7. The stall is not the same failure mode as the
three prior opus-4-7 silent terminations. Tool-use count
was stuck at 134 with the agent's last visible action
being a `cargo build --workspace --all-targets --locked`
that may have been in a long compile cycle when the
pi-tool's tracking lost track. Steering messages did not
elicit a response. The worker's worktree contained the two
clean commits which the lead recovered via cherry-pick.

## Next step

Stage 12b dispatch covers Commits 3 + 4 + 5. The stage 12
branch is closed; Stage 13 (embed API impl) can run in
parallel because it does not depend on the run-loop wiring.
