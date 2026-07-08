# Manual Pages

Dynomite ships Unix manual pages for its executables. They are the
authoritative reference for command-line flags and are installed
alongside the binaries by the packaging in
[`dist/`](DYN_SRC_BASE/dist).

## `dynomited(8)`

The server daemon. Source:
[`crates/dynomited/man/dynomited.8`](DYN_SRC_BASE/crates/dynomited/man/dynomited.8).

View it locally from a checkout:

```sh
man crates/dynomited/man/dynomited.8
# or, after install:
man 8 dynomited
```

It documents every flag (`--conf-file`, `--test-conf`, and the rest), the
`data_store` backends, and the signal handling. The page is generated
from the binary's own argument parser -- run
`cargo run -p dynomited --bin gen-man` to regenerate it after a flag
change, which keeps the man page and the `--help` output from drifting.

```admonish tip title="--help mirrors the man page"
Every flag in `dynomited(8)` is also shown by `dynomited --help`. The man
page adds the prose sections (description, backends, files, signals) that
do not fit in `--help`.
```

## `dyn-admin`

The cluster administration CLI. It is self-documenting via `--help` on
each subcommand:

```sh
dyn-admin --help
dyn-admin ring --help
dyn-admin distribution-dump --help
```

See [Admin CLI (dyn-admin)](../operations/admin.md) for a task-oriented
walk-through of the subcommands.

## `dyn-hash-tool`

A small utility for computing the hashes and tokens Dynomite uses, useful
when reasoning about key placement. See its `--help`:

```sh
dyn-hash-tool --help
```

## Regenerating man pages

The `dynomited.8` page is generated, not hand-maintained. The generator
lives at
[`crates/dynomited/src/bin/gen-man.rs`](DYN_SRC_BASE/crates/dynomited/src/bin/gen-man.rs)
and writes to `crates/dynomited/man/dynomited.8`:

```sh
cargo run -p dynomited --bin gen-man
```

Run it in the same commit as any change to `dynomited`'s flags, so the
shipped page always matches the binary.
