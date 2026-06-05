# Conformance suite

The conformance suite verifies that `dynomited` (the Rust port)
behaves equivalently to the upstream C `dynomite` daemon on a
representative workload. It is the entry gate for "drop-in
replacement" claims: every supported transport, every
consistency level, and every Redis command class the C harness
exercised has a corresponding Rust scenario.

## Layout

The suite is one Cargo integration-test binary plus a
companion differential rig:

```
crates/dynomited/tests/
  conformance.rs                  - test crate entry
  conformance/
    mod.rs                        - cluster spawner, redis
                                    backend spawner, RESP client
    single_node.rs                - 1-node Redis workload
    three_node_single_dc.rs       - 3-node single-DC workload
    multi_dc.rs                   - 2 DCs * 2 racks * 2 nodes
    quic_transport.rs             - QUIC transport (gated)
    python_harness.rs             - Rust adaptation of the
                                    functional test scenarios
  differential.rs                 - C-vs-Rust corpus driver
  fixtures/conformance/commands.txt - 100+ RESP/Memcached lines
```

Each scenario starts with a runtime check for `valkey-server` on
`PATH`. When Redis is missing the test prints a skip notice and
returns successfully; the suite never fails just because Redis
is not installed.

## Running locally

The Nix flake provides `valkey-server`, `cargo-nextest`, and
the rest of the toolchain. From the workspace root:

```bash
nix develop
cargo nextest run --profile conformance \
    -p dynomited \
    --features integration \
    --test conformance --test differential
```

Add `--features integration,quic` to also exercise the QUIC
transport scenarios. Without `--features quic` the QUIC file is
not compiled.

## JUnit output

The `conformance` profile in `.config/nextest.toml` writes a
JUnit XML report to `target/nextest/conformance/junit.xml`.
`scripts/check.sh` mirrors that file to
`target/junit/conformance.xml` so CI workflows can upload it
verbatim. Both paths are disposable (`target/` is gitignored);
the canonical run is invocation-by-invocation.

## Differential rig

`tests/differential.rs` reads
`tests/fixtures/conformance/commands.txt`, decodes each line
into a wire frame, and drives it through the Rust cluster.
Set `CONFORMANCE_C_BINARY=/path/to/dynomite` to also drive the
C reference; without that env var the rig records the Rust
replies under `target/conformance/divergence/<id>.rust` and
skips the byte-equivalence assertion (the C reference build is
not yet wired into the workspace).

## Cleanup discipline

Every spawned `valkey-server` and `dynomited` child runs in its
own process group (`std::os::unix::process::CommandExt::process_group(0)`).
The `Cluster` Drop impl sends `SIGTERM` to each process group,
waits a short grace window, then upgrades to `SIGKILL`. The
unit test
`helpers::tests::drop_kills_child_process_group` proves the
guarantee end-to-end: spawning a long-sleeping child and
dropping the wrapper terminates the entire group, even on a
panic-driven unwind.

## Adding scenarios

1. Drop a new `*.rs` file under `tests/conformance/`.
2. Add `mod <name>;` (or `#[path = ...] mod <name>;`) to
   `tests/conformance.rs`.
3. Use the `helpers::Cluster::launch` builder to spin up the
   topology you need; assert against `RespClient` replies.
4. Update `docs/parity.md` with any new C-side behaviour the
   scenario covers.

## What the suite does NOT cover

* The 1-hour chaos test (PLAN.md Stage 16).
* The >= 95% coverage gate (PLAN.md Stage 15). Stage 14 pushes
  the listener / conn-FSM / dispatcher modules above 90%; the
  remaining gap is documented as a Stage 15 prerequisite in
  `docs/parity.md` Deviations.
* Live byte-level differential against the C reference for
  workloads beyond the 100-command corpus. That gate lights up
  once a static-lib build of `dynomite` is wired into
  `target/cref/` (a Stage 16 packaging task).
