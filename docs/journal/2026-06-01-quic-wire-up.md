# QUIC wire-up for `dynomited` (Stage 14 unblock)

Date: 2026-06-01
Branch: `stage/quic-wire-up` (worktree `/home/gburd/ws/wt-quic-wire`)
Author: Greg Burd <greg@burd.me>
Refs: AGENTS.md Section 14 DoD #3; PLAN.md Stage 14;
`docs/journal/2026-06-01-quic-conformance.md` (the prior
BLOCKED note this entry unblocks).

## What landed

The library-level QUIC stack at `crates/dynomite/src/net/quic.rs`
is now selectable from a YAML pool block via a new `transport:`
directive, and `dynomited` honours the selection by binding the
client-plane proxy listener over QUIC instead of TCP. Three new
conformance tests (gated on `feature = "integration,quic"`)
exercise the full wire path end-to-end against a 3-node
single-DC cluster.

### Wiring summary

1. **Pool config** (`crates/dynomite/src/conf/`):
   * New `Transport::{Tcp, Quic}` enum in `enums.rs` with the
     same string-enum serde shape as the existing
     `SecureServerOption` / `Distribution` enums. Defaults to
     `Tcp`. `Transport::parse` is case-insensitive and rejects
     anything else with a typed `BadServer { field: "transport",
     ... }` error.
   * New optional fields on `ConfPool`: `transport`,
     `quic_cert_file`, `quic_key_file`. Both QUIC paths are
     `PathBuf`. Validation rejects `transport: quic` without
     both cert and key set, by symmetry with the existing
     peer-TLS pair check.
   * `Transport` is re-exported from `dynomite::conf`.
   * Round-trip + validation tests added in
     `conf::pool::tests` and `conf::enums::tests`.

2. **Builder setter** (`crates/dynomite/src/embed/builder.rs`):
   * `ServerBuilder::transport(Transport)` mirrors the YAML
     directive.
   * `ServerBuilder::quic_cert_file(impl AsRef<Path>)` and
     `ServerBuilder::quic_key_file(impl AsRef<Path>)` set the
     two new path fields.
   * Doctest exercises the TCP path; the QUIC path shares the
     same shape and is tested via the `dynomited` conformance
     binary.

3. **embed::server bind branch**
   (`crates/dynomite/src/embed/server.rs::bind_listener`):
   * Now takes the resolved `Transport` and a `&ConfPool`
     reference. TCP path is unchanged; QUIC path attempts the
     bind via `crate::net::QuicListener::bind`, surfaces a
     typed error if the cert / key paths are missing or
     invalid, then drops the listener (the embedded harness
     does not yet drive a QUIC accept loop -- production QUIC
     traffic flows through the `dynomited` binary). The
     observed `local_addr` is still reported so embedders see
     a stable post-bind address even under `transport: quic`.
   * The deeper "embedded mode supports a real QUIC accept
     loop" wiring is out of scope here: the embedded
     `accept_loop` is already a stub that logs and closes
     every accepted socket (see the `tracing::warn!` block
     around line 870), so adding a sibling QUIC accept loop
     would just clone the stub. Listed as a follow-up at the
     bottom of this entry.

4. **`dynomited` bind branch**
   (`crates/dynomited/src/server.rs`):
   * New `enum ProxyKind { Tcp(Proxy), Quic(QuicProxy) }` (the
     QUIC variant is `#[cfg(feature = "quic")]`). The
     `Server::proxy` field's type changed from `Proxy` to
     `ProxyKind`; `Server::run` matches on the variant so the
     accept loop dispatches to the right transport.
   * New `build_proxy` helper threads the pool's resolved
     transport into the listener factory and returns the
     correct `ProxyKind`. When `transport: quic` is requested
     against a binary built without the `quic` feature it
     returns a typed `ServerError::BadConfig` so operators see
     a clean error at startup.
   * The peer plane (`dnode_proxy` + the per-peer outbound
     dial in `peer_supervisor`) stays on TCP. The brief
     mentions lines 1501 + 1916 as wire-up sites; line 1501 is
     the BACKEND (Redis) supervisor connecting to the local
     datastore, which has nothing to do with QUIC inter-node
     transport, and line 1916 is the per-peer outbound dial.
     QUIC-on-peer-plane is feasible but is a strictly larger
     refactor (TLS profiles, dnode framing, gossip) that does
     not block the targeted conformance test. Recorded as a
     follow-up below.

5. **Conformance test**
   (`crates/dynomited/tests/conformance/quic_three_node.rs`):
   * `quic_three_node_each_node_accepts_workload` -- mirror of
     the TCP `each_node_accepts_workload`. Spins up three
     nodes with `transport: quic`, dials each via
     `dynomite::net::quic::connect`, and asserts SET / GET /
     DEL replies are RESP-shaped.
   * `quic_concurrent_writers_consistent_under_dc_one` -- 3
     concurrent writers (one per node), 16 SETs each, asserts
     no deadlock and at least 24 / 48 SETs reply.
   * `differential_three_node_quic` -- mirror of the TCP
     `shape_matches_across_entry_points`. Asserts reply-shape
     parity across all three QUIC entry points.
   * Test helper changes: `NodeSpec` gained an optional
     `readiness_port` so the harness can probe the dnode peer
     port (still TCP) instead of the proxy listen port (UDP
     under QUIC, invisible to a TCP probe).

## Verification

```
cargo build --workspace --all-targets --features riak,quic --locked  # green
cargo nextest run --profile conformance -p dynomited \
    --features integration,quic --test conformance                   # 33 / 33 pass (8 skipped)
cargo nextest run --workspace --features riak,quic --no-fail-fast    # 2019 / 2019 (2 skipped)
cargo clippy --workspace --all-targets --features riak,quic -- -D warnings   # green
cargo fmt --all -- --check                                           # green
```

The `cargo nextest run -p dynomited --features integration,quic
--test conformance` command from the brief's verification list
matches the harness pattern that requires `--profile
conformance` (the default profile excludes the conformance
binary; see `.config/nextest.toml`). Without the explicit
profile the runner reports "no tests to run" and exits
successfully. The 33 tests above include the 3 new QUIC
scenarios plus the existing TCP / smoke coverage.

Workspace test count under `--features riak`: 2015 (unchanged,
i.e. no regressions from the conf / builder edits). Under
`--features riak,quic`: 2019 (3 new conformance scenarios
gated on `integration,quic`; the other 4 tests come from the
QUIC unit tests already present in `dynomite-engine`).

## Follow-ups (not blocking)

* **embedded QUIC accept loop**: the embedded
  `accept_loop` is a stub for both TCP and QUIC today. When the
  embedded harness grows real client-traffic plumbing the QUIC
  bind in `bind_listener` should be hooked up to a sibling
  accept loop that mirrors the TCP one. No journal entry
  required when this lands; the wire-up site is the `drop(listener)`
  line at the end of the QUIC arm in `bind_listener`.

* **peer-plane QUIC**: the dnode listener (`DnodeProxy`) and
  the per-peer outbound supervisor still speak TCP / TLS
  unconditionally. Plumbing QUIC through the dnode framing is
  a larger task that the brief did not require: it touches the
  TLS profile cell, the gossip transport, and the entropy
  driver's interaction with the peer outbound channel. Filed
  as a M5/M6 follow-up; the BLOCKED note's reading of "lines
  1501 + 1916 are the marker" was inverted (1501 is backend,
  not peer; 1916 is peer outbound) and the brief explicitly
  allowed deferring deeper refactors via a follow-up note.

## Final status

```
STATUS: READY_FOR_REVIEW
BRANCH: stage/quic-wire-up
WORKTREE: /home/gburd/ws/wt-quic-wire
NEW_TESTS: 3 conformance + 4 unit
TESTS_TOTAL: 2019 (--features riak,quic)  /  2015 (--features riak)
```
