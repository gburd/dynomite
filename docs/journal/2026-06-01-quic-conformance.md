# BLOCKED: QUIC conformance differential test (Stage 14)

Date: 2026-06-01
Branch: `stage/quic-conformance` (worktree `/home/gburd/ws/wt-quic-diff`)
Author: Greg Burd <greg@burd.me>
Refs: AGENTS.md Section 14 DoD #3; `docs/journal/2026-06-01-dod-audit.md`
(item #3); PLAN.md Stage 14.

## BLOCKED: quic transport not exposed in dynomited config

The brief asked for a Stage 14 conformance differential test that
exercises the QUIC transport end-to-end through `dynomited`,
mirroring the TCP scenarios in
`crates/dynomited/tests/conformance/three_node_single_dc.rs` and
`crates/dynomited/tests/conformance/multi_dc.rs`. The brief
included an explicit escape hatch (instruction #5):

> if the engine doesn't expose a QUIC transport flag
> end-to-end, that's a real gap; add a
> `BLOCKED: quic transport not exposed in dynomited config`
> note to `docs/journal/<date>-quic-conformance.md` and stop.
> Do not stub the test.

This entry records the gap and the unblock plan.

## What the engine actually exposes today

QUIC primitives exist at the **library** level only:

* `crates/dynomite/src/net/quic.rs` (500 lines) -- a complete
  `QuicListener` + `QuicTransport` + `connect()` triad backed
  by `quiche`, gated on `feature = "quic"`.
* `crates/dynomite/src/io/reactor.rs` -- the `Transport`
  trait that both `TcpTransport` and `QuicTransport`
  implement.
* `crates/dynomite/tests/stage_09_quic.rs` -- a self-contained
  end-to-end loopback test of the library QUIC stack
  (cert pair via `rcgen`, listener bind, `connect()`, byte
  round-trip).
* `crates/dynomited/tests/conformance/quic_transport.rs` --
  a smoke test that wraps `QuicListener` + `connect()` and
  pushes a RESP-shaped frame (`*3\r\n$3\r\nSET\r\n...` / `+OK\r\n`)
  through the resulting stream. Gated on `feature = "quic"` and
  wired into `crates/dynomited/tests/conformance.rs` already.

QUIC is **not** wired into:

1. **The `Pool` config** (`crates/dynomite/src/conf/pool.rs`):
   no `transport:` field, no `peer_transport:` field, no
   QUIC-specific cert/key paths. Searching the conf module
   for `quic`, `transport`, `Transport`, `udp`, or `protocol`
   returns zero hits.
2. **The embed `Builder`** (`crates/dynomite/src/embed/builder.rs`):
   no `with_peer_transport` or analogous setter; the only
   transport-shaped knob is `secure_server_option` which
   selects TLS scope (none/rack/datacenter/all) over **TCP**.
3. **The `dynomited` binary** (`crates/dynomited/src/server.rs`):
   the file has zero references to `dynomite::net::quic`,
   `QuicListener`, `QuicConfig`, or `QuicTransport`. Every
   listener uses `tokio::net::TcpListener::bind`; every dialler
   constructs a `TcpTransport`. Specifically:
   * line 1501: client-plane accept wraps the socket in
     `TcpTransport::new(stream, ConnRole::Server)`.
   * line 1916: peer-plane (dnode) accept wraps the socket in
     `TcpTransport::new(stream, ConnRole::DnodePeerServer)`.
   * lines 285-286: `dyn_listen` is parsed as a `SocketAddr`
     and bound via `TcpListener::bind` in `embed::server`
     (`bind_listener` at `crates/dynomite/src/embed/server.rs:797`).

There is therefore no path by which a YAML file fed to
`dynomited -c <config>` can select QUIC for either the client
or the peer plane. The brief's targeted tests
(`quic_three_node_single_dc_round_trip`,
`quic_concurrent_writers_consistent_under_dc_one`,
`differential_three_node_single_dc_quic`) cannot be authored
without first plumbing QUIC through the `Pool` -> `embed::Server`
-> `dynomited::server::Server` build chain.

This is also corroborated by the in-tree DoD audit
(`docs/journal/2026-06-01-dod-audit.md` row #3):

> | 3 | Stage 14 differential test for both transports |
> | TCP done; QUIC not exercised under chaos |

## Why the brief's escape hatch is the right action

The brief is explicit: "Do not stub the test." Three avenues
were considered:

1. **Stub the cluster tests with `#[ignore]`.** Rejected:
   the brief forbids stubs, and an ignored 3-node QUIC test
   would falsely advertise that DoD #3 is satisfied.
2. **Substitute library-level QUIC tests for the cluster
   tests.** Rejected: an extended library-only smoke test
   (e.g. SET/GET/DEL through `QuicListener` + `connect()`)
   does not exercise the cluster, the dispatcher, or the
   dnode peer plane. It would not satisfy DoD #3 ("the
   Stage 14 differential test passes for both transports")
   any more than the existing `quic_transport.rs` smoke
   already does. Pretending otherwise would understate
   the gap.
3. **Land the BLOCKED note and stop.** Selected.

The existing `crates/dynomited/tests/conformance/quic_transport.rs`
already covers the QUIC-as-byte-pipe surface; nothing further
is needed at the library level for this brief. The remaining
work is the binary-level plumbing called out below.

## Unblock plan

To make the targeted tests authorable, the engine needs (in
order):

1. **Pool config**: add `peer_transport: <tcp|quic>` (default
   `tcp`) plus `peer_quic_cert:` / `peer_quic_key:` to
   `crates/dynomite/src/conf/pool.rs`. Validate in
   `Pool::validate` so QUIC selection without cert/key fails
   at parse time. Update the YAML schema docs in
   `docs/book/src/configuration.md`.
2. **Embed builder**: add a `peer_transport(PeerTransport)`
   setter on `crates/dynomite/src/embed/builder.rs` that
   threads through to `Pool::peer_transport`. Mirror the
   existing `secure_server_option` shape.
3. **Server bind**: extend `crates/dynomite/src/embed/server.rs`
   `bind_listener` (or add `bind_peer_listener`) so when
   `peer_transport == quic` it returns a
   `dynomite::net::quic::QuicListener` instead of a
   `tokio::net::TcpListener`. The accept loop and the
   downstream `Transport` consumer already abstract over
   `Box<dyn Transport>`, so the change is local to the
   listener factory.
4. **Dnode dialler**: in `crates/dynomited/src/server.rs`
   around line 1883 (the `transport: Box<dyn Transport>`
   construction), branch on the configured
   `peer_transport` and either build a `TcpTransport` (today)
   or call `dynomite::net::quic::connect(peer_addr, cfg)`
   to obtain a `QuicTransport`. Both already implement the
   shared `Transport` trait (`crates/dynomite/src/io/reactor.rs`).
5. **Cert distribution**: decide whether QUIC peer certs reuse
   the existing `secure_server_option` PEM material or carry
   their own. The existing rustls profile cell
   (`server.rs:205-208`) is the natural place to extend.

Once steps 1-4 land, the conformance harness in
`crates/dynomited/tests/conformance/mod.rs` need only render
`peer_transport: quic` plus the cert paths into
`NodeSpec::render_yaml` and the brief's three targeted tests
become straightforward translations of the TCP scenarios.
Step 5 is a deployment polish item; tests can supply per-node
self-signed certs the same way `stage_09_quic.rs` already does.

## What I changed in this branch

Nothing under `crates/`. The only file touched is this journal
entry, committed on `stage/quic-conformance` so the lead has
an audit trail for the BLOCKED status.

## Verification

`scripts/check.sh` was not run because no source files
changed; the only artefact is this Markdown note, which is
ASCII and tracked in `docs/journal/`.

## Final status

```
STATUS: BLOCKED
BRANCH: stage/quic-conformance
NEW_TESTS: 0
QUIC_FEATURE: quic (library-level only; not wired into dynomited)
RESULT: BLOCKED: quic transport not exposed in dynomited config
NOTES: Library QUIC stack at crates/dynomite/src/net/quic.rs is
       feature-complete (Stage 9). The smoke test at
       crates/dynomited/tests/conformance/quic_transport.rs
       already exercises QUIC-as-byte-pipe under
       --features integration,quic. Cluster-shaped scenarios
       (3-node single-DC, concurrent writers, differential)
       require Pool/Builder/Server plumbing per the unblock
       plan above before they can be authored without stubs.
```
