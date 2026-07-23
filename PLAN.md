# Dynomite -> Rust Port: Staged Plan

This is the authoritative, end-to-end plan for porting the Netflix Dynomite C
codebase into a single Rust workspace. The result must be:

1. A drop-in replacement server (`dynomited`) functionally identical to the C
   `dynomite` daemon, including configuration format, on-the-wire DNODE
   protocol semantics, gossip, consistency model, hashing, and stats.
2. A library crate (`dynomite`) that exposes the same engine for embedding in
   other Rust programs, with a stable, documented programmatic API.
3. Connectivity between Dynomite peers via either TCP (default, matching the
   original) or QUIC (optional, via the `quiche` crate). Both transports must
   support IPv4 and IPv6.
4. Documentation, examples, integration tests, fuzz targets, property tests,
   and benchmarks delivered alongside the code.

The new code does not need to be byte-for-byte protocol-compatible with the C
binary, but every algorithm, state machine, configuration knob, and
user-visible behavior must match. The C source is the canonical specification.
When in doubt, re-read the C and reproduce its behavior precisely.

---

## 0. Inventory of the C source

These are the files the port covers. Every `.c`/`.h` here must end up
represented in Rust (one or more modules per logical unit).

### Core (`_/dynomite/src/`)

| C file(s) | Purpose | Primary Rust target |
|---|---|---|
| `dynomite.c` | `main()`: arg parsing, daemonize, signal setup, log init, spawn workers | `dynomited` binary in `crates/dynomited/src/main.rs` |
| `dyn_core.{c,h}` | Process state machine, context, worker loop, top-level pool init/teardown, secondary thread spawn (stats, gossip, entropy) | `core::context`, `core::loop_` |
| `dyn_setting.{c,h}` | Global runtime knobs (msgs/sec) | `core::setting` |
| `dyn_signal.{c,h}` | UNIX signal -> action table | `core::signal` |
| `dyn_log.{c,h}` | Leveled logger (LOG_EMERG..LOG_PVERB), file rotation on SIGHUP | `core::log` |
| `dyn_array.{c,h}` | Generic dynamic array | replaced by `Vec<T>`; helpers in `util::array` if a 1:1 callsite needs them |
| `dyn_string.{c,h}` | Length-tagged string with `string_*` API | replaced by `bytes::Bytes` / `String`; thin `util::dyn_string` shim where needed |
| `dyn_mbuf.{c,h}` | Pooled message buffer chain with iovec-style slabs | `io::mbuf` (custom; performance-critical) |
| `dyn_cbuf.h` | Lock-free single-producer/single-consumer ring buffer | `io::cbuf` |
| `dyn_queue.h` | sys/queue.h-style intrusive lists | replaced by `VecDeque`/`std::collections::LinkedList` per call site |
| `dyn_rbtree.{c,h}` | Red-black tree (used for token ring lookups) | `BTreeMap`/`util::rbtree` (only if ordering semantics force it) |
| `dyn_dict.{c,h}` | Hash dictionary (Redis-style, generic) | replaced by `HashMap` / `ahash`-keyed map |
| `dyn_dict_msg_id.{c,h}` | Specialized msg_id -> msg dict | `core::msg_index` |
| `dyn_ring_queue.{c,h}` | C2G/G2C cross-thread queues | `core::ring_queue` (built on `crossbeam-channel`) |
| `dyn_histogram.{c,h}` | Latency histogram | `stats::histogram` |
| `dyn_stats.{c,h}` | Pool/server/peer metric tables, REST stats endpoint | `stats::*` |
| `dyn_util.{c,h}` | Time, atoi, hostname, sockaddr utilities | `util::*` |
| `dyn_types.{c,h}` | `rstatus_t`, `msec_t`, `msgid_t`, `sockinfo`, `string` | `core::types` |
| `dyn_message.{c,h}` | `struct msg` request/response, fragment, coalesce, response handlers | `msg::*` |
| `dyn_request.c` | Request lifecycle: forward, quorum, retries, timeouts | `msg::request` |
| `dyn_response.c` | Response lifecycle | `msg::response` |
| `dyn_response_mgr.{c,h}` | Per-DC response manager: quorum tracking, checksums, reconcile | `msg::response_mgr` |
| `dyn_connection.{c,h}` + `dyn_connection_internal.{c,h}` | `struct conn` and ops vtable for all 6 connection roles | `net::conn` (with role-specific impls) |
| `dyn_connection_pool.{c,h}` | Per-server outbound connection pooling | `net::conn_pool` |
| `dyn_client.{c,h}` | Client (datastore-protocol) connection state | `net::client` |
| `dyn_proxy.{c,h}` | Datastore listener (PROXY) | `net::proxy` |
| `dyn_server.{c,h}` | `struct datastore`, `struct server_pool`, `datacenter`, `rack`, server pool init, `server_pool_dispatch`, auto-eject, hash ring | `cluster::pool`, `cluster::server`, `cluster::dispatch` |
| `dyn_dnode_client.{c,h}` | Inbound peer connection (DNODE_PEER_CLIENT) | `net::dnode_client` |
| `dyn_dnode_proxy.{c,h}` | Peer listener (DNODE_PEER_PROXY) | `net::dnode_proxy` |
| `dyn_dnode_peer.{c,h}` | Peer state, outbound peer conn (DNODE_PEER_SERVER), forwarding, retries | `cluster::peer` |
| `dyn_dnode_msg.{c,h}` | DNODE wire framing (parse/write `dmsg`, magic 2014, type ids, encrypted payload) | `proto::dnode` |
| `dyn_dnode_request.c` | Peer-side request handling | `cluster::peer_request` |
| `dyn_gossip.{c,h}` | Gossip thread: SYN/ACK/SHUTDOWN, datacenter/rack/node tables | `cluster::gossip` |
| `dyn_node_snitch.{c,h}` | DC/rack proximity snitch | `cluster::snitch` |
| `dyn_vnode.{c,h}` | vnode/token math | `cluster::vnode` |
| `dyn_task.{c,h}` | Periodic timer-driven task scheduler | `core::task` |
| `dyn_conf.{c,h}` | YAML 1.1 parser, `conf_pool`, validation, transform to runtime structs | `conf::*` |
| `dyn_crypto.{c,h}` | AES-256-CBC payload crypto, RSA key wrap, base64, PEM loading | `crypto::*` |
| `dyn_setting.c` | global tunable msgs_per_sec | `core::setting` |
| `dyn_test.c` | Integration test harness (DNODE parser fuzz-y inputs) | folded into `tests/` integration tests |

### Subdirectories

| Path | Files | Rust target |
|---|---|---|
| `src/event/` | `dyn_event.h`, `dyn_epoll.c`, `dyn_kqueue.c`, `dyn_evport.c` | replaced by a single `net::reactor` built on `tokio` (see Stage 2) |
| `src/hashkit/` | `dyn_hashkit.{c,h}`, `dyn_token.{c,h}`, `dyn_crc16.c`, `dyn_crc32.c`, `dyn_fnv.c`, `dyn_hsieh.c`, `dyn_jenkins.c`, `dyn_md5.c`, `dyn_murmur.c`, `dyn_murmur3.c`, `dyn_one_at_a_time.c`, `dyn_ketama.c`, `dyn_modula.c`, `dyn_random.c` | `hashkit::*` (one module per algorithm); `hashkit::token`, `hashkit::ketama`, `hashkit::modula` |
| `src/proto/` | `dyn_proto.h`, `dyn_redis.c`, `dyn_redis_repair.c`, `dyn_proto_repair.h`, `dyn_memcache.c` | `proto::redis`, `proto::redis::repair`, `proto::memcache` |
| `src/seedsprovider/` | `dyn_seeds_provider.h`, `dyn_dns.c`, `dyn_florida.c` | `seeds::dns`, `seeds::florida`, `seeds::simple` |
| `src/entropy/` | `dyn_entropy.h`, `dyn_entropy_snd.c`, `dyn_entropy_rcv.c`, `dyn_entropy_util.c` | `entropy::{send, receive, util}` |
| `src/tools/` | `dyn_hash_tool.c` | `crates/dyn-hash-tool/` |

### Contrib

| Path | Note |
|---|---|
| `contrib/murmur3/` | Hand-rolled MurmurHash3 - replace with crate `fasthash` or open-coded `hashkit::murmur3` (we open-code to avoid a heavy dep). |
| `contrib/yaml-0.1.4/` | Bundled libyaml - replaced by `serde_yaml` parsing onto a typed config schema. |
| `contrib/fmemopen.{c,h}` | Stub for non-Linux fmemopen - unneeded in Rust. |

### Resources to carry over

* `conf/*.yml` -> copied verbatim into `crates/dynomited/conf/` for the test
  matrix; the parser must accept them unchanged.
* `man/dynomite.8` -> regenerated as `crates/dynomited/man/dynomited.8` by
  `clap_mangen` and content-checked against the original wording.
* `docs/dyn_protocol.txt`, `docs/florida.md`, `notes/*` -> copied into
  `docs/reference/` and reformatted as markdown without changing content.
* `bin/*.sh`, `init/*` -> mirrored into `dist/` for packaging.
* `test/*.py` -> kept as the Python integration harness; ported to call the
  Rust binary instead of the C one and used as one of the conformance test
  suites (Stage 12).

---

## 1. Workspace layout

```
dynomite/                       # repo root (cwd)
  Cargo.toml                    # workspace
  rust-toolchain.toml           # pinned toolchain
  flake.nix flake.lock          # Nix dev shell
  PLAN.md AGENTS.md README.md LICENSE NOTICE
  crates/
    dynomite/                   # library crate (the engine)
      src/
        lib.rs
        core/                   # context, log, signal, types, setting, task, ring_queue
        util/                   # mbuf-adjacent helpers, time, sockaddr, base64
        io/                     # mbuf, cbuf, reactor (transport-agnostic)
        net/                    # tcp + quic transports, conn fsm, proxy/dnode_proxy listeners, conn_pool
        proto/                  # redis, redis::repair, memcache, dnode wire codec
        msg/                    # msg, request, response, response_mgr, msg_index
        cluster/                # pool, server, peer, dispatch, gossip, snitch, vnode
        hashkit/                # all hash algos + token + ketama + modula + random
        seeds/                  # simple, dns, florida
        crypto/                 # aes, rsa wrap, base64, pem
        entropy/                # send, receive, util
        stats/                  # pool/server/peer counters, histogram, REST endpoint
        conf/                   # serde_yaml schema, validation, transform
        embed/                  # public embedding API: builder, handles, hooks
      tests/                    # integration tests using the public API
      benches/                  # criterion micro+macro
      examples/                 # `embedded.rs`, `cluster3.rs`, ...
    dynomited/                  # binary crate (server)
      src/main.rs
      conf/                     # the original YAML files, byte-for-byte
      man/                      # generated manpage
      tests/                    # cli + smoke
    dyn-hash-tool/              # ports src/tools/dyn_hash_tool.c
    fuzz/                       # cargo-fuzz harnesses
  docs/
    book/                       # mdBook source for the reference manual
    reference/                  # carry-over of original notes/, docs/, recommendation.md
  test/                         # Python conformance harness (ported from C tree)
  scripts/                      # dev helpers (tc netem, multi-node launchers)
  dist/                         # packaging assets, systemd units
  .github/workflows/            # CI
```

---

## 2. Cross-cutting choices (locked decisions)

* **Async runtime**: `tokio` (multi-thread). The C event loop is an epoll/
  kqueue/evport reactor with `EVENT_READ`/`EVENT_WRITE`/`EVENT_ERR`. Each C
  worker becomes a tokio task graph; we fold the original event-loop code into
  `tokio::select!`-driven per-connection state machines. We do **not** wrap
  raw `epoll`; the behavior is preserved one level up.
* **Transports**:
  * TCP via `tokio::net::TcpStream`/`TcpListener` (default; on-the-wire
    DNODE framing identical to C).
  * QUIC via `quiche` 0.21+. We wrap `quiche::Connection` behind the same
    `Transport` trait used by TCP. One QUIC stream per logical DNODE message
    pipeline (request stream + response stream), or a single bidirectional
    stream multiplexing dmsgs - implementation must match the framing tests in
    Stage 7.
  * Both transports support v4 and v6 listeners (configurable; bind syntax
    accepts `[::1]:8101` and `0.0.0.0:8101`).
* **Crypto**: pure-Rust RustCrypto stack: `aes` + `cbc` for
  AES-128-CBC (the C-compatible peer cipher), `aes-gcm` for the
  authenticated AES-256-GCM peer cipher added in response to the
  2026-07-23 review, `rsa` + `sha1` for RSA OAEP, `rand` for the
  CSPRNG, and the rsa crate's PKCS#1/PKCS#8 PEM helpers. AES-128-CBC
  PKCS#1v1.5 wrapping are required to match `dyn_crypto.c`. Pick one and stick
  with it; record the choice in AGENTS.md once Stage 6 begins.
* **Config**: `serde` + `serde_yaml`. The C parser walks libyaml events and
  fills `conf_pool` field-by-field; we mirror this with `#[serde(rename = ...)]`
  and a `validate()` pass that reproduces every check in
  `conf_validate_pool()`.
* **Hashing**: open-coded under `hashkit/` to match C bit-for-bit. Verified
  with vector tests captured by running the C `dyn_hash_tool` against a fixed
  corpus (Stage 4).
* **Logging**: `tracing` + `tracing-subscriber`. Map LOG_EMERG..LOG_PVERB to
  tracing levels; preserve the original `dyn_log.c` verbosity numbering for
  the `-v` flag.
* **CLI**: `clap` derive. Flags must match C 1:1 (-h, -V, -t, -d, -D, -v, -o,
  -c, -p, -x, -m, -M, -g).
* **Errors**: `thiserror` for typed errors at module boundaries; `anyhow` only
  in the binary and tests.
* **Minimum dependency set** (final list - no growth without an AGENTS.md
  update):
  `tokio`, `tokio-util`, `bytes`, `serde`, `serde_yaml`, `clap`, `tracing`,
  `tracing-subscriber`, `thiserror`, `anyhow`, `crossbeam-channel`,
  `crossbeam-queue` (Stage 2 SPSC ring), `parking_lot`, `ahash`, `quiche`,
  `aes`, `aes-gcm`, `cbc`, `cipher`, `rsa`, `sha1`, `rand`, `rand_core`, `base64`,
  `nix` (signals/daemonize), `socket2` (advanced socket opts), `time`,
  `httparse` (only for the stats REST endpoint - if avoidable, hand-roll).
  Dev-only: `criterion`, `hegeltest` (the property-testing crate the
  user originally referred to as "Hegel";
  https://github.com/hegeldev/hegel-rust),
  `cargo-fuzz`/`libfuzzer-sys`,
  `assert_cmd`, `predicates`, `tempfile`, `clap_mangen`.

---

## 3. Stages

Each stage is a closed unit of work with explicit entry, deliverables, and
exit gates. The "exit gate" is what a self-review (see AGENTS.md) must
confirm before declaring the stage done. Stages 3-12 may run in parallel
where their deps allow (see the dependency notes). Stage numbering reflects
the canonical critical path.

### Stage 0 - Foundation

**Goal**: Empty workspace builds, lints, formats; CI green; Nix flake produces
a reproducible dev shell.

* `Cargo.toml` workspace with the four crates listed above.
* `rust-toolchain.toml` pinning a stable toolchain.
* `flake.nix` providing: rustc/cargo, clippy, rustfmt, cargo-nextest,
  cargo-fuzz, cargo-criterion, mdbook, pkg-config, quiche build deps
  (cmake, perl), iproute2 (`tc`), netcat, redis-server and memcached for
  integration tests, python3 with PyYAML for the legacy harness.
* `.github/workflows/ci.yml`: build, fmt, clippy `-D warnings`, nextest, doc.
* Empty `lib.rs` per crate, README, LICENSE/NOTICE pulled from the C tree
  with attribution updated for Apache 2.0.

**Exit gate**: `nix develop -c cargo build --workspace` and `cargo clippy
--workspace --all-targets -- -D warnings` and `cargo fmt --check` all succeed.

### Stage 1 - Core types & utilities

**Files**: `dyn_types.{c,h}`, `dyn_string.{c,h}`, `dyn_array.{c,h}`,
`dyn_util.{c,h}`, `dyn_setting.{c,h}`, `dyn_log.{c,h}`, `dyn_signal.{c,h}`,
`dyn_queue.h`, `dyn_cbuf.h`, `dyn_ring_queue.{c,h}`, `dyn_dict.{c,h}`,
`dyn_dict_msg_id.{c,h}`, `dyn_rbtree.{c,h}`, `dyn_histogram.{c,h}`,
`dyn_task.{c,h}`.

* Port `rstatus_t` -> `pub type Status = Result<(), DynError>` with a typed
  `DynError` enum mirroring DN_ERROR/DN_EAGAIN/DN_ENOMEM.
* Port `msec_t`, `msgid_t`, `sockinfo` (mirror the union behavior with
  `SocketAddr` plus an explicit `family`).
* `dyn_string` -> primary type is `bytes::Bytes` for zero-copy slices that
  used to be `(uint8_t*, len)` in C, plus a thin compatibility helper for the
  pname/name parsing pattern.
* `dyn_array` -> use `Vec<T>`. The two-arg `array_each_2_t` callbacks become
  closures.
* `dyn_log` -> `tracing` macros plus a `init_log(level, path)` function. SIGHUP
  handler reopens the log file.
* `dyn_signal` -> `nix` signal masks. Reproduce the table in
  `dyn_signal.c::signals[]`.
* `dyn_ring_queue` -> `crossbeam_channel::bounded` for C2G/G2C, with the same
  capacities (`C2G_InQ_SIZE`, `C2G_OutQ_SIZE`).
* `dyn_dict`, `dyn_dict_msg_id` -> `ahash::HashMap<...>` typed wrappers.
* `dyn_rbtree` -> `BTreeMap` for the token ring (the C code only relies on
  ordered iteration and lower-bound lookup).
* `dyn_histogram` -> port the bucket layout (powers of 2, fixed bins) verbatim
  so percentile output matches.
* `dyn_task` -> `tokio::time::interval`-based scheduler with the same task
  registration shape (`task_register(period_ms, callback, ctx)`).

**Tests**: per-module unit tests, plus property tests for histogram
percentiles and ring queue ordering.

**Exit gate**: 100% of public functions in these C files have a Rust
counterpart with at least one unit test exercising the same edge cases the
C code asserts on.

### Stage 2 - I/O substrate

**Files**: `dyn_mbuf.{c,h}`, `dyn_cbuf.h`, `src/event/*` -> reactor.

* `mbuf`:
  * Pooled fixed-size chunks (default 16 KiB, tunable - see C `MBUF_SIZE`).
  * Chain via `VecDeque<Mbuf>` plus an inline cursor for partial writes.
  * Recycle pool using a thread-local + global free list (`parking_lot::Mutex`
    + `Vec`).
  * APIs to mirror `mbuf_get`, `mbuf_put`, `mbuf_split`, `mbuf_copy`,
    `mbuf_insert`, `mbuf_remove`, `mbuf_recv`, `mbuf_send`, `mbuf_full`,
    `mbuf_empty`, `mbuf_length`, `mbuf_size`.
* Reactor: a `Connection` trait bridging `tokio::io::AsyncRead/AsyncWrite` and
  the `quiche` connection. The C `event_add_*`/`event_del_*` functions become
  `select!` arms and watch channels.
* Time helpers: `dn_msec_now()` -> `tokio::time::Instant` + monotonic millis.

**Tests**: split/copy/insert/remove behaviors verified against the C
contract (write a small C harness that emits a JSON trace of operations on
random inputs; replay the same trace through the Rust mbuf and assert byte
equivalence).

**Exit gate**: mbuf semantics match the C contract on a 100k-operation
trace; reactor unit test loops a TCP echo end-to-end.

### Stage 3 - Hashkit & tokens

**Files**: everything under `src/hashkit/`.

* Port each algorithm bit-for-bit:
  * `one_at_a_time`, `md5_signature`, `crc16`, `crc32`, `crc32a`, `fnv1_64`,
    `fnv1a_64`, `fnv1_32`, `fnv1a_32`, `hsieh`, `murmur` (Murmur1 32-bit), and
    `murmur3` (the 128-bit variant from contrib/).
  * `hash_func` dispatch keyed by `hash_type_t`.
* `dyn_token`: 4-byte digest big-int operations
  (`init_token`, `size_dyn_token`, `dyn_token_cmp`, `set_int_dyn_token`).
  Internal storage is `Vec<u32>`; preserve sign and base-`UINT_MAX_PLUS_ONE`
  arithmetic exactly.
* `ketama` and `modula` distributions. Build the continuum of
  `(token, server)` pairs identically (same total points-per-server formula).
* `dyn_random`: ports the seedable PRNG used elsewhere; replace the libc
  rand-backed seeding with `nix::time::clock_gettime` + the same mixing.

**Conformance test**: write a corpus of 10k keys; run the C `dyn_hash_tool`
on each (`dyn_hash_tool -k <key> -h <algo>` for every algo) to capture
expected outputs; compile that corpus into `tests/hashkit_vectors.rs` and
require exact equality.

**Exit gate**: every algorithm + every distribution + token arithmetic
matches the captured C vectors.

Depends on Stage 1 only. **Can run in parallel with Stages 2, 4, 5.**

### Stage 4 - Configuration

**Files**: `dyn_conf.{c,h}`, `dyn_setting.c` (already in Stage 1).

* Define a `Config` struct mirroring `conf_pool` exactly, with `Option<T>`
  for unset values to reproduce `CONF_UNSET_NUM`/`CONF_UNSET_BOOL`.
* `serde_yaml` parsing keyed on the pool name (top-level map: `pool_name:
  {...}`). Defaults applied in `Config::finalize()` mirror the
  `conf_pool_each_transform()` logic.
* Validation: port every `conf_validate_*` check, including:
  * `secure_server_option` is one of {none, rack, datacenter, all}
  * `data_store` is one of {0, 1}
  * `read_consistency`/`write_consistency` is one of
    {DC_ONE, DC_QUORUM, DC_SAFE_QUORUM, DC_EACH_SAFE_QUORUM}
  * Listen/dyn_listen address parsing (IPv4, IPv6, hostnames; ports 1..65535)
  * Token list / vnode constraints
  * Seed list parsing: `address:port:rack:dc:tokens`
  * Server list parsing: `name:port:weight`
* `--test-conf` (`-t`) flag emits the same human-readable success/failure
  text as the C version's stderr output.

**Tests**: every YAML in `_/dynomite/conf/` round-trips into the Rust
`Config`; produce a snapshot test fixture per file.

**Exit gate**: parsing all bundled configs is identical to the C parser's
field-by-field interpretation (verified by instrumenting the C version once
to dump its parsed struct as JSON, then diffing).

Depends on Stage 1. **Can run in parallel with Stages 2, 3, 5.**

### Stage 5 - Stats & histogram

**Files**: `dyn_stats.{c,h}`, `dyn_histogram.{c,h}`, REST endpoint inside
`dyn_stats.c`.

* Port the metric tables: `STATS_POOL_CODEC`, `STATS_SERVER_CODEC`,
  `STATS_POOL_SHADOW_CODEC` (counters/gauges).
* Aggregation thread: an `interval` task that snapshots and rolls.
* HTTP endpoint on `stats_listen`: minimal hand-rolled HTTP/1.1 (or `httparse`
  + writer) returning the same JSON layout. The exact JSON keys are part of
  the public contract; capture the C output and snapshot-test against it.
* `--describe-stats` (`-D`) prints the same description table.

**Exit gate**: snapshot tests prove structural equivalence to the C
JSON output for an empty cluster - the field set, ordering, and value
types match what the reference engine emits, while the byte-level
formatting (whitespace, trailing newlines, comma trim placement) is a
recorded Deviation in `docs/parity.md` because the Rust writer is
allocation-friendly and emits a single compact line.

Depends on Stage 1. **Parallelizable with Stages 2-4.**

### Stage 6 - Crypto

**Files**: `dyn_crypto.{c,h}`.

* Port `crypto_init` (loads PEM RSA private key from `pem_key_file`,
  generates an AES key on first use).
* `aes_encrypt`, `aes_decrypt` (AES-256-CBC, PKCS#7 padding), with the same
  IV layout (16 bytes prepended).
* `dyn_aes_encrypt(_msg)`: pipe through an mbuf chain.
* `base64_encode/decode`.
* `generate_aes_key`: 32 bytes from a CSPRNG.
* RSA wrap/unwrap during the DNODE handshake (CRYPTO_HANDSHAKE message).

Use the pure-Rust RustCrypto stack (`aes`, `cbc`, `rsa`, `sha1`,
`rand`). It avoids the linker-symbol clash with quiche's bundled
BoringSSL that any C-backed OpenSSL binding triggers, keeps the
workspace `forbid(unsafe_code)`-clean, and stays portable across
the Nix dev shell and stock Linux/macOS dev hosts.

**Tests**:
* Round-trip property tests (encrypt -> decrypt = identity for arbitrary
  payload).
* Cross-impl: encrypt a fixed payload with the C code (a one-time fixture),
  decrypt with Rust, and vice versa, locking in compatibility for any
  inter-version handshake.

**Exit gate**: cross-impl round-trips succeed on a fixed corpus and the
DNODE handshake message described in `dyn_dnode_msg.c::dmsg_to_string` parses
identically.

Depends on Stages 1, 2.

### Stage 7 - Message model + DNODE codec

**Files**: `dyn_message.{c,h}`, `dyn_response_mgr.{c,h}`,
`dyn_dnode_msg.{c,h}`, `dyn_request.c`, `dyn_response.c`,
`dyn_dnode_request.c`.

* `Msg`: id, type (`MsgType` enum mirroring `MSG_TYPE_CODEC` exactly - all
  REQ_MC_*/REQ_REDIS_*/RSP_*/RSP_REDIS_* variants), mbuf chain, parser state,
  fragment list, owner connection ref, response list.
* `MsgQueue` (replaces `msg_tqh`): a `VecDeque<MsgRef>`.
* DNODE wire codec: parse the dyn_parse_state_t state machine - DYN_START,
  DYN_MAGIC_STRING (`2014` literal), DYN_MSG_ID, DYN_TYPE_ID, DYN_BIT_FIELD,
  DYN_VERSION, DYN_SAME_DC, DYN_STAR, DYN_DATA_LEN, DYN_DATA,
  DYN_SPACES_BEFORE_PAYLOAD_LEN, DYN_PAYLOAD_LEN, DYN_CRLF_BEFORE_DONE,
  DYN_DONE, DYN_POST_DONE - byte for byte. Serialization via `dmsg_write`.
* `dmsg_type_t`: DMSG_REQ, DMSG_REQ_FORWARD, DMSG_RES, CRYPTO_HANDSHAKE,
  GOSSIP_SYN/REPLY/ACK/DIGEST_*, GOSSIP_SHUTDOWN.
* Response manager: per-DC quorum tracking. Re-implement
  `rspmgr_check_is_done`, `rspmgr_get_response`, `rspmgr_submit_response`,
  `msg_local_one_rsp_handler`, `init_response_mgr_all_dcs`, including the
  checksum-based read repair selection.
* Read repair plumbing (`is_read_repairs_enabled`).

**Tests**:
* Replay the literal corpus from `dyn_test.c` (the multi-message blob with
  embedded magic markers and varying payload sizes) and assert the parser
  states + emitted messages are identical.
* Property tests: random valid dmsg encode/decode round-trip.

**Exit gate**: Rust parser passes 100% of the `dyn_test.c` test inputs.

Depends on Stages 2, 6.

### Stage 8 - Protocol parsers (Redis, Memcached)

**Files**: `proto/dyn_redis.c`, `proto/dyn_redis_repair.c`,
`proto/dyn_memcache.c`, `proto/dyn_proto.h`, `proto/dyn_proto_repair.h`.

This is the largest single port (~6.5k LOC of state-machine code).

* `redis_parse_req`/`redis_parse_rsp` are huge switch-driven byte parsers.
  Port them as one Rust function each, preserving every state name, every
  case, every error path. Use `#[derive(Copy, Clone, Eq, PartialEq, Debug)]`
  enums for the parse states, and a `Parser` struct to hold cursor state.
* The full Redis command catalog (`MSG_TYPE_CODEC` REQ_REDIS_* entries) must
  classify identically; the lookup table is keyed on the (case-insensitive)
  command name.
* `redis_fragment` for multi-key commands (MGET, MSET, DEL, EXISTS, ...).
* `redis_pre_coalesce`/`redis_post_coalesce` for response merging.
* `redis_repair`: rewrite_query, rewrite_query_with_timestamp_md,
  make_repair_query, clear_repair_md_for_key, reconcile_responses.
  Mirror the per-command handling tables.
* `memcache_parse_req/rsp`, `memcache_fragment`, `memcache_pre/post_coalesce`,
  multikey detection, and the same repair stubs (memcache repairs are
  no-ops in the C source - mirror that exactly).

**Tests**:
* Hand-curated table of (input bytes, expected `MsgType`, expected key list,
  expected response form). Aim for >95% command coverage including malformed
  inputs.
* Property tests: arbitrary RESP and memcached ASCII inputs - parser must not
  panic or read out of bounds.
* Differential test against `redis-cli`/`memcached` with `cargo test
  --features integration` running real servers (Stage 0 brings these in via
  Nix).

**Exit gate**: the Rust parsers reject and accept the same inputs as the C
parsers on a curated 1k-input corpus.

Depends on Stages 2, 7. **Memcached and Redis subparts can be done in
parallel; redis_repair depends on the redis core port.**

### Stage 9 - Connection FSMs and transports

**Files**: `dyn_connection*.{c,h}`, `dyn_proxy.{c,h}`,
`dyn_dnode_proxy.{c,h}`, `dyn_dnode_client.{c,h}`, `dyn_client.{c,h}`,
`dyn_connection_pool.{c,h}`.

* Define the six conn roles (PROXY, CLIENT, SERVER, DNODE_PEER_PROXY,
  DNODE_PEER_CLIENT, DNODE_PEER_SERVER) as variants of an enum `ConnRole`.
* `Conn` carries a transport (`tokio::TcpStream` or `quic::Stream`), recv
  mbuf chain, send mbuf chain, recv-msg-queue, send-msg-queue, parser state,
  owner ref, role-specific ops vtable (recv/send/recv_next/recv_done/
  send_next/send_done/close/active/ref/unref/enqueue/dequeue/response_handler).
* Listener tasks: PROXY accept loop, DNODE_PEER_PROXY accept loop. Each
  spawns per-conn tasks.
* Connection pool: per-`datastore` and per-`peer` outbound pools with the
  same `max_connections` / preconnect / reconnect-with-backoff behavior.
* Auto-eject: on consecutive failures >= `server_failure_limit`, mark server
  unavailable until `server_retry_timeout_ms`.
* QUIC: a `Transport` trait abstracting bytes-in/bytes-out + close. The QUIC
  impl uses `quiche` with one bidirectional stream per peer-conn (mirrors a
  single TCP stream). Configurable per-pool: `transport: tcp | quic`.
* IPv4 + IPv6: parsing accepts both `host:port` and `[host]:port`.

**Tests**:
* Loopback echo for each role using both transports.
* Auto-eject simulated by injecting connect failures in the test transport.
* QUIC end-to-end test using the bundled `aws-lc-rs` rustls in `quiche`'s
  default config; a self-signed cert harness in tests/.

**Exit gate**: each role's FSM behaves identically to the C version on a
hand-curated trace of (event, expected action) pairs derived by reading
`dyn_connection.c::core_recv`/`core_send` etc.

Depends on Stages 2, 7. Memcache vs Redis parser availability is a soft
dep - the conn FSM is parser-agnostic.

### Stage 10 - Cluster: pools, peers, gossip, snitch, vnode, dispatch

**Files**: `dyn_server.{c,h}`, `dyn_dnode_peer.{c,h}`, `dyn_dnode_proxy.c`
(forwarding), `dyn_gossip.{c,h}`, `dyn_node_snitch.{c,h}`, `dyn_vnode.{c,h}`,
`seedsprovider/*`.

* `ServerPool`: owner of datastore + dnode listeners, datacenters, racks,
  peers, hash ring, consistency knobs.
* `Datacenter`/`Rack`/`Peer` structs.
* Token ring: red-black-tree-equivalent ordered map `BTreeMap<DynToken,
  PeerRef>`; lookup via `range(..key).next_back()` to mirror C `rbtree_lower`.
* Consistency engine:
  * DC_ONE - send to one replica (the local rack preferred via snitch).
  * DC_QUORUM - send to N, wait for majority (matches `quorum_responses`).
  * DC_SAFE_QUORUM - DC_QUORUM + checksum compare; on mismatch trigger
    repair.
  * DC_EACH_SAFE_QUORUM - per-DC safe quorum.
* Read repair plumbing wired to `proto::redis::repair` and `proto::memcache`.
* Auto-eject (already in Stage 9) integrated with pool-level metrics.
* Snitch: DC/rack proximity ordering for replica selection.
* Vnode: per-peer multi-token configuration. The C code declares it not yet
  implemented in some branches (single-token assumption); we faithfully port
  the data structures and any code paths that already exist.
* Gossip: dedicated tokio task that periodically picks a peer, sends GOSSIP_
  SYN, processes ACK/SHUTDOWN, updates the dc/rack/node tables, handles
  SHUTDOWN by removing nodes. Seeds providers:
  * `simple_provider` - the conf-loaded list.
  * `florida_provider` - HTTP GET to a Florida endpoint, parse the seed list.
  * `dns_provider` - DNS A/AAAA resolution + SRV optional.
* `dyn_setting.c` `msgs_per_sec` rate limiter applied to inter-DC traffic.

**Tests**:
* 3-node single-DC cluster spun up in-process; assert key->peer routing
  matches the C ring math for a fixed corpus of 10k keys.
* Multi-DC: 2 DCs, 2 racks each, 2 peers each. Each consistency level
  produces the expected request fan-out.
* Gossip simulation: scripted seed list, assert convergence within N rounds.

**Exit gate**: a 6-node cluster (2 DCs * 3 racks) services a fixed Redis
workload; outputs match a single-node Redis run (no data loss).

Depends on Stages 1-9.

### Stage 11 - Entropy reconciliation

**Files**: `entropy/dyn_entropy*.c`.

* TCP listener + worker thread that ships a snapshot of local Redis state
  to/from a peer using the entropy protocol.
* Send (`dyn_entropy_snd.c`): connect to receiver, AES-encrypt local snapshot
  (RDB or RESP stream), transmit.
* Receive (`dyn_entropy_rcv.c`): accept, decrypt, replay onto local datastore.
* Util: key/IV file loaders (`recon_key.pem`, `recon_iv.pem` from
  `_/dynomite/conf/`).

**Tests**: in-process roundtrip with a mocked datastore; verify the same
RDB-like blob comes out the other side after decryption.

**Exit gate**: send/receive roundtrip across two `dynomited` instances
preserves a pre-loaded keyset.

Depends on Stages 6, 9.

### Stage 12 - Server binary, CLI, daemonization, manpage

**Files**: `dynomite.c`.

* `main` -> Rust `main()` calling `dynomite::Server::new(config).run()`.
* `clap`-derived CLI matching every flag: -h, -V, -t, -d, -D, -v, -o, -c,
  -p, -x, -m, -M, -g (gossip toggle).
* Daemonize via `nix::unistd::{fork, setsid}` + redirect to `-o` log path.
* PID file write/lock.
* `--describe-stats` calls into `stats::describe()` and prints the same text.
* `clap_mangen` produces `dynomited.8` matching the original wording.

**Tests**: `assert_cmd` smoke tests for every flag, including `-t` against
all bundled configs.

**Exit gate**: `dynomited -t -c conf/dynomite.yml` emits the same
success/failure text as the C binary on every config under
`_/dynomite/conf/`.

Depends on all prior stages.

### Stage 13 - Embedding API (`dynomite::embed`)

**Goal**: First-class library mode. An embedding program can construct,
start, drive, and stop a Dynomite engine in-process and observe everything.

* `Server::builder()` -> `ServerBuilder` with fluent setters that mirror the
  YAML schema 1:1, plus typed setters for things you would not put in YAML
  (custom transports, custom seeds providers, custom datastore connectors).
* `Server::start()` -> returns a `ServerHandle` that exposes:
  * `shutdown()` -> graceful stop matching the C SIGTERM path.
  * `reload(config)` -> matches SIGHUP behavior.
  * `stats()` -> snapshot struct (typed).
  * `subscribe_events()` -> async stream of `ServerEvent` (peer up/down,
    config reload, gossip round, auto-eject, repair triggered, connection
    accepted/closed).
  * `describe_stats()` -> machine-readable.
  * `inject_request(...)` for tests/embedding (skip the network, hand a
    parsed `Msg` directly to `cluster::dispatch`).
  * `peers()`, `datacenters()`, `ring()` for inspection.
* Pluggable hooks via traits:
  * `trait Datastore` - implement to embed Dynomite in front of any storage
    (the default impls drive Redis or Memcached over TCP; library users can
    plug their own in-process store).
  * `trait SeedsProvider` - for custom service-discovery integrations.
  * `trait Transport` - for custom network transports beyond TCP/QUIC.
  * `trait CryptoProvider` - HSM/KMS integration.
  * `trait MetricsSink` - export stats elsewhere (Prometheus, OTel, ...).

**Documentation**: `docs/book/src/embedding.md` walks every trait, every
hook, and includes runnable examples in `crates/dynomite/examples/`.

**Tests**: a `tests/embed_*.rs` suite that:
* Builds an in-process 3-node cluster using `inject_request` only.
* Plugs a `MockDatastore` and asserts the dispatcher's request fan-out.
* Plugs a `MockSeedsProvider` and asserts gossip convergence.
* Drives a real cluster via the public API (no `dynomited` binary
  involved).

**Exit gate**: every public API has rustdoc with at least one runnable
doctest; `cargo test --doc` passes.

### Stage 14 - Conformance & regression suites

* Port `_/dynomite/test/*.py` to call `dynomited` instead of `dynomite`.
* Add a Rust integration test crate that:
  * Spins up clusters with TCP and QUIC transports.
  * Runs the original Python harness via `pytest` from a `cargo test`
    wrapper using `assert_cmd`.
  * Captures pass/fail per scenario into JUnit XML for CI.
* Differential test rig: same workload against C `dynomite` (built once,
  cached as a CI artifact) and Rust `dynomited`; assert response equivalence
  on a Redis trace.

### Stage 15 - Fuzz, property, bench, coverage gate

* `cargo-fuzz` targets:
  * `proto_redis_parse`
  * `proto_memcache_parse`
  * `dnode_parse`
  * `conf_parse`
  * `crypto_aes_decrypt`
* Property tests (`hegeltest`, the user's specified property-testing
  framework; see https://github.com/hegeldev/hegel-rust):
  * Hash + token arithmetic invariants.
  * Quorum decision table (given N/M responses, decide is_done correctly).
  * mbuf split/merge identity.
  * Dispatch routing: same key always routes to same primary (under fixed
    ring).
  * State-machine tests (`#[hegel::state_machine]`) for the `MinStack`-style
    invariants on the gossip and response-manager state.
* Benches (`criterion`):
  * Micro (per-component, individually meaningful):
    - Parser throughput per protocol per command class (Redis SET/GET/MGET/
      MSET/HSET/ZADD/EVAL; Memcache get/set/cas) on representative
      payload sizes (16/64/256/1024/8192 bytes).
    - mbuf alloc/free hot path; pool recycling; split/merge.
    - Hash funcs: each algorithm on 16/64/256/1024 byte keys.
    - Token arithmetic: `set_int_dyn_token`, `dyn_token_cmp`, ring lookup.
    - DNODE codec: encode + parse over message size sweep.
    - Crypto: AES-128-CBC over 16/64/256/1024/4096 byte payloads;
      RSA OAEP wrap/unwrap; PEM key load.
    - ResponseMgr quorum decision table.
  * Macro (end-to-end, with tc/netem injected impairments):
    - 3-node TCP cluster get/set throughput vs the C reference (when a
      static-lib build of dynomite is available; otherwise vs raw
      Redis as the upper bound).
    - 3-node QUIC cluster get/set throughput.
    - 6-node multi-DC throughput across consistency levels.
    - Gossip round latency at 100 / 500 / 1000 nodes.
    - Quorum read tail latency under simulated packet loss
      (`tc qdisc add dev lo root netem loss 1%/5%/10%`).
    - Cold-start time to first request through.
  Each criterion bench has a baseline checked into
  `crates/dynomite/benches/baseline/`; CI fails on >10% regression
  (configurable per-bench).
* Coverage gate: `cargo llvm-cov --workspace --all-features` must report
  >= 95% line coverage AND >= 95% branch coverage AND >= 95% function
  coverage. The current Stage 11 baseline is 79.62% line / 78.38% branch;
  Stage 14's conformance harness pushes the network and FSM modules above
  90%, and Stage 15 closes the remaining gap. Anything below 95% must be
  documented as an explicit Deviation in `docs/parity.md` with a
  technical justification (e.g. listener accept loops only exercised by
  the chaos test in Stage 16).

### Stage 16 - Docs, packaging, release, chaos verification

* `mdBook` reference under `docs/book/`:
  * Architecture overview (carry over `notes/` and the C wiki content
    under `docs/reference/`).
  * Configuration reference (auto-generated from the `serde` schema +
    hand-written explanations matching the README block in the C tree).
  * DNODE wire protocol (port `docs/dyn_protocol.txt`).
  * Embedding guide (Stage 13 doc).
  * Operations runbook (port `notes/recommendation.md`).
* `dist/` packaging: systemd unit (port from `_/dynomite/init/`), Docker
  image (port from `_/dynomite/docker/`), .deb/.rpm scripts.
* `CHANGELOG.md`.
* Chaos / punishment / stress test: a 1-hour test run that
  simulates the full set of failures distributed systems must
  survive in production. The test lives at
  `crates/dynomite/tests/stage_16_chaos.rs`, runs under
  `cargo nextest run --test stage_16_chaos --release` and asserts
  the cluster keeps serving requests through the entire hour with
  zero data loss within quorum constraints. Specifically:
  * Cluster bootstrap: 3-node single-DC. Grow to 9 nodes across 3
    DCs over the first 10 minutes, then shrink back to 3 over the
    last 10. Mid-run nodes join and leave randomly; the test
    asserts no client request observes a permanent error.
  * Workload mix:
    - 50% read traffic, 50% write traffic (steady state).
    - Spike windows shifting the mix to 90% writes for 60 seconds
      every 10 minutes.
    - 3 concurrent client populations with different latency-
      sensitivity profiles (interactive, batch, background).
  * Concurrent failure injectors driven by
    `scripts/netem/`:
    - `partition_dc.sh`: 3 random 30-second cross-DC partitions.
    - `slow_peer.sh`: rotating 200ms one-way delay on one peer
      per minute.
    - `flap.sh`: connectivity flaps at 1s intervals against one
      randomly-chosen peer per 5-minute window.
    - `gc_pause.sh`: SIGSTOP/SIGCONT on one peer for 5 seconds
      every 7 minutes.
    - `clock_skew.sh`: faketime skew of 0..30s applied to one
      peer at the 30-minute mark.
    - Random peer kills (SIGKILL) every 12 minutes; the supervisor
      restarts the killed peer within 5 seconds.
  * State-transition coverage: the test exercises every cluster
    state transition from `dyn_core.h::dyn_state_t` (INIT,
    STANDBY, WRITES_ONLY, RESUMING, NORMAL, JOINING, DOWN,
    RESET, UNKNOWN) and asserts that every legal transition path
    is observed at least once over the run.
  * Invariants asserted continuously:
    - No request ever returns the wrong key's value.
    - Quorum-acknowledged writes survive any single-node failure.
    - DC_EACH_SAFE_QUORUM writes survive a full DC partition.
    - Auto-eject reinstates within `server_retry_timeout_ms`
      after the failure clears.
    - Gossip converges within 60 seconds of any topology change.
    - No tokio task is detached without explicit `into_detached()`.
  * Cleanup: when the test finishes, all spawned dynomited
    instances are killed, all Redis backends are torn down,
    netem qdiscs are removed, faketime is unset. The test must
    leave the host in the same state it found it in. Asserted
    by a post-test sweep that compares `tc qdisc show dev lo`
    output against a captured baseline.
  * Test report: `target/chaos/<run-id>/report.md` is produced
    summarising the run (start/end times, failure injections
    applied, peer state-transition counts, per-window throughput
    + tail latency, any invariant violations). The report is the
    artifact the release tag references.
* CI dual-platform integration:
  * `.github/workflows/ci.yml` for GitHub Actions (already in
    place; extend to run the Stage 14 conformance suite + the
    Stage 15 fuzz smoke + the Stage 15 coverage gate).
  * `.forgejo/workflows/ci.yml` for Codeberg Forgejo Actions
    (mirror of the GitHub workflow; same gates; the same
    `scripts/check.sh` is the single source of truth so both
    runners exercise identical commands).
  * Both pipelines must pass before tag.
* Author normalisation: rewrite history so every commit on
  `main` is authored by `Greg Burd <greg@burd.me>`. Done with
  `git filter-repo --commit-callback` on the entire history
  immediately before the tag. Verify via
  `git log --format='%an <%ae>' | sort -u` returning exactly
  one line.
* Release artifacts:
  * `CHANGELOG.md` summarising every stage.
  * `git tag -a v0.1.0 -s -m "..." ` (signed) on the
    post-rewrite HEAD.
  * `git push origin main && git push origin v0.1.0`.
  * Forgejo mirror push.
* Final release checklist (see AGENTS.md).

---

## 4. Stage dependency graph

```
0 -> 1 -> 2 -> 7 -> 8 -> 9 -> 10 -> 11 -> 12 -> 13 -> 14 -> 15 -> 16
          \-> 3 (parallel, joins at 10)
          \-> 4 (parallel, joins at 12)
          \-> 5 (parallel, joins at 12)
          \-> 6 -> 7
```

---

## 5. Definition of done (per stage and overall)

A stage is done iff all of:

1. Every C function listed for the stage has a Rust counterpart (or an
   explicit "deliberately unused" entry in `docs/parity.md` justifying its
   omission - e.g. `dn_array_each_2_t` callbacks subsumed by closures).
2. `cargo build --workspace --all-targets`,
   `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo fmt --check`, and `cargo nextest run --workspace` all pass.
3. The stage's own tests + every prior stage's tests pass.
4. Self-review (see AGENTS.md) by an independent agent confirms parity by
   reading the C and the Rust side-by-side and checking the parity matrix.
5. `docs/parity.md` is updated to reflect new coverage.

The overall port is done iff:

* The Stage 14 conformance suite passes for both transports.
* The Stage 15 fuzzers run for >=1 hour each with no findings.
* The Stage 16 docs are complete and `mdbook build` is clean.
* `dynomited` and the C `dynomite` produce equivalent responses on the
  differential workload (Stage 14).

---

## 6. Parity tracking

A living `docs/parity.md` document maps every C symbol (function, type,
macro, global) to its Rust home or to a justified omission. Every PR that
touches Rust code must update `docs/parity.md`. CI fails if the C code adds
new symbols (it should not) or if the parity table references symbols that
no longer exist on either side.
