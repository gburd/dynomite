# Deviation Ledger

Single source of truth for every place the Rust engine intentionally
behaves differently from C Dynomite (`~/src/dynomite` at `2046b05`).

Rules:

* Every entry has a `Status` of `AUTHORIZED`, `PROPOSED`, or `REVERTED`.
* `AUTHORIZED` requires an `Authorized-by:` line written by the owner (or
  recorded verbatim from the owner's instruction, with the date).
* An agent may add `PROPOSED` entries with evidence. It may not mark one
  `AUTHORIZED`. Code relying on a `PROPOSED` entry must not merge.
* `scripts/check_parity.sh` fails on any ledger row with
  `status=deviation` that does not reference an `AUTHORIZED` entry here.

Evidence an entry needs: the C behaviour (file:line), the Rust behaviour,
the axis it changes, and a measurement or argument that the Rust behaviour
is better on that axis without being worse on any other.

---

## 0. Scope principles (owner rulings, 2026-11-02)

These bound every entry below. They are not deviations; they define what
"functionally identical" does and does not require.

### P1: No C interoperability or migration

* Rust Dynomite nodes never talk to C Dynomite nodes. Mixed C/Rust
  clusters, C-compatible wire formats on the peer plane, gossip, and
  entropy, and migration of a running C cluster or its data are all
  non-goals.
* Consequence: where C behaviour existed only to fix a wire or file
  format, the Rust engine chooses the best format on its merits. The C
  *logic* (what is computed, when, with what guarantees) is still
  transliterated; the *encoding* is free.
* Consequence: the differential suite compares C and Rust clusters side by
  side through the client-facing protocols (RESP, memcache, admin HTTP),
  never by mixing nodes.
* Authorized-by: Greg Burd, 2026-11-02 ("there is no need/goal to have C
  Dynomite interoperate with Rust Dynomite ... nodes from Rust don't need to
  talk to nodes from C dynomite").

### P2: Configuration is Rust-native

* No requirement to read C `dynomite.yml` files or keep its key names,
  nesting, or value encodings. The Rust configuration is designed as a
  Rust-native schema (section C, DEV-B11).
* What is preserved: every C configuration *capability* (every knob the C
  engine exposes has a Rust equivalent with the same effect and default),
  and validation that rejects invalid configurations with a precise error.
* Consequence for testing: the differential suite carries a C config and
  its equivalent Rust config per scenario, and asserts identical behaviour.
* Authorized-by: Greg Burd, 2026-11-02 ("no need to be able to migrate from
  a C cluster to a Rust cluster or to have the config shaped the same way.
  Transform the config into something more Rust native if there is a better
  pattern").

### P3: No embedded or delegated Lua

* Dynomite does not include a Lua interpreter and does not generate Lua for
  the datastore to run. Wherever C Dynomite builds a Lua script (today: the
  read-repair `EVAL` scripts in `proto/dyn_proto_repair.h`), the script's
  logic is transliterated into Rust and executed by the engine.
* Client-issued `EVAL` / `EVALSHA` / `SCRIPT *` commands are still proxied to
  the datastore as C does; P3 is about scripts Dynomite itself authors.
* Authorized-by: Greg Burd, 2026-11-02 ("make it an explicit choice that
  we're not going to be including a Lua interpreter in Dynomite ... we'll
  transliterate/transform from Lua into Rust").

## A. Substrate (structural)

### DEV-S1: tokio replaces the C event loop

* C: `src/event/dyn_epoll.c`, `dyn_kqueue.c`, `dyn_evport.c`, the event
  base in `dyn_core.c` (`core_loop`, `core_recv`, `core_send`,
  `core_timeout`, `core_close`).
* Rust: tokio multi-thread runtime; per-connection tasks; `tokio::time`
  for timeouts.
* Status: AUTHORIZED
* Authorized-by: Greg Burd, 2026-10 ("Switching the C event loop ... with
  tokio are approved deviations").
* Obligation: every C event-loop function gets a `replaced` ledger row and
  a behavioural test of its observable effect (timeout firing, close
  ordering, EOF handling).

### DEV-S2: Rust collections replace `dyn_dict`, `dyn_rbtree`, `dyn_array`

* Status: AUTHORIZED
* Authorized-by: Greg Burd, 2026-10.
* Obligation: where C exposes iteration order or tie-breaking (token ring
  lookup via rbtree, dict iteration in stats output), the Rust replacement
  reproduces it and a test pins it.

### DEV-S3: ownership replaces connection ref-counting

* C: `conn_get` / `conn_put`, `server_ref` / `server_unref`,
  `dnode_peer_ref` / `unref`.
* Status: AUTHORIZED
* Authorized-by: Greg Burd, 2026-10.
* Obligation: connection lifecycle events (close-on-error, pool return,
  eject) match C timing and order; tested.

## B. Additions (must be strictly additive)

Each is AUTHORIZED by Greg Burd, 2026-10 ("noted as approved
deviations/additions/improvements"). Shared obligation: with the addition
left unconfigured the binary behaves as the C binary; the differential
suite runs each scenario with the addition off to prove it.

* DEV-A1: dyniak (Riak-compatible surface on noxu).
* DEV-A2: noxu datastore (`data_store: noxu` / `dyniak`).
* DEV-A3: QUIC transport (client and peer planes).
* DEV-A4: vector and text search (RediSearch `FT.*`).
* DEV-A5: CRDTs.
* DEV-A6: cross-node transactions (XA 2PC, RAMP).
* DEV-A7: hinted handoff.
* DEV-A8: active anti-entropy.
* DEV-A9: random-slicing distribution.
* DEV-A10: embedding API (`dynomite::embed`).

## C. Behavioural deviations under review

All `PROPOSED` until the owner decides. Source: `docs/parity.md`
"Deviations" (agent-recorded before this ledger existed). Each must be
re-evaluated against the C behaviour with evidence before a decision.

| ID | Topic | parity.md heading | Agent recommendation |
|---|---|---|---|
| DEV-B1 | Read repair by vote count, single-key GET only | D2 | DECIDED: see DEV-R1 (C timestamp scheme, Lua logic in Rust) |
| DEV-B2 | AES-256-GCM peer cipher alongside C CBC | D-crypto | DECIDED: see DEV-C1 (keep) |
| DEV-B3 | Entropy PKCS#7 padding vs C caller zero-padding | Stage 11 | DECIDED: see DEV-E1 |
| DEV-B4 | Entropy length-prefixed chunks vs C fixed `cipher_size` | Stage 11 | DECIDED: see DEV-E1 |
| DEV-B5 | Entropy snd/rcv framing unified | Stage 11 | DECIDED: see DEV-E1 |
| DEV-B6 | `BGREWRITEAOF` over RESP instead of `system("redis-cli ...")` | Stage 11 | Keep (removes a shell-out; same effect); needs evidence |
| DEV-B7 | Entropy throughput throttler removed | Stage 11 | DECIDED: see DEV-E2 (restore, configurable) |
| DEV-B8 | `DynErrorCode::BadFormat.message()` stable string vs `strerror(8)` | Stage 7 | Revert to C text unless clients ignore it |
| DEV-B9 | Per-call `Crypter` vs long-lived `EVP_CIPHER_CTX` | Stage 6 | Keep if output identical (substrate-like); oracle test decides |
| DEV-B10 | RSA padding choice | Stage 6 | Must match C exactly (interop); oracle test decides |
| DEV-B11 | Config validation stricter than C (several Stage 4 entries) | Stage 4 | DECIDED: see DEV-K1 (Rust-native config; strict validation is correct) |
| DEV-B12 | Gossip failure detection: phi-accrual / SWIM vs C timestamp liveness | membership | DECIDED: see DEV-F1 |
| DEV-B13 | Snitch rack-distance helpers, florida client on tokio, `is_aws_env` string, response-mgr clamp | Stage 10 | Case by case |
| DEV-B14 | Hinted handoff at the proxy layer | D1 | Covered by DEV-A7 if off by default; verify |
| DEV-B15 | `replication_strategy` walk-N-successors, `chash_keyfun` | D4/D5 | Covered by additions if opt-in; verify |
| DEV-B16 | `dn_coredump_init` (RLIMIT_CORE) omitted | dynomite.c | Revert (C behaviour) unless embed users need opt-out |
| DEV-B17 | Signal set: SIGUSR1/2, SIGTTIN/TTOU, SIGSEGV semantics not reproduced | D-audit | Revert (C behaviour) |

## D. Decided behavioural deviations

### DEV-R1: Read repair is the C timestamp scheme, with the Lua logic in Rust

* C: `proto/dyn_redis_repair.c`, `proto/dyn_proto_repair.h`. A write is
  rewritten into an `EVAL` of a generated script that records the write's
  timestamp in per-datastore sorted sets (`._add-set`, `._rem-set`, plus
  per-key composite sets for field-level commands) and applies the write
  only if it is not older than what is recorded. A read under quorum
  returns each replica's value with its timestamp; the coordinator picks
  the freshest and repairs stale replicas. Thirteen commands are
  repairable (GET SET DEL HGET HSET HMSET HDEL SADD SREM SISMEMBER ZADD
  ZREM ZSCORE). Gate: `read_repairs_enabled && read_consistency > DC_ONE &&
  write_consistency > DC_ONE` (`dyn_core.c:673`).
* Rust (target): the same algorithm, the same thirteen commands, the same
  gate, the same add-set / rem-set bookkeeping and tombstone semantics. Per
  P3 the script bodies are transliterated into Rust: the engine issues the
  primitive datastore commands the script would have run (`ZSCORE`, `ZADD`,
  `ZREM`, the original command) and evaluates the comparisons itself.
* The atomicity the Lua script got for free from Redis must be preserved:
  the per-key read-compare-write runs as one Redis `MULTI`/`EXEC` with a
  `WATCH` on the key and both sets, retried on abort. An oracle test drives
  concurrent writers with interleaved timestamps through the C engine (real
  Lua) and the Rust engine and asserts identical final datastore state and
  identical add/rem-set contents.
* The current vote-count repair (parity.md D2) is removed.
* Status: AUTHORIZED
* Authorized-by: Greg Burd, 2026-11-02 (P3 ruling; transliterate the Lua
  into Rust). The `MULTI`/`WATCH` mechanism is the agent's proposal for
  preserving the script's atomicity and is subject to the oracle test
  above; if it cannot match the Lua outcomes under concurrency it comes
  back to the owner.

### DEV-F1: Failure detection defaults to C gossip timestamp liveness

* C: `dyn_gossip.c` marks a node DOWN when its gossip timestamp is older
  than the configured window (`dyn_gossip.c:205-216`).
* Rust: C timestamp liveness is the default and is transliterated exactly.
  phi-accrual and SWIM + Lifeguard remain as opt-in membership modes
  selected by an explicit configuration value.
* Status: AUTHORIZED
* Authorized-by: Greg Burd, 2026-11-02 ("I agree with that failure
  detection recommendation").

### DEV-C1: AES-256-GCM peer cipher

* C: AES-128-CBC with the key reused as the IV, no authentication tag
  (`dyn_crypto.c`, `EVP_EncryptInit_ex(ctx, cipher, NULL, key, key)`).
* Rust: AES-256-GCM, fresh random 96-bit nonce per message, 128-bit tag.
  Under P1 there is no C peer to interoperate with, so AES-256-GCM is the
  peer-plane default; the C CBC primitive is kept only as the
  byte-pinned oracle test of `dyn_crypto.c`.
* Better because: CBC with a fixed IV is deterministic (identical plaintext
  prefixes leak) and unauthenticated (tampering and wrong keys go
  undetected); GCM is authenticated and non-deterministic, and uses all 32
  key bytes. Throughput is at least as good on AES-NI hardware (performance
  gate verifies).
* Status: AUTHORIZED
* Authorized-by: Greg Burd, 2026-11-02 ("keep").

### DEV-E1: Entropy channel uses length-prefixed AEAD frames

* C behaviour (`entropy/dyn_entropy_util.c`, `dyn_entropy_snd.c`,
  `dyn_entropy_rcv.c`), with defects found while reviewing it:
  * Each chunk is AES-128-CBC with a pre-shared key and a fixed pre-shared
    IV, no authentication. Every chunk reuses the IV.
  * Encryption disables padding (`EVP_CIPHER_CTX_set_padding(ctx, 0)`) but
    decryption leaves PKCS#7 padding on: the two directions disagree, and
    a plaintext chunk that is not a multiple of 16 bytes fails to encrypt.
  * The sender transmits `sizeof(ciphertext)` (the full `cipher_size`
    buffer), not the encrypted length, so the receiver cannot tell a short
    last chunk from padding.
  * `nchunk = ceil(st_size / buffer_size) + 1` applies `ceil` to an integer
    division, so files that are an exact multiple of `buffer_size` produce
    an extra empty chunk.
* Rust (target): each chunk is one AES-256-GCM frame (`DEV-C1` primitive)
  with a 4-byte big-endian length prefix; the header carries a protocol
  version, the total length, and a chunk count, and is itself
  authenticated as associated data of the first frame. A single framing is
  used for send and receive. The C *logic* is kept: AOF snapshot via
  `BGREWRITEAOF`, chunked streaming, the snapshot/reconcile mode switch, the
  receiver's key / old value / new value record shape, and the flush to the
  datastore.
* Better because: authenticated (truncation, reordering and tampering are
  detected), no IV reuse, exact last-chunk handling, no padding mismatch,
  and no off-by-one chunk. P1 removes the only reason to keep the C bytes.
* Status: AUTHORIZED
* Authorized-by: Greg Burd, 2026-11-02 ("With that in mind, make the best
  choice for entropy wire format, padding, and throttle").

### DEV-E2: Entropy send throttle restored and configurable

* C: the sender caps throughput at `THROUGHPUT_THROTTLE` (10,000,000
  bytes/s, comment says 10 Mbps) by sleeping when it runs ahead
  (`dyn_entropy_snd.c:36`, `:241-256`). The Rust port removed it.
* Rust (target): restore the throttle as a token bucket on the async send
  path with the C default of 10,000,000 bytes/s, configurable, `0` meaning
  unlimited. The C algorithm resets its window only after a sleep, which
  lets bursts exceed the cap between sleeps; a token bucket enforces the
  same average rate without that burst.
* Better because: it keeps the C protection (a snapshot stream must not
  saturate the node's NIC or starve client traffic) and makes the cap
  tunable per deployment.
* Status: AUTHORIZED
* Authorized-by: Greg Burd, 2026-11-02 (same ruling as DEV-E1).

### DEV-K1: Rust-native configuration schema

* C: a flat YAML map under a pool name with C-specific value encodings
  (`dyn_conf.c` `conf_commands`; e.g. `secure_server_option: datacenter`,
  `servers: - host:port:weight`, `tokens: '...'`, numeric `data_store`).
* Rust (target): a typed, nested, `serde`-derived schema designed for Rust
  (sections for node identity, client listener, datastore, peer plane,
  gossip, consistency, crypto, stats, entropy), human-readable durations and
  sizes, enums instead of magic numbers, `deny_unknown_fields`, and strict
  validation with precise errors. File format: TOML (Rust ecosystem
  standard, already a workspace dependency via `dyniak-bench`, and avoids
  the unmaintained `serde_yaml`).
* Kept from C: every capability and its default value and effect. The
  ledger row for each `conf_commands` entry names the Rust field that
  carries it, and the differential suite runs each scenario with a C YAML
  and the equivalent Rust TOML.
* The Stage-4 "stricter than C" entries (DEV-B11) are subsumed: strict
  validation of a Rust-native schema is the intended behaviour.
* The schema design is presented to the owner before it lands (it is the
  most user-visible surface of the binary).
* Status: AUTHORIZED (direction); schema design PENDING owner review.
* Authorized-by: Greg Burd, 2026-11-02 ("Transform the config into
  something more Rust native if there is a better pattern").

## F. Findings awaiting an owner decision

Found by the C oracle (`crates/c-oracle`). The C behaviour is reproduced
exactly in the oracle tests so the finding cannot drift; the Rust side is
held at its current behaviour until the owner decides.

### DEV-H1: C `murmur3` is a constant hash

* C: `src/hashkit/dyn_murmur3.c` sizes the token to 4 words and returns
  without hashing; the `MurmurHash3_x86_128(...)` call has been commented
  out since 2013-12-10 (upstream `9eb54c1`). Every key yields the zero
  token, so with `hash: murmur3` every key routes to the same node.
  `hash: murmur` (MurmurHash2) is the C default and is unaffected.
* Rust, before this decision: real `MurmurHash3_x86_128`, as a 4-word
  token. That was also broken: node tokens are one word, and
  `cmp_dyn_token` compares length before magnitude, so every 4-word key
  token sorted above every node and routed to the first node. Measured on
  a 3-node `dynomited` cluster: 300 of 300 keys on one node.
* Decision: `hash: murmur3` is `MurmurHash3_x86_32` with the C seed
  `0xc0a1e5ce`, emitted as a one-word ring token like every other hash.
  The owner asked for the real MurmurHash3 unless a better hash is known
  for this purpose. For 32-bit ring placement, MurmurHash3_x86_32 has full
  avalanche and passes SMHasher; faster modern hashes (xxHash3, wyhash)
  buy throughput, not placement quality, at key lengths where hashing is
  a negligible share of a proxied request. It is also the hash the
  Netflix `dyno` client's `Murmur3HashPartitioner` computes.
  `MurmurHash3_x86_128` stays available as `hashkit::murmur3_x86_128` for
  internal wide fingerprints (HyperLogLog), unchanged.
* Pinned by: `c-oracle/tests/hashkit_oracle.rs::c_murmur3_is_constant_zero`
  (C side) and `::murmur3_ring_token_is_c_contrib_x86_32` (Rust equals the
  C tree's own contrib `MurmurHash3_x86_32`), plus
  `dynomite/tests/regression_hash_placement.rs`.
* Status: AUTHORIZED (owner, 2026-11-02)

### DEV-H2: C `one_at_a_time` sign-extends bytes >= 0x80

* C: `src/hashkit/dyn_one_at_a_time.c` reads the key through
  `const char *` and widens each byte with `(uint32_t)*ptr`. Where `char`
  is signed (x86-64 and the common Linux targets the C is built for), bytes
  0x80..0xff are sign-extended to 0xffffff80..0xffffffff before the add.
  On a target with unsigned `char` (Linux aarch64) the C result differs, so
  the C hash is not even portable across C builds.
* Rust: reads `u8`, which is the unsigned (and Jenkins-reference) result.
* ASCII-only keys (every byte < 0x80) agree; pinned by
  `one_at_a_time_matches_c_on_ascii_keys`.
* Options: (a) transliterate the x86-64 C result (sign extension) for byte
  parity with the C reference; (b) keep unsigned, matching the published
  algorithm and the C on unsigned-`char` targets.
* Agent recommendation: (b), given P1 (no C cluster ever shares a ring with
  a Rust one, so no key placement must match a running C node).
* Decision: read bytes unsigned.
* Pinned by: `c-oracle/tests/hashkit_oracle.rs::one_at_a_time_matches_c_on_ascii_keys`
  (exact C agreement below 0x80) and `::one_at_a_time_reads_bytes_unsigned`.
* Status: AUTHORIZED (owner, 2026-11-02)

### DEV-H3: ketama, modula, and random distributions are dead code in C

* C: `src/hashkit/dyn_ketama.c`, `dyn_modula.c`, and `dyn_random.c` exist
  but are not in `libhashkit_a_SOURCES` (`src/hashkit/Makefile.am`), so they
  are not compiled into `dynomite`. The `distribution:` config key is
  accepted and ignored (`conf_set_deprecated`, `src/dyn_conf.c:1091`); the
  C always uses vnode distribution (`CONF_DEFAULT_DIST DIST_VNODE`).
* Consequence for the ledger: their symbols are `dead`, not `todo`. The
  Rust engine's random-slicing distribution is the authorised addition
  DEV-A9 and is unaffected.
* This is a ledger classification, not a behaviour change; no decision is
  needed unless the owner wants ketama/modula/random as additions.
* Status: RECORDED

### DEV-H4: multiple tokens per node crash the C engine

* C: the README states one token per node ("Currently, we don't support
  vnode yet so this only works with one token"; `README.md:70`). The code
  accepts a comma list anyway: `derive_tokens` (`dyn_token.c:185`) parses
  every component, and `vnode_update` (`dyn_vnode.c:91-110`) then calls
  `array_push(&rack->continuums)` once but writes `token_cnt` entries via
  `array_get(orig_cnt + j)`. With `tokens: '101,2147483648,3000000000'`:
  * the debug build aborts on `assert 'idx < a->nelem' failed`
    (`dyn_array.c:135`) in `vnode_update`;
  * the release build (`--enable-debug=no`, asserts compiled out) writes
    past the continuum buffer and dies with SIGSEGV moments later in
    `gossip_pool_init` (heap corruption).
  Reproduced 2026-11-02 against both builds of `2046b05`.
* Rust: `Rack::add_peer_tokens` (`cluster/datacenter.rs`) pushes one
  continuum entry per token, so multiple tokens per node work and every
  token routes.
* Options: (a) transliterate the C contract: accept exactly one token per
  node and reject a list with a clear config error (the C has no working
  multi-token behaviour to copy; its observable behaviour is a crash);
  (b) keep multi-token support as an authorised addition (vnodes).
* Agent recommendation: (a) for the transliterated core, matching the
  documented C contract; multi-token support can come back later as a
  separately authorised addition if wanted. The parse path itself
  (`parse_dyn_token`, `cmp_dyn_token`) is oracle-verified either way.
* Decision (owner, 2026-11-02): reject multi-token lists and close the
  hole. `conf::TokenList::parse` admits exactly one token in
  `0..=4294967295`; `tokens:`, `dyn_seeds`, and seeds-provider records all
  go through it. Also rejected, same rationale: negative tokens (C stores
  signum `-1` as `0xffffffff`, so they sort above every positive token)
  and tokens above `u32::MAX` (C `parse_dyn_token` keeps one word and
  wraps modulo 2^32; the embed path silently mapped them to 0).
* Pinned by: `conf/tokens.rs::regression_multi_token_list_rejected`,
  `::regression_negative_token_rejected`,
  `::regression_out_of_range_token_rejected`,
  `cluster/gossip.rs::regression_multi_token_seed_rejected`,
  `conf/server.rs::regression_dyn_seed_multi_token_rejected`.
* Status: AUTHORIZED

### DEV-P1: C swallows the reply after a bare Redis error word

* C: `redis_parse_rsp` (`src/proto/dyn_redis.c:2418`), state `SW_ERROR`,
  classifies the error word when it reaches a space or a CR, sets
  `SW_RUNTO_CRLF`, and `break`s, so the loop's `p++` consumes the
  byte. When that byte is the CR of a reply with no message text
  (`-ERR\r\n`, `-MYERR\r\n`, as a script's `{err="MYERR"}` returns),
  `SW_RUNTO_CRLF` then skips the LF and runs to the next CR: the reply
  does not complete. With nothing behind it the parse stays
  `MSG_PARSE_AGAIN` until the request times out; with a pipelined reply
  behind it, C reports one reply covering both (`-ERR\r\n+OK\r\n` is
  consumed whole, 11 bytes) and every later reply on the connection is
  matched to the wrong request.
* Rust: `redis_parse_rsp` does not advance at that byte, so the CR goes
  to `RuntoCrlf` and the error completes at its own CR LF (6 bytes).
  Replies with message text (`-ERR msg\r\n`) parse identically in both.
* Evidence: `c-oracle/tests/redis_rsp_oracle.rs::c_bare_error_word_runs_to_next_crlf`
  pins the C behaviour; the oracle properties compare every other input
  byte for byte and compare this class against C with a space inserted.
* Recommendation: keep the Rust behaviour. Matching C would stall or
  desynchronise a client on a valid RESP reply.
* Status: PROPOSED

### DEV-P2: nested multibulk replies end at the inner count (finding)

* C: `redis_parse_rsp` handles a nested multibulk element by re-entering
  `SW_MULTIBULK`, which overwrites `rntokens` with the inner count. The
  outer count is lost, so the reply ends after the inner array; this is
  only correct when the nested array is the last element (the
  `SCAN`/`SSCAN` cursor shape the C comment names). For
  `*2\r\n*1\r\n$1\r\na\r\n$1\r\nb\r\n` C completes at byte 15 and
  `$1\r\nb\r\n` is parsed as the next reply. Replies with a nested
  array before the last element (`EXEC`, `CLUSTER SLOTS`, `COMMAND`,
  `XRANGE`, `GEOPOS`) desynchronise the server connection.
* Rust: identical to C (oracle-verified); no code change.
* Evidence: `c-oracle/tests/redis_rsp_oracle.rs::nested_multibulk_ends_at_inner_count`.
* Recommendation: owner ruling on whether to parse nested arrays with a
  depth stack as an authorised fix. Until then Rust stays equal to C.
* Status: PROPOSED

## E. Decisions log

Owner decisions are appended here with date and the exact instruction.

* 2026-10: substrate DEV-S1..S3 and additions DEV-A1..A10 authorised (see
  above). All of section C remains PROPOSED.
* 2026-11-02: scope principles P1 (no C interop/migration), P2
  (Rust-native config), P3 (no Lua). Decided DEV-R1 (read repair),
  DEV-F1 (failure detection), DEV-C1 (AES-256-GCM, keep), DEV-E1 and
  DEV-E2 (entropy format, padding, throttle), DEV-K1 (config direction;
  schema design still to be reviewed).
* 2026-11-02: DEV-H1 AUTHORIZED ("use the real MurmurHash3 unless there is
  an even better hash function known for this purpose"), implemented as
  `MurmurHash3_x86_32`, one-word ring token. DEV-H2 AUTHORIZED
  ("unsigned"). DEV-H4 AUTHORIZED ("reject multi-token lists, close that
  bug/loop-hole"); the same single parser also rejects negative and
  above-`u32::MAX` tokens. Token width stays 32-bit (owner question on
  `u64` answered in the journal: no placement benefit at any plausible
  node count; widening is a separate change if ever wanted).
* 2026-11-02: defects found in authorised additions while implementing the
  above, fixed without changing their contracts: random slicing (DEV-A9)
  received 32-bit hashes in the low half of a `u64` and put every key in
  the first slice (`hashkit::hash64` now lifts them to the high half); the
  dyniak replica router (DEV-A1) looked a 64-bit hash up in a ring of
  32-bit node tokens (`router.rs` now uses the 32-bit ring token).
  `murmur3_x64_64` as a vnode ring token is folded to its high word.
  Pinned by `dynomite/tests/regression_hash_placement.rs` and
  `dyniak/src/router.rs::regression_every_hash_spreads_over_the_node_token_ring`.
