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
* Status: AUTHORIZED
* Authorized-by: Greg Burd, 2026-11-04 ("keep Rust behavior")

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
* Rust: identical to C (oracle-verified); no code change. Ruling: keep
  the Rust behaviour as it is, which is C's. Not a deviation.
* Evidence: `c-oracle/tests/redis_rsp_oracle.rs::nested_multibulk_ends_at_inner_count`.
* Status: RECORDED
* Authorized-by: Greg Burd, 2026-11-04 ("keep Rust behavior")

### DEV-R1a: repair-script atomicity by running the logic on the datastore connection

* C: each rewritten write or repairable read is an `EVAL` of a Lua
  script; Redis runs a script atomically with respect to every other
  command.
* DEV-R1 (authorised) asked for `WATCH`/`MULTI`/`EXEC` per key, retried
  on abort, "subject to the oracle test; if it cannot match the Lua
  outcomes it comes back to the owner". It cannot, for two reasons
  visible in the scripts themselves (`repair/logic.rs`, oracle-checked
  against the real Lua in `tests/repair_logic_oracle.rs`):
  1. The scripts read their own writes. HSET/SADD/ZADD/HDEL loop over
     fields and each iteration's `ZSCORE` must see the previous
     iteration's `ZADD`/`ZREM` (duplicate fields); HDEL's closing
     `ZCARD rem_set` counts the `ZREM`s it just made. Inside `MULTI`
     every read returns `QUEUED`.
  2. Some writes depend on a reply: ZADD records the field only when
     the `ZADD` reply is not nil (NX/XX/INCR not applied), and GET
     drops a stale add-set entry only when the value is nil.
  Emulating both needs a client-side model of Redis sorted-set and
  command semantics, which is a second implementation of Redis.
* Proposal: run the script logic inline on the node's single connection
  to its datastore. Every command a node sends its datastore goes over
  that one connection (the C `server_connections` option is deprecated;
  the Rust pool opens one), so a script run there, with the connection
  driver waiting for the script's replies before taking the next
  request, is atomic with respect to every other command the node
  issues, which is all the repair scheme relies on. The only traffic it
  is not atomic against is a client talking to the datastore directly,
  bypassing Dynomite; Lua would also cover that.
* Evidence required before merge: the DEV-R1 concurrency oracle (C
  engine with real Lua vs Rust engine, interleaved writers with
  conflicting timestamps through the same node, identical final data
  and metadata sets).
* Status: AUTHORIZED
* Authorized-by: Greg Burd, 2026-11-04 ("agreed")

### DEV-R2: stale field writes clear the field's delete marker (C finding)

* C: `HSET_SCRIPT`, `SADD_SCRIPT` and `ZADD_SCRIPT` run
  `redis.call('ZREM', rem_set, field)` whenever the field has a delete
  marker, before checking whether the write is older than the delete.
  `HDEL_SCRIPT` checks first. A stale HSET therefore erases the
  tombstone it lost to.
* Reproduced against the real Lua (valkey 9.1): HSET f at t3, HDEL f at
  t5 -> HGET is `[R, 5, nil]`; HSET f at t1 (stale) replies nil and HGET
  becomes `[X, 0, nil]`; HSET f at t2 (still older than the delete) then
  succeeds and HGET is `[E, 2, v2]`: the deleted field is back.
* Rust: identical to C (`repair/logic.rs`, oracle-verified).
* Recommendation: authorise the HDEL ordering for the three write
  scripts (remove the marker only when the write is not stale). It is a
  one-line change per script and cannot change any non-stale outcome.
* Rust: the three write scripts take the HDEL ordering. The repair-logic
  oracle compares against the C scripts with this one change applied to
  the C text, so every other step stays pinned to the real Lua; a
  directed test pins that a stale write leaves the delete marker and the
  field stays deleted.
* Status: AUTHORIZED
* Authorized-by: Greg Burd, 2026-11-04 ("agreed, fix the C bug and ensure this works properly in Rust")

### DEV-H5: empty latency histograms report 1, not 0 (C finding)

* C: `histo_compute` publishes `bucket_offsets[0]` (1) for a percentile
  whose threshold count is 0, which includes an empty histogram; the
  Rust histogram now does the same (`c-oracle/tests/histogram_oracle.rs`).
  An idle node's `/info` latency percentiles read 1 microsecond.
* C also skips `histo_compute` when `stats->updated == 0`; Rust computes
  on every snapshot, so after a reset with no traffic C keeps the old
  values and Rust shows 1. Not yet checked against C.
* Recommendation: keep C's 1 (it is what C operators see); align the
  compute-on-update condition in T12 when the stats surface is
  transliterated.
* Status: AUTHORIZED
* Authorized-by: Greg Burd, 2026-11-04 ("sure")

### DEV-U1: C `string_compare` orders any length mismatch as greater (finding)

* C: `string_compare` (src/dyn_string.c:98) returns
  `s1->len - s2->len > 0 ? 1 : -1` on mismatched lengths. Both operands
  are `uint32_t`, so the difference is unsigned and the test is true
  whenever the lengths differ: `compare("a", "ab")` and
  `compare("ab", "a")` both return 1. Equality and equal-length order are
  correct. The only ordering caller is `rack_name_cmp`
  (src/dyn_dnode_peer.c:1380) passed to `array_sort` -> `qsort` in
  `preselect_remote_rack_for_replication`; qsort with an inconsistent
  comparator gives an implementation-defined rack order when rack names
  in a DC differ in length, which picks the remote rack writes replicate to.
* Rust: `util::dyn_string::string_compare` sorts the shorter first;
  `Datacenter::sort_racks` sorts rack names lexicographically.
* Evidence: crates/c-oracle/tests/util_oracle.rs::string_compare_matches_c
  pins equality, equal-length order and the C length-mismatch answer.
* Recommendation: keep a consistent total order (Rust behaviour); C
  configurations whose rack names in one DC have equal length behave as C.
* Status: PROPOSED

### DEV-R3: a reply equal to an old maximum is not repaired (C finding)

* C: `find_most_updated_rsp` (src/proto/dyn_redis_repair.c:711) marks a
  reply stale only when its timestamp is below the running maximum at the
  time it is seen, or when a later reply beats the current winner. A
  reply EQUAL to a maximum that a later reply then beats is never marked:
  for timestamps 5, 5, 9 the first replica is repaired and the second is
  left at 5.
* Rust: identical (`proto/redis/repair/make.rs::find_most_updated`,
  pinned by `equal_stale_reply_after_new_max_is_not_marked`).
* Effect: the second replica stays stale until a later read; with
  DC_SAFE_QUORUM the next read of the key repairs it.
* Recommendation: fix it (mark every reply below the final maximum). It
  only adds repair writes C would have needed anyway on the next read.
* Status: PROPOSED

### DEV-R4: same-millisecond writes leave replicas divergent (C finding)

* C: the coordinator stamps each write with `current_timestamp_in_millis`
  (src/dyn_client.c ~345); the repair scripts let a write win when its
  timestamp is not older (`<` comparisons), so two writes of one key in
  the same millisecond through different coordinators carry equal
  timestamps and each replica keeps whichever reached it last.
  `find_most_updated_rsp` sees no newer reply, so a repairing read does
  not reconcile them: the replicas stay divergent until a later write.
* Rust: identical.
* Evidence: optest `read_repair::concurrent_writers_converge_after_repairing_reads`
  (three writers through three nodes, 60 conflicting ops each) leaves
  one key divergent by such a tie in about 1 of 8 runs, for C and for
  Rust alike; the scenario reports ties and fails on any other
  disagreement.
* Recommendation: break ties deterministically (e.g. append the
  coordinator's node index to the timestamp, or compare the value bytes
  when timestamps are equal) so equal-timestamp writes converge. That
  changes the stored score format (or the scripts' comparison), so it
  is an owner decision.
* Status: PROPOSED

### DEV-N1: peer reconnect wait 100 ms doubling to 5 s, not C's 1 s doubling to 10 s

* C: a peer connection pool that fails schedules its reconnect after
  `MIN_WAIT_BEFORE_RECONNECT_IN_SECS` (1 s), doubling to
  `MAX_WAIT_BEFORE_RECONNECT_IN_SECS` (10 s)
  (`dyn_connection_pool.c` `conn_pool_notify_conn_errored`,
  `dyn_dnode_peer.c:188`). C routes a request to the key's owner whatever
  the owner's gossip state (`dnode_peer_for_key_on_rack`), so a slow
  redial costs nothing until a request actually needs the peer.
* Rust: the dispatcher routes only to peers the failure detector marks
  NORMAL, and a peer becomes NORMAL on its first heartbeat over a working
  link. With C's 1 s wait, a node started 0.3 s before its peers redials
  them a full second later; until then its view has them DOWN and a write
  for a key they own fails with `Failed to achieve Quorum` (conformance
  `three_node_single_dc::each_node_accepts_workload`, 3 of 3 runs). The
  Rust supervisor therefore keeps its previous wait, 100 ms doubling to
  5 s, while adopting C's other reconnect behaviour (requests refused at
  once with `Peer: Peer Node is not connected` while it waits).
* Evidence: the cluster probe (3 nodes, one rack, started 0.3 s apart)
  answered the first write `Failed to achieve Quorum` 3 of 3 times with
  the 1 s wait and 0 of 10 times with 100 ms.
* Status: PROPOSED. Nothing new relies on it: 100 ms / 5 s is the wait
  `main` already had; the change to C's schedule is what waits on this
  ruling.
* Recommendation: keep 100 ms / 5 s until routing stops gating on
  gossip state (the C rule); then adopt C's schedule. The alternative,
  routing to DOWN owners as C does, changes the failure behaviour every
  optest partition scenario pins, so it is a larger decision.

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
* 2026-11-04: DEV-R1a AUTHORIZED ("agreed"): repair-script logic runs
  inline on the node's datastore connection. DEV-R2 AUTHORIZED ("agreed,
  fix the C bug and ensure this works properly in Rust"). DEV-P1
  AUTHORIZED ("keep Rust behavior"). DEV-P2 decided ("keep Rust
  behavior"): Rust already equals C, so nothing changes and it is
  RECORDED, not a deviation. DEV-H5
  AUTHORIZED ("sure"). `crc32` stays a valid ring hash ("crc32 is fine
  unless you know of a better alternative"): it is libmemcached's 15-bit
  `((~crc) >> 16) & 0x7fff`, so on a 32-bit token ring every key lands
  below 32768 and goes to the first node. The better alternative already exists in the same table,
  `crc32a` (full 32-bit CRC); the configuration chapter warns about it and
  points to `crc32a` / `murmur3`. Pinned by
  `regression_hash_placement.rs::crc32_reaches_one_vnode_and_crc32a_spreads`.
* 2026-11-02: defects found in authorised additions while implementing the
  above, fixed without changing their contracts: random slicing (DEV-A9)
  received 32-bit hashes in the low half of a `u64` and put every key in
  the first slice (`hashkit::hash64` now lifts them to the high half); the
  dyniak replica router (DEV-A1) looked a 64-bit hash up in a ring of
  32-bit node tokens (`router.rs` now uses the 32-bit ring token).
  `murmur3_x64_64` as a vnode ring token is folded to its high word.
  Pinned by `dynomite/tests/regression_hash_placement.rs` and
  `dyniak/src/router.rs::regression_every_hash_spreads_over_the_node_token_ring`.
