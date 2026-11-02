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
deviations/additions/improvements"). Shared obligation: with a C
configuration file (no Rust-only keys) the binary behaves as the C binary;
the differential suite runs the C configs unchanged to prove it.

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
| DEV-B1 | Read repair by vote count, single-key GET only | D2 | Revert to C timestamp/Lua scheme as default; keep vote count only as an opt-in mode if wanted |
| DEV-B2 | AES-256-GCM peer cipher alongside C CBC | D-crypto | Keep as opt-in (CBC default for C interop); security gain is real (CBC key-as-IV is deterministic and unauthenticated) |
| DEV-B3 | Entropy PKCS#7 padding vs C caller zero-padding | Stage 11 | Revert unless C-node interop is proven unaffected |
| DEV-B4 | Entropy length-prefixed chunks vs C fixed `cipher_size` | Stage 11 | Revert to C framing as default (interop) |
| DEV-B5 | Entropy snd/rcv framing unified | Stage 11 | Revert to C framing as default (interop) |
| DEV-B6 | `BGREWRITEAOF` over RESP instead of `system("redis-cli ...")` | Stage 11 | Keep (removes a shell-out; same effect); needs evidence |
| DEV-B7 | Entropy throughput throttler removed | Stage 11 | Revert (C behaviour protects the datastore) |
| DEV-B8 | `DynErrorCode::BadFormat.message()` stable string vs `strerror(8)` | Stage 7 | Revert to C text unless clients ignore it |
| DEV-B9 | Per-call `Crypter` vs long-lived `EVP_CIPHER_CTX` | Stage 6 | Keep if output identical (substrate-like); oracle test decides |
| DEV-B10 | RSA padding choice | Stage 6 | Must match C exactly (interop); oracle test decides |
| DEV-B11 | Config validation stricter than C (several Stage 4 entries: `secure_server_option`, empty strings, numeric weight, bracketed IPv6, `*_connections: 0`) | Stage 4 | Case by case; rejecting a config C accepts is a compatibility break |
| DEV-B12 | Gossip failure detection: phi-accrual / SWIM vs C timestamp liveness | membership | Make C timestamp liveness the default; phi/SWIM opt-in |
| DEV-B13 | Snitch rack-distance helpers, florida client on tokio, `is_aws_env` string, response-mgr clamp | Stage 10 | Case by case |
| DEV-B14 | Hinted handoff at the proxy layer | D1 | Covered by DEV-A7 if off by default; verify |
| DEV-B15 | `replication_strategy` walk-N-successors, `chash_keyfun` | D4/D5 | Covered by additions if opt-in; verify |
| DEV-B16 | `dn_coredump_init` (RLIMIT_CORE) omitted | dynomite.c | Revert (C behaviour) unless embed users need opt-out |
| DEV-B17 | Signal set: SIGUSR1/2, SIGTTIN/TTOU, SIGSEGV semantics not reproduced | D-audit | Revert (C behaviour) |

## D. Decisions log

Owner decisions are appended here with date and the exact instruction.

* 2026-10: substrate DEV-S1..S3 and additions DEV-A1..A10 authorised (see
  above). All of section C remains PROPOSED.
