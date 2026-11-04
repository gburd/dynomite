# 2026-11-04 Transliteration progress: rulings, T2/T1/T3 closures, T12, fragmentation, fixes

Ledger: 47/1964 rows done at the start of 2026-11-03, 111 after T2,
679/1964 now (2 pending an owner decision: DEV-U1).

## Owner rulings recorded (deviations.md section E)

DEV-R1a (repair-script atomicity inline on the datastore connection),
DEV-R2 (fix C's stale-write tombstone erase), DEV-P1 (keep Rust's
bare-error-word parse), DEV-H5 (empty histograms report 1) AUTHORIZED;
DEV-P2 RECORDED (Rust already equals C); crc32 stays a valid ring hash
and the configuration chapter warns about it (15-bit; one node per rack
owns every key under vnode) and points to crc32a / murmur3.

## Landed

* T2 hashkit closed: every src/hashkit file in closed-files.txt;
  get_hash_type / get_hash_func oracle.
* T1 / T3: 20 utility and substrate files closed (dyn_queue.h, dyn_dict*,
  dyn_types*, dyn_log*, dyn_cbuf.h, dyn_array.h, dyn_task*, dyn_ring_queue*,
  ...). New oracles: util_oracle.rs (dn_atoi, dn_atoui, dn_valid_port,
  count_digits, string_compare, msgs_per_sec, the signal table, log level
  up/down/set/loggable incl. through C signal_handler), mbuf_oracle.rs
  (mbuf_copy/length/empty/rewind/split, _dn_strchr/_dn_strrchr, key/arg
  pos lengths).
* T12 part 1: the admin HTTP command surface (every C endpoint; bodies
  byte-identical to C; consistency, read repairs, timeout factor, log
  level, peer state and node state runtime-settable).
* Multi-key fragmentation (MGET/MSET/DEL/EXISTS split per rack owner,
  merged in client key order) with an optest scenario both engines pass
  and a stateright model with a negative control.
* T8 units 1-2: the ten repair scripts as Rust (oracle against the real
  Lua, with DEV-R2 applied), the internal repair-script request, and its
  execution on the replica's datastore connection. Coordinator rewrite,
  repairable-read reconcile and cleanup are in progress on work/t8.

## Bugs found by C-vs-Rust probing and fixed

* Client replies were written in completion order, not request order:
  pipelined clients received replies paired with the wrong requests.
* With its datastore down a node left requests hanging (lost with the
  connection or queued through the reconnect backoff); it now answers
  `-ERR Storage: Datastore refused connection` at once, as C does.
* Dynomite-generated errors use C's text `-ERR <source> <message>`.
* The backend connection kept only the latest responder (found by the
  fragmentation worker): two in-flight requests on one backend connection
  got each other's replies.
* SIGTTIN/SIGTTOU/SIGUSR1/SIGUSR2 were unhandled (SIGUSR killed the
  process); SO_KEEPALIVE was never set on outbound links.
* Dependency advisories (rustls, webpki, tokio, wasmtime, noxu yank) and a
  dnode header add-overflow panic (quickfuzz), earlier the same day.

## C bugs found (reproduced faithfully or recorded)

* string_compare treats any length mismatch as "greater" (unsigned
  subtraction), giving qsort an inconsistent comparator for rack order:
  DEV-U1, PROPOSED, needs an owner ruling.
* The C signal handler logs via localtime (not async-signal-safe) and can
  deadlock on glibc's timezone lock; the node then never answers again.
  Recorded by optest io_faults::c_signal_handler_can_deadlock_is_recorded.
* /peer/<op>/all resets every peer whatever op is asked; /set_timeout_factor
  truncates to a signed byte before clamping (200 -> 1). Reproduced.
* MGET with a failing fragment closes the client connection in C
  (redis_post_coalesce_mget sets owner->err); Rust replies the error.

## Still open (not deviations; work items)

* T8 slices: coordinator rewrite, repairable-read reconcile + repair write
  + metadata stripping, delete cleanup, un-ignore
  func_test_safe_quorum_read_repair, LWW model.
* Timeout factor is stored but no per-request timeout uses it yet (T6
  msg_tmo_*); request gating by node state is T9.
* mbuf capacity per buffer (C 16320 data bytes vs Rust 16368) and
  mbuf_data_size as the Redis key-length limit; unix-socket path length
  check; dyn_rbtree tie order test (DEV-S2 obligation).
* Memcache multi-key fragmentation is not wired.
