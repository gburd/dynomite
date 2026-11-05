# 2026-11-05 Request timeouts, node-state gate, memcache fragmentation

Ledger: 704/1964 done at the start of the day.

## Burner

chef expires; the test host moved to woof (176550300114).
`scripts/ec2-dist/burner-testhost.sh` (renamed from chef-testhost.sh)
takes `PROFILE` (default woof), keeps a per-profile account guard, and
gains a `cref` phase that clones and builds the C reference at 2046b05.
The full optest suite passed on woof with `OPTEST_REQUIRE=1`. The chef
host (optest-20261103-133836) is torn down.

## Landed

* **T9 node-state gate** (`cluster::dispatch::local_gate`,
  `LocalBackend`). Probed against C per state for keys the node owns:
  STANDBY refuses reads and writes, WRITES_ONLY and RESUMING refuse
  reads, WRITES_ONLY and JOINING hold requests unsent, RESUMING sends and
  flushes the held queue, NORMAL sends when nothing is held. A held
  request fails "Storage: Datastore refused connection" at its datastore
  timeout. optest `node_state` passes on both engines.
* **T6 request timeouts.** `OutboundRequest::deadline` (api-change
  journal 2026-11-05) from C's formula (timeout; peer +200 same DC /
  +5000 remote, +20000 for a write; times the factor). Both the datastore
  and peer drivers close the connection at the earliest deadline and
  answer every outstanding request (Storage / Peer refused connection);
  while a peer redials, data requests get "Peer Node is not connected".
  The three stall probes (frozen datastore local and relayed, frozen
  peer) and the isolated DC_QUORUM node now answer like C. optest
  `request_timeout` passes on both engines.
* **DC_ONE answers from the own rack** (C swallow rule). A fast
  "not connected" from a remote rack no longer fails a write the local
  rack applied. DST model `dc_one_answer` with negative control.
* **Gossip frames are held across a redial** instead of dropped; a node
  started before its peers no longer sees them DOWN for a gossip
  interval.
* **Peer-forwarded requests use the pool's datastore parser.** The dnode
  listener parsed everything as RESP, so a Rust memcache rack never
  stored a key a peer owned. `regression_memcache_peer`.
* **mbuf data size and redis key limit equal C** (16320 writable, key
  bound 16336 at the default size; follows `mbuf_size`). c-oracle check
  over every legal size.
* **Memcache get/gets split per owner** and merged with C's logic.
  optest `memcache_fragment`: C crashes on every retrieval (DEV-M1); Rust
  returns memcached's VALUE blocks.
* **Test ports below the ephemeral range** (tests/support/port.rs),
  ending the three_node_single_dc bind flake (6/6 failures under port
  churn before, 0/6 after).

## Owner rulings (2026-11-05)

* DEV-N1 AUTHORIZED: keep the 100 ms / 5 s peer redial wait until
  routing stops depending on gossip state.
* DEV-M1 AUTHORIZED: keep the memcache semantics, C's reply order
  after a miss included.
* Still open: DEV-U1, DEV-R3, DEV-R4.

## Follow-up, same day

* **Timeout, drop and ejection counters on /info.** dynomited builds a
  real `Stats` aggregator and refreshes `/info` every `stats_interval`.
  server_eof / server_timedout / server_err, server_dropped_requests,
  server_ejects / server_ejected_at, peer_connections, peer_eof /
  peer_timedout / remote_peer_timedout / peer_err, peer_timedout_requests
  / remote_peer_timedout_requests and peer_ejected_at are counted at
  the C points. After a frozen datastore and a frozen peer every one of
  them equals C's (optest `timeouts_are_counted_in_info`).
* **Datastore reconnect on C's schedule.** 1 s doubling to
  `server_retry_timeout / 1000` s (default 10 s), reset on connect, no
  jitter. Time to serve again after the datastore returns, down 0.5 /
  2 / 9 s: C 0.81 / 2.82 / 14.8 s, Rust 0.81 / 2.79 / 14.8 s.
* `check.sh` now runs `cargo doc -D warnings` (a broken intra-doc link
  had reached main), and loom's unused `futures` feature is dropped to
  clear RUSTSEC-2026-0441 (pin-utils unmaintained).
