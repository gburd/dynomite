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

## Proposed, need the owner

* DEV-N1: peer redial wait stays 100 ms doubling to 5 s (main's value),
  not C's 1 s / 10 s, because Rust routes only to peers gossip marks
  NORMAL and C routes regardless.
* DEV-M1: C's memcache retrieval crash; Rust's semantics; question
  whether to match blocks to keys by name (C's merge reorders after a
  miss).
* Carried: DEV-U1, DEV-R3, DEV-R4.

## Not done

* Stats counters peer_timedout_requests / remote_peer_timedout_requests
  / server_dropped_requests exist but are not incremented.
* The datastore reconnect uses Rust's 50 ms jittered backoff, not C's
  1 s doubling (`server_retry_timeout`).
