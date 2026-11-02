# 2026-11-02: hash and token rulings (DEV-H1, DEV-H2, DEV-H4)

Owner rulings: H1 "use the real MurmurHash3 unless there is an even better
hash function known for this purpose"; H2 "unsigned"; H4 "reject
multi-token lists, close that bug/loop-hole".

## What implementing them uncovered

Checking where each ruling lands found three placement collapses already
shipped. Each one sent every key to one node with no error and no metric.

| Config | Before | After |
|---|---|---|
| `hash: murmur3` (vnode) | 300/0/0 | 98/102/100 |
| `hash: murmur3_x64_64` (vnode) | 300/0/0 (library probe) | 110/83/107 |
| `distribution: random_slicing` + any 32-bit hash (incl. default `murmur`) | 300/0/0 | 90/104/106 |
| `random_slicing` + `murmur3_x64_64` | 106/98/96 | 83/107/110 |
| dyniak router with `murmur3` / `murmur3_x64_64` | 20000/0/0 | spreads |

Numbers are keys per backend on a 3-node `dynomited` cluster on localhost,
300 `SET`s through node 1 (dyniak row: 20,000 `primary_index` lookups).

Causes:

1. Token order (`cmp_dyn_token`, `DynToken::cmp`) compares word count
   before magnitude. Node tokens are one word. The Rust `murmur3` emitted
   four words and `murmur3_x64_64` two, so every key token sorted above
   every node and wrapped to the first.
2. `hashkit::hash64` put 32-bit hashes in the low half of a `u64`; the
   random-slicing table partitions the full 64-bit space, so every key fell
   in the first slice. The only shipped random-slicing tests used
   `murmur3_x64_64`, which masked it. The riak build defaults to
   `random_slicing`, so a riak deployment with the default `hash:` hit it.
3. The dyniak router looked a 64-bit hash up in a ring whose points are
   32-bit node tokens.

## Changes

* `hash: murmur3` = `MurmurHash3_x86_32`, seed `0xc0a1e5ce`, one word.
  Oracle-checked against the C tree's own contrib `MurmurHash3_x86_32`.
  `MurmurHash3_x86_128` kept as `hashkit::murmur3_x86_128` for HLL; HLL
  item hashes pinned unchanged by `regression_hll_item_hash_is_stable`.
* `murmur3_x64_64` as a ring token folds to its high word (one word).
* `hash64` lifts 32-bit hashes into the high half.
* dyniak router uses the 32-bit ring token.
* `TokenList::parse` admits exactly one token in `0..=u32::MAX`; every
  ingestion path (`tokens:`, `dyn_seeds`, seeds-provider records, embed)
  goes through it. The embed path previously turned unparseable or
  oversized tokens into 0 silently.
* `one_at_a_time`: already unsigned; now pinned against an independent
  unsigned reference.

## Why MurmurHash3_x86_32 and not something "better"

For ring placement the hash needs uniform output and full avalanche over
32 bits. MurmurHash3_x86_32 passes SMHasher. xxHash3 and wyhash are faster
per byte, which matters for bulk hashing, not for a hash of a short key
inside a proxied network request. MurmurHash3_x86_32 is also what the
Netflix `dyno` client's `Murmur3HashPartitioner` computes, so a
token-aware client and the server agree. (`dyno`'s default partitioner is
`Murmur1HashPartitioner`, whose `hash(byte[], int)` is the MurmurHash2
variant that dynomite's default `hash: murmur` implements.)

## Token width: u32 vs u64

Kept at u32. With one token per node and a few hundred nodes, a 2^32 ring
gives each node about 10^7 positions of granularity; widening buys no
placement quality. It would change the gossip and config wire format and
the DNODE handshake, which is a separate change if ever wanted.

## Gates

* DST: `model-tests/src/ring.rs::TokenOrderModel`, placement reach under
  the real token order, with a negative control (multi-word key tokens)
  that the checker catches.
* Elle: `scripts/consistency/check.sh` green (placement-only change; no
  history semantics changed).
* Regression tests fail on the pre-change tree (verified):
  `dynomite/tests/regression_hash_placement.rs`,
  `dyniak/src/router.rs::regression_every_hash_spreads_over_the_node_token_ring`.

## Not fixed here

* `crc32` (libmemcached) is a 15-bit hash by definition and only reaches
  the first node on a 32-bit ring, in C as here. Kept for parity;
  excluded from the spread test. Worth an owner note: it should probably
  be rejected as a ring hash.
* FNV-1 family clusters on keys that differ only in the final byte (the
  last byte only XORs the low 8 bits). Algorithm property, matches C.
* `cargo doc` without `--all-features` fails on feature-gated dyniak
  intra-doc links (pre-existing; the CI build uses `--all-features`).

## Process incident

Unison (`unison-ws-*`, `-repeat watch -prefer newer`) syncs `~/ws` to
other hosts. A `git stash` / `stash pop` during a negative-control run
raced it, and unison wrote older copies back over uncommitted edits.
Recovered from the dangling stash commit. Rule recorded in agent memory:
no `git stash` in `~/ws`; do negative controls in a scratch worktree.
