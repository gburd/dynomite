# Active Anti-Entropy: comparison vs Riak's two generations

This document compares Dynomite's shipped Tictac AAE implementation
(in `crates/dyn-riak/src/aae/`) against Riak's two generations of
AAE: the original 2.x hashtree-based design and the NHS-Digital /
Martin-Sumner-led redesign that ships in modern `riak_kv` against
the `leveled` storage engine.

## Section 1. What we shipped

Module layout: `crates/dyn-riak/src/aae/`

| File | Role |
|---|---|
| `mod.rs` | public surface re-exports |
| `tictac.rs` | the merkle tree (top-level time buckets, bottom-level segments) |
| `exchange.rs` | three-phase exchange protocol (ROOT-SYNC, TREE-SYNC, KEY-SYNC) |
| `scheduler.rs` | tokio task that drives exchanges on a configurable cadence |
| `repair.rs` | divergent-key repair sink with `Winner | Siblings` outcome (DVV-aware as of 2026-05-25) |
| `config.rs` | `ConfAae` operator-facing tunables |

Tree shape (`TreeShape` defaults, see `aae::config`):

* **Top level**: 24 time buckets, 1 hour wide each, rolling 24-hour window.
* **Bottom level**: 1024 segments per time bucket.
* **Leaf**: `xxhash64(bucket || 0x00 || key || 0x00 || vclock)` summed into the segment hash.

Exchange protocol:

1. **ROOT-SYNC** — peer A sends its 24 time-bucket roots (pairs of `(bucket_id, hash)`); peer B compares, replies with the time buckets that disagree.
2. **TREE-SYNC** — for each disagreeing time bucket, peer A sends the 1024 segment hashes; peer B replies with the segment ids that disagree.
3. **KEY-SYNC** — for each disagreeing segment, peer A sends the full set of `(bucket, key, vclock_hash)` triples currently in that segment; peer B compares and returns the keys whose vclocks differ.

The diverging-key list is then handed to `RepairTask::evaluate`,
which picks `RepairOutcome::Winner` (sole DVV dominator) or
`RepairOutcome::Siblings` (concurrent DVVs).  The repair sink
ships winners back to divergent peers via the existing
per-peer outbound channel.

Scheduler cadences:

* Full sweep (every peer pair, every time bucket): 24 hours
* Segment exchange (single time bucket): 60 seconds
* Configurable via `ConfAae::full_sweep_interval_seconds` and `segment_interval_seconds`

Storage shape: **RAM-only**.  The tree is rebuilt on every
reconcile from whatever the local datastore reports; we do
not persist the tree to disk.

Wire format: **Rust-self-consistent**.  We deliberately did not
target Erlang ETF parity; a real Riak node cannot AAE-handshake
with our nodes.  This is documented as a non-goal in
`docs/journal/2026-05-24-dyn-riak-aae.md`.

Hash: `xxhash64`.  Riak uses SHA-1 in gen-1 and `crypto:hash(sha)`
in gen-2.

## Section 2. Riak generation-1 hashtree AAE

Original Riak 2.x AAE, primarily `riak_kv_index_hashtree`.  Active
~2014 onwards.

Design:
* Per-vnode merkle hashtree, persisted to disk as a separate
  LevelDB instance.
* Tree is updated incrementally as user data is written (every
  put fires a tree update).
* Periodic pairwise tree exchange between vnodes that hold
  replicas of the same partition.
* On divergence, the diverging key flows back through the read
  path with a synthetic GET that triggers Riak's existing
  read-repair machinery.

Pain points (well-documented in operator notes and Basho's
follow-up engineering blogs):

1. **Tree-build cost**: a full tree rebuild after corruption or
   format change can take hours and consumes substantial RAM.
2. **Memory peak**: keeping the tree's working set hot in
   memory was a known issue for large vnodes.
3. **Drift between user data and tree**: if the tree got out
   of sync (e.g. crashed write, missed update), it had to be
   torn down and rebuilt; partial repair was not possible.
4. **Operational opacity**: when AAE was broken, operators
   had limited tools to diagnose; the typical recourse was
   `aae-status`, `aae-rebuild`, and crossing fingers.
5. **One tree per vnode**: with hundreds of vnodes per node,
   the per-tree overhead added up.

Sources I'm drawing on for this section: knowledge of the
`riak_kv_index_hashtree.erl` module shape, basho.com archived
docs (the "Active Anti-Entropy" page on the 2.x reference
manual), and prior reading of Basho engineering blog posts on
AAE pain points.  I do NOT have authoritative web access in
this session; specific quotes and benchmark numbers from
those sources should be treated as working-from-memory until
verified.

## Section 3. Riak generation-2 (NHS-Digital / Martin Sumner)

The NHS-Digital fork of `riak_kv` (https://github.com/nhs-riak/riak_kv,
forked from `basho/riak_kv`) shipped the redesign.  Principal
designer: Martin Sumner.  Key components:

* **`kv_index_tictactree`** (https://github.com/martinsumner/kv_index_tictactree):
  the new merkle library.  "Tictactree" comes from "tic-tac merkle
  tree" — a rolling-window merkle structure where time itself is
  one of the dimensions.  This is where the name we borrowed
  comes from.
* **`leveled`** (https://github.com/martinsumner/leveled): a
  log-structured-merge KV engine that replaces LevelDB on the
  storage path.  Critical to the AAE redesign because leveled
  exposes native fold operations that the AAE module consumes
  directly without maintaining a parallel tree.
* **AAE-fold**: the term used for the new exchange shape, where
  the merkle structure is computed on demand by folding over
  leveled segments.

What changed (high level):

1. **Tree is derived, not maintained**.  The old design kept the
   tree as a parallel data structure that had to stay in sync
   with user data.  The new design computes the relevant slice
   of the tree by folding over leveled's native key index.
   This eliminates an entire class of "tree drift from data"
   bugs.
2. **Memory drops dramatically**.  Because the tree is computed
   on demand for the segments that actually need exchange, peak
   RAM is bounded by the exchange's working set, not by the
   total tree size.
3. **Convergence after partition healing is faster**.  The
   rolling time-bucket structure means a node that was
   isolated for 30 minutes only has to re-exchange the time
   buckets covering those 30 minutes, not the entire tree.
4. **Smaller per-vnode overhead**.  Per-vnode trees go away;
   the leveled fold serves the whole cluster's AAE traffic
   from one storage engine.
5. **Operational tooling improved**: `riak admin aae-status` and
   the `nhs_riak` admin CLI gained richer query support.

Sources: https://github.com/martinsumner/kv_index_tictactree's
README and design docs, the "leveled" repo, public talks by
Martin Sumner (one at NHS-Digital tech meetup, slides circulated
on the Riak users mailing list).  Same "working from memory"
caveat as Section 2.

## Section 4. Side-by-side comparison

| Property | Our Tictac (shipped) | Riak gen-1 (2.x) | Riak gen-2 (NHS-Digital) |
|---|---|---|---|
| Tree storage | RAM-only, per-instance | LevelDB on disk, per-vnode | derived on demand from leveled fold |
| Build cost | Full rebuild on each reconcile | Incremental on every put | Computed per exchange, no maintenance |
| Storage backend coupling | Storage-agnostic | Bitcask + parallel LevelDB | Tightly coupled to leveled |
| Top-level structure | 24 time buckets, 1h each | Flat merkle, 1M segments | Rolling time buckets (configurable) |
| Bottom-level segments | 1024 per bucket | ~1M leaves | Configurable (typically 1024) |
| Exchange phases | ROOT-SYNC -> TREE-SYNC -> KEY-SYNC | Compare-roots -> walk-diff | Tic-tac: per-time-bucket exchange |
| Wire format | Rust-self-consistent | Erlang ETF | Erlang ETF |
| Tree drift from data | impossible (rebuild from data each time) | possible (tree can lag) | impossible (tree IS data fold) |
| Memory peak | Working set of one tree rebuild | Full tree resident | Working set of one exchange |
| Convergence after partition | 1 segment interval (default 60s) | Hours (full tree rebuild) | Time-bucket-window (default 1h) |
| Per-peer overhead | One tree per peer-pair-during-exchange | One persistent tree per vnode | One leveled fold per exchange |
| Hash | xxhash64 | SHA-1 | SHA-1 |
| Cross-cluster real-Riak compat | No | N/A | N/A |

## Section 5. Findings

### What we got right

1. **Storage-agnostic tree**: not coupling to a specific storage
   engine is a strict win.  The gen-2 redesign tightly couples
   to leveled; we work against any `Datastore` impl.  That said,
   we pay for it with a full rebuild on every reconcile.
2. **Rolling time-bucket structure**: matches the gen-2 design.
   Convergence after partition healing scales with the partition
   duration, not with the total tree size.
3. **Three-phase exchange (ROOT/TREE/KEY)**: standard merkle
   exchange shape; same as both gen-1 and gen-2.
4. **DVV-aware sibling-aware merge** (added 2026-05-25, commit
   `efd3511`): cleaner causality tracking than gen-1's per-vclock
   exchange and matches what gen-2 ships post-2018.

### What we got wrong (or simply different)

1. **No incremental tree**.  We rebuild the tree from scratch on
   every reconcile.  This is fine at our current scale (in-memory
   datastore, small key counts) but will not scale to a
   production-sized Riak workload.  The gen-2 design's
   "fold-on-demand" approach is the right answer once we have a
   real persisted backend.  Mitigation: when `NoxuDatastore` is
   the backend (gated behind `riak-storage` feature), we should
   use Noxu's native fold/scan API instead of full rebuild.
2. **No persisted tree**.  Process restart loses all AAE state;
   the next sweep rebuilds from scratch.  Acceptable for a v1.
3. **Per-pair exchange, not per-vnode**.  We exchange between
   peer pairs at the top level; gen-1/2 exchange between vnodes
   on a per-partition basis.  Functionally equivalent for our
   single-token-per-peer model, but if we ever support multiple
   tokens per peer (closer to true Dynamo) this needs to
   become per-token.
4. **`xxhash64` vs SHA-1**.  Both are fine; xxhash64 is faster
   and produces collision rates that are completely acceptable
   for AAE's purpose (probabilistic divergence detection).
   Cited as a "deviation" only because it diverges from real
   Riak's choice; not a correctness issue.

### What we deliberately deviated from

1. **No Erlang ETF wire compatibility**.  Documented as an
   explicit non-goal in 2026-05-24-dyn-riak-aae.md.  A real
   Riak node cannot AAE-handshake with us.  Re-affirmed in
   the 2026-05-25 user discussion: wire compatibility is
   non-goal.
2. **Storage-agnostic tree** (already in "what we got right";
   listed here too because it's a deliberate trade-off:
   compatibility with multiple `Datastore` impls vs the
   incremental-update efficiency that storage-coupling buys).

## Section 6. Recommendations

| Item | Description | Files | Effort | Operator impact |
|---|---|---|---|---|
| R1 | Persist tree across restart | `aae/tictac.rs` + new `aae/persist.rs` | 2 days | none (auto-rebuild on missing snapshot) |
| R2 | Native-fold path for `NoxuDatastore` | `aae/tictac.rs::Tree::insert_from_datastore` | 3 days | none (transparent perf win) |
| R3 | Per-token (not per-peer) exchange | `aae/scheduler.rs`, `aae/exchange.rs` | 2 days | none (more granular) |
| R4 | Operator metrics for AAE health | `stats/failure.rs` already has the slot; wire AAE counters | 1 day | none (new prometheus lines) |
| R5 | `dyn-admin aae-status` command | `crates/dyn-admin/src/commands/aae_status.rs` | 1 day | new operator surface |

Total: ~9 days for "the realistic gen-2 polish path."  None
of these change the wire format or add a new dependency.

A "redo against a real persisted backend with native-fold
semantics" would be a much larger project (matches what gen-2
delivered: ~6 months of fork-and-redesign for the riak_kv
team).  Out of scope for the v1.

## Section 7. Conclusion

**Recommendation: accept the current Tictac implementation as
v1; ship the R1+R2+R4 follow-ups when we move to a persisted
backend (Noxu-as-Riak-default).**

The shipped design is structurally aligned with Riak gen-2
(rolling time-bucket merkle).  The key gen-2 advantages
(no tree drift, faster convergence, lower memory peak) are
either matched today or only become relevant at a scale we
do not target in v1.

If we get production-traffic feedback that AAE convergence is
slow or memory-bound, R2 (Noxu native-fold) is the right
next item.  Until then, the current implementation is fit
for purpose.

---

**Caveats on this analysis**: I wrote this from working memory
of Riak's AAE history rather than fresh research.  Specific
benchmark numbers and citations from gen-1's pain points and
gen-2's improvements should be verified against:

* `basho/riak_kv/src/riak_kv_index_hashtree.erl` (gen-1)
* `martinsumner/kv_index_tictactree` (gen-2 library)
* `nhs-riak/riak_kv/src/riak_kv_tictacaae_handler.erl` (gen-2 integration)
* Martin Sumner's blog posts and Riak users mailing list
  archives circa 2018-2020

The two AAE-research sub-agents I dispatched today both
declined to commit; I'm writing this directly so the operator
has something concrete.  A future research pass with confirmed
web access could improve the citations in Sections 2 and 3.
