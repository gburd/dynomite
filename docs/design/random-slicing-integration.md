# Random-Slicing Integration Design

Status: DRAFT, awaiting operator approval
Author: design-only sub-agent, 2026-05-25
Branch: design/random-slicing
Scope: design document only; no source code modified
Related queue item: docs/post-chaos-queue.md "Random-slicing investigation"
Related operational finding: docs/post-chaos-queue.md "P3-1.1 Token-coverage validation"

---

## Section 1. What is random slicing?

Random slicing is a partition-assignment technique for distributed
key-value stores. The hash space is divided into a small number of
contiguous intervals; each interval is owned by exactly one
"claimant" (in our terminology, a peer). The two defining properties
are:

1. **Full coverage by construction.** The intervals are laid out
   end-to-end so their union is the entire hash space. The last
   interval absorbs any rounding remainder from integer division, so
   there is no possible value of `hash(key)` that maps to no peer.
2. **Local rebalance on membership change.** When a claimant is added
   or removed, only its slice (or, when size-targeting, a few
   neighbouring slices) changes. Keys outside the affected slices
   keep their assignment.

The technique was popularised in the dynamo/Riak literature as an
alternative to consistent hashing with virtual nodes (vnodes). The
operator-facing pitch is "you cannot accidentally configure a hole
in the ring", which is exactly the failure mode that bit us in
chaos pass-3 (see Section 4 below).

---

## Section 2. How does it differ from our current vnode-based distribution?

### Section 2.1 Replication in Dynomite vs classic Dynamo (operator clarification 2026-05-25)

A careful reader will notice that random slicing per se does
not say anything about replication. Three points clarify how
the distribution algorithm interacts with our replication
model:

1. **Classic Dynamo (e.g., Riak) replicates by walking N
   successors on the ring.** The hash function picks a
   primary token; the next `N - 1` tokens (in ring order) own
   the replicas. Removing or adding a node shifts the
   successor windows for everyone touching the affected
   range.
2. **Dynomite replicates by topology**, not by successor walk.
   The same token is replicated across DCs and racks: the
   per-DC / per-rack ring is consulted independently and the
   dispatcher fans out to the same logical position in every
   ring. Replicas are siblings on a multi-DC graph, not
   neighbours on a single ring.
3. **Riak bucket-types' `n_val` is a soft cap on fan-out**;
   it does **not** synthesize successors. When a bucket type
   sets `n_val: 3`, the dispatcher trims its replica list to
   at most three targets (rack-local first, then sibling
   racks, then sibling DCs); it does not invent extra peers
   to hit `n_val` when the topology is smaller. Random
   slicing changes how the *primary* peer is chosen for a
   given key; it does not change the replica fan-out shape.

This matters for the migration plan in Section 5: switching
from `vnode` to `random_slicing` rebalances which peer is the
*primary* for each key, but the per-DC / per-rack replica set
is unchanged. The data-movement cost is bounded by the
fraction of keys whose primary changes, not by the topology
size.

## Section 2.0 Side-by-side comparison

We currently use a per-rack continuum: each peer publishes a list of
tokens (32-bit positions on the ring) and the dispatcher maps a key
to the smallest continuum entry whose token is greater than or equal
to `hash(key)`, with a wrap-around to the first entry. The shape is
defined in `crates/dynomite/src/cluster/datacenter.rs` and the
binary search lives in `crates/dynomite/src/cluster/vnode.rs`.

Side-by-side:

| Concern | Vnode (current) | Random slicing (proposed) |
|---|---|---|
| Coverage | Operator-supplied tokens; gaps are silent. The chaos pass-3 outage was caused by 3 hosts publishing a token plan designed for 4, leaving 25% of the ring unowned. | Full coverage by construction. Builder rounds the last interval up to absorb remainder; impossible to leave a gap. |
| Adding a peer | Operator must edit every peer's token list (or the gossip plan picks new tokens that bisect existing intervals). New peer's ranges are pulled from each existing peer's vnodes; data movement is `~1/N` per existing peer. | New peer slices off its share from neighbours. With uniform weights the math is identical: `~1/N` per existing peer. With weights, only the affected weight group rebalances. |
| Removing a peer | Departing peer's tokens become orphans unless redistributed; orphaning produces NoTargets like pass-3. | Departing peer's interval merges into a neighbour; `~1/N` data redistributed; coverage preserved automatically. |
| Weighted distribution | Retrofitted: operator hand-tunes the number of tokens per peer to approximate weight. Error-prone. | Native: builder accepts `(name, weight)` or `(name, size)` pairs and apportions interval lengths in proportion. |
| Lookup cost | `O(log V)` binary search where `V = sum(tokens_per_peer)`. Per-rack continuum can be hundreds of entries when vnodes are configured aggressively. | `O(log P)` binary search where `P = number of peers in the rack`. Smaller table; better cache behaviour. |
| Configuration knob | `tokens: '0,1073741824,2147483648,3221225472'` per peer; meaningless to operators; trivial to mis-edit. | `weight: 1.0` per peer (or `weight: 0.5` / `2.0` for asymmetric racks). Default is uniform. |
| Reslice atomicity | Operator changes YAML, restarts; transient ring-mismatch during rolling restart is observed by clients as `NoTargets`. | Identical at the engine layer; the new builder is pure and runs at config-load and SIGHUP-reload. The "atomic publish" property is a property of `ServerPool::rebuild_ring`, not of the distribution algorithm. |

The single most important practical difference is the configuration
knob. Vnode token lists are a foot-gun; per-peer weights are not.

---

## Section 3. Reference implementation review

The reference is `hash_demo.c` in the repository root (442 lines,
single translation unit, no external deps beyond `libm`).

### 3.1 Data layout

```c
typedef struct {
    uint64_t *lower_bounds;          // ascending, size == count
    uint64_t *interval_sizes;        // parallel to lower_bounds
    char (*lb_to_c)[MAX_NAME_LEN];   // parallel to lower_bounds
    size_t count;
    size_t capacity;
} hash_partitions_t;
```

The three parallel arrays are cache-friendly during a lookup (binary
search reads `lower_bounds[mid]` repeatedly; the name array is only
read once at the end). The fixed `MAX_NAME_LEN = 256` is a layout
nit: it pins the per-entry footprint at 256 bytes plus 16 bytes of
metadata, which is fine for tens of thousands of claimants but is
nevertheless a static cap. A Rust port replaces `lb_to_c` with a
`Vec<String>` (or, more economically, `Vec<u32>` of peer indices, see
Section 4).

The `interval_sizes` array is technically redundant given
`lower_bounds`: any size can be reconstructed as
`lower_bounds[i+1] - lower_bounds[i]` (with a wrap for the last
entry). The reference keeps both for pretty-printing convenience.

### 3.2 Build path

Three constructors:

* `hash_partitions_create(claimants, n)` - uniform weights. Each
  interval is `UINT64_MAX / n`, and the **last interval is bumped
  up by the rounding remainder** so the union is the full ring.

* `hash_partitions_create_with_sizes(sizes, n)` - operator supplies
  the absolute interval size per claimant. Last-interval rounding
  applies.

* `hash_partitions_create_with_weights(weights, n, decimals)` -
  operator supplies relative weights. Builder rounds each weight to
  `decimals` places, computes `sum`, and converts each weight to a
  size as `floor(UINT64_MAX * w / sum)`. There is a defensive special
  case: if `w == sum` (all other weights rounded to zero) the
  claimant gets `UINT64_MAX` outright. Then it delegates to
  `create_with_sizes`.

The weight rounding is the only subtle behaviour. With
`decimal_digits=2`, weights `[1.0, 2.0, 4.0]` round to themselves and
sum to 7; sizes are `[U/7, 2U/7, 4U/7]` with the last bumped up to
absorb the `U mod 7` remainder. With weights summing to numbers that
do not divide `UINT64_MAX` cleanly, the bump still keeps coverage
exact.

### 3.3 Lookup

`hash_partitions_get_claimant(hp, hash)`:

```
if hash >= lower_bounds[count - 1]:
    return lb_to_c[count - 1]      // tail fast-path
left = 0; right = count - 1
while left <= right:
    mid = left + (right - left) / 2
    if hash >= lower_bounds[mid]:
        if mid is last OR hash < lower_bounds[mid + 1]:
            return lb_to_c[mid]
        left = mid + 1
    else:
        if mid == 0: break
        right = mid - 1
return NULL
```

This is the floor-bound search ("largest `lower_bounds[i] <= hash`").
It is `O(log count)`. The tail fast-path before the loop avoids one
iteration in the common high-bit case.

The hash function is FNV-1a over the raw key bytes:
`hash = 0xcbf29ce484222325`, then for each byte:
`hash ^= b; hash *= 0x100000001b3`. FNV is chosen for simplicity and
deterministic mixing; the article explicitly notes the partitioning
property is independent of the hash function.

### 3.4 Pretty-print

`hash_partitions_pretty_print` normalises every interval to the
smallest interval and prints `node X relative-size R`. Useful as a
configuration sanity-check; we will want a parallel feature in the
Rust admin RPC.

### 3.5 Bugs and limitations

Findings from a careful read:

* **Fixed `MAX_NAME_LEN = 256`** caps claimant identifier length
  silently via `strncpy`. A peer named beyond 255 bytes is truncated;
  collisions become possible. Rust port should use `String` and not
  cap.
* **No thread-safety.** Reads and writes share the struct without
  locks. Atomic re-slice (see "atomicity" row in Section 2) needs
  extra machinery on the embedding side; the Rust port gets this for
  free via `ServerPool::rebuild_ring` rebuilding the whole rack table
  and swapping it under the existing lock.
* **No validation of monotonic lower bounds.**
  `create_with_sizes` trusts caller-supplied sizes. If a size of zero
  appears, the builder silently produces two adjacent entries with
  the same `lower_bounds`, and the binary search returns the
  later-indexed claimant. Rust port should reject zero-sized
  intervals at build time (return `Err`).
* **Weight-rounding edge case.** If all rounded weights end up zero
  (e.g. weights `[0.001, 0.001]` with `decimals=2`), the builder
  divides by zero (`fraction = w / sum` with `sum == 0`). On glibc
  this yields `NaN`, which the cast to `uint64_t` returns 0 for, and
  the resulting `hash_partitions_t` has all-zero intervals. The Rust
  port must reject this configuration with a typed error.
* **No re-balance API.** The reference rebuilds from scratch on every
  membership change. That is the right model for our embedding too;
  no need to port a delta-update path.
* **Memory allocation is unchecked in the demos.** `hash_partitions_t`
  returned from a builder can be `NULL`; the demo functions
  dereference without checking. We do not inherit this issue
  (`Result` covers it).

None of the bugs change the algorithm; they are translation hazards
to avoid in the Rust port.

---

## Section 4. Where would random-slicing fit in our codebase?

### 4.1 New configuration variant

Today `crates/dynomite/src/conf/pool.rs` carries a deprecated
`distribution: Option<String>` that is parsed for a warning and then
ignored: there is no live `Distribution` enum in the Rust port
because vnode is the only mode. The fix is to introduce a real enum:

```rust
// crates/dynomite/src/conf/pool.rs (sketch)
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Distribution {
    Vnode,           // current behaviour, default
    RandomSlicing,   // new
}

impl Default for Distribution {
    fn default() -> Self { Self::Vnode }
}
```

The deprecated `distribution: Option<String>` field stays for
back-compat warning (`distribution: ketama` still emits the same
"deprecated; ignored" warning today) but a typed sibling field
`distribution_kind: Distribution` is added. A YAML migration table:

| YAML | Resolved kind |
|---|---|
| absent | `Vnode` |
| `vnode` | `Vnode` |
| `random_slicing` | `RandomSlicing` |
| `ketama`, `modula`, `random` | `Vnode` plus deprecation warning (today's behaviour) |

### 4.2 New module: `hashkit::random_slicing`

```
crates/dynomite/src/hashkit/random_slicing.rs
```

Public surface (sketch; final names settled in implementation):

```rust
/// Slice table built once per rack at config-load / reload.
#[derive(Clone, Debug)]
pub struct RandomSlices {
    /// Strict-ascending lower bounds, length == peer_idx.len().
    lower_bounds: Vec<u64>,
    /// Parallel array; peer index for each interval.
    peer_idx: Vec<u32>,
}

impl RandomSlices {
    /// Build from a list of peer indices, uniform weights.
    pub fn from_uniform(peers: &[u32]) -> Result<Self, RandomSlicesError>;

    /// Build from `(peer_idx, weight)` pairs; weights rounded to
    /// `decimals` places before normalising.
    pub fn from_weights(
        pairs: &[(u32, f64)],
        decimals: u32,
    ) -> Result<Self, RandomSlicesError>;

    /// Build from absolute size hints (typically used by the admin
    /// RPC for forced-rebalance operations). `sizes` must sum to no
    /// more than `u64::MAX`; the last interval is bumped up to fill
    /// the ring.
    pub fn from_sizes(pairs: &[(u32, u64)]) -> Result<Self, RandomSlicesError>;

    /// Lookup: returns the peer index that owns `hash`.
    /// `O(log N)` over the peer count.
    pub fn claimant_for(&self, hash: u64) -> u32;

    /// Iterate `(peer_idx, lower_bound, size)` for diagnostics.
    pub fn intervals(&self) -> impl Iterator<Item = (u32, u64, u64)> + '_;
}

#[derive(Debug, thiserror::Error)]
pub enum RandomSlicesError {
    #[error("at least one claimant required")]
    Empty,
    #[error("zero-sized interval at index {0}")]
    ZeroInterval(usize),
    #[error("weights sum to zero after rounding")]
    WeightsSumZero,
    #[error("size sum {0} exceeds u64::MAX")]
    SizeOverflow(u128),
}
```

The peer-index storage replaces `char[MAX_NAME_LEN]` in the C
reference; a peer's name is recovered through the existing
`Peer::endpoint()` accessor. This keeps the slice table small
(`16` bytes per slice) and avoids a string copy on every lookup.

The 64-bit hash space differs from the 32-bit `DynToken` we use
today. Two options:

* **Truncate**: feed the existing `hash(HashType::Murmur3, key)` into
  a `u32` (the low 32 bits of `DynToken::mag[0]`) and pre-shift the
  slice table to a `u32` ring. Loses 32 bits of entropy but keeps the
  hashing path identical to vnode.
* **Widen**: build a `hashkit::hash64` shim that returns `u64` from a
  64-bit murmur3 (we already have a 64-bit hash inside Riak compat
  via the dnode-hash path; reuse it). Slice table is `u64` and
  matches the article's worked example.

Recommendation: widen. The cost is a one-time addition of
`murmur3_x64_64`; the benefit is "future-proof slice resolution"
when racks grow past a few thousand peers (collision rate on
random `u32` over thousands of peers is non-trivial). See the open
question in Section 7.

### 4.3 Wiring into the rack

Today `Rack` owns a `Vec<Continuum>` and exposes
`continuums()` plus `sort_continuums()`. The proposed change is
additive: a `Rack` carries an enum:

```rust
enum RackRing {
    Continuum(Vec<Continuum>),
    RandomSlicing(RandomSlices),
}
```

with both `add_peer_tokens` (continuum path) and a new
`set_random_slices(slices)` available. `sort_continuums()` becomes a
no-op for the random-slicing variant. The dispatcher's
`collect_routable` (in `crates/dynomite/src/cluster/dispatch.rs`)
becomes:

```rust
match rack.ring() {
    RackRing::Continuum(c) => vnode::dispatch(c, &token32),
    RackRing::RandomSlicing(s) => Some(s.claimant_for(hash64)),
}
```

The `ServerPool::rebuild_ring` path (the one entry point that
populates a rack's ring at config-load and at reload) gains a branch
on `cfg.distribution_kind`:

* `Vnode`: today's path - `rebuild_continuums()` then
  `Rack::sort_continuums()`.
* `RandomSlicing`: collect `(peer_idx, weight)` pairs per rack from
  the peer config, call `RandomSlices::from_weights`, hand the result
  to `Rack::set_random_slices`.

### 4.4 Per-peer config knob

Today the YAML carries `tokens: '0,...'`. The proposed
random-slicing knob:

```yaml
servers:
  - host: peer-a
    port: 8101
    weight: 1.0     # default 1.0 if omitted
  - host: peer-b
    port: 8101
    weight: 1.0
```

Validation (at config-load, before `rebuild_ring`):

* If `distribution_kind == RandomSlicing`, all peers in a single rack
  must specify either all-`weight` or all-default (uniform). Mixed
  with `tokens: ...` is rejected with a clear error (the operator is
  trying to migrate but missed a peer).
* If `distribution_kind == Vnode`, `weight:` is ignored with a
  warning so a YAML can be authored once and switched between modes
  by toggling the pool-level field.

### 4.5 ASCII diagram

```
                     vnode (today)
                     -------------
key --hash32--> token --binary search--> continuum entry --> peer_idx
                          (per-rack continuum, V entries
                           where V = sum(tokens_per_peer))

                     random slicing (proposed)
                     -------------------------
key --hash64--> u64  --binary search--> slice  ----------> peer_idx
                          (per-rack slice table, P entries
                           where P = peer_count_in_rack)
```

The lookup is the same shape. The data shrinks. The configuration
gets safer.

---

## Section 5. Migration path

Operators who want to switch from vnode to random-slicing should
follow a four-step procedure. None of the steps require operator
downtime if the SIGHUP-reload path is used (see
`docs/journal/2026-05-24-sighup-reload.md`).

### 5.1 Pre-run: shadow the new distribution

Add a `--distribution-shadow=random_slicing` startup flag (and a
matching YAML key `distribution_shadow:`) that builds **both** ring
representations at config-load. The dispatcher uses the live
distribution as today but, on every routing decision, also computes
the would-be peer for the shadow and increments
`metrics.distribution_shadow_disagreement_total{from,to}` when they
differ.

Operators run shadow for a working day, watch the disagreement
metric, sample a few keys with `dyn-hash-tool` to confirm the new
distribution agrees with their mental model, and only then schedule
the cutover.

### 5.2 Cutover: change YAML, SIGHUP

Rolling restart is unnecessary; the SIGHUP-reload path already
rebuilds the rack ring atomically. The operator:

1. Edits the pool's YAML: sets `distribution: random_slicing` and
   adds `weight: 1.0` to each peer (or omits, which defaults to
   uniform).
2. Issues `kill -HUP $(pgrep dynomited)` on every node.
3. Watches `dist::peer_state` and the no-targets metric to confirm
   the new ring is in effect everywhere.

### 5.3 Data movement

The first request after the cutover that lands on a peer who does
not have the key locally returns a miss (memcache) or kicks off a
read-repair (Riak / Redis dyn-mode). The cluster converges through
the entropy reconciliation path that already exists; no new
mechanism is needed. The amount of data moved is `O(K * (1 - p))`
where `K` is total keys and `p` is the agreement rate between the
old and new distribution. For typical migrations from "operator
hand-balanced vnodes" to "uniform random slices", `p` is small and
nearly all keys move; this is the worst-case cost of any
distribution change and is no worse than vnode-to-vnode reslicing
with a different token plan.

### 5.4 Rollback

If shadow disagreed but the operator cut over anyway and is now
unhappy, revert the YAML and SIGHUP again. The vnode ring is
deterministically rebuilt from the unchanged `tokens:` lists.
Keys written under random-slicing are returned by their original
vnode owner once that owner runs read-repair against the new
primary. The operator should leave shadow on for another day to
confirm convergence.

---

## Section 6. Effort estimate

Targeting one week of focused work plus review, mirroring Stage-3
and Stage-10 cadence in `PLAN.md`.

| Task | Effort |
|---|---|
| Port `RandomSlices` + builders + `RandomSlicesError`; cover with unit tests, hegeltest properties (full-coverage invariant; deterministic claimant; rebalance moves only `O(1/N)` keys); criterion bench against vnode lookup | 2 days |
| Add 64-bit murmur3 (or settle for 32-bit truncation; see open question 7.1) and the `hash64` dispatch helper, with golden-vector tests | 0.5 day |
| Introduce `Distribution` enum; promote the deprecated YAML field to a typed sibling; round-trip serde tests; deprecation-warning tests for `ketama`/`modula`/`random` | 0.5 day |
| Extend `Rack` with `RackRing` enum; update `sort_continuums()` semantics; thread `distribution_kind` through `ServerPool::rebuild_ring`; integration test that flips a pool from vnode to random-slicing and back | 1 day |
| Update `collect_routable` and the dispatch-plan tests; add a regression test that pins the pass-3 token-coverage failure (3 hosts, hand-broken vnode plan) and shows random-slicing routes the orphan quadrant | 1 day |
| Add `--distribution-shadow` flag, the `distribution_shadow_disagreement_total` metric, and the admin-RPC dump that pretty-prints the slice table | 0.5 day |
| Documentation: `docs/parity.md` Deviation entry, mdBook section on distribution modes, embedding-API doc update, `examples/random_slicing.rs`, journal entry | 0.5 day |
| Total | **6 days** |

The estimate excludes shadow-runtime soak validation (parallel
to the chaos pass-4 budget) and the eventual deprecation of
`tokens:` configuration (out of scope for this design).

---

## Section 7. Open questions

The questions below need an operator decision before implementation
starts.

### 7.1 Hash width

* **Option A (truncate)**: keep the existing 32-bit murmur3 and
  build a `u32` slice table. Aligns with `DynToken`. Cheaper.
* **Option B (widen)**: add `murmur3_x64_64` and use a `u64` slice
  table. Aligns with the article's formulation. Future-proof.

Recommendation: **B**. The `u64` ring is the canonical formulation,
the cost is a single new hash function (we already vendor the
`murmur3_x86_32` body, the 64-bit twin is ~50 lines), and 32-bit
collisions become measurable past a few thousand peers per rack.

### 7.2 Hash family

The C reference uses FNV-1a; today we use murmur3 throughout. The
algorithm is independent of the hash. Recommendation: **murmur3**
for consistency with the rest of the engine. FNV is simpler but
already present only in the legacy `hashkit::fnv` module; using two
different hash families across the codebase would double the
golden-vector surface area without buying anything.

### 7.3 Bucket-types `chash_keyfun` interaction

Riak bucket-types can override the chash key function. That override
runs **before** the ring lookup; it modifies the key (e.g., strips a
sibling-tag), not the ring layout. Random slicing is invoked with
the post-override key just like vnode is. Recommendation: **no
special handling needed**; document the ordering in the bucket-types
note and add a regression test to pin it.

### 7.4 Default and deprecation

* Should `Distribution::Vnode` stay the default and random-slicing
  be opt-in? Yes, for backward compat through at least 0.2.x.
* Should we eventually deprecate vnode? **Maybe**, after we have
  six months of production telemetry from operators who opted in.
  Deferred decision; not part of this design.

### 7.5 Heterogeneous racks

Within a single rack, do we ever expect mixed weights (e.g.,
`weight: 1.0` on small boxes, `weight: 2.0` on large)? The article
says yes; our deployments today are uniform. Recommendation:
**support weights from day one** because the C reference does and
the math is no harder; ship the uniform path as the default and the
weighted path as the advanced knob.

### 7.6 Atomic publish vs single-rack rebuild

Today `ServerPool::rebuild_ring` rebuilds **all** racks under one
write lock. That is the atomic-publish guarantee. Random slicing
does not change the lock discipline; we should resist the temptation
to rebuild a single rack in isolation, even though the algorithm
would technically allow it. **Recommendation: keep rebuild_ring
all-or-nothing.**

---

## Section 8. Recommendation

**Yes, ship random slicing as an opt-in `Distribution::RandomSlicing`
variant.** The case in three lines:

1. The exact failure mode behind the chaos pass-3 14-point success
   drop (3-of-4 hosts running with a 4-host token plan, 25% of the
   ring orphaned) is structurally impossible under random slicing.
   That on its own is worth the implementation cost.
2. The operator-facing knob shrinks from "list of magic 32-bit
   integers per peer" to "one float per peer". This is a strict
   ergonomic improvement that the chaos team has been asking for
   since the first multi-host pass.
3. The implementation is tractable (Section 6: 6 days for a working
   feature plus shadow-mode validation). No new dependencies. No
   change to the dispatcher's contract. `Distribution::Vnode` stays
   the default, so existing deployments are untouched.

The token-coverage validator from `P3-1.1` should still ship; it is
strictly orthogonal (it catches operator error in the vnode path)
and remains useful for operators who do not migrate. Random slicing
makes the validator unnecessary in racks that opt in; it does not
make it redundant elsewhere.

Suggested branch sequence:

1. `feat/distribution-enum` (the typed config variant; low risk;
   merges first).
2. `feat/random-slicing-engine` (the `hashkit::random_slicing`
   module; pure; tested in isolation).
3. `feat/random-slicing-rack` (the `RackRing` enum and dispatcher
   wiring; observable behaviour change).
4. `feat/distribution-shadow` (the shadow knob; off by default).
5. `feat/random-slicing-docs` (mdBook + parity entry + embedding
   example).

Each step is independently reviewable and revertible.
