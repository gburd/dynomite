# Failure Handling

<div class="dyn-hero">
<span class="dyn-tagline">Nodes fail, links partition, clocks drift.
Dynomite is built to stay available through all of it and to heal the
damage afterward.</span>

Failure handling in Dynomite is layered: an adaptive detector decides
when a peer is dead, gossip ejects and readmits it, hinted handoff keeps
writes durable while it is gone, and read repair plus Merkle-tree
anti-entropy reconcile the divergence that a failure leaves behind.
</div>

This chapter walks the phi-accrual failure detector and the peer state
machine, auto-eject and auto-rejoin, durable hinted handoff, read repair,
the anti-entropy overview, and the guarantees that hold across a
partition.

## Detecting failure: phi-accrual

A naive detector flips a peer between "alive" and "dead" on a
missed-heartbeat count. That is brittle: a network hiccup trips it, and a
genuinely slow link never does. Dynomite uses a **phi-accrual** detector
([`PhiAccrual`](DYN_SRC_BASE/crates/dynomite/src/cluster/failure_detector.rs)),
the same family Cassandra, Akka, and Riak deploy, which produces a
continuous suspicion level `phi(t)` instead of a boolean.

Phi is the negative log-probability that a heartbeat *would not have
arrived yet* given the historical inter-arrival distribution. Modelling
arrivals as exponential gives the closed form the detector computes:

```text
    phi(t) = elapsed_ms / (mean_interval_ms * ln(10))
```

<dl class="dyn-facts">
<dt>phi = 1.0</dt>
<dd>~10% chance the heartbeat is merely late.</dd>
<dt>phi = 2.0</dt>
<dd>~1% chance.</dd>
<dt>phi = 8.0</dt>
<dd>~10^-8 -- "almost certainly dead". This is the default threshold
(<code>DEFAULT_THRESHOLD</code>), matching Cassandra's
<code>phi_convict_threshold</code>.</dd>
</dl>

The detector keeps a sliding window of the last ~100 inter-arrival times
per peer, so it adapts to each link's real cadence. A jittery link that
normally varies wildly will not be convicted by a one-second gap that
would convict a metronome-steady link -- higher observed variance widens
the tolerated gap. Two guards keep the math honest: the mean interval is
clamped at a floor (default 1s) so burst arrivals cannot make phi spike,
and phi is `0.0` when no heartbeat has ever been recorded (no data is not
the same as dead) or when fewer than two heartbeats give no inter-arrival
sample.

```admonish note title="Scope: peer plane only"
The phi-accrual detector is for the dnode peer plane, driven by gossip
heartbeats. The backend datastore (redis / memcache) is not
heartbeat-driven -- it is exercised by real client traffic -- so backend
health uses a consecutive-failure auto-eject tracker instead. Do not wire
phi-accrual into backend supervision.
```

## The peer state machine

Each peer carries a
[`PeerState`](DYN_SRC_BASE/crates/dynomite/src/cluster/peer.rs).
Only `Normal` and `Joining` are routable; `Down` and `Leaving` are not.
A remote peer starts `Down` and is promoted only after its first
below-threshold heartbeat; the local node starts `Joining`.

```mermaid
stateDiagram-v2
  [*] --> Down: remote peer created
  [*] --> Joining: local node created
  Joining --> Normal: bootstrap complete,<br/>heartbeats flowing
  Down --> Normal: first heartbeat,<br/>phi <= threshold
  Normal --> Down: phi > threshold<br/>(silence) or announced departure
  Down --> Normal: heartbeats resume,<br/>phi <= threshold
  Normal --> Leaving: graceful departure
  Leaving --> [*]
```
<p class="dyn-caption">Peer lifecycle. The routable states are Normal and
Joining; the failure detector drives the Normal/Down toggle, and a
graceful departure moves a peer to Leaving so it stops receiving traffic
before it goes.</p>

The gossip handler is the single owner of these transitions once gossip is
wired. On each inbound heartbeat it records the arrival and, if phi is
below threshold, promotes the peer to `Normal` at once. On each periodic
tick it evaluates every non-local peer and toggles `Normal` / `Down` from
the current phi. A peer that flaps -- goes silent, is convicted, then
resumes -- produces exactly one `Normal -> Down` and one `Down -> Normal`
transition per cycle, each surfaced as a metric and a structured event.

```mermaid
sequenceDiagram
  participant P as peer
  participant FD as phi detector
  participant GH as gossip handler
  loop steady state
    P->>FD: heartbeat (1 Hz)
    Note over FD: phi stays < 1.0
  end
  Note over P: peer goes silent
  GH->>FD: tick: phi(now)?
  FD-->>GH: phi = 12 (> 8)
  GH->>GH: mark peer Down, emit PeerDown
  Note over P: peer recovers
  P->>FD: heartbeat resumes
  GH->>GH: phi <= 8, promote Normal, emit PeerUp
```
<p class="dyn-caption">Silence raises phi past the threshold on the next
tick and convicts the peer; a resumed heartbeat promotes it back. The
detector's window means recovery is judged on the same adaptive baseline
as conviction.</p>

## Auto-eject and auto-rejoin

Eviction and readmission are gossip-driven, not operator-driven.

* **Auto-eject.** When a peer crosses the phi threshold (or announces its
  own departure), the handler marks it `Down`. The next continuum rebuild
  drops it from the routable set, and the dispatcher stops planning
  requests to it. No manual intervention, no config edit.
* **Auto-rejoin.** When a `Down` peer starts gossiping again and its phi
  falls below threshold, the handler promotes it back to `Normal` on the
  next heartbeat, and it re-enters the routable set on the next rebuild.
  Its failure detector is reset on re-add so old jitter does not bias the
  fresh baseline.

The whole loop is closed by gossip and the detector; the operator's role
is to fix the underlying fault, not to click a node back in.

## Hinted handoff

A write whose target replica is `Down` would normally just miss that
replica. Hinted handoff makes the write durable anyway: the coordinator
records a **hint** -- the on-the-wire request bytes, the intended peer
index, and an expiry deadline -- and a background drainer ships it to the
peer once the peer returns to `Normal`.

```mermaid
sequenceDiagram
  participant Cl as client
  participant Co as coordinator
  participant R1 as replica r1 (up)
  participant R2 as replica r2 (DOWN)
  participant HS as hint store
  Cl->>Co: SET k v (DC_QUORUM)
  Co->>R1: forward SET
  Co->>HS: record hint for r2
  Note over Co: synthetic +OK on r2's behalf<br/>counts toward quorum
  R1-->>Co: +OK
  Co-->>Cl: +OK
  Note over R2: r2 recovers -> Normal
  HS->>R2: drainer ships hinted SET
  R2-->>HS: +OK, hint cleared
```
<p class="dyn-caption">Hinted handoff keeps a write durable across a
replica outage. The hint counts toward the consistency threshold at write
time and is replayed to the replica when it returns, so the down replica
catches up without a full repair.</p>

Handoff is only active when the hint store is wired *and* the pool sets
`enable_hinted_handoff`. When active, `Down` write targets are kept in the
routable set so the dispatcher can hint them; a synthetic `+OK` is fed to
the coalescer on the hinted target's behalf so the surviving replicas plus
the hint can meet the consistency threshold. Without handoff, a `Down`
target is simply skipped.

### Durability of hints

The hint store
([`HintStore`](DYN_SRC_BASE/crates/dynomite/src/cluster/hints.rs))
has two modes:

<dl class="dyn-facts">
<dt>RAM-only (<code>HintStore::new</code>)</dt>
<dd>Hints live only in per-peer in-memory queues and are lost if the
coordinator restarts.</dd>
<dt>Durable (<code>HintStore::open</code>, with <code>hint_dir</code>)</dt>
<dd>One append-only segment file per peer under
<code>&lt;dir&gt;/peer-&lt;idx&gt;.hints</code>. Each record is framed with
a length, an IEEE CRC-32 of the body, a wall-clock deadline, and the
payload. Hints survive a coordinator restart -- replay re-anchors each
deadline to the current clock and drops any already-expired hint.</dd>
</dl>

The durable format is torn-tail safe: a crash mid-append leaves at most a
trailing partial record, and replay detects it two ways -- a short read
before the body completes, or a body whose CRC does not match -- and stops
cleanly at the first damaged record, keeping every intact record before
it. A torn tail never panics and never surfaces an error from `open`.

Hints are bounded by `max_bytes` and expire after `hint_ttl_seconds`
(default one day). Over-capacity or zero-TTL enqueues are rejected so the
store cannot grow without bound; when the store is full the coordinator
falls back to its no-quorum error path rather than silently dropping the
write.

## Read repair

Read repair heals divergence that a quorum read observes: the majority
value is returned to the client and the stale replicas are written back
through the same channels. It is covered in full in
[Replication and Consistency](./consistency.md); the important boundary is
that read repair only heals replicas a real read touched. Keys that are
written but rarely read, and replicas that were down during the read, are
left for anti-entropy.

## Anti-entropy: the entropy reconciliation channel

Anti-entropy is the background reconciliation that does not depend on a
client reading a key. The base engine's `entropy` module is a pluggable
snapshot-exchange transport: a sender periodically ships a
[`SnapshotSource`](https://docs.rs/dynomite)-produced byte blob to a
peer's [`SnapshotSink`](https://docs.rs/dynomite) over an
AES-128-CBC-encrypted, length-prefixed link, and the receiver replays
it. The transport itself has no notion of a Merkle tree, a key range, or
a divergent subset -- it ships whatever bytes the plugged-in
`SnapshotSource` returns and hands whatever bytes it receives to the
plugged-in `SnapshotSink`. Building a Merkle-tree-aware source (compact
digests, descend-on-mismatch, ship only the divergent range) is an
embedder responsibility on top of this transport, not something the
channel does itself.

`dynomited`'s shipped default wires an empty `StaticSnapshot` as the
source: when `recon_key_file:` / `recon_iv_file:` are configured, the
entropy task ticks on its cadence, negotiates with the peer, and ships a
zero-length snapshot -- in practice a heartbeat that verifies peer
reachability and exercises the wire framing, not a data reconciliation.
A deployment that wants real Merkle-tree anti-entropy on this channel
supplies its own `SnapshotSource` / `SnapshotSink` pair through the
embedding API.

```mermaid
flowchart TD
  R1["replica r1<br/>SnapshotSource"] --> SEND["entropy sender:<br/>encrypt, frame, ship"]
  SEND --> R2["replica r2<br/>SnapshotSink"]
  R2 --> APPLY["apply() replays the<br/>plugged-in sink"]
```
<p class="dyn-caption">The entropy channel moves an opaque snapshot
blob from a source to a sink; whether that blob is a Merkle-tree digest,
a full dataset dump, or (the shipped default) nothing at all is decided
by which `SnapshotSource` is plugged in.</p>

This channel sits beneath read repair and hinted handoff as the intended
safety net for divergence that neither of the other two mechanisms
reached -- writes that were never read back, hints that expired before
the peer recovered, or data that drifted during a long partition -- but
only once a Merkle-aware (or otherwise reconciling) source is wired in;
the shipped heartbeat-only default does not reconcile anything by
itself. The Dyniak layer's own AAE design (a Tictac merkle tree plus a
three-phase exchange) is unit-tested library code with the same
not-yet-wired-into-the-running-binary caveat; see
[Dyniak AAE](../dyniak/aae.md) for the detail.

## What happens during a partition

Put the three mechanisms together and the partition story is:

```mermaid
sequenceDiagram
  participant A as DC-A side
  participant B as DC-B side
  Note over A,B: link between DCs drops
  Note over A: A's detector convicts B's peers (phi > 8)
  Note over B: B's detector convicts A's peers
  Note over A,B: each side keeps serving with its<br/>own reachable replicas
  Note over A: writes to B's replicas -> hinted (if enabled)
  Note over A,B: link heals
  A->>B: gossip reconverges membership
  A->>B: hints drained to recovered replicas
  A->>B: read repair + anti-entropy reconcile divergence
```
<p class="dyn-caption">A cross-DC partition. Each side stays available on
its own replicas, records hints for the unreachable side, and reconciles
via gossip, hint drain, read repair, and anti-entropy once the link
heals.</p>

<dl class="dyn-facts">
<dt>Availability</dt>
<dd>Both sides of a partition keep serving reads and writes against the
replicas they can still reach. There is no leader to lose, so neither
side stalls.</dd>
<dt>Durability within quorum</dt>
<dd>A write that meets its consistency level on the reachable side is not
lost: it is on that side's replicas, and -- if hinted handoff is enabled
-- queued for the unreachable replicas. When the partition heals, gossip
readmits the peers, hints drain, and read repair closes any gap a
subsequent read observes. The entropy channel can close a remaining gap
too, but only once a reconciling `SnapshotSource` is wired in; the
shipped default is a heartbeat, not a reconciler (see above).</dd>
<dt>Consistency</dt>
<dd>Divergent writes on the two sides are reconciled after the heal, not
prevented during the split. Under the strict consistency levels a request
that cannot meet its level returns a no-quorum error rather than a
divergent answer (see <a href="./consistency.md">Replication and
Consistency</a>).</dd>
</dl>

```admonish warning title="The guarantee is quorum-scoped"
"No data loss" holds for writes that met their consistency level on a
side that survived. A write acknowledged at DC_ONE that landed only on a
replica which then failed permanently before hinting or anti-entropy
could copy it elsewhere is a genuine loss -- that is the trade DC_ONE
buys you. Choose the level that matches your durability requirement; see
[Replication and Consistency](./consistency.md).
```

```admonish note title="Road not taken: fencing / leader-based failover"
Dynomite does not fence a partitioned node or fail writes over to a
leader, because there is no leader and no fencing token. The Dynamo model
accepts concurrent writes on both sides of a partition and reconciles
after the fact; that is what keeps both sides available. Systems that
need strict single-writer semantics use a consensus layer instead and pay
the availability cost during partition. See
[Roads Not Taken](../reference/roads-not-taken.md).
```

## Where to go next

* [Membership and Gossip](./gossip.md) -- how the failure detector is fed
  and how ejection / readmission propagate.
* [Replication and Consistency](./consistency.md) -- read repair, the
  consistency levels, and what a no-quorum error means.
* [The Ring and the Token Space](./ring.md) -- how a `Down` peer drops out
  of routing on the next continuum rebuild.
* [Dyniak AAE](../dyniak/aae.md) -- the full Merkle-tree anti-entropy and
  the transactional reconciliation path.
* [Configuration](../configuration.md) -- the `enable_hinted_handoff`,
  `hint_dir`, `hint_ttl_seconds`, and failure-detector knobs.
</content>
