# Buckets, Keys, and Objects

The Dyniak data model is Riak's data model: a flat namespace of
*buckets*, each holding *objects* addressed by a *key*. An object is a
value plus metadata -- a content type, secondary-index entries, links,
and a causal-context blob. This chapter walks the model from the outside
in, then covers the two things that trip up newcomers to a Dynamo-style
store: siblings and conflict resolution.

If you have not yet stood up a node, do that first in
[Getting Started with Dyniak](./getting-started.md); the examples here
assume a running gateway on `127.0.0.1:8098` (HTTP) and `127.0.0.1:8087`
(PBC).

## Buckets and keys

A bucket is a named container for objects. Buckets are cheap: there is
no create step for a plain bucket -- you write an object into a bucket
and the bucket exists. A key is an arbitrary byte string that names an
object inside a bucket. The pair `(bucket, key)` is the object's full
address.

```mermaid
flowchart LR
  subgraph b1 [bucket: users]
    k1[key: alice] --> v1[value + metadata]
    k2[key: bob] --> v2[value + metadata]
  end
  subgraph b2 [bucket: sessions]
    k3[key: s-9f3a] --> v3[value + metadata]
  end
```
<p class="dyn-caption">A bucket is a flat namespace of keys; each key
addresses one object, which is a value plus its metadata. Buckets do
not nest.</p>

Over HTTP the address is a URL path:

```sh
# address:  bucket "users", key "alice"
curl http://127.0.0.1:8098/buckets/users/keys/alice
```

Over PBC the address is the `bucket` and `key` fields of the request
message. The routing layer hashes the pair (or just the bucket, if the
bucket's `chash_keyfun` property says so -- see below) to choose the
ring position that owns the object.

## The object: value plus metadata

An object carries more than its value. The HTTP envelope makes each
piece explicit:

```json
{
  "value": "Alice Liddell",
  "content_type": "text/plain",
  "indexes": [
    {"name": "age_int", "value": "42"},
    {"name": "city_bin", "value": "seattle"}
  ],
  "links": [
    {"bucket": "users", "key": "bob", "tag": "friend"}
  ]
}
```

The pieces:

<dl class="dyn-facts">
<dt>value</dt>
<dd>The opaque payload. Dyniak stores the bytes verbatim; it does not
interpret them except where a query feature (2i, search, MapReduce)
asks it to.</dd>
<dt>content_type</dt>
<dd>A MIME type describing the value. It is round-tripped, not enforced:
Dyniak stores whatever you send and returns it. Set it so clients (and
MapReduce phases) know how to read the value.</dd>
<dt>indexes</dt>
<dd>Secondary-index entries. Each is a <code>(name, value)</code> pair;
the name suffix <code>_int</code> or <code>_bin</code> selects an
integer or binary index. Covered in
<a href="./mapreduce.md">Secondary Indexes and MapReduce</a>.</dd>
<dt>links</dt>
<dd>Typed pointers to other objects, each a
<code>(bucket, key, tag)</code> triple. Covered in
<a href="./links.md">Links and Link Walking</a>.</dd>
</dl>

### Content types over the wire

Over HTTP, the value's content type is negotiated two ways. The
gateway's transport encoding -- how the whole envelope is serialized --
is chosen from the `Accept` header (`application/json`,
`application/cbor`, or `application/x-protobuf`). The value's own
content type is a field inside the envelope. Because the envelope is
persisted in a canonical, codec-independent form, a value written as
JSON is fetchable as CBOR and vice versa:

```sh
# write as JSON
curl -X PUT http://127.0.0.1:8098/buckets/users/keys/alice \
  -H 'Content-Type: application/json' \
  -d '{"value": "Alice", "content_type": "text/plain"}'

# read the same object as CBOR
curl http://127.0.0.1:8098/buckets/users/keys/alice \
  -H 'Accept: application/cbor' --output alice.cbor
```

Over PBC the value and its content type ride in the `RpbContent`
message; indexes are `RpbPair` entries and links are `RpbLink` entries
inside the same `RpbContent`.

## Bucket properties

A bucket's behaviour is governed by its properties. Fetch them:

```sh
curl -s http://127.0.0.1:8098/buckets/users/props
```

```json
{
  "props": {
    "name": "users",
    "n_val": 3,
    "allow_mult": false,
    "last_write_wins": false,
    "r": "quorum", "w": "quorum",
    "pr": 0, "pw": 0,
    "dw": "quorum", "rw": "quorum",
    "basic_quorum": false,
    "notfound_ok": true
  }
}
```

The properties that matter most day to day:

* **`n_val`** -- the replication factor: how many copies of each object
  the ring keeps. The default is 3. This one is honored: the router
  places each key on `n_val` replicas.
* **`r` / `w`** -- read and write quorums. `"quorum"` means a majority
  of `n_val`.
* **`pr` / `pw`** -- primary-replica read and write quorums.
* **`dw`** -- durable-write quorum.
* **`allow_mult`** -- whether the bucket keeps siblings on a conflict.
* **`last_write_wins`** -- whether conflicts are resolved by timestamp.

```admonish warning title="Which bucket properties are enforced today"
`n_val` (the replica count) and `allow_mult` (sibling retention) change
behavior, as does `ttl` (object expiry, swept up by the reaper -- see
below) and `precommit_module` (a WASM hook run on every write -- see
below). Of the quorum knobs, `r` and `w` ARE enforced: a `handle_get`
waits for `r` replica responses and a `handle_put` waits for `w`
replica acks across the key's replica set before answering, failing
below quorum, with an availability fallback on a fire-and-forget
transport. `pr`, `pw`, and `dw` are accepted, stored, and echoed back
on `GetBucket`, but are **not yet applied** on the read/write path --
no request handler reads them, and there is no durable-write signal
from storage to enforce `dw` against. They need the sloppy-quorum /
fallback-node distinction and a storage durability signal that are not
yet modelled. The PBC `GetBucket` reflects the stored per-bucket
properties; the HTTP `/props` GET still echoes Riak's documented
defaults.
```

Set properties with `PUT`:

```sh
curl -X PUT http://127.0.0.1:8098/buckets/users/props \
  -H 'Content-Type: application/json' \
  -d '{"props": {"n_val": 5, "allow_mult": true}}'
```

```admonish note title="Two Dyniak-specific bucket properties"
Beyond the Riak set, Dyniak adds `chash_keyfun` (route on
`<bucket>/<key>`, on `<bucket>` only, or through a custom WASM module)
and `replication_strategy` (per-DC/per-rack quorum fan-out, the
Dynomite default, versus walk-N-successors, the Riak default). Both are
documented with their wire encodings in
[Riak mode ops](../operations/riak.md#bucket-properties).
```

### Precommit hooks

A bucket may name a WASM module via the `precommit_module` property.
That module runs on every write to the bucket, before the value is
committed: it may accept the value unchanged, transform it, or veto the
write outright with a rejection reason. This is the same linear-memory
WASM ABI used by custom `chash_keyfun` modules and MapReduce phases.
Postcommit hooks -- Riak's fire-and-forget post-write notification --
are not implemented.

### The object-TTL reaper

A bucket's `ttl` property (in seconds) marks its objects for expiry.
Expiry is not instantaneous: a background reaper sweeps on an interval
(`reap_interval_seconds`, default 300 seconds) and reaps at most a
bounded number of candidates per sweep (`reap_max_per_cycle`, default
10,000), so a single cycle cannot stall behind an unbounded scan. An
expired object may therefore live briefly past its nominal TTL until
the next sweep visits its partition.

## Causal context

Every object read returns a small *context* blob that encodes the
object's causal history. The client's job is simple: read the blob,
hold it, and echo it back on the next write of the same key. The server
uses the echoed context to decide whether the new write supersedes what
is stored (a normal update) or diverged from it (a conflict).

```mermaid
sequenceDiagram
  participant C as client
  participant S as dyniak node
  C->>S: GET users/alice
  S-->>C: value + context v1
  Note over C: client edits the value
  C->>S: PUT users/alice (value2, context v1)
  Note over S: context v1 matches stored history
  S-->>C: 204 (clean update)
```
<p class="dyn-caption">The read-modify-write cycle. The client echoes
the context it read; the server uses it to distinguish a clean update
from a conflict. A client that skips the context risks creating an
unnecessary sibling.</p>

Dyniak encodes the context as an Interval Tree Clock (ITC) rather than
Riak's dotted version vector. The two answer the same question -- did
these two writes see each other, or did they diverge? -- but ITC scales
with the *currently-live* actor population rather than with every actor
that ever participated, which suits Dynomite's dynamic-membership
cluster model. Retired nodes leave no residual cost in the clock.

```admonish warning title="Opaque context: the compatibility boundary"
The context blob is opaque on the wire. A client that round-trips the
bytes verbatim keeps working unchanged. A client that cracks the blob
open and parses it as a Riak DVV must switch decoders, because the byte
shape is ITC, not DVV. This is the one place Dyniak's byte
compatibility with Riak breaks; the semantics are identical, the bytes
are not. The rationale and citations are in
[Riak mode ops](../operations/riak.md#causality-tracking).
```

## Conflict resolution and siblings

Here is the heart of the Dynamo model. Because Dyniak is masterless and
eventually consistent, two clients can write the same key at the same
time without either seeing the other. Dyniak tracks causality with an
interval tree clock (ITC) carried as the object context, so it can tell
whether two writes are causally ordered (one descends from the other)
or genuinely concurrent.

### How a conflict is resolved today

* **Causally ordered writes**: the later write descends from the
  earlier context and simply supersedes it. No conflict.
* **Concurrent writes** (both descend from the same context, neither
  saw the other): Dyniak *detects* the conflict via the ITC comparison
  and emits a `dyniak::aae::repair` warning, then resolves it to a
  single deterministic value -- the lexicographically-largest of the
  conflicting values, ties broken by the encoded clock. The read
  returns that one value.

```admonish note title="Siblings are retained under allow_mult"
With `allow_mult` set on the bucket, two writes concurrent under the
per-object causal context are BOTH retained as siblings -- no concurrent
write is lost. A PBC read returns every sibling as its own
`RpbContent`; an HTTP read returns `300 Multiple Choices`. The client
resolves the siblings and writes the resolution back carrying the
context the read returned (the causal join of the siblings), which
supersedes them all.

Without `allow_mult` (the default), a concurrent write collapses to a
single deterministic value, so a concurrent write can be lost -- use
`allow_mult`, or a CRDT, when that matters.

When the merge of concurrent writes is well-defined by the data (a
count, a set, a map), a **convergent data type (CRDT)** is still the
better tool: it merges automatically with no client-side sibling
resolution. See [Convergent Data Types](./crdts.md).
```

### With `last_write_wins: true`

The store keeps only the write with the higher timestamp and discards
the other -- an explicit, documented last-write-wins policy. Use it
when the value is disposable or the write rate makes conflicts
vanishingly rare. (In practice the lexicographic fallback above already
collapses concurrent opaque writes to one value; `last_write_wins`
makes that intent explicit at the bucket level.)

```admonish note title="Road not taken: CRDTs over last-write-wins-only"
Dyniak ships convergent data types (CRDTs) as the recommended option
for the common concurrent-write cases -- counters, sets, maps. A CRDT
merges concurrent writes *automatically and correctly* with no lost
write, because the merge is defined by the data type's algebra rather
than by a timestamp or lexicographic race. If your value is a count, a
set, or a map, reach for a CRDT before you reach for an opaque object.
See [Convergent Data Types](./crdts.md) and
[Roads Not Taken](../reference/roads-not-taken.md).
```

## A worked read-modify-write

Putting the pieces together, the canonical update loop reads (carrying
the causal context), modifies, and writes back:

```python
import riak

client = riak.RiakClient(host='127.0.0.1', pb_port=8087)
bucket = client.bucket('users')

# fetch (carries the causal context)
obj = bucket.get('alice')

# modify; the client library echoes the context back on store, so a
# write that descends from this read supersedes it cleanly.
obj.data = update(obj.data)
obj.store()
```

The client library carries the context for you. A write that descends
from the read supersedes it; two writes that both descend from the same
context are concurrent, and -- as described above -- Dyniak currently
resolves them to a single deterministic value rather than surfacing
siblings. If losing one of two concurrent writes to the same key is not
acceptable, model the value as a CRDT, the subject of the next chapter.

## Where to next

* [Convergent Data Types](./crdts.md) -- make conflict resolution
  automatic.
* [Links and Link Walking](./links.md) -- connect objects into a graph.
* [Secondary Indexes and MapReduce](./mapreduce.md) -- query objects by
  index value.
* [Dyniak wire protocols](../protocols/dyniak.md) -- the exact PBC and
  HTTP surface.
