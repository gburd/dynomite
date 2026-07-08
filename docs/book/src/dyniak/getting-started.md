# Getting Started with Dyniak

This chapter takes you from an empty checkout to a running Dyniak node
you can write objects to, and then to a small cluster. It assumes you
have read [Introduction to Dyniak](./index.md) for the shape of the
system. Every shell command assumes you have run `nix develop` first, as
described in the manual's [conventions](../intro.md#conventions-used-in-this-manual).

## Step 1: build dynomited with the riak feature

Dyniak is compiled into the server only when you ask for it:

```sh
cargo build -p dynomited --features riak
```

Without the feature, `dynomited` builds and runs identically to a Redis
or Memcache deployment; the `riak:` configuration block is still parsed
and validated but is a no-op at run time. With the feature, the server
gains the PBC and HTTP listeners and the `data_store: dyniak` backend.

```admonish tip title="Confirm the feature is present"
A quick way to confirm you built the right binary: start it against a
config that selects `data_store: dyniak` (below). A binary built
without `--features riak` rejects that value at validation time with
"dyniak data_store requires dynomited built with --features riak", so
you find out immediately rather than at first request.
```

## Step 2: write a minimal Dyniak config

A Dyniak pool needs three things beyond a normal pool: `data_store:
dyniak` (or the integer `2`), a `noxu_path:` for the on-disk
environment, and a `riak:` block naming the listener addresses. Save
this as `dyniak.yml`:

```yaml
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '0'
  data_store: dyniak
  noxu_path: /tmp/dyniak-noxu
  servers:
  - 127.0.0.1:6379:1        # placeholder, ignored under dyniak
  riak:
    pbc_listen: 127.0.0.1:8087
    http_listen: 127.0.0.1:8098
```

A few things to understand about this config:

* **`data_store: dyniak`** selects the Noxu-backed Riak backend. The
  integer form `2` means the same thing; it sits alongside the historic
  `0` (valkey / redis) and `1` (memcache).
* **`noxu_path:`** is where the pool opens its in-process, transactional
  Noxu environment. It is created if absent. This directory is the
  durable home of every object, index, and transaction log.
* **`servers:`** is preserved for schema compatibility but is not
  contacted. A Dyniak pool does not run a RESP client proxy and does not
  dial an external backend, so the `listen:` address is not bound and
  the placeholder `127.0.0.1:6379:1` is never dialled. All client
  traffic enters through the PBC and HTTP listeners.
* **`riak.pbc_listen`** and **`riak.http_listen`** name the two client
  listeners. Either can be omitted to disable it; both can run
  side-by-side and share a single backend.

```admonish note title="Why the servers list stays"
It would be tidier to drop `servers:` for a Dyniak pool, but keeping it
means one config schema across all three protocol layers, so an
operator's tooling and validation do not need a Dyniak special case.
The placeholder is conventional; see
[Riak mode ops](../operations/riak.md#noxu-as-the-backing-datastore).
```

Validate the config before you start it:

```sh
dynomited -t -c dyniak.yml
```

The `-t` flag parses and validates without binding any sockets. It is
the same validator CI runs, so a config that passes `-t` starts
cleanly.

## Step 3: start the node

```sh
dynomited -c dyniak.yml
```

The node opens the Noxu environment at `/tmp/dyniak-noxu`, binds the
PBC listener on `127.0.0.1:8087` and the HTTP gateway on
`127.0.0.1:8098`, and waits for clients. Confirm it is alive with the
HTTP liveness probe:

```sh
curl -s http://127.0.0.1:8098/ping
# OK
```

## Step 4: write and read an object over HTTP

The HTTP gateway is the human-debuggable path. It uses Riak's route
shape: an object lives at `/buckets/<bucket>/keys/<key>`. Store one:

```sh
curl -s -X PUT http://127.0.0.1:8098/buckets/users/keys/alice \
  -H 'Content-Type: application/json' \
  -d '{"value": "Alice Liddell", "content_type": "text/plain"}'
```

A successful store replies `204 No Content`. Read it back:

```sh
curl -s http://127.0.0.1:8098/buckets/users/keys/alice
# {"value":"Alice Liddell","content_type":"text/plain", ...}
```

The gateway negotiates the response encoding from the `Accept` header:
`application/json`, `application/cbor`, or `application/x-protobuf`. A
value stored under one encoding is fetchable under any other, because
the object is persisted in a canonical, codec-independent envelope.

Ask for CBOR instead:

```sh
curl -s http://127.0.0.1:8098/buckets/users/keys/alice \
  -H 'Accept: application/cbor' --output alice.cbor
```

Delete it:

```sh
curl -s -X DELETE http://127.0.0.1:8098/buckets/users/keys/alice
# 204 No Content
```

As in Riak, deleting an absent key is not an error -- the delete path
replies `204 No Content` whether or not the key existed.

```admonish note title="The full HTTP route table"
The routes shown here are the object-level subset. The gateway also
exposes `/stats`, `/buckets?buckets=true` (list buckets),
`/buckets/{b}/keys?keys=true` (list keys), `/buckets/{b}/props`
(bucket properties), `/mapred`, `/transactions`, and the search routes.
The complete table is in [Dyniak wire protocols](../protocols/dyniak.md#http-routes).
```

## Step 5: the same operation over PBC

Production clients use PBC -- the binary protocol -- because it is
lower overhead and it is what the Riak client libraries speak by
default. You almost never hand-assemble PBC frames; you point a Riak
client library at the `pbc_listen` port and it does the framing for
you. The framing itself is simple: a 4-byte big-endian length, a
1-byte message code, then a `prost`-encoded protobuf body.

Here is the same put/get using the Python `riak` client:

```python
import riak

client = riak.RiakClient(host='127.0.0.1', pb_port=8087)
bucket = client.bucket('users')

obj = bucket.new('alice', data='Alice Liddell')
obj.content_type = 'text/plain'
obj.store()

fetched = bucket.get('alice')
print(fetched.data)     # Alice Liddell
```

The client library issues an `RpbPutReq` and an `RpbGetReq` over the
socket; Dyniak decodes each frame, routes the key through the Dynomite
ring, reads or writes the Noxu environment, and encodes the framed
reply. The [wire protocol chapter](../protocols/dyniak.md#pbc-message-surface)
lists the full PBC message surface.

## Step 6: choose your quorum

Riak's per-request quorum knobs are honoured. Ask for a read that
requires two replicas to agree:

```python
fetched = bucket.get('alice', r=2)
```

Or over HTTP with a query parameter:

```sh
curl -s 'http://127.0.0.1:8098/buckets/users/keys/alice?r=2'
```

The knobs are `r`, `w`, `pr`, `pw`, `dw`, and `rw`, with the same
meaning they carry in Riak: how many replicas (or primary replicas)
must respond before the request is considered done. On a single-node
cluster every quorum is trivially satisfied; the knobs earn their keep
once you have peers, which is the next step. The replication and
consistency model underneath these knobs is described in
[Consistency](../architecture/consistency.md).

## Step 7: grow to a small cluster

A single node is a store, not a cluster. To get replication, add peers.
Each node runs its own `dynomited` with its own `noxu_path:` and its
own token, and they discover one another over gossip on the `dyn_listen`
peer plane. A minimal three-node config differs from the single-node
one in the `tokens:` value (each node owns a different slice of the
ring) and the peer wiring.

Node A (`dyniak-a.yml`):

```yaml
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '0'
  data_store: dyniak
  noxu_path: /tmp/dyniak-a
  servers:
  - 127.0.0.1:6379:1
  riak:
    pbc_listen: 127.0.0.1:8087
    http_listen: 127.0.0.1:8098
```

Nodes B and C are identical except for their listener ports, their
`noxu_path:`, and their `tokens:` (for example `1431655765` and
`2863311530` to split a three-way ring evenly). The peers find each
other through the seed list and gossip; the mechanics of standing up
the ring are the same as any Dynomite cluster and are covered in
[Your First Cluster](../getting-started/first-cluster.md).

Once the ring is formed, a write to any node with `w=2` is replicated
to the owning peers before the write is acknowledged, and a read with
`r=2` waits for two replicas to agree. The client still connects to any
one node and never learns the topology.

```admonish tip title="Default distribution is random_slicing"
A Riak-mode pool defaults its `distribution:` to `random_slicing` when
a Riak listener is configured, so a 3-of-4 host topology cannot
silently leave a quarter of the ring unowned -- the classic Riak
behaviour. You can force the legacy `vnode` mode explicitly; see
[Distribution Modes](../operations/distribution.md).
```

## Where to next

You now have a running Dyniak node (or a small cluster) and can read
and write objects over both wire protocols. From here:

* [Buckets, Keys, and Objects](./objects.md) explains the data model,
  content types, siblings, and bucket properties in depth.
* [Convergent Data Types](./crdts.md) shows how to store counters,
  sets, and maps that merge cleanly under concurrent writes.
* [Distributed Transactions](./transactions.md) covers multi-key atomic
  updates.
* For operator concerns -- listener tuning, AAE cadence, 2i storage
  layout -- see [Riak mode ops](../operations/riak.md).
