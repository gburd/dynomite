# Tutorial: Vector + Text + Regex Search via valkey-cli

This walkthrough takes a fresh checkout to a running search stack on
your laptop in about ten minutes. Every command in this chapter is
copy-paste runnable against the binary you build below; the responses
you see here are the verbatim wire output captured while writing the
chapter.

By the end you will:

- have a single `dynomited` node listening on `127.0.0.1:18402`, backed
  by a local Redis on `127.0.0.1:22122`;
- have run k-NN vector search, trigram substring search, and TRE
  approximate regex search via `valkey-cli`;
- understand the wire-protocol shape of each FT.* response and how to
  encode the binary float blob a vector query needs.

## 1. Prerequisites

You need:

- Rust 1.90 or newer (`cargo --version` should report `>= 1.90`).
- `valkey-cli` and `valkey-server` (any 6.x, 7.x, or 8.x release works).
- `python3` (used as a small encoder for binary float blobs).
- On Linux, `libopenblas-dev` (Debian/Ubuntu) or the equivalent
  `openblas` package on your distro. The vector engine pulls in
  `turbovec`, which links against OpenBLAS at the system layer.
- The `tre-sys` git submodule, which the FT.REGEX path uses for
  approximate-regex matching.

If you have Nix, `nix develop` from the repo root pins all of the
above. Otherwise, install the system packages explicitly:

```sh
# Debian / Ubuntu
sudo apt-get install -y valkey-server redis-tools libopenblas-dev python3

# macOS (Homebrew)
brew install redis openblas python3
```

Pull the submodule the first time you build:

```sh
git submodule update --init crates/tre-sys/vendor/tre
```

## 2. Build and start dynomited

Build the server with the `riak` feature enabled. The feature pulls in
the storage and admin paths the FT.* commands need:

```sh
cargo build --release -p dynomited --features riak
```

Start a local Redis on the default backing port (`22122`). Dynomite
proxies non-FT.* keys to this Redis; FT.* commands are answered from
the in-process vector + text registry, so the backing Redis only sees
your raw `HSET` / `HGET` / `DEL` traffic:

```sh
mkdir -p /tmp/dynomite-tutorial
cd /tmp/dynomite-tutorial
valkey-server --port 22122 --daemonize yes \
    --dir /tmp/dynomite-tutorial \
    --pidfile /tmp/dynomite-tutorial/redis.pid \
    --logfile /tmp/dynomite-tutorial/redis.log
valkey-cli -p 22122 PING
```

Expected output:

```text
PONG
```

Drop a minimal dynomited config in the same scratch directory:

```sh
cat > /tmp/dynomite-tutorial/dynomite.yml <<'EOF'
tutorial:
  listen: 127.0.0.1:18402
  dyn_listen: 127.0.0.1:18401
  stats_listen: 127.0.0.1:18422
  data_store: 0
  servers:
  - 127.0.0.1:22122:1
  tokens: '437425602'
  dyn_seed_provider: simple_provider
EOF
```

The keys are:

- `listen:` is the client-facing RESP port. Your `valkey-cli` connects
  here.
- `dyn_listen:` is the inter-node DYN_O_MITE port. We are not running
  a cluster yet but the field is required.
- `stats_listen:` is the HTTP stats endpoint (`/stats`,
  `/cluster-info.txt`, Prometheus metrics).
- `data_store: 0` selects the Redis backing family.
- `servers:` lists the backing-store endpoints. We point at the local
  Redis from the previous step.

Start dynomited from the workspace root, with the absolute path to the
config file. It runs in the foreground; background it with `&` so the
shell stays usable:

```sh
cd /home/<you>/ws/dynomite        # the repo root
nohup ./target/release/dynomited \
    -c /tmp/dynomite-tutorial/dynomite.yml \
    > /tmp/dynomite-tutorial/dynomited.log 2>&1 &
sleep 2
valkey-cli -p 18402 PING
```

Expected output:

```text
PONG
```

If the `PONG` does not come back, inspect
`/tmp/dynomite-tutorial/dynomited.log`. The most common cause is one
of the three ports above being in use; pick a different number for the
listener that conflicts and re-edit the YAML.

## 3. Vector search: FT.CREATE, HSET, FT.SEARCH KNN

Create a four-dimensional cosine HNSW index against the `docs:`
prefix. `title` is a TEXT field and `vec` is the vector field:

```sh
valkey-cli -p 18402 FT.CREATE myidx \
    ON HASH PREFIX 1 docs: \
    SCHEMA \
        title TEXT \
        vec VECTOR HNSW 6 TYPE FLOAT32 DIM 4 DISTANCE_METRIC COSINE
```

Expected output:

```text
OK
```

The `HNSW 6` token reads as "HNSW algorithm with six attribute pairs
following": `TYPE FLOAT32`, `DIM 4`, and `DISTANCE_METRIC COSINE`.
RediSearch chose this counted-attribute shape; we accept it
unchanged.

Confirm the index registered, then peek at its metadata:

```sh
valkey-cli -p 18402 FT.LIST
valkey-cli -p 18402 FT.INFO myidx
```

Expected output:

```text
myidx
index_name
myidx
algorithm
HNSW
distance_metric
COSINE
vector_field
vec
vector_type
FLOAT32
dim
4
prefixes
docs:
schema_fields
title
TEXT
num_docs
0
tracked_rows
0
```

### Encode a vector

A `FLOAT32` `DIM 4` vector is sixteen bytes: four IEEE-754 single
precision floats, each four bytes, all in little-endian order.
For `[1.0, 0.0, 0.0, 0.0]` the bytes are:

```sh
python3 -c "import struct; print(struct.pack('<4f', 1.0, 0.0, 0.0, 0.0).hex())"
```

```text
0000803f000000000000000000000000
```

Read left to right: `0x3f800000` is the IEEE-754 encoding of `1.0`,
written little-endian as `00 00 80 3f`; the next twelve bytes are
three zero floats.

If you do not have Python handy, `printf` works for fixed values:

```sh
printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' | xxd
```

```text
00000000: 0000 803f 0000 0000 0000 0000 0000 0000  ...?............
```

The mistake to avoid: native byte order is not always little-endian.
Use Python's `<` format prefix or `struct.pack('<4f', ...)`; `>4f`
will silently produce big-endian bytes that decode as different
floats, and the index will store and search the wrong values.

### Insert documents

`valkey-cli -x` reads its last argument from standard input. Pipe the
binary blob through it so the shell never has to handle the embedded
NUL bytes:

```sh
python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f', 1.0, 0.0, 0.0, 0.0))" \
    | valkey-cli -p 18402 -x HSET docs:1 title "first hello world" vec

python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f', 0.9, 0.1, 0.0, 0.0))" \
    | valkey-cli -p 18402 -x HSET docs:2 title "second hello there" vec

python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f', 0.0, 1.0, 0.0, 0.0))" \
    | valkey-cli -p 18402 -x HSET docs:3 title "third orthogonal" vec
```

Expected output (one line per HSET):

```text
2
2
2
```

The `2` is the count of fields written by HSET (`title` and `vec`),
not a status code. If you see `-ERR dimension mismatch: index=4, payload=N`
your packed vector was not exactly sixteen bytes; double-check the
`<4f` format and that you sent four floats.

### Run a k-NN query

The query vector goes on the wire the same way: sixteen bytes of
little-endian f32. Place `PARAMS 2 blob` at the end of the command so
the binary blob piped via `-x` lands in the right argument slot:

```sh
python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f', 1.0, 0.0, 0.0, 0.0))" \
    | valkey-cli -p 18402 -x FT.SEARCH myidx '*=>[KNN 3 @vec $blob]' PARAMS 2 blob
```

Expected output:

```text
3
docs:1
__vec_score
0.000000
title
first hello world
docs:2
__vec_score
0.006116
title
second hello there
docs:3
__vec_score
1.000000
title
third orthogonal
```

The wire shape is RESP `*7`: total count, then three (doc-id, fields)
pairs. Each fields array contains the implicit `__vec_score` (cosine
distance, smaller is closer) plus every metadata field the index
stores. Hits are returned closest-first.

Add a `RETURN <n> <field>...` clause to project a subset, and a
`LIMIT <off> <cnt>` clause to paginate. Always keep `PARAMS` last
when you pipe the vector via `-x`:

```sh
python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f', 1.0, 0.0, 0.0, 0.0))" \
    | valkey-cli -p 18402 -x FT.SEARCH myidx '*=>[KNN 3 @vec $blob]' \
        RETURN 1 title LIMIT 0 5 PARAMS 2 blob
```

Expected output (only `title` is projected; `__vec_score` is omitted):

```text
3
docs:1
title
first hello world
docs:2
title
second hello there
docs:3
title
third orthogonal
```

## 4. Text search: trigram substring lookup

The `title` field above was already declared `TEXT`. Substring queries
use the `@field:substring` form. Insert a few more rows so the result
sets are interesting:

```sh
for i in 10 11 12 13; do
    case $i in
      10) t="hello world" ;;
      11) t="helo world" ;;
      12) t="helllo world" ;;
      13) t="hxllo world" ;;
    esac
    python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f', 0.${i}, 0.0, 0.0, 0.0))" \
        | valkey-cli -p 18402 -x HSET docs:$i title "$t" vec
done
```

Expected output (one `2` per row).

Run the substring query:

```sh
valkey-cli -p 18402 FT.SEARCH myidx '@title:hello'
```

Expected output:

```text
3
docs:1
title
first hello world
docs:10
title
hello world
docs:2
title
second hello there
```

Notice that `helo`, `helllo`, and `hxllo` are not matches: the text
path is exact substring, not approximate. For that, jump to FT.REGEX
in the next section.

### Inspect the trigram funnel via FT.EXPLAIN

dynomited does not implement Redis MONITOR (the proxy semantics make
that command impossible to replay faithfully across the cluster).
Instead, FT.EXPLAIN prints the trigrams the planner extracted from the
query string. That is the same trigram set the funnel uses to walk
the postings list:

```sh
valkey-cli -p 18402 FT.EXPLAIN myidx '@title:hello'
```

Expected output:

```text
@title: SUBSTRING
  field: title
  query: hello
  index: trigram+bloom
  trigrams: ["hel", "ell", "llo"]
```

Three trigrams of three bytes each. Each trigram has a postings list
of doc IDs that contain it, the planner intersects them, and a final
exact-substring recheck filters the candidate set.

Compare with a vector EXPLAIN:

```sh
valkey-cli -p 18402 FT.EXPLAIN myidx '*=>[KNN 5 @vec $blob]'
```

Expected output:

```text
VECTOR KNN
  index: myidx
  algorithm: HNSW
  metric: COSINE
  field: vec
  k: 5
  dim: 4
  param: $blob
```

## 5. Regex search: FT.REGEX with K=0, K=1, K=2

`FT.REGEX <idx> <field> <pattern> [K=<n>]` is a Dynomite extension
on top of the RediSearch surface. `K=0` is exact match, `K=1` allows
one edit (insert, delete, or substitute), `K=2` allows two. The
underlying engine is TRE (the same library that powers `agrep(1)`).

`K=0`: exact match. Only the four documents that literally contain
`hello` come back.

```sh
valkey-cli -p 18402 FT.REGEX myidx title 'hello' K=0
```

Expected output:

```text
3
docs:1
title
first hello world
docs:2
title
second hello there
docs:10
title
hello world
```

`K=1`: one typo. The result set picks up `helo`, `helllo`, and
`hxllo`:

```sh
valkey-cli -p 18402 FT.REGEX myidx title 'hello' K=1
```

Expected output:

```text
6
docs:1
title
first hello world
docs:2
title
second hello there
docs:10
title
hello world
docs:11
title
helo world
docs:12
title
helllo world
docs:13
title
hxllo world
```

`K=2`: two typos. With this corpus the result set is the same as
`K=1`; on a larger corpus you would see additional matches drift in:

```sh
valkey-cli -p 18402 FT.REGEX myidx title 'hello' K=2
```

Real regex syntax works too. The pattern is anchored by the underlying
TRE engine; group, alternation, and character class metacharacters all
behave the way `regex(7)` documents:

```sh
valkey-cli -p 18402 FT.REGEX myidx title 'h(e|x)l*o' K=0
```

Expected output:

```text
6
docs:1
title
first hello world
docs:2
title
second hello there
docs:10
title
hello world
docs:11
title
helo world
docs:12
title
helllo world
docs:13
title
hxllo world
```

`K` must be a non-negative integer. A negative value is a parse
error:

```sh
valkey-cli -p 18402 FT.REGEX myidx title 'hello' K=-1
```

Expected output:

```text
ERR syntax error: FT.REGEX: invalid K= value -1
```

## 6. Combined queries: text plus vector ranking

A document with both a TEXT body and a VECTOR field exposes two query
shapes against the same row. The current build keeps the two queries
separate at the wire layer; the application combines them.

Add a body-bearing document:

```sh
valkey-cli -p 18402 FT.ALTER myidx ADD body TEXT
python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f', 0.5, 0.5, 0.0, 0.0))" \
    | valkey-cli -p 18402 -x HSET docs:30 \
        title "with body" \
        body "this body talks about machine learning" \
        vec
```

Expected output:

```text
OK
3
```

Filter by body keyword:

```sh
valkey-cli -p 18402 FT.SEARCH myidx '@body:machine'
```

Expected output:

```text
1
docs:30
body
this body talks about machine learning
```

Rank the same set by vector distance to a query point. The doc
matching the body filter should come back first if its vector is
closest:

```sh
python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f', 0.5, 0.5, 0.0, 0.0))" \
    | valkey-cli -p 18402 -x FT.SEARCH myidx '*=>[KNN 3 @vec $blob]' PARAMS 2 blob
```

Expected output (truncated; closer matches first):

```text
3
docs:30
__vec_score
0.000000
body
this body talks about machine learning
title
with body
...
```

To compose the two: run the text query, take its doc IDs, then run
KNN and filter the KNN result down to that doc-ID set client-side.
A native hybrid query syntax (RediSearch's `(@body:machine)=>[KNN ...]`
pre-filter form) is on the roadmap; track it via the
`stage/ft-search-distributed` worktree. Until that lands, the two-
phase application pattern above is the correct shape.

## 7. Operations: FT.INFO, FT.LIST, FT.DROPINDEX, FT.ALTER

`FT.INFO <name>` prints schema metadata, index counters, and the
configured prefixes:

```sh
valkey-cli -p 18402 FT.INFO myidx
```

The output is a flat key/value array. The integer fields you will
care about most:

- `num_docs`: how many documents the underlying engine considers
  live.
- `tracked_rows`: rows the registry has indexed so far. Drift between
  the two indicates stale entries from a partial flush; the AAE
  scheduler reconciles this on the next pass.

`FT.LIST` (or its alias `FT._LIST`) returns every registered index
name:

```sh
valkey-cli -p 18402 FT.LIST
```

Expected output:

```text
myidx
```

`FT.ALTER <idx> ADD <field> <type>` adds a metadata field to a live
index. Subsequent HSETs that supply the new field get indexed; rows
written before the ALTER do not retroactively pick it up. Supported
types are `TEXT` and `TAG`:

```sh
valkey-cli -p 18402 FT.ALTER myidx ADD genre TAG
```

`FT.DROPINDEX <idx> [DD]` removes the index. With `DD`, the
underlying documents are deleted from the backing store; without it,
the rows survive and you can re-index them with another FT.CREATE:

```sh
valkey-cli -p 18402 FT.CREATE empty ON HASH PREFIX 1 emp: \
    SCHEMA body TEXT vec VECTOR HNSW 6 TYPE FLOAT32 DIM 4 \
    DISTANCE_METRIC COSINE
valkey-cli -p 18402 FT.DROPINDEX empty
valkey-cli -p 18402 FT.LIST
```

Expected output:

```text
OK
OK
myidx
```

## 8. Common gotchas

The patterns below have caught everyone porting their first
RediSearch app to dynomite:

- **Vector bytes are little-endian f32, packed end-to-end.** A
  `DIM 4 TYPE FLOAT32` index expects exactly sixteen bytes per
  vector, four bytes per dim, low byte first. The Python idiom is
  `struct.pack('<4f', a, b, c, d)`. The `<` is mandatory; the
  default is host byte order.

- **Wrong vector length is a clean `-ERR`, not a corruption.** An
  HSET that ships fifteen, seventeen, or zero bytes for a four-dim
  index produces:

  ```text
  -ERR dimension mismatch: index=4, payload=N
  ```

  Nothing is written. Fix the encoder and retry.

- **FT.SEARCH on an empty index returns zero results, not an
  error.** A freshly-created index responds with `*0` to any query.
  The error path is reserved for parse and schema problems.

- **K must be `>= 0` in FT.REGEX.** `K=-1`, `K=foo`, and `K=` all
  parse-fail with a structured `-ERR`.

- **Text fields are byte-level, with UTF-8 implicit.** Trigrams are
  three raw bytes, not three Unicode codepoints. ASCII substrings
  match exactly. Multi-byte sequences (latin-1 high bytes,
  CJK codepoints, emoji) work, but only when the query bytes are an
  exact contiguous prefix of the encoded form. Match `cafe`, not
  the U+00E9 character; the latter would split across two trigrams
  the user did not type.

- **`valkey-cli -x` reads stdin into the LAST argument.** Always put
  the binary blob's PARAMS clause at the end of your FT.SEARCH
  command line. Putting `RETURN`, `LIMIT`, or `SORTBY` after
  `PARAMS 2 blob` will not work because the blob lands one slot too
  early.

- **MONITOR is not implemented.** The proxy layer cannot replay a
  monitor stream faithfully across the cluster. Use FT.EXPLAIN to
  inspect the planner output, the stats endpoint
  (`http://127.0.0.1:18422/stats`) for counters, and `dyn-admin
  metrics` for the Prometheus surface.

## 9. Cleanup

When you are done, stop the server and the backing Redis:

```sh
pkill -f "dynomited.*tutorial"
valkey-cli -p 22122 SHUTDOWN NOSAVE 2>/dev/null || true
rm -rf /tmp/dynomite-tutorial
```

## 10. Multi-node preview

A single dynomite node already accepts every FT.* command. Three
dynomited processes form a cluster by sharing a `dyn_seeds:` list,
so writes hash to the right node and reads return from a quorum.

Cluster topology is documented in
[`docs/book/src/operations/distribution.md`](./operations/distribution.md);
example three-node configurations live in
`crates/dynomited/conf/redis_rack{1,2,3}_node.yml`. In the current
build each node maintains its own FT.* registry: KNN, substring,
and regex queries answer from the local node only. The cluster-
wide broadcast that fans an FT.SEARCH across every primary peer
and merges the top-K hits is in flight on the
`stage/ft-search-distributed` branch. Watch
[`docs/dynvec/fold-into-redis-path.md`](https://codeberg.org/gregburd/dynomite/src/branch/main/docs/dynvec/fold-into-redis-path.md)
for the design overview and the corresponding journal entry for
status.

## 11. Where to go from here

- **Embedding cookbook:** [`embedding/cookbook.md`](./embedding/cookbook.md)
  covers spinning a dynomite server up inside another Rust binary,
  with full FT.* support against a custom datastore trait.
- **Architecture:** [`architecture.md`](./architecture.md) walks the
  request lifecycle from the listener through the dispatcher, the
  cluster routing layer, and the backend pool.
- **Vector design:** [`docs/dynvec/architecture.md`](https://codeberg.org/gregburd/dynomite/src/branch/main/docs/dynvec/architecture.md)
  documents the dynvec engine that backs HSET indexing, HNSW graph
  layout, and the encoding/distance modules.
- **Text design:** [`docs/dyntext/design.md`](https://codeberg.org/gregburd/dynomite/src/branch/main/docs/dyntext/design.md)
  documents the trigram + bloom funnel that backs `@field:substring`
  and FT.REGEX.
- **Production-readiness evidence:** the chaos-test reports under
  [`dist/chaos-reports/v0.1.0/`](https://codeberg.org/gregburd/dynomite/src/branch/main/dist/chaos-reports/v0.1.0)
  contain the full failure-injection results for the multi-host
  pre-tag pass. `multi-host-pass-8-redis.md` is the most recent
  Redis-backend run.
