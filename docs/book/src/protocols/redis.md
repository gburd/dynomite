# Valkey (RESP)

The `valkey` data store speaks RESP, the request/response protocol
that Valkey and Redis share. The wire format is vendor neutral: the
engine parses and routes RESP without any Valkey- or Redis-specific
assumptions, so the historical `data_store: redis` alias and a
`data_store: valkey` value select the exact same code path.

The implementation lives in
[`dynomite::proto::redis`](https://docs.rs/dynomite-engine/latest/dynomite/proto/redis/index.html).

## Wire format

RESP frames are length-prefixed. A request is an array of bulk
strings; the first element is the command name and the remainder are
its arguments:

```
*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
```

The engine consumes the stream with two byte-driven state machines:
one for requests (`redis_parse_req`) and one for responses
(`redis_parse_rsp`). The parser is total: any byte sequence either
yields a complete message or a parse error, never a panic.

## Command handling

Each parsed request is classified into a `MsgType` (for example
`ReqRedisGet`, `ReqRedisSet`, `ReqRedisDel`). Classification drives:

* **Routing** -- the key (or hash tag, when `hash_tag:` is set)
  selects the target peers through the configured distribution.
* **Multi-key fragmentation** -- commands that carry several keys
  (`MGET`, `MSET`, `DEL`, ...) are split into per-peer fragments and
  the per-fragment replies are coalesced back into one client reply.
* **Read repair** -- on a quorum read whose replicas disagree, the
  repair surface issues corrective writes. Read repair fires on
  `GET`-class reads.
* **Verification** -- request shape is validated before dispatch.

## Backend authentication

When `redis_requirepass:` is set, the engine sends `AUTH <pw>` on
every backend connection immediately after the TCP (or Unix-socket)
handshake. See [Configuration](../configuration.md#backend-authentication).

## Search extension (FT.*)

When `dynomited` is built with the `search` feature, the
`dynomite-search` extension layers the RediSearch `FT.*` command
family on top of the RESP dispatcher: `FT.CREATE`, `FT.SEARCH`,
`FT.REGEX`, `FT.AGGREGATE`, and the `FT.SUG*` suggestion commands,
plus vector KNN. Indexes are cluster-coordinated: a query fans out to
every primary peer and the per-peer hits are merged and ranked. Set
`search_index_dir:` to make the index registry durable across
restarts. See the
[search tutorial](../tutorial-search.md) for an end-to-end walk-through.
