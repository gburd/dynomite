# Memcache

The `memcache` data store speaks the Memcached ASCII (text) protocol.
The implementation lives in
[`dynomite::proto::memcache`](../../crate/dynomite/proto/memcache/index.html).

## Wire format

Memcached commands are newline-terminated ASCII. The engine consumes
the stream with one byte-driven state machine for requests
(`memcache_parse_req`) and one for responses (`memcache_parse_rsp`).
Like the RESP parser, the memcache parser is total: arbitrary input
either yields a complete message or a parse error.

```
get user:42\r\n
set user:42 0 0 3\r\nabc\r\n
```

## Command handling

Parsed requests are classified into `MsgType` values covering the
storage, retrieval, arithmetic, and lifecycle commands: `get`,
`gets`, `set`, `add`, `replace`, `append`, `prepend`, `cas`,
`delete`, `incr`, `decr`, `touch`, and `quit`.

Classification drives the same machinery as the RESP path:

* **Routing** by key (or hash tag) through the configured
  distribution.
* **Multi-key fragmentation** for the multi-key `get` / `gets`
  forms, with per-peer reply coalescing.

## Limitations

* The binary protocol is not implemented; only the ASCII protocol.
* Memcache backends are not authenticated. `AUTH` is RESP-specific,
  and memcache binary SASL is not implemented, so `redis_requirepass:`
  is ignored for a memcache `data_store`.
* Read repair is a placeholder for memcache: memcache has no
  versioned reconciliation the way the RESP path does.
