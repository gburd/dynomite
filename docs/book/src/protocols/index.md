# Protocols

A Dynomite node fronts one backend data store, chosen by the
`data_store:` configuration key. Each backend has its own client wire
protocol:

* [Valkey (RESP)](./redis.md) -- `data_store: valkey` (alias `redis`).
  The vendor-neutral RESP request/response protocol.
* [Memcache](./memcache.md) -- `data_store: memcache`. The Memcached
  ASCII text protocol.
* [Dyniak (Riak PBC / HTTP)](./dyniak.md) -- `data_store: dyniak`. The
  built-in Riak-compatible store, exposed over Riak Protocol Buffers
  (PBC) and an HTTP gateway. Requires a binary built with
  `--features riak`.

Independently of the client protocol, nodes talk to one another over
the [DNODE](./dnode.md) framing on the peer plane.
