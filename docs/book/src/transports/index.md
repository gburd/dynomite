# Transports

A Dynomite node moves bytes over two transports. Both support IPv4
and IPv6, and both are available on the client plane, the peer
(DNODE) plane, and the dyniak Riak listeners.

* [TCP](./tcp.md) -- the historical default, always available.
* [QUIC](./quic.md) -- available when the engine is built with the
  `quic` Cargo feature.

The transport is transport-agnostic above the byte layer: the same
connection state machines and protocol parsers run regardless of
which transport carries the bytes. Only the socket plumbing differs.
