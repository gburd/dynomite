# TCP

TCP is the default transport and is always compiled in. It carries:

* the **client plane** (the `listen:` address serving RESP or
  memcache),
* the **peer plane** (the `dyn_listen:` address speaking DNODE between
  nodes), and
* the **dyniak** Riak PBC (`riak.pbc_listen`) and HTTP
  (`riak.http_listen`) listeners.

## Addresses

`listen:`, `dyn_listen:`, and `stats_listen:` take an IPv4 or IPv6
`host:port`. A value beginning with `/` is a Unix domain socket path
instead of a TCP address; see
[Configuration](../configuration.md#endpoints-and-unix-sockets).

## TLS

The peer plane runs over TLS when `peer_tls_cert:` and
`peer_tls_key:` are set (with optional mutual-TLS via `peer_tls_ca:`
and per-DC material via `peer_tls_profiles:`). The dyniak listeners
terminate TLS when `riak.tls_cert:` and `riak.tls_key:` are set. In
every case, setting one half of a cert/key pair without the other is
rejected at configuration validation time.

When no TLS material is configured, the corresponding plane runs in
plaintext, matching the historical default.
