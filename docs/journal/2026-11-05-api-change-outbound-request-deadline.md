# api-change: OutboundRequest gains a `deadline` field

`dynomite::net::server::OutboundRequest` has a new public field:

```rust
pub deadline: Option<tokio::time::Instant>,
```

A driver (datastore or peer) that still owes a reply past this instant
closes its connection with `ETIMEDOUT` and answers every request
outstanding on it with the connection error
(`Storage: Datastore refused connection` /
`Peer: Peer Node refused connection`), then reconnects.

The cluster dispatcher fills it with `ServerPool::request_deadline`:
`timeout:` for the local datastore, `timeout: + 200` (same DC) or
`+ 5000` (remote DC) for a peer, `+ 20000` more for a write, all scaled
by the runtime timeout factor (`/set_timeout_factor/<n>`) read at
dispatch time. A zero `timeout:` (datastore) or a request that expects
no reply yields `None`.

New public helpers: `ServerPool::server_timeout`,
`ServerPool::dnode_peer_timeout`, `ServerPool::request_deadline`,
`msg::message::msg_tmo_insert`, `net::server::request_timer`,
`net::server::request_timed_out`.

## Migration

Struct-literal constructions of `OutboundRequest` must add the field.
Use `deadline: None` for fire-and-forget traffic (gossip, hints,
best-effort repairs) and for callers that rely on their own timeout;
use `pool.request_deadline(&msg, peer_dc)` for data-plane requests that
should fail like the dispatcher's.
