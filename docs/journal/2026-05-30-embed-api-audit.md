# 2026-05-30: embed API audit + cookbook polish

Branch: `stage/embed-api-polish` (off `main`).

## Audit findings

The brief asked to confirm that the embedding API supports the
typical "use Dynomite as a crate" flow. The audit walked
`crates/dynomite/src/embed/{mod,builder,server,hooks,events,
snapshots,error}.rs` against the user's checklist and the
implementation tree.

### Already shipped (verified)

* `Server::builder(pool_name)` returning a fluent
  `ServerBuilder`.
* Fluent setters covering every common embedding scenario:
    * `listen`, `dyn_listen`, `stats_listen` (socket addresses).
    * `data_store` (Redis / Memcache / Noxu).
    * `dyn_seeds`, `dyn_seed_provider`, `enable_gossip`,
      `gossip_interval`.
    * `secure_server_option`, `pem_key_file`.
    * `read_consistency`, `write_consistency`,
      `read_repairs_enabled`.
    * `hash`, `mbuf_size`, `max_msgs`, `client_connections`,
      `datastore_connections`, `local_peer_connections`,
      `remote_peer_connections`, `preconnect`,
      `auto_eject_hosts`, `server_failure_limit`,
      `server_retry_timeout`, `timeout`, `stats_interval`.
    * `datacenter`, `rack`, `tokens`, `tokens_str`.
    * Hook setters: `datastore`, `seeds_provider`,
      `crypto_provider`, `metrics_sink`.
* `Server::start` returning `Result<ServerHandle, EmbedError>`.
* `ServerHandle::shutdown().await` (idempotent).
* `ServerHandle::stats() -> Snapshot`,
  `ServerHandle::describe_stats() -> Vec<MetricSpec>`.
* `ServerHandle::events() -> Arc<EventManager>` for
  `ClusterEvent` subscribers, plus `subscribe_events() ->
  EventStream` for the lower-level `ServerEvent` stream.
* `ServerHandle::peers/datacenters/ring` snapshots,
  `inject_request`, `reload`, listen-addr accessors.
* Hook traits: `Datastore`, `SeedsProvider`, `CryptoProvider`,
  `MetricsSink` with default impls
  (`MemoryDatastore` / `RedisDatastore` / `MemcacheDatastore`,
  `SimpleSeedsProvider` / `DnsSeedsProvider` /
  `FloridaSeedsProvider`, `RustCryptoProvider`,
  `LoggingMetricsSink`).

### Gaps found and fixed (additive)

1. **No `ServerHandle::stats_handle() -> Arc<Stats>`**.
   `stats()` returns a `Snapshot`, but the brief explicitly
   asked for an `Arc<Stats>` accessor so embedders can read
   live counters without going through the snapshot path.
   Fixed by adding `ServerHandle::stats_handle()` that clones
   the existing `Arc<Stats>` from `ServerInner`.
2. **No `ServerHandle::join().await`**. `shutdown` cancels
   the task set; there was no way to park on the task set
   without cancelling it first. The "start, then sleep on a
   signal, then shutdown" pattern in the cookbook needed a
   way to park. Fixed by adding `ServerHandle::join()` that
   drains the task set without cancellation.
3. **No `Server::start_with(builder)` convenience**. The
   user's brief suggested `Server::start(builder)` as the
   canonical shape; the existing API was
   `builder.build()?.start().await`. Fixed by adding the
   helper `Server::start_with(builder)` that does build +
   start in one call. The chained form is still available.
4. **`listen("127.0.0.1:0")` failed at runtime with
   `EACCES`**. The builder's `socket_to_listen` helper
   delegated to `ConfListen::parse`, which rejects port `0`
   (a YAML invariant: production configs should not list port
   `0`). The fallback was `127.0.0.1:1`, which then triggered
   a permission-denied bind on Linux. The minimal cookbook
   example wants port `0` (kernel-assigned ephemeral) to
   avoid hard-coding ports. Fixed by adding a
   crate-private `ConfListen::from_socket_addr(addr)` that
   bypasses the YAML port-zero rejection, and by routing the
   builder's setters through it.

### Acknowledged gap (deferred)

5. **No custom-Transport listener slot on the builder**.
   The `Transport` trait is shipped (`TcpTransport`,
   `dynomite::embed::Transport`); writing custom impls is
   straightforward (see
   `embedded_custom_transport_sketch.rs`). What is missing is
   a `Box<dyn TransportListener>` setter so the builder can
   spawn the embedder's listener instead of `TcpListener`.
   The cookbook documents this clearly; the wiring is
   tracked as Stage 14b.

## Changes summary

* `crates/dynomite/src/conf/endpoint.rs`: added
  `pub(crate) fn ConfListen::from_socket_addr` (port-0
  capable, used by the embed builder only).
* `crates/dynomite/src/embed/builder.rs`: routed
  `listen` / `dyn_listen` / `stats_listen` setters through
  the new `from_socket_addr` constructor; removed the
  `socket_to_listen` helper; expanded the rustdoc on the
  three listen-address setters to call out the
  port-0-equals-ephemeral semantics.
* `crates/dynomite/src/embed/server.rs`: added
  `ServerHandle::stats_handle()`, `ServerHandle::join()`,
  expanded the doc on `shutdown` to call out idempotence and
  its interaction with `join`.
* `crates/dynomite/src/embed/mod.rs`: added
  `Server::start_with(builder)` convenience.
* `crates/dynomite/examples/embedded_minimal.rs`: new
  smallest-possible runnable example.
* `crates/dynomite/tests/embed_api.rs`: new test file with
  the four cookbook-mandated tests.
* `docs/book/src/embedding/cookbook.md`: new chapter (eight
  sections plus "production readiness" + "when to embed").
* `docs/book/src/SUMMARY.md`: wired the cookbook in.

## Verification

```
cargo build -p dynomite --examples           # green
cargo run --example embedded_minimal         # green (kernel-assigned ports)
cargo nextest run -p dynomite --test embed_api
  4 passed; 0 failed
cargo nextest run -p dynomite --test stage_13_embed
  10 passed; 0 failed
cargo test --doc -p dynomite                 # green
cargo clippy -p dynomite --all-targets --all-features -- -D warnings  # green
cargo fmt --all -- --check                   # clean
```

## SemVer impact

All changes are additive. `Server::start_with`,
`ServerHandle::stats_handle`, `ServerHandle::join`, and the
port-0 acceptance on the listen setters are new public
methods / accepted inputs; nothing existing changed shape.
The crate-private `ConfListen::from_socket_addr` is invisible
to downstream callers.

`cargo public-api` diff (recorded for the next API-stability
PR pair):

```
+ pub fn dynomite::embed::Server::start_with(builder: dynomite::embed::ServerBuilder)
+ pub fn dynomite::embed::ServerHandle::stats_handle(&self) -> alloc::sync::Arc<dynomite::stats::Stats>
+ pub fn dynomite::embed::ServerHandle::join(&self)
```

(The `Future`-returning signatures are summarised; the full
output goes in the next stage-merge journal entry.)

## Notes

* The cookbook's "Custom Transport" section documents the
  Stage 14b deferral honestly: the embedder can write a
  `Transport` impl today, but plugging a listener factory has
  to wait for the builder slot.
* The cookbook references the committed chaos reports under
  `dist/chaos-reports/v0.1.0/` as the "production readiness"
  evidence. Those reports already cover the four
  backend / transport pairs the engine officially supports.
