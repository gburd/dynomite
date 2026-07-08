# Server lifecycle

`dynomite::embed::Server` is the primary entry point of the
embedding API. This page pins the shape of the builder, the running
handle, and the event stream.

## `ServerBuilder`

```text
impl Server {
    pub fn builder() -> ServerBuilder;
}
```

`ServerBuilder` is a fluent, owned builder. Every YAML-visible field
on `ConfPool` (see `crates/dynomite/src/conf/pool.rs`) gets a
matching setter. Setter names use Rust style; the YAML key is the
documented mapping.

Representative YAML-mirroring setters (the full set is one-to-one
with `ConfPool` fields):

```text
fn listen(self, addr: SocketAddr) -> Self;
fn dyn_listen(self, addr: SocketAddr) -> Self;
fn stats_listen(self, addr: SocketAddr) -> Self;

fn hash(self, h: HashType) -> Self;
fn hash_tag(self, tag: [u8; 2]) -> Self;
fn distribution(self, d: Distribution) -> Self;
fn data_store(self, d: DataStore) -> Self;

fn timeout(self, ms: u32) -> Self;
fn backlog(self, n: u32) -> Self;
fn client_connections(self, n: u32) -> Self;
fn datastore_connections(self, n: u8) -> Self;
fn local_peer_connections(self, n: u8) -> Self;
fn remote_peer_connections(self, n: u8) -> Self;

fn preconnect(self, on: bool) -> Self;
fn auto_eject_hosts(self, on: bool) -> Self;
fn server_retry_timeout_ms(self, ms: u32) -> Self;
fn server_failure_limit(self, n: u32) -> Self;

fn datacenter(self, dc: impl Into<String>) -> Self;
fn rack(self, rack: impl Into<String>) -> Self;
fn tokens(self, t: TokenList) -> Self;
fn dyn_seeds(self, seeds: Vec<ConfDynSeed>) -> Self;
fn dyn_seed_provider(self, name: impl Into<String>) -> Self;

fn read_consistency(self, c: ConsistencyLevel) -> Self;
fn write_consistency(self, c: ConsistencyLevel) -> Self;
fn read_repairs_enabled(self, on: bool) -> Self;

fn secure_server_option(self, opt: SecureServerOption) -> Self;
fn pem_key_file(self, path: PathBuf) -> Self;
fn recon_key_file(self, path: PathBuf) -> Self;
fn recon_iv_file(self, path: PathBuf) -> Self;

fn mbuf_size(self, n: usize) -> Self;
fn max_msgs(self, n: usize) -> Self;
fn conn_msg_rate(self, n: u32) -> Self;
fn gos_interval_ms(self, ms: u32) -> Self;
fn stats_interval_ms(self, ms: u32) -> Self;
fn enable_gossip(self, on: bool) -> Self;
```

Two convenience constructors short-circuit the field-by-field path:

```text
fn from_config(cfg: Config) -> ServerBuilder;
fn from_yaml_file(path: impl AsRef<Path>) -> Result<ServerBuilder, ConfError>;
```

In addition to the YAML-mirroring fields, the builder exposes typed
setters for slots that have no YAML form. These are the only way to
inject a custom hook implementation:

```text
fn transport(self, t: Box<dyn Transport>) -> Self;
fn seeds_provider(self, p: Box<dyn SeedsProvider>) -> Self;
fn datastore(self, d: Box<dyn Datastore>) -> Self;
fn crypto(self, c: Box<dyn CryptoProvider>) -> Self;
fn metrics_sink(self, s: Box<dyn MetricsSink>) -> Self;
```

Each defaults to the in-crate implementation when unset. See
[Hooks and traits](./hooks.md) for the trait contracts.

`build()` validates the assembled config (the same `ConfPool::validate`
used for YAML) and returns:

```text
fn build(self) -> Result<Server, BuildError>;
```

## `Server::start`

```text
impl Server {
    pub fn start(self) -> ServerHandle;
}
```

`start` is non-blocking. It spawns the background tasks
(listeners, gossip, stats, entropy) onto the current tokio runtime
and returns. The returned `ServerHandle` is `Clone + Send + Sync`;
multiple consumers can hold one. Dropping the last `ServerHandle`
does **not** shut the server down - only `shutdown()` does.

## `ServerHandle`

```text
impl ServerHandle {
    pub async fn shutdown(&self) -> Result<(), ShutdownError>;
    pub async fn reload(&self, cfg: Config) -> Result<(), ReloadError>;

    pub fn stats(&self) -> Snapshot;
    pub fn describe_stats(&self) -> StatsManifest;

    pub fn subscribe_events(&self) -> EventStream;

    pub async fn inject_request(&self, req: Msg) -> Result<Msg, InjectError>;

    pub fn peers(&self) -> Vec<PeerSnapshot>;
    pub fn datacenters(&self) -> Vec<DatacenterSnapshot>;
    pub fn ring(&self) -> RingSnapshot;
}
```

* `shutdown` is the graceful path: drain listeners, close peer
  connections, flush stats, then return.
* `reload` is the in-process equivalent of SIGHUP. The new `Config`
  is validated before any state is touched; on failure the running
  config is left untouched.
* `stats` returns the live `dynomite::stats::Snapshot` (already
  used by the REST endpoint).
* `describe_stats` returns a machine-readable manifest of every
  stat the engine exposes (name, kind, unit), suitable for plugging
  into a `MetricsSink`.
* `subscribe_events` returns an async `Stream<Item = ServerEvent>`
  built on `tokio::sync::broadcast`. Lagging consumers receive a
  `ServerEvent::Lagged { missed }` marker and resume.
* `inject_request` hands a parsed `Msg` to the dispatcher as if it
  had arrived from a client, skipping the proxy listener. The
  returned future resolves when the response message is ready. This
  is the entry point used by the in-process tests in `tests/embed_*.rs`.
* `peers`, `datacenters`, and `ring` return point-in-time
  snapshots of cluster topology for inspection and dashboards.

## `ServerEvent`

```text
pub enum ServerEvent {
    PeerUp(PeerId),
    PeerDown { peer: PeerId, reason: PeerDownReason },
    ConfigReloaded { generation: u64 },
    GossipRound { round: u64, peers: u32 },
    AutoEjected { peer: PeerId, failures: u32 },
    RepairTriggered { key_hash: u64, dc: String },
    ConnectionAccepted { conn_id: ConnId, role: ConnRole },
    ConnectionClosed { conn_id: ConnId, reason: CloseReason },
    Lagged { missed: u64 },
}
```

Variants are non-exhaustive at the type level; consumers must use a
wildcard arm.

## Snapshot types

```text
pub struct PeerSnapshot {
    pub id: PeerId,
    pub addr: SocketAddr,
    pub dc: String,
    pub rack: String,
    pub state: PeerState,
    pub tokens: Vec<DynToken>,
    pub last_seen_ms: Msec,
}

pub struct DatacenterSnapshot {
    pub name: String,
    pub racks: Vec<RackSnapshot>,
}

pub struct RingSnapshot {
    pub entries: Vec<RingEntry>, // (token, owner peer id)
    pub generation: u64,
}
```

These are owned, `Clone`, and free of internal locks. Holding one
across an `await` is safe.

## SemVer policy

Pre-1.0, every change in this module produces a `docs/journal/`
entry titled `api-change: <summary>` with a migration note and the
`cargo public-api` diff. 1.0 is cut when the Stage 14 conformance
suite is green, the Stage 15 fuzz soak is clean, and `cargo
public-api` reports zero diffs over two consecutive PRs.

## Implementation

Stage 13 lands the surface above in:

* `crates/dynomite/src/embed/mod.rs` - re-exports.
* `crates/dynomite/src/embed/builder.rs` - [`ServerBuilder`].
* `crates/dynomite/src/embed/server.rs` - [`Server`], [`ServerHandle`].
* `crates/dynomite/src/embed/events.rs` - [`ServerEvent`], [`EventStream`].
* `crates/dynomite/src/embed/snapshots.rs` - [`PeerSnapshot`],
  [`DatacenterSnapshot`], [`RingSnapshot`].
* `crates/dynomite/src/embed/error.rs` -- the `EmbedError` type.

The runtime tasks (gossip, stats, metrics, accept loops) live in
`server.rs` next to [`ServerHandle`]; they are spawned by
[`Server::start`] and observe a single `tokio_util::sync::CancellationToken`.

<!-- API reference-link definitions (resolve the type shorthands above
     to the generated rustdoc on docs.rs). -->
[`ServerBuilder`]: https://docs.rs/dynomite-engine/latest/dynomite/embed/struct.ServerBuilder.html
[`Server`]: https://docs.rs/dynomite-engine/latest/dynomite/embed/struct.Server.html
[`Server::start`]: https://docs.rs/dynomite-engine/latest/dynomite/embed/struct.Server.html
[`ServerHandle`]: https://docs.rs/dynomite-engine/latest/dynomite/embed/struct.ServerHandle.html
[`ServerEvent`]: https://docs.rs/dynomite-engine/latest/dynomite/embed/enum.ServerEvent.html
[`EventStream`]: https://docs.rs/dynomite-engine/latest/dynomite/embed/struct.EventStream.html
[`PeerSnapshot`]: https://docs.rs/dynomite-engine/latest/dynomite/embed/struct.PeerSnapshot.html
[`DatacenterSnapshot`]: https://docs.rs/dynomite-engine/latest/dynomite/embed/struct.DatacenterSnapshot.html
[`RingSnapshot`]: https://docs.rs/dynomite-engine/latest/dynomite/embed/struct.RingSnapshot.html
