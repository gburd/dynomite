# Hooks and traits

Five public traits expose every cross-cutting concern that an
embedder may want to override. Each has at least one default
implementation shipped in-crate so a vanilla embedding works with
zero hook boilerplate.

The traits are object-safe (`Box<dyn Trait>` works) and `Send +
Sync` so they can be shared across tokio tasks. All async methods
return boxed futures (`Pin<Box<dyn Future + Send + 'a>>`) to keep
the trait object-safe; in practice, callers use the
`#[async_trait]` macro.

## `Datastore`

The datastore trait is the abstraction over Redis / Memcached /
anything-else that a Dynomite node fronts.

```text
#[async_trait]
pub trait Datastore: Send + Sync + 'static {
    async fn connect(&self) -> Result<Box<dyn DatastoreConn>, DatastoreError>;
    fn protocol(&self) -> Protocol;        // Redis | Memcache | Custom
    fn supports(&self, cmd: MsgType) -> bool;
}

#[async_trait]
pub trait DatastoreConn: Send {
    async fn dispatch(&mut self, req: Msg) -> Result<Msg, DatastoreError>;
    async fn close(self: Box<Self>) -> Result<(), DatastoreError>;
}
```

**When to implement.** Front any storage with the Dynomite gossip /
ring / quorum layer. The typical case is keeping the default Redis
or Memcache implementation; custom impls are useful when the
backing store lives in-process (no socket round-trip) or speaks a
private protocol.

**Default impls.** `dynomite::embed::hooks::RedisDatastore` and
`dynomite::embed::hooks::MemcacheDatastore` ship in-crate (plus
`MemoryDatastore` for in-process use), each selected by
`data_store:` in the YAML schema.

**Custom example.** An in-process B-Tree backend
(`InMemoryBTreeDatastore`) that implements `DatastoreConn::dispatch`
by calling directly into a `BTreeMap<Bytes, Bytes>` guarded by an
`async_lock::RwLock`. Used in tests to remove socket and protocol
overhead, and in single-process embeddings where the data lives
next to the application logic.

## `SeedsProvider`

Service-discovery integration. The seeds provider produces the
list of peer Dynomite nodes at startup and on a configurable
refresh interval.

```text
#[async_trait]
pub trait SeedsProvider: Send + Sync + 'static {
    async fn fetch(&self) -> Result<Vec<Seed>, SeedsError>;
    fn refresh_interval(&self) -> Duration { Duration::from_secs(30) }
}

pub struct Seed {
    pub addr: SocketAddr,
    pub dc: String,
    pub rack: String,
    pub tokens: Vec<DynToken>,
}
```

**When to implement.** Any cluster topology that does not fit a
static seeds list. K8s, Consul, and AWS-tagged-EC2 are the common
cases.

**Default impls** (all under `dynomite::embed::seeds`):

* `SimpleSeedsProvider` - reads `dyn_seeds:` from the YAML.
* `DnsSeedsProvider` - resolves an A / AAAA record and treats each
  resolved address as a seed.
* `FloridaSeedsProvider` - HTTP poller for the Florida service that
  the reference C version integrates with.

**Custom example.** A `K8sSeedsProvider` that uses the kube-rs
`Api::watch` stream over a `Service` selector. Each event mutates
an internal `Vec<Seed>`; `fetch` returns a clone. The refresh
interval is irrelevant when the watch stream pushes updates, so
`refresh_interval` returns `Duration::MAX`.

## `Transport`

`Transport` lives in `dynomite::io::reactor`. It captures any
`AsyncRead + AsyncWrite + Send + Unpin` byte stream the engine
should accept connections on.

```text
pub trait Transport: AsyncRead + AsyncWrite + Send + Unpin {
    fn role(&self) -> ConnRole;
    fn peer_addr(&self) -> Option<SocketAddr>;
}
```

The embedding builder accepts a transport **factory** rather than a
single transport, because every accepted connection needs its own
transport instance:

```text
#[async_trait]
pub trait TransportListener: Send + Sync + 'static {
    async fn accept(&self) -> Result<Box<dyn Transport>, TransportError>;
    fn local_addr(&self) -> Option<SocketAddr>;
}
```

**When to implement.** Anything beyond TCP and QUIC: Unix domain
sockets, mutually authenticated TLS, in-memory pipes for tests, and
embedded message-bus integrations.

**Default impls.** `TcpTransport` and the QUIC variant
(`QuicTransport` / `QuicListener`) both ship in-crate, with
their listener equivalents wired into the embedding builder.

**Custom example.** A `UnixDomainTransport` whose `role` is
`ConnRole::Client` and whose `peer_addr` is `None`. Useful for
co-located producer / consumer processes where TCP loopback
overhead is unwanted. A second use case is `MutualTlsTransport`,
which wraps a `tokio_rustls::TlsStream` after a client cert
verification step that calls into an internal CA.

## `CryptoProvider`

HSM / KMS abstraction. The default in-crate implementation is the
existing `dynomite::crypto::Crypto`, which uses OpenSSL with a
PEM-loaded RSA private key.

```text
pub trait CryptoProvider: Send + Sync + 'static {
    fn rsa_size(&self) -> usize;
    fn rsa_encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError>;
    fn rsa_decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError>;
    fn aes_encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError>;
    fn aes_decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError>;
    fn aes_key(&self) -> &[u8; 32];
}
```

**When to implement.** Any deployment where the RSA private key
must not leave a hardware boundary. The provider holds a handle to
the HSM / KMS session; `rsa_decrypt` calls the device.

**Default impl.** `dynomite::embed::crypto::OpensslCryptoProvider`
is a thin newtype around the existing `dynomite::crypto::Crypto`.
Selected automatically when the YAML schema configures
`pem_key_file`.

**Custom example.** An `AwsKmsCryptoProvider` that stores only the
KMS key ARN locally and forwards `rsa_decrypt` calls to the
`Decrypt` API. AES operations stay local on the AES key derived by
the DNODE handshake; only RSA is offloaded.

## `MetricsSink`

Exporter abstraction. The default sink is the built-in REST endpoint
already implemented in `dynomite::stats::rest`.

```text
#[async_trait]
pub trait MetricsSink: Send + Sync + 'static {
    async fn emit(&self, snapshot: &Snapshot) -> Result<(), MetricsError>;
    fn flush_interval(&self) -> Duration { Duration::from_secs(10) }
    fn manifest(&self) -> StatsManifest { StatsManifest::default() }
}
```

The aggregator periodically calls `emit` with the latest
snapshot. Implementations are responsible for any wire-format
conversion.

**When to implement.** Any monitoring backend that is not the
built-in JSON REST endpoint.

**Default impl.** `dynomite::embed::metrics::RestMetricsSink` -
mounts the JSON snapshot at the address from `stats_listen:`.

**Custom examples:**

* `PrometheusMetricsSink` - converts each `Snapshot` to the
  Prometheus exposition format and serves it on `/metrics`. Counters
  in the snapshot become `_total`; histograms are exported as
  `_bucket` series using the bucket boundaries from `Snapshot`.
* `OtelMetricsSink` - sends the snapshot through an
  `opentelemetry-otlp` exporter. `flush_interval` is set to match
  the OTel collector's expected push cadence (typically 60s).

## Discoverability

Each trait lives in `crates/dynomite/src/embed/hooks.rs` and is
re-exported at [`dynomite::embed`]. A typical embedder writes
`use dynomite::embed::SimpleSeedsProvider;` without reaching into
the internals.

Default implementations shipped in-crate:

* `dynomite::embed::MemoryDatastore` - in-process datastore for
  examples and tests.
* `dynomite::embed::RedisDatastore` / `MemcacheDatastore` -
  protocol-tagged datastores that carry the backend target; the
  wire protocol bridge runs in the dispatcher path.
* `dynomite::embed::SimpleSeedsProvider` /
  `DnsSeedsProvider` / `FloridaSeedsProvider` - wrappers around
  the in-crate providers from `dynomite::seeds`.
* `dynomite::embed::RustCryptoProvider` - wraps
  [`dynomite::crypto::Crypto`].
* `dynomite::embed::LoggingMetricsSink` - emits one
  `tracing::info` event per flush.

The `PrometheusMetricsSink` mentioned in the design above is not
shipped by Stage 13 to avoid adding a new workspace dependency;
the `LoggingMetricsSink` is the default sink.
Recorded as a Deviation in `docs/parity.md`.
