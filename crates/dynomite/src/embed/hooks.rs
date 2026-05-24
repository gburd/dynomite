//! Pluggable hook traits exposed to embedders.
//!
//! The five traits in this module compose the public hook surface
//! that an embedding program plugs into a [`crate::embed::Server`]
//! to override the in-crate defaults: backing datastore, seeds
//! provider, network transport listener, crypto provider, and
//! metrics sink.
//!
//! Every trait is object-safe (`Box<dyn Trait>` works) and
//! `Send + Sync` so the implementor can be shared across tokio
//! tasks. Async methods return [`BoxFuture`] handles to keep the
//! trait dyn-compatible without depending on the `async_trait`
//! crate.
//!
//! Default implementations ship next to each trait. Re-exports
//! at the [`crate::embed`] root mean a typical embedder writes
//! `use dynomite::embed::SimpleSeedsProvider;` without reaching
//! into nested submodules.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use thiserror::Error;

use crate::conf::{ConfDynSeed, DataStore};
use crate::msg::{Msg, MsgType};
use crate::seeds::{
    dns::DnsSeedsProvider as InnerDnsSeedsProvider,
    florida::FloridaSeedsProvider as InnerFloridaSeedsProvider,
    simple::SimpleSeedsProvider as InnerSimpleSeedsProvider, SeedsError, SeedsProvider as RawSeeds,
};
use crate::stats::{describe_stats, MetricSpec, Snapshot};

/// Convenience alias for boxed futures returned by hook traits.
///
/// # Examples
///
/// ```
/// use dynomite::embed::hooks::BoxFuture;
/// fn _adapter() -> BoxFuture<'static, u32> {
///     Box::pin(async { 42 })
/// }
/// ```
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ---------- Datastore -------------------------------------------------------

/// Errors produced by a [`Datastore`] implementation.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DatastoreError {
    /// The datastore declined to handle this request type.
    #[error("unsupported request: {0:?}")]
    Unsupported(MsgType),
    /// The datastore returned an internal error message.
    #[error("datastore error: {0}")]
    Backend(String),
    /// I/O failure talking to the backing store.
    #[error("io error: {0}")]
    Io(String),
}

/// Logical wire protocol exposed by a datastore.
///
/// Mirrors the `data_store:` setting in the YAML config. The
/// `Custom` variant exists for embedders fronting a private
/// protocol; it carries no semantics inside the engine and is
/// reported only through [`Datastore::protocol`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Protocol {
    /// Redis RESP.
    Redis,
    /// Memcached text/binary.
    Memcache,
    /// Embedder-defined protocol.
    Custom,
}

impl From<DataStore> for Protocol {
    fn from(d: DataStore) -> Self {
        match d {
            DataStore::Redis => Protocol::Redis,
            DataStore::Memcache => Protocol::Memcache,
        }
    }
}

/// Backing datastore the engine forwards routed requests to.
///
/// Implementations are kept stateless on the trait surface; live
/// connection state lives behind [`Datastore::dispatch`] in the
/// implementor's chosen pool.
///
/// # Examples
///
/// ```
/// use dynomite::embed::hooks::{Datastore, MemoryDatastore, Protocol};
/// use dynomite::msg::{Msg, MsgType};
/// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
/// let ds = MemoryDatastore::new();
/// assert_eq!(ds.protocol(), Protocol::Custom);
/// let req = Msg::new(1, MsgType::ReqRedisGet, true);
/// let _rsp = ds.dispatch(req).await.unwrap();
/// assert_eq!(ds.dispatch_count(), 1);
/// # });
/// ```
pub trait Datastore: Send + Sync {
    /// Return the wire protocol the datastore speaks.
    fn protocol(&self) -> Protocol;

    /// Predicate used by the dispatcher to short-circuit
    /// commands the backend cannot serve.
    fn supports(&self, _cmd: MsgType) -> bool {
        true
    }

    /// Forward a routed request and return the response message.
    ///
    /// The returned future is `'static`; the caller may move it
    /// across tokio tasks freely.
    fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>>;

    /// Stream the names of every bucket the datastore is aware of,
    /// one [`bytes::Bytes`] per bucket.
    ///
    /// The default implementation returns a one-shot stream that
    /// yields a single [`DatastoreError::Unsupported`] item, so an
    /// existing impl that does not enumerate buckets does not have
    /// to be modified to compile against the new trait surface.
    /// Implementations that can enumerate override this method.
    ///
    /// The returned stream is `'static` and `Send`; transports that
    /// stream chunks of bucket names to clients (PBC frames, HTTP
    /// chunked bodies, ...) consume it from a tokio task.
    fn list_buckets_stream(&self) -> DatastoreByteStream {
        Box::pin(unsupported_byte_stream())
    }

    /// Stream every key in `bucket`, one [`bytes::Bytes`] per key.
    ///
    /// Same default as [`Datastore::list_buckets_stream`]: a
    /// single-item stream carrying [`DatastoreError::Unsupported`].
    fn list_keys_stream(&self, _bucket: &[u8]) -> DatastoreByteStream {
        Box::pin(unsupported_byte_stream())
    }
}

/// In-memory datastore used by examples and integration tests.
///
/// Stores the "command -> response" pairing for the simplest
/// commands without speaking the real protocol. Production
/// embedders use [`RedisDatastore`] or [`MemcacheDatastore`].
#[derive(Debug, Default, Clone)]
pub struct MemoryDatastore {
    inner: Arc<Mutex<MemoryStore>>,
}

#[derive(Debug, Default)]
struct MemoryStore {
    calls: u64,
}

impl MemoryDatastore {
    /// Build a fresh empty store.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::hooks::MemoryDatastore;
    /// let ds = MemoryDatastore::new();
    /// assert_eq!(ds.dispatch_count(), 0);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of times [`Datastore::dispatch`] has been invoked.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::hooks::MemoryDatastore;
    /// let ds = MemoryDatastore::new();
    /// assert_eq!(ds.dispatch_count(), 0);
    /// ```
    #[must_use]
    pub fn dispatch_count(&self) -> u64 {
        self.inner.lock().calls
    }
}

impl Datastore for MemoryDatastore {
    fn protocol(&self) -> Protocol {
        Protocol::Custom
    }

    fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            inner.lock().calls += 1;
            let mut rsp = Msg::new(req.id(), MsgType::Unknown, false);
            rsp.set_parent_id(req.id());
            Ok(rsp)
        })
    }

    fn list_buckets_stream(&self) -> DatastoreByteStream {
        let snapshot = self.list_buckets_snapshot();
        Box::pin(VecByteStream {
            items: snapshot.into_iter(),
        })
    }

    fn list_keys_stream(&self, bucket: &[u8]) -> DatastoreByteStream {
        let snapshot = self.list_keys_snapshot(bucket);
        Box::pin(VecByteStream {
            items: snapshot.into_iter(),
        })
    }
}

/// Default Redis-fronting datastore.
///
/// Stage 13 ships this as a thin marker around the supplied
/// connection target; the actual wire protocol bridge lives in
/// the dispatcher path of [`crate::cluster::dispatch`]. The
/// default impl satisfies the [`Datastore`] contract for the
/// embed surface so an embedder can construct a builder without
/// wiring a custom backend.
#[derive(Debug, Clone)]
pub struct RedisDatastore {
    target: String,
}

impl RedisDatastore {
    /// Build a new Redis-fronting datastore.
    ///
    /// `target` is informational and is reported back through
    /// [`RedisDatastore::target`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::hooks::RedisDatastore;
    /// let r = RedisDatastore::new("127.0.0.1:6379");
    /// assert_eq!(r.target(), "127.0.0.1:6379");
    /// ```
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
        }
    }

    /// Return the configured target string.
    #[must_use]
    pub fn target(&self) -> &str {
        &self.target
    }
}

impl Datastore for RedisDatastore {
    fn protocol(&self) -> Protocol {
        Protocol::Redis
    }

    fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
        Box::pin(async move {
            let mut rsp = Msg::new(req.id(), MsgType::RspRedisStatus, false);
            rsp.set_parent_id(req.id());
            Ok(rsp)
        })
    }
}

/// Default Memcache-fronting datastore.
///
/// Mirrors [`RedisDatastore`] for the Memcache wire protocol.
#[derive(Debug, Clone)]
pub struct MemcacheDatastore {
    target: String,
}

impl MemcacheDatastore {
    /// Build a new Memcache-fronting datastore.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::hooks::MemcacheDatastore;
    /// let m = MemcacheDatastore::new("127.0.0.1:11211");
    /// assert_eq!(m.target(), "127.0.0.1:11211");
    /// ```
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
        }
    }

    /// Return the configured target string.
    #[must_use]
    pub fn target(&self) -> &str {
        &self.target
    }
}

impl Datastore for MemcacheDatastore {
    fn protocol(&self) -> Protocol {
        Protocol::Memcache
    }

    fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
        Box::pin(async move {
            let mut rsp = Msg::new(req.id(), MsgType::RspMcEnd, false);
            rsp.set_parent_id(req.id());
            Ok(rsp)
        })
    }
}

// ---------- SeedsProvider ---------------------------------------------------

/// Pluggable seeds provider.
///
/// The trait is the embed-API mirror of
/// [`crate::seeds::SeedsProvider`]; the in-crate providers are
/// re-exported below as default implementations.
///
/// # Examples
///
/// ```
/// use dynomite::embed::hooks::{SeedsProvider, SimpleSeedsProvider};
/// use dynomite::conf::ConfDynSeed;
/// let sp = SimpleSeedsProvider::new(vec![ConfDynSeed::parse("h:1:r:d:1").unwrap()]);
/// assert_eq!(sp.fetch().unwrap().len(), 1);
/// ```
pub trait SeedsProvider: Send + Sync {
    /// Return the current list of seeds.
    fn fetch(&self) -> Result<Vec<ConfDynSeed>, SeedsError>;

    /// Refresh interval used by the gossip task between calls to
    /// [`SeedsProvider::fetch`].
    fn refresh_interval(&self) -> Duration {
        Duration::from_secs(30)
    }
}

// LegacySeedsAdapter removed: the type would have leaked the
// in-crate `crate::seeds::SeedsProvider` trait onto the public
// API via its generic bound. Embedders that need to lift an
// existing `SeedsProvider` impl wrap it in
// `Box<dyn SeedsProvider>` directly and pass it to
// [`crate::embed::ServerBuilder::seeds_provider`].

/// Re-exported [`crate::seeds::simple::SimpleSeedsProvider`] with
/// the embed [`SeedsProvider`] trait already implemented.
#[derive(Debug, Clone, Default)]
pub struct SimpleSeedsProvider {
    inner: InnerSimpleSeedsProvider,
}

impl SimpleSeedsProvider {
    /// Build a new in-memory provider.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::hooks::{SeedsProvider, SimpleSeedsProvider};
    /// let p = SimpleSeedsProvider::new(Vec::new());
    /// assert_eq!(p.fetch().unwrap().len(), 0);
    /// ```
    #[must_use]
    pub fn new(seeds: Vec<ConfDynSeed>) -> Self {
        Self {
            inner: InnerSimpleSeedsProvider::new(seeds),
        }
    }
}

impl SeedsProvider for SimpleSeedsProvider {
    fn fetch(&self) -> Result<Vec<ConfDynSeed>, SeedsError> {
        self.inner.get_seeds()
    }
}

/// DNS-resolving seeds provider, re-exported under the embed
/// trait.
pub type DnsSeedsProvider = InnerDnsSeedsProvider;

impl SeedsProvider for DnsSeedsProvider {
    fn fetch(&self) -> Result<Vec<ConfDynSeed>, SeedsError> {
        self.get_seeds()
    }
}

/// Florida HTTP seeds provider, re-exported under the embed
/// trait.
pub type FloridaSeedsProvider = InnerFloridaSeedsProvider;

impl SeedsProvider for FloridaSeedsProvider {
    fn fetch(&self) -> Result<Vec<ConfDynSeed>, SeedsError> {
        self.get_seeds()
    }
}

// ---------- CryptoProvider --------------------------------------------------

/// Errors produced by a [`CryptoProvider`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CryptoProviderError {
    /// Encryption failed.
    #[error("encryption failed: {0}")]
    Encrypt(String),
    /// Decryption failed.
    #[error("decryption failed: {0}")]
    Decrypt(String),
    /// The provider was misconfigured (key length, padding, ...).
    #[error("misconfiguration: {0}")]
    Misconfigured(String),
}

/// Pluggable AES + RSA provider for the DNODE peer protocol.
///
/// The default in-crate provider [`RustCryptoProvider`] wraps
/// [`crate::crypto::Crypto`]. HSM / KMS integrations implement
/// the trait against their hardware bridge.
///
/// # Examples
///
/// ```no_run
/// use dynomite::embed::hooks::{CryptoProvider, RustCryptoProvider};
/// use dynomite::crypto::Crypto;
/// // Construct the underlying Crypto from a PEM file at runtime.
/// let crypto = Crypto::from_pem("/etc/dynomite/dynomite.pem").unwrap();
/// let provider = RustCryptoProvider::new(crypto);
/// assert!(provider.rsa_size() > 0);
/// ```
pub trait CryptoProvider: Send + Sync {
    /// Modulus length in bytes for the configured RSA key.
    fn rsa_size(&self) -> usize;

    /// Borrow the AES key buffer used by the DNODE handshake.
    fn aes_key(&self) -> [u8; crate::crypto::AES_KEYLEN];

    /// AES-encrypt `plaintext` under the provider's key.
    fn aes_encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoProviderError>;

    /// AES-decrypt `ciphertext` under the provider's key.
    fn aes_decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoProviderError>;

    /// RSA-encrypt `plaintext` under the provider's public key.
    fn rsa_encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoProviderError>;

    /// RSA-decrypt `ciphertext` under the provider's private key.
    fn rsa_decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoProviderError>;
}

/// Default crypto provider built on the in-crate
/// [`crate::crypto::Crypto`] (RustCrypto-backed AES + RSA).
///
/// Despite the historical "Openssl" name in the design doc, the
/// implementation uses the workspace's RustCrypto stack. Recorded
/// as a Deviation in `docs/parity.md`.
#[derive(Debug)]
pub struct RustCryptoProvider {
    crypto: Arc<crate::crypto::Crypto>,
}

impl RustCryptoProvider {
    /// Wrap an existing [`crate::crypto::Crypto`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dynomite::embed::hooks::{CryptoProvider, RustCryptoProvider};
    /// use dynomite::crypto::Crypto;
    /// // Production embedders load the key from disk:
    /// let crypto = Crypto::from_pem("/etc/dynomite/dynomite.pem").unwrap();
    /// let p = RustCryptoProvider::new(crypto);
    /// assert!(p.rsa_size() >= 128);
    /// ```
    #[must_use]
    pub fn new(crypto: crate::crypto::Crypto) -> Self {
        Self {
            crypto: Arc::new(crypto),
        }
    }

    /// Construct from an [`Arc`]-shared crypto bundle.
    #[must_use]
    pub fn from_arc(crypto: Arc<crate::crypto::Crypto>) -> Self {
        Self { crypto }
    }
}

impl CryptoProvider for RustCryptoProvider {
    fn rsa_size(&self) -> usize {
        self.crypto.rsa_size()
    }

    fn aes_key(&self) -> [u8; crate::crypto::AES_KEYLEN] {
        *self.crypto.aes_key()
    }

    fn aes_encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoProviderError> {
        crate::crypto::Crypto::aes_encrypt(plaintext, self.crypto.aes_key())
            .map_err(|e| CryptoProviderError::Encrypt(e.to_string()))
    }

    fn aes_decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoProviderError> {
        crate::crypto::Crypto::aes_decrypt(ciphertext, self.crypto.aes_key())
            .map_err(|e| CryptoProviderError::Decrypt(e.to_string()))
    }

    fn rsa_encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoProviderError> {
        self.crypto
            .rsa_encrypt(plaintext)
            .map_err(|e| CryptoProviderError::Encrypt(e.to_string()))
    }

    fn rsa_decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoProviderError> {
        self.crypto
            .rsa_decrypt(ciphertext)
            .map_err(|e| CryptoProviderError::Decrypt(e.to_string()))
    }
}

// ---------- MetricsSink -----------------------------------------------------

/// Errors produced by a [`MetricsSink`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MetricsError {
    /// The sink could not flush the snapshot.
    #[error("metrics flush failed: {0}")]
    Flush(String),
}

/// Pluggable metrics exporter.
///
/// The default sink ([`LoggingMetricsSink`]) emits one `tracing`
/// event per flush. A `PrometheusMetricsSink` is intentionally
/// not shipped by default to avoid adding a dependency to the
/// workspace; see the embedding cookbook for the recommended
/// adapter shape. Recorded as a Deviation in `docs/parity.md`.
///
/// # Examples
///
/// ```
/// use dynomite::embed::hooks::{LoggingMetricsSink, MetricsSink};
/// use dynomite::stats::Snapshot;
/// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
/// let sink = LoggingMetricsSink::new("test");
/// sink.emit(&Snapshot::default()).await.unwrap();
/// # });
/// ```
pub trait MetricsSink: Send + Sync {
    /// Push a stats snapshot to the sink.
    fn emit<'a>(&'a self, snapshot: &'a Snapshot) -> BoxFuture<'a, Result<(), MetricsError>>;

    /// Flush interval requested by the sink.
    fn flush_interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    /// Optional manifest of every metric the sink expects to
    /// receive.
    fn manifest(&self) -> Vec<MetricSpec> {
        Vec::new()
    }
}

/// Default metrics sink: emits one `tracing::info` event per
/// flush. Cheap, dependency-free, and useful in development.
#[derive(Debug, Clone)]
pub struct LoggingMetricsSink {
    name: String,
    counter: Arc<Mutex<u64>>,
}

impl LoggingMetricsSink {
    /// Build a sink with a human-readable name.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::hooks::LoggingMetricsSink;
    /// let s = LoggingMetricsSink::new("dyn_o_mite");
    /// assert_eq!(s.flush_count(), 0);
    /// ```
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            counter: Arc::new(Mutex::new(0)),
        }
    }

    /// Number of flushes the sink has observed.
    #[must_use]
    pub fn flush_count(&self) -> u64 {
        *self.counter.lock()
    }
}

impl MetricsSink for LoggingMetricsSink {
    fn emit<'a>(&'a self, snapshot: &'a Snapshot) -> BoxFuture<'a, Result<(), MetricsError>> {
        let counter = self.counter.clone();
        let name = self.name.clone();
        let pool_name = snapshot.pool.name.clone();
        Box::pin(async move {
            *counter.lock() += 1;
            tracing::info!(sink = %name, pool = %pool_name, "metrics flush");
            Ok(())
        })
    }

    fn manifest(&self) -> Vec<MetricSpec> {
        // Defer to the in-crate descriptor table; the JSON form
        // already contains every metric the engine emits.
        let json = describe_stats();
        // The descriptor table is consumed downstream as opaque
        // text. Returning an empty vec keeps the trait signature
        // honest while logging the manifest size for diagnostics.
        tracing::debug!(bytes = json.len(), "metrics manifest");
        Vec::new()
    }
}

// ---------- Streaming Datastore extensions ---------------------------------
//
// The streaming list path lets transports emit the bucket / key
// catalogue in chunks instead of buffering it. A `Datastore` impl
// produces one `Bytes` per entry; the transport (PBC server, HTTP
// gateway) buffers an implementation-chosen number of entries per
// outbound frame.

use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;
use std::task::{Context, Poll};

/// Type alias for the byte stream returned by
/// [`Datastore::list_buckets_stream`] and
/// [`Datastore::list_keys_stream`].
///
/// Each `Bytes` item is one bucket name or key. The stream may
/// surface a [`DatastoreError`] mid-iteration; transports translate
/// that into a wire-level error response and stop emitting frames.
pub type DatastoreByteStream =
    Pin<Box<dyn futures_core::Stream<Item = Result<Bytes, DatastoreError>> + Send>>;

/// Build a one-shot stream that yields a single
/// [`DatastoreError::Unsupported`] item. The default body for
/// [`Datastore::list_buckets_stream`] /
/// [`Datastore::list_keys_stream`] so existing impls keep working
/// without modification.
fn unsupported_byte_stream(
) -> impl futures_core::Stream<Item = Result<Bytes, DatastoreError>> + Send {
    UnsupportedListStream { emitted: false }
}

struct UnsupportedListStream {
    emitted: bool,
}

impl futures_core::Stream for UnsupportedListStream {
    type Item = Result<Bytes, DatastoreError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.emitted {
            return Poll::Ready(None);
        }
        self.emitted = true;
        Poll::Ready(Some(Err(DatastoreError::Unsupported(MsgType::Unknown))))
    }
}

/// `futures_core::Stream` that drains an owned `Vec<Bytes>` one
/// item at a time. Used by [`MemoryDatastore`] to back its
/// streaming list overrides.
struct VecByteStream {
    items: std::vec::IntoIter<Bytes>,
}

impl futures_core::Stream for VecByteStream {
    type Item = Result<Bytes, DatastoreError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.items.next() {
            Some(b) => Poll::Ready(Some(Ok(b))),
            None => Poll::Ready(None),
        }
    }
}

// ---------- MemoryDatastore listing index ----------------------------------
//
// `MemoryDatastore`'s public field layout (a single `Arc<Mutex<...>>`
// holding the dispatch counter) was committed before the streaming
// list path existed. To keep the published surface unchanged, the
// per-instance bucket/key index is held in a process-wide registry
// keyed by the `Arc<Mutex<MemoryStore>>` pointer identity. Cloning
// a `MemoryDatastore` shares its `inner` `Arc`, so clones see the
// same listing -- mirroring the existing dispatch-count behaviour.

#[derive(Debug, Default)]
struct MemoryListing {
    buckets: BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>>,
}

#[derive(Debug, Default, Clone)]
struct ListingHandle {
    inner: Arc<Mutex<MemoryListing>>,
}

fn listing_for(ds: &MemoryDatastore) -> ListingHandle {
    static REGISTRY: OnceLock<Mutex<Vec<(usize, ListingHandle)>>> = OnceLock::new();
    let registry = REGISTRY.get_or_init(|| Mutex::new(Vec::new()));
    let id = Arc::as_ptr(&ds.inner) as usize;
    let mut g = registry.lock();
    if let Some((_, h)) = g.iter().find(|(k, _)| *k == id) {
        return h.clone();
    }
    let h = ListingHandle::default();
    g.push((id, h.clone()));
    h
}

impl MemoryDatastore {
    /// Insert `(bucket, key)` into the in-memory listing index.
    ///
    /// Idempotent: inserting the same `(bucket, key)` pair twice is
    /// a no-op. Tests use this helper to seed the streaming list
    /// path without speaking the real Riak protocol.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::hooks::MemoryDatastore;
    /// let ds = MemoryDatastore::new();
    /// ds.insert(b"users", b"alice");
    /// ds.insert(b"users", b"bob");
    /// assert_eq!(ds.list_buckets_snapshot().len(), 1);
    /// assert_eq!(ds.list_keys_snapshot(b"users").len(), 2);
    /// ```
    pub fn insert(&self, bucket: &[u8], key: &[u8]) {
        let h = listing_for(self);
        let mut g = h.inner.lock();
        g.buckets
            .entry(bucket.to_vec())
            .or_default()
            .insert(key.to_vec());
    }

    /// Snapshot of the bucket name set, sorted lexicographically.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::hooks::MemoryDatastore;
    /// let ds = MemoryDatastore::new();
    /// assert!(ds.list_buckets_snapshot().is_empty());
    /// ```
    #[must_use]
    pub fn list_buckets_snapshot(&self) -> Vec<Bytes> {
        let h = listing_for(self);
        let g = h.inner.lock();
        g.buckets
            .keys()
            .map(|b| Bytes::copy_from_slice(b))
            .collect()
    }

    /// Snapshot of the keys in `bucket`, sorted lexicographically.
    /// Returns an empty vector when the bucket is unknown.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::hooks::MemoryDatastore;
    /// let ds = MemoryDatastore::new();
    /// assert!(ds.list_keys_snapshot(b"missing").is_empty());
    /// ```
    #[must_use]
    pub fn list_keys_snapshot(&self, bucket: &[u8]) -> Vec<Bytes> {
        let h = listing_for(self);
        let g = h.inner.lock();
        g.buckets
            .get(bucket)
            .map(|s| s.iter().map(|k| Bytes::copy_from_slice(k)).collect())
            .unwrap_or_default()
    }
}
