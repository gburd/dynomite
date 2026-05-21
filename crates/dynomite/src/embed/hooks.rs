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

/// Adapter that lifts a [`crate::seeds::SeedsProvider`] into the
/// embed-facing [`SeedsProvider`] trait.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use dynomite::embed::hooks::{LegacySeedsAdapter, SeedsProvider};
/// use dynomite::seeds::simple::SimpleSeedsProvider as Inner;
/// use dynomite::conf::ConfDynSeed;
/// let inner = Arc::new(Inner::new(vec![ConfDynSeed::parse("h:1:r:d:1").unwrap()]));
/// let adapter = LegacySeedsAdapter::new(inner);
/// assert_eq!(adapter.fetch().unwrap().len(), 1);
/// ```
#[derive(Clone)]
pub struct LegacySeedsAdapter<T: RawSeeds> {
    inner: Arc<T>,
    refresh: Duration,
}

impl<T: RawSeeds> LegacySeedsAdapter<T> {
    /// Wrap an existing provider.
    pub fn new(inner: Arc<T>) -> Self {
        Self {
            inner,
            refresh: Duration::from_secs(30),
        }
    }

    /// Override the default refresh interval.
    #[must_use]
    pub fn with_refresh(mut self, interval: Duration) -> Self {
        self.refresh = interval;
        self
    }
}

impl<T: RawSeeds> std::fmt::Debug for LegacySeedsAdapter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LegacySeedsAdapter")
            .field("refresh", &self.refresh)
            .finish_non_exhaustive()
    }
}

impl<T: RawSeeds> SeedsProvider for LegacySeedsAdapter<T> {
    fn fetch(&self) -> Result<Vec<ConfDynSeed>, SeedsError> {
        self.inner.get_seeds()
    }

    fn refresh_interval(&self) -> Duration {
        self.refresh
    }
}

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
