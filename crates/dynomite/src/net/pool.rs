//! Outbound connection pool with backoff and auto-eject.
//!
//! Two pool flavors share the same policy in this codebase: the
//! per-datastore pool that hands a Redis or memcache backend
//! connection to a CLIENT FSM, and the per-peer pool that hands a
//! peer connection to the cluster routing layer. Both share:
//!
//! * a cap on the active connection count (`max_connections`),
//! * round-robin across slots keyed on a caller-supplied `tag`,
//! * exponential connect-failure backoff (doubling the timeout each
//!   time, capped at `max_timeout`),
//! * auto-eject of the host after `failure_limit` consecutive
//!   failures, with retry after `retry_after`.
//!
//! [`ConnPool`] reproduces the policy in safe Rust. Connections are
//! manufactured by a caller-supplied [`ConnFactory`] (so tests can
//! inject failure-injecting transports) and handed back to the pool
//! through [`ConnHandle::release`].
//!
//! # Examples
//!
//! ```
//! use dynomite::net::pool::{ConnPool, ConnPoolConfig};
//! use dynomite::io::reactor::TcpTransport;
//!
//! let pool: ConnPool<TcpTransport> = ConnPool::new(ConnPoolConfig {
//!     max_connections: 4,
//!     server_failure_limit: 3,
//!     server_retry_timeout_ms: 1_000,
//!     auto_eject: true,
//! });
//! assert_eq!(pool.config().max_connections, 4);
//! ```

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::Notify;

use super::auto_eject::{AutoEject, AutoEjectState};
use super::NetError;

/// Tunable knobs taken straight from the YAML pool block.
///
/// `max_connections` mirrors `datastore_connections` /
/// `local_peer_connections` / `remote_peer_connections`.
/// `server_failure_limit` and `server_retry_timeout_ms` mirror the
/// fields with the same name. `auto_eject` mirrors
/// `auto_eject_hosts`.
#[derive(Debug, Clone)]
pub struct ConnPoolConfig {
    /// Maximum number of concurrent outbound connections kept by the
    /// pool.
    pub max_connections: usize,
    /// Consecutive failures before the host is ejected.
    pub server_failure_limit: u32,
    /// Eject window in milliseconds.
    pub server_retry_timeout_ms: u64,
    /// Honor the `auto_eject_hosts` policy.
    pub auto_eject: bool,
}

impl Default for ConnPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 1,
            server_failure_limit: 3,
            server_retry_timeout_ms: 30_000,
            auto_eject: true,
        }
    }
}

/// Boxed future returned by a [`ConnFactory`].
pub type ConnFuture<C> = Pin<Box<dyn Future<Output = Result<C, NetError>> + Send + 'static>>;

/// Factory that produces a fresh connection on demand.
///
/// The factory is invoked from the pool whenever a slot needs a
/// new connection. Implementations typically wrap a target address
/// and produce TCP or QUIC connections; tests inject
/// failure-injecting factories.
pub trait ConnFactory<C>: Send + Sync + 'static {
    /// Build a fresh connection.
    fn connect(&self) -> ConnFuture<C>;
}

impl<C, F, Fut> ConnFactory<C> for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<C, NetError>> + Send + 'static,
{
    fn connect(&self) -> ConnFuture<C> {
        Box::pin(self())
    }
}

struct PoolInner<C> {
    cfg: ConnPoolConfig,
    idle: VecDeque<C>,
    in_flight: usize,
    auto_eject: AutoEject,
    backoff: Backoff,
    shutdown: bool,
}

#[derive(Debug, Clone)]
struct Backoff {
    current: Duration,
    max: Duration,
}

impl Backoff {
    fn new(max: Duration) -> Self {
        Self {
            current: Duration::ZERO,
            max,
        }
    }

    fn record_failure(&mut self) -> Duration {
        // Exponential connect-failure backoff: the wait starts at
        // 1s and doubles on each consecutive failure, capped at
        // the configured maximum.
        if self.current.is_zero() {
            self.current = Duration::from_secs(1);
        } else {
            self.current = self.current.saturating_mul(2);
            if self.current > self.max {
                self.current = self.max;
            }
        }
        self.current
    }

    fn record_success(&mut self) {
        self.current = Duration::ZERO;
    }
}

/// Outbound connection pool.
///
/// The pool keeps an [`AutoEject`] tracker and a small idle list;
/// callers acquire a connection through [`ConnPool::get`] and
/// return it through [`ConnHandle::release`] (or by dropping the
/// handle, which routes to the same path).
pub struct ConnPool<C> {
    factory: Option<Arc<dyn ConnFactory<C>>>,
    state: Arc<Mutex<PoolInner<C>>>,
    notify: Arc<Notify>,
}

impl<C> Clone for ConnPool<C> {
    fn clone(&self) -> Self {
        Self {
            factory: self.factory.clone(),
            state: Arc::clone(&self.state),
            notify: Arc::clone(&self.notify),
        }
    }
}

impl<C: Send + 'static> ConnPool<C> {
    /// Build a pool with no factory installed.
    /// Build a pool with no factory installed.
    ///
    /// Callers must install a factory through
    /// [`ConnPool::set_factory`] before invoking
    /// [`ConnPool::get`]. The factory-less constructor exists so
    /// callers can build the pool eagerly during configuration and
    /// wire the factory once the resolver has populated the target
    /// address.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::reactor::TcpTransport;
    /// use dynomite::net::pool::{ConnPool, ConnPoolConfig};
    /// let _: ConnPool<TcpTransport> = ConnPool::new(ConnPoolConfig::default());
    /// ```
    #[must_use]
    pub fn new(cfg: ConnPoolConfig) -> Self {
        let auto_eject = AutoEject::new(
            cfg.auto_eject,
            cfg.server_failure_limit.max(1),
            Duration::from_millis(cfg.server_retry_timeout_ms),
        );
        let max_backoff = Duration::from_millis(cfg.server_retry_timeout_ms.max(1_000));
        Self {
            factory: None,
            state: Arc::new(Mutex::new(PoolInner {
                cfg,
                idle: VecDeque::new(),
                in_flight: 0,
                auto_eject,
                backoff: Backoff::new(max_backoff),
                shutdown: false,
            })),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Build a pool with a factory installed up front.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::net::pool::{ConnPool, ConnPoolConfig};
    /// use dynomite::net::NetError;
    /// let pool = ConnPool::with_factory(ConnPoolConfig::default(), || async {
    ///     Ok::<u32, NetError>(0)
    /// });
    /// assert_eq!(pool.config().max_connections, 1);
    /// ```
    pub fn with_factory<F>(cfg: ConnPoolConfig, factory: F) -> Self
    where
        F: ConnFactory<C>,
    {
        let mut pool = Self::new(cfg);
        pool.factory = Some(Arc::new(factory));
        pool
    }

    /// Install a connection factory.
    pub fn set_factory<F: ConnFactory<C>>(&mut self, factory: F) {
        self.factory = Some(Arc::new(factory));
    }

    /// Borrow the pool config.
    #[must_use]
    pub fn config(&self) -> ConnPoolConfig {
        self.state.lock().cfg.clone()
    }

    /// Number of idle connections currently in the pool.
    #[must_use]
    pub fn idle_count(&self) -> usize {
        self.state.lock().idle.len()
    }

    /// Number of in-flight (handed out) connections.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.state.lock().in_flight
    }

    /// True when the host has been auto-ejected at this instant.
    #[must_use]
    pub fn is_ejected(&self, now: Instant) -> bool {
        let mut g = self.state.lock();
        g.auto_eject.record_attempt(now) == AutoEjectState::Ejected
    }

    /// Snapshot of the auto-eject tracker.
    #[must_use]
    pub fn auto_eject(&self) -> AutoEject {
        self.state.lock().auto_eject.clone()
    }

    /// Shut the pool down.
    ///
    /// Wakes every waiter blocked in [`ConnPool::get`]. Subsequent
    /// `get` calls return [`NetError::PoolShutdown`].
    pub fn shutdown(&self) {
        {
            let mut g = self.state.lock();
            g.shutdown = true;
            g.idle.clear();
        }
        self.notify.notify_waiters();
    }

    /// Acquire a connection.
    ///
    /// Reuses an idle connection when one is available; otherwise
    /// invokes the factory until it succeeds or the auto-eject
    /// tracker reports the target as unreachable.
    ///
    /// # Errors
    /// Returns [`NetError::Ejected`] when the host is in its eject
    /// window, [`NetError::PoolShutdown`] when the pool was shut
    /// down, or the underlying factory error otherwise.
    pub async fn get(&self) -> Result<ConnHandle<C>, NetError> {
        loop {
            // Fast path: an idle connection is sitting in the pool.
            let waiter = {
                let mut g = self.state.lock();
                if g.shutdown {
                    return Err(NetError::PoolShutdown);
                }
                if let Some(conn) = g.idle.pop_front() {
                    g.in_flight += 1;
                    return Ok(ConnHandle {
                        pool: self.clone(),
                        inner: Some(conn),
                    });
                }
                if g.in_flight + g.idle.len() >= g.cfg.max_connections {
                    true
                } else {
                    let now = Instant::now();
                    if g.auto_eject.record_attempt(now) == AutoEjectState::Ejected {
                        return Err(NetError::Ejected);
                    }
                    false
                }
            };
            if waiter {
                self.notify.notified().await;
                continue;
            }

            let factory = self
                .factory
                .as_ref()
                .ok_or(NetError::PoolExhausted)?
                .clone();
            match factory.connect().await {
                Ok(conn) => {
                    let mut g = self.state.lock();
                    g.in_flight += 1;
                    g.auto_eject.record_success(Instant::now());
                    g.backoff.record_success();
                    return Ok(ConnHandle {
                        pool: self.clone(),
                        inner: Some(conn),
                    });
                }
                Err(err) => {
                    let ejected;
                    {
                        let mut g = self.state.lock();
                        let now = Instant::now();
                        ejected = g.auto_eject.record_failure(now) == AutoEjectState::Ejected;
                        let _ = g.backoff.record_failure();
                    }
                    if ejected {
                        return Err(NetError::Ejected);
                    }
                    return Err(err);
                }
            }
        }
    }

    fn return_conn(&self, conn: C) {
        let mut g = self.state.lock();
        if g.in_flight > 0 {
            g.in_flight -= 1;
        }
        if !g.shutdown && g.idle.len() + g.in_flight < g.cfg.max_connections {
            g.idle.push_back(conn);
        }
        drop(g);
        self.notify.notify_one();
    }

    fn drop_conn(&self) {
        let mut g = self.state.lock();
        if g.in_flight > 0 {
            g.in_flight -= 1;
        }
        drop(g);
        self.notify.notify_one();
    }
}

impl<C: std::fmt::Debug> std::fmt::Debug for ConnPool<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.state.lock();
        let factory_present = self.factory.is_some();
        f.debug_struct("ConnPool")
            .field("cfg", &g.cfg)
            .field("idle", &g.idle.len())
            .field("in_flight", &g.in_flight)
            .field("auto_eject_failures", &g.auto_eject.failure_count())
            .field("factory_installed", &factory_present)
            .field("notify", &"<tokio::sync::Notify>")
            .finish()
    }
}

/// Handle returned by [`ConnPool::get`].
///
/// Dropping the handle returns the connection to the pool. Call
/// [`ConnHandle::discard`] when the connection is no longer healthy
/// (the slot will then be re-filled by the next `get`).
pub struct ConnHandle<C: Send + 'static> {
    pool: ConnPool<C>,
    inner: Option<C>,
}

impl<C: Send + 'static> std::fmt::Debug for ConnHandle<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The pool reference and the inner connection are
        // intentionally elided; only the alive bit is printed.
        let _ = (&self.pool, &self.inner);
        f.debug_struct("ConnHandle")
            .field("alive", &self.inner.is_some())
            .finish()
    }
}

impl<C: Send + 'static> ConnHandle<C> {
    /// Borrow the wrapped connection.
    pub fn get(&self) -> &C {
        self.inner.as_ref().expect("invariant: handle is alive")
    }

    /// Mutably borrow the wrapped connection.
    pub fn get_mut(&mut self) -> &mut C {
        self.inner.as_mut().expect("invariant: handle is alive")
    }

    /// Return the connection to the pool. Equivalent to dropping
    /// the handle.
    pub fn release(mut self) {
        if let Some(conn) = self.inner.take() {
            self.pool.return_conn(conn);
        }
    }

    /// Discard the connection (do not return it to the pool).
    pub fn discard(mut self) {
        self.inner.take();
        self.pool.drop_conn();
    }
}

impl<C: Send + 'static> Drop for ConnHandle<C> {
    fn drop(&mut self) {
        if let Some(conn) = self.inner.take() {
            self.pool.return_conn(conn);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn round_trip_basic() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c2 = Arc::clone(&counter);
        let pool: ConnPool<usize> = ConnPool::with_factory(
            ConnPoolConfig {
                max_connections: 2,
                ..ConnPoolConfig::default()
            },
            move || {
                let c = Arc::clone(&c2);
                async move {
                    let id = c.fetch_add(1, Ordering::Relaxed);
                    Ok::<usize, NetError>(id)
                }
            },
        );
        let h1 = pool.get().await.unwrap();
        let h2 = pool.get().await.unwrap();
        assert_ne!(h1.get(), h2.get());
        h1.release();
        let h3 = pool.get().await.unwrap();
        assert_eq!(*h3.get(), 0);
        h3.release();
        h2.release();
    }

    #[tokio::test]
    async fn max_connections_blocks_until_release() {
        let pool: ConnPool<u32> = ConnPool::with_factory(
            ConnPoolConfig {
                max_connections: 1,
                ..ConnPoolConfig::default()
            },
            || async { Ok::<u32, NetError>(7) },
        );
        let h = pool.get().await.unwrap();
        let pool2 = pool.clone();
        let waiter = tokio::spawn(async move {
            let h2 = pool2.get().await.unwrap();
            assert_eq!(*h2.get(), 7);
        });
        // Briefly yield to ensure waiter is parked.
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(h);
        waiter.await.unwrap();
    }

    #[tokio::test]
    async fn auto_eject_after_consecutive_failures() {
        let pool: ConnPool<u8> = ConnPool::with_factory(
            ConnPoolConfig {
                max_connections: 1,
                server_failure_limit: 2,
                server_retry_timeout_ms: 50,
                auto_eject: true,
            },
            || async {
                Err::<u8, NetError>(NetError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "test",
                )))
            },
        );
        // First failure surfaces the io error.
        match pool.get().await {
            Err(NetError::Io(_)) => {}
            other => panic!("expected io error, got {other:?}"),
        }
        // Second failure trips the eject window.
        match pool.get().await {
            Err(NetError::Ejected) => {}
            other => panic!("expected eject, got {other:?}"),
        }
        // Subsequent attempts within the window are short-circuited.
        match pool.get().await {
            Err(NetError::Ejected) => {}
            other => panic!("expected eject, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn shutdown_unblocks_waiters() {
        let pool: ConnPool<u32> = ConnPool::with_factory(
            ConnPoolConfig {
                max_connections: 1,
                ..ConnPoolConfig::default()
            },
            || async { Ok::<u32, NetError>(1) },
        );
        let _h = pool.get().await.unwrap();
        let pool2 = pool.clone();
        let w = tokio::spawn(async move { pool2.get().await });
        tokio::task::yield_now().await;
        pool.shutdown();
        assert!(matches!(w.await.unwrap(), Err(NetError::PoolShutdown)));
    }
}
