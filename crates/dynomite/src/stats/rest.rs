//! Hand-rolled HTTP/1.1 server exposing the stats snapshot as JSON.
//!
//! This module implements a minimal stats query surface: a `GET /`
//! (and `GET /info`) request that returns the latest snapshot. Every
//! other request returns an empty `200 OK` with body `OK\r\n`.

#![allow(clippy::needless_continue)]

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::Instrument as _;

use crate::admin::cluster_info::{format_text, ClusterInfoSnapshot, RingSnapshot};
use crate::stats::prometheus::render_prometheus;
use crate::stats::snapshot::Snapshot;

/// Type alias for a closure that produces a fresh
/// [`ClusterInfoSnapshot`] every time the `/cluster-info.txt`
/// route is hit.
///
/// The closure must be `Send + Sync` so a clone can be moved
/// into each accept-handler task. The runtime owns the closure;
/// embedders set it via
/// [`StatsServer::with_cluster_info_provider`].
pub type ClusterInfoProvider = Arc<dyn Fn() -> ClusterInfoSnapshot + Send + Sync>;

/// Type alias for a closure that produces a fresh
/// [`RingSnapshot`] every time the `/ring` route is hit.
///
/// The closure must be `Send + Sync` so a clone can be moved
/// into each accept-handler task. Embedders set it via
/// [`StatsServer::with_ring_provider`]; when unset the `/ring`
/// route returns `503 Service Unavailable`.
pub type RingProvider = Arc<dyn Fn() -> RingSnapshot + Send + Sync>;

/// Maximum number of bytes the server will read for an HTTP request
/// line plus headers. Requests larger than this are rejected.
///
/// # Examples
///
/// ```
/// assert!(dynomite::stats::MAX_REQUEST_BYTES >= 1024);
/// ```
pub const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// Maximum number of headers parsed in a single request.
///
/// # Examples
///
/// ```
/// assert!(dynomite::stats::MAX_HEADERS > 0);
/// ```
pub const MAX_HEADERS: usize = 32;

/// Maximum time the server waits for a single read from a connected
/// peer before closing the socket. Protects tokio tasks from
/// slow-loris clients.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// A bound TCP listener serving the stats endpoint.
///
/// Construct via [`StatsServer::bind`] then call
/// [`StatsServer::run`] to accept connections in a loop, or
/// [`StatsServer::accept_one`] for one-shot tests.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use dynomite::stats::{Snapshot, StatsServer};
/// use parking_lot::Mutex;
///
/// # async fn _example() -> std::io::Result<()> {
/// let sink = Arc::new(Mutex::new(Snapshot::default()));
/// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?;
/// let _addr = server.local_addr()?;
/// # Ok(())
/// # }
/// ```
pub struct StatsServer {
    listener: TcpListener,
    source: Arc<Mutex<Snapshot>>,
    cluster_info: Option<ClusterInfoProvider>,
    ring: Option<RingProvider>,
}

impl StatsServer {
    /// Bind a listener at `addr`. Returns the bound server alongside
    /// its actual local address (useful when binding to port 0).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let _server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn bind(addr: SocketAddr, source: Arc<Mutex<Snapshot>>) -> io::Result<Self> {
        // Bind via the shared helper so the stats listener sets
        // SO_REUSEADDR like the client and dnode listeners. Without it,
        // a fast restart (kill + relaunch) fails with EADDRINUSE while
        // the previous socket lingers in TIME_WAIT, and because the
        // server builds all listeners as a unit that failure aborts the
        // whole process.
        let listener = crate::net::listener::bind_dual_stack(
            addr,
            crate::net::listener::BindOptions::default(),
        )?;
        Ok(Self {
            listener,
            source,
            cluster_info: None,
            ring: None,
        })
    }

    /// Attach a [`ClusterInfoProvider`] so the server answers
    /// `GET /cluster-info.txt` with a freshly assembled
    /// snapshot. When no provider is registered the route
    /// returns `503 Service Unavailable`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::admin::cluster_info::ClusterInfoSnapshot;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?
    ///     .with_cluster_info_provider(Arc::new(ClusterInfoSnapshot::synthetic));
    /// drop(server);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_cluster_info_provider(mut self, provider: ClusterInfoProvider) -> Self {
        self.cluster_info = Some(provider);
        self
    }

    /// Attach a [`RingProvider`] so the server answers
    /// `GET /ring` with a freshly assembled, JSON-serialized
    /// view of every peer's tokens, dc, rack, and state. When no
    /// provider is registered the route returns `503 Service
    /// Unavailable`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::admin::cluster_info::RingSnapshot;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?
    ///     .with_ring_provider(Arc::new(RingSnapshot::default));
    /// drop(server);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_ring_provider(mut self, provider: RingProvider) -> Self {
        self.ring = Some(provider);
        self
    }

    /// Returns the local socket address the server is listening on.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?;
    /// let addr = server.local_addr()?;
    /// assert!(addr.port() != 0);
    /// # Ok(())
    /// # }
    /// ```
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept a single connection, serve one HTTP/1.1 request, and
    /// return.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?;
    /// server.accept_one().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn accept_one(&self) -> io::Result<()> {
        let (sock, _peer) = self.listener.accept().await?;
        let snapshot = self.source.lock().clone();
        let cluster_info = self.cluster_info.clone();
        let ring = self.ring.clone();
        serve_connection(sock, snapshot, cluster_info, ring).await
    }

    /// Run the accept loop until cancelled. Each connection is handled
    /// on a fresh task so a slow client cannot stall the listener.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?;
    /// let _ = tokio::spawn(async move { server.run().await });
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run(self) -> io::Result<()> {
        let span = tracing::info_span!(
            "stats_server.run",
            local = %self.listener.local_addr().map_or_else(|_| String::from("?"), |a| a.to_string()),
        );
        let listener = self.listener;
        let source = self.source;
        let cluster_info = self.cluster_info;
        let ring = self.ring;
        async move {
            loop {
                let (sock, _peer) = listener.accept().await?;
                let snapshot = source.lock().clone();
                let ci = cluster_info.clone();
                let rg = ring.clone();
                tokio::spawn(async move {
                    let _ = serve_connection(sock, snapshot, ci, rg).await;
                });
            }
        }
        .instrument(span)
        .await
    }
}

async fn serve_connection(
    mut sock: TcpStream,
    snapshot: Snapshot,
    cluster_info: Option<ClusterInfoProvider>,
    ring: Option<RingProvider>,
) -> io::Result<()> {
    let mut buf = vec![0u8; MAX_REQUEST_BYTES];
    let mut filled = 0usize;
    loop {
        if filled == buf.len() {
            return write_response(&mut sock, 400, "Bad Request", b"").await;
        }
        let read_result = tokio::time::timeout(READ_TIMEOUT, sock.read(&mut buf[filled..])).await;
        let Ok(Ok(n)) = read_result else {
            // Read error or timeout: close silently, matching the
            // reference error path which drops the connection without
            // writing a response.
            let _ = sock.shutdown().await;
            return Ok(());
        };
        if n == 0 {
            break;
        }
        filled += n;
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&buf[..filled]) {
            Ok(httparse::Status::Complete(_)) => {
                return handle_parsed(&mut sock, &req, snapshot, cluster_info, ring).await;
            }
            Ok(httparse::Status::Partial) => continue,
            Err(_) => {
                return write_response(&mut sock, 400, "Bad Request", b"").await;
            }
        }
    }
    Ok(())
}

async fn handle_parsed(
    sock: &mut TcpStream,
    req: &httparse::Request<'_, '_>,
    snapshot: Snapshot,
    cluster_info: Option<ClusterInfoProvider>,
    ring: Option<RingProvider>,
) -> io::Result<()> {
    let path = req.path.unwrap_or("/");
    if !matches!(req.method, Some("GET")) {
        return write_response(sock, 405, "Method Not Allowed", b"").await;
    }
    match path {
        "/" | "/info" | "/stats" => {
            let body = snapshot.to_json();
            write_json_response(sock, body.as_bytes()).await
        }
        "/metrics" => {
            let body = render_prometheus(&snapshot);
            write_metrics_response(sock, body.as_bytes()).await
        }
        "/cluster-info.txt" => match cluster_info {
            Some(provider) => {
                let snap = provider();
                let mut body: Vec<u8> = Vec::with_capacity(4096);
                if format_text(&snap, &mut body).is_err() {
                    return write_response(sock, 500, "Internal Server Error", b"").await;
                }
                write_text_response(sock, &body).await
            }
            None => write_response(sock, 503, "Service Unavailable", b"").await,
        },
        "/ring" | "/ring.json" => match ring {
            Some(provider) => match serde_json::to_vec(&provider()) {
                Ok(body) => write_json_response(sock, &body).await,
                Err(_) => write_response(sock, 500, "Internal Server Error", b"").await,
            },
            None => write_response(sock, 503, "Service Unavailable", b"").await,
        },
        _ => write_response(sock, 200, "OK", b"OK\r\n").await,
    }
}

async fn write_text_response(sock: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=us-ascii\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(header.as_bytes()).await?;
    sock.write_all(body).await?;
    sock.shutdown().await?;
    Ok(())
}

async fn write_response(
    sock: &mut TcpStream,
    code: u16,
    reason: &str,
    body: &[u8],
) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(header.as_bytes()).await?;
    if !body.is_empty() {
        sock.write_all(body).await?;
    }
    sock.shutdown().await?;
    Ok(())
}

async fn write_json_response(sock: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(header.as_bytes()).await?;
    sock.write_all(body).await?;
    sock.shutdown().await?;
    Ok(())
}

async fn write_metrics_response(sock: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(header.as_bytes()).await?;
    sock.write_all(body).await?;
    sock.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: the stats listener must set SO_REUSEADDR so a fast
    /// restart (kill + immediate rebind, as the qualification harness
    /// does between consistency-level runs) does not fail with
    /// EADDRINUSE while the previous socket lingers. Bind, capture the
    /// concrete address, drop the server, and rebind the same address
    /// immediately -- this succeeds with SO_REUSEADDR and would fail a
    /// plain TcpListener::bind under TIME_WAIT.
    #[tokio::test]
    async fn stats_bind_is_reusable_after_drop() {
        let sink = Arc::new(Mutex::new(Snapshot::default()));
        let first =
            StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink.clone()).expect("first bind");
        let addr = first.local_addr().expect("addr");
        drop(first);
        // Immediate rebind of the same concrete address must succeed.
        let second = StatsServer::bind(addr, sink);
        assert!(
            second.is_ok(),
            "rebind of {addr} failed (SO_REUSEADDR not set?): {:?}",
            second.err()
        );
    }
}
