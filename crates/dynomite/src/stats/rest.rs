//! Hand-rolled HTTP/1.1 server exposing the stats snapshot as JSON.
//!
//! The C reference handles a handful of query commands. This module
//! implements a minimal subset: a `GET /` (and `GET /info`) request
//! that returns the latest snapshot. Every other request returns an
//! empty `200 OK` with body `OK\r\n`, matching the C fallback path.

#![allow(clippy::needless_continue)]

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::Instrument as _;

use crate::stats::snapshot::Snapshot;

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
/// peer before closing the socket. Mirrors the implicit blocking-recv
/// behavior of the reference engine while protecting tokio tasks from
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
/// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink).await?;
/// let _addr = server.local_addr()?;
/// # Ok(())
/// # }
/// ```
pub struct StatsServer {
    listener: TcpListener,
    source: Arc<Mutex<Snapshot>>,
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
    /// let _server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn bind(addr: SocketAddr, source: Arc<Mutex<Snapshot>>) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, source })
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
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink).await?;
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
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink).await?;
    /// server.accept_one().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn accept_one(&self) -> io::Result<()> {
        let (sock, _peer) = self.listener.accept().await?;
        let snapshot = self.source.lock().clone();
        serve_connection(sock, snapshot).await
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
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink).await?;
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
        async move {
            loop {
                let (sock, _peer) = listener.accept().await?;
                let snapshot = source.lock().clone();
                tokio::spawn(async move {
                    let _ = serve_connection(sock, snapshot).await;
                });
            }
        }
        .instrument(span)
        .await
    }
}

async fn serve_connection(mut sock: TcpStream, snapshot: Snapshot) -> io::Result<()> {
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
                return handle_parsed(&mut sock, &req, snapshot).await;
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
) -> io::Result<()> {
    let path = req.path.unwrap_or("/");
    if !matches!(req.method, Some("GET")) {
        return write_response(sock, 405, "Method Not Allowed", b"").await;
    }
    match path {
        "/" | "/info" => {
            let body = snapshot.to_json();
            write_json_response(sock, body.as_bytes()).await
        }
        _ => write_response(sock, 200, "OK", b"OK\r\n").await,
    }
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
