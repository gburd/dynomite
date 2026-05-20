//! Hand-rolled HTTP/1.1 server exposing the stats snapshot as JSON.
//!
//! The C reference handles a handful of query commands. This module
//! implements a minimal subset: a `GET /` (and `GET /info`) request
//! that returns the latest snapshot. Every other request returns an
//! empty `200 OK` with body `OK\r\n`, matching the C fallback path.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::stats::snapshot::Snapshot;

/// Maximum number of bytes the server will read for an HTTP request
/// line plus headers. Requests larger than this are rejected.
pub const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// Maximum number of headers parsed in a single request.
pub const MAX_HEADERS: usize = 32;

/// A bound TCP listener serving the stats endpoint.
///
/// Construct via [`StatsServer::bind`] then call
/// [`StatsServer::run`] to accept connections in a loop, or
/// [`StatsServer::accept_one`] for one-shot tests.
pub struct StatsServer {
    listener: TcpListener,
    source: Arc<Mutex<Snapshot>>,
}

impl StatsServer {
    /// Bind a listener at `addr`. Returns the bound server alongside
    /// its actual local address (useful when binding to port 0).
    pub async fn bind(addr: SocketAddr, source: Arc<Mutex<Snapshot>>) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, source })
    }

    /// Returns the local socket address the server is listening on.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept a single connection, serve one HTTP/1.1 request, and
    /// return.
    pub async fn accept_one(&self) -> io::Result<()> {
        let (sock, _peer) = self.listener.accept().await?;
        let snapshot = self.source.lock().clone();
        serve_connection(sock, snapshot).await
    }

    /// Run the accept loop until cancelled. Each connection is handled
    /// on a fresh task so a slow client cannot stall the listener.
    pub async fn run(&self) -> io::Result<()> {
        loop {
            let (sock, _peer) = self.listener.accept().await?;
            let snapshot = self.source.lock().clone();
            tokio::spawn(async move {
                let _ = serve_connection(sock, snapshot).await;
            });
        }
    }
}

async fn serve_connection(mut sock: TcpStream, snapshot: Snapshot) -> io::Result<()> {
    let mut buf = vec![0u8; MAX_REQUEST_BYTES];
    let mut filled = 0usize;
    loop {
        if filled == buf.len() {
            return write_response(&mut sock, 400, "Bad Request", b"").await;
        }
        let n = sock.read(&mut buf[filled..]).await?;
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
    let route = path.split('?').next().unwrap_or(path);
    match route {
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
