//! Florida HTTP seeds provider.
//!
//! The reference engine opens a TCP socket to
//! `127.0.0.1:8080`, writes
//! `GET /REST/v1/admin/get_seeds HTTP/1.0`, looks for `200 OK`,
//! drains the response, and treats the body as a
//! `host:port:rack:dc:tokens|...` blob. The Rust port preserves
//! the wire shape: a hand-rolled HTTP/1.0 client over
//! `tokio::net::TcpStream` (the locked dependency set forbids
//! `hyper`/`reqwest`), with `httparse` parsing the response
//! header.
//!
//! The provider is async-native: [`FloridaSeedsProvider::fetch`]
//! returns a future the caller awaits. The blocking
//! [`SeedsProvider::get_seeds`] entry point spins up a private
//! tokio current-thread runtime, which mirrors the C engine's
//! synchronous `florida_get_seeds` behaviour.
//!
//! # Examples
//!
//! ```
//! use dynomite::seeds::florida::FloridaSeedsProvider;
//! let p = FloridaSeedsProvider::new("127.0.0.1".into(), 8080);
//! assert_eq!(p.host(), "127.0.0.1");
//! ```

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::cluster::gossip::parse_seed_blob;
use crate::conf::ConfDynSeed;
use crate::seeds::{SeedsError, SeedsProvider};

const DEFAULT_REQUEST: &[u8] =
    b"GET /REST/v1/admin/get_seeds HTTP/1.0\r\nHost: 127.0.0.1\r\nUser-Agent: HTMLGET 1.0\r\n\r\n";
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// Florida-style HTTP seeds provider.
#[derive(Clone, Debug)]
pub struct FloridaSeedsProvider {
    host: String,
    port: u16,
    request: Vec<u8>,
}

impl FloridaSeedsProvider {
    /// Build a provider pointed at `host:port` with the default
    /// `GET /REST/v1/admin/get_seeds` request.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::seeds::florida::FloridaSeedsProvider;
    /// let p = FloridaSeedsProvider::new("127.0.0.1".into(), 8080);
    /// assert_eq!(p.port(), 8080);
    /// ```
    #[must_use]
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            request: DEFAULT_REQUEST.to_vec(),
        }
    }

    /// Override the HTTP request line (mirrors the
    /// `DYNOMITE_FLORIDA_REQUEST` env var override).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::seeds::florida::FloridaSeedsProvider;
    /// let p = FloridaSeedsProvider::new("h".into(), 1)
    ///     .with_request(b"GET / HTTP/1.0\r\n\r\n".to_vec());
    /// assert!(!p.request().is_empty());
    /// ```
    #[must_use]
    pub fn with_request(mut self, request: Vec<u8>) -> Self {
        self.request = request;
        self
    }

    /// Configured host.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Configured port.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// HTTP request bytes.
    #[must_use]
    pub fn request(&self) -> &[u8] {
        &self.request
    }

    /// Async fetch: connect, send the HTTP request, read the
    /// response, parse the body as a seed blob, and return a list
    /// of [`ConfDynSeed`].
    ///
    /// # Errors
    ///
    /// Returns [`SeedsError::Io`] for transport failures,
    /// [`SeedsError::Http`] when the response is not `200`,
    /// [`SeedsError::Parse`] when the body fails seed parsing.
    pub async fn fetch(&self) -> Result<Vec<ConfDynSeed>, SeedsError> {
        let addr = format!("{}:{}", self.host, self.port);
        let socket: SocketAddr = addr
            .parse()
            .map_err(|e| SeedsError::Parse(format!("bad florida addr '{addr}': {e}")))?;
        let mut stream = TcpStream::connect(socket).await?;
        stream.write_all(&self.request).await?;
        stream.shutdown().await.ok();
        let mut buf = Vec::with_capacity(8 * 1024);
        loop {
            let mut chunk = [0u8; 4 * 1024];
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            if buf.len() + n > MAX_RESPONSE_BYTES {
                return Err(SeedsError::Http(format!(
                    "florida response exceeded {MAX_RESPONSE_BYTES} bytes"
                )));
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        let body = parse_http_response(&buf)?;
        let body_str =
            std::str::from_utf8(body).map_err(|e| SeedsError::Parse(format!("body utf-8: {e}")))?;
        let body_str = body_str.trim();
        if body_str.is_empty() {
            return Err(SeedsError::NoFreshSeeds);
        }
        let records = parse_seed_blob(body_str).map_err(SeedsError::Parse)?;
        let mut out = Vec::with_capacity(records.len());
        for rec in records {
            // Render back into the canonical pname format and
            // re-parse so the result is a fully-validated
            // ConfDynSeed.
            let tokens: Vec<String> = rec.tokens.iter().map(|t| t.get_int().to_string()).collect();
            let raw = format!(
                "{}:{}:{}:{}:{}",
                rec.host,
                rec.port,
                rec.rack,
                rec.dc,
                tokens.join(","),
            );
            let seed = ConfDynSeed::parse(&raw).map_err(|e| SeedsError::Parse(e.to_string()))?;
            out.push(seed);
        }
        Ok(out)
    }
}

fn parse_http_response(buf: &[u8]) -> Result<&[u8], SeedsError> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut response = httparse::Response::new(&mut headers);
    let parsed = response
        .parse(buf)
        .map_err(|e| SeedsError::Http(format!("response parse: {e}")))?;
    let body_start = match parsed {
        httparse::Status::Complete(n) => n,
        httparse::Status::Partial => {
            return Err(SeedsError::Http("incomplete response header".into()))
        }
    };
    let code = response
        .code
        .ok_or_else(|| SeedsError::Http("missing status code".into()))?;
    if code != 200 {
        return Err(SeedsError::Http(format!("status {code}")));
    }
    Ok(&buf[body_start..])
}

impl SeedsProvider for FloridaSeedsProvider {
    fn get_seeds(&self) -> Result<Vec<ConfDynSeed>, SeedsError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .map_err(SeedsError::Io)?;
        rt.block_on(self.fetch())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    async fn canned_server(body: &'static [u8], status: &'static str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await;
                let header = format!("HTTP/1.0 {status}\r\nContent-Type: text/plain\r\n\r\n");
                let _ = sock.write_all(header.as_bytes()).await;
                let _ = sock.write_all(body).await;
                let _ = sock.shutdown().await;
            }
        });
        port
    }

    #[tokio::test]
    async fn ok_response_parsed() {
        let port =
            canned_server(b"127.0.0.1:8101:rA:dc1:1|127.0.0.2:8101:rA:dc1:2", "200 OK").await;
        let p = FloridaSeedsProvider::new("127.0.0.1".into(), port);
        let v = p.fetch().await.unwrap();
        assert_eq!(v.len(), 2);
    }

    #[tokio::test]
    async fn bad_status_is_error() {
        let port = canned_server(b"nope", "500 Internal Server Error").await;
        let p = FloridaSeedsProvider::new("127.0.0.1".into(), port);
        assert!(matches!(p.fetch().await, Err(SeedsError::Http(_))));
    }

    #[tokio::test]
    async fn empty_body_is_no_fresh() {
        let port = canned_server(b"   ", "200 OK").await;
        let p = FloridaSeedsProvider::new("127.0.0.1".into(), port);
        assert!(matches!(p.fetch().await, Err(SeedsError::NoFreshSeeds)));
    }

    #[test]
    fn default_request_includes_get_seeds() {
        let p = FloridaSeedsProvider::new("127.0.0.1".into(), 8080);
        assert!(std::str::from_utf8(p.request())
            .unwrap()
            .contains("GET /REST/v1/admin/get_seeds"));
    }
}
