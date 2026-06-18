//! Async transport abstraction over the connection state machine.
//!
//! The C engine's per-platform `src/event/dyn_{epoll,kqueue,evport}.c`
//! reactor is replaced wholesale by tokio. This module defines a
//! [`Transport`] trait that downstream stages drive through
//! `tokio::io::{AsyncRead, AsyncWrite}` and a [`ConnRole`] enum that
//! mirrors the connection-role enumerations carried on the C `struct
//! conn` (`client`, `server`, `proxy`, plus their dnode peer
//! variants).
//!
//! The trait is intentionally narrow:
//!
//! * [`Transport::role`] returns the connection role for routing /
//!   metric tagging.
//! * [`Transport::peer_addr`] returns the remote address when the
//!   transport is connected to a known endpoint. QUIC, for example,
//!   may not always have one.
//!
//! `Transport` does not expose any TCP-specific operations; this is
//! deliberate so the Stage 9 QUIC implementation can wrap a
//! `quiche::Connection` in a `QuicTransport` newtype without changing
//! callers.
//!
//! [`TcpTransport`] is the TCP implementation, a newtype wrapper
//! around [`tokio::net::TcpStream`].
//!
//! # Examples
//!
//! ```
//! use dynomite::io::reactor::{ConnRole, TcpTransport, Transport};
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//! use tokio::net::{TcpListener, TcpStream};
//!
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
//! let addr = listener.local_addr().unwrap();
//! let server = tokio::spawn(async move {
//!     let (sock, _) = listener.accept().await.unwrap();
//!     let mut t = TcpTransport::new(sock, ConnRole::Server);
//!     let mut buf = [0u8; 5];
//!     t.read_exact(&mut buf).await.unwrap();
//!     t.write_all(&buf).await.unwrap();
//! });
//! let sock = TcpStream::connect(addr).await.unwrap();
//! let mut client = TcpTransport::new(sock, ConnRole::Client);
//! client.write_all(b"hello").await.unwrap();
//! let mut out = [0u8; 5];
//! client.read_exact(&mut out).await.unwrap();
//! assert_eq!(&out, b"hello");
//! server.await.unwrap();
//! # });
//! ```

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// Role tag for a connection. Mirrors the role enumeration on
/// `struct conn` in the C engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnRole {
    /// Client-facing listener that accepted the connection.
    Proxy,
    /// Connection from a client driver (Redis or Memcached).
    Client,
    /// Connection to a backend datastore (Redis or Memcached).
    Server,
    /// Listener that accepts dnode peer connections from other nodes.
    DnodePeerProxy,
    /// Inbound dnode peer connection (a remote node connected to us).
    DnodePeerClient,
    /// Outbound dnode peer connection (we connected to a remote node).
    DnodePeerServer,
}

impl ConnRole {
    /// True when the role represents a listening socket. Mirrors the
    /// `is_listener` predicate used by the C reactor's dispatch
    /// branches.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::reactor::ConnRole;
    /// assert!(ConnRole::Proxy.is_listener());
    /// assert!(!ConnRole::Client.is_listener());
    /// ```
    pub fn is_listener(self) -> bool {
        matches!(self, Self::Proxy | Self::DnodePeerProxy)
    }

    /// True when the role represents a peer-to-peer dnode link.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::reactor::ConnRole;
    /// assert!(ConnRole::DnodePeerClient.is_dnode_peer());
    /// assert!(!ConnRole::Client.is_dnode_peer());
    /// ```
    pub fn is_dnode_peer(self) -> bool {
        matches!(
            self,
            Self::DnodePeerProxy | Self::DnodePeerClient | Self::DnodePeerServer
        )
    }
}

/// Generic async byte-stream the engine reads and writes through.
///
/// Implementors must be [`Send`] and [`Unpin`] so they fit the tokio
/// task model. Adding a new transport (Stage 9 QUIC, for example)
/// means newtyping its connection handle and implementing
/// [`AsyncRead`], [`AsyncWrite`], and `Transport`.
pub trait Transport: AsyncRead + AsyncWrite + Send + Unpin {
    /// Connection role.
    fn role(&self) -> ConnRole;

    /// Remote socket address when the transport is connected to one.
    /// Returns `None` for transports that do not surface a
    /// `SocketAddr` (for example, QUIC over a virtual interface).
    fn peer_addr(&self) -> Option<SocketAddr>;
}

/// TCP-backed [`Transport`].
///
/// Newtype around [`tokio::net::TcpStream`]. The role is supplied at
/// construction time because the same TCP socket type is used for
/// every C role variant; the discriminator lives one level up in the
/// listener registration.
#[derive(Debug)]
pub struct TcpTransport {
    inner: TcpStream,
    role: ConnRole,
}

impl TcpTransport {
    /// Wrap a TCP stream with the given role tag.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::reactor::{ConnRole, TcpTransport, Transport};
    /// use tokio::net::{TcpListener, TcpStream};
    ///
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    /// let addr = listener.local_addr().unwrap();
    /// let _accept = tokio::spawn(async move {
    ///     let (s, _) = listener.accept().await.unwrap();
    ///     drop(s);
    /// });
    /// let sock = TcpStream::connect(addr).await.unwrap();
    /// let t = TcpTransport::new(sock, ConnRole::Client);
    /// assert_eq!(t.role(), ConnRole::Client);
    /// # });
    /// ```
    pub fn new(stream: TcpStream, role: ConnRole) -> Self {
        Self {
            inner: stream,
            role,
        }
    }

    /// Borrow the wrapped tokio stream.
    pub fn get_ref(&self) -> &TcpStream {
        &self.inner
    }

    /// Mutably borrow the wrapped tokio stream. Useful for setting
    /// per-socket options (`set_nodelay`, `set_linger`, ...) without
    /// re-implementing them on the wrapper.
    pub fn get_mut(&mut self) -> &mut TcpStream {
        &mut self.inner
    }

    /// Consume the wrapper and return the inner stream.
    pub fn into_inner(self) -> TcpStream {
        self.inner
    }
}

impl Transport for TcpTransport {
    fn role(&self) -> ConnRole {
        self.role
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.inner.peer_addr().ok()
    }
}

impl AsyncRead for TcpTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// Unix-domain-socket-backed [`Transport`].
///
/// Newtype around [`tokio::net::UnixStream`], used when a datastore
/// backend is configured with a filesystem path rather than a
/// `host:port` endpoint. Unix sockets do not expose a
/// [`SocketAddr`], so [`Transport::peer_addr`] returns `None`.
#[cfg(unix)]
#[derive(Debug)]
pub struct UnixTransport {
    inner: tokio::net::UnixStream,
    role: ConnRole,
}

#[cfg(unix)]
impl UnixTransport {
    /// Wrap a Unix-domain stream with the given role tag.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::reactor::{ConnRole, Transport, UnixTransport};
    /// use tokio::net::{UnixListener, UnixStream};
    ///
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let dir = std::env::temp_dir().join(format!("dyn-ut-{}", std::process::id()));
    /// let _ = std::fs::remove_file(&dir);
    /// let listener = UnixListener::bind(&dir).unwrap();
    /// let _accept = tokio::spawn(async move {
    ///     let (s, _) = listener.accept().await.unwrap();
    ///     drop(s);
    /// });
    /// let sock = UnixStream::connect(&dir).await.unwrap();
    /// let t = UnixTransport::new(sock, ConnRole::Server);
    /// assert_eq!(t.role(), ConnRole::Server);
    /// assert!(t.peer_addr().is_none());
    /// let _ = std::fs::remove_file(&dir);
    /// # });
    /// ```
    pub fn new(stream: tokio::net::UnixStream, role: ConnRole) -> Self {
        Self {
            inner: stream,
            role,
        }
    }

    /// Consume the wrapper and return the inner stream.
    pub fn into_inner(self) -> tokio::net::UnixStream {
        self.inner
    }
}

#[cfg(unix)]
impl Transport for UnixTransport {
    fn role(&self) -> ConnRole {
        self.role
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        None
    }
}

#[cfg(unix)]
impl AsyncRead for UnixTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

#[cfg(unix)]
impl AsyncWrite for UnixTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    #[test]
    fn role_predicates() {
        assert!(ConnRole::Proxy.is_listener());
        assert!(ConnRole::DnodePeerProxy.is_listener());
        assert!(!ConnRole::Server.is_listener());
        assert!(ConnRole::DnodePeerServer.is_dnode_peer());
        assert!(!ConnRole::Server.is_dnode_peer());
    }

    #[tokio::test]
    async fn tcp_transport_round_trip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (s, _) = listener.accept().await.unwrap();
            let mut t = TcpTransport::new(s, ConnRole::Server);
            assert_eq!(t.role(), ConnRole::Server);
            let mut buf = [0u8; 4];
            t.read_exact(&mut buf).await.unwrap();
            t.write_all(&buf).await.unwrap();
        });
        let sock = TcpStream::connect(addr).await.unwrap();
        let mut client = TcpTransport::new(sock, ConnRole::Client);
        assert_eq!(client.role(), ConnRole::Client);
        assert!(client.peer_addr().is_some());
        client.write_all(b"ping").await.unwrap();
        let mut out = [0u8; 4];
        client.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"ping");
        server.await.unwrap();
    }
}
