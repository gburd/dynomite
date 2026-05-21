//! `embedded_custom_transport_sketch` - sketch, not a runnable
//! transport plug-in.
//!
//! This example demonstrates the **shape** of a custom
//! [`dynomite::embed::Transport`] implementation. It does **not**
//! plug a custom listener into the embedded engine: wiring a
//! `Box<dyn TransportListener>` into [`dynomite::embed::ServerBuilder`]
//! is tracked as Stage 14b; until that lands, the embedded engine
//! is in-process only and `inject_request` is the sanctioned
//! traffic entry point.
//!
//! Run with
//! `cargo run --example embedded_custom_transport_sketch`.
//!
//! What this sketch shows:
//!
//! 1. How to construct an `AsyncRead + AsyncWrite + Send + Unpin`
//!    type out of a `tokio::io::DuplexStream` (the same pattern
//!    works for `tokio_rustls::TlsStream`, a Unix-domain socket,
//!    a QUIC stream, etc.).
//! 2. How that type implements the `Transport` trait by carrying
//!    a `ConnRole` tag and reporting a synthetic `peer_addr`.
//!
//! What this sketch does **not** show (deferred to Stage 14b):
//!
//! * Plugging a listener that yields these transports into
//!   `ServerBuilder`. The builder does not yet accept a
//!   `Box<dyn TransportListener>`; once it does, swapping the
//!   `tokio::net::TcpListener` in the embed runtime for an
//!   embedder-supplied source becomes a one-line setter.

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use dynomite::embed::{ConnRole, Transport};

/// In-memory transport wrapping a tokio duplex stream.
///
/// Demonstrates the trait shape only; not wired into the embed
/// runtime.
pub struct PipeTransport {
    inner: tokio::io::DuplexStream,
    role: ConnRole,
    peer_addr: SocketAddr,
}

impl PipeTransport {
    fn new(inner: tokio::io::DuplexStream, role: ConnRole, peer_addr: SocketAddr) -> Self {
        Self {
            inner,
            role,
            peer_addr,
        }
    }
}

impl AsyncRead for PipeTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for PipeTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Transport for PipeTransport {
    fn role(&self) -> ConnRole {
        self.role
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        Some(self.peer_addr)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Build a paired duplex transport.
    let (a, b) = tokio::io::duplex(1024);
    let synthetic: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut left = PipeTransport::new(a, ConnRole::Client, synthetic);
    let mut right = PipeTransport::new(b, ConnRole::Server, synthetic);

    // 2. Demonstrate the AsyncRead + AsyncWrite shape.
    left.write_all(b"hello").await?;
    let mut buf = [0u8; 5];
    right.read_exact(&mut buf).await?;
    assert_eq!(&buf, b"hello");

    // 3. Demonstrate the Transport trait surface. A real
    //    embedder would hand a stream of these (yielded by some
    //    listener) to the engine via a builder method that does
    //    not yet exist.
    let _: ConnRole = Transport::role(&left);
    let _: Option<SocketAddr> = Transport::peer_addr(&left);

    eprintln!(
        "custom transport sketch ok: left.role={:?}, right.role={:?}",
        Transport::role(&left),
        Transport::role(&right)
    );
    eprintln!(
        "(wiring a Box<dyn TransportListener> into ServerBuilder is tracked as Stage 14b; \
         until that lands, embedded servers use inject_request for in-process traffic.)"
    );
    Ok(())
}
