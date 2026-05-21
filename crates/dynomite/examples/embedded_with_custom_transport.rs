//! `embedded_with_custom_transport` - illustrative-only.
//!
//! The Stage 13 embedding API exposes [`dynomite::embed::Transport`]
//! re-exposed via `io::reactor`. The trait already abstracts
//! over `AsyncRead + AsyncWrite + Send + Unpin` so an embedder can
//! plug a TLS-wrapped transport, a Unix domain socket, an
//! in-memory pipe, etc.
//!
//! Wiring a TLS transport requires `tokio-rustls`, which is not
//! in the workspace dependency set; this example sketches the
//! shape but ships an in-memory pipe instead so it builds and
//! runs without an extra dependency. Marked as a Deviation in
//! `docs/parity.md`.
//!
//! Run with `cargo run --example embedded_with_custom_transport`.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use dynomite::embed::ConnRole;

/// In-memory transport wrapping a tokio duplex stream. The role
/// tag is reported back to the embed runtime for stats and event
/// labelling.
pub struct PipeTransport {
    inner: tokio::io::DuplexStream,
    role: ConnRole,
}

impl PipeTransport {
    fn new(inner: tokio::io::DuplexStream, role: ConnRole) -> Self {
        Self { inner, role }
    }

    fn role(&self) -> ConnRole {
        self.role
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (a, b) = tokio::io::duplex(1024);
    let mut left = PipeTransport::new(a, ConnRole::Client);
    let mut right = PipeTransport::new(b, ConnRole::Server);
    left.write_all(b"hello").await?;
    let mut buf = [0u8; 5];
    right.read_exact(&mut buf).await?;
    assert_eq!(&buf, b"hello");
    eprintln!(
        "custom transport ok: left.role={:?}, right.role={:?}",
        left.role(),
        right.role()
    );
    eprintln!("(swap PipeTransport for tokio_rustls::TlsStream to wire mTLS)");
    Ok(())
}
