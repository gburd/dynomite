//! QUIC transport (feature `quic`).
//!
//! Wraps a [`quiche::Connection`] in a tokio-driven event loop so the
//! Stage 9 connection FSMs can drop the QUIC implementation in beside
//! the TCP one. The wire-level shape mirrors a single bidirectional
//! TCP stream: the engine opens stream id `4` (the lowest
//! client-initiated bidirectional stream) and reads / writes
//! application bytes through it. Multi-stream multiplexing is left
//! to future revisions.
//!
//! The transport is intentionally thin: most of the role-specific
//! logic lives in the transport-agnostic [`crate::net::Conn`] FSM, so
//! this module only owns the UDP socket, the quiche packet pump, and
//! the per-connection channels exposed through [`AsyncRead`] /
//! [`AsyncWrite`].
//!
//! TLS material is configured through [`QuicConfig`]; tests use a
//! self-signed cert generated at test-binary startup. Production
//! deployments must supply real certificates.
//!
//! # Examples
//!
//! ```ignore
//! // QUIC requires the `quic` feature; this example is exercised by
//! // the Stage 9 integration tests.
//! use dynomite::net::quic::QuicConfig;
//! let _cfg = QuicConfig::server_with_cert_paths("server.crt", "server.key");
//! ```
//!
//! See `crates/dynomite/tests/stage_09_quic.rs` for a runnable
//! end-to-end test (gated on the `quic` feature).

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::PollSender;

use crate::io::reactor::{ConnRole, Transport};

const STREAM_ID: u64 = 0;
const MAX_DATAGRAM_SIZE: usize = 1350;

/// QUIC TLS / ALPN configuration.
#[derive(Debug, Clone)]
pub struct QuicConfig {
    /// Path to the server certificate chain (PEM).
    pub cert_chain_path: Option<String>,
    /// Path to the server private key (PEM).
    pub priv_key_path: Option<String>,
    /// Application-layer protocol negotiation labels in wire format.
    /// The Stage 9 default is `b"\\x05dynom"`.
    pub alpn: Vec<u8>,
    /// Disable certificate verification on the client side. Tests
    /// flip this on; production must leave it `false`.
    pub insecure: bool,
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            cert_chain_path: None,
            priv_key_path: None,
            alpn: b"\x05dynom".to_vec(),
            insecure: false,
        }
    }
}

impl QuicConfig {
    /// Build a server-flavoured config from cert+key paths.
    #[must_use]
    pub fn server_with_cert_paths<S: Into<String>>(cert: S, key: S) -> Self {
        Self {
            cert_chain_path: Some(cert.into()),
            priv_key_path: Some(key.into()),
            ..Self::default()
        }
    }

    /// Build a client-flavoured config that skips certificate
    /// verification. Tests only.
    #[must_use]
    pub fn client_insecure() -> Self {
        Self {
            insecure: true,
            ..Self::default()
        }
    }

    fn build(&self, is_server: bool) -> Result<quiche::Config, io::Error> {
        let mut cfg = quiche::Config::new(quiche::PROTOCOL_VERSION)
            .map_err(|e| io::Error::other(format!("quiche::Config: {e:?}")))?;
        cfg.set_application_protos_wire_format(&self.alpn)
            .map_err(|e| io::Error::other(format!("set_application_protos: {e:?}")))?;
        cfg.set_max_idle_timeout(30_000);
        cfg.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
        cfg.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
        cfg.set_initial_max_data(10_000_000);
        cfg.set_initial_max_stream_data_bidi_local(1_000_000);
        cfg.set_initial_max_stream_data_bidi_remote(1_000_000);
        cfg.set_initial_max_stream_data_uni(1_000_000);
        cfg.set_initial_max_streams_bidi(100);
        cfg.set_initial_max_streams_uni(100);
        cfg.set_disable_active_migration(true);
        if is_server {
            if let Some(cert) = &self.cert_chain_path {
                cfg.load_cert_chain_from_pem_file(cert)
                    .map_err(|e| io::Error::other(format!("load_cert_chain: {e:?}")))?;
            }
            if let Some(key) = &self.priv_key_path {
                cfg.load_priv_key_from_pem_file(key)
                    .map_err(|e| io::Error::other(format!("load_priv_key: {e:?}")))?;
            }
        } else {
            cfg.verify_peer(!self.insecure);
        }
        Ok(cfg)
    }
}

/// QUIC-backed [`Transport`].
///
/// `tx` is wrapped in a [`PollSender`] so [`AsyncWrite::poll_write`]
/// can return [`Poll::Pending`] (after registering the caller's
/// waker) when the driver task's inbox is full, instead of busy-
/// looping on a zero-byte short write.
pub struct QuicTransport {
    role: ConnRole,
    peer_addr: SocketAddr,
    rx: mpsc::Receiver<Vec<u8>>,
    tx: PollSender<Vec<u8>>,
    pending_read: Vec<u8>,
    closed: Arc<Notify>,
    _driver: Arc<DriverHandle>,
}

struct DriverHandle {
    join: Mutex<Option<JoinHandle<()>>>,
    closed: Arc<Notify>,
}

impl Drop for DriverHandle {
    fn drop(&mut self) {
        self.closed.notify_waiters();
        if let Some(h) = self.join.lock().take() {
            h.abort();
        }
    }
}

impl QuicTransport {
    /// Local address of the underlying UDP socket.
    #[must_use]
    pub fn peer_addr_socket(&self) -> SocketAddr {
        self.peer_addr
    }
}

impl Transport for QuicTransport {
    fn role(&self) -> ConnRole {
        self.role
    }
    fn peer_addr(&self) -> Option<SocketAddr> {
        Some(self.peer_addr)
    }
}

impl AsyncRead for QuicTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.pending_read.is_empty() {
            let take = self.pending_read.len().min(buf.remaining());
            let bytes: Vec<u8> = self.pending_read.drain(..take).collect();
            buf.put_slice(&bytes);
            return Poll::Ready(Ok(()));
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(bytes)) => {
                let take = bytes.len().min(buf.remaining());
                buf.put_slice(&bytes[..take]);
                if take < bytes.len() {
                    self.pending_read.extend_from_slice(&bytes[take..]);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for QuicTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.tx.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                match self.tx.send_item(buf.to_vec()) {
                    Ok(()) => Poll::Ready(Ok(buf.len())),
                    Err(_) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "quic driver shut down",
                    ))),
                }
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "quic driver shut down",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.closed.notify_waiters();
        Poll::Ready(Ok(()))
    }
}

/// QUIC server listener.
pub struct QuicListener {
    socket: Arc<UdpSocket>,
    local_addr: SocketAddr,
    config: QuicConfig,
    seed: [u8; 16],
}

impl QuicListener {
    /// Bind a QUIC listener.
    ///
    /// # Errors
    /// Forwarded from `tokio::net::UdpSocket::bind`.
    pub async fn bind(addr: SocketAddr, config: QuicConfig) -> io::Result<Self> {
        let sock = UdpSocket::bind(addr).await?;
        let local_addr = sock.local_addr()?;
        let mut seed = [0u8; 16];
        // Fill with a deterministic-ish seed; the only requirement is
        // uniqueness within a process for the SCID.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        seed[..16].copy_from_slice(&now.to_le_bytes());
        Ok(Self {
            socket: Arc::new(sock),
            local_addr,
            config,
            seed,
        })
    }

    /// Local address.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept the next QUIC connection.
    ///
    /// # Errors
    /// Forwarded I/O errors.
    pub async fn accept(&self) -> io::Result<QuicTransport> {
        let mut config = self.config.build(true)?;
        // Loop reading datagrams until we observe an Initial packet.
        let mut buf = vec![0u8; 65535];
        loop {
            let (n, peer) = self.socket.recv_from(&mut buf).await?;
            let pkt = &mut buf[..n];
            let Ok(hdr) = quiche::Header::from_slice(pkt, quiche::MAX_CONN_ID_LEN) else {
                continue;
            };
            if hdr.ty != quiche::Type::Initial {
                continue;
            }
            let scid = quiche::ConnectionId::from_ref(&self.seed);
            let conn = quiche::accept(&scid, None, self.local_addr, peer, &mut config)
                .map_err(|e| io::Error::other(format!("quiche::accept: {e:?}")))?;
            let transport = spawn_driver(
                Arc::clone(&self.socket),
                conn,
                peer,
                ConnRole::Server,
                Some(pkt.to_vec()),
            );
            return Ok(transport);
        }
    }
}

/// Connect a QUIC client to `peer`.
///
/// # Errors
/// Returns the underlying socket / quiche errors.
pub async fn connect(peer: SocketAddr, config: QuicConfig) -> io::Result<QuicTransport> {
    let bind = if peer.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let sock = UdpSocket::bind(bind).await?;
    sock.connect(peer).await?;
    let local_addr = sock.local_addr()?;
    let mut quiche_cfg = config.build(false)?;
    let mut scid = [0u8; quiche::MAX_CONN_ID_LEN];
    for (i, b) in scid.iter_mut().enumerate() {
        *b = u8::try_from(i & 0xff).unwrap_or(0).wrapping_add(0xa5);
    }
    let scid = quiche::ConnectionId::from_ref(&scid);
    let conn = quiche::connect(None, &scid, local_addr, peer, &mut quiche_cfg)
        .map_err(|e| io::Error::other(format!("quiche::connect: {e:?}")))?;
    let transport = spawn_driver(Arc::new(sock), conn, peer, ConnRole::Client, None);
    Ok(transport)
}

#[allow(clippy::too_many_lines)]
fn spawn_driver(
    socket: Arc<UdpSocket>,
    mut conn: quiche::Connection,
    peer: SocketAddr,
    role: ConnRole,
    prime: Option<Vec<u8>>,
) -> QuicTransport {
    let (driver_to_app_tx, driver_to_app_rx) = mpsc::channel::<Vec<u8>>(64);
    let (app_to_driver_tx, mut app_to_driver_rx) = mpsc::channel::<Vec<u8>>(64);
    let closed = Arc::new(Notify::new());
    let closed_for_driver = Arc::clone(&closed);
    let local_addr = socket.local_addr().unwrap_or(peer);

    let task = tokio::spawn(async move {
        let mut buf = vec![0u8; 65535];
        let mut out_buf = vec![0u8; MAX_DATAGRAM_SIZE];
        let mut pending_app_bytes: Vec<u8> = Vec::new();

        if let Some(mut pkt) = prime {
            let info = quiche::RecvInfo {
                from: peer,
                to: local_addr,
            };
            let _ = conn.recv(pkt.as_mut_slice(), info);
        }

        let mut done = false;
        while !done {
            loop {
                match app_to_driver_rx.try_recv() {
                    Ok(bytes) => pending_app_bytes.extend_from_slice(&bytes),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        done = true;
                        break;
                    }
                }
            }
            if done {
                break;
            }

            // Drain the pending buffer onto stream 0 once the
            // handshake is up. Errors that mean "not ready yet"
            // (Done, StreamLimit, FlowControl) leave the bytes in
            // the buffer and we retry on the next iteration.
            if conn.is_established() && !pending_app_bytes.is_empty() {
                let mut off = 0;
                while off < pending_app_bytes.len() {
                    match conn.stream_send(STREAM_ID, &pending_app_bytes[off..], false) {
                        Ok(written) => off += written,
                        Err(quiche::Error::Done)
                        | Err(quiche::Error::StreamLimit)
                        | Err(quiche::Error::FlowControl) => break,
                        Err(e) => {
                            tracing::debug!(?role, ?e, "quic stream_send error");
                            done = true;
                            break;
                        }
                    }
                }
                if off > 0 {
                    pending_app_bytes.drain(..off);
                }
            }

            // Drain any readable streams to the app.
            for sid in conn.readable().collect::<Vec<_>>() {
                while let Ok((read, _fin)) = conn.stream_recv(sid, &mut buf) {
                    if read == 0 {
                        break;
                    }
                    tracing::trace!(?role, read, "stream_recv -> app");
                    if driver_to_app_tx.send(buf[..read].to_vec()).await.is_err() {
                        done = true;
                        break;
                    }
                }
            }

            // Send pending QUIC packets.
            loop {
                match conn.send(&mut out_buf) {
                    Ok((written, info)) => {
                        if written == 0 {
                            break;
                        }
                        let _ = socket.send_to(&out_buf[..written], info.to).await;
                    }
                    Err(quiche::Error::Done) => break,
                    Err(e) => {
                        tracing::debug!(?role, ?e, "quic conn.send error");
                        done = true;
                        break;
                    }
                }
            }

            if conn.is_closed() {
                break;
            }

            // Cap the loop wake interval so newly-queued application
            // bytes are picked up promptly. Without this the driver
            // would only re-enter the try_recv block at the QUIC
            // idle-timeout cadence (multiple seconds), which is far
            // too coarse for an interactive proxy.
            let timeout = conn
                .timeout()
                .unwrap_or(Duration::from_millis(50))
                .min(Duration::from_millis(10));
            tokio::select! {
                () = closed_for_driver.notified() => {
                    done = true;
                }
                () = tokio::time::sleep(timeout) => {
                    conn.on_timeout();
                }
                res = socket.recv_from(&mut buf) => {
                    if let Ok((n, from)) = res {
                        let info = quiche::RecvInfo { from, to: local_addr };
                        if let Err(e) = conn.recv(&mut buf[..n], info) {
                            tracing::debug!(?role, ?e, "quic conn.recv error");
                        }
                    } else {
                        done = true;
                    }
                }
            }
        }
        let _ = conn.close(true, 0, b"");
    });

    QuicTransport {
        role,
        peer_addr: peer,
        rx: driver_to_app_rx,
        tx: PollSender::new(app_to_driver_tx),
        pending_read: Vec::new(),
        closed: Arc::clone(&closed),
        _driver: Arc::new(DriverHandle {
            join: Mutex::new(Some(task)),
            closed,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_default_alpn() {
        let cfg = QuicConfig::default();
        assert_eq!(cfg.alpn, b"\x05dynom".to_vec());
    }

    #[test]
    fn server_config_paths() {
        let cfg = QuicConfig::server_with_cert_paths("/tmp/c", "/tmp/k");
        assert_eq!(cfg.cert_chain_path.as_deref(), Some("/tmp/c"));
        assert_eq!(cfg.priv_key_path.as_deref(), Some("/tmp/k"));
    }

    #[test]
    fn client_insecure_flag() {
        assert!(QuicConfig::client_insecure().insecure);
    }
}
