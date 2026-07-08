//! QUIC transport (feature `quic`).
//!
//! Wraps a [`quiche::Connection`] in a tokio-driven event loop so the
//! connection FSMs can use the QUIC implementation beside
//! the TCP one. The wire-level shape mirrors a single bidirectional
//! TCP stream: the engine opens stream id `4` (the lowest
//! client-initiated bidirectional stream) and reads / writes
//! application bytes through it. Multi-stream multiplexing is not
//! implemented.
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
//! // the QUIC integration tests.
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
use tracing::Instrument as _;

use crate::conf::DataStore;
use crate::io::reactor::{ConnRole, Transport};
use crate::net::client::{client_loop, ClientHandler};
use crate::net::conn::Conn;
use crate::net::dispatcher::Dispatcher;
use crate::net::NetError;

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
    /// The default is `b"\\x05dynom"`.
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
            Poll::Ready(Ok(())) => match self.tx.send_item(buf.to_vec()) {
                Ok(()) => Poll::Ready(Ok(buf.len())),
                Err(_) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "quic driver shut down",
                ))),
            },
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
///
/// A single UDP socket carries every client's datagrams. QUIC requires
/// one demultiplexer to own the socket read side and route each
/// datagram to the right connection by peer address; multiple
/// competing `recv_from` callers (an accept loop plus each connection
/// driver) would steal each other's packets and stall a connection
/// after its first exchange. The listener therefore spawns one demux
/// task on `bind` that reads the socket, routes datagrams to per-
/// connection inbound channels, creates a connection on a fresh
/// Initial packet, and hands the accepted [`QuicTransport`] to
/// [`accept`](Self::accept) over a channel.
pub struct QuicListener {
    local_addr: SocketAddr,
    accept_rx: tokio::sync::Mutex<mpsc::Receiver<QuicTransport>>,
    _demux: Arc<DemuxHandle>,
}

struct DemuxHandle {
    join: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for DemuxHandle {
    fn drop(&mut self) {
        if let Some(h) = self.join.lock().take() {
            h.abort();
        }
    }
}

/// A datagram routed by the demux task to a connection driver.
type Datagram = Vec<u8>;

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
        let socket = Arc::new(sock);
        let (accept_tx, accept_rx) = mpsc::channel::<QuicTransport>(16);
        let demux = tokio::spawn(demux_loop(
            Arc::clone(&socket),
            local_addr,
            config,
            seed,
            accept_tx,
        ));
        Ok(Self {
            local_addr,
            accept_rx: tokio::sync::Mutex::new(accept_rx),
            _demux: Arc::new(DemuxHandle {
                join: Mutex::new(Some(demux)),
            }),
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
    /// Returns an I/O error if the demux task has stopped (the socket
    /// closed or errored).
    pub async fn accept(&self) -> io::Result<QuicTransport> {
        let mut rx = self.accept_rx.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "quic demux task stopped"))
    }
}

/// Owns the listener socket, reads every datagram, and routes it to
/// the connection registered for its peer address. A datagram whose
/// peer is unknown and that carries an Initial packet starts a new
/// connection: the demux creates its per-connection inbound channel,
/// spawns its driver, and publishes the [`QuicTransport`] on
/// `accept_tx`. Non-Initial datagrams from unknown peers are dropped.
async fn demux_loop(
    socket: Arc<UdpSocket>,
    local_addr: SocketAddr,
    config: QuicConfig,
    seed: [u8; 16],
    accept_tx: mpsc::Sender<QuicTransport>,
) {
    let mut routes: std::collections::HashMap<SocketAddr, mpsc::Sender<Datagram>> =
        std::collections::HashMap::new();
    let mut buf = vec![0u8; 65535];
    loop {
        let Ok((n, peer)) = socket.recv_from(&mut buf).await else {
            return;
        };
        // Route to an existing connection if we have one; prune it if
        // its driver has gone away.
        if let Some(tx) = routes.get(&peer) {
            if tx.send(buf[..n].to_vec()).await.is_ok() {
                continue;
            }
            routes.remove(&peer);
        }
        // No live route: only an Initial packet may open a new
        // connection.
        let Ok(hdr) = quiche::Header::from_slice(&mut buf[..n], quiche::MAX_CONN_ID_LEN) else {
            continue;
        };
        if hdr.ty != quiche::Type::Initial {
            continue;
        }
        let Ok(mut quiche_cfg) = config.build(true) else {
            continue;
        };
        let scid = quiche::ConnectionId::from_ref(&seed);
        let Ok(conn) = quiche::accept(&scid, None, local_addr, peer, &mut quiche_cfg) else {
            continue;
        };
        let (inbound_tx, inbound_rx) = mpsc::channel::<Datagram>(64);
        let transport = spawn_driver(
            Arc::clone(&socket),
            conn,
            peer,
            ConnRole::Server,
            InboundSource::Channel(inbound_rx),
            Some(buf[..n].to_vec()),
        );
        routes.insert(peer, inbound_tx);
        if accept_tx.send(transport).await.is_err() {
            return;
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
    // The client owns its own connected UDP socket, so its driver reads
    // datagrams straight off the socket -- there is only ever one peer
    // and no demultiplexing to do.
    let transport = spawn_driver(
        Arc::new(sock),
        conn,
        peer,
        ConnRole::Client,
        InboundSource::Socket,
        None,
    );
    Ok(transport)
}

/// Where a connection driver reads inbound datagrams from.
///
/// A client driver owns a connected socket and reads it directly
/// ([`Socket`](Self::Socket)); a server driver shares the listener
/// socket, so the listener's demux task feeds it datagrams over a
/// channel ([`Channel`](Self::Channel)).
enum InboundSource {
    Socket,
    Channel(mpsc::Receiver<Datagram>),
}

#[allow(clippy::too_many_lines)]
fn spawn_driver(
    socket: Arc<UdpSocket>,
    mut conn: quiche::Connection,
    peer: SocketAddr,
    role: ConnRole,
    mut inbound: InboundSource,
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
        // Reused batch buffer for the app-to-driver select branch.
        let mut inbound_batch: Vec<Vec<u8>> = Vec::new();

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
                        Err(
                            quiche::Error::Done
                            | quiche::Error::StreamLimit
                            | quiche::Error::FlowControl,
                        ) => break,
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

            // Cap the loop wake interval so a scheduled QUIC timeout
            // (loss detection, idle) still fires promptly. Newly-queued
            // application bytes and inbound datagrams wake the select
            // directly through their own branches, so this is only a
            // ceiling, not the polling cadence.
            let timeout = conn
                .timeout()
                .unwrap_or(Duration::from_millis(50))
                .min(Duration::from_millis(10));
            match &mut inbound {
                InboundSource::Socket => {
                    tokio::select! {
                        () = closed_for_driver.notified() => { done = true; }
                        () = tokio::time::sleep(timeout) => { conn.on_timeout(); }
                        // Wake as soon as the app queues bytes so the
                        // next loop iteration flushes them onto the
                        // stream without waiting for a timer tick.
                        n = app_to_driver_rx.recv_many(&mut inbound_batch, 32) => {
                            if n == 0 {
                                done = true;
                            } else {
                                for b in inbound_batch.drain(..) {
                                    pending_app_bytes.extend_from_slice(&b);
                                }
                            }
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
                InboundSource::Channel(rx) => {
                    tokio::select! {
                        () = closed_for_driver.notified() => { done = true; }
                        () = tokio::time::sleep(timeout) => { conn.on_timeout(); }
                        n = app_to_driver_rx.recv_many(&mut inbound_batch, 32) => {
                            if n == 0 {
                                done = true;
                            } else {
                                for b in inbound_batch.drain(..) {
                                    pending_app_bytes.extend_from_slice(&b);
                                }
                            }
                        }
                        dgram = rx.recv() => {
                            match dgram {
                                Some(mut pkt) => {
                                    let info = quiche::RecvInfo { from: peer, to: local_addr };
                                    if let Err(e) = conn.recv(pkt.as_mut_slice(), info) {
                                        tracing::debug!(?role, ?e, "quic conn.recv error");
                                    }
                                }
                                None => { done = true; }
                            }
                        }
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

/// QUIC client-plane proxy listener.
///
/// Mirrors the [`crate::net::Proxy`] role for QUIC: every
/// accepted [`QuicTransport`] is wrapped in a [`Conn`] and
/// driven through the same [`client_loop`] the TCP path uses.
/// The accept loop is single-threaded by design (the QUIC
/// listener owns one UDP socket and serialises Initial-packet
/// processing) but each accepted connection is spawned onto
/// its own task so client_loops run concurrently.
///
/// # Examples
///
/// ```ignore
/// use std::net::SocketAddr;
/// use std::sync::Arc;
/// use dynomite::net::quic::{QuicConfig, QuicProxy};
/// use dynomite::net::NoopDispatcher;
///
/// # async fn build() {
/// let cfg = QuicConfig::server_with_cert_paths("/tmp/c.pem", "/tmp/k.pem");
/// let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
/// let _proxy = QuicProxy::bind(addr, Arc::new(NoopDispatcher), cfg).await.unwrap();
/// # }
/// ```
pub struct QuicProxy {
    listener: QuicListener,
    dispatcher: Arc<dyn Dispatcher>,
    data_store: DataStore,
    response_capacity: usize,
}

impl QuicProxy {
    /// Bind a QUIC proxy listener to the given address.
    ///
    /// The supplied [`QuicConfig`] must carry a server
    /// certificate chain and matching private key; the QUIC
    /// stack rejects unconfigured server roles at handshake
    /// time.
    ///
    /// # Errors
    /// Forwarded from the underlying [`QuicListener::bind`] /
    /// `tokio::net::UdpSocket::bind` calls.
    pub async fn bind<A: Into<SocketAddr>>(
        addr: A,
        dispatcher: Arc<dyn Dispatcher>,
        config: QuicConfig,
    ) -> Result<Self, NetError> {
        let listener = QuicListener::bind(addr.into(), config)
            .await
            .map_err(NetError::Io)?;
        Ok(Self {
            listener,
            dispatcher,
            data_store: DataStore::Valkey,
            response_capacity: 64,
        })
    }

    /// Override the datastore the per-client FSMs will parse.
    /// Defaults to [`DataStore::Valkey`].
    #[must_use]
    pub fn with_data_store(mut self, ds: DataStore) -> Self {
        self.data_store = ds;
        self
    }

    /// Override the response-channel capacity per accepted
    /// connection.
    #[must_use]
    pub fn with_response_capacity(mut self, n: usize) -> Self {
        self.response_capacity = n.max(1);
        self
    }

    /// Local address of the bound UDP socket.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr()
    }

    /// Drive the accept loop until the supplied cancel future
    /// resolves.
    ///
    /// Each accepted [`QuicTransport`] is tagged
    /// [`ConnRole::Client`] and handed to a per-connection
    /// [`client_loop`] task.
    ///
    /// # Errors
    /// Forwarded from the listener accept call.
    #[tracing::instrument(
        name = "quic_proxy.run",
        skip_all,
        fields(local = %self.listener.local_addr()),
    )]
    pub async fn run(
        self,
        cancel: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    ) -> Result<(), NetError> {
        let mut cancel = cancel;
        let mut clients: Vec<JoinHandle<Result<(), NetError>>> = Vec::new();
        let listener = self.listener;
        loop {
            tokio::select! {
                () = &mut cancel => break,
                res = listener.accept() => {
                    let transport = match res {
                        Ok(t) => t,
                        Err(e) => return Err(NetError::Io(e)),
                    };
                    let role = ConnRole::Client;
                    let peer = transport.peer_addr_socket();
                    let conn_transport: Box<dyn Transport> = Box::new(transport);
                    let conn = Conn::new(conn_transport, role);
                    let dispatcher = Arc::clone(&self.dispatcher);
                    let cap = self.response_capacity;
                    let ds = self.data_store;
                    tracing::debug!(?peer, "quic_proxy accepted client");
                    let accept_span = tracing::info_span!(
                        "client.accept",
                        peer = %peer,
                    );
                    let handle = tokio::spawn(
                        async move {
                            let (tx, rx) = mpsc::channel(cap);
                            let handler = ClientHandler::new(dispatcher, tx, ds);
                            client_loop(conn, handler, rx).await
                        }
                        .instrument(accept_span),
                    );
                    clients.push(handle);
                }
            }
            clients.retain(|h| !h.is_finished());
        }
        for h in clients {
            // Match the TCP path's drain budget; see
            // `Proxy::run` for the rationale.
            let _ = tokio::time::timeout(Duration::from_millis(250), h).await;
        }
        Ok(())
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
