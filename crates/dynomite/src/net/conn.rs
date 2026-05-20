//! Per-connection state and the shared event loop.
//!
//! [`Conn`] is the Rust counterpart of the C engine's `struct conn`.
//! It owns:
//!
//! * the [`Transport`] the connection reads and writes through,
//! * the recv mbuf chain ([`MbufQueue`]) the parser drains,
//! * the send mbuf chain the writer drains,
//! * the in-queue (`imsg_q`) of incoming requests waiting to be
//!   forwarded,
//! * the out-queue (`omsg_q`) of outstanding requests awaiting
//!   responses,
//! * the parser scratch state for partial messages,
//! * counters and lifecycle bits the FSM sets and reads.
//!
//! The role-specific behavior (PROXY accepts, CLIENT parses
//! datastore requests, SERVER pumps to the backend, etc.) lives in
//! the sibling modules; [`Conn`] hosts the shared data shape and a
//! small set of top-level lifecycle methods so multiple roles can
//! reuse the same skeleton.
//!
//! # Examples
//!
//! ```no_run
//! use dynomite::io::reactor::{ConnRole, TcpTransport};
//! use dynomite::net::Conn;
//! use tokio::net::TcpStream;
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
//! let transport = Box::new(TcpTransport::new(stream, ConnRole::Server));
//! let mut conn = Conn::new(transport, ConnRole::Server);
//! assert_eq!(conn.role(), ConnRole::Server);
//! conn.close();
//! # });
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::core::types::MsgId;
use crate::io::mbuf::{MbufPool, MbufQueue};
use crate::io::reactor::{ConnRole, Transport};
use crate::msg::{ConsistencyLevel, Msg, MsgQueue};

use super::NetError;

/// Maximum queue depth before back-pressure kicks in.
///
/// Mirrors the reference `MAX_CONN_QUEUE_SIZE` constant in
/// `dyn_connection.h`. The C engine logs and rejects on overflow;
/// the Rust port surfaces the same behavior through
/// [`Conn::enqueue_in`] / [`Conn::enqueue_out`] which return
/// [`NetError::PoolExhausted`] when the cap is reached.
pub const MAX_CONN_QUEUE_SIZE: usize = 20_000;

/// Lightweight rolling counters carried by every [`Conn`].
///
/// Mirrors the per-connection byte counters and event totals that
/// the C engine carries on `struct conn` (`recv_bytes`, `send_bytes`,
/// `events`).
#[derive(Debug, Default, Clone)]
pub struct ConnStats {
    /// Bytes successfully read into the recv mbuf chain.
    pub recv_bytes: u64,
    /// Bytes successfully written from the send mbuf chain.
    pub send_bytes: u64,
    /// Number of messages enqueued onto `imsg_q`.
    pub recv_msgs: u64,
    /// Number of messages enqueued onto `omsg_q`.
    pub send_msgs: u64,
    /// Number of times the read path completed.
    pub recv_events: u64,
    /// Number of times the write path completed.
    pub send_events: u64,
}

/// Stable, process-unique connection identifier.
///
/// Connections are identified by a monotonic 64-bit counter so the
/// id stays unique across reconnects and across transports (TCP,
/// QUIC).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ConnHandle(u64);

impl ConnHandle {
    /// Borrow the raw 64-bit id.
    ///
    /// # Examples
    /// ```
    /// # use dynomite::io::reactor::{ConnRole, TcpTransport};
    /// # use dynomite::net::Conn;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    /// let addr = listener.local_addr().unwrap();
    /// let _accept = tokio::spawn(async move {
    ///     let (s, _) = listener.accept().await.unwrap();
    ///     drop(s);
    /// });
    /// let s = tokio::net::TcpStream::connect(addr).await.unwrap();
    /// let transport = Box::new(TcpTransport::new(s, ConnRole::Server));
    /// let conn = Conn::new(transport, ConnRole::Server);
    /// assert!(conn.handle().raw() > 0);
    /// # });
    /// ```
    #[must_use]
    pub fn raw(self) -> u64 {
        self.0
    }
}

static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

fn next_handle() -> ConnHandle {
    ConnHandle(NEXT_HANDLE.fetch_add(1, Ordering::Relaxed))
}

/// Connection state shared across role-specific FSMs.
#[allow(clippy::struct_excessive_bools)]
pub struct Conn {
    handle: ConnHandle,
    role: ConnRole,
    transport: Option<Box<dyn Transport>>,
    peer_addr: Option<SocketAddr>,
    recv: MbufQueue,
    send: MbufQueue,
    imsg_q: MsgQueue,
    omsg_q: MsgQueue,
    rmsg: Option<Msg>,
    smsg: Option<Msg>,
    stats: ConnStats,
    eof: bool,
    done: bool,
    err: Option<String>,
    read_consistency: ConsistencyLevel,
    write_consistency: ConsistencyLevel,
    same_dc: bool,
    dyn_mode: bool,
    dnode_secured: bool,
    crypto_key_sent: bool,
    aes_key: Option<[u8; 32]>,
    outstanding: HashMap<MsgId, MsgId>,
    pool: MbufPool,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Lifecycle bits and counters - the transport handle and
        // the mbuf pool intentionally render as opaque.
        let _ = (
            &self.transport,
            &self.read_consistency,
            &self.write_consistency,
            &self.same_dc,
            &self.dyn_mode,
            &self.dnode_secured,
            &self.crypto_key_sent,
            &self.aes_key,
            &self.outstanding,
            &self.pool,
            &self.stats,
            &self.rmsg,
            &self.smsg,
        );
        f.debug_struct("Conn")
            .field("handle", &self.handle)
            .field("role", &self.role)
            .field("peer_addr", &self.peer_addr)
            .field("recv_chain", &self.recv.len())
            .field("send_chain", &self.send.len())
            .field("imsg_q", &self.imsg_q.len())
            .field("omsg_q", &self.omsg_q.len())
            .field("recv_bytes", &self.stats.recv_bytes)
            .field("send_bytes", &self.stats.send_bytes)
            .field("eof", &self.eof)
            .field("done", &self.done)
            .field("err", &self.err)
            .field("read_consistency", &self.read_consistency)
            .field("write_consistency", &self.write_consistency)
            .field("same_dc", &self.same_dc)
            .field("dyn_mode", &self.dyn_mode)
            .field("dnode_secured", &self.dnode_secured)
            .field("crypto_key_sent", &self.crypto_key_sent)
            .field("aes_key_set", &self.aes_key.is_some())
            .field("outstanding", &self.outstanding.len())
            .finish()
    }
}

impl Conn {
    /// Build a new connection wrapping `transport` and tagged with
    /// `role`.
    ///
    /// The connection starts with empty mbuf chains, no in-flight
    /// messages, and the default consistency knobs (`DcOne`).
    /// Role-specific drivers in this module set extra fields (the
    /// dnode peer paths set `same_dc`, `dyn_mode`, etc.) before
    /// invoking [`Conn::run`].
    ///
    /// # Examples
    /// ```no_run
    /// use dynomite::io::reactor::{ConnRole, TcpTransport};
    /// use dynomite::net::Conn;
    /// use tokio::net::TcpStream;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let s = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    /// let _ = Conn::new(Box::new(TcpTransport::new(s, ConnRole::Server)), ConnRole::Server);
    /// # });
    /// ```
    pub fn new(transport: Box<dyn Transport>, role: ConnRole) -> Self {
        let peer_addr = transport.peer_addr();
        Self {
            handle: next_handle(),
            role,
            transport: Some(transport),
            peer_addr,
            recv: MbufQueue::new(),
            send: MbufQueue::new(),
            imsg_q: MsgQueue::new(),
            omsg_q: MsgQueue::new(),
            rmsg: None,
            smsg: None,
            stats: ConnStats::default(),
            eof: false,
            done: false,
            err: None,
            read_consistency: ConsistencyLevel::DcOne,
            write_consistency: ConsistencyLevel::DcOne,
            same_dc: true,
            dyn_mode: matches!(
                role,
                ConnRole::DnodePeerProxy | ConnRole::DnodePeerClient | ConnRole::DnodePeerServer
            ),
            dnode_secured: false,
            crypto_key_sent: false,
            aes_key: None,
            outstanding: HashMap::new(),
            pool: MbufPool::default(),
        }
    }

    /// Stable connection handle.
    #[must_use]
    pub fn handle(&self) -> ConnHandle {
        self.handle
    }

    /// Connection role.
    #[must_use]
    pub fn role(&self) -> ConnRole {
        self.role
    }

    /// Remote address, if the transport could surface one.
    #[must_use]
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    /// Borrow the rolling counters.
    #[must_use]
    pub fn stats(&self) -> &ConnStats {
        &self.stats
    }

    /// Borrow the recv mbuf chain.
    #[must_use]
    pub fn recv_chain(&self) -> &MbufQueue {
        &self.recv
    }

    /// Mutably borrow the recv mbuf chain.
    pub fn recv_chain_mut(&mut self) -> &mut MbufQueue {
        &mut self.recv
    }

    /// Borrow the send mbuf chain.
    #[must_use]
    pub fn send_chain(&self) -> &MbufQueue {
        &self.send
    }

    /// Mutably borrow the send mbuf chain.
    pub fn send_chain_mut(&mut self) -> &mut MbufQueue {
        &mut self.send
    }

    /// Borrow the in-queue.
    #[must_use]
    pub fn imsg_q(&self) -> &MsgQueue {
        &self.imsg_q
    }

    /// Mutably borrow the in-queue.
    pub fn imsg_q_mut(&mut self) -> &mut MsgQueue {
        &mut self.imsg_q
    }

    /// Borrow the out-queue.
    #[must_use]
    pub fn omsg_q(&self) -> &MsgQueue {
        &self.omsg_q
    }

    /// Mutably borrow the out-queue.
    pub fn omsg_q_mut(&mut self) -> &mut MsgQueue {
        &mut self.omsg_q
    }

    /// Borrow the partially-received message, if any.
    #[must_use]
    pub fn rmsg(&self) -> Option<&Msg> {
        self.rmsg.as_ref()
    }

    /// Mutably borrow the partially-received message.
    pub fn rmsg_mut(&mut self) -> Option<&mut Msg> {
        self.rmsg.as_mut()
    }

    /// Take ownership of the partially-received message, leaving
    /// the slot empty.
    pub fn take_rmsg(&mut self) -> Option<Msg> {
        self.rmsg.take()
    }

    /// Install the partially-received message slot.
    pub fn set_rmsg(&mut self, msg: Option<Msg>) {
        self.rmsg = msg;
    }

    /// Borrow the partially-sent message, if any.
    #[must_use]
    pub fn smsg(&self) -> Option<&Msg> {
        self.smsg.as_ref()
    }

    /// Take the partially-sent message.
    pub fn take_smsg(&mut self) -> Option<Msg> {
        self.smsg.take()
    }

    /// Install the partially-sent message slot.
    pub fn set_smsg(&mut self, msg: Option<Msg>) {
        self.smsg = msg;
    }

    /// True when the peer has closed its half of the connection.
    #[must_use]
    pub fn is_eof(&self) -> bool {
        self.eof
    }

    /// Mark the peer half as closed.
    pub fn set_eof(&mut self) {
        self.eof = true;
    }

    /// True when the connection has finished (either side closed
    /// and all queued work drained).
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Mark the connection as done.
    pub fn set_done(&mut self) {
        self.done = true;
    }

    /// Last recorded error, if any.
    #[must_use]
    pub fn err(&self) -> Option<&str> {
        self.err.as_deref()
    }

    /// Record a transport-level error string.
    pub fn set_err<S: Into<String>>(&mut self, msg: S) {
        self.err = Some(msg.into());
    }

    /// Read consistency level applied to incoming reads on this
    /// connection.
    #[must_use]
    pub fn read_consistency(&self) -> ConsistencyLevel {
        self.read_consistency
    }

    /// Write consistency level applied to incoming writes on this
    /// connection.
    #[must_use]
    pub fn write_consistency(&self) -> ConsistencyLevel {
        self.write_consistency
    }

    /// Update the read consistency level.
    pub fn set_read_consistency(&mut self, c: ConsistencyLevel) {
        self.read_consistency = c;
    }

    /// Update the write consistency level.
    pub fn set_write_consistency(&mut self, c: ConsistencyLevel) {
        self.write_consistency = c;
    }

    /// True for peer connections inside the local datacenter.
    #[must_use]
    pub fn same_dc(&self) -> bool {
        self.same_dc
    }

    /// Set the same-DC bit. The dnode-peer drivers update this
    /// after consulting the snitch.
    pub fn set_same_dc(&mut self, on: bool) {
        self.same_dc = on;
    }

    /// True when the connection participates in the dnode peer
    /// plane.
    #[must_use]
    pub fn dyn_mode(&self) -> bool {
        self.dyn_mode
    }

    /// True when the dnode handshake exchanged a per-connection
    /// AES key.
    #[must_use]
    pub fn dnode_secured(&self) -> bool {
        self.dnode_secured
    }

    /// Mark the connection as secured (the handshake completed).
    pub fn set_dnode_secured(&mut self, on: bool) {
        self.dnode_secured = on;
    }

    /// True when the local end has emitted the encrypted AES key.
    #[must_use]
    pub fn crypto_key_sent(&self) -> bool {
        self.crypto_key_sent
    }

    /// Mark the AES key as sent.
    pub fn set_crypto_key_sent(&mut self, on: bool) {
        self.crypto_key_sent = on;
    }

    /// Return the per-connection AES key, if one has been
    /// installed.
    #[must_use]
    pub fn aes_key(&self) -> Option<&[u8; 32]> {
        self.aes_key.as_ref()
    }

    /// Install (or replace) the per-connection AES key.
    pub fn set_aes_key(&mut self, key: [u8; 32]) {
        self.aes_key = Some(key);
    }

    /// Borrow the outstanding-msg map.
    #[must_use]
    pub fn outstanding(&self) -> &HashMap<MsgId, MsgId> {
        &self.outstanding
    }

    /// Mutably borrow the outstanding-msg map.
    pub fn outstanding_mut(&mut self) -> &mut HashMap<MsgId, MsgId> {
        &mut self.outstanding
    }

    /// Borrow the pool used for fresh mbuf chunks.
    #[must_use]
    pub fn mbuf_pool(&self) -> &MbufPool {
        &self.pool
    }

    /// Replace the mbuf pool. Useful when the embedding application
    /// wants every connection to share a single pool.
    pub fn set_mbuf_pool(&mut self, pool: MbufPool) {
        self.pool = pool;
    }

    /// Take ownership of the underlying transport, leaving the
    /// connection in a half-closed state. Returns `None` if the
    /// transport has already been moved out (typically by
    /// [`Conn::run`]).
    pub fn take_transport(&mut self) -> Option<Box<dyn Transport>> {
        self.transport.take()
    }

    /// Reinstall a transport. Used by reconnect logic in
    /// [`crate::net::ConnPool`].
    pub fn set_transport(&mut self, transport: Box<dyn Transport>) {
        self.peer_addr = transport.peer_addr();
        self.transport = Some(transport);
    }

    /// True when a transport is currently installed.
    #[must_use]
    pub fn has_transport(&self) -> bool {
        self.transport.is_some()
    }

    /// Mutably borrow the transport. Driver loops use this to drive
    /// `AsyncRead` / `AsyncWrite` directly.
    pub fn transport_mut(&mut self) -> Option<&mut Box<dyn Transport>> {
        self.transport.as_mut()
    }

    /// Enqueue a request onto the in-queue.
    ///
    /// # Errors
    /// Returns [`NetError::PoolExhausted`] when the queue is at
    /// [`MAX_CONN_QUEUE_SIZE`].
    pub fn enqueue_in(&mut self, msg: Msg) -> Result<(), NetError> {
        if self.imsg_q.len() >= MAX_CONN_QUEUE_SIZE {
            return Err(NetError::PoolExhausted);
        }
        self.imsg_q.push_back(msg);
        self.stats.recv_msgs += 1;
        Ok(())
    }

    /// Enqueue a message onto the out-queue.
    ///
    /// # Errors
    /// Returns [`NetError::PoolExhausted`] when the queue is at
    /// [`MAX_CONN_QUEUE_SIZE`].
    pub fn enqueue_out(&mut self, msg: Msg) -> Result<(), NetError> {
        if self.omsg_q.len() >= MAX_CONN_QUEUE_SIZE {
            return Err(NetError::PoolExhausted);
        }
        self.omsg_q.push_back(msg);
        self.stats.send_msgs += 1;
        Ok(())
    }

    /// Drop the transport and mark the connection as done.
    pub fn close(&mut self) {
        self.transport = None;
        self.done = true;
    }

    /// Idle no-op driver hook.
    ///
    /// Stage 9 wires the PROXY / CLIENT / SERVER / DNODE_PEER_*
    /// roles into dedicated drivers in the sibling modules. Calling
    /// `run` on a [`Conn`] without first installing a driver does
    /// nothing: the connection idles until [`Conn::close`] is
    /// invoked. Real drivers (for example [`super::client::client_loop`])
    /// take a `&mut Conn` directly.
    ///
    /// # Errors
    ///
    /// Returns [`NetError::Closed`] when the transport has already
    /// been moved out, e.g. by an earlier driver.
    pub fn run(&mut self) -> Result<(), NetError> {
        if self.transport.is_none() {
            return Err(NetError::Closed);
        }
        if self.done {
            return Ok(());
        }
        Ok(())
    }

    /// Bump the recv-bytes counter.
    pub fn record_recv(&mut self, bytes: usize) {
        self.stats.recv_bytes += bytes as u64;
        self.stats.recv_events += 1;
    }

    /// Bump the send-bytes counter.
    pub fn record_send(&mut self, bytes: usize) {
        self.stats.send_bytes += bytes as u64;
        self.stats.send_events += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::reactor::TcpTransport;
    use crate::msg::MsgType;
    use tokio::net::{TcpListener, TcpStream};

    async fn pair() -> (Conn, Conn) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let (s, _) = listener.accept().await.unwrap();
            s
        });
        let client = TcpStream::connect(addr).await.unwrap();
        let server = accept.await.unwrap();
        let c = Conn::new(
            Box::new(TcpTransport::new(client, ConnRole::Client)),
            ConnRole::Client,
        );
        let s = Conn::new(
            Box::new(TcpTransport::new(server, ConnRole::Server)),
            ConnRole::Server,
        );
        (c, s)
    }

    #[tokio::test]
    async fn enqueue_in_and_out() {
        let (mut c, _s) = pair().await;
        c.enqueue_in(Msg::new(1, MsgType::ReqRedisGet, true))
            .unwrap();
        c.enqueue_out(Msg::new(2, MsgType::RspRedisStatus, false))
            .unwrap();
        assert_eq!(c.imsg_q().len(), 1);
        assert_eq!(c.omsg_q().len(), 1);
        assert_eq!(c.stats().recv_msgs, 1);
        assert_eq!(c.stats().send_msgs, 1);
    }

    #[tokio::test]
    async fn close_drops_transport() {
        let (mut c, _s) = pair().await;
        assert!(c.has_transport());
        c.close();
        assert!(!c.has_transport());
        assert!(c.is_done());
    }

    #[tokio::test]
    async fn handle_is_unique() {
        let (a, b) = pair().await;
        assert_ne!(a.handle(), b.handle());
    }

    #[tokio::test]
    async fn role_seed_drives_dyn_mode() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _accept = tokio::spawn(async move {
            let (s, _) = listener.accept().await.unwrap();
            drop(s);
        });
        let s = TcpStream::connect(addr).await.unwrap();
        let c = Conn::new(
            Box::new(TcpTransport::new(s, ConnRole::DnodePeerServer)),
            ConnRole::DnodePeerServer,
        );
        assert!(c.dyn_mode());
        assert!(c.same_dc());
    }
}
