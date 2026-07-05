//! CLIENT-role connection driver.
//!
//! The CLIENT role is the engine's view of an inbound connection
//! from a client (RESP or memcache). The driver:
//!
//! 1. Reads bytes from the [`crate::io::reactor::Transport`] into
//!    the connection's recv mbuf chain.
//! 2. Drives the appropriate datastore parser
//!    ([`crate::proto::redis::redis_parse_req`] or
//!    [`crate::proto::memcache::memcache_parse_req`]) over the
//!    chain.
//! 3. Hands every fully-parsed request to the configured
//!    [`crate::net::Dispatcher`] and waits for a response on the
//!    per-connection mpsc channel.
//! 4. Writes the response bytes back to the transport.
//!
//! The driver runs a single tokio `select!` per iteration, draining
//! pending response bytes first so the loop's read / write
//! arms never block on a saturated peer.
//!
//! The driver does not reach into the cluster layer or the entropy
//! reconciliation directly; both plug in through the [`Dispatcher`]
//! hook the proxy installs, keeping the driver decoupled from
//! routing and repair.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::conf::DataStore;
use crate::core::types::MsgId;
use crate::io::reactor::ConnRole;
use crate::msg::{response, Msg, MsgParseResult, MsgType};
use crate::net::conn::Conn;
use crate::net::dispatcher::{DispatchOutcome, Dispatcher, OutboundEnvelope};
use crate::net::NetError;

/// Read buffer size for the client driver.
///
/// Sized to the typical RESP bulk header so inline GET/SET
/// commands fit in a single read; larger payloads are appended
/// across iterations.
const CLIENT_READ_CHUNK: usize = 4096;

/// Outcome reported by [`client_loop`] when it finishes.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ClientLoopOutcome {
    /// Peer closed (EOF) and queues drained cleanly.
    Eof,
    /// Driver was asked to exit by the dispatcher.
    Cancelled,
}

/// Sink that applies an inbound cross-node object-replica op to
/// the local datastore.
///
/// The dnode peer-receive loop calls [`Self::apply`] once per
/// [`DmsgType::RiakReplica`](crate::proto::dnode::DmsgType::RiakReplica)
/// frame with the frame's opaque payload bytes (the `dyniak`
/// routing layer owns the payload encoding). The engine does not
/// interpret the payload; it hands it to the sink, which decodes
/// it and applies the write to its LOCAL object store.
///
/// Applying a replica op is terminal: the sink must NOT re-forward
/// it to other peers, so a replica write fans out exactly once.
/// The call is fire-and-forget from the wire's perspective (no
/// reply frame is produced), matching Riak's eventual-consistency
/// model where anti-entropy and read-repair reconcile any peer
/// that missed a write.
pub trait ReplicaApplySink: Send + Sync {
    /// Apply the replica op carried by `payload` to the local
    /// datastore. Errors are logged by the implementor and
    /// swallowed; the receive loop never blocks on the result.
    fn apply<'a>(&'a self, payload: &'a [u8]) -> BoxFuture<'a, ()>;
}

/// Boxed future returned by [`ReplicaApplySink::apply`].
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Client-side request handler bundle.
///
/// Built by [`crate::net::proxy::Proxy`] and passed into
/// [`client_loop`].
pub struct ClientHandler {
    dispatcher: Arc<dyn Dispatcher>,
    response_tx: mpsc::Sender<OutboundEnvelope>,
    data_store: DataStore,
    next_msg_id: u64,
    read_timeout: Option<Duration>,
    gossip: Option<Arc<crate::cluster::gossip::GossipHandler>>,
    replica_sink: Option<Arc<dyn ReplicaApplySink>>,
}

impl ClientHandler {
    /// Build a client handler.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::DataStore;
    /// use dynomite::net::{ClientHandler, NoopDispatcher};
    /// use std::sync::Arc;
    /// use tokio::sync::mpsc;
    /// let (tx, _rx) = mpsc::channel(1);
    /// let _h = ClientHandler::new(Arc::new(NoopDispatcher), tx, DataStore::Valkey);
    /// ```
    #[must_use]
    pub fn new(
        dispatcher: Arc<dyn Dispatcher>,
        response_tx: mpsc::Sender<OutboundEnvelope>,
        data_store: DataStore,
    ) -> Self {
        Self {
            dispatcher,
            response_tx,
            data_store,
            next_msg_id: 1,
            read_timeout: None,
            gossip: None,
            replica_sink: None,
        }
    }

    /// Set the per-read timeout. None disables it.
    #[must_use]
    pub fn with_read_timeout(mut self, t: Option<Duration>) -> Self {
        self.read_timeout = t;
        self
    }

    /// Attach a gossip handler. Inbound peer connections served
    /// through this handler dispatch gossip-class dnode frames
    /// into the supplied handler instead of the datastore parser.
    /// Data-plane connections (CLIENT role) leave it `None`.
    #[must_use]
    pub fn with_gossip(mut self, gossip: Arc<crate::cluster::gossip::GossipHandler>) -> Self {
        self.gossip = Some(gossip);
        self
    }

    /// Attach a [`ReplicaApplySink`]. Inbound peer connections
    /// served through this handler apply
    /// [`DmsgType::RiakReplica`](crate::proto::dnode::DmsgType::RiakReplica)
    /// frames to the local datastore via the sink instead of
    /// routing them through the datastore parser, and never
    /// re-forward them.
    #[must_use]
    pub fn with_replica_sink(mut self, sink: Arc<dyn ReplicaApplySink>) -> Self {
        self.replica_sink = Some(sink);
        self
    }

    /// Borrow the attached replica-apply sink, if any.
    #[must_use]
    pub fn replica_sink(&self) -> Option<&Arc<dyn ReplicaApplySink>> {
        self.replica_sink.as_ref()
    }

    /// Borrow the attached gossip handler, if any.
    #[must_use]
    pub fn gossip(&self) -> Option<&Arc<crate::cluster::gossip::GossipHandler>> {
        self.gossip.as_ref()
    }

    /// Datastore the handler parses.
    #[must_use]
    pub fn data_store(&self) -> DataStore {
        self.data_store
    }

    /// Borrow the dispatcher this handler routes parsed requests
    /// into. Exposed so role-specific drivers (CLIENT,
    /// DNODE_PEER_CLIENT) can share the same dispatch contract.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::DataStore;
    /// use dynomite::net::{ClientHandler, NoopDispatcher};
    /// use std::sync::Arc;
    /// use tokio::sync::mpsc;
    /// let (tx, _rx) = mpsc::channel(1);
    /// let h = ClientHandler::new(Arc::new(NoopDispatcher), tx, DataStore::Valkey);
    /// let _ = h.dispatcher();
    /// ```
    #[must_use]
    pub fn dispatcher(&self) -> &Arc<dyn Dispatcher> {
        &self.dispatcher
    }

    /// Borrow the per-connection response sender. The dispatcher
    /// uses a clone of this channel to push asynchronously-produced
    /// responses back to the FSM.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::DataStore;
    /// use dynomite::net::{ClientHandler, NoopDispatcher};
    /// use std::sync::Arc;
    /// use tokio::sync::mpsc;
    /// let (tx, _rx) = mpsc::channel(1);
    /// let h = ClientHandler::new(Arc::new(NoopDispatcher), tx, DataStore::Valkey);
    /// let _clone = h.response_tx().clone();
    /// ```
    #[must_use]
    pub fn response_tx(&self) -> &mpsc::Sender<OutboundEnvelope> {
        &self.response_tx
    }

    fn alloc_msg_id(&mut self) -> MsgId {
        let id = self.next_msg_id;
        self.next_msg_id = self.next_msg_id.wrapping_add(1).max(1);
        id
    }
}

/// Drive the client FSM until the peer closes or the dispatcher
/// asks the driver to exit.
///
/// `rx` receives responses produced by the dispatcher; the driver
/// writes the response bytes to the transport in the order it
/// received them.
///
/// # Errors
/// Any transport-level error is returned. Parse errors are surfaced
/// as [`NetError::Parse`] and end the loop after sending a synthetic
/// error response when possible.
#[tracing::instrument(
    name = "client_loop",
    skip_all,
    fields(
        role = ?conn.role(),
        peer = tracing::field::Empty,
    ),
)]
pub async fn client_loop(
    mut conn: Conn,
    mut handler: ClientHandler,
    mut rx: mpsc::Receiver<OutboundEnvelope>,
) -> Result<(), NetError> {
    debug_assert!(matches!(
        conn.role(),
        ConnRole::Client | ConnRole::DnodePeerClient
    ));

    let mut read_buf = vec![0u8; CLIENT_READ_CHUNK];
    let mut accumulated: Vec<u8> = Vec::new();
    let mut pending_writes: Vec<u8> = Vec::new();

    loop {
        // Flush any buffered response bytes first so the loop
        // exit conditions never block on a full peer.
        if !pending_writes.is_empty() {
            let transport = conn.transport_mut().ok_or(NetError::Closed)?;
            transport.write_all(&pending_writes).await?;
            conn.record_send(pending_writes.len());
            pending_writes.clear();
        }

        if conn.is_eof() && conn.imsg_q().is_empty() && conn.omsg_q().is_empty() {
            conn.set_done();
            return Ok(());
        }

        let read_fut = async {
            let n = match conn.transport_mut() {
                Some(t) => t.read(&mut read_buf).await,
                None => return Ok::<usize, std::io::Error>(0),
            };
            n
        };

        tokio::select! {
            res = read_fut => {
                let n = res?;
                if n == 0 {
                    conn.set_eof();
                    if conn.omsg_q().is_empty() {
                        conn.set_done();
                        return Ok(());
                    }
                    continue;
                }
                conn.record_recv(n);
                accumulated.extend_from_slice(&read_buf[..n]);
                drive_parser(&mut conn, &mut handler, &mut accumulated).await?;
            }
            Some(env) = rx.recv() => {
                handle_response(&mut conn, &env, &mut pending_writes);
            }
            else => {
                conn.set_done();
                return Ok(());
            }
        }
    }
}

#[tracing::instrument(
    name = "client.parse_loop",
    skip_all,
    fields(accumulated = accumulated.len()),
)]
async fn drive_parser(
    conn: &mut Conn,
    handler: &mut ClientHandler,
    accumulated: &mut Vec<u8>,
) -> Result<(), NetError> {
    use crate::proto::memcache::memcache_parse_req;
    use crate::proto::redis::redis_parse_req;

    while !accumulated.is_empty() {
        let id = handler.alloc_msg_id();
        let mut msg = Msg::new(id, MsgType::Unknown, true);
        let consumed_before = msg.parser_pos();
        let parse_result = match handler.data_store {
            DataStore::Valkey | DataStore::Dyniak => redis_parse_req(&mut msg, accumulated),
            DataStore::Memcache => memcache_parse_req(&mut msg, accumulated),
        };
        match parse_result {
            MsgParseResult::Ok => {
                let consumed = msg.parser_pos();
                if consumed == 0 {
                    return Err(NetError::Parse(
                        "parser reported Ok with no bytes consumed".to_string(),
                    ));
                }
                // Per-request span - one is created for every
                // fully-parsed inbound message. Cross-task work
                // (dispatch.plan, backend.send / parse, peer.send /
                // parse, client.send) attaches as children via the
                // captured `tracing::Span` on the OutboundRequest /
                // OutboundEnvelope envelopes.
                let req_span = tracing::info_span!(
                    "client.parse",
                    msg_id = msg.id(),
                    msg_type = ?msg.ty(),
                    bytes = consumed,
                );
                let was_quit = msg.flags().quit;
                let quit_msg_id = if was_quit { Some(msg.id()) } else { None };
                let inline_send: Option<OutboundEnvelope> = req_span.in_scope(|| {
                    // Carry the consumed wire bytes inside the
                    // msg so the dispatcher can forward them to a
                    // backend without having to re-encode. The bytes
                    // live on the msg's own mbuf chain across
                    // recv -> filter -> forward.
                    let pool = conn.mbuf_pool().clone();
                    let mut buf = pool.get();
                    buf.recv(&accumulated[..consumed]);
                    msg.mbufs_mut().push_back(buf);
                    msg.recompute_mlen();
                    accumulated.drain(0..consumed);
                    let _ = consumed_before;
                    conn.outstanding_mut().insert(msg.id(), msg.id());
                    // The placeholder enqueue cannot fail under
                    // normal operation; if it does we surface it
                    // via the outer `?`.
                    conn.enqueue_out(Msg::new(msg.id(), msg.ty(), true))
                        .map_err(|e: NetError| e)?;
                    let outcome = handler
                        .dispatcher
                        .dispatch(msg, handler.response_tx.clone());
                    let inline = match outcome {
                        DispatchOutcome::Pending | DispatchOutcome::Drop => None,
                        DispatchOutcome::Inline(rsp) | DispatchOutcome::Error(rsp) => {
                            Some(OutboundEnvelope {
                                req_id: rsp.id(),
                                rsp,
                                span: tracing::Span::current(),
                                source_peer_idx: None,
                            })
                        }
                    };
                    Ok::<Option<OutboundEnvelope>, NetError>(inline)
                })?;
                if let Some(env) = inline_send {
                    let _ = handler.response_tx.send(env).await;
                }
                if let Some(qid) = quit_msg_id {
                    // Real Redis replies `+OK\r\n` to QUIT and then
                    // closes the client connection. Synthesize the
                    // reply here (the dispatcher returned `Drop`
                    // because there is no key to route) and send it
                    // through the same `response_tx` used by every
                    // other reply, which pops the placeholder we
                    // pushed onto `omsg_q` above. Without this the
                    // outer client loop's exit condition
                    // (`omsg_q.is_empty()`) is never met and the
                    // connection deadlocks until the kernel times
                    // out the read.
                    let pool = conn.mbuf_pool().clone();
                    let mut anchor = Msg::new(qid, MsgType::ReqRedisQuit, true);
                    anchor.set_parent_id(qid);
                    let rsp = response::make_simple_redis(&anchor, &pool, b"+OK\r\n");
                    let env = OutboundEnvelope {
                        req_id: qid,
                        rsp,
                        span: req_span.clone(),
                        source_peer_idx: None,
                    };
                    let _ = handler.response_tx.send(env).await;
                    // Close the connection after replying.
                    conn.set_eof();
                    return Ok(());
                }
            }
            MsgParseResult::Again
            | MsgParseResult::Repair
            | MsgParseResult::Fragment
            | MsgParseResult::Noop => {
                let consumed = msg.parser_pos();
                if consumed > 0 {
                    accumulated.drain(0..consumed);
                } else {
                    return Ok(());
                }
            }
            MsgParseResult::Error | MsgParseResult::OomError | MsgParseResult::DynoConfig => {
                return Err(NetError::Parse(format!("{parse_result:?}")));
            }
        }
    }
    Ok(())
}

fn handle_response(conn: &mut Conn, env: &OutboundEnvelope, pending: &mut Vec<u8>) {
    let _enter = env.span.enter();
    let bytes_len: usize = env.rsp.mbufs().iter().map(|b| b.readable().len()).sum();
    let _send_span =
        tracing::info_span!("client.send", req_id = env.req_id, bytes = bytes_len,).entered();
    for buf in env.rsp.mbufs() {
        pending.extend_from_slice(buf.readable());
    }
    // Pop the matching outstanding entry.
    conn.outstanding_mut().remove(&env.req_id);
    // Pop the placeholder we pushed on enqueue.
    if let Some(front) = conn.omsg_q_mut().front() {
        if front.id() == env.req_id {
            let _ = conn.omsg_q_mut().pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_msg_id_is_monotonic() {
        let (tx, _rx) = mpsc::channel(1);
        let mut h = ClientHandler::new(Arc::new(crate::net::NoopDispatcher), tx, DataStore::Valkey);
        let a = h.alloc_msg_id();
        let b = h.alloc_msg_id();
        assert_eq!(a + 1, b);
    }
}
