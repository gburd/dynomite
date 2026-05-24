//! SERVER-role connection driver.
//!
//! The SERVER role holds an outbound connection to the backend
//! datastore (Redis or memcache). The driver pulls requests off
//! the connection's in-queue, encodes them onto the wire, and
//! parses response bytes back into [`Msg`]s that it dispatches to
//! the originating client.
//!
//! Stage 9 ships a minimal, transport-agnostic driver suitable
//! for the loopback echo integration test. Stage 10 wires the
//! driver to the cluster's [`Dispatcher`] so client / server
//! connections form a complete request-response pipeline.
//!
//! [`Dispatcher`]: crate::net::Dispatcher
//! [`Msg`]: crate::msg::Msg

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::Instrument as _;

use crate::conf::DataStore;
use crate::core::types::MsgId;
use crate::io::reactor::ConnRole;
use crate::msg::{Msg, MsgParseResult, MsgType};
use crate::net::conn::Conn;
use crate::net::dispatcher::OutboundEnvelope;
use crate::net::NetError;
use crate::proto::dnode::DmsgType;

/// Outbound server-side connection driver.
///
/// The struct owns the transport-bound [`Conn`] plus the receiving
/// half of the request channel that feeds it.
pub struct ServerConn {
    conn: Conn,
    requests: mpsc::Receiver<OutboundRequest>,
    data_store: DataStore,
    pending_responses: std::collections::VecDeque<(MsgId, tracing::Span)>,
}

/// Envelope sent into the server driver.
///
/// The driver writes `bytes` to the transport then awaits a
/// response, which it forwards as an [`OutboundEnvelope`] on
/// `responder` along with `req_id`.
///
/// `span` carries the originating client request's
/// [`tracing::Span`] across the mpsc channel boundary so the
/// receiving task's work nests under the originating client
/// span when distributed tracing is enabled. The default value
/// is [`tracing::Span::none`], which has no overhead when no
/// subscriber is installed.
///
/// `ty` selects the dnode message-type header emitted on the
/// peer plane. Data-plane callers leave it at
/// [`DmsgType::Req`]; the gossip task uses
/// [`DmsgType::GossipSyn`] / [`DmsgType::GossipShutdown`] for
/// fire-and-forget control frames whose `responder` is never
/// signalled.
#[derive(Debug)]
pub struct OutboundRequest {
    /// Wire bytes already encoded by the dispatcher.
    pub bytes: Vec<u8>,
    /// Request id for response tagging.
    pub req_id: MsgId,
    /// Channel the driver pushes the parsed response onto.
    pub responder: mpsc::Sender<OutboundEnvelope>,
    /// Originating client request span; entered by the receiver
    /// to nest backend / peer work under the request tree.
    pub span: tracing::Span,
    /// dnode message-type header emitted by the peer driver.
    /// Defaults to [`DmsgType::Req`] for data-plane requests.
    pub ty: DmsgType,
}

impl ServerConn {
    /// Wrap an outbound [`Conn`] with the given request-channel
    /// receiver and data-store flavor.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dynomite::conf::DataStore;
    /// use dynomite::io::reactor::{ConnRole, TcpTransport};
    /// use dynomite::net::{Conn, ServerConn};
    /// use tokio::sync::mpsc;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let s = tokio::net::TcpStream::connect("127.0.0.1:6379").await.unwrap();
    /// let conn = Conn::new(Box::new(TcpTransport::new(s, ConnRole::Server)), ConnRole::Server);
    /// let (_tx, rx) = mpsc::channel(8);
    /// let _ = ServerConn::new(conn, rx, DataStore::Redis);
    /// # });
    /// ```
    #[must_use]
    pub fn new(
        conn: Conn,
        requests: mpsc::Receiver<OutboundRequest>,
        data_store: DataStore,
    ) -> Self {
        debug_assert!(matches!(
            conn.role(),
            ConnRole::Server | ConnRole::DnodePeerServer
        ));
        Self {
            conn,
            requests,
            data_store,
            pending_responses: std::collections::VecDeque::new(),
        }
    }

    /// Borrow the underlying connection.
    #[must_use]
    pub fn conn(&self) -> &Conn {
        &self.conn
    }

    /// Mutably borrow the underlying connection.
    pub fn conn_mut(&mut self) -> &mut Conn {
        &mut self.conn
    }

    /// Drive the server FSM until either the request channel is
    /// closed or the transport hits EOF / error.
    ///
    /// # Errors
    /// Surfaces transport- and protocol-level errors.
    pub async fn run(mut self) -> Result<(), NetError> {
        let mut read_buf = vec![0u8; 4096];
        let mut accumulated = Vec::<u8>::new();
        let mut pending_responder: Option<mpsc::Sender<OutboundEnvelope>> = None;

        loop {
            if self.conn.is_eof() && self.pending_responses.is_empty() {
                self.conn.set_done();
                return Ok(());
            }

            tokio::select! {
                res = self.requests.recv() => {
                    let Some(out_req) = res else {
                        // Channel closed; drain pending responses and exit.
                        if self.pending_responses.is_empty() {
                            self.conn.set_done();
                            return Ok(());
                        }
                        continue;
                    };
                    let send_span = tracing::info_span!(
                        parent: &out_req.span,
                        "backend.send",
                        req_id = out_req.req_id,
                        bytes = out_req.bytes.len(),
                    );
                    let req_span = out_req.span.clone();
                    let req_bytes = out_req.bytes;
                    let transport = self.conn.transport_mut().ok_or(NetError::Closed)?;
                    let write_res = async { transport.write_all(&req_bytes).await }
                        .instrument(send_span)
                        .await;
                    write_res?;
                    self.conn.record_send(req_bytes.len());
                    self.pending_responses.push_back((out_req.req_id, req_span));
                    pending_responder = Some(out_req.responder);
                }
                read_res = async {
                    if let Some(t) = self.conn.transport_mut() {
                        t.read(&mut read_buf).await
                    } else {
                        Ok(0)
                    }
                } => {
                    let n = read_res?;
                    if n == 0 {
                        self.conn.set_eof();
                        continue;
                    }
                    self.conn.record_recv(n);
                    accumulated.extend_from_slice(&read_buf[..n]);
                    self.drive_response_parser(&mut accumulated, &mut pending_responder).await?;
                }
            }
        }
    }

    async fn drive_response_parser(
        &mut self,
        accumulated: &mut Vec<u8>,
        responder: &mut Option<mpsc::Sender<OutboundEnvelope>>,
    ) -> Result<(), NetError> {
        use crate::proto::memcache::memcache_parse_rsp;
        use crate::proto::redis::redis_parse_rsp;

        while !accumulated.is_empty() {
            let head_span = self
                .pending_responses
                .front()
                .map_or_else(tracing::Span::current, |(_, s)| s.clone());
            let id = self.pending_responses.front().map_or(0, |(i, _)| *i);
            let mut msg = Msg::new(id, MsgType::Unknown, false);
            let result = match self.data_store {
                DataStore::Redis => redis_parse_rsp(&mut msg, accumulated),
                DataStore::Memcache => memcache_parse_rsp(&mut msg, accumulated),
            };
            match result {
                MsgParseResult::Ok => {
                    let consumed = msg.parser_pos();
                    if consumed == 0 {
                        return Err(NetError::Parse("server parser stalled".into()));
                    }
                    let bytes = accumulated[..consumed].to_vec();
                    accumulated.drain(0..consumed);
                    let (req_id, req_span) =
                        self.pending_responses.pop_front().unwrap_or((0, head_span));
                    let parse_span = tracing::info_span!(
                        parent: &req_span,
                        "backend.parse",
                        req_id,
                        bytes = consumed,
                    );
                    let env = parse_span.in_scope(|| {
                        let mut rsp = msg;
                        let pool = self.conn.mbuf_pool().clone();
                        let mut buf = pool.get();
                        buf.recv(&bytes);
                        rsp.mbufs_mut().push_back(buf);
                        rsp.recompute_mlen();
                        OutboundEnvelope {
                            req_id,
                            rsp,
                            span: req_span,
                        }
                    });
                    if let Some(sender) = responder.as_ref() {
                        let _ = sender.send(env).await;
                    }
                }
                MsgParseResult::Again => return Ok(()),
                MsgParseResult::Repair | MsgParseResult::Noop | MsgParseResult::Fragment => {
                    let consumed = msg.parser_pos();
                    if consumed > 0 {
                        accumulated.drain(0..consumed);
                    } else {
                        return Ok(());
                    }
                }
                MsgParseResult::Error | MsgParseResult::OomError | MsgParseResult::DynoConfig => {
                    return Err(NetError::Parse(format!("{result:?}")));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::reactor::TcpTransport;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn build_server_conn() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _accept = tokio::spawn(async move {
            let (s, _) = listener.accept().await.unwrap();
            drop(s);
        });
        let s = TcpStream::connect(addr).await.unwrap();
        let conn = Conn::new(
            Box::new(TcpTransport::new(s, ConnRole::Server)),
            ConnRole::Server,
        );
        let (_tx, rx) = mpsc::channel(1);
        let server = ServerConn::new(conn, rx, DataStore::Redis);
        assert_eq!(server.conn().role(), ConnRole::Server);
    }
}
