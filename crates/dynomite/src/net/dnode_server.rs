//! DNODE_PEER_SERVER-role connection driver.
//!
//! Outbound peer connection: the local node initiates the link to
//! a remote peer. The driver wraps every outgoing request in a
//! dnode header (and, when the pool's `secure_server_option`
//! requires it, encrypts the payload), pumps the wire bytes, then
//! parses the response that comes back and delivers it through
//! the per-request responder channel.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::Instrument as _;

use crate::core::types::MsgId;
use crate::io::reactor::ConnRole;
use crate::msg::{Msg, MsgParseResult, MsgType};
use crate::net::conn::Conn;
use crate::net::dispatcher::OutboundEnvelope;
use crate::net::server::OutboundRequest;
use crate::net::NetError;
use crate::proto::dnode::{dmsg_write, DmsgType, DnodeParser, ParseStep};

/// True when the dnode message type expects a matching response
/// frame from the peer (data plane). Gossip variants are
/// fire-and-forget and never push onto the per-connection
/// pending queue.
fn is_data_plane_ty(ty: DmsgType) -> bool {
    matches!(
        ty,
        DmsgType::Req | DmsgType::ReqForward | DmsgType::Res | DmsgType::Unknown
    )
}

/// Outbound DNODE peer connection driver.
pub struct DnodeServerConn {
    conn: Conn,
    requests: mpsc::Receiver<OutboundRequest>,
    pending: std::collections::VecDeque<(
        MsgId,
        tracing::Span,
        Option<u32>,
        mpsc::Sender<OutboundEnvelope>,
    )>,
}

impl DnodeServerConn {
    /// Wrap an outbound peer connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dynomite::io::reactor::{ConnRole, TcpTransport};
    /// use dynomite::net::{Conn, DnodeServerConn};
    /// use tokio::sync::mpsc;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let s = tokio::net::TcpStream::connect("127.0.0.1:0").await.unwrap();
    /// let conn = Conn::new(Box::new(TcpTransport::new(s, ConnRole::DnodePeerServer)), ConnRole::DnodePeerServer);
    /// let (_tx, rx) = mpsc::channel(8);
    /// let _ = DnodeServerConn::new(conn, rx);
    /// # });
    /// ```
    #[must_use]
    pub fn new(conn: Conn, requests: mpsc::Receiver<OutboundRequest>) -> Self {
        debug_assert!(matches!(conn.role(), ConnRole::DnodePeerServer));
        Self {
            conn,
            requests,
            pending: std::collections::VecDeque::new(),
        }
    }

    /// Drive the FSM.
    ///
    /// # Errors
    /// Forwarded transport / DNODE parse errors.
    pub async fn run(mut self) -> Result<(), NetError> {
        let mut requests = std::mem::replace(&mut self.requests, {
            let (_tx, rx) = mpsc::channel::<OutboundRequest>(1);
            rx
        });
        self.run_with(&mut requests).await
    }

    /// Drive the FSM using a borrowed request receiver. Useful
    /// for reconnect supervisors that own the receiver across
    /// multiple connection attempts and pass it in by reference.
    ///
    /// # Errors
    /// Forwarded transport / DNODE parse errors.
    pub async fn run_with(
        &mut self,
        requests: &mut mpsc::Receiver<OutboundRequest>,
    ) -> Result<(), NetError> {
        let mut read_buf = vec![0u8; 4096];
        let mut accumulated = Vec::<u8>::new();
        let mut parser = DnodeParser::new();

        loop {
            if self.conn.is_eof() && self.pending.is_empty() {
                self.conn.set_done();
                return Ok(());
            }

            tokio::select! {
                req = requests.recv() => {
                    let Some(req) = req else { continue; };
                    let send_span = tracing::info_span!(
                        parent: &req.span,
                        "peer.send",
                        req_id = req.req_id,
                        bytes = req.bytes.len(),
                    );
                    let req_span = req.span.clone();
                    let req_bytes = req.bytes;
                    let req_id = req.req_id;
                    let req_ty = req.ty;
                    let mut header_buf = self.conn.mbuf_pool().get();
                    dmsg_write(
                        &mut header_buf,
                        req_id,
                        if matches!(req_ty, DmsgType::Unknown) { DmsgType::Req } else { req_ty },
                        0,
                        true,
                        None,
                        u32::try_from(req_bytes.len()).unwrap_or(u32::MAX),
                    )?;
                    let header_len = header_buf.readable().len();
                    let transport = self.conn.transport_mut().ok_or(NetError::Closed)?;
                    let write_res = async {
                        transport.write_all(header_buf.readable()).await?;
                        transport.write_all(&req_bytes).await?;
                        Ok::<(), std::io::Error>(())
                    }
                    .instrument(send_span)
                    .await;
                    write_res?;
                    self.conn.record_send(header_len + req_bytes.len());
                    if is_data_plane_ty(req_ty) {
                        // Pair the responder WITH its req_id in the
                        // pending queue. A single shared responder slot
                        // would deliver a reply to whichever request
                        // was enqueued LAST, so under fan-out (several
                        // concurrent requests multiplexed on one peer
                        // connection) replies cross wires. FIFO order
                        // matches the dnode reply order.
                        self.pending.push_back((
                            req_id,
                            req_span,
                            req.target_peer_idx,
                            req.responder,
                        ));
                    } else {
                        // Gossip / control-plane frames are
                        // fire-and-forget; the responder is
                        // dropped here and the originator does
                        // not block waiting for an ACK.
                        drop(req.responder);
                    }
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
                    self.drive_response(&mut accumulated, &mut parser).await?;
                }
            }
        }
    }

    async fn drive_response(
        &mut self,
        accumulated: &mut Vec<u8>,
        parser: &mut DnodeParser,
    ) -> Result<(), NetError> {
        loop {
            if accumulated.is_empty() {
                return Ok(());
            }
            let step = parser.step(accumulated.as_slice());
            match step {
                ParseStep::NeedMore { .. } => return Ok(()),
                ParseStep::Error { consumed } => {
                    return Err(NetError::Dnode(format!(
                        "dnode peer-server parse error after {consumed} bytes"
                    )));
                }
                ParseStep::HeaderDone { consumed } => {
                    let dmsg = parser.take_dmsg();
                    let plen = dmsg.plen as usize;
                    let total = consumed + plen;
                    if accumulated.len() < total {
                        parser.reset();
                        return Ok(());
                    }
                    let payload = accumulated[consumed..total].to_vec();
                    accumulated.drain(0..total);
                    parser.reset();

                    // Build the response Msg from the payload bytes.
                    // Pop the pending entry whose req_id this reply
                    // answers (dnode replies arrive in request order),
                    // recovering its paired responder so the reply
                    // goes back to the request that issued it.
                    let (req_id, req_span, source_peer_idx, reply_to) =
                        match self.pending.pop_front() {
                            Some(entry) => (entry.0, entry.1, entry.2, Some(entry.3)),
                            None => (dmsg.id, tracing::Span::current(), None, None),
                        };
                    let parse_span = tracing::info_span!(
                        parent: &req_span,
                        "peer.parse",
                        req_id,
                        bytes = plen,
                    );
                    let env = parse_span.in_scope(|| {
                        let mut rsp = Msg::new(req_id, MsgType::Unknown, false);
                        let pool = self.conn.mbuf_pool().clone();
                        let mut buf = pool.get();
                        buf.recv(&payload);
                        rsp.mbufs_mut().push_back(buf);
                        rsp.recompute_mlen();
                        rsp.set_dmsg(dmsg);
                        // Mark parse outcome so consumers can branch
                        // on a successful peer round-trip.
                        rsp.set_parse_result(MsgParseResult::Ok);
                        OutboundEnvelope {
                            req_id,
                            rsp,
                            span: req_span,
                            source_peer_idx,
                        }
                    });
                    if let Some(sender) = reply_to.as_ref() {
                        let _ = sender.send(env).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::reactor::TcpTransport;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn build_and_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _accept = tokio::spawn(async move {
            let (s, _) = listener.accept().await.unwrap();
            drop(s);
        });
        let s = TcpStream::connect(addr).await.unwrap();
        let conn = Conn::new(
            Box::new(TcpTransport::new(s, ConnRole::DnodePeerServer)),
            ConnRole::DnodePeerServer,
        );
        let (_tx, rx) = mpsc::channel(1);
        let _server = DnodeServerConn::new(conn, rx);
    }

    /// Regression: two requests multiplexed on one peer connection must
    /// have their replies delivered to their OWN responders. A single
    /// shared responder slot delivered every reply to the last-enqueued
    /// request, crossing wires under fan-out. The peer here echoes two
    /// dnode-framed replies in request order; responder A must receive
    /// req_id 1's reply and responder B req_id 2's.
    #[tokio::test]
    async fn concurrent_requests_route_replies_to_their_own_responders() {
        use crate::proto::dnode::{dmsg_write, DmsgType, DnodeParser, ParseStep};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Mock peer: read two dnode request frames, then reply to each
        // with a distinct payload in the SAME order, echoing the
        // request's id in the reply header.
        let peer = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut s, _) = listener.accept().await.unwrap();
            let mut acc = Vec::new();
            let mut parser = DnodeParser::new();
            let mut ids = Vec::new();
            let mut buf = [0u8; 4096];
            while ids.len() < 2 {
                let n = s.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                acc.extend_from_slice(&buf[..n]);
                while let ParseStep::HeaderDone { consumed } = parser.step(&acc) {
                    let dmsg = parser.take_dmsg();
                    let total = consumed + dmsg.plen as usize;
                    if acc.len() < total {
                        parser.reset();
                        break;
                    }
                    ids.push(dmsg.id);
                    acc.drain(0..total);
                    parser.reset();
                }
            }
            // Reply in request order: id[0] -> "+A\r\n", id[1] -> "+B\r\n".
            let pool = crate::io::mbuf::MbufPool::default();
            for (i, id) in ids.iter().enumerate() {
                let payload: &[u8] = if i == 0 { b"+A\r\n" } else { b"+B\r\n" };
                let mut hb = pool.get();
                let plen = u32::try_from(payload.len()).unwrap_or(0);
                dmsg_write(&mut hb, *id, DmsgType::Res, 0, false, None, plen).unwrap();
                s.write_all(hb.readable()).await.unwrap();
                s.write_all(payload).await.unwrap();
            }
            s.flush().await.unwrap();
            // Keep the socket open briefly so replies flush.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let conn = Conn::new(
            Box::new(TcpTransport::new(stream, ConnRole::DnodePeerServer)),
            ConnRole::DnodePeerServer,
        );
        let (req_tx, req_rx) = mpsc::channel::<OutboundRequest>(4);
        let (resp_a_tx, mut resp_a_rx) = mpsc::channel::<OutboundEnvelope>(1);
        let (resp_b_tx, mut resp_b_rx) = mpsc::channel::<OutboundEnvelope>(1);
        // Enqueue two requests with DISTINCT responders BEFORE the
        // driver runs, so both are pending when replies arrive.
        req_tx
            .send(OutboundRequest {
                bytes: b"*1\r\n$4\r\nPING\r\n".to_vec(),
                req_id: 1,
                responder: resp_a_tx,
                span: tracing::Span::current(),
                ty: DmsgType::Req,
                target_peer_idx: None,
            })
            .await
            .unwrap();
        req_tx
            .send(OutboundRequest {
                bytes: b"*1\r\n$4\r\nPING\r\n".to_vec(),
                req_id: 2,
                responder: resp_b_tx,
                span: tracing::Span::current(),
                ty: DmsgType::Req,
                target_peer_idx: None,
            })
            .await
            .unwrap();
        drop(req_tx);

        let server = DnodeServerConn::new(conn, req_rx);
        let driver = tokio::spawn(async move {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(3), server.run()).await;
        });

        let a = tokio::time::timeout(std::time::Duration::from_secs(2), resp_a_rx.recv())
            .await
            .expect("responder A timed out")
            .expect("responder A closed");
        let b = tokio::time::timeout(std::time::Duration::from_secs(2), resp_b_rx.recv())
            .await
            .expect("responder B timed out")
            .expect("responder B closed");
        // Responder A must receive req_id 1's reply, B req_id 2's.
        assert_eq!(a.req_id, 1, "responder A got the wrong request's reply");
        assert_eq!(b.req_id, 2, "responder B got the wrong request's reply");
        peer.abort();
        driver.abort();
    }
}
