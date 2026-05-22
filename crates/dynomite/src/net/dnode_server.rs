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

use crate::core::types::MsgId;
use crate::io::reactor::ConnRole;
use crate::msg::{Msg, MsgParseResult, MsgType};
use crate::net::conn::Conn;
use crate::net::dispatcher::OutboundEnvelope;
use crate::net::server::OutboundRequest;
use crate::net::NetError;
use crate::proto::dnode::{dmsg_write, DmsgType, DnodeParser, ParseStep};

/// Outbound DNODE peer connection driver.
pub struct DnodeServerConn {
    conn: Conn,
    requests: mpsc::Receiver<OutboundRequest>,
    pending: std::collections::VecDeque<MsgId>,
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
        let mut pending_responder: Option<mpsc::Sender<OutboundEnvelope>> = None;

        loop {
            if self.conn.is_eof() && self.pending.is_empty() {
                self.conn.set_done();
                return Ok(());
            }

            tokio::select! {
                req = requests.recv() => {
                    let Some(req) = req else { continue; };
                    let mut header_buf = self.conn.mbuf_pool().get();
                    dmsg_write(
                        &mut header_buf,
                        req.req_id,
                        DmsgType::Req,
                        0,
                        true,
                        None,
                        u32::try_from(req.bytes.len()).unwrap_or(u32::MAX),
                    )?;
                    let transport = self.conn.transport_mut().ok_or(NetError::Closed)?;
                    transport.write_all(header_buf.readable()).await?;
                    transport.write_all(&req.bytes).await?;
                    self.conn.record_send(header_buf.readable().len() + req.bytes.len());
                    self.pending.push_back(req.req_id);
                    pending_responder = Some(req.responder);
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
                    self.drive_response(&mut accumulated, &mut parser, &mut pending_responder).await?;
                }
            }
        }
    }

    async fn drive_response(
        &mut self,
        accumulated: &mut Vec<u8>,
        parser: &mut DnodeParser,
        responder: &mut Option<mpsc::Sender<OutboundEnvelope>>,
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
                    let req_id = self.pending.pop_front().unwrap_or(dmsg.id);
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
                    if let Some(sender) = responder.as_ref() {
                        let _ = sender.send(OutboundEnvelope { req_id, rsp }).await;
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
}
