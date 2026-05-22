//! Inbound peer-connection driver for the dnode peer plane.
//!
//! The local node is the receiver. The driver:
//!
//! 1. Reads bytes off the transport into a contiguous buffer.
//! 2. Drives the dnode header parser ([`crate::proto::dnode::DnodeParser`])
//!    over the buffer until a full `Dmsg` header has been observed.
//! 3. If the header marks the payload as encrypted, decrypts it
//!    using the per-connection AES key bound during the handshake
//!    via [`crate::crypto::Crypto`]. When the header indicates a
//!    plaintext payload (the peer-plane was negotiated unsecured),
//!    the bytes pass through unchanged.
//! 4. Drives the datastore parser over the (decrypted) payload to
//!    reconstruct a [`Msg`].
//! 5. Hands the parsed [`Msg`] to the supplied
//!    [`ClientHandler`]'s dispatcher and routes the dispatcher's
//!    response back through the per-connection responder channel.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::msg::Msg;
use crate::msg::MsgParseResult;
use crate::msg::MsgType;
use crate::net::client::ClientHandler;
use crate::net::conn::Conn;
use crate::net::dispatcher::OutboundEnvelope;
use crate::net::NetError;
use crate::proto::dnode::{DnodeParser, ParseStep};

/// Type alias for the dnode client handler bundle.
pub type DnodeClientHandler = ClientHandler;

/// Drive a DNODE_PEER_CLIENT FSM until the peer closes.
///
/// `rx` receives responses produced by the cluster dispatcher; the
/// driver writes the response bytes back through the same
/// transport.
///
/// # Errors
/// Surfaces transport- and DNODE-level errors.
pub async fn dnode_client_loop(
    mut conn: Conn,
    handler: ClientHandler,
    mut rx: mpsc::Receiver<OutboundEnvelope>,
) -> Result<(), NetError> {
    let mut read_buf = vec![0u8; 4096];
    let mut accumulated = Vec::<u8>::new();
    let mut parser = DnodeParser::new();

    loop {
        if conn.is_eof() && conn.imsg_q().is_empty() && conn.omsg_q().is_empty() {
            conn.set_done();
            return Ok(());
        }

        tokio::select! {
            res = async {
                if let Some(t) = conn.transport_mut() {
                    t.read(&mut read_buf).await
                } else {
                    Ok(0)
                }
            } => {
                let n = res?;
                if n == 0 {
                    conn.set_eof();
                    continue;
                }
                conn.record_recv(n);
                accumulated.extend_from_slice(&read_buf[..n]);
                drive_dnode_parser(&mut conn, &handler, &mut accumulated, &mut parser).await?;
            }
            Some(env) = rx.recv() => {
                // Forward dispatcher-produced responses back to
                // the peer over this same transport. The wire
                // framing already lives in the response's mbuf
                // chain; the peer-server side prepends its own
                // dnode header for outbound writes.
                let bytes: Vec<u8> = env
                    .rsp
                    .mbufs()
                    .iter()
                    .flat_map(|b| b.readable().to_vec())
                    .collect();
                if !bytes.is_empty() {
                    if let Some(t) = conn.transport_mut() {
                        t.write_all(&bytes).await?;
                        conn.record_send(bytes.len());
                    }
                }
                conn.outstanding_mut().remove(&env.req_id);
                if let Some(front) = conn.omsg_q_mut().front() {
                    if front.id() == env.req_id {
                        let _ = conn.omsg_q_mut().pop_front();
                    }
                }
            }
        }
    }
}

async fn drive_dnode_parser(
    conn: &mut Conn,
    handler: &ClientHandler,
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
                    "dnode header parse error after {consumed} bytes"
                )));
            }
            ParseStep::HeaderDone { consumed } => {
                let header_end = consumed;
                let dmsg = parser.take_dmsg();
                let plen = dmsg.plen as usize;
                let total = header_end + plen;
                if accumulated.len() < total {
                    // Wait for more bytes for the payload; rewind
                    // by stashing what we have. The parser was
                    // moved to PostDone but we need it to retry
                    // header parsing on the next chunk.
                    parser.reset();
                    return Ok(());
                }
                let payload = accumulated[header_end..total].to_vec();
                accumulated.drain(0..total);
                parser.reset();

                // Decrypt if the dnode header indicates the payload
                // is encrypted and we have an AES key.
                let decoded = if dmsg.is_encrypted() {
                    let Some(key) = conn.aes_key() else {
                        // No key has been negotiated yet; the
                        // peer-plane handshake should have run
                        // first. Surface a single opaque parse
                        // error and let the driver close the
                        // connection.
                        return Err(NetError::Dnode(
                            "dnode payload marked encrypted but no aes key bound".into(),
                        ));
                    };
                    decrypt_dnode_payload(key, &payload)?
                } else {
                    payload
                };

                // Feed the decoded payload through the datastore
                // parser to reconstruct a Msg.
                let mut msg = Msg::new(dmsg.id, MsgType::Unknown, true);
                msg.set_dmsg(dmsg);
                let parse_result = match handler.data_store() {
                    crate::conf::DataStore::Redis => {
                        crate::proto::redis::redis_parse_req(&mut msg, &decoded)
                    }
                    crate::conf::DataStore::Memcache => {
                        crate::proto::memcache::memcache_parse_req(&mut msg, &decoded)
                    }
                };
                match parse_result {
                    MsgParseResult::Ok | MsgParseResult::Noop => {
                        let pool = conn.mbuf_pool().clone();
                        let mut buf = pool.get();
                        buf.recv(&decoded);
                        msg.mbufs_mut().push_back(buf);
                        msg.recompute_mlen();
                        conn.outstanding_mut().insert(msg.id(), msg.id());
                        conn.enqueue_out(Msg::new(msg.id(), msg.ty(), true))?;
                        // Hand the parsed peer request to the
                        // configured dispatcher. The dispatcher
                        // either takes ownership and replies
                        // asynchronously through `responder`, or it
                        // returns an inline / error response that
                        // we forward immediately, or it asks the
                        // FSM to drop the request.
                        let outcome = handler
                            .dispatcher()
                            .dispatch(msg, handler.response_tx().clone());
                        match outcome {
                            crate::net::dispatcher::DispatchOutcome::Pending
                            | crate::net::dispatcher::DispatchOutcome::Drop => {}
                            crate::net::dispatcher::DispatchOutcome::Inline(rsp)
                            | crate::net::dispatcher::DispatchOutcome::Error(rsp) => {
                                let env = OutboundEnvelope {
                                    req_id: rsp.id(),
                                    rsp,
                                    span: tracing::Span::current(),
                                };
                                let _ = handler.response_tx().send(env).await;
                            }
                        }
                    }
                    MsgParseResult::Again => return Ok(()),
                    other => {
                        return Err(NetError::Parse(format!("dnode payload parse: {other:?}")));
                    }
                }
            }
        }
    }
}

/// Decrypt a dnode peer-plane payload using the per-connection AES
/// key.
///
/// AES-128-CBC with PKCS#7 padding, IV from the trailing 16 bytes
/// of the 32-byte key buffer. Returns a single opaque
/// [`NetError::Dnode`] on failure regardless of whether the
/// underlying error was bad padding, a length mismatch, or a
/// key/iv mismatch, so peers cannot distinguish the cases (the
/// padding-oracle surface flagged in the Stage 6 review).
fn decrypt_dnode_payload(
    key: &[u8; crate::crypto::AES_KEYLEN],
    payload: &[u8],
) -> Result<Vec<u8>, NetError> {
    crate::crypto::Crypto::aes_decrypt(payload, key)
        .map_err(|_| NetError::Dnode("dnode payload decrypt failed".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::reactor::{ConnRole, TcpTransport};
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
        let _conn = Conn::new(
            Box::new(TcpTransport::new(s, ConnRole::DnodePeerClient)),
            ConnRole::DnodePeerClient,
        );
    }
}
