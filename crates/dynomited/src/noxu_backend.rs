//! In-process backend supervisor that delegates Redis requests
//! to a [`dyniak::datastore::NoxuDatastore`].
//!
//! When the operator selects `data_store: noxu` in the YAML
//! configuration, the [`crate::server::Server`] builder routes
//! the local-datastore mpsc channel through this module instead
//! of dialling a remote backend over TCP. The supervisor parses
//! each incoming request as Redis (RESP), executes it against
//! the in-process Noxu environment, encodes a RESP response,
//! and pushes the response back onto the request's responder
//! channel.
//!
//! The supervisor is entirely opt-in: the module is gated behind
//! the `riak` Cargo feature, and a binary built without that
//! feature has no path that selects this code.
//!
//! # Wire protocol
//!
//! The proxy parses inbound Redis requests and the cluster
//! dispatcher re-encodes them as RESP bytes; the supervisor
//! consumes the same byte stream. Today it understands the
//! single-key subset that round-trips through:
//!
//! * `GET k`  -> bulk string with the stored value or `$-1` for
//!   a missing key.
//! * `SET k v` -> `+OK`.
//! * `DEL k [...]` -> integer count of removed keys.
//! * `PING` -> `+PONG`.
//!
//! Unknown commands surface as a RESP simple-error so the
//! client receives a structured response rather than a hung
//! request.

use std::sync::Arc;

use dynomite::io::mbuf::MbufPool;
use dynomite::msg::{Msg, MsgParseResult, MsgType};
use dynomite::net::dispatcher::OutboundEnvelope;
use dynomite::net::server::OutboundRequest;
use dynomite::net::NetError;
use dynomite::proto::redis::redis_parse_req;
use tokio::sync::mpsc;
use tracing::Instrument as _;

use dyniak::datastore::NoxuDatastore;

/// Long-running supervisor that owns the request channel for a
/// Noxu-backed local datastore.
///
/// Exits when the receiver half is closed (the dispatcher is
/// dropped). Error responses produced for malformed / unknown
/// requests do not terminate the supervisor; only a permanent
/// failure of the response channel ends the loop.
///
/// # Errors
///
/// Returns `Ok(())` on a clean shutdown. The signature mirrors
/// the legacy `backend_supervisor` to keep `Server::build`'s
/// task-handle wiring uniform; today this body cannot fail.
#[tracing::instrument(name = "noxu_backend_supervisor", skip_all)]
pub async fn noxu_backend_supervisor(
    datastore: Arc<NoxuDatastore>,
    mut rx: mpsc::Receiver<OutboundRequest>,
) -> Result<(), NetError> {
    let mbuf_pool = MbufPool::default();
    while let Some(req) = rx.recv().await {
        let resp_bytes = execute_request(datastore.as_ref(), &req.bytes);
        let parse_span = tracing::info_span!(
            parent: &req.span,
            "noxu_backend.parse",
            req_id = req.req_id,
            bytes = resp_bytes.len(),
        );
        let envelope = parse_span.in_scope(|| {
            let mut rsp = Msg::new(req.req_id, MsgType::Unknown, false);
            // Parse the synthesised response so the cluster
            // dispatcher receives a fully-typed Msg with the
            // correct response type tag, exactly as it would
            // from the TCP backend driver.
            let _ = dynomite::proto::redis::redis_parse_rsp(&mut rsp, &resp_bytes);
            let mut buf = mbuf_pool.get();
            buf.recv(&resp_bytes);
            rsp.mbufs_mut().push_back(buf);
            rsp.recompute_mlen();
            OutboundEnvelope {
                req_id: req.req_id,
                rsp,
                span: req.span.clone(),
                source_peer_idx: None,
            }
        });
        let send = async { req.responder.send(envelope).await };
        let _ = send.instrument(parse_span).await;
    }
    Ok(())
}

/// Parse a single RESP request and execute it against the Noxu
/// datastore, returning the RESP response bytes.
///
/// On parse failure, on unknown commands, or on backend errors
/// the function returns a RESP simple-error (`-ERR ...`) rather
/// than panicking. The caller then ships the bytes back through
/// the responder channel like any other response.
fn execute_request(datastore: &NoxuDatastore, bytes: &[u8]) -> Vec<u8> {
    let mut msg = Msg::new(0, MsgType::Unknown, true);
    let result = redis_parse_req(&mut msg, bytes);
    if !matches!(result, MsgParseResult::Ok) {
        return error_resp("parse failed");
    }
    let ty = msg.ty();
    match ty {
        MsgType::ReqRedisGet => execute_get(datastore, &msg),
        MsgType::ReqRedisSet => execute_set(datastore, &msg, bytes),
        MsgType::ReqRedisDel => execute_del(datastore, &msg),
        MsgType::ReqRedisPing => simple_string("PONG"),
        other => error_resp(&format!("command unsupported by noxu backend: {other:?}")),
    }
}

fn execute_get(datastore: &NoxuDatastore, msg: &Msg) -> Vec<u8> {
    let Some(key) = first_key(msg) else {
        return error_resp("GET: missing key");
    };
    match datastore.get(key) {
        Ok(Some(v)) => bulk_string(&v),
        Ok(None) => null_bulk(),
        Err(e) => error_resp(&format!("noxu: {e}")),
    }
}

fn execute_set(datastore: &NoxuDatastore, msg: &Msg, bytes: &[u8]) -> Vec<u8> {
    let Some(key) = first_key(msg) else {
        return error_resp("SET: missing key");
    };
    // For SET the value is the second non-command argument; the
    // RESP request is `*3, $3 SET, $klen <k>, $vlen <v>`.
    // Re-extract from the wire bytes so we do not depend on
    // whether the parser populated `args` for the SET class.
    let Some(value) = resp_extract_arg(bytes, 2) else {
        return error_resp("SET: missing value");
    };
    match datastore.put(key, &value) {
        Ok(()) => simple_string("OK"),
        Err(e) => error_resp(&format!("noxu: {e}")),
    }
}

fn execute_del(datastore: &NoxuDatastore, msg: &Msg) -> Vec<u8> {
    if msg.keys().is_empty() {
        return error_resp("DEL: missing key");
    }
    let mut count: i64 = 0;
    for kp in msg.keys() {
        match datastore.delete(kp.key()) {
            Ok(true) => count += 1,
            Ok(false) => {}
            Err(e) => return error_resp(&format!("noxu: {e}")),
        }
    }
    integer_resp(count)
}

fn first_key(msg: &Msg) -> Option<&[u8]> {
    msg.keys().first().map(dynomite::msg::KeyPos::key)
}

/// Extract the bulk-string at `index` (0-indexed) from a RESP
/// multi-bulk request. Index 0 is the command verb, index 1 is
/// the first key, index 2 is the value for `SET k v`, etc.
/// Returns `None` on malformed input.
fn resp_extract_arg(bytes: &[u8], index: usize) -> Option<Vec<u8>> {
    let mut p = 0usize;
    if bytes.first() != Some(&b'*') {
        return None;
    }
    p += 1;
    let crlf = find_crlf(bytes, p)?;
    p = crlf + 2;
    let mut idx = 0usize;
    loop {
        if p >= bytes.len() || bytes[p] != b'$' {
            return None;
        }
        p += 1;
        let crlf = find_crlf(bytes, p)?;
        let len_str = std::str::from_utf8(&bytes[p..crlf]).ok()?;
        let len: usize = len_str.parse().ok()?;
        p = crlf + 2;
        if p + len > bytes.len() {
            return None;
        }
        let arg = &bytes[p..p + len];
        if idx == index {
            return Some(arg.to_vec());
        }
        p += len + 2;
        idx += 1;
    }
}

fn find_crlf(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i + 1 < bytes.len() {
        if bytes[i] == b'\r' && bytes[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn simple_string(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(3 + s.len());
    out.push(b'+');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

fn integer_resp(n: i64) -> Vec<u8> {
    format!(":{n}\r\n").into_bytes()
}

fn error_resp(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + s.len());
    out.push(b'-');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

fn bulk_string(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + value.len());
    out.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
    out
}

fn null_bulk() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn build_datastore() -> (TempDir, Arc<NoxuDatastore>) {
        let dir = TempDir::new().expect("tempdir");
        let ds = Arc::new(NoxuDatastore::open_in(dir.path()).expect("open"));
        (dir, ds)
    }

    #[test]
    fn get_set_del_round_trip() {
        let (_dir, ds) = build_datastore();
        let set_req = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        assert_eq!(execute_request(&ds, set_req), b"+OK\r\n");
        let get_req = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        assert_eq!(execute_request(&ds, get_req), b"$3\r\nbar\r\n");
        let del_req = b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n";
        assert_eq!(execute_request(&ds, del_req), b":1\r\n");
        assert_eq!(execute_request(&ds, get_req), b"$-1\r\n");
    }

    #[test]
    fn ping_returns_pong() {
        let (_dir, ds) = build_datastore();
        let req = b"*1\r\n$4\r\nPING\r\n";
        assert_eq!(execute_request(&ds, req), b"+PONG\r\n");
    }

    #[test]
    fn unsupported_command_returns_error() {
        let (_dir, ds) = build_datastore();
        let req = b"*3\r\n$5\r\nLPUSH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let resp = execute_request(&ds, req);
        assert!(
            resp.starts_with(b"-"),
            "unexpected response: {:?}",
            std::str::from_utf8(&resp).unwrap_or("<non-utf8>")
        );
    }

    #[tokio::test]
    async fn supervisor_round_trips_set_get_del() {
        let (_dir, ds) = build_datastore();
        let (req_tx, req_rx) = mpsc::channel::<OutboundRequest>(16);
        let h = tokio::spawn(noxu_backend_supervisor(ds.clone(), req_rx));

        let resp = round_trip(&req_tx, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", 7).await;
        assert!(resp.starts_with(b"+OK"), "got {resp:?}");

        let resp = round_trip(&req_tx, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", 8).await;
        assert!(resp.starts_with(b"$3"), "got {resp:?}");
        assert!(resp.windows(3).any(|w| w == b"bar"));

        let resp = round_trip(&req_tx, b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n", 9).await;
        assert!(resp.starts_with(b":1"), "got {resp:?}");

        drop(req_tx);
        let _ = h.await;
    }

    async fn round_trip(tx: &mpsc::Sender<OutboundRequest>, bytes: &[u8], req_id: u64) -> Vec<u8> {
        use dynomite::proto::dnode::DmsgType;
        let (rsp_tx, mut rsp_rx) = mpsc::channel::<OutboundEnvelope>(1);
        let req = OutboundRequest {
            bytes: bytes.to_vec(),
            req_id,
            responder: rsp_tx,
            span: tracing::Span::none(),
            ty: DmsgType::Req,
            target_peer_idx: None,
        };
        tx.send(req).await.expect("send");
        let env = rsp_rx.recv().await.expect("recv");
        let mut out = Vec::new();
        for buf in env.rsp.mbufs() {
            out.extend_from_slice(buf.readable());
        }
        out
    }
}
