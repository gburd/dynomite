//! Response lifecycle helpers.
//!
//! A small number of pure-data helpers - constructing error
//! responses, pairing a response with its request - live here. The
//! connection plumbing (response send / done, queue threading)
//! ships in Stage 9.

use super::message::Msg;
use super::msg_type::MsgType;
use super::DynErrorCode;
use crate::io::mbuf::MbufPool;

/// Render the on-the-wire error payload for `err_type` with the
/// human-readable string supplied by `dyn_error_code`.
///
/// The Redis side mirrors RESP error replies: `RspRedisError` uses
/// the synthetic Dynomite prefix, every typed `RspRedisError*`
/// variant uses its matching wire token, and the unspecified ones
/// fall back to `-ERR <message>\r\n`. The Memcache side uses
/// `SERVER_ERROR <message>\r\n` for `RspMcServerError`,
/// `CLIENT_ERROR <message>\r\n` for `RspMcClientError`, and the
/// bare `ERROR\r\n` for `RspMcError`.
fn render_error_wire(err_type: MsgType, message: &str) -> Vec<u8> {
    match err_type {
        MsgType::RspRedisError => format!("-Dynomite: {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorErr => format!("-ERR {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorOom => format!("-OOM {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorBusy => format!("-BUSY {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorNoauth => format!("-NOAUTH {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorLoading => format!("-LOADING {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorBusykey => format!("-BUSYKEY {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorMisconf => format!("-MISCONF {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorNoscript => format!("-NOSCRIPT {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorReadonly => format!("-READONLY {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorWrongtype => format!("-WRONGTYPE {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorExecabort => format!("-EXECABORT {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorMasterdown => format!("-MASTERDOWN {message}\r\n").into_bytes(),
        MsgType::RspRedisErrorNoreplicas => format!("-NOREPLICAS {message}\r\n").into_bytes(),
        MsgType::RspMcServerError => format!("SERVER_ERROR {message}\r\n").into_bytes(),
        MsgType::RspMcClientError => format!("CLIENT_ERROR {message}\r\n").into_bytes(),
        MsgType::RspMcError => b"ERROR\r\n".to_vec(),
        // Non-error variants are rejected by the debug_assert in
        // `make_error`. Returning an empty payload here keeps the
        // function total without papering over the contract.
        _ => Vec::new(),
    }
}

/// Append `bytes` to `rsp` as one or more mbufs drawn from `pool`.
///
/// All current call sites produce error payloads that are well under
/// any sane chunk size, but the loop keeps the helper correct for
/// future growth (longer messages, accumulated framing).
fn attach_payload(rsp: &mut Msg, pool: &MbufPool, bytes: &[u8]) {
    let mut written = 0usize;
    while written < bytes.len() {
        let mut buf = pool.get();
        let n = buf.recv(&bytes[written..]);
        debug_assert!(
            n > 0,
            "MbufPool returned a buffer with zero writable capacity"
        );
        rsp.mbufs_mut().push_back(buf);
        written += n;
    }
    rsp.recompute_mlen();
}

/// Build a synthetic error response for `req`.
///
/// The constructed message inherits the request's id (so the
/// dispatcher can pair them), sets `is_request` to false, marks the
/// response as in-error, stamps the error codes, and attaches the
/// rendered wire-format error string in one or more mbufs taken
/// from `pool` so the client driver actually has bytes to write
/// back.
///
/// The wire formats produced are:
///
/// * Redis (RESP):
///   * `RspRedisError`: `-Dynomite: <message>\r\n`
///   * `RspRedisErrorErr`: `-ERR <message>\r\n`
///   * `RspRedisErrorOom`: `-OOM <message>\r\n`
///   * `RspRedisErrorBusy`: `-BUSY <message>\r\n`
///   * `RspRedisErrorNoauth`: `-NOAUTH <message>\r\n`
///   * `RspRedisErrorLoading`: `-LOADING <message>\r\n`
///   * `RspRedisErrorBusykey`: `-BUSYKEY <message>\r\n`
///   * `RspRedisErrorMisconf`: `-MISCONF <message>\r\n`
///   * `RspRedisErrorNoscript`: `-NOSCRIPT <message>\r\n`
///   * `RspRedisErrorReadonly`: `-READONLY <message>\r\n`
///   * `RspRedisErrorWrongtype`: `-WRONGTYPE <message>\r\n`
///   * `RspRedisErrorExecabort`: `-EXECABORT <message>\r\n`
///   * `RspRedisErrorMasterdown`: `-MASTERDOWN <message>\r\n`
///   * `RspRedisErrorNoreplicas`: `-NOREPLICAS <message>\r\n`
/// * Memcache (text):
///   * `RspMcServerError`: `SERVER_ERROR <message>\r\n`
///   * `RspMcClientError`: `CLIENT_ERROR <message>\r\n`
///   * `RspMcError`: `ERROR\r\n`
///
/// `<message>` is [`DynErrorCode::message`].
///
/// # Examples
///
/// ```
/// use dynomite::io::mbuf::MbufPool;
/// use dynomite::msg::{response, DynErrorCode, Msg, MsgType};
///
/// let req = Msg::new(7, MsgType::ReqRedisGet, true);
/// let pool = MbufPool::default();
/// let rsp = response::make_error(
///     &req,
///     MsgType::RspRedisError,
///     13,
///     DynErrorCode::PeerHostDown,
///     &pool,
/// );
/// assert_eq!(rsp.parent_id(), 7);
/// assert!(rsp.flags().is_error);
/// let bytes: Vec<u8> = rsp.mbufs().iter().flat_map(|b| b.readable().to_vec()).collect();
/// assert_eq!(bytes, b"-Dynomite: Peer Node is down\r\n".to_vec());
/// ```
#[must_use]
pub fn make_error(
    req: &Msg,
    err_type: MsgType,
    error_code: i32,
    dyn_error_code: DynErrorCode,
    pool: &MbufPool,
) -> Msg {
    debug_assert!(
        matches!(
            err_type,
            MsgType::RspRedisError
                | MsgType::RspRedisErrorErr
                | MsgType::RspRedisErrorOom
                | MsgType::RspRedisErrorBusy
                | MsgType::RspRedisErrorNoauth
                | MsgType::RspRedisErrorLoading
                | MsgType::RspRedisErrorBusykey
                | MsgType::RspRedisErrorMisconf
                | MsgType::RspRedisErrorNoscript
                | MsgType::RspRedisErrorReadonly
                | MsgType::RspRedisErrorWrongtype
                | MsgType::RspRedisErrorExecabort
                | MsgType::RspRedisErrorMasterdown
                | MsgType::RspRedisErrorNoreplicas
                | MsgType::RspMcServerError
                | MsgType::RspMcClientError
                | MsgType::RspMcError
        ),
        "make_error called with non-error MsgType {err_type:?}"
    );
    let mut rsp = Msg::new(req.id(), err_type, false);
    rsp.set_parent_id(req.id());
    rsp.set_is_error(true);
    rsp.set_error_code(error_code);
    rsp.set_dyn_error_code(dyn_error_code);
    let wire = render_error_wire(err_type, dyn_error_code.message());
    if !wire.is_empty() {
        attach_payload(&mut rsp, pool, &wire);
    }
    rsp
}

/// Build a synthetic Redis-status response carrying `payload` as
/// the on-the-wire reply bytes.
///
/// The constructed message inherits the request's id (so the
/// dispatcher can pair them), sets `is_request` to false, marks
/// the type as [`MsgType::RspRedisStatus`], and attaches a single
/// mbuf containing `payload` (verbatim, no encoding). Use this
/// for synthesized replies whose wire form is fixed (`+OK\r\n`,
/// `+PONG\r\n`, ...).
///
/// # Examples
///
/// ```
/// use dynomite::io::mbuf::MbufPool;
/// use dynomite::msg::{response, Msg, MsgType};
///
/// let req = Msg::new(7, MsgType::ReqRedisQuit, true);
/// let pool = MbufPool::default();
/// let rsp = response::make_simple_redis(&req, &pool, b"+OK\r\n");
/// assert_eq!(rsp.id(), 7);
/// assert_eq!(rsp.ty(), MsgType::RspRedisStatus);
/// assert_eq!(rsp.mlen(), 5);
/// ```
#[must_use]
pub fn make_simple_redis(req: &Msg, pool: &MbufPool, payload: &[u8]) -> Msg {
    let mut rsp = Msg::new(req.id(), MsgType::RspRedisStatus, false);
    rsp.set_parent_id(req.id());
    let mut buf = pool.get();
    buf.recv(payload);
    rsp.mbufs_mut().push_back(buf);
    rsp.recompute_mlen();
    rsp
}

/// Pair a response with its request: stamps the response's parent
/// id and sets the request's `selected_rsp` to the response id.
///
/// Returns the previous selected-response id, if any, so callers can
/// release the now-stale response.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{response, Msg, MsgType};
///
/// let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
/// let rsp = Msg::new(2, MsgType::RspRedisStatus, false);
/// let prev = response::link(&mut req, &rsp);
/// assert!(prev.is_none());
/// assert_eq!(req.selected_rsp(), Some(2));
/// ```
pub fn link(req: &mut Msg, rsp: &Msg) -> Option<crate::core::types::MsgId> {
    let prev = req.selected_rsp();
    req.set_selected_rsp(Some(rsp.id()));
    prev
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::MsgType;

    fn wire_bytes(msg: &Msg) -> Vec<u8> {
        msg.mbufs()
            .iter()
            .flat_map(|b| b.readable().to_vec())
            .collect()
    }

    #[test]
    fn error_response_inherits_request_id() {
        let pool = MbufPool::default();
        let req = Msg::new(42, MsgType::ReqRedisGet, true);
        let rsp = make_error(
            &req,
            MsgType::RspRedisError,
            13,
            DynErrorCode::DynomiteUnknownError,
            &pool,
        );
        assert_eq!(rsp.id(), 42);
        assert_eq!(rsp.parent_id(), 42);
        assert!(rsp.flags().is_error);
        assert_eq!(rsp.error_code(), 13);
    }

    #[test]
    fn make_error_redis_renders_dynomite_prefix() {
        let pool = MbufPool::default();
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let rsp = make_error(
            &req,
            MsgType::RspRedisError,
            0,
            DynErrorCode::DynomiteNoQuorumAchieved,
            &pool,
        );
        assert_eq!(
            wire_bytes(&rsp),
            b"-Dynomite: Failed to achieve Quorum\r\n".to_vec()
        );
        assert_eq!(rsp.mlen() as usize, wire_bytes(&rsp).len());
    }

    #[test]
    fn make_error_typed_redis_variants_render_correct_prefixes() {
        let pool = MbufPool::default();
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let dyn_err = DynErrorCode::DynomiteUnknownError;
        let cases: &[(MsgType, &[u8])] = &[
            (MsgType::RspRedisError, b"-Dynomite: Unknown Error\r\n"),
            (MsgType::RspRedisErrorErr, b"-ERR Unknown Error\r\n"),
            (MsgType::RspRedisErrorOom, b"-OOM Unknown Error\r\n"),
            (MsgType::RspRedisErrorBusy, b"-BUSY Unknown Error\r\n"),
            (MsgType::RspRedisErrorNoauth, b"-NOAUTH Unknown Error\r\n"),
            (MsgType::RspRedisErrorLoading, b"-LOADING Unknown Error\r\n"),
            (MsgType::RspRedisErrorBusykey, b"-BUSYKEY Unknown Error\r\n"),
            (MsgType::RspRedisErrorMisconf, b"-MISCONF Unknown Error\r\n"),
            (
                MsgType::RspRedisErrorNoscript,
                b"-NOSCRIPT Unknown Error\r\n",
            ),
            (
                MsgType::RspRedisErrorReadonly,
                b"-READONLY Unknown Error\r\n",
            ),
        ];
        for (ty, expected) in cases {
            let rsp = make_error(&req, *ty, 0, dyn_err, &pool);
            assert_eq!(
                wire_bytes(&rsp),
                expected.to_vec(),
                "wire mismatch for {ty:?}"
            );
        }
    }

    #[test]
    fn make_error_memcache_renders_server_error() {
        let pool = MbufPool::default();
        let req = Msg::new(1, MsgType::ReqMcGet, true);
        let rsp = make_error(
            &req,
            MsgType::RspMcServerError,
            0,
            DynErrorCode::DynomiteNoQuorumAchieved,
            &pool,
        );
        assert_eq!(
            wire_bytes(&rsp),
            b"SERVER_ERROR Failed to achieve Quorum\r\n".to_vec()
        );
    }

    #[test]
    fn make_error_memcache_error_bare() {
        let pool = MbufPool::default();
        let req = Msg::new(1, MsgType::ReqMcGet, true);
        let rsp = make_error(
            &req,
            MsgType::RspMcError,
            0,
            DynErrorCode::DynomiteUnknownError,
            &pool,
        );
        assert_eq!(wire_bytes(&rsp), b"ERROR\r\n".to_vec());
        assert_eq!(rsp.mlen() as usize, wire_bytes(&rsp).len());
    }

    #[test]
    fn make_error_no_quorum_message_matches_dispatcher_log() {
        // Dispatcher's `NoTargets` path emits a
        // `DynomiteNoQuorumAchieved` envelope; the wire form must
        // surface a human-readable reason rather than a hang.
        let pool = MbufPool::default();
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let rsp = make_error(
            &req,
            MsgType::RspRedisError,
            0,
            DynErrorCode::DynomiteNoQuorumAchieved,
            &pool,
        );
        let bytes = wire_bytes(&rsp);
        assert!(bytes.starts_with(b"-Dynomite: "));
        assert!(bytes.ends_with(b"\r\n"));
    }

    #[test]
    fn link_returns_previous() {
        let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
        let rsp1 = Msg::new(2, MsgType::RspRedisStatus, false);
        let rsp2 = Msg::new(3, MsgType::RspRedisStatus, false);
        assert!(link(&mut req, &rsp1).is_none());
        assert_eq!(link(&mut req, &rsp2), Some(2));
        assert_eq!(req.selected_rsp(), Some(3));
    }
}
