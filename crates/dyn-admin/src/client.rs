//! Wire-level helpers shared by the `dyn-admin` subcommands.
//!
//! Two surfaces:
//!
//! * [`PbcClient`] -- an async PBC client that wraps the framer
//!   shipped in [`dyniak::proto::pb::framer`]. It encodes a
//!   `prost::Message` request, frames it under the requested message
//!   code, reads the response frame, and either decodes the body as
//!   the expected response type or surfaces an
//!   [`AdminError::Server`] when the peer replied with `RpbErrorResp`.
//! * [`http_get`] -- a minimal HTTP/1.1 GET issued over plain TCP.
//!   speaks plain HTTP and closes the connection after a single
//!   response, so a 60-line client is sufficient. Avoiding `reqwest`
//!   keeps the dyn-admin dep tree small; the deviation is recorded
//!   in the v0 journal entry.

use std::time::Duration;

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use dyniak::proto::pb::{read_frame, write_frame, Frame, MessageCode, RpbErrorResp};

use crate::error::AdminError;

/// Default per-connection timeout applied to PBC requests when the
/// caller does not override it. Matches the upper bound a `riak-admin`
/// invocation typically tolerates before the operator hits Ctrl-C.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Async PBC client connected to a single Dynomite node.
///
/// One instance per connection; subcommands open a fresh client per
/// call. The struct owns the [`TcpStream`] so it cleanly closes the
/// socket on drop.
pub struct PbcClient {
    stream: TcpStream,
    timeout: Duration,
}

impl PbcClient {
    /// Connect to a node's PBC listener at `addr` and apply the
    /// default timeout.
    pub async fn connect(addr: &str) -> Result<Self, AdminError> {
        Self::connect_with_timeout(addr, DEFAULT_TIMEOUT).await
    }

    /// Connect to a node's PBC listener at `addr` with a caller-
    /// supplied timeout.
    pub async fn connect_with_timeout(addr: &str, timeout: Duration) -> Result<Self, AdminError> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| AdminError::Timeout {
                op: "tcp connect".into(),
            })?
            .map_err(|e| AdminError::Connect {
                addr: addr.to_string(),
                source: e,
            })?;
        Ok(Self { stream, timeout })
    }

    /// Encode `req`, frame it under `code`, send it, and read back
    /// one response frame. Returns the raw frame so the caller can
    /// branch on the message code (for example to handle both a
    /// happy-path response and an inbound `RpbErrorResp`).
    pub async fn round_trip<R>(&mut self, code: MessageCode, req: &R) -> Result<Frame, AdminError>
    where
        R: Message,
    {
        let body = req.encode_to_vec();
        let frame = Frame::new(code.as_u8(), body);
        let exchange = async {
            write_frame(&mut self.stream, &frame).await?;
            read_frame(&mut self.stream).await
        };
        let response = tokio::time::timeout(self.timeout, exchange)
            .await
            .map_err(|_| AdminError::Timeout {
                op: format!("pbc round-trip code={}", code.as_u8()),
            })?
            .map_err(AdminError::from)?;
        Ok(response)
    }

    /// Convenience helper: round-trip and decode the response body as
    /// `Resp`, surfacing an [`AdminError::Server`] when the peer
    /// replied with `RpbErrorResp` and an [`AdminError::Protocol`]
    /// when the message code does not match `expected`.
    pub async fn call<Req, Resp>(
        &mut self,
        code: MessageCode,
        expected: MessageCode,
        req: &Req,
    ) -> Result<Resp, AdminError>
    where
        Req: Message,
        Resp: Message + Default,
    {
        let frame = self.round_trip(code, req).await?;
        decode_or_error::<Resp>(&frame, expected)
    }
}

/// Decode `frame` as `Resp` when its code matches `expected`.
///
/// When the code is [`MessageCode::ErrorResp`] the body is decoded as
/// [`RpbErrorResp`] and surfaced as [`AdminError::Server`].
/// Any other code is reported as [`AdminError::Protocol`].
pub fn decode_or_error<Resp>(frame: &Frame, expected: MessageCode) -> Result<Resp, AdminError>
where
    Resp: Message + Default,
{
    if frame.code == MessageCode::ErrorResp.as_u8() {
        let err = RpbErrorResp::decode(frame.body.as_slice())?;
        return Err(AdminError::Server {
            errcode: err.errcode,
            errmsg: String::from_utf8_lossy(&err.errmsg).into_owned(),
        });
    }
    if frame.code != expected.as_u8() {
        return Err(AdminError::Protocol(format!(
            "unexpected response code: got {}, expected {}",
            frame.code,
            expected.as_u8()
        )));
    }
    let resp = Resp::decode(frame.body.as_slice())?;
    Ok(resp)
}

/// Issue an HTTP/1.1 `GET <path>` against `addr` and return the
/// response body as a UTF-8 string.
///
/// The implementation is deliberately minimal: open a TCP connection,
/// write the request line plus a `Host:` and a `Connection: close`
/// header, then read until EOF and split on the first blank line.
/// The dynomite stats endpoint always sets `Content-Length` and
/// closes the connection after one response so an `httparse`-style
/// streaming parse is not required.
///
/// # Errors
///
/// * [`AdminError::Connect`] if the TCP connect fails.
/// * [`AdminError::Timeout`] if the read exceeds [`DEFAULT_TIMEOUT`].
/// * [`AdminError::Http`] if the response status line is malformed
///   or the status code is outside `200..=299`.
pub async fn http_get(addr: &str, path: &str) -> Result<String, AdminError> {
    http_get_with_timeout(addr, path, DEFAULT_TIMEOUT).await
}

/// As [`http_get`], with a caller-supplied timeout.
pub async fn http_get_with_timeout(
    addr: &str,
    path: &str,
    timeout: Duration,
) -> Result<String, AdminError> {
    let exchange = async {
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| AdminError::Connect {
                addr: addr.to_string(),
                source: e,
            })?;
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUser-Agent: dyn-admin/{ver}\r\n\
             Accept: */*\r\nConnection: close\r\n\r\n",
            ver = env!("CARGO_PKG_VERSION"),
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(AdminError::Io)?;
        stream.flush().await.map_err(AdminError::Io)?;
        let mut buf = Vec::with_capacity(4096);
        stream.read_to_end(&mut buf).await.map_err(AdminError::Io)?;
        Ok::<Vec<u8>, AdminError>(buf)
    };
    let raw = tokio::time::timeout(timeout, exchange)
        .await
        .map_err(|_| AdminError::Timeout {
            op: format!("http get {path}"),
        })??;
    parse_http_response(&raw)
}

/// Split an HTTP/1.1 response into status + body.
///
/// Public for testing. Returns the body as a UTF-8 string.
pub fn parse_http_response(raw: &[u8]) -> Result<String, AdminError> {
    let split = find_blank_line(raw)
        .ok_or_else(|| AdminError::Http("missing CRLF CRLF separator in HTTP response".into()))?;
    let head = &raw[..split];
    let body = &raw[split + 4..];
    let head_str =
        std::str::from_utf8(head).map_err(|_| AdminError::Http("non-UTF-8 HTTP head".into()))?;
    let status_line = head_str
        .lines()
        .next()
        .ok_or_else(|| AdminError::Http("empty HTTP head".into()))?;
    let mut iter = status_line.split_whitespace();
    let _version = iter.next();
    let code = iter
        .next()
        .ok_or_else(|| AdminError::Http("missing status code".into()))?
        .parse::<u16>()
        .map_err(|_| AdminError::Http("non-numeric status code".into()))?;
    if !(200..=299).contains(&code) {
        return Err(AdminError::Http(format!("HTTP {code}")));
    }
    let body =
        String::from_utf8(body.to_vec()).map_err(|_| AdminError::Http("non-UTF-8 body".into()))?;
    Ok(body)
}

fn find_blank_line(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_http_response() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        let body = parse_http_response(raw).expect("parse");
        assert_eq!(body, "hello");
    }

    #[test]
    fn surfaces_non_2xx_status() {
        let raw = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        let err = parse_http_response(raw).expect_err("404");
        match err {
            AdminError::Http(s) => assert!(s.contains("404")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn rejects_response_without_separator() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n";
        assert!(matches!(parse_http_response(raw), Err(AdminError::Http(_))));
    }
}
