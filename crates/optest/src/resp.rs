//! Minimal blocking RESP client with per-call timeouts.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

/// A decoded RESP value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// `+...`
    Simple(String),
    /// `-...`
    Error(String),
    /// `:...`
    Int(i64),
    /// `$...`, `None` for the null bulk.
    Bulk(Option<Vec<u8>>),
    /// `*...`, `None` for the null array.
    Array(Option<Vec<Value>>),
}

impl Value {
    /// Bulk payload as UTF-8, if this is a non-null bulk string.
    ///
    /// # Examples
    ///
    /// ```
    /// use optest::resp::Value;
    /// assert_eq!(Value::Bulk(Some(b"v".to_vec())).text(), Some("v".into()));
    /// assert_eq!(Value::Bulk(None).text(), None);
    /// ```
    pub fn text(&self) -> Option<String> {
        match self {
            Value::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            Value::Simple(s) => Some(s.clone()),
            _ => None,
        }
    }

    /// True for `+OK`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert!(optest::resp::Value::Simple("OK".into()).is_ok());
    /// ```
    pub fn is_ok(&self) -> bool {
        matches!(self, Value::Simple(s) if s == "OK")
    }
}

/// Why a RESP call did not produce a value.
#[derive(Debug, thiserror::Error)]
pub enum RespError {
    /// Connect, read, or write failed; includes timeouts.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The peer sent bytes that are not RESP.
    #[error("malformed reply: {0}")]
    Malformed(String),
    /// The peer closed the connection mid-reply.
    #[error("connection closed")]
    Closed,
}

impl RespError {
    /// True when the failure is a timeout (the outcome is unknown: the
    /// request may or may not have been applied).
    pub fn is_timeout(&self) -> bool {
        matches!(self, RespError::Io(e) if matches!(e.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut))
    }
}

/// A blocking connection to one RESP endpoint.
#[derive(Debug)]
pub struct Client {
    s: TcpStream,
    rx: Vec<u8>,
}

impl Client {
    /// Connect with `timeout` applied to the connect and to every read
    /// and write.
    pub fn connect(addr: SocketAddr, timeout: Duration) -> Result<Self, RespError> {
        let s = TcpStream::connect_timeout(&addr, timeout)?;
        s.set_read_timeout(Some(timeout))?;
        s.set_write_timeout(Some(timeout))?;
        s.set_nodelay(true)?;
        Ok(Self { s, rx: Vec::new() })
    }

    /// Send one command and read one reply.
    pub fn cmd<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Result<Value, RespError> {
        self.s.write_all(&encode(args))?;
        self.read_reply()
    }

    /// Read the next reply.
    pub fn read_reply(&mut self) -> Result<Value, RespError> {
        loop {
            if let Some((v, n)) = decode(&self.rx, 0)? {
                self.rx.drain(..n);
                return Ok(v);
            }
            let mut buf = [0u8; 16 * 1024];
            let n = self.s.read(&mut buf)?;
            if n == 0 {
                return Err(RespError::Closed);
            }
            self.rx.extend_from_slice(&buf[..n]);
        }
    }
}

/// Encode a command as a RESP array of bulk strings.
///
/// # Examples
///
/// ```
/// assert_eq!(optest::resp::encode(&["GET", "k"]), b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
/// ```
pub fn encode<A: AsRef<[u8]>>(args: &[A]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        let a = a.as_ref();
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn line(buf: &[u8], at: usize) -> Option<(&[u8], usize)> {
    let rel = buf.get(at..)?.windows(2).position(|w| w == b"\r\n")?;
    Some((&buf[at..at + rel], at + rel + 2))
}

fn int(b: &[u8]) -> Result<i64, RespError> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RespError::Malformed(format!("bad integer {b:?}")))
}

/// Decode one value starting at `at`; `None` when more bytes are needed.
fn decode(buf: &[u8], at: usize) -> Result<Option<(Value, usize)>, RespError> {
    let Some(&tag) = buf.get(at) else {
        return Ok(None);
    };
    let Some((body, next)) = line(buf, at + 1) else {
        return Ok(None);
    };
    let text = || String::from_utf8_lossy(body).into_owned();
    match tag {
        b'+' => Ok(Some((Value::Simple(text()), next))),
        b'-' => Ok(Some((Value::Error(text()), next))),
        b':' => Ok(Some((Value::Int(int(body)?), next))),
        b'$' => {
            let n = int(body)?;
            if n < 0 {
                return Ok(Some((Value::Bulk(None), next)));
            }
            let n = usize::try_from(n).map_err(|_| RespError::Malformed("bulk len".into()))?;
            if buf.len() < next + n + 2 {
                return Ok(None);
            }
            Ok(Some((
                Value::Bulk(Some(buf[next..next + n].to_vec())),
                next + n + 2,
            )))
        }
        b'*' => {
            let n = int(body)?;
            if n < 0 {
                return Ok(Some((Value::Array(None), next)));
            }
            let mut items = Vec::new();
            let mut pos = next;
            for _ in 0..n {
                match decode(buf, pos)? {
                    Some((v, p)) => {
                        items.push(v);
                        pos = p;
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((Value::Array(Some(items)), pos)))
        }
        other => Err(RespError::Malformed(format!("tag 0x{other:02x}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_partial_and_nested() {
        let full = b"*2\r\n$1\r\na\r\n*1\r\n:5\r\n";
        for cut in 0..full.len() {
            assert!(decode(&full[..cut], 0).unwrap().is_none(), "cut {cut}");
        }
        let (v, n) = decode(full, 0).unwrap().unwrap();
        assert_eq!(n, full.len());
        assert_eq!(
            v,
            Value::Array(Some(vec![
                Value::Bulk(Some(b"a".to_vec())),
                Value::Array(Some(vec![Value::Int(5)]))
            ]))
        );
    }
}
