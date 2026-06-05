//! RESP-2 driver for Redis-compatible servers (Redis, dynomited,
//! KeyDB). Hand-rolled blocking client; the bytes that go on the
//! wire come straight from this module so the driver is honest
//! about what is being measured.

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::Rng;

use crate::config::DriverConfig;
use crate::driver::{Driver, DriverOutcome};
use crate::error::BenchError;
use crate::keygen::KeyGen;
use crate::valgen::ValGen;

const SUPPORTED: &[&str] = &[
    "get",
    "set",
    "incr",
    "decr",
    "del",
    "hset",
    "hget",
    "sadd",
    "sismember",
    "zadd",
    "zrangebyscore",
    "ft_create",
    "ft_search",
    "ft_sugadd",
    "ft_sugget",
];

/// RESP-2 reply variants that we recognise. Used internally to
/// decide between success and `-ERR ...` paths and exposed under
/// `cfg(test)` for the unit tests that assert on parsed payloads.
#[derive(Debug)]
enum RespReply {
    Status(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<RespReply>>),
}

impl std::fmt::Display for RespReply {
    /// Pretty representation. Used by `Driver::run` callers when
    /// logging an unexpected reply, and by tests when asserting on
    /// the parser output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Status(s) => write!(f, "+{s}"),
            Self::Error(e) => write!(f, "-{e}"),
            Self::Integer(v) => write!(f, ":{v}"),
            Self::Bulk(None) => f.write_str("$-1"),
            Self::Bulk(Some(bytes)) => write!(f, "${}", bytes.len()),
            Self::Array(None) => f.write_str("*-1"),
            Self::Array(Some(items)) => write!(f, "*{}", items.len()),
        }
    }
}

impl RespReply {
    /// Return `Some(error_text)` when the reply is an `-ERR`
    /// surface, else `None`. Used by the production `run` path.
    fn err_text(&self) -> Option<&str> {
        match self {
            Self::Error(e) => Some(e.as_str()),
            _ => None,
        }
    }

    /// Return `Some(status_text)` for `+OK` and friends. The
    /// production path does not need this; tests do.
    #[cfg(test)]
    fn status(&self) -> Option<&str> {
        if let Self::Status(s) = self {
            Some(s.as_str())
        } else {
            None
        }
    }

    /// Return `Some(integer)` for `:N` replies; the production
    /// path ignores the value, but tests assert on it.
    #[cfg(test)]
    fn integer(&self) -> Option<i64> {
        if let Self::Integer(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// Return the bulk reply payload as `Some(bytes)` (or
    /// `Some(b"")` for the null bulk surface), or `None` if the
    /// reply is not a bulk frame. The double-`Option` shape makes
    /// the `null bulk` versus `empty bulk` distinction explicit
    /// for tests that need to assert on it; the production path
    /// does not call this.
    #[cfg(test)]
    #[allow(clippy::option_option)]
    fn bulk(&self) -> Option<Option<&[u8]>> {
        if let Self::Bulk(b) = self {
            Some(b.as_deref())
        } else {
            None
        }
    }
}

/// The driver itself. Owns the TCP socket and a small read buffer.
pub struct RedisDriver {
    host: String,
    port: u16,
    timeout: Duration,
    sock: Option<TcpStream>,
    rbuf: Vec<u8>,
    rstart: usize,
}

impl RedisDriver {
    /// Construct a driver from configuration. Connects lazily on
    /// the first op.
    pub fn new(cfg: &DriverConfig) -> Result<Self, BenchError> {
        Ok(Self {
            host: cfg.host.clone(),
            port: cfg.port,
            timeout: Duration::from_millis(cfg.timeout_ms),
            sock: None,
            rbuf: Vec::with_capacity(4096),
            rstart: 0,
        })
    }

    fn ensure_connected(&mut self) -> io::Result<()> {
        if self.sock.is_some() {
            return Ok(());
        }
        let addr = (self.host.as_str(), self.port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no addrs"))?;
        let s = TcpStream::connect_timeout(&addr, self.timeout)?;
        s.set_read_timeout(Some(self.timeout))?;
        s.set_write_timeout(Some(self.timeout))?;
        s.set_nodelay(true)?;
        self.sock = Some(s);
        self.rbuf.clear();
        self.rstart = 0;
        Ok(())
    }

    fn drop_socket(&mut self) {
        if let Some(s) = self.sock.take() {
            let _ = s.shutdown(Shutdown::Both);
        }
        self.rbuf.clear();
        self.rstart = 0;
    }

    fn send(&mut self, parts: &[&[u8]]) -> io::Result<()> {
        self.ensure_connected()?;
        let mut buf = Vec::with_capacity(64 + parts.iter().map(|p| p.len() + 16).sum::<usize>());
        buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            buf.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            buf.extend_from_slice(p);
            buf.extend_from_slice(b"\r\n");
        }
        match self.sock.as_mut() {
            Some(s) => s.write_all(&buf),
            None => Err(io::Error::other("socket missing")),
        }
    }

    fn fill_buffer(&mut self) -> io::Result<()> {
        // Compact occasionally so the read buffer does not grow
        // without bound on long-running drivers.
        if self.rstart > 0 && self.rstart >= self.rbuf.len() / 2 {
            self.rbuf.drain(..self.rstart);
            self.rstart = 0;
        }
        let mut chunk = [0u8; 4096];
        let s = self
            .sock
            .as_mut()
            .ok_or_else(|| io::Error::other("not connected"))?;
        let n = s.read(&mut chunk)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "peer closed"));
        }
        self.rbuf.extend_from_slice(&chunk[..n]);
        Ok(())
    }

    fn read_line(&mut self) -> io::Result<Vec<u8>> {
        loop {
            if let Some(pos) = find_crlf(&self.rbuf[self.rstart..]) {
                let abs_end = self.rstart + pos;
                let line = self.rbuf[self.rstart..abs_end].to_vec();
                self.rstart = abs_end + 2;
                return Ok(line);
            }
            self.fill_buffer()?;
        }
    }

    fn read_n(&mut self, n: usize) -> io::Result<Vec<u8>> {
        while self.rbuf.len() - self.rstart < n {
            self.fill_buffer()?;
        }
        let out = self.rbuf[self.rstart..self.rstart + n].to_vec();
        self.rstart += n;
        Ok(out)
    }

    fn read_reply(&mut self) -> io::Result<RespReply> {
        let line = self.read_line()?;
        if line.is_empty() {
            return Err(io::Error::other("empty reply"));
        }
        let prefix = line[0];
        let rest = &line[1..];
        match prefix {
            b'+' => Ok(RespReply::Status(
                String::from_utf8_lossy(rest).into_owned(),
            )),
            b'-' => Ok(RespReply::Error(String::from_utf8_lossy(rest).into_owned())),
            b':' => {
                let s = std::str::from_utf8(rest)
                    .map_err(|e| io::Error::other(format!("non-utf8 integer: {e}")))?;
                let v: i64 = s
                    .parse()
                    .map_err(|e| io::Error::other(format!("bad integer: {e}")))?;
                Ok(RespReply::Integer(v))
            }
            b'$' => {
                let s = std::str::from_utf8(rest)
                    .map_err(|e| io::Error::other(format!("non-utf8 length: {e}")))?;
                let n: i64 = s
                    .parse()
                    .map_err(|e| io::Error::other(format!("bad length: {e}")))?;
                if n < 0 {
                    Ok(RespReply::Bulk(None))
                } else {
                    let n = n as usize;
                    let data = self.read_n(n)?;
                    let _ = self.read_n(2)?; // trailing CRLF
                    Ok(RespReply::Bulk(Some(data)))
                }
            }
            b'*' => {
                let s = std::str::from_utf8(rest)
                    .map_err(|e| io::Error::other(format!("non-utf8 array length: {e}")))?;
                let n: i64 = s
                    .parse()
                    .map_err(|e| io::Error::other(format!("bad array length: {e}")))?;
                if n < 0 {
                    Ok(RespReply::Array(None))
                } else {
                    let mut items = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        items.push(self.read_reply()?);
                    }
                    Ok(RespReply::Array(Some(items)))
                }
            }
            other => Err(io::Error::other(format!("unknown RESP prefix `{other}`"))),
        }
    }

    /// Send one command and return the parsed reply (or an error
    /// with the raw RESP -ERR text). Used by the unit tests; the
    /// production path goes through `call_check`.
    fn call(&mut self, parts: &[&[u8]]) -> io::Result<RespReply> {
        self.send(parts)?;
        self.read_reply()
    }

    fn call_check(&mut self, parts: &[&[u8]]) -> Result<RespReply, String> {
        match self.call(parts) {
            Ok(r) => {
                if let Some(e) = r.err_text() {
                    return Err(format!("RESP error: {e}"));
                }
                Ok(r)
            }
            Err(e) => {
                self.drop_socket();
                Err(format!("io error: {e}"))
            }
        }
    }

    fn op_get(&mut self, key: &[u8]) -> Result<DriverOutcome, String> {
        self.call_check(&[b"GET", key])?;
        Ok(DriverOutcome::Ok)
    }

    fn op_set(&mut self, key: &[u8], val: &[u8]) -> Result<DriverOutcome, String> {
        self.call_check(&[b"SET", key, val])?;
        Ok(DriverOutcome::Ok)
    }

    fn op_incr(&mut self, key: &[u8]) -> Result<DriverOutcome, String> {
        self.call_check(&[b"INCR", key])?;
        Ok(DriverOutcome::Ok)
    }

    fn op_decr(&mut self, key: &[u8]) -> Result<DriverOutcome, String> {
        self.call_check(&[b"DECR", key])?;
        Ok(DriverOutcome::Ok)
    }

    fn op_del(&mut self, key: &[u8]) -> Result<DriverOutcome, String> {
        self.call_check(&[b"DEL", key])?;
        Ok(DriverOutcome::Ok)
    }

    fn op_hset(&mut self, key: &[u8], val: &[u8]) -> Result<DriverOutcome, String> {
        self.call_check(&[b"HSET", key, b"f", val])?;
        Ok(DriverOutcome::Ok)
    }

    fn op_hget(&mut self, key: &[u8]) -> Result<DriverOutcome, String> {
        self.call_check(&[b"HGET", key, b"f"])?;
        Ok(DriverOutcome::Ok)
    }

    fn op_sadd(&mut self, key: &[u8], val: &[u8]) -> Result<DriverOutcome, String> {
        self.call_check(&[b"SADD", key, val])?;
        Ok(DriverOutcome::Ok)
    }

    fn op_sismember(&mut self, key: &[u8], val: &[u8]) -> Result<DriverOutcome, String> {
        self.call_check(&[b"SISMEMBER", key, val])?;
        Ok(DriverOutcome::Ok)
    }

    fn op_zadd(
        &mut self,
        key: &[u8],
        val: &[u8],
        rng: &mut SmallRng,
    ) -> Result<DriverOutcome, String> {
        let score: u32 = rng.random_range(0..100_000);
        let score_s = score.to_string();
        self.call_check(&[b"ZADD", key, score_s.as_bytes(), val])?;
        Ok(DriverOutcome::Ok)
    }

    fn op_zrangebyscore(&mut self, key: &[u8]) -> Result<DriverOutcome, String> {
        self.call_check(&[b"ZRANGEBYSCORE", key, b"0", b"100000"])?;
        Ok(DriverOutcome::Ok)
    }

    fn op_ft_create(&mut self, key: &[u8]) -> Result<DriverOutcome, String> {
        // best-effort idempotent index creation; we ignore "Index
        // already exists" errors so the workload can mix create
        // and search with no orchestration. Schema includes a
        // single VECTOR field to satisfy dynomite-search's
        // current requirement that every index declare at least
        // one VECTOR.
        match self.call(&[
            b"FT.CREATE",
            key,
            b"ON",
            b"HASH",
            b"PREFIX",
            b"1",
            b"doc:",
            b"SCHEMA",
            b"f",
            b"TEXT",
            b"v",
            b"VECTOR",
            b"HNSW",
            b"6",
            b"TYPE",
            b"FLOAT32",
            b"DIM",
            b"4",
            b"DISTANCE_METRIC",
            b"COSINE",
        ]) {
            Ok(reply) => {
                if let Some(e) = reply.err_text() {
                    if e.to_ascii_lowercase().contains("already exists") {
                        return Ok(DriverOutcome::Ok);
                    }
                    return Err(format!("RESP error: {e}"));
                }
                Ok(DriverOutcome::Ok)
            }
            Err(e) => {
                self.drop_socket();
                Err(format!("io error: {e}"))
            }
        }
    }

    fn op_ft_search(&mut self, key: &[u8]) -> Result<DriverOutcome, String> {
        // Tolerate "index not found" so a workload mixing
        // create + search does not race-fail when the create
        // hasn't landed on this connection yet (the search and
        // create can target different shards under a routed
        // dynomited proxy).
        match self.call(&[b"FT.SEARCH", key, b"*", b"LIMIT", b"0", b"10"]) {
            Ok(reply) => {
                if let Some(e) = reply.err_text() {
                    let low = e.to_ascii_lowercase();
                    if low.contains("index not found") || low.contains("unknown index") {
                        return Ok(DriverOutcome::Ok);
                    }
                    return Err(format!("RESP error: {e}"));
                }
                Ok(DriverOutcome::Ok)
            }
            Err(e) => {
                self.drop_socket();
                Err(format!("io error: {e}"))
            }
        }
    }

    fn op_ft_sugadd(&mut self, key: &[u8], val: &[u8]) -> Result<DriverOutcome, String> {
        self.call_check(&[b"FT.SUGADD", key, val, b"1"])?;
        Ok(DriverOutcome::Ok)
    }

    fn op_ft_sugget(&mut self, key: &[u8], val: &[u8]) -> Result<DriverOutcome, String> {
        // Use the first byte of the value as the prefix probe so
        // the operation does not need to allocate. Falls back to
        // "a" when the value is empty (cannot happen in practice
        // because ValGen guarantees n >= 1).
        let probe: &[u8] = if val.is_empty() { b"a" } else { &val[..1] };
        self.call_check(&[b"FT.SUGGET", key, probe])?;
        Ok(DriverOutcome::Ok)
    }
}

impl Driver for RedisDriver {
    fn supported_ops(&self) -> &'static [&'static str] {
        SUPPORTED
    }

    fn run(
        &mut self,
        op: &str,
        keygen: &mut KeyGen,
        valgen: &ValGen,
        rng: &mut SmallRng,
    ) -> DriverOutcome {
        let key = keygen.next(rng);
        let key_b = key.as_bytes();
        let result = match op {
            "get" => self.op_get(key_b),
            "set" => {
                let v = valgen.next(rng);
                self.op_set(key_b, &v)
            }
            "incr" => self.op_incr(key_b),
            "decr" => self.op_decr(key_b),
            "del" => self.op_del(key_b),
            "hset" => {
                let v = valgen.next(rng);
                self.op_hset(key_b, &v)
            }
            "hget" => self.op_hget(key_b),
            "sadd" => {
                let v = valgen.next(rng);
                self.op_sadd(key_b, &v)
            }
            "sismember" => {
                let v = valgen.next(rng);
                self.op_sismember(key_b, &v)
            }
            "zadd" => {
                let v = valgen.next(rng);
                self.op_zadd(key_b, &v, rng)
            }
            "zrangebyscore" => self.op_zrangebyscore(key_b),
            "ft_create" => self.op_ft_create(key_b),
            "ft_search" => self.op_ft_search(key_b),
            "ft_sugadd" => {
                let v = valgen.next(rng);
                self.op_ft_sugadd(key_b, &v)
            }
            "ft_sugget" => {
                let v = valgen.next(rng);
                self.op_ft_sugget(key_b, &v)
            }
            other => return DriverOutcome::Err(format!("unsupported op `{other}`")),
        };
        match result {
            Ok(o) => o,
            Err(e) => DriverOutcome::Err(e),
        }
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    use super::*;

    fn open_loopback() -> (TcpListener, u16) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        (l, port)
    }

    #[test]
    fn parses_status_integer_bulk() {
        let (listener, port) = open_loopback();
        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            // Three commands, three replies.
            let mut buf = [0u8; 4096];
            // First: SET key val -> +OK
            let _ = s.read(&mut buf).unwrap();
            s.write_all(b"+OK\r\n").unwrap();
            // Second: INCR key -> :1
            let _ = s.read(&mut buf).unwrap();
            s.write_all(b":1\r\n").unwrap();
            // Third: GET key -> $3\r\nval\r\n
            let _ = s.read(&mut buf).unwrap();
            s.write_all(b"$3\r\nval\r\n").unwrap();
            // Hold the connection open briefly so the client can
            // observe the writes.
            thread::sleep(Duration::from_millis(50));
        });

        let mut d = RedisDriver {
            host: "127.0.0.1".into(),
            port,
            timeout: Duration::from_millis(500),
            sock: None,
            rbuf: Vec::new(),
            rstart: 0,
        };

        let r = d.call(&[b"SET", b"k", b"v"]).unwrap();
        assert_eq!(r.status(), Some("OK"));

        let r = d.call(&[b"INCR", b"k"]).unwrap();
        assert_eq!(r.integer(), Some(1));

        let r = d.call(&[b"GET", b"k"]).unwrap();
        assert_eq!(r.bulk(), Some(Some(b"val".as_slice())));

        server.join().unwrap();
    }

    #[test]
    fn classifies_eof_as_io_error() {
        let (listener, port) = open_loopback();
        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf).unwrap();
            // Half a reply: prefix + length, no payload, then EOF.
            s.write_all(b"$5\r\n").unwrap();
            // Drop the stream so the client sees EOF mid-bulk.
            drop(s);
        });

        let mut d = RedisDriver {
            host: "127.0.0.1".into(),
            port,
            timeout: Duration::from_millis(500),
            sock: None,
            rbuf: Vec::new(),
            rstart: 0,
        };

        let r = d.call(&[b"GET", b"k"]);
        assert!(r.is_err(), "expected EOF error, got {r:?}");
        let e = r.err().unwrap();
        let msg = format!("{e}");
        assert!(
            crate::error::classify_driver_error(&msg) == crate::error::DriverErrorClass::Closed,
            "expected Closed, got msg `{msg}`"
        );

        server.join().unwrap();
    }

    #[test]
    fn returns_resp_error() {
        let (listener, port) = open_loopback();
        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf).unwrap();
            s.write_all(b"-ERR no such key\r\n").unwrap();
            thread::sleep(Duration::from_millis(50));
        });

        let mut d = RedisDriver {
            host: "127.0.0.1".into(),
            port,
            timeout: Duration::from_millis(500),
            sock: None,
            rbuf: Vec::new(),
            rstart: 0,
        };

        let r = d.call(&[b"GET", b"missing"]).unwrap();
        assert!(
            r.err_text().is_some_and(|e| e.contains("no such key")),
            "got {r:?}"
        );

        server.join().unwrap();
    }

    #[test]
    #[allow(dead_code)]
    fn driver_supports_canonical_ops() {
        // _stream is unused but kept alive so the listener doesn't
        // tear down while construction runs.
        let cfg = DriverConfig {
            kind: crate::config::DriverKind::Redis,
            host: "127.0.0.1".into(),
            port: 0,
            timeout_ms: 100,
            bucket: "b".into(),
            encoding: crate::config::HttpEncoding::Json,
        };
        let d = RedisDriver::new(&cfg).unwrap();
        let ops = d.supported_ops();
        assert!(ops.contains(&"get"));
        assert!(ops.contains(&"set"));
        assert!(ops.contains(&"ft_search"));
    }

    #[allow(dead_code)]
    fn _typecheck_dyn() {
        fn is_driver<T: Driver>() {}
        is_driver::<RedisDriver>();
    }

    // The unused TcpStream import lives here so the test module
    // compiles even if all the network tests were filtered out.
    #[allow(dead_code)]
    fn _connect_kept_alive() -> Option<TcpStream> {
        None
    }
}
