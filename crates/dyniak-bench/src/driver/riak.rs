//! Riak Protocol Buffer Client (PBC) driver.
//!
//! Hand-rolled protobuf encoder for the small subset of Riak
//! messages used in this benchmark: `RpbPingReq`, `RpbGetReq`,
//! `RpbPutReq`, `RpbDelReq`, plus a minimal `RpbDtUpdateReq`
//! variant for counter increments.

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::Mutex;
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::Rng;

use crate::config::DriverConfig;
use crate::driver::{Driver, DriverOutcome};
use crate::error::BenchError;
use crate::keygen::KeyGen;
use crate::valgen::ValGen;

const RIAK_CODE_ERROR_RESP: u8 = 0;
const RIAK_CODE_PING_REQ: u8 = 1;
const RIAK_CODE_PING_RESP: u8 = 2;
const RIAK_CODE_GET_REQ: u8 = 9;
const RIAK_CODE_GET_RESP: u8 = 10;
const RIAK_CODE_PUT_REQ: u8 = 11;
const RIAK_CODE_PUT_RESP: u8 = 12;
const RIAK_CODE_DEL_REQ: u8 = 13;
const RIAK_CODE_DEL_RESP: u8 = 14;
const RIAK_CODE_DT_UPDATE_REQ: u8 = 82;
const RIAK_CODE_DT_UPDATE_RESP: u8 = 83;

const PB_WIRE_VARINT: u32 = 0;
const PB_WIRE_LEN_DELIM: u32 = 2;

const SUPPORTED: &[&str] = &[
    "ping",
    "get",
    "put",
    "del",
    "counter_inc",
    "map_update",
    "set_add",
];

/// Bounded recent-key memory shared by all driver instances so the
/// `get` workload has a positive hit rate without cross-task
/// coordination. Trimmed every time it grows past the cap.
static RECENT_KEYS: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());
const RECENT_CAP: usize = 1024;

fn remember(k: &[u8]) {
    let mut g = RECENT_KEYS.lock().expect("recent keys mutex poisoned");
    if g.len() >= RECENT_CAP {
        g.drain(..(RECENT_CAP / 4));
    }
    g.push(k.to_vec());
}

fn recent_or(rng: &mut SmallRng, fallback: &[u8]) -> Vec<u8> {
    let g = RECENT_KEYS.lock().expect("recent keys mutex poisoned");
    if !g.is_empty() && rng.random_bool(0.5) {
        let i = rng.random_range(0..g.len());
        g[i].clone()
    } else {
        fallback.to_vec()
    }
}

fn pb_encode_varint(mut n: u64, out: &mut Vec<u8>) {
    while n > 0x7F {
        out.push(((n & 0x7F) as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn pb_encode_tag(field: u32, wire: u32, out: &mut Vec<u8>) {
    pb_encode_varint(u64::from((field << 3) | wire), out);
}

fn pb_encode_bytes_field(field: u32, value: &[u8], out: &mut Vec<u8>) {
    pb_encode_tag(field, PB_WIRE_LEN_DELIM, out);
    pb_encode_varint(value.len() as u64, out);
    out.extend_from_slice(value);
}

fn pb_encode_varint_field(field: u32, value: u64, out: &mut Vec<u8>) {
    pb_encode_tag(field, PB_WIRE_VARINT, out);
    pb_encode_varint(value, out);
}

/// `RpbGetReq{ bucket, key }`.
fn encode_get(bucket: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bucket.len() + key.len() + 8);
    pb_encode_bytes_field(1, bucket, &mut out);
    pb_encode_bytes_field(2, key, &mut out);
    out
}

/// `RpbPutReq{ bucket, key, content{value} }`. Field 4 is the
/// `RpbContent value` shortcut Riak accepts when a single content
/// blob is being written.
fn encode_put(bucket: &[u8], key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut content = Vec::with_capacity(value.len() + 4);
    pb_encode_bytes_field(1, value, &mut content);

    let mut out = Vec::with_capacity(bucket.len() + key.len() + value.len() + 16);
    pb_encode_bytes_field(1, bucket, &mut out);
    pb_encode_bytes_field(2, key, &mut out);
    pb_encode_bytes_field(4, &content, &mut out);
    out
}

/// `RpbDelReq{ bucket, key }`.
fn encode_del(bucket: &[u8], key: &[u8]) -> Vec<u8> {
    encode_get(bucket, key)
}

/// Build a counter-increment `RpbDtUpdateReq` body. Wire layout:
/// `bucket=1, key=2, type=3 (string "counters"), op=4 -> CounterOp { increment=1 }`.
fn encode_counter_inc(bucket: &[u8], bucket_type: &[u8], key: &[u8], delta: i64) -> Vec<u8> {
    // CounterOp { increment = sint64 } at field 1 of CounterOp.
    let mut counter_op = Vec::new();
    pb_encode_varint_field(1, zigzag_encode(delta), &mut counter_op);
    let counter_op_packed = {
        let mut buf = Vec::with_capacity(counter_op.len() + 4);
        // CounterOp wraps inside DtOp at field 1.
        pb_encode_bytes_field(1, &counter_op, &mut buf);
        buf
    };

    let mut out = Vec::new();
    pb_encode_bytes_field(1, bucket, &mut out);
    pb_encode_bytes_field(2, key, &mut out);
    pb_encode_bytes_field(3, bucket_type, &mut out);
    pb_encode_bytes_field(4, &counter_op_packed, &mut out);
    out
}

/// Build a `set_add` DtUpdate body: `SetOp{ adds = bytes }`.
fn encode_set_add(bucket: &[u8], bucket_type: &[u8], key: &[u8], member: &[u8]) -> Vec<u8> {
    let mut set_op = Vec::new();
    // SetOp { adds = repeated bytes } -> tag 1 (length-delimited).
    pb_encode_bytes_field(1, member, &mut set_op);
    let set_op_packed = {
        // DtOp wraps SetOp at field 2.
        let mut buf = Vec::with_capacity(set_op.len() + 4);
        pb_encode_bytes_field(2, &set_op, &mut buf);
        buf
    };

    let mut out = Vec::new();
    pb_encode_bytes_field(1, bucket, &mut out);
    pb_encode_bytes_field(2, key, &mut out);
    pb_encode_bytes_field(3, bucket_type, &mut out);
    pb_encode_bytes_field(4, &set_op_packed, &mut out);
    out
}

/// Build a `map_update` DtUpdate body. Keeps the schema deliberately
/// minimal (one register update) so the workload stays within the
/// surface our reference servers actually accept.
fn encode_map_update(
    bucket: &[u8],
    bucket_type: &[u8],
    key: &[u8],
    field_name: &[u8],
    val: &[u8],
) -> Vec<u8> {
    // RegisterOp = bytes (the new value). Wrapped at field 4 of MapUpdate.
    // MapField{ name=1 bytes, type=2 enum REGISTER=3 }.
    let mut map_field = Vec::new();
    pb_encode_bytes_field(1, field_name, &mut map_field);
    pb_encode_varint_field(2, 3, &mut map_field); // REGISTER

    let mut map_update = Vec::new();
    pb_encode_bytes_field(1, &map_field, &mut map_update);
    pb_encode_bytes_field(4, val, &mut map_update); // RegisterOp

    let mut map_op = Vec::new();
    // MapOp { updates = repeated MapUpdate } at tag 3.
    pb_encode_bytes_field(3, &map_update, &mut map_op);

    let mut dt_op = Vec::new();
    // DtOp wraps MapOp at field 3.
    pb_encode_bytes_field(3, &map_op, &mut dt_op);

    let mut out = Vec::new();
    pb_encode_bytes_field(1, bucket, &mut out);
    pb_encode_bytes_field(2, key, &mut out);
    pb_encode_bytes_field(3, bucket_type, &mut out);
    pb_encode_bytes_field(4, &dt_op, &mut out);
    out
}

fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

fn pb_decode_varint(buf: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut n = 0u64;
    let mut shift = 0u32;
    let start = *pos;
    while *pos < buf.len() {
        let b = buf[*pos];
        *pos += 1;
        n |= u64::from(b & 0x7F) << shift;
        if b < 0x80 {
            return Ok(n);
        }
        shift += 7;
        if shift > 63 {
            return Err(format!("varint too long at {start}"));
        }
    }
    Err(format!("truncated varint at {start}"))
}

fn decode_error_resp(body: &[u8]) -> String {
    let mut errmsg = String::new();
    let mut errcode = 0u64;
    let mut pos = 0usize;
    while pos < body.len() {
        let Ok(tag) = pb_decode_varint(body, &mut pos) else {
            break;
        };
        let field = tag >> 3;
        let wire = tag & 0x07;
        match wire {
            v if v == u64::from(PB_WIRE_LEN_DELIM) => {
                let Ok(len) = pb_decode_varint(body, &mut pos) else {
                    break;
                };
                let len = len as usize;
                if pos + len > body.len() {
                    break;
                }
                let chunk = &body[pos..pos + len];
                pos += len;
                if field == 1 {
                    errmsg = String::from_utf8_lossy(chunk).into_owned();
                }
            }
            v if v == u64::from(PB_WIRE_VARINT) => {
                let Ok(val) = pb_decode_varint(body, &mut pos) else {
                    break;
                };
                if field == 2 {
                    errcode = val;
                }
            }
            _ => break,
        }
    }
    format!("RpbErrorResp(code={errcode}): {errmsg}")
}

/// The Riak PBC driver. Owns one TCP socket per worker.
pub struct RiakPbcDriver {
    host: String,
    port: u16,
    bucket: Vec<u8>,
    timeout: Duration,
    sock: Option<TcpStream>,
}

impl RiakPbcDriver {
    /// Construct from configuration. The TCP socket is opened
    /// lazily on the first op.
    pub fn new(cfg: &DriverConfig) -> Result<Self, BenchError> {
        Ok(Self {
            host: cfg.host.clone(),
            port: cfg.port,
            bucket: cfg.bucket.as_bytes().to_vec(),
            timeout: Duration::from_millis(cfg.timeout_ms),
            sock: None,
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
        Ok(())
    }

    fn drop_socket(&mut self) {
        if let Some(s) = self.sock.take() {
            let _ = s.shutdown(Shutdown::Both);
        }
    }

    fn read_n(&mut self, n: usize) -> io::Result<Vec<u8>> {
        let s = self
            .sock
            .as_mut()
            .ok_or_else(|| io::Error::other("not connected"))?;
        let mut buf = vec![0u8; n];
        s.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Send one `(code, body)` frame and read back one frame.
    pub fn call(&mut self, code: u8, body: &[u8]) -> io::Result<(u8, Vec<u8>)> {
        self.ensure_connected()?;
        let len = u32::try_from(1 + body.len()).map_err(|_| io::Error::other("frame too large"))?;
        let mut frame = Vec::with_capacity(5 + body.len());
        frame.extend_from_slice(&len.to_be_bytes());
        frame.push(code);
        frame.extend_from_slice(body);
        match self.sock.as_mut() {
            Some(s) => s.write_all(&frame)?,
            None => return Err(io::Error::other("socket missing")),
        }

        let head = self.read_n(4)?;
        let mut len_buf = [0u8; 4];
        len_buf.copy_from_slice(&head);
        let total = u32::from_be_bytes(len_buf) as usize;
        if total < 1 {
            return Err(io::Error::other("zero-length frame"));
        }
        let code_byte = self.read_n(1)?[0];
        let body = if total > 1 {
            self.read_n(total - 1)?
        } else {
            Vec::new()
        };
        Ok((code_byte, body))
    }

    fn call_check(&mut self, code: u8, body: &[u8], expected: u8) -> Result<Vec<u8>, String> {
        match self.call(code, body) {
            Ok((c, b)) if c == expected => Ok(b),
            Ok((c, b)) if c == RIAK_CODE_ERROR_RESP => {
                Err(format!("riak error: {}", decode_error_resp(&b)))
            }
            Ok((c, _)) => {
                self.drop_socket();
                Err(format!("unexpected reply code {c}"))
            }
            Err(e) => {
                self.drop_socket();
                Err(format!("io error: {e}"))
            }
        }
    }
}

impl Driver for RiakPbcDriver {
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
        match op {
            "ping" => match self.call_check(RIAK_CODE_PING_REQ, &[], RIAK_CODE_PING_RESP) {
                Ok(_) => DriverOutcome::Ok,
                Err(e) => DriverOutcome::Err(e),
            },
            "put" => {
                let key = keygen.next(rng);
                let val = valgen.next(rng);
                let body = encode_put(&self.bucket, key.as_bytes(), &val);
                let bucket_owned = self.bucket.clone();
                drop(bucket_owned);
                match self.call_check(RIAK_CODE_PUT_REQ, &body, RIAK_CODE_PUT_RESP) {
                    Ok(_) => {
                        remember(key.as_bytes());
                        DriverOutcome::Ok
                    }
                    Err(e) => DriverOutcome::Err(e),
                }
            }
            "get" => {
                let fresh = keygen.next(rng);
                let key = recent_or(rng, fresh.as_bytes());
                let body = encode_get(&self.bucket, &key);
                match self.call_check(RIAK_CODE_GET_REQ, &body, RIAK_CODE_GET_RESP) {
                    Ok(_) => DriverOutcome::Ok,
                    Err(e) => DriverOutcome::Err(e),
                }
            }
            "del" => {
                let fresh = keygen.next(rng);
                let key = recent_or(rng, fresh.as_bytes());
                let body = encode_del(&self.bucket, &key);
                match self.call_check(RIAK_CODE_DEL_REQ, &body, RIAK_CODE_DEL_RESP) {
                    Ok(_) => DriverOutcome::Ok,
                    Err(e) => DriverOutcome::Err(e),
                }
            }
            "counter_inc" => {
                let key = keygen.next(rng);
                let body = encode_counter_inc(&self.bucket, b"counters", key.as_bytes(), 1);
                match self.call_check(RIAK_CODE_DT_UPDATE_REQ, &body, RIAK_CODE_DT_UPDATE_RESP) {
                    Ok(_) => DriverOutcome::Ok,
                    Err(e) => DriverOutcome::Err(e),
                }
            }
            "set_add" => {
                let key = keygen.next(rng);
                let val = valgen.next(rng);
                let body = encode_set_add(&self.bucket, b"sets", key.as_bytes(), &val);
                match self.call_check(RIAK_CODE_DT_UPDATE_REQ, &body, RIAK_CODE_DT_UPDATE_RESP) {
                    Ok(_) => DriverOutcome::Ok,
                    Err(e) => DriverOutcome::Err(e),
                }
            }
            "map_update" => {
                let key = keygen.next(rng);
                let val = valgen.next(rng);
                let body = encode_map_update(&self.bucket, b"maps", key.as_bytes(), b"f", &val);
                match self.call_check(RIAK_CODE_DT_UPDATE_REQ, &body, RIAK_CODE_DT_UPDATE_RESP) {
                    Ok(_) => DriverOutcome::Ok,
                    Err(e) => DriverOutcome::Err(e),
                }
            }
            other => DriverOutcome::Err(format!("unsupported op `{other}`")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trip() {
        let cases: &[u64] = &[0, 1, 127, 128, 16_383, 16_384, 1_000_000, u64::MAX / 2];
        for &v in cases {
            let mut buf = Vec::new();
            pb_encode_varint(v, &mut buf);
            let mut pos = 0usize;
            let decoded = pb_decode_varint(&buf, &mut pos).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn zigzag_round_trip() {
        let cases: &[i64] = &[0, 1, -1, 2, -2, 100, -100, i64::MAX / 4, i64::MIN / 4];
        for &v in cases {
            let z = zigzag_encode(v);
            let back = ((z >> 1) as i64) ^ (-((z & 1) as i64));
            assert_eq!(back, v);
        }
    }

    #[test]
    fn encode_get_layout() {
        let body = encode_get(b"chaos", b"k1");
        // Tag for field 1 (bucket), len-delim wire = (1<<3)|2 = 0x0a.
        assert_eq!(body[0], 0x0a);
        assert_eq!(body[1], 5);
        assert_eq!(&body[2..7], b"chaos");
        // Tag for field 2 (key), len-delim wire = (2<<3)|2 = 0x12.
        assert_eq!(body[7], 0x12);
        assert_eq!(body[8], 2);
        assert_eq!(&body[9..11], b"k1");
    }

    #[test]
    fn encode_put_includes_value() {
        let body = encode_put(b"b", b"k", b"v");
        // Last byte must be 'v' since that's the deepest payload
        // and there is exactly one byte of value.
        assert_eq!(*body.last().unwrap(), b'v');
        // Body includes the bucket, key, and content wrapper.
        assert!(body.len() > b"bkv".len());
    }

    #[test]
    fn decode_error_text() {
        // Build a small RpbErrorResp body manually:
        //   field 1 (errmsg) = "boom", field 2 (errcode) = 7.
        let mut buf = Vec::new();
        pb_encode_bytes_field(1, b"boom", &mut buf);
        pb_encode_varint_field(2, 7, &mut buf);
        let s = decode_error_resp(&buf);
        assert!(s.contains("boom"));
        assert!(s.contains('7'));
    }
}
