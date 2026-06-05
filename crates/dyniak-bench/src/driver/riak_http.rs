//! Riak HTTP driver (feature `http`).
//!
//! Uses the blocking `reqwest` client to drive ``GET / PUT /
//! DELETE`` against `/buckets/<bucket>/keys/<key>`. The driver
//! preserves any `X-Riak-Vclock` value the server returns so that
//! subsequent `PUT`s on the same key are sent with the right
//! causal context header. The vclock cache is bounded.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::Rng;

use crate::config::{DriverConfig, HttpEncoding};
use crate::driver::{Driver, DriverOutcome};
use crate::error::BenchError;
use crate::keygen::KeyGen;
use crate::txn_workload::{
    build_txn_body, to_hex, TxnPut, DEFAULT_TXN_ABORT_FRACTION, DEFAULT_TXN_BATCH_SIZE,
};
use crate::valgen::ValGen;

const SUPPORTED: &[&str] = &["get", "put", "del", "txn"];
const VCLOCK_CAP: usize = 1024;

static VCLOCKS: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);

fn vclock_get(key: &str) -> Option<String> {
    let g = VCLOCKS.lock().expect("vclocks mutex poisoned");
    g.as_ref().and_then(|m| m.get(key).cloned())
}

fn vclock_put(key: &str, value: &str) {
    let mut g = VCLOCKS.lock().expect("vclocks mutex poisoned");
    let m = g.get_or_insert_with(HashMap::new);
    if m.len() >= VCLOCK_CAP {
        // Evict an arbitrary subset; cheap and correct.
        let drop_n = VCLOCK_CAP / 4;
        let to_drop: Vec<String> = m.keys().take(drop_n).cloned().collect();
        for k in to_drop {
            m.remove(&k);
        }
    }
    m.insert(key.to_string(), value.to_string());
}

fn vclock_drop(key: &str) {
    let mut g = VCLOCKS.lock().expect("vclocks mutex poisoned");
    if let Some(m) = g.as_mut() {
        m.remove(key);
    }
}

/// HTTP driver state. Holds a `reqwest::blocking::Client` which is
/// itself thread-safe + pooled, but each driver instance owns its
/// own clone for clarity.
pub struct RiakHttpDriver {
    base: String,
    bucket: String,
    client: reqwest::blocking::Client,
    encoding: HttpEncoding,
}

impl RiakHttpDriver {
    /// Construct from configuration. Connection setup is deferred
    /// until the first request.
    pub fn new(cfg: &DriverConfig) -> Result<Self, BenchError> {
        let scheme = if cfg.port == 443 { "https" } else { "http" };
        let base = format!("{scheme}://{}:{}", cfg.host, cfg.port);
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_millis(cfg.timeout_ms))
            .connect_timeout(Duration::from_millis(cfg.timeout_ms))
            .build()
            .map_err(|e| BenchError::Driver(format!("reqwest builder: {e}")))?;
        Ok(Self {
            base,
            bucket: cfg.bucket.clone(),
            client,
            encoding: cfg.encoding,
        })
    }

    fn url_for(&self, key: &str) -> String {
        format!("{}/buckets/{}/keys/{}", self.base, self.bucket, key)
    }

    fn op_get(&self, key: &str) -> Result<DriverOutcome, String> {
        let url = self.url_for(key);
        let resp = self
            .client
            .get(&url)
            .header("accept", self.encoding.content_type())
            .send()
            .map_err(|e| classify(&e.to_string(), &e))?;
        if let Some(v) = resp.headers().get("x-riak-vclock") {
            if let Ok(v) = v.to_str() {
                vclock_put(key, v);
            }
        }
        let st = resp.status();
        if !st.is_success() && st.as_u16() != 404 {
            return Err(format!("http error: {st}"));
        }
        Ok(DriverOutcome::Ok)
    }

    fn op_put(&self, key: &str, value: &[u8]) -> Result<DriverOutcome, String> {
        let url = self.url_for(key);
        let body = encode_envelope(self.encoding, value);
        let mut req = self
            .client
            .put(&url)
            .header("content-type", self.encoding.content_type())
            .header("accept", self.encoding.content_type())
            .body(body);
        if let Some(vc) = vclock_get(key) {
            req = req.header("x-riak-vclock", vc);
        }
        let resp = req.send().map_err(|e| classify(&e.to_string(), &e))?;
        if let Some(v) = resp.headers().get("x-riak-vclock") {
            if let Ok(v) = v.to_str() {
                vclock_put(key, v);
            }
        }
        let st = resp.status();
        if !st.is_success() {
            return Err(format!("http error: {st}"));
        }
        Ok(DriverOutcome::Ok)
    }

    fn op_del(&self, key: &str) -> Result<DriverOutcome, String> {
        let url = self.url_for(key);
        let resp = self
            .client
            .delete(&url)
            .send()
            .map_err(|e| classify(&e.to_string(), &e))?;
        let st = resp.status();
        if !st.is_success() && st.as_u16() != 404 {
            return Err(format!("http error: {st}"));
        }
        vclock_drop(key);
        Ok(DriverOutcome::Ok)
    }

    /// Multi-key atomic transaction op. Builds a batch of
    /// [`DEFAULT_TXN_BATCH_SIZE`] puts and, a
    /// [`DEFAULT_TXN_ABORT_FRACTION`] fraction of the time, requests a
    /// deliberate abort so the workload exercises the server's
    /// rollback path as well as commit.
    ///
    /// The batch is POSTed to the bucket-scoped
    /// `/buckets/<bucket>/transactions` route. A committed batch
    /// replies `200`; an aborted batch replies `409` -- both are
    /// expected outcomes, so an abort that returns `409` counts as a
    /// successful op, and a commit that returns `200` likewise. Any
    /// other status is an error.
    fn op_txn(
        &self,
        keygen: &mut KeyGen,
        valgen: &ValGen,
        rng: &mut SmallRng,
    ) -> Result<DriverOutcome, String> {
        let abort = rng.random_bool(DEFAULT_TXN_ABORT_FRACTION);
        let puts: Vec<TxnPut> = (0..DEFAULT_TXN_BATCH_SIZE)
            .map(|_| {
                let key = keygen.next(rng);
                let value_hex = to_hex(&valgen.next(rng));
                TxnPut { key, value_hex }
            })
            .collect();
        let body = build_txn_body(&self.bucket, &puts, abort);
        let url = format!("{}/buckets/{}/transactions", self.base, self.bucket);
        let resp = self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .map_err(|e| classify(&e.to_string(), &e))?;
        let st = resp.status();
        if abort {
            if st.as_u16() == 409 {
                Ok(DriverOutcome::Ok)
            } else {
                Err(format!("expected 409 for aborted txn, got {st}"))
            }
        } else if st.is_success() {
            Ok(DriverOutcome::Ok)
        } else {
            Err(format!("http error: {st}"))
        }
    }
}

/// Encode `value` into an `HttpObject` envelope under `encoding`.
///
/// The envelope carries only the `value` field (tag 1); the
/// gateway fills the optional `content_type` and `indexes` with
/// their defaults on decode. The three encoders mirror exactly
/// what `dyniak`'s object codecs expect:
///
/// * `json` -- `{"value":[b0,b1,...]}` with decimal byte values.
/// * `cbor` -- a canonical CBOR map `{"value": [..]}`.
/// * `protobuf` -- a single length-delimited field 1.
fn encode_envelope(encoding: HttpEncoding, value: &[u8]) -> Vec<u8> {
    match encoding {
        HttpEncoding::Json => json_envelope(value),
        HttpEncoding::Cbor => cbor_envelope(value),
        HttpEncoding::Protobuf => protobuf_envelope(value),
    }
}

/// Build a JSON `HttpObject` envelope: `{"value":[b0,b1,...]}`.
fn json_envelope(value: &[u8]) -> Vec<u8> {
    let mut out = String::with_capacity(value.len() * 4 + 16);
    out.push_str("{\"value\":[");
    for (i, b) in value.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&b.to_string());
    }
    out.push_str("]}");
    out.into_bytes()
}

/// Build a canonical CBOR `HttpObject` envelope: a one-pair map
/// whose text key `value` maps to an array of unsigned bytes.
fn cbor_envelope(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() * 2 + 8);
    // Map with one pair.
    out.push(0xa1);
    // Text-string key "value" (major type 3).
    cbor_head(0x60, 5, &mut out);
    out.extend_from_slice(b"value");
    // Array of value.len() unsigned integers (major type 4).
    cbor_head(0x80, value.len() as u64, &mut out);
    for &b in value {
        // Each byte as an unsigned integer (major type 0).
        cbor_head(0x00, u64::from(b), &mut out);
    }
    out
}

/// Emit a canonical CBOR head: the major-type base byte `base`
/// (already shifted into the high three bits) carrying argument
/// `val`, using the shortest encoding.
fn cbor_head(base: u8, val: u64, out: &mut Vec<u8>) {
    if val < 24 {
        out.push(base | (val as u8));
    } else if val < 0x100 {
        out.push(base | 24);
        out.push(val as u8);
    } else if val < 0x1_0000 {
        out.push(base | 25);
        out.extend_from_slice(&(val as u16).to_be_bytes());
    } else if val < 0x1_0000_0000 {
        out.push(base | 26);
        out.extend_from_slice(&(val as u32).to_be_bytes());
    } else {
        out.push(base | 27);
        out.extend_from_slice(&val.to_be_bytes());
    }
}

/// Build a protobuf `HttpObject` envelope: field 1 (`value`),
/// wire type 2 (length-delimited).
fn protobuf_envelope(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 8);
    out.push(0x0a); // (field 1 << 3) | wire type 2.
    write_varint(value.len() as u64, &mut out);
    out.extend_from_slice(value);
    out
}

/// Append `val` as a base-128 LEB varint.
fn write_varint(mut val: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (val & 0x7f) as u8;
        val >>= 7;
        if val == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn classify(msg: &str, err: &reqwest::Error) -> String {
    if err.is_timeout() {
        format!("timeout: {msg}")
    } else if err.is_connect() {
        format!("closed: {msg}")
    } else {
        msg.to_string()
    }
}

impl Driver for RiakHttpDriver {
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
        let result = match op {
            "get" => {
                let key = keygen.next(rng);
                self.op_get(&key)
            }
            "put" => {
                let key = keygen.next(rng);
                let v = valgen.next(rng);
                self.op_put(&key, &v)
            }
            "del" => {
                let key = keygen.next(rng);
                self.op_del(&key)
            }
            "txn" => self.op_txn(keygen, valgen, rng),
            other => return DriverOutcome::Err(format!("unsupported op `{other}`")),
        };
        match result {
            Ok(o) => o,
            Err(e) => DriverOutcome::Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_envelope_is_value_array() {
        let body = json_envelope(b"AB");
        assert_eq!(body, br#"{"value":[65,66]}"#.to_vec());
        assert_eq!(json_envelope(b""), br#"{"value":[]}"#.to_vec());
    }

    #[test]
    fn protobuf_envelope_is_field_one_length_delimited() {
        // field 1, wire type 2, length 3, then the bytes.
        let body = protobuf_envelope(b"abc");
        assert_eq!(body, vec![0x0a, 0x03, b'a', b'b', b'c']);
    }

    #[test]
    fn varint_encodes_multibyte_lengths() {
        let mut out = Vec::new();
        write_varint(300, &mut out);
        // 300 = 0b100101100 -> 0xac, 0x02.
        assert_eq!(out, vec![0xac, 0x02]);
        // A 200-byte value forces a two-byte length prefix.
        let body = protobuf_envelope(&[b'x'; 200]);
        assert_eq!(&body[..3], &[0x0a, 0xc8, 0x01]);
        assert_eq!(body.len(), 3 + 200);
    }

    #[test]
    fn cbor_envelope_small_value_is_canonical_map() {
        // map(1) { "value": [65, 66] }.
        // 0xa1 map(1); 0x65 'value'; 0x82 array(2); 0x18 0x41; 0x18 0x42.
        let body = cbor_envelope(b"AB");
        assert_eq!(
            body,
            vec![0xa1, 0x65, b'v', b'a', b'l', b'u', b'e', 0x82, 0x18, 0x41, 0x18, 0x42]
        );
    }

    #[test]
    fn cbor_head_uses_shortest_form() {
        let mut out = Vec::new();
        cbor_head(0x80, 2, &mut out); // array(2)
        assert_eq!(out, vec![0x82]);
        out.clear();
        cbor_head(0x80, 100, &mut out); // array, 1-byte arg
        assert_eq!(out, vec![0x98, 100]);
        out.clear();
        cbor_head(0x80, 300, &mut out); // array, 2-byte arg
        assert_eq!(out, vec![0x99, 0x01, 0x2c]);
    }

    #[test]
    fn encode_envelope_dispatches_on_encoding() {
        assert_eq!(
            encode_envelope(HttpEncoding::Json, b"A"),
            br#"{"value":[65]}"#.to_vec()
        );
        assert_eq!(
            encode_envelope(HttpEncoding::Protobuf, b"A"),
            vec![0x0a, 0x01, b'A']
        );
        assert_eq!(
            encode_envelope(HttpEncoding::Cbor, b"A"),
            vec![0xa1, 0x65, b'v', b'a', b'l', b'u', b'e', 0x81, 0x18, 0x41]
        );
    }
}
