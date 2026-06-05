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

use crate::config::DriverConfig;
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
        let mut req = self
            .client
            .put(&url)
            .header("content-type", "application/octet-stream")
            .body(value.to_vec());
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
