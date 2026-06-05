//! Riak Protocol Buffer Client (PBC) driver over QUIC.
//!
//! Speaks the exact same PBC framing as the TCP
//! [`super::riak::RiakPbcDriver`] -- a 4-byte big-endian length
//! prefix, a one-byte message code, then the protobuf body -- but
//! carries the bytes over a QUIC bidirectional stream dialled with
//! the engine's shared quiche integration
//! ([`dynomite::net::quic`]) instead of a TCP socket.
//!
//! The driver protobuf encoders are shared with the TCP driver
//! ([`super::riak`]) so the QUIC workload is byte-identical on the
//! wire; only the transport differs.
//!
//! The engine's QUIC client is async (it spawns a tokio packet
//! pump), so this driver owns a small single-worker tokio runtime
//! and drives each blocking [`Driver::run`] op through
//! `Runtime::block_on`. The packet-pump task lives on the
//! runtime's worker thread for the lifetime of the driver, so the
//! connection keeps making progress between ops.

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use rand::rngs::SmallRng;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::runtime::Runtime;

use dynomite::net::quic::{connect, QuicConfig, QuicTransport};

use crate::config::DriverConfig;
use crate::driver::riak::{
    decode_error_resp, encode_del, encode_get, encode_put, recent_or, remember, RIAK_CODE_DEL_REQ,
    RIAK_CODE_DEL_RESP, RIAK_CODE_ERROR_RESP, RIAK_CODE_GET_REQ, RIAK_CODE_GET_RESP,
    RIAK_CODE_PING_REQ, RIAK_CODE_PING_RESP, RIAK_CODE_PUT_REQ, RIAK_CODE_PUT_RESP,
};
use crate::driver::{Driver, DriverOutcome};
use crate::error::BenchError;
use crate::keygen::KeyGen;
use crate::valgen::ValGen;

/// Ops the QUIC driver speaks. A deliberate subset of the TCP
/// driver's vocabulary: the QUIC path exists to prove the
/// transport, not to re-implement the data-type workloads.
const SUPPORTED: &[&str] = &["ping", "get", "put", "del"];

/// Split halves of a connected [`QuicTransport`].
type Halves = (ReadHalf<QuicTransport>, WriteHalf<QuicTransport>);

/// The Riak PBC-over-QUIC driver. Owns one QUIC connection and a
/// single-worker tokio runtime per worker.
pub struct RiakQuicDriver {
    addr: SocketAddr,
    bucket: Vec<u8>,
    timeout: Duration,
    rt: Runtime,
    conn: Option<Halves>,
}

impl RiakQuicDriver {
    /// Construct from configuration. The QUIC connection is opened
    /// lazily on the first op.
    ///
    /// # Errors
    ///
    /// Returns [`BenchError::Config`] when the `host:port` pair
    /// does not resolve, or [`BenchError::Engine`] when the
    /// backing tokio runtime cannot be built.
    pub fn new(cfg: &DriverConfig) -> Result<Self, BenchError> {
        let addr = (cfg.host.as_str(), cfg.port)
            .to_socket_addrs()
            .map_err(|e| BenchError::Config(format!("resolve {}:{}: {e}", cfg.host, cfg.port)))?
            .next()
            .ok_or_else(|| {
                BenchError::Config(format!("no addresses for {}:{}", cfg.host, cfg.port))
            })?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| BenchError::Engine(format!("quic driver runtime: {e}")))?;
        Ok(Self {
            addr,
            bucket: cfg.bucket.as_bytes().to_vec(),
            timeout: Duration::from_millis(cfg.timeout_ms),
            rt,
            conn: None,
        })
    }

    /// Open the QUIC connection if it is not already up, dialling
    /// the listener with the insecure client config (the bench
    /// does not verify certificates).
    fn ensure_connected(&mut self) -> io::Result<()> {
        if self.conn.is_some() {
            return Ok(());
        }
        let addr = self.addr;
        let timeout = self.timeout;
        let halves = self.rt.block_on(async move {
            let cfg = QuicConfig::client_insecure();
            let transport = tokio::time::timeout(timeout, connect(addr, cfg))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "quic connect timed out"))??;
            Ok::<Halves, io::Error>(tokio::io::split(transport))
        })?;
        self.conn = Some(halves);
        Ok(())
    }

    fn drop_conn(&mut self) {
        self.conn = None;
    }

    /// Send one `(code, body)` frame and read back one frame,
    /// bounded by the configured timeout.
    fn call(&mut self, code: u8, body: &[u8]) -> io::Result<(u8, Vec<u8>)> {
        self.ensure_connected()?;
        let timeout = self.timeout;
        let Some((reader, writer)) = self.conn.as_mut() else {
            return Err(io::Error::other("quic connection missing"));
        };
        self.rt.block_on(async move {
            tokio::time::timeout(timeout, call_async(reader, writer, code, body))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "quic call timed out"))?
        })
    }

    fn call_check(&mut self, code: u8, body: &[u8], expected: u8) -> Result<Vec<u8>, String> {
        match self.call(code, body) {
            Ok((c, b)) if c == expected => Ok(b),
            Ok((c, b)) if c == RIAK_CODE_ERROR_RESP => {
                Err(format!("riak error: {}", decode_error_resp(&b)))
            }
            Ok((c, _)) => {
                self.drop_conn();
                Err(format!("unexpected reply code {c}"))
            }
            Err(e) => {
                self.drop_conn();
                Err(format!("io error: {e}"))
            }
        }
    }
}

/// Frame one PBC request onto the QUIC stream and read one frame
/// back. Frame layout matches the TCP driver: a 4-byte big-endian
/// length (covering the code byte plus the body), the code byte,
/// then the body.
async fn call_async(
    reader: &mut ReadHalf<QuicTransport>,
    writer: &mut WriteHalf<QuicTransport>,
    code: u8,
    body: &[u8],
) -> io::Result<(u8, Vec<u8>)> {
    let len = u32::try_from(1 + body.len()).map_err(|_| io::Error::other("frame too large"))?;
    let mut frame = Vec::with_capacity(5 + body.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.push(code);
    frame.extend_from_slice(body);
    writer.write_all(&frame).await?;
    writer.flush().await?;

    let mut head = [0u8; 4];
    reader.read_exact(&mut head).await?;
    let total = u32::from_be_bytes(head) as usize;
    if total < 1 {
        return Err(io::Error::other("zero-length frame"));
    }
    let mut code_buf = [0u8; 1];
    reader.read_exact(&mut code_buf).await?;
    let body = if total > 1 {
        let mut b = vec![0u8; total - 1];
        reader.read_exact(&mut b).await?;
        b
    } else {
        Vec::new()
    };
    Ok((code_buf[0], body))
}

impl Driver for RiakQuicDriver {
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
            other => DriverOutcome::Err(format!("unsupported op `{other}`")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DriverKind;

    fn cfg() -> DriverConfig {
        DriverConfig {
            kind: DriverKind::RiakQuic,
            host: "127.0.0.1".to_string(),
            port: 0,
            timeout_ms: 1000,
            bucket: "bench".to_string(),
            encoding: crate::config::HttpEncoding::Json,
        }
    }

    #[test]
    fn new_resolves_and_reports_supported_ops() {
        let d = RiakQuicDriver::new(&cfg()).expect("construct quic driver");
        assert_eq!(d.supported_ops(), &["ping", "get", "put", "del"]);
    }
}
