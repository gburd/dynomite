//! Entropy receiver.
//!
//! Drives the server side of the reconciliation channel: bind,
//! accept, validate the negotiation, decrypt incoming chunks, and
//! hand the plaintext snapshot to a [`SnapshotSink`].

use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use aes::Aes128;
use cbc::cipher::block_padding::Pkcs7;
use cbc::cipher::{BlockDecryptMut, KeyIvInit};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream as TokioTcpStream};
use tokio::task::JoinHandle;

use crate::entropy::util::EntropyMaterial;
use crate::entropy::{
    BoxedSnapshotSink, EntropyConfig, EntropyError, EntropyResult, NegotiationHeader,
    SnapshotHeader, SnapshotSink, MAX_CIPHER_SIZE,
};

type Aes128CbcDec = cbc::Decryptor<Aes128>;

/// Entropy receiver driver.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
/// use std::sync::Arc;
/// use dynomite::entropy::{
///     receive::RedisReplaySink, EntropyConfig, EntropyReceiver,
/// };
///
/// # async fn run() {
/// let cfg = EntropyConfig {
///     key_file: PathBuf::from("conf/recon_key.pem"),
///     iv_file: PathBuf::from("conf/recon_iv.pem"),
///     listen_addr: "127.0.0.1:8105".parse().unwrap(),
///     send_addr: None,
///     peer_endpoint: "127.0.0.1:8105".parse().unwrap(),
///     buffer_size: 16 * 1024,
///     header_size: 1024,
///     encrypt: true,
/// };
/// let sink = Arc::new(RedisReplaySink::default());
/// let handle = EntropyReceiver::run(cfg, sink).await.unwrap();
/// handle.abort();
/// # }
/// ```
pub struct EntropyReceiver {
    listener: TcpListener,
    cfg: EntropyConfig,
    sink: BoxedSnapshotSink,
}

impl EntropyReceiver {
    /// Bind a receiver to `cfg.listen_addr` and spawn the accept
    /// loop on a tokio task.
    ///
    /// Each accepted connection is handled in line on the same
    /// task, mirroring the reference engine's single-threaded
    /// entropy loop. The returned handle resolves to `Ok(())` only
    /// after the listener is shut down (e.g. by aborting the task)
    /// or to an error if the bind fails.
    ///
    /// # Errors
    /// Forwards anything from key loading or socket bind.
    pub async fn run(
        cfg: EntropyConfig,
        sink: BoxedSnapshotSink,
    ) -> EntropyResult<JoinHandle<EntropyResult<()>>> {
        let recv = Self::bind(cfg, sink).await?;
        Ok(tokio::spawn(async move { recv.accept_loop().await }))
    }

    /// Bind without spawning. Used by tests that want to drive the
    /// accept loop on the caller's task.
    ///
    /// # Errors
    /// Forwards anything from key loading or socket bind.
    pub async fn bind(cfg: EntropyConfig, sink: BoxedSnapshotSink) -> EntropyResult<Self> {
        cfg.validate()?;
        // Eagerly validate that key material is loadable when
        // encryption is in use. Repeated reads happen on every
        // accept so the listener is also useful as an end-to-end
        // smoke test.
        if cfg.encrypt {
            let _ = crate::entropy::util::load_material(&cfg.key_file, &cfg.iv_file)?;
        }
        let listener = TcpListener::bind(cfg.listen_addr).await?;
        Ok(Self {
            listener,
            cfg,
            sink,
        })
    }

    /// Local address the receiver is bound to. Useful for tests
    /// that bind to `:0` and then dial the kernel-assigned port.
    ///
    /// # Errors
    /// Forwarded from the underlying socket call.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept exactly one connection, process it, and return.
    ///
    /// # Errors
    /// Forwards I/O, protocol, and crypto errors from the worker.
    pub async fn accept_one(self) -> EntropyResult<usize> {
        let (sock, _peer) = self.listener.accept().await?;
        handle_one(sock, &self.cfg, self.sink.as_ref()).await
    }

    /// Accept connections until the task is aborted.
    pub async fn accept_loop(self) -> EntropyResult<()> {
        loop {
            let (sock, peer) = self.listener.accept().await?;
            tracing::debug!(?peer, "entropy receiver accepted connection");
            if let Err(e) = handle_one(sock, &self.cfg, self.sink.as_ref()).await {
                tracing::warn!(?e, "entropy worker errored");
            }
        }
    }
}

async fn handle_one(
    mut stream: TokioTcpStream,
    cfg: &EntropyConfig,
    sink: &dyn SnapshotSink,
) -> EntropyResult<usize> {
    stream.set_nodelay(true)?;
    let neg = read_negotiation(&mut stream).await?;
    let snap = read_snapshot_header(&mut stream, &neg).await?;
    let material = if snap.encrypt_flag == 1 {
        Some(crate::entropy::util::load_material(
            &cfg.key_file,
            &cfg.iv_file,
        )?)
    } else {
        None
    };
    let plaintext = read_chunks(&mut stream, &neg, &snap, material.as_ref()).await?;
    sink.apply(&plaintext).map_err(|e| match e {
        EntropyError::Sink(msg) => EntropyError::Sink(msg),
        other => EntropyError::Sink(other.to_string()),
    })?;
    Ok(plaintext.len())
}

async fn read_negotiation(stream: &mut TokioTcpStream) -> EntropyResult<NegotiationHeader> {
    let mut wire = [0u8; NegotiationHeader::SIZE];
    stream.read_exact(&mut wire).await?;
    NegotiationHeader::from_wire(&wire)
}

async fn read_snapshot_header(
    stream: &mut TokioTcpStream,
    neg: &NegotiationHeader,
) -> EntropyResult<SnapshotHeader> {
    let mut buf = vec![0u8; neg.header_size as usize];
    stream.read_exact(&mut buf).await?;
    SnapshotHeader::from_wire(&buf)
}

async fn read_chunks(
    stream: &mut TokioTcpStream,
    neg: &NegotiationHeader,
    snap: &SnapshotHeader,
    material: Option<&EntropyMaterial>,
) -> EntropyResult<Vec<u8>> {
    let total_len = snap.total_len as usize;
    let mut plaintext = Vec::with_capacity(total_len);
    let mut len_buf = [0u8; 4];
    while plaintext.len() < total_len {
        stream.read_exact(&mut len_buf).await?;
        let chunk_len = u32::from_be_bytes(len_buf) as usize;
        if chunk_len == 0 {
            return Err(EntropyError::Protocol("zero-length chunk".to_string()));
        }
        if chunk_len > MAX_CIPHER_SIZE || chunk_len > neg.cipher_size as usize {
            return Err(EntropyError::Protocol(format!(
                "chunk_len {chunk_len} exceeds negotiated cipher_size {}",
                neg.cipher_size
            )));
        }
        let mut payload = vec![0u8; chunk_len];
        stream.read_exact(&mut payload).await?;
        let chunk_plain = if let Some(mat) = material {
            decrypt_chunk(&payload, mat)?
        } else {
            payload
        };
        let take = (total_len - plaintext.len()).min(chunk_plain.len());
        if take < chunk_plain.len() {
            return Err(EntropyError::Protocol(format!(
                "chunk overshoots total_len: have {} more after {} bytes",
                chunk_plain.len() - take,
                plaintext.len() + take
            )));
        }
        plaintext.extend_from_slice(&chunk_plain[..take]);
    }

    // Drain any final framing length the sender might emit; for
    // well-behaved senders the stream is now at EOF. If extra
    // bytes arrive, treat that as a protocol violation.
    let mut probe = [0u8; 1];
    match stream.read(&mut probe).await {
        Ok(0) | Err(_) => {}
        Ok(_) => {
            return Err(EntropyError::Protocol(
                "trailing bytes after declared total_len".to_string(),
            ));
        }
    }

    Ok(plaintext)
}

/// AES-128-CBC decrypt one ciphertext chunk using the entropy
/// key + IV. Returned plaintext has any PKCS#7 padding stripped.
///
/// Pure helper exposed for the integration tests.
///
/// # Errors
/// [`EntropyError::Crypto`] if the input length is not a positive
/// multiple of 16 or the trailing block has invalid padding.
pub fn decrypt_chunk(ciphertext: &[u8], material: &EntropyMaterial) -> EntropyResult<Vec<u8>> {
    if ciphertext.is_empty() || !ciphertext.len().is_multiple_of(16) {
        return Err(EntropyError::Crypto(format!(
            "ciphertext length {} is not a positive multiple of 16",
            ciphertext.len()
        )));
    }
    let key = material.key().as_bytes();
    let iv = material.iv().as_bytes();
    let cipher = Aes128CbcDec::new(key.into(), iv.into());
    cipher
        .decrypt_padded_vec_mut::<Pkcs7>(ciphertext)
        .map_err(|e| EntropyError::Crypto(format!("PKCS#7 unpad failed: {e}")))
}

/// Default sink: pushes the decrypted snapshot to a local Redis
/// instance over a fresh TCP connection.
///
/// The reference engine's receiver opens a connection to Redis on
/// `127.0.0.1:22122` and writes each per-key payload as it
/// arrives. The Rust default is a single-shot equivalent: it
/// connects to the configured Redis endpoint, writes the entire
/// decrypted snapshot, and closes. Embedders that want a custom
/// replay strategy plug in their own [`SnapshotSink`] through the
/// Stage 13 API.
///
/// # Examples
///
/// ```
/// use dynomite::entropy::receive::RedisReplaySink;
/// let sink = RedisReplaySink::default();
/// drop(sink);
/// ```
pub struct RedisReplaySink {
    /// Address of the local Redis instance.
    pub redis_addr: SocketAddr,
    /// Connect/read timeout.
    pub timeout: Duration,
}

impl Default for RedisReplaySink {
    fn default() -> Self {
        Self {
            redis_addr: "127.0.0.1:22122".parse().expect("static literal parses"),
            timeout: Duration::from_secs(30),
        }
    }
}

impl RedisReplaySink {
    /// Override the Redis address.
    #[must_use]
    pub fn with_redis_addr(mut self, addr: SocketAddr) -> Self {
        self.redis_addr = addr;
        self
    }
}

impl SnapshotSink for RedisReplaySink {
    fn apply(&self, snapshot: &[u8]) -> EntropyResult<()> {
        let mut sock = TcpStream::connect_timeout(&self.redis_addr, self.timeout)
            .map_err(|e| EntropyError::Sink(format!("connect to redis: {e}")))?;
        sock.set_write_timeout(Some(self.timeout))
            .map_err(|e| EntropyError::Sink(format!("redis timeout: {e}")))?;
        sock.write_all(snapshot)
            .map_err(|e| EntropyError::Sink(format!("redis write: {e}")))?;
        Ok(())
    }
}

/// In-memory sink used by tests and embedders that just want to
/// inspect the decrypted snapshot.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use dynomite::entropy::receive::MemorySink;
/// use dynomite::entropy::SnapshotSink;
/// let sink = Arc::new(MemorySink::default());
/// sink.apply(b"hi").unwrap();
/// assert_eq!(sink.take(), b"hi");
/// ```
#[derive(Default)]
pub struct MemorySink {
    inner: parking_lot::Mutex<Vec<u8>>,
}

impl MemorySink {
    /// Drain the buffered snapshot, leaving the sink empty.
    #[must_use]
    pub fn take(&self) -> Vec<u8> {
        std::mem::take(&mut *self.inner.lock())
    }

    /// Borrow the current contents (clone).
    #[must_use]
    pub fn snapshot(&self) -> Vec<u8> {
        self.inner.lock().clone()
    }
}

impl SnapshotSink for MemorySink {
    fn apply(&self, snapshot: &[u8]) -> EntropyResult<()> {
        let mut buf = self.inner.lock();
        buf.clear();
        buf.extend_from_slice(snapshot);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entropy::send::encrypt_chunk;
    use crate::entropy::util::{EntropyIv, EntropyKey, ENTROPY_IV_LEN, ENTROPY_KEY_LEN};

    fn material() -> EntropyMaterial {
        EntropyMaterial::new(
            EntropyKey::from_bytes([0x10; ENTROPY_KEY_LEN]),
            EntropyIv::from_bytes([0x42; ENTROPY_IV_LEN]),
        )
    }

    #[test]
    fn decrypt_chunk_rejects_short_buffer() {
        let mat = material();
        let err = decrypt_chunk(&[], &mat).unwrap_err();
        assert!(matches!(err, EntropyError::Crypto(_)));
    }

    #[test]
    fn decrypt_chunk_rejects_misaligned() {
        let mat = material();
        let err = decrypt_chunk(&[0u8; 17], &mat).unwrap_err();
        assert!(matches!(err, EntropyError::Crypto(_)));
    }

    #[test]
    fn encrypt_decrypt_round_trip() {
        let mat = material();
        let pt = b"the quick brown fox jumps over the lazy dog";
        let ct = encrypt_chunk(pt, &mat).unwrap();
        let plain = decrypt_chunk(&ct, &mat).unwrap();
        assert_eq!(plain, pt);
    }

    #[test]
    fn decrypt_chunk_rejects_tamper() {
        let mat = material();
        let pt = b"the quick brown fox";
        let mut ct = encrypt_chunk(pt, &mat).unwrap();
        // Flip a bit in the last (padding) block; PKCS#7 unpad
        // will reject this with overwhelming probability.
        let last = ct.last_mut().unwrap();
        *last ^= 0xff;
        let err = decrypt_chunk(&ct, &mat).unwrap_err();
        assert!(matches!(err, EntropyError::Crypto(_)));
    }

    #[test]
    fn memory_sink_round_trips() {
        let sink = MemorySink::default();
        sink.apply(b"abc").unwrap();
        assert_eq!(sink.snapshot(), b"abc");
        let drained = sink.take();
        assert_eq!(drained, b"abc");
        assert!(sink.snapshot().is_empty());
    }
}
