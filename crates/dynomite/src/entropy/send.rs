//! Entropy sender.
//!
//! Drives the client side of the reconciliation channel: dial the
//! peer, negotiate, then push a snapshot from a [`SnapshotSource`]
//! one chunk at a time.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use aes::Aes128;
use cbc::cipher::block_padding::Pkcs7;
use cbc::cipher::{BlockEncryptMut, KeyIvInit};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream as TokioTcpStream;
use tokio::task::JoinHandle;

use crate::entropy::util::EntropyMaterial;
use crate::entropy::{
    BoxedSnapshotSource, EntropyConfig, EntropyError, EntropyResult, NegotiationHeader,
    SnapshotHeader, SnapshotSource, ENTROPY_COMMAND_SEND, ENTROPY_MAGIC,
};

type Aes128CbcEnc = cbc::Encryptor<Aes128>;

/// Entropy sender driver.
///
/// Operationally, calling [`run`](EntropySender::run) spawns a
/// tokio task that performs one push and then exits.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
/// use std::sync::Arc;
/// use dynomite::entropy::{
///     EntropyConfig, EntropySender, RedisLocalSnapshot,
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
/// let source = Arc::new(RedisLocalSnapshot::default());
/// let handle = EntropySender::run(cfg, source);
/// handle.await.unwrap().unwrap();
/// # }
/// ```
pub struct EntropySender;

impl EntropySender {
    /// Spawn a tokio task that performs one snapshot push.
    ///
    /// The returned [`JoinHandle`] resolves to the push result.
    /// The task uses `cfg.peer_endpoint` to dial the receiver and
    /// `cfg.send_addr` (when set) for the local bind.
    #[must_use]
    pub fn run(
        cfg: EntropyConfig,
        source: BoxedSnapshotSource,
    ) -> JoinHandle<EntropyResult<usize>> {
        tokio::spawn(async move { Self::push(cfg, source).await })
    }

    /// Perform one snapshot push and return the number of plaintext
    /// bytes transferred. Used directly by tests; the spawned
    /// variant just calls this on a tokio task.
    ///
    /// # Errors
    /// Forwards anything from key loading, source acquisition,
    /// dialing, or transport.
    pub async fn push(cfg: EntropyConfig, source: BoxedSnapshotSource) -> EntropyResult<usize> {
        cfg.validate()?;
        let material = if cfg.encrypt {
            Some(crate::entropy::util::load_material(
                &cfg.key_file,
                &cfg.iv_file,
            )?)
        } else {
            None
        };
        let snapshot = collect_snapshot(source).await?;

        let mut stream = dial(&cfg).await?;
        write_negotiation(&mut stream, &cfg).await?;
        write_snapshot_header(&mut stream, &cfg, snapshot.len()).await?;
        write_chunks(&mut stream, &cfg, &snapshot, material.as_ref()).await?;
        stream.shutdown().await?;
        Ok(snapshot.len())
    }
}

async fn collect_snapshot(source: BoxedSnapshotSource) -> EntropyResult<Vec<u8>> {
    tokio::task::spawn_blocking(move || source.snapshot())
        .await
        .map_err(|e| EntropyError::Source(format!("snapshot task panicked: {e}")))?
}

async fn dial(cfg: &EntropyConfig) -> EntropyResult<TokioTcpStream> {
    let socket = match cfg.peer_endpoint {
        SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
        SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
    };
    if let Some(local) = cfg.send_addr {
        socket.bind(local)?;
    }
    let stream = socket.connect(cfg.peer_endpoint).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

async fn write_negotiation(stream: &mut TokioTcpStream, cfg: &EntropyConfig) -> EntropyResult<()> {
    let hdr = NegotiationHeader {
        magic: ENTROPY_MAGIC,
        command: ENTROPY_COMMAND_SEND,
        header_size: u32::try_from(cfg.header_size)
            .map_err(|_| EntropyError::Config("header_size > u32::MAX".to_string()))?,
        buffer_size: u32::try_from(cfg.buffer_size)
            .map_err(|_| EntropyError::Config("buffer_size > u32::MAX".to_string()))?,
        cipher_size: u32::try_from(cipher_capacity(cfg.buffer_size))
            .map_err(|_| EntropyError::Config("cipher_size > u32::MAX".to_string()))?,
    };
    stream.write_all(&hdr.to_wire()).await?;
    Ok(())
}

async fn write_snapshot_header(
    stream: &mut TokioTcpStream,
    cfg: &EntropyConfig,
    total_len: usize,
) -> EntropyResult<()> {
    let total_len_u32 = u32::try_from(total_len)
        .map_err(|_| EntropyError::Source(format!("snapshot too large: {total_len} bytes")))?;
    let hdr = SnapshotHeader {
        total_len: total_len_u32,
        encrypt_flag: u32::from(cfg.encrypt),
    };
    let bytes = hdr.to_wire(cfg.header_size)?;
    stream.write_all(&bytes).await?;
    Ok(())
}

async fn write_chunks(
    stream: &mut TokioTcpStream,
    cfg: &EntropyConfig,
    snapshot: &[u8],
    material: Option<&EntropyMaterial>,
) -> EntropyResult<()> {
    let buf = cfg.buffer_size;
    let mut offset = 0;
    while offset < snapshot.len() {
        let end = (offset + buf).min(snapshot.len());
        let plaintext = &snapshot[offset..end];
        let payload: Vec<u8> = if let Some(mat) = material {
            encrypt_chunk(plaintext, mat)?
        } else {
            plaintext.to_vec()
        };
        let chunk_len = u32::try_from(payload.len())
            .map_err(|_| EntropyError::Protocol("chunk too large".to_string()))?;
        stream.write_all(&chunk_len.to_be_bytes()).await?;
        stream.write_all(&payload).await?;
        offset = end;
    }
    Ok(())
}

/// AES-128-CBC encrypt one chunk using the entropy key + IV.
///
/// Pure helper exposed for the integration tests.
///
/// # Errors
/// [`EntropyError::Crypto`] if the cipher reports a fault.
pub fn encrypt_chunk(plaintext: &[u8], material: &EntropyMaterial) -> EntropyResult<Vec<u8>> {
    let key = material.key().as_bytes();
    let iv = material.iv().as_bytes();
    let cipher = Aes128CbcEnc::new(key.into(), iv.into());
    Ok(cipher.encrypt_padded_vec_mut::<Pkcs7>(plaintext))
}

/// PKCS#7 cipher overhead is at most one block; the negotiated
/// cipher capacity is therefore plaintext + 16 bytes. Production
/// servers can safely advertise more headroom.
pub(crate) fn cipher_capacity(buffer_size: usize) -> usize {
    buffer_size + 16
}

/// Default snapshot source: pulls a Redis snapshot from a local
/// Redis instance.
///
/// The reference engine pipes the on-disk AOF file produced by
/// `BGREWRITEAOF` over the entropy channel. The Rust default
/// follows that contract: it issues a `BGREWRITEAOF` over the
/// configured Redis TCP endpoint, waits for the command to be
/// acknowledged, then reads the AOF file from disk. Embedders
/// that need a different snapshot strategy plug in their own
/// [`SnapshotSource`] through the Stage 13 API.
///
/// # Examples
///
/// ```
/// use dynomite::entropy::RedisLocalSnapshot;
/// let source = RedisLocalSnapshot::default();
/// drop(source);
/// ```
pub struct RedisLocalSnapshot {
    /// Address of the local Redis instance.
    pub redis_addr: SocketAddr,
    /// Path to the AOF file on disk.
    pub aof_path: std::path::PathBuf,
    /// Connect/read timeout.
    pub timeout: Duration,
    /// Number of `BGREWRITEAOF` retries before giving up. The
    /// reference engine retries once after a 10 second sleep; we
    /// expose the count and pause as parameters.
    pub bgrewrite_retries: u32,
    /// Pause between `BGREWRITEAOF` retries.
    pub bgrewrite_retry_pause: Duration,
}

impl Default for RedisLocalSnapshot {
    fn default() -> Self {
        Self {
            redis_addr: "127.0.0.1:22122".parse().expect("static literal parses"),
            aof_path: std::path::PathBuf::from("/mnt/data/nfredis/appendonly.aof"),
            timeout: Duration::from_secs(30),
            bgrewrite_retries: 1,
            bgrewrite_retry_pause: Duration::from_secs(10),
        }
    }
}

impl RedisLocalSnapshot {
    /// Override the Redis address.
    #[must_use]
    pub fn with_redis_addr(mut self, addr: SocketAddr) -> Self {
        self.redis_addr = addr;
        self
    }

    /// Override the AOF on-disk path.
    #[must_use]
    pub fn with_aof_path(mut self, path: std::path::PathBuf) -> Self {
        self.aof_path = path;
        self
    }

    fn bgrewriteaof(&self) -> EntropyResult<()> {
        let mut last_err: Option<EntropyError> = None;
        for attempt in 0..=self.bgrewrite_retries {
            match self.try_bgrewriteaof() {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < self.bgrewrite_retries {
                        std::thread::sleep(self.bgrewrite_retry_pause);
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            EntropyError::Source("bgrewriteaof failed without error".to_string())
        }))
    }

    fn try_bgrewriteaof(&self) -> EntropyResult<()> {
        let mut sock = TcpStream::connect_timeout(&self.redis_addr, self.timeout)
            .map_err(|e| EntropyError::Source(format!("connect to redis: {e}")))?;
        sock.set_read_timeout(Some(self.timeout))
            .map_err(|e| EntropyError::Source(format!("redis timeout: {e}")))?;
        sock.set_write_timeout(Some(self.timeout))
            .map_err(|e| EntropyError::Source(format!("redis timeout: {e}")))?;
        sock.write_all(b"*1\r\n$13\r\nBGREWRITEAOF\r\n")
            .map_err(|e| EntropyError::Source(format!("redis write: {e}")))?;
        let mut buf = [0u8; 256];
        let n = sock
            .read(&mut buf)
            .map_err(|e| EntropyError::Source(format!("redis read: {e}")))?;
        let reply = &buf[..n];
        if reply.first() != Some(&b'+') {
            return Err(EntropyError::Source(format!(
                "BGREWRITEAOF rejected: {}",
                String::from_utf8_lossy(reply)
            )));
        }
        Ok(())
    }
}

impl SnapshotSource for RedisLocalSnapshot {
    fn snapshot(&self) -> EntropyResult<Vec<u8>> {
        self.bgrewriteaof()?;
        // Brief pause to let Redis flush the AOF rewrite to disk.
        std::thread::sleep(Duration::from_secs(1));
        let bytes = std::fs::read(&self.aof_path).map_err(|e| {
            EntropyError::Source(format!("read AOF {}: {e}", self.aof_path.display()))
        })?;
        Ok(bytes)
    }
}

/// In-memory snapshot source useful in tests and embedders that
/// already hold the snapshot in RAM.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use dynomite::entropy::send::StaticSnapshot;
/// use dynomite::entropy::SnapshotSource;
/// let s = Arc::new(StaticSnapshot::new(b"hello".to_vec()));
/// assert_eq!(s.snapshot().unwrap(), b"hello");
/// ```
pub struct StaticSnapshot {
    bytes: Vec<u8>,
}

impl StaticSnapshot {
    /// Wrap a fixed byte vector.
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

impl SnapshotSource for StaticSnapshot {
    fn snapshot(&self) -> EntropyResult<Vec<u8>> {
        Ok(self.bytes.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entropy::util::{EntropyIv, EntropyKey, ENTROPY_IV_LEN, ENTROPY_KEY_LEN};

    fn material() -> EntropyMaterial {
        EntropyMaterial::new(
            EntropyKey::from_bytes([0x10; ENTROPY_KEY_LEN]),
            EntropyIv::from_bytes([0x42; ENTROPY_IV_LEN]),
        )
    }

    #[test]
    fn encrypt_chunk_round_trips_with_pkcs7() {
        use aes::Aes128;
        use cbc::cipher::block_padding::Pkcs7;
        use cbc::cipher::{BlockDecryptMut, KeyIvInit};
        type Dec = cbc::Decryptor<Aes128>;

        let mat = material();
        let pt = b"hello entropy world";
        let ct = encrypt_chunk(pt, &mat).unwrap();
        // PKCS#7 always extends to the next 16-byte block.
        assert!(ct.len() >= pt.len());
        assert_eq!(ct.len() % 16, 0);

        let dec = Dec::new(mat.key().as_bytes().into(), mat.iv().as_bytes().into());
        let plain = dec.decrypt_padded_vec_mut::<Pkcs7>(&ct).unwrap();
        assert_eq!(plain, pt);
    }

    #[test]
    fn cipher_capacity_includes_pkcs7_block() {
        assert_eq!(cipher_capacity(16), 32);
        assert_eq!(cipher_capacity(15), 31);
    }

    #[test]
    fn static_snapshot_returns_payload() {
        let s = StaticSnapshot::new(b"abc".to_vec());
        assert_eq!(s.snapshot().unwrap(), b"abc");
    }

    #[test]
    fn arc_static_snapshot_returns_payload() {
        let s: BoxedSnapshotSource = std::sync::Arc::new(StaticSnapshot::new(vec![1, 2, 3]));
        assert_eq!(s.snapshot().unwrap(), vec![1, 2, 3]);
    }
}
