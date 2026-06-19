//! Entropy reconciliation channel.
//!
//! The entropy module ships a snapshot of the local datastore to a
//! peer reconciliation engine (and vice versa) over a TCP
//! connection. Each chunk of the snapshot is encrypted with
//! AES-128-CBC using a pre-shared key and IV loaded from the
//! conf-configured `recon_key.pem` and `recon_iv.pem` files. The
//! key and IV used here are independent from the per-peer
//! AES session key established during the DNODE handshake; the
//! entropy channel uses its own pre-shared material.
//!
//! # Layout
//!
//! * [`util`] holds the on-disk key/IV loaders and validated
//!   wrappers.
//! * [`send`] drives the client side: connect, write the
//!   negotiation header, stream the snapshot.
//! * [`receive`] drives the server side: bind, accept, replay the
//!   plaintext into a [`SnapshotSink`].
//! * The top-level [`EntropyConfig`] gathers the operator-visible
//!   knobs.
//!
//! # Wire format
//!
//! The reconciliation protocol uses a single, length-prefixed
//! framing that is symmetric across sender and receiver. See
//! [`docs/parity.md`](../../../docs/parity.md) for the precise
//! divergence list.
//!
//! ```text
//! Negotiation header (20 bytes, big-endian u32 fields):
//!     magic        = 0x64640001
//!     command      = 1   (SEND: data flowing sender -> receiver)
//!     header_size  = N   (size in bytes of the snapshot header below)
//!     buffer_size  = M   (max plaintext bytes per chunk)
//!     cipher_size  = C   (max ciphertext bytes per chunk)
//!
//! Snapshot header (header_size bytes, big-endian u32 fields then
//! zero-padded):
//!     total_len    = L   (total plaintext length in bytes)
//!     encrypt_flag = 0|1 (1 if chunks are AES-128-CBC encrypted)
//!     <header_size - 8 bytes of zero padding>
//!
//! Chunks (repeated until L plaintext bytes have been delivered):
//!     u32 BE chunk_len
//!     <chunk_len bytes of payload>
//!
//! When encrypt_flag is set each payload is the AES-128-CBC
//! ciphertext of the matching plaintext chunk, PKCS#7-padded to a
//! 16-byte boundary; the receiver strips the padding before
//! delivering bytes to its sink.
//! ```
//!
//! # Example
//!
//! ```no_run
//! use std::path::PathBuf;
//! use dynomite::entropy::{EntropyConfig, EntropyReceiver, EntropySender};
//!
//! let cfg = EntropyConfig {
//!     key_file: PathBuf::from("/etc/dynomite/recon_key.pem"),
//!     iv_file: PathBuf::from("/etc/dynomite/recon_iv.pem"),
//!     listen_addr: "127.0.0.1:8105".parse().unwrap(),
//!     send_addr: None,
//!     peer_endpoint: "127.0.0.1:8105".parse().unwrap(),
//!     buffer_size: 16 * 1024,
//!     header_size: 1024,
//!     encrypt: true,
//! };
//! drop(cfg);
//! ```

pub mod driver;
pub mod receive;
pub mod send;
pub mod util;

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use thiserror::Error;

pub use crate::entropy::receive::EntropyReceiver;
pub use crate::entropy::send::{EntropySender, RedisLocalSnapshot};
pub use crate::entropy::util::{
    EntropyIv, EntropyKey, EntropyMaterial, ENTROPY_IV_LEN, ENTROPY_KEY_LEN,
};

/// Box a [`SnapshotSource`] into the [`BoxedSnapshotSource`] alias
/// expected by [`EntropySender::run`].
///
/// # Examples
///
/// ```
/// use dynomite::entropy::{boxed_source, send::StaticSnapshot, SnapshotSource};
/// let s = boxed_source(StaticSnapshot::new(vec![1, 2, 3]));
/// assert_eq!(s.snapshot().unwrap(), vec![1, 2, 3]);
/// ```
#[must_use]
pub fn boxed_source<S: SnapshotSource + 'static>(source: S) -> BoxedSnapshotSource {
    Arc::new(source)
}

/// Box a [`SnapshotSink`] into the [`BoxedSnapshotSink`] alias
/// expected by [`EntropyReceiver::run`].
///
/// # Examples
///
/// ```
/// use dynomite::entropy::{boxed_sink, receive::MemorySink, SnapshotSink};
/// let sink = boxed_sink(MemorySink::default());
/// sink.apply(b"hi").unwrap();
/// ```
#[must_use]
pub fn boxed_sink<S: SnapshotSink + 'static>(sink: S) -> BoxedSnapshotSink {
    Arc::new(sink)
}

/// Magic word that opens every entropy negotiation header.
///
/// The magic word (`64640001`) that opens every entropy
/// negotiation header.
pub const ENTROPY_MAGIC: u32 = 0x6464_0001;

/// Negotiation command: sender pushes a snapshot to the receiver.
pub const ENTROPY_COMMAND_SEND: u32 = 1;

/// Default plaintext chunk size, in bytes (16 KiB).
pub const DEFAULT_BUFFER_SIZE: usize = 16 * 1024;

/// Default ciphertext chunk capacity, in bytes. The reference
/// engine reserves an extra 1 KiB above the plaintext buffer to
/// hold cipher overhead; we mirror that headroom, which is far
/// more than PKCS#7 needs.
pub const DEFAULT_CIPHER_SIZE: usize = DEFAULT_BUFFER_SIZE + 1024;

/// Default snapshot header size, in bytes. Matches the reference
/// engine's `MAX_HEADER_SIZE`.
pub const DEFAULT_HEADER_SIZE: usize = 1024;

/// Hard ceiling on the negotiated `header_size`, in bytes.
///
/// Hard ceiling validated on the negotiation header size.
pub const MAX_HEADER_SIZE: usize = 1024;

/// Hard ceiling on the negotiated `buffer_size`, in bytes (5 MiB).
pub const MAX_BUFFER_SIZE: usize = 5 * 1024 * 1024;

/// Hard ceiling on the negotiated `cipher_size`, in bytes (5 MiB).
pub const MAX_CIPHER_SIZE: usize = 5 * 1024 * 1024;

/// Hard ceiling on a single snapshot's plaintext size, in bytes (4 GiB).
///
/// This bound exists because the receiver stages every chunk into a
/// `Vec<u8>` for replay and so MUST cap the upfront allocation. A
/// malicious sender that completes the negotiation handshake could
/// otherwise declare `total_len = u32::MAX` and trigger a 4 GiB
/// allocation attempt before any payload bytes arrive. The cap below
/// is a generous practical ceiling (4 GiB minus 1) that still lets
/// large RDB snapshots through; embedders can plug their own
/// `SnapshotSink` to apply tighter bounds.
pub const MAX_SNAPSHOT_SIZE: usize = u32::MAX as usize - 1;

/// Cap the receiver's pre-allocation hint to avoid a malicious or
/// malformed `total_len` triggering an oversized allocation up
/// front. Real RDB snapshots stream chunk-by-chunk; the receiver
/// reallocates as plaintext arrives if the snapshot is genuinely
/// larger than the hint.
pub const SAFE_PREALLOC: usize = 16 * 1024 * 1024;

/// Operator-facing configuration for the entropy worker.
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
/// use dynomite::entropy::EntropyConfig;
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
/// assert_eq!(cfg.buffer_size, 16 * 1024);
/// ```
#[derive(Debug, Clone)]
pub struct EntropyConfig {
    /// On-disk path to the AES-128 key file.
    pub key_file: PathBuf,
    /// On-disk path to the AES-128-CBC IV file.
    pub iv_file: PathBuf,
    /// Address the [`EntropyReceiver`] binds to.
    pub listen_addr: SocketAddr,
    /// Optional local bind address for the [`EntropySender`]. When
    /// `None` the kernel assigns an ephemeral port on the wildcard
    /// address.
    pub send_addr: Option<SocketAddr>,
    /// Address the [`EntropySender`] dials.
    pub peer_endpoint: SocketAddr,
    /// Plaintext chunk size (bytes). Must be a multiple of 16 when
    /// encryption is enabled and must not exceed
    /// [`MAX_BUFFER_SIZE`].
    pub buffer_size: usize,
    /// Snapshot header size (bytes). Must not exceed
    /// [`MAX_HEADER_SIZE`].
    pub header_size: usize,
    /// Whether per-chunk payloads are AES-128-CBC encrypted.
    pub encrypt: bool,
}

impl EntropyConfig {
    /// Validate the cross-field invariants the protocol demands.
    ///
    /// # Errors
    /// [`EntropyError::Config`] when any invariant is violated.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::PathBuf;
    /// use dynomite::entropy::EntropyConfig;
    /// let mut cfg = EntropyConfig {
    ///     key_file: PathBuf::from("k"),
    ///     iv_file: PathBuf::from("v"),
    ///     listen_addr: "127.0.0.1:0".parse().unwrap(),
    ///     send_addr: None,
    ///     peer_endpoint: "127.0.0.1:0".parse().unwrap(),
    ///     buffer_size: 32,
    ///     header_size: 64,
    ///     encrypt: true,
    /// };
    /// assert!(cfg.validate().is_ok());
    /// cfg.buffer_size = 0;
    /// assert!(cfg.validate().is_err());
    /// ```
    pub fn validate(&self) -> Result<(), EntropyError> {
        if self.buffer_size == 0 || self.buffer_size > MAX_BUFFER_SIZE {
            return Err(EntropyError::Config(format!(
                "buffer_size {} out of range (1..={MAX_BUFFER_SIZE})",
                self.buffer_size
            )));
        }
        if self.header_size < 8 || self.header_size > MAX_HEADER_SIZE {
            return Err(EntropyError::Config(format!(
                "header_size {} out of range (8..={MAX_HEADER_SIZE})",
                self.header_size
            )));
        }
        if self.encrypt && !self.buffer_size.is_multiple_of(16) {
            return Err(EntropyError::Config(format!(
                "buffer_size {} must be a multiple of 16 with encryption enabled",
                self.buffer_size
            )));
        }
        Ok(())
    }
}

/// Snapshot byte source.
///
/// Implementations are pluggable via the
/// embedding API; the engine ships [`RedisLocalSnapshot`] as the
/// default. Implementations are expected to be cheap to clone
/// (e.g. shared via [`Arc`]) but each call to [`snapshot`] may
/// produce a different blob.
///
/// [`snapshot`]: SnapshotSource::snapshot
pub trait SnapshotSource: Send + Sync {
    /// Produce one snapshot of the local state as a contiguous
    /// byte buffer. The sender treats the bytes as opaque; the
    /// receiver replays them through its [`SnapshotSink`].
    ///
    /// # Errors
    /// Implementation-defined.
    fn snapshot(&self) -> Result<Vec<u8>, EntropyError>;
}

/// Boxed [`SnapshotSource`] handed to [`EntropySender`].
pub type BoxedSnapshotSource = Arc<dyn SnapshotSource>;

impl<T> SnapshotSource for Arc<T>
where
    T: SnapshotSource + ?Sized,
{
    fn snapshot(&self) -> Result<Vec<u8>, EntropyError> {
        (**self).snapshot()
    }
}

/// Receiver-side hook that consumes the decrypted snapshot.
///
/// Implementations are pluggable via the
/// embedding API. The engine ships an in-memory implementation
/// for tests and the [`receive::RedisReplaySink`] default for
/// production wiring.
pub trait SnapshotSink: Send + Sync {
    /// Apply the full plaintext snapshot to the local datastore.
    /// Called exactly once per connection.
    ///
    /// # Errors
    /// Implementation-defined.
    fn apply(&self, snapshot: &[u8]) -> Result<(), EntropyError>;
}

/// Boxed [`SnapshotSink`] handed to [`EntropyReceiver`].
pub type BoxedSnapshotSink = Arc<dyn SnapshotSink>;

impl<T> SnapshotSink for Arc<T>
where
    T: SnapshotSink + ?Sized,
{
    fn apply(&self, snapshot: &[u8]) -> Result<(), EntropyError> {
        (**self).apply(snapshot)
    }
}

/// Errors raised by the entropy module.
#[derive(Debug, Error)]
pub enum EntropyError {
    /// I/O or socket failure.
    #[error("entropy io: {0}")]
    Io(#[from] io::Error),
    /// Invalid configuration.
    #[error("entropy config: {0}")]
    Config(String),
    /// Key or IV material is missing or malformed.
    #[error("entropy key material: {0}")]
    KeyMaterial(String),
    /// Wire-protocol violation.
    #[error("entropy protocol: {0}")]
    Protocol(String),
    /// AES-CBC decryption failure (bad padding or wrong key).
    #[error("entropy crypto: {0}")]
    Crypto(String),
    /// Snapshot source produced an error.
    #[error("entropy source: {0}")]
    Source(String),
    /// Snapshot sink rejected the replay.
    #[error("entropy sink: {0}")]
    Sink(String),
}

/// Convenience type alias.
pub type EntropyResult<T> = Result<T, EntropyError>;

/// Negotiation header that opens every entropy connection.
///
/// All five fields are 32-bit big-endian unsigned integers on the
/// wire.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct NegotiationHeader {
    /// Magic word; must equal [`ENTROPY_MAGIC`].
    pub magic: u32,
    /// Direction marker. [`ENTROPY_COMMAND_SEND`] = data flows
    /// from sender to receiver.
    pub command: u32,
    /// Snapshot-header size, in bytes.
    pub header_size: u32,
    /// Plaintext chunk size, in bytes.
    pub buffer_size: u32,
    /// Ciphertext chunk capacity, in bytes.
    pub cipher_size: u32,
}

impl NegotiationHeader {
    /// Wire size in bytes.
    pub const SIZE: usize = 5 * 4;

    /// Encode the header to wire bytes.
    #[must_use]
    pub fn to_wire(self) -> [u8; Self::SIZE] {
        let mut out = [0u8; Self::SIZE];
        out[0..4].copy_from_slice(&self.magic.to_be_bytes());
        out[4..8].copy_from_slice(&self.command.to_be_bytes());
        out[8..12].copy_from_slice(&self.header_size.to_be_bytes());
        out[12..16].copy_from_slice(&self.buffer_size.to_be_bytes());
        out[16..20].copy_from_slice(&self.cipher_size.to_be_bytes());
        out
    }

    /// Decode and validate a wire-format negotiation header.
    ///
    /// # Errors
    /// [`EntropyError::Protocol`] if any field is out of range or
    /// the magic word is wrong.
    pub fn from_wire(bytes: &[u8; Self::SIZE]) -> Result<Self, EntropyError> {
        let magic = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let command = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
        let header_size = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        let buffer_size = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        let cipher_size = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
        if magic != ENTROPY_MAGIC {
            return Err(EntropyError::Protocol(format!(
                "bad magic 0x{magic:08x}, expected 0x{ENTROPY_MAGIC:08x}"
            )));
        }
        if command != ENTROPY_COMMAND_SEND {
            return Err(EntropyError::Protocol(format!(
                "unsupported command {command}"
            )));
        }
        if header_size < 8 || header_size as usize > MAX_HEADER_SIZE {
            return Err(EntropyError::Protocol(format!(
                "header_size {header_size} out of range"
            )));
        }
        if buffer_size == 0 || buffer_size as usize > MAX_BUFFER_SIZE {
            return Err(EntropyError::Protocol(format!(
                "buffer_size {buffer_size} out of range"
            )));
        }
        if cipher_size == 0 || cipher_size as usize > MAX_CIPHER_SIZE {
            return Err(EntropyError::Protocol(format!(
                "cipher_size {cipher_size} out of range"
            )));
        }
        Ok(Self {
            magic,
            command,
            header_size,
            buffer_size,
            cipher_size,
        })
    }
}

/// Per-snapshot header carried inside the variable-sized header
/// region declared by the negotiation step.
///
/// The first eight bytes are two big-endian `u32`s: the total
/// plaintext length and the encrypt flag. The remaining bytes are
/// reserved-for-future-use and must be transmitted as zero.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SnapshotHeader {
    /// Total plaintext length, in bytes.
    pub total_len: u32,
    /// `1` when chunks are AES-128-CBC encrypted, `0` otherwise.
    pub encrypt_flag: u32,
}

impl SnapshotHeader {
    /// Encode into a `header_size`-byte buffer.
    ///
    /// # Errors
    /// [`EntropyError::Config`] if `header_size` is shorter than
    /// the eight-byte fixed prefix.
    pub fn to_wire(self, header_size: usize) -> Result<Vec<u8>, EntropyError> {
        if header_size < 8 {
            return Err(EntropyError::Config(format!(
                "header_size {header_size} smaller than fixed 8-byte prefix"
            )));
        }
        let mut out = vec![0u8; header_size];
        out[0..4].copy_from_slice(&self.total_len.to_be_bytes());
        out[4..8].copy_from_slice(&self.encrypt_flag.to_be_bytes());
        Ok(out)
    }

    /// Decode from a `header_size`-byte buffer.
    ///
    /// # Errors
    /// [`EntropyError::Protocol`] if the buffer is shorter than
    /// the fixed prefix or carries an unknown `encrypt_flag`.
    pub fn from_wire(bytes: &[u8]) -> Result<Self, EntropyError> {
        if bytes.len() < 8 {
            return Err(EntropyError::Protocol(format!(
                "snapshot header too short ({} bytes)",
                bytes.len()
            )));
        }
        let total_len = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let encrypt_flag = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
        if encrypt_flag > 1 {
            return Err(EntropyError::Protocol(format!(
                "unknown encrypt_flag {encrypt_flag}"
            )));
        }
        Ok(Self {
            total_len,
            encrypt_flag,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiation_header_round_trips() {
        let hdr = NegotiationHeader {
            magic: ENTROPY_MAGIC,
            command: ENTROPY_COMMAND_SEND,
            header_size: 1024,
            buffer_size: 16 * 1024,
            cipher_size: 17 * 1024,
        };
        let wire = hdr.to_wire();
        let parsed = NegotiationHeader::from_wire(&wire).unwrap();
        assert_eq!(parsed, hdr);
    }

    #[test]
    fn negotiation_header_rejects_bad_magic() {
        let mut wire = NegotiationHeader {
            magic: 0xdead_beef,
            command: ENTROPY_COMMAND_SEND,
            header_size: 1024,
            buffer_size: 16 * 1024,
            cipher_size: 17 * 1024,
        }
        .to_wire();
        // Force-write the bad magic in case the constructor ever changes.
        wire[0..4].copy_from_slice(&0xdead_beefu32.to_be_bytes());
        let err = NegotiationHeader::from_wire(&wire).unwrap_err();
        assert!(matches!(err, EntropyError::Protocol(_)));
    }

    #[test]
    fn snapshot_header_round_trips() {
        let hdr = SnapshotHeader {
            total_len: 4096,
            encrypt_flag: 1,
        };
        let wire = hdr.to_wire(64).unwrap();
        assert_eq!(wire.len(), 64);
        for byte in &wire[8..] {
            assert_eq!(*byte, 0);
        }
        let parsed = SnapshotHeader::from_wire(&wire).unwrap();
        assert_eq!(parsed, hdr);
    }

    #[test]
    fn snapshot_header_rejects_bad_flag() {
        let mut wire = vec![0u8; 16];
        wire[4..8].copy_from_slice(&5u32.to_be_bytes());
        let err = SnapshotHeader::from_wire(&wire).unwrap_err();
        assert!(matches!(err, EntropyError::Protocol(_)));
    }

    #[test]
    fn config_validate_rejects_zero_buffer() {
        let cfg = EntropyConfig {
            key_file: PathBuf::from("k"),
            iv_file: PathBuf::from("v"),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            send_addr: None,
            peer_endpoint: "127.0.0.1:0".parse().unwrap(),
            buffer_size: 0,
            header_size: 64,
            encrypt: true,
        };
        assert!(cfg.validate().is_err());
    }
}
