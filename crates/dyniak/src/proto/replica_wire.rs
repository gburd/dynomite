//! Wire codec for [`PeerOp`] cross-node replica ops.
//!
//! A [`PeerOp`] travels between nodes as the payload of a
//! [`DmsgType::RiakReplica`](dynomite::proto::dnode::DmsgType::RiakReplica)
//! dnode frame. The encoding is deliberately compact and
//! self-describing:
//!
//! ```text
//! op-kind(1)  0 = Put, 1 = Get, 2 = Del
//! bucket_type (u32 be length prefix + bytes)
//! bucket      (u32 be length prefix + bytes)
//! key         (u32 be length prefix + bytes)
//! value       (u32 be length prefix + bytes)   -- Put only
//! ```
//!
//! All multi-byte integers are big-endian. The codec uses only
//! the standard library. Decoding is total: any truncation or
//! unknown op-kind surfaces a [`ReplicaWireError`] rather than
//! panicking, so a corrupt peer frame cannot crash the receive
//! loop.
//!
//! # Examples
//!
//! ```
//! use dyniak::proto::replica_wire::{decode_peer_op, encode_peer_op};
//! use dyniak::router::PeerOp;
//!
//! let op = PeerOp::Put {
//!     bucket_type: b"default".to_vec(),
//!     bucket: b"users".to_vec(),
//!     key: b"alice".to_vec(),
//!     value: b"hello".to_vec(),
//! };
//! let bytes = encode_peer_op(&op);
//! assert_eq!(decode_peer_op(&bytes).unwrap(), op);
//! ```

use crate::router::PeerOp;

/// Op-kind discriminator for a [`PeerOp::Put`] frame.
const KIND_PUT: u8 = 0;
/// Op-kind discriminator for a [`PeerOp::Get`] frame.
const KIND_GET: u8 = 1;
/// Op-kind discriminator for a [`PeerOp::Del`] frame.
const KIND_DEL: u8 = 2;

/// Error decoding a [`PeerOp`] from its wire payload.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ReplicaWireError {
    /// The buffer ended before a declared field was fully read.
    #[error("replica wire: truncated payload")]
    Truncated,
    /// The op-kind byte was not one of the known discriminators.
    #[error("replica wire: unknown op-kind {0}")]
    BadKind(u8),
    /// Trailing bytes remained after decoding a complete op.
    #[error("replica wire: {0} trailing bytes after op")]
    Trailing(usize),
}

/// Serialise a [`PeerOp`] to its wire payload.
#[must_use]
pub fn encode_peer_op(op: &PeerOp) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    match op {
        PeerOp::Put {
            bucket_type,
            bucket,
            key,
            value,
        } => {
            out.push(KIND_PUT);
            put_bytes(&mut out, bucket_type);
            put_bytes(&mut out, bucket);
            put_bytes(&mut out, key);
            put_bytes(&mut out, value);
        }
        PeerOp::Get {
            bucket_type,
            bucket,
            key,
        } => {
            out.push(KIND_GET);
            put_bytes(&mut out, bucket_type);
            put_bytes(&mut out, bucket);
            put_bytes(&mut out, key);
        }
        PeerOp::Del {
            bucket_type,
            bucket,
            key,
        } => {
            out.push(KIND_DEL);
            put_bytes(&mut out, bucket_type);
            put_bytes(&mut out, bucket);
            put_bytes(&mut out, key);
        }
    }
    out
}

/// Parse a [`PeerOp`] from its wire payload.
///
/// # Errors
///
/// [`ReplicaWireError::Truncated`] when the buffer is short of a
/// declared field, [`ReplicaWireError::BadKind`] when the op-kind
/// byte is unknown, and [`ReplicaWireError::Trailing`] when bytes
/// remain after a complete op (a malformed frame).
pub fn decode_peer_op(buf: &[u8]) -> Result<PeerOp, ReplicaWireError> {
    let mut r = Reader::new(buf);
    let kind = r.u8()?;
    let op = match kind {
        KIND_PUT => {
            let bucket_type = r.bytes()?;
            let bucket = r.bytes()?;
            let key = r.bytes()?;
            let value = r.bytes()?;
            PeerOp::Put {
                bucket_type,
                bucket,
                key,
                value,
            }
        }
        KIND_GET => {
            let bucket_type = r.bytes()?;
            let bucket = r.bytes()?;
            let key = r.bytes()?;
            PeerOp::Get {
                bucket_type,
                bucket,
                key,
            }
        }
        KIND_DEL => {
            let bucket_type = r.bytes()?;
            let bucket = r.bytes()?;
            let key = r.bytes()?;
            PeerOp::Del {
                bucket_type,
                bucket,
                key,
            }
        }
        other => return Err(ReplicaWireError::BadKind(other)),
    };
    let rest = r.remaining();
    if rest != 0 {
        return Err(ReplicaWireError::Trailing(rest));
    }
    Ok(op)
}

/// Append a `u32` big-endian length prefix followed by `bytes`.
fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Cursor over a wire payload that reads length-prefixed fields
/// without ever indexing past the end of the buffer.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn u8(&mut self) -> Result<u8, ReplicaWireError> {
        let b = *self.buf.get(self.pos).ok_or(ReplicaWireError::Truncated)?;
        self.pos += 1;
        Ok(b)
    }

    fn u32(&mut self) -> Result<u32, ReplicaWireError> {
        let end = self.pos.checked_add(4).ok_or(ReplicaWireError::Truncated)?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(ReplicaWireError::Truncated)?;
        let n = u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]);
        self.pos = end;
        Ok(n)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, ReplicaWireError> {
        let len = self.u32()? as usize;
        let end = self
            .pos
            .checked_add(len)
            .ok_or(ReplicaWireError::Truncated)?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(ReplicaWireError::Truncated)?;
        self.pos = end;
        Ok(slice.to_vec())
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_put() -> PeerOp {
        PeerOp::Put {
            bucket_type: b"default".to_vec(),
            bucket: b"users".to_vec(),
            key: b"alice".to_vec(),
            value: b"payload-bytes".to_vec(),
        }
    }

    fn sample_get() -> PeerOp {
        PeerOp::Get {
            bucket_type: b"maps".to_vec(),
            bucket: b"carts".to_vec(),
            key: b"cart-9".to_vec(),
        }
    }

    fn sample_del() -> PeerOp {
        PeerOp::Del {
            bucket_type: Vec::new(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
        }
    }

    #[test]
    fn round_trip_put() {
        let op = sample_put();
        assert_eq!(decode_peer_op(&encode_peer_op(&op)).unwrap(), op);
    }

    #[test]
    fn round_trip_get() {
        let op = sample_get();
        assert_eq!(decode_peer_op(&encode_peer_op(&op)).unwrap(), op);
    }

    #[test]
    fn round_trip_del() {
        let op = sample_del();
        assert_eq!(decode_peer_op(&encode_peer_op(&op)).unwrap(), op);
    }

    #[test]
    fn round_trip_empty_value_put() {
        let op = PeerOp::Put {
            bucket_type: b"default".to_vec(),
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
            value: Vec::new(),
        };
        assert_eq!(decode_peer_op(&encode_peer_op(&op)).unwrap(), op);
    }

    #[test]
    fn truncated_at_every_prefix_is_rejected() {
        // Every strict prefix of a valid Put frame must decode to
        // an error, never a panic and never a partial op.
        for op in [sample_put(), sample_get(), sample_del()] {
            let full = encode_peer_op(&op);
            for cut in 0..full.len() {
                let err = decode_peer_op(&full[..cut]);
                assert!(
                    err.is_err(),
                    "prefix of len {cut} of {op:?} decoded to {err:?}"
                );
            }
            // The full frame decodes cleanly.
            assert_eq!(decode_peer_op(&full).unwrap(), op);
        }
    }

    #[test]
    fn empty_buffer_is_truncated() {
        assert_eq!(decode_peer_op(&[]), Err(ReplicaWireError::Truncated));
    }

    #[test]
    fn unknown_kind_is_rejected() {
        // Op-kind 7 is not a known discriminator.
        assert_eq!(decode_peer_op(&[7]), Err(ReplicaWireError::BadKind(7)));
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = encode_peer_op(&sample_del());
        bytes.push(0xFF);
        assert_eq!(decode_peer_op(&bytes), Err(ReplicaWireError::Trailing(1)));
    }

    #[test]
    fn oversized_length_prefix_does_not_allocate_past_buffer() {
        // A length prefix that claims more bytes than the buffer
        // holds must be a clean Truncated error, not an OOM.
        let mut bytes = vec![KIND_DEL];
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(decode_peer_op(&bytes), Err(ReplicaWireError::Truncated));
    }
}
