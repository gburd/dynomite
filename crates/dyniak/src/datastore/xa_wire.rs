//! Wire codec for the cross-node XA phase messages.
//!
//! The cross-node [`crate::datastore::xa::XaCoordinator`] carries the
//! same prepare / vote / commit / rollback phases the local
//! coordinator already drives, but to a remote peer over the dnode
//! peer plane. Each phase travels as the payload of a dnode frame
//! whose [`dynomite::proto::dnode::DmsgType`] selects the phase
//! ([`XaPrepare`](dynomite::proto::dnode::DmsgType::XaPrepare),
//! [`XaVote`](dynomite::proto::dnode::DmsgType::XaVote),
//! [`XaCommit`](dynomite::proto::dnode::DmsgType::XaCommit),
//! [`XaRollback`](dynomite::proto::dnode::DmsgType::XaRollback),
//! [`XaAck`](dynomite::proto::dnode::DmsgType::XaAck)).
//!
//! The payload is a self-describing, length-prefixed byte stream
//! built with the standard library only (no external codec). Every
//! length is a big-endian `u32`; every byte string is a length
//! prefix followed by its bytes. The format is deliberately minimal:
//! a [`WireXid`] plus, for the prepare phase, the branch's writes.
//!
//! # Examples
//!
//! ```
//! use dyniak::datastore::xa_wire::{WireXid, XaPrepareMsg, XaWriteOp};
//!
//! let msg = XaPrepareMsg {
//!     xid: WireXid { format_id: 7, gtrid: b"g".to_vec(), bqual: b"east".to_vec() },
//!     env: b"east".to_vec(),
//!     writes: vec![XaWriteOp::Put {
//!         bucket: b"u".to_vec(),
//!         key: b"alice".to_vec(),
//!         value: b"a".to_vec(),
//!         indexes: vec![(b"age_int".to_vec(), b"42".to_vec())],
//!     }],
//! };
//! let bytes = msg.encode();
//! let back = XaPrepareMsg::decode(&bytes).expect("round trip");
//! assert_eq!(back, msg);
//! ```

use crate::txn::TxnOp;

/// Error raised when an XA wire payload is malformed or truncated.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum XaWireError {
    /// The buffer ended before a declared field finished.
    #[error("xa wire payload truncated")]
    Truncated,
    /// A discriminator byte was outside its documented range.
    #[error("xa wire payload: unknown tag {0}")]
    BadTag(u8),
}

/// Portable form of a [`noxu::xa::Xid`] for the wire.
///
/// `noxu`'s `Xid` is not serialisable here without pulling the
/// engine type into the wire surface, so the codec carries the three
/// raw components and the receiver rebuilds the `Xid`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireXid {
    /// XA format identifier.
    pub format_id: i32,
    /// Global transaction id (at most 64 bytes).
    pub gtrid: Vec<u8>,
    /// Branch qualifier (at most 64 bytes).
    pub bqual: Vec<u8>,
}

/// One write applied by the receiving branch during prepare.
///
/// Mirrors [`TxnOp`] but is owned by the wire module so the codec
/// does not depend on the `TxnOp` serde derive (the local coordinator
/// path uses `TxnOp` directly; the remote path lowers to this).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum XaWriteOp {
    /// Store an object, fanning `indexes` into the 2i layer.
    Put {
        /// Bucket name.
        bucket: Vec<u8>,
        /// Object key.
        key: Vec<u8>,
        /// Object value bytes.
        value: Vec<u8>,
        /// `(index_name, value)` 2i entries.
        indexes: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// Remove an object and its 2i entries.
    Delete {
        /// Bucket name.
        bucket: Vec<u8>,
        /// Object key.
        key: Vec<u8>,
    },
}

impl XaWriteOp {
    /// Lower a [`TxnOp`] into its wire form.
    #[must_use]
    pub fn from_txn_op(op: &TxnOp) -> Self {
        match op {
            TxnOp::Put {
                bucket,
                key,
                value,
                indexes,
            } => Self::Put {
                bucket: bucket.clone(),
                key: key.clone(),
                value: value.clone(),
                indexes: indexes.clone(),
            },
            TxnOp::Delete { bucket, key } => Self::Delete {
                bucket: bucket.clone(),
                key: key.clone(),
            },
        }
    }

    /// Raise the wire form back into a [`TxnOp`] the local apply path
    /// can replay.
    #[must_use]
    pub fn into_txn_op(self) -> TxnOp {
        match self {
            Self::Put {
                bucket,
                key,
                value,
                indexes,
            } => TxnOp::Put {
                bucket,
                key,
                value,
                indexes,
            },
            Self::Delete { bucket, key } => TxnOp::Delete { bucket, key },
        }
    }
}

/// Prepare-phase request: deliver a branch's writes and elicit a
/// vote. One round-trip carries start + apply + end + prepare.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XaPrepareMsg {
    /// Transaction branch identifier.
    pub xid: WireXid,
    /// Name of the environment (resource manager) that owns the
    /// branch on the receiver.
    pub env: Vec<u8>,
    /// Writes to apply before voting.
    pub writes: Vec<XaWriteOp>,
}

/// Branch vote returned for an [`XaPrepareMsg`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum XaVote {
    /// Branch prepared durably and must be committed.
    Ok,
    /// Branch performed no writes; nothing to commit or roll back.
    ReadOnly,
    /// Branch could not prepare; the coordinator must roll back.
    Abort,
}

/// Commit / rollback request body: just the branch identifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XaResolveMsg {
    /// Transaction branch identifier.
    pub xid: WireXid,
    /// Name of the environment that owns the branch.
    pub env: Vec<u8>,
}

/// Acknowledgement returned for a commit / rollback request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct XaAckMsg {
    /// True when the resolution completed (or was already complete);
    /// false when the receiver could not resolve the branch.
    pub ok: bool,
}

// --- encoding helpers (big-endian, length-prefixed) ---------------

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    let len = u32::try_from(b.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(b);
}

fn put_xid(out: &mut Vec<u8>, xid: &WireXid) {
    out.extend_from_slice(&xid.format_id.to_be_bytes());
    put_bytes(out, &xid.gtrid);
    put_bytes(out, &xid.bqual);
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], XaWireError> {
        let end = self.pos.checked_add(n).ok_or(XaWireError::Truncated)?;
        if end > self.buf.len() {
            return Err(XaWireError::Truncated);
        }
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u32(&mut self) -> Result<u32, XaWireError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i32(&mut self) -> Result<i32, XaWireError> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u8(&mut self) -> Result<u8, XaWireError> {
        Ok(self.take(1)?[0])
    }

    fn bytes(&mut self) -> Result<Vec<u8>, XaWireError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn xid(&mut self) -> Result<WireXid, XaWireError> {
        let format_id = self.i32()?;
        let gtrid = self.bytes()?;
        let bqual = self.bytes()?;
        Ok(WireXid {
            format_id,
            gtrid,
            bqual,
        })
    }
}

impl XaPrepareMsg {
    /// Serialise the prepare request to its wire payload.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        put_xid(&mut out, &self.xid);
        put_bytes(&mut out, &self.env);
        let count = u32::try_from(self.writes.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&count.to_be_bytes());
        for w in &self.writes {
            match w {
                XaWriteOp::Put {
                    bucket,
                    key,
                    value,
                    indexes,
                } => {
                    out.push(0u8);
                    put_bytes(&mut out, bucket);
                    put_bytes(&mut out, key);
                    put_bytes(&mut out, value);
                    let icount = u32::try_from(indexes.len()).unwrap_or(u32::MAX);
                    out.extend_from_slice(&icount.to_be_bytes());
                    for (name, val) in indexes {
                        put_bytes(&mut out, name);
                        put_bytes(&mut out, val);
                    }
                }
                XaWriteOp::Delete { bucket, key } => {
                    out.push(1u8);
                    put_bytes(&mut out, bucket);
                    put_bytes(&mut out, key);
                }
            }
        }
        out
    }

    /// Parse a prepare request from its wire payload.
    ///
    /// # Errors
    ///
    /// [`XaWireError::Truncated`] when the buffer is short and
    /// [`XaWireError::BadTag`] when a write discriminator is out of
    /// range.
    pub fn decode(buf: &[u8]) -> Result<Self, XaWireError> {
        let mut r = Reader::new(buf);
        let xid = r.xid()?;
        let env = r.bytes()?;
        let count = r.u32()? as usize;
        let mut writes = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            let tag = r.u8()?;
            match tag {
                0 => {
                    let bucket = r.bytes()?;
                    let key = r.bytes()?;
                    let value = r.bytes()?;
                    let icount = r.u32()? as usize;
                    let mut indexes = Vec::with_capacity(icount.min(1024));
                    for _ in 0..icount {
                        let name = r.bytes()?;
                        let val = r.bytes()?;
                        indexes.push((name, val));
                    }
                    writes.push(XaWriteOp::Put {
                        bucket,
                        key,
                        value,
                        indexes,
                    });
                }
                1 => {
                    let bucket = r.bytes()?;
                    let key = r.bytes()?;
                    writes.push(XaWriteOp::Delete { bucket, key });
                }
                other => return Err(XaWireError::BadTag(other)),
            }
        }
        Ok(Self { xid, env, writes })
    }
}

impl XaVote {
    /// Encode the vote as a single tagged byte.
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        vec![match self {
            Self::Ok => 0,
            Self::ReadOnly => 1,
            Self::Abort => 2,
        }]
    }

    /// Decode a vote byte.
    ///
    /// # Errors
    ///
    /// [`XaWireError::Truncated`] on an empty buffer and
    /// [`XaWireError::BadTag`] on an out-of-range byte.
    pub fn decode(buf: &[u8]) -> Result<Self, XaWireError> {
        match buf.first() {
            Some(0) => Ok(Self::Ok),
            Some(1) => Ok(Self::ReadOnly),
            Some(2) => Ok(Self::Abort),
            Some(other) => Err(XaWireError::BadTag(*other)),
            None => Err(XaWireError::Truncated),
        }
    }
}

impl XaResolveMsg {
    /// Serialise the commit / rollback request.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32);
        put_xid(&mut out, &self.xid);
        put_bytes(&mut out, &self.env);
        out
    }

    /// Parse a commit / rollback request.
    ///
    /// # Errors
    ///
    /// [`XaWireError::Truncated`] when the buffer is short.
    pub fn decode(buf: &[u8]) -> Result<Self, XaWireError> {
        let mut r = Reader::new(buf);
        let xid = r.xid()?;
        let env = r.bytes()?;
        Ok(Self { xid, env })
    }
}

impl XaAckMsg {
    /// Encode the ack as a single boolean byte.
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        vec![u8::from(self.ok)]
    }

    /// Decode an ack byte.
    ///
    /// # Errors
    ///
    /// [`XaWireError::Truncated`] on an empty buffer.
    pub fn decode(buf: &[u8]) -> Result<Self, XaWireError> {
        match buf.first() {
            Some(0) => Ok(Self { ok: false }),
            Some(_) => Ok(Self { ok: true }),
            None => Err(XaWireError::Truncated),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_xid() -> WireXid {
        WireXid {
            format_id: 0x6479_6e6b,
            gtrid: vec![0, 0, 0, 0, 0, 0, 0, 7],
            bqual: b"east".to_vec(),
        }
    }

    #[test]
    fn prepare_round_trips_with_mixed_writes() {
        let msg = XaPrepareMsg {
            xid: sample_xid(),
            env: b"east".to_vec(),
            writes: vec![
                XaWriteOp::Put {
                    bucket: b"u".to_vec(),
                    key: b"alice".to_vec(),
                    value: b"a".to_vec(),
                    indexes: vec![(b"age_int".to_vec(), b"42".to_vec())],
                },
                XaWriteOp::Delete {
                    bucket: b"u".to_vec(),
                    key: b"bob".to_vec(),
                },
            ],
        };
        let back = XaPrepareMsg::decode(&msg.encode()).expect("round trip");
        assert_eq!(back, msg);
    }

    #[test]
    fn vote_round_trips() {
        for v in [XaVote::Ok, XaVote::ReadOnly, XaVote::Abort] {
            assert_eq!(XaVote::decode(&v.encode()).unwrap(), v);
        }
        assert_eq!(XaVote::decode(&[]), Err(XaWireError::Truncated));
        assert_eq!(XaVote::decode(&[9]), Err(XaWireError::BadTag(9)));
    }

    #[test]
    fn resolve_round_trips() {
        let msg = XaResolveMsg {
            xid: sample_xid(),
            env: b"west".to_vec(),
        };
        let back = XaResolveMsg::decode(&msg.encode()).expect("round trip");
        assert_eq!(back, msg);
    }

    #[test]
    fn ack_round_trips() {
        for ok in [true, false] {
            assert_eq!(XaAckMsg::decode(&XaAckMsg { ok }.encode()).unwrap().ok, ok);
        }
        assert_eq!(XaAckMsg::decode(&[]), Err(XaWireError::Truncated));
    }

    #[test]
    fn truncated_prepare_is_rejected_not_panicked() {
        let full = XaPrepareMsg {
            xid: sample_xid(),
            env: b"east".to_vec(),
            writes: vec![XaWriteOp::Delete {
                bucket: b"u".to_vec(),
                key: b"bob".to_vec(),
            }],
        }
        .encode();
        // Every truncation point must produce an error, never a panic.
        for cut in 0..full.len() {
            assert!(XaPrepareMsg::decode(&full[..cut]).is_err());
        }
    }

    #[test]
    fn txn_op_lowering_round_trips() {
        let op = TxnOp::Put {
            bucket: b"u".to_vec(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            indexes: vec![(b"i".to_vec(), b"1".to_vec())],
        };
        let wire = XaWriteOp::from_txn_op(&op);
        assert_eq!(wire.into_txn_op(), op);
    }
}
