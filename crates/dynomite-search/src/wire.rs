//! On-the-wire codec for cluster-coordinated FT.SEARCH.
//!
//! This module defines the binary serialisation format the
//! coordinator uses when broadcasting an FT.SEARCH to remote
//! peers via the [`dynomite::proto::dnode::DmsgType::FtSearchReq`]
//! and [`dynomite::proto::dnode::DmsgType::FtSearchRep`] DNODE
//! frames.
//!
//! # Format choice
//!
//! The codec is a small, hand-rolled length-prefixed layout
//! that uses only the standard library, mirroring the
//! [`dynomite::proto::dnode::Handshake`] approach. Pulling in a
//! heavier serde codec was rejected because:
//!
//! * the message shapes are tiny and stable;
//! * the FT.SEARCH path is hot, so allocation and parse cost
//!   matter;
//! * keeping the codec in this module keeps the cluster-FT
//!   surface honest: any new field shows up here and is
//!   covered by the round-trip tests below.
//!
//! All multi-byte integers are little-endian. Lengths are
//! `u32` so individual fields are bounded at 4 GiB which is
//! well above any realistic vector / pattern payload.
//!
//! # Wire layout
//!
//! ## Request (`FtSearchReq`)
//!
//! ```text
//! magic(4)   = "FTQ1"
//! flags(2)   = 0
//! top_k(4)   = u32 (LE)
//! table_len  = u32 (LE)
//! table      = utf-8 bytes
//! query_tag  = u8  (0=KNN, 1=Text, 2=Regex)
//! query body = ...   (depends on tag, see below)
//! ```
//!
//! ## Reply (`FtSearchRep`)
//!
//! ```text
//! magic(4)        = "FTR1"
//! flags(2)        = 0
//! timed_out(1)    = 0|1
//! hit_count(4)    = u32 (LE)
//! repeat hit_count times:
//!     doc_id_len  = u32 (LE)
//!     doc_id      = bytes
//!     score       = f32 (LE)
//! ```
//!
//! Tag bodies:
//!
//! ```text
//! KNN:    field_len(4) field_utf8 bytes_len(4) vector_bytes
//!         ef_present(1) [ef(4)]
//! Text:   field_len(4) field_utf8 query_len(4) query_bytes
//! Regex:  field_len(4) field_utf8 pattern_len(4) pattern_utf8
//!         max_errors(2)
//! ```

use std::convert::TryFrom;

use thiserror::Error;

use super::query_fsm::{BroadcastRequest, HitWithScore, PeerReply, SerializedQuery};

/// Magic literal that opens every encoded
/// [`BroadcastRequest`] payload.
pub const REQ_MAGIC: [u8; 4] = *b"FTQ1";

/// Magic literal that opens every encoded
/// [`PeerReply`] payload.
pub const REP_MAGIC: [u8; 4] = *b"FTR1";

const TAG_KNN: u8 = 0;
const TAG_TEXT: u8 = 1;
const TAG_REGEX: u8 = 2;

/// Errors raised by the cluster-FT codec.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum CodecError {
    /// Payload was shorter than required by the layout.
    #[error("FT search payload truncated")]
    Truncated,
    /// Payload header magic did not match.
    #[error("FT search payload bad magic")]
    BadMagic,
    /// Payload reserved-flags field was non-zero.
    #[error("FT search payload bad flags")]
    BadFlags,
    /// Encoded length exceeds the remaining slice.
    #[error("FT search field length out of range")]
    LengthOverflow,
    /// Embedded UTF-8 string did not parse.
    #[error("FT search field not utf-8")]
    BadUtf8,
    /// Query body tag byte is not one of the known variants.
    #[error("FT search unknown query tag {0}")]
    BadTag(u8),
}

/// Encode a [`BroadcastRequest`] to a binary payload suitable
/// for the [`dynomite::proto::dnode::DmsgType::FtSearchReq`]
/// DNODE frame.
///
/// # Examples
///
/// ```
/// use dynomite_search::query_fsm::{BroadcastRequest, SerializedQuery};
/// use dynomite_search::wire::{decode_request, encode_request};
///
/// let req = BroadcastRequest {
///     table: "idx".into(),
///     query: SerializedQuery::Text {
///         field: "body".into(),
///         query: b"foo".to_vec(),
///     },
///     top_k: 10,
/// };
/// let bytes = encode_request(&req);
/// let back = decode_request(&bytes).unwrap();
/// assert_eq!(req, back);
/// ```
#[must_use]
pub fn encode_request(req: &BroadcastRequest) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(&REQ_MAGIC);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&req.top_k.to_le_bytes());
    write_bytes(&mut out, req.table.as_bytes());
    match &req.query {
        SerializedQuery::Knn {
            vector_field,
            vector_bytes,
            ef,
        } => {
            out.push(TAG_KNN);
            write_bytes(&mut out, vector_field.as_bytes());
            write_bytes(&mut out, vector_bytes);
            match ef {
                Some(value) => {
                    out.push(1);
                    out.extend_from_slice(&value.to_le_bytes());
                }
                None => out.push(0),
            }
        }
        SerializedQuery::Text { field, query } => {
            out.push(TAG_TEXT);
            write_bytes(&mut out, field.as_bytes());
            write_bytes(&mut out, query);
        }
        SerializedQuery::Regex {
            field,
            pattern,
            max_errors,
        } => {
            out.push(TAG_REGEX);
            write_bytes(&mut out, field.as_bytes());
            write_bytes(&mut out, pattern.as_bytes());
            out.extend_from_slice(&max_errors.to_le_bytes());
        }
    }
    out
}

/// Decode a [`BroadcastRequest`] previously produced by
/// [`encode_request`].
///
/// # Errors
///
/// Returns [`CodecError`] when the payload is truncated, the
/// magic header is wrong, or any embedded string is not valid
/// UTF-8.
pub fn decode_request(bytes: &[u8]) -> Result<BroadcastRequest, CodecError> {
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.take_array::<4>()?;
    if magic != REQ_MAGIC {
        return Err(CodecError::BadMagic);
    }
    let flags = cursor.take_u16()?;
    if flags != 0 {
        return Err(CodecError::BadFlags);
    }
    let top_k = cursor.take_u32()?;
    let table_bytes = cursor.take_bytes()?.to_vec();
    let table = String::from_utf8(table_bytes).map_err(|_| CodecError::BadUtf8)?;
    let tag = cursor.take_u8()?;
    let query = match tag {
        TAG_KNN => {
            let field_bytes = cursor.take_bytes()?.to_vec();
            let vector_field = String::from_utf8(field_bytes).map_err(|_| CodecError::BadUtf8)?;
            let vector_bytes = cursor.take_bytes()?.to_vec();
            let ef_present = cursor.take_u8()?;
            let ef = match ef_present {
                0 => None,
                1 => Some(cursor.take_u32()?),
                _ => return Err(CodecError::BadFlags),
            };
            SerializedQuery::Knn {
                vector_field,
                vector_bytes,
                ef,
            }
        }
        TAG_TEXT => {
            let field_bytes = cursor.take_bytes()?.to_vec();
            let field = String::from_utf8(field_bytes).map_err(|_| CodecError::BadUtf8)?;
            let query = cursor.take_bytes()?.to_vec();
            SerializedQuery::Text { field, query }
        }
        TAG_REGEX => {
            let field_bytes = cursor.take_bytes()?.to_vec();
            let field = String::from_utf8(field_bytes).map_err(|_| CodecError::BadUtf8)?;
            let pattern_bytes = cursor.take_bytes()?.to_vec();
            let pattern = String::from_utf8(pattern_bytes).map_err(|_| CodecError::BadUtf8)?;
            let max_errors = cursor.take_u16()?;
            SerializedQuery::Regex {
                field,
                pattern,
                max_errors,
            }
        }
        other => return Err(CodecError::BadTag(other)),
    };
    Ok(BroadcastRequest {
        table,
        query,
        top_k,
    })
}

/// Encode a [`PeerReply`] (one peer's per-peer top-K) for the
/// [`dynomite::proto::dnode::DmsgType::FtSearchRep`] DNODE frame.
///
/// # Examples
///
/// ```
/// use dynomite_search::query_fsm::{HitWithScore, PeerReply};
/// use dynomite_search::wire::{decode_reply, encode_reply};
///
/// let reply = PeerReply {
///     hits: vec![HitWithScore {
///         doc_id: b"key:1".to_vec(),
///         score: 0.25,
///     }],
///     timed_out: false,
/// };
/// let bytes = encode_reply(&reply);
/// let back = decode_reply(&bytes).unwrap();
/// assert_eq!(reply, back);
/// ```
#[must_use]
pub fn encode_reply(reply: &PeerReply) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + reply.hits.len() * 24);
    out.extend_from_slice(&REP_MAGIC);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.push(u8::from(reply.timed_out));
    let count = u32::try_from(reply.hits.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&count.to_le_bytes());
    let max = count as usize;
    for hit in reply.hits.iter().take(max) {
        write_bytes(&mut out, &hit.doc_id);
        out.extend_from_slice(&hit.score.to_le_bytes());
    }
    out
}

/// Decode a [`PeerReply`] previously produced by
/// [`encode_reply`].
///
/// # Errors
///
/// Returns [`CodecError`] when the payload is truncated or the
/// magic header is wrong.
pub fn decode_reply(bytes: &[u8]) -> Result<PeerReply, CodecError> {
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.take_array::<4>()?;
    if magic != REP_MAGIC {
        return Err(CodecError::BadMagic);
    }
    let flags = cursor.take_u16()?;
    if flags != 0 {
        return Err(CodecError::BadFlags);
    }
    let timed_out_byte = cursor.take_u8()?;
    if timed_out_byte > 1 {
        return Err(CodecError::BadFlags);
    }
    let timed_out = timed_out_byte == 1;
    let count = cursor.take_u32()?;
    let count_usize = usize::try_from(count).map_err(|_| CodecError::LengthOverflow)?;
    let mut hits: Vec<HitWithScore> = Vec::with_capacity(count_usize.min(64));
    for _ in 0..count_usize {
        let doc_id = cursor.take_bytes()?.to_vec();
        let score = cursor.take_f32()?;
        hits.push(HitWithScore { doc_id, score });
    }
    Ok(PeerReply { hits, timed_out })
}

// ---- helpers -----------------------------------------------------------

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_le_bytes());
    let max = len as usize;
    out.extend_from_slice(&bytes[..bytes.len().min(max)]);
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn require(&self, want: usize) -> Result<(), CodecError> {
        if self
            .pos
            .checked_add(want)
            .is_none_or(|end| end > self.buf.len())
        {
            return Err(CodecError::Truncated);
        }
        Ok(())
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        self.require(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(&self.buf[self.pos..self.pos + N]);
        self.pos += N;
        Ok(out)
    }

    fn take_u8(&mut self) -> Result<u8, CodecError> {
        self.require(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn take_u16(&mut self) -> Result<u16, CodecError> {
        let bytes = self.take_array::<2>()?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn take_u32(&mut self) -> Result<u32, CodecError> {
        let bytes = self.take_array::<4>()?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn take_f32(&mut self) -> Result<f32, CodecError> {
        let bytes = self.take_array::<4>()?;
        Ok(f32::from_le_bytes(bytes))
    }

    fn take_bytes(&mut self) -> Result<&'a [u8], CodecError> {
        let len = self.take_u32()? as usize;
        self.require(len)?;
        let out = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn knn_request() -> BroadcastRequest {
        BroadcastRequest {
            table: "ix".into(),
            query: SerializedQuery::Knn {
                vector_field: "v".into(),
                vector_bytes: vec![0x00, 0x01, 0x02, 0x03],
                ef: Some(64),
            },
            top_k: 5,
        }
    }

    #[test]
    fn knn_round_trip() {
        let req = knn_request();
        let bytes = encode_request(&req);
        let back = decode_request(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn knn_round_trip_no_ef() {
        let mut req = knn_request();
        if let SerializedQuery::Knn { ef, .. } = &mut req.query {
            *ef = None;
        }
        let bytes = encode_request(&req);
        let back = decode_request(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn text_round_trip() {
        let req = BroadcastRequest {
            table: "idx".into(),
            query: SerializedQuery::Text {
                field: "body".into(),
                query: b"foo bar".to_vec(),
            },
            top_k: 3,
        };
        let bytes = encode_request(&req);
        assert_eq!(decode_request(&bytes).unwrap(), req);
    }

    #[test]
    fn regex_round_trip() {
        let req = BroadcastRequest {
            table: "idx".into(),
            query: SerializedQuery::Regex {
                field: "body".into(),
                pattern: "ab.*c".into(),
                max_errors: 2,
            },
            top_k: 7,
        };
        let bytes = encode_request(&req);
        assert_eq!(decode_request(&bytes).unwrap(), req);
    }

    #[test]
    fn reply_round_trip() {
        let reply = PeerReply {
            hits: vec![
                HitWithScore {
                    doc_id: b"a".to_vec(),
                    score: 0.10,
                },
                HitWithScore {
                    doc_id: b"longer:doc:id".to_vec(),
                    score: 0.42,
                },
            ],
            timed_out: false,
        };
        let bytes = encode_reply(&reply);
        let back = decode_reply(&bytes).unwrap();
        assert_eq!(reply, back);
    }

    #[test]
    fn reply_with_timed_out_flag() {
        let reply = PeerReply {
            hits: Vec::new(),
            timed_out: true,
        };
        let bytes = encode_reply(&reply);
        let back = decode_reply(&bytes).unwrap();
        assert!(back.timed_out);
        assert!(back.hits.is_empty());
    }

    #[test]
    fn reply_with_no_hits() {
        let reply = PeerReply {
            hits: Vec::new(),
            timed_out: false,
        };
        let bytes = encode_reply(&reply);
        let back = decode_reply(&bytes).unwrap();
        assert_eq!(reply, back);
    }

    #[test]
    fn truncated_request_rejected() {
        let req = knn_request();
        let bytes = encode_request(&req);
        for n in 0..bytes.len() {
            assert_eq!(decode_request(&bytes[..n]), Err(CodecError::Truncated));
        }
    }

    #[test]
    fn bad_magic_rejected() {
        let bytes = vec![b'X'; 32];
        assert_eq!(decode_request(&bytes).unwrap_err(), CodecError::BadMagic);
        assert_eq!(decode_reply(&bytes).unwrap_err(), CodecError::BadMagic);
    }

    #[test]
    fn bad_tag_rejected() {
        let mut bytes = encode_request(&knn_request());
        // Locate and overwrite the tag byte (right after table
        // bytes). Re-derive its index from the layout: 4 magic +
        // 2 flags + 4 top_k + 4 table_len + table_len bytes.
        let table_len_offset = 4 + 2 + 4;
        let table_len = u32::from_le_bytes(
            bytes[table_len_offset..table_len_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let tag_offset = table_len_offset + 4 + table_len;
        bytes[tag_offset] = 0xff;
        assert_eq!(
            decode_request(&bytes).unwrap_err(),
            CodecError::BadTag(0xff)
        );
    }

    #[test]
    fn non_zero_flags_rejected() {
        let mut bytes = encode_request(&knn_request());
        bytes[4] = 0x01;
        assert_eq!(decode_request(&bytes).unwrap_err(), CodecError::BadFlags);
    }
}
