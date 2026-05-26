//! Tictac AAE per-pair exchange protocol.
//!
//! Two peers run a three-phase exchange to localise the
//! divergent keys between their per-vnode merkle trees:
//!
//! ```text
//! ROOT-SYNC: peer A sends its top-level root vector;
//!            peer B replies with its own. Element-wise
//!            comparison yields the diverging time-bucket ids.
//! TREE-SYNC: for each diverging time bucket, both sides
//!            exchange the segment-hash vector and produce
//!            the diverging segment ids.
//! KEY-SYNC:  for each (time_bucket, segment) the sides
//!            exchange the [`KeyEntry`] sets and surface the
//!            symmetric difference as repair candidates.
//! ```
//!
//! # Wire format
//!
//! Every frame uses the same length-prefixed shape the
//! `crate::dynomite::entropy::send` module uses for snapshot
//! chunks (a four-byte big-endian length followed by the
//! payload), framed inside a fixed-size header so a peer can
//! decode without prior negotiation:
//!
//! ```text
//! 4 bytes BE magic       (0x71_45_41_45 "qEAE")
//! 1 byte  phase code     (1 ROOT-SYNC, 2 TREE-SYNC, 3 KEY-SYNC)
//! 1 byte  reserved       (always 0)
//! 2 bytes BE flags       (currently always 0)
//! 4 bytes BE payload_len (max 16 MiB)
//! payload_len bytes BE payload
//! ```
//!
//! Each phase has its own payload codec (see [`encode_root_sync`],
//! [`encode_tree_sync`], [`encode_key_sync`]). Failure to decode
//! a frame surfaces an [`ExchangeError`]; the scheduler treats
//! a decode error as a peer-down event and aborts the current
//! exchange (the next sweep tick will retry).

use std::io;

use crate::aae::tictac::{KeyEntry, Tree, TreeError};

/// Magic word that opens every exchange frame on the wire.
/// "qEAE" in big-endian ASCII; "q" denotes "queue", matching
/// the dnode framing convention.
pub const EXCHANGE_MAGIC: u32 = 0x7145_4145;

/// Maximum payload length permitted in a single frame. Mirrors
/// the entropy module's `MAX_BUFFER_SIZE` (5 MiB) but capped at
/// 16 MiB to match the Riak PBC framer's ceiling.
pub const MAX_PAYLOAD_LEN: u32 = 16 * 1024 * 1024;

/// Frame header size on the wire (12 bytes).
pub const FRAME_HEADER_LEN: usize = 12;

/// Phase code for `ROOT-SYNC`.
pub const PHASE_ROOT_SYNC: u8 = 1;
/// Phase code for `TREE-SYNC`.
pub const PHASE_TREE_SYNC: u8 = 2;
/// Phase code for `KEY-SYNC`.
pub const PHASE_KEY_SYNC: u8 = 3;

/// One frame on the exchange wire.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExchangeFrame {
    /// Phase code; one of [`PHASE_ROOT_SYNC`],
    /// [`PHASE_TREE_SYNC`], [`PHASE_KEY_SYNC`].
    pub phase: u8,
    /// Reserved field; always zero on the wire.
    pub reserved: u8,
    /// Per-frame flags. No flags are defined yet; the field
    /// exists so that future protocol revs can flip semantics
    /// without rebumping the magic.
    pub flags: u16,
    /// Phase-specific payload bytes.
    pub payload: Vec<u8>,
}

impl ExchangeFrame {
    /// Build a frame with the given phase and payload.
    /// `flags` and `reserved` are zero.
    #[must_use]
    pub fn new(phase: u8, payload: Vec<u8>) -> Self {
        Self {
            phase,
            reserved: 0,
            flags: 0,
            payload,
        }
    }

    /// Encode the frame into wire bytes.
    ///
    /// # Errors
    /// Returns [`ExchangeError::PayloadTooLarge`] when the
    /// payload exceeds [`MAX_PAYLOAD_LEN`].
    pub fn to_wire(&self) -> Result<Vec<u8>, ExchangeError> {
        let len = u32::try_from(self.payload.len())
            .map_err(|_| ExchangeError::PayloadTooLarge(self.payload.len()))?;
        if len > MAX_PAYLOAD_LEN {
            return Err(ExchangeError::PayloadTooLarge(self.payload.len()));
        }
        let mut out = Vec::with_capacity(FRAME_HEADER_LEN + self.payload.len());
        out.extend_from_slice(&EXCHANGE_MAGIC.to_be_bytes());
        out.push(self.phase);
        out.push(self.reserved);
        out.extend_from_slice(&self.flags.to_be_bytes());
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&self.payload);
        Ok(out)
    }

    /// Decode one frame from a contiguous byte slice. Returns
    /// the frame plus the byte count consumed.
    ///
    /// # Errors
    /// Returns [`ExchangeError::ShortFrame`] when the buffer is
    /// shorter than the header or shorter than the declared
    /// payload length, [`ExchangeError::BadMagic`] when the
    /// magic word does not match, and
    /// [`ExchangeError::PayloadTooLarge`] when the declared
    /// length exceeds [`MAX_PAYLOAD_LEN`].
    pub fn from_wire(bytes: &[u8]) -> Result<(Self, usize), ExchangeError> {
        if bytes.len() < FRAME_HEADER_LEN {
            return Err(ExchangeError::ShortFrame(bytes.len()));
        }
        let magic = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        if magic != EXCHANGE_MAGIC {
            return Err(ExchangeError::BadMagic(magic));
        }
        let phase = bytes[4];
        let reserved = bytes[5];
        let flags = u16::from_be_bytes(bytes[6..8].try_into().unwrap());
        let payload_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(ExchangeError::PayloadTooLarge(payload_len as usize));
        }
        let total = FRAME_HEADER_LEN + payload_len as usize;
        if bytes.len() < total {
            return Err(ExchangeError::ShortFrame(bytes.len()));
        }
        let payload = bytes[FRAME_HEADER_LEN..total].to_vec();
        Ok((
            Self {
                phase,
                reserved,
                flags,
                payload,
            },
            total,
        ))
    }
}

/// Encode a `ROOT-SYNC` payload: `n` repeated as 4 BE bytes,
/// followed by `n` records of `(u32 BE bucket_id, u64 BE
/// root_hash)`.
#[must_use]
pub fn encode_root_sync(roots: &[(u32, u64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + roots.len() * 12);
    let n = u32::try_from(roots.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&n.to_be_bytes());
    for (idx, hash) in roots {
        out.extend_from_slice(&idx.to_be_bytes());
        out.extend_from_slice(&hash.to_be_bytes());
    }
    out
}

/// Decode a `ROOT-SYNC` payload.
///
/// # Errors
/// [`ExchangeError::BadPayload`] when the buffer is shorter
/// than the declared count or carries a trailing partial
/// record.
pub fn decode_root_sync(bytes: &[u8]) -> Result<Vec<(u32, u64)>, ExchangeError> {
    if bytes.len() < 4 {
        return Err(ExchangeError::BadPayload("root_sync truncated".to_string()));
    }
    let n = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let needed = 4 + n * 12;
    if bytes.len() < needed {
        return Err(ExchangeError::BadPayload(format!(
            "root_sync expected {needed} bytes, got {}",
            bytes.len()
        )));
    }
    let mut out = Vec::with_capacity(n);
    let mut off = 4;
    for _ in 0..n {
        let idx = u32::from_be_bytes(bytes[off..off + 4].try_into().unwrap());
        let hash = u64::from_be_bytes(bytes[off + 4..off + 12].try_into().unwrap());
        out.push((idx, hash));
        off += 12;
    }
    Ok(out)
}

/// Encode a `TREE-SYNC` payload: `(time_bucket: u32 BE)`,
/// `(n: u32 BE)`, then `n` records of `(segment_id: u32 BE,
/// segment_hash: u64 BE)`.
#[must_use]
pub fn encode_tree_sync(time_bucket: u32, segments: &[(u32, u64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + segments.len() * 12);
    out.extend_from_slice(&time_bucket.to_be_bytes());
    let n = u32::try_from(segments.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&n.to_be_bytes());
    for (idx, hash) in segments {
        out.extend_from_slice(&idx.to_be_bytes());
        out.extend_from_slice(&hash.to_be_bytes());
    }
    out
}

/// Decode a `TREE-SYNC` payload. Returns the time bucket and
/// the segment vector.
///
/// # Errors
/// [`ExchangeError::BadPayload`] when the buffer is malformed.
pub fn decode_tree_sync(bytes: &[u8]) -> Result<(u32, Vec<(u32, u64)>), ExchangeError> {
    if bytes.len() < 8 {
        return Err(ExchangeError::BadPayload("tree_sync truncated".to_string()));
    }
    let tb = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
    let n = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let needed = 8 + n * 12;
    if bytes.len() < needed {
        return Err(ExchangeError::BadPayload(format!(
            "tree_sync expected {needed} bytes, got {}",
            bytes.len()
        )));
    }
    let mut out = Vec::with_capacity(n);
    let mut off = 8;
    for _ in 0..n {
        let idx = u32::from_be_bytes(bytes[off..off + 4].try_into().unwrap());
        let hash = u64::from_be_bytes(bytes[off + 4..off + 12].try_into().unwrap());
        out.push((idx, hash));
        off += 12;
    }
    Ok((tb, out))
}

/// Encode a `KEY-SYNC` payload: `(time_bucket: u32 BE,
/// segment: u32 BE, n: u32 BE)`, then `n` records of
/// `(bucket_len: u32 BE, bucket_bytes, key_len: u32 BE,
/// key_bytes, vclock_len: u32 BE, vclock_bytes)`.
#[must_use]
pub fn encode_key_sync(time_bucket: u32, segment: u32, entries: &[KeyEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&time_bucket.to_be_bytes());
    out.extend_from_slice(&segment.to_be_bytes());
    let n = u32::try_from(entries.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&n.to_be_bytes());
    for entry in entries {
        write_lp(&mut out, &entry.bucket);
        write_lp(&mut out, &entry.key);
        write_lp(&mut out, &entry.vclock);
    }
    out
}

fn write_lp(out: &mut Vec<u8>, bytes: &[u8]) {
    let n = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&n.to_be_bytes());
    out.extend_from_slice(bytes);
}

fn read_lp(bytes: &[u8], off: &mut usize) -> Result<Vec<u8>, ExchangeError> {
    if bytes.len() < *off + 4 {
        return Err(ExchangeError::BadPayload(
            "key_sync length-prefix truncated".to_string(),
        ));
    }
    let n = u32::from_be_bytes(bytes[*off..*off + 4].try_into().unwrap()) as usize;
    *off += 4;
    if bytes.len() < *off + n {
        return Err(ExchangeError::BadPayload(format!(
            "key_sync field expected {n} bytes, only {} remain",
            bytes.len() - *off
        )));
    }
    let v = bytes[*off..*off + n].to_vec();
    *off += n;
    Ok(v)
}

/// Decode a `KEY-SYNC` payload. Returns
/// `(time_bucket, segment, entries)`.
///
/// # Errors
/// [`ExchangeError::BadPayload`] when the buffer is malformed.
pub fn decode_key_sync(bytes: &[u8]) -> Result<(u32, u32, Vec<KeyEntry>), ExchangeError> {
    if bytes.len() < 12 {
        return Err(ExchangeError::BadPayload("key_sync truncated".to_string()));
    }
    let tb = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
    let seg = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
    let n = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let mut off = 12;
    let mut entries = Vec::with_capacity(n);
    for _ in 0..n {
        let bucket = read_lp(bytes, &mut off)?;
        let key = read_lp(bytes, &mut off)?;
        let vclock = read_lp(bytes, &mut off)?;
        entries.push(KeyEntry {
            bucket,
            key,
            vclock,
        });
    }
    Ok((tb, seg, entries))
}

/// Per-pair exchange driver. Drives `local` against a `remote`
/// view ([`PeerView`]) and returns the [`Divergence`]s the
/// repair scheduler should act on.
///
/// The driver is sync because the underlying tree operations are
/// in-process; in the wire-driven path the [`PeerView`] impl is
/// the place where the tokio I/O lives.
pub struct Exchange<'a, V: PeerView> {
    local: &'a Tree,
    remote: V,
}

/// Trait that abstracts the "remote peer" side of an exchange.
/// In tests this is satisfied by a second in-memory [`Tree`];
/// in production the impl drives the wire protocol over a tokio
/// stream.
pub trait PeerView {
    /// Phase 1: fetch the peer's `roots()` view.
    ///
    /// # Errors
    /// Implementation-defined.
    fn roots(&self) -> Result<Vec<(u32, u64)>, ExchangeError>;

    /// Phase 2: fetch the peer's `segments()` view for one
    /// time bucket.
    ///
    /// # Errors
    /// Implementation-defined.
    fn segments(&self, time_bucket: u32) -> Result<Vec<(u32, u64)>, ExchangeError>;

    /// Phase 3: fetch the peer's `keys_in_segment()` view for
    /// one (time_bucket, segment) pair.
    ///
    /// # Errors
    /// Implementation-defined.
    fn keys_in_segment(
        &self,
        time_bucket: u32,
        segment: u32,
    ) -> Result<Vec<KeyEntry>, ExchangeError>;
}

/// In-process [`PeerView`] backed by a borrowed [`Tree`]. The
/// scheduler tests wrap the "remote" tree in this view so the
/// exchange logic can be exercised without going through the
/// wire codec.
#[derive(Debug, Clone, Copy)]
pub struct LocalPeerView<'a> {
    tree: &'a Tree,
}

impl<'a> LocalPeerView<'a> {
    /// Wrap a borrowed tree.
    #[must_use]
    pub fn new(tree: &'a Tree) -> Self {
        Self { tree }
    }
}

impl PeerView for LocalPeerView<'_> {
    fn roots(&self) -> Result<Vec<(u32, u64)>, ExchangeError> {
        Ok(self.tree.roots())
    }

    fn segments(&self, time_bucket: u32) -> Result<Vec<(u32, u64)>, ExchangeError> {
        self.tree.segments(time_bucket).map_err(ExchangeError::from)
    }

    fn keys_in_segment(
        &self,
        time_bucket: u32,
        segment: u32,
    ) -> Result<Vec<KeyEntry>, ExchangeError> {
        self.tree
            .keys_in_segment(time_bucket, segment)
            .map_err(ExchangeError::from)
    }
}

/// One divergence between the local and remote trees.
///
/// `local_only` are keys present (or at a different vclock) on
/// the local side; `remote_only` are the symmetric difference
/// in the other direction. The repair scheduler decides
/// per-divergence whether to push or pull (or, in the LWW
/// happy path, simply pick the higher-versioned vclock).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Divergence {
    /// Top-level time bucket containing the divergence.
    pub time_bucket: u32,
    /// Bottom-level segment id containing the divergence.
    pub segment: u32,
    /// Entries the local tree has that the remote tree does
    /// not (or has at a different vclock).
    pub local_only: Vec<KeyEntry>,
    /// Entries the remote tree has that the local tree does
    /// not (or has at a different vclock).
    pub remote_only: Vec<KeyEntry>,
}

impl<'a, V: PeerView> Exchange<'a, V> {
    /// Build a fresh exchange.
    #[must_use]
    pub fn new(local: &'a Tree, remote: V) -> Self {
        Self { local, remote }
    }

    /// Drive the full three-phase exchange and return every
    /// divergence found.
    ///
    /// # Errors
    /// Forwards anything from the [`PeerView`].
    pub fn run(&self) -> Result<Vec<Divergence>, ExchangeError> {
        let local_roots = self.local.roots();
        let remote_roots = self.remote.roots()?;
        let dr = Tree::diverging_time_buckets(&local_roots, &remote_roots);

        let mut out = Vec::new();
        for tb in dr {
            let local_segs = self.local.segments(tb)?;
            let remote_segs = self.remote.segments(tb)?;
            let ds = Tree::diverging_segments(&local_segs, &remote_segs);
            for seg in ds {
                let local_keys = self.local.keys_in_segment(tb, seg)?;
                let remote_keys = self.remote.keys_in_segment(tb, seg)?;
                let (local_only, remote_only) = symmetric_difference(&local_keys, &remote_keys);
                if local_only.is_empty() && remote_only.is_empty() {
                    continue;
                }
                out.push(Divergence {
                    time_bucket: tb,
                    segment: seg,
                    local_only,
                    remote_only,
                });
            }
        }
        Ok(out)
    }
}

/// Compute the symmetric difference of two `KeyEntry` slices.
/// Stable; preserves input order on each side.
fn symmetric_difference(a: &[KeyEntry], b: &[KeyEntry]) -> (Vec<KeyEntry>, Vec<KeyEntry>) {
    let bset: std::collections::BTreeSet<&KeyEntry> = b.iter().collect();
    let aset: std::collections::BTreeSet<&KeyEntry> = a.iter().collect();
    let only_a = a.iter().filter(|e| !bset.contains(e)).cloned().collect();
    let only_b = b.iter().filter(|e| !aset.contains(e)).cloned().collect();
    (only_a, only_b)
}

/// Errors raised by the exchange protocol.
#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
    /// The wire frame ended before its declared length.
    #[error("short exchange frame ({0} bytes)")]
    ShortFrame(usize),
    /// The wire frame's magic word did not match
    /// [`EXCHANGE_MAGIC`].
    #[error("bad exchange magic 0x{0:08x}")]
    BadMagic(u32),
    /// A frame's declared payload length exceeds
    /// [`MAX_PAYLOAD_LEN`].
    #[error("exchange payload too large ({0} bytes)")]
    PayloadTooLarge(usize),
    /// A phase-specific payload could not be decoded.
    #[error("exchange payload: {0}")]
    BadPayload(String),
    /// The local or remote tree raised an error.
    #[error("exchange tree: {0}")]
    Tree(#[from] TreeError),
    /// I/O failure on the underlying transport.
    #[error("exchange io: {0}")]
    Io(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aae::tictac::TreeShape;

    fn shape() -> TreeShape {
        TreeShape {
            n_time_buckets: 4,
            n_segments: 32,
            time_window_seconds: 60,
        }
    }

    #[test]
    fn frame_round_trips() {
        let f = ExchangeFrame::new(PHASE_ROOT_SYNC, vec![1, 2, 3, 4]);
        let wire = f.to_wire().unwrap();
        let (parsed, n) = ExchangeFrame::from_wire(&wire).unwrap();
        assert_eq!(n, wire.len());
        assert_eq!(parsed, f);
    }

    #[test]
    fn frame_rejects_bad_magic() {
        let mut wire = ExchangeFrame::new(PHASE_ROOT_SYNC, vec![])
            .to_wire()
            .unwrap();
        wire[0..4].copy_from_slice(&0xdead_beefu32.to_be_bytes());
        assert!(matches!(
            ExchangeFrame::from_wire(&wire),
            Err(ExchangeError::BadMagic(_))
        ));
    }

    #[test]
    fn root_sync_round_trips() {
        let v = vec![(0u32, 0xaaaa_bbbb_ccccu64), (1, 0xdead), (2, 0)];
        let wire = encode_root_sync(&v);
        assert_eq!(decode_root_sync(&wire).unwrap(), v);
    }

    #[test]
    fn tree_sync_round_trips() {
        let v = vec![(0u32, 1u64), (5, 99)];
        let wire = encode_tree_sync(7, &v);
        let (tb, parsed) = decode_tree_sync(&wire).unwrap();
        assert_eq!(tb, 7);
        assert_eq!(parsed, v);
    }

    #[test]
    fn key_sync_round_trips() {
        let entries = vec![
            KeyEntry {
                bucket: b"users".to_vec(),
                key: b"alice".to_vec(),
                vclock: b"vc1".to_vec(),
            },
            KeyEntry {
                bucket: b"posts".to_vec(),
                key: b"42".to_vec(),
                vclock: b"vc99".to_vec(),
            },
        ];
        let wire = encode_key_sync(3, 17, &entries);
        let (tb, seg, parsed) = decode_key_sync(&wire).unwrap();
        assert_eq!(tb, 3);
        assert_eq!(seg, 17);
        assert_eq!(parsed, entries);
    }

    #[test]
    fn exchange_in_memory_surfaces_divergent_key() {
        let mut a = Tree::new(shape());
        let mut b = Tree::new(shape());
        for i in 0..200u32 {
            let k = format!("k{i}");
            a.insert(b"users", k.as_bytes(), b"vc1", 0);
            b.insert(b"users", k.as_bytes(), b"vc1", 0);
        }
        b.update(b"users", b"k7", b"vc1", b"vc2", 0, 0);

        let view = LocalPeerView::new(&b);
        let ex = Exchange::new(&a, view);
        let divs = ex.run().unwrap();

        // Updating one key from vc1 -> vc2 produces at most two
        // diverging segments (the old vc1-segment now has the
        // local-only entry, the new vc2-segment now has the
        // remote-only entry; if both vclocks land in the same
        // segment the divergence collapses to one). Either way
        // the symmetric difference must surface both halves of
        // the k7 update.
        assert!((1..=2).contains(&divs.len()), "unexpected divs: {divs:?}");
        let local_keys: Vec<_> = divs.iter().flat_map(|d| d.local_only.iter()).collect();
        let remote_keys: Vec<_> = divs.iter().flat_map(|d| d.remote_only.iter()).collect();
        assert!(local_keys
            .iter()
            .any(|e| e.key == b"k7" && e.vclock == b"vc1"));
        assert!(remote_keys
            .iter()
            .any(|e| e.key == b"k7" && e.vclock == b"vc2"));
    }
}
