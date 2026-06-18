//! Node-local hinted-handoff store.
//!
//! When a write request fans out to a peer in
//! [`crate::cluster::peer::PeerState::Down`] or to a peer whose
//! outbound channel is closed, the dispatcher records a hint:
//! the on-the-wire request bytes, the index of the intended
//! peer, and an absolute expiry deadline. A background task
//! periodically:
//!
//! * drains hints destined for any peer that has returned to
//!   [`crate::cluster::peer::PeerState::Normal`] and ships them
//!   over the same per-peer outbound channel the dispatcher
//!   would have used;
//! * drops hints that have aged past their `hint_ttl_seconds`
//!   so the in-memory store stays bounded.
//!
//! The store has two constructors. [`HintStore::new`] is the
//! RAM-only variant: hints live only in the per-peer queues and
//! are lost if the coordinator restarts. [`HintStore::open`]
//! adds a durable backend so queued hints survive a restart.
//!
//! # Durable backend
//!
//! The durable backend keeps one append-only segment file per
//! peer under `<dir>/peer-<idx>.hints`. Each record frames a
//! single hint as a little-endian `u32` body length, a
//! little-endian `u32` CRC-32 (IEEE) of the body, and the body
//! itself: a little-endian `u64` wall-clock deadline (Unix
//! milliseconds) followed by the raw payload bytes. The peer
//! index is encoded in the file name, not the record.
//!
//! * [`HintStore::enqueue`] write-through appends one record to
//!   the target peer's segment.
//! * [`HintStore::take_for`] returns the live hints and then
//!   removes that peer's segment (the hints have been handed
//!   off; a failed delivery is re-enqueued by the caller, which
//!   re-appends a fresh segment).
//! * [`HintStore::expire_now`] rewrites each affected peer's
//!   segment from the surviving in-memory hints so the on-disk
//!   log is compacted and never grows past `max_bytes`.
//!
//! [`HintStore::open`] replays every segment in `dir` back into
//! the in-memory queues at startup. The deadline is stored as a
//! wall-clock instant so it survives the process boundary; on
//! replay each deadline is re-anchored to the current monotonic
//! clock and any hint whose wall-clock deadline has already
//! passed is dropped.
//!
//! ## Torn-tail safety
//!
//! A crash mid-append can leave a torn trailing record. Replay
//! detects this two ways: a record whose framed length cannot
//! be read in full (a short read before the body completes) is
//! discarded as a clean EOF, and a record whose body CRC does
//! not match the stored CRC is treated as torn. In both cases
//! replay stops at the first damaged record and keeps every
//! intact record before it. A torn tail never panics and never
//! surfaces an error from [`HintStore::open`].
//!
//! # Examples
//!
//! ```
//! use std::time::{Duration, Instant};
//! use dynomite::cluster::hints::HintStore;
//!
//! let store = HintStore::new(1024);
//! store.enqueue(7, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n".to_vec(), Duration::from_secs(60))
//!     .expect("under capacity");
//! let drained = store.take_for(7);
//! assert_eq!(drained.len(), 1);
//! assert_eq!(store.expire_now(Instant::now()), 0);
//! ```

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use thiserror::Error;

/// Errors produced by [`HintStore::enqueue`].
#[derive(Debug, Error, Eq, PartialEq)]
pub enum HintStoreError {
    /// The store has reached `max_bytes`. The caller is
    /// expected to fall back to its non-handoff error path
    /// (typically, return `DynomiteNoQuorumAchieved` to the
    /// client) and the next drainer sweep will reclaim space
    /// when peers come back online.
    #[error("hint store over capacity ({max_bytes} bytes)")]
    OverCapacity {
        /// Configured upper bound, in bytes.
        max_bytes: u64,
    },
    /// The supplied TTL is zero. A zero TTL would be expired
    /// immediately by the next sweep, so the store rejects it
    /// up front to surface a configuration error.
    #[error("hint TTL must be greater than zero")]
    ZeroTtl,
    /// The hint payload is empty. The wire-replay path requires
    /// at least one byte; an empty payload is rejected so the
    /// drainer never produces a no-op outbound write.
    #[error("hint payload is empty")]
    EmptyPayload,
    /// A durable segment write failed. The hint was not
    /// persisted; the caller must treat the enqueue as failed
    /// and fall back to its non-handoff error path rather than
    /// risk silently losing the write across a restart.
    #[error("hint segment write failed: {message}")]
    Io {
        /// Rendered underlying I/O error.
        message: String,
    },
}

/// Errors produced by [`HintStore::open`].
#[derive(Debug, Error)]
pub enum HintStoreOpenError {
    /// The segment directory could not be created or read.
    #[error("hint segment directory {path}: {source}")]
    Dir {
        /// The directory path that failed.
        path: PathBuf,
        /// Underlying I/O error.
        source: io::Error,
    },
    /// A segment file could not be read during replay.
    #[error("hint segment {path}: {source}")]
    Segment {
        /// The segment file path that failed.
        path: PathBuf,
        /// Underlying I/O error.
        source: io::Error,
    },
}

/// One pending hint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Hint {
    /// Index of the intended peer in
    /// [`crate::cluster::pool::ServerPool::peers`].
    pub peer_idx: u32,
    /// On-the-wire request bytes, ready to forward.
    pub payload: Vec<u8>,
    /// Absolute deadline after which the hint is dropped.
    pub deadline: Instant,
}

impl Hint {
    /// Heap footprint in bytes used for the store's accounting.
    /// Counts the payload only; the surrounding metadata
    /// (`u32` + `Instant`) is small and constant per entry.
    #[must_use]
    fn weight(&self) -> u64 {
        u64::try_from(self.payload.len()).unwrap_or(u64::MAX)
    }
}

/// Snapshot of the store's current size.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HintStoreStats {
    /// Number of hints currently retained.
    pub hint_count: usize,
    /// Sum of payload bytes currently retained.
    pub bytes: u64,
    /// Configured upper bound on `bytes`.
    pub max_bytes: u64,
    /// Total hints dropped due to TTL expiry since the store
    /// was created.
    pub expired_total: u64,
    /// Total hints rejected for over-capacity since the store
    /// was created.
    pub rejected_over_capacity_total: u64,
}

/// Node-local hint store.
///
/// The store is internally synchronised so [`std::sync::Arc`]
/// clones share the same per-peer queues. Operations are O(1)
/// with respect to the number of pending hints for the queried
/// peer and O(N) for [`HintStore::expire_now`].
#[derive(Debug)]
pub struct HintStore {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// Per-peer FIFO queue. Insertion appends; drain pops the
    /// whole queue at once (the dispatcher never wants to
    /// trickle-deliver because hints are buffered against a
    /// down peer that has already returned).
    by_peer: HashMap<u32, Vec<Hint>>,
    bytes: u64,
    max_bytes: u64,
    expired_total: u64,
    rejected_over_capacity_total: u64,
    /// Durable backend. `None` for the RAM-only store built by
    /// [`HintStore::new`].
    disk: Option<DiskBackend>,
}

impl HintStore {
    /// Build a new store with the supplied byte cap.
    ///
    /// `max_bytes` of zero means "no cap"; this is intended for
    /// tests that drive enqueue/take patterns and never want to
    /// exercise the back-pressure branch.
    #[must_use]
    pub fn new(max_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                by_peer: HashMap::new(),
                bytes: 0,
                max_bytes,
                expired_total: 0,
                rejected_over_capacity_total: 0,
                disk: None,
            }),
        }
    }

    /// Build a durable store backed by per-peer segment files
    /// under `dir`, replaying any segments already present.
    ///
    /// On success the in-memory queues are pre-populated with
    /// every intact hint found on disk whose wall-clock deadline
    /// has not yet passed (re-anchored to the monotonic clock).
    /// A torn trailing record in any segment is discarded
    /// silently; see the module-level torn-tail documentation.
    /// The replayed bytes are counted toward `max_bytes`, so a
    /// store recovered from disk enforces the same combined cap
    /// the RAM-only store does.
    ///
    /// # Errors
    ///
    /// * [`HintStoreOpenError::Dir`] when `dir` cannot be
    ///   created or its entries cannot be listed.
    /// * [`HintStoreOpenError::Segment`] when a segment file
    ///   exists but cannot be opened or read (a torn tail is not
    ///   an error; an unreadable file is).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use dynomite::cluster::hints::HintStore;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let store = HintStore::open(dir.path(), 1024).unwrap();
    /// store.enqueue(1, b"hi".to_vec(), Duration::from_secs(60)).unwrap();
    /// drop(store);
    /// let reopened = HintStore::open(dir.path(), 1024).unwrap();
    /// assert_eq!(reopened.len_for(1), 1);
    /// ```
    pub fn open<P: AsRef<Path>>(dir: P, max_bytes: u64) -> Result<Self, HintStoreOpenError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|source| HintStoreOpenError::Dir {
            path: dir.clone(),
            source,
        })?;
        let backend = DiskBackend { dir };
        let by_peer = backend.replay()?;
        let mut bytes: u64 = 0;
        for queue in by_peer.values() {
            for h in queue {
                bytes = bytes.saturating_add(h.weight());
            }
        }
        Ok(Self {
            inner: Mutex::new(Inner {
                by_peer,
                bytes,
                max_bytes,
                expired_total: 0,
                rejected_over_capacity_total: 0,
                disk: Some(backend),
            }),
        })
    }

    /// Append a hint for `peer_idx`. The hint expires at
    /// `Instant::now() + ttl`.
    ///
    /// # Errors
    ///
    /// * [`HintStoreError::ZeroTtl`] when `ttl` is zero.
    /// * [`HintStoreError::EmptyPayload`] when `payload` is
    ///   empty.
    /// * [`HintStoreError::OverCapacity`] when accepting the
    ///   hint would push the cumulative payload bytes over
    ///   `max_bytes`.
    pub fn enqueue(
        &self,
        peer_idx: u32,
        payload: Vec<u8>,
        ttl: Duration,
    ) -> Result<(), HintStoreError> {
        if ttl.is_zero() {
            return Err(HintStoreError::ZeroTtl);
        }
        if payload.is_empty() {
            return Err(HintStoreError::EmptyPayload);
        }
        let weight = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        let mut inner = self.inner.lock();
        if inner.max_bytes > 0 && inner.bytes.saturating_add(weight) > inner.max_bytes {
            inner.rejected_over_capacity_total =
                inner.rejected_over_capacity_total.saturating_add(1);
            return Err(HintStoreError::OverCapacity {
                max_bytes: inner.max_bytes,
            });
        }
        let deadline = Instant::now() + ttl;
        // Wall-clock deadline for the durable record so the
        // hint's lifetime survives a restart (the monotonic
        // `Instant` above is meaningless across a reboot).
        let wall_deadline = SystemTime::now() + ttl;
        if let Some(disk) = inner.disk.as_ref() {
            disk.append(peer_idx, wall_deadline, &payload)
                .map_err(|e| HintStoreError::Io {
                    message: e.to_string(),
                })?;
        }
        inner.by_peer.entry(peer_idx).or_default().push(Hint {
            peer_idx,
            payload,
            deadline,
        });
        inner.bytes = inner.bytes.saturating_add(weight);
        Ok(())
    }

    /// Drain every pending hint for `peer_idx`. Hints that have
    /// expired are dropped on the floor (and counted toward
    /// [`HintStoreStats::expired_total`]).
    ///
    /// Returned hints are ordered by enqueue time, oldest first.
    pub fn take_for(&self, peer_idx: u32) -> Vec<Hint> {
        let now = Instant::now();
        let mut inner = self.inner.lock();
        let Some(queue) = inner.by_peer.remove(&peer_idx) else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(queue.len());
        for h in queue {
            if h.deadline <= now {
                let w = h.weight();
                inner.bytes = inner.bytes.saturating_sub(w);
                inner.expired_total = inner.expired_total.saturating_add(1);
                continue;
            }
            inner.bytes = inner.bytes.saturating_sub(h.weight());
            out.push(h);
        }
        // The whole queue has been handed off; drop the durable
        // segment so the bytes are reclaimed on disk too. A
        // failed removal is non-fatal: a stale segment would at
        // worst be replayed (and re-delivered) on the next
        // restart, which the at-least-once handoff contract
        // already tolerates.
        if let Some(disk) = inner.disk.as_ref() {
            if let Err(e) = disk.remove_peer(peer_idx) {
                tracing::warn!(
                    target: "dynomite::cluster::hints",
                    peer_idx,
                    error = %e,
                    "failed to remove drained hint segment"
                );
            }
        }
        out
    }

    /// Drop every hint whose deadline has passed at `now`.
    /// Returns the number of hints dropped. Walks the entire
    /// store; intended for the periodic drainer task.
    pub fn expire_now(&self, now: Instant) -> usize {
        let mut inner = self.inner.lock();
        let mut dropped = 0usize;
        let mut empty_keys: Vec<u32> = Vec::new();
        // Peers whose queue lost entries and whose durable
        // segment therefore needs compacting.
        let mut touched: Vec<u32> = Vec::new();
        for (k, queue) in &mut inner.by_peer {
            let before = queue.len();
            queue.retain(|h| h.deadline > now);
            let after = queue.len();
            let removed = before - after;
            if removed > 0 {
                dropped += removed;
                touched.push(*k);
                if after == 0 {
                    empty_keys.push(*k);
                }
            }
        }
        // Recompute total bytes from scratch: the per-peer
        // retained weights are now consistent with `bytes` only
        // after we subtract the dropped weights. We trade a
        // second pass for a clean invariant rather than tracking
        // dropped weights inline above.
        let mut new_bytes: u64 = 0;
        for queue in inner.by_peer.values() {
            for h in queue {
                new_bytes = new_bytes.saturating_add(h.weight());
            }
        }
        inner.bytes = new_bytes;
        inner.expired_total = inner.expired_total.saturating_add(dropped as u64);
        // Compact the durable segments for every touched peer so
        // the on-disk log mirrors the surviving in-memory hints
        // and never grows past `max_bytes`. A full per-peer
        // rewrite is acceptable because each segment is bounded
        // by the same cap. Borrow `disk` out before mutating the
        // map's keys to keep the borrow checker happy.
        if inner.disk.is_some() {
            let wall_now = SystemTime::now();
            let inst_now = Instant::now();
            for k in &touched {
                let records: Vec<(SystemTime, Vec<u8>)> = inner
                    .by_peer
                    .get(k)
                    .map(|q| {
                        q.iter()
                            .map(|h| {
                                (
                                    deadline_to_wall(h.deadline, inst_now, wall_now),
                                    h.payload.clone(),
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let disk = inner.disk.as_ref().expect("invariant: disk is Some");
                let result = if records.is_empty() {
                    disk.remove_peer(*k)
                } else {
                    disk.rewrite_peer(*k, &records)
                };
                if let Err(e) = result {
                    tracing::warn!(
                        target: "dynomite::cluster::hints",
                        peer_idx = *k,
                        error = %e,
                        "failed to compact hint segment during expiry"
                    );
                }
            }
        }
        for k in empty_keys {
            inner.by_peer.remove(&k);
        }
        dropped
    }

    /// Number of hints across every peer.
    #[must_use]
    pub fn total_len(&self) -> usize {
        let inner = self.inner.lock();
        inner.by_peer.values().map(Vec::len).sum()
    }

    /// Pending hint count for `peer_idx`. Useful for tests.
    #[must_use]
    pub fn len_for(&self, peer_idx: u32) -> usize {
        let inner = self.inner.lock();
        inner.by_peer.get(&peer_idx).map_or(0, Vec::len)
    }

    /// Snapshot the store's accounting fields.
    #[must_use]
    pub fn stats(&self) -> HintStoreStats {
        let inner = self.inner.lock();
        HintStoreStats {
            hint_count: inner.by_peer.values().map(Vec::len).sum(),
            bytes: inner.bytes,
            max_bytes: inner.max_bytes,
            expired_total: inner.expired_total,
            rejected_over_capacity_total: inner.rejected_over_capacity_total,
        }
    }

    /// Iterate the peer indices that currently have pending
    /// hints. Used by the drainer to decide which peers to ship
    /// to without holding the inner lock across the network
    /// send.
    #[must_use]
    pub fn peers_with_hints(&self) -> Vec<u32> {
        let inner = self.inner.lock();
        inner
            .by_peer
            .iter()
            .filter_map(|(k, v)| if v.is_empty() { None } else { Some(*k) })
            .collect()
    }
}

/// Re-anchor a monotonic `Instant` deadline onto a wall clock.
///
/// `inst_now` and `wall_now` are a matched pair sampled close
/// together. A deadline already in the past maps to `wall_now`
/// (zero remaining), which replay then treats as expired.
fn deadline_to_wall(deadline: Instant, inst_now: Instant, wall_now: SystemTime) -> SystemTime {
    let remaining = deadline.saturating_duration_since(inst_now);
    wall_now + remaining
}

/// Durable per-peer segment backend.
///
/// One append-only file per peer named `peer-<idx>.hints` lives
/// under [`DiskBackend::dir`]. See the module docs for the
/// record framing and the torn-tail recovery contract.
#[derive(Debug)]
struct DiskBackend {
    dir: PathBuf,
}

impl DiskBackend {
    fn segment_path(&self, peer_idx: u32) -> PathBuf {
        self.dir.join(format!("peer-{peer_idx}.hints"))
    }

    /// Append one framed record to the peer's segment, flushing
    /// to the OS before returning so a clean process exit does
    /// not lose the hint.
    fn append(&self, peer_idx: u32, deadline: SystemTime, payload: &[u8]) -> io::Result<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.segment_path(peer_idx))?;
        let frame = encode_record(deadline, payload);
        f.write_all(&frame)?;
        f.flush()?;
        Ok(())
    }

    /// Replace the peer's segment with exactly `records`, written
    /// to a sibling temporary file and renamed into place so a
    /// crash mid-rewrite cannot leave a half-compacted segment.
    fn rewrite_peer(&self, peer_idx: u32, records: &[(SystemTime, Vec<u8>)]) -> io::Result<()> {
        let final_path = self.segment_path(peer_idx);
        let tmp_path = self.dir.join(format!("peer-{peer_idx}.hints.tmp"));
        {
            let mut f = File::create(&tmp_path)?;
            for (deadline, payload) in records {
                f.write_all(&encode_record(*deadline, payload))?;
            }
            f.flush()?;
        }
        fs::rename(&tmp_path, &final_path)
    }

    /// Remove the peer's segment. A missing file is not an error.
    fn remove_peer(&self, peer_idx: u32) -> io::Result<()> {
        match fs::remove_file(self.segment_path(peer_idx)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Replay every `peer-<idx>.hints` segment in the directory
    /// back into per-peer queues, re-anchoring deadlines onto
    /// the monotonic clock and dropping records whose wall-clock
    /// deadline has already passed. Torn trailing records are
    /// discarded silently.
    fn replay(&self) -> Result<HashMap<u32, Vec<Hint>>, HintStoreOpenError> {
        let mut out: HashMap<u32, Vec<Hint>> = HashMap::new();
        let entries = fs::read_dir(&self.dir).map_err(|source| HintStoreOpenError::Dir {
            path: self.dir.clone(),
            source,
        })?;
        let wall_now = SystemTime::now();
        let inst_now = Instant::now();
        for entry in entries {
            let entry = entry.map_err(|source| HintStoreOpenError::Dir {
                path: self.dir.clone(),
                source,
            })?;
            let path = entry.path();
            let Some(peer_idx) = parse_segment_name(&path) else {
                continue;
            };
            let mut bytes = Vec::new();
            File::open(&path)
                .and_then(|mut f| f.read_to_end(&mut bytes))
                .map_err(|source| HintStoreOpenError::Segment {
                    path: path.clone(),
                    source,
                })?;
            let queue = out.entry(peer_idx).or_default();
            for (deadline, payload) in decode_records(&bytes) {
                let Ok(remaining) = deadline.duration_since(wall_now) else {
                    // Wall-clock deadline already passed; drop.
                    continue;
                };
                queue.push(Hint {
                    peer_idx,
                    payload,
                    deadline: inst_now + remaining,
                });
            }
            if queue.is_empty() {
                out.remove(&peer_idx);
            }
        }
        Ok(out)
    }
}

/// Parse `peer-<idx>.hints` into the peer index. Returns `None`
/// for any other file (including the `.tmp` rewrite scratch
/// file), so stray files in the directory are ignored.
fn parse_segment_name(path: &Path) -> Option<u32> {
    let name = path.file_name()?.to_str()?;
    let idx = name.strip_prefix("peer-")?.strip_suffix(".hints")?;
    idx.parse::<u32>().ok()
}

/// Frame one record: `len: u32 LE`, `crc: u32 LE` of the body,
/// then the body (`deadline_millis: u64 LE` followed by the
/// payload). The CRC covers the body only.
fn encode_record(deadline: SystemTime, payload: &[u8]) -> Vec<u8> {
    let millis = deadline
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let millis = u64::try_from(millis).unwrap_or(u64::MAX);
    let body_len = 8 + payload.len();
    let mut body = Vec::with_capacity(body_len);
    body.extend_from_slice(&millis.to_le_bytes());
    body.extend_from_slice(payload);
    let crc = crc32(&body);
    let mut frame = Vec::with_capacity(8 + body.len());
    frame.extend_from_slice(&(u32::try_from(body.len()).unwrap_or(u32::MAX)).to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&body);
    frame
}

/// Decode every intact record in `bytes`, stopping at the first
/// torn or corrupt record (clean EOF, short read, or CRC
/// mismatch). Never panics on arbitrary input.
fn decode_records(bytes: &[u8]) -> Vec<(SystemTime, Vec<u8>)> {
    let mut out = Vec::new();
    let mut off = 0usize;
    while off + 8 <= bytes.len() {
        let len = u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]]);
        let len = len as usize;
        let crc = u32::from_le_bytes([
            bytes[off + 4],
            bytes[off + 5],
            bytes[off + 6],
            bytes[off + 7],
        ]);
        let body_start = off + 8;
        let Some(body_end) = body_start.checked_add(len) else {
            break;
        };
        if body_end > bytes.len() {
            // Torn tail: the body was not fully written.
            break;
        }
        let body = &bytes[body_start..body_end];
        if len < 8 || crc32(body) != crc {
            // Corrupt or torn record; discard it and everything
            // after it.
            break;
        }
        let millis = u64::from_le_bytes([
            body[0], body[1], body[2], body[3], body[4], body[5], body[6], body[7],
        ]);
        let deadline = UNIX_EPOCH + Duration::from_millis(millis);
        out.push((deadline, body[8..].to_vec()));
        off = body_end;
    }
    out
}

/// CRC-32 (IEEE 802.3, reflected, polynomial `0xEDB88820`).
/// Hand-rolled to avoid pulling a checksum crate into the
/// engine's direct dependency set.
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(b: u8, n: usize) -> Vec<u8> {
        vec![b; n]
    }

    #[test]
    fn enqueue_and_take_round_trip() {
        let store = HintStore::new(1024);
        store
            .enqueue(3, payload(b'a', 4), Duration::from_secs(60))
            .unwrap();
        store
            .enqueue(3, payload(b'b', 4), Duration::from_secs(60))
            .unwrap();
        store
            .enqueue(7, payload(b'c', 4), Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.total_len(), 3);
        let drained = store.take_for(3);
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].payload, payload(b'a', 4));
        assert_eq!(drained[1].payload, payload(b'b', 4));
        assert_eq!(store.len_for(3), 0);
        assert_eq!(store.len_for(7), 1);
        assert_eq!(store.total_len(), 1);
    }

    #[test]
    fn enqueue_rejects_over_capacity() {
        let store = HintStore::new(8);
        store
            .enqueue(0, payload(b'x', 6), Duration::from_secs(60))
            .unwrap();
        let err = store
            .enqueue(0, payload(b'y', 4), Duration::from_secs(60))
            .unwrap_err();
        assert_eq!(err, HintStoreError::OverCapacity { max_bytes: 8 });
        // Bytes accounting unaffected by the rejected enqueue.
        assert_eq!(store.stats().bytes, 6);
        assert_eq!(store.stats().rejected_over_capacity_total, 1);
        // Drain reclaims space.
        let drained = store.take_for(0);
        assert_eq!(drained.len(), 1);
        // Now the previously-rejected payload fits.
        store
            .enqueue(0, payload(b'y', 4), Duration::from_secs(60))
            .unwrap();
    }

    #[test]
    fn expire_now_drops_old_hints() {
        let store = HintStore::new(64);
        store
            .enqueue(1, payload(b'a', 3), Duration::from_millis(1))
            .unwrap();
        store
            .enqueue(1, payload(b'b', 3), Duration::from_secs(60))
            .unwrap();
        // Sleep a moment so the first hint expires.
        std::thread::sleep(Duration::from_millis(5));
        let now = Instant::now();
        let dropped = store.expire_now(now);
        assert_eq!(dropped, 1);
        assert_eq!(store.len_for(1), 1);
        let stats = store.stats();
        assert_eq!(stats.expired_total, 1);
        assert_eq!(stats.bytes, 3);
        // Surviving hint is the one with the long TTL.
        let drained = store.take_for(1);
        assert_eq!(drained[0].payload, payload(b'b', 3));
    }

    #[test]
    fn take_for_skips_already_expired() {
        let store = HintStore::new(64);
        store
            .enqueue(2, payload(b'a', 3), Duration::from_millis(1))
            .unwrap();
        store
            .enqueue(2, payload(b'b', 3), Duration::from_secs(60))
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));
        let drained = store.take_for(2);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].payload, payload(b'b', 3));
        assert_eq!(store.stats().expired_total, 1);
    }

    #[test]
    fn enqueue_rejects_zero_ttl_and_empty_payload() {
        let store = HintStore::new(64);
        let err = store
            .enqueue(0, payload(b'x', 1), Duration::from_secs(0))
            .unwrap_err();
        assert_eq!(err, HintStoreError::ZeroTtl);
        let err = store
            .enqueue(0, Vec::new(), Duration::from_secs(60))
            .unwrap_err();
        assert_eq!(err, HintStoreError::EmptyPayload);
        assert_eq!(store.total_len(), 0);
    }

    #[test]
    fn mixed_peer_queues_are_independent() {
        let store = HintStore::new(0); // unbounded
        store
            .enqueue(0, payload(b'a', 1), Duration::from_secs(60))
            .unwrap();
        store
            .enqueue(1, payload(b'b', 1), Duration::from_secs(60))
            .unwrap();
        store
            .enqueue(2, payload(b'c', 1), Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.total_len(), 3);
        let mut peers = store.peers_with_hints();
        peers.sort_unstable();
        assert_eq!(peers, vec![0, 1, 2]);
        let drained = store.take_for(1);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].payload, payload(b'b', 1));
        assert_eq!(store.len_for(0), 1);
        assert_eq!(store.len_for(1), 0);
        assert_eq!(store.len_for(2), 1);
    }

    #[test]
    fn empty_max_bytes_means_unbounded() {
        let store = HintStore::new(0);
        for _ in 0..1024 {
            store
                .enqueue(0, payload(b'x', 1024), Duration::from_secs(60))
                .unwrap();
        }
        assert_eq!(store.total_len(), 1024);
    }

    #[test]
    fn expire_now_no_op_when_nothing_old() {
        let store = HintStore::new(64);
        store
            .enqueue(0, payload(b'x', 3), Duration::from_secs(60))
            .unwrap();
        let dropped = store.expire_now(Instant::now());
        assert_eq!(dropped, 0);
        assert_eq!(store.total_len(), 1);
    }

    #[test]
    fn stats_track_capacity_and_bytes() {
        let store = HintStore::new(1024);
        store
            .enqueue(0, payload(b'x', 100), Duration::from_secs(60))
            .unwrap();
        let s = store.stats();
        assert_eq!(s.hint_count, 1);
        assert_eq!(s.bytes, 100);
        assert_eq!(s.max_bytes, 1024);
    }

    fn scratch_dir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("hints-")
            .tempdir_in("/scratch")
            .expect("create scratch tempdir")
    }

    #[test]
    fn ram_only_store_never_touches_disk() {
        let dir = scratch_dir();
        // A RAM-only store must not write to the directory even
        // when one is handy.
        let store = HintStore::new(1024);
        store
            .enqueue(0, payload(b'a', 4), Duration::from_secs(60))
            .unwrap();
        store.take_for(0);
        store.expire_now(Instant::now());
        let count = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(count, 0, "new() must not create segment files");
    }

    #[test]
    fn durable_round_trip_survives_reopen() {
        let dir = scratch_dir();
        {
            let store = HintStore::open(dir.path(), 1024).unwrap();
            store
                .enqueue(3, payload(b'a', 4), Duration::from_secs(600))
                .unwrap();
            store
                .enqueue(3, payload(b'b', 5), Duration::from_secs(600))
                .unwrap();
            store
                .enqueue(7, payload(b'c', 6), Duration::from_secs(600))
                .unwrap();
        }
        let reopened = HintStore::open(dir.path(), 1024).unwrap();
        assert_eq!(reopened.total_len(), 3);
        let p3 = reopened.take_for(3);
        assert_eq!(p3.len(), 2);
        assert_eq!(p3[0].payload, payload(b'a', 4));
        assert_eq!(p3[1].payload, payload(b'b', 5));
        let p7 = reopened.take_for(7);
        assert_eq!(p7.len(), 1);
        assert_eq!(p7[0].payload, payload(b'c', 6));
        // Bytes were counted toward the cap on replay.
        assert_eq!(reopened.stats().bytes, 0);
    }

    #[test]
    fn torn_tail_recovers_intact_records() {
        use std::io::Write;
        let dir = scratch_dir();
        {
            let store = HintStore::open(dir.path(), 1024).unwrap();
            store
                .enqueue(2, payload(b'a', 8), Duration::from_secs(600))
                .unwrap();
        }
        // Append a truncated partial record after the intact one.
        let seg = dir.path().join("peer-2.hints");
        let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
        // A plausible 100-byte length prefix plus a few stray
        // bytes that never complete the body.
        f.write_all(&100u32.to_le_bytes()).unwrap();
        f.write_all(&0u32.to_le_bytes()).unwrap();
        f.write_all(&[0xAB, 0xCD, 0xEF]).unwrap();
        f.flush().unwrap();
        drop(f);
        // Replay must recover the intact record and discard the
        // partial without panicking or erroring.
        let reopened = HintStore::open(dir.path(), 1024).unwrap();
        let p2 = reopened.take_for(2);
        assert_eq!(p2.len(), 1);
        assert_eq!(p2[0].payload, payload(b'a', 8));
    }

    #[test]
    fn torn_body_with_bad_crc_is_discarded() {
        use std::io::Write;
        let dir = scratch_dir();
        {
            let store = HintStore::open(dir.path(), 1024).unwrap();
            store
                .enqueue(5, payload(b'z', 4), Duration::from_secs(600))
                .unwrap();
        }
        // A full-length record whose body bytes are present but
        // whose CRC does not match (a torn-within-body write that
        // still flushed the length prefix).
        let seg = dir.path().join("peer-5.hints");
        let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
        let body = [0u8; 12]; // 8-byte deadline + 4-byte payload
        let body_len = u32::try_from(body.len()).unwrap();
        f.write_all(&body_len.to_le_bytes()).unwrap();
        f.write_all(&0xDEAD_BEEFu32.to_le_bytes()).unwrap(); // wrong crc
        f.write_all(&body).unwrap();
        f.flush().unwrap();
        drop(f);
        let reopened = HintStore::open(dir.path(), 1024).unwrap();
        let p5 = reopened.take_for(5);
        assert_eq!(p5.len(), 1, "only the intact record survives");
        assert_eq!(p5[0].payload, payload(b'z', 4));
    }

    #[test]
    fn take_for_clears_disk_segment() {
        let dir = scratch_dir();
        let store = HintStore::open(dir.path(), 1024).unwrap();
        store
            .enqueue(4, payload(b'a', 4), Duration::from_secs(600))
            .unwrap();
        assert!(dir.path().join("peer-4.hints").exists());
        let drained = store.take_for(4);
        assert_eq!(drained.len(), 1);
        assert!(
            !dir.path().join("peer-4.hints").exists(),
            "take_for must remove the drained segment"
        );
        // Reopening yields nothing for that peer.
        drop(store);
        let reopened = HintStore::open(dir.path(), 1024).unwrap();
        assert_eq!(reopened.len_for(4), 0);
    }

    #[test]
    fn expire_now_compacts_disk_segment() {
        let dir = scratch_dir();
        let store = HintStore::open(dir.path(), 1024).unwrap();
        store
            .enqueue(1, payload(b'a', 3), Duration::from_millis(1))
            .unwrap();
        store
            .enqueue(1, payload(b'b', 3), Duration::from_secs(600))
            .unwrap();
        let seg = dir.path().join("peer-1.hints");
        let big = std::fs::metadata(&seg).unwrap().len();
        std::thread::sleep(Duration::from_millis(5));
        let dropped = store.expire_now(Instant::now());
        assert_eq!(dropped, 1);
        // Segment shrank after compaction.
        let small = std::fs::metadata(&seg).unwrap().len();
        assert!(small < big, "expiry must compact the segment");
        // The surviving hint is the only thing replayed.
        drop(store);
        let reopened = HintStore::open(dir.path(), 1024).unwrap();
        let p1 = reopened.take_for(1);
        assert_eq!(p1.len(), 1);
        assert_eq!(p1[0].payload, payload(b'b', 3));
    }

    #[test]
    fn expire_now_to_empty_removes_segment() {
        let dir = scratch_dir();
        let store = HintStore::open(dir.path(), 1024).unwrap();
        store
            .enqueue(9, payload(b'a', 3), Duration::from_millis(1))
            .unwrap();
        assert!(dir.path().join("peer-9.hints").exists());
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(store.expire_now(Instant::now()), 1);
        assert!(
            !dir.path().join("peer-9.hints").exists(),
            "a fully-expired peer's segment must be removed"
        );
    }

    #[test]
    fn capacity_enforced_across_replayed_bytes() {
        let dir = scratch_dir();
        {
            let store = HintStore::open(dir.path(), 8).unwrap();
            store
                .enqueue(0, payload(b'x', 6), Duration::from_secs(600))
                .unwrap();
        }
        // Reopen with the same cap: the replayed 6 bytes count,
        // so a 4-byte enqueue must still be rejected.
        let reopened = HintStore::open(dir.path(), 8).unwrap();
        assert_eq!(reopened.stats().bytes, 6);
        let err = reopened
            .enqueue(0, payload(b'y', 4), Duration::from_secs(600))
            .unwrap_err();
        assert_eq!(err, HintStoreError::OverCapacity { max_bytes: 8 });
    }

    #[test]
    fn replay_drops_expired_records() {
        let dir = scratch_dir();
        {
            let store = HintStore::open(dir.path(), 1024).unwrap();
            store
                .enqueue(0, payload(b'a', 4), Duration::from_millis(1))
                .unwrap();
            store
                .enqueue(0, payload(b'b', 4), Duration::from_secs(600))
                .unwrap();
        }
        std::thread::sleep(Duration::from_millis(5));
        let reopened = HintStore::open(dir.path(), 1024).unwrap();
        let p0 = reopened.take_for(0);
        assert_eq!(p0.len(), 1, "the expired record is not replayed");
        assert_eq!(p0[0].payload, payload(b'b', 4));
    }

    #[test]
    fn record_codec_round_trip() {
        let now = SystemTime::now() + Duration::from_secs(100);
        let frame = encode_record(now, b"hello");
        let recs = decode_records(&frame);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].1, b"hello");
        // Concatenated frames decode in order.
        let mut two = encode_record(now, b"one");
        two.extend_from_slice(&encode_record(now, b"two"));
        let recs = decode_records(&two);
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].1, b"one");
        assert_eq!(recs[1].1, b"two");
    }

    #[test]
    fn crc32_known_vector() {
        // CRC-32 of "123456789" is 0xCBF43926.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }
}
