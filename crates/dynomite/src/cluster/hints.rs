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
//! The v1 store is RAM-only. The natural follow-up is an
//! on-disk variant (one segment file per peer, replayed at
//! startup); see `docs/journal/2026-05-23-hinted-handoff.md`
//! for the deferral note.
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
use std::time::{Duration, Instant};

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
            }),
        }
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
        out
    }

    /// Drop every hint whose deadline has passed at `now`.
    /// Returns the number of hints dropped. Walks the entire
    /// store; intended for the periodic drainer task.
    pub fn expire_now(&self, now: Instant) -> usize {
        let mut inner = self.inner.lock();
        let mut dropped = 0usize;
        let mut empty_keys: Vec<u32> = Vec::new();
        for (k, queue) in &mut inner.by_peer {
            let before = queue.len();
            queue.retain(|h| h.deadline > now);
            let after = queue.len();
            let removed = before - after;
            if removed > 0 {
                dropped += removed;
                // Recompute the bytes lost. The queue does not
                // remember which entries it dropped, so we walk
                // the original-vs-retained delta below using a
                // single pass (cheaper than the alternative of
                // collecting weights up front).
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
}
