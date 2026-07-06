//! RAMP-Fast coordinator over the local Noxu store.
//!
//! [`RampCoordinator`] runs the RAMP-Fast write and read protocols
//! against a [`RampStore`] backend. The write path is two-phase and
//! non-blocking; the read path is one round plus the conditional
//! second round decided by the pure core in [`crate::ramp::select`].
//!
//! # Storage layout
//!
//! RAMP needs per-item version history plus a per-key latest-visible
//! pointer, so it uses its own keyspace under the raw Noxu store
//! (disjoint from the primary K/V and 2i tags in
//! [`crate::datastore::noxu`]):
//!
//! * **Versioned item** under `V\0{key}\0<ts-be>` -> an encoded
//!   [`crate::ramp::RampItem`] (value + sibling metadata). Written in
//!   PREPARE, invisible until COMMIT.
//! * **Latest-visible pointer** under `L\0{key}` -> the big-endian
//!   timestamp of the version a reader should see. Advanced in COMMIT.
//!   Its absence means the key has no committed RAMP write yet.
//!
//! A reader never blocks: round 1 reads the pointer and the version it
//! points at; if the pointer is absent the key has no RAMP value. The
//! two-phase split guarantees that whenever a pointer names `ts`, the
//! version at `ts` is already durably present (PREPARE happened before
//! COMMIT), so a second-round fetch by timestamp always succeeds.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::datastore::noxu::{NoxuDatastore, NoxuDatastoreError};
use crate::ramp::{select, RampClock, RampItem, Timestamp};

/// Storage prefix for a versioned RAMP item.
const VER_TAG: &[u8] = b"V\0";
/// Storage prefix for a key's latest-visible timestamp pointer.
const PTR_TAG: &[u8] = b"L\0";

/// A fracture-free RAMP read result: the `key -> value` snapshot paired
/// with the number of read rounds the coordinator used (1 or 2).
pub type RampReadResult = (BTreeMap<Vec<u8>, Vec<u8>>, u8);

/// Errors surfaced by the RAMP coordinator.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RampError {
    /// The backing store rejected an operation.
    #[error("ramp store: {0}")]
    Store(String),
    /// A committed pointer named a timestamp whose version record is
    /// missing. This must never happen given the two-phase ordering
    /// (PREPARE writes the version before COMMIT advances the
    /// pointer); it indicates store corruption.
    #[error("ramp: dangling version pointer for key {key:?} at ts {ts}")]
    DanglingVersion {
        /// The key whose pointer dangled.
        key: Vec<u8>,
        /// The timestamp the pointer named.
        ts: Timestamp,
    },
    /// A stored version record failed to decode.
    #[error("ramp: corrupt version record for key {key:?} at ts {ts}")]
    CorruptVersion {
        /// The key whose version record was unreadable.
        key: Vec<u8>,
        /// The timestamp of the unreadable record.
        ts: Timestamp,
    },
    /// The write set was empty.
    #[error("ramp: empty write set")]
    EmptyWrite,
}

impl From<NoxuDatastoreError> for RampError {
    fn from(e: NoxuDatastoreError) -> Self {
        Self::Store(e.to_string())
    }
}

/// The versioned-storage operations RAMP needs from a backend.
///
/// Kept as a trait so the coordinator does not name a concrete store
/// on its own surface and so tests can drive an in-memory backend. The
/// production implementation is on [`NoxuDatastore`].
pub trait RampStore {
    /// PREPARE: write the versioned, invisible item `item` under its
    /// key and timestamp. Overwriting an identical `(key, ts)` is
    /// idempotent (PREPARE may be retried).
    ///
    /// # Errors
    ///
    /// [`RampError::Store`] on a backend failure.
    fn put_version(&self, item: &RampItem) -> Result<(), RampError>;

    /// COMMIT: advance `key`'s latest-visible pointer to `ts`. Never
    /// moves the pointer backwards (a stale re-delivered COMMIT is
    /// idempotent), so the visible version is monotonic per key.
    ///
    /// # Errors
    ///
    /// [`RampError::Store`] on a backend failure.
    fn commit_pointer(&self, key: &[u8], ts: Timestamp) -> Result<(), RampError>;

    /// Read `key`'s latest-visible timestamp, or `None` if the key has
    /// no committed RAMP write.
    ///
    /// # Errors
    ///
    /// [`RampError::Store`] on a backend failure.
    fn latest_visible(&self, key: &[u8]) -> Result<Option<Timestamp>, RampError>;

    /// Read the versioned item stored for `(key, ts)`, or `None` if no
    /// such version exists.
    ///
    /// # Errors
    ///
    /// [`RampError::CorruptVersion`] if a record exists but does not
    /// decode; [`RampError::Store`] on a backend failure.
    fn get_version(&self, key: &[u8], ts: Timestamp) -> Result<Option<RampItem>, RampError>;
}

/// Encode a versioned-item storage key: `V\0{key}\0<ts-be>`.
fn version_key(key: &[u8], ts: Timestamp) -> Vec<u8> {
    let mut out = Vec::with_capacity(VER_TAG.len() + key.len() + 1 + 8);
    out.extend_from_slice(VER_TAG);
    out.extend_from_slice(key);
    out.push(0);
    out.extend_from_slice(&ts.to_be_bytes());
    out
}

/// Encode a latest-visible pointer storage key: `L\0{key}`.
fn pointer_key(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PTR_TAG.len() + key.len());
    out.extend_from_slice(PTR_TAG);
    out.extend_from_slice(key);
    out
}

/// Encode a [`RampItem`]'s stored body (value + sibling metadata).
///
/// Layout: `<u32-be sibling-count>` then for each sibling
/// `<u32-be len><bytes>`, then the raw value bytes to end. The key and
/// ts are recoverable from the storage key, so only the value and
/// siblings are stored in the body.
fn encode_body(item: &RampItem) -> Vec<u8> {
    let mut out = Vec::new();
    let count = u32::try_from(item.siblings.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&count.to_be_bytes());
    for sib in &item.siblings {
        let len = u32::try_from(sib.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(sib);
    }
    out.extend_from_slice(&item.value);
    out
}

/// Decode a body written by [`encode_body`] back into `(siblings,
/// value)`. Returns `None` on any truncation / length mismatch.
fn decode_body(bytes: &[u8]) -> Option<(Vec<Vec<u8>>, Vec<u8>)> {
    let mut pos = 0usize;
    let count = read_u32(bytes, &mut pos)? as usize;
    let mut siblings = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u32(bytes, &mut pos)? as usize;
        let end = pos.checked_add(len)?;
        if end > bytes.len() {
            return None;
        }
        siblings.push(bytes[pos..end].to_vec());
        pos = end;
    }
    Some((siblings, bytes[pos..].to_vec()))
}

/// Read a big-endian `u32` at `*pos`, advancing `*pos`. `None` on
/// truncation.
fn read_u32(bytes: &[u8], pos: &mut usize) -> Option<u32> {
    let end = pos.checked_add(4)?;
    if end > bytes.len() {
        return None;
    }
    let v = u32::from_be_bytes([
        bytes[*pos],
        bytes[*pos + 1],
        bytes[*pos + 2],
        bytes[*pos + 3],
    ]);
    *pos = end;
    Some(v)
}

impl RampStore for NoxuDatastore {
    fn put_version(&self, item: &RampItem) -> Result<(), RampError> {
        let vk = version_key(&item.key, item.ts);
        self.put(&vk, &encode_body(item))?;
        Ok(())
    }

    fn commit_pointer(&self, key: &[u8], ts: Timestamp) -> Result<(), RampError> {
        let pk = pointer_key(key);
        // Monotonic: never move the pointer backwards, so a stale
        // re-delivered COMMIT is a no-op and the visible version only
        // ever advances.
        if let Some(cur) = self.get(&pk)? {
            if cur.len() == 8 {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&cur);
                if u64::from_be_bytes(buf) >= ts {
                    return Ok(());
                }
            }
        }
        self.put(&pk, &ts.to_be_bytes())?;
        Ok(())
    }

    fn latest_visible(&self, key: &[u8]) -> Result<Option<Timestamp>, RampError> {
        let pk = pointer_key(key);
        match self.get(&pk)? {
            Some(v) if v.len() == 8 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                Ok(Some(u64::from_be_bytes(buf)))
            }
            _ => Ok(None),
        }
    }

    fn get_version(&self, key: &[u8], ts: Timestamp) -> Result<Option<RampItem>, RampError> {
        let vk = version_key(key, ts);
        match self.get(&vk)? {
            Some(body) => {
                let (siblings, value) =
                    decode_body(&body).ok_or_else(|| RampError::CorruptVersion {
                        key: key.to_vec(),
                        ts,
                    })?;
                Ok(Some(RampItem::new(key.to_vec(), ts, siblings, value)))
            }
            None => Ok(None),
        }
    }
}

/// One write in a RAMP write transaction: a key and the value to store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RampWrite {
    /// The key to write.
    pub key: Vec<u8>,
    /// The value bytes.
    pub value: Vec<u8>,
}

/// The RAMP-Fast coordinator.
///
/// Wraps a [`RampStore`] and a [`RampClock`]. One coordinator instance
/// mints monotonically increasing timestamps; distinct coordinators use
/// distinct ids so their timestamps never collide (see [`RampClock`]).
pub struct RampCoordinator<S: RampStore> {
    store: S,
    clock: RampClock,
}

impl<S: RampStore> RampCoordinator<S> {
    /// Create a coordinator with the given store and coordinator id.
    pub fn new(store: S, coordinator_id: u16) -> Self {
        Self {
            store,
            clock: RampClock::new(coordinator_id),
        }
    }

    /// Borrow the backing store (for reads outside a coordinator round).
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Run a RAMP-Fast write transaction over `writes`.
    ///
    /// Two phases, both non-blocking:
    ///
    /// 1. **PREPARE**: pick one timestamp `ts` for the batch and write
    ///    every item as a versioned, invisible record carrying the set
    ///    of sibling keys the batch wrote.
    /// 2. **COMMIT**: advance every key's latest-visible pointer to
    ///    `ts`.
    ///
    /// Returns the transaction's timestamp on success.
    ///
    /// # Errors
    ///
    /// [`RampError::EmptyWrite`] for an empty batch; [`RampError::Store`]
    /// on a backend failure during either phase.
    pub fn write(&mut self, writes: &[RampWrite]) -> Result<Timestamp, RampError> {
        if writes.is_empty() {
            return Err(RampError::EmptyWrite);
        }
        let ts = self.clock.mint();
        let all_keys: Vec<Vec<u8>> = writes.iter().map(|w| w.key.clone()).collect();

        // PREPARE: every item invisible, tagged with its siblings.
        for w in writes {
            let siblings: Vec<Vec<u8>> =
                all_keys.iter().filter(|k| *k != &w.key).cloned().collect();
            let item = RampItem::new(w.key.clone(), ts, siblings, w.value.clone());
            self.store.put_version(&item)?;
        }
        // COMMIT: make every item visible.
        for w in writes {
            self.store.commit_pointer(&w.key, ts)?;
        }
        Ok(ts)
    }

    /// Run a RAMP-Fast read transaction over `keys`, returning a
    /// fracture-free snapshot: `key -> value` for every key that has a
    /// committed RAMP write (keys with no RAMP value are omitted).
    ///
    /// Round 1 fetches each key's latest-visible version and its
    /// metadata. The pure core [`select`] decides which siblings, if
    /// any, are missing; round 2 fetches exactly those by timestamp.
    /// The common contention-free case skips round 2.
    ///
    /// # Errors
    ///
    /// [`RampError::DanglingVersion`] if a committed pointer names a
    /// missing version (store corruption); [`RampError::Store`] on a
    /// backend failure.
    pub fn read(&self, keys: &[Vec<u8>]) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, RampError> {
        let (snapshot, _rounds) = self.read_with_rounds(keys)?;
        Ok(snapshot)
    }

    /// Like [`Self::read`] but also reports how many rounds the read
    /// took (1 or 2). Used by tests / benches to observe the
    /// conditional-second-round behaviour.
    ///
    /// # Errors
    ///
    /// See [`Self::read`].
    pub fn read_with_rounds(&self, keys: &[Vec<u8>]) -> Result<RampReadResult, RampError> {
        read_rounds(&self.store, keys)
    }
}

// ------------------------------------------------------------------
// Process-wide RAMP clock + one-shot coordinator helpers.
// ------------------------------------------------------------------

/// Process-wide monotonic counter for the shared HTTP coordinator.
///
/// The HTTP layer holds a shared (`Arc`-clonable) [`NoxuDatastore`],
/// not a `&mut RampCoordinator`, so its timestamp source must be
/// thread-safe. This atomic gives strictly increasing values across
/// concurrent requests on one node; combined with the node's
/// coordinator id it satisfies RAMP's uniqueness + per-writer
/// monotonicity requirement.
static HTTP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Mint the next process-wide RAMP timestamp for coordinator `id`.
fn next_http_ts(id: u16) -> Timestamp {
    let n = HTTP_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    (u64::from(id) << 48) | (n & 0x0000_ffff_ffff_ffff)
}

/// Run a one-shot RAMP-Fast write against `store` using the shared
/// process-wide clock. This is the entry point the HTTP handler calls
/// (it cannot hold a long-lived `&mut RampCoordinator`).
///
/// # Errors
///
/// [`RampError::EmptyWrite`] for an empty batch; [`RampError::Store`]
/// on a backend failure.
pub fn ramp_write<S: RampStore>(
    store: &S,
    coordinator_id: u16,
    writes: &[RampWrite],
) -> Result<Timestamp, RampError> {
    if writes.is_empty() {
        return Err(RampError::EmptyWrite);
    }
    let ts = next_http_ts(coordinator_id);
    let all_keys: Vec<Vec<u8>> = writes.iter().map(|w| w.key.clone()).collect();
    for w in writes {
        let siblings: Vec<Vec<u8>> = all_keys.iter().filter(|k| *k != &w.key).cloned().collect();
        let item = RampItem::new(w.key.clone(), ts, siblings, w.value.clone());
        store.put_version(&item)?;
    }
    for w in writes {
        store.commit_pointer(&w.key, ts)?;
    }
    Ok(ts)
}

/// Run a one-shot RAMP-Fast read against `store`, returning the
/// fracture-free snapshot and the round count (1 or 2). The read
/// logic is identical to [`RampCoordinator::read_with_rounds`]; this
/// free form lets the HTTP handler read without a coordinator handle.
///
/// # Errors
///
/// See [`RampCoordinator::read_with_rounds`].
pub fn ramp_read<S: RampStore>(store: &S, keys: &[Vec<u8>]) -> Result<RampReadResult, RampError> {
    read_rounds(store, keys)
}

/// The shared RAMP-Fast read core: round 1 + conditional round 2.
///
/// Both [`RampCoordinator::read_with_rounds`] and the free
/// [`ramp_read`] delegate here so there is exactly one implementation
/// of the read-atomic read path. Returns the fracture-free snapshot
/// and the round count (1 or 2).
///
/// # Errors
///
/// [`RampError::DanglingVersion`] if a committed pointer names a
/// missing version; [`RampError::Store`] on a backend failure.
fn read_rounds<S: RampStore>(store: &S, keys: &[Vec<u8>]) -> Result<RampReadResult, RampError> {
    // Round 1: latest-visible version + metadata per key.
    let mut round1: Vec<RampItem> = Vec::with_capacity(keys.len());
    for key in keys {
        if let Some(ts) = store.latest_visible(key)? {
            let item = store
                .get_version(key, ts)?
                .ok_or_else(|| RampError::DanglingVersion {
                    key: key.clone(),
                    ts,
                })?;
            round1.push(item);
        }
    }

    // Decide the second round with the pure core.
    let missing = select(&round1);
    let two_rounds = if missing.is_empty() { 1 } else { 2 };

    // Round 2: fetch exactly the missing versions by timestamp and
    // overlay them on the round-1 snapshot. PREPARE ordering
    // guarantees the version is present even if it is not yet visible.
    let mut chosen: BTreeMap<Vec<u8>, RampItem> = BTreeMap::new();
    for item in round1 {
        chosen.insert(item.key.clone(), item);
    }
    for (key, ts) in missing {
        let item = store
            .get_version(&key, ts)?
            .ok_or_else(|| RampError::DanglingVersion {
                key: key.clone(),
                ts,
            })?;
        chosen.insert(key, item);
    }

    let snapshot = chosen
        .into_iter()
        .map(|(k, item)| (k, item.value))
        .collect();
    Ok((snapshot, two_rounds))
}

// ------------------------------------------------------------------
// HTTP / JSON data-transfer objects.
// ------------------------------------------------------------------

/// JSON request body for `POST /ramp/transactions` (a RAMP write).
///
/// ```json
/// { "writes": [ {"key": "a", "value": "1"}, {"key": "b", "value": "2"} ] }
/// ```
///
/// Every write is applied as one atomic RAMP-Fast transaction: all
/// keys share one timestamp and each carries the others as sibling
/// metadata, so a concurrent RAMP read sees all of them or none.
#[derive(Clone, Debug, Deserialize)]
pub struct HttpRampWriteRequest {
    /// The key/value writes in this transaction.
    pub writes: Vec<HttpRampWrite>,
}

/// One `(key, value)` write in an [`HttpRampWriteRequest`].
#[derive(Clone, Debug, Deserialize)]
pub struct HttpRampWrite {
    /// Key (UTF-8).
    pub key: String,
    /// Value (UTF-8).
    pub value: String,
}

/// JSON response body for `POST /ramp/transactions`.
#[derive(Clone, Debug, Serialize)]
pub struct HttpRampWriteResponse {
    /// Always `"committed"` on success.
    pub result: String,
    /// The transaction's RAMP timestamp.
    pub ts: Timestamp,
    /// Number of keys written.
    pub keys: usize,
}

/// JSON request body for `POST /ramp/read` (a RAMP read).
///
/// ```json
/// { "keys": ["a", "b"] }
/// ```
#[derive(Clone, Debug, Deserialize)]
pub struct HttpRampReadRequest {
    /// The keys to read atomically.
    pub keys: Vec<String>,
}

/// JSON response body for `POST /ramp/read`.
///
/// The `snapshot` is a fracture-free map of `key -> value`; keys with
/// no committed RAMP write are omitted. `rounds` is 1 (contention
/// free) or 2 (a second round repaired a would-be fractured read).
#[derive(Clone, Debug, Serialize)]
pub struct HttpRampReadResponse {
    /// The atomic snapshot, `key -> value` (UTF-8, lossy).
    pub snapshot: BTreeMap<String, String>,
    /// Number of read rounds the coordinator used (1 or 2).
    pub rounds: u8,
}

impl HttpRampWriteRequest {
    /// Lower the JSON request into the coordinator's write list.
    #[must_use]
    pub fn into_writes(self) -> Vec<RampWrite> {
        self.writes
            .into_iter()
            .map(|w| RampWrite {
                key: w.key.into_bytes(),
                value: w.value.into_bytes(),
            })
            .collect()
    }
}

impl HttpRampReadRequest {
    /// Lower the JSON request into the coordinator's key list.
    #[must_use]
    pub fn into_keys(self) -> Vec<Vec<u8>> {
        self.keys.into_iter().map(String::into_bytes).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    /// In-memory RAMP store for the coordinator unit tests. Models the
    /// version records and pointers directly so the coordinator can be
    /// exercised without a real Noxu environment. It also exposes a
    /// hook to expose a partially-applied write (PREPARE done, COMMIT
    /// pending) for the fractured-read regression test.
    /// A stored version's body: `(siblings, value)`.
    type MemVersion = (Vec<Vec<u8>>, Vec<u8>);

    #[derive(Default)]
    struct MemStore {
        versions: RefCell<HashMap<(Vec<u8>, Timestamp), MemVersion>>,
        pointers: RefCell<HashMap<Vec<u8>, Timestamp>>,
    }

    impl RampStore for MemStore {
        fn put_version(&self, item: &RampItem) -> Result<(), RampError> {
            self.versions.borrow_mut().insert(
                (item.key.clone(), item.ts),
                (item.siblings.clone(), item.value.clone()),
            );
            Ok(())
        }
        fn commit_pointer(&self, key: &[u8], ts: Timestamp) -> Result<(), RampError> {
            let mut p = self.pointers.borrow_mut();
            let e = p.entry(key.to_vec()).or_insert(ts);
            if ts > *e {
                *e = ts;
            }
            Ok(())
        }
        fn latest_visible(&self, key: &[u8]) -> Result<Option<Timestamp>, RampError> {
            Ok(self.pointers.borrow().get(key).copied())
        }
        fn get_version(&self, key: &[u8], ts: Timestamp) -> Result<Option<RampItem>, RampError> {
            Ok(self
                .versions
                .borrow()
                .get(&(key.to_vec(), ts))
                .map(|(sib, val)| RampItem::new(key.to_vec(), ts, sib.clone(), val.clone())))
        }
    }

    fn w(key: &[u8], value: &[u8]) -> RampWrite {
        RampWrite {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    #[test]
    fn body_round_trips() {
        let item = RampItem::new(
            b"a".to_vec(),
            9,
            vec![b"b".to_vec(), b"cc".to_vec()],
            b"hello".to_vec(),
        );
        let bytes = encode_body(&item);
        let (sibs, val) = decode_body(&bytes).expect("decode");
        assert_eq!(sibs, item.siblings);
        assert_eq!(val, item.value);
    }

    #[test]
    fn decode_body_rejects_truncation() {
        assert!(decode_body(&[0, 0, 0]).is_none());
        // Claims one sibling of length 5 but supplies nothing.
        assert!(decode_body(&[0, 0, 0, 1, 0, 0, 0, 5]).is_none());
    }

    #[test]
    fn write_then_read_is_atomic() {
        let mut c = RampCoordinator::new(MemStore::default(), 0);
        c.write(&[w(b"a", b"1"), w(b"b", b"2")]).expect("write");
        let snap = c.read(&[b"a".to_vec(), b"b".to_vec()]).expect("read");
        assert_eq!(snap.get(b"a".as_slice()), Some(&b"1".to_vec()));
        assert_eq!(snap.get(b"b".as_slice()), Some(&b"2".to_vec()));
    }

    #[test]
    fn contention_free_read_is_one_round() {
        let mut c = RampCoordinator::new(MemStore::default(), 0);
        c.write(&[w(b"a", b"1"), w(b"b", b"2")]).expect("write");
        let (_snap, rounds) = c
            .read_with_rounds(&[b"a".to_vec(), b"b".to_vec()])
            .expect("read");
        assert_eq!(rounds, 1, "no concurrency -> single round");
    }

    #[test]
    fn partial_write_triggers_second_round_repair() {
        // A writer that has PREPAREd both keys but only COMMITted `a`
        // (pointer on `b` still names an older version). A reader must
        // NOT see a fractured snapshot: it repairs `b` to the new txn.
        let store = MemStore::default();
        // Pre-existing older values for both keys, from an earlier txn
        // at ts=1 (no siblings visible yet at that point).
        store
            .put_version(&RampItem::new(b"a".to_vec(), 1, vec![], b"a-old".to_vec()))
            .unwrap();
        store
            .put_version(&RampItem::new(b"b".to_vec(), 1, vec![], b"b-old".to_vec()))
            .unwrap();
        store.commit_pointer(b"a", 1).unwrap();
        store.commit_pointer(b"b", 1).unwrap();

        // New txn at ts=5 writes both a and b: PREPARE both, COMMIT
        // only a (b's COMMIT is "in flight").
        store
            .put_version(&RampItem::new(
                b"a".to_vec(),
                5,
                vec![b"b".to_vec()],
                b"a-new".to_vec(),
            ))
            .unwrap();
        store
            .put_version(&RampItem::new(
                b"b".to_vec(),
                5,
                vec![b"a".to_vec()],
                b"b-new".to_vec(),
            ))
            .unwrap();
        store.commit_pointer(b"a", 5).unwrap();
        // b's pointer still names ts=1 -- the partial-apply window.

        let c = RampCoordinator::new(store, 0);
        let (snap, rounds) = c
            .read_with_rounds(&[b"a".to_vec(), b"b".to_vec()])
            .expect("read");
        assert_eq!(rounds, 2, "partial apply must force the second round");
        // Fracture-free: the reader sees BOTH new values, not a@5 + b@1.
        assert_eq!(snap.get(b"a".as_slice()), Some(&b"a-new".to_vec()));
        assert_eq!(snap.get(b"b".as_slice()), Some(&b"b-new".to_vec()));
    }

    #[test]
    fn empty_write_is_rejected() {
        let mut c = RampCoordinator::new(MemStore::default(), 0);
        assert!(matches!(c.write(&[]), Err(RampError::EmptyWrite)));
    }
}
