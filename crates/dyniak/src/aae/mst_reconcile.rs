//! Divergence-proportional anti-entropy reconcile via a Merkle
//! Search Tree (MST).
//!
//! The shipped Tictac path (see [`crate::aae::tictac`] and
//! [`crate::aae::exchange`]) compares two peers by walking a
//! fixed segment grid: a single differing key dirties its whole
//! segment, and the walk still touches every segment root, so
//! reconcile cost grows with the dataset size. This module is
//! the alternative: it builds a [`hashtree::mst::Mst`] over the
//! local key set (via the same noxu storage fold the Tictac
//! rebuild uses) and diffs it against a peer's MST, transferring
//! work proportional to the *symmetric difference* of the two
//! key sets, not their total size.
//!
//! # How it plugs in
//!
//! The MST reconcile is an alternative behind the existing AAE
//! machinery, selected by [`crate::aae::config::ConfAae::reconcile_mode`].
//! It does not replace the Tictac tree: when the mode is
//! [`crate::aae::config::ReconcileMode::Tictac`] (the default)
//! nothing here runs, so a deployment that has not opted in is
//! byte-for-byte unaffected, and a delta-shipping hook added to
//! the shared exchange path composes cleanly.
//!
//! The flat MST keyspace maps a `(bucket, key)` pair to a single
//! composite key `bucket || 0x00 || key`; the value side is a
//! `blake3(value)` content digest, so a value change flips the
//! MST entry exactly as a `(bucket, key, vclock)` change flips a
//! Tictac leaf.
//!
//! # Reconcile protocol
//!
//! 1. Both peers build their MST from local storage.
//! 2. The initiator diffs its MST against the peer's MST
//!    ([`hashtree::mst::Mst::diff`]), yielding the exact keys
//!    that are present-or-differing on one side only, at a cost
//!    that scales with the divergence.
//! 3. For each key the peer has that the local side lacks (or
//!    holds at a different value), the local side fetches the
//!    object and applies it. The symmetric case is what the peer
//!    does in its own sweep; a single exchange run repairs one
//!    direction, and the pair converges after both directions
//!    run (the AAE scheduler already drives both peers).
//!
//! Gated behind the `noxu` feature: the fetch/apply repair path
//! needs a concrete [`NoxuDatastore`].

use hashtree::mst::{value_hash, Mst, MstDiff};
use hashtree::Hash;

use crate::datastore::noxu::{NoxuDatastore, NoxuDatastoreError};

/// Separator byte between bucket and key in the composite MST
/// key. NUL is rejected inside bucket names by the datastore, so
/// it cannot appear inside a bucket and the split is
/// unambiguous.
const COMPOSITE_SEP: u8 = 0x00;

/// Errors raised by the MST reconcile path.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MstReconcileError {
    /// The underlying datastore fold, fetch, or apply failed.
    #[error("mst reconcile datastore: {0}")]
    Datastore(#[from] NoxuDatastoreError),
    /// A composite MST key could not be split back into
    /// `(bucket, key)`; indicates a corrupt or foreign key on
    /// the peer's side.
    #[error("mst reconcile: malformed composite key")]
    MalformedKey,
}

/// Encode a `(bucket, key)` pair into the flat MST composite
/// key `bucket || 0x00 || key`.
#[must_use]
pub fn composite_key(bucket: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bucket.len() + 1 + key.len());
    out.extend_from_slice(bucket);
    out.push(COMPOSITE_SEP);
    out.extend_from_slice(key);
    out
}

/// Split a composite MST key back into `(bucket, key)`.
///
/// # Errors
/// [`MstReconcileError::MalformedKey`] when no separator byte is
/// present.
pub fn split_composite(composite: &[u8]) -> Result<(&[u8], &[u8]), MstReconcileError> {
    let idx = composite
        .iter()
        .position(|b| *b == COMPOSITE_SEP)
        .ok_or(MstReconcileError::MalformedKey)?;
    Ok((&composite[..idx], &composite[idx + 1..]))
}

/// Content digest of a stored value, used as the MST value side.
/// Two peers holding the same bytes for a key produce the same
/// digest and so agree on that key without transferring the
/// value; a byte difference flips the digest and surfaces the
/// key in the diff.
#[must_use]
pub fn value_digest(value: &[u8]) -> Hash {
    value_hash(value)
}

/// Build an [`Mst`] over the datastore's primary records by
/// walking storage in key order (the same fold the Tictac
/// rebuild uses).
///
/// # Errors
/// Surfaces [`MstReconcileError::Datastore`] when the cursor
/// walk fails.
pub fn build_mst(db: &NoxuDatastore) -> Result<Mst, MstReconcileError> {
    let mut pairs: Vec<(Vec<u8>, Hash)> = Vec::new();
    db.fold_primary(|bucket, key, value| {
        pairs.push((composite_key(bucket, key), value_digest(value)));
        Ok(())
    })?;
    Ok(Mst::from_pairs(pairs))
}

/// Outcome of one MST reconcile run in a single direction.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReconcileOutcome {
    /// Number of objects fetched from the peer and applied
    /// locally.
    pub applied: usize,
    /// Number of keys the peer must fetch from this side to
    /// converge (surfaced for the caller to drive the reverse
    /// direction / metrics; not applied here).
    pub to_push: usize,
    /// Node-hash comparisons the diff walk made. The
    /// divergence-proportional cost metric.
    pub comparisons: usize,
}

/// A source of objects the local side can fetch during repair.
/// In production this is the peer connection; in tests it is a
/// second in-memory datastore. Keeping it a trait keeps the
/// reconcile logic testable without a wire mock.
pub trait ObjectSource {
    /// Fetch the object bytes stored under `(bucket, key)` on the
    /// peer, or `None` if the peer no longer has it.
    ///
    /// # Errors
    /// Implementation-defined; surfaced verbatim to the caller.
    fn fetch(&self, bucket: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, MstReconcileError>;
}

/// An [`ObjectSource`] backed by a borrowed [`NoxuDatastore`]
/// (the in-process test / same-host peer case).
pub struct DatastoreSource<'a> {
    db: &'a NoxuDatastore,
}

impl<'a> DatastoreSource<'a> {
    /// Wrap a borrowed datastore as an object source.
    #[must_use]
    pub fn new(db: &'a NoxuDatastore) -> Self {
        Self { db }
    }
}

impl ObjectSource for DatastoreSource<'_> {
    fn fetch(&self, bucket: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, MstReconcileError> {
        Ok(self.db.get_object(bucket, key)?)
    }
}

/// Run the divergence-proportional reconcile against a peer's
/// MST, in the pull direction: fetch every object the peer has
/// that the local side lacks or holds at a different value, and
/// apply it locally.
///
/// The `local` MST is the diff base; `peer` is the peer's tree
/// (fetched over the wire, or built from a second local store in
/// tests). `source` supplies the object bytes for the keys the
/// diff says to pull.
///
/// Returns the [`ReconcileOutcome`] with the applied count and
/// the divergence-proportional comparison cost. The reverse
/// direction (keys the peer must pull from us) is reported in
/// [`ReconcileOutcome::to_push`] but not applied here; the
/// scheduler drives the peer's own sweep for that.
///
/// # Errors
/// Surfaces [`MstReconcileError`] from the fetch or the local
/// apply.
pub fn reconcile_pull<S: ObjectSource>(
    local_db: &NoxuDatastore,
    local: &Mst,
    peer: &Mst,
    source: &S,
) -> Result<ReconcileOutcome, MstReconcileError> {
    let diff: MstDiff = local.diff(peer);
    let mut applied = 0usize;
    for composite in diff.only_there() {
        let (bucket, key) = split_composite(composite)?;
        if let Some(value) = source.fetch(bucket, key)? {
            // Apply with no 2i entries: the AAE repair path
            // reconciles the primary object body; 2i is derived
            // from the object and rebuilt by the datastore's own
            // put fan-out. Keeping the repair 2i-free matches the
            // Tictac repair, which also reconciles the primary
            // record only.
            local_db.put_object(bucket, key, &value, &[])?;
            applied += 1;
        }
    }
    Ok(ReconcileOutcome {
        applied,
        to_push: diff.only_here().len(),
        comparisons: diff.comparisons(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_ds() -> (TempDir, NoxuDatastore) {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        (dir, ds)
    }

    #[test]
    fn composite_round_trips() {
        let c = composite_key(b"users", b"alice");
        let (b, k) = split_composite(&c).expect("split");
        assert_eq!(b, b"users");
        assert_eq!(k, b"alice");
    }

    #[test]
    fn identical_stores_have_equal_mst_roots() {
        let (_da, a) = open_ds();
        let (_db, b) = open_ds();
        for i in 0..1000u32 {
            let k = format!("k{i:06}");
            let v = format!("v{i}");
            a.put_object(b"users", k.as_bytes(), v.as_bytes(), &[])
                .expect("put a");
            b.put_object(b"users", k.as_bytes(), v.as_bytes(), &[])
                .expect("put b");
        }
        let ma = build_mst(&a).expect("build a");
        let mb = build_mst(&b).expect("build b");
        assert_eq!(ma.root(), mb.root());
        assert_eq!(ma.diff(&mb).diff_len(), 0);
    }

    #[test]
    fn reconcile_pulls_missing_objects_and_converges() {
        let (_da, a) = open_ds();
        let (_db, b) = open_ds();
        // Shared keys 0..900.
        for i in 0..900u32 {
            let k = format!("k{i:06}");
            let v = format!("v{i}");
            a.put_object(b"users", k.as_bytes(), v.as_bytes(), &[])
                .expect("put a");
            b.put_object(b"users", k.as_bytes(), v.as_bytes(), &[])
                .expect("put b");
        }
        // b has 100 extra keys a lacks.
        for i in 900..1000u32 {
            let k = format!("k{i:06}");
            let v = format!("v{i}");
            b.put_object(b"users", k.as_bytes(), v.as_bytes(), &[])
                .expect("put b extra");
        }

        let ma = build_mst(&a).expect("build a");
        let mb = build_mst(&b).expect("build b");
        let source = DatastoreSource::new(&b);
        let outcome = reconcile_pull(&a, &ma, &mb, &source).expect("reconcile");
        assert_eq!(outcome.applied, 100, "a should pull 100 missing objects");

        // After the pull, a's MST must match b's.
        let ma2 = build_mst(&a).expect("rebuild a");
        assert_eq!(ma2.root(), mb.root(), "reconcile must converge roots");
        assert_eq!(ma2.diff(&mb).diff_len(), 0);
    }

    #[test]
    fn reconcile_is_divergence_proportional() {
        let (_da, a) = open_ds();
        let (_db, b) = open_ds();
        // 10000 shared keys, 20 divergent (b has newer values).
        for i in 0..10000u32 {
            let k = format!("k{i:06}");
            let v = format!("v{i}");
            a.put_object(b"users", k.as_bytes(), v.as_bytes(), &[])
                .expect("put a");
            b.put_object(b"users", k.as_bytes(), v.as_bytes(), &[])
                .expect("put b");
        }
        for i in 0..20u32 {
            let k = format!("k{i:06}");
            b.put_object(b"users", k.as_bytes(), b"CHANGED", &[])
                .expect("update b");
        }
        let ma = build_mst(&a).expect("build a");
        let mb = build_mst(&b).expect("build b");
        let source = DatastoreSource::new(&b);
        let outcome = reconcile_pull(&a, &ma, &mb, &source).expect("reconcile");
        assert_eq!(outcome.applied, 20);
        // Cost must be far below the dataset size.
        assert!(
            outcome.comparisons < 2000,
            "reconcile not divergence-proportional: {} comparisons for 20 diffs / 10000 keys",
            outcome.comparisons
        );
        let ma2 = build_mst(&a).expect("rebuild a");
        assert_eq!(ma2.root(), mb.root());
    }
}
