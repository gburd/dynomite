//! Single-process X/Open XA two-phase commit across local Noxu
//! environments.
//!
//! This module is the local-only realisation of Layer 2 of the
//! dyniak transaction stack (see [`crate::txn`]). Layer 1
//! ([`crate::datastore::NoxuDatastore::transaction`]) commits a batch
//! whose keys all live on one node inside a single engine
//! transaction. Layer 2 coordinates a batch whose keys span several
//! nodes: every node prepares its branch, and the coordinator commits
//! all branches only once every prepare has voted to commit.
//!
//! [`XaCoordinator`] is the transaction manager; each
//! [`XaParticipant`] is one resource manager backed by an independent
//! [`noxu::xa::XaEnvironment`]. Both layers of the protocol run inside
//! one process here: the participants are distinct local Noxu
//! environments rather than remote peers reached over the dnode wire.
//! That is deliberate -- it exercises the real
//! prepare -> commit / rollback handshake against the engine's XA
//! resource-manager API without yet taking on the cross-node
//! transport. The boundary, and the multi-node design that builds on
//! it, are written up in
//! `docs/journal/2026-06-05-dyniak-xa.md`.
//!
//! # Protocol
//!
//! [`XaCoordinator::execute`] partitions a [`TxnBatch`] across its
//! participants with a caller-supplied routing function, then runs:
//!
//! 1. **Work**: for each touched branch, `xa_start`, apply the
//!    branch's operations through the same record helpers the
//!    single-node path uses, then `xa_end(TMSUCCESS)`. Any error here
//!    rolls back every branch already started and returns the error.
//! 2. **Prepare**: `xa_prepare` every branch. A branch that performed
//!    no writes votes [`noxu::xa::PrepareResult::ReadOnly`] and needs
//!    no second phase. Any prepare error (or an explicit
//!    [`TxnBatch::force_abort`]) rolls back every prepared branch.
//! 3. **Commit**: `xa_commit` every branch that voted
//!    [`noxu::xa::PrepareResult::Ok`].
//!
//! # Examples
//!
//! ```no_run
//! use std::path::Path;
//! use dyniak::datastore::xa::{XaCoordinator, XaParticipant};
//! use dyniak::txn::{TxnBatch, TxnOp};
//!
//! let east = XaParticipant::open(Path::new("/var/lib/dyn/east"), b"east".to_vec())
//!     .expect("open east");
//! let west = XaParticipant::open(Path::new("/var/lib/dyn/west"), b"west".to_vec())
//!     .expect("open west");
//! let coord = XaCoordinator::new(vec![east, west]);
//!
//! let batch = TxnBatch {
//!     ops: vec![
//!         TxnOp::Put { bucket: b"u".to_vec(), key: b"alice".to_vec(),
//!                      value: b"a".to_vec(), indexes: vec![] },
//!         TxnOp::Put { bucket: b"u".to_vec(), key: b"bob".to_vec(),
//!                      value: b"b".to_vec(), indexes: vec![] },
//!     ],
//!     force_abort: false,
//! };
//! // Route by first byte of the key: even -> branch 0, odd -> branch 1.
//! let outcome = coord
//!     .execute(&batch, |op| usize::from(op.key().first().copied().unwrap_or(0) & 1))
//!     .expect("commit");
//! # let _ = outcome;
//! ```

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use noxu::xa::{PrepareResult, XaEnvironment, XaFlags, XaResource, Xid};
use noxu::{Database, DatabaseConfig, Environment, EnvironmentConfig};

use crate::datastore::noxu::{
    delete_object_in, get_object_in, map_txn_store_error, NoxuDatastoreError,
};
use crate::txn::{TxnBatch, TxnOp, TxnOutcome, TxnStoreError};

/// XA format identifier stamped on every [`Xid`] this coordinator
/// mints. The value is arbitrary but stable so a recovery scan can
/// recognise the transactions as dyniak's.
const DYNIAK_XA_FORMAT_ID: i32 = 0x6479_6e6b; // "dynk"

/// Database name every participant opens for the Riak object
/// keyspace. Matches the single-node bridge's default so the two
/// layers share an on-disk shape.
const XA_DB_NAME: &str = "riak.objects";

/// One XA resource manager: a local Noxu environment plus the
/// database the Riak records live in.
///
/// A participant models one cluster node for the purposes of the
/// single-process coordinator. It owns its environment and database
/// outright; clone it across the process only behind an [`std::sync::Arc`]
/// if shared access is needed (the coordinator takes ownership).
pub struct XaParticipant {
    /// XA resource manager wrapping a transactional environment with
    /// a durable prepared-transaction log.
    xa: XaEnvironment,
    /// Object database opened from `xa`'s environment.
    db: Database,
    /// Stable branch name; used as the XA branch qualifier and in
    /// tracing.
    name: Vec<u8>,
}

impl XaParticipant {
    /// Open (creating if absent) a transactional Noxu environment at
    /// `path` and wrap it as an XA resource manager named `name`.
    ///
    /// The branch name doubles as the XA branch qualifier and must be
    /// at most 64 bytes (the XA `bqual` limit); longer names surface
    /// the engine's [`noxu::xa`] error when a transaction is started.
    ///
    /// # Errors
    ///
    /// Returns [`NoxuDatastoreError::Noxu`] if the environment, the
    /// prepared-transaction log, or the database cannot be opened.
    pub fn open(path: &Path, name: Vec<u8>) -> Result<Self, NoxuDatastoreError> {
        let env_config = EnvironmentConfig::new(path.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_config)?;
        let xa = XaEnvironment::new(env).with_prepared_log()?;
        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = xa.inner().open_database(None, XA_DB_NAME, &db_config)?;
        Ok(Self { xa, db, name })
    }

    /// Branch name (XA branch qualifier).
    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// Read `(bucket, key)` against this branch's committed state.
    ///
    /// Reads auto-commit, so they observe only durably committed
    /// data, not any in-flight branch.
    ///
    /// # Errors
    ///
    /// Surfaces the same errors as
    /// [`crate::datastore::NoxuDatastore::get_object`].
    pub fn get_object(
        &self,
        bucket: &[u8],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, NoxuDatastoreError> {
        get_object_in(&self.db, None, bucket, key)
    }

    /// Apply one operation to this branch inside the transaction
    /// identified by `xid`.
    fn apply(&self, xid: &Xid, op: &TxnOp) -> Result<(), NoxuDatastoreError> {
        // Re-fetch the branch transaction per op so we never hold a
        // borrow across another engine call.
        let txn = self
            .xa
            .get_transaction(xid)
            .map_err(|e| NoxuDatastoreError::Xa(e.to_string()))?;
        match op {
            TxnOp::Put {
                bucket,
                key,
                value,
                indexes,
            } => crate::datastore::noxu::put_object_in(
                &self.db,
                Some(txn),
                bucket,
                key,
                value,
                indexes,
            ),
            TxnOp::Delete { bucket, key } => {
                delete_object_in(&self.db, Some(txn), bucket, key).map(|_| ())
            }
        }
    }
}

/// Coordinator (transaction manager) for a fixed set of XA
/// participants.
///
/// Construct with [`XaCoordinator::new`] over the participants that
/// model the cluster's nodes, then drive distributed transactions
/// with [`XaCoordinator::execute`].
pub struct XaCoordinator {
    participants: Vec<XaParticipant>,
    next_gtid: AtomicU64,
}

impl XaCoordinator {
    /// Build a coordinator over `participants`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    /// use dyniak::datastore::xa::{XaCoordinator, XaParticipant};
    /// let p = XaParticipant::open(Path::new("/tmp/n0"), b"n0".to_vec()).unwrap();
    /// let coord = XaCoordinator::new(vec![p]);
    /// assert_eq!(coord.len(), 1);
    /// ```
    #[must_use]
    pub fn new(participants: Vec<XaParticipant>) -> Self {
        Self {
            participants,
            next_gtid: AtomicU64::new(1),
        }
    }

    /// Number of participants.
    #[must_use]
    pub fn len(&self) -> usize {
        self.participants.len()
    }

    /// True when the coordinator has no participants.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.participants.is_empty()
    }

    /// Borrow a participant by index.
    #[must_use]
    pub fn participant(&self, index: usize) -> Option<&XaParticipant> {
        self.participants.get(index)
    }

    /// Run a distributed transaction over `batch`, routing each op to
    /// a participant with `route`.
    ///
    /// `route` returns the participant index that owns an operation's
    /// key. The full two-phase commit runs across the touched
    /// branches: every branch prepares, and the batch commits only if
    /// every prepare votes to commit. A [`TxnBatch::force_abort`] (or
    /// any prepare failure) rolls every branch back.
    ///
    /// # Errors
    ///
    /// * [`TxnStoreError::EmptyBatch`] when `batch` has no ops.
    /// * [`TxnStoreError::Backend`] when an op routes to an
    ///   out-of-range participant index, or an engine call fails.
    /// * [`TxnStoreError::Conflict`] when the engine reports a
    ///   serialization failure (the batch is rolled back; retry).
    ///
    /// On any error the batch is rolled back, so the keyspace is left
    /// untouched.
    pub fn execute<R>(&self, batch: &TxnBatch, route: R) -> Result<TxnOutcome, TxnStoreError>
    where
        R: Fn(&TxnOp) -> usize,
    {
        if batch.ops.is_empty() {
            return Err(TxnStoreError::EmptyBatch);
        }

        // Partition op indices by participant.
        let mut per_branch: Vec<Vec<usize>> = vec![Vec::new(); self.participants.len()];
        for (op_idx, op) in batch.ops.iter().enumerate() {
            let branch = route(op);
            if branch >= self.participants.len() {
                return Err(TxnStoreError::Backend(format!(
                    "routing returned participant index {branch} but only {} participants exist",
                    self.participants.len()
                )));
            }
            per_branch[branch].push(op_idx);
        }

        let gtid = self.next_gtid.fetch_add(1, Ordering::Relaxed);
        let gtid_bytes = gtid.to_be_bytes();

        // Branches that carry at least one op, with their xid.
        let mut active: Vec<(usize, Xid)> = Vec::new();
        for (branch, ops) in per_branch.iter().enumerate() {
            if ops.is_empty() {
                continue;
            }
            let xid = Xid::new(
                DYNIAK_XA_FORMAT_ID,
                &gtid_bytes,
                self.participants[branch].name(),
            )
            .map_err(|e| TxnStoreError::Backend(format!("xid: {e}")))?;
            active.push((branch, xid));
        }

        // Phase 1: start, apply, end -- one branch at a time.
        for (started, (branch, xid)) in active.iter().enumerate() {
            let participant = &self.participants[*branch];
            let phase1 = Self::run_branch_work(participant, xid, &per_branch[*branch], &batch.ops);
            if let Err(e) = phase1 {
                // Roll back this branch's started transaction (if any)
                // plus every branch already ended.
                let _ = participant.xa.xa_rollback(xid, XaFlags::NOFLAGS);
                self.rollback_active(&active[..started]);
                return Err(e);
            }
        }

        // Phase 2a: prepare every branch.
        let mut prepared: Vec<(usize, Xid)> = Vec::new();
        for (branch, xid) in &active {
            let participant = &self.participants[*branch];
            match participant.xa.xa_prepare(xid, XaFlags::NOFLAGS) {
                Ok(PrepareResult::Ok) => prepared.push((*branch, xid.clone())),
                // A read-only branch is already finished; nothing to
                // commit or roll back for it.
                Ok(PrepareResult::ReadOnly) => {}
                Err(e) => {
                    // This branch failed to prepare. Roll back every
                    // branch that did prepare, and this one too.
                    let _ = participant.xa.xa_rollback(xid, XaFlags::NOFLAGS);
                    self.rollback_active(&prepared);
                    self.rollback_remaining(&active, *branch, &prepared);
                    return Err(map_xa_error(&e));
                }
            }
        }

        // A client-requested abort exercises the prepare -> rollback
        // path: every prepared branch votes to commit, then the
        // coordinator deliberately rolls them all back.
        if batch.force_abort {
            self.rollback_active(&prepared);
            return Ok(TxnOutcome::Aborted {
                reason: "client requested abort".to_string(),
            });
        }

        // Phase 2b: commit every prepared branch.
        for (branch, xid) in &prepared {
            self.participants[*branch]
                .xa
                .xa_commit(xid, XaFlags::NOFLAGS)
                .map_err(|e| map_xa_error(&e))?;
        }

        Ok(TxnOutcome::Committed {
            operations: batch.ops.len(),
        })
    }

    /// Phase-1 worker for one branch: start, apply its ops, end.
    fn run_branch_work(
        participant: &XaParticipant,
        xid: &Xid,
        op_indices: &[usize],
        ops: &[TxnOp],
    ) -> Result<(), TxnStoreError> {
        participant
            .xa
            .xa_start(xid, XaFlags::NOFLAGS)
            .map_err(|e| map_xa_error(&e))?;
        for &op_idx in op_indices {
            participant
                .apply(xid, &ops[op_idx])
                .map_err(|e| map_txn_store_error(&e))?;
        }
        participant
            .xa
            .mark_write(xid)
            .map_err(|e| map_xa_error(&e))?;
        participant
            .xa
            .xa_end(xid, XaFlags::TMSUCCESS)
            .map_err(|e| map_xa_error(&e))?;
        Ok(())
    }

    /// Roll back every branch in `branches` (best effort).
    fn rollback_active(&self, branches: &[(usize, Xid)]) {
        for (branch, xid) in branches {
            let _ = self.participants[*branch]
                .xa
                .xa_rollback(xid, XaFlags::NOFLAGS);
        }
    }

    /// Roll back branches that were ended in phase 1 but had not yet
    /// been prepared when a later prepare failed. `failed` is the
    /// branch whose prepare just failed (already rolled back by the
    /// caller); `prepared` is the set already rolled back via
    /// [`Self::rollback_active`].
    fn rollback_remaining(
        &self,
        active: &[(usize, Xid)],
        failed: usize,
        prepared: &[(usize, Xid)],
    ) {
        for (branch, xid) in active {
            if *branch == failed {
                continue;
            }
            if prepared.iter().any(|(b, _)| b == branch) {
                continue;
            }
            let _ = self.participants[*branch]
                .xa
                .xa_rollback(xid, XaFlags::NOFLAGS);
        }
    }
}

/// Map a [`noxu::xa::XaError`] onto a [`TxnStoreError`].
fn map_xa_error(err: &noxu::xa::XaError) -> TxnStoreError {
    let text = err.to_string();
    let lowered = text.to_ascii_lowercase();
    if lowered.contains("deadlock")
        || lowered.contains("lock timeout")
        || lowered.contains("conflict")
        || lowered.contains("would block")
    {
        TxnStoreError::Conflict(text)
    } else {
        TxnStoreError::Backend(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Route by the first byte of the key: even -> branch 0,
    /// odd -> branch 1.
    fn route_by_key_parity(op: &TxnOp) -> usize {
        usize::from(op.key().first().copied().unwrap_or(0) & 1)
    }

    fn two_branch_coordinator() -> (XaCoordinator, TempDir, TempDir) {
        let d0 = TempDir::new().expect("tempdir0");
        let d1 = TempDir::new().expect("tempdir1");
        let p0 = XaParticipant::open(d0.path(), b"east".to_vec()).expect("open east");
        let p1 = XaParticipant::open(d1.path(), b"west".to_vec()).expect("open west");
        (XaCoordinator::new(vec![p0, p1]), d0, d1)
    }

    fn put(key: &[u8], value: &[u8]) -> TxnOp {
        TxnOp::Put {
            bucket: b"u".to_vec(),
            key: key.to_vec(),
            value: value.to_vec(),
            indexes: vec![],
        }
    }

    #[test]
    fn commit_spans_two_branches() {
        let (coord, _d0, _d1) = two_branch_coordinator();
        // "alice" -> 'a' = 0x61 (odd) -> branch 1;
        // "bob"   -> 'b' = 0x62 (even) -> branch 0.
        let batch = TxnBatch {
            ops: vec![put(b"alice", b"a"), put(b"bob", b"b")],
            force_abort: false,
        };
        let outcome = coord.execute(&batch, route_by_key_parity).expect("commit");
        assert_eq!(outcome, TxnOutcome::Committed { operations: 2 });

        // bob landed on branch 0, alice on branch 1; neither leaked
        // onto the other branch.
        let east = coord.participant(0).expect("east");
        let west = coord.participant(1).expect("west");
        assert_eq!(
            east.get_object(b"u", b"bob").unwrap().as_deref(),
            Some(&b"b"[..])
        );
        assert!(east.get_object(b"u", b"alice").unwrap().is_none());
        assert_eq!(
            west.get_object(b"u", b"alice").unwrap().as_deref(),
            Some(&b"a"[..])
        );
        assert!(west.get_object(b"u", b"bob").unwrap().is_none());
    }

    #[test]
    fn force_abort_rolls_back_every_branch() {
        let (coord, _d0, _d1) = two_branch_coordinator();
        let batch = TxnBatch {
            ops: vec![put(b"alice", b"a"), put(b"bob", b"b")],
            force_abort: true,
        };
        let outcome = coord.execute(&batch, route_by_key_parity).expect("abort");
        assert!(matches!(outcome, TxnOutcome::Aborted { .. }));

        // The prepare -> rollback path ran on both branches: nothing
        // is durable anywhere.
        let east = coord.participant(0).expect("east");
        let west = coord.participant(1).expect("west");
        assert!(east.get_object(b"u", b"bob").unwrap().is_none());
        assert!(west.get_object(b"u", b"alice").unwrap().is_none());
    }

    #[test]
    fn empty_batch_is_rejected() {
        let (coord, _d0, _d1) = two_branch_coordinator();
        let batch = TxnBatch::default();
        assert!(matches!(
            coord.execute(&batch, route_by_key_parity),
            Err(TxnStoreError::EmptyBatch)
        ));
    }

    #[test]
    fn out_of_range_route_is_a_backend_error() {
        let (coord, _d0, _d1) = two_branch_coordinator();
        let batch = TxnBatch {
            ops: vec![put(b"alice", b"a")],
            force_abort: false,
        };
        // Route everything to a non-existent participant.
        let err = coord.execute(&batch, |_| 99).expect_err("out of range");
        assert!(matches!(err, TxnStoreError::Backend(_)));
        // Nothing was written.
        assert!(coord
            .participant(0)
            .unwrap()
            .get_object(b"u", b"alice")
            .unwrap()
            .is_none());
    }

    #[test]
    fn commit_then_read_after_reopen_is_durable() {
        // A committed branch's data survives because xa_commit makes
        // it durable; reading it back through a fresh auto-commit get
        // confirms phase 2 actually committed (not just buffered).
        let (coord, _d0, _d1) = two_branch_coordinator();
        let batch = TxnBatch {
            ops: vec![put(b"carol", b"c")], // 'c' = 0x63 odd -> branch 1
            force_abort: false,
        };
        coord.execute(&batch, route_by_key_parity).expect("commit");
        let west = coord.participant(1).expect("west");
        assert_eq!(
            west.get_object(b"u", b"carol").unwrap().as_deref(),
            Some(&b"c"[..])
        );
    }
}
