//! Cross-node leg of the XA two-phase commit.
//!
//! The local coordinator in [`crate::datastore::xa`] drives the XA
//! phases against in-process resource managers. This module carries
//! the identical phases to a *remote* peer over the dnode peer plane
//! and resolves the resulting failure modes (presumed-abort prepare,
//! commit-in-doubt forward recovery) that only arise once a branch
//! lives on the far side of a network.
//!
//! The pieces:
//!
//! * [`XaTransport`] -- the seam. One method per phase; the
//!   coordinator awaits each. The production impl frames the phase as
//!   a dnode message and round-trips it to the owning peer; tests
//!   inject a mock to exercise abort / timeout paths without TCP.
//! * [`RemoteXaBranch`] -- a branch reached through an
//!   [`XaTransport`].
//! * [`XaBranch`] -- `Local(XaParticipant)` or `Remote(RemoteXaBranch)`;
//!   the cross-node coordinator drives a mix of both.
//! * [`XaPeer`] -- the receiver. Owns the local resource managers and
//!   turns an inbound prepare / commit / rollback into the right
//!   `noxu` XA call, idempotently.
//! * [`InDoubtLog`] -- a durable append-only record of branches that
//!   voted Ok but whose commit could not be confirmed, so a recovery
//!   pass can drive them forward.
//! * [`CrossNodeCoordinator`] -- the async coordinator that runs the
//!   protocol over [`XaBranch`]es and, on a cold restart, re-drives
//!   any logged in-doubt commits with
//!   [`CrossNodeCoordinator::recover_in_doubt`].
//!
//! # Failure model (presumed abort, forward commit)
//!
//! * A prepare-phase timeout or transport error is an abort vote:
//!   every branch that may have prepared is rolled back.
//! * Once every branch has voted Ok the transaction is *committed*.
//!   A commit-phase failure to reach a peer is **not** an abort: the
//!   branch is durably prepared on the peer, so the only correct
//!   resolution is forward. The coordinator retries the commit with
//!   bounded backoff; if it still cannot confirm, it records the
//!   branch in the [`InDoubtLog`] for a later recovery pass and
//!   surfaces an in-doubt error to the caller. It never rolls back a
//!   branch that voted Ok in the commit phase.
//!
//! # Cold-restart recovery scan
//!
//! The durable in-doubt log and the bounded commit retry resolve
//! transient peer unavailability *within* one coordinator run. A
//! coordinator that itself restarts recovers any still-unconfirmed
//! commits with [`CrossNodeCoordinator::recover_in_doubt`]: it reads
//! the records back with [`InDoubtLog::load`] and re-drives each
//! commit over the same transport path phase 2 uses. Because the peer
//! commits idempotently, re-driving a commit that already landed is a
//! safe no-op; a confirmed commit retires its record (a tombstone),
//! and a record whose peer is still down stays in the log for a later,
//! re-runnable pass. Recovery only ever drives a prepared branch
//! *forward* (presumed commit); it never rolls one back.
//! [`CrossNodeCoordinator::new_with_recovery`] runs the scan at
//! construction for the server's boot path, while
//! [`CrossNodeCoordinator::new`] keeps a non-scanning constructor for
//! in-memory and test coordinators.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use noxu::xa::{PrepareResult, XaError, XaFlags, XaResource, Xid};

use crate::datastore::xa::{XaParticipant, DYNIAK_XA_FORMAT_ID};
use crate::datastore::xa_wire::{WireXid, XaVote, XaWriteOp};
use crate::txn::{TxnBatch, TxnOp, TxnOutcome, TxnStoreError};

/// Future returned by [`XaTransport`] methods.
pub type XaFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Errors a transport can surface while carrying an XA phase to a
/// peer.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum XaTransportError {
    /// The peer did not answer within the phase deadline.
    #[error("xa transport timeout")]
    Timeout,
    /// The connection failed or the peer replied with a malformed
    /// frame.
    #[error("xa transport: {0}")]
    Transport(String),
}

/// Transport seam for a remote XA branch.
///
/// Each method carries one phase to the peer that owns the branch
/// and awaits its reply. Implementors frame the call as a dnode
/// message (see [`crate::datastore::xa_wire`]); the coordinator does
/// not know or care how the bytes travel.
pub trait XaTransport: Send + Sync {
    /// Carry prepare to the peer: deliver `writes` for the branch and
    /// await its vote. `env` names the resource manager that owns the
    /// branch on the receiver.
    fn prepare<'a>(
        &'a self,
        xid: &'a WireXid,
        env: &'a [u8],
        writes: &'a [XaWriteOp],
    ) -> XaFuture<'a, Result<XaVote, XaTransportError>>;

    /// Carry commit to the peer and await its ack. Must be safe to
    /// retry (the receiver commits idempotently).
    fn commit<'a>(
        &'a self,
        xid: &'a WireXid,
        env: &'a [u8],
    ) -> XaFuture<'a, Result<(), XaTransportError>>;

    /// Carry rollback to the peer and await its ack. Must be safe to
    /// retry (the receiver rolls back idempotently).
    fn rollback<'a>(
        &'a self,
        xid: &'a WireXid,
        env: &'a [u8],
    ) -> XaFuture<'a, Result<(), XaTransportError>>;
}

/// A transaction branch reached over an [`XaTransport`].
pub struct RemoteXaBranch {
    transport: Arc<dyn XaTransport>,
    env: Vec<u8>,
}

impl RemoteXaBranch {
    /// Build a remote branch backed by `transport`, owning the
    /// resource manager named `env` on the peer.
    #[must_use]
    pub fn new(transport: Arc<dyn XaTransport>, env: Vec<u8>) -> Self {
        Self { transport, env }
    }

    /// Branch name (the owning environment's name, used as the XA
    /// branch qualifier).
    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.env
    }
}

/// One participant in a cross-node transaction: either a local
/// resource manager or a remote one reached over the wire.
///
/// The cross-node coordinator drives a `Vec<XaBranch>`; the
/// `Local` arm reuses the existing [`XaParticipant`] verbatim, so a
/// transaction can span the coordinator's own node and any number of
/// peers in one protocol run.
pub enum XaBranch {
    /// In-process resource manager. Boxed so the enum does not carry
    /// the full [`XaParticipant`] inline next to the small
    /// [`RemoteXaBranch`] variant.
    Local(Box<XaParticipant>),
    /// Resource manager on a peer.
    Remote(RemoteXaBranch),
}

impl XaBranch {
    /// Branch name (XA branch qualifier).
    #[must_use]
    pub fn name(&self) -> &[u8] {
        match self {
            Self::Local(p) => p.name(),
            Self::Remote(r) => r.name(),
        }
    }
}

/// Convert a [`noxu::xa::Xid`] to its portable wire form.
fn wire_xid(xid: &Xid) -> WireXid {
    WireXid {
        format_id: xid.format_id,
        gtrid: xid.global_transaction_id.clone(),
        bqual: xid.branch_qualifier.clone(),
    }
}

/// Receiver-side handler: owns the local resource managers a peer
/// hosts and turns inbound XA phases into `noxu` XA calls.
///
/// One [`XaParticipant`] per environment name. Commit and rollback
/// are idempotent: a retry for an `Xid` the peer has already resolved
/// returns success rather than an error, because `noxu`'s `xa_commit`
/// / `xa_rollback` report [`XaError::NotFound`] for an already-removed
/// branch and we treat that as "already done".
pub struct XaPeer {
    participants: Vec<(Vec<u8>, XaParticipant)>,
    next_reply_id: std::sync::atomic::AtomicU64,
}

impl XaPeer {
    /// Build a peer over `(env_name, participant)` pairs.
    #[must_use]
    pub fn new(participants: Vec<(Vec<u8>, XaParticipant)>) -> Self {
        Self {
            participants,
            next_reply_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    fn participant(&self, env: &[u8]) -> Option<&XaParticipant> {
        self.participants
            .iter()
            .find(|(name, _)| name.as_slice() == env)
            .map(|(_, p)| p)
    }

    /// Borrow the participant that owns environment `env`, if any.
    ///
    /// Useful for asserting committed state directly against a peer's
    /// resource manager in integration tests.
    #[must_use]
    pub fn participant_for(&self, env: &[u8]) -> Option<&XaParticipant> {
        self.participant(env)
    }

    fn to_xid(wire: &WireXid) -> Result<Xid, XaTransportError> {
        Xid::new(wire.format_id, &wire.gtrid, &wire.bqual)
            .map_err(|e| XaTransportError::Transport(format!("xid: {e}")))
    }

    /// Handle an inbound prepare: run start + apply + end + prepare on
    /// the named local participant and return the vote.
    ///
    /// A missing environment, a start/apply/end failure, or a prepare
    /// error all yield [`XaVote::Abort`] (presumed abort). A branch
    /// that performed no writes votes [`XaVote::ReadOnly`].
    ///
    /// # Errors
    ///
    /// Returns [`XaTransportError::Transport`] only when the `Xid`
    /// itself is malformed; protocol-level prepare failures surface as
    /// an [`XaVote::Abort`] so the coordinator's presumed-abort logic
    /// can roll the transaction back.
    pub fn handle_prepare(
        &self,
        xid_wire: &WireXid,
        env: &[u8],
        writes: &[XaWriteOp],
    ) -> Result<XaVote, XaTransportError> {
        let xid = Self::to_xid(xid_wire)?;
        let Some(participant) = self.participant(env) else {
            return Ok(XaVote::Abort);
        };
        if participant.xa().xa_start(&xid, XaFlags::NOFLAGS).is_err() {
            return Ok(XaVote::Abort);
        }
        for w in writes {
            let op: TxnOp = w.clone().into_txn_op();
            if participant.apply_op(&xid, &op).is_err() {
                let _ = participant.xa().xa_rollback(&xid, XaFlags::NOFLAGS);
                return Ok(XaVote::Abort);
            }
        }
        if participant.xa().mark_write(&xid).is_err() {
            let _ = participant.xa().xa_rollback(&xid, XaFlags::NOFLAGS);
            return Ok(XaVote::Abort);
        }
        if participant.xa().xa_end(&xid, XaFlags::TMSUCCESS).is_err() {
            let _ = participant.xa().xa_rollback(&xid, XaFlags::NOFLAGS);
            return Ok(XaVote::Abort);
        }
        match participant.xa().xa_prepare(&xid, XaFlags::NOFLAGS) {
            Ok(PrepareResult::Ok) => Ok(XaVote::Ok),
            Ok(PrepareResult::ReadOnly) => Ok(XaVote::ReadOnly),
            Err(_) => {
                let _ = participant.xa().xa_rollback(&xid, XaFlags::NOFLAGS);
                Ok(XaVote::Abort)
            }
        }
    }

    /// Handle an inbound commit. Idempotent: an `Xid` already
    /// committed (so absent from the branch map) is reported as
    /// success.
    ///
    /// Returns `true` when the branch is committed (now or already),
    /// `false` when the environment is unknown.
    #[must_use]
    pub fn handle_commit(&self, xid_wire: &WireXid, env: &[u8]) -> bool {
        Self::resolve(self.participant(env), xid_wire, true)
    }

    /// Handle an inbound rollback. Idempotent in the same way as
    /// [`Self::handle_commit`].
    #[must_use]
    pub fn handle_rollback(&self, xid_wire: &WireXid, env: &[u8]) -> bool {
        Self::resolve(self.participant(env), xid_wire, false)
    }

    fn resolve(participant: Option<&XaParticipant>, xid_wire: &WireXid, commit: bool) -> bool {
        let Some(participant) = participant else {
            return false;
        };
        let Ok(xid) = Self::to_xid(xid_wire) else {
            return false;
        };
        let result = if commit {
            participant.xa().xa_commit(&xid, XaFlags::NOFLAGS)
        } else {
            participant.xa().xa_rollback(&xid, XaFlags::NOFLAGS)
        };
        match result {
            // Idempotency: a retry after the branch was already
            // resolved finds no branch (`NotFound`) and is treated as
            // success, the same as a fresh resolution.
            Ok(()) | Err(XaError::NotFound) => true,
            Err(_) => false,
        }
    }
}

/// One durable in-doubt record: a branch that voted Ok but whose
/// commit could not be confirmed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InDoubtRecord {
    /// XA format id.
    pub format_id: i32,
    /// Global transaction id (hex).
    pub gtrid: Vec<u8>,
    /// Branch qualifier (hex).
    pub bqual: Vec<u8>,
    /// Owning environment name.
    pub env: Vec<u8>,
}

impl InDoubtRecord {
    fn from_parts(xid: &WireXid, env: &[u8]) -> Self {
        Self {
            format_id: xid.format_id,
            gtrid: xid.gtrid.clone(),
            bqual: xid.bqual.clone(),
            env: env.to_vec(),
        }
    }

    /// The branch's wire identifier.
    #[must_use]
    pub fn xid(&self) -> WireXid {
        WireXid {
            format_id: self.format_id,
            gtrid: self.gtrid.clone(),
            bqual: self.bqual.clone(),
        }
    }

    fn to_line(&self, tag: LineTag) -> String {
        // One record per line: `tag format_id hex(gtrid) hex(bqual)
        // hex(env)`. `tag` is `+` for an in-doubt record and `-` for a
        // tombstone retiring an earlier record. Hex keeps every field
        // ASCII and unambiguous.
        format!(
            "{} {} {} {} {}",
            tag.as_char(),
            self.format_id,
            hex(&self.gtrid),
            hex(&self.bqual),
            hex(&self.env),
        )
    }

    fn from_line(line: &str) -> Option<(LineTag, Self)> {
        let mut it = line.split_whitespace();
        let tag = LineTag::from_str(it.next()?)?;
        let format_id: i32 = it.next()?.parse().ok()?;
        let gtrid = unhex(it.next()?)?;
        let bqual = unhex(it.next()?)?;
        let env = unhex(it.next()?)?;
        Some((
            tag,
            Self {
                format_id,
                gtrid,
                bqual,
                env,
            },
        ))
    }

    /// Identity used to net records against tombstones: a record is
    /// retired by a tombstone with the same xid and env.
    fn key(&self) -> (i32, Vec<u8>, Vec<u8>, Vec<u8>) {
        (
            self.format_id,
            self.gtrid.clone(),
            self.bqual.clone(),
            self.env.clone(),
        )
    }
}

/// Whether a log line records a new in-doubt branch (`Record`) or
/// retires an earlier one (`Tombstone`).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum LineTag {
    Record,
    Tombstone,
}

impl LineTag {
    fn as_char(self) -> char {
        match self {
            Self::Record => '+',
            Self::Tombstone => '-',
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "+" => Some(Self::Record),
            "-" => Some(Self::Tombstone),
            _ => None,
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    if bytes.is_empty() {
        return "-".to_string();
    }
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s == "-" {
        return Some(Vec::new());
    }
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = (bytes[i] as char).to_digit(16)?;
        let lo = (bytes[i + 1] as char).to_digit(16)?;
        out.push(u8::try_from(hi * 16 + lo).ok()?);
        i += 2;
    }
    Some(out)
}

/// Durable append-only log of in-doubt branches.
///
/// Each [`Self::record`] appends one line and `fsync`s the file
/// before returning, so a coordinator crash immediately afterwards
/// still leaves the record on disk for [`Self::load`] to surface to a
/// recovery pass. The log is the artifact that makes cold-restart
/// commit recovery possible.
///
/// # Retirement (tombstone-on-resolve)
///
/// When a recovery pass confirms a branch's commit it does not
/// rewrite the log; it appends a *tombstone* line ([`Self::resolve`])
/// naming the same xid and env. [`Self::load`] nets tombstones
/// against records, so a retired branch is invisible to the next
/// scan. This keeps the log strictly append-only and crash-safe: a
/// crash after the commit lands on the peer but before the tombstone
/// is written simply leaves the record in place, and the next scan
/// re-drives the commit -- which is idempotent on the peer, so the
/// replay is a no-op. (Compaction would also work; tombstoning is
/// chosen because it needs no temp-file rename and a partial write
/// only ever leaves a truncated last line, which `load` skips.)
#[derive(Clone, Debug)]
pub struct InDoubtLog {
    path: PathBuf,
}

impl InDoubtLog {
    /// Open (or create on first write) an in-doubt log at `path`.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Path the log records to.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Durably append one in-doubt record.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if the line cannot be
    /// written or `fsync`ed.
    pub fn record(&self, xid: &WireXid, env: &[u8]) -> std::io::Result<()> {
        self.append(LineTag::Record, &InDoubtRecord::from_parts(xid, env))
    }

    /// Durably append a tombstone retiring an earlier in-doubt record
    /// for the same xid and env, after its commit has been confirmed.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if the line cannot be
    /// written or `fsync`ed.
    pub fn resolve(&self, xid: &WireXid, env: &[u8]) -> std::io::Result<()> {
        self.append(LineTag::Tombstone, &InDoubtRecord::from_parts(xid, env))
    }

    fn append(&self, tag: LineTag, rec: &InDoubtRecord) -> std::io::Result<()> {
        use std::io::Write as _;
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut line = rec.to_line(tag);
        line.push('\n');
        f.write_all(line.as_bytes())?;
        // Durability: a coordinator crash after this point still
        // leaves the line for the recovery pass.
        f.sync_all()?;
        Ok(())
    }

    /// Read every still-unresolved in-doubt branch. Returns an empty
    /// vector when the log has never been written or when every
    /// recorded branch has since been retired by a tombstone.
    ///
    /// Records and tombstones are netted in append order: each
    /// tombstone retires the matching earlier record. A record that
    /// outlives every tombstone for its key is returned; the result
    /// preserves the order in which the surviving records were first
    /// written.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if the file exists
    /// but cannot be read.
    pub fn load(&self) -> std::io::Result<Vec<InDoubtRecord>> {
        let s = match std::fs::read_to_string(&self.path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        // Net tombstones against records. A truncated final line (a
        // crash mid-write) fails to parse and is skipped, leaving the
        // matching record live so the scan re-drives the idempotent
        // commit.
        let mut live: Vec<InDoubtRecord> = Vec::new();
        for line in s.lines() {
            let Some((tag, rec)) = InDoubtRecord::from_line(line) else {
                continue;
            };
            match tag {
                LineTag::Record => live.push(rec),
                LineTag::Tombstone => {
                    let key = rec.key();
                    if let Some(pos) = live.iter().position(|r| r.key() == key) {
                        live.remove(pos);
                    }
                }
            }
        }
        Ok(live)
    }
}

/// Bounded retry policy for the commit phase.
#[derive(Copy, Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum number of commit attempts per branch before the branch
    /// is declared in-doubt.
    pub max_attempts: u32,
    /// Backoff applied before the first retry; doubles each attempt.
    pub base_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_backoff: Duration::from_millis(20),
        }
    }
}

/// Outcome of one [`CrossNodeCoordinator::recover_in_doubt`] pass.
///
/// The counts sum to the number of in-doubt records the scan read:
/// `recovered + still_in_doubt + errors == records examined`.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    /// Branches whose commit was confirmed and whose record was
    /// retired (tombstoned) from the log.
    pub recovered: usize,
    /// Branches whose owning peer was still unreachable; the record
    /// stays in the log for a later, re-runnable pass.
    pub still_in_doubt: usize,
    /// Records that could not be acted on (no branch owns the env, a
    /// malformed xid, or a tombstone write failed); the record stays
    /// in the log.
    pub errors: usize,
}

/// Async coordinator for a cross-node transaction.
///
/// Holds a mix of [`XaBranch::Local`] and [`XaBranch::Remote`]
/// participants and runs the presumed-abort / forward-commit protocol
/// over them. Construct with [`Self::new`], then drive a batch with
/// [`Self::execute`].
pub struct CrossNodeCoordinator {
    branches: Vec<XaBranch>,
    next_gtid: std::sync::atomic::AtomicU64,
    in_doubt: InDoubtLog,
    retry: RetryPolicy,
}

impl CrossNodeCoordinator {
    /// Build a coordinator over `branches`, recording in-doubt
    /// branches to `in_doubt`.
    #[must_use]
    pub fn new(branches: Vec<XaBranch>, in_doubt: InDoubtLog) -> Self {
        Self {
            branches,
            next_gtid: std::sync::atomic::AtomicU64::new(1),
            in_doubt,
            retry: RetryPolicy::default(),
        }
    }

    /// Override the commit retry policy (tests use a tight policy to
    /// keep timeouts fast).
    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Number of branches.
    #[must_use]
    pub fn len(&self) -> usize {
        self.branches.len()
    }

    /// True when the coordinator has no branches.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.branches.is_empty()
    }

    /// Borrow a branch by index.
    #[must_use]
    pub fn branch(&self, index: usize) -> Option<&XaBranch> {
        self.branches.get(index)
    }

    /// Build a coordinator over `branches` and immediately run a
    /// cold-restart recovery scan against `in_doubt` before returning.
    ///
    /// This is the startup entry point: a server constructs its
    /// coordinator this way on boot so any commit a previous incarnation
    /// left in-doubt is driven forward before normal operation begins.
    /// The scan is a bounded blocking pass (in-doubt sets are small and
    /// commit is idempotent on the peer, so a re-driven commit that
    /// already landed is a cheap no-op); it runs to completion before
    /// the constructed coordinator is handed back.
    ///
    /// Test and in-memory coordinators that have no durable backlog
    /// keep using [`Self::new`], which never scans -- so there is no
    /// behaviour change for them. Use this constructor only when a real
    /// durable in-doubt log path is configured.
    ///
    /// Returns the coordinator paired with the scan's
    /// [`RecoveryReport`].
    ///
    /// # Errors
    ///
    /// Returns [`TxnStoreError::Backend`] if the in-doubt log exists but
    /// cannot be read.
    pub async fn new_with_recovery(
        branches: Vec<XaBranch>,
        in_doubt: InDoubtLog,
    ) -> Result<(Self, RecoveryReport), TxnStoreError> {
        Self::new_with_recovery_retry(branches, in_doubt, RetryPolicy::default()).await
    }

    /// [`Self::new_with_recovery`] with an explicit commit retry policy
    /// for the recovery pass.
    ///
    /// # Errors
    ///
    /// As [`Self::new_with_recovery`].
    pub async fn new_with_recovery_retry(
        branches: Vec<XaBranch>,
        in_doubt: InDoubtLog,
        retry: RetryPolicy,
    ) -> Result<(Self, RecoveryReport), TxnStoreError> {
        let coord = Self::new(branches, in_doubt).with_retry(retry);
        let report = coord.recover_in_doubt().await?;
        Ok((coord, report))
    }

    /// Re-drive every still-unresolved in-doubt branch forward to
    /// commit.
    ///
    /// Reads the durable in-doubt log with [`InDoubtLog::load`] and,
    /// for each surviving record, re-issues the commit over the same
    /// path phase 2 uses: a local branch commits inline, a remote
    /// branch re-sends `XA_COMMIT` through its [`XaTransport`]. Because
    /// the receiver commits idempotently, re-driving a commit that
    /// actually landed before the crash returns success and is not a
    /// double-apply.
    ///
    /// On a confirmed commit the record is retired with
    /// [`InDoubtLog::resolve`] (a tombstone), so a second pass does not
    /// re-drive it. A record whose peer is still unreachable is left in
    /// the log; the scan is re-runnable and never rolls a prepared
    /// branch back -- the only correct resolution for a branch that
    /// voted Ok is forward (presumed commit).
    ///
    /// Running the scan with no surviving records is a no-op. Running
    /// it twice in a row, the second pass sees only the records the
    /// first could not resolve.
    ///
    /// # Errors
    ///
    /// Returns [`TxnStoreError::Backend`] if the in-doubt log cannot be
    /// read. Per-record failures (unreachable peer, unknown env,
    /// failed tombstone write) are counted in the returned
    /// [`RecoveryReport`], not surfaced as an error, so one bad record
    /// never aborts the scan of the rest.
    pub async fn recover_in_doubt(&self) -> Result<RecoveryReport, TxnStoreError> {
        let records = self
            .in_doubt
            .load()
            .map_err(|e| TxnStoreError::Backend(format!("in-doubt log read failed: {e}")))?;
        let mut report = RecoveryReport::default();
        for rec in &records {
            let wire = rec.xid();
            let Some(branch_idx) = self
                .branches
                .iter()
                .position(|b| b.name() == rec.env.as_slice())
            else {
                // No branch owns this env on this coordinator; leave
                // the record for an incarnation that does. Never drop
                // an unconfirmed commit.
                report.errors += 1;
                continue;
            };
            match self.redrive_commit(branch_idx, &wire).await {
                Ok(()) => {
                    // Commit confirmed: retire the record. A crash
                    // before this tombstone lands just replays the
                    // idempotent commit on the next pass.
                    if let Err(e) = self.in_doubt.resolve(&wire, &rec.env) {
                        tracing::warn!("in-doubt resolve (tombstone) write failed: {e}");
                        report.errors += 1;
                    } else {
                        report.recovered += 1;
                    }
                }
                Err(()) => {
                    // Peer still unreachable; the record stays. The
                    // scan is re-runnable.
                    report.still_in_doubt += 1;
                }
            }
        }
        Ok(report)
    }

    /// Re-drive the commit for an in-doubt branch identified by
    /// `branch_idx` and `wire`. `Ok(())` when the commit was confirmed
    /// (idempotently), `Err(())` when the peer is still unreachable.
    async fn redrive_commit(&self, branch_idx: usize, wire: &WireXid) -> Result<(), ()> {
        match &self.branches[branch_idx] {
            XaBranch::Local(participant) => {
                let Ok(xid) = Xid::new(wire.format_id, &wire.gtrid, &wire.bqual) else {
                    return Err(());
                };
                // A local commit is idempotent the same way the peer's
                // is: noxu reports `NotFound` for a branch already
                // committed, which is success for recovery.
                match participant.xa().xa_commit(&xid, XaFlags::NOFLAGS) {
                    Ok(()) | Err(XaError::NotFound) => Ok(()),
                    Err(_) => Err(()),
                }
            }
            XaBranch::Remote(remote) => {
                let mut backoff = self.retry.base_backoff;
                for attempt in 0..self.retry.max_attempts {
                    if remote.transport.commit(wire, &remote.env).await.is_ok() {
                        return Ok(());
                    }
                    if attempt + 1 < self.retry.max_attempts {
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.saturating_mul(2);
                    }
                }
                Err(())
            }
        }
    }

    /// Run a cross-node transaction over `batch`, routing each op to a
    /// branch with `route`.
    ///
    /// Mirrors [`crate::datastore::xa::XaCoordinator::execute`]'s
    /// decision logic exactly -- prepare, gather votes, commit only on
    /// unanimous Ok/ReadOnly, presumed-abort otherwise -- but delivers
    /// each phase to local and remote branches alike and resolves the
    /// network failure modes documented on the module.
    ///
    /// # Errors
    ///
    /// * [`TxnStoreError::EmptyBatch`] for an empty batch.
    /// * [`TxnStoreError::Backend`] for an out-of-range route, a
    ///   malformed `Xid`, a prepare-phase abort (presumed abort), or a
    ///   commit that ended in-doubt (the affected branches are durably
    ///   recorded in the in-doubt log first).
    pub async fn execute<R>(&self, batch: &TxnBatch, route: R) -> Result<TxnOutcome, TxnStoreError>
    where
        R: Fn(&TxnOp) -> usize,
    {
        if batch.ops.is_empty() {
            return Err(TxnStoreError::EmptyBatch);
        }

        let mut per_branch: Vec<Vec<usize>> = vec![Vec::new(); self.branches.len()];
        for (idx, op) in batch.ops.iter().enumerate() {
            let b = route(op);
            if b >= self.branches.len() {
                return Err(TxnStoreError::Backend(format!(
                    "routing returned branch index {b} but only {} branches exist",
                    self.branches.len()
                )));
            }
            per_branch[b].push(idx);
        }

        let gtid = self
            .next_gtid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let gtid_bytes = gtid.to_be_bytes();

        // Active branches: those carrying at least one op, each with
        // its xid and wire xid.
        let mut active: Vec<ActiveBranch> = Vec::new();
        for (branch_idx, ops) in per_branch.iter().enumerate() {
            if ops.is_empty() {
                continue;
            }
            let xid = Xid::new(
                DYNIAK_XA_FORMAT_ID,
                &gtid_bytes,
                self.branches[branch_idx].name(),
            )
            .map_err(|e| TxnStoreError::Backend(format!("xid: {e}")))?;
            let wire = wire_xid(&xid);
            active.push(ActiveBranch {
                branch_idx,
                xid,
                wire,
                op_indices: ops.clone(),
            });
        }

        // Phase 1: prepare every active branch (locals inline, remotes
        // in parallel), gather votes.
        let votes = self.prepare_all(&active, batch).await;

        // Decide: presumed abort on any Abort vote or transport error.
        let mut to_commit: Vec<&ActiveBranch> = Vec::new();
        let mut abort = false;
        for (ab, vote) in active.iter().zip(votes.iter()) {
            match vote {
                Ok(XaVote::Ok) => to_commit.push(ab),
                Ok(XaVote::ReadOnly) => {}
                Ok(XaVote::Abort) | Err(_) => abort = true,
            }
        }

        if abort || batch.force_abort {
            self.rollback_all(&active).await;
            if abort {
                return Err(TxnStoreError::Backend(
                    "cross-node prepare aborted; transaction rolled back".to_string(),
                ));
            }
            return Ok(TxnOutcome::Aborted {
                reason: "client requested abort".to_string(),
            });
        }

        // Phase 2: commit every Ok branch, with forward recovery on a
        // commit-phase failure.
        let mut in_doubt = false;
        for ab in &to_commit {
            if let Err(()) = self.commit_branch(ab).await {
                // The branch voted Ok and is durably prepared on the
                // peer; never roll it back. Record it and keep going
                // (other branches must still be driven to completion).
                if let Err(e) = self
                    .in_doubt
                    .record(&ab.wire, self.branches[ab.branch_idx].name())
                {
                    return Err(TxnStoreError::Backend(format!(
                        "commit in-doubt and in-doubt log write failed: {e}"
                    )));
                }
                in_doubt = true;
            }
        }

        if in_doubt {
            return Err(TxnStoreError::Backend(
                "cross-node commit in-doubt; branches recorded in in-doubt log for recovery"
                    .to_string(),
            ));
        }

        Ok(TxnOutcome::Committed {
            operations: batch.ops.len(),
        })
    }

    async fn prepare_all(
        &self,
        active: &[ActiveBranch],
        batch: &TxnBatch,
    ) -> Vec<Result<XaVote, XaTransportError>> {
        // Local branches prepare inline (no I/O); remote branches
        // prepare concurrently so a multi-peer transaction pays one
        // round-trip of latency, not one per peer.
        let mut votes: Vec<Option<Result<XaVote, XaTransportError>>> = vec![None; active.len()];
        let mut remote_futs = Vec::new();
        for (slot, ab) in active.iter().enumerate() {
            match &self.branches[ab.branch_idx] {
                XaBranch::Local(participant) => {
                    votes[slot] = Some(Ok(local_prepare(
                        participant,
                        &ab.xid,
                        &ab.op_indices,
                        &batch.ops,
                    )));
                }
                XaBranch::Remote(remote) => {
                    let writes: Vec<XaWriteOp> = ab
                        .op_indices
                        .iter()
                        .map(|&i| XaWriteOp::from_txn_op(&batch.ops[i]))
                        .collect();
                    remote_futs.push(async move {
                        let vote = remote
                            .transport
                            .prepare(&ab.wire, &remote.env, &writes)
                            .await;
                        (slot, vote)
                    });
                }
            }
        }
        for (slot, vote) in futures_util::future::join_all(remote_futs).await {
            votes[slot] = Some(vote);
        }
        votes
            .into_iter()
            .map(|v| v.expect("invariant: every active branch produced a vote"))
            .collect()
    }

    async fn rollback_all(&self, active: &[ActiveBranch]) {
        for ab in active {
            match &self.branches[ab.branch_idx] {
                XaBranch::Local(participant) => {
                    let _ = participant.xa().xa_rollback(&ab.xid, XaFlags::NOFLAGS);
                }
                XaBranch::Remote(remote) => {
                    let _ = remote.transport.rollback(&ab.wire, &remote.env).await;
                }
            }
        }
    }

    /// Commit one branch with bounded retry and backoff. `Ok(())` when
    /// the commit was confirmed; `Err(())` when every attempt failed
    /// (the caller records the branch as in-doubt).
    async fn commit_branch(&self, ab: &ActiveBranch) -> Result<(), ()> {
        match &self.branches[ab.branch_idx] {
            XaBranch::Local(participant) => {
                // A local commit cannot suffer a transport failure; the
                // engine commit is the source of truth.
                participant
                    .xa()
                    .xa_commit(&ab.xid, XaFlags::NOFLAGS)
                    .map_err(|_| ())
            }
            XaBranch::Remote(remote) => {
                let mut backoff = self.retry.base_backoff;
                for attempt in 0..self.retry.max_attempts {
                    match remote.transport.commit(&ab.wire, &remote.env).await {
                        Ok(()) => return Ok(()),
                        Err(_) => {
                            if attempt + 1 < self.retry.max_attempts {
                                tokio::time::sleep(backoff).await;
                                backoff = backoff.saturating_mul(2);
                            }
                        }
                    }
                }
                Err(())
            }
        }
    }
}

/// One active branch in flight during [`CrossNodeCoordinator::execute`].
struct ActiveBranch {
    branch_idx: usize,
    xid: Xid,
    wire: WireXid,
    op_indices: Vec<usize>,
}

/// Run start + apply + end + prepare on a local participant and map
/// the result to an [`XaVote`], matching the receiver-side handler so
/// local and remote branches vote identically.
fn local_prepare(
    participant: &XaParticipant,
    xid: &Xid,
    op_indices: &[usize],
    ops: &[TxnOp],
) -> XaVote {
    if participant.xa().xa_start(xid, XaFlags::NOFLAGS).is_err() {
        return XaVote::Abort;
    }
    for &i in op_indices {
        if participant.apply_op(xid, &ops[i]).is_err() {
            let _ = participant.xa().xa_rollback(xid, XaFlags::NOFLAGS);
            return XaVote::Abort;
        }
    }
    if participant.xa().mark_write(xid).is_err() {
        let _ = participant.xa().xa_rollback(xid, XaFlags::NOFLAGS);
        return XaVote::Abort;
    }
    if participant.xa().xa_end(xid, XaFlags::TMSUCCESS).is_err() {
        let _ = participant.xa().xa_rollback(xid, XaFlags::NOFLAGS);
        return XaVote::Abort;
    }
    match participant.xa().xa_prepare(xid, XaFlags::NOFLAGS) {
        Ok(PrepareResult::Ok) => XaVote::Ok,
        Ok(PrepareResult::ReadOnly) => XaVote::ReadOnly,
        Err(_) => {
            let _ = participant.xa().xa_rollback(xid, XaFlags::NOFLAGS);
            XaVote::Abort
        }
    }
}

// ------------------------------------------------------------------
// dnode peer-plane transport and receiver loop.
// ------------------------------------------------------------------

use std::net::SocketAddr;

use dynomite::io::mbuf::MbufPool;
use dynomite::proto::dnode::{dmsg_write, DmsgType, DnodeParser, ParseStep};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

use crate::datastore::xa_wire::{XaAckMsg, XaPrepareMsg, XaResolveMsg, XaWireError};

/// Build one dnode frame (header + XA payload) ready for the wire.
fn frame(
    pool: &MbufPool,
    msg_id: u64,
    ty: DmsgType,
    payload: &[u8],
) -> Result<Vec<u8>, XaTransportError> {
    let mut header = pool.get();
    let plen = u32::try_from(payload.len()).unwrap_or(u32::MAX);
    dmsg_write(&mut header, msg_id, ty, 0, true, None, plen)
        .map_err(|e| XaTransportError::Transport(format!("dnode header: {e:?}")))?;
    let mut out = header.readable().to_vec();
    out.extend_from_slice(payload);
    Ok(out)
}

/// Read exactly one dnode frame from `stream`, returning its type and
/// payload bytes.
async fn read_frame(
    stream: &mut TcpStream,
    accumulated: &mut Vec<u8>,
) -> Result<(DmsgType, Vec<u8>), XaTransportError> {
    let mut buf = [0u8; 4096];
    let mut parser = DnodeParser::new();
    loop {
        let step = parser.step(accumulated.as_slice());
        match step {
            ParseStep::HeaderDone { consumed } => {
                let dmsg = parser.take_dmsg();
                let plen = dmsg.plen as usize;
                let total = consumed + plen;
                if accumulated.len() < total {
                    parser.reset();
                } else {
                    let payload = accumulated[consumed..total].to_vec();
                    accumulated.drain(0..total);
                    return Ok((dmsg.ty, payload));
                }
            }
            ParseStep::Error { consumed } => {
                return Err(XaTransportError::Transport(format!(
                    "dnode parse error after {consumed} bytes"
                )));
            }
            ParseStep::NeedMore { .. } => {}
        }
        let n = stream
            .read(&mut buf)
            .await
            .map_err(|e| XaTransportError::Transport(e.to_string()))?;
        if n == 0 {
            return Err(XaTransportError::Transport("peer closed".to_string()));
        }
        accumulated.extend_from_slice(&buf[..n]);
    }
}

/// dnode peer-plane [`XaTransport`].
///
/// Each phase opens (or reuses) a TCP connection to the peer, writes
/// the dnode-framed phase message, and reads back the framed reply.
/// One in-flight phase per connection: the coordinator awaits each
/// reply before the next phase, so a single connection is reused
/// across the prepare / commit / rollback of one branch.
pub struct DnodeXaTransport {
    addr: SocketAddr,
    pool: MbufPool,
    next_id: std::sync::atomic::AtomicU64,
    timeout: Duration,
    // One persistent connection guarded by an async mutex; reconnect
    // on failure. The lock is held across `.await` and serialises the
    // single connection's phases (the coordinator awaits each phase's
    // reply before issuing the next, so there is never more than one
    // in-flight phase per branch).
    conn: tokio::sync::Mutex<Option<TcpStream>>,
}

impl DnodeXaTransport {
    /// Connect lazily to the peer at `addr`.
    #[must_use]
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            pool: MbufPool::default(),
            next_id: std::sync::atomic::AtomicU64::new(1),
            timeout: Duration::from_secs(5),
            conn: tokio::sync::Mutex::new(None),
        }
    }

    /// Override the per-phase round-trip timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn round_trip(
        &self,
        ty: DmsgType,
        payload: Vec<u8>,
    ) -> Result<(DmsgType, Vec<u8>), XaTransportError> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes = frame(&self.pool, id, ty, &payload)?;
        let mut guard = self.conn.lock().await;
        // Establish the connection if absent.
        if guard.is_none() {
            let s = tokio::time::timeout(self.timeout, TcpStream::connect(self.addr))
                .await
                .map_err(|_| XaTransportError::Timeout)?
                .map_err(|e| XaTransportError::Transport(e.to_string()))?;
            *guard = Some(s);
        }
        let stream = guard.as_mut().expect("connection just established");
        let exchange = async {
            stream
                .write_all(&bytes)
                .await
                .map_err(|e| XaTransportError::Transport(e.to_string()))?;
            let mut acc = Vec::new();
            read_frame(stream, &mut acc).await
        };
        match tokio::time::timeout(self.timeout, exchange).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(e)) => {
                // Drop the connection so the next phase reconnects.
                *guard = None;
                Err(e)
            }
            Err(_) => {
                *guard = None;
                Err(XaTransportError::Timeout)
            }
        }
    }
}

impl XaTransport for DnodeXaTransport {
    fn prepare<'a>(
        &'a self,
        xid: &'a WireXid,
        env: &'a [u8],
        writes: &'a [XaWriteOp],
    ) -> XaFuture<'a, Result<XaVote, XaTransportError>> {
        Box::pin(async move {
            let payload = XaPrepareMsg {
                xid: xid.clone(),
                env: env.to_vec(),
                writes: writes.to_vec(),
            }
            .encode();
            let (ty, body) = self.round_trip(DmsgType::XaPrepare, payload).await?;
            if ty != DmsgType::XaVote {
                return Err(XaTransportError::Transport(format!(
                    "expected XaVote, got {ty:?}"
                )));
            }
            XaVote::decode(&body)
                .map_err(|e: XaWireError| XaTransportError::Transport(e.to_string()))
        })
    }

    fn commit<'a>(
        &'a self,
        xid: &'a WireXid,
        env: &'a [u8],
    ) -> XaFuture<'a, Result<(), XaTransportError>> {
        Box::pin(async move {
            let payload = XaResolveMsg {
                xid: xid.clone(),
                env: env.to_vec(),
            }
            .encode();
            let (ty, body) = self.round_trip(DmsgType::XaCommit, payload).await?;
            ack_to_result(ty, &body)
        })
    }

    fn rollback<'a>(
        &'a self,
        xid: &'a WireXid,
        env: &'a [u8],
    ) -> XaFuture<'a, Result<(), XaTransportError>> {
        Box::pin(async move {
            let payload = XaResolveMsg {
                xid: xid.clone(),
                env: env.to_vec(),
            }
            .encode();
            let (ty, body) = self.round_trip(DmsgType::XaRollback, payload).await?;
            ack_to_result(ty, &body)
        })
    }
}

fn ack_to_result(ty: DmsgType, body: &[u8]) -> Result<(), XaTransportError> {
    if ty != DmsgType::XaAck {
        return Err(XaTransportError::Transport(format!(
            "expected XaAck, got {ty:?}"
        )));
    }
    match XaAckMsg::decode(body).map_err(|e| XaTransportError::Transport(e.to_string()))? {
        XaAckMsg { ok: true } => Ok(()),
        XaAckMsg { ok: false } => Err(XaTransportError::Transport(
            "peer reported unresolved branch".to_string(),
        )),
    }
}

/// Serve XA phase frames from a single accepted connection against
/// `peer`, replying with the framed vote / ack for each.
///
/// One connection is driven until the peer closes. Errors tear the
/// connection down (the coordinator reconnects on its next phase).
async fn serve_xa_conn(
    mut stream: TcpStream,
    peer: Arc<XaPeer>,
    pool: MbufPool,
) -> Result<(), XaTransportError> {
    let mut acc = Vec::new();
    loop {
        let (ty, payload) = match read_frame(&mut stream, &mut acc).await {
            Ok(v) => v,
            // Peer closed: a clean end of the connection's lifetime.
            Err(XaTransportError::Transport(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        let (reply_ty, reply_body) = match ty {
            DmsgType::XaPrepare => {
                let msg = XaPrepareMsg::decode(&payload)
                    .map_err(|e| XaTransportError::Transport(e.to_string()))?;
                let vote = peer.handle_prepare(&msg.xid, &msg.env, &msg.writes)?;
                (DmsgType::XaVote, vote.encode())
            }
            DmsgType::XaCommit => {
                let msg = XaResolveMsg::decode(&payload)
                    .map_err(|e| XaTransportError::Transport(e.to_string()))?;
                let ok = peer.handle_commit(&msg.xid, &msg.env);
                (DmsgType::XaAck, XaAckMsg { ok }.encode())
            }
            DmsgType::XaRollback => {
                let msg = XaResolveMsg::decode(&payload)
                    .map_err(|e| XaTransportError::Transport(e.to_string()))?;
                let ok = peer.handle_rollback(&msg.xid, &msg.env);
                (DmsgType::XaAck, XaAckMsg { ok }.encode())
            }
            other => {
                return Err(XaTransportError::Transport(format!(
                    "unexpected dnode type on xa peer plane: {other:?}"
                )));
            }
        };
        let id = peer
            .next_reply_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes = frame(&pool, id, reply_ty, &reply_body)?;
        stream
            .write_all(&bytes)
            .await
            .map_err(|e| XaTransportError::Transport(e.to_string()))?;
    }
}

/// Run an XA peer-plane receiver on `listener`, serving every
/// inbound connection against `peer` until the listener is dropped.
///
/// Spawn this on the peer node; the coordinator's
/// [`DnodeXaTransport`] connects to `listener`'s address. Each
/// connection is handled by its own task so the prepare / commit /
/// rollback of independent transactions do not serialise.
///
/// # Errors
///
/// Returns a [`XaTransportError::Transport`] only if the listener
/// itself fails; per-connection errors are logged via `tracing` and
/// do not stop the loop.
pub async fn serve_xa_peer(
    listener: TcpListener,
    peer: Arc<XaPeer>,
) -> Result<(), XaTransportError> {
    let pool = MbufPool::default();
    loop {
        let (stream, _addr) = listener
            .accept()
            .await
            .map_err(|e| XaTransportError::Transport(e.to_string()))?;
        let peer = Arc::clone(&peer);
        let pool = pool.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_xa_conn(stream, peer, pool).await {
                tracing::warn!("xa peer connection ended with error: {e}");
            }
        });
    }
}
