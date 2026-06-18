//! Cross-node XA two-phase commit integration tests.
//!
//! Exercises the network leg of the XA coordinator: a transaction
//! spanning a local branch and a remote branch reached over a
//! transport. Two transports are used: an in-test mock (for the
//! abort / timeout / in-doubt paths that are awkward to provoke over
//! real TCP) and the real dnode loopback transport (two in-process
//! nodes on localhost dnode ports).
//!
//! Gated on the `noxu` feature; without it the file compiles to an
//! empty module.

#![cfg(feature = "noxu")]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dyniak::datastore::xa_net::{
    serve_xa_peer, CrossNodeCoordinator, DnodeXaTransport, InDoubtLog, RemoteXaBranch, RetryPolicy,
    XaBranch, XaFuture, XaPeer, XaTransport, XaTransportError,
};
use dyniak::datastore::xa_wire::{WireXid, XaVote, XaWriteOp};
use dyniak::datastore::XaParticipant;
use dyniak::txn::{TxnBatch, TxnOp, TxnOutcome};
use tempfile::TempDir;
use tokio::net::TcpListener;

/// Scratch root for env / log paths (AGENTS.md: use /scratch, not /tmp).
fn scratch_dir() -> TempDir {
    let base = std::path::Path::new("/scratch");
    if base.is_dir() {
        TempDir::new_in(base).expect("tempdir in /scratch")
    } else {
        // Fall back to the platform temp dir when /scratch is absent
        // (CI sandboxes without it still run the test).
        TempDir::new().expect("tempdir")
    }
}

fn put(key: &[u8], value: &[u8]) -> TxnOp {
    TxnOp::Put {
        bucket: b"u".to_vec(),
        key: key.to_vec(),
        value: value.to_vec(),
        indexes: vec![],
    }
}

/// Route by first byte parity: even -> branch 0 (local), odd ->
/// branch 1 (remote). "bob" (0x62) -> 0, "alice" (0x61) -> 1.
fn route_by_parity(op: &TxnOp) -> usize {
    usize::from(op.key().first().copied().unwrap_or(0) & 1)
}

// ------------------------------------------------------------------
// Mock transport that drives a real in-process XaPeer, with knobs to
// fail the prepare or commit phase a configurable number of times.
// ------------------------------------------------------------------

struct MockTransport {
    peer: Arc<XaPeer>,
    fail_prepare: bool,
    commit_failures_remaining: AtomicU32,
    commit_calls: AtomicU32,
}

impl MockTransport {
    fn new(peer: Arc<XaPeer>) -> Self {
        Self {
            peer,
            fail_prepare: false,
            commit_failures_remaining: AtomicU32::new(0),
            commit_calls: AtomicU32::new(0),
        }
    }

    fn failing_prepare(peer: Arc<XaPeer>) -> Self {
        let mut t = Self::new(peer);
        t.fail_prepare = true;
        t
    }

    fn flaky_commit(peer: Arc<XaPeer>, failures: u32) -> Self {
        let t = Self::new(peer);
        t.commit_failures_remaining
            .store(failures, Ordering::SeqCst);
        t
    }
}

impl XaTransport for MockTransport {
    fn prepare<'a>(
        &'a self,
        xid: &'a WireXid,
        env: &'a [u8],
        writes: &'a [XaWriteOp],
    ) -> XaFuture<'a, Result<XaVote, XaTransportError>> {
        Box::pin(async move {
            if self.fail_prepare {
                return Err(XaTransportError::Timeout);
            }
            self.peer.handle_prepare(xid, env, writes)
        })
    }

    fn commit<'a>(
        &'a self,
        xid: &'a WireXid,
        env: &'a [u8],
    ) -> XaFuture<'a, Result<(), XaTransportError>> {
        Box::pin(async move {
            self.commit_calls.fetch_add(1, Ordering::SeqCst);
            // Fail the first N commit attempts to simulate a peer that
            // is unreachable in the commit phase.
            if self.commit_failures_remaining.load(Ordering::SeqCst) > 0 {
                self.commit_failures_remaining
                    .fetch_sub(1, Ordering::SeqCst);
                return Err(XaTransportError::Timeout);
            }
            if self.peer.handle_commit(xid, env) {
                Ok(())
            } else {
                Err(XaTransportError::Transport("unresolved".into()))
            }
        })
    }

    fn rollback<'a>(
        &'a self,
        xid: &'a WireXid,
        env: &'a [u8],
    ) -> XaFuture<'a, Result<(), XaTransportError>> {
        Box::pin(async move {
            let _ = self.peer.handle_rollback(xid, env);
            Ok(())
        })
    }
}

fn open_participant(dir: &TempDir, name: &[u8]) -> XaParticipant {
    XaParticipant::open(dir.path(), name.to_vec()).expect("open participant")
}

fn tight_retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 4,
        base_backoff: Duration::from_millis(1),
    }
}

// ------------------------------------------------------------------
// 1. Mock cross-node commit: local + remote both vote Ok -> commit.
// ------------------------------------------------------------------

#[tokio::test]
async fn mock_cross_node_commit_is_atomic() {
    let d_local = scratch_dir();
    let d_remote = scratch_dir();
    let d_log = scratch_dir();

    let local = open_participant(&d_local, b"east");
    let remote_peer = Arc::new(XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d_remote, b"west"),
    )]));
    let transport: Arc<dyn XaTransport> = Arc::new(MockTransport::new(Arc::clone(&remote_peer)));

    let branches = vec![
        XaBranch::Local(Box::new(local)),
        XaBranch::Remote(RemoteXaBranch::new(transport, b"west".to_vec())),
    ];
    let log = InDoubtLog::new(d_log.path().join("indoubt.log"));
    let coord = CrossNodeCoordinator::new(branches, log).with_retry(tight_retry());

    let batch = TxnBatch {
        ops: vec![put(b"bob", b"b"), put(b"alice", b"a")],
        force_abort: false,
    };
    let outcome = coord
        .execute(&batch, route_by_parity)
        .await
        .expect("commit");
    assert_eq!(outcome, TxnOutcome::Committed { operations: 2 });

    // bob landed on the local branch; alice on the remote peer.
    let XaBranch::Local(local) = coord.branch(0).unwrap() else {
        panic!("branch 0 is local");
    };
    assert_eq!(
        local.get_object(b"u", b"bob").unwrap().as_deref(),
        Some(&b"b"[..])
    );
    assert!(local.get_object(b"u", b"alice").unwrap().is_none());
    assert_eq!(
        remote_peer
            .participant_for(b"west")
            .unwrap()
            .get_object(b"u", b"alice")
            .unwrap()
            .as_deref(),
        Some(&b"a"[..])
    );
}

// ------------------------------------------------------------------
// 2. Prepare-phase peer failure -> global rollback (presumed abort).
// ------------------------------------------------------------------

#[tokio::test]
async fn prepare_phase_peer_failure_rolls_back_everything() {
    let d_local = scratch_dir();
    let d_remote = scratch_dir();
    let d_log = scratch_dir();

    let local = open_participant(&d_local, b"east");
    let remote_peer = Arc::new(XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d_remote, b"west"),
    )]));
    // The remote branch's prepare always fails (peer refused / timed out).
    let transport: Arc<dyn XaTransport> =
        Arc::new(MockTransport::failing_prepare(Arc::clone(&remote_peer)));

    let branches = vec![
        XaBranch::Local(Box::new(local)),
        XaBranch::Remote(RemoteXaBranch::new(transport, b"west".to_vec())),
    ];
    let log = InDoubtLog::new(d_log.path().join("indoubt.log"));
    let coord = CrossNodeCoordinator::new(branches, log).with_retry(tight_retry());

    let batch = TxnBatch {
        ops: vec![put(b"bob", b"b"), put(b"alice", b"a")],
        force_abort: false,
    };
    let err = coord
        .execute(&batch, route_by_parity)
        .await
        .expect_err("prepare-phase failure aborts");
    assert!(format!("{err}").contains("aborted"));

    // Neither branch committed: the local write was rolled back and
    // the remote never prepared.
    let XaBranch::Local(local) = coord.branch(0).unwrap() else {
        panic!("branch 0 is local");
    };
    assert!(local.get_object(b"u", b"bob").unwrap().is_none());
    assert!(remote_peer
        .participant_for(b"west")
        .unwrap()
        .get_object(b"u", b"alice")
        .unwrap()
        .is_none());
}

// ------------------------------------------------------------------
// 3. Commit-phase timeout -> in-doubt log written + commit retried
//    on peer return -> eventual atomic commit.
// ------------------------------------------------------------------

#[tokio::test]
async fn commit_phase_timeout_recovers_within_retry_budget() {
    let d_local = scratch_dir();
    let d_remote = scratch_dir();
    let d_log = scratch_dir();

    let local = open_participant(&d_local, b"east");
    let remote_peer = Arc::new(XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d_remote, b"west"),
    )]));
    // Fail the first two commit attempts, then succeed on the third:
    // the bounded retry (max 4) drives it home without ever touching
    // the in-doubt log.
    let mock = Arc::new(MockTransport::flaky_commit(Arc::clone(&remote_peer), 2));
    let transport: Arc<dyn XaTransport> = Arc::clone(&mock) as Arc<dyn XaTransport>;

    let branches = vec![
        XaBranch::Local(Box::new(local)),
        XaBranch::Remote(RemoteXaBranch::new(transport, b"west".to_vec())),
    ];
    let log_path = d_log.path().join("indoubt.log");
    let log = InDoubtLog::new(&log_path);
    let coord = CrossNodeCoordinator::new(branches, log.clone()).with_retry(tight_retry());

    let batch = TxnBatch {
        ops: vec![put(b"bob", b"b"), put(b"alice", b"a")],
        force_abort: false,
    };
    let outcome = coord
        .execute(&batch, route_by_parity)
        .await
        .expect("commit");
    assert_eq!(outcome, TxnOutcome::Committed { operations: 2 });
    // Retried 3 times total (2 failures + 1 success).
    assert_eq!(mock.commit_calls.load(Ordering::SeqCst), 3);
    // Resolved within budget: no in-doubt record.
    assert!(log.load().expect("load log").is_empty());
    assert_eq!(
        remote_peer
            .participant_for(b"west")
            .unwrap()
            .get_object(b"u", b"alice")
            .unwrap()
            .as_deref(),
        Some(&b"a"[..])
    );
}

#[tokio::test]
async fn commit_phase_exhausts_retry_then_in_doubt_log_drives_recovery() {
    let d_local = scratch_dir();
    let d_remote = scratch_dir();
    let d_log = scratch_dir();

    let local = open_participant(&d_local, b"east");
    let remote_peer = Arc::new(XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d_remote, b"west"),
    )]));
    // Fail more times than the retry budget so the coordinator gives
    // up and records the branch as in-doubt -- but the peer DID
    // durably prepare, so the branch is committed-in-doubt, never
    // rolled back.
    let mock = Arc::new(MockTransport::flaky_commit(Arc::clone(&remote_peer), 100));
    let transport: Arc<dyn XaTransport> = Arc::clone(&mock) as Arc<dyn XaTransport>;

    let branches = vec![
        XaBranch::Local(Box::new(local)),
        XaBranch::Remote(RemoteXaBranch::new(transport, b"west".to_vec())),
    ];
    let log_path = d_log.path().join("indoubt.log");
    let log = InDoubtLog::new(&log_path);
    let coord = CrossNodeCoordinator::new(branches, log.clone()).with_retry(tight_retry());

    let batch = TxnBatch {
        ops: vec![put(b"alice", b"a")], // odd -> remote only
        force_abort: false,
    };
    let err = coord
        .execute(&batch, route_by_parity)
        .await
        .expect_err("commit ends in-doubt");
    assert!(format!("{err}").contains("in-doubt"));

    // The in-doubt record is durable: a fresh InDoubtLog reading the
    // same path sees the branch.
    let records = InDoubtLog::new(&log_path).load().expect("load");
    assert_eq!(records.len(), 1, "exactly one in-doubt branch recorded");
    let rec = &records[0];
    assert_eq!(rec.env, b"west");

    // The branch is durably prepared on the peer (not rolled back).
    // A recovery pass re-drives the commit now that the peer answers.
    assert!(remote_peer.handle_commit(&rec.xid(), &rec.env));
    // Now the remote write is visible -- atomicity preserved by
    // forward recovery, never by rollback.
    assert_eq!(
        remote_peer
            .participant_for(b"west")
            .unwrap()
            .get_object(b"u", b"alice")
            .unwrap()
            .as_deref(),
        Some(&b"a"[..])
    );
}

// ------------------------------------------------------------------
// 4. Idempotent commit / rollback replay on the peer.
// ------------------------------------------------------------------

#[tokio::test]
async fn peer_commit_is_idempotent() {
    let d_remote = scratch_dir();
    let peer = XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d_remote, b"west"),
    )]);

    // Drive prepare on the peer directly, then commit it twice.
    let xid = WireXid {
        format_id: 0x6479_6e6b,
        gtrid: 7u64.to_be_bytes().to_vec(),
        bqual: b"west".to_vec(),
    };
    let writes = vec![XaWriteOp::Put {
        bucket: b"u".to_vec(),
        key: b"alice".to_vec(),
        value: b"a".to_vec(),
        indexes: vec![],
    }];
    assert_eq!(
        peer.handle_prepare(&xid, b"west", &writes).unwrap(),
        XaVote::Ok
    );

    assert!(peer.handle_commit(&xid, b"west"), "first commit succeeds");
    // Second delivery (coordinator retry) must be a no-op success,
    // not a double-apply and not an error.
    assert!(
        peer.handle_commit(&xid, b"west"),
        "replayed commit is idempotent"
    );

    assert_eq!(
        peer.participant_for(b"west")
            .unwrap()
            .get_object(b"u", b"alice")
            .unwrap()
            .as_deref(),
        Some(&b"a"[..])
    );
}

#[tokio::test]
async fn peer_rollback_is_idempotent() {
    let d_remote = scratch_dir();
    let peer = XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d_remote, b"west"),
    )]);

    let xid = WireXid {
        format_id: 0x6479_6e6b,
        gtrid: 9u64.to_be_bytes().to_vec(),
        bqual: b"west".to_vec(),
    };
    let writes = vec![XaWriteOp::Put {
        bucket: b"u".to_vec(),
        key: b"bob".to_vec(),
        value: b"b".to_vec(),
        indexes: vec![],
    }];
    assert_eq!(
        peer.handle_prepare(&xid, b"west", &writes).unwrap(),
        XaVote::Ok
    );
    assert!(peer.handle_rollback(&xid, b"west"));
    assert!(
        peer.handle_rollback(&xid, b"west"),
        "replayed rollback is idempotent"
    );
    assert!(peer
        .participant_for(b"west")
        .unwrap()
        .get_object(b"u", b"bob")
        .unwrap()
        .is_none());
}

// ------------------------------------------------------------------
// 5. Real dnode loopback: two in-process nodes, multi-key txn.
// ------------------------------------------------------------------

async fn spawn_peer(env_name: &[u8]) -> (Arc<XaPeer>, std::net::SocketAddr, TempDir) {
    let dir = scratch_dir();
    let peer = Arc::new(XaPeer::new(vec![(
        env_name.to_vec(),
        open_participant(&dir, env_name),
    )]));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let peer_clone = Arc::clone(&peer);
    tokio::spawn(async move {
        let _ = serve_xa_peer(listener, peer_clone).await;
    });
    (peer, addr, dir)
}

#[tokio::test]
async fn dnode_loopback_commit_is_atomic() {
    let d_local = scratch_dir();
    let d_log = scratch_dir();
    let local = open_participant(&d_local, b"east");

    let (remote_peer, addr, _d_remote) = spawn_peer(b"west").await;
    let transport: Arc<dyn XaTransport> =
        Arc::new(DnodeXaTransport::new(addr).with_timeout(Duration::from_secs(2)));

    let branches = vec![
        XaBranch::Local(Box::new(local)),
        XaBranch::Remote(RemoteXaBranch::new(transport, b"west".to_vec())),
    ];
    let log = InDoubtLog::new(d_log.path().join("indoubt.log"));
    let coord = CrossNodeCoordinator::new(branches, log).with_retry(tight_retry());

    let batch = TxnBatch {
        ops: vec![put(b"bob", b"b"), put(b"alice", b"a")],
        force_abort: false,
    };
    let outcome = coord
        .execute(&batch, route_by_parity)
        .await
        .expect("commit");
    assert_eq!(outcome, TxnOutcome::Committed { operations: 2 });

    let XaBranch::Local(local) = coord.branch(0).unwrap() else {
        panic!("branch 0 is local");
    };
    assert_eq!(
        local.get_object(b"u", b"bob").unwrap().as_deref(),
        Some(&b"b"[..])
    );
    assert_eq!(
        remote_peer
            .participant_for(b"west")
            .unwrap()
            .get_object(b"u", b"alice")
            .unwrap()
            .as_deref(),
        Some(&b"a"[..])
    );
}

#[tokio::test]
async fn dnode_loopback_abort_leaves_nothing_visible() {
    let d_local = scratch_dir();
    let d_log = scratch_dir();
    let local = open_participant(&d_local, b"east");

    // The peer hosts only "west"; route the remote branch to an
    // environment the peer does not own so its prepare votes Abort.
    let (_remote_peer, addr, _d_remote) = spawn_peer(b"west").await;
    let transport: Arc<dyn XaTransport> =
        Arc::new(DnodeXaTransport::new(addr).with_timeout(Duration::from_secs(2)));

    let branches = vec![
        XaBranch::Local(Box::new(local)),
        // Branch qualifier "ghost" is unknown to the peer -> Abort.
        XaBranch::Remote(RemoteXaBranch::new(transport, b"ghost".to_vec())),
    ];
    let log = InDoubtLog::new(d_log.path().join("indoubt.log"));
    let coord = CrossNodeCoordinator::new(branches, log).with_retry(tight_retry());

    let batch = TxnBatch {
        ops: vec![put(b"bob", b"b"), put(b"alice", b"a")],
        force_abort: false,
    };
    let err = coord
        .execute(&batch, route_by_parity)
        .await
        .expect_err("unknown remote env aborts");
    assert!(format!("{err}").contains("aborted"));

    // The local branch's write was rolled back.
    let XaBranch::Local(local) = coord.branch(0).unwrap() else {
        panic!("branch 0 is local");
    };
    assert!(local.get_object(b"u", b"bob").unwrap().is_none());
}
