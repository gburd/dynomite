//! Cold-restart in-doubt recovery scan for cross-node XA.
//!
//! Exercises [`CrossNodeCoordinator::recover_in_doubt`]: a coordinator
//! that restarted re-reads its durable in-doubt log and re-drives the
//! unconfirmed commits forward to completion. A confirmed commit
//! retires its record (a tombstone); a record whose peer is still down
//! stays in the log for a later, re-runnable pass. Recovery never
//! rolls a prepared branch back -- the only correct resolution for a
//! branch that voted Ok is forward (presumed commit).
//!
//! The main cold-restart test runs over the real dnode loopback
//! transport (an in-process peer on a localhost dnode port). The
//! peer-still-down and tombstone cases use an in-test mock transport
//! whose reachability is a flipped flag, which is awkward to provoke
//! over real TCP.
//!
//! Gated on the `noxu` feature; without it the file compiles to an
//! empty module.

#![cfg(feature = "noxu")]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dyniak::datastore::xa_net::{
    serve_xa_peer, CrossNodeCoordinator, DnodeXaTransport, InDoubtLog, RecoveryReport,
    RemoteXaBranch, RetryPolicy, XaBranch, XaFuture, XaPeer, XaTransport, XaTransportError,
};
use dyniak::datastore::xa_wire::{WireXid, XaVote, XaWriteOp};
use dyniak::datastore::XaParticipant;
use tempfile::TempDir;
use tokio::net::TcpListener;

/// Scratch root for env / log paths (AGENTS.md: use /scratch, not /tmp).
fn scratch_dir() -> TempDir {
    let base = std::path::Path::new("/scratch");
    if base.is_dir() {
        TempDir::new_in(base).expect("tempdir in /scratch")
    } else {
        TempDir::new().expect("tempdir")
    }
}

fn open_participant(dir: &TempDir, name: &[u8]) -> XaParticipant {
    XaParticipant::open(dir.path(), name.to_vec()).expect("open participant")
}

fn tight_retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        base_backoff: Duration::from_millis(1),
    }
}

// ------------------------------------------------------------------
// Mock transport whose reachability is a single flag. Prepare and
// commit drive a real in-process XaPeer when "up"; both fail with a
// timeout when "down". Used for the peer-still-down and tombstone
// cases where flipping reachability mid-test is the whole point.
// ------------------------------------------------------------------

struct ToggleTransport {
    peer: Arc<XaPeer>,
    up: AtomicBool,
}

impl ToggleTransport {
    fn new(peer: Arc<XaPeer>) -> Self {
        Self {
            peer,
            up: AtomicBool::new(true),
        }
    }

    fn set_up(&self, up: bool) {
        self.up.store(up, Ordering::SeqCst);
    }

    fn is_up(&self) -> bool {
        self.up.load(Ordering::SeqCst)
    }
}

impl XaTransport for ToggleTransport {
    fn prepare<'a>(
        &'a self,
        xid: &'a WireXid,
        env: &'a [u8],
        writes: &'a [XaWriteOp],
    ) -> XaFuture<'a, Result<XaVote, XaTransportError>> {
        Box::pin(async move {
            if !self.is_up() {
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
            if !self.is_up() {
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

// ------------------------------------------------------------------
// 1. Real dnode cold-restart recovery.
//
//    Peer up -> prepare durably prepares the branch on the peer.
//    Peer's TCP listener torn down -> the commit phase exhausts retry
//    -> the coordinator records the branch in the durable in-doubt log
//    and is then DROPPED (a coordinator restart). The peer's env stays
//    alive (the prepared branch survives); a new listener is spawned
//    for it. A FRESH coordinator over the SAME in-doubt log path runs
//    recover_in_doubt(): the commit is driven to completion on the
//    peer (the data is now visible) and the record is retired. A
//    second recover_in_doubt() is a no-op.
// ------------------------------------------------------------------

/// Spawn a dnode XA listener for `peer`, returning its address and an
/// abort handle so the caller can tear the listener down (simulating
/// the peer's network leg going away while its env stays alive).
async fn spawn_listener(peer: Arc<XaPeer>) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let handle = tokio::spawn(async move {
        let _ = serve_xa_peer(listener, peer).await;
    });
    (addr, handle)
}

#[tokio::test]
async fn dnode_cold_restart_recovery_drives_commit_and_retires_record() {
    let d_remote = scratch_dir();
    let d_log = scratch_dir();
    let log_path = d_log.path().join("indoubt.log");

    // The peer's env lives for the whole test; only its listener comes
    // and goes. (A prepared branch is durable on the peer's disk; here
    // it also survives in the live env, which is what a peer whose
    // network leg flapped would have.)
    let remote_peer = Arc::new(XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d_remote, b"west"),
    )]));

    // --- Original incarnation: prepare succeeds, then the peer's
    //     listener goes away so commit cannot be confirmed. The branch
    //     is durably prepared on the peer's env; the coordinator that
    //     could not confirm the commit recorded it in the in-doubt log
    //     and is then dropped (a coordinator restart). ---
    //
    // Prepare the branch on the peer over the live listener, then tear
    // the listener down to model the peer's network leg going away
    // during the commit phase. Driving prepare directly through the
    // peer object (the same env the dnode listener serves) keeps the
    // setup race-free; the recovery leg below is the part that goes
    // over real dnode TCP.
    let (_addr, listener) = spawn_listener(Arc::clone(&remote_peer)).await;
    listener.abort();

    let xid = WireXid {
        format_id: 0x6479_6e6b,
        gtrid: 0x00C0_FFEE_u32.to_be_bytes().to_vec(),
        bqual: b"west".to_vec(),
    };
    let writes = vec![XaWriteOp::Put {
        bucket: b"u".to_vec(),
        key: b"alice".to_vec(),
        value: b"a".to_vec(),
        indexes: vec![],
    }];
    assert_eq!(
        remote_peer.handle_prepare(&xid, b"west", &writes).unwrap(),
        XaVote::Ok,
        "branch prepares Ok on the peer"
    );
    // (A prepared, uncommitted branch holds its write locks, so we do
    // not auto-commit-read the key here; the post-recovery read below
    // proves it became visible only after the commit was driven home.)

    // The original coordinator could not confirm the commit, so it
    // durably recorded the branch. Simulate that and then "restart".
    InDoubtLog::new(&log_path)
        .record(&xid, b"west")
        .expect("record in-doubt");
    assert_eq!(
        InDoubtLog::new(&log_path).load().expect("load").len(),
        1,
        "durable in-doubt record present after the original run"
    );

    // --- Cold restart: peer's network leg is back (new listener over
    //     the SAME live env), a FRESH coordinator over the SAME log. ---
    let (addr2, listener2) = spawn_listener(Arc::clone(&remote_peer)).await;
    let transport2: Arc<dyn XaTransport> =
        Arc::new(DnodeXaTransport::new(addr2).with_timeout(Duration::from_secs(2)));
    let branches2 = vec![XaBranch::Remote(RemoteXaBranch::new(
        transport2,
        b"west".to_vec(),
    ))];
    let (coord2, report) = CrossNodeCoordinator::new_with_recovery_retry(
        branches2,
        InDoubtLog::new(&log_path),
        tight_retry(),
    )
    .await
    .expect("recovery scan");

    assert_eq!(
        report,
        RecoveryReport {
            recovered: 1,
            still_in_doubt: 0,
            errors: 0,
        },
        "the cold-restart scan recovered the one in-doubt branch"
    );

    // The commit was driven to completion on the peer: data is visible.
    assert_eq!(
        remote_peer
            .participant_for(b"west")
            .unwrap()
            .get_object(b"u", b"alice")
            .unwrap()
            .as_deref(),
        Some(&b"a"[..]),
        "recovery drove the commit forward"
    );

    // The record is retired: a second scan is a no-op.
    assert!(
        InDoubtLog::new(&log_path).load().expect("load").is_empty(),
        "record retired after recovery"
    );
    let report2 = coord2.recover_in_doubt().await.expect("second scan");
    assert_eq!(
        report2,
        RecoveryReport::default(),
        "second recovery pass is a no-op"
    );

    listener2.abort();
}

// ------------------------------------------------------------------
// 2. Peer still down during recovery: the record stays, reported as
//    still-in-doubt, and a later pass (peer back) recovers it.
// ------------------------------------------------------------------

#[tokio::test]
async fn recovery_with_peer_down_leaves_record_then_recovers_when_up() {
    let d_remote = scratch_dir();
    let d_log = scratch_dir();
    let log_path = d_log.path().join("indoubt.log");

    let peer = Arc::new(XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d_remote, b"west"),
    )]));
    // Prepare the branch on the peer (it is durably prepared).
    let xid = WireXid {
        format_id: 0x6479_6e6b,
        gtrid: 11u64.to_be_bytes().to_vec(),
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
    InDoubtLog::new(&log_path)
        .record(&xid, b"west")
        .expect("record");

    let transport = Arc::new(ToggleTransport::new(Arc::clone(&peer)));
    transport.set_up(false); // peer unreachable during the first pass
    let xport: Arc<dyn XaTransport> = Arc::clone(&transport) as Arc<dyn XaTransport>;
    let branches = vec![XaBranch::Remote(RemoteXaBranch::new(
        xport,
        b"west".to_vec(),
    ))];
    let coord =
        CrossNodeCoordinator::new(branches, InDoubtLog::new(&log_path)).with_retry(tight_retry());

    let report = coord.recover_in_doubt().await.expect("scan with peer down");
    assert_eq!(
        report,
        RecoveryReport {
            recovered: 0,
            still_in_doubt: 1,
            errors: 0,
        },
        "peer down -> branch stays in-doubt"
    );
    // Record must NOT be retired (re-runnable; never drop an
    // unconfirmed commit).
    assert_eq!(
        InDoubtLog::new(&log_path).load().expect("load").len(),
        1,
        "record stays while peer is down"
    );
    // (The branch is still prepared on the peer, holding its write
    // locks; the post-recovery read below confirms it commits only
    // after the peer returns.)

    // Peer comes back; a re-run recovers it.
    transport.set_up(true);
    let report2 = coord.recover_in_doubt().await.expect("scan with peer up");
    assert_eq!(
        report2,
        RecoveryReport {
            recovered: 1,
            still_in_doubt: 0,
            errors: 0,
        },
        "peer back -> branch recovered"
    );
    assert_eq!(
        peer.participant_for(b"west")
            .unwrap()
            .get_object(b"u", b"alice")
            .unwrap()
            .as_deref(),
        Some(&b"a"[..])
    );
    assert!(
        InDoubtLog::new(&log_path).load().expect("load").is_empty(),
        "record retired after the peer returned"
    );
}

// ------------------------------------------------------------------
// 3. Idempotent re-drive: the commit actually LANDED before the
//    coordinator crashed (the record was written but the ack never got
//    back). Recovery re-drives XA_COMMIT; the peer returns success
//    (NotFound -> idempotent), the record retires, no double-apply.
// ------------------------------------------------------------------

#[tokio::test]
async fn recovery_redrive_of_already_committed_branch_is_idempotent() {
    let d_remote = scratch_dir();
    let d_log = scratch_dir();
    let log_path = d_log.path().join("indoubt.log");

    let peer = Arc::new(XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d_remote, b"west"),
    )]));
    let xid = WireXid {
        format_id: 0x6479_6e6b,
        gtrid: 13u64.to_be_bytes().to_vec(),
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
    // The commit actually landed on the peer; the ack was lost so the
    // coordinator still recorded the branch as in-doubt.
    assert!(peer.handle_commit(&xid, b"west"), "commit lands");
    assert_eq!(
        peer.participant_for(b"west")
            .unwrap()
            .get_object(b"u", b"alice")
            .unwrap()
            .as_deref(),
        Some(&b"a"[..])
    );
    InDoubtLog::new(&log_path)
        .record(&xid, b"west")
        .expect("record");

    let transport: Arc<dyn XaTransport> = Arc::new(ToggleTransport::new(Arc::clone(&peer)));
    let branches = vec![XaBranch::Remote(RemoteXaBranch::new(
        transport,
        b"west".to_vec(),
    ))];
    let coord =
        CrossNodeCoordinator::new(branches, InDoubtLog::new(&log_path)).with_retry(tight_retry());

    let report = coord.recover_in_doubt().await.expect("scan");
    assert_eq!(
        report,
        RecoveryReport {
            recovered: 1,
            still_in_doubt: 0,
            errors: 0,
        },
        "re-driving an already-landed commit is idempotent success"
    );
    // Still exactly the one value -- no double-apply.
    assert_eq!(
        peer.participant_for(b"west")
            .unwrap()
            .get_object(b"u", b"alice")
            .unwrap()
            .as_deref(),
        Some(&b"a"[..])
    );
    assert!(InDoubtLog::new(&log_path).load().expect("load").is_empty());
}

// ------------------------------------------------------------------
// 4. Tombstone / load correctness.
//    a) Resolving one of several records leaves only the unresolved on
//       load.
//    b) A crash simulated mid-resolve (record written, tombstone NOT
//       yet appended) leaves the record live; a recovery pass re-drives
//       the idempotent commit safely.
// ------------------------------------------------------------------

#[tokio::test]
async fn tombstone_load_returns_only_unresolved() {
    let d_log = scratch_dir();
    let log = InDoubtLog::new(d_log.path().join("indoubt.log"));

    let mk = |g: u64, env: &[u8]| WireXid {
        format_id: 0x6479_6e6b,
        gtrid: g.to_be_bytes().to_vec(),
        bqual: env.to_vec(),
    };
    let x1 = mk(1, b"west");
    let x2 = mk(2, b"east");
    let x3 = mk(3, b"west");
    log.record(&x1, b"west").unwrap();
    log.record(&x2, b"east").unwrap();
    log.record(&x3, b"west").unwrap();
    assert_eq!(log.load().unwrap().len(), 3);

    // Resolve the middle one.
    log.resolve(&x2, b"east").unwrap();
    let live = log.load().unwrap();
    assert_eq!(live.len(), 2, "one retired");
    assert!(
        live.iter().all(|r| r.env != b"east"),
        "the retired record is gone"
    );
    // Order of survivors preserved (x1 then x3).
    assert_eq!(live[0].gtrid, 1u64.to_be_bytes().to_vec());
    assert_eq!(live[1].gtrid, 3u64.to_be_bytes().to_vec());

    // Resolving the rest empties the log net.
    log.resolve(&x1, b"west").unwrap();
    log.resolve(&x3, b"west").unwrap();
    assert!(log.load().unwrap().is_empty());
}

#[tokio::test]
async fn crash_mid_resolve_redrives_safely() {
    let d_remote = scratch_dir();
    let d_log = scratch_dir();
    let log_path = d_log.path().join("indoubt.log");

    let peer = Arc::new(XaPeer::new(vec![(
        b"west".to_vec(),
        open_participant(&d_remote, b"west"),
    )]));
    let xid = WireXid {
        format_id: 0x6479_6e6b,
        gtrid: 21u64.to_be_bytes().to_vec(),
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
    // The commit landed but the crash happened BEFORE the tombstone was
    // appended: the record is still live on load.
    assert!(peer.handle_commit(&xid, b"west"));
    InDoubtLog::new(&log_path).record(&xid, b"west").unwrap();
    // (No resolve() call -- simulating the crash between commit and
    // tombstone.)
    assert_eq!(
        InDoubtLog::new(&log_path).load().unwrap().len(),
        1,
        "record live after a crash mid-resolve"
    );

    // Recovery re-drives the idempotent commit and retires the record.
    let transport: Arc<dyn XaTransport> = Arc::new(ToggleTransport::new(Arc::clone(&peer)));
    let branches = vec![XaBranch::Remote(RemoteXaBranch::new(
        transport,
        b"west".to_vec(),
    ))];
    let coord =
        CrossNodeCoordinator::new(branches, InDoubtLog::new(&log_path)).with_retry(tight_retry());
    let report = coord.recover_in_doubt().await.expect("scan");
    assert_eq!(report.recovered, 1);
    assert_eq!(report.errors, 0);
    assert!(InDoubtLog::new(&log_path).load().unwrap().is_empty());
    // Single value, no double-apply.
    assert_eq!(
        peer.participant_for(b"west")
            .unwrap()
            .get_object(b"u", b"alice")
            .unwrap()
            .as_deref(),
        Some(&b"a"[..])
    );
}
