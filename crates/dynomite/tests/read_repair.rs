//! Read-repair integration tests.
//!
//! Three-replica in-process cluster. Each replica is a tokio task that
//! pulls outbound requests off a per-peer channel, records them, and
//! answers with a scripted reply. With read repairs on and both
//! consistencies above `DC_ONE`:
//!
//! * every repair-aware command reaches the replicas as a repair-script
//!   request; under `DC_ONE` it is forwarded unchanged;
//! * a repairable read answers the client with the freshest replica's
//!   plain value and sends the add repair to each stale replica;
//! * a delete answered by every replica is followed by its cleanup
//!   script on each of them;
//! * writes to one key stamped in the same millisecond by concurrent
//!   client tasks reach every replica in one order.

use std::sync::Arc;
use std::time::Duration;

use dynomite::cluster::dispatch::ClusterDispatcher;
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::hashkit::DynToken;
use dynomite::io::mbuf::MbufPool;
use dynomite::msg::{set_read_repairs_enabled, ConsistencyLevel, Msg, MsgParseResult, MsgType};
use dynomite::net::dispatcher::{Dispatcher, OutboundEnvelope};
use dynomite::net::server::OutboundRequest;
use dynomite::proto::dnode::DmsgType;
use dynomite::proto::redis::redis_parse_req;
use dynomite::proto::redis::repair::request::{
    encode_command, Script, ScriptRequest, REPAIR_SCRIPT_CMD,
};
use tokio::sync::mpsc;

type Recorded = (u32, DmsgType, Vec<u8>);

/// Spawn one replica that records every request and answers a
/// repair-script read with `read_reply` and anything else with `:1`.
fn spawn_replica(
    peer_idx: u32,
    read_reply: Vec<u8>,
    mut rx: mpsc::Receiver<OutboundRequest>,
    recorded: mpsc::Sender<Recorded>,
) {
    tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            let _ = recorded.send((peer_idx, req.ty, req.bytes.clone())).await;
            let is_read = ScriptRequest::decode(&req.bytes)
                .is_some_and(|s| matches!(s.script, Some(Script::Get | Script::Hget)));
            let payload = if is_read {
                read_reply.clone()
            } else {
                b":1\r\n".to_vec()
            };
            let mut rsp = Msg::new(req.req_id, MsgType::Unknown, false);
            rsp.set_parent_id(req.req_id);
            rsp.append_bytes(&MbufPool::default(), &payload);
            let _ = req
                .responder
                .send(OutboundEnvelope {
                    req_id: req.req_id,
                    rsp,
                    span: req.span,
                    source_peer_idx: req.target_peer_idx,
                })
                .await;
        }
    });
}

fn local_peer(idx: u32, rack: &str, tok: u32, is_local: bool) -> Peer {
    let mut p = Peer::new(
        idx,
        PeerEndpoint::tcp(
            format!("10.0.0.{idx}"),
            8101 + u16::try_from(idx).unwrap_or(0),
        ),
        rack.into(),
        "dc1".into(),
        vec![DynToken::from_u32(tok)],
        is_local,
        true,
        false,
    );
    p.set_state(PeerState::Normal, 0);
    p
}

fn pool_three_replicas(level: ConsistencyLevel) -> Arc<ServerPool> {
    let cfg = PoolConfig {
        dc: "dc1".into(),
        rack: "rA".into(),
        read_consistency: level,
        write_consistency: level,
        ..PoolConfig::default()
    };
    let peers = vec![
        // Make peer 0 NON-local so the dispatcher does NOT
        // short-circuit a single-rack DC_QUORUM into the local
        // datastore branch. With three peers across three racks
        // the planner produces a Replicas plan with three
        // targets.
        local_peer(0, "rA", 1_000, false),
        local_peer(1, "rB", 2_000, false),
        local_peer(2, "rC", 3_000, false),
    ];
    let pool = ServerPool::new(cfg, peers);
    pool.preselect_remote_racks();
    Arc::new(pool)
}

/// A parsed client request.
fn request(id: u64, args: &[&str]) -> Msg {
    let wire = encode_command(args);
    let mut m = Msg::new(id, MsgType::Unknown, true);
    assert_eq!(redis_parse_req(&mut m, &wire), MsgParseResult::Ok);
    m.append_bytes(&MbufPool::default(), &wire);
    m
}

fn ts_reply(status: &str, ts: u64, value: &str) -> Vec<u8> {
    encode_command(&[status, &ts.to_string(), value])
}

/// Dispatch `req` to three replicas answering a read with `replies`;
/// returns the client's reply bytes and every request the replicas saw
/// within a short window.
async fn run(level: ConsistencyLevel, req: Msg, replies: [Vec<u8>; 3]) -> (Vec<u8>, Vec<Recorded>) {
    let (rec_tx, mut rec_rx) = mpsc::channel::<Recorded>(64);
    let mut disp = ClusterDispatcher::new(pool_three_replicas(level));
    for (i, reply) in replies.into_iter().enumerate() {
        let (tx, rx) = mpsc::channel::<OutboundRequest>(8);
        let idx = u32::try_from(i).unwrap();
        spawn_replica(idx, reply, rx, rec_tx.clone());
        disp = disp.with_peer_backend(idx, tx);
    }
    drop(rec_tx);
    let (client_tx, mut client_rx) = mpsc::channel::<OutboundEnvelope>(4);
    let _ = disp.dispatch(req, client_tx);
    let env = tokio::time::timeout(Duration::from_secs(5), client_rx.recv())
        .await
        .expect("timeout")
        .expect("no reply");
    let mut seen = Vec::new();
    while let Ok(Some(r)) = tokio::time::timeout(Duration::from_millis(300), rec_rx.recv()).await {
        seen.push(r);
    }
    let bytes: Vec<u8> = env
        .rsp
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect();
    (bytes, seen)
}

fn is_script(bytes: &[u8]) -> bool {
    ScriptRequest::decode(bytes).is_some()
        && bytes
            .windows(REPAIR_SCRIPT_CMD.len())
            .any(|w| w == REPAIR_SCRIPT_CMD)
}

// One test owns the process-wide read-repair flag, so the phases run in
// sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamp_read_repair() {
    let ok = || encode_command(&["E", "1", "v"]);
    set_read_repairs_enabled(true);

    // DC_ONE: the gate is off and the SET is forwarded unchanged.
    let set = request(1, &["SET", "k", "v"]);
    let (_, seen) = run(ConsistencyLevel::DcOne, set, [ok(), ok(), ok()]).await;
    assert!(!seen.is_empty());
    assert!(seen
        .iter()
        .all(|(_, _, b)| b.as_slice() == encode_command(&["SET", "k", "v"])));

    // DC_SAFE_QUORUM: every replica gets the repair-script SET.
    let set = request(2, &["SET", "k", "v"]);
    let (_, seen) = run(ConsistencyLevel::DcSafeQuorum, set, [ok(), ok(), ok()]).await;
    assert_eq!(seen.len(), 3);
    for (_, _, b) in &seen {
        let s = ScriptRequest::decode(b).expect("repair script");
        assert_eq!((s.script, s.key.as_slice()), (Some(Script::Set), &b"k"[..]));
        assert_eq!(s.values, vec![b"v".to_vec()]);
    }

    // A read: replica 1 holds the newest value; the client gets it plain
    // and replicas 0 and 2 get the add repair at its timestamp. The two
    // stale replies carry the same timestamp: every reply below the
    // newest is repaired, whatever order the replies arrive in.
    let get = request(3, &["GET", "k"]);
    let replies = [
        ts_reply("E", 5, "old"),
        ts_reply("E", 9, "new"),
        ts_reply("E", 5, "old"),
    ];
    let (bytes, seen) = run(ConsistencyLevel::DcSafeQuorum, get, replies).await;
    assert_eq!(bytes, b"$3\r\nnew\r\n");
    let mut repaired: Vec<u32> = seen
        .iter()
        .filter_map(|(p, ty, b)| {
            let s = ScriptRequest::decode(b)?;
            (s.script == Some(Script::Set)).then(|| {
                assert_eq!(*ty, DmsgType::ReqForward);
                assert_eq!((s.ts, s.values.clone()), (9, vec![b"new".to_vec()]));
                *p
            })
        })
        .collect();
    repaired.sort_unstable();
    assert_eq!(repaired, vec![0, 2]);

    // The freshest state is a delete: the stale replica is repaired by
    // a delete, the client sees nil.
    let get = request(4, &["GET", "k"]);
    let r_nil = b"*3\r\n$1\r\nR\r\n$1\r\n9\r\n$-1\r\n".to_vec();
    let replies = [r_nil.clone(), ts_reply("E", 5, "old"), r_nil];
    let (bytes, seen) = run(ConsistencyLevel::DcSafeQuorum, get, replies).await;
    assert_eq!(bytes, b"$-1\r\n");
    let dels: Vec<u32> = seen
        .iter()
        .filter(|(_, _, b)| ScriptRequest::decode(b).is_some_and(|s| s.script == Some(Script::Del)))
        .map(|(p, _, _)| *p)
        .collect();
    assert_eq!(dels, vec![1]);

    // Equal timestamps: nothing to repair, the value is answered plain.
    let get = request(5, &["HGET", "h", "f"]);
    let same = || ts_reply("E", 7, "x");
    let (bytes, seen) = run(
        ConsistencyLevel::DcSafeQuorum,
        get,
        [same(), same(), same()],
    )
    .await;
    assert_eq!(bytes, b"$1\r\nx\r\n");
    assert_eq!(seen.len(), 3, "no repair writes: {seen:?}");

    // A delete answered by every replica is followed by its cleanup.
    let del = request(6, &["SREM", "s", "m"]);
    let (_, seen) = run(ConsistencyLevel::DcSafeQuorum, del, [ok(), ok(), ok()]).await;
    let cleanups: Vec<ScriptRequest> = seen
        .iter()
        .filter_map(|(_, _, b)| ScriptRequest::decode(b))
        .filter(|s| s.script == Some(Script::CleanupHdel))
        .collect();
    assert_eq!(cleanups.len(), 3);
    assert!(cleanups.iter().all(|s| s.fields == vec![b"m".to_vec()]));
    assert!(seen.iter().all(|(_, _, b)| is_script(b)));

    same_millisecond_writes_reach_every_replica_in_one_order().await;

    set_read_repairs_enabled(false);
}

/// Many tasks on a multi-thread runtime SET one key with a frozen clock,
/// so every write carries the same timestamp. A replica keeps the write
/// it applies last, so every replica must receive them in the same order.
async fn same_millisecond_writes_reach_every_replica_in_one_order() {
    const WRITERS: usize = 16;
    const WRITES: usize = 50;
    let mut disp = ClusterDispatcher::new(pool_three_replicas(ConsistencyLevel::DcSafeQuorum))
        .with_repair_clock(|| 1_800_000_000_000);
    let (rec_tx, mut rec_rx) = mpsc::channel::<Recorded>(WRITERS * WRITES * 3);
    for i in 0..3u32 {
        let (tx, rx) = mpsc::channel::<OutboundRequest>(WRITERS * WRITES);
        spawn_replica(i, b":1\r\n".to_vec(), rx, rec_tx.clone());
        disp = disp.with_peer_backend(i, tx);
    }
    drop(rec_tx);
    let disp = Arc::new(disp);
    let tasks: Vec<_> = (0..WRITERS)
        .map(|w| {
            let disp = Arc::clone(&disp);
            tokio::spawn(async move {
                for i in 0..WRITES {
                    let id = u64::try_from(1_000 + w * WRITES + i).unwrap();
                    let (client_tx, mut client_rx) = mpsc::channel::<OutboundEnvelope>(4);
                    let _ =
                        disp.dispatch(request(id, &["SET", "k", &format!("w{w}i{i}")]), client_tx);
                    let _ = tokio::time::timeout(Duration::from_secs(5), client_rx.recv()).await;
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.expect("writer");
    }
    drop(disp);
    let mut order: [Vec<Vec<u8>>; 3] = Default::default();
    let mut stamps = std::collections::BTreeSet::new();
    while let Ok(Some((p, _, b))) =
        tokio::time::timeout(Duration::from_secs(2), rec_rx.recv()).await
    {
        let s = ScriptRequest::decode(&b).expect("repair script");
        stamps.insert(s.ts);
        order[usize::try_from(p).unwrap()].push(s.values[0].clone());
    }
    assert_eq!(stamps.len(), 1, "one frozen millisecond, one stamp");
    assert_eq!(order[0].len(), WRITERS * WRITES);
    assert_eq!(order[0], order[1]);
    assert_eq!(order[0], order[2]);
}
