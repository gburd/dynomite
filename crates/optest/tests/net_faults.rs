//! Replication, placement, and network-partition behaviour.
//!
//! Each scenario runs once per engine: against the C reference (when its
//! binary is present) to pin what Dynomite does, and against `dynomited`,
//! which must do the same.

#[macro_use]
mod common;

use std::time::Duration;

use common::{assert_history, eventually, op, seed, start_or_skip};
use optest::check::Guarantee;
use optest::cluster::{node_ip, Engine};
use optest::history::{History, Op, Outcome};
use optest::net::{Impairment, Netem};
use optest::ClusterSpec;

const T: Duration = Duration::from_secs(5);

/// The C reference first (it defines the expected behaviour), then the
/// Rust engine. `OPTEST_ENGINES=rust|c` narrows the list.
fn engines() -> Vec<Engine> {
    let mut e = Vec::new();
    let only = std::env::var("OPTEST_ENGINES").unwrap_or_default();
    if optest::Tools::discover().c_dynomite.is_some() && only != "rust" {
        e.push(Engine::C);
    }
    if only != "c" {
        e.push(Engine::Rust);
    }
    e
}

/// One DC, three racks of one node: every write reaches all three
/// replicas (replication factor = racks), whichever node it enters by.
#[test]
fn rf3_every_write_lands_on_every_rack() {
    sandboxed!("rf3_every_write_lands_on_every_rack");
    for engine in engines() {
        let Some(c) = start_or_skip(ClusterSpec::grid(1, 3, 1).engine(engine)) else {
            return;
        };
        for i in 0..90 {
            let mut cl = c.nodes[i % 3].client(T).expect("connect");
            assert!(cl
                .cmd(&["SET", &format!("k{i}"), "v"])
                .expect("set")
                .is_ok());
        }
        let sizes = eventually("replication", Duration::from_secs(10), || {
            let s = c.store_sizes().ok()?;
            (s.iter().all(|&n| n == 90)).then_some(s)
        });
        eprintln!("[{engine:?}] replicas per rack: {sizes:?}");
    }
}

/// One rack of three nodes: each key lives on exactly one node and every
/// node owns a share (no placement collapse).
#[test]
fn single_rack_partitions_the_keyspace() {
    sandboxed!("single_rack_partitions_the_keyspace");
    for engine in engines() {
        let Some(c) = start_or_skip(ClusterSpec::grid(1, 1, 3).engine(engine)) else {
            return;
        };
        let mut cl = c.nodes[0].client(T).expect("connect");
        for i in 0..600 {
            assert!(cl
                .cmd(&["SET", &format!("key:{i}"), "v"])
                .expect("set")
                .is_ok());
        }
        let sizes = c.store_sizes().expect("dbsize");
        eprintln!("[{engine:?}] keys per node: {sizes:?}");
        assert_eq!(
            sizes.iter().sum::<i64>(),
            600,
            "{engine:?}: every key stored once"
        );
        assert!(
            sizes.iter().all(|&n| n > 100),
            "{engine:?}: collapse {sizes:?}"
        );
    }
}

/// DC_ONE through an isolated node keeps serving (local rack answers);
/// after healing, writes made during the partition reach the other racks
/// only through later traffic, and no read ever returns a wrong value.
#[test]
fn dc_one_partitioned_node_serves_locally_and_stays_correct() {
    sandboxed!("dc_one_partitioned_node_serves_locally_and_stays_correct");
    for engine in engines() {
        let Some(c) = start_or_skip(ClusterSpec::grid(1, 3, 1).engine(engine)) else {
            return;
        };
        let mut net = Netem::new(seed()).expect("tc");
        net.isolate(node_ip(0), &c.others(0)).expect("isolate");
        let mut h = History::new();
        for i in 0..40 {
            let k = format!("p{i}");
            let o = op(
                &c,
                &mut h,
                0,
                0,
                Op::Write {
                    key: k.clone(),
                    value: format!("{k}=1"),
                },
                T,
            );
            assert_eq!(
                o,
                Outcome::Ok,
                "{engine:?}: DC_ONE write through isolated node"
            );
            op(
                &c,
                &mut h,
                1,
                0,
                Op::Read {
                    key: k,
                    value: None,
                },
                T,
            );
        }
        assert!(
            net.matched().expect("tc -s") > 0,
            "partition saw no traffic"
        );
        net.heal().expect("heal");
        for i in 0..40 {
            let k = format!("p{i}");
            for n in 1..3 {
                op(
                    &c,
                    &mut h,
                    2 + n,
                    n,
                    Op::Read {
                        key: k.clone(),
                        value: None,
                    },
                    T,
                );
            }
        }
        assert_history(
            &format!("dc_one_partition_{engine:?}"),
            &h,
            Guarantee::Eventual,
        );
    }
}

/// DC_QUORUM with one of three replicas cut off: writes and reads through
/// the majority side succeed, and every read after an acknowledged write
/// sees it.
#[test]
fn dc_quorum_survives_minority_partition() {
    sandboxed!("dc_quorum_survives_minority_partition");
    for engine in engines() {
        let spec = ClusterSpec::grid(1, 3, 1)
            .engine(engine)
            .with("read_consistency", "DC_QUORUM")
            .with("write_consistency", "DC_QUORUM");
        let Some(c) = start_or_skip(spec) else {
            return;
        };
        let mut net = Netem::new(seed()).expect("tc");
        net.isolate(node_ip(2), &c.others(2))
            .expect("isolate node 2");
        let mut h = History::new();
        let mut acked = 0;
        for i in 0..60 {
            let k = format!("q{i}");
            let entry = i % 2;
            if op(
                &c,
                &mut h,
                0,
                entry,
                Op::Write {
                    key: k.clone(),
                    value: format!("{k}=1"),
                },
                T,
            ) == Outcome::Ok
            {
                acked += 1;
            }
            op(
                &c,
                &mut h,
                1,
                1 - entry,
                Op::Read {
                    key: k,
                    value: None,
                },
                T,
            );
        }
        eprintln!("[{engine:?}] {acked}/60 quorum writes acknowledged under minority partition");
        assert!(
            acked >= 55,
            "{engine:?}: majority side lost availability ({acked}/60)"
        );
        assert_history(
            &format!("dc_quorum_minority_{engine:?}"),
            &h,
            Guarantee::Quorum,
        );
    }
}

/// DC_QUORUM through a node cut off from both peers cannot form a quorum:
/// it must fail requests (an error or a timeout), never acknowledge them.
#[test]
fn dc_quorum_isolated_node_refuses() {
    sandboxed!("dc_quorum_isolated_node_refuses");
    for engine in engines() {
        let spec = ClusterSpec::grid(1, 3, 1)
            .engine(engine)
            .with("read_consistency", "DC_QUORUM")
            .with("write_consistency", "DC_QUORUM");
        let Some(c) = start_or_skip(spec) else {
            return;
        };
        let mut net = Netem::new(seed()).expect("tc");
        net.isolate(node_ip(0), &c.others(0)).expect("isolate");
        let mut h = History::new();
        let mut acked = 0;
        for i in 0..10 {
            let k = format!("x{i}");
            let o = op(
                &c,
                &mut h,
                0,
                0,
                Op::Write {
                    key: k.clone(),
                    value: format!("{k}=1"),
                },
                T,
            );
            if o == Outcome::Ok {
                acked += 1;
            }
        }
        let replies: Vec<String> = h
            .events()
            .iter()
            .map(|e| format!("{:?}", e.outcome))
            .collect();
        eprintln!("[{engine:?}] isolated DC_QUORUM writes: {replies:?}");
        assert_eq!(
            acked, 0,
            "{engine:?}: isolated node acknowledged {acked} quorum writes"
        );
        net.heal().expect("heal");
        assert_history(
            &format!("dc_quorum_isolated_{engine:?}"),
            &h,
            Guarantee::Quorum,
        );
    }
}

/// A 300 ms one-way delay toward one replica does not change results; a
/// DC_QUORUM request completes on the two fast replicas.
#[test]
fn slow_replica_does_not_block_quorum() {
    sandboxed!("slow_replica_does_not_block_quorum");
    for engine in engines() {
        let spec = ClusterSpec::grid(1, 3, 1)
            .engine(engine)
            .with("read_consistency", "DC_QUORUM")
            .with("write_consistency", "DC_QUORUM");
        let Some(c) = start_or_skip(spec) else {
            return;
        };
        let mut net = Netem::new(seed()).expect("tc");
        net.impair_both(node_ip(0), node_ip(2), &Impairment::delay(300))
            .expect("delay");
        let mut cl = c.nodes[0].client(T).expect("connect");
        let t0 = std::time::Instant::now();
        for i in 0..20 {
            assert!(cl
                .cmd(&["SET", &format!("s{i}"), "v"])
                .expect("set")
                .is_ok());
        }
        let per_op = t0.elapsed() / 20;
        eprintln!("[{engine:?}] DC_QUORUM SET with one slow replica: {per_op:?}/op");
        // Two fast replicas form the quorum, so a request costs a local
        // round trip, nowhere near the 300 ms the slow replica adds.
        assert!(
            per_op < Duration::from_millis(20),
            "{engine:?}: waited on the slow replica"
        );
    }
}

/// SIGSTOP one replica for five seconds (a GC pause): DC_QUORUM keeps
/// serving through the other two, and the paused node rejoins on SIGCONT.
#[test]
fn paused_replica_is_tolerated_and_rejoins() {
    sandboxed!("paused_replica_is_tolerated_and_rejoins");
    for engine in engines() {
        let spec = ClusterSpec::grid(1, 3, 1)
            .engine(engine)
            .with("read_consistency", "DC_QUORUM")
            .with("write_consistency", "DC_QUORUM");
        let Some(c) = start_or_skip(spec) else {
            return;
        };
        c.nodes[2].pause().expect("SIGSTOP");
        let mut h = History::new();
        for i in 0..30 {
            let k = format!("g{i}");
            op(
                &c,
                &mut h,
                0,
                i % 2,
                Op::Write {
                    key: k.clone(),
                    value: format!("{k}=1"),
                },
                T,
            );
            op(
                &c,
                &mut h,
                1,
                (i + 1) % 2,
                Op::Read {
                    key: k,
                    value: None,
                },
                T,
            );
        }
        c.nodes[2].resume().expect("SIGCONT");
        let mut cl = c.nodes[2].client(T).expect("paused node reconnects");
        assert!(
            cl.cmd(&["PING"]).is_ok(),
            "{engine:?}: resumed node answers"
        );
        assert_history(&format!("paused_replica_{engine:?}"), &h, Guarantee::Quorum);
    }
}
