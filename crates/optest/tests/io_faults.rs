//! I/O and allocation faults: libfiu, libumem, libdirectio.
//!
//! The question each scenario asks is what Dynomite does when a syscall or
//! an allocation fails underneath it. The C reference answers first; the
//! Rust engine must not do worse: no wrong data, no hang past its timeout,
//! and when it does exit, a clean nonzero exit, never a hang or silent
//! corruption.

#[macro_use]
mod common;

use std::time::Duration;

use common::{assert_history, op, start_or_skip};
use optest::check::Guarantee;
use optest::cluster::Engine;
use optest::fault::{errno, Faults, FiuPoint};
use optest::history::{History, Op, Outcome};
use optest::ClusterSpec;

const T: Duration = Duration::from_secs(5);

fn engines() -> Vec<Engine> {
    let only = std::env::var("OPTEST_ENGINES").unwrap_or_default();
    let mut e = Vec::new();
    if optest::Tools::discover().c_dynomite.is_some() && only != "rust" {
        e.push(Engine::C);
    }
    if only != "c" {
        e.push(Engine::Rust);
    }
    e
}

fn with_faults(mut spec: ClusterSpec, node: usize, f: Faults) -> ClusterSpec {
    spec.nodes[node].faults = f;
    spec
}

/// Drive `n` write+read pairs through `entry`, recording the history.
fn workload(c: &optest::Cluster, h: &mut History, entry: usize, tag: &str, n: usize) -> usize {
    let mut ok = 0;
    for i in 0..n {
        let k = format!("{tag}{i}");
        if op(
            c,
            h,
            0,
            entry,
            Op::Write {
                key: k.clone(),
                value: format!("{k}=1"),
            },
            T,
        ) == Outcome::Ok
        {
            ok += 1;
        }
        op(
            c,
            h,
            1,
            entry,
            Op::Read {
                key: k,
                value: None,
            },
            T,
        );
    }
    ok
}

/// libfiu control preloaded but idle: the shim itself changes nothing.
#[test]
fn fiu_preload_idle_is_transparent() {
    sandboxed!("fiu_preload_idle_is_transparent");
    for engine in engines() {
        let f = Faults {
            fiu_control: true,
            ..Faults::default()
        };
        let spec = with_faults(ClusterSpec::grid(1, 3, 1).engine(engine), 0, f);
        let Some(c) = start_or_skip(spec) else { return };
        let mut h = History::new();
        let ok = workload(&c, &mut h, 0, "idle", 30);
        assert_eq!(ok, 30, "{engine:?}: idle libfiu changed behaviour");
        assert_history(&format!("fiu_idle_{engine:?}"), &h, Guarantee::Eventual);
    }
}

/// 5% of `write(2)` calls in the entry node fail with EIO for a window,
/// then heal. The fault covers every descriptor the process writes:
/// client and peer sockets, the datastore link, and the runtime's own
/// wakeup descriptors. Clients may see errors or timeouts during the window, but
/// never a wrong value, and the node serves normally once healed.
#[test]
fn fiu_write_errors_then_heal() {
    sandboxed!("fiu_write_errors_then_heal");
    for engine in engines() {
        let f = Faults {
            fiu_control: true,
            fiu_seed: 17,
            ..Faults::default()
        };
        let spec = with_faults(ClusterSpec::grid(1, 3, 1).engine(engine), 0, f);
        let Some(mut c) = start_or_skip(spec) else {
            return;
        };
        let fiu = c.nodes[0].fiu().expect("fiu control");
        let mut h = History::new();
        fiu.enable(&FiuPoint::random("posix/io/rw/write", 0.05, errno::EIO))
            .expect("enable");
        let during = workload(&c, &mut h, 0, "w", 60);
        let alive_during = c.nodes[0].alive();
        if alive_during {
            fiu.disable("posix/io/rw/write").expect("disable");
        }
        if !c.nodes[0].alive() {
            eprintln!(
                "[{engine:?}] entry node exited under write EIO: {:?}",
                c.nodes[0].exit_status()
            );
            c.restart(0).expect("restart after exit");
        }
        let after = workload(&c, &mut h, 0, "a", 30);
        eprintln!(
            "[{engine:?}] write EIO 5%: {during}/60 ok during, survived={alive_during}, {after}/30 ok after heal"
        );
        if after != 30 {
            let _ = h.write_jsonl(
                &std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
                    .join(format!("fiu_write_{engine:?}_fail.jsonl")),
            );
            let log = std::fs::read_to_string(c.nodes[0].log_path()).unwrap_or_default();
            let clean: String = log
                .chars()
                .filter(|ch| ch.is_ascii() && (*ch == '\n' || !ch.is_control()))
                .collect();
            for l in clean
                .lines()
                .filter(|l| l.contains("WARN") || l.contains("ERROR"))
                .take(12)
            {
                eprintln!("[{engine:?}] LOG {}", &l[..l.len().min(220)]);
            }
        }
        assert_eq!(
            after, 30,
            "{engine:?}: did not recover after the fault healed"
        );
        assert_history(&format!("fiu_write_{engine:?}"), &h, Guarantee::Eventual);
    }
}

/// Every new outbound `connect(2)` from one node fails. Established peer
/// links are unaffected, so service continues; the node must not crash.
#[test]
fn fiu_connect_refused_does_not_crash() {
    sandboxed!("fiu_connect_refused_does_not_crash");
    for engine in engines() {
        let f = Faults {
            fiu_control: true,
            ..Faults::default()
        };
        let spec = with_faults(ClusterSpec::grid(1, 3, 1).engine(engine), 1, f);
        let Some(mut c) = start_or_skip(spec) else {
            return;
        };
        let fiu = c.nodes[1].fiu().expect("fiu control");
        fiu.enable(&FiuPoint::always(
            "posix/io/net/connect",
            errno::ECONNREFUSED,
        ))
        .expect("enable");
        // Restart node 2 so node 1 must redial it and cannot.
        c.nodes[2].crash();
        c.launch(2).expect("relaunch node 2");
        c.wait_ready(2, Duration::from_secs(30))
            .expect("node 2 ready");
        let mut h = History::new();
        workload(&c, &mut h, 1, "c", 20);
        assert!(
            c.nodes[1].alive(),
            "{engine:?}: node crashed when connect(2) failed"
        );
        fiu.disable("posix/io/net/connect").expect("disable");
        assert_history(&format!("fiu_connect_{engine:?}"), &h, Guarantee::Eventual);
    }
}

/// libumem as the allocator, with guards: a clean run proves the engine
/// has no heap overrun or use-after-free on the request path that umem's
/// redzones catch.
#[test]
fn umem_guards_clean_run() {
    sandboxed!("umem_guards_clean_run");
    for engine in engines() {
        let f = Faults {
            umem_guards: true,
            ..Faults::default()
        };
        let spec = with_faults(ClusterSpec::grid(1, 3, 1).engine(engine), 0, f);
        let Some(mut c) = start_or_skip(spec) else {
            return;
        };
        let mut h = History::new();
        let ok = workload(&c, &mut h, 0, "g", 200);
        assert!(
            c.nodes[0].alive(),
            "{engine:?}: died under umem guards: {:?}",
            c.nodes[0].exit_status()
        );
        assert_eq!(ok, 200, "{engine:?}");
        assert_history(&format!("umem_guards_{engine:?}"), &h, Guarantee::Eventual);
    }
}

/// Injected allocation failures (umem `guards,mtbf=N`: a debug cache
/// returns NULL when the high-resolution clock is a multiple of N, so the
/// failures are frequent and not reproducible run to run; FAINT gives the
/// deterministic one-site-at-a-time sweep). C checks malloc's return and
/// fails the request; Rust aborts on allocation failure. Every engine must
/// never return a wrong value, and must either keep serving or exit.
#[test]
fn umem_alloc_failures_never_corrupt() {
    sandboxed!("umem_alloc_failures_never_corrupt");
    for engine in engines() {
        let f = Faults {
            umem_mtbf: Some(100_003),
            ..Faults::default()
        };
        let spec = with_faults(ClusterSpec::grid(1, 1, 1).engine(engine), 0, f);
        let Some(mut c) = start_or_skip(spec) else {
            return;
        };
        let mut h = History::new();
        let ok = workload(&c, &mut h, 0, "m", 200);
        let alive = c.nodes[0].alive();
        let serving = alive
            && c.nodes[0]
                .client(Duration::from_secs(3))
                .and_then(|mut cl| cl.cmd(&["PING"]))
                .is_ok();
        eprintln!(
            "[{engine:?}] umem guards,mtbf=100003: {ok}/200 writes ok, alive={alive} serving={serving} exit={:?}",
            c.nodes[0].exit_status()
        );
        assert!(
            !alive || serving,
            "{engine:?}: alive but not serving under allocation failures"
        );
        assert_history(&format!("umem_mtbf_{engine:?}"), &h, Guarantee::Eventual);
    }
}

/// libfiu's default for a failing `write(2)` picks a random errno from the
/// call's documented list, including `EBADFD`. Delivered to an internal
/// descriptor (tokio's eventfd waker) that is fatal to the runtime. The C
/// engine shrugs these off; the Rust engine may exit, but it must exit:
/// a process that is alive with its ports open and no longer serves is
/// the failure (regression: a worker-thread panic left exactly that).
#[test]
fn fiu_write_random_errno_exits_rather_than_hangs() {
    sandboxed!("fiu_write_random_errno_exits_rather_than_hangs");
    for engine in engines() {
        let f = Faults {
            fiu_control: true,
            fiu_seed: 17,
            ..Faults::default()
        };
        let spec = with_faults(ClusterSpec::grid(1, 3, 1).engine(engine), 0, f);
        let Some(mut c) = start_or_skip(spec) else {
            return;
        };
        let fiu = c.nodes[0].fiu().expect("fiu control");
        fiu.enable(&FiuPoint::Random {
            name: "posix/io/rw/write".into(),
            p: 0.05,
            errno: None,
        })
        .expect("enable");
        let mut h = History::new();
        let n = workload(&c, &mut h, 0, "r", 60);
        let _ = fiu.disable("posix/io/rw/write");
        let alive = c.nodes[0].alive();
        let serving = c.nodes[0]
            .client(Duration::from_secs(3))
            .and_then(|mut cl| cl.cmd(&["PING"]))
            .is_ok();
        eprintln!(
            "[{engine:?}] write random-errno 5%: {n}/60 ok, alive={alive} serving={serving} exit={:?}",
            c.nodes[0].exit_status()
        );
        assert!(
            !alive || serving,
            "{engine:?}: process alive but not serving (hung)"
        );
        assert_history(
            &format!("fiu_write_random_{engine:?}"),
            &h,
            Guarantee::Eventual,
        );
    }
}
