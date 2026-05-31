//! Integration tests for the [`sup`] crate.
//!
//! Each test exercises one specific guarantee of the supervisor's
//! contract. The tests use real (not paused) tokio time so the
//! backoff and shutdown timers fire on the wall clock; the chosen
//! durations are short enough to keep total test runtime under a
//! few seconds.

#![cfg(not(loom))]

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sup::{
    BackoffSpec, ChildExit, ChildSpec, RestartPolicy, RestartStrategy, SupError, SupExit,
    Supervised, Supervisor,
};
use tokio::time::{sleep, timeout};

/// Convenience: snappy backoff for tests where we don't want to wait.
fn snappy_backoff() -> BackoffSpec {
    BackoffSpec::fixed(Duration::from_millis(1), Duration::from_millis(10), 1.0)
}

/// A child that runs a configurable number of times with a script of
/// outcomes. `Ok` outcomes complete normally; `Err` outcomes return
/// an error; `Panic` outcomes panic.
#[derive(Clone)]
enum Outcome {
    Ok,
    Err(String),
    Panic(String),
}

struct Scripted {
    name: String,
    runs: Arc<AtomicUsize>,
    outcomes: Arc<Mutex<Vec<Outcome>>>,
    /// Default outcome once the script is exhausted.
    fallback: Outcome,
}

impl Scripted {
    fn new(name: &str, outcomes: Vec<Outcome>, fallback: Outcome) -> Self {
        Self {
            name: name.to_string(),
            runs: Arc::new(AtomicUsize::new(0)),
            outcomes: Arc::new(Mutex::new(outcomes)),
            fallback,
        }
    }

    fn runs(&self) -> Arc<AtomicUsize> {
        self.runs.clone()
    }
}

impl Supervised for Scripted {
    type Output = ();
    fn name(&self) -> &str {
        &self.name
    }
    async fn run(&mut self) -> Result<Self::Output, SupError> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        let next = {
            let mut q = self.outcomes.lock().expect("outcome queue poisoned");
            if q.is_empty() {
                self.fallback.clone()
            } else {
                q.remove(0)
            }
        };
        match next {
            Outcome::Ok => Ok(()),
            Outcome::Err(msg) => Err(SupError::Child {
                name: self.name.clone(),
                message: msg,
            }),
            Outcome::Panic(msg) => panic!("{msg}"),
        }
    }
}

/// A child that simply runs forever (with periodic yield) and counts
/// how many times it has been started. Useful for shutdown and
/// strategy cascading tests.
struct Forever {
    name: String,
    starts: Arc<AtomicUsize>,
    started: Arc<tokio::sync::Notify>,
}

impl Forever {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            starts: Arc::new(AtomicUsize::new(0)),
            started: Arc::new(tokio::sync::Notify::new()),
        }
    }
    fn starts(&self) -> Arc<AtomicUsize> {
        self.starts.clone()
    }
    fn started(&self) -> Arc<tokio::sync::Notify> {
        self.started.clone()
    }
}

impl Supervised for Forever {
    type Output = ();
    fn name(&self) -> &str {
        &self.name
    }
    async fn run(&mut self) -> Result<Self::Output, SupError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.started.notify_waiters();
        loop {
            sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Wait for `cond` to become true, polling every 5ms up to `deadline`.
async fn wait_until<F: FnMut() -> bool>(deadline: Duration, mut cond: F) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if cond() {
            return true;
        }
        sleep(Duration::from_millis(5)).await;
    }
    cond()
}

#[tokio::test]
async fn permanent_child_restarts_on_normal_exit() {
    let child = Scripted::new("perm", vec![], Outcome::Ok);
    let runs = child.runs();
    let mut sup = Supervisor::new(RestartStrategy::OneForOne);
    sup.add_child(ChildSpec {
        spec: child,
        restart: RestartPolicy::Permanent,
        backoff: snappy_backoff(),
    });
    let h = sup.handle();
    let runner = tokio::spawn(sup.run());

    // Wait until the child has been restarted at least 5 times.
    assert!(
        wait_until(Duration::from_secs(2), || runs.load(Ordering::SeqCst) >= 5).await,
        "permanent child did not restart enough times: {}",
        runs.load(Ordering::SeqCst)
    );
    h.shutdown().await.expect("shutdown signal");
    let exit = timeout(Duration::from_secs(2), runner)
        .await
        .expect("supervisor stopped in time")
        .expect("no panic");
    assert_eq!(exit, SupExit::Shutdown);
}

#[tokio::test]
async fn transient_child_does_not_restart_on_normal_exit() {
    let child = Scripted::new("trans", vec![Outcome::Ok], Outcome::Ok);
    let runs = child.runs();
    let mut sup = Supervisor::new(RestartStrategy::OneForOne);
    sup.add_child(ChildSpec {
        spec: child,
        restart: RestartPolicy::Transient,
        backoff: snappy_backoff(),
    });
    let runner = tokio::spawn(sup.run());

    let exit = timeout(Duration::from_secs(2), runner)
        .await
        .expect("supervisor stopped in time")
        .expect("no panic");
    assert_eq!(exit, SupExit::AllChildrenStopped);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn transient_child_restarts_on_panic() {
    // Panic on first run, succeed on second; transient policy means
    // the second (Ok) exit removes the child and the supervisor
    // stops naturally.
    let child = Scripted::new(
        "trans-panic",
        vec![Outcome::Panic("boom".into()), Outcome::Ok],
        Outcome::Ok,
    );
    let runs = child.runs();
    let mut sup = Supervisor::new(RestartStrategy::OneForOne);
    sup.add_child(ChildSpec {
        spec: child,
        restart: RestartPolicy::Transient,
        backoff: snappy_backoff(),
    });
    let runner = tokio::spawn(sup.run());
    let exit = timeout(Duration::from_secs(2), runner)
        .await
        .expect("supervisor stopped in time")
        .expect("no panic");
    assert_eq!(exit, SupExit::AllChildrenStopped);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn temporary_child_never_restarts() {
    let child = Scripted::new(
        "temp",
        vec![Outcome::Err("oops".into())],
        Outcome::Err("oops".into()),
    );
    let runs = child.runs();
    let mut sup = Supervisor::new(RestartStrategy::OneForOne);
    sup.add_child(ChildSpec {
        spec: child,
        restart: RestartPolicy::Temporary,
        backoff: snappy_backoff(),
    });
    let runner = tokio::spawn(sup.run());
    let exit = timeout(Duration::from_secs(2), runner)
        .await
        .expect("supervisor stopped in time")
        .expect("no panic");
    assert_eq!(exit, SupExit::AllChildrenStopped);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn one_for_all_restarts_all_on_one_failure() {
    // Two siblings A and B. A is permanent and runs forever; B fails
    // once then runs forever. With OneForAll, A should be aborted
    // and restarted when B fails, so A's start count goes from 1
    // to 2.
    let a = Forever::new("a");
    let a_starts = a.starts();
    let b = Scripted::new(
        "b",
        vec![Outcome::Err("first".into())],
        Outcome::Ok, // exhausted -> Ok next, but we shut down before
    );
    let b_runs = b.runs();

    let mut sup = Supervisor::new(RestartStrategy::OneForAll);
    sup.add_child(ChildSpec {
        spec: a,
        restart: RestartPolicy::Permanent,
        backoff: snappy_backoff(),
    });
    sup.add_child(ChildSpec {
        spec: b,
        restart: RestartPolicy::Permanent,
        backoff: snappy_backoff(),
    });
    let h = sup.handle();
    let runner = tokio::spawn(sup.run());

    // Wait until A has been started at least twice (i.e. it was
    // restarted as part of the OneForAll cascade triggered by B's
    // failure) AND B has run at least twice (its initial failure
    // plus one restart).
    assert!(
        wait_until(Duration::from_secs(2), || {
            a_starts.load(Ordering::SeqCst) >= 2 && b_runs.load(Ordering::SeqCst) >= 2
        })
        .await,
        "OneForAll cascade did not restart sibling: a={} b={}",
        a_starts.load(Ordering::SeqCst),
        b_runs.load(Ordering::SeqCst),
    );

    h.shutdown().await.expect("shutdown signal");
    let exit = timeout(Duration::from_secs(2), runner)
        .await
        .expect("supervisor stopped in time")
        .expect("no panic");
    assert_eq!(exit, SupExit::Shutdown);
}

#[tokio::test]
async fn rest_for_one_restarts_failed_and_subsequent_children() {
    // Three siblings A, B, C added in order. B fails. We expect:
    // A is NOT restarted. B is restarted. C IS restarted.
    let a = Forever::new("a");
    let a_starts = a.starts();
    let b = Scripted::new("b", vec![Outcome::Err("first".into())], Outcome::Ok);
    let b_runs = b.runs();
    let c = Forever::new("c");
    let c_starts = c.starts();
    let c_notify = c.started();

    let mut sup = Supervisor::new(RestartStrategy::RestForOne);
    sup.add_child(ChildSpec {
        spec: a,
        restart: RestartPolicy::Permanent,
        backoff: snappy_backoff(),
    });
    sup.add_child(ChildSpec {
        spec: b,
        restart: RestartPolicy::Permanent,
        backoff: snappy_backoff(),
    });
    sup.add_child(ChildSpec {
        spec: c,
        restart: RestartPolicy::Permanent,
        backoff: snappy_backoff(),
    });
    let h = sup.handle();
    let runner = tokio::spawn(sup.run());

    // Wait until C has restarted (its start count >= 2).
    let _ = c_notify; // notify is not needed; the start counter suffices
    assert!(
        wait_until(Duration::from_secs(2), || {
            c_starts.load(Ordering::SeqCst) >= 2 && b_runs.load(Ordering::SeqCst) >= 2
        })
        .await,
        "RestForOne did not restart C after B failed: a={} b={} c={}",
        a_starts.load(Ordering::SeqCst),
        b_runs.load(Ordering::SeqCst),
        c_starts.load(Ordering::SeqCst),
    );

    // A should still be at exactly 1 start: it was registered before
    // B and is therefore unaffected by B's failure.
    let a_now = a_starts.load(Ordering::SeqCst);
    assert_eq!(a_now, 1, "RestForOne should not restart predecessors");

    h.shutdown().await.expect("shutdown signal");
    let exit = timeout(Duration::from_secs(2), runner)
        .await
        .expect("supervisor stopped in time")
        .expect("no panic");
    assert_eq!(exit, SupExit::Shutdown);
}

#[tokio::test]
async fn backoff_doubles_consecutive_failures_with_jitter() {
    // Record the timestamp of each run so we can measure inter-run
    // gaps. With factor=2.0, jitter=0.1, and start=50ms, the gaps
    // should be roughly 50, 100, 200, 400 ms (within +/- 10%).
    struct Recording {
        name: String,
        stamps: Arc<Mutex<Vec<Instant>>>,
    }
    impl Supervised for Recording {
        type Output = ();
        fn name(&self) -> &str {
            &self.name
        }
        async fn run(&mut self) -> Result<Self::Output, SupError> {
            self.stamps
                .lock()
                .expect("stamps poisoned")
                .push(Instant::now());
            Err(SupError::Child {
                name: self.name.clone(),
                message: "always".into(),
            })
        }
    }

    let stamps = Arc::new(Mutex::new(Vec::new()));
    let child = Recording {
        name: "boff".into(),
        stamps: stamps.clone(),
    };
    let mut sup = Supervisor::new(RestartStrategy::OneForOne);
    sup.add_child(ChildSpec {
        spec: child,
        restart: RestartPolicy::Permanent,
        backoff: BackoffSpec {
            start: Duration::from_millis(50),
            max: Duration::from_secs(10),
            factor: 2.0,
            jitter: 0.1,
        },
    });
    let h = sup.handle();
    let runner = tokio::spawn(sup.run());

    // Wait for at least 5 runs.
    assert!(
        wait_until(Duration::from_secs(5), || {
            stamps.lock().expect("stamps poisoned").len() >= 5
        })
        .await,
        "did not record enough runs",
    );
    h.shutdown().await.expect("shutdown signal");
    let _ = timeout(Duration::from_secs(2), runner).await;

    let recorded = stamps.lock().expect("stamps poisoned").clone();
    assert!(recorded.len() >= 5, "len = {}", recorded.len());
    // Compute gaps between consecutive runs.
    let mut gaps = Vec::new();
    for w in recorded.windows(2) {
        gaps.push(w[1].duration_since(w[0]));
    }
    // Expected midpoints: 50ms, 100ms, 200ms, 400ms (the first gap
    // is the delay before the SECOND run, i.e. backoff for failures=1
    // -> 50 * 2^0 = 50ms). Let the bound be generous: each measured
    // gap must be in [0.5x, 2.0x] of its target to allow for OS
    // scheduling jitter on busy runners. The crucial property is
    // monotone roughly-doubling growth.
    let targets_ms = [50.0_f64, 100.0, 200.0, 400.0];
    for (i, gap) in gaps.iter().enumerate().take(targets_ms.len()) {
        let measured = gap.as_secs_f64() * 1000.0;
        let target = targets_ms[i];
        assert!(
            measured >= target * 0.5 && measured <= target * 2.0,
            "gap {i} = {measured:.1}ms out of [0.5x, 2.0x] of target {target}ms",
        );
    }
    // And confirm strict monotone-ish growth between gap[0] and
    // gap[3]: the last gap should be at least 2x the first.
    if gaps.len() >= 4 {
        assert!(
            gaps[3] >= gaps[0] * 2,
            "gap[3]={:?} should be >= 2*gap[0]={:?}",
            gaps[3],
            gaps[0]
        );
    }
}

#[tokio::test]
async fn shutdown_terminates_all_children_within_timeout() {
    let mut sup =
        Supervisor::new(RestartStrategy::OneForOne).with_shutdown_timeout(Duration::from_secs(2));
    for i in 0..4 {
        sup.add_child(ChildSpec {
            spec: Forever::new(&format!("w{i}")),
            restart: RestartPolicy::Permanent,
            backoff: snappy_backoff(),
        });
    }
    let h = sup.handle();
    let runner = tokio::spawn(sup.run());

    // Give children time to actually start.
    sleep(Duration::from_millis(50)).await;
    let t0 = Instant::now();
    h.shutdown().await.expect("shutdown signal");
    let exit = timeout(Duration::from_secs(3), runner)
        .await
        .expect("supervisor stopped in time")
        .expect("no panic");
    let elapsed = t0.elapsed();
    assert_eq!(exit, SupExit::Shutdown);
    assert!(
        elapsed < Duration::from_secs(1),
        "shutdown took {elapsed:?}, expected sub-second",
    );
}

#[tokio::test]
async fn panic_in_child_is_caught_and_does_not_crash_supervisor() {
    // A child that panics every run, with Temporary policy so it's
    // dropped after a single panic and the supervisor exits cleanly
    // by way of AllChildrenStopped. The crucial assertion is that
    // the supervisor's run() future itself does NOT propagate the
    // child's panic.
    let child = Scripted::new(
        "panicker",
        vec![Outcome::Panic("kaboom".into())],
        Outcome::Panic("kaboom".into()),
    );
    let runs = child.runs();
    let mut sup = Supervisor::new(RestartStrategy::OneForOne);
    sup.add_child(ChildSpec {
        spec: child,
        restart: RestartPolicy::Temporary,
        backoff: snappy_backoff(),
    });
    let runner = tokio::spawn(sup.run());
    let exit = timeout(Duration::from_secs(2), runner)
        .await
        .expect("supervisor stopped in time")
        .expect("supervisor task itself did not panic");
    assert_eq!(exit, SupExit::AllChildrenStopped);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

/// Bonus regression test: a child that returns `Err` produces a
/// [`ChildExit::Err`] classification, while a child that panics
/// produces [`ChildExit::Panic`]. We can't observe these directly
/// (the supervisor consumes them) but the public `is_abnormal`
/// helper is exercised here for documentation.
#[test]
fn child_exit_is_abnormal_classification() {
    assert!(!ChildExit::Ok.is_abnormal());
    assert!(!ChildExit::Cancelled.is_abnormal());
    assert!(ChildExit::Err("e".into()).is_abnormal());
    assert!(ChildExit::Panic("p".into()).is_abnormal());
}

#[tokio::test]
async fn shutdown_handle_after_finish_returns_not_running() {
    let child = Scripted::new("done", vec![Outcome::Ok], Outcome::Ok);
    let mut sup = Supervisor::new(RestartStrategy::OneForOne);
    sup.add_child(ChildSpec {
        spec: child,
        restart: RestartPolicy::Transient,
        backoff: snappy_backoff(),
    });
    let h = sup.handle();
    let runner = tokio::spawn(sup.run());
    let _ = timeout(Duration::from_secs(2), runner).await;
    // Allow the finished flag to publish.
    sleep(Duration::from_millis(20)).await;
    assert!(h.is_finished(), "handle should observe finished");
    assert!(matches!(h.shutdown().await, Err(SupError::NotRunning)));
}

#[tokio::test]
async fn simple_one_for_one_enforces_max_children() {
    let mut sup = Supervisor::new(RestartStrategy::SimpleOneForOne { max_children: 2 });
    let _ = sup.try_add_child(ChildSpec {
        spec: Forever::new("a"),
        restart: RestartPolicy::Permanent,
        backoff: snappy_backoff(),
    });
    let _ = sup.try_add_child(ChildSpec {
        spec: Forever::new("b"),
        restart: RestartPolicy::Permanent,
        backoff: snappy_backoff(),
    });
    let third = sup.try_add_child(ChildSpec {
        spec: Forever::new("c"),
        restart: RestartPolicy::Permanent,
        backoff: snappy_backoff(),
    });
    assert!(matches!(third, Err(SupError::ChildLimitReached)));
    let h = sup.handle();
    let runner = tokio::spawn(sup.run());
    sleep(Duration::from_millis(30)).await;
    h.shutdown().await.expect("shutdown");
    let _ = timeout(Duration::from_secs(2), runner).await;
    let _ = AtomicU32::new(0); // placate any unused-import check
}
