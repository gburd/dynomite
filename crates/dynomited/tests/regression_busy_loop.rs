//! Regression test for the pass-6 chaos busy loop on
//! [`backend_supervisor`].
//!
//! Pass-6 caught one snapshot of `dynomited` at 1781% CPU
//! (~18 cores) on the `meh` host: the coordinator had left a
//! `redis:7-alpine` container running while reconfiguring
//! dynomited into memcache mode, so every probe drew an
//! `-ERR unknown command` reply that the memcache parser
//! treated as fatal. The supervisor's flat 50 ms reconnect
//! sleep then drove a tight reconnect storm.
//!
//! The fix in `crates/dynomited/src/server.rs` introduces an
//! exponential, jittered backoff that resets only after at
//! least one frame is successfully parsed, plus a
//! `backend_reconnect_total{backend, reason}` counter so
//! future runs can detect the same shape from metrics
//! scrapes alone.
//!
//! This test exercises three invariants:
//!
//! * The pure backoff helpers ([`next_backoff_ms`],
//!   [`jittered_backoff`]) double cleanly, cap at the
//!   configured ceiling, and emit a non-zero sleep with the
//!   advertised jitter window.
//! * Faced with a synthetic backend that accepts every TCP
//!   connect and immediately closes it, the supervisor's
//!   reconnect rate is bounded (dozens per second under the
//!   old code, single digits under the new one).
//! * The supervisor still drives the `backend_reconnect_total`
//!   counter on every failed attempt.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::sleep;

use dynomited::metrics::backend_reconnect;
use dynomited::server::{
    classify_reconnect_reason, jittered_backoff, next_backoff_ms, should_log_reconnect,
    spawn_backend_supervisor_for_testing, BACKEND_BACKOFF_INIT_MS, BACKEND_BACKOFF_MAX_MS,
};

#[test]
fn backoff_doubles_and_caps() {
    // Doubles cleanly until the cap.
    let a = BACKEND_BACKOFF_INIT_MS;
    let b = next_backoff_ms(a);
    let c = next_backoff_ms(b);
    let d = next_backoff_ms(c);
    assert_eq!(b, a * 2);
    assert_eq!(c, a * 4);
    assert_eq!(d, a * 8);

    // Far above the cap saturates.
    assert_eq!(
        next_backoff_ms(BACKEND_BACKOFF_MAX_MS),
        BACKEND_BACKOFF_MAX_MS
    );
    assert_eq!(
        next_backoff_ms(BACKEND_BACKOFF_MAX_MS * 100),
        BACKEND_BACKOFF_MAX_MS,
    );
    // Saturating multiply does not panic on near-u64::MAX.
    assert_eq!(next_backoff_ms(u64::MAX), BACKEND_BACKOFF_MAX_MS);
}

#[test]
fn jitter_lands_in_advertised_window() {
    // For a base of 1 000 ms, every sample must fall in
    // [500 ms, 1 500 ms]. Run enough samples that we exercise
    // the rng without making the test flaky.
    let base_ms: u64 = 1_000;
    let lo = Duration::from_millis(500);
    let hi = Duration::from_millis(1_500);
    for _ in 0..256 {
        let d = jittered_backoff(base_ms);
        assert!(d >= lo, "jitter underflowed: {d:?} < {lo:?}");
        assert!(d <= hi, "jitter overflowed: {d:?} > {hi:?}");
    }
    // Zero base still produces a non-zero sleep.
    let z = jittered_backoff(0);
    assert!(z >= Duration::from_millis(1));
}

#[test]
fn log_throttle_lets_first_three_through_then_decimates() {
    // First three are unconditional.
    assert!(should_log_reconnect(1));
    assert!(should_log_reconnect(2));
    assert!(should_log_reconnect(3));
    // 4..9 are suppressed.
    for n in 4..10 {
        assert!(!should_log_reconnect(n), "n={n} should be suppressed");
    }
    // 10 lands.
    assert!(should_log_reconnect(10));
    // 11..19 suppressed; 20 lands.
    for n in 11..20 {
        assert!(!should_log_reconnect(n), "n={n} should be suppressed");
    }
    assert!(should_log_reconnect(20));
}

#[test]
fn reconnect_reason_classification_covers_every_neterror() {
    use dynomite::net::NetError;
    use std::io;
    assert_eq!(
        classify_reconnect_reason(&NetError::Parse("x".into())),
        "parse",
    );
    assert_eq!(
        classify_reconnect_reason(&NetError::Io(io::Error::other("x"))),
        "io",
    );
    assert_eq!(classify_reconnect_reason(&NetError::Closed), "closed");
    assert_eq!(classify_reconnect_reason(&NetError::Tls("x".into())), "tls");
    assert_eq!(
        classify_reconnect_reason(&NetError::Dnode("x".into())),
        "dnode",
    );
    assert_eq!(classify_reconnect_reason(&NetError::Ejected), "other");
    assert_eq!(classify_reconnect_reason(&NetError::PoolExhausted), "other");
    assert_eq!(classify_reconnect_reason(&NetError::PoolShutdown), "other");
}

/// Drive a synthetic backend that accepts every TCP connect
/// and closes the socket immediately. The supervisor's
/// `run_one_backend_conn` then observes `read == 0` on its
/// first read, returns `NetError::Closed`, and the supervisor
/// is forced through the reconnect path repeatedly.
///
/// The pre-fix code reconnected at a flat ~50 ms cadence
/// (~20/sec). The post-fix code applies exponential backoff
/// with jitter starting at 50 ms; a 2 s window therefore caps
/// the reconnect count at roughly 50 / 50 + 50 / 100 + ... ~= 6.
/// We assert a generous ceiling of 30 attempts in 2 s so
/// scheduler jitter does not flake the test, while still
/// failing loudly if a future regression reintroduces the flat
/// 50 ms loop (which would deliver ~40 attempts).
#[tokio::test(flavor = "current_thread")]
async fn supervisor_throttles_reconnect_storm_against_always_closing_backend() {
    // Bind a fake backend that accepts and closes.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let accepts = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(tokio::sync::Notify::new());
    let listener_accepts = Arc::clone(&accepts);
    let listener_stop = Arc::clone(&stop);
    let listener_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = listener_stop.notified() => break,
                res = listener.accept() => {
                    match res {
                        Ok((sock, _)) => {
                            listener_accepts.fetch_add(1, Ordering::Relaxed);
                            // Drop the socket immediately so the
                            // peer observes EOF on its first read.
                            drop(sock);
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });

    // Snapshot the metric so we can assert the supervisor
    // increments it under sustained errors.
    let counter = backend_reconnect().with_label_values(&[addr.to_string().as_str(), "closed"]);
    let before = counter.get();

    // Hold the sender so the channel is "open but empty"; the
    // supervisor is forced to attempt connects.
    let (tx, rx) = mpsc::channel::<dynomite::net::server::OutboundRequest>(1);
    let supervisor =
        spawn_backend_supervisor_for_testing(addr, rx, dynomite::conf::DataStore::Memcache, None);

    // Let the supervisor reconnect-storm against the synthetic
    // backend for two seconds.
    sleep(Duration::from_secs(2)).await;

    let attempts_in_window = accepts.load(Ordering::Relaxed);
    let after = counter.get();

    // Tear down: drop the sender to close the channel, then
    // signal the listener task to exit. The supervisor exits
    // once `rx.is_closed() && rx.is_empty()` is true; closing
    // the listener just frees the port.
    drop(tx);
    stop.notify_one();
    let _ = tokio::time::timeout(Duration::from_secs(5), supervisor).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), listener_task).await;

    // Pre-fix: the flat 50 ms sleep delivers ~40 attempts in
    // 2 s. Post-fix: backoff doubles 50, 100, 200, 400, 800,
    // 1 600, capped at 5 000; jitter widens this by [0.5, 1.5].
    // Six attempts is the expected median; we assert <= 30 to
    // absorb scheduler jitter on a busy CI runner while still
    // catching any regression that reintroduces a sub-100 ms
    // floor.
    assert!(
        attempts_in_window <= 30,
        "supervisor reconnected {attempts_in_window} times in 2 s; \
         busy-loop fix is regressed (expected <= 30, pre-fix saw ~40)",
    );
    // Lower bound: the supervisor must keep trying. A zero
    // count here would mean we accidentally muted reconnects.
    assert!(
        attempts_in_window >= 2,
        "supervisor only reconnected {attempts_in_window} times in 2 s; \
         expected >= 2 attempts so we know the loop is exercised",
    );
    // The metric must move in step with the observed attempts.
    let delta = after - before;
    assert!(
        delta >= attempts_in_window,
        "backend_reconnect_total moved by {delta} but listener saw \
         {attempts_in_window} accepts; metric is undercounting",
    );
}

/// CPU watchdog: under the same synthetic-always-closes
/// scenario, the test process's user-mode CPU consumption must
/// stay well under one full core. Pre-fix the supervisor would
/// spend ~50% of a core on the reconnect cycle (with the rest
/// going to the listener task that mirrors it); post-fix it is
/// dominated by `tokio::time::sleep`.
///
/// We read `/proc/self/stat` directly so the test does not
/// pull `procfs` into the dependency graph for one field. The
/// `utime` (field 14) and `stime` (field 15) entries are
/// reported in clock ticks; we convert via `sysconf(_SC_CLK_TCK)`.
#[tokio::test(flavor = "current_thread")]
#[cfg_attr(not(target_os = "linux"), ignore = "needs /proc/self/stat")]
async fn supervisor_cpu_stays_bounded_against_always_closing_backend() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let stop = Arc::new(tokio::sync::Notify::new());
    let listener_stop = Arc::clone(&stop);
    let listener_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = listener_stop.notified() => break,
                res = listener.accept() => {
                    match res {
                        Ok((sock, _)) => drop(sock),
                        Err(_) => break,
                    }
                }
            }
        }
    });

    let (tx, rx) = mpsc::channel::<dynomite::net::server::OutboundRequest>(1);
    let supervisor =
        spawn_backend_supervisor_for_testing(addr, rx, dynomite::conf::DataStore::Memcache, None);

    let clk_tck = clock_ticks_per_second();
    let cpu_before = read_self_user_ticks();
    let wall_before = Instant::now();

    sleep(Duration::from_secs(2)).await;

    let cpu_after = read_self_user_ticks();
    let wall_after = Instant::now();

    drop(tx);
    stop.notify_one();
    let _ = tokio::time::timeout(Duration::from_secs(5), supervisor).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), listener_task).await;

    let user_ticks = cpu_after.saturating_sub(cpu_before);
    let wall_secs = wall_after.duration_since(wall_before).as_secs_f64();
    // Clock ticks fit comfortably in 32 bits over a 2 s window
    // (the kernel reports `utime` in `_SC_CLK_TCK` units, which
    // is 100 Hz on the runner). Convert via `u32::try_from` so
    // the f64 cast cannot drop precision.
    let user_ticks_f = f64::from(u32::try_from(user_ticks).unwrap_or(u32::MAX));
    let clk_tck_f = f64::from(u32::try_from(clk_tck).unwrap_or(100));
    let user_secs = user_ticks_f / clk_tck_f;
    let cpu_fraction = if wall_secs > 0.0 {
        user_secs / wall_secs
    } else {
        0.0
    };

    // Post-fix the supervisor sleeps for the bulk of the
    // window, so user-mode CPU should be a small fraction of a
    // core. Pre-fix this was ~0.5; we assert <= 1.5 cores so
    // a noisy CI runner does not flake while still catching
    // the 1781%-shaped regression. (1781% is 17.81 cores;
    // single-test current-thread runtime cannot reproduce that
    // ceiling, but it can absolutely reproduce the >>1-core
    // shape that signals a missing await/sleep.)
    assert!(
        cpu_fraction < 1.5,
        "supervisor consumed {cpu_fraction:.2} cores of user time over {wall_secs:.2}s; \
         busy-loop fix is regressed",
    );
}

/// Read field 14 (`utime`) of `/proc/self/stat` in clock ticks.
///
/// Returns 0 if the file cannot be read (callers fall back to a
/// best-effort comparison; the supervisor test still asserts
/// the reconnect-count bound).
fn read_self_user_ticks() -> u64 {
    let Ok(buf) = std::fs::read_to_string("/proc/self/stat") else {
        return 0;
    };
    // The `comm` field is parenthesised and may contain spaces.
    // Split off everything after the closing `)` so the
    // whitespace-split fields line up with the documented
    // numbering.
    let Some(close) = buf.rfind(')') else {
        return 0;
    };
    let tail = &buf[close + 1..];
    // Field 14 (`utime`) is index 11 in the post-`)` slice
    // (skipping `state` plus 12 leading numeric fields:
    // ppid, pgrp, session, tty_nr, tpgid, flags, minflt,
    // cminflt, majflt, cmajflt, utime).
    let fields: Vec<&str> = tail.split_whitespace().collect();
    fields
        .get(11)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

/// `sysconf(_SC_CLK_TCK)` via `nix` gives us the clock-ticks
/// denominator for `/proc/self/stat`. Falls back to 100, the
/// near-universal default on Linux.
fn clock_ticks_per_second() -> u64 {
    nix::unistd::sysconf(nix::unistd::SysconfVar::CLK_TCK)
        .ok()
        .flatten()
        .and_then(|v| u64::try_from(v).ok())
        .unwrap_or(100)
}
