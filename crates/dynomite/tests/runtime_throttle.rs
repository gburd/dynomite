//! Integration tests for [`dynomite::runtime::Throttle`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dynomite::runtime::Throttle;
use hegel::generators as gs;
use hegel::TestCase;

#[tokio::test]
async fn throttle_burst_up_to_capacity() {
    let t = Throttle::new(64, 1);
    // The bucket is created full; one acquire of `capacity`
    // tokens must succeed without waiting.
    let start = Instant::now();
    assert!(t.try_acquire(64));
    assert!(start.elapsed() < Duration::from_millis(50));
    assert_eq!(t.available(), 0);
    assert!(!t.try_acquire(1), "bucket must be empty after burst");
}

#[tokio::test]
async fn throttle_refills_at_configured_rate() {
    // 1000 tokens/sec means each 10 ms slice should refill ~10
    // tokens. We empty the bucket then sleep and verify the
    // refill is at least the floor of the expected count and at
    // most a generous slack ceiling (the OS scheduler can give
    // us a longer sleep than requested).
    let t = Throttle::new(100, 1_000);
    assert!(t.try_acquire(100));
    tokio::time::sleep(Duration::from_millis(50)).await;
    // refill is computed lazily; trigger it via try_acquire(0).
    let _ = t.try_acquire(0);
    let avail = t.available();
    assert!(
        (25..=100).contains(&avail),
        "expected ~50 tokens after 50ms at 1000/s, got {avail}"
    );
}

#[tokio::test]
async fn throttle_acquire_blocks_when_empty_then_unblocks_after_refill() {
    // 100 tokens/sec, capacity 5. After draining, an acquire(5)
    // must wait at least the time needed to refill 5 tokens
    // (50ms at 100/s).
    let t = Arc::new(Throttle::with_name("blocking", 5, 100));
    assert!(t.try_acquire(5));
    let start = Instant::now();
    let t2 = Arc::clone(&t);
    let handle = tokio::spawn(async move { t2.acquire(5).await });
    handle.await.unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(40),
        "acquire returned in {elapsed:?}, expected at least ~50ms"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "acquire took unexpectedly long: {elapsed:?}"
    );
}

#[tokio::test]
async fn throttle_zero_request_is_noop() {
    let t = Throttle::new(4, 1);
    assert!(t.try_acquire(0));
    t.acquire(0).await;
    assert_eq!(t.available(), 4);
}

#[tokio::test]
async fn throttle_request_above_capacity_fails_fast() {
    let t = Throttle::new(4, 1);
    assert!(!t.try_acquire(5));
}

#[tokio::test]
async fn throttle_accessors_report_construction_parameters() {
    let t = Throttle::with_name("acc", 16, 3);
    assert_eq!(t.capacity(), 16);
    assert_eq!(t.refill_per_sec(), 3);
    // Bucket starts full.
    assert_eq!(t.available(), 16);
}

#[tokio::test]
async fn throttle_debug_includes_state_fields() {
    let t = Throttle::with_name("dbg", 7, 2);
    let s = format!("{t:?}");
    assert!(s.contains("Throttle"), "missing struct name: {s}");
    assert!(s.contains("dbg"), "missing queue name: {s}");
    assert!(s.contains("capacity"), "missing capacity field: {s}");
    assert!(s.contains("refill_per_sec"), "missing refill field: {s}");
    assert!(s.contains("available"), "missing available field: {s}");
}

#[tokio::test]
#[should_panic(expected = "zero refill rate")]
async fn throttle_zero_refill_acquire_panics_when_drained() {
    // capacity 2, refill 0: after draining, acquire can never
    // recover, so the contract is to panic rather than deadlock.
    let t = Throttle::new(2, 0);
    assert!(t.try_acquire(2));
    t.acquire(2).await;
}

#[tokio::test]
#[should_panic(expected = "> capacity")]
async fn throttle_acquire_above_capacity_panics() {
    let t = Throttle::new(4, 1);
    t.acquire(5).await;
}

#[tokio::test]
async fn throttle_concurrent_acquires_share_budget() {
    let t = Arc::new(Throttle::with_name("shared", 8, 1));
    let granted = Arc::new(AtomicU64::new(0));

    let mut tasks = Vec::new();
    for _ in 0..32u64 {
        let t = Arc::clone(&t);
        let granted = Arc::clone(&granted);
        tasks.push(tokio::spawn(async move {
            if t.try_acquire(1) {
                granted.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for h in tasks {
        h.await.unwrap();
    }
    // The bucket starts with 8 tokens; refill is 1/s and the
    // test runs in <1s, so at most 8 + 1 tokens may have been
    // granted.
    let g = granted.load(Ordering::SeqCst);
    assert!(
        (8..=9).contains(&g),
        "granted {g}, want 8 (or 8+1 with refill)"
    );
}

#[hegel::test(test_cases = 64)]
fn throttle_never_grants_more_than_capacity_plus_refill_times_elapsed(tc: TestCase) {
    let capacity = tc.draw(gs::integers::<u64>().min_value(1).max_value(64));
    let refill = tc.draw(gs::integers::<u64>().min_value(1).max_value(1_000));
    // A small mix of request sizes: zero (no-op), one, and a
    // burst of capacity. All must respect the rate envelope.
    let req_sizes: Vec<u64> = (0..16)
        .map(|_| tc.draw(gs::integers::<u64>().min_value(0).max_value(capacity)))
        .collect();

    let t = Throttle::with_name("prop", capacity, refill);
    let start = Instant::now();
    let mut granted: u128 = 0;
    for n in &req_sizes {
        if t.try_acquire(*n) {
            granted += u128::from(*n);
        }
    }
    let elapsed_micros: u128 = start.elapsed().as_micros();
    // Headroom: the bucket may have been refilled in the gap
    // between `start` and the first try_acquire; allow one extra
    // millisecond of refill plus a flat 1-token rounding margin.
    let envelope = u128::from(capacity)
        + (u128::from(refill).saturating_mul(elapsed_micros + 1_000)) / 1_000_000
        + 1;
    assert!(
        granted <= envelope,
        "granted {granted} > envelope {envelope} (capacity={capacity}, refill={refill}, elapsed_us={elapsed_micros})"
    );
}
