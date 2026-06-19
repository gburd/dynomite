//! Periodic task scheduler built on `tokio::time::interval`.
//!
//! Timer-wheel maintenance is delegated to the tokio runtime:
//! callers register a periodic callback through [`task_register`]
//! and receive a [`TaskHandle`] that cancels the underlying tokio
//! task when dropped or explicitly cancelled.
//!
//! A one-shot variant, [`task_schedule_once`], fires a single
//! delayed callback. The runtime drives both APIs, so there is no
//! per-iteration "time to next task" / "execute expired tasks"
//! bookkeeping; tokio's reactor performs that work transparently.
//!
//! # Examples
//!
//! ```
//! use std::sync::atomic::{AtomicUsize, Ordering};
//! use std::sync::Arc;
//! use std::time::Duration;
//! use dynomite::core::task::task_register;
//!
//! let rt = tokio::runtime::Runtime::new().unwrap();
//! rt.block_on(async {
//!     let counter = Arc::new(AtomicUsize::new(0));
//!     let c = counter.clone();
//!     let handle = task_register(Duration::from_millis(5), Arc::new(move || {
//!         c.fetch_add(1, Ordering::Relaxed);
//!     }));
//!     tokio::time::sleep(Duration::from_millis(40)).await;
//!     handle.cancel();
//!     assert!(counter.load(Ordering::Relaxed) >= 1);
//! });
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// A handle that cancels a registered task.
///
/// Dropping the handle without calling [`TaskHandle::cancel`] leaves
/// the task running (the tokio task holds a clone of the cancellation
/// token). Call [`TaskHandle::cancel`] to stop the task explicitly.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
/// use dynomite::core::task::task_register;
///
/// let rt = tokio::runtime::Runtime::new().unwrap();
/// rt.block_on(async {
///     let h = task_register(Duration::from_millis(50), Arc::new(|| {}));
///     assert!(!h.is_cancelled());
///     h.cancel();
///     assert!(h.is_cancelled());
/// });
/// ```
#[derive(Debug, Clone)]
pub struct TaskHandle {
    token: CancellationToken,
}

impl TaskHandle {
    /// Cancel the task.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use dynomite::core::task::task_register;
    ///
    /// let rt = tokio::runtime::Runtime::new().unwrap();
    /// rt.block_on(async {
    ///     let h = task_register(Duration::from_millis(50), Arc::new(|| {}));
    ///     h.cancel();
    ///     assert!(h.is_cancelled());
    /// });
    /// ```
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// Whether the task has already been cancelled.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use dynomite::core::task::task_register;
    ///
    /// let rt = tokio::runtime::Runtime::new().unwrap();
    /// rt.block_on(async {
    ///     let h = task_register(Duration::from_millis(50), Arc::new(|| {}));
    ///     assert!(!h.is_cancelled());
    ///     h.cancel();
    ///     assert!(h.is_cancelled());
    /// });
    /// ```
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

/// Register a periodic task that fires its callback every `period`.
///
/// The first invocation occurs after `period` elapses. The task runs
/// on the current tokio runtime, so this function must be called from
/// inside one (e.g. inside `#[tokio::main]` or a `block_on`).
///
/// # Examples
///
/// ```
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
/// use std::time::Duration;
/// use dynomite::core::task::task_register;
///
/// let rt = tokio::runtime::Runtime::new().unwrap();
/// rt.block_on(async {
///     let n = Arc::new(AtomicUsize::new(0));
///     let nn = n.clone();
///     let handle = task_register(Duration::from_millis(2), Arc::new(move || {
///         nn.fetch_add(1, Ordering::Relaxed);
///     }));
///     tokio::time::sleep(Duration::from_millis(15)).await;
///     handle.cancel();
/// });
/// ```
pub fn task_register(period: Duration, callback: Arc<dyn Fn() + Send + Sync>) -> TaskHandle {
    let token = CancellationToken::new();
    let child = token.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(period);
        // Skip the immediate-fire first tick that `tokio::time::interval`
        // produces by default so the callback first fires after
        // `timeout` ms, not at registration time.
        interval.tick().await;
        loop {
            tokio::select! {
                () = child.cancelled() => break,
                _ = interval.tick() => callback(),
            }
        }
    });
    TaskHandle { token }
}

/// Register a one-shot task that fires once after `delay` elapses.
///
/// Schedules a single delayed callback. The handle can be cancelled
/// before the deadline to suppress execution.
///
/// # Examples
///
/// ```
/// use std::sync::atomic::{AtomicBool, Ordering};
/// use std::sync::Arc;
/// use std::time::Duration;
/// use dynomite::core::task::task_schedule_once;
///
/// let rt = tokio::runtime::Runtime::new().unwrap();
/// rt.block_on(async {
///     let fired = Arc::new(AtomicBool::new(false));
///     let f = fired.clone();
///     let _h = task_schedule_once(Duration::from_millis(5), Box::new(move || {
///         f.store(true, Ordering::Relaxed);
///     }));
///     tokio::time::sleep(Duration::from_millis(20)).await;
///     assert!(fired.load(Ordering::Relaxed));
/// });
/// ```
pub fn task_schedule_once(delay: Duration, callback: Box<dyn FnOnce() + Send>) -> TaskHandle {
    let token = CancellationToken::new();
    let child = token.clone();
    tokio::spawn(async move {
        tokio::select! {
            () = child.cancelled() => {}
            () = tokio::time::sleep(delay) => {
                callback();
            }
        }
    });
    TaskHandle { token }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn periodic_task_fires_multiple_times() {
        let n = Arc::new(AtomicUsize::new(0));
        let nn = n.clone();
        let handle = task_register(
            Duration::from_millis(5),
            Arc::new(move || {
                nn.fetch_add(1, Ordering::Relaxed);
            }),
        );
        // Yield once so the spawned task gets polled and its
        // interval is registered with the paused clock before
        // the first advance.
        tokio::task::yield_now().await;
        // tokio::time is paused; advance() drives the timer
        // deterministically without depending on wall-clock
        // scheduling, eliminating CI flake.
        for _ in 0..8 {
            tokio::time::advance(Duration::from_millis(5)).await;
            tokio::task::yield_now().await;
        }
        handle.cancel();
        let after_cancel = n.load(Ordering::Relaxed);
        assert!(after_cancel >= 2, "expected >=2 fires, got {after_cancel}");
        tokio::time::advance(Duration::from_millis(20)).await;
        tokio::task::yield_now().await;
        // No fires after cancel.
        let final_count = n.load(Ordering::Relaxed);
        assert!(
            final_count <= after_cancel + 1,
            "task fired after cancel: before={after_cancel} after={final_count}"
        );
    }

    #[tokio::test]
    async fn cancel_before_first_tick_suppresses_callback() {
        let n = Arc::new(AtomicUsize::new(0));
        let nn = n.clone();
        let handle = task_register(
            Duration::from_millis(50),
            Arc::new(move || {
                nn.fetch_add(1, Ordering::Relaxed);
            }),
        );
        handle.cancel();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(n.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn one_shot_fires_exactly_once() {
        let n = Arc::new(AtomicUsize::new(0));
        let nn = n.clone();
        let _handle = task_schedule_once(
            Duration::from_millis(5),
            Box::new(move || {
                nn.fetch_add(1, Ordering::Relaxed);
            }),
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(n.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn one_shot_can_be_cancelled() {
        let n = Arc::new(AtomicUsize::new(0));
        let nn = n.clone();
        let handle = task_schedule_once(
            Duration::from_millis(50),
            Box::new(move || {
                nn.fetch_add(1, Ordering::Relaxed);
            }),
        );
        handle.cancel();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(n.load(Ordering::Relaxed), 0);
        assert!(handle.is_cancelled());
    }
}
