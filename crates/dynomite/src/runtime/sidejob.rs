//! Bounded-mailbox actor wrapper, modelled on Riak's `sidejob` library.
//!
//! A [`Sidejob`] owns a tokio task that drains a fixed-capacity mpsc
//! channel and runs a user-supplied async handler against each
//! request. The capacity is the back-pressure knob: when callers
//! [`Sidejob::submit`] requests faster than the handler can complete
//! them, the channel saturates and new submissions return
//! [`SidejobError::Overloaded`] without blocking the caller.
//!
//! The handler runs serially inside the actor, so it sees one request
//! at a time. Each request is handed to its handler-future inside a
//! `tokio::spawn` so a panic in user code only kills the per-request
//! task: the actor loop keeps draining the mailbox and subsequent
//! submits succeed normally. Callers waiting on a panicked request
//! observe [`SidejobError::Stopped`] when the reply channel is
//! dropped.
//!
//! # Examples
//!
//! ```
//! use dynomite::runtime::{Sidejob, SidejobError};
//! # tokio::runtime::Builder::new_current_thread()
//! #     .enable_all()
//! #     .build()
//! #     .unwrap()
//! #     .block_on(async {
//! let job: Sidejob<u32, u32> =
//!     Sidejob::spawn("doubler", 4, |n| async move { n * 2 });
//! assert_eq!(job.submit(21).await.unwrap(), 42);
//! # let _: Result<u32, SidejobError> = job.submit(0).await;
//! # });
//! ```

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::runtime::metrics;

/// Reasons a submit may fail.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SidejobError {
    /// The mailbox is full. Drop the request and let the caller
    /// retry against another stage or report a 503-equivalent.
    #[error("sidejob mailbox is full")]
    Overloaded,
    /// The actor task is no longer running, so no further requests
    /// can be served. This also covers the case where a handler
    /// panicked on a specific request and dropped the reply channel.
    #[error("sidejob actor has stopped")]
    Stopped,
}

/// Handle to a sidejob actor.
///
/// `Sidejob` is cheap to clone: clones share the same underlying
/// channel and overload counter, so spreading the handle across
/// dispatcher tasks does not change the back-pressure semantics.
pub struct Sidejob<Req, Reply> {
    /// Static name used as the `name` label on
    /// `sidejob_overload_total`.
    name: &'static str,
    /// Sender half of the mailbox. The receiver lives inside the
    /// spawned actor task. When all senders are dropped, the actor
    /// loop exits cleanly.
    tx: mpsc::Sender<(Req, oneshot::Sender<Reply>)>,
    /// Process-local counter of `Overloaded` rejections. The
    /// Prometheus counter is the cluster-wide rollup; this field
    /// gives unit tests a deterministic value to assert against.
    full_failures: Arc<AtomicU64>,
}

impl<Req, Reply> Clone for Sidejob<Req, Reply> {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            tx: self.tx.clone(),
            full_failures: Arc::clone(&self.full_failures),
        }
    }
}

impl<Req, Reply> Sidejob<Req, Reply>
where
    Req: Send + 'static,
    Reply: Send + 'static,
{
    /// Spawn a new sidejob actor.
    ///
    /// `name` is recorded as the `name` label on
    /// `sidejob_overload_total` and used in tracing log lines.
    /// `capacity` is the maximum number of in-flight requests the
    /// mailbox will hold before it starts rejecting submits with
    /// [`SidejobError::Overloaded`].
    ///
    /// The handler is `FnMut` so callers may close over mutable
    /// state; because the actor loop is serial, the handler is
    /// called against one request at a time.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero. A zero-capacity sidejob would
    /// reject every submit and is almost certainly a configuration
    /// mistake.
    pub fn spawn<F, Fut>(name: &'static str, capacity: usize, mut handler: F) -> Self
    where
        F: FnMut(Req) -> Fut + Send + 'static,
        Fut: Future<Output = Reply> + Send + 'static,
    {
        assert!(capacity > 0, "sidejob capacity must be > 0");
        // Touch the metric family eagerly so the first overload
        // event does not have to acquire the registry lock under
        // contention.
        let _ = metrics::sidejob_overload().with_label_values(&[name]);

        let (tx, mut rx) = mpsc::channel::<(Req, oneshot::Sender<Reply>)>(capacity);
        tokio::spawn(async move {
            while let Some((req, reply_tx)) = rx.recv().await {
                let fut = handler(req);
                // Run the handler future inside its own task so a
                // panic only kills the per-request task. The actor
                // loop keeps running and subsequent submits succeed.
                match tokio::spawn(fut).await {
                    Ok(reply) => {
                        // Receiver may have given up; that is not a
                        // sidejob bug, so we silently drop.
                        let _ = reply_tx.send(reply);
                    }
                    Err(join_err) => {
                        if join_err.is_panic() {
                            tracing::warn!(
                                sidejob = name,
                                "handler panicked; reply channel dropped"
                            );
                        }
                        // Dropping `reply_tx` surfaces
                        // SidejobError::Stopped to the caller for
                        // this one request, while the actor stays
                        // alive for future requests.
                        drop(reply_tx);
                    }
                }
            }
            tracing::debug!(sidejob = name, "actor loop exited (channel closed)");
        });

        Self {
            name,
            tx,
            full_failures: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Submit a request and await the handler's reply.
    ///
    /// Returns [`SidejobError::Overloaded`] immediately if the
    /// mailbox is full, or [`SidejobError::Stopped`] if the actor
    /// (or this specific request's per-request task) has gone away
    /// before producing a reply.
    pub async fn submit(&self, req: Req) -> Result<Reply, SidejobError> {
        let rx = self.try_submit(req)?;
        rx.await.map_err(|_| SidejobError::Stopped)
    }

    /// Submit a request without awaiting the reply.
    ///
    /// On success the caller receives the [`oneshot::Receiver`] for
    /// the reply and may await it (or drop it) at leisure. Like
    /// [`Sidejob::submit`], a full mailbox returns
    /// [`SidejobError::Overloaded`] immediately.
    pub fn try_submit(&self, req: Req) -> Result<oneshot::Receiver<Reply>, SidejobError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        match self.tx.try_send((req, reply_tx)) {
            Ok(()) => Ok(reply_rx),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.full_failures.fetch_add(1, Ordering::Relaxed);
                metrics::sidejob_overload()
                    .with_label_values(&[self.name])
                    .inc();
                Err(SidejobError::Overloaded)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(SidejobError::Stopped),
        }
    }

    /// Number of submits this handle has observed being rejected
    /// with [`SidejobError::Overloaded`] over its entire lifetime.
    /// Cloned handles share the same counter.
    pub fn full_failures(&self) -> u64 {
        self.full_failures.load(Ordering::Relaxed)
    }

    /// Static name used for metric labels.
    pub fn name(&self) -> &'static str {
        self.name
    }
}

impl<Req, Reply> std::fmt::Debug for Sidejob<Req, Reply> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sidejob")
            .field("name", &self.name)
            .field("capacity", &self.tx.max_capacity())
            .field("full_failures", &self.full_failures.load(Ordering::Relaxed))
            .finish()
    }
}
