//! Runtime primitives for bounded back-pressure.
//!
//! The dispatcher and other internal pipelines need to refuse work fast
//! when downstream stages cannot keep up, rather than buffer requests
//! indefinitely and grow process memory without bound. This module
//! provides two complementary tools, modelled after the building blocks
//! used by the Riak Core stack:
//!
//! * [`Sidejob`] wraps a per-stage worker actor with a fixed-size
//!   tokio mailbox. Submissions to a full mailbox return
//!   [`SidejobError::Overloaded`] immediately so the caller can fail
//!   fast (the moral equivalent of a 503).
//! * [`Throttle`] is a token-bucket admission control gate. Internal
//!   queues call [`Throttle::try_acquire`] to fast-fail or
//!   [`Throttle::acquire`] to wait for tokens to refill at a
//!   configured rate.
//!
//! Both tools register Prometheus metric families against the default
//! process-wide registry the first time they are constructed:
//!
//! * `sidejob_overload_total{name="..."}` - counter, incremented every
//!   time a submit is rejected because the mailbox is full.
//! * `throttle_wait_seconds{queue="..."}` - histogram (1 ms .. 10 s),
//!   recording the time a caller waited inside
//!   [`Throttle::acquire`] before tokens became available.

mod metrics;
mod sidejob;
mod throttle;

pub use sidejob::{Sidejob, SidejobError};
pub use throttle::{Throttle, ThrottleError};
