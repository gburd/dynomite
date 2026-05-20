//! Global runtime settings.
//!
//! The C reference exposes a single tunable, `msgs_per_sec`, accessed
//! through paired `msgs_per_sec()` / `set_msgs_per_sec()` functions.
//! The Rust port stores it in an [`AtomicU32`] so the getter and setter
//! are wait-free and lock-free.

use std::sync::atomic::{AtomicU32, Ordering};

/// Default value for [`msgs_per_sec`].
///
/// # Examples
///
/// ```
/// use dynomite::core::setting::DEFAULT_MSGS_PER_SEC;
/// assert_eq!(DEFAULT_MSGS_PER_SEC, 50_000);
/// ```
pub const DEFAULT_MSGS_PER_SEC: u32 = 50_000;

static MSGS_PER_SEC: AtomicU32 = AtomicU32::new(DEFAULT_MSGS_PER_SEC);

/// Return the current per-connection message-rate ceiling.
///
/// # Examples
///
/// ```
/// use dynomite::core::setting::{msgs_per_sec, DEFAULT_MSGS_PER_SEC};
/// assert!(msgs_per_sec() >= 1);
/// // The default may have been mutated by a previous test; check the constant
/// // independently to avoid cross-test ordering hazards.
/// assert_eq!(DEFAULT_MSGS_PER_SEC, 50_000);
/// ```
pub fn msgs_per_sec() -> u32 {
    MSGS_PER_SEC.load(Ordering::Relaxed)
}

/// Update the per-connection message-rate ceiling.
///
/// # Examples
///
/// ```
/// use dynomite::core::setting::{msgs_per_sec, set_msgs_per_sec};
/// let prev = msgs_per_sec();
/// set_msgs_per_sec(7);
/// assert_eq!(msgs_per_sec(), 7);
/// set_msgs_per_sec(prev);
/// ```
pub fn set_msgs_per_sec(value: u32) {
    MSGS_PER_SEC.store(value, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let prev = msgs_per_sec();
        set_msgs_per_sec(123);
        assert_eq!(msgs_per_sec(), 123);
        set_msgs_per_sec(prev);
        assert_eq!(msgs_per_sec(), prev);
    }
}
