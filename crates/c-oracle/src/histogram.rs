//! Safe wrapper over the C estimated histogram (`src/dyn_histogram.c`).
//!
//! [`CHistogram`] has the exact layout of `struct histogram`
//! (`src/dyn_histogram.h`), so the C functions operate on it in place and
//! tests read every field back for comparison with the Rust
//! transliteration.

use std::os::raw::c_int;

/// `BUCKET_SIZE` in `src/dyn_histogram.h`.
pub const BUCKET_SIZE: usize = 94;

/// Mirrors `struct histogram` in `src/dyn_histogram.h`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CHistogram {
    /// `buckets`: per-bucket observation counts.
    pub buckets: [u64; BUCKET_SIZE],
    /// `mean`: set by `histo_compute`.
    pub mean: u64,
    /// `val_95th`: set by `histo_compute`.
    pub val_95th: u64,
    /// `val_99th`: set by `histo_compute`.
    pub val_99th: u64,
    /// `val_999th`: set by `histo_compute`.
    pub val_999th: u64,
    /// `val_max`: set by `histo_add`.
    pub val_max: u64,
}

/// `rstatus_t` is `int` (`src/dyn_types.h`); `DN_OK` is 0.
type RStatus = c_int;

extern "C" {
    fn histo_init(histo: *mut CHistogram) -> RStatus;
    fn histo_reset(histo: *mut CHistogram) -> RStatus;
    fn histo_add(histo: *mut CHistogram, val: u64);
    fn histo_compute(histo: *mut CHistogram);
}

impl CHistogram {
    /// A histogram initialised by C `histo_init`, which also (re)builds the
    /// process-wide `bucket_offsets` table. Every call writes the same
    /// table values; nextest runs each oracle test in its own process.
    pub fn new() -> Self {
        let mut h = Self {
            buckets: [u64::MAX; BUCKET_SIZE],
            mean: u64::MAX,
            val_95th: u64::MAX,
            val_99th: u64::MAX,
            val_999th: u64::MAX,
            val_max: u64::MAX,
        };
        // SAFETY: `h` is a live, exclusively borrowed `struct histogram`
        // with the C layout; histo_init only writes its fields and the
        // C-private `bucket_offsets` array.
        let rc = unsafe { histo_init(&raw mut h) };
        assert_eq!(rc, 0, "histo_init failed");
        h
    }

    /// C `histo_reset`.
    pub fn reset(&mut self) {
        // SAFETY: `self` is an exclusively borrowed C-layout histogram.
        let rc = unsafe { histo_reset(self) };
        assert_eq!(rc, 0, "histo_reset failed");
    }

    /// C `histo_add`.
    pub fn add(&mut self, val: u64) {
        // SAFETY: `self` is an exclusively borrowed C-layout histogram;
        // `bucket_offsets` was built by `histo_init` in `new`.
        unsafe { histo_add(self, val) }
    }

    /// C `histo_compute`.
    pub fn compute(&mut self) {
        // SAFETY: `self` is an exclusively borrowed C-layout histogram.
        unsafe { histo_compute(self) }
    }
}

impl Default for CHistogram {
    fn default() -> Self {
        Self::new()
    }
}
