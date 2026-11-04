//! Safe wrappers over one C mbuf (`src/dyn_mbuf.c`) through
//! `csrc/mbuf_shim.c`, the `dyn_string.h` search helpers, and the
//! `dyn_util.c` keypos / clock helpers.
//!
//! The shim owns a single process-global mbuf; callers serialise through
//! [`crate::util::GLOBALS`].

use std::os::raw::c_int;

extern "C" {
    fn oracle_mbuf_reset();
    fn oracle_mbuf_copy(d: *const u8, n: u32) -> c_int;
    fn oracle_mbuf_length() -> u32;
    fn oracle_mbuf_empty() -> c_int;
    fn oracle_mbuf_rewind();
    fn oracle_mbuf_read(out: *mut u8) -> u32;
    fn oracle_mbuf_split(at: u32, tail_out: *mut u8) -> c_int;
    fn oracle_mbuf_geometry(size: u32, out: *mut u32);
    fn oracle_strchr(p: *const u8, n: u32, c: u8) -> *const u8;
    fn oracle_strrchr(p: *const u8, n: u32, c: u8) -> *const u8;
    fn keypos_elem_len(elem: *const CKeyPos) -> u32;
    fn argpos_elem_len(elem: *const CArgPos) -> u32;
    fn dn_usec_now() -> u64;
    fn current_timestamp_in_millis() -> u64;
}

/// Mirrors `struct keypos` in `src/dyn_message.h`.
#[repr(C)]
struct CKeyPos {
    start: *const u8,
    end: *const u8,
    tag_start: *const u8,
    tag_end: *const u8,
}

/// Mirrors `struct argpos` in `src/dyn_message.h`.
#[repr(C)]
struct CArgPos {
    start: *const u8,
    end: *const u8,
}

/// Largest payload the shim copies out in one call (one mbuf).
const MAX: usize = 16384;

/// Empty the shim mbuf (C `mbuf_rewind`).
pub fn reset() {
    // SAFETY: the shim allocates its mbuf on first use; no pointers cross.
    unsafe { oracle_mbuf_reset() }
}

/// C `mbuf_copy` of `d`; `false` when it would not fit (C asserts).
pub fn copy(d: &[u8]) -> bool {
    // SAFETY: `d` is valid for `d.len()` bytes; C reads exactly that many.
    unsafe { oracle_mbuf_copy(d.as_ptr(), u32::try_from(d.len()).expect("fits u32")) == 0 }
}

/// C `mbuf_init(size)` geometry: `(mbuf_data_size(), end - start,
/// end_extra - start)` of a fresh mbuf. The default size is restored
/// afterwards; callers hold [`crate::util::GLOBALS`].
pub fn geometry(size: u32) -> (u32, u32, u32) {
    let mut out = [0u32; 3];
    // SAFETY: `out` holds the three u32 slots the shim writes.
    unsafe { oracle_mbuf_geometry(size, out.as_mut_ptr()) };
    (out[0], out[1], out[2])
}

/// C `mbuf_length`.
pub fn length() -> u32 {
    // SAFETY: reads the shim mbuf cursors.
    unsafe { oracle_mbuf_length() }
}

/// C `mbuf_empty`.
pub fn empty() -> bool {
    // SAFETY: reads the shim mbuf cursors.
    unsafe { oracle_mbuf_empty() != 0 }
}

/// C `mbuf_rewind`.
pub fn rewind() {
    // SAFETY: resets the shim mbuf cursors.
    unsafe { oracle_mbuf_rewind() }
}

/// The readable bytes `[pos, last)`.
pub fn read() -> Vec<u8> {
    let mut out = vec![0u8; MAX];
    // SAFETY: `out` holds MAX bytes, at least one mbuf of data.
    let n = unsafe { oracle_mbuf_read(out.as_mut_ptr()) };
    out.truncate(n as usize);
    out
}

/// C `mbuf_split` at `pos + at`; returns the tail, or `None` when C refuses.
pub fn split(at: u32) -> Option<Vec<u8>> {
    let mut out = vec![0u8; MAX];
    // SAFETY: `out` holds MAX bytes; the tail is at most one mbuf.
    let n = unsafe { oracle_mbuf_split(at, out.as_mut_ptr()) };
    let n = usize::try_from(n).ok()?;
    out.truncate(n);
    Some(out)
}

fn offset(base: &[u8], p: *const u8) -> Option<usize> {
    (!p.is_null()).then(|| p as usize - base.as_ptr() as usize)
}

/// C `_dn_strchr` over all of `hay`.
pub fn strchr(hay: &[u8], c: u8) -> Option<usize> {
    // SAFETY: C scans `[p, p + n)` of the live slice.
    offset(hay, unsafe {
        oracle_strchr(hay.as_ptr(), u32::try_from(hay.len()).expect("u32"), c)
    })
}

/// C `_dn_strrchr` from the last byte of `hay` back to its first.
pub fn strrchr(hay: &[u8], c: u8) -> Option<usize> {
    // SAFETY: C scans `[p, p + n - 1]` downwards of the live slice.
    offset(hay, unsafe {
        oracle_strrchr(hay.as_ptr(), u32::try_from(hay.len()).expect("u32"), c)
    })
}

/// C `keypos_elem_len` for a key `buf` whose hash tag is `tag`.
pub fn keypos_len(buf: &[u8], tag: std::ops::Range<usize>) -> u32 {
    let base = buf.as_ptr();
    // SAFETY: only pointer arithmetic within `buf` (C subtracts them).
    let k = unsafe {
        CKeyPos {
            start: base,
            end: base.add(buf.len()),
            tag_start: base.add(tag.start),
            tag_end: base.add(tag.end),
        }
    };
    // SAFETY: `k` is a valid keypos; C reads the two tag pointers.
    unsafe { keypos_elem_len(&raw const k) }
}

/// C `argpos_elem_len` for an argument spanning all of `buf`.
pub fn argpos_len(buf: &[u8]) -> u32 {
    // SAFETY: `end` is one past the end of `buf`.
    let a = CArgPos {
        start: buf.as_ptr(),
        end: unsafe { buf.as_ptr().add(buf.len()) },
    };
    // SAFETY: `a` is a valid argpos; C subtracts its pointers.
    unsafe { argpos_elem_len(&raw const a) }
}

/// C `dn_usec_now` (wall clock, microseconds).
pub fn usec_now() -> u64 {
    // SAFETY: reads the clock.
    unsafe { dn_usec_now() }
}

/// C `current_timestamp_in_millis` (wall clock, milliseconds).
pub fn millis_now() -> u64 {
    // SAFETY: reads the clock.
    unsafe { current_timestamp_in_millis() }
}
