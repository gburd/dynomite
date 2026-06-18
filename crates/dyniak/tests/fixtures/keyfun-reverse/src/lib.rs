//! Custom keyfun fixture compiled to `wasm32-unknown-unknown`.
//!
//! Routing rule: `<bucket>:<reversed key>`. The bucket is copied
//! verbatim, a single `:` separator is inserted, then the key
//! bytes are appended in reverse order. The host asserts the
//! produced route bytes exactly, and proves a Std bucket (which
//! routes on `<bucket>/<key>`) lands on a different ring position.
//!
//! The module speaks the dyniak keyfun ABI:
//!
//! * `keyfun_alloc(len) -> ptr` -- bump allocator over a static
//!   arena.
//! * `keyfun_route(in_ptr, in_len, out_ptr_ptr, out_len_ptr) -> i32`
//!   -- reads the framed `(bucket, key)` input
//!   (`bucket_len:u32-le | bucket | key_len:u32-le | key`), writes
//!   the route bytes, stores `(out_ptr, out_len)` in the meta
//!   slot, and returns 0.
//!
//! This is operator-supplied code: it links no std and uses a
//! fixed-size bump arena so it needs no allocator. The `unsafe`
//! here is the minimum required to expose a linear-memory ABI to
//! the host; the workspace `forbid(unsafe_code)` lint does not
//! apply because this fixture is excluded from the workspace.

#![no_std]
#![allow(unsafe_code)]

use core::panic::PanicInfo;

/// Bump arena. 64 KiB is far more than any routing key needs.
const ARENA_SIZE: usize = 64 * 1024;
static mut ARENA: [u8; ARENA_SIZE] = [0; ARENA_SIZE];
static mut NEXT: usize = 0;

#[panic_handler]
fn panic(_: &PanicInfo) -> ! {
    // A keyfun must never panic into the host; loop so the host's
    // fuel / epoch watchdog reclaims us if we ever get here.
    loop {}
}

/// Bump-allocate `len` bytes from the static arena. Returns the
/// arena offset (which is also the linear-memory pointer because
/// the arena is the only large static).
#[no_mangle]
pub extern "C" fn keyfun_alloc(len: i32) -> i32 {
    let len = if len < 0 { 0 } else { len as usize };
    unsafe {
        let ptr = NEXT;
        // Saturate rather than wrap; an over-large request returns
        // the current top and the host's bounds checks catch the
        // overflow as a trap.
        NEXT = NEXT.saturating_add(len);
        let base = core::ptr::addr_of!(ARENA) as usize;
        (base + ptr) as i32
    }
}

/// Read a little-endian u32 from linear memory at `ptr`.
unsafe fn load_u32(ptr: usize) -> usize {
    let b = ptr as *const u8;
    let v = u32::from_le_bytes([*b, *b.add(1), *b.add(2), *b.add(3)]);
    v as usize
}

/// Compute `<bucket>:<reversed key>` and report it through the
/// meta slot.
#[no_mangle]
pub extern "C" fn keyfun_route(
    in_ptr: i32,
    _in_len: i32,
    out_ptr_ptr: i32,
    out_len_ptr: i32,
) -> i32 {
    unsafe {
        let inp = in_ptr as usize;
        let blen = load_u32(inp);
        let bucket = inp + 4;
        let klen = load_u32(bucket + blen);
        let key = bucket + blen + 4;

        let out_len = blen + 1 + klen;
        let out = keyfun_alloc(out_len as i32) as usize as *mut u8;

        // Copy bucket verbatim.
        let src = bucket as *const u8;
        for i in 0..blen {
            *out.add(i) = *src.add(i);
        }
        // Separator.
        *out.add(blen) = b':';
        // Key reversed.
        let ksrc = key as *const u8;
        for i in 0..klen {
            *out.add(blen + 1 + i) = *ksrc.add(klen - 1 - i);
        }

        // Write the meta slot: out_ptr then out_len, LE u32.
        let op = out_ptr_ptr as usize as *mut u8;
        let optr = (out as usize) as u32;
        for (i, byte) in optr.to_le_bytes().iter().enumerate() {
            *op.add(i) = *byte;
        }
        let lp = out_len_ptr as usize as *mut u8;
        let olen = out_len as u32;
        for (i, byte) in olen.to_le_bytes().iter().enumerate() {
            *lp.add(i) = *byte;
        }
    }
    0
}
