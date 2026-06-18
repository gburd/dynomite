//! MapReduce map-phase fixture compiled to
//! `wasm32-unknown-unknown`.
//!
//! Map rule: for each inbound object `{bucket,key,value,data}`,
//! emit the numeric `value` field doubled (`value * 2`) as a bare
//! JSON number. Non-numeric inputs emit JSON `null`. The host
//! asserts the doubled outputs exactly, proving the production
//! Rust -> WASM path for MapReduce phases (the other phase tests
//! drive hand-written WAT).
//!
//! The module speaks the dyniak phase ABI:
//!
//! * `phase_alloc(len) -> ptr` -- bump allocator over a static
//!   arena.
//! * `phase_apply(in_ptr, in_len, out_ptr_ptr, out_len_ptr) -> i32`
//!   -- decodes the CBOR `Vec<serde_json::Value>` input, applies
//!   the map, re-encodes the CBOR `Vec<serde_json::Value>` output,
//!   stores `(out_ptr, out_len)` in the meta slot, and returns 0.
//!
//! This builds with `std` (the default for
//! `wasm32-unknown-unknown`) so it can reuse `serde_json` and
//! `ciborium`, exactly as a real operator-supplied phase would.
//! The `unsafe` blocks are the minimum needed to expose the
//! linear-memory ABI; the workspace `forbid(unsafe_code)` lint
//! does not apply because this fixture is excluded from the
//! workspace.

#![allow(unsafe_code)]

use serde_json::Value;

/// Bump-allocate `len` bytes inside a leaked `Vec`, returning the
/// pointer. The host only ever reads/writes within `[ptr, ptr+len)`
/// for the buffers it asked for, and each invocation runs in a
/// fresh wasm instance, so leaking is the simplest correct arena.
#[no_mangle]
pub extern "C" fn phase_alloc(len: i32) -> i32 {
    let len = usize::try_from(len).unwrap_or(0);
    let mut buf = Vec::<u8>::with_capacity(len);
    buf.resize(len, 0);
    let ptr = buf.as_mut_ptr() as i32;
    core::mem::forget(buf);
    ptr
}

/// Apply the doubling map.
#[no_mangle]
pub extern "C" fn phase_apply(
    in_ptr: i32,
    in_len: i32,
    out_ptr_ptr: i32,
    out_len_ptr: i32,
) -> i32 {
    let in_len = match usize::try_from(in_len) {
        Ok(n) => n,
        Err(_) => return 1,
    };
    let input: &[u8] = unsafe { core::slice::from_raw_parts(in_ptr as *const u8, in_len) };

    let values: Vec<Value> = match ciborium::de::from_reader(input) {
        Ok(v) => v,
        Err(_) => return 1,
    };

    let mapped: Vec<Value> = values
        .into_iter()
        .map(|v| {
            let n = v
                .get("value")
                .or(Some(&v))
                .and_then(Value::as_i64);
            match n {
                Some(x) => Value::from(x * 2),
                None => Value::Null,
            }
        })
        .collect();

    let mut out_bytes = Vec::<u8>::new();
    if ciborium::ser::into_writer(&mapped, &mut out_bytes).is_err() {
        return 1;
    }

    let out_len = out_bytes.len();
    let out_ptr = phase_alloc(out_len as i32);
    unsafe {
        core::ptr::copy_nonoverlapping(out_bytes.as_ptr(), out_ptr as *mut u8, out_len);
        let op = out_ptr_ptr as *mut u8;
        for (i, byte) in (out_ptr as u32).to_le_bytes().iter().enumerate() {
            *op.add(i) = *byte;
        }
        let lp = out_len_ptr as *mut u8;
        for (i, byte) in (out_len as u32).to_le_bytes().iter().enumerate() {
            *lp.add(i) = *byte;
        }
    }
    0
}
