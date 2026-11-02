//! Safe wrapper over C `vnode_dispatch` (`src/dyn_vnode.c`), the ring
//! lookup that maps a key's token to the peer index owning it.

use crate::hashkit::CToken;

/// Mirrors `struct continuum` in `src/dyn_core.h`:
/// `{ uint32_t index; uint32_t value; struct dyn_token *token; }`.
#[repr(C)]
struct CContinuum {
    index: u32,
    value: u32,
    token: *const CToken,
}

/// Mirrors `struct array` in `src/dyn_array.h`:
/// `{ uint32_t nelem; void *elem; size_t size; uint32_t nalloc; }`.
#[repr(C)]
struct CArray {
    nelem: u32,
    elem: *mut CContinuum,
    size: usize,
    nalloc: u32,
}

extern "C" {
    fn vnode_dispatch(continuums: *mut CArray, ncontinuum: u32, token: *const CToken) -> u32;
}

/// Run C `vnode_dispatch` over a ring of `(token, peer_index)` entries,
/// which must already be sorted by token ascending (C sorts in
/// `vnode_rack_verify_continuum` before any dispatch). Returns the peer
/// index C selects for `key_token`.
///
/// # Panics
///
/// Panics on an empty ring: the C `ASSERT`s `ncontinuum != 0`.
pub fn dispatch(ring: &[(CToken, u32)], key_token: &CToken) -> u32 {
    assert!(
        !ring.is_empty(),
        "C vnode_dispatch requires a non-empty ring"
    );
    // Tokens must outlive the continuum array that points into them.
    let tokens: Vec<CToken> = ring.iter().map(|(t, _)| *t).collect();
    let mut entries: Vec<CContinuum> = ring
        .iter()
        .zip(&tokens)
        .map(|((_, idx), tok)| CContinuum {
            index: *idx,
            value: 0,
            token: std::ptr::from_ref(tok),
        })
        .collect();
    let n = u32::try_from(entries.len()).expect("ring fits u32");
    let mut array = CArray {
        nelem: n,
        elem: entries.as_mut_ptr(),
        size: std::mem::size_of::<CContinuum>(),
        nalloc: n,
    };
    // SAFETY: `array` describes `entries` exactly (nelem == nalloc == len,
    // element size matches), every `token` pointer refers into `tokens`,
    // which outlives the call, and `key_token` is a live token. The C only
    // reads the array and tokens.
    unsafe { vnode_dispatch(&raw mut array, n, key_token) }
}
