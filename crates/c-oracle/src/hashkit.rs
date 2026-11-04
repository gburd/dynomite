//! Safe wrappers over the C hashkit (`src/hashkit/*.c`).
//!
//! Each C hash function fills a `struct dyn_token` from a key. The wrappers
//! hand the C an exact-layout token, then return its fields as a
//! [`CToken`] so tests can compare them with the Rust transliteration.

use std::os::raw::{c_int, c_uchar, c_ulong, c_void};

/// Mirrors `struct dyn_token` in `src/hashkit/dyn_token.h`:
/// `{ uint32_t signum; uint32_t mag[4]; uint32_t len; }`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CToken {
    /// `signum`: 0 for a zero token, 1 otherwise.
    pub signum: u32,
    /// `mag`: magnitude words, least significant first.
    pub mag: [u32; 4],
    /// `len`: number of meaningful words in `mag`.
    pub len: u32,
}

impl CToken {
    /// The meaningful magnitude words (`mag[..len]`).
    pub fn words(&self) -> &[u32] {
        let n = usize::try_from(self.len).unwrap_or(0).min(self.mag.len());
        &self.mag[..n]
    }
}

/// `rstatus_t` is `int` (`src/dyn_types.h`); `DN_OK` is 0.
type RStatus = c_int;
type HashFn = unsafe extern "C" fn(*const c_uchar, usize, *mut CToken) -> RStatus;

extern "C" {
    fn hash_crc16(key: *const c_uchar, len: usize, token: *mut CToken) -> RStatus;
    fn hash_crc32(key: *const c_uchar, len: usize, token: *mut CToken) -> RStatus;
    fn hash_crc32a(key: *const c_uchar, len: usize, token: *mut CToken) -> u32;
    fn hash_fnv1_64(key: *const c_uchar, len: usize, token: *mut CToken) -> RStatus;
    fn hash_fnv1a_64(key: *const c_uchar, len: usize, token: *mut CToken) -> RStatus;
    fn hash_fnv1_32(key: *const c_uchar, len: usize, token: *mut CToken) -> RStatus;
    fn hash_fnv1a_32(key: *const c_uchar, len: usize, token: *mut CToken) -> RStatus;
    fn hash_hsieh(key: *const c_uchar, len: usize, token: *mut CToken) -> RStatus;
    fn hash_jenkins(key: *const c_uchar, len: usize, token: *mut CToken) -> RStatus;
    fn hash_md5(key: *const c_uchar, len: usize, token: *mut CToken) -> RStatus;
    fn hash_murmur(key: *const c_uchar, len: usize, token: *mut CToken) -> RStatus;
    fn hash_murmur3(key: *const c_uchar, len: usize, token: *mut CToken) -> RStatus;
    fn hash_one_at_a_time(key: *const c_uchar, len: usize, token: *mut CToken) -> RStatus;
    // Declared as the definition in dyn_md5.c has it (`unsigned long`);
    // dyn_hashkit.h's prototype says `unsigned int`, a C-side mismatch.
    fn md5_signature(key: *mut c_uchar, length: c_ulong, result: *mut c_uchar);
    fn crc32_sz(buf: *const c_uchar, len: usize, in_crc32: u32) -> u32;
    fn cmp_dyn_token(t1: *const CToken, t2: *const CToken) -> i32;
    fn parse_dyn_token(start: *mut u8, len: u32, token: *mut CToken) -> RStatus;
    fn MurmurHash3_x86_32(key: *const c_void, len: c_int, seed: u32, out: *mut c_void);
    fn get_hash_type(hash_name: *const CString) -> c_int;
    fn get_hash_func(hash_type: c_int) -> Option<HashFn>;
}

/// Mirrors `struct string` in `src/dyn_string.h`: `{ uint32_t len; uint8_t *data; }`.
#[repr(C)]
struct CString {
    len: u32,
    data: *const u8,
}

/// C `get_hash_type`: the `HASH_CODEC` index of `name`, or `HASH_INVALID`
/// (the number of hash types) when the name is unknown.
pub fn hash_type_index(name: &[u8]) -> i32 {
    let s = CString {
        len: u32::try_from(name.len()).expect("hash name fits u32"),
        data: name.as_ptr(),
    };
    // SAFETY: `s` points at `name`, valid for `s.len` bytes for the whole
    // call; `get_hash_type` only reads `len` bytes of it.
    unsafe { get_hash_type(&raw const s) }
}

/// C `get_hash_func` followed by a call: hash `key` with the function C
/// selects for `index`, or `None` when C has no function for it
/// (`index` out of range). `key` must be non-empty (see [`hash`]).
pub fn hash_by_index(index: i32, key: &[u8]) -> Option<CToken> {
    assert!(!key.is_empty(), "the C hashes leave an empty key undefined");
    // SAFETY: `get_hash_func` only range-checks `index` and reads a static
    // table.
    let f = unsafe { get_hash_func(index) }?;
    let mut token = CToken::default();
    // SAFETY: `f` is one of the hash_* functions; it reads `key.len()`
    // bytes of `key` and writes only through `token`. `hash_crc32a` is
    // declared returning uint32_t, which is returned in the same register
    // as rstatus_t on every supported ABI and is not inspected here.
    unsafe { f(key.as_ptr(), key.len(), &raw mut token) };
    Some(token)
}

/// The C hash functions selectable by `hash:` in the C config
/// (`src/hashkit/dyn_hashkit.h`), in the order of `HASH_CODEC`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CHash {
    /// `hash_one_at_a_time`
    OneAtATime,
    /// `hash_md5`
    Md5,
    /// `hash_crc16`
    Crc16,
    /// `hash_crc32` (libmemcached variant)
    Crc32,
    /// `hash_crc32a` (standard CRC-32)
    Crc32a,
    /// `hash_fnv1_64`
    Fnv164,
    /// `hash_fnv1a_64`
    Fnv1a64,
    /// `hash_fnv1_32`
    Fnv132,
    /// `hash_fnv1a_32`
    Fnv1a32,
    /// `hash_hsieh`
    Hsieh,
    /// `hash_murmur`
    Murmur,
    /// `hash_jenkins`
    Jenkins,
    /// `hash_murmur3`
    Murmur3,
}

impl CHash {
    /// Every C hash function, for exhaustive oracle sweeps.
    pub const ALL: [CHash; 13] = [
        CHash::OneAtATime,
        CHash::Md5,
        CHash::Crc16,
        CHash::Crc32,
        CHash::Crc32a,
        CHash::Fnv164,
        CHash::Fnv1a64,
        CHash::Fnv132,
        CHash::Fnv1a32,
        CHash::Hsieh,
        CHash::Murmur,
        CHash::Jenkins,
        CHash::Murmur3,
    ];
}

/// Run C hash function `h` over `key` and return the token it produced.
///
/// Returns `None` for an empty key. The C hash functions do not define a
/// result there: `hash_jenkins` returns its mixing state `c` as the status
/// and leaves the token unwritten, and `hash_hsieh` returns 0 with the token
/// unwritten. C never reaches that path: its only caller,
/// `dnode_peer_pool_server` (`src/dyn_dnode_peer.c:800`), routes a
/// zero-length key to peer index 0 without hashing. Empty-key routing is
/// tested against that contract, not against these functions.
pub fn hash(h: CHash, key: &[u8]) -> Option<CToken> {
    if key.is_empty() {
        return None;
    }
    let mut token = CToken::default();
    let f: HashFn = match h {
        CHash::OneAtATime => hash_one_at_a_time,
        CHash::Md5 => hash_md5,
        CHash::Crc16 => hash_crc16,
        CHash::Crc32 => hash_crc32,
        CHash::Crc32a => {
            // hash_crc32a returns uint32_t rather than rstatus_t, so it does
            // not fit HashFn; call it directly.
            // SAFETY: `key` is a valid slice for `key.len()` bytes and
            // `token` is a live, exactly-laid-out `struct dyn_token`. The C
            // function reads `key_length` bytes and writes only `*token`.
            unsafe { hash_crc32a(key.as_ptr(), key.len(), &raw mut token) };
            return Some(token);
        }
        CHash::Fnv164 => hash_fnv1_64,
        CHash::Fnv1a64 => hash_fnv1a_64,
        CHash::Fnv132 => hash_fnv1_32,
        CHash::Fnv1a32 => hash_fnv1a_32,
        CHash::Hsieh => hash_hsieh,
        CHash::Murmur => hash_murmur,
        CHash::Jenkins => hash_jenkins,
        CHash::Murmur3 => hash_murmur3,
    };
    // SAFETY: as above. Every hash_* reads exactly `key_length` bytes from
    // `key` and writes only through `token`.
    let rc = unsafe { f(key.as_ptr(), key.len(), &raw mut token) };
    assert_eq!(rc, 0, "C {h:?} returned rstatus {rc}");
    Some(token)
}

/// C `md5_signature`: the raw 16-byte MD5 digest of `key`.
pub fn md5(key: &[u8]) -> [u8; 16] {
    let mut out = [0u8; 16];
    let mut buf = key.to_vec();
    let len = c_ulong::try_from(buf.len()).expect("key fits in unsigned long");
    // SAFETY: `buf` is an owned copy (the C signature takes a non-const
    // pointer but does not write the key); `out` holds the 16 bytes the C
    // writes.
    unsafe { md5_signature(buf.as_mut_ptr(), len, out.as_mut_ptr()) };
    out
}

/// C `crc32_sz`: CRC-32 of `buf`, continuing from `in_crc32`.
pub fn crc32_sz_c(buf: &[u8], in_crc32: u32) -> u32 {
    // SAFETY: reads `buf.len()` bytes from a valid slice; no writes.
    unsafe { crc32_sz(buf.as_ptr(), buf.len(), in_crc32) }
}

/// C `cmp_dyn_token`: -1, 0, or 1.
pub fn cmp_tokens(a: &CToken, b: &CToken) -> i32 {
    // SAFETY: both pointers reference live, correctly laid out tokens; the
    // C reads them only.
    unsafe { cmp_dyn_token(a, b) }
}

/// C `parse_dyn_token`: parse a decimal token string. The C has no error
/// return; it `ASSERT`s `len > 0` and, after a leading `-`, at least one
/// digit. Inputs that would trip those asserts return `None` instead of
/// being passed to the C.
pub fn parse_token(text: &[u8]) -> Option<CToken> {
    if text.is_empty() || text == b"-" {
        return None;
    }
    let mut buf = text.to_vec();
    let len = u32::try_from(buf.len()).ok()?;
    let mut token = CToken::default();
    // SAFETY: `buf` is an owned, writable copy of `len` bytes; `token` is a
    // live token the C fills.
    let rc = unsafe { parse_dyn_token(buf.as_mut_ptr(), len, &raw mut token) };
    (rc == 0).then_some(token)
}

/// C contrib `MurmurHash3_x86_32` (`contrib/murmur3/murmur3.c`), the
/// reference the Rust `hash: murmur3` ring hash is checked against.
///
/// # Panics
///
/// Panics if `key` is longer than `i32::MAX` bytes (the C takes `int`).
pub fn murmur3_x86_32(key: &[u8], seed: u32) -> u32 {
    let len = c_int::try_from(key.len()).expect("key fits a C int");
    let mut out: u32 = 0;
    // SAFETY: `key` is valid for `len` bytes; `out` is a live, aligned u32,
    // the 4 bytes the C writes.
    unsafe { MurmurHash3_x86_32(key.as_ptr().cast(), len, seed, (&raw mut out).cast()) };
    out
}
