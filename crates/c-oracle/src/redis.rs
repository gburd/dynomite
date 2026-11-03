//! Safe wrapper over C `redis_parse_rsp` (`src/proto/dyn_redis.c`), the
//! Redis response parser, compiled against the release configuration.
//!
//! [`parse_rsp`] runs the parser once over a single mbuf holding the
//! input and reports what the C left on the message.

use std::ffi::{c_char, CStr};
use std::sync::Mutex;

const MAX_ARGS: usize = 64;

/// Mirrors `struct oracle_rsp_out` in `csrc/redis_shim.c`.
#[repr(C)]
struct COut {
    result: i32,
    ty: i32,
    integer: u32,
    is_error: i32,
    nkeys: u32,
    pos: u32,
    state: u32,
    nargs: u32,
    ntokens: u32,
    arg_start: [u32; MAX_ARGS],
    arg_end: [u32; MAX_ARGS],
}

extern "C" {
    fn oracle_redis_parse_rsp_split(data: *const u8, len: u32, split: u32, out: *mut COut) -> i32;
    fn oracle_msg_type_name(ty: i32) -> *const c_char;
    fn oracle_mbuf_capacity() -> u32;
    fn oracle_set_read_repairs(enabled: i32);
}

/// The C mbuf free list and logger are process globals.
static LOCK: Mutex<()> = Mutex::new(());

/// C `msg_parse_result_t` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseResult {
    /// `MSG_PARSE_OK`.
    Ok,
    /// `MSG_PARSE_ERROR`.
    Error,
    /// `MSG_PARSE_REPAIR`.
    Repair,
    /// `MSG_PARSE_FRAGMENT`.
    Fragment,
    /// `MSG_PARSE_AGAIN`.
    Again,
    /// `MSG_PARSE_NOOP`.
    Noop,
    /// `MSG_PARSE_DYNO_CONFIG`.
    DynoConfig,
    /// `MSG_OOM_ERROR`.
    Oom,
}

impl ParseResult {
    fn from_c(v: i32) -> Self {
        match v {
            0 => Self::Ok,
            1 => Self::Error,
            2 => Self::Repair,
            3 => Self::Fragment,
            4 => Self::Again,
            5 => Self::Noop,
            6 => Self::DynoConfig,
            7 => Self::Oom,
            other => panic!("unknown msg_parse_result {other}"),
        }
    }
}

/// Observable outcome of one C `redis_parse_rsp` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RspOutcome {
    /// `r->result`.
    pub result: ParseResult,
    /// `r->type` as its `MSG_TYPE_CODEC` name without the `MSG_` prefix,
    /// for example `RSP_REDIS_STATUS`.
    pub type_name: String,
    /// `r->integer`.
    pub integer: u32,
    /// `r->is_error`.
    pub is_error: bool,
    /// `r->nkeys`.
    pub nkeys: u32,
    /// `r->pos` as an offset from the start of the input.
    pub pos: usize,
    /// `r->state` (the parser state machine position).
    pub state: u32,
    /// `r->ntokens`.
    pub ntokens: u32,
    /// `r->args` as `(start, end)` offsets into the input.
    pub args: Vec<(usize, usize)>,
}

/// Largest input [`parse_rsp`] accepts: one mbuf of the default size.
pub fn max_input() -> usize {
    // SAFETY: returns a compile-time constant.
    unsafe { oracle_mbuf_capacity() as usize }
}

/// Set the flag the C `is_read_repairs_enabled` returns.
pub fn set_read_repairs(enabled: bool) {
    let _g = LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // SAFETY: writes a plain static under the lock.
    unsafe { oracle_set_read_repairs(i32::from(enabled)) }
}

/// Run C `redis_parse_rsp` once over `input` held in a single mbuf.
///
/// # Panics
///
/// Panics when `input` is longer than [`max_input`] or the C harness
/// cannot allocate.
pub fn parse_rsp(input: &[u8]) -> RspOutcome {
    parse_rsp_split(input, input.len())
}

/// Run C `redis_parse_rsp` over `input` received in two reads: the first
/// `split` bytes, then (when that parse returns `MSG_PARSE_AGAIN`) the rest
/// appended to the same mbuf, resuming from the saved parser state.
///
/// # Panics
///
/// Panics when `input` is longer than [`max_input`] or the C harness
/// cannot allocate.
pub fn parse_rsp_split(input: &[u8], split: usize) -> RspOutcome {
    assert!(input.len() <= max_input(), "input exceeds one mbuf");
    let len = u32::try_from(input.len()).expect("len fits u32");
    let split = u32::try_from(split.min(input.len())).expect("split fits u32");
    let mut out = COut {
        result: 0,
        ty: 0,
        integer: 0,
        is_error: 0,
        nkeys: 0,
        pos: 0,
        state: 0,
        nargs: 0,
        ntokens: 0,
        arg_start: [0; MAX_ARGS],
        arg_end: [0; MAX_ARGS],
    };
    let _g = LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // SAFETY: `input` is live for `len` bytes, `out` is a valid
    // `oracle_rsp_out`, and the lock serialises access to C globals.
    let rc = unsafe { oracle_redis_parse_rsp_split(input.as_ptr(), len, split, &raw mut out) };
    assert_eq!(rc, 0, "C oracle harness setup failed");
    // SAFETY: the shim returns a pointer into a static string table or
    // NULL for an out-of-range type.
    let name = unsafe { oracle_msg_type_name(out.ty) };
    assert!(!name.is_null(), "C type {} out of range", out.ty);
    // SAFETY: non-null, NUL-terminated static string.
    let type_name = unsafe { CStr::from_ptr(name) }
        .to_str()
        .expect("ASCII type name")
        .to_owned();
    let n = (out.nargs as usize).min(MAX_ARGS);
    RspOutcome {
        result: ParseResult::from_c(out.result),
        type_name,
        integer: out.integer,
        is_error: out.is_error != 0,
        nkeys: out.nkeys,
        pos: out.pos as usize,
        state: out.state,
        ntokens: out.ntokens,
        args: (0..n)
            .map(|i| (out.arg_start[i] as usize, out.arg_end[i] as usize))
            .collect(),
    }
}
