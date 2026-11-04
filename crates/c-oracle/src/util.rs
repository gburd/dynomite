//! Safe wrappers over C `dyn_util.c`, `dyn_string.c`, `dyn_log.c` and
//! `dyn_setting.c` pure-logic functions.
//!
//! The C logger level and the per-connection message rate are process
//! globals; callers that mutate them serialise through [`GLOBALS`].

use std::cmp::Ordering;
use std::os::raw::{c_char, c_int};
use std::sync::Mutex;

/// Serialises tests that mutate the C logger or setting globals.
pub static GLOBALS: Mutex<()> = Mutex::new(());

/// Mirrors `struct string` in `src/dyn_string.h`: `{ uint32_t len; uint8_t *data; }`.
#[repr(C)]
struct CString {
    len: u32,
    data: *const u8,
}

extern "C" {
    fn _dn_atoi(line: *const u8, n: usize) -> c_int;
    fn _dn_atoui(line: *const u8, n: usize) -> u32;
    fn dn_valid_port(n: c_int) -> bool;
    fn count_digits(arg: u64) -> c_int;
    fn string_compare(s1: *const CString, s2: *const CString) -> c_int;
    fn log_init(level: c_int, name: *const c_char) -> c_int;
    fn log_level_up();
    fn log_level_down();
    fn log_level_set(level: c_int);
    fn log_loggable(level: c_int) -> c_int;
    fn msgs_per_sec() -> u32;
    fn set_msgs_per_sec(tokens_per_sec: u32);
    fn signal_handler(signo: c_int);
}

/// C `dn_atoi`: `-1` on empty input, a non-digit, or a negative result.
pub fn atoi(line: &[u8]) -> i32 {
    // SAFETY: `line` is valid for `line.len()` bytes; C reads exactly `n`.
    unsafe { _dn_atoi(line.as_ptr(), line.len()) }
}

/// C `dn_atoui`: `0` on empty input or a non-digit.
pub fn atoui(line: &[u8]) -> u32 {
    // SAFETY: `line` is valid for `line.len()` bytes; C reads exactly `n`.
    unsafe { _dn_atoui(line.as_ptr(), line.len()) }
}

/// C `dn_valid_port`.
pub fn valid_port(n: i32) -> bool {
    // SAFETY: pure function of an integer.
    unsafe { dn_valid_port(n) }
}

/// C `count_digits`.
pub fn digits(arg: u64) -> i32 {
    // SAFETY: pure function of an integer (snprintf into a NULL, 0 buffer).
    unsafe { count_digits(arg) }
}

/// C `string_compare`, reduced to its sign.
pub fn compare(a: &[u8], b: &[u8]) -> Ordering {
    let s1 = CString {
        len: u32::try_from(a.len()).expect("fits u32"),
        data: a.as_ptr(),
    };
    let s2 = CString {
        len: u32::try_from(b.len()).expect("fits u32"),
        data: b.as_ptr(),
    };
    // SAFETY: both structs point at live slices of the stated length; C
    // reads at most `len` bytes of each.
    unsafe { string_compare(&raw const s1, &raw const s2) }.cmp(&0)
}

/// C `log_init` to `/dev/null` (once per process), then
/// `log_level_set(level)`, which clamps exactly as `log_init` does.
pub fn log_open(level: i32) {
    static OPEN: std::sync::Once = std::sync::Once::new();
    OPEN.call_once(|| {
        // SAFETY: the path is a NUL-terminated static string.
        let rc = unsafe { log_init(level, c"/dev/null".as_ptr()) };
        assert_eq!(rc, 0, "log_init(/dev/null) failed");
    });
    level_set(level);
}

/// C `log_level_up`.
pub fn level_up() {
    // SAFETY: mutates the C logger global; callers hold [`GLOBALS`].
    unsafe { log_level_up() }
}

/// C `log_level_down`.
pub fn level_down() {
    // SAFETY: mutates the C logger global; callers hold [`GLOBALS`].
    unsafe { log_level_down() }
}

/// C `log_level_set`.
pub fn level_set(level: i32) {
    // SAFETY: mutates the C logger global; callers hold [`GLOBALS`].
    unsafe { log_level_set(level) }
}

/// C `log_loggable`.
pub fn loggable(level: i32) -> bool {
    // SAFETY: reads the C logger global.
    unsafe { log_loggable(level) != 0 }
}

/// The current C logger level, recovered through `log_loggable` (the
/// struct is file-static in `dyn_log.c`).
pub fn level() -> i32 {
    (0..=11).rev().find(|&l| loggable(l)).unwrap_or(-1)
}

/// C `msgs_per_sec`.
pub fn conn_msg_rate() -> u32 {
    // SAFETY: reads a C global.
    unsafe { msgs_per_sec() }
}

/// C `set_msgs_per_sec`.
pub fn set_conn_msg_rate(v: u32) {
    // SAFETY: writes a C global; callers hold [`GLOBALS`].
    unsafe { set_msgs_per_sec(v) }
}

/// C `signal_handler` for a signal whose C action neither exits nor
/// raises (`SIGUSR1`, `SIGUSR2`, `SIGTTIN`, `SIGTTOU`, `SIGHUP`).
pub fn handle_signal(signo: i32) {
    assert!(
        [10, 12, 21, 22, 1].contains(&signo),
        "signal {signo} would exit or raise in C"
    );
    // SAFETY: for these signals C only logs and adjusts the logger
    // global (callers hold [`GLOBALS`]); it never exits or raises.
    unsafe { signal_handler(signo) }
}
