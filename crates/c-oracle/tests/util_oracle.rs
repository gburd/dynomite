//! Oracle: C `dyn_util.c`, `dyn_string.c`, `dyn_log.c`, `dyn_setting.c`
//! and `dyn_signal.c` pure logic against the Rust engine.

#![cfg(not(c_oracle_absent))]

use std::cmp::Ordering;

use c_oracle::util as c;
use dynomite::core::{log, setting, signal};
use dynomite::util::{atoi, dyn_string, time};
use hegel::generators as gs;
use hegel::TestCase;
use nix::sys::signal::Signal;

/// Digit strings with an occasional non-digit, long enough to overflow.
fn draw_numeric(tc: &TestCase) -> Vec<u8> {
    let raw: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>().max_value(11)).max_size(14));
    raw.into_iter()
        .map(|d| {
            if d < 10 {
                b'0' + d
            } else {
                b"x-"[usize::from(d - 10)]
            }
        })
        .collect()
}

#[hegel::test(test_cases = 1000)]
fn dn_atoi_matches_c(tc: TestCase) {
    let line = draw_numeric(&tc);
    assert_eq!(
        atoi::dn_atoi(&line).unwrap_or(-1),
        c::atoi(&line),
        "{line:?}"
    );
}

#[hegel::test(test_cases = 1000)]
fn dn_atoui_matches_c(tc: TestCase) {
    let line = draw_numeric(&tc);
    assert_eq!(
        atoi::dn_atoui(&line).unwrap_or(0),
        c::atoui(&line),
        "{line:?}"
    );
}

#[hegel::test(test_cases = 500)]
fn dn_valid_port_matches_c(tc: TestCase) {
    let n: i32 = tc.draw(gs::integers::<i32>());
    let near: i32 = tc.draw(gs::integers::<i32>().min_value(-3).max_value(65_540));
    for v in [n, near, 0, 1, 65_535, 65_536] {
        assert_eq!(atoi::valid_port(v), c::valid_port(v), "{v}");
    }
}

#[hegel::test(test_cases = 500)]
fn count_digits_matches_c(tc: TestCase) {
    let n: u64 = tc.draw(gs::integers::<u64>());
    let shift: u32 = tc.draw(gs::integers::<u32>().max_value(63));
    for v in [n, n >> shift, 0, u64::MAX] {
        assert_eq!(
            i64::from(time::count_digits(v)),
            i64::from(c::digits(v)),
            "{v}"
        );
    }
}

/// `string_compare` equality agrees with C, and equal-length ordering
/// agrees. Different lengths: C computes `s1->len - s2->len > 0` on
/// `uint32_t`, so it answers 1 whichever string is shorter; Rust sorts
/// the shorter first (PROPOSED DEV-U1).
#[hegel::test(test_cases = 1000)]
fn string_compare_matches_c(tc: TestCase) {
    let gen = || gs::vecs(gs::integers::<u8>().min_value(1).max_value(4)).max_size(4);
    let a: Vec<u8> = tc.draw(gen());
    let b: Vec<u8> = tc.draw(gen());
    let (r, cr) = (dyn_string::string_compare(&a, &b), c::compare(&a, &b));
    assert_eq!(r == Ordering::Equal, cr == Ordering::Equal, "{a:?} {b:?}");
    if a.len() == b.len() {
        assert_eq!(r, cr, "{a:?} {b:?}");
    } else {
        assert_eq!(
            cr,
            Ordering::Greater,
            "C length-mismatch answer {a:?} {b:?}"
        );
    }
}

#[derive(Debug, Clone, Copy)]
enum LevelOp {
    Up,
    Down,
    Set(u8),
    SigUp,
    SigDown,
    SigNoop(i32),
}

/// Log level up / down / set and the signal-driven level changes, applied
/// to the C logger and the Rust level in lockstep.
#[hegel::test(test_cases = 300)]
fn log_level_ops_match_c(tc: TestCase) {
    let _g = c::GLOBALS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let start: u8 = tc.draw(gs::integers::<u8>().max_value(20));
    c::log_open(i32::from(start));
    log::log_level_set(start);
    let n: usize = tc.draw(gs::integers::<usize>().max_value(30));
    for _ in 0..n {
        let op = match tc.draw(gs::integers::<u8>().max_value(5)) {
            0 => LevelOp::Up,
            1 => LevelOp::Down,
            2 => LevelOp::Set(tc.draw(gs::integers::<u8>())),
            3 => LevelOp::SigUp,
            4 => LevelOp::SigDown,
            _ => LevelOp::SigNoop(if tc.draw(gs::booleans()) { 10 } else { 12 }),
        };
        match op {
            LevelOp::Up => {
                c::level_up();
                log::log_level_increment();
            }
            LevelOp::Down => {
                c::level_down();
                log::log_level_decrement();
            }
            LevelOp::Set(v) => {
                c::level_set(i32::from(v));
                log::log_level_set(v);
            }
            LevelOp::SigUp => {
                c::handle_signal(Signal::SIGTTIN as i32);
                assert!(!signal::dispatch(Signal::SIGTTIN).expect("dispatch"));
            }
            LevelOp::SigDown => {
                c::handle_signal(Signal::SIGTTOU as i32);
                assert!(!signal::dispatch(Signal::SIGTTOU).expect("dispatch"));
            }
            LevelOp::SigNoop(s) => {
                c::handle_signal(s);
                let sig = Signal::try_from(s).expect("signal");
                assert!(!signal::dispatch(sig).expect("dispatch"));
            }
        }
        assert_eq!(i32::from(log::current_level()), c::level(), "after {op:?}");
        for l in 0..=12u8 {
            assert_eq!(
                log::log_loggable(l),
                c::loggable(i32::from(l)),
                "loggable {l} after {op:?}"
            );
        }
    }
}

#[test]
fn msgs_per_sec_matches_c() {
    let _g = c::GLOBALS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(c::conn_msg_rate(), setting::DEFAULT_MSGS_PER_SEC);
    for v in [0, 1, 7, 50_000, u32::MAX] {
        c::set_conn_msg_rate(v);
        setting::set_msgs_per_sec(v);
        assert_eq!(setting::msgs_per_sec(), c::conn_msg_rate());
    }
    c::set_conn_msg_rate(setting::DEFAULT_MSGS_PER_SEC);
    setting::set_msgs_per_sec(setting::DEFAULT_MSGS_PER_SEC);
}

/// The Rust signal table lists the C `signals[]` table
/// (`src/dyn_signal.c`) in the same order, with the same names, and
/// ignores exactly the signals C sets to `SIG_IGN`.
#[test]
fn signal_table_matches_c_source() {
    let root = std::env::var_os("DYNOMITE_C_REF").map_or_else(
        || std::path::PathBuf::from(std::env::var_os("HOME").expect("HOME")).join("src/dynomite"),
        std::path::PathBuf::from,
    );
    let src = std::fs::read_to_string(root.join("src/dyn_signal.c")).expect("read dyn_signal.c");
    let table = src.split("signals[] = {").nth(1).expect("signals table");
    let table = table.split("{0, NULL").next().expect("table end");
    let c_rows: Vec<(String, bool)> = table
        .split('{')
        .filter_map(|e| {
            let name = e.split('"').nth(1)?;
            Some((name.to_owned(), e.contains("SIG_IGN")))
        })
        .collect();
    let rust_rows: Vec<(String, bool)> = signal::default_actions()
        .iter()
        .map(|e| (e.name.to_owned(), e.action == signal::SignalAction::Ignore))
        .collect();
    assert_eq!(rust_rows, c_rows);
}
