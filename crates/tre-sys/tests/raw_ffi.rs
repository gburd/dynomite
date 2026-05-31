//! Direct exercises of the raw `tre-sys` FFI surface.
//!
//! These tests deliberately work in `unsafe` blocks; they are
//! the contract suite that documents how the higher-level safe
//! wrapper in `dyntext::tre` is meant to call into TRE.
//!
//! Each test compiles a small pattern, runs the approximate
//! matcher with explicit cost weights, and frees the pattern.
//! No global state is touched; every test owns its `regex_t`.

use std::ffi::CString;
use std::os::raw::c_int;
use std::ptr;

use tre_sys::{
    default_regaparams, empty_regex_t, regamatch_t, tre_reganexec, tre_regfree, tre_regncomp,
    REG_EXTENDED, REG_NOMATCH, REG_OK,
};

/// Build a pattern and run a single approximate match. Returns
/// the TRE return code (REG_OK / REG_NOMATCH / REG_E*) and the
/// realised cost when the match succeeds.
fn match_with(pattern: &[u8], text: &[u8], max_err: i32, cost_subst: i32) -> (c_int, Option<i32>) {
    // SAFETY: `regex_t` is zero-initialised by `empty_regex_t`
    // and is freed via `tre_regfree` at the end of this scope.
    // The pattern slice outlives the `tre_regncomp` call (it is
    // borrowed before any FFI call and the call returns before
    // we drop it).
    unsafe {
        let mut preg = empty_regex_t();
        let pc = pattern.as_ptr().cast::<i8>();
        let rc = tre_regncomp(
            std::ptr::addr_of_mut!(preg),
            pc,
            pattern.len(),
            REG_EXTENDED,
        );
        assert_eq!(rc, REG_OK, "compile failed for pattern {pattern:?}");

        let mut params = default_regaparams();
        params.max_err = max_err;
        params.max_cost = max_err.saturating_mul(cost_subst.max(1));
        params.cost_subst = cost_subst;

        let mut amatch = regamatch_t {
            nmatch: 0,
            pmatch: ptr::null_mut(),
            cost: 0,
            num_ins: 0,
            num_del: 0,
            num_subst: 0,
        };

        let tc = text.as_ptr().cast::<i8>();
        let exec_rc = tre_reganexec(
            std::ptr::addr_of!(preg),
            tc,
            text.len(),
            std::ptr::addr_of_mut!(amatch),
            params,
            0,
        );

        tre_regfree(std::ptr::addr_of_mut!(preg));

        let cost = if exec_rc == REG_OK {
            Some(amatch.cost)
        } else {
            None
        };
        (exec_rc, cost)
    }
}

#[test]
fn regaexec_exact_match_returns_zero() {
    let (rc, cost) = match_with(b"hello", b"the hello world", 0, 1);
    assert_eq!(rc, REG_OK);
    assert_eq!(cost, Some(0));
}

#[test]
fn regaexec_one_typo_returns_match_with_cost_1() {
    // One substitution: 'helXo' vs 'hello'.
    let (rc, cost) = match_with(b"hello", b"the helXo world", 1, 1);
    assert_eq!(rc, REG_OK);
    assert_eq!(cost, Some(1));
}

#[test]
fn regaexec_too_many_typos_returns_no_match() {
    // Three substitutions but max_err = 1.
    let (rc, cost) = match_with(b"hello", b"helXXX", 1, 1);
    assert_eq!(rc, REG_NOMATCH);
    assert!(cost.is_none());
}

#[test]
fn regaexec_compile_invalid_pattern_returns_error() {
    // SAFETY: same lifetime / freeing discipline as
    // `match_with`. `tre_regncomp` is documented to leave
    // `preg` in an indeterminate state on failure, so we do
    // NOT call `tre_regfree` after a failed compile.
    unsafe {
        let mut preg = empty_regex_t();
        let bad: &[u8] = b"unbalanced(";
        let rc = tre_regncomp(
            std::ptr::addr_of_mut!(preg),
            bad.as_ptr().cast::<i8>(),
            bad.len(),
            REG_EXTENDED,
        );
        assert_ne!(
            rc, REG_OK,
            "compile of {bad:?} should fail but returned REG_OK",
        );
    }
}

#[test]
fn regerror_formats_known_code() {
    // SAFETY: the error buffer is fully owned by this scope;
    // we pass a writable pointer with the correct capacity to
    // tre_regerror. `preg` is null because POSIX `regerror`
    // tolerates a null `preg` for canned (non-pattern-specific)
    // codes.
    let mut buf = [0u8; 128];
    let written = unsafe {
        tre_sys::tre_regerror(
            REG_NOMATCH,
            std::ptr::null(),
            buf.as_mut_ptr().cast::<i8>(),
            buf.len(),
        )
    };
    assert!(written > 0);
    // The returned string must be NUL-terminated within the
    // first `written` bytes.
    let nul = buf
        .iter()
        .position(|&b| b == 0)
        .expect("regerror null-terminates output");
    let msg = std::str::from_utf8(&buf[..nul]).expect("regerror returns ASCII text");
    assert!(!msg.is_empty(), "regerror returned an empty message");
}

#[test]
fn cstring_pattern_compiles_too() {
    // Sanity-check that a CString-style (NUL-terminated)
    // pattern works just as well as a raw byte slice with an
    // explicit length. This test is here so reviewers can see
    // the two paths agree.
    let pat = CString::new("foo").expect("pattern has no NULs");
    // SAFETY: `preg` is owned and freed in this scope; `pat`
    // outlives the FFI call.
    unsafe {
        let mut preg = empty_regex_t();
        let rc = tre_regncomp(
            std::ptr::addr_of_mut!(preg),
            pat.as_ptr(),
            pat.as_bytes().len(),
            REG_EXTENDED,
        );
        assert_eq!(rc, REG_OK);
        tre_regfree(std::ptr::addr_of_mut!(preg));
    }
}
