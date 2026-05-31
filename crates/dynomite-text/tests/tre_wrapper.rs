//! Integration tests for the safe TRE wrapper.

use dyntext::tre::{TreCompiledPattern, TreError, TreMatchOpts};

fn opts_with_errors(max_errors: u16) -> TreMatchOpts {
    TreMatchOpts {
        max_errors,
        ..TreMatchOpts::default()
    }
}

#[test]
fn compile_simple_pattern_succeeds() {
    let pat = TreCompiledPattern::compile(b"hello", opts_with_errors(0));
    assert!(pat.is_ok());
}

#[test]
fn compile_invalid_pattern_returns_error() {
    let pat = TreCompiledPattern::compile(b"unbalanced(", opts_with_errors(0));
    assert!(matches!(pat, Err(TreError::Compile(_))));
}

#[test]
fn compile_with_max_errors_and_zero_costs_is_invalid() {
    let bad = TreMatchOpts {
        max_errors: 2,
        cost_ins: 0,
        cost_del: 0,
        cost_subst: 0,
        ..TreMatchOpts::default()
    };
    let res = TreCompiledPattern::compile(b"foo", bad);
    assert!(matches!(res, Err(TreError::InvalidOptions(_))));
}

#[test]
fn is_match_exact() {
    let pat =
        TreCompiledPattern::compile(b"connection refused", opts_with_errors(0)).expect("compile");
    assert!(pat.is_match(b"errno: connection refused"));
    assert!(!pat.is_match(b"errno: connection accepted"));
}

#[test]
fn is_match_one_deletion_with_max_errors_1() {
    // Pattern is "connection"; haystack has "cnnection".
    let pat = TreCompiledPattern::compile(b"connection", opts_with_errors(1)).expect("compile");
    assert!(pat.is_match(b"errno: cnnection refused"));
}

#[test]
fn is_match_one_insertion_with_max_errors_1() {
    // Pattern is "errno"; haystack has "errnno".
    let pat = TreCompiledPattern::compile(b"errno", opts_with_errors(1)).expect("compile");
    assert!(pat.is_match(b"errnno: connection refused"));
}

#[test]
fn is_match_one_substitution_with_max_errors_1() {
    // Pattern "hello"; haystack "hellp".
    let pat = TreCompiledPattern::compile(b"hello", opts_with_errors(1)).expect("compile");
    assert!(pat.is_match(b"there hellp world"));
}

#[test]
fn is_match_too_many_errors_returns_false() {
    // 4 deletions in "errno: connection refused" -> "errno cnntion rfsed".
    let pat =
        TreCompiledPattern::compile(br"errno: \w+ refused", opts_with_errors(1)).expect("compile");
    assert!(!pat.is_match(b"errno cnntion rfsed"));
}

#[test]
fn brief_demo_assertions_hold() {
    // Reproduces the four assertions from the brief verbatim:
    //
    //     assert!(pat.is_match(b"errno: connection refused"));
    //     assert!(pat.is_match(b"errno: cnnection refused"));
    //     assert!(pat.is_match(b"errnno: connection refused"));
    //     assert!(!pat.is_match(b"errno cnntion rfsed"));
    let opts = TreMatchOpts {
        max_errors: 1,
        cost_ins: 1,
        cost_del: 1,
        cost_subst: 1,
        ..TreMatchOpts::default()
    };
    let pat = TreCompiledPattern::compile(br"errno: \w+ refused", opts).expect("compile");

    assert!(pat.is_match(b"errno: connection refused"));
    assert!(pat.is_match(b"errno: cnnection refused"));
    assert!(pat.is_match(b"errnno: connection refused"));
    assert!(!pat.is_match(b"errno cnntion rfsed"));
}

#[test]
fn matches_returns_position_and_cost() {
    let pat = TreCompiledPattern::compile(b"hello", opts_with_errors(1)).expect("compile");
    let m = pat.matches(b"why hellp there").expect("match present");
    assert_eq!(m.cost, 1);
    assert_eq!(m.n_subst + m.n_del + m.n_ins, 1);
    // We do not yet capture submatches, so `start`/`end`
    // bracket the whole haystack rather than the match span.
    assert_eq!(m.start, 0);
    assert_eq!(m.end, b"why hellp there".len());
}

#[test]
fn dropped_pattern_is_freed() {
    // Pattern is small; we instantiate and drop without using
    // it. Valgrind / leak checkers should report no leaks. The
    // test exists so a future regression that forgets the
    // `Drop` impl on `TreCompiledPattern` (or the inner
    // `OwnedRegex`) shows up as a leak under the soak path.
    for _ in 0..1024 {
        let pat = TreCompiledPattern::compile(b"abc", opts_with_errors(0)).expect("compile");
        drop(pat);
    }
}

#[test]
fn case_insensitive_flag_is_honoured() {
    let opts = TreMatchOpts {
        case_insensitive: true,
        ..TreMatchOpts::default()
    };
    let pat = TreCompiledPattern::compile(b"hello", opts).expect("compile");
    assert!(pat.is_match(b"HELLO world"));
    assert!(pat.is_match(b"Hello there"));
}

#[test]
fn empty_haystack_does_not_match_nonempty_pattern() {
    let pat = TreCompiledPattern::compile(b"abc", opts_with_errors(0)).expect("compile");
    assert!(!pat.is_match(b""));
}

#[test]
fn zero_errors_rejects_one_typo() {
    let pat = TreCompiledPattern::compile(b"hello", opts_with_errors(0)).expect("compile");
    assert!(pat.is_match(b"hello world"));
    assert!(!pat.is_match(b"hellp world"));
}
