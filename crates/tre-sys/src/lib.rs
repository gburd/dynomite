//! Low-level FFI bindings for the TRE approximate-regex C
//! library.
//!
//! This crate is intentionally thin. It exposes:
//!
//! * The opaque `regex_t` struct (matching TRE's layout).
//! * The `regaparams_t` and `regamatch_t` POD structs used by
//!   the approximate matcher.
//! * `extern "C"` declarations for the handful of TRE entry
//!   points the safe wrapper in `dyntext::tre` actually calls:
//!   compile, approximate-execute, free, and error formatting.
//!
//! Higher-level concerns (lifetimes, error types, `Drop`,
//! `Send`/`Sync` markers) live in `dyntext::tre`. This crate is
//! the only place in the workspace where `unsafe` is allowed;
//! see `docs/journal/2026-05-30-tre-sys-unsafe.md` for the
//! rationale.
//!
//! # Build
//!
//! `build.rs` first probes for a system-installed `libtre` via
//! `pkg-config` (looking for >= 0.8.0). If that fails, and the
//! `vendored` Cargo feature is enabled (default), the build
//! script compiles the bundled TRE 0.8.0 source under
//! `vendor/tre/` as a static library.
//!
//! # Safety boundary
//!
//! All `unsafe extern "C"` declarations are confined to the
//! private `raw` module. Callers must uphold:
//!
//! * `regex_t` is initialised by [`tre_regncomp`] before
//!   any other call uses it.
//! * `regex_t` is freed by [`tre_regfree`] exactly once.
//! * `regamatch_t::pmatch` either points to a writable buffer
//!   of `nmatch` `regmatch_t` slots or `nmatch` is zero.
//! * Strings passed to `tre_regncomp` and `tre_reganexec` are
//!   valid for the indicated length; embedded NULs are
//!   permitted.
//! * `regex_t` is not shared between threads. TRE keeps no
//!   mutable global state in the call paths we use, but the
//!   compiled TNFA owned by `regex_t` is not internally
//!   synchronised.

#![doc(html_root_url = "https://docs.rs/tre-sys/0.0.1")]

pub use raw::{
    regamatch_t, regaparams_t, regex_t, regmatch_t, regoff_t, tre_reganexec,
    tre_regaparams_default, tre_regerror, tre_regfree, tre_regncomp, REG_BADBR, REG_BADPAT,
    REG_BADRPT, REG_EBRACE, REG_EBRACK, REG_ECOLLATE, REG_ECTYPE, REG_EESCAPE, REG_EPAREN,
    REG_ERANGE, REG_ESPACE, REG_ESUBREG, REG_EXTENDED, REG_ICASE, REG_NEWLINE, REG_NOMATCH,
    REG_NOSUB, REG_OK,
};

#[allow(non_camel_case_types)]
mod raw {
    //! Raw `extern "C"` bindings to TRE.
    //!
    //! `regoff_t`, `regex_t`, `regmatch_t`, `regaparams_t`,
    //! and `regamatch_t` keep TRE's snake_case names so the
    //! ABI is obvious; the `non_camel_case_types` lint is
    //! relaxed at the module level for the same reason.
    //!
    //! # Safety
    //!
    //! Every item in this module is unsafe to use without
    //! careful regard for the contract documented in the
    //! crate-level docs of `tre-sys`. Specifically:
    //!
    //! * `regex_t` must be zero-initialised before
    //!   [`tre_regncomp`] is called and must not be moved or
    //!   copied after compilation succeeds (it owns a heap
    //!   pointer in `value`).
    //! * The pointer in `value` is allocated by TRE's internal
    //!   `xmalloc` and is freed by [`tre_regfree`]. Calling
    //!   [`tre_regfree`] twice or before a successful compile
    //!   is undefined behaviour.
    //! * The string buffers passed to [`tre_regncomp`] and
    //!   [`tre_reganexec`] are read-only; TRE does not retain
    //!   pointers into them after the call returns.
    //! * `regamatch_t::pmatch` must be a writable buffer of
    //!   `nmatch` slots or `nmatch` must be zero. TRE writes
    //!   into the slots on a successful match.
    //! * `regaparams_t` is passed by value; its fields are
    //!   POD `c_int`s. The wrapper layer normalises field
    //!   ranges (clamping `u16` -> `c_int`).
    //! * Multi-threaded use of a single `regex_t` is not
    //!   supported. The wrapper makes this explicit by holding
    //!   a `PhantomData<*const ()>` to drop `Send`/`Sync`.

    use libc::{c_char, c_int, size_t};

    /// Match offset type. TRE defines this as `int`; we mirror
    /// the typedef so callers can express `regmatch_t` field
    /// ranges naturally.
    pub type regoff_t = c_int;

    /// Compiled-pattern handle.
    ///
    /// `re_nsub` reports the number of parenthesised
    /// subexpressions; `value` is an opaque pointer to TRE's
    /// internal `tre_tnfa_t`. Both fields are written by
    /// [`tre_regncomp`] and consumed by the other entry
    /// points; callers must treat the struct as opaque.
    #[repr(C)]
    #[derive(Debug)]
    pub struct regex_t {
        /// Number of parenthesised subexpressions.
        pub re_nsub: size_t,
        /// Opaque pointer to TRE's compiled `tre_tnfa_t`.
        pub value: *mut libc::c_void,
    }

    /// Submatch byte range. `rm_so == -1` and `rm_eo == -1`
    /// indicate an unmatched group.
    #[repr(C)]
    #[derive(Debug, Clone, Copy)]
    pub struct regmatch_t {
        /// Byte offset of the start of the match.
        pub rm_so: regoff_t,
        /// Byte offset one past the end of the match.
        pub rm_eo: regoff_t,
    }

    /// Approximate-match parameter struct passed by value.
    ///
    /// The fields mirror TRE's `regaparams_t` exactly. Costs
    /// are non-negative integers; "unset / unlimited" is
    /// signalled by `c_int::MAX` for a max_* field, matching
    /// the convention installed by [`tre_regaparams_default`].
    #[repr(C)]
    #[derive(Debug, Clone, Copy)]
    pub struct regaparams_t {
        /// Cost of inserting one character.
        pub cost_ins: c_int,
        /// Cost of deleting one character.
        pub cost_del: c_int,
        /// Cost of substituting one character.
        pub cost_subst: c_int,
        /// Maximum total cost permitted for a match.
        pub max_cost: c_int,
        /// Maximum number of insertions in a match.
        pub max_ins: c_int,
        /// Maximum number of deletions in a match.
        pub max_del: c_int,
        /// Maximum number of substitutions in a match.
        pub max_subst: c_int,
        /// Maximum number of edits (any kind) in a match.
        pub max_err: c_int,
    }

    /// Approximate-match result struct.
    ///
    /// On entry, `nmatch` is the capacity of `pmatch` and
    /// `pmatch` points to writable storage (or is null when
    /// `nmatch` is zero). On a successful match, TRE fills
    /// the `pmatch` slots and writes the cost / per-edit
    /// counts.
    #[repr(C)]
    #[derive(Debug)]
    pub struct regamatch_t {
        /// Capacity of `pmatch` (slots, not bytes).
        pub nmatch: size_t,
        /// Submatch storage; may be null when `nmatch == 0`.
        pub pmatch: *mut regmatch_t,
        /// Total cost of the chosen match.
        pub cost: c_int,
        /// Number of inserts in the chosen match.
        pub num_ins: c_int,
        /// Number of deletes in the chosen match.
        pub num_del: c_int,
        /// Number of substitutes in the chosen match.
        pub num_subst: c_int,
    }

    /// Compile flag: use POSIX extended regex syntax.
    pub const REG_EXTENDED: c_int = 1;
    /// Compile flag: case-insensitive match.
    pub const REG_ICASE: c_int = REG_EXTENDED << 1;
    /// Compile flag: anchor `^` / `$` at newline boundaries.
    pub const REG_NEWLINE: c_int = REG_ICASE << 1;
    /// Compile flag: skip submatch reporting.
    pub const REG_NOSUB: c_int = REG_NEWLINE << 1;

    /// Match succeeded (or operation OK).
    pub const REG_OK: c_int = 0;
    /// No match found.
    pub const REG_NOMATCH: c_int = 1;
    /// Invalid regular expression.
    pub const REG_BADPAT: c_int = 2;
    /// Unknown collating element.
    pub const REG_ECOLLATE: c_int = 3;
    /// Unknown character class name.
    pub const REG_ECTYPE: c_int = 4;
    /// Trailing backslash in pattern.
    pub const REG_EESCAPE: c_int = 5;
    /// Invalid back-reference.
    pub const REG_ESUBREG: c_int = 6;
    /// Bracket-expression imbalance.
    pub const REG_EBRACK: c_int = 7;
    /// Parenthesis imbalance.
    pub const REG_EPAREN: c_int = 8;
    /// Brace imbalance.
    pub const REG_EBRACE: c_int = 9;
    /// Invalid content of `{}`.
    pub const REG_BADBR: c_int = 10;
    /// Invalid use of range operator.
    pub const REG_ERANGE: c_int = 11;
    /// Out of memory.
    pub const REG_ESPACE: c_int = 12;
    /// Invalid use of repetition operator.
    pub const REG_BADRPT: c_int = 13;

    // Symbol bindings to the linked TRE library. Names must
    // match the C ABI verbatim.
    extern "C" {
        /// Compile `regex` (a buffer of `n` bytes) into `preg`.
        /// Returns `REG_OK` on success, or one of the
        /// `REG_E*` codes.
        pub fn tre_regncomp(
            preg: *mut regex_t,
            regex: *const c_char,
            n: size_t,
            cflags: c_int,
        ) -> c_int;

        /// Run an approximate match against the first `len`
        /// bytes of `string`. The matcher is invoked through
        /// `tre_match_approx` which selects `STR_BYTE` when
        /// the C runtime's `MB_CUR_MAX == 1`. The vendored
        /// build disables `TRE_MULTIBYTE`, forcing this to be
        /// the case unconditionally; system-libtre links must
        /// run under a single-byte locale (e.g. `LC_ALL=C`).
        pub fn tre_reganexec(
            preg: *const regex_t,
            string: *const c_char,
            len: size_t,
            match_: *mut regamatch_t,
            params: regaparams_t,
            eflags: c_int,
        ) -> c_int;

        /// Initialise `params` with TRE's default cost weights
        /// (1 per insert/delete/substitute, no overall limit).
        pub fn tre_regaparams_default(params: *mut regaparams_t);

        /// Format `errcode` as a human-readable string.
        /// Returns the number of bytes that would be written
        /// (excluding the terminating NUL).
        pub fn tre_regerror(
            errcode: c_int,
            preg: *const regex_t,
            errbuf: *mut c_char,
            errbuf_size: size_t,
        ) -> size_t;

        /// Release the resources held by a successfully
        /// compiled `regex_t`.
        pub fn tre_regfree(preg: *mut regex_t);
    }
}

/// Convenience constructor for a freshly zeroed
/// [`regex_t`] suitable for handing to
/// [`tre_regncomp`].
///
/// Callers still need to call `tre_regncomp` before using the
/// returned struct; this helper just spares the boilerplate of
/// `MaybeUninit::zeroed`.
#[must_use]
pub fn empty_regex_t() -> regex_t {
    regex_t {
        re_nsub: 0,
        value: std::ptr::null_mut(),
    }
}

/// Convenience constructor for a [`regaparams_t`] populated by
/// [`tre_regaparams_default`].
#[must_use]
pub fn default_regaparams() -> regaparams_t {
    let mut params = regaparams_t {
        cost_ins: 0,
        cost_del: 0,
        cost_subst: 0,
        max_cost: 0,
        max_ins: 0,
        max_del: 0,
        max_subst: 0,
        max_err: 0,
    };
    // SAFETY: `params` is a fully-initialised, writable
    // `regaparams_t`. TRE's `tre_regaparams_default` only
    // writes into the eight `int` fields; it does not allocate
    // or read uninitialised memory.
    unsafe { tre_regaparams_default(std::ptr::addr_of_mut!(params)) };
    params
}

/// Outcome of a successful approximate match returned by
/// [`safe_reganexec`]. The submatch storage is intentionally
/// elided because dyntext does not yet need it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SafeMatch {
    /// Total cost of the match.
    pub cost: i32,
    /// Number of insertions.
    pub num_ins: i32,
    /// Number of deletions.
    pub num_del: i32,
    /// Number of substitutions.
    pub num_subst: i32,
}

/// Result of a `tre_reganexec` call as a Rust enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecOutcome {
    /// A match was found within the requested budget.
    Match(SafeMatch),
    /// No match within the budget.
    NoMatch,
    /// TRE returned a non-`REG_OK`, non-`REG_NOMATCH` code.
    Error(libc::c_int),
}

/// Safe wrapper around `tre_regncomp`.
///
/// `regex_t` is initialised by this call on success. On
/// failure the caller must NOT call [`safe_regfree`] on the
/// passed-in `regex_t` (TRE leaves it in an indeterminate
/// state); the returned `Err(code)` is the only legal
/// observation of the input.
///
/// The `regex` slice is read by TRE only during this call; no
/// pointer is retained.
pub fn safe_regncomp(
    preg: &mut regex_t,
    regex: &[u8],
    cflags: libc::c_int,
) -> Result<(), libc::c_int> {
    // SAFETY:
    // * `preg` is a writable, fully-initialised `regex_t`
    //   provided by the caller.
    // * `regex.as_ptr()` is valid for `regex.len()` bytes for
    //   the duration of the call.
    // * TRE does not retain pointers from `regex` after this
    //   call returns.
    let rc = unsafe {
        tre_regncomp(
            preg,
            regex.as_ptr().cast::<libc::c_char>(),
            regex.len(),
            cflags,
        )
    };
    if rc == REG_OK {
        Ok(())
    } else {
        Err(rc)
    }
}

/// Safe wrapper around `tre_reganexec`.
///
/// `preg` must point at a `regex_t` that was successfully
/// initialised by [`safe_regncomp`]. The text slice is read
/// during the call and not retained. No submatch storage is
/// requested; for that, drop down to the raw FFI directly.
pub fn safe_reganexec(
    preg: &regex_t,
    text: &[u8],
    params: regaparams_t,
    eflags: libc::c_int,
) -> ExecOutcome {
    let mut amatch = regamatch_t {
        nmatch: 0,
        pmatch: std::ptr::null_mut(),
        cost: 0,
        num_ins: 0,
        num_del: 0,
        num_subst: 0,
    };

    // SAFETY:
    // * `preg` is a successfully-compiled `regex_t` (the
    //   `&regex_t` reference is the function's contract; the
    //   compile invariant is maintained by the caller).
    // * `text.as_ptr()` is valid for `text.len()` bytes for
    //   the duration of the call.
    // * `amatch` is a fully-initialised `regamatch_t`. With
    //   `nmatch = 0` and `pmatch = null`, TRE writes only the
    //   `cost` / `num_*` fields; it does not dereference
    //   `pmatch`.
    // * `params` is passed by value (POD).
    let rc = unsafe {
        tre_reganexec(
            preg,
            text.as_ptr().cast::<libc::c_char>(),
            text.len(),
            std::ptr::addr_of_mut!(amatch),
            params,
            eflags,
        )
    };

    match rc {
        REG_OK => ExecOutcome::Match(SafeMatch {
            cost: amatch.cost,
            num_ins: amatch.num_ins,
            num_del: amatch.num_del,
            num_subst: amatch.num_subst,
        }),
        REG_NOMATCH => ExecOutcome::NoMatch,
        other => ExecOutcome::Error(other),
    }
}

/// Safe wrapper around `tre_regfree`.
///
/// `preg` must have been successfully initialised by
/// [`safe_regncomp`]. After this call, `preg` is left in an
/// indeterminate state; callers should immediately drop it.
pub fn safe_regfree(preg: &mut regex_t) {
    // SAFETY: `preg` is a successfully-compiled `regex_t`
    // (caller invariant). `tre_regfree` releases the heap
    // pointer in `preg.value` and zeroes it.
    unsafe { tre_regfree(preg) };
}

/// Safe wrapper around `tre_regerror`. Returns the message as
/// an owned `String` (UTF-8 lossy if TRE produces unusual
/// bytes).
#[must_use]
pub fn safe_regerror(rc: libc::c_int, preg: Option<&regex_t>) -> String {
    let mut buf = [0u8; 256];
    let preg_ptr = preg.map_or(std::ptr::null(), std::ptr::from_ref::<regex_t>);
    // SAFETY: `buf` is owned and writable for `buf.len()`
    // bytes. `preg_ptr` is either null (TRE handles that for
    // canned codes) or points at a valid `regex_t`.
    let written = unsafe {
        tre_regerror(
            rc,
            preg_ptr,
            buf.as_mut_ptr().cast::<libc::c_char>(),
            buf.len(),
        )
    };
    if written == 0 {
        return format!("tre error code {rc}");
    }
    let nul = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..nul]).into_owned()
}

/// Allocate a freshly compiled pattern owned by Rust.
///
/// On success, the returned [`OwnedRegex`] frees the TRE
/// resources on drop. The flag word is forwarded to
/// [`safe_regncomp`].
pub fn compile(pattern: &[u8], cflags: libc::c_int) -> Result<OwnedRegex, libc::c_int> {
    let mut raw = empty_regex_t();
    safe_regncomp(&mut raw, pattern, cflags)?;
    Ok(OwnedRegex { raw })
}

/// RAII handle for a compiled TRE regex.
///
/// Construct via [`compile`]. The native `regex_t` is freed
/// on drop via [`safe_regfree`].
pub struct OwnedRegex {
    raw: regex_t,
}

impl OwnedRegex {
    /// Borrow the underlying [`regex_t`] for use with
    /// [`safe_reganexec`] or [`safe_regerror`].
    #[must_use]
    pub fn as_raw(&self) -> &regex_t {
        &self.raw
    }

    /// Number of parenthesised subexpressions in the
    /// compiled pattern.
    #[must_use]
    pub fn nsub(&self) -> usize {
        self.raw.re_nsub
    }
}

impl std::fmt::Debug for OwnedRegex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedRegex")
            .field("re_nsub", &self.raw.re_nsub)
            .finish_non_exhaustive()
    }
}

impl Drop for OwnedRegex {
    fn drop(&mut self) {
        safe_regfree(&mut self.raw);
    }
}
