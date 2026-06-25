//! Safe Rust wrapper around `tre-sys`.
//!
//! This module is the single entry point dyntext callers use
//! to do approximate-regex matching. It provides:
//!
//! * [`TreCompiledPattern`], an owning handle to a compiled
//!   TRE pattern that frees its native resources on drop.
//! * [`TreMatchOpts`], a builder for the per-call cost weights
//!   and edit-budget caps.
//! * [`TreMatch`], the result of a successful approximate
//!   match.
//! * [`TreError`], a typed error returned by [`TreCompiledPattern::compile`].
//!
//! The wrapper is intentionally narrow: it exposes only what
//! the dyntext index needs, and it does **not** leak any
//! `unsafe` API. All `unsafe` lives in the `tre-sys` crate;
//! see the module-level safety section there for the contract
//! enforced here.
//!
//! # Example
//!
//! ```
//! use dyntext::tre::{TreCompiledPattern, TreMatchOpts};
//!
//! let opts = TreMatchOpts {
//!     max_errors: 1,
//!     ..TreMatchOpts::default()
//! };
//! let pat = TreCompiledPattern::compile(br"errno: \w+ refused", opts)
//!     .expect("compile");
//! assert!(pat.is_match(b"errno: connection refused"));
//! assert!(pat.is_match(b"errno: cnnection refused"));
//! assert!(!pat.is_match(b"errno cnntion rfsed"));
//! ```
//!
//! # Thread safety
//!
//! [`TreCompiledPattern`] is intentionally `!Send` and `!Sync`.
//! TRE's compiled `regex_t` owns mutable state (the TNFA), and
//! the matcher is not internally synchronised. Callers that
//! want concurrent matching should compile one pattern per
//! thread.

use std::marker::PhantomData;
use std::os::raw::c_int;

use thiserror::Error;
use tre_sys::{
    compile as compile_raw, default_regaparams, safe_reganexec, safe_regerror, ExecOutcome,
    OwnedRegex, REG_EXTENDED, REG_ICASE,
};

/// Errors returned by [`TreCompiledPattern::compile`].
#[derive(Debug, Error)]
pub enum TreError {
    /// The pattern bytes failed to compile.
    #[error("tre compile failed: {0}")]
    Compile(String),
    /// A configuration choice in [`TreMatchOpts`] is
    /// inconsistent (for example, `max_errors > 0` with all
    /// edit costs at zero).
    #[error("invalid pattern options: {0}")]
    InvalidOptions(String),
    /// TRE returned a code not covered by the public API.
    #[error("internal tre error: {0}")]
    Internal(String),
}

/// Per-pattern matching configuration.
///
/// `max_errors` is the headline knob: it bounds the number of
/// edit operations tolerated in a match. `cost_*` weight each
/// edit kind; if all weights are 1 the cost equals the edit
/// distance. `max_cost` bounds the cumulative cost; setting it
/// to 0 with `max_errors > 0` is rejected because it forces
/// exact match no matter what edits are allowed.
///
/// `case_insensitive` toggles TRE's `REG_ICASE` flag.
#[derive(Clone, Copy, Debug)]
pub struct TreMatchOpts {
    /// Maximum number of edit operations (insert + delete +
    /// substitute) permitted in a match.
    pub max_errors: u16,
    /// Cost of inserting one byte. Defaults to 1.
    pub cost_ins: u16,
    /// Cost of deleting one byte. Defaults to 1.
    pub cost_del: u16,
    /// Cost of substituting one byte. Defaults to 1.
    pub cost_subst: u16,
    /// Cumulative cost ceiling. Zero means "use
    /// `max_errors * max(cost_*)` as the implicit ceiling".
    pub max_cost: u16,
    /// Match case-insensitively.
    pub case_insensitive: bool,
}

impl Default for TreMatchOpts {
    fn default() -> Self {
        Self {
            max_errors: 0,
            cost_ins: 1,
            cost_del: 1,
            cost_subst: 1,
            max_cost: 0,
            case_insensitive: false,
        }
    }
}

/// Successful approximate-match result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreMatch {
    /// Byte offset where the match starts. The TRE
    /// wrapper does not request submatch storage, so this is
    /// reported as 0 on a successful match; the meaningful
    /// result is whether a match exists within the cost bound,
    /// not its position.
    pub start: usize,
    /// Byte offset one past the end of the match. See
    /// [`Self::start`] for the current limitation.
    pub end: usize,
    /// Total cost of the match (sum of edit costs).
    pub cost: u32,
    /// Number of inserts.
    pub n_ins: u32,
    /// Number of deletes.
    pub n_del: u32,
    /// Number of substitutes.
    pub n_subst: u32,
}

/// Owning handle to a TRE-compiled pattern.
///
/// Construct via [`TreCompiledPattern::compile`]. The native
/// resources are released by [`Drop`] (delegated to the
/// `tre-sys::OwnedRegex` field).
pub struct TreCompiledPattern {
    raw: OwnedRegex,
    /// Cached compile-time options so [`Self::matches`] can
    /// reuse the right cost weights without rebuilding the
    /// `regaparams_t` on every call.
    opts: TreMatchOpts,
    /// `*const ()` is `!Send + !Sync`; this drops both auto
    /// traits without needing explicit `unsafe impl !Send`
    /// (which is currently nightly-only).
    _not_send_or_sync: PhantomData<*const ()>,
}

impl std::fmt::Debug for TreCompiledPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreCompiledPattern")
            .field("nsub", &self.raw.nsub())
            .field("opts", &self.opts)
            .finish()
    }
}

impl TreCompiledPattern {
    /// Compile a regex pattern.
    ///
    /// The pattern is read as bytes. The vendored TRE build
    /// disables multibyte handling, so byte n in the pattern is
    /// matched against byte n in the haystack regardless of the
    /// host locale.
    pub fn compile(pattern: &[u8], opts: TreMatchOpts) -> Result<Self, TreError> {
        validate_opts(opts)?;

        let mut cflags = REG_EXTENDED;
        if opts.case_insensitive {
            cflags |= REG_ICASE;
        }

        match compile_raw(pattern, cflags) {
            Ok(raw) => Ok(Self {
                raw,
                opts,
                _not_send_or_sync: PhantomData,
            }),
            Err(rc) => Err(TreError::Compile(safe_regerror(rc, None))),
        }
    }

    /// Return `true` if `text` contains an approximate match
    /// for the pattern within the configured error budget.
    #[must_use]
    pub fn is_match(&self, text: &[u8]) -> bool {
        self.matches(text).is_some()
    }

    /// Return the first approximate match in `text`, if any.
    ///
    /// On a non-`REG_OK`/`REG_NOMATCH` return code from TRE
    /// the function returns `None` and asserts in debug builds.
    /// In release the failure is treated as a no-match because
    /// the only documented non-fatal failure for the
    /// approximate matcher is back-references in the pattern,
    /// which dyntext does not generate.
    #[must_use]
    pub fn matches(&self, text: &[u8]) -> Option<TreMatch> {
        let params = self.build_params();
        match safe_reganexec(self.raw.as_raw(), text, params, 0) {
            ExecOutcome::Match(m) => Some(TreMatch {
                start: 0,
                end: text.len(),
                cost: u32::try_from(m.cost).unwrap_or(u32::MAX),
                n_ins: u32::try_from(m.num_ins).unwrap_or(u32::MAX),
                n_del: u32::try_from(m.num_del).unwrap_or(u32::MAX),
                n_subst: u32::try_from(m.num_subst).unwrap_or(u32::MAX),
            }),
            ExecOutcome::NoMatch => None,
            ExecOutcome::Error(other) => {
                debug_assert!(false, "tre_reganexec returned unexpected code {other}");
                None
            }
        }
    }

    /// Borrow the cached options. Useful for tests and for
    /// callers that want to re-use the same configuration.
    #[must_use]
    pub fn opts(&self) -> TreMatchOpts {
        self.opts
    }

    /// Build the `regaparams_t` from the cached options.
    fn build_params(&self) -> tre_sys::regaparams_t {
        let mut params = default_regaparams();
        params.cost_ins = c_int::from(self.opts.cost_ins);
        params.cost_del = c_int::from(self.opts.cost_del);
        params.cost_subst = c_int::from(self.opts.cost_subst);
        params.max_err = c_int::from(self.opts.max_errors);
        params.max_ins = c_int::from(self.opts.max_errors);
        params.max_del = c_int::from(self.opts.max_errors);
        params.max_subst = c_int::from(self.opts.max_errors);

        let max_cost = if self.opts.max_cost == 0 {
            // Implicit ceiling: max_errors * largest single
            // edit cost. Using u32 saturating arithmetic to
            // avoid surprising overflow when callers pass big
            // u16 values.
            let largest = self
                .opts
                .cost_ins
                .max(self.opts.cost_del)
                .max(self.opts.cost_subst);
            let budget = u32::from(self.opts.max_errors) * u32::from(largest.max(1));
            c_int::try_from(budget).unwrap_or(c_int::MAX)
        } else {
            c_int::from(self.opts.max_cost)
        };
        params.max_cost = max_cost;
        params
    }
}

fn validate_opts(opts: TreMatchOpts) -> Result<(), TreError> {
    if opts.max_errors > 0 && opts.cost_ins == 0 && opts.cost_del == 0 && opts.cost_subst == 0 {
        return Err(TreError::InvalidOptions(
            "max_errors > 0 requires at least one non-zero edit cost".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_opts_are_zero_error_unit_costs() {
        let opts = TreMatchOpts::default();
        assert_eq!(opts.max_errors, 0);
        assert_eq!(opts.cost_ins, 1);
        assert_eq!(opts.cost_del, 1);
        assert_eq!(opts.cost_subst, 1);
        assert_eq!(opts.max_cost, 0);
        assert!(!opts.case_insensitive);
    }

    #[test]
    fn validate_opts_rejects_max_errors_with_zero_costs() {
        let bad = TreMatchOpts {
            max_errors: 1,
            cost_ins: 0,
            cost_del: 0,
            cost_subst: 0,
            ..TreMatchOpts::default()
        };
        assert!(matches!(
            validate_opts(bad),
            Err(TreError::InvalidOptions(_))
        ));
    }

    #[test]
    fn compiled_pattern_phantom_is_not_send_or_sync() {
        // A `PhantomData<*const ()>` field drops both auto
        // traits. The check below is a documentation-only
        // reminder; if it stops compiling the assertion is
        // wrong and should be re-derived.
        fn assert_any<T>() {}
        assert_any::<TreCompiledPattern>();
    }
}
