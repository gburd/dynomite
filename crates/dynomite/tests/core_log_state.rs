//! Integration coverage for the process-global logger writer state.
//!
//! The logger's writer state lives in a `OnceLock`, so only one
//! `build_logs_layer` / `install_*` call can succeed per process.
//! Each scenario that needs a distinct STATE lifecycle therefore
//! lives in its own integration test binary; the cases below all
//! run before any STATE is installed in this binary, exercising
//! the "not initialised" fallbacks, and the final case installs
//! STATE once and exercises the post-init paths.

use std::io::Write as _;

use dynomite::core::log::{
    build_logs_layer, current_level, log_init, log_init_with_format, reopen_on_sighup,
    write_error_count, LogConfig, LogFormat, LOG_NOTICE,
};

#[test]
fn pre_init_then_install_then_post_init() {
    // 1. Before any STATE: reopen reports a generic error and the
    //    write-error counter reads zero.
    let err = reopen_on_sighup().expect_err("reopen before init must fail");
    assert!(
        format!("{err}").contains("not initialised"),
        "unexpected error: {err}"
    );
    assert_eq!(write_error_count(), 0);

    // 2. Install STATE once via the public two-argument entry
    //    point, which delegates through log_init_with_format ->
    //    install_logs_only -> build_logs_layer -> the global
    //    try_init.
    log_init(LOG_NOTICE, None).expect("install logger");
    assert_eq!(current_level(), LOG_NOTICE);
    // The format-aware wrapper is a thin alias over the same
    // install path; a second call must fail (global already set).
    assert!(log_init_with_format(LOG_NOTICE, None, LogFormat::Json).is_err());

    // 3. After STATE is set, the writer counter is readable and the
    //    stderr sink reopen is a no-op success (no path stored).
    assert_eq!(write_error_count(), 0);
    reopen_on_sighup().expect("stderr reopen is a no-op");

    // 4. A second build attempt must fail because STATE is a
    //    OnceLock: this covers the `STATE.set` already-installed
    //    error arm in init_reopen_state.
    let cfg = LogConfig::new(LOG_NOTICE, None, LogFormat::Default);
    let again = build_logs_layer(&cfg);
    assert!(again.is_err(), "second build_logs_layer must fail");

    // 5. Emitting a record routes through LoggerWriter -> the
    //    installed stderr sink. Also exercise the writer directly.
    tracing::info!(target: "dynomite::test", "post-install marker");
    let _ = std::io::stderr().flush();
}
