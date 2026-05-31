//! Build script for `tre-sys`.
//!
//! Tries to link a system-installed `libtre` first via
//! `pkg-config`. If that fails (and the `vendored` Cargo
//! feature is enabled), compiles the bundled TRE source under
//! `vendor/tre/` into a static library via the `cc` crate.
//!
//! The fallback build is byte-mode only: `TRE_WCHAR` and
//! `TRE_MULTIBYTE` are off, `TRE_APPROX` is on. The compiled
//! library exposes the standard `tre_*` symbols that
//! `src/lib.rs` declares as `extern "C"`.

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=config/tre-config.h");
    println!("cargo:rerun-if-changed=config/config.h");
    println!("cargo:rerun-if-env-changed=TRE_NO_PKG_CONFIG");

    if env::var_os("TRE_NO_PKG_CONFIG").is_none() && try_pkg_config() {
        return;
    }

    if cfg!(feature = "vendored") {
        build_vendored();
    } else {
        // No system libtre and no vendored fallback. The crate
        // still compiles its Rust code; calls into the FFI
        // layer will fail at link time, which is the documented
        // failure mode.
        println!(
            "cargo:warning=tre-sys: neither pkg-config nor the `vendored` feature found a TRE \
             library to link against. Enable the `vendored` feature or install libtre-dev."
        );
    }
}

/// Attempt to locate a system-installed `libtre` via
/// `pkg-config`. Returns `true` on success; the `pkg-config`
/// crate emits the appropriate `cargo:rustc-link-*` directives
/// itself.
fn try_pkg_config() -> bool {
    pkg_config::Config::new()
        .atleast_version("0.8.0")
        .probe("tre")
        .is_ok()
}

/// Compile the vendored TRE source as a static library and
/// link it into the consuming crate.
fn build_vendored() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo"));
    let vendor = manifest_dir.join("vendor").join("tre");
    let lib_dir = vendor.join("lib");
    let config_dir = manifest_dir.join("config");

    assert!(
        vendor.join("lib").join("regcomp.c").exists(),
        "tre-sys: vendored TRE source not found at {}. The git submodule is missing; run \
         `git submodule update --init crates/tre-sys/vendor/tre`.",
        vendor.display()
    );

    let sources = [
        "lib/regcomp.c",
        "lib/regexec.c",
        "lib/regerror.c",
        "lib/tre-ast.c",
        "lib/tre-compile.c",
        "lib/tre-filter.c",
        "lib/tre-match-approx.c",
        "lib/tre-match-backtrack.c",
        "lib/tre-match-parallel.c",
        "lib/tre-mem.c",
        "lib/tre-parse.c",
        "lib/tre-stack.c",
        "lib/xmalloc.c",
    ];

    let mut build = cc::Build::new();
    build
        .include(&config_dir)
        .include(&lib_dir)
        .define("HAVE_CONFIG_H", None)
        // Silence the C source's signed/unsigned and ternary
        // warnings; TRE is mature upstream code and we treat it
        // as a third-party blob.
        .warnings(false)
        .extra_warnings(false);

    if !cfg!(target_env = "msvc") {
        build
            .flag_if_supported("-Wno-unused-parameter")
            .flag_if_supported("-Wno-unused-variable")
            .flag_if_supported("-Wno-unused-but-set-variable")
            .flag_if_supported("-Wno-sign-compare")
            .flag_if_supported("-Wno-implicit-fallthrough")
            .flag_if_supported("-fvisibility=hidden");
    }

    for src in sources {
        let path = vendor.join(src);
        assert!(
            path.exists(),
            "tre-sys: missing vendored source {}",
            path.display()
        );
        build.file(path);
    }

    build.compile("tre");
    println!("cargo:rustc-link-lib=static=tre");
}
