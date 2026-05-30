//! Build script for `dynvecdb`.
//!
//! `turbovec` (transitively, `ndarray`) requires `-lopenblas`
//! at link time on Linux/macOS. Standalone Rust toolchains do
//! not always have `openblas` on the linker search path, even
//! when the shared library is installed system-wide. To keep
//! the build reproducible without forcing every embedder to
//! configure their environment, this script:
//!
//! 1. honours `OPENBLAS_LIB_DIR` if set;
//! 2. otherwise probes a small set of well-known directories;
//! 3. falls back to the linker's default search path.
//!
//! Set `OPENBLAS_LIB_DIR` in CI when the system openblas lives
//! outside the probed list.
//!
//! No openblas symbols are referenced from `dynvecdb` itself;
//! the script only forwards the search path so the linker can
//! satisfy turbovec's transitive requirement.

use std::path::Path;

fn main() {
    println!("cargo:rerun-if-env-changed=OPENBLAS_LIB_DIR");
    println!("cargo:rerun-if-changed=build.rs");

    if let Ok(p) = std::env::var("OPENBLAS_LIB_DIR") {
        if !p.is_empty() {
            println!("cargo:rustc-link-search=native={p}");
            return;
        }
    }

    let candidates = [
        // Debian / Ubuntu multi-arch.
        "/usr/lib/x86_64-linux-gnu",
        "/usr/lib/aarch64-linux-gnu",
        // RHEL family / generic.
        "/usr/lib64",
        "/usr/lib",
        // Homebrew x86_64 / Apple silicon.
        "/usr/local/opt/openblas/lib",
        "/opt/homebrew/opt/openblas/lib",
    ];

    for dir in candidates {
        if has_openblas(dir) {
            println!("cargo:rustc-link-search=native={dir}");
            return;
        }
    }

    // Last-resort scan of the Nix store. A devshell that pins
    // `openblas` exposes it under /nix/store/*-openblas-*/lib;
    // walk the immediate children of /nix/store and pick the
    // first match. This keeps the dynvecdb build green inside
    // a Nix-managed dev environment without forcing the user
    // to set OPENBLAS_LIB_DIR by hand.
    if let Ok(entries) = std::fs::read_dir("/nix/store") {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(s) = name.to_str() else { continue };
            if !s.contains("-openblas-") {
                continue;
            }
            let lib = entry.path().join("lib");
            if has_openblas(&lib) {
                println!("cargo:rustc-link-search=native={}", lib.display());
                return;
            }
        }
    }

    // No probe hit. The link will fail with the original
    // diagnostic; surface a hint via cargo's warning channel
    // so the operator knows where to look.
    println!("cargo:warning=dynvecdb: no openblas found; set OPENBLAS_LIB_DIR if linking fails");
}

fn has_openblas(dir: impl AsRef<Path>) -> bool {
    let dir = dir.as_ref();
    if !dir.is_dir() {
        return false;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(s) = name.to_str() else { continue };
        if s.starts_with("libopenblas.") {
            return true;
        }
    }
    false
}
