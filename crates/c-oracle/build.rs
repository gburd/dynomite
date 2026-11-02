//! Build script for `c-oracle`.
//!
//! Compiles the C Dynomite reference sources named in [`UNITS`] from
//! `$DYNOMITE_C_REF` (default `~/src/dynomite`) into a static library the
//! oracle tests link against. The C tree must be configured: the build
//! reads `config.h` from `target/cref/build` (written by
//! `scripts/build_cref.sh`) or from `$DYNOMITE_C_REF` itself.
//!
//! When no configured C tree is found the build emits the
//! `c_oracle_absent` cfg and compiles no C. The oracle tests are gated on
//! `not(c_oracle_absent)`, so hosts without the C reference still build the
//! workspace, and CI hosts that have it run the oracles as a hard gate.

use std::env;
use std::path::{Path, PathBuf};

/// C translation units compiled into the oracle library, relative to the
/// C tree root. Pure-logic units only: no event loop, no sockets in use.
const UNITS: &[&str] = &[
    "src/dyn_util.c",
    "src/dyn_log.c",
    "src/dyn_string.c",
    "src/dyn_array.c",
    "src/hashkit/dyn_crc16.c",
    "src/hashkit/dyn_crc32.c",
    "src/hashkit/dyn_fnv.c",
    "src/hashkit/dyn_hsieh.c",
    "src/hashkit/dyn_jenkins.c",
    "src/hashkit/dyn_md5.c",
    "src/hashkit/dyn_murmur.c",
    "src/hashkit/dyn_murmur3.c",
    "src/hashkit/dyn_one_at_a_time.c",
    "src/hashkit/dyn_token.c",
    "src/dyn_vnode.c",
    "contrib/murmur3/murmur3.c",
];

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=DYNOMITE_C_REF");
    println!("cargo::rustc-check-cfg=cfg(c_oracle_absent)");

    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let workspace = manifest
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf();

    let Some((src_root, config_dir)) = locate_c_tree(&workspace) else {
        println!(
            "cargo:warning=c-oracle: no configured C Dynomite tree found; oracle tests are skipped. \
             Set DYNOMITE_C_REF and run scripts/build_cref.sh."
        );
        println!("cargo:rustc-cfg=c_oracle_absent");
        return;
    };

    let mut build = cc::Build::new();
    build
        .include(&config_dir)
        .include(src_root.join("src"))
        .include(src_root.join("src/hashkit"))
        .include(src_root.join("contrib/murmur3"))
        .define("HAVE_CONFIG_H", None)
        .define("_GNU_SOURCE", None)
        // The C tree predates modern warning promotion and relies on
        // common-symbol semantics; match the flags scripts/build_cref.sh
        // uses so the oracle is the same code the reference binary runs.
        .flag_if_supported("-fcommon")
        .flag_if_supported("-Wno-int-conversion")
        .flag_if_supported("-Wno-incompatible-pointer-types")
        .flag_if_supported("-Wno-implicit-function-declaration")
        .warnings(false)
        .opt_level(2);
    // The C reference defines its own `MD5_Init`, `MD5_Update`, and
    // `MD5_Final` (src/hashkit/dyn_md5.c), which collide with OpenSSL's
    // libcrypto when a workspace build links both. Rename the C copies to
    // private symbols; only dyn_md5.c defines or calls them.
    for sym in ["MD5_Init", "MD5_Update", "MD5_Final"] {
        build.define(sym, Some(format!("c_oracle_{sym}").as_str()));
    }
    for unit in UNITS {
        let path = src_root.join(unit);
        println!("cargo:rerun-if-changed={}", path.display());
        build.file(path);
    }
    // Link-only stand-ins for symbols the compiled units reference but the
    // oracled functions never call (see csrc/link_stubs.c).
    let stubs = manifest.join("csrc/link_stubs.c");
    println!("cargo:rerun-if-changed={}", stubs.display());
    build.file(stubs);
    build.compile("dynomite_c_oracle");
    println!("cargo:rustc-link-lib=m");
}

/// Returns `(source root, directory holding the generated config.h)`.
fn locate_c_tree(workspace: &Path) -> Option<(PathBuf, PathBuf)> {
    let src_root = env::var_os("DYNOMITE_C_REF")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|h| PathBuf::from(h).join("src/dynomite")))?;
    if !src_root.join("src/dyn_core.h").is_file() {
        return None;
    }
    // Prefer the configured mirror build_cref.sh writes; fall back to a
    // tree that was configured in place.
    let mirror = workspace.join("target/cref/build");
    for dir in [mirror, src_root.clone()] {
        if dir.join("config.h").is_file() {
            return Some((src_root, dir));
        }
    }
    None
}
