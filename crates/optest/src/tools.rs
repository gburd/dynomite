//! External tool discovery.

use std::path::{Path, PathBuf};

/// Paths to every external program and preload library the harness can
/// use. `None` means the host lacks it; scenarios that need it skip.
#[derive(Clone, Debug, Default)]
pub struct Tools {
    /// `unshare` (util-linux).
    pub unshare: Option<PathBuf>,
    /// `tc` (iproute2).
    pub tc: Option<PathBuf>,
    /// `ip` (iproute2).
    pub ip: Option<PathBuf>,
    /// `valkey-server` or `redis-server`.
    pub redis_server: Option<PathBuf>,
    /// `memcached`.
    pub memcached: Option<PathBuf>,
    /// The `dynomited` binary under test.
    pub dynomited: Option<PathBuf>,
    /// The C reference `dynomite` binary (differential runs).
    pub c_dynomite: Option<PathBuf>,
    /// libfiu `fiu_run_preload.so`.
    pub fiu_run_preload: Option<PathBuf>,
    /// libfiu `fiu_posix_preload.so`.
    pub fiu_posix_preload: Option<PathBuf>,
    /// libumem `libumem_malloc.so`.
    pub umem_malloc: Option<PathBuf>,
    /// libdirectio `libdirectio.so.*`.
    pub directio: Option<PathBuf>,
    /// FAINT `faint` driver.
    pub faint: Option<PathBuf>,
    /// libfaketime `libfaketime.so.1`.
    pub faketime: Option<PathBuf>,
}

impl Tools {
    /// Discover tools from `PATH`, the `OPTEST_FAULT_PREFIX` install prefix
    /// (default `$HOME/fault/prefix`, what `scripts/ec2-dist/chef-testhost.sh`
    /// builds), and explicit overrides:
    ///
    /// * `OPTEST_DYNOMITED` - the binary under test (else
    ///   `CARGO_BIN_EXE_dynomited`-style lookup next to the test binary,
    ///   else `PATH`).
    /// * `OPTEST_C_DYNOMITE` - the C reference binary (else
    ///   `target/cref/build/src/dynomite`).
    ///
    /// # Examples
    ///
    /// ```
    /// let t = optest::Tools::discover();
    /// // Discovery never fails; it only reports what is present.
    /// let _ = t.tc.is_some();
    /// ```
    pub fn discover() -> Self {
        let prefix = std::env::var_os("OPTEST_FAULT_PREFIX")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join("fault/prefix")));
        let lib = |name: &str| {
            prefix
                .as_ref()
                .map(|p| p.join("lib").join(name))
                .filter(|p| p.is_file())
        };
        let lib_glob = |stem: &str| {
            let dir = prefix.as_ref()?.join("lib");
            let mut hits: Vec<PathBuf> = std::fs::read_dir(&dir)
                .ok()?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with(stem))
                })
                .collect();
            hits.sort();
            hits.pop()
        };
        Self {
            unshare: which("unshare"),
            tc: which("tc"),
            ip: which("ip"),
            redis_server: which("valkey-server").or_else(|| which("redis-server")),
            memcached: which("memcached"),
            dynomited: env_path("OPTEST_DYNOMITED")
                .or_else(sibling_dynomited)
                .or_else(|| which("dynomited")),
            c_dynomite: env_path("OPTEST_C_DYNOMITE").or_else(cref_binary),
            fiu_run_preload: lib("fiu_run_preload.so"),
            fiu_posix_preload: lib("fiu_posix_preload.so"),
            umem_malloc: lib("libumem_malloc.so"),
            directio: lib_glob("libdirectio.so"),
            faint: prefix
                .as_ref()
                .map(|p| p.join("bin/faint"))
                .filter(|p| p.is_file())
                .or_else(|| which("faint")),
            faketime: find_faketime(),
        }
    }

    /// True when `OPTEST_REQUIRE=1`: a missing tool is a failure, not a skip.
    pub fn required() -> bool {
        std::env::var("OPTEST_REQUIRE").is_ok_and(|v| v == "1")
    }
}

/// Look `name` up on `PATH`, then in the sbin directories Debian keeps
/// `tc` and `ip` in for non-root users.
pub fn which(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path)
        .chain(["/usr/sbin", "/sbin", "/usr/local/sbin"].map(PathBuf::from))
        .map(|d| d.join(name))
        .find(|p| is_executable(p))
}

fn is_executable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    p.metadata()
        .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

fn env_path(var: &str) -> Option<PathBuf> {
    std::env::var_os(var)
        .map(PathBuf::from)
        .filter(|p| p.is_file())
}

/// `target/<profile>/dynomited` next to the running test binary, which
/// cargo places in `target/<profile>/deps/`.
fn sibling_dynomited() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let profile_dir = exe.parent()?.parent()?;
    Some(profile_dir.join("dynomited")).filter(|p| is_executable(p))
}

fn cref_binary() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    // target/<profile>/deps/<test> -> target/cref/build/src/dynomite
    let target = exe.parent()?.parent()?.parent()?;
    Some(target.join("cref/build/src/dynomite")).filter(|p| is_executable(p))
}

fn find_faketime() -> Option<PathBuf> {
    if let Some(p) = env_path("OPTEST_FAKETIME_LIB") {
        return Some(p);
    }
    let bin = which("faketime")?;
    let root = bin.parent()?.parent()?;
    [
        root.join("lib/faketime/libfaketime.so.1"),
        root.join("lib/x86_64-linux-gnu/faketime/libfaketime.so.1"),
        PathBuf::from("/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1"),
    ]
    .into_iter()
    .find(|p| p.is_file())
}
