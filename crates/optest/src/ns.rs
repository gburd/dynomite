//! Per-test sandbox: a private user, network, PID, and mount namespace.
//!
//! [`in_sandbox`] re-executes the running test binary under
//! `unshare --user --map-root-user --net --mount --pid --fork --mount-proc`,
//! filtered to the one test that called it. Inside, the process is root
//! of namespaces that hold nothing but a loopback device and a private
//! `/run`, so `ip netns`, bridges, veth pairs, and `tc` all work without
//! host privileges. The cluster layer then gives every node its own
//! network namespace on a bridge (see [`crate::cluster`]). When the test
//! exits the kernel tears everything down: namespaces, qdiscs, sockets,
//! and every child process. A panicking test cannot leak a node or a
//! netem rule onto the host.

use std::process::Command;

use crate::tools::Tools;

/// Environment variable that marks the re-executed child.
const SANDBOX_ENV: &str = "OPTEST_IN_SANDBOX";

/// Outcome of [`in_sandbox`] for the calling test.
#[derive(Debug, PartialEq, Eq)]
pub enum Sandbox {
    /// This process is the sandboxed child: run the scenario body.
    Inside,
    /// This process is the parent; the child ran and passed.
    Passed,
    /// Sandboxing is unavailable here and `OPTEST_REQUIRE` is unset.
    Skipped(String),
}

/// Run the calling test inside a fresh namespace sandbox.
///
/// Call it first in a test, with the test's full path (`module::name`,
/// as `cargo test` prints it):
///
/// ```no_run
/// #[test]
/// fn partition_heals() {
///     match optest::ns::in_sandbox("partition_heals") {
///         optest::ns::Sandbox::Inside => { /* scenario body */ }
///         optest::ns::Sandbox::Passed => return,
///         optest::ns::Sandbox::Skipped(why) => { eprintln!("skip: {why}"); return; }
///     }
/// }
/// ```
///
/// In the parent this spawns the child, waits, and panics (failing the
/// test) if the child failed, forwarding its output. In the child it sets
/// up the loopback device and returns [`Sandbox::Inside`].
///
/// # Panics
///
/// In the parent, when the sandboxed child fails, or when sandboxing is
/// unavailable and `OPTEST_REQUIRE=1`.
pub fn in_sandbox(test_name: &str) -> Sandbox {
    if std::env::var_os(SANDBOX_ENV).is_some() {
        bring_up_loopback();
        return Sandbox::Inside;
    }
    let tools = Tools::discover();
    let (Some(unshare), Some(_ip)) = (tools.unshare.as_ref(), tools.ip.as_ref()) else {
        return skip_or_fail("unshare or ip not found".into());
    };
    if !probe_userns(unshare) {
        return skip_or_fail("unprivileged user+net namespaces are disabled".into());
    }
    let exe = std::env::current_exe().expect("current test executable");
    let out = Command::new(unshare)
        .args([
            "--user",
            "--map-root-user",
            "--net",
            "--mount",
            "--pid",
            "--fork",
            "--mount-proc",
            "--kill-child",
        ])
        .arg(&exe)
        .args([test_name, "--exact", "--nocapture", "--test-threads=1"])
        .env(SANDBOX_ENV, "1")
        .output()
        .expect("spawn sandboxed test");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    // The child is a libtest run filtered to exactly this test: it must
    // report one passed test, or the filter did not match.
    let ran = stdout.contains("1 passed") || stdout.contains("test result: ok. 1 passed");
    assert!(
        out.status.success() && ran,
        "sandboxed scenario `{test_name}` failed ({}):\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        out.status
    );
    if std::env::var_os("OPTEST_VERBOSE").is_some() {
        eprintln!("{stdout}{stderr}");
    }
    Sandbox::Passed
}

fn skip_or_fail(why: String) -> Sandbox {
    assert!(!Tools::required(), "OPTEST_REQUIRE=1 but {why}");
    Sandbox::Skipped(why)
}

fn probe_userns(unshare: &std::path::Path) -> bool {
    Command::new(unshare)
        .args(["--user", "--map-root-user", "--net", "true"])
        .status()
        .is_ok_and(|s| s.success())
}

fn bring_up_loopback() {
    let ip = crate::tools::which("ip").expect("ip inside sandbox");
    let ok = Command::new(&ip)
        .args(["link", "set", "lo", "up"])
        .status()
        .is_ok_and(|s| s.success());
    assert!(ok, "could not bring up loopback in the sandbox");
    // `ip netns add` bind-mounts namespace handles under /run/netns; give
    // the sandbox its own /run so that needs no host privilege.
    let mounted = Command::new("mount")
        .args(["-t", "tmpfs", "tmpfs", "/run"])
        .status()
        .is_ok_and(|s| s.success());
    assert!(mounted, "could not mount a private /run in the sandbox");
    std::fs::create_dir_all("/run/netns").expect("create /run/netns");
}
