//! Deterministic allocation-failure sweep with FAINT.
//!
//! FAINT profiles every `malloc`/`calloc`/`realloc` call site a run
//! reaches, then reruns the program once per site with exactly that
//! allocation failing. The run here is `-t -c <conf>` (parse and validate
//! a configuration), the one code path that is identical across engines
//! and needs no network. The property: a failed allocation ends the
//! process with an orderly signal or status (C: whatever it does;
//! Rust: `SIGABRT` from the allocation-error handler), never a hang, and
//! for the Rust engine never `SIGSEGV`: memory safety must hold even when
//! the allocator fails.

use std::process::Command;

fn faint_or_skip() -> Option<(std::path::PathBuf, optest::Tools)> {
    let t = optest::Tools::discover();
    if let Some(f) = t.faint.clone() {
        return Some((f, t));
    }
    assert!(
        !optest::Tools::required(),
        "OPTEST_REQUIRE=1 but FAINT is missing"
    );
    eprintln!("[optest] skip: FAINT not found");
    None
}

/// Run FAINT over `bin -t -c conf` in `dir`; returns its summary lines.
fn sweep(faint: &std::path::Path, bin: &std::path::Path, dir: &std::path::Path) -> String {
    let conf = dir.join("c.yml");
    std::fs::write(
        &conf,
        "dyn_o_mite:\n  listen: 127.0.0.1:19102\n  dyn_listen: 127.0.0.1:19101\n  \
         stats_listen: 127.0.0.1:19222\n  tokens: '0'\n  servers:\n  - 127.0.0.1:19379:1\n  \
         data_store: 0\n",
    )
    .expect("write conf");
    // Each injected failure aborts the Rust engine; without `ulimit -c 0`
    // every abort writes a core of the (large, debug) binary and the sweep
    // spends its time in the kernel.
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg("ulimit -c 0; exec timeout \"$0\" \"$@\"")
        .arg("900s")
        .arg(faint)
        .args([
            "--no-logfile",
            "--none",
            "--enable",
            "malloc",
            "--enable",
            "calloc",
            "--enable",
            "realloc",
        ])
        .arg(bin)
        .args(["-t", "-c"])
        .arg(&conf)
        // FAINT keeps its profile in the working directory; each sweep owns
        // one. stdin is closed so nothing in the run can wait on a terminal.
        .current_dir(dir)
        .stdin(std::process::Stdio::null());
    let out = cmd.output().expect("run faint");
    // FAINT writes its report to stderr.
    let stdout = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "faint itself failed or timed out: {}\n--- report tail ---\n{}",
        out.status,
        optest::cluster::tail(&stdout, 30)
    );
    stdout
}

fn statuses(report: &str) -> Vec<String> {
    report
        .lines()
        .filter_map(|l| {
            l.split_once("Exited, status: ")
                .map(|(_, s)| s.trim().to_string())
        })
        .collect()
}

#[test]
fn rust_config_path_fails_stop_on_every_allocation_failure() {
    let Some((faint, tools)) = faint_or_skip() else {
        return;
    };
    let Some(bin) = tools.dynomited.clone() else {
        assert!(!optest::Tools::required(), "dynomited missing");
        return;
    };
    let dir = tempfile::tempdir().expect("tmp");
    let report = sweep(&faint, &bin, dir.path());
    let st = statuses(&report);
    let injected = report
        .lines()
        .find_map(|l| {
            l.split_once("Injecting ")
                .and_then(|(_, r)| r.split_whitespace().next()?.parse::<usize>().ok())
        })
        .expect("FAINT reported an injection count");
    eprintln!("[Rust] FAINT: {injected} allocation sites; outcomes {st:?}");
    assert!(injected > 0, "profiling found no allocation sites");
    for s in &st {
        assert!(
            !s.contains("Segmentation"),
            "Rust engine segfaulted after an allocation failure: {s}\n{report}"
        );
        // Orderly outcomes: success (the failed allocation was handled),
        // status 1 (reported error), or SIGABRT (allocation-error handler).
        assert!(
            s.starts_with('0') || s.starts_with('1') || s.contains("Aborted"),
            "unexpected outcome {s}"
        );
    }
}

/// The C reference on the same sweep, recorded for comparison: its config
/// path does not check every allocation and segfaults on most of them.
/// This pins that finding; it is not a requirement on C.
#[test]
fn c_config_path_allocation_sweep_is_recorded() {
    let Some((faint, tools)) = faint_or_skip() else {
        return;
    };
    let Some(bin) = tools.c_dynomite.clone() else {
        return;
    };
    let dir = tempfile::tempdir().expect("tmp");
    let report = sweep(&faint, &bin, dir.path());
    let st = statuses(&report);
    let segv = st.iter().filter(|s| s.contains("Segmentation")).count();
    eprintln!(
        "[C] FAINT: {} injections, {segv} SIGSEGV; outcomes {st:?}",
        st.len()
    );
    assert!(!st.is_empty());
}
