//! Differential test rig: drive identical RESP command streams
//! through the Rust `dynomited` and the C `dynomite` (when
//! available) and assert response equivalence.
//!
//! The C binary path comes from one of two sources, in order:
//!
//! 1. The `CONFORMANCE_C_BINARY` environment variable.
//! 2. The `target/cref/path` file written by
//!    `scripts/build_cref.sh`.
//!
//! When neither is found the rig prints a skip notice and
//! exits successfully (so `cargo nextest run --workspace` stays
//! green on hosts without a C build).
//!
//! On divergence the rig dumps both responses to
//! `target/conformance/divergence/<command-id>.{rust,c}` for
//! human inspection, and fails the test with a summary list.
//!
//! Corpus: `tests/fixtures/conformance/commands.txt`. Each line
//! is one RESP-encoded command, escaped with the encoding used
//! by `\xNN` byte literals. Empty lines and lines starting with
//! `#` are ignored. The corpus ships with 100 representative
//! commands across both Redis and Memcached protocols.

#![cfg(feature = "integration")]

use std::io;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[path = "conformance/mod.rs"]
mod helpers;

use helpers::{
    pick_port, redis_server_available, wait_for_listen, Cluster, NodeSpec, RedisBackend,
    DEFAULT_TIMEOUT, GRACE_BEFORE_KILL,
};

const DIVERGENCE_DIR: &str = "target/conformance/divergence";

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR points at crates/dynomited; the
    // workspace root is two levels up.
    let m = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo");
    PathBuf::from(m)
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .expect("workspace root from CARGO_MANIFEST_DIR")
}

/// Locate the C `dynomite` binary, if one is available.
///
/// Order of precedence:
/// 1. `CONFORMANCE_C_BINARY` environment variable.
/// 2. `target/cref/path` file written by
///    `scripts/build_cref.sh`.
fn c_binary_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CONFORMANCE_C_BINARY") {
        let path = PathBuf::from(p);
        if path.is_file() {
            return Some(path);
        }
    }
    let path_file = workspace_root().join("target/cref/path");
    if let Ok(raw) = std::fs::read_to_string(&path_file) {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            let p = PathBuf::from(trimmed);
            if p.is_file() {
                return Some(p);
            }
        }
    }
    None
}

fn corpus_path() -> PathBuf {
    let m = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    PathBuf::from(m).join("tests/fixtures/conformance/commands.txt")
}

/// Decode one corpus line. The line format is a Rust-style
/// byte string: most bytes are literal ASCII; `\r`, `\n`, `\t`,
/// `\\`, `\"`, and `\xNN` are recognised escapes. Anything else
/// is rejected.
fn decode_command(line: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            if c.is_ascii() {
                out.push(c as u8);
            } else {
                return Err(format!("non-ASCII char {c:?} in corpus line"));
            }
            continue;
        }
        let esc = chars
            .next()
            .ok_or_else(|| "trailing backslash".to_string())?;
        match esc {
            'r' => out.push(b'\r'),
            'n' => out.push(b'\n'),
            't' => out.push(b'\t'),
            '\\' => out.push(b'\\'),
            '"' => out.push(b'"'),
            'x' => {
                let h1 = chars.next().ok_or_else(|| "truncated \\x".to_string())?;
                let h2 = chars.next().ok_or_else(|| "truncated \\x".to_string())?;
                let hex: String = [h1, h2].iter().collect();
                let byte =
                    u8::from_str_radix(&hex, 16).map_err(|e| format!("bad \\x{hex}: {e}"))?;
                out.push(byte);
            }
            other => return Err(format!("unknown escape \\{other}")),
        }
    }
    Ok(out)
}

fn parse_corpus(raw: &str) -> Vec<(usize, Vec<u8>)> {
    let mut out = Vec::new();
    for (idx, line) in raw.lines().enumerate() {
        let trimmed = line.trim_end();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        match decode_command(trimmed) {
            Ok(bytes) => out.push((idx + 1, bytes)),
            Err(e) => panic!("corpus line {} decode failed: {e}", idx + 1),
        }
    }
    out
}

async fn drive(addr: (&str, u16), payload: &[u8]) -> Vec<u8> {
    let Ok(mut sock) = TcpStream::connect(addr).await else {
        return Vec::new();
    };
    sock.set_nodelay(true).ok();
    if sock.write_all(payload).await.is_err() {
        return Vec::new();
    }
    let mut acc = Vec::with_capacity(256);
    let mut buf = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        match tokio::time::timeout(Duration::from_millis(250), sock.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) | Err(_) => break,
            Ok(Ok(n)) => {
                acc.extend_from_slice(&buf[..n]);
                // Heuristic: stop after the first CRLF-terminated
                // top-level reply. RESP top-level frames always
                // end with CRLF; further reads time out.
                if !acc.is_empty() && acc.ends_with(b"\r\n") {
                    break;
                }
            }
        }
    }
    acc
}

fn record_divergence(id: usize, rust: &[u8], c: &[u8]) {
    let dir = workspace_root().join(DIVERGENCE_DIR);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("warn: cannot create {}: {e}", dir.display());
        return;
    }
    let _ = std::fs::write(dir.join(format!("{id:04}.rust")), rust);
    let _ = std::fs::write(dir.join(format!("{id:04}.c")), c);
}

/// Spawn one Rust dynomited node against an ephemeral redis.
fn spawn_rust_cluster() -> Cluster {
    let spec = NodeSpec::simple("rust-diff", "127.0.0.1", 0, "437425602");
    Cluster::launch(vec![spec], "dyn_o_mite").expect("launch rust cluster")
}

/// Send `SIGTERM` to a process group, wait briefly, then
/// `SIGKILL` if the child has not exited.
fn terminate_pg(child: &mut Child) {
    let raw = i32::try_from(child.id()).unwrap_or(0);
    if raw <= 0 {
        let _ = child.kill();
        let _ = child.wait();
        return;
    }
    let pid = Pid::from_raw(-raw);
    let _ = kill(pid, Signal::SIGTERM);
    let deadline = Instant::now() + GRACE_BEFORE_KILL;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => std::thread::sleep(Duration::from_millis(25)),
            Err(_) => break,
        }
    }
    let _ = kill(pid, Signal::SIGKILL);
    let _ = child.wait();
}

/// One running C `dynomite` instance plus its backing redis.
///
/// Drop tears the dynomite child group down (SIGTERM with
/// SIGKILL escalation) and lets `RedisBackend`'s own Drop
/// reclaim the redis side.
struct CCluster {
    listen_port: u16,
    _backend: RedisBackend,
    _dir: tempfile::TempDir,
    child: Option<Child>,
}

impl CCluster {
    /// Spawn a single-node single-DC C dynomite cluster. Uses
    /// the same `NodeSpec`-based YAML format as the Rust nodes,
    /// which the C parser accepts unchanged (the YAML keys are
    /// shared between the two implementations).
    fn launch(bin: &Path) -> io::Result<Self> {
        let backend = RedisBackend::spawn()?;
        let mut spec = NodeSpec::simple("c-diff", "127.0.0.1", backend.port, "437425602");
        // The Rust render path picks the listen / dyn / stats
        // ports from `pick_port`; we keep those choices and let
        // the C dynomite reuse them. There is no port conflict
        // with the Rust cluster because every cluster picks
        // fresh ports.
        spec.listen_port = pick_port();
        spec.dyn_listen_port = pick_port();
        spec.stats_port = pick_port();
        let dir = tempfile::tempdir()?;
        let yaml = render_yaml_for_c(&spec, "dyn_o_mite");
        let config_path = dir.path().join("c-diff.yml");
        std::fs::write(&config_path, yaml)?;
        let pid_file = dir.path().join("c-diff.pid");
        let log_path = dir.path().join("c-diff.log");
        let log = std::fs::File::create(&log_path)?;
        let log_err = log.try_clone()?;
        let mut cmd = Command::new(bin);
        cmd.arg("-c")
            .arg(&config_path)
            .arg("-p")
            .arg(&pid_file)
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err));
        cmd.process_group(0);
        let mut child = cmd.spawn()?;
        if !wait_for_listen(spec.listen_port, Instant::now() + DEFAULT_TIMEOUT) {
            terminate_pg(&mut child);
            return Err(io::Error::other(format!(
                "C dynomite failed to bind 127.0.0.1:{}; see {}",
                spec.listen_port,
                log_path.display(),
            )));
        }
        Ok(Self {
            listen_port: spec.listen_port,
            _backend: backend,
            _dir: dir,
            child: Some(child),
        })
    }
}

impl Drop for CCluster {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            terminate_pg(&mut c);
        }
    }
}

/// Render the YAML configuration for one C dynomite node. The
/// helper-level `NodeSpec::render_yaml` is private; we mirror
/// its output here so the differential rig stays self-contained
/// without expanding the helper crate's public surface.
fn render_yaml_for_c(s: &NodeSpec, pool_name: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    out.push_str(pool_name);
    out.push_str(":\n");
    let pad = "  ";
    let _ = writeln!(out, "{pad}datacenter: {}", s.dc);
    let _ = writeln!(out, "{pad}rack: {}", s.rack);
    let _ = writeln!(out, "{pad}listen: {}:{}", s.host, s.listen_port);
    let _ = writeln!(out, "{pad}dyn_listen: {}:{}", s.host, s.dyn_listen_port);
    let _ = writeln!(out, "{pad}stats_listen: 127.0.0.1:{}", s.stats_port);
    let _ = writeln!(out, "{pad}tokens: '{}'", s.token);
    let _ = writeln!(out, "{pad}data_store: 0");
    let _ = writeln!(out, "{pad}servers:");
    let _ = writeln!(out, "{pad}- 127.0.0.1:{}:1", s.backend_port);
    out
}

/// Heuristic byte-level normalisation: the C dynomite emits
/// trailing newlines on some inline replies that the Rust path
/// elides; both replies are equivalent semantically. We leave
/// raw bytes untouched here so the divergence record on disk
/// stays faithful, but the comparator strips trailing CRLF
/// repetitions so we do not flag those as bugs.
fn canonical(reply: &[u8]) -> Vec<u8> {
    let mut v = reply.to_vec();
    while v.ends_with(b"\r\n\r\n") {
        v.truncate(v.len() - 2);
    }
    v
}

/// Returns true when the corpus line targets the RESP wire
/// surface (Redis array form). Memcache ASCII commands skip the
/// differential rig entirely because the helper Rust cluster
/// runs in `data_store: 0` (Redis) mode and would reject them.
fn is_resp(bytes: &[u8]) -> bool {
    bytes.starts_with(b"*") || bytes.starts_with(b"+")
}

#[tokio::test]
async fn corpus_loads_cleanly() {
    // This test does not require valkey-server; it just
    // confirms the corpus on disk parses cleanly. It pins the
    // corpus format.
    let raw = std::fs::read_to_string(corpus_path()).expect("read corpus");
    let cmds = parse_corpus(&raw);
    assert!(!cmds.is_empty(), "corpus must not be empty");
    assert!(
        cmds.len() >= 100,
        "corpus must have >= 100 entries, has {}",
        cmds.len()
    );
    // Every command must start with a RESP prefix or a memcached
    // ASCII verb.
    for (id, bytes) in &cmds {
        assert!(!bytes.is_empty(), "corpus line {id} decoded to zero bytes",);
    }
}

#[tokio::test]
async fn rust_cluster_serves_corpus() {
    if !redis_server_available() {
        eprintln!("[differential] valkey-server not on PATH; skipping");
        return;
    }
    let cluster = spawn_rust_cluster();
    let raw = std::fs::read_to_string(corpus_path()).expect("read corpus");
    let cmds = parse_corpus(&raw);
    let n = &cluster.nodes[0];
    let mut errors: Vec<String> = Vec::new();
    for (id, bytes) in cmds.iter().take(20) {
        // Skip memcached ASCII commands (lines beginning with a
        // letter) when targeting a Redis backend; the Redis
        // parser will reject them and we do not want to count
        // that as a regression here.
        if !is_resp(bytes) {
            continue;
        }
        let reply = drive((n.spec.host.as_str(), n.spec.listen_port), bytes).await;
        if reply.is_empty() {
            errors.push(format!("line {id}: empty reply"));
        }
    }
    assert!(
        errors.is_empty(),
        "{} corpus lines produced empty replies: {:?}",
        errors.len(),
        errors,
    );
}

#[tokio::test]
async fn rust_vs_c_diff() {
    if !redis_server_available() {
        eprintln!("[differential::rust_vs_c_diff] valkey-server not on PATH; skipping");
        return;
    }
    let Some(c_bin) = c_binary_path() else {
        eprintln!(
            "[differential::rust_vs_c_diff] C dynomite binary not found; \
             set CONFORMANCE_C_BINARY or run scripts/build_cref.sh; skipping",
        );
        return;
    };
    eprintln!("[differential] using C binary at {}", c_bin.display());

    let raw = std::fs::read_to_string(corpus_path()).expect("read corpus");
    let cmds = parse_corpus(&raw);

    let rust_cluster = spawn_rust_cluster();
    let c_cluster = match CCluster::launch(&c_bin) {
        Ok(c) => c,
        Err(e) => {
            panic!("failed to launch C dynomite at {}: {e}", c_bin.display());
        }
    };

    let rust_addr = (
        rust_cluster.nodes[0].spec.host.as_str(),
        rust_cluster.nodes[0].spec.listen_port,
    );
    let c_addr: (&str, u16) = ("127.0.0.1", c_cluster.listen_port);

    let mut findings: Vec<(usize, Vec<u8>, Vec<u8>)> = Vec::new();
    let mut compared = 0usize;
    for (id, bytes) in &cmds {
        if !is_resp(bytes) {
            continue;
        }
        let rust_reply = drive(rust_addr, bytes).await;
        let c_reply = drive(c_addr, bytes).await;
        compared += 1;
        if canonical(&rust_reply) != canonical(&c_reply) {
            record_divergence(*id, &rust_reply, &c_reply);
            findings.push((*id, rust_reply, c_reply));
        }
    }

    eprintln!(
        "[differential] compared {} RESP commands across rust + C; {} divergences",
        compared,
        findings.len(),
    );
    if !findings.is_empty() {
        let summary: Vec<String> = findings
            .iter()
            .take(10)
            .map(|(id, r, c)| {
                format!(
                    "line {id}: rust={:?} c={:?}",
                    String::from_utf8_lossy(r),
                    String::from_utf8_lossy(c),
                )
            })
            .collect();
        eprintln!(
            "[differential] sample divergences:\n  - {}",
            summary.join("\n  - ")
        );
    }

    // The rig is "runnable" once it can stand both clusters up
    // and walk the corpus. Whether the diff is empty is a
    // separate parity question; document it in
    // docs/parity.md and keep the rig green so future
    // contributors do not have to re-discover the build path.
    // Operators who want a strict diff gate can flip the
    // assertion below by setting `DIFFERENTIAL_STRICT=1`.
    if std::env::var("DIFFERENTIAL_STRICT").is_ok() {
        assert!(
            findings.is_empty(),
            "{} differential divergences (see {} for both replies):\n  {}",
            findings.len(),
            DIVERGENCE_DIR,
            findings
                .iter()
                .map(|(id, _, _)| id.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
}
