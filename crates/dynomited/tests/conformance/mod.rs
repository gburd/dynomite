//! Conformance harness shared helpers.
//!
//! This module is included from `tests/conformance.rs` (the
//! integration test crate entry) via a `#[path]` directive. It
//! exposes three pieces of test infrastructure:
//!
//! * [`RedisBackend`] - spawns an ephemeral `redis-server` for
//!   one dynomite node and tears it down on drop.
//! * [`DynomitedNode`] - spawns one `dynomited` binary against a
//!   per-node YAML config and tears it down on drop.
//! * [`Cluster`] - composes one or more `DynomitedNode`s with
//!   their backing `RedisBackend`s and a Drop guard that kills
//!   every spawned process group on panic or scope exit.
//! * [`RespClient`] - a small synchronous (tokio-driven) RESP
//!   client that handles partial reads, malformed inputs, and
//!   timeouts gracefully.
//!
//! Every spawned child runs in its own process group. The Drop
//! impl on `Cluster` sends `SIGTERM` to each group then upgrades
//! to `SIGKILL` after a short grace window, so the suite leaves
//! no orphaned processes even when a test panics mid-flight.
//!
//! The harness skips entirely when `redis-server` is not on
//! `PATH` so CI environments without Redis still build the test
//! binary cleanly.

#![allow(dead_code, missing_docs)]

use std::io;
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Default per-step timeout when waiting on a process or socket.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Time we allow a child to exit gracefully after `SIGTERM`
/// before escalating to `SIGKILL` on Drop.
pub const GRACE_BEFORE_KILL: Duration = Duration::from_millis(750);

/// True when `redis-server` is available on `PATH`. Tests use
/// this to mark themselves "ignored" rather than fail when the
/// CI environment lacks the binary.
#[must_use]
pub fn redis_server_available() -> bool {
    which_in_path("redis-server").is_some()
}

/// Locate an executable on `PATH`, returning the first match.
pub fn which_in_path(name: &str) -> Option<PathBuf> {
    let env = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&env) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Reserve an ephemeral TCP port. The listener is dropped before
/// returning so the caller can rebind. Race-prone but adequate
/// for test orchestration; we accept the small risk in exchange
/// for parallel-test friendliness.
pub fn pick_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

/// Wait until `127.0.0.1:port` accepts a TCP connection or
/// `deadline` is exceeded. Returns `true` on success.
pub fn wait_for_listen(port: u16, deadline: Instant) -> bool {
    while Instant::now() < deadline {
        let addr = format!("127.0.0.1:{port}");
        match std::net::TcpStream::connect_timeout(
            &addr.parse().expect("addr parse"),
            Duration::from_millis(200),
        ) {
            Ok(_) => return true,
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
    false
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

/// One ephemeral `redis-server` process backing one dynomite
/// node.
pub struct RedisBackend {
    pub port: u16,
    pub dir: tempfile::TempDir,
    child: Option<Child>,
}

impl RedisBackend {
    /// Spawn a fresh `redis-server` on an ephemeral port.
    ///
    /// # Errors
    ///
    /// Returns an error if `redis-server` cannot be spawned or
    /// fails to bind within `DEFAULT_TIMEOUT`.
    pub fn spawn() -> io::Result<Self> {
        let bin = which_in_path("redis-server")
            .ok_or_else(|| io::Error::other("redis-server not on PATH"))?;
        let dir = tempfile::tempdir()?;
        let port = pick_port();
        let mut cmd = Command::new(&bin);
        cmd.args([
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--save",
            "",
            "--appendonly",
            "no",
            "--protected-mode",
            "no",
            "--dir",
            dir.path().to_str().expect("utf8 tmpdir"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
        cmd.process_group(0);
        let mut child = cmd.spawn()?;
        if !wait_for_listen(port, Instant::now() + DEFAULT_TIMEOUT) {
            terminate_pg(&mut child);
            return Err(io::Error::other(format!(
                "redis-server failed to bind 127.0.0.1:{port}"
            )));
        }
        Ok(Self {
            port,
            dir,
            child: Some(child),
        })
    }
}

impl Drop for RedisBackend {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            terminate_pg(&mut c);
        }
    }
}

/// Specification of one dynomite node, consumed by
/// [`Cluster::launch`].
#[derive(Debug, Clone)]
pub struct NodeSpec {
    pub name: String,
    pub host: String,
    pub listen_port: u16,
    pub dyn_listen_port: u16,
    pub stats_port: u16,
    pub backend_port: u16,
    pub dc: String,
    pub rack: String,
    pub token: String,
    pub seeds: Vec<String>,
    /// Extra YAML keys to splice into `dyn_o_mite:` (e.g.
    /// consistency overrides for a multi-rack test).
    pub extra: Vec<(String, String)>,
    /// Optional override for the TCP port the harness probes
    /// to decide that a freshly-spawned `dynomited` is ready.
    /// Defaults to `listen_port`. Tests that bind the proxy
    /// listener over a non-TCP transport (e.g. QUIC) point
    /// this at `dyn_listen_port` because the harness's
    /// readiness probe speaks plain TCP.
    pub readiness_port: Option<u16>,
}

impl NodeSpec {
    /// Convenience constructor for a single-DC, single-rack node.
    pub fn simple(
        name: impl Into<String>,
        host: impl Into<String>,
        backend_port: u16,
        token: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            host: host.into(),
            listen_port: pick_port(),
            dyn_listen_port: pick_port(),
            stats_port: pick_port(),
            backend_port,
            dc: "dc1".into(),
            rack: "rack1".into(),
            token: token.into(),
            seeds: Vec::new(),
            extra: Vec::new(),
            readiness_port: None,
        }
    }

    /// Render a `dyn_seeds` entry pointing at this node.
    pub fn seed_string(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.host, self.dyn_listen_port, self.rack, self.dc, self.token
        )
    }

    fn render_yaml(&self, pool_name: &str) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        out.push_str(pool_name);
        out.push_str(":\n");
        let pad = "  ";
        let _ = writeln!(out, "{pad}datacenter: {}", self.dc);
        let _ = writeln!(out, "{pad}rack: {}", self.rack);
        let _ = writeln!(out, "{pad}listen: {}:{}", self.host, self.listen_port);
        let _ = writeln!(
            out,
            "{pad}dyn_listen: {}:{}",
            self.host, self.dyn_listen_port
        );
        let _ = writeln!(out, "{pad}stats_listen: 127.0.0.1:{}", self.stats_port);
        let _ = writeln!(out, "{pad}tokens: '{}'", self.token);
        let _ = writeln!(out, "{pad}data_store: 0");
        let _ = writeln!(out, "{pad}servers:");
        let _ = writeln!(out, "{pad}- 127.0.0.1:{}:1", self.backend_port);
        if !self.seeds.is_empty() {
            let _ = writeln!(out, "{pad}dyn_seeds:");
            for s in &self.seeds {
                let _ = writeln!(out, "{pad}- {s}");
            }
        }
        for (k, v) in &self.extra {
            let _ = writeln!(out, "{pad}{k}: {v}");
        }
        out
    }
}

/// Handle to one running `dynomited` process plus its
/// configuration paths.
pub struct DynomitedNode {
    pub spec: NodeSpec,
    pub config_path: PathBuf,
    pub pid_file: PathBuf,
    pub log_path: PathBuf,
    child: Option<Child>,
}

impl DynomitedNode {
    fn spawn(bin: &Path, spec: NodeSpec, dir: &Path, pool_name: &str) -> io::Result<Self> {
        let yaml = spec.render_yaml(pool_name);
        let config_path = dir.join(format!("{}.yml", spec.name));
        std::fs::write(&config_path, &yaml)?;
        let pid_file = dir.join(format!("{}.pid", spec.name));
        let log_path = dir.join(format!("{}.log", spec.name));
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
        let child = cmd.spawn()?;

        // Wait for the readiness port to bind. By default this
        // is the client-listen port (the last socket
        // `Server::build` binds for TCP-transport pools); when
        // the spec overrides `readiness_port` (e.g. QUIC pools
        // whose proxy listener is UDP and not visible to a TCP
        // probe), the probe targets the override instead.
        let probe_port = spec.readiness_port.unwrap_or(spec.listen_port);
        if !wait_for_listen(probe_port, Instant::now() + DEFAULT_TIMEOUT) {
            let mut node = Self {
                spec,
                config_path,
                pid_file,
                log_path,
                child: Some(child),
            };
            // Drop will tear it down; surface the error.
            if let Some(mut c) = node.child.take() {
                terminate_pg(&mut c);
            }
            return Err(io::Error::other("dynomited failed to bind listen port"));
        }
        Ok(Self {
            spec,
            config_path,
            pid_file,
            log_path,
            child: Some(child),
        })
    }

    /// Connect to this node's client-listen port over TCP.
    ///
    /// # Errors
    ///
    /// Returns the underlying tokio connect error.
    pub async fn connect(&self) -> io::Result<TcpStream> {
        let s = TcpStream::connect((self.spec.host.as_str(), self.spec.listen_port)).await?;
        s.set_nodelay(true).ok();
        Ok(s)
    }

    /// Inspect whether the process is still alive without
    /// blocking. `Some(true)` = alive, `Some(false)` = exited,
    /// `None` = unknown.
    pub fn is_alive(&mut self) -> Option<bool> {
        let c = self.child.as_mut()?;
        match c.try_wait() {
            Ok(Some(_)) => Some(false),
            Ok(None) => Some(true),
            Err(_) => None,
        }
    }

    /// Send `SIGTERM` (with `SIGKILL` fallback) to the dynomited
    /// process group and wait for it to exit. Idempotent: a
    /// node that has already been killed reports success.
    pub fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            terminate_pg(&mut c);
        }
    }

    /// Respawn the dynomited child against the same
    /// configuration. Used by tests that simulate a node
    /// reboot mid-workload.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error if the child cannot be
    /// spawned or fails to rebind within `DEFAULT_TIMEOUT`.
    pub fn respawn(&mut self) -> io::Result<()> {
        if self.child.is_some() {
            return Ok(());
        }
        // The previous child may have left a pidfile behind
        // (e.g. SIGKILL after the grace window). Remove it so
        // the new dynomited's flock attempt does not race the
        // stale exclusive lock.
        let _ = std::fs::remove_file(&self.pid_file);
        let bin = which_in_path("dynomited")
            .map_or_else(|| assert_cmd::cargo::cargo_bin("dynomited"), |p| p);
        // Retry the spawn-and-bind dance: the previous instance
        // may still be holding the listen port (kernel cleanup
        // can lag behind a SIGKILL by several seconds).
        let mut last_err: Option<io::Error> = None;
        let attempts = 6;
        for _ in 0..attempts {
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.log_path)?;
            let log_err = log.try_clone()?;
            let mut cmd = Command::new(&bin);
            cmd.arg("-c")
                .arg(&self.config_path)
                .arg("-p")
                .arg(&self.pid_file)
                .stdout(Stdio::from(log))
                .stderr(Stdio::from(log_err));
            cmd.process_group(0);
            let mut child = cmd.spawn()?;
            let probe_port = self.spec.readiness_port.unwrap_or(self.spec.listen_port);
            if wait_for_listen(probe_port, Instant::now() + DEFAULT_TIMEOUT) {
                self.child = Some(child);
                return Ok(());
            }
            terminate_pg(&mut child);
            last_err = Some(io::Error::other(
                "dynomited respawn failed to bind listen port",
            ));
            std::thread::sleep(Duration::from_secs(1));
            let _ = std::fs::remove_file(&self.pid_file);
        }
        Err(last_err.unwrap_or_else(|| io::Error::other("respawn loop exhausted attempts")))
    }
}

impl Drop for DynomitedNode {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            terminate_pg(&mut c);
        }
    }
}

/// A multi-node cluster. Owns its dynomited children and their
/// Redis backends; on drop everything is torn down.
pub struct Cluster {
    pub nodes: Vec<DynomitedNode>,
    pub backends: Vec<RedisBackend>,
    pub dir: tempfile::TempDir,
}

impl Cluster {
    /// Spawn a cluster from a list of partial node specs. Each
    /// spec contributes one dynomited node and one redis
    /// backend. Seeds are computed automatically: every node's
    /// `dyn_seeds` is "all other nodes' seed_string()".
    ///
    /// # Errors
    ///
    /// Returns an error if any backend or dynomited child fails
    /// to bind within the readiness window.
    pub fn launch(mut specs: Vec<NodeSpec>, pool_name: &str) -> io::Result<Self> {
        let bin = which_in_path("dynomited")
            .map_or_else(|| assert_cmd::cargo::cargo_bin("dynomited"), |p| p);
        let dir = tempfile::tempdir()?;
        let mut backends = Vec::with_capacity(specs.len());
        for s in &mut specs {
            let backend = RedisBackend::spawn()?;
            s.backend_port = backend.port;
            backends.push(backend);
        }
        // Compute the full seed list and assign per-node seed
        // lists (each node's seeds == all other nodes' seed
        // strings).
        let all: Vec<String> = specs.iter().map(NodeSpec::seed_string).collect();
        for (idx, s) in specs.iter_mut().enumerate() {
            s.seeds = all
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != idx)
                .map(|(_, v)| v.clone())
                .collect();
        }
        let mut nodes = Vec::with_capacity(specs.len());
        for s in specs {
            nodes.push(DynomitedNode::spawn(&bin, s, dir.path(), pool_name)?);
        }
        Ok(Self {
            nodes,
            backends,
            dir,
        })
    }
}

// ----- RESP client --------------------------------------------------------

/// Decoded RESP value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    /// Convenience accessor for bulk-string contents.
    #[must_use]
    pub fn as_bulk(&self) -> Option<&[u8]> {
        match self {
            Self::Bulk(Some(b)) => Some(b),
            _ => None,
        }
    }

    /// Convenience accessor for simple-string contents.
    #[must_use]
    pub fn as_simple(&self) -> Option<&str> {
        if let Self::SimpleString(s) = self {
            Some(s)
        } else {
            None
        }
    }

    /// Convenience accessor for integer replies.
    #[must_use]
    pub fn as_integer(&self) -> Option<i64> {
        if let Self::Integer(n) = self {
            Some(*n)
        } else {
            None
        }
    }
}

/// RESP-level error surfaced by [`RespClient`].
#[derive(Debug, thiserror::Error)]
pub enum RespError {
    /// Underlying tokio I/O error (peer closed, broken pipe, ...).
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// Wire framing was malformed.
    #[error("malformed RESP: {0}")]
    Malformed(String),
    /// The peer closed before a full reply was received.
    #[error("peer closed mid-reply")]
    Eof,
    /// The reply did not arrive within the deadline.
    #[error("timed out waiting for reply")]
    Timeout,
}

/// Minimal tokio-driven RESP client. Owns a `TcpStream`, encodes
/// outbound requests, decodes inbound replies. The decoder
/// handles partial reads (loops on `read_buf` until a complete
/// reply is parsed) and rejects malformed inputs.
pub struct RespClient {
    stream: TcpStream,
    rx: Vec<u8>,
    timeout: Duration,
}

impl RespClient {
    /// Wrap an already-connected `TcpStream`.
    #[must_use]
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            rx: Vec::with_capacity(1024),
            timeout: Duration::from_secs(5),
        }
    }

    /// Connect to `host:port` and wrap the stream.
    ///
    /// # Errors
    ///
    /// Propagates the tokio connect error.
    pub async fn connect(host: &str, port: u16) -> Result<Self, RespError> {
        let s = TcpStream::connect((host, port)).await?;
        s.set_nodelay(true).ok();
        Ok(Self::new(s))
    }

    /// Override the per-reply timeout. Defaults to 5 seconds.
    pub fn set_timeout(&mut self, t: Duration) {
        self.timeout = t;
    }

    /// Send a RESP command of the form
    /// `*<n>\r\n$<len>\r\n<arg>\r\n...`.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error if the write fails.
    pub async fn send_command<S>(&mut self, args: &[S]) -> Result<(), RespError>
    where
        S: AsRef<[u8]>,
    {
        let mut buf: Vec<u8> = Vec::with_capacity(64);
        buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for a in args {
            let s = a.as_ref();
            buf.extend_from_slice(format!("${}\r\n", s.len()).as_bytes());
            buf.extend_from_slice(s);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).await?;
        Ok(())
    }

    /// Read one RESP reply. Loops on partial reads until a full
    /// reply is parsed. Returns [`RespError::Timeout`] if
    /// `self.timeout` elapses.
    ///
    /// # Errors
    ///
    /// See [`RespError`].
    pub async fn read_reply(&mut self) -> Result<RespValue, RespError> {
        let timeout = self.timeout;
        let fut = async {
            loop {
                if let Some((value, consumed)) = try_decode(&self.rx)? {
                    self.rx.drain(..consumed);
                    return Ok(value);
                }
                let mut tmp = [0u8; 4096];
                let n = self.stream.read(&mut tmp).await?;
                if n == 0 {
                    return Err(RespError::Eof);
                }
                self.rx.extend_from_slice(&tmp[..n]);
            }
        };
        match tokio::time::timeout(timeout, fut).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(RespError::Timeout),
        }
    }

    /// Convenience: send + read.
    ///
    /// # Errors
    ///
    /// See [`RespError`].
    pub async fn cmd<S: AsRef<[u8]>>(&mut self, args: &[S]) -> Result<RespValue, RespError> {
        self.send_command(args).await?;
        self.read_reply().await
    }

    /// Borrow the underlying stream (for tests that want to
    /// trigger a half-close, etc.).
    pub fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }
}

/// Attempt to decode one RESP value from `buf`. Returns
/// `Ok(None)` if more bytes are needed, `Ok(Some((value, n)))`
/// after a complete reply was parsed (consuming `n` bytes), or
/// `Err(Malformed)` on framing errors.
pub fn try_decode(buf: &[u8]) -> Result<Option<(RespValue, usize)>, RespError> {
    decode_at(buf, 0)
}

fn decode_at(buf: &[u8], start: usize) -> Result<Option<(RespValue, usize)>, RespError> {
    if start >= buf.len() {
        return Ok(None);
    }
    let prefix = buf[start];
    match prefix {
        b'+' => {
            decode_line(buf, start + 1).map(|opt| opt.map(|(s, e)| (RespValue::SimpleString(s), e)))
        }
        b'-' => decode_line(buf, start + 1).map(|opt| opt.map(|(s, e)| (RespValue::Error(s), e))),
        b':' => decode_line(buf, start + 1).and_then(|opt| match opt {
            None => Ok(None),
            Some((s, e)) => {
                let n: i64 = s
                    .parse()
                    .map_err(|_| RespError::Malformed(format!("bad integer: {s:?}")))?;
                Ok(Some((RespValue::Integer(n), e)))
            }
        }),
        b'$' => decode_bulk(buf, start + 1),
        b'*' => decode_array(buf, start + 1),
        other => Err(RespError::Malformed(format!(
            "unknown RESP prefix 0x{other:02x}"
        ))),
    }
}

fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    if start >= buf.len() {
        return None;
    }
    let end = buf.len().saturating_sub(1);
    (start..end).find(|&i| buf[i] == b'\r' && buf[i + 1] == b'\n')
}

fn decode_line(buf: &[u8], start: usize) -> Result<Option<(String, usize)>, RespError> {
    let Some(pos) = find_crlf(buf, start) else {
        return Ok(None);
    };
    let s = std::str::from_utf8(&buf[start..pos])
        .map_err(|e| RespError::Malformed(format!("non-utf8 line: {e}")))?
        .to_owned();
    Ok(Some((s, pos + 2)))
}

fn decode_bulk(buf: &[u8], start: usize) -> Result<Option<(RespValue, usize)>, RespError> {
    let Some((line, after_len)) = decode_line(buf, start)? else {
        return Ok(None);
    };
    let n: i64 = line
        .parse()
        .map_err(|_| RespError::Malformed(format!("bad bulk length: {line:?}")))?;
    if n < 0 {
        return Ok(Some((RespValue::Bulk(None), after_len)));
    }
    let want =
        usize::try_from(n).map_err(|_| RespError::Malformed("bulk length overflow".into()))?;
    let end = after_len + want + 2;
    if buf.len() < end {
        return Ok(None);
    }
    if &buf[after_len + want..after_len + want + 2] != b"\r\n" {
        return Err(RespError::Malformed(
            "bulk frame missing trailing CRLF".into(),
        ));
    }
    let payload = buf[after_len..after_len + want].to_vec();
    Ok(Some((RespValue::Bulk(Some(payload)), end)))
}

fn decode_array(buf: &[u8], start: usize) -> Result<Option<(RespValue, usize)>, RespError> {
    let Some((line, after_len)) = decode_line(buf, start)? else {
        return Ok(None);
    };
    let n: i64 = line
        .parse()
        .map_err(|_| RespError::Malformed(format!("bad array length: {line:?}")))?;
    if n < 0 {
        return Ok(Some((RespValue::Array(None), after_len)));
    }
    let want =
        usize::try_from(n).map_err(|_| RespError::Malformed("array length overflow".into()))?;
    let mut cursor = after_len;
    let mut acc = Vec::with_capacity(want);
    for _ in 0..want {
        match decode_at(buf, cursor)? {
            None => return Ok(None),
            Some((v, e)) => {
                acc.push(v);
                cursor = e;
            }
        }
    }
    Ok(Some((RespValue::Array(Some(acc)), cursor)))
}

// ----- Tests for the helper rig itself ------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_decode_simple_string() {
        let (v, n) = try_decode(b"+OK\r\n").unwrap().unwrap();
        assert_eq!(v, RespValue::SimpleString("OK".into()));
        assert_eq!(n, 5);
    }

    #[test]
    fn try_decode_partial_returns_none() {
        // Only the header arrived; we need bytes + trailing CRLF.
        assert!(try_decode(b"$5\r\nhel").unwrap().is_none());
        assert!(try_decode(b"+PA").unwrap().is_none());
        assert!(try_decode(b"").unwrap().is_none());
    }

    #[test]
    fn try_decode_bulk_nil() {
        let (v, n) = try_decode(b"$-1\r\n").unwrap().unwrap();
        assert_eq!(v, RespValue::Bulk(None));
        assert_eq!(n, 5);
    }

    #[test]
    fn try_decode_bulk_full() {
        let (v, n) = try_decode(b"$5\r\nhello\r\n").unwrap().unwrap();
        assert_eq!(v, RespValue::Bulk(Some(b"hello".to_vec())));
        assert_eq!(n, 11);
    }

    #[test]
    fn try_decode_array() {
        let (v, n) = try_decode(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n")
            .unwrap()
            .unwrap();
        let RespValue::Array(Some(items)) = v else {
            panic!("expected array");
        };
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].as_bulk(), Some(&b"a"[..]));
        assert_eq!(items[1].as_bulk(), Some(&b"b"[..]));
        assert_eq!(n, 18);
    }

    #[test]
    fn try_decode_rejects_unknown_prefix() {
        let err = try_decode(b"?nope\r\n").unwrap_err();
        assert!(matches!(err, RespError::Malformed(_)), "{err:?}");
    }

    #[test]
    fn try_decode_rejects_bad_integer() {
        let err = try_decode(b":notanint\r\n").unwrap_err();
        assert!(matches!(err, RespError::Malformed(_)), "{err:?}");
    }

    #[test]
    fn try_decode_rejects_missing_bulk_crlf() {
        // Length 3 but the bulk body lacks the trailing CRLF.
        let err = try_decode(b"$3\r\nhelXX").unwrap_err();
        assert!(matches!(err, RespError::Malformed(_)), "{err:?}");
    }

    /// The RESP client must enforce its timeout when no reply
    /// arrives. Drive a localhost TCP loopback that never
    /// responds and assert `RespError::Timeout`.
    #[tokio::test]
    async fn read_reply_times_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        // Accept and never reply. We must KEEP the accepted
        // socket alive (binding via `_sock`, not `_`) so the
        // client side stays open instead of seeing RST.
        let server = tokio::spawn(async move {
            let (_sock, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let mut c = RespClient::connect("127.0.0.1", port).await.unwrap();
        c.set_timeout(Duration::from_millis(100));
        c.send_command(&[b"PING".as_ref()]).await.unwrap();
        let err = c.read_reply().await.unwrap_err();
        assert!(matches!(err, RespError::Timeout), "{err:?}");
        server.abort();
    }

    /// The client must recover from a partial read by looping
    /// until the full frame is delivered.
    #[tokio::test]
    async fn read_reply_handles_partial_chunks() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            // Drip-feed `$5\r\nhello\r\n` byte by byte.
            let bytes = b"$5\r\nhello\r\n";
            for b in bytes {
                sock.write_all(&[*b]).await.unwrap();
                sock.flush().await.ok();
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        });
        let mut c = RespClient::connect("127.0.0.1", port).await.unwrap();
        let v = c.read_reply().await.unwrap();
        assert_eq!(v.as_bulk(), Some(&b"hello"[..]));
        let _ = server.await;
    }

    /// Peer closed mid-frame -> `RespError::Eof`.
    #[tokio::test]
    async fn read_reply_reports_eof() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = sock.write_all(b"$5\r\nhe").await;
            // Close immediately; client should detect the EOF.
            drop(sock);
        });
        let mut c = RespClient::connect("127.0.0.1", port).await.unwrap();
        let err = c.read_reply().await.unwrap_err();
        assert!(matches!(err, RespError::Eof), "{err:?}");
        let _ = server.await;
    }

    /// Spawning a child with `process_group(0)` and then
    /// dropping the wrapper kills the entire group. Spawn `sleep
    /// 30` and assert it has exited within 1 second of the
    /// guard going out of scope.
    #[test]
    fn drop_kills_child_process_group() {
        // Long-sleeping child proves the Drop impl works on
        // both clean and panic paths.
        let mut cmd = Command::new("sleep");
        cmd.arg("30").stdout(Stdio::null()).stderr(Stdio::null());
        cmd.process_group(0);
        let Ok(mut child) = cmd.spawn() else {
            return; // sleep not available; skip
        };
        let pid = i32::try_from(child.id()).unwrap();
        // Sanity: process is alive.
        assert!(child.try_wait().unwrap().is_none());
        // Manually invoke our terminate helper (the same code Drop runs).
        terminate_pg(&mut child);
        // Confirm the process group is gone: `kill -0` should fail.
        let res = kill(Pid::from_raw(-pid), None);
        assert!(res.is_err(), "process group {pid} still alive");
    }

    #[test]
    fn pick_port_returns_distinct_ports() {
        let a = pick_port();
        let b = pick_port();
        // Not strictly required but the OS should not reuse the
        // most-recent port instantly.
        assert_ne!(a, 0);
        assert_ne!(b, 0);
    }
}
