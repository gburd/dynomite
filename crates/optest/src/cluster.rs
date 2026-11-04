//! Multi-node clusters inside the sandbox.
//!
//! Node `i` (0-based) runs in its own network namespace `n<i>`, attached
//! to a bridge in the sandbox by a veth pair, with address
//! `10.77.0.(i+1)/24`. Its dynomite process and its private datastore
//! both run inside that namespace, so every byte a node sends to a peer
//! leaves through its own `eth0` with its own source address, whatever the
//! engine does with its sockets. Network faults are netem qdiscs on a
//! node's `eth0` keyed by destination address ([`crate::net`]).
//!
//! The test process stays in the sandbox's root namespace, which owns the
//! bridge (`10.77.0.254`) and reaches every node directly.
//!
//! Topologies follow the C conventions: within a DC each rack holds a full
//! replica of the token range, so "3 racks x 1 node" is replication factor
//! 3.

use std::fmt::Write as _;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

use crate::fault::{Faults, FiuControl};
use crate::resp::{Client, RespError, Value};
use crate::tools::Tools;
use crate::{Error, Result};

/// Client (RESP or memcache) port on every node address.
pub const CLIENT_PORT: u16 = 8102;
/// Peer (dnode) port.
pub const PEER_PORT: u16 = 8101;
/// Stats / admin HTTP port.
pub const STATS_PORT: u16 = 22222;
/// Datastore port.
pub const STORE_PORT: u16 = 6379;

/// Which engine a node runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
    /// The Rust `dynomited` under test.
    Rust,
    /// The C reference `dynomite`.
    C,
}

/// Which datastore backs a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Store {
    /// valkey / redis, `data_store: 0`.
    Redis,
    /// memcached, `data_store: 1`.
    Memcache,
}

/// One node's placement and configuration.
#[derive(Clone, Debug)]
pub struct NodeSpec {
    /// Datacenter name.
    pub dc: String,
    /// Rack name.
    pub rack: String,
    /// The node's single ring token.
    pub token: u32,
    /// Extra pool directives (`key: value` YAML lines), e.g.
    /// `("read_consistency", "DC_QUORUM")`.
    pub extra: Vec<(String, String)>,
    /// Faults preloaded into this node's dynomite process.
    pub faults: Faults,
}

/// A whole cluster description.
#[derive(Clone, Debug)]
pub struct ClusterSpec {
    /// Nodes, in index order.
    pub nodes: Vec<NodeSpec>,
    /// Engine for every node.
    pub engine: Engine,
    /// Datastore for every node.
    pub store: Store,
    /// Directives applied to every node before its own `extra`.
    pub common: Vec<(String, String)>,
}

impl ClusterSpec {
    /// `dcs` datacenters, each with `racks` racks of `per_rack` nodes, with
    /// tokens evenly spaced within a rack (the C `cluster_generator.py`
    /// layout).
    ///
    /// # Examples
    ///
    /// ```
    /// let s = optest::ClusterSpec::grid(1, 3, 1);
    /// assert_eq!(s.nodes.len(), 3);
    /// assert!(s.nodes.iter().all(|n| n.token == 0));
    /// ```
    pub fn grid(dcs: usize, racks: usize, per_rack: usize) -> Self {
        let layout: Vec<(String, Vec<(String, usize)>)> = (0..dcs)
            .map(|d| {
                (
                    format!("dc{}", d + 1),
                    (0..racks)
                        .map(|r| (format!("rack{}", r + 1), per_rack))
                        .collect(),
                )
            })
            .collect();
        Self::layout(&layout)
    }

    /// An explicit topology: `(dc, [(rack, node count)])`, as the C test
    /// request files describe it (`cluster_desc`). Tokens within a rack of
    /// `n` nodes are `i * (2^32 / n)`, the C `tokens_for_rack`.
    ///
    /// # Examples
    ///
    /// ```
    /// let s = optest::ClusterSpec::layout(&[
    ///     ("dc1".into(), vec![("rack1".into(), 1)]),
    ///     ("dc2".into(), vec![("rack1".into(), 1), ("rack2".into(), 2)]),
    /// ]);
    /// assert_eq!(s.nodes.len(), 4);
    /// assert_eq!(s.nodes[3].token, 1 << 31);
    /// ```
    pub fn layout(dcs: &[(String, Vec<(String, usize)>)]) -> Self {
        let mut nodes = Vec::new();
        for (dc, racks) in dcs {
            for (rack, count) in racks {
                let stride = (1u64 << 32) / u64::try_from(*count).expect("count fits u64");
                for k in 0..*count {
                    let t = (u64::try_from(k).expect("index fits u64") * stride) % (1u64 << 32);
                    nodes.push(NodeSpec {
                        dc: dc.clone(),
                        rack: rack.clone(),
                        token: u32::try_from(t).expect("token below 2^32"),
                        extra: Vec::new(),
                        faults: Faults::default(),
                    });
                }
            }
        }
        Self {
            nodes,
            engine: Engine::Rust,
            store: Store::Redis,
            common: Vec::new(),
        }
    }

    /// Set a directive on every node.
    #[must_use]
    pub fn with(mut self, key: &str, value: &str) -> Self {
        self.common.push((key.into(), value.into()));
        self
    }

    /// Select the engine.
    #[must_use]
    pub fn engine(mut self, e: Engine) -> Self {
        self.engine = e;
        self
    }
}

/// Address of node `i`.
///
/// # Examples
///
/// ```
/// assert_eq!(optest::cluster::node_ip(0).to_string(), "10.77.0.1");
/// ```
pub fn node_ip(i: usize) -> Ipv4Addr {
    Ipv4Addr::new(10, 77, 0, u8::try_from(i + 1).expect("at most 253 nodes"))
}

/// Network namespace name of node `i`.
///
/// # Examples
///
/// ```
/// assert_eq!(optest::cluster::netns(2), "n2");
/// ```
pub fn netns(i: usize) -> String {
    format!("n{i}")
}

/// A command that runs `program` inside node `i`'s network namespace.
pub fn in_netns(i: usize, program: &Path) -> Command {
    let ip = crate::tools::which("ip").unwrap_or_else(|| PathBuf::from("ip"));
    let mut c = Command::new(ip);
    c.args(["netns", "exec", &netns(i)]).arg(program);
    c
}

fn run(cmd: &mut Command) -> Result<()> {
    let out = cmd.output()?;
    if out.status.success() {
        Ok(())
    } else {
        Err(Error::Command {
            cmd: format!("{cmd:?}"),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }
}

/// Create the bridge and one namespace per node. Idempotent per sandbox:
/// namespaces that already exist are reused, so several clusters can run
/// one after another in the same test.
pub fn build_topology(n: usize) -> Result<()> {
    let ip = crate::tools::which("ip").ok_or(Error::MissingTool("ip"))?;
    let have_bridge = Command::new(&ip)
        .args(["link", "show", "br0"])
        .output()
        .is_ok_and(|o| o.status.success());
    if !have_bridge {
        run(Command::new(&ip).args(["link", "add", "br0", "type", "bridge"]))?;
        run(Command::new(&ip).args(["addr", "add", "10.77.0.254/24", "dev", "br0"]))?;
        run(Command::new(&ip).args(["link", "set", "br0", "up"]))?;
    }
    for i in 0..n {
        let ns = netns(i);
        if Path::new("/run/netns").join(&ns).exists() {
            continue;
        }
        let veth = format!("v{i}");
        run(Command::new(&ip).args(["netns", "add", &ns]))?;
        run(Command::new(&ip).args([
            "link", "add", &veth, "type", "veth", "peer", "name", "eth0", "netns", &ns,
        ]))?;
        run(Command::new(&ip).args(["link", "set", &veth, "master", "br0", "up"]))?;
        let addr = format!("{}/24", node_ip(i));
        run(Command::new(&ip).args(["-n", &ns, "link", "set", "lo", "up"]))?;
        run(Command::new(&ip).args(["-n", &ns, "addr", "add", &addr, "dev", "eth0"]))?;
        run(Command::new(&ip).args(["-n", &ns, "link", "set", "eth0", "up"]))?;
    }
    Ok(())
}

/// A running node: its datastore and its dynomite process.
#[derive(Debug)]
pub struct Node {
    /// Index in the cluster.
    pub index: usize,
    /// Spec it was launched from.
    pub spec: NodeSpec,
    /// Working directory (config, logs, pid, fiu FIFO).
    pub dir: PathBuf,
    store: Option<Child>,
    proc: Option<Child>,
}

impl Node {
    /// The node's loopback address.
    pub fn ip(&self) -> Ipv4Addr {
        node_ip(self.index)
    }

    /// Client endpoint.
    pub fn client_addr(&self) -> SocketAddr {
        SocketAddr::from((self.ip(), CLIENT_PORT))
    }

    /// Datastore endpoint (bypasses dynomite).
    pub fn store_addr(&self) -> SocketAddr {
        SocketAddr::from((self.ip(), STORE_PORT))
    }

    /// Stats endpoint.
    pub fn stats_addr(&self) -> SocketAddr {
        SocketAddr::from((self.ip(), STATS_PORT))
    }

    /// PID of the dynomite process, if running.
    pub fn pid(&self) -> Option<u32> {
        self.proc.as_ref().map(Child::id)
    }

    /// libfiu control for this node (requires `faults.fiu_control` or
    /// launch-time fiu points).
    pub fn fiu(&self) -> Result<FiuControl> {
        let pid = self
            .pid()
            .ok_or_else(|| Error::Protocol("node not running".into()))?;
        FiuControl::attach(&self.dir.join("fiu"), pid)
    }

    /// Path of the node's log.
    pub fn log_path(&self) -> PathBuf {
        self.dir.join("node.log")
    }

    fn signal(&self, sig: Signal) -> Result<()> {
        let pid = self
            .pid()
            .ok_or_else(|| Error::Protocol("node not running".into()))?;
        kill(
            Pid::from_raw(i32::try_from(pid).expect("pid fits i32")),
            sig,
        )
        .map_err(|e| Error::Io(std::io::Error::from(e)))
    }

    /// Freeze the process (a GC pause / VM stall).
    pub fn pause(&self) -> Result<()> {
        self.signal(Signal::SIGSTOP)
    }

    /// Resume a paused process.
    pub fn resume(&self) -> Result<()> {
        self.signal(Signal::SIGCONT)
    }

    /// Send an arbitrary signal (SIGHUP reload, SIGUSR1 log level, ...).
    pub fn send_signal(&self, sig: Signal) -> Result<()> {
        self.signal(sig)
    }

    /// `SIGKILL` the dynomite process (crash). The datastore keeps running.
    pub fn crash(&mut self) {
        if let Some(mut c) = self.proc.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// True while the dynomite process is alive.
    pub fn alive(&mut self) -> bool {
        self.proc
            .as_mut()
            .is_some_and(|c| c.try_wait().ok().flatten().is_none())
    }

    /// Exit status if the dynomite process has exited.
    pub fn exit_status(&mut self) -> Option<std::process::ExitStatus> {
        self.proc.as_mut().and_then(|c| c.try_wait().ok().flatten())
    }

    /// A client connection to this node's client port.
    pub fn client(&self, timeout: Duration) -> std::result::Result<Client, RespError> {
        Client::connect(self.client_addr(), timeout)
    }

    /// A client connection straight to this node's datastore.
    pub fn store_client(&self) -> std::result::Result<Client, RespError> {
        Client::connect(self.store_addr(), Duration::from_secs(5))
    }

    /// Fetch an admin/stats path (e.g. `/info`) with a plain HTTP GET.
    pub fn stats(&self, path: &str) -> Result<String> {
        use std::io::{Read, Write};
        let mut s =
            std::net::TcpStream::connect_timeout(&self.stats_addr(), Duration::from_secs(3))?;
        s.set_read_timeout(Some(Duration::from_secs(3)))?;
        // One write: the C stats server reads the request with a single
        // recv, so a request split across segments (as `write!` on an
        // unbuffered stream does) parses as "GET" with no path (400) and
        // the unread rest makes the close a reset.
        s.write_all(format!("GET {path} HTTP/1.0\r\nHost: x\r\n\r\n").as_bytes())?;
        let mut raw = Vec::new();
        s.read_to_end(&mut raw)?;
        let body = String::from_utf8_lossy(&raw).into_owned();
        Ok(body
            .split_once("\r\n\r\n")
            .map_or(body.clone(), |(_, b)| b.to_string()))
    }

    /// Number of keys held by this node's datastore.
    pub fn store_dbsize(&self) -> Result<i64> {
        let mut c = self
            .store_client()
            .map_err(|e| Error::Protocol(e.to_string()))?;
        match c
            .cmd(&["DBSIZE"])
            .map_err(|e| Error::Protocol(e.to_string()))?
        {
            Value::Int(n) => Ok(n),
            other => Err(Error::Protocol(format!("DBSIZE replied {other:?}"))),
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        for c in [self.proc.take(), self.store.take()].into_iter().flatten() {
            let mut c = c;
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// A running cluster.
#[derive(Debug)]
pub struct Cluster {
    /// Nodes, in index order.
    pub nodes: Vec<Node>,
    /// What the cluster was launched from.
    pub spec: ClusterSpec,
    tools: Tools,
    _dir: tempfile::TempDir,
}

impl Cluster {
    /// Start every datastore, then every dynomite node, and wait until each
    /// node accepts client connections.
    pub fn start(spec: ClusterSpec) -> Result<Self> {
        let tools = Tools::discover();
        build_topology(spec.nodes.len())?;
        let dir = tempfile::Builder::new().prefix("optest-").tempdir()?;
        let mut nodes = Vec::with_capacity(spec.nodes.len());
        for (i, ns) in spec.nodes.iter().enumerate() {
            let ndir = dir.path().join(format!("n{i}"));
            std::fs::create_dir_all(&ndir)?;
            let store = start_store(&tools, spec.store, i, &ndir)?;
            nodes.push(Node {
                index: i,
                spec: ns.clone(),
                dir: ndir,
                store: Some(store),
                proc: None,
            });
        }
        let mut c = Self {
            nodes,
            spec,
            tools,
            _dir: dir,
        };
        for i in 0..c.nodes.len() {
            c.launch(i)?;
        }
        for i in 0..c.nodes.len() {
            c.wait_ready(i, Duration::from_secs(30))?;
        }
        c.wait_meshed(Duration::from_secs(30))?;
        Ok(c)
    }

    /// The YAML config for node `i`.
    pub fn render_config(&self, i: usize) -> String {
        let n = &self.spec.nodes[i];
        let ip = node_ip(i);
        let mut y = String::from("dyn_o_mite:\n");
        let _ = writeln!(y, "  datacenter: {}", n.dc);
        let _ = writeln!(y, "  rack: {}", n.rack);
        let _ = writeln!(y, "  listen: {ip}:{CLIENT_PORT}");
        let _ = writeln!(y, "  dyn_listen: {ip}:{PEER_PORT}");
        let _ = writeln!(y, "  stats_listen: {ip}:{STATS_PORT}");
        let _ = writeln!(y, "  tokens: '{}'", n.token);
        let _ = writeln!(y, "  servers:\n  - {ip}:{STORE_PORT}:1");
        let ds = match self.spec.store {
            Store::Redis => 0,
            Store::Memcache => 1,
        };
        let _ = writeln!(y, "  data_store: {ds}");
        let _ = writeln!(y, "  dyn_seed_provider: simple_provider");
        let seeds: Vec<String> = (0..self.spec.nodes.len())
            .filter(|&j| j != i)
            .map(|j| {
                let o = &self.spec.nodes[j];
                format!(
                    "  - {}:{PEER_PORT}:{}:{}:{}",
                    node_ip(j),
                    o.rack,
                    o.dc,
                    o.token
                )
            })
            .collect();
        if !seeds.is_empty() {
            let _ = writeln!(y, "  dyn_seeds:\n{}", seeds.join("\n"));
        }
        for (k, v) in self.spec.common.iter().chain(n.extra.iter()) {
            let _ = writeln!(y, "  {k}: {v}");
        }
        y
    }

    fn binary(&self) -> Result<PathBuf> {
        match self.spec.engine {
            Engine::Rust => {
                let bin = self
                    .tools
                    .dynomited
                    .clone()
                    .ok_or(Error::MissingTool("dynomited"))?;
                crate::tools::check_fresh(&bin)?;
                Ok(bin)
            }
            Engine::C => self
                .tools
                .c_dynomite
                .clone()
                .ok_or(Error::MissingTool("C dynomite")),
        }
    }

    /// (Re)start node `i`'s dynomite process inside its namespace (its
    /// datastore keeps state).
    pub fn launch(&mut self, i: usize) -> Result<()> {
        let bin = self.binary()?;
        let conf = self.nodes[i].dir.join("node.yml");
        std::fs::write(&conf, self.render_config(i))?;
        let log = std::fs::File::create(self.nodes[i].log_path())?;
        let mut cmd = in_netns(i, &bin);
        cmd.arg("-c").arg(&conf).arg("-v").arg("6");
        cmd.stdout(log.try_clone()?)
            .stderr(log)
            .stdin(Stdio::null());
        self.nodes[i]
            .spec
            .faults
            .apply(&self.tools, &mut cmd, &self.nodes[i].dir.join("fiu"))?;
        self.nodes[i].proc = Some(cmd.spawn()?);
        Ok(())
    }

    /// Crash and relaunch node `i`, then wait for it and its peer links.
    pub fn restart(&mut self, i: usize) -> Result<()> {
        self.nodes[i].crash();
        self.launch(i)?;
        self.wait_ready(i, Duration::from_secs(30))?;
        self.wait_meshed(Duration::from_secs(30))
    }

    /// Wait until node `i` accepts a client connection and answers PING
    /// (or exits, which is an error).
    pub fn wait_ready(&mut self, i: usize, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(st) = self.nodes[i].exit_status() {
                let log = std::fs::read_to_string(self.nodes[i].log_path()).unwrap_or_default();
                return Err(Error::Protocol(format!(
                    "node {i} exited during startup ({st}):\n{}",
                    tail(&log, 40)
                )));
            }
            if let Ok(mut c) = self.nodes[i].client(Duration::from_millis(500)) {
                if c.cmd(&["PING"]).is_ok() {
                    return Ok(());
                }
            }
            if Instant::now() > deadline {
                return Err(Error::Timeout(format!("node {i} not ready")));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Wait until every node holds an established outbound peer connection
    /// to every other node. Readiness of the client port alone is not
    /// enough: a node accepts clients before its peer links are up, and a
    /// write sent then reaches only the local rack.
    pub fn wait_meshed(&mut self, timeout: Duration) -> Result<()> {
        let n = self.nodes.len();
        if n < 2 {
            return Ok(());
        }
        let deadline = Instant::now() + timeout;
        loop {
            let mut missing = Vec::new();
            for i in 0..n {
                let peers = established_peers(i)?;
                for j in (0..n).filter(|&j| j != i) {
                    if !peers.contains(&node_ip(j)) {
                        missing.push((i, j));
                    }
                }
            }
            if missing.is_empty() {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(Error::Timeout(format!(
                    "peer mesh incomplete: missing {missing:?}"
                )));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Addresses of every node except `i`.
    pub fn others(&self, i: usize) -> Vec<Ipv4Addr> {
        (0..self.nodes.len())
            .filter(|&j| j != i)
            .map(node_ip)
            .collect()
    }

    /// Stop node `i`'s datastore (SHUTDOWN NOSAVE through its own
    /// client) and wait for the process to exit; the dynomite node keeps
    /// running.
    pub fn stop_store(&mut self, i: usize) -> Result<()> {
        if let Ok(mut c) = self.nodes[i].store_client() {
            let _ = c.cmd(&["SHUTDOWN", "NOSAVE"]);
        }
        if let Some(mut p) = self.nodes[i].store.take() {
            let _ = p.kill();
            let _ = p.wait();
        }
        Ok(())
    }

    /// Keys stored in each node's datastore, in node order.
    pub fn store_sizes(&self) -> Result<Vec<i64>> {
        self.nodes.iter().map(Node::store_dbsize).collect()
    }
}

fn start_store(tools: &Tools, store: Store, node: usize, dir: &Path) -> Result<Child> {
    let ip = node_ip(node);
    let (bin, args): (PathBuf, Vec<String>) = match store {
        Store::Redis => (
            tools
                .redis_server
                .clone()
                .ok_or(Error::MissingTool("valkey-server"))?,
            vec![
                "--bind".into(),
                ip.to_string(),
                "--port".into(),
                STORE_PORT.to_string(),
                "--save".into(),
                String::new(),
                "--appendonly".into(),
                "no".into(),
                "--protected-mode".into(),
                "no".into(),
                "--dir".into(),
                dir.display().to_string(),
            ],
        ),
        Store::Memcache => (
            tools
                .memcached
                .clone()
                .ok_or(Error::MissingTool("memcached"))?,
            vec![
                "-l".into(),
                ip.to_string(),
                "-p".into(),
                STORE_PORT.to_string(),
                "-U".into(),
                "0".into(),
            ],
        ),
    };
    let child = in_netns(node, &bin)
        .args(&args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::net::TcpStream::connect_timeout(
        &SocketAddr::from((ip, STORE_PORT)),
        Duration::from_millis(200),
    )
    .is_err()
    {
        if Instant::now() > deadline {
            return Err(Error::Timeout(format!("datastore on {ip}")));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(child)
}

/// Peer addresses node `i` holds an established outbound connection to
/// (remote port [`PEER_PORT`]), from its namespace's `/proc/net/tcp`.
fn established_peers(i: usize) -> Result<Vec<Ipv4Addr>> {
    let out = in_netns(i, Path::new("cat"))
        .arg("/proc/net/tcp")
        .output()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let want = format!("{PEER_PORT:04X}");
    Ok(text
        .lines()
        .skip(1)
        .filter_map(|l| {
            let f: Vec<&str> = l.split_whitespace().collect();
            // f[2] = rem_address "HEXIP:PORT", f[3] = state (01 = ESTABLISHED)
            if f.len() <= 3 || f[3] != "01" {
                return None;
            }
            let (hip, port) = f[2].split_once(':')?;
            if port != want {
                return None;
            }
            let v = u32::from_str_radix(hip, 16).ok()?;
            // /proc/net/tcp prints the address in host byte order of a
            // network-order u32, i.e. little-endian octets on x86.
            Some(Ipv4Addr::from(v.to_le_bytes()))
        })
        .collect())
}

/// Last `n` lines of `s`.
pub fn tail(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}
