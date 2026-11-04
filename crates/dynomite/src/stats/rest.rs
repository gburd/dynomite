//! Hand-rolled HTTP/1.1 server exposing the stats snapshot as JSON.
//!
//! `GET /` and `GET /info` return the latest snapshot. The admin
//! command surface (`/help`, `/ping`, `/setloglevel/<n>`,
//! `/loglevelup`, `/logleveldown`, `/historeset`, `/cluster_describe`,
//! `/get_consistency`, `/set_consistency/<read|write>/<level>`,
//! `/get_timeout_factor`, `/set_timeout_factor/<n>`,
//! `/peer/<up|down|reset>/<name|all>`, `/read_repairs/<enable|disable>`,
//! `/state/<get_state|standby|writes_only|normal|resuming>`) acts on
//! the [`ServerPool`] attached with [`StatsServer::with_admin_pool`].
//! Any other path is a ping and answers `OK\r\n`. `/metrics`,
//! `/cluster-info.txt`, `/ring` and `/stats` are additional read-only
//! views.

#![allow(clippy::needless_continue)]

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::Instrument as _;

use crate::admin::cluster_info::{format_text, ClusterInfoSnapshot, RingSnapshot};
use crate::cluster::peer::PeerState;
use crate::cluster::pool::{DynState, ServerPool};
use crate::msg::ConsistencyLevel;
use crate::stats::prometheus::render_prometheus;
use crate::stats::snapshot::Snapshot;

/// Type alias for a closure that produces a fresh
/// [`ClusterInfoSnapshot`] every time the `/cluster-info.txt`
/// route is hit.
///
/// The closure must be `Send + Sync` so a clone can be moved
/// into each accept-handler task. The runtime owns the closure;
/// embedders set it via
/// [`StatsServer::with_cluster_info_provider`].
pub type ClusterInfoProvider = Arc<dyn Fn() -> ClusterInfoSnapshot + Send + Sync>;

/// Type alias for a closure that produces a fresh
/// [`RingSnapshot`] every time the `/ring` route is hit.
///
/// The closure must be `Send + Sync` so a clone can be moved
/// into each accept-handler task. Embedders set it via
/// [`StatsServer::with_ring_provider`]; when unset the `/ring`
/// route returns `503 Service Unavailable`.
pub type RingProvider = Arc<dyn Fn() -> RingSnapshot + Send + Sync>;

/// Type alias for the closure `/historeset` invokes to clear the
/// latency and queue histograms on the next aggregation.
///
/// Embedders set it via [`StatsServer::with_histo_reset`]; when
/// unset `/historeset` only acknowledges.
pub type HistoResetHook = Arc<dyn Fn() + Send + Sync>;

/// Reply body for commands that only acknowledge.
const OK_BODY: &[u8] = b"OK\r\n";

/// Reply body for `/help`.
const HELP_BODY: &[u8] = b"/info\n/help\n/ping\n/cluster_describe\n\
/setloglevel/<0-11>\n/loglevelup\n/logleveldown\n/historeset\n\
/get_consistency\n/set_consistency/<read|write>/<dc_one|dc_quorum|dc_safe_quorum>\n\
/get_timeout_factor\n/set_timeout_factor/<1-10>\n/peer/<up|down|reset>\n\
/state/<get_state|standby|writes_only|normal|resuming>\n\n";

/// Maximum number of bytes the server will read for an HTTP request
/// line plus headers. Requests larger than this are rejected.
///
/// # Examples
///
/// ```
/// assert!(dynomite::stats::MAX_REQUEST_BYTES >= 1024);
/// ```
pub const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// Maximum number of headers parsed in a single request.
///
/// # Examples
///
/// ```
/// assert!(dynomite::stats::MAX_HEADERS > 0);
/// ```
pub const MAX_HEADERS: usize = 32;

/// Maximum time the server waits for a single read from a connected
/// peer before closing the socket. Protects tokio tasks from
/// slow-loris clients.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// A bound TCP listener serving the stats endpoint.
///
/// Construct via [`StatsServer::bind`] then call
/// [`StatsServer::run`] to accept connections in a loop, or
/// [`StatsServer::accept_one`] for one-shot tests.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use dynomite::stats::{Snapshot, StatsServer};
/// use parking_lot::Mutex;
///
/// # async fn _example() -> std::io::Result<()> {
/// let sink = Arc::new(Mutex::new(Snapshot::default()));
/// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?;
/// let _addr = server.local_addr()?;
/// # Ok(())
/// # }
/// ```
pub struct StatsServer {
    listener: TcpListener,
    source: Arc<Mutex<Snapshot>>,
    routes: Routes,
}

/// Optional providers shared by every connection handler.
#[derive(Clone, Default)]
struct Routes {
    cluster_info: Option<ClusterInfoProvider>,
    ring: Option<RingProvider>,
    admin: Option<Arc<ServerPool>>,
    histo_reset: Option<HistoResetHook>,
}

impl StatsServer {
    /// Bind a listener at `addr`. Returns the bound server alongside
    /// its actual local address (useful when binding to port 0).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let _server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn bind(addr: SocketAddr, source: Arc<Mutex<Snapshot>>) -> io::Result<Self> {
        // Bind via the shared helper so the stats listener sets
        // SO_REUSEADDR like the client and dnode listeners. Without it,
        // a fast restart (kill + relaunch) fails with EADDRINUSE while
        // the previous socket lingers in TIME_WAIT, and because the
        // server builds all listeners as a unit that failure aborts the
        // whole process.
        let listener = crate::net::listener::bind_dual_stack(
            addr,
            crate::net::listener::BindOptions::default(),
        )?;
        Ok(Self {
            listener,
            source,
            routes: Routes::default(),
        })
    }

    /// Attach a [`ClusterInfoProvider`] so the server answers
    /// `GET /cluster-info.txt` with a freshly assembled
    /// snapshot. When no provider is registered the route
    /// returns `503 Service Unavailable`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::admin::cluster_info::ClusterInfoSnapshot;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?
    ///     .with_cluster_info_provider(Arc::new(ClusterInfoSnapshot::synthetic));
    /// drop(server);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_cluster_info_provider(mut self, provider: ClusterInfoProvider) -> Self {
        self.routes.cluster_info = Some(provider);
        self
    }

    /// Attach a [`RingProvider`] so the server answers
    /// `GET /ring` with a freshly assembled, JSON-serialized
    /// view of every peer's tokens, dc, rack, and state. When no
    /// provider is registered the route returns `503 Service
    /// Unavailable`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::admin::cluster_info::RingSnapshot;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?
    ///     .with_ring_provider(Arc::new(RingSnapshot::default));
    /// drop(server);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_ring_provider(mut self, provider: RingProvider) -> Self {
        self.routes.ring = Some(provider);
        self
    }

    /// Attach the [`ServerPool`] the admin commands read and mutate
    /// (consistency, timeout factor, peer and local-node state,
    /// `/cluster_describe`). Without a pool those commands answer
    /// `503 Service Unavailable`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let pool = Arc::new(ServerPool::new(PoolConfig::default(), Vec::new()));
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?
    ///     .with_admin_pool(pool);
    /// drop(server);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_admin_pool(mut self, pool: Arc<ServerPool>) -> Self {
        self.routes.admin = Some(pool);
        self
    }

    /// Attach the [`HistoResetHook`] `/historeset` invokes.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?
    ///     .with_histo_reset(Arc::new(|| {}));
    /// drop(server);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_histo_reset(mut self, hook: HistoResetHook) -> Self {
        self.routes.histo_reset = Some(hook);
        self
    }

    /// Returns the local socket address the server is listening on.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?;
    /// let addr = server.local_addr()?;
    /// assert!(addr.port() != 0);
    /// # Ok(())
    /// # }
    /// ```
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept a single connection, serve one HTTP/1.1 request, and
    /// return.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?;
    /// server.accept_one().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn accept_one(&self) -> io::Result<()> {
        let (sock, _peer) = self.listener.accept().await?;
        let snapshot = self.source.lock().clone();
        serve_connection(sock, snapshot, self.routes.clone()).await
    }

    /// Run the accept loop until cancelled. Each connection is handled
    /// on a fresh task so a slow client cannot stall the listener.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use dynomite::stats::{Snapshot, StatsServer};
    /// use parking_lot::Mutex;
    ///
    /// # async fn _example() -> std::io::Result<()> {
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)?;
    /// let _ = tokio::spawn(async move { server.run().await });
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run(self) -> io::Result<()> {
        let span = tracing::info_span!(
            "stats_server.run",
            local = %self.listener.local_addr().map_or_else(|_| String::from("?"), |a| a.to_string()),
        );
        let listener = self.listener;
        let source = self.source;
        let routes = self.routes;
        async move {
            loop {
                let (sock, _peer) = listener.accept().await?;
                let snapshot = source.lock().clone();
                let rt = routes.clone();
                tokio::spawn(async move {
                    let _ = serve_connection(sock, snapshot, rt).await;
                });
            }
        }
        .instrument(span)
        .await
    }
}

async fn serve_connection(
    mut sock: TcpStream,
    snapshot: Snapshot,
    routes: Routes,
) -> io::Result<()> {
    let mut buf = vec![0u8; MAX_REQUEST_BYTES];
    let mut filled = 0usize;
    loop {
        if filled == buf.len() {
            return write_response(&mut sock, 400, "Bad Request", b"").await;
        }
        let read_result = tokio::time::timeout(READ_TIMEOUT, sock.read(&mut buf[filled..])).await;
        let Ok(Ok(n)) = read_result else {
            // Read error or timeout: close silently, matching the
            // reference error path which drops the connection without
            // writing a response.
            let _ = sock.shutdown().await;
            return Ok(());
        };
        if n == 0 {
            break;
        }
        filled += n;
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&buf[..filled]) {
            Ok(httparse::Status::Complete(_)) => {
                return handle_parsed(&mut sock, &req, snapshot, routes).await;
            }
            Ok(httparse::Status::Partial) => continue,
            Err(_) => {
                // A request line that is not `GET <path> HTTP/1.x` is
                // refused and then answered like an unknown command.
                return write_response(&mut sock, 400, "Bad Request", OK_BODY).await;
            }
        }
    }
    Ok(())
}

async fn handle_parsed(
    sock: &mut TcpStream,
    req: &httparse::Request<'_, '_>,
    snapshot: Snapshot,
    routes: Routes,
) -> io::Result<()> {
    let path = req.path.unwrap_or("/");
    if !matches!(req.method, Some("GET")) {
        return write_response(sock, 405, "Method Not Allowed", b"").await;
    }
    match path {
        "/" | "/info" | "/stats" => {
            let body = snapshot.to_json();
            write_json_response(sock, body.as_bytes()).await
        }
        "/metrics" => {
            let body = render_prometheus(&snapshot);
            write_metrics_response(sock, body.as_bytes()).await
        }
        "/cluster-info.txt" => match routes.cluster_info {
            Some(provider) => {
                let snap = provider();
                let mut body: Vec<u8> = Vec::with_capacity(4096);
                if format_text(&snap, &mut body).is_err() {
                    return write_response(sock, 500, "Internal Server Error", b"").await;
                }
                write_text_response(sock, &body).await
            }
            None => write_response(sock, 503, "Service Unavailable", b"").await,
        },
        "/ring" | "/ring.json" => match routes.ring {
            Some(provider) => match serde_json::to_vec(&provider()) {
                Ok(body) => write_json_response(sock, &body).await,
                Err(_) => write_response(sock, 500, "Internal Server Error", b"").await,
            },
            None => write_response(sock, 503, "Service Unavailable", b"").await,
        },
        _ => match stats_send_rsp(parse_request(path.as_bytes()), &routes) {
            Some(body) => write_json_response(sock, &body).await,
            None => write_response(sock, 503, "Service Unavailable", b"").await,
        },
    }
}

/// An admin command decoded from the request path.
#[derive(Debug, PartialEq, Eq)]
enum StatsCmd {
    /// Unknown or malformed command; acknowledged with `OK`.
    Unknown,
    Help,
    Ping,
    SetLogLevel(Vec<u8>),
    LogLevelUp,
    LogLevelDown,
    HistoReset,
    ClusterDescribe,
    GetConsistency,
    SetConsistency {
        read: bool,
        level: ConsistencyLevel,
    },
    GetTimeoutFactor,
    SetTimeoutFactor(Vec<u8>),
    Peer(PeerState, Vec<u8>),
    ReadRepairs(bool),
    State(DynState),
    GetState,
}

/// Decode an admin path. Exact-match commands compare the whole
/// path; the parameterised ones match a prefix and read the argument
/// at a fixed offset past it.
fn parse_request(path: &[u8]) -> StatsCmd {
    let tail = |n: usize| path.get(n..).unwrap_or_default();
    match path {
        b"/help" => return StatsCmd::Help,
        b"/ping" => return StatsCmd::Ping,
        b"/loglevelup" => return StatsCmd::LogLevelUp,
        b"/logleveldown" => return StatsCmd::LogLevelDown,
        b"/historeset" => return StatsCmd::HistoReset,
        b"/cluster_describe" => return StatsCmd::ClusterDescribe,
        b"/get_consistency" => return StatsCmd::GetConsistency,
        b"/get_timeout_factor" => return StatsCmd::GetTimeoutFactor,
        _ => {}
    }
    if let Some(val) = path.strip_prefix(b"/setloglevel") {
        return match val.strip_prefix(b"/") {
            Some(v) => StatsCmd::SetLogLevel(v.to_vec()),
            None => StatsCmd::Unknown,
        };
    }
    if let Some(op) = path.strip_prefix(b"/set_consistency") {
        let (read, ty) = if let Some(ty) = op.strip_prefix(b"/read") {
            (true, ty)
        } else if let Some(ty) = op.strip_prefix(b"/write") {
            (false, ty)
        } else {
            return StatsCmd::Unknown;
        };
        let level = if ty.eq_ignore_ascii_case(b"/dc_one") {
            ConsistencyLevel::DcOne
        } else if ty.eq_ignore_ascii_case(b"/dc_quorum") {
            ConsistencyLevel::DcQuorum
        } else if ty.eq_ignore_ascii_case(b"/dc_safe_quorum") {
            ConsistencyLevel::DcSafeQuorum
        } else {
            return StatsCmd::Unknown;
        };
        return StatsCmd::SetConsistency { read, level };
    }
    if let Some(val) = path.strip_prefix(b"/set_timeout_factor") {
        return match val.strip_prefix(b"/") {
            Some(v) => StatsCmd::SetTimeoutFactor(v.to_vec()),
            None => StatsCmd::Unknown,
        };
    }
    if let Some(ps) = path.strip_prefix(b"/peer") {
        // The peer name starts one byte past the state word, whatever
        // that byte is.
        let name = |n: usize| ps.get(n..).unwrap_or_default().to_vec();
        return if ps.starts_with(b"/down") {
            StatsCmd::Peer(PeerState::Down, name(6))
        } else if ps.starts_with(b"/up") {
            StatsCmd::Peer(PeerState::Normal, name(4))
        } else if ps.starts_with(b"/reset") {
            StatsCmd::Peer(PeerState::Reset, name(7))
        } else {
            StatsCmd::Ping
        };
    }
    if let Some(op) = path.strip_prefix(b"/read_repairs") {
        return if op.starts_with(b"/enable") {
            StatsCmd::ReadRepairs(true)
        } else if op.starts_with(b"/disable") {
            StatsCmd::ReadRepairs(false)
        } else {
            StatsCmd::Unknown
        };
    }
    if path.starts_with(b"/state") {
        match tail(7) {
            b"standby" => return StatsCmd::State(DynState::Standby),
            b"writes_only" => return StatsCmd::State(DynState::WritesOnly),
            b"normal" => return StatsCmd::State(DynState::Normal),
            b"resuming" => return StatsCmd::State(DynState::Resuming),
            b"get_state" => return StatsCmd::GetState,
            _ => {}
        }
    }
    StatsCmd::Ping
}

/// Leading decimal integer of `s` read the way `%d` does (leading
/// whitespace, optional sign, digits, saturating at the `long` range,
/// `0` when there are no digits), narrowed to the signed byte the
/// level and factor arguments are stored in.
fn scan_i8(s: &[u8]) -> i8 {
    let s = s.trim_ascii_start();
    let (neg, digits) = match s.first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let mut v: i64 = 0;
    for d in digits.iter().take_while(|b| b.is_ascii_digit()) {
        v = v.saturating_mul(10).saturating_add(i64::from(d - b'0'));
    }
    if neg {
        v = v.saturating_neg();
    }
    // Only the low byte of the converted value is kept.
    v.to_le_bytes()[0].cast_signed()
}

/// Execute `cmd` and build the reply body. Returns `None` when the
/// command needs the admin pool and none is attached.
fn stats_send_rsp(cmd: StatsCmd, routes: &Routes) -> Option<Vec<u8>> {
    use crate::core::log::{log_level_decrement, log_level_increment, log_level_set};
    let pool = routes.admin.as_deref();
    let body = match cmd {
        StatsCmd::Unknown | StatsCmd::Ping => OK_BODY.to_vec(),
        StatsCmd::Help => HELP_BODY.to_vec(),
        StatsCmd::SetLogLevel(v) => {
            let level = scan_i8(&v).clamp(0, 11).cast_unsigned();
            log_level_set(level);
            OK_BODY.to_vec()
        }
        StatsCmd::LogLevelUp => {
            log_level_increment();
            OK_BODY.to_vec()
        }
        StatsCmd::LogLevelDown => {
            log_level_decrement();
            OK_BODY.to_vec()
        }
        StatsCmd::HistoReset => {
            if let Some(hook) = routes.histo_reset.as_ref() {
                hook();
            }
            OK_BODY.to_vec()
        }
        StatsCmd::ClusterDescribe => cluster_describe(pool?),
        StatsCmd::GetConsistency => format!(
            "Read Consistency: {}\r\nWrite Consistency: {}\r\n",
            pool?.read_consistency().name(),
            pool?.write_consistency().name()
        )
        .into_bytes(),
        StatsCmd::SetConsistency { read, level } => {
            if read {
                pool?.set_read_consistency(level);
            } else {
                pool?.set_write_consistency(level);
            }
            OK_BODY.to_vec()
        }
        StatsCmd::GetTimeoutFactor => {
            format!("Timeout factor: {}\n", pool?.timeout_factor()).into_bytes()
        }
        StatsCmd::SetTimeoutFactor(v) => {
            let f = scan_i8(&v).clamp(1, 10).cast_unsigned();
            pool?.set_timeout_factor(f);
            OK_BODY.to_vec()
        }
        StatsCmd::Peer(state, name) => {
            set_peer_state(pool?, state, &name);
            OK_BODY.to_vec()
        }
        StatsCmd::ReadRepairs(on) => {
            crate::msg::set_read_repairs_enabled(on);
            if on {
                b"Read Repairs: ENABLED\r\n".to_vec()
            } else {
                b"Read Repairs: DISABLED\r\n".to_vec()
            }
        }
        StatsCmd::State(state) => {
            pool?.set_local_state(state);
            OK_BODY.to_vec()
        }
        StatsCmd::GetState => format!("State: {}\n", pool?.local_state().name()).into_bytes(),
    };
    Some(body)
}

/// `/peer/<op>/<name>`: `all` resets every peer, including the local
/// node, whatever `<op>` was; otherwise the first peer whose host
/// equals `name` takes the requested state.
fn set_peer_state(pool: &ServerPool, state: PeerState, name: &[u8]) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let mut peers = pool.peers().write();
    for p in peers.iter_mut() {
        if name == b"all" {
            p.set_state(PeerState::Reset, now);
        } else if p.endpoint().host().as_bytes() == name {
            p.set_state(state, now);
            break;
        }
    }
}

/// JSON writer that keeps the trailing-comma convention of the
/// cluster description: every value is followed by `,`, and closing
/// a nesting level drops the comma before it.
struct DescBuf(Vec<u8>);

impl DescBuf {
    fn add_string(&mut self, key: &str, val: &str) {
        self.0
            .extend_from_slice(format!("\"{key}\":\"{val}\",").as_bytes());
    }

    fn add_num(&mut self, key: &str, val: i64, last: bool) {
        let sep = if last { "" } else { "," };
        self.0
            .extend_from_slice(format!("\"{key}\":{val}{sep}\n").as_bytes());
    }

    fn begin(&mut self, key: Option<&str>, arr: bool) {
        let open = if arr { '[' } else { '{' };
        match key {
            Some(k) => self
                .0
                .extend_from_slice(format!("\"{k}\": {open}").as_bytes()),
            None => self.0.push(open as u8),
        }
    }

    fn end(&mut self, arr: bool) {
        let spaces = self
            .0
            .iter()
            .rev()
            .take_while(|b| b.is_ascii_whitespace())
            .count();
        let at = self.0.len() - spaces;
        if at > 0 && self.0[at - 1] == b',' {
            self.0.remove(at - 1);
        }
        self.0.push(if arr { b']' } else { b'}' });
        self.0.push(b',');
    }

    fn footer(&mut self) {
        if let Some(last) = self.0.last_mut() {
            *last = b'}';
        }
        self.0.push(b'\n');
    }
}

/// `/cluster_describe`: every datacenter, its racks and their nodes
/// with name, host, dnode port and first token word. The local node
/// reports its public IPv4 and hostname when the environment
/// provides them.
fn cluster_describe(pool: &ServerPool) -> Vec<u8> {
    use crate::cluster::snitch::{public_hostname, public_ip4, DEFAULT_ENV};
    let peers = pool.peers().read();
    let dcs = pool.datacenters().read();
    let local_name = peers.first().map(|p| p.endpoint().host().to_string());
    let mut env = |k: &str| std::env::var(k).ok();
    // shortcut: the env label is the default (`aws`), not the pool's `env:`; plumb it through PoolConfig if a non-aws label must pick PUBLIC_* variables.
    let (ip4, hostname) = match local_name.as_deref() {
        Some(n) => (
            public_ip4(DEFAULT_ENV, n, &mut env),
            public_hostname(DEFAULT_ENV, n, &mut env),
        ),
        None => (None, None),
    };
    let mut b = DescBuf(Vec::with_capacity(1024 * peers.len().max(1)));
    b.begin(None, false);
    b.begin(Some("dcs"), true);
    for dc in dcs.iter() {
        b.begin(None, false);
        b.add_string("name", dc.name());
        b.begin(Some("racks"), true);
        for rack in dc.racks() {
            b.begin(None, false);
            b.add_string("name", rack.name());
            b.begin(Some("servers"), true);
            for p in peers
                .iter()
                .filter(|p| p.dc() == dc.name() && p.rack() == rack.name())
            {
                let host = p.endpoint().host();
                let (name, host) = if p.is_local() {
                    (
                        ip4.clone().unwrap_or_else(|| host.to_string()),
                        hostname.clone().unwrap_or_else(|| host.to_string()),
                    )
                } else {
                    (host.to_string(), host.to_string())
                };
                b.begin(None, false);
                b.add_string("name", &name);
                b.add_string("host", &host);
                b.add_num("port", i64::from(p.endpoint().port()), false);
                let token = p
                    .tokens()
                    .first()
                    .and_then(|t| t.mag().first().copied())
                    .unwrap_or(0);
                b.add_num("token", i64::from(token), true);
                b.end(false);
            }
            b.end(true);
            b.end(false);
        }
        b.end(true);
        b.end(false);
    }
    b.end(true);
    b.footer();
    b.0
}

async fn write_text_response(sock: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=us-ascii\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(header.as_bytes()).await?;
    sock.write_all(body).await?;
    sock.shutdown().await?;
    Ok(())
}

async fn write_response(
    sock: &mut TcpStream,
    code: u16,
    reason: &str,
    body: &[u8],
) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(header.as_bytes()).await?;
    if !body.is_empty() {
        sock.write_all(body).await?;
    }
    sock.shutdown().await?;
    Ok(())
}

async fn write_json_response(sock: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(header.as_bytes()).await?;
    sock.write_all(body).await?;
    sock.shutdown().await?;
    Ok(())
}

async fn write_metrics_response(sock: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(header.as_bytes()).await?;
    sock.write_all(body).await?;
    sock.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: the stats listener must set SO_REUSEADDR so a fast
    /// restart (kill + immediate rebind, as the qualification harness
    /// does between consistency-level runs) does not fail with
    /// EADDRINUSE while the previous socket lingers. Bind, capture the
    /// concrete address, drop the server, and rebind the same address
    /// immediately -- this succeeds with SO_REUSEADDR and would fail a
    /// plain TcpListener::bind under TIME_WAIT.
    #[tokio::test]
    async fn stats_bind_is_reusable_after_drop() {
        let sink = Arc::new(Mutex::new(Snapshot::default()));
        let first =
            StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink.clone()).expect("first bind");
        let addr = first.local_addr().expect("addr");
        drop(first);
        // Immediate rebind of the same concrete address must succeed.
        let second = StatsServer::bind(addr, sink);
        assert!(
            second.is_ok(),
            "rebind of {addr} failed (SO_REUSEADDR not set?): {:?}",
            second.err()
        );
    }
}
