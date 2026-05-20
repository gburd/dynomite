//! Dual-stack listener helpers.
//!
//! Each `listen:` / `dyn_listen:` directive binds a single socket:
//! `0.0.0.0:port` opens an IPv4-only socket, `[::]:port` opens an
//! IPv6 socket whose `IPV6_V6ONLY` flag matches the platform
//! default (on Linux, the `/proc/sys/net/ipv6/bindv6only` knob,
//! usually `0`).
//!
//! The Stage 9 Rust wiring uses [`socket2::Socket`] to open the
//! socket explicitly so the engine can:
//!
//! * bind to a single address family when the YAML specified a
//!   concrete address (`192.0.2.1:8102`, `[::1]:8102`),
//! * bind to a v6 wildcard with `IPV6_V6ONLY=0` when the YAML
//!   specified `[::]:port`, accepting both v4 and v6 clients on
//!   one listener (matching most platforms' default),
//! * bind to a v4 wildcard when the YAML specified `0.0.0.0:port`.
//!
//! Callers that want strict-v6 behavior pass
//! [`BindOptions::v6_only`].
//!
//! # Examples
//!
//! ```
//! use dynomite::net::listener::{bind_dual_stack, BindOptions};
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
//! let listener = bind_dual_stack(addr, BindOptions::default()).unwrap();
//! assert!(listener.local_addr().unwrap().ip().is_loopback());
//! # });
//! ```

use std::io;
use std::net::SocketAddr;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;

/// Configuration knobs for [`bind_dual_stack`].
#[derive(Copy, Clone, Debug, Default)]
pub struct BindOptions {
    /// When the bind address is a v6 wildcard (`[::]`), set the
    /// `IPV6_V6ONLY` flag instead of accepting v4-mapped clients.
    /// The default (`false`) accepts both families, matching the
    /// platform default on Linux.
    pub v6_only: bool,
    /// `SO_REUSEADDR`. Defaults to `true`.
    pub reuseaddr: bool,
    /// TCP listen backlog. Defaults to `1024`. The configured pool
    /// `backlog` knob (Stage 4) feeds this field at startup.
    pub backlog: i32,
}

impl BindOptions {
    /// Build options with `v6_only = true` and the other knobs at
    /// their defaults.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::net::listener::BindOptions;
    /// assert!(BindOptions::v6_only_strict().v6_only);
    /// ```
    #[must_use]
    pub fn v6_only_strict() -> Self {
        Self {
            v6_only: true,
            ..Self::default_filled()
        }
    }

    fn default_filled() -> Self {
        Self {
            v6_only: false,
            reuseaddr: true,
            backlog: 1024,
        }
    }
}

/// Bind a TCP listener using dual-stack semantics.
///
/// When `addr` is a v6 wildcard (`::`) and `opts.v6_only` is
/// `false` (the default), the listener accepts both v4 and v6
/// clients via v4-mapped addresses on platforms that support it
/// (Linux, macOS, *BSD).
///
/// # Errors
///
/// Returns the underlying `io::Error` from `socket(2)`,
/// `setsockopt(2)`, `bind(2)`, or `listen(2)`.
///
/// # Examples
///
/// ```
/// use dynomite::net::listener::{bind_dual_stack, BindOptions};
/// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
/// let addr: std::net::SocketAddr = "[::1]:0".parse().unwrap();
/// let l = bind_dual_stack(addr, BindOptions::default()).unwrap();
/// assert!(l.local_addr().unwrap().is_ipv6());
/// # });
/// ```
pub fn bind_dual_stack(addr: SocketAddr, opts: BindOptions) -> io::Result<TcpListener> {
    let opts = if opts.backlog == 0 {
        BindOptions {
            backlog: 1024,
            ..opts
        }
    } else {
        opts
    };

    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_nonblocking(true)?;
    if opts.reuseaddr {
        socket.set_reuse_address(true)?;
    }
    if addr.is_ipv6() {
        // The default on most platforms accepts both v4 and v6
        // clients when bound to `[::]`. The caller can flip
        // `v6_only_strict` to opt out.
        socket.set_only_v6(opts.v6_only)?;
    }
    socket.bind(&addr.into())?;
    socket.listen(opts.backlog)?;
    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[tokio::test]
    async fn bind_v4_loopback() {
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        let l = bind_dual_stack(addr, BindOptions::default()).unwrap();
        assert!(l.local_addr().unwrap().is_ipv4());
    }
}
