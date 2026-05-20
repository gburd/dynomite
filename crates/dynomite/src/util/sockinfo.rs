//! Socket address resolution.
//!
//! The C engine carries a `struct sockinfo` that unions
//! `sockaddr_in`, `sockaddr_in6`, and `sockaddr_un` with explicit
//! family/length fields. Rust's [`SocketAddr`] already covers the
//! Internet families directly; UNIX domain sockets are represented as
//! a path-bearing variant. The wrapper is mostly a typed parser that
//! mirrors `dn_resolve` and friends.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;

use crate::core::types::DynError;
use crate::util::atoi::valid_port;

/// Address-family-tagged socket endpoint.
///
/// # Examples
///
/// ```
/// use dynomite::util::sockinfo::SockInfo;
/// let info = SockInfo::resolve("127.0.0.1", 8101).unwrap();
/// assert!(info.is_inet());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SockInfo {
    /// IPv4 endpoint.
    Inet(SocketAddr),
    /// IPv6 endpoint.
    Inet6(SocketAddr),
    /// UNIX domain socket path.
    Unix(PathBuf),
}

impl SockInfo {
    /// Whether this endpoint refers to an Internet socket (v4 or v6).
    pub fn is_inet(&self) -> bool {
        matches!(self, Self::Inet(_) | Self::Inet6(_))
    }

    /// Return the underlying [`SocketAddr`] when the endpoint is an
    /// Internet socket.
    pub fn as_socket_addr(&self) -> Option<SocketAddr> {
        match self {
            Self::Inet(s) | Self::Inet6(s) => Some(*s),
            Self::Unix(_) => None,
        }
    }

    /// Resolve a hostname or literal IP plus port into a [`SockInfo`].
    ///
    /// Names that begin with `/` are treated as UNIX domain socket
    /// paths and are returned unmodified. Everything else is fed to
    /// [`std::net::ToSocketAddrs`]; the first matching entry wins,
    /// mirroring the behavior of `dn_resolve_inet` in the C reference.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::util::sockinfo::SockInfo;
    /// let v4 = SockInfo::resolve("127.0.0.1", 6379).unwrap();
    /// assert!(v4.is_inet());
    /// let unix = SockInfo::resolve("/var/run/foo.sock", 0).unwrap();
    /// assert!(!unix.is_inet());
    /// ```
    pub fn resolve(name: &str, port: u16) -> Result<Self, DynError> {
        if name.starts_with('/') {
            return Ok(Self::Unix(PathBuf::from(name)));
        }
        if !valid_port(i32::from(port)) {
            return Err(DynError::generic(format!("invalid port: {port}")));
        }
        let mut addrs = (name, port).to_socket_addrs().map_err(DynError::Io)?;
        let addr = addrs
            .next()
            .ok_or_else(|| DynError::generic(format!("no address for {name}:{port}")))?;
        Ok(match addr.ip() {
            IpAddr::V4(_) => Self::Inet(addr),
            IpAddr::V6(_) => Self::Inet6(addr),
        })
    }

    /// Construct a `SockInfo` from an explicit IPv4 address and port.
    pub fn from_v4(addr: Ipv4Addr, port: u16) -> Self {
        Self::Inet(SocketAddr::new(IpAddr::V4(addr), port))
    }

    /// Construct a `SockInfo` from an explicit IPv6 address and port.
    pub fn from_v6(addr: Ipv6Addr, port: u16) -> Self {
        Self::Inet6(SocketAddr::new(IpAddr::V6(addr), port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_loopback_v4() {
        let s = SockInfo::resolve("127.0.0.1", 6379).unwrap();
        assert_eq!(
            s.as_socket_addr().unwrap().ip(),
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))
        );
        assert!(matches!(s, SockInfo::Inet(_)));
    }

    #[test]
    fn resolve_loopback_v6() {
        let s = SockInfo::resolve("::1", 6379).unwrap();
        assert!(matches!(s, SockInfo::Inet6(_)));
    }

    #[test]
    fn unix_socket_passthrough() {
        let s = SockInfo::resolve("/tmp/x.sock", 0).unwrap();
        assert!(matches!(s, SockInfo::Unix(_)));
        assert!(!s.is_inet());
    }

    #[test]
    fn invalid_port_is_rejected() {
        let err = SockInfo::resolve("127.0.0.1", 0).unwrap_err();
        assert!(err.to_string().contains("invalid port"));
    }
}
