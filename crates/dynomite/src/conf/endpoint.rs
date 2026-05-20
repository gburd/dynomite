//! `listen` / `dyn_listen` / `stats_listen` endpoint parsing.
//!
//! Endpoints are stringly-typed in YAML; we parse them into a typed
//! [`ConfListen`] preserving the original `pname` (the raw string) and
//! the host / port pieces. Both `host:port` and `[ipv6]:port` syntaxes
//! are accepted, plus bare IPv6 addresses split at the rightmost colon
//! (matching the C reference's `dn_strrchr(.., ':')` behavior). Unix
//! socket paths starting with `/` are also accepted.

use std::fmt;
use std::net::IpAddr;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};

use super::error::ConfError;

/// A parsed `listen:` / `dyn_listen:` / `stats_listen:` endpoint.
///
/// Resolution to a `sockinfo` is intentionally not represented here
/// because address resolution is deferred to the runtime layer.
///
/// # Examples
///
/// ```
/// use dynomite::conf::ConfListen;
/// let l = ConfListen::parse("listen", "127.0.0.1:8102").unwrap();
/// assert_eq!(l.name(), "127.0.0.1");
/// assert_eq!(l.port(), 8102);
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfListen {
    pname: String,
    name: String,
    port: u16,
    kind: EndpointKind,
}

/// Address family of a [`ConfListen`].
///
/// # Examples
///
/// ```
/// use dynomite::conf::{ConfListen, EndpointKind};
/// assert_eq!(
///     ConfListen::parse("listen", "[::1]:8101").unwrap().kind(),
///     EndpointKind::V6,
/// );
/// assert_eq!(
///     ConfListen::parse("listen", "/tmp/d.sock").unwrap().kind(),
///     EndpointKind::UnixPath,
/// );
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EndpointKind {
    /// IPv4 numeric address.
    V4,
    /// IPv6 numeric address.
    V6,
    /// DNS hostname; resolution is deferred.
    Hostname,
    /// Filesystem path to a Unix domain socket.
    UnixPath,
}

impl ConfListen {
    /// Parse a raw endpoint string for the named directive.
    ///
    /// `field` names the directive; it is folded into the error so
    /// callers can produce helpful diagnostics.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::{ConfListen, EndpointKind};
    /// let l = ConfListen::parse("dyn_listen", "node-1.example.com:8101").unwrap();
    /// assert_eq!(l.kind(), EndpointKind::Hostname);
    /// assert_eq!(l.port(), 8101);
    /// assert!(ConfListen::parse("dyn_listen", "node-1.example.com").is_err());
    /// ```
    pub fn parse(field: &'static str, raw: &str) -> Result<Self, ConfError> {
        if raw.is_empty() {
            return Err(ConfError::BadAddr {
                field,
                value: raw.to_string(),
                reason: "empty value".to_string(),
            });
        }
        if raw.starts_with('/') {
            return Ok(Self {
                pname: raw.to_string(),
                name: raw.to_string(),
                port: 0,
                kind: EndpointKind::UnixPath,
            });
        }

        let (host, port_str) = split_host_port(raw).ok_or_else(|| ConfError::BadAddr {
            field,
            value: raw.to_string(),
            reason: "missing 'host:port' separator".to_string(),
        })?;

        let port: u16 = match port_str.parse::<u16>() {
            Ok(p) if p > 0 => p,
            Ok(_) | Err(_) => {
                return Err(ConfError::BadAddr {
                    field,
                    value: raw.to_string(),
                    reason: "port must be a number in 1..=65535".to_string(),
                });
            }
        };

        let kind = classify_host(host).ok_or_else(|| ConfError::BadAddr {
            field,
            value: raw.to_string(),
            reason: "host portion is empty or malformed".to_string(),
        })?;

        Ok(Self {
            pname: raw.to_string(),
            name: host.to_string(),
            port,
            kind,
        })
    }

    /// The original textual value (`name:port`).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfListen;
    /// let l = ConfListen::parse("listen", "127.0.0.1:8102").unwrap();
    /// assert_eq!(l.pname(), "127.0.0.1:8102");
    /// ```
    pub fn pname(&self) -> &str {
        &self.pname
    }

    /// The host portion (without surrounding brackets, if any).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfListen;
    /// let l = ConfListen::parse("listen", "[::1]:8101").unwrap();
    /// assert_eq!(l.name(), "::1");
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The port number; `0` for Unix socket paths.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfListen;
    /// assert_eq!(ConfListen::parse("listen", "127.0.0.1:8102").unwrap().port(), 8102);
    /// assert_eq!(ConfListen::parse("listen", "/tmp/d.sock").unwrap().port(), 0);
    /// ```
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The endpoint kind classification.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::{ConfListen, EndpointKind};
    /// let l = ConfListen::parse("listen", "127.0.0.1:8102").unwrap();
    /// assert_eq!(l.kind(), EndpointKind::V4);
    /// ```
    pub fn kind(&self) -> EndpointKind {
        self.kind
    }
}

impl fmt::Display for ConfListen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.pname)
    }
}

impl Serialize for ConfListen {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&self.pname)
    }
}

impl<'de> Deserialize<'de> for ConfListen {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = ConfListen;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a 'host:port' or '[ipv6]:port' endpoint string")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                ConfListen::parse("listen", v).map_err(|e| E::custom(e.to_string()))
            }
        }
        de.deserialize_str(V)
    }
}

/// Split a `host:port` (or `[ipv6]:port`) string into its two halves.
///
/// Returns `None` if no colon separates the parts. For bracketed IPv6
/// addresses, the brackets are stripped from the returned host slice.
fn split_host_port(raw: &str) -> Option<(&str, &str)> {
    if let Some(rest) = raw.strip_prefix('[') {
        let close = rest.find(']')?;
        let host = &rest[..close];
        let after = &rest[close + 1..];
        let port = after.strip_prefix(':')?;
        if host.is_empty() || port.is_empty() {
            return None;
        }
        return Some((host, port));
    }

    let idx = raw.rfind(':')?;
    let (host, port) = raw.split_at(idx);
    let port = &port[1..];
    if host.is_empty() || port.is_empty() {
        return None;
    }
    Some((host, port))
}

fn classify_host(host: &str) -> Option<EndpointKind> {
    if host.is_empty() {
        return None;
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Some(match ip {
            IpAddr::V4(_) => EndpointKind::V4,
            IpAddr::V6(_) => EndpointKind::V6,
        });
    }
    if host
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.' || b == b'_')
    {
        Some(EndpointKind::Hostname)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv4_host_port() {
        let l = ConfListen::parse("listen", "127.0.0.1:8102").unwrap();
        assert_eq!(l.name(), "127.0.0.1");
        assert_eq!(l.port(), 8102);
        assert_eq!(l.kind(), EndpointKind::V4);
        assert_eq!(l.to_string(), "127.0.0.1:8102");
    }

    #[test]
    fn ipv6_bracketed() {
        let l = ConfListen::parse("listen", "[::1]:8101").unwrap();
        assert_eq!(l.name(), "::1");
        assert_eq!(l.port(), 8101);
        assert_eq!(l.kind(), EndpointKind::V6);
    }

    #[test]
    fn hostname_accepted() {
        let l = ConfListen::parse("listen", "node-1.example.com:22222").unwrap();
        assert_eq!(l.name(), "node-1.example.com");
        assert_eq!(l.port(), 22222);
        assert_eq!(l.kind(), EndpointKind::Hostname);
    }

    #[test]
    fn unix_path_accepted() {
        let l = ConfListen::parse("listen", "/tmp/dynomite.sock").unwrap();
        assert_eq!(l.kind(), EndpointKind::UnixPath);
        assert_eq!(l.port(), 0);
    }

    #[test]
    fn missing_port_rejected() {
        assert!(ConfListen::parse("listen", "127.0.0.1").is_err());
        assert!(ConfListen::parse("listen", "127.0.0.1:").is_err());
    }

    #[test]
    fn out_of_range_port_rejected() {
        assert!(ConfListen::parse("listen", "127.0.0.1:0").is_err());
        assert!(ConfListen::parse("listen", "127.0.0.1:99999").is_err());
    }

    #[test]
    fn malformed_ipv6_rejected() {
        assert!(ConfListen::parse("listen", "[::1:8101").is_err());
        assert!(ConfListen::parse("listen", "[]:8101").is_err());
    }
}
