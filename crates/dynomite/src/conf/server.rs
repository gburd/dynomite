//! Datastore server and dynomite peer seed parsing.
//!
//! `servers:` entries take the colon-delimited form
//! `host:port:weight [name]` (or `/path/sock:weight [name]` for a Unix
//! domain socket). `dyn_seeds:` entries take the longer form
//! `host:port:rack:dc:tokens [name]`. Both forms allow an optional
//! trailing space-delimited friendly name.

use std::fmt;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};

use super::error::ConfError;
use super::tokens::TokenList;

/// Default ketama port: when a server omits an explicit name and runs
/// on this port, the port is dropped from the consistent-hash key for
/// libmemcached compatibility. Mirrors `CONF_DEFAULT_KETAMA_PORT`.
const KETAMA_DEFAULT_PORT: u16 = 11_211;

/// A `servers:` entry: a single backing datastore endpoint.
///
/// # Examples
///
/// ```
/// use dynomite::conf::ConfServer;
/// let s = ConfServer::parse("127.0.0.1:6379:1 redis_a").unwrap();
/// assert_eq!(s.host(), "127.0.0.1");
/// assert_eq!(s.port(), 6379);
/// assert_eq!(s.weight(), 1);
/// assert_eq!(s.name(), "redis_a");
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfServer {
    pname: String,
    name: String,
    host: String,
    port: u16,
    weight: u32,
    is_unix: bool,
}

impl ConfServer {
    /// Parse a `host:port:weight [name]` (or `/path:weight [name]`) string.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfServer;
    /// let unix = ConfServer::parse("/tmp/redis.sock:1").unwrap();
    /// assert!(unix.is_unix());
    /// assert_eq!(unix.port(), 0);
    /// assert!(ConfServer::parse("").is_err());
    /// ```
    pub fn parse(raw: &str) -> Result<Self, ConfError> {
        let bad = |reason: &str| ConfError::BadServer {
            field: "servers",
            value: raw.to_string(),
            reason: reason.to_string(),
        };

        if raw.is_empty() {
            return Err(bad("empty value"));
        }

        let (head, friendly_name) = split_optional_friendly_name(raw);
        let head = head.trim_end();

        if let Some(rest) = head.strip_prefix('/') {
            // /path/socket:weight
            let (path_no_prefix, weight) =
                split_last_colon(rest).ok_or_else(|| bad("unix path requires ':weight' suffix"))?;
            let weight = parse_weight(weight).ok_or_else(|| bad("invalid weight"))?;
            let path = format!("/{path_no_prefix}");
            let name = friendly_name.map_or_else(|| path.clone(), str::to_string);
            let pname = head.to_string();
            return Ok(Self {
                pname,
                name,
                host: path,
                port: 0,
                weight,
                is_unix: true,
            });
        }

        // host:port:weight
        let (head_no_weight, weight_str) =
            split_last_colon(head).ok_or_else(|| bad("expected 'host:port:weight'"))?;
        let weight = parse_weight(weight_str).ok_or_else(|| bad("invalid weight"))?;

        let (host, port_str) =
            split_last_colon(head_no_weight).ok_or_else(|| bad("expected 'host:port:weight'"))?;
        let port = parse_port(port_str).ok_or_else(|| bad("port must be in 1..=65535"))?;
        if host.is_empty() {
            return Err(bad("empty host"));
        }

        let name = match friendly_name {
            Some(n) => n.to_string(),
            None => {
                if port == KETAMA_DEFAULT_PORT {
                    host.to_string()
                } else {
                    format!("{host}:{port_str}")
                }
            }
        };

        Ok(Self {
            pname: head.to_string(),
            name,
            host: host.to_string(),
            port,
            weight,
            is_unix: false,
        })
    }

    /// The original `host:port:weight` portion (without any friendly name).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfServer;
    /// let s = ConfServer::parse("127.0.0.1:6379:1 redis_a").unwrap();
    /// assert_eq!(s.pname(), "127.0.0.1:6379:1");
    /// ```
    pub fn pname(&self) -> &str {
        &self.pname
    }
    /// The hashing-key name for this server.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfServer;
    /// // Port 11211 is treated as a default and dropped from the name.
    /// assert_eq!(ConfServer::parse("10.0.0.1:11211:1").unwrap().name(), "10.0.0.1");
    /// assert_eq!(ConfServer::parse("10.0.0.1:6379:1").unwrap().name(), "10.0.0.1:6379");
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Hostname or IP address for an inet entry, or the Unix socket path.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfServer;
    /// assert_eq!(ConfServer::parse("127.0.0.1:6379:1").unwrap().host(), "127.0.0.1");
    /// ```
    pub fn host(&self) -> &str {
        &self.host
    }
    /// TCP port; `0` for Unix socket entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfServer;
    /// assert_eq!(ConfServer::parse("127.0.0.1:6379:1").unwrap().port(), 6379);
    /// assert_eq!(ConfServer::parse("/var/run/r.sock:1").unwrap().port(), 0);
    /// ```
    pub fn port(&self) -> u16 {
        self.port
    }
    /// Configured weight (parsed for backward compatibility; the engine
    /// ignores it once parsed).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfServer;
    /// assert_eq!(ConfServer::parse("127.0.0.1:6379:42").unwrap().weight(), 42);
    /// ```
    pub fn weight(&self) -> u32 {
        self.weight
    }
    /// Whether this entry refers to a Unix domain socket.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfServer;
    /// assert!(ConfServer::parse("/var/run/r.sock:1").unwrap().is_unix());
    /// assert!(!ConfServer::parse("127.0.0.1:6379:1").unwrap().is_unix());
    /// ```
    pub fn is_unix(&self) -> bool {
        self.is_unix
    }
}

impl fmt::Display for ConfServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.pname)
    }
}

impl Serialize for ConfServer {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&self.pname)
    }
}

impl<'de> Deserialize<'de> for ConfServer {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = ConfServer;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a 'host:port:weight' or '/path:weight' server entry")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                ConfServer::parse(v).map_err(|e| E::custom(e.to_string()))
            }
        }
        de.deserialize_str(V)
    }
}

/// A `dyn_seeds:` entry: a peer dynomite node with rack / dc / tokens.
///
/// # Examples
///
/// ```
/// use dynomite::conf::ConfDynSeed;
/// let s = ConfDynSeed::parse("127.0.0.2:8101:rack2:dc2:1383429731").unwrap();
/// assert_eq!(s.rack(), "rack2");
/// assert_eq!(s.dc(), "dc2");
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfDynSeed {
    pname: String,
    name: String,
    host: String,
    port: u16,
    rack: String,
    dc: String,
    tokens: TokenList,
}

impl ConfDynSeed {
    /// Parse a `host:port:rack:dc:tokens [name]` entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfDynSeed;
    /// let s = ConfDynSeed::parse("h:1:r:d:1,2,3 friendly").unwrap();
    /// assert_eq!(s.tokens().len(), 3);
    /// assert_eq!(s.name(), "friendly");
    /// assert!(ConfDynSeed::parse("a:b:c:d").is_err());
    /// ```
    pub fn parse(raw: &str) -> Result<Self, ConfError> {
        let bad = |reason: &str| ConfError::BadServer {
            field: "dyn_seeds",
            value: raw.to_string(),
            reason: reason.to_string(),
        };

        if raw.is_empty() {
            return Err(bad("empty value"));
        }

        let (head, friendly_name) = split_optional_friendly_name(raw);
        let head = head.trim_end();

        // tokens
        let (head, tokens_str) =
            split_last_colon(head).ok_or_else(|| bad("expected 'host:port:rack:dc:tokens'"))?;
        // dc
        let (head, dc) =
            split_last_colon(head).ok_or_else(|| bad("expected 'host:port:rack:dc:tokens'"))?;
        // rack
        let (head, rack) =
            split_last_colon(head).ok_or_else(|| bad("expected 'host:port:rack:dc:tokens'"))?;
        // port
        let (host, port_str) =
            split_last_colon(head).ok_or_else(|| bad("expected 'host:port:rack:dc:tokens'"))?;

        if host.is_empty() {
            return Err(bad("empty host"));
        }
        if rack.is_empty() {
            return Err(bad("empty rack"));
        }
        if dc.is_empty() {
            return Err(bad("empty dc"));
        }

        let port = parse_port(port_str).ok_or_else(|| bad("port must be in 1..=65535"))?;
        let tokens = TokenList::parse(tokens_str).map_err(|e| ConfError::BadServer {
            field: "dyn_seeds",
            value: raw.to_string(),
            reason: e.to_string(),
        })?;

        let name = match friendly_name {
            Some(n) => n.to_string(),
            None => {
                if port == KETAMA_DEFAULT_PORT {
                    host.to_string()
                } else {
                    format!("{host}:{port_str}")
                }
            }
        };

        Ok(Self {
            pname: head.to_string(),
            name,
            host: host.to_string(),
            port,
            rack: rack.to_string(),
            dc: dc.to_string(),
            tokens,
        })
    }

    /// The colon-joined `host:port` portion (rack, dc and tokens are
    /// stripped from the input during parsing).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfDynSeed;
    /// let s = ConfDynSeed::parse("h:1:r:d:1 friendly").unwrap();
    /// assert_eq!(s.pname(), "h:1");
    /// ```
    pub fn pname(&self) -> &str {
        &self.pname
    }
    /// Hashing-key name.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfDynSeed;
    /// assert_eq!(
    ///     ConfDynSeed::parse("h:1:r:d:1 friendly").unwrap().name(),
    ///     "friendly",
    /// );
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Hostname or IP.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfDynSeed;
    /// assert_eq!(ConfDynSeed::parse("node-a:1:r:d:1").unwrap().host(), "node-a");
    /// ```
    pub fn host(&self) -> &str {
        &self.host
    }
    /// TCP port.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfDynSeed;
    /// assert_eq!(ConfDynSeed::parse("h:8101:r:d:1").unwrap().port(), 8101);
    /// ```
    pub fn port(&self) -> u16 {
        self.port
    }
    /// Logical rack.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfDynSeed;
    /// assert_eq!(ConfDynSeed::parse("h:1:rack-x:d:1").unwrap().rack(), "rack-x");
    /// ```
    pub fn rack(&self) -> &str {
        &self.rack
    }
    /// Logical datacenter.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfDynSeed;
    /// assert_eq!(ConfDynSeed::parse("h:1:r:dc-x:1").unwrap().dc(), "dc-x");
    /// ```
    pub fn dc(&self) -> &str {
        &self.dc
    }
    /// Token list owned by this seed.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfDynSeed;
    /// assert_eq!(ConfDynSeed::parse("h:1:r:d:1,2,3").unwrap().tokens().len(), 3);
    /// ```
    pub fn tokens(&self) -> &TokenList {
        &self.tokens
    }
}

impl fmt::Display for ConfDynSeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.pname)
    }
}

impl Serialize for ConfDynSeed {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&self.pname)
    }
}

impl<'de> Deserialize<'de> for ConfDynSeed {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = ConfDynSeed;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a 'host:port:rack:dc:tokens' dyn_seeds entry")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                ConfDynSeed::parse(v).map_err(|e| E::custom(e.to_string()))
            }
        }
        de.deserialize_str(V)
    }
}

/// Strip the optional space-separated friendly name suffix from a
/// colon-delimited entry. Returns `(head, name)`.
fn split_optional_friendly_name(raw: &str) -> (&str, Option<&str>) {
    if let Some(idx) = raw.rfind(' ') {
        // Anything after the last space is the friendly name when the
        // head still contains a colon; the C parser is greedy on the
        // rightmost space.
        let (head, tail) = raw.split_at(idx);
        let tail = &tail[1..];
        if !tail.is_empty() && head.contains(':') {
            return (head, Some(tail));
        }
    }
    (raw, None)
}

fn split_last_colon(s: &str) -> Option<(&str, &str)> {
    let idx = s.rfind(':')?;
    Some((&s[..idx], &s[idx + 1..]))
}

fn parse_port(s: &str) -> Option<u16> {
    let n: u16 = s.parse().ok()?;
    if n > 0 {
        Some(n)
    } else {
        None
    }
}

fn parse_weight(s: &str) -> Option<u32> {
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_basic() {
        let s = ConfServer::parse("127.0.0.1:22122:1").unwrap();
        assert_eq!(s.host(), "127.0.0.1");
        assert_eq!(s.port(), 22122);
        assert_eq!(s.weight(), 1);
        assert_eq!(s.name(), "127.0.0.1:22122");
        assert!(!s.is_unix());
    }

    #[test]
    fn server_with_friendly_name() {
        let s = ConfServer::parse("127.0.0.1:6379:1 redis_a").unwrap();
        assert_eq!(s.host(), "127.0.0.1");
        assert_eq!(s.port(), 6379);
        assert_eq!(s.name(), "redis_a");
        assert_eq!(s.pname(), "127.0.0.1:6379:1");
    }

    #[test]
    fn server_default_ketama_port_drops_port_from_name() {
        let s = ConfServer::parse("10.0.0.1:11211:1").unwrap();
        assert_eq!(s.name(), "10.0.0.1");
    }

    #[test]
    fn server_unix_socket() {
        let s = ConfServer::parse("/tmp/redis.sock:1").unwrap();
        assert!(s.is_unix());
        assert_eq!(s.host(), "/tmp/redis.sock");
        assert_eq!(s.port(), 0);
    }

    #[test]
    fn server_bad_format() {
        assert!(ConfServer::parse("just-a-host").is_err());
        assert!(ConfServer::parse("a:b:c").is_err());
        assert!(ConfServer::parse("").is_err());
    }

    #[test]
    fn dyn_seed_basic() {
        let s = ConfDynSeed::parse("127.0.0.2:8101:rack2:dc2:1383429731").unwrap();
        assert_eq!(s.host(), "127.0.0.2");
        assert_eq!(s.port(), 8101);
        assert_eq!(s.rack(), "rack2");
        assert_eq!(s.dc(), "dc2");
        assert_eq!(s.tokens().to_string(), "1383429731");
    }

    #[test]
    fn dyn_seed_multi_tokens() {
        let s = ConfDynSeed::parse("h:1:r:d:1,2,3 friendly").unwrap();
        assert_eq!(s.tokens().len(), 3);
        assert_eq!(s.name(), "friendly");
    }

    #[test]
    fn dyn_seed_bad() {
        assert!(ConfDynSeed::parse("a:b:c:d").is_err());
        assert!(ConfDynSeed::parse("h:1:r::1").is_err());
    }
}
