//! Typed enums for configuration values that the C parser stored as
//! free-form strings or small integer codes.

use std::fmt;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};

use super::error::ConfError;

macro_rules! string_enum_serde {
    ($t:ty) => {
        impl Serialize for $t {
            fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
                ser.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $t {
            fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
                struct V;
                impl Visitor<'_> for V {
                    type Value = $t;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str(concat!("a string naming a ", stringify!($t)))
                    }
                    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                        <$t>::parse(v).map_err(|e| E::custom(e.to_string()))
                    }
                    fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                        self.visit_str(&v)
                    }
                }
                de.deserialize_str(V)
            }
        }
    };
}

string_enum_serde!(SecureServerOption);
string_enum_serde!(HashType);

/// Datastore family selected by `data_store:`.
///
/// # Examples
///
/// ```
/// use dynomite::conf::DataStore;
/// assert_eq!(DataStore::from_int(0).unwrap(), DataStore::Redis);
/// assert_eq!(DataStore::Redis.as_int(), 0);
/// assert_eq!(DataStore::from_name("noxu").unwrap(), DataStore::Noxu);
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum DataStore {
    /// Redis (RESP) datastore. Encoded as `0` in YAML.
    Redis,
    /// Memcached ASCII datastore. Encoded as `1` in YAML.
    Memcache,
    /// In-process Noxu DB datastore (Riak-shaped). Encoded as
    /// `2` in YAML, or as the string `noxu`. Selecting this
    /// variant requires `dynomited` to be built with
    /// `--features riak` and a sibling `noxu_path:` knob.
    Noxu,
}

impl DataStore {
    /// Parse a `data_store:` value as it appears in YAML.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::DataStore;
    /// assert_eq!(DataStore::from_int(1).unwrap(), DataStore::Memcache);
    /// assert_eq!(DataStore::from_int(2).unwrap(), DataStore::Noxu);
    /// assert!(DataStore::from_int(7).is_err());
    /// ```
    pub fn from_int(v: i64) -> Result<Self, ConfError> {
        match v {
            0 => Ok(DataStore::Redis),
            1 => Ok(DataStore::Memcache),
            2 => Ok(DataStore::Noxu),
            n => Err(ConfError::BadDataStore(n)),
        }
    }

    /// Parse the textual form of a `data_store:` value, as
    /// accepted in YAML alongside the integer form.
    ///
    /// Comparison is case-insensitive against `redis`,
    /// `memcache`, `memcached`, and `noxu`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::DataStore;
    /// assert_eq!(DataStore::from_name("REDIS").unwrap(), DataStore::Redis);
    /// assert!(DataStore::from_name("sql").is_err());
    /// ```
    pub fn from_name(s: &str) -> Result<Self, ConfError> {
        if s.eq_ignore_ascii_case("redis") {
            Ok(DataStore::Redis)
        } else if s.eq_ignore_ascii_case("memcache") || s.eq_ignore_ascii_case("memcached") {
            Ok(DataStore::Memcache)
        } else if s.eq_ignore_ascii_case("noxu") {
            Ok(DataStore::Noxu)
        } else {
            Err(ConfError::BadDataStore(-1))
        }
    }

    /// Encode back to the small integer used in YAML.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::DataStore;
    /// assert_eq!(DataStore::Memcache.as_int(), 1);
    /// assert_eq!(DataStore::Noxu.as_int(), 2);
    /// ```
    pub fn as_int(self) -> i64 {
        match self {
            DataStore::Redis => 0,
            DataStore::Memcache => 1,
            DataStore::Noxu => 2,
        }
    }

    /// Return the canonical lower-case textual name.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::DataStore;
    /// assert_eq!(DataStore::Noxu.as_name(), "noxu");
    /// ```
    pub fn as_name(self) -> &'static str {
        match self {
            DataStore::Redis => "redis",
            DataStore::Memcache => "memcache",
            DataStore::Noxu => "noxu",
        }
    }
}

/// Inter-node security mode selected by `secure_server_option:`.
///
/// # Examples
///
/// ```
/// use dynomite::conf::SecureServerOption;
/// assert_eq!(
///     SecureServerOption::parse("datacenter").unwrap(),
///     SecureServerOption::Datacenter,
/// );
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum SecureServerOption {
    /// No inter-node TLS.
    None,
    /// TLS only between racks (within a DC).
    Rack,
    /// TLS only between datacenters.
    Datacenter,
    /// TLS between all nodes.
    All,
}

impl SecureServerOption {
    /// Parse a `secure_server_option:` value, case-sensitively.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::SecureServerOption;
    /// assert_eq!(SecureServerOption::parse("none").unwrap(), SecureServerOption::None);
    /// assert!(SecureServerOption::parse("NONE").is_err());
    /// ```
    pub fn parse(s: &str) -> Result<Self, ConfError> {
        match s {
            "none" => Ok(SecureServerOption::None),
            "rack" => Ok(SecureServerOption::Rack),
            "datacenter" => Ok(SecureServerOption::Datacenter),
            "all" => Ok(SecureServerOption::All),
            other => Err(ConfError::BadSecure(other.to_string())),
        }
    }

    /// Render back to the YAML string form.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::SecureServerOption;
    /// assert_eq!(SecureServerOption::All.as_str(), "all");
    /// ```
    pub fn as_str(self) -> &'static str {
        match self {
            SecureServerOption::None => "none",
            SecureServerOption::Rack => "rack",
            SecureServerOption::Datacenter => "datacenter",
            SecureServerOption::All => "all",
        }
    }
}

impl fmt::Display for SecureServerOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Quorum policy for read or write paths.
///
/// # Examples
///
/// ```
/// use dynomite::conf::ConsistencyLevel;
/// let lvl = ConsistencyLevel::parse("read_consistency", "DC_QUORUM").unwrap();
/// assert_eq!(lvl, ConsistencyLevel::DcQuorum);
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum ConsistencyLevel {
    /// Single replica acknowledgement.
    DcOne,
    /// Majority within a single datacenter.
    DcQuorum,
    /// Majority within a single datacenter with checksum repair.
    DcSafeQuorum,
    /// Majority within every datacenter, with checksum repair.
    DcEachSafeQuorum,
}

impl ConsistencyLevel {
    /// Parse a `read_consistency` or `write_consistency` value.
    ///
    /// Comparison is case-insensitive against the canonical names
    /// `DC_ONE`, `DC_QUORUM`, `DC_SAFE_QUORUM`, and
    /// `DC_EACH_SAFE_QUORUM`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConsistencyLevel;
    /// assert_eq!(
    ///     ConsistencyLevel::parse("read_consistency", "dc_one").unwrap(),
    ///     ConsistencyLevel::DcOne,
    /// );
    /// assert!(ConsistencyLevel::parse("read_consistency", "nope").is_err());
    /// ```
    pub fn parse(field: &'static str, s: &str) -> Result<Self, ConfError> {
        if s.eq_ignore_ascii_case("dc_one") {
            Ok(ConsistencyLevel::DcOne)
        } else if s.eq_ignore_ascii_case("dc_quorum") {
            Ok(ConsistencyLevel::DcQuorum)
        } else if s.eq_ignore_ascii_case("dc_safe_quorum") {
            Ok(ConsistencyLevel::DcSafeQuorum)
        } else if s.eq_ignore_ascii_case("dc_each_safe_quorum") {
            Ok(ConsistencyLevel::DcEachSafeQuorum)
        } else {
            Err(ConfError::BadConsistency {
                field,
                value: s.to_string(),
            })
        }
    }

    /// Render back to the canonical YAML name.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConsistencyLevel;
    /// assert_eq!(ConsistencyLevel::DcSafeQuorum.as_str(), "DC_SAFE_QUORUM");
    /// ```
    pub fn as_str(self) -> &'static str {
        match self {
            ConsistencyLevel::DcOne => "DC_ONE",
            ConsistencyLevel::DcQuorum => "DC_QUORUM",
            ConsistencyLevel::DcSafeQuorum => "DC_SAFE_QUORUM",
            ConsistencyLevel::DcEachSafeQuorum => "DC_EACH_SAFE_QUORUM",
        }
    }
}

impl fmt::Display for ConsistencyLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Hash algorithm selected by `hash:`.
///
/// The names mirror the algorithm tags accepted by the YAML parser.
/// Stage 3 owns the hashing math; this enum models only the configured
/// choice so the parser can echo it back without depending on the
/// hashkit module.
///
/// # Examples
///
/// ```
/// use dynomite::conf::HashType;
/// assert_eq!(HashType::parse("murmur3").unwrap(), HashType::Murmur3);
/// assert_eq!(HashType::Md5.as_str(), "md5");
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum HashType {
    /// One-at-a-time hash.
    OneAtATime,
    /// MD5 (truncated for ketama).
    Md5,
    /// CRC-16.
    Crc16,
    /// CRC-32.
    Crc32,
    /// CRC-32 ARM.
    Crc32a,
    /// 64-bit FNV-1.
    Fnv1_64,
    /// 64-bit FNV-1a.
    Fnv1a64,
    /// 32-bit FNV-1.
    Fnv1_32,
    /// 32-bit FNV-1a.
    Fnv1a32,
    /// Paul Hsieh's hash.
    Hsieh,
    /// Murmur hash (32-bit, version 1).
    Murmur,
    /// Bob Jenkins's hash.
    Jenkins,
    /// Murmur hash 3 (128-bit).
    Murmur3,
}

impl HashType {
    /// Parse a `hash:` value (case-sensitive).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::HashType;
    /// assert_eq!(HashType::parse("fnv1a_64").unwrap(), HashType::Fnv1a64);
    /// assert!(HashType::parse("FNV1A_64").is_err());
    /// ```
    pub fn parse(s: &str) -> Result<Self, ConfError> {
        Ok(match s {
            "one_at_a_time" => HashType::OneAtATime,
            "md5" => HashType::Md5,
            "crc16" => HashType::Crc16,
            "crc32" => HashType::Crc32,
            "crc32a" => HashType::Crc32a,
            "fnv1_64" => HashType::Fnv1_64,
            "fnv1a_64" => HashType::Fnv1a64,
            "fnv1_32" => HashType::Fnv1_32,
            "fnv1a_32" => HashType::Fnv1a32,
            "hsieh" => HashType::Hsieh,
            "murmur" => HashType::Murmur,
            "jenkins" => HashType::Jenkins,
            "murmur3" => HashType::Murmur3,
            other => return Err(ConfError::BadHash(other.to_string())),
        })
    }

    /// Render back to the canonical YAML name.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::HashType;
    /// assert_eq!(HashType::Crc32a.as_str(), "crc32a");
    /// ```
    pub fn as_str(self) -> &'static str {
        match self {
            HashType::OneAtATime => "one_at_a_time",
            HashType::Md5 => "md5",
            HashType::Crc16 => "crc16",
            HashType::Crc32 => "crc32",
            HashType::Crc32a => "crc32a",
            HashType::Fnv1_64 => "fnv1_64",
            HashType::Fnv1a64 => "fnv1a_64",
            HashType::Fnv1_32 => "fnv1_32",
            HashType::Fnv1a32 => "fnv1a_32",
            HashType::Hsieh => "hsieh",
            HashType::Murmur => "murmur",
            HashType::Jenkins => "jenkins",
            HashType::Murmur3 => "murmur3",
        }
    }
}

impl fmt::Display for HashType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_store_round_trip() {
        assert_eq!(DataStore::from_int(0).unwrap(), DataStore::Redis);
        assert_eq!(DataStore::from_int(1).unwrap(), DataStore::Memcache);
        assert_eq!(DataStore::from_int(2).unwrap(), DataStore::Noxu);
        assert!(matches!(
            DataStore::from_int(7),
            Err(ConfError::BadDataStore(7))
        ));
        assert_eq!(DataStore::from_name("noxu").unwrap(), DataStore::Noxu);
        assert_eq!(DataStore::from_name("REDIS").unwrap(), DataStore::Redis);
        assert!(DataStore::from_name("sql").is_err());
        assert_eq!(DataStore::Noxu.as_name(), "noxu");
    }

    #[test]
    fn secure_round_trip() {
        for s in ["none", "rack", "datacenter", "all"] {
            assert_eq!(SecureServerOption::parse(s).unwrap().as_str(), s);
        }
        assert!(SecureServerOption::parse("nope").is_err());
    }

    #[test]
    fn consistency_case_insensitive() {
        assert_eq!(
            ConsistencyLevel::parse("read_consistency", "dc_one").unwrap(),
            ConsistencyLevel::DcOne
        );
        assert_eq!(
            ConsistencyLevel::parse("read_consistency", "DC_SAFE_QUORUM").unwrap(),
            ConsistencyLevel::DcSafeQuorum
        );
        assert!(ConsistencyLevel::parse("read_consistency", "garbage").is_err());
    }

    #[test]
    fn hash_round_trip() {
        for &name in &[
            "one_at_a_time",
            "md5",
            "crc16",
            "crc32",
            "crc32a",
            "fnv1_64",
            "fnv1a_64",
            "fnv1_32",
            "fnv1a_32",
            "hsieh",
            "murmur",
            "jenkins",
            "murmur3",
        ] {
            assert_eq!(HashType::parse(name).unwrap().as_str(), name);
        }
    }
}
