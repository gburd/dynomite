//! Message type discriminant.
//!
//! Every datastore-bound request and response carries a `MsgType` tag
//! that identifies which command or reply class the message belongs
//! to. The variants are paired one-for-one with the entries in the
//! reference engine's `MSG_TYPE_CODEC` X-macro: order, count, and
//! string spellings all line up so the integer indices and the names
//! returned by [`MsgType::name`] remain compatible across the two
//! implementations.
//!
//! The enum is exhaustive: 182 named variants plus the trailing
//! `EndIdx` sentinel. Helpers are provided to round-trip integer
//! indices and to classify a tag as a request or a response.

use core::fmt;

macro_rules! define_msg_types {
    ($( ($variant:ident, $name:literal) ),+ $(,)?) => {
        /// Message type discriminant.
        ///
        /// Variants enumerate every datastore command and response
        /// class supported by the engine, in declaration order.
        ///
        /// # Examples
        ///
        /// ```
        /// use dynomite::msg::MsgType;
        ///
        /// assert_eq!(MsgType::Unknown.as_index(), 0);
        /// assert_eq!(MsgType::ReqMcGet.name(), "REQ_MC_GET");
        /// assert!(MsgType::ReqRedisGet.is_request());
        /// ```
        #[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
        #[non_exhaustive]
        pub enum MsgType {
            $(
                #[doc = concat!("`", $name, "`")]
                $variant,
            )+
        }

        impl MsgType {
            const ALL: &'static [MsgType] = &[ $( MsgType::$variant, )+ ];
            const NAMES: &'static [&'static str] = &[ $( $name, )+ ];
        }
    };
}

define_msg_types![
    (Unknown, "UNKNOWN"),
    (ReqMcGet, "REQ_MC_GET"),
    (ReqMcGets, "REQ_MC_GETS"),
    (ReqMcDelete, "REQ_MC_DELETE"),
    (ReqMcCas, "REQ_MC_CAS"),
    (ReqMcSet, "REQ_MC_SET"),
    (ReqMcAdd, "REQ_MC_ADD"),
    (ReqMcReplace, "REQ_MC_REPLACE"),
    (ReqMcAppend, "REQ_MC_APPEND"),
    (ReqMcPrepend, "REQ_MC_PREPEND"),
    (ReqMcIncr, "REQ_MC_INCR"),
    (ReqMcDecr, "REQ_MC_DECR"),
    (ReqMcTouch, "REQ_MC_TOUCH"),
    (ReqMcQuit, "REQ_MC_QUIT"),
    (RspMcNum, "RSP_MC_NUM"),
    (RspMcStored, "RSP_MC_STORED"),
    (RspMcNotStored, "RSP_MC_NOT_STORED"),
    (RspMcExists, "RSP_MC_EXISTS"),
    (RspMcNotFound, "RSP_MC_NOT_FOUND"),
    (RspMcEnd, "RSP_MC_END"),
    (RspMcValue, "RSP_MC_VALUE"),
    (RspMcDeleted, "RSP_MC_DELETED"),
    (RspMcTouched, "RSP_MC_TOUCHED"),
    (RspMcError, "RSP_MC_ERROR"),
    (RspMcClientError, "RSP_MC_CLIENT_ERROR"),
    (RspMcServerError, "RSP_MC_SERVER_ERROR"),
    (ReqRedisDel, "REQ_REDIS_DEL"),
    (ReqRedisExists, "REQ_REDIS_EXISTS"),
    (ReqRedisExpire, "REQ_REDIS_EXPIRE"),
    (ReqRedisExpireat, "REQ_REDIS_EXPIREAT"),
    (ReqRedisPexpire, "REQ_REDIS_PEXPIRE"),
    (ReqRedisPexpireat, "REQ_REDIS_PEXPIREAT"),
    (ReqRedisPersist, "REQ_REDIS_PERSIST"),
    (ReqRedisPttl, "REQ_REDIS_PTTL"),
    (ReqRedisScan, "REQ_REDIS_SCAN"),
    (ReqRedisSort, "REQ_REDIS_SORT"),
    (ReqRedisTtl, "REQ_REDIS_TTL"),
    (ReqRedisType, "REQ_REDIS_TYPE"),
    (ReqRedisAppend, "REQ_REDIS_APPEND"),
    (ReqRedisBitcount, "REQ_REDIS_BITCOUNT"),
    (ReqRedisBitpos, "REQ_REDIS_BITPOS"),
    (ReqRedisDecr, "REQ_REDIS_DECR"),
    (ReqRedisDecrby, "REQ_REDIS_DECRBY"),
    (ReqRedisDump, "REQ_REDIS_DUMP"),
    (ReqRedisGet, "REQ_REDIS_GET"),
    (ReqRedisGetbit, "REQ_REDIS_GETBIT"),
    (ReqRedisGetrange, "REQ_REDIS_GETRANGE"),
    (ReqRedisGetset, "REQ_REDIS_GETSET"),
    (ReqRedisIncr, "REQ_REDIS_INCR"),
    (ReqRedisIncrby, "REQ_REDIS_INCRBY"),
    (ReqRedisIncrbyfloat, "REQ_REDIS_INCRBYFLOAT"),
    (ReqRedisMset, "REQ_REDIS_MSET"),
    (ReqRedisMget, "REQ_REDIS_MGET"),
    (ReqRedisPsetex, "REQ_REDIS_PSETEX"),
    (ReqRedisRestore, "REQ_REDIS_RESTORE"),
    (ReqRedisSet, "REQ_REDIS_SET"),
    (ReqRedisSetbit, "REQ_REDIS_SETBIT"),
    (ReqRedisSetex, "REQ_REDIS_SETEX"),
    (ReqRedisSetnx, "REQ_REDIS_SETNX"),
    (ReqRedisSetrange, "REQ_REDIS_SETRANGE"),
    (ReqRedisStrlen, "REQ_REDIS_STRLEN"),
    (ReqRedisHdel, "REQ_REDIS_HDEL"),
    (ReqRedisHexists, "REQ_REDIS_HEXISTS"),
    (ReqRedisHget, "REQ_REDIS_HGET"),
    (ReqRedisHgetall, "REQ_REDIS_HGETALL"),
    (ReqRedisHincrby, "REQ_REDIS_HINCRBY"),
    (ReqRedisHincrbyfloat, "REQ_REDIS_HINCRBYFLOAT"),
    (ReqRedisHkeys, "REQ_REDIS_HKEYS"),
    (ReqRedisHlen, "REQ_REDIS_HLEN"),
    (ReqRedisHmget, "REQ_REDIS_HMGET"),
    (ReqRedisHmset, "REQ_REDIS_HMSET"),
    (ReqRedisHset, "REQ_REDIS_HSET"),
    (ReqRedisHsetnx, "REQ_REDIS_HSETNX"),
    (ReqRedisHscan, "REQ_REDIS_HSCAN"),
    (ReqRedisHvals, "REQ_REDIS_HVALS"),
    (ReqRedisHstrlen, "REQ_REDIS_HSTRLEN"),
    (ReqRedisKeys, "REQ_REDIS_KEYS"),
    (ReqRedisInfo, "REQ_REDIS_INFO"),
    (ReqRedisLindex, "REQ_REDIS_LINDEX"),
    (ReqRedisLinsert, "REQ_REDIS_LINSERT"),
    (ReqRedisLlen, "REQ_REDIS_LLEN"),
    (ReqRedisLpop, "REQ_REDIS_LPOP"),
    (ReqRedisLpush, "REQ_REDIS_LPUSH"),
    (ReqRedisLpushx, "REQ_REDIS_LPUSHX"),
    (ReqRedisLrange, "REQ_REDIS_LRANGE"),
    (ReqRedisLrem, "REQ_REDIS_LREM"),
    (ReqRedisLset, "REQ_REDIS_LSET"),
    (ReqRedisLtrim, "REQ_REDIS_LTRIM"),
    (ReqRedisPing, "REQ_REDIS_PING"),
    (ReqRedisQuit, "REQ_REDIS_QUIT"),
    (ReqRedisRpop, "REQ_REDIS_RPOP"),
    (ReqRedisRpoplpush, "REQ_REDIS_RPOPLPUSH"),
    (ReqRedisRpush, "REQ_REDIS_RPUSH"),
    (ReqRedisRpushx, "REQ_REDIS_RPUSHX"),
    (ReqRedisSadd, "REQ_REDIS_SADD"),
    (ReqRedisScard, "REQ_REDIS_SCARD"),
    (ReqRedisSdiff, "REQ_REDIS_SDIFF"),
    (ReqRedisSdiffstore, "REQ_REDIS_SDIFFSTORE"),
    (ReqRedisSinter, "REQ_REDIS_SINTER"),
    (ReqRedisSinterstore, "REQ_REDIS_SINTERSTORE"),
    (ReqRedisSismember, "REQ_REDIS_SISMEMBER"),
    (ReqRedisSlaveof, "REQ_REDIS_SLAVEOF"),
    (ReqRedisSmembers, "REQ_REDIS_SMEMBERS"),
    (ReqRedisSmove, "REQ_REDIS_SMOVE"),
    (ReqRedisSpop, "REQ_REDIS_SPOP"),
    (ReqRedisSrandmember, "REQ_REDIS_SRANDMEMBER"),
    (ReqRedisSrem, "REQ_REDIS_SREM"),
    (ReqRedisSunion, "REQ_REDIS_SUNION"),
    (ReqRedisSunionstore, "REQ_REDIS_SUNIONSTORE"),
    (ReqRedisSscan, "REQ_REDIS_SSCAN"),
    (ReqRedisZadd, "REQ_REDIS_ZADD"),
    (ReqRedisZcard, "REQ_REDIS_ZCARD"),
    (ReqRedisZcount, "REQ_REDIS_ZCOUNT"),
    (ReqRedisZincrby, "REQ_REDIS_ZINCRBY"),
    (ReqRedisZinterstore, "REQ_REDIS_ZINTERSTORE"),
    (ReqRedisZlexcount, "REQ_REDIS_ZLEXCOUNT"),
    (ReqRedisZrange, "REQ_REDIS_ZRANGE"),
    (ReqRedisZrangebylex, "REQ_REDIS_ZRANGEBYLEX"),
    (ReqRedisZrangebyscore, "REQ_REDIS_ZRANGEBYSCORE"),
    (ReqRedisZrank, "REQ_REDIS_ZRANK"),
    (ReqRedisZrem, "REQ_REDIS_ZREM"),
    (ReqRedisZremrangebyrank, "REQ_REDIS_ZREMRANGEBYRANK"),
    (ReqRedisZremrangebylex, "REQ_REDIS_ZREMRANGEBYLEX"),
    (ReqRedisZremrangebyscore, "REQ_REDIS_ZREMRANGEBYSCORE"),
    (ReqRedisZrevrange, "REQ_REDIS_ZREVRANGE"),
    (ReqRedisZrevrangebylex, "REQ_REDIS_ZREVRANGEBYLEX"),
    (ReqRedisZrevrangebyscore, "REQ_REDIS_ZREVRANGEBYSCORE"),
    (ReqRedisZrevrank, "REQ_REDIS_ZREVRANK"),
    (ReqRedisZscore, "REQ_REDIS_ZSCORE"),
    (ReqRedisZunionstore, "REQ_REDIS_ZUNIONSTORE"),
    (ReqRedisZscan, "REQ_REDIS_ZSCAN"),
    (ReqRedisEval, "REQ_REDIS_EVAL"),
    (ReqRedisEvalsha, "REQ_REDIS_EVALSHA"),
    (ReqRedisGeoadd, "REQ_REDIS_GEOADD"),
    (ReqRedisGeoradius, "REQ_REDIS_GEORADIUS"),
    (ReqRedisGeodist, "REQ_REDIS_GEODIST"),
    (ReqRedisGeohash, "REQ_REDIS_GEOHASH"),
    (ReqRedisGeopos, "REQ_REDIS_GEOPOS"),
    (ReqRedisGeoradiusbymember, "REQ_REDIS_GEORADIUSBYMEMBER"),
    (ReqRedisUnlink, "REQ_REDIS_UNLINK"),
    (ReqRedisJsonset, "REQ_REDIS_JSONSET"),
    (ReqRedisJsonget, "REQ_REDIS_JSONGET"),
    (ReqRedisJsondel, "REQ_REDIS_JSONDEL"),
    (ReqRedisJsontype, "REQ_REDIS_JSONTYPE"),
    (ReqRedisJsonmget, "REQ_REDIS_JSONMGET"),
    (ReqRedisJsonarrappend, "REQ_REDIS_JSONARRAPPEND"),
    (ReqRedisJsonarrinsert, "REQ_REDIS_JSONARRINSERT"),
    (ReqRedisJsonarrlen, "REQ_REDIS_JSONARRLEN"),
    (ReqRedisJsonobjkeys, "REQ_REDIS_JSONOBJKEYS"),
    (ReqRedisJsonobjlen, "REQ_REDIS_JSONOBJLEN"),
    (ReqRedisPfadd, "REQ_REDIS_PFADD"),
    (ReqRedisPfcount, "REQ_REDIS_PFCOUNT"),
    (ReqRedisConfig, "REQ_REDIS_CONFIG"),
    (ReqRedisScript, "REQ_REDIS_SCRIPT"),
    (ReqRedisScriptLoad, "REQ_REDIS_SCRIPT_LOAD"),
    (ReqRedisScriptExists, "REQ_REDIS_SCRIPT_EXISTS"),
    (ReqRedisScriptFlush, "REQ_REDIS_SCRIPT_FLUSH"),
    (ReqRedisScriptKill, "REQ_REDIS_SCRIPT_KILL"),
    (RspRedisStatus, "RSP_REDIS_STATUS"),
    (RspRedisInteger, "RSP_REDIS_INTEGER"),
    (RspRedisBulk, "RSP_REDIS_BULK"),
    (RspRedisMultibulk, "RSP_REDIS_MULTIBULK"),
    (RspRedisError, "RSP_REDIS_ERROR"),
    (RspRedisErrorErr, "RSP_REDIS_ERROR_ERR"),
    (RspRedisErrorOom, "RSP_REDIS_ERROR_OOM"),
    (RspRedisErrorBusy, "RSP_REDIS_ERROR_BUSY"),
    (RspRedisErrorNoauth, "RSP_REDIS_ERROR_NOAUTH"),
    (RspRedisErrorLoading, "RSP_REDIS_ERROR_LOADING"),
    (RspRedisErrorBusykey, "RSP_REDIS_ERROR_BUSYKEY"),
    (RspRedisErrorMisconf, "RSP_REDIS_ERROR_MISCONF"),
    (RspRedisErrorNoscript, "RSP_REDIS_ERROR_NOSCRIPT"),
    (RspRedisErrorReadonly, "RSP_REDIS_ERROR_READONLY"),
    (RspRedisErrorWrongtype, "RSP_REDIS_ERROR_WRONGTYPE"),
    (RspRedisErrorExecabort, "RSP_REDIS_ERROR_EXECABORT"),
    (RspRedisErrorMasterdown, "RSP_REDIS_ERROR_MASTERDOWN"),
    (RspRedisErrorNoreplicas, "RSP_REDIS_ERROR_NOREPLICAS"),
    (HackSettingConnConsistency, "HACK_SETTING_CONN_CONSISTENCY"),
    (Sentinel, "SENTINEL"),
    (ReqRedisFtCreate, "REQ_REDIS_FT_CREATE"),
    (ReqRedisFtSearch, "REQ_REDIS_FT_SEARCH"),
    (ReqRedisFtInfo, "REQ_REDIS_FT_INFO"),
    (ReqRedisFtList, "REQ_REDIS_FT_LIST"),
    (ReqRedisFtDropindex, "REQ_REDIS_FT_DROPINDEX"),
    (ReqRedisFtRegex, "REQ_REDIS_FT_REGEX"),
    (ReqRedisFtSugadd, "REQ_REDIS_FT_SUGADD"),
    (ReqRedisFtSugget, "REQ_REDIS_FT_SUGGET"),
    (ReqRedisFtSugdel, "REQ_REDIS_FT_SUGDEL"),
    (ReqRedisFtSuglen, "REQ_REDIS_FT_SUGLEN"),
    (ReqRedisFtUnknown, "REQ_REDIS_FT_UNKNOWN"),
    (EndIdx, "END_IDX"),
];

impl MsgType {
    /// Number of declared variants, including `Unknown`, `Sentinel`,
    /// and `EndIdx`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgType;
    /// assert!(MsgType::COUNT > 100);
    /// ```
    pub const COUNT: usize = Self::ALL.len();

    /// Integer index of this variant. Matches the integer value the
    /// reference engine assigns to the corresponding `MSG_TYPE_CODEC`
    /// entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgType;
    /// assert_eq!(MsgType::Unknown.as_index(), 0);
    /// assert_eq!(MsgType::ReqMcGet.as_index(), 1);
    /// ```
    #[must_use]
    pub const fn as_index(self) -> u32 {
        self as u32
    }

    /// Recover the variant from its integer index. Returns `None`
    /// when `index` is out of range.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgType;
    ///
    /// assert_eq!(MsgType::from_index(0), Some(MsgType::Unknown));
    /// assert_eq!(MsgType::from_index(MsgType::COUNT as u32), None);
    /// ```
    #[must_use]
    pub fn from_index(index: u32) -> Option<MsgType> {
        Self::ALL.get(index as usize).copied()
    }

    /// Canonical uppercase name as it appears in the C source.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgType;
    /// assert_eq!(MsgType::ReqRedisGet.name(), "REQ_REDIS_GET");
    /// assert_eq!(MsgType::EndIdx.name(), "END_IDX");
    /// ```
    #[must_use]
    pub fn name(&self) -> &'static str {
        Self::NAMES[self.as_index() as usize]
    }

    /// True when this variant identifies a datastore request
    /// (`REQ_*`).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgType;
    /// assert!(MsgType::ReqMcGet.is_request());
    /// assert!(!MsgType::RspMcStored.is_request());
    /// assert!(!MsgType::Unknown.is_request());
    /// ```
    #[must_use]
    pub fn is_request(&self) -> bool {
        self.name().starts_with("REQ_")
    }

    /// True when this variant identifies a datastore response
    /// (`RSP_*`).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgType;
    /// assert!(MsgType::RspRedisBulk.is_response());
    /// assert!(!MsgType::ReqMcGet.is_response());
    /// ```
    #[must_use]
    pub fn is_response(&self) -> bool {
        self.name().starts_with("RSP_")
    }
}

impl fmt::Display for MsgType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_is_zero() {
        assert_eq!(MsgType::Unknown.as_index(), 0);
        assert_eq!(MsgType::Unknown.name(), "UNKNOWN");
    }

    #[test]
    fn end_idx_is_last() {
        assert_eq!(
            MsgType::EndIdx.as_index() + 1,
            u32::try_from(MsgType::COUNT).unwrap(),
        );
    }

    #[test]
    fn from_index_round_trip() {
        let count_u32 = u32::try_from(MsgType::COUNT).unwrap();
        for i in 0..count_u32 {
            let ty = MsgType::from_index(i).unwrap();
            assert_eq!(ty.as_index(), i);
        }
        assert!(MsgType::from_index(count_u32).is_none());
        assert!(MsgType::from_index(u32::MAX).is_none());
    }

    #[test]
    fn classification_partition() {
        let count_u32 = u32::try_from(MsgType::COUNT).unwrap();
        for i in 0..count_u32 {
            let ty = MsgType::from_index(i).unwrap();
            // Each variant is at most one of request/response.
            assert!(!(ty.is_request() && ty.is_response()));
        }
        assert!(MsgType::ReqMcSet.is_request());
        assert!(MsgType::RspRedisStatus.is_response());
        assert!(!MsgType::Sentinel.is_request());
        assert!(!MsgType::Sentinel.is_response());
    }

    #[test]
    fn names_are_unique_uppercase() {
        let mut seen = std::collections::HashSet::new();
        for &name in MsgType::NAMES {
            assert!(name.chars().all(|c| c.is_ascii_uppercase() || c == '_'));
            assert!(seen.insert(name), "duplicate name: {name}");
        }
    }
}
