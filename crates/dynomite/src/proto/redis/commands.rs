//! Redis command catalog and classification helpers.
//!
//! This module centralises the command lookup the parser uses
//! after it has read the keyword token, plus the predicate set the
//! state machine consults to decide how many arguments a command
//! takes. The classification mirrors the reference engine's
//! `redis_arg{0,1,2,3,n,x,kvx,upto1,eval,argz,error}` helpers.
//!
//! All lookups are case-insensitive ASCII.

use crate::msg::MsgType;

/// Argument-shape classification for a Redis request command.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CommandClass {
    /// Command takes no key (`PING`, `QUIT`, `SCRIPT FLUSH`, ...).
    Argz,
    /// Command takes exactly one key and zero arguments.
    Arg0,
    /// Command takes one key and exactly one argument.
    Arg1,
    /// Command takes one key and exactly two arguments.
    Arg2,
    /// Command takes one key and exactly three arguments.
    Arg3,
    /// Command takes one key and zero-or-more arguments (variadic).
    ArgN,
    /// Command takes one or more keys (`MGET`, `DEL`, `EXISTS`).
    ArgX,
    /// Command takes one or more key/value pairs (`MSET`).
    ArgKvX,
    /// Command takes one key and zero or one argument (`INFO`).
    ArgUpto1,
    /// Command is `EVAL` or `EVALSHA` (special two-arg + key list +
    /// args layout).
    ArgEval,
}

impl CommandClass {
    /// True when the command takes no key.
    #[must_use]
    pub fn is_argz(self) -> bool {
        matches!(self, Self::Argz)
    }

    /// True when the command is variadic over keys.
    #[must_use]
    pub fn is_argx(self) -> bool {
        matches!(self, Self::ArgX)
    }

    /// True when the command is variadic over key/value pairs.
    #[must_use]
    pub fn is_argkvx(self) -> bool {
        matches!(self, Self::ArgKvX)
    }

    /// True when the command is `EVAL` or `EVALSHA`.
    #[must_use]
    pub fn is_argeval(self) -> bool {
        matches!(self, Self::ArgEval)
    }
}

/// Classify a Redis request type into its argument-shape category.
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::redis::commands::{classify, CommandClass};
///
/// assert_eq!(classify(MsgType::ReqRedisGet), CommandClass::Arg0);
/// assert_eq!(classify(MsgType::ReqRedisSet), CommandClass::ArgN);
/// assert_eq!(classify(MsgType::ReqRedisMget), CommandClass::ArgX);
/// assert_eq!(classify(MsgType::ReqRedisMset), CommandClass::ArgKvX);
/// assert_eq!(classify(MsgType::ReqRedisEval), CommandClass::ArgEval);
/// assert_eq!(classify(MsgType::ReqRedisPing), CommandClass::Argz);
/// ```
#[must_use]
pub fn classify(ty: MsgType) -> CommandClass {
    use MsgType as M;
    match ty {
        // argz
        M::ReqRedisPing
        | M::ReqRedisQuit
        | M::ReqRedisScriptFlush
        | M::ReqRedisScriptKill => CommandClass::Argz,

        // arg0
        M::ReqRedisPersist
        | M::ReqRedisPttl
        | M::ReqRedisTtl
        | M::ReqRedisType
        | M::ReqRedisDump
        | M::ReqRedisDecr
        | M::ReqRedisGet
        | M::ReqRedisIncr
        | M::ReqRedisStrlen
        | M::ReqRedisHgetall
        | M::ReqRedisHkeys
        | M::ReqRedisHlen
        | M::ReqRedisHvals
        | M::ReqRedisLlen
        | M::ReqRedisLpop
        | M::ReqRedisRpop
        | M::ReqRedisScard
        | M::ReqRedisSmembers
        | M::ReqRedisSrandmember
        | M::ReqRedisZcard
        | M::ReqRedisKeys
        | M::ReqRedisPfcount => CommandClass::Arg0,

        // arg1
        M::ReqRedisExpire
        | M::ReqRedisExpireat
        | M::ReqRedisPexpire
        | M::ReqRedisPexpireat
        | M::ReqRedisAppend
        | M::ReqRedisDecrby
        | M::ReqRedisGetbit
        | M::ReqRedisGetset
        | M::ReqRedisIncrby
        | M::ReqRedisIncrbyfloat
        | M::ReqRedisSetnx
        | M::ReqRedisHexists
        | M::ReqRedisHget
        | M::ReqRedisLindex
        | M::ReqRedisLpushx
        | M::ReqRedisRpoplpush
        | M::ReqRedisRpushx
        | M::ReqRedisSismember
        | M::ReqRedisZrank
        | M::ReqRedisZrevrank
        | M::ReqRedisZscore
        | M::ReqRedisSlaveof
        | M::ReqRedisConfig
        | M::ReqRedisScriptLoad
        | M::ReqRedisScriptExists => CommandClass::Arg1,

        // arg2
        M::ReqRedisGetrange
        | M::ReqRedisPsetex
        | M::ReqRedisSetbit
        | M::ReqRedisSetex
        | M::ReqRedisSetrange
        | M::ReqRedisHincrby
        | M::ReqRedisHincrbyfloat
        | M::ReqRedisHset
        | M::ReqRedisHsetnx
        | M::ReqRedisLrange
        | M::ReqRedisLrem
        | M::ReqRedisLset
        | M::ReqRedisLtrim
        | M::ReqRedisSmove
        | M::ReqRedisZcount
        | M::ReqRedisZincrby
        | M::ReqRedisZlexcount
        | M::ReqRedisZremrangebylex
        | M::ReqRedisZremrangebyrank
        | M::ReqRedisZremrangebyscore
        | M::ReqRedisRestore => CommandClass::Arg2,

        // arg3
        M::ReqRedisLinsert => CommandClass::Arg3,

        // argn
        M::ReqRedisSort
        | M::ReqRedisBitcount
        | M::ReqRedisBitpos
        | M::ReqRedisSet
        | M::ReqRedisScan
        | M::ReqRedisHdel
        | M::ReqRedisHmget
        | M::ReqRedisHmset
        | M::ReqRedisHscan
        | M::ReqRedisLpush
        | M::ReqRedisRpush
        | M::ReqRedisSadd
        | M::ReqRedisSdiff
        | M::ReqRedisSdiffstore
        | M::ReqRedisSinter
        | M::ReqRedisSinterstore
        | M::ReqRedisSrem
        | M::ReqRedisSunion
        | M::ReqRedisSunionstore
        | M::ReqRedisSscan
        | M::ReqRedisSpop
        | M::ReqRedisZadd
        | M::ReqRedisZinterstore
        | M::ReqRedisZrange
        | M::ReqRedisZrangebyscore
        | M::ReqRedisZrem
        | M::ReqRedisZrevrange
        | M::ReqRedisZrangebylex
        | M::ReqRedisZrevrangebylex
        | M::ReqRedisZrevrangebyscore
        | M::ReqRedisZunionstore
        | M::ReqRedisZscan
        | M::ReqRedisPfadd
        | M::ReqRedisGeoadd
        | M::ReqRedisGeoradius
        | M::ReqRedisGeodist
        | M::ReqRedisGeohash
        | M::ReqRedisGeopos
        | M::ReqRedisGeoradiusbymember
        | M::ReqRedisJsonset
        | M::ReqRedisJsonget
        | M::ReqRedisJsondel
        | M::ReqRedisJsontype
        | M::ReqRedisJsonmget
        | M::ReqRedisJsonarrappend
        | M::ReqRedisJsonarrinsert
        | M::ReqRedisJsonarrlen
        | M::ReqRedisJsonobjkeys
        | M::ReqRedisJsonobjlen
        | M::ReqRedisUnlink => CommandClass::ArgN,

        // argx (multi-key)
        M::ReqRedisMget | M::ReqRedisDel | M::ReqRedisExists => CommandClass::ArgX,

        // argkvx (multi key/value)
        M::ReqRedisMset => CommandClass::ArgKvX,

        // argupto1
        M::ReqRedisInfo => CommandClass::ArgUpto1,

        // argeval
        M::ReqRedisEval | M::ReqRedisEvalsha => CommandClass::ArgEval,

        _ => CommandClass::Arg0,
    }
}

/// True when `ty` is a Redis error response variant.
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::redis::commands::is_redis_error;
///
/// assert!(is_redis_error(MsgType::RspRedisErrorErr));
/// assert!(!is_redis_error(MsgType::RspRedisStatus));
/// ```
#[must_use]
pub fn is_redis_error(ty: MsgType) -> bool {
    use MsgType as M;
    matches!(
        ty,
        M::RspRedisError
            | M::RspRedisErrorErr
            | M::RspRedisErrorOom
            | M::RspRedisErrorBusy
            | M::RspRedisErrorNoauth
            | M::RspRedisErrorLoading
            | M::RspRedisErrorBusykey
            | M::RspRedisErrorMisconf
            | M::RspRedisErrorNoscript
            | M::RspRedisErrorReadonly
            | M::RspRedisErrorWrongtype
            | M::RspRedisErrorExecabort
            | M::RspRedisErrorMasterdown
            | M::RspRedisErrorNoreplicas
    )
}

/// Routing override the parser stamps on a request based on its
/// command type. `None` means the default `Normal` routing.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct CommandTraits {
    /// True when the command is a read.
    pub is_read: bool,
    /// True when the command sets the `quit` flag.
    pub quit: bool,
    /// Routing override class, if any.
    pub routing: RoutingOverride,
}

/// Routing override stamped on a parsed request.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum RoutingOverride {
    /// No override; use the configured per-pool routing.
    #[default]
    None,
    /// Send to the local node only.
    LocalNodeOnly,
    /// Apply key hashing but stay within the local rack.
    TokenOwnerLocalRackOnly,
    /// Send to all nodes / racks / DCs.
    AllNodesAllRacksAllDcs,
}

/// Lookup a Redis command keyword (case-insensitive ASCII) and
/// return the message type plus its parser traits.
///
/// Returns `None` when the keyword is not a known command.
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::redis::commands::lookup;
///
/// let (ty, traits) = lookup(b"GET").unwrap();
/// assert_eq!(ty, MsgType::ReqRedisGet);
/// assert!(traits.is_read);
/// ```
#[must_use]
pub fn lookup(keyword: &[u8]) -> Option<(MsgType, CommandTraits)> {
    if keyword.is_empty() || keyword.len() > 32 {
        return None;
    }
    let mut buf = [0u8; 32];
    for (i, &b) in keyword.iter().enumerate() {
        buf[i] = b.to_ascii_lowercase();
    }
    let key = &buf[..keyword.len()];
    let (ty, is_read, quit, routing) = match key {
        // length 3
        b"get" => (MsgType::ReqRedisGet, true, false, RoutingOverride::None),
        b"set" => (MsgType::ReqRedisSet, false, false, RoutingOverride::None),
        b"ttl" => (MsgType::ReqRedisTtl, false, false, RoutingOverride::None),
        b"del" => (MsgType::ReqRedisDel, false, false, RoutingOverride::None),
        // length 4
        b"pttl" => (MsgType::ReqRedisPttl, true, false, RoutingOverride::None),
        b"decr" => (MsgType::ReqRedisDecr, false, false, RoutingOverride::None),
        b"dump" => (MsgType::ReqRedisDump, true, false, RoutingOverride::None),
        b"hdel" => (MsgType::ReqRedisHdel, false, false, RoutingOverride::None),
        b"hget" => (MsgType::ReqRedisHget, true, false, RoutingOverride::None),
        b"hlen" => (MsgType::ReqRedisHlen, true, false, RoutingOverride::None),
        b"hset" => (MsgType::ReqRedisHset, false, false, RoutingOverride::None),
        b"incr" => (MsgType::ReqRedisIncr, false, false, RoutingOverride::None),
        b"keys" => (
            MsgType::ReqRedisKeys,
            true,
            false,
            RoutingOverride::LocalNodeOnly,
        ),
        b"info" => (
            MsgType::ReqRedisInfo,
            true,
            false,
            RoutingOverride::LocalNodeOnly,
        ),
        b"llen" => (MsgType::ReqRedisLlen, true, false, RoutingOverride::None),
        b"lpop" => (MsgType::ReqRedisLpop, false, false, RoutingOverride::None),
        b"lrem" => (MsgType::ReqRedisLrem, false, false, RoutingOverride::None),
        b"lset" => (MsgType::ReqRedisLset, false, false, RoutingOverride::None),
        b"mget" => (MsgType::ReqRedisMget, true, false, RoutingOverride::None),
        b"mset" => (MsgType::ReqRedisMset, false, false, RoutingOverride::None),
        b"ping" => (
            MsgType::ReqRedisPing,
            true,
            false,
            RoutingOverride::LocalNodeOnly,
        ),
        b"rpop" => (MsgType::ReqRedisRpop, false, false, RoutingOverride::None),
        b"sadd" => (MsgType::ReqRedisSadd, false, false, RoutingOverride::None),
        b"scan" => (
            MsgType::ReqRedisScan,
            true,
            false,
            RoutingOverride::LocalNodeOnly,
        ),
        b"spop" => (MsgType::ReqRedisSpop, false, false, RoutingOverride::None),
        b"srem" => (MsgType::ReqRedisSrem, false, false, RoutingOverride::None),
        b"type" => (MsgType::ReqRedisType, true, false, RoutingOverride::None),
        b"zadd" => (MsgType::ReqRedisZadd, false, false, RoutingOverride::None),
        b"zrem" => (MsgType::ReqRedisZrem, false, false, RoutingOverride::None),
        b"eval" => (MsgType::ReqRedisEval, false, false, RoutingOverride::None),
        b"sort" => (MsgType::ReqRedisSort, true, false, RoutingOverride::None),
        b"quit" => (MsgType::ReqRedisQuit, false, true, RoutingOverride::None),
        b"load" => (
            MsgType::ReqRedisScriptLoad,
            false,
            false,
            RoutingOverride::AllNodesAllRacksAllDcs,
        ),
        b"kill" => (
            MsgType::ReqRedisScriptKill,
            false,
            false,
            RoutingOverride::AllNodesAllRacksAllDcs,
        ),
        // length 5
        b"hkeys" => (
            MsgType::ReqRedisHkeys,
            true,
            false,
            RoutingOverride::TokenOwnerLocalRackOnly,
        ),
        b"hmget" => (MsgType::ReqRedisHmget, true, false, RoutingOverride::None),
        b"hmset" => (MsgType::ReqRedisHmset, false, false, RoutingOverride::None),
        b"hvals" => (
            MsgType::ReqRedisHvals,
            true,
            false,
            RoutingOverride::TokenOwnerLocalRackOnly,
        ),
        b"hscan" => (
            MsgType::ReqRedisHscan,
            true,
            false,
            RoutingOverride::TokenOwnerLocalRackOnly,
        ),
        b"lpush" => (MsgType::ReqRedisLpush, false, false, RoutingOverride::None),
        b"ltrim" => (MsgType::ReqRedisLtrim, false, false, RoutingOverride::None),
        b"rpush" => (MsgType::ReqRedisRpush, false, false, RoutingOverride::None),
        b"scard" => (MsgType::ReqRedisScard, true, false, RoutingOverride::None),
        b"sdiff" => (MsgType::ReqRedisSdiff, true, false, RoutingOverride::None),
        b"setex" => (MsgType::ReqRedisSetex, false, false, RoutingOverride::None),
        b"setnx" => (MsgType::ReqRedisSetnx, false, false, RoutingOverride::None),
        b"smove" => (MsgType::ReqRedisSmove, false, false, RoutingOverride::None),
        b"sscan" => (
            MsgType::ReqRedisSscan,
            true,
            false,
            RoutingOverride::TokenOwnerLocalRackOnly,
        ),
        b"zcard" => (MsgType::ReqRedisZcard, true, false, RoutingOverride::None),
        b"zrank" => (MsgType::ReqRedisZrank, true, false, RoutingOverride::None),
        b"zscan" => (
            MsgType::ReqRedisZscan,
            true,
            false,
            RoutingOverride::TokenOwnerLocalRackOnly,
        ),
        b"pfadd" => (MsgType::ReqRedisPfadd, false, false, RoutingOverride::None),
        b"flush" => (
            MsgType::ReqRedisScriptFlush,
            false,
            false,
            RoutingOverride::AllNodesAllRacksAllDcs,
        ),
        // length 6
        b"append" => (MsgType::ReqRedisAppend, false, false, RoutingOverride::None),
        b"decrby" => (MsgType::ReqRedisDecrby, false, false, RoutingOverride::None),
        b"exists" => (
            MsgType::ReqRedisExists,
            true,
            false,
            RoutingOverride::None,
        ),
        b"expire" => (MsgType::ReqRedisExpire, false, false, RoutingOverride::None),
        b"getbit" => (MsgType::ReqRedisGetbit, true, false, RoutingOverride::None),
        b"getset" => (MsgType::ReqRedisGetset, false, false, RoutingOverride::None),
        b"psetex" => (MsgType::ReqRedisPsetex, false, false, RoutingOverride::None),
        b"hsetnx" => (MsgType::ReqRedisHsetnx, false, false, RoutingOverride::None),
        b"incrby" => (MsgType::ReqRedisIncrby, false, false, RoutingOverride::None),
        b"lindex" => (MsgType::ReqRedisLindex, true, false, RoutingOverride::None),
        b"lpushx" => (MsgType::ReqRedisLpushx, false, false, RoutingOverride::None),
        b"lrange" => (MsgType::ReqRedisLrange, true, false, RoutingOverride::None),
        b"rpushx" => (MsgType::ReqRedisRpushx, false, false, RoutingOverride::None),
        b"setbit" => (MsgType::ReqRedisSetbit, false, false, RoutingOverride::None),
        b"sinter" => (MsgType::ReqRedisSinter, true, false, RoutingOverride::None),
        b"strlen" => (MsgType::ReqRedisStrlen, true, false, RoutingOverride::None),
        b"sunion" => (MsgType::ReqRedisSunion, true, false, RoutingOverride::None),
        b"zcount" => (MsgType::ReqRedisZcount, true, false, RoutingOverride::None),
        b"zrange" => (MsgType::ReqRedisZrange, true, false, RoutingOverride::None),
        b"zscore" => (MsgType::ReqRedisZscore, true, false, RoutingOverride::None),
        b"config" => (
            MsgType::ReqRedisConfig,
            true,
            false,
            RoutingOverride::LocalNodeOnly,
        ),
        b"geoadd" => (MsgType::ReqRedisGeoadd, false, false, RoutingOverride::None),
        b"geopos" => (MsgType::ReqRedisGeopos, true, false, RoutingOverride::None),
        b"unlink" => (MsgType::ReqRedisUnlink, false, false, RoutingOverride::None),
        b"script" => (MsgType::ReqRedisScript, false, false, RoutingOverride::None),
        b"bitpos" => (MsgType::ReqRedisBitpos, true, false, RoutingOverride::None),
        // length 7
        b"persist" => (
            MsgType::ReqRedisPersist,
            false,
            false,
            RoutingOverride::None,
        ),
        b"pexpire" => (
            MsgType::ReqRedisPexpire,
            false,
            false,
            RoutingOverride::None,
        ),
        b"hexists" => (
            MsgType::ReqRedisHexists,
            true,
            false,
            RoutingOverride::None,
        ),
        b"hgetall" => (
            MsgType::ReqRedisHgetall,
            true,
            false,
            RoutingOverride::TokenOwnerLocalRackOnly,
        ),
        b"hincrby" => (
            MsgType::ReqRedisHincrby,
            false,
            false,
            RoutingOverride::None,
        ),
        b"linsert" => (
            MsgType::ReqRedisLinsert,
            false,
            false,
            RoutingOverride::None,
        ),
        b"zincrby" => (
            MsgType::ReqRedisZincrby,
            false,
            false,
            RoutingOverride::None,
        ),
        b"evalsha" => (
            MsgType::ReqRedisEvalsha,
            false,
            false,
            RoutingOverride::None,
        ),
        b"restore" => (
            MsgType::ReqRedisRestore,
            false,
            false,
            RoutingOverride::None,
        ),
        b"slaveof" => (
            MsgType::ReqRedisSlaveof,
            false,
            false,
            RoutingOverride::LocalNodeOnly,
        ),
        b"pfcount" => (
            MsgType::ReqRedisPfcount,
            false,
            false,
            RoutingOverride::None,
        ),
        b"geohash" => (
            MsgType::ReqRedisGeohash,
            true,
            false,
            RoutingOverride::None,
        ),
        b"geodist" => (
            MsgType::ReqRedisGeodist,
            true,
            false,
            RoutingOverride::None,
        ),
        b"hstrlen" => (
            MsgType::ReqRedisHstrlen,
            true,
            false,
            RoutingOverride::None,
        ),
        // length 8
        b"expireat" => (
            MsgType::ReqRedisExpireat,
            false,
            false,
            RoutingOverride::None,
        ),
        b"bitcount" => (
            MsgType::ReqRedisBitcount,
            true,
            false,
            RoutingOverride::None,
        ),
        b"getrange" => (
            MsgType::ReqRedisGetrange,
            true,
            false,
            RoutingOverride::None,
        ),
        b"setrange" => (
            MsgType::ReqRedisSetrange,
            false,
            false,
            RoutingOverride::None,
        ),
        b"smembers" => (
            MsgType::ReqRedisSmembers,
            true,
            false,
            RoutingOverride::None,
        ),
        b"zrevrank" => (
            MsgType::ReqRedisZrevrank,
            true,
            false,
            RoutingOverride::None,
        ),
        b"json.set" => (
            MsgType::ReqRedisJsonset,
            false,
            false,
            RoutingOverride::None,
        ),
        b"json.get" => (
            MsgType::ReqRedisJsonget,
            true,
            false,
            RoutingOverride::None,
        ),
        b"json.del" => (
            MsgType::ReqRedisJsondel,
            false,
            false,
            RoutingOverride::None,
        ),
        // length 9
        b"pexpireat" => (
            MsgType::ReqRedisPexpireat,
            false,
            false,
            RoutingOverride::None,
        ),
        b"rpoplpush" => (
            MsgType::ReqRedisRpoplpush,
            false,
            false,
            RoutingOverride::None,
        ),
        b"sismember" => (
            MsgType::ReqRedisSismember,
            true,
            false,
            RoutingOverride::None,
        ),
        b"zlexcount" => (
            MsgType::ReqRedisZlexcount,
            true,
            false,
            RoutingOverride::None,
        ),
        b"zrevrange" => (
            MsgType::ReqRedisZrevrange,
            true,
            false,
            RoutingOverride::None,
        ),
        b"georadius" => (
            MsgType::ReqRedisGeoradius,
            true,
            false,
            RoutingOverride::None,
        ),
        b"json.type" => (
            MsgType::ReqRedisJsontype,
            true,
            false,
            RoutingOverride::None,
        ),
        b"json.mget" => (
            MsgType::ReqRedisJsonmget,
            true,
            false,
            RoutingOverride::None,
        ),
        // length 10
        b"sdiffstore" => (
            MsgType::ReqRedisSdiffstore,
            false,
            false,
            RoutingOverride::None,
        ),
        // length 11
        b"incrbyfloat" => (
            MsgType::ReqRedisIncrbyfloat,
            false,
            false,
            RoutingOverride::None,
        ),
        b"sinterstore" => (
            MsgType::ReqRedisSinterstore,
            false,
            false,
            RoutingOverride::None,
        ),
        b"srandmember" => (
            MsgType::ReqRedisSrandmember,
            true,
            false,
            RoutingOverride::None,
        ),
        b"sunionstore" => (
            MsgType::ReqRedisSunionstore,
            true,
            false,
            RoutingOverride::None,
        ),
        b"zinterstore" => (
            MsgType::ReqRedisZinterstore,
            true,
            false,
            RoutingOverride::None,
        ),
        b"zrangebylex" => (
            MsgType::ReqRedisZrangebylex,
            true,
            false,
            RoutingOverride::None,
        ),
        b"zunionstore" => (
            MsgType::ReqRedisZunionstore,
            true,
            false,
            RoutingOverride::None,
        ),
        b"json.arrlen" => (
            MsgType::ReqRedisJsonarrlen,
            true,
            false,
            RoutingOverride::None,
        ),
        b"json.objlen" => (
            MsgType::ReqRedisJsonobjlen,
            true,
            false,
            RoutingOverride::None,
        ),
        // length 12
        b"hincrbyfloat" => (
            MsgType::ReqRedisHincrbyfloat,
            false,
            false,
            RoutingOverride::None,
        ),
        b"json.objkeys" => (
            MsgType::ReqRedisJsonobjkeys,
            true,
            false,
            RoutingOverride::None,
        ),
        // length 13
        b"zrangebyscore" => (
            MsgType::ReqRedisZrangebyscore,
            true,
            false,
            RoutingOverride::None,
        ),
        // length 14
        b"zremrangebylex" => (
            MsgType::ReqRedisZremrangebylex,
            false,
            false,
            RoutingOverride::None,
        ),
        b"zrevrangebylex" => (
            MsgType::ReqRedisZrevrangebylex,
            true,
            false,
            RoutingOverride::None,
        ),
        b"json.arrappend" => (
            MsgType::ReqRedisJsonarrappend,
            false,
            false,
            RoutingOverride::None,
        ),
        b"json.arrinsert" => (
            MsgType::ReqRedisJsonarrinsert,
            false,
            false,
            RoutingOverride::None,
        ),
        // length 15
        b"zremrangebyrank" => (
            MsgType::ReqRedisZremrangebyrank,
            false,
            false,
            RoutingOverride::None,
        ),
        // length 16
        b"zremrangebyscore" => (
            MsgType::ReqRedisZremrangebyscore,
            false,
            false,
            RoutingOverride::None,
        ),
        b"zrevrangebyscore" => (
            MsgType::ReqRedisZrevrangebyscore,
            true,
            false,
            RoutingOverride::None,
        ),
        // length 17
        b"georadiusbymember" => (
            MsgType::ReqRedisGeoradiusbymember,
            true,
            false,
            RoutingOverride::None,
        ),
        // length 28: dynomite config
        b"dyno_config:conn_consistency" => (
            MsgType::HackSettingConnConsistency,
            false,
            false,
            RoutingOverride::None,
        ),
        _ => return None,
    };
    Some((
        ty,
        CommandTraits {
            is_read,
            quit,
            routing,
        },
    ))
}

/// Look up a Redis error response keyword (case-sensitive: error
/// keywords are uppercase on the wire) and return the
/// corresponding [`MsgType`].
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::redis::commands::error_lookup;
///
/// assert_eq!(error_lookup(b"-ERR"), Some(MsgType::RspRedisErrorErr));
/// assert_eq!(error_lookup(b"-WRONGTYPE"), Some(MsgType::RspRedisErrorWrongtype));
/// ```
#[must_use]
pub fn error_lookup(token: &[u8]) -> Option<MsgType> {
    match token {
        b"-ERR" => Some(MsgType::RspRedisErrorErr),
        b"-OOM" => Some(MsgType::RspRedisErrorOom),
        b"-BUSY" => Some(MsgType::RspRedisErrorBusy),
        b"-NOAUTH" => Some(MsgType::RspRedisErrorNoauth),
        b"-LOADING" => Some(MsgType::RspRedisErrorLoading),
        b"-BUSYKEY" => Some(MsgType::RspRedisErrorBusykey),
        b"-MISCONF" => Some(MsgType::RspRedisErrorMisconf),
        b"-NOSCRIPT" => Some(MsgType::RspRedisErrorNoscript),
        b"-READONLY" => Some(MsgType::RspRedisErrorReadonly),
        b"-WRONGTYPE" => Some(MsgType::RspRedisErrorWrongtype),
        b"-EXECABORT" => Some(MsgType::RspRedisErrorExecabort),
        b"-MASTERDOWN" => Some(MsgType::RspRedisErrorMasterdown),
        b"-NOREPLICAS" => Some(MsgType::RspRedisErrorNoreplicas),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_get_set() {
        let (ty, t) = lookup(b"GET").unwrap();
        assert_eq!(ty, MsgType::ReqRedisGet);
        assert!(t.is_read);
        let (ty, t) = lookup(b"set").unwrap();
        assert_eq!(ty, MsgType::ReqRedisSet);
        assert!(!t.is_read);
    }

    #[test]
    fn lookup_unknown() {
        assert!(lookup(b"NOTACOMMAND").is_none());
    }

    #[test]
    fn classify_get_is_arg0() {
        assert_eq!(classify(MsgType::ReqRedisGet), CommandClass::Arg0);
    }

    #[test]
    fn classify_mset_is_argkvx() {
        assert_eq!(classify(MsgType::ReqRedisMset), CommandClass::ArgKvX);
    }

    #[test]
    fn error_lookup_wrongtype() {
        assert_eq!(error_lookup(b"-WRONGTYPE"), Some(MsgType::RspRedisErrorWrongtype));
    }
}
