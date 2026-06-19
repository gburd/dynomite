//! Exhaustive coverage of the Redis command catalog
//! (`proto::redis::commands`).
//!
//! The `lookup` table and the `classify` switch are large
//! per-command dispatch blocks. These tests drive every recognised
//! keyword through `lookup`, confirm it round-trips into a
//! non-`Unknown` `MsgType`, and that `classify` is total over the
//! returned type. They also exercise the case-insensitive path, the
//! FT.* unknown-keyword fallthrough, the length-bound rejection, and
//! the `is_redis_error` / `error_lookup` predicates.

use dynomite::msg::MsgType;
use dynomite::proto::redis::commands::{
    classify, error_lookup, is_redis_error, lookup, CommandClass, RoutingOverride,
};

/// Every keyword the `lookup` table recognises. Kept in sync with
/// the `match key` arms in commands.rs; a missing keyword here only
/// reduces coverage, it never produces a false pass.
const KEYWORDS: &[&[u8]] = &[
    b"append",
    b"bitcount",
    b"bitpos",
    b"config",
    b"decr",
    b"decrby",
    b"dump",
    b"del",
    b"eval",
    b"evalsha",
    b"exists",
    b"expire",
    b"expireat",
    b"flush",
    b"geoadd",
    b"geodist",
    b"geohash",
    b"geopos",
    b"georadius",
    b"georadiusbymember",
    b"get",
    b"getbit",
    b"getrange",
    b"getset",
    b"hdel",
    b"hexists",
    b"hget",
    b"hgetall",
    b"hincrby",
    b"hincrbyfloat",
    b"hkeys",
    b"hlen",
    b"hmget",
    b"hmset",
    b"hscan",
    b"hset",
    b"hsetnx",
    b"hstrlen",
    b"hvals",
    b"incr",
    b"incrby",
    b"incrbyfloat",
    b"info",
    b"keys",
    b"kill",
    b"lindex",
    b"linsert",
    b"llen",
    b"load",
    b"lpop",
    b"lpush",
    b"lpushx",
    b"lrange",
    b"lrem",
    b"lset",
    b"ltrim",
    b"mget",
    b"mset",
    b"persist",
    b"pexpire",
    b"pexpireat",
    b"pfadd",
    b"pfcount",
    b"ping",
    b"psetex",
    b"pttl",
    b"quit",
    b"restore",
    b"rpop",
    b"rpoplpush",
    b"rpush",
    b"rpushx",
    b"sadd",
    b"scan",
    b"scard",
    b"script",
    b"sdiff",
    b"sdiffstore",
    b"set",
    b"setbit",
    b"setex",
    b"setnx",
    b"setrange",
    b"sinter",
    b"sinterstore",
    b"sismember",
    b"slaveof",
    b"smembers",
    b"smove",
    b"sort",
    b"spop",
    b"srandmember",
    b"srem",
    b"sscan",
    b"strlen",
    b"sunion",
    b"sunionstore",
    b"ttl",
    b"type",
    b"unlink",
    b"zadd",
    b"zcard",
    b"zcount",
    b"zincrby",
    b"zinterstore",
    b"zlexcount",
    b"zrange",
    b"zrangebylex",
    b"zrangebyscore",
    b"zrank",
    b"zrem",
    b"zremrangebylex",
    b"zremrangebyrank",
    b"zremrangebyscore",
    b"zrevrange",
    b"zrevrangebylex",
    b"zrevrangebyscore",
    b"zrevrank",
    b"zscan",
    b"zscore",
    b"zunionstore",
    b"json.set",
    b"json.get",
    b"json.del",
    b"json.type",
    b"json.mget",
    b"json.arrlen",
    b"json.objlen",
    b"json.objkeys",
    b"json.arrappend",
    b"json.arrinsert",
    b"ft.create",
    b"ft.search",
    b"ft.info",
    b"ft.list",
    b"ft._list",
    b"ft.dropindex",
    b"ft.regex",
    b"ft.sugadd",
    b"ft.sugget",
    b"ft.sugdel",
    b"ft.suglen",
    b"dyno_config:conn_consistency",
];

#[test]
fn every_known_keyword_resolves_and_classifies() {
    // Each catalog keyword maps to a non-Unknown type and a total
    // classify result. This walks every `match key` arm in lookup.
    for kw in KEYWORDS {
        let (ty, _traits) = lookup(kw).unwrap_or_else(|| panic!("keyword {kw:?} should resolve"));
        assert_ne!(ty, MsgType::Unknown, "keyword {kw:?} resolved to Unknown");
        // classify must be total: it returns a CommandClass for any
        // request type without panicking.
        let _ = classify(ty);
    }
}

#[test]
fn lookup_is_case_insensitive() {
    // The table lowercases the keyword first; an uppercase keyword
    // resolves to the same type as its lowercase form.
    for kw in KEYWORDS {
        let upper: Vec<u8> = kw.iter().map(u8::to_ascii_uppercase).collect();
        let lower = lookup(kw).map(|(ty, _)| ty);
        let upper_ty = lookup(&upper).map(|(ty, _)| ty);
        assert_eq!(lower, upper_ty, "case mismatch for {kw:?}");
    }
}

#[test]
fn classify_covers_every_command_class() {
    // Confirm each CommandClass arm in classify is reachable from a
    // real command keyword, so the giant switch is fully walked.
    let expect: &[(&[u8], CommandClass)] = &[
        (b"ping", CommandClass::Argz),
        (b"get", CommandClass::Arg0),
        (b"expire", CommandClass::Arg1),
        (b"setex", CommandClass::Arg2),
        (b"linsert", CommandClass::Arg3),
        (b"set", CommandClass::ArgN),
        (b"mget", CommandClass::ArgX),
        (b"mset", CommandClass::ArgKvX),
        (b"info", CommandClass::ArgUpto1),
        (b"eval", CommandClass::ArgEval),
    ];
    for (kw, class) in expect {
        let (ty, _) = lookup(kw).unwrap();
        assert_eq!(classify(ty), *class, "class mismatch for {kw:?}");
    }
}

#[test]
fn command_class_predicates_are_consistent() {
    // The is_* convenience predicates agree with the variant they
    // name and reject the others.
    assert!(CommandClass::Argz.is_argz());
    assert!(!CommandClass::Arg0.is_argz());
    assert!(CommandClass::ArgX.is_argx());
    assert!(!CommandClass::Argz.is_argx());
    assert!(CommandClass::ArgKvX.is_argkvx());
    assert!(!CommandClass::ArgX.is_argkvx());
    assert!(CommandClass::ArgEval.is_argeval());
    assert!(!CommandClass::ArgN.is_argeval());
}

#[test]
fn routing_overrides_are_stamped() {
    // Commands carry their routing override; check a representative
    // of each RoutingOverride arm.
    let cases: &[(&[u8], RoutingOverride)] = &[
        (b"get", RoutingOverride::None),
        (b"ping", RoutingOverride::LocalNodeOnly),
        (b"hkeys", RoutingOverride::TokenOwnerLocalRackOnly),
        (b"load", RoutingOverride::AllNodesAllRacksAllDcs),
    ];
    for (kw, routing) in cases {
        let (_, traits) = lookup(kw).unwrap();
        assert_eq!(traits.routing, *routing, "routing mismatch for {kw:?}");
    }
    // RoutingOverride default is None.
    assert_eq!(RoutingOverride::default(), RoutingOverride::None);
}

#[test]
fn quit_command_sets_quit_trait() {
    let (ty, traits) = lookup(b"quit").unwrap();
    assert_eq!(ty, MsgType::ReqRedisQuit);
    assert!(traits.quit);
    // Non-quit command leaves the flag clear.
    let (_, traits) = lookup(b"get").unwrap();
    assert!(!traits.quit);
}

#[test]
fn unknown_ft_keyword_maps_to_ft_unknown() {
    // A FT.* keyword that is not in the recognised set falls through
    // to ReqRedisFtUnknown with LocalNodeOnly routing.
    let (ty, traits) = lookup(b"ft.aggregate").unwrap();
    assert_eq!(ty, MsgType::ReqRedisFtUnknown);
    assert!(traits.is_read);
    assert_eq!(traits.routing, RoutingOverride::LocalNodeOnly);
    assert_eq!(classify(ty), CommandClass::ArgN);
    // Uppercase form also resolves to the unknown FT variant.
    let (ty2, _) = lookup(b"FT.AGGREGATE").unwrap();
    assert_eq!(ty2, MsgType::ReqRedisFtUnknown);
}

#[test]
fn ft_typed_keywords_classify_correctly() {
    // The recognised FT.* keywords land on their typed variants and
    // their declared classes (the FT-specific classify arms).
    assert_eq!(classify(MsgType::ReqRedisFtInfo), CommandClass::Arg0);
    assert_eq!(classify(MsgType::ReqRedisFtList), CommandClass::Argz);
    assert_eq!(classify(MsgType::ReqRedisFtSuglen), CommandClass::Arg0);
    assert_eq!(classify(MsgType::ReqRedisFtSugdel), CommandClass::Arg1);
    assert_eq!(classify(MsgType::ReqRedisFtSugadd), CommandClass::ArgN);
    assert_eq!(classify(MsgType::ReqRedisFtCreate), CommandClass::ArgN);
}

#[test]
fn lookup_rejects_empty_and_oversized_keywords() {
    // Empty keyword and anything over 32 bytes are rejected before
    // the lowercase copy, guarding the fixed-size buffer.
    assert!(lookup(b"").is_none());
    let too_long = vec![b'a'; 33];
    assert!(lookup(&too_long).is_none());
    // Exactly 32 bytes is accepted by the length guard (and then
    // fails the table lookup, returning None via the wildcard arm).
    let exactly_32 = vec![b'a'; 32];
    assert!(lookup(&exactly_32).is_none());
}

#[test]
fn lookup_unknown_keyword_is_none() {
    assert!(lookup(b"notacommand").is_none());
    assert!(lookup(b"zzz").is_none());
}

#[test]
fn classify_unknown_type_falls_through_to_arg0() {
    // The classify wildcard arm maps anything not explicitly listed
    // (response types, the Unknown sentinel) to Arg0.
    assert_eq!(classify(MsgType::Unknown), CommandClass::Arg0);
    assert_eq!(classify(MsgType::RspRedisStatus), CommandClass::Arg0);
}

#[test]
fn error_lookup_covers_every_keyword() {
    // Each recognised error keyword maps to an is_redis_error type;
    // an unknown error keyword returns None.
    let errors: &[(&[u8], MsgType)] = &[
        (b"-ERR", MsgType::RspRedisErrorErr),
        (b"-OOM", MsgType::RspRedisErrorOom),
        (b"-BUSY", MsgType::RspRedisErrorBusy),
        (b"-NOAUTH", MsgType::RspRedisErrorNoauth),
        (b"-LOADING", MsgType::RspRedisErrorLoading),
        (b"-BUSYKEY", MsgType::RspRedisErrorBusykey),
        (b"-MISCONF", MsgType::RspRedisErrorMisconf),
        (b"-NOSCRIPT", MsgType::RspRedisErrorNoscript),
        (b"-READONLY", MsgType::RspRedisErrorReadonly),
        (b"-WRONGTYPE", MsgType::RspRedisErrorWrongtype),
        (b"-EXECABORT", MsgType::RspRedisErrorExecabort),
        (b"-MASTERDOWN", MsgType::RspRedisErrorMasterdown),
        (b"-NOREPLICAS", MsgType::RspRedisErrorNoreplicas),
    ];
    for (kw, ty) in errors {
        assert_eq!(error_lookup(kw), Some(*ty), "error keyword {kw:?}");
        assert!(is_redis_error(*ty), "{ty:?} should be an error");
    }
    assert_eq!(error_lookup(b"-NOTANERROR"), None);
    assert_eq!(error_lookup(b"+OK"), None);
}

#[test]
fn is_redis_error_rejects_non_errors() {
    // The base RspRedisError variant is an error; ordinary response
    // types are not.
    assert!(is_redis_error(MsgType::RspRedisError));
    assert!(!is_redis_error(MsgType::RspRedisStatus));
    assert!(!is_redis_error(MsgType::RspRedisInteger));
    assert!(!is_redis_error(MsgType::ReqRedisGet));
}
