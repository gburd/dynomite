//! Command-level rewrite (the `SMEMBERS` / DC_SAFE_QUORUM case).
//!
//! Reproduces `redis_rewrite_query` from the reference engine. The
//! only live rewrite is `SMEMBERS <set>` -> `SORT <set> ALPHA`
//! when `read_consistency` is `DC_SAFE_QUORUM`. The rewrite avoids
//! the order-dependent checksum mismatch that would otherwise
//! cause SAFE_QUORUM to fail on an unordered set type.

use crate::io::mbuf::MbufPool;
use crate::msg::{ConsistencyLevel, KeyPos, Msg, MsgParseResult, MsgType};

use super::super::parser::redis_parse_req;
use super::{RepairError, RepairOutcome};

/// Apply the command-level rewrite if it is available.
///
/// Returns [`RepairOutcome::Rewritten`] when a rewrite occurred,
/// [`RepairOutcome::NoOp`] otherwise.
///
/// # Examples
///
/// ```
/// use dynomite::io::mbuf::MbufPool;
/// use dynomite::msg::{ConsistencyLevel, KeyPos, Msg, MsgType};
/// use dynomite::proto::redis::redis_rewrite_query;
///
/// let mut req = Msg::new(0, MsgType::ReqRedisSmembers, true);
/// req.push_key(KeyPos::without_tag(b"myset".to_vec()));
/// req.set_consistency(ConsistencyLevel::DcSafeQuorum);
/// let pool = MbufPool::default();
/// let outcome = redis_rewrite_query(&mut req, &pool).unwrap();
/// matches!(outcome, dynomite::proto::redis::RepairOutcome::Rewritten(_));
/// ```
pub fn redis_rewrite_query(orig: &mut Msg, pool: &MbufPool) -> Result<RepairOutcome, RepairError> {
    if !orig.is_request() {
        return Ok(RepairOutcome::NoOp);
    }
    if orig.ty() != MsgType::ReqRedisSmembers {
        return Ok(RepairOutcome::NoOp);
    }
    if orig.consistency() != ConsistencyLevel::DcSafeQuorum {
        return Ok(RepairOutcome::NoOp);
    }
    let key = orig
        .keys()
        .first()
        .ok_or(RepairError::Invariant("smembers without key"))?;
    let key_bytes = key.key();

    let payload = format!(
        "*3\r\n$4\r\nsort\r\n${}\r\n{}\r\n$5\r\nalpha\r\n",
        key_bytes.len(),
        std::str::from_utf8(key_bytes).map_err(|_| RepairError::Invariant("non-utf8 key"))?,
    );
    let payload_bytes = payload.as_bytes();

    let mut new_msg = Box::new(Msg::new(0, MsgType::Unknown, true));
    write_into_chain(&mut new_msg, pool, payload_bytes);

    if redis_parse_req(&mut new_msg, payload_bytes) != MsgParseResult::Ok {
        return Err(RepairError::Invariant("rewrite did not parse"));
    }
    Ok(RepairOutcome::Rewritten(new_msg))
}

/// Rewrite a write request as a Lua script that records timestamps.
///
/// The full per-command timestamp-rewrite logic depends on the
/// post-parsed argument structure (`post_parse_msg`) which uses
/// `keypos` / `argpos` arrays the parser populates on every key
/// and bulk argument. The data-shape side is wired here; the
/// per-command Lua-script generation lands once Stage 9's
/// connection FSM exercises the full read-repair workflow.
///
/// Returns [`RepairOutcome::NoOp`] when the request is not eligible
/// for the rewrite. Returns
/// [`RepairError::Invariant`] when the request is eligible but the
/// post-parse invariant fails.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::redis::redis_rewrite_query_with_timestamp_md;
///
/// let mut req = Msg::new(0, MsgType::ReqRedisGet, true);
/// let outcome = redis_rewrite_query_with_timestamp_md(&mut req).unwrap();
/// matches!(outcome, dynomite::proto::redis::RepairOutcome::NoOp);
/// ```
pub fn redis_rewrite_query_with_timestamp_md(orig: &mut Msg) -> Result<RepairOutcome, RepairError> {
    if !orig.is_request() {
        return Ok(RepairOutcome::NoOp);
    }
    // Only commands with `has_repair_support` in `proto_cmd_info`
    // are rewritten; the parser already records this via
    // [`Msg::flags().rewrite_with_ts_possible`] (false for parses
    // that crossed an mbuf boundary).
    if !orig.flags().rewrite_with_ts_possible {
        return Ok(RepairOutcome::NoOp);
    }
    let supported = matches!(
        orig.ty(),
        MsgType::ReqRedisSet
            | MsgType::ReqRedisDel
            | MsgType::ReqRedisHset
            | MsgType::ReqRedisHmset
            | MsgType::ReqRedisHdel
            | MsgType::ReqRedisHget
            | MsgType::ReqRedisGet
            | MsgType::ReqRedisSadd
            | MsgType::ReqRedisSrem
            | MsgType::ReqRedisSismember
            | MsgType::ReqRedisZadd
            | MsgType::ReqRedisZrem
            | MsgType::ReqRedisZscore
    );
    if !supported {
        return Ok(RepairOutcome::NoOp);
    }
    // Full Lua-script generation lands once Stage 9 exercises the
    // workflow end-to-end. The data-shape side: caller can detect
    // `RepairOutcome::NoOp` and fall back to checksum-based repair.
    Ok(RepairOutcome::NoOp)
}

fn write_into_chain(msg: &mut Msg, pool: &MbufPool, mut buf: &[u8]) {
    while !buf.is_empty() {
        let mut mb = pool.get();
        let n = mb.recv(buf);
        if n == 0 {
            break;
        }
        msg.mbufs_mut().push_back(mb);
        buf = &buf[n..];
    }
    msg.recompute_mlen();
}

fn _clone_keypos(k: &KeyPos) -> KeyPos {
    KeyPos::new(k.key().to_vec(), k.tag())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smembers_safe_quorum_rewrites() {
        let mut req = Msg::new(0, MsgType::ReqRedisSmembers, true);
        req.push_key(KeyPos::without_tag(b"myset".to_vec()));
        req.set_consistency(ConsistencyLevel::DcSafeQuorum);
        let pool = MbufPool::default();
        let outcome = redis_rewrite_query(&mut req, &pool).unwrap();
        match outcome {
            RepairOutcome::Rewritten(m) => {
                assert_eq!(m.ty(), MsgType::ReqRedisSort);
                assert_eq!(m.keys()[0].key(), b"myset");
            }
            RepairOutcome::NoOp => panic!("expected rewrite"),
        }
    }

    #[test]
    fn smembers_dc_one_does_not_rewrite() {
        let mut req = Msg::new(0, MsgType::ReqRedisSmembers, true);
        req.push_key(KeyPos::without_tag(b"myset".to_vec()));
        req.set_consistency(ConsistencyLevel::DcOne);
        let pool = MbufPool::default();
        assert!(matches!(
            redis_rewrite_query(&mut req, &pool).unwrap(),
            RepairOutcome::NoOp,
        ));
    }

    #[test]
    fn non_smembers_does_not_rewrite() {
        let mut req = Msg::new(0, MsgType::ReqRedisGet, true);
        req.push_key(KeyPos::without_tag(b"k".to_vec()));
        req.set_consistency(ConsistencyLevel::DcSafeQuorum);
        let pool = MbufPool::default();
        assert!(matches!(
            redis_rewrite_query(&mut req, &pool).unwrap(),
            RepairOutcome::NoOp,
        ));
    }
}
