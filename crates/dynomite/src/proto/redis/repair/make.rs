//! Repair-message construction.
//!
//! A repairable read rewritten into a repair script is answered by each
//! replica with `[status, ts, value]` ([`TsReply`]). The coordinator picks
//! the freshest reply ([`find_most_updated`]), builds the write that
//! brings the stale replicas up to date ([`repair_request`]), and hands
//! the client the plain value ([`TsReply::client_reply`]).

use crate::msg::MsgType;

use super::logic::Reply;
use super::request::{cmd_info, encode_reply, parse_reply, ScriptRequest, ADD_SET, REM_SET};

/// One replica's reply to a repairable read: `[status, ts, value]`.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::make::TsReply;
///
/// let r = TsReply::parse(b"*3\r\n$1\r\nE\r\n$2\r\n17\r\n$1\r\nv\r\n").unwrap();
/// assert_eq!((r.status, r.ts), (b'E', 17));
/// assert_eq!(r.client_reply(), b"$1\r\nv\r\n");
/// assert!(TsReply::parse(b"$1\r\nv\r\n").is_none());
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TsReply {
    /// `E` (the value exists), `R` (it was removed) or `X` (no record).
    pub status: u8,
    /// Timestamp of the last write or delete, in milliseconds.
    pub ts: u64,
    /// The reply the plain command would have produced.
    pub value: Reply,
}

impl TsReply {
    /// Parse a complete reply; `None` when `wire` is not a
    /// `[status, ts, value]` array.
    #[must_use]
    pub fn parse(wire: &[u8]) -> Option<Self> {
        let (Reply::Array(items), used) = parse_reply(wire)? else {
            return None;
        };
        if used != wire.len() {
            return None;
        }
        let [Reply::Bulk(Some(status)), ts, value] = <[Reply; 3]>::try_from(items).ok()? else {
            return None;
        };
        let status = *status.first()?;
        let ts = match ts {
            Reply::Integer(n) => u64::try_from(n).ok()?,
            Reply::Bulk(Some(d)) if status != b'X' => std::str::from_utf8(&d).ok()?.parse().ok()?,
            _ if status == b'X' => 0,
            _ => return None,
        };
        Some(Self { status, ts, value })
    }

    /// The reply bytes the client sees: the value without its metadata.
    #[must_use]
    pub fn client_reply(&self) -> Vec<u8> {
        encode_reply(&self.value)
    }
}

/// The freshest reply among a repairable read's replies.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::Reply;
/// use dynomite::proto::redis::repair::make::{find_most_updated, TsReply};
///
/// let r = |status, ts| TsReply { status, ts, value: Reply::Bulk(None) };
/// let m = find_most_updated(&[r(b'E', 5), r(b'X', 0), r(b'R', 9)]).unwrap();
/// assert_eq!((m.winner, m.repair_by_add), (2, false));
/// assert_eq!(m.needs_repair, vec![true, false, false]);
/// assert!(find_most_updated(&[r(b'E', 5), r(b'E', 5)]).is_none());
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MostUpdated {
    /// Index of the reply with the largest timestamp.
    pub winner: usize,
    /// The freshest state is a value (repair by writing it) rather than a
    /// delete (repair by deleting).
    pub repair_by_add: bool,
    /// Per reply: it is older than the winner and must be repaired.
    pub needs_repair: Vec<bool>,
}

/// Find the reply with the largest timestamp (the first one on a tie) and
/// mark every reply older than it for repair. Replies with no record (`X`)
/// take no part. `None` when no reply needs a repair.
#[must_use]
pub fn find_most_updated(replies: &[TsReply]) -> Option<MostUpdated> {
    let mut biggest_ts = 0;
    let mut most_updated: Option<usize> = None;
    let mut repair_by_add = false;
    for (i, cur) in replies.iter().enumerate() {
        if cur.status == b'X' {
            continue;
        }
        if cur.ts > biggest_ts {
            biggest_ts = cur.ts;
            most_updated = Some(i);
            repair_by_add = cur.status == b'E';
        }
    }
    // Staleness is judged against the final maximum, so a reply equal to
    // a maximum that a later reply beats is repaired too (DEV-R3).
    let needs_repair: Vec<bool> = replies
        .iter()
        .map(|r| r.status != b'X' && r.ts < biggest_ts)
        .collect();
    let winner = most_updated.filter(|_| needs_repair.contains(&true))?;
    Some(MostUpdated {
        winner,
        repair_by_add,
        needs_repair,
    })
}

/// The write that repairs a stale replica of `read` (the rewritten read
/// of type `read_ty`) to the state in `winner`: the command's add repair
/// with the winner's value when `repair_by_add`, its remove repair
/// otherwise, stamped with the winner's timestamp.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{keypos::KeyPos, Msg, MsgType};
/// use dynomite::proto::redis::repair::logic::Reply;
/// use dynomite::proto::redis::repair::make::{repair_request, TsReply};
/// use dynomite::proto::redis::repair::request::{Script, ScriptRequest};
///
/// let mut get = Msg::new(1, MsgType::ReqRedisGet, true);
/// get.push_key(KeyPos::without_tag(b"k".to_vec()));
/// let read = ScriptRequest::from_request(&get, 1).unwrap();
/// let winner = TsReply { status: b'E', ts: 9, value: Reply::Bulk(Some(b"v".to_vec())) };
/// let fix = repair_request(&read, MsgType::ReqRedisGet, &winner, true).unwrap();
/// assert_eq!((fix.script, fix.ts), (Some(Script::Set), 9));
/// assert_eq!((fix.orig_cmd, fix.values), (b"SET".to_vec(), vec![b"v".to_vec()]));
/// ```
#[must_use]
pub fn repair_request(
    read: &ScriptRequest,
    read_ty: MsgType,
    winner: &TsReply,
    repair_by_add: bool,
) -> Option<ScriptRequest> {
    let info = cmd_info(read_ty)?;
    let repair_ty = if repair_by_add {
        info.repair_by_add
    } else {
        info.repair_by_rem
    }?;
    let repair = cmd_info(repair_ty)?;
    let (add_set, rem_set) = if repair_by_add {
        (ADD_SET, REM_SET)
    } else {
        (REM_SET, ADD_SET)
    };
    let values = if winner.status == b'E' {
        vec![match &winner.value {
            Reply::Bulk(Some(b)) | Reply::Status(b) => b.clone(),
            Reply::Integer(n) => n.to_string().into_bytes(),
            _ => return None,
        }]
    } else {
        Vec::new()
    };
    Some(ScriptRequest {
        script: Some(repair.script),
        key: read.key.clone(),
        add_set: add_set.to_vec(),
        rem_set: rem_set.to_vec(),
        orig_cmd: repair.cmd_str.to_vec(),
        ts: winner.ts,
        opts: Vec::new(),
        fields: read.fields.clone(),
        values,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::keypos::{ArgPos, KeyPos};
    use crate::msg::Msg;
    use crate::proto::redis::repair::request::Script;

    fn r(status: u8, ts: u64) -> TsReply {
        TsReply {
            status,
            ts,
            value: Reply::Bulk(Some(b"v".to_vec())),
        }
    }

    #[test]
    fn no_record_replies_take_no_part() {
        assert!(find_most_updated(&[r(b'X', 0), r(b'X', 0)]).is_none());
        let m = find_most_updated(&[r(b'X', 0), r(b'E', 4), r(b'E', 2)]).unwrap();
        assert_eq!((m.winner, m.repair_by_add), (1, true));
        assert_eq!(m.needs_repair, vec![false, false, true]);
    }

    // Both replies equal to a maximum that a later reply beats are
    // repaired, not only the one that held the maximum.
    #[test]
    fn equal_stale_replies_before_new_max_are_all_marked() {
        let m = find_most_updated(&[r(b'E', 5), r(b'E', 5), r(b'E', 9)]).unwrap();
        assert_eq!(m.winner, 2);
        assert_eq!(m.needs_repair, vec![true, true, false]);
    }

    // Ties for the maximum keep the first reply as the winner and are not
    // repaired, whatever their status.
    #[test]
    fn ties_for_the_maximum_are_not_marked() {
        assert!(find_most_updated(&[r(b'E', 9), r(b'R', 9)]).is_none());
        let m = find_most_updated(&[r(b'R', 9), r(b'E', 9), r(b'E', 2)]).unwrap();
        assert_eq!((m.winner, m.repair_by_add), (0, false));
        assert_eq!(m.needs_repair, vec![false, false, true]);
    }

    /// For any replies, the winner is the first reply with the largest
    /// timestamp among those with a record, and the stale set is exactly
    /// the replies with a record whose timestamp is below the winner's.
    #[hegel::test]
    fn stale_set_is_every_reply_below_the_winner(tc: hegel::TestCase) {
        use hegel::generators as gs;
        let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(6));
        let replies: Vec<TsReply> = (0..n)
            .map(|_| {
                let status = tc.draw(gs::sampled_from(b"ERX"));
                r(status, tc.draw(gs::integers::<u64>().max_value(4)))
            })
            .collect();
        let live = |x: &TsReply| x.status != b'X';
        let max = replies.iter().filter(|x| live(x)).map(|x| x.ts).max();
        let stale: Vec<bool> = replies
            .iter()
            .map(|x| live(x) && max.is_some_and(|m| x.ts < m))
            .collect();
        match find_most_updated(&replies) {
            None => assert!(!stale.contains(&true), "{replies:?}"),
            Some(m) => {
                let w = &replies[m.winner];
                assert!(live(w) && Some(w.ts) == max, "{replies:?}");
                assert!(replies[..m.winner].iter().all(|x| !live(x) || x.ts < w.ts));
                assert_eq!(m.repair_by_add, w.status == b'E');
                assert_eq!(m.needs_repair, stale, "{replies:?}");
            }
        }
    }

    #[test]
    fn parse_rejects_other_shapes() {
        use crate::proto::redis::repair::request::encode_reply;
        let wire = |items: Vec<Reply>| encode_reply(&Reply::Array(items));
        let x = wire(vec![
            Reply::Bulk(Some(b"X".to_vec())),
            Reply::Integer(0),
            Reply::Bulk(None),
        ]);
        assert_eq!(TsReply::parse(&x).map(|t| t.ts), Some(0));
        let int_ts = wire(vec![
            Reply::Bulk(Some(b"E".to_vec())),
            Reply::Integer(3),
            Reply::Integer(1),
        ]);
        assert_eq!(
            TsReply::parse(&int_ts).map(|t| t.client_reply()),
            Some(b":1\r\n".to_vec())
        );
        for bad in [
            wire(vec![Reply::Bulk(Some(b"E".to_vec())), Reply::Integer(1)]),
            wire(vec![
                Reply::Integer(1),
                Reply::Integer(1),
                Reply::Integer(1),
            ]),
            wire(vec![
                Reply::Bulk(Some(b"".to_vec())),
                Reply::Integer(1),
                Reply::Integer(1),
            ]),
            wire(vec![
                Reply::Bulk(Some(b"E".to_vec())),
                Reply::Bulk(Some(b"x".to_vec())),
                Reply::Integer(1),
            ]),
            wire(vec![
                Reply::Bulk(Some(b"E".to_vec())),
                Reply::Bulk(None),
                Reply::Integer(1),
            ]),
            wire(vec![
                Reply::Bulk(Some(b"E".to_vec())),
                Reply::Integer(-1),
                Reply::Integer(1),
            ]),
            [x.clone(), b"+".to_vec()].concat(),
            b":1\r\n".to_vec(),
        ] {
            assert!(TsReply::parse(&bad).is_none(), "{bad:?}");
        }
    }

    fn read(ty: MsgType, field: Option<&[u8]>) -> ScriptRequest {
        let mut m = Msg::new(1, ty, true);
        m.push_key(KeyPos::without_tag(b"k".to_vec()));
        if let Some(f) = field {
            m.push_arg(ArgPos::new(f.to_vec()));
        }
        ScriptRequest::from_request(&m, 1).unwrap()
    }

    #[test]
    fn repair_by_add_and_rem_per_read() {
        let e = |v: Reply| TsReply {
            status: b'E',
            ts: 8,
            value: v,
        };
        let removed = TsReply {
            status: b'R',
            ts: 8,
            value: Reply::Bulk(None),
        };
        let cases = [
            (
                MsgType::ReqRedisHget,
                Script::Hset,
                &b"HSET"[..],
                Script::Hdel,
                &b"HDEL"[..],
            ),
            (
                MsgType::ReqRedisSismember,
                Script::Sadd,
                b"SADD",
                Script::Hdel,
                b"SREM",
            ),
            (
                MsgType::ReqRedisZscore,
                Script::Zadd,
                b"ZADD",
                Script::Hdel,
                b"ZREM",
            ),
        ];
        for (ty, add, add_cmd, rem, rem_cmd) in cases {
            let q = read(ty, Some(b"f"));
            let fix = repair_request(&q, ty, &e(Reply::Integer(1)), true).unwrap();
            assert_eq!((fix.script, fix.orig_cmd.as_slice()), (Some(add), add_cmd));
            assert_eq!((fix.add_set.as_slice(), fix.ts), (ADD_SET, 8));
            assert_eq!(
                (fix.fields, fix.values),
                (vec![b"f".to_vec()], vec![b"1".to_vec()])
            );
            let fix = repair_request(&q, ty, &removed, false).unwrap();
            assert_eq!((fix.script, fix.orig_cmd.as_slice()), (Some(rem), rem_cmd));
            assert_eq!(fix.add_set, REM_SET);
            assert!(fix.values.is_empty());
        }
        let q = read(MsgType::ReqRedisGet, None);
        let fix = repair_request(
            &q,
            MsgType::ReqRedisGet,
            &e(Reply::Status(b"s".to_vec())),
            true,
        );
        assert_eq!(fix.map(|f| f.values), Some(vec![b"s".to_vec()]));
        assert!(repair_request(&q, MsgType::ReqRedisGet, &e(Reply::Bulk(None)), true).is_none());
        assert!(repair_request(&q, MsgType::ReqRedisSet, &removed, false).is_none());
        assert!(repair_request(&q, MsgType::ReqRedisLpush, &removed, false).is_none());
    }
}
