//! The internal repair-script request and its execution.
//!
//! A repair-aware write or read is forwarded to each replica as one RESP
//! array whose first element is [`REPAIR_SCRIPT_CMD`], a command name no
//! datastore implements. The replica's datastore connection recognises it
//! with [`ScriptRequest::decode`], runs the matching [`super::logic`]
//! script with a [`ScriptRun`] on that same connection, and answers with
//! the script's reply encoded by [`encode_reply`].
//!
//! Wire layout (every element a bulk string):
//!
//! ```text
//! DYNOMITE.REPAIR-SCRIPT <script> <key> <add_set> <rem_set> <orig_cmd>
//!     <num_opts> <num_fields> <num_values> <ts> <opt>... <field>... <value>...
//! ```

use crate::msg::{Msg, MsgType};

use super::logic::{self, RepairStore, Reply, ScriptKeys, ScriptResult};

/// Command name that marks a repair-script request.
pub const REPAIR_SCRIPT_CMD: &[u8] = b"DYNOMITE.REPAIR-SCRIPT";
/// Top-level set of write timestamps.
pub const ADD_SET: &[u8] = b"._add-set";
/// Top-level set of delete timestamps.
pub const REM_SET: &[u8] = b"._rem-set";

/// Which script a request runs.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::request::Script;
///
/// assert_eq!(Script::from_name(Script::Hget.name()), Some(Script::Hget));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Script {
    /// Whole-value write.
    Set,
    /// Whole-value read with metadata.
    Get,
    /// Key delete.
    Del,
    /// Field writes with values (HSET, HMSET).
    Hset,
    /// Field deletes (HDEL, SREM, ZREM).
    Hdel,
    /// Field read with metadata (HGET, SISMEMBER, ZSCORE).
    Hget,
    /// Sorted-set member writes.
    Zadd,
    /// Set member writes.
    Sadd,
    /// Drop a key's delete marker once every replica deleted it.
    CleanupDel,
    /// Drop a field's delete marker once every replica deleted it.
    CleanupHdel,
}

const SCRIPTS: [(Script, &[u8]); 10] = [
    (Script::Set, b"SET"),
    (Script::Get, b"GET"),
    (Script::Del, b"DEL"),
    (Script::Hset, b"HSET"),
    (Script::Hdel, b"HDEL"),
    (Script::Hget, b"HGET"),
    (Script::Zadd, b"ZADD"),
    (Script::Sadd, b"SADD"),
    (Script::CleanupDel, b"CLEANUP_DEL"),
    (Script::CleanupHdel, b"CLEANUP_HDEL"),
];

impl Script {
    /// Wire name of the script.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::proto::redis::repair::request::Script;
    ///
    /// assert_eq!(Script::CleanupDel.name(), b"CLEANUP_DEL");
    /// ```
    #[must_use]
    pub fn name(self) -> &'static [u8] {
        SCRIPTS
            .iter()
            .find(|(s, _)| *s == self)
            .map_or(b"", |(_, n)| n)
    }

    /// Parse a wire name.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::proto::redis::repair::request::Script;
    ///
    /// assert_eq!(Script::from_name(b"ZADD"), Some(Script::Zadd));
    /// assert_eq!(Script::from_name(b"LPUSH"), None);
    /// ```
    #[must_use]
    pub fn from_name(name: &[u8]) -> Option<Self> {
        SCRIPTS.iter().find(|(_, n)| *n == name).map(|(s, _)| *s)
    }
}

/// Per-command repair traits.
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::redis::repair::request::cmd_info;
///
/// let get = cmd_info(MsgType::ReqRedisGet).unwrap();
/// assert!(get.is_repairable);
/// assert_eq!(get.repair_by_add, Some(MsgType::ReqRedisSet));
/// assert!(cmd_info(MsgType::ReqRedisLpush).is_none());
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CmdInfo {
    /// Command name passed to the script as the original command.
    pub cmd_str: &'static [u8],
    /// Script the command is rewritten into.
    pub script: Script,
    /// Deletes record their timestamp in the remove set.
    pub is_delete: bool,
    /// Reads whose replies carry status and timestamp.
    pub is_repairable: bool,
    /// Command that repairs a stale replica when the freshest value exists.
    pub repair_by_add: Option<MsgType>,
    /// Command that repairs a stale replica when the freshest value is deleted.
    pub repair_by_rem: Option<MsgType>,
}

const fn info(
    cmd_str: &'static [u8],
    script: Script,
    is_delete: bool,
    repair: Option<(MsgType, MsgType)>,
) -> CmdInfo {
    let (repair_by_add, repair_by_rem) = match repair {
        Some((a, r)) => (Some(a), Some(r)),
        None => (None, None),
    };
    CmdInfo {
        cmd_str,
        script,
        is_delete,
        is_repairable: repair.is_some(),
        repair_by_add,
        repair_by_rem,
    }
}

/// Repair traits of `ty`, or `None` when the command has no repair support.
#[must_use]
pub fn cmd_info(ty: MsgType) -> Option<CmdInfo> {
    use MsgType as M;
    Some(match ty {
        M::ReqRedisDel => info(b"DEL", Script::Del, true, None),
        M::ReqRedisGet => info(
            b"GET",
            Script::Get,
            false,
            Some((M::ReqRedisSet, M::ReqRedisDel)),
        ),
        M::ReqRedisSet => info(b"SET", Script::Set, false, None),
        M::ReqRedisHdel => info(b"HDEL", Script::Hdel, true, None),
        M::ReqRedisHget => info(
            b"HGET",
            Script::Hget,
            false,
            Some((M::ReqRedisHset, M::ReqRedisHdel)),
        ),
        M::ReqRedisHmset => info(b"HMSET", Script::Hset, false, None),
        M::ReqRedisHset => info(b"HSET", Script::Hset, false, None),
        M::ReqRedisSadd => info(b"SADD", Script::Sadd, false, None),
        M::ReqRedisSismember => info(
            b"SISMEMBER",
            Script::Hget,
            false,
            Some((M::ReqRedisSadd, M::ReqRedisSrem)),
        ),
        M::ReqRedisSrem => info(b"SREM", Script::Hdel, true, None),
        M::ReqRedisZadd => info(b"ZADD", Script::Zadd, false, None),
        M::ReqRedisZrem => info(b"ZREM", Script::Hdel, true, None),
        M::ReqRedisZscore => info(
            b"ZSCORE",
            Script::Hget,
            false,
            Some((M::ReqRedisZadd, M::ReqRedisZrem)),
        ),
        _ => return None,
    })
}

/// A decoded repair-script request.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::request::{Script, ScriptRequest};
///
/// let req = ScriptRequest::cleanup(Script::CleanupDel, b"k", None, 9);
/// let wire = req.encode();
/// assert_eq!(ScriptRequest::decode(&wire), Some(req));
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScriptRequest {
    /// Script to run.
    pub script: Option<Script>,
    /// Primary key.
    pub key: Vec<u8>,
    /// Set the operation records its timestamp in.
    pub add_set: Vec<u8>,
    /// Set checked for a newer opposite operation.
    pub rem_set: Vec<u8>,
    /// Original command name.
    pub orig_cmd: Vec<u8>,
    /// Timestamp in milliseconds.
    pub ts: u64,
    /// ZADD options (NX, XX, CH, INCR).
    pub opts: Vec<Vec<u8>>,
    /// Fields, members or set elements.
    pub fields: Vec<Vec<u8>>,
    /// Values or scores, paired with `fields` when both are present.
    pub values: Vec<Vec<u8>>,
}

// Token count for a run starting at `start` that advances by `jump`.
fn total_tokens(jump: usize, start: usize, min: usize, nelem: usize) -> usize {
    if jump == 0 {
        return min;
    }
    let n = nelem.saturating_sub(start);
    n / jump + n % jump
}

fn take(args: &[Vec<u8>], first: usize, jump: usize, count: usize) -> Option<Vec<Vec<u8>>> {
    let step = jump.max(1);
    (0..count)
        .map(|i| args.get(first + i * step).cloned())
        .collect()
}

fn is_zadd_opt(arg: &[u8]) -> bool {
    [&b"NX"[..], b"XX", b"INCR", b"CH"]
        .iter()
        .any(|o| arg.len() >= o.len() && arg[..o.len()].eq_ignore_ascii_case(o))
}

impl ScriptRequest {
    /// Build the request a client command is rewritten into, or `None` when
    /// the command has no repair support or lacks the arguments it needs.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{keypos::ArgPos, KeyPos, Msg, MsgType};
    /// use dynomite::proto::redis::repair::request::{Script, ScriptRequest};
    ///
    /// let mut m = Msg::new(1, MsgType::ReqRedisHset, true);
    /// m.push_key(KeyPos::without_tag(b"h".to_vec()));
    /// m.push_arg(ArgPos::new(b"f".to_vec()));
    /// m.push_arg(ArgPos::new(b"v".to_vec()));
    /// let r = ScriptRequest::from_request(&m, 5).unwrap();
    /// assert_eq!(r.script, Some(Script::Hset));
    /// assert_eq!((r.fields, r.values), (vec![b"f".to_vec()], vec![b"v".to_vec()]));
    /// ```
    #[must_use]
    pub fn from_request(req: &Msg, ts: u64) -> Option<Self> {
        let info = cmd_info(req.ty())?;
        // A multi-key DEL is split into one request per key before it is
        // rewritten; an unsplit one keeps its plain form.
        if req.keys().len() != 1 {
            return None;
        }
        // The script key is the hash-tag range, as routing uses.
        let key = req.keys()[0].tag_bytes().to_vec();
        let args: Vec<Vec<u8>> = req.args().iter().map(|a| a.bytes().to_vec()).collect();
        let nelem = args.len();
        // (first_field_pos, field_jump, min_fields, first_value_pos, value_jump, min_values),
        // positions 1-based over the non-key arguments; 0 means absent.
        let (ff, fj, fmin, fv, vj, vmin) = match req.ty() {
            MsgType::ReqRedisSet => (0, 0, 0, 1, 0, 1),
            MsgType::ReqRedisHdel
            | MsgType::ReqRedisSadd
            | MsgType::ReqRedisSrem
            | MsgType::ReqRedisZrem => (1, 1, 1, 0, 0, 0),
            MsgType::ReqRedisHget | MsgType::ReqRedisSismember | MsgType::ReqRedisZscore => {
                (1, 0, 1, 0, 0, 0)
            }
            MsgType::ReqRedisHmset => (1, 2, 1, 2, 2, 1),
            MsgType::ReqRedisHset => (1, 0, 1, 2, 0, 1),
            MsgType::ReqRedisZadd => (2, 2, 1, 1, 2, 1),
            _ => (0, 0, 0, 0, 0, 0),
        };
        let opts: Vec<Vec<u8>> = if req.ty() == MsgType::ReqRedisZadd {
            args.iter()
                .take_while(|a| is_zadd_opt(a))
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        let nopt = opts.len();
        let fields = if ff == 0 {
            Vec::new()
        } else {
            let first = ff + nopt - 1;
            take(&args, first, fj, total_tokens(fj, first, fmin, nelem))?
        };
        let values = if fv == 0 {
            Vec::new()
        } else {
            let first = fv + nopt - 1;
            take(&args, first, vj, total_tokens(vj, first, vmin, nelem))?
        };
        let (add_set, rem_set) = if info.is_delete {
            (REM_SET, ADD_SET)
        } else {
            (ADD_SET, REM_SET)
        };
        Some(Self {
            script: Some(info.script),
            key,
            add_set: add_set.to_vec(),
            rem_set: rem_set.to_vec(),
            orig_cmd: info.cmd_str.to_vec(),
            ts,
            opts,
            fields,
            values,
        })
    }

    /// A metadata cleanup request for `key` (and `field` for field deletes).
    #[must_use]
    pub fn cleanup(script: Script, key: &[u8], field: Option<&[u8]>, ts: u64) -> Self {
        Self {
            script: Some(script),
            key: key.to_vec(),
            add_set: ADD_SET.to_vec(),
            rem_set: REM_SET.to_vec(),
            orig_cmd: Vec::new(),
            ts,
            opts: Vec::new(),
            fields: field.map(|f| vec![f.to_vec()]).unwrap_or_default(),
            values: Vec::new(),
        }
    }

    /// Encode as the RESP request a replica's datastore connection runs.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let script = self.script.map_or(&b""[..], Script::name);
        let counts = [
            self.opts.len().to_string(),
            self.fields.len().to_string(),
            self.values.len().to_string(),
            self.ts.to_string(),
        ];
        let mut args: Vec<&[u8]> = vec![
            REPAIR_SCRIPT_CMD,
            script,
            &self.key,
            &self.add_set,
            &self.rem_set,
            &self.orig_cmd,
        ];
        args.extend(counts.iter().map(String::as_bytes));
        args.extend(self.opts.iter().map(Vec::as_slice));
        args.extend(self.fields.iter().map(Vec::as_slice));
        args.extend(self.values.iter().map(Vec::as_slice));
        encode_command(&args)
    }

    /// Decode a request produced by [`ScriptRequest::encode`]; `None` when
    /// `wire` is not a complete repair-script request.
    #[must_use]
    pub fn decode(wire: &[u8]) -> Option<Self> {
        let (Reply::Array(items), used) = parse_reply(wire)? else {
            return None;
        };
        if used != wire.len() {
            return None;
        }
        let mut args = items.into_iter().map(|r| match r {
            Reply::Bulk(Some(b)) => Some(b),
            _ => None,
        });
        let mut next = || args.next().flatten();
        if next()? != REPAIR_SCRIPT_CMD {
            return None;
        }
        let script = Some(Script::from_name(&next()?)?);
        let key = next()?;
        let add_set = next()?;
        let rem_set = next()?;
        let orig_cmd = next()?;
        let mut num = || -> Option<u64> { std::str::from_utf8(&next()?).ok()?.parse().ok() };
        let nopt = usize::try_from(num()?).ok()?;
        let nfld = usize::try_from(num()?).ok()?;
        let nval = usize::try_from(num()?).ok()?;
        let ts = num()?;
        let mut list = |n: usize| (0..n).map(|_| next()).collect::<Option<Vec<_>>>();
        let opts = list(nopt)?;
        let fields = list(nfld)?;
        let values = list(nval)?;
        if next().is_some() {
            return None;
        }
        Some(Self {
            script,
            key,
            add_set,
            rem_set,
            orig_cmd,
            ts,
            opts,
            fields,
            values,
        })
    }

    /// Run the script against `store`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::proto::redis::repair::logic::Reply;
    /// use dynomite::proto::redis::repair::request::{Script, ScriptRequest};
    ///
    /// let req = ScriptRequest::cleanup(Script::CleanupDel, b"k", None, 9);
    /// let mut store = |_: &[&[u8]]| Reply::Bulk(None);
    /// assert_eq!(req.run(&mut store), Ok(Reply::Integer(0)));
    /// ```
    pub fn run(&self, store: &mut impl RepairStore) -> ScriptResult {
        let keys = ScriptKeys {
            key: &self.key,
            add_set: &self.add_set,
            rem_set: &self.rem_set,
        };
        let ts = self.ts.to_string();
        let ts = ts.as_bytes();
        let cmd = self.orig_cmd.as_slice();
        let fields: Vec<&[u8]> = self.fields.iter().map(Vec::as_slice).collect();
        let values: Vec<&[u8]> = self.values.iter().map(Vec::as_slice).collect();
        let opts: Vec<&[u8]> = self.opts.iter().map(Vec::as_slice).collect();
        let pairs: Vec<(&[u8], &[u8])> =
            fields.iter().copied().zip(values.iter().copied()).collect();
        let arity = || {
            Err(Reply::Error(
                b"ERR wrong number of arguments for repair script".to_vec(),
            ))
        };
        match self.script {
            Some(Script::Set) => match values.first() {
                Some(v) => logic::set_script(store, keys, cmd, ts, v),
                None => arity(),
            },
            Some(Script::Get) => logic::get_script(store, keys, cmd),
            Some(Script::Del) => logic::del_script(store, keys, cmd, ts),
            Some(Script::Hset) => logic::hset_script(store, keys, cmd, ts, &pairs),
            Some(Script::Hdel) => logic::hdel_script(store, keys, cmd, ts, &fields),
            Some(Script::Hget) => match fields.first() {
                Some(f) => logic::hget_script(store, keys, cmd, f),
                None => arity(),
            },
            Some(Script::Zadd) => logic::zadd_script(store, keys, cmd, ts, &opts, &pairs),
            Some(Script::Sadd) => logic::sadd_script(store, keys, cmd, ts, &fields),
            Some(Script::CleanupDel) => logic::cleanup_del_script(store, keys, ts),
            Some(Script::CleanupHdel) => match fields.first() {
                Some(f) => logic::cleanup_hdel_script(store, keys, ts, f),
                None => arity(),
            },
            None => arity(),
        }
    }
}

/// One step of a script run over an asynchronous connection.
#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// Send this command and [`ScriptRun::feed`] its reply.
    Command(Vec<Vec<u8>>),
    /// The script finished with this result.
    Done(ScriptResult),
}

/// Drives a script one datastore command at a time.
///
/// The script is replayed from the start on every step with the replies
/// gathered so far; it is deterministic, so the replay issues the same
/// commands and stops at the first one without a reply.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::Reply;
/// use dynomite::proto::redis::repair::request::{Script, ScriptRequest, ScriptRun, Step};
///
/// let mut run = ScriptRun::new(ScriptRequest::cleanup(Script::CleanupDel, b"k", None, 9));
/// assert_eq!(run.step(), Step::Command(vec![b"ZSCORE".to_vec(), b"._rem-set".to_vec(), b"k".to_vec()]));
/// run.feed(Reply::Bulk(None));
/// assert_eq!(run.step(), Step::Done(Ok(Reply::Integer(0))));
/// ```
#[derive(Debug)]
pub struct ScriptRun {
    req: ScriptRequest,
    replies: Vec<Reply>,
}

struct Replay<'a> {
    replies: &'a [Reply],
    pos: usize,
    next: Option<Vec<Vec<u8>>>,
}

impl RepairStore for Replay<'_> {
    fn call(&mut self, args: &[&[u8]]) -> Reply {
        if let Some(r) = self.replies.get(self.pos) {
            self.pos += 1;
            return r.clone();
        }
        self.next = Some(args.iter().map(|a| a.to_vec()).collect());
        // An error reply aborts the script at this command.
        Reply::Error(Vec::new())
    }
}

impl ScriptRun {
    /// Start a run of `req`.
    #[must_use]
    pub fn new(req: ScriptRequest) -> Self {
        Self {
            req,
            replies: Vec::new(),
        }
    }

    /// Next command to send, or the script's result.
    pub fn step(&mut self) -> Step {
        let mut replay = Replay {
            replies: &self.replies,
            pos: 0,
            next: None,
        };
        let result = self.req.run(&mut replay);
        match replay.next {
            Some(cmd) => Step::Command(cmd),
            None => Step::Done(result),
        }
    }

    /// Record the reply to the command the last [`ScriptRun::step`] returned.
    pub fn feed(&mut self, reply: Reply) {
        self.replies.push(reply);
    }
}

/// Encode a command as a RESP array of bulk strings.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::request::encode_command;
///
/// assert_eq!(encode_command(&[&b"GET"[..], b"k"]), b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
/// ```
#[must_use]
pub fn encode_command<A: AsRef<[u8]>>(args: &[A]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        let a = a.as_ref();
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Encode a reply in RESP.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::Reply;
/// use dynomite::proto::redis::repair::request::encode_reply;
///
/// let r = Reply::Array(vec![Reply::Bulk(Some(b"X".to_vec())), Reply::Integer(0), Reply::Bulk(None)]);
/// assert_eq!(encode_reply(&r), b"*3\r\n$1\r\nX\r\n:0\r\n$-1\r\n");
/// ```
#[must_use]
pub fn encode_reply(reply: &Reply) -> Vec<u8> {
    let mut out = Vec::new();
    write_reply(reply, &mut out);
    out
}

fn write_reply(reply: &Reply, out: &mut Vec<u8>) {
    match reply {
        Reply::Status(s) => {
            out.push(b'+');
            out.extend_from_slice(s);
            out.extend_from_slice(b"\r\n");
        }
        Reply::Error(e) => {
            out.push(b'-');
            out.extend_from_slice(e);
            out.extend_from_slice(b"\r\n");
        }
        Reply::Integer(n) => out.extend_from_slice(format!(":{n}\r\n").as_bytes()),
        Reply::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
        Reply::Bulk(Some(b)) => {
            out.extend_from_slice(format!("${}\r\n", b.len()).as_bytes());
            out.extend_from_slice(b);
            out.extend_from_slice(b"\r\n");
        }
        Reply::Array(items) => {
            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
            for i in items {
                write_reply(i, out);
            }
        }
    }
}

/// Parse one RESP reply from the front of `buf`; `None` when incomplete or
/// malformed. Returns the reply and the bytes it used.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::Reply;
/// use dynomite::proto::redis::repair::request::parse_reply;
///
/// assert_eq!(parse_reply(b":7\r\n+OK"), Some((Reply::Integer(7), 4)));
/// assert_eq!(parse_reply(b"$3\r\nab"), None);
/// ```
#[must_use]
pub fn parse_reply(buf: &[u8]) -> Option<(Reply, usize)> {
    let line_end = buf.windows(2).position(|w| w == b"\r\n")?;
    let line = &buf[1..line_end];
    let rest = line_end + 2;
    let int = || -> Option<i64> { std::str::from_utf8(line).ok()?.parse().ok() };
    match *buf.first()? {
        b'+' => Some((Reply::Status(line.to_vec()), rest)),
        b'-' => Some((Reply::Error(line.to_vec()), rest)),
        b':' => Some((Reply::Integer(int()?), rest)),
        b'$' => {
            let n = int()?;
            if n < 0 {
                return Some((Reply::Bulk(None), rest));
            }
            let end = rest.checked_add(usize::try_from(n).ok()?)?;
            if buf.get(end..end + 2)? != b"\r\n" {
                return None;
            }
            Some((Reply::Bulk(Some(buf[rest..end].to_vec())), end + 2))
        }
        b'*' => {
            let n = int()?;
            if n < 0 {
                return Some((Reply::Bulk(None), rest));
            }
            let mut pos = rest;
            let mut items = Vec::new();
            for _ in 0..n {
                let (item, used) = parse_reply(&buf[pos..])?;
                items.push(item);
                pos += used;
            }
            Some((Reply::Array(items), pos))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::MsgParseResult;
    use crate::proto::redis::redis_parse_req;

    fn parsed(wire: &[u8]) -> Msg {
        let mut m = Msg::new(1, MsgType::Unknown, true);
        assert_eq!(redis_parse_req(&mut m, wire), MsgParseResult::Ok);
        m
    }

    fn words(s: &str) -> Vec<Vec<u8>> {
        s.split_whitespace()
            .map(|w| w.as_bytes().to_vec())
            .collect()
    }

    // `cmd` is the client command; `opts`, `fields`, `values` the expected
    // script argument lists, space separated.
    fn check(cmd: &str, script: Script, opts: &str, fields: &str, values: &str, is_delete: bool) {
        let m = parsed(&encode_command(&words(cmd)));
        let r = ScriptRequest::from_request(&m, 42).expect("rewrite");
        assert_eq!(r.script, Some(script), "{cmd}");
        assert_eq!(r.key, b"k");
        assert_eq!(r.orig_cmd, words(cmd)[0]);
        assert_eq!(r.opts, words(opts), "{cmd}");
        assert_eq!(r.fields, words(fields), "{cmd}");
        assert_eq!(r.values, words(values), "{cmd}");
        let sets = if is_delete {
            (REM_SET, ADD_SET)
        } else {
            (ADD_SET, REM_SET)
        };
        assert_eq!((r.add_set.as_slice(), r.rem_set.as_slice()), sets);
        assert_eq!(ScriptRequest::decode(&r.encode()), Some(r));
    }

    #[test]
    fn rewrite_layout_per_command() {
        check("SET k v", Script::Set, "", "", "v", false);
        check("GET k", Script::Get, "", "", "", false);
        check("DEL k", Script::Del, "", "", "", true);
        check("HSET k f v", Script::Hset, "", "f", "v", false);
        check(
            "HMSET k f1 v1 f2 v2",
            Script::Hset,
            "",
            "f1 f2",
            "v1 v2",
            false,
        );
        check("HDEL k a b", Script::Hdel, "", "a b", "", true);
        check("HGET k f", Script::Hget, "", "f", "", false);
        check("SADD k a b", Script::Sadd, "", "a b", "", false);
        check("SREM k a", Script::Hdel, "", "a", "", true);
        check("SISMEMBER k a", Script::Hget, "", "a", "", false);
        check(
            "ZADD k NX CH 1 m1 2 m2",
            Script::Zadd,
            "NX CH",
            "m1 m2",
            "1 2",
            false,
        );
        check("ZREM k m", Script::Hdel, "", "m", "", true);
        check("ZSCORE k m", Script::Hget, "", "m", "", false);
        let incr = parsed(&encode_command(&words("INCR k")));
        assert!(ScriptRequest::from_request(&incr, 1).is_none());
    }

    #[test]
    fn decode_rejects_other_commands_and_truncation() {
        assert!(ScriptRequest::decode(&encode_command(&[&b"GET"[..], b"k"])).is_none());
        let wire = ScriptRequest::cleanup(Script::CleanupHdel, b"k", Some(b"f"), 3).encode();
        assert!(ScriptRequest::decode(&wire[..wire.len() - 1]).is_none());
    }

    #[test]
    fn run_steps_match_a_direct_run() {
        let req =
            ScriptRequest::from_request(&parsed(&encode_command(&[&b"SET"[..], b"k", b"v"])), 5)
                .expect("rewrite");
        let mut log = Vec::new();
        let mut direct = |args: &[&[u8]]| {
            log.push(args.iter().map(|a| a.to_vec()).collect::<Vec<_>>());
            match args[0] {
                b"SET" => Reply::Status(b"OK".to_vec()),
                b"ZSCORE" => Reply::Bulk(None),
                _ => Reply::Integer(1),
            }
        };
        let want = req.run(&mut direct);
        let mut run = ScriptRun::new(req);
        let mut sent = Vec::new();
        let got = loop {
            match run.step() {
                Step::Command(cmd) => {
                    let r = match cmd[0].as_slice() {
                        b"SET" => Reply::Status(b"OK".to_vec()),
                        b"ZSCORE" => Reply::Bulk(None),
                        _ => Reply::Integer(1),
                    };
                    sent.push(cmd);
                    run.feed(r);
                }
                Step::Done(r) => break r,
            }
        };
        assert_eq!(got, want);
        assert_eq!(sent, log);
    }

    #[test]
    fn reply_round_trip() {
        let r = Reply::Array(vec![
            Reply::Bulk(Some(b"E".to_vec())),
            Reply::Bulk(Some(b"17".to_vec())),
            Reply::Array(vec![
                Reply::Status(b"OK".to_vec()),
                Reply::Error(b"ERR x".to_vec()),
            ]),
        ]);
        let wire = encode_reply(&r);
        assert_eq!(parse_reply(&wire), Some((r, wire.len())));
    }
}
