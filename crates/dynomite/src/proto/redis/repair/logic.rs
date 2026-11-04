//! Read-repair metadata scripts, evaluated by the engine.
//!
//! A repair-aware write records its timestamp in sorted sets kept next to
//! the data. The top-level add set (`._add-set`) and remove set
//! (`._rem-set`) hold one member per primary key, scored by the timestamp
//! of the last write or delete. The composite sets `<set>_<key>` hold one
//! member per field of a hash, set or sorted set. A write is applied only
//! when it is not older than what the sets record; a read returns the value
//! together with its recorded status and timestamp so the coordinator can
//! pick the freshest replica.
//!
//! Each function runs one script against a [`RepairStore`]: it issues the
//! same datastore commands in the same order and evaluates the same
//! comparisons. Several branches depend on how a script sees replies:
//!
//! * A nil bulk reply is `false`. Every other reply is truthy, including the
//!   integer `0`, so a check on an `EXISTS` reply always passes.
//! * Timestamps are compared as numbers: the timestamp argument and every
//!   `ZSCORE` reply are parsed as decimal numbers before `<` or `>`.
//! * An error reply from any command aborts the script and becomes its
//!   result ([`Err`]); writes issued before the error stay applied.
//! * A script value returned to the client keeps the reply it came from;
//!   the literal `0` timestamp of a missing entry is an integer reply and a
//!   recorded timestamp is the bulk string `ZSCORE` produced.
//!
//! Functions take the script's `KEYS` as a [`ScriptKeys`] and the rest of
//! its arguments in argument order. Slots a script never reads are left
//! out, and every field count is the length of the slice that carries the
//! fields.

/// A datastore reply, as a script sees it and as it is returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// Status reply (`+OK`).
    Status(Vec<u8>),
    /// Error reply (`-ERR ...`), without the leading `-`.
    Error(Vec<u8>),
    /// Integer reply (`:1`).
    Integer(i64),
    /// Bulk string reply; `None` is the nil bulk (`$-1`).
    Bulk(Option<Vec<u8>>),
    /// Multi-bulk reply.
    Array(Vec<Reply>),
}

impl Reply {
    /// `false` only for the nil bulk reply.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::proto::redis::repair::logic::Reply;
    ///
    /// assert!(Reply::Integer(0).is_truthy());
    /// assert!(!Reply::Bulk(None).is_truthy());
    /// ```
    pub fn is_truthy(&self) -> bool {
        !matches!(self, Reply::Bulk(None))
    }

    fn bulk(bytes: &[u8]) -> Self {
        Reply::Bulk(Some(bytes.to_vec()))
    }
}

/// Outcome of one script: `Ok` is the script's reply, `Err` the error
/// reply of the command that aborted it.
pub type ScriptResult = Result<Reply, Reply>;

/// The datastore a script runs against.
///
/// Any `FnMut(&[&[u8]]) -> Reply` closure is a store, which keeps tests and
/// examples short.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::{RepairStore, Reply};
///
/// let mut store = |args: &[&[u8]]| Reply::Integer(args.len() as i64);
/// assert_eq!(store.call(&[b"ZCARD", b"s"]), Reply::Integer(2));
/// ```
pub trait RepairStore {
    /// Run one command (`args[0]` is the command name) and return its reply.
    fn call(&mut self, args: &[&[u8]]) -> Reply;
}

impl<F> RepairStore for F
where
    F: FnMut(&[&[u8]]) -> Reply,
{
    fn call(&mut self, args: &[&[u8]]) -> Reply {
        self(args)
    }
}

/// The three `KEYS` every script receives.
///
/// Deletes pass the add and remove sets swapped, so a delete records its
/// timestamp in the remove set through the same script shape.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::ScriptKeys;
///
/// let keys = ScriptKeys { key: b"k", add_set: b"._add-set", rem_set: b"._rem-set" };
/// assert_eq!(keys.key, b"k");
/// ```
#[derive(Clone, Copy, Debug)]
pub struct ScriptKeys<'a> {
    /// The primary key the command touches.
    pub key: &'a [u8],
    /// Top-level set the write records its timestamp in.
    pub add_set: &'a [u8],
    /// Top-level set checked for a newer opposite operation.
    pub rem_set: &'a [u8],
}

impl ScriptKeys<'_> {
    fn composite_add(&self) -> Vec<u8> {
        [self.add_set, b"_", self.key].concat()
    }

    fn composite_rem(&self) -> Vec<u8> {
        [self.rem_set, b"_", self.key].concat()
    }
}

type Step<T> = Result<T, Reply>;

fn call(store: &mut impl RepairStore, args: &[&[u8]]) -> Step<Reply> {
    match store.call(args) {
        Reply::Error(e) => Err(Reply::Error(e)),
        reply => Ok(reply),
    }
}

fn zscore(store: &mut impl RepairStore, set: &[u8], member: &[u8]) -> Step<Reply> {
    call(store, &[b"ZSCORE", set, member])
}

// shortcut: decimal only (no hex), timestamps and ZSCORE replies are decimal.
fn to_number(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes).ok()?.trim().parse().ok()
}

fn reply_number(reply: &Reply) -> Option<f64> {
    match reply {
        Reply::Bulk(Some(bytes)) => to_number(bytes),
        _ => None,
    }
}

fn numbers(cur_ts: &[u8], recorded: &Reply) -> Step<(f64, f64)> {
    match (to_number(cur_ts), reply_number(recorded)) {
        (Some(a), Some(b)) => Ok((a, b)),
        _ => Err(Reply::Error(
            b"ERR user_script: attempt to compare nil with number".to_vec(),
        )),
    }
}

fn older(cur_ts: &[u8], recorded: &Reply) -> Step<bool> {
    numbers(cur_ts, recorded).map(|(a, b)| a < b)
}

fn newer(cur_ts: &[u8], recorded: &Reply) -> Step<bool> {
    numbers(cur_ts, recorded).map(|(a, b)| a > b)
}

// shortcut: only integer replies add up; the commands summed (HDEL, SREM,
// ZREM, SADD) reply with integers, so string-to-number coercion is unneeded.
fn add_count(total: i64, reply: &Reply) -> Step<i64> {
    match reply {
        Reply::Integer(n) => Ok(total + n),
        _ => Err(Reply::Error(
            b"ERR user_script: attempt to perform arithmetic on a non-number value".to_vec(),
        )),
    }
}

/// Top-level prologue of the field writes: `false` when the key carries a
/// newer remove, otherwise clear that remove and raise the key's add entry.
fn claim_key(store: &mut impl RepairStore, keys: ScriptKeys<'_>, cur_ts: &[u8]) -> Step<bool> {
    let removed = zscore(store, keys.rem_set, keys.key)?;
    if removed.is_truthy() {
        if older(cur_ts, &removed)? {
            return Ok(false);
        }
        call(store, &[b"ZREM", keys.rem_set, keys.key])?;
    }
    let added = zscore(store, keys.add_set, keys.key)?;
    if added.is_truthy() {
        if newer(cur_ts, &added)? {
            call(store, &[b"ZADD", keys.add_set, cur_ts, keys.key])?;
        }
    } else {
        call(store, &[b"ZADD", keys.add_set, cur_ts, keys.key])?;
    }
    Ok(true)
}

/// Per-field staleness check. A field's remove entry is dropped only when
/// this operation is not older than it: a stale write or delete leaves the
/// newer remove entry in place, so the field stays deleted (DEV-R2).
fn field_is_stale(
    store: &mut impl RepairStore,
    add_set: &[u8],
    rem_set: &[u8],
    cur_ts: &[u8],
    field: &[u8],
) -> Step<bool> {
    let added = zscore(store, add_set, field)?;
    let removed = zscore(store, rem_set, field)?;
    if removed.is_truthy() {
        let stale = older(cur_ts, &removed)?;
        if !stale {
            call(store, &[b"ZREM", rem_set, field])?;
        }
        Ok(stale)
    } else if added.is_truthy() {
        older(cur_ts, &added)
    } else {
        Ok(false)
    }
}

/// Write a whole value (`SET key value`) unless a newer write or delete is
/// recorded; a stale write replies `-1`.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::{set_script, Reply, ScriptKeys};
///
/// let mut log = Vec::new();
/// let mut store = |args: &[&[u8]]| {
///     log.push(String::from_utf8(args.join(&b' ')).unwrap());
///     match args[0] {
///         b"SET" => Reply::Status(b"OK".to_vec()),
///         b"ZSCORE" => Reply::Bulk(None),
///         _ => Reply::Integer(1),
///     }
/// };
/// let keys = ScriptKeys { key: b"k", add_set: b"._add-set", rem_set: b"._rem-set" };
/// let reply = set_script(&mut store, keys, b"SET", b"5", b"v");
/// assert_eq!(reply, Ok(Reply::Status(b"OK".to_vec())));
/// assert_eq!(
///     log,
///     ["ZSCORE ._add-set k", "ZSCORE ._rem-set k", "ZADD ._add-set 5 k", "SET k v"]
/// );
/// ```
pub fn set_script(
    store: &mut impl RepairStore,
    keys: ScriptKeys<'_>,
    orig_cmd: &[u8],
    cur_ts: &[u8],
    value: &[u8],
) -> ScriptResult {
    let added = zscore(store, keys.add_set, keys.key)?;
    let removed = zscore(store, keys.rem_set, keys.key)?;
    if removed.is_truthy() {
        if older(cur_ts, &removed)? {
            return Ok(Reply::Integer(-1));
        }
        call(store, &[b"ZREM", keys.rem_set, keys.key])?;
    } else if added.is_truthy() && older(cur_ts, &added)? {
        return Ok(Reply::Integer(-1));
    }
    call(store, &[b"ZADD", keys.add_set, cur_ts, keys.key])?;
    call(store, &[orig_cmd, keys.key, value])
}

/// Read a whole value (`GET key`) with its status and timestamp:
/// `[E, ts, value]` when written, `[R, ts, value]` when deleted,
/// `[X, 0, value]` when nothing is recorded. An add entry whose value is
/// gone is dropped.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::{get_script, Reply, ScriptKeys};
///
/// let mut store = |args: &[&[u8]]| match (args[0], args[1]) {
///     (b"GET", _) => Reply::Bulk(Some(b"v".to_vec())),
///     (b"ZSCORE", b"._add-set") => Reply::Bulk(Some(b"5".to_vec())),
///     _ => Reply::Bulk(None),
/// };
/// let keys = ScriptKeys { key: b"k", add_set: b"._add-set", rem_set: b"._rem-set" };
/// let bulk = |s: &[u8]| Reply::Bulk(Some(s.to_vec()));
/// assert_eq!(
///     get_script(&mut store, keys, b"GET"),
///     Ok(Reply::Array(vec![bulk(b"E"), bulk(b"5"), bulk(b"v")]))
/// );
/// ```
pub fn get_script(
    store: &mut impl RepairStore,
    keys: ScriptKeys<'_>,
    orig_cmd: &[u8],
) -> ScriptResult {
    let value = call(store, &[orig_cmd, keys.key])?;
    let added = zscore(store, keys.add_set, keys.key)?;
    if added.is_truthy() && value.is_truthy() {
        return Ok(Reply::Array(vec![Reply::bulk(b"E"), added, value]));
    } else if added.is_truthy() {
        call(store, &[b"ZREM", keys.add_set, keys.key])?;
    }
    let removed = zscore(store, keys.rem_set, keys.key)?;
    if removed.is_truthy() {
        return Ok(Reply::Array(vec![Reply::bulk(b"R"), removed, value]));
    }
    Ok(Reply::Array(vec![
        Reply::bulk(b"X"),
        Reply::Integer(0),
        value,
    ]))
}

/// Delete a key (`DEL key`), called with the sets swapped (see
/// [`ScriptKeys`]). Replies `0` without deleting when a newer write is
/// recorded or the key already has a delete entry. The existence check
/// always passes, so the key's composite sets are dropped and the delete
/// is recorded even for a missing key.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::{del_script, Reply, ScriptKeys};
///
/// let mut log = Vec::new();
/// let mut store = |args: &[&[u8]]| {
///     log.push(String::from_utf8(args.join(&b' ')).unwrap());
///     match args[0] {
///         b"ZSCORE" => Reply::Bulk(None),
///         _ => Reply::Integer(0),
///     }
/// };
/// let keys = ScriptKeys { key: b"k", add_set: b"._rem-set", rem_set: b"._add-set" };
/// assert_eq!(del_script(&mut store, keys, b"DEL", b"7"), Ok(Reply::Integer(0)));
/// assert_eq!(log[2..], ["EXISTS k", "DEL ._rem-set_k", "DEL ._add-set_k",
///     "ZADD ._rem-set 7 k", "DEL k"]);
/// ```
pub fn del_script(
    store: &mut impl RepairStore,
    keys: ScriptKeys<'_>,
    orig_cmd: &[u8],
    cur_ts: &[u8],
) -> ScriptResult {
    let added = zscore(store, keys.add_set, keys.key)?;
    let removed = zscore(store, keys.rem_set, keys.key)?;
    if removed.is_truthy() {
        if older(cur_ts, &removed)? {
            return Ok(Reply::Integer(0));
        }
        call(store, &[b"ZREM", keys.rem_set, keys.key])?;
    } else if added.is_truthy() {
        return Ok(Reply::Integer(0));
    }
    let exists = call(store, &[b"EXISTS", keys.key])?;
    if exists.is_truthy() {
        call(store, &[b"DEL", &keys.composite_add()])?;
        call(store, &[b"DEL", &keys.composite_rem()])?;
        call(store, &[b"ZADD", keys.add_set, cur_ts, keys.key])?;
        return call(store, &[orig_cmd, keys.key]);
    }
    Ok(Reply::Integer(0))
}

/// Write hash fields (`HSET` / `HMSET key field value ...`), skipping each
/// field that has a newer write or delete. Replies `0` when the key has a
/// newer delete, the bulk string `OK` for more than one pair, and otherwise
/// the last applied command's reply (nil when none applied).
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::{hset_script, Reply, ScriptKeys};
///
/// let mut store = |args: &[&[u8]]| match args[0] {
///     b"ZSCORE" => Reply::Bulk(None),
///     _ => Reply::Integer(1),
/// };
/// let keys = ScriptKeys { key: b"h", add_set: b"._add-set", rem_set: b"._rem-set" };
/// let pairs: [(&[u8], &[u8]); 2] = [(b"f1", b"a"), (b"f2", b"b")];
/// assert_eq!(
///     hset_script(&mut store, keys, b"HMSET", b"3", &pairs),
///     Ok(Reply::Bulk(Some(b"OK".to_vec())))
/// );
/// ```
pub fn hset_script(
    store: &mut impl RepairStore,
    keys: ScriptKeys<'_>,
    orig_cmd: &[u8],
    cur_ts: &[u8],
    pairs: &[(&[u8], &[u8])],
) -> ScriptResult {
    let add_set = keys.composite_add();
    let rem_set = keys.composite_rem();
    if !claim_key(store, keys, cur_ts)? {
        return Ok(Reply::Integer(0));
    }
    let mut ret = Reply::Bulk(None);
    for &(field, value) in pairs {
        if !field_is_stale(store, &add_set, &rem_set, cur_ts, field)? {
            call(store, &[b"ZADD", &add_set, cur_ts, field])?;
            ret = call(store, &[orig_cmd, keys.key, field, value])?;
        }
    }
    if pairs.len() > 1 {
        Ok(Reply::bulk(b"OK"))
    } else {
        Ok(ret)
    }
}

/// Remove members (`HDEL`, `SREM`, `ZREM key field ...`), called with the
/// sets swapped (see [`ScriptKeys`]), skipping each field that has a newer
/// write. Replies the summed removal count. When no field write entries
/// remain, the key's top-level write entry is dropped.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::{hdel_script, Reply, ScriptKeys};
///
/// let mut store = |args: &[&[u8]]| match args[0] {
///     b"ZSCORE" => Reply::Bulk(None),
///     _ => Reply::Integer(1),
/// };
/// let keys = ScriptKeys { key: b"h", add_set: b"._rem-set", rem_set: b"._add-set" };
/// let fields: [&[u8]; 2] = [b"f1", b"f2"];
/// assert_eq!(hdel_script(&mut store, keys, b"HDEL", b"3", &fields), Ok(Reply::Integer(2)));
/// ```
pub fn hdel_script(
    store: &mut impl RepairStore,
    keys: ScriptKeys<'_>,
    orig_cmd: &[u8],
    cur_ts: &[u8],
    fields: &[&[u8]],
) -> ScriptResult {
    let add_set = keys.composite_add();
    let rem_set = keys.composite_rem();
    let mut ret = 0;
    for &field in fields {
        if !field_is_stale(store, &add_set, &rem_set, cur_ts, field)? {
            call(store, &[b"ZADD", &add_set, cur_ts, field])?;
            ret = add_count(ret, &call(store, &[orig_cmd, keys.key, field])?)?;
        }
    }
    let card = call(store, &[b"ZCARD", &rem_set])?;
    if card == Reply::Integer(0) {
        call(store, &[b"ZREM", keys.rem_set, keys.key])?;
    }
    Ok(Reply::Integer(ret))
}

/// Read one member (`HGET`, `SISMEMBER`, `ZSCORE key field`) with its
/// status and timestamp, as [`get_script`] does for whole values. A delete
/// of the key or the field gives `R`; a field never written, or written
/// under a key with no write entry, gives `X`; a written field whose value
/// is gone gives `[X, 0, value]`.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::{hget_script, Reply, ScriptKeys};
///
/// let mut store = |args: &[&[u8]]| match args[0] {
///     b"ZSCORE" => Reply::Bulk(None),
///     _ => Reply::Integer(0),
/// };
/// let keys = ScriptKeys { key: b"s", add_set: b"._add-set", rem_set: b"._rem-set" };
/// assert_eq!(
///     hget_script(&mut store, keys, b"SISMEMBER", b"m"),
///     Ok(Reply::Array(vec![Reply::Bulk(Some(b"X".to_vec())), Reply::Integer(0),
///         Reply::Integer(0)]))
/// );
/// ```
pub fn hget_script(
    store: &mut impl RepairStore,
    keys: ScriptKeys<'_>,
    orig_cmd: &[u8],
    field: &[u8],
) -> ScriptResult {
    let add_set = keys.composite_add();
    let rem_set = keys.composite_rem();
    let mut status: &[u8] = b"E";
    let mut ts = Reply::Integer(0);
    let key_removed = zscore(store, keys.rem_set, keys.key)?;
    if key_removed.is_truthy() {
        status = b"R";
        ts = key_removed;
    } else {
        let removed = zscore(store, &rem_set, field)?;
        if removed.is_truthy() {
            ts = removed;
            status = b"R";
        }
    }
    if status != b"R" {
        let key_added = zscore(store, keys.add_set, keys.key)?;
        if key_added.is_truthy() {
            let added = zscore(store, &add_set, field)?;
            if added.is_truthy() {
                ts = added;
            } else {
                status = b"X";
            }
        } else {
            status = b"X";
        }
    }
    let value = call(store, &[orig_cmd, keys.key, field])?;
    if status == b"E" && !value.is_truthy() {
        return Ok(Reply::Array(vec![
            Reply::bulk(b"X"),
            Reply::Integer(0),
            value,
        ]));
    }
    Ok(Reply::Array(vec![Reply::bulk(status), ts, value]))
}

/// Add sorted-set members (`ZADD key [opts] score member ...`), with
/// `pairs` as `(member, score)`. Each applied member runs its own
/// `ZADD key opts... score member`; a member's write entry is recorded only
/// when that reply is not nil. With more than three options no command
/// runs. Replies `0` when the key has a newer delete, otherwise the last
/// applied command's reply (nil when none applied).
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::{zadd_script, Reply, ScriptKeys};
///
/// let mut log = Vec::new();
/// let mut store = |args: &[&[u8]]| {
///     log.push(String::from_utf8(args.join(&b' ')).unwrap());
///     match args[0] {
///         b"ZSCORE" => Reply::Bulk(None),
///         _ => Reply::Integer(1),
///     }
/// };
/// let keys = ScriptKeys { key: b"z", add_set: b"._add-set", rem_set: b"._rem-set" };
/// let opts: [&[u8]; 1] = [b"CH"];
/// let pairs: [(&[u8], &[u8]); 1] = [(b"m", b"2.5")];
/// assert_eq!(zadd_script(&mut store, keys, b"ZADD", b"4", &opts, &pairs), Ok(Reply::Integer(1)));
/// assert_eq!(log[log.len() - 2..], ["ZADD z CH 2.5 m", "ZADD ._add-set_z 4 m"]);
/// ```
pub fn zadd_script(
    store: &mut impl RepairStore,
    keys: ScriptKeys<'_>,
    orig_cmd: &[u8],
    cur_ts: &[u8],
    opts: &[&[u8]],
    pairs: &[(&[u8], &[u8])],
) -> ScriptResult {
    let add_set = keys.composite_add();
    let rem_set = keys.composite_rem();
    if !claim_key(store, keys, cur_ts)? {
        return Ok(Reply::Integer(0));
    }
    let mut ret = Reply::Bulk(None);
    for &(field, value) in pairs {
        if !field_is_stale(store, &add_set, &rem_set, cur_ts, field)? {
            ret = if opts.len() <= 3 {
                let mut args: Vec<&[u8]> = vec![orig_cmd, keys.key];
                args.extend_from_slice(opts);
                args.extend_from_slice(&[value, field]);
                call(store, &args)?
            } else {
                Reply::Bulk(None)
            };
            if ret.is_truthy() {
                call(store, &[b"ZADD", &add_set, cur_ts, field])?;
            }
        }
    }
    Ok(ret)
}

/// Add set members (`SADD key member ...`), skipping each member that has
/// a newer write or delete. Replies `0` when the key has a newer delete,
/// otherwise the summed add count.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::{sadd_script, Reply, ScriptKeys};
///
/// let mut store = |args: &[&[u8]]| match args[0] {
///     b"ZSCORE" => Reply::Bulk(None),
///     _ => Reply::Integer(1),
/// };
/// let keys = ScriptKeys { key: b"s", add_set: b"._add-set", rem_set: b"._rem-set" };
/// let members: [&[u8]; 3] = [b"a", b"b", b"a"];
/// assert_eq!(sadd_script(&mut store, keys, b"SADD", b"1", &members), Ok(Reply::Integer(3)));
/// ```
pub fn sadd_script(
    store: &mut impl RepairStore,
    keys: ScriptKeys<'_>,
    orig_cmd: &[u8],
    cur_ts: &[u8],
    members: &[&[u8]],
) -> ScriptResult {
    let add_set = keys.composite_add();
    let rem_set = keys.composite_rem();
    if !claim_key(store, keys, cur_ts)? {
        return Ok(Reply::Integer(0));
    }
    let mut ret = 0;
    for &field in members {
        if !field_is_stale(store, &add_set, &rem_set, cur_ts, field)? {
            call(store, &[b"ZADD", &add_set, cur_ts, field])?;
            ret = add_count(ret, &call(store, &[orig_cmd, keys.key, field])?)?;
        }
    }
    Ok(Reply::Integer(ret))
}

/// After a whole-key delete, drop the key's remove entry unless it is newer
/// than `cur_ts`. Replies the `ZREM` count, or `0`.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::{cleanup_del_script, Reply, ScriptKeys};
///
/// let mut store = |args: &[&[u8]]| match args[0] {
///     b"ZSCORE" => Reply::Bulk(Some(b"9".to_vec())),
///     _ => Reply::Integer(1),
/// };
/// let keys = ScriptKeys { key: b"k", add_set: b"._add-set", rem_set: b"._rem-set" };
/// assert_eq!(cleanup_del_script(&mut store, keys, b"5"), Ok(Reply::Integer(0)));
/// assert_eq!(cleanup_del_script(&mut store, keys, b"9"), Ok(Reply::Integer(1)));
/// ```
pub fn cleanup_del_script(
    store: &mut impl RepairStore,
    keys: ScriptKeys<'_>,
    cur_ts: &[u8],
) -> ScriptResult {
    let removed = zscore(store, keys.rem_set, keys.key)?;
    if removed.is_truthy() {
        if older(cur_ts, &removed)? {
            return Ok(Reply::Integer(0));
        }
        return call(store, &[b"ZREM", keys.rem_set, keys.key]);
    }
    Ok(Reply::Integer(0))
}

/// After a member delete, drop the field's remove entry unless it is newer
/// than `cur_ts`, and the key's top-level remove entry once no field remove
/// entries remain. Replies the `ZREM` count, `0` when the entry is newer,
/// and nil when the field has no remove entry.
///
/// # Examples
///
/// ```
/// use dynomite::proto::redis::repair::logic::{cleanup_hdel_script, Reply, ScriptKeys};
///
/// let mut store = |args: &[&[u8]]| match args[0] {
///     b"ZSCORE" => Reply::Bulk(None),
///     _ => Reply::Integer(1),
/// };
/// let keys = ScriptKeys { key: b"h", add_set: b"._add-set", rem_set: b"._rem-set" };
/// assert_eq!(cleanup_hdel_script(&mut store, keys, b"5", b"f"), Ok(Reply::Bulk(None)));
/// ```
pub fn cleanup_hdel_script(
    store: &mut impl RepairStore,
    keys: ScriptKeys<'_>,
    cur_ts: &[u8],
    field: &[u8],
) -> ScriptResult {
    let rem_set = keys.composite_rem();
    let removed = zscore(store, &rem_set, field)?;
    if removed.is_truthy() {
        if older(cur_ts, &removed)? {
            return Ok(Reply::Integer(0));
        }
        let ret = call(store, &[b"ZREM", &rem_set, field])?;
        let remaining = call(store, &[b"ZCARD", &rem_set])?;
        if remaining == Reply::Integer(0) {
            call(store, &[b"ZREM", keys.rem_set, keys.key])?;
        }
        return Ok(ret);
    }
    Ok(Reply::Bulk(None))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store that answers from a script of replies, in order, recording
    /// the commands; the arms real Redis cannot reach (a falsy `EXISTS`, a
    /// non-numeric score, a non-integer count) are driven this way.
    struct Scripted {
        replies: std::collections::VecDeque<Reply>,
        seen: Vec<String>,
    }

    impl Scripted {
        fn new(replies: Vec<Reply>) -> Self {
            Self {
                replies: replies.into(),
                seen: Vec::new(),
            }
        }
    }

    impl RepairStore for Scripted {
        fn call(&mut self, args: &[&[u8]]) -> Reply {
            self.seen
                .push(String::from_utf8_lossy(&args.join(&b' ')).into_owned());
            self.replies.pop_front().unwrap_or(Reply::Bulk(None))
        }
    }

    const KEYS: ScriptKeys<'static> = ScriptKeys {
        key: b"k",
        add_set: b"._add-set",
        rem_set: b"._rem-set",
    };

    #[test]
    fn non_numeric_score_is_a_script_error() {
        let mut s = Scripted::new(vec![Reply::Bulk(None), Reply::bulk(b"not-a-number")]);
        let r = set_script(&mut s, KEYS, b"SET", b"3", b"v");
        assert_eq!(
            r,
            Err(Reply::Error(
                b"ERR user_script: attempt to compare nil with number".to_vec()
            ))
        );
        let mut s = Scripted::new(vec![Reply::Bulk(None), Reply::Integer(4)]);
        assert!(
            set_script(&mut s, KEYS, b"SET", b"3", b"v").is_err(),
            "an integer score is not a bulk number"
        );
    }

    #[test]
    fn non_integer_count_is_a_script_error() {
        // HDEL with no metadata: the field is not stale, ZADD, then HDEL
        // replies with a status instead of an integer.
        let mut s = Scripted::new(vec![
            Reply::Bulk(None),
            Reply::Bulk(None),
            Reply::Status(b"OK".to_vec()),
            Reply::Status(b"OK".to_vec()),
        ]);
        let r = hdel_script(&mut s, KEYS, b"HDEL", b"3", &[b"f"]);
        assert_eq!(
            r,
            Err(Reply::Error(
                b"ERR user_script: attempt to perform arithmetic on a non-number value".to_vec()
            ))
        );
    }

    #[test]
    fn get_of_a_recorded_key_whose_value_is_gone_drops_the_add_entry() {
        // GET -> nil; ZSCORE add -> "2"; ZREM add; ZSCORE rem -> nil.
        let mut s = Scripted::new(vec![
            Reply::Bulk(None),
            Reply::bulk(b"2"),
            Reply::Integer(1),
            Reply::Bulk(None),
        ]);
        let r = get_script(&mut s, KEYS, b"GET").unwrap();
        assert_eq!(
            r,
            Reply::Array(vec![
                Reply::bulk(b"X"),
                Reply::Integer(0),
                Reply::Bulk(None)
            ])
        );
        assert!(
            s.seen.iter().any(|c| c == "ZREM ._add-set k"),
            "{:?}",
            s.seen
        );
    }

    #[test]
    fn delete_of_a_key_that_does_not_exist_replies_zero() {
        // No metadata, then EXISTS -> nil (falsy): nothing is written.
        let mut s = Scripted::new(vec![
            Reply::Bulk(None),
            Reply::Bulk(None),
            Reply::Bulk(None),
        ]);
        assert_eq!(
            del_script(&mut s, KEYS, b"DEL", b"3").unwrap(),
            Reply::Integer(0)
        );
        assert!(
            !s.seen.iter().any(|c| c.starts_with("ZADD")),
            "{:?}",
            s.seen
        );
    }

    #[test]
    fn stale_hash_write_against_a_top_level_delete_replies_zero() {
        // ZSCORE rem k -> "5"; the write at ts 3 is older.
        let mut s = Scripted::new(vec![Reply::bulk(b"5")]);
        assert_eq!(
            hset_script(&mut s, KEYS, b"HSET", b"3", &[(b"f", b"v")]).unwrap(),
            Reply::Integer(0)
        );
    }

    #[test]
    fn hget_of_a_recorded_field_with_no_value_reports_absent() {
        // rem k nil, rem field nil, add k "2", add field "2", HGET nil.
        let mut s = Scripted::new(vec![
            Reply::Bulk(None),
            Reply::Bulk(None),
            Reply::bulk(b"2"),
            Reply::bulk(b"2"),
            Reply::Bulk(None),
        ]);
        let r = hget_script(&mut s, KEYS, b"HGET", b"f").unwrap();
        assert_eq!(
            r,
            Reply::Array(vec![
                Reply::bulk(b"X"),
                Reply::Integer(0),
                Reply::Bulk(None)
            ])
        );
    }

    #[test]
    fn zadd_with_more_than_three_options_writes_nothing() {
        // claim_key: rem nil, add nil, ZADD add; field: add nil, rem nil.
        let mut s = Scripted::new(vec![
            Reply::Bulk(None),
            Reply::Bulk(None),
            Reply::Integer(1),
            Reply::Bulk(None),
            Reply::Bulk(None),
        ]);
        let opts: [&[u8]; 4] = [b"NX", b"CH", b"INCR", b"GT"];
        let r = zadd_script(&mut s, KEYS, b"ZADD", b"3", &opts, &[(b"m", b"1")]).unwrap();
        assert_eq!(r, Reply::Bulk(None));
        assert!(
            !s.seen.iter().any(|c| c.starts_with("ZADD k")),
            "{:?}",
            s.seen
        );
    }

    #[test]
    fn hash_cleanup_keeps_the_top_level_entry_while_fields_remain() {
        // ZSCORE rem field -> "2"; ZREM -> 1; ZCARD -> 1 (another field).
        let mut s = Scripted::new(vec![
            Reply::bulk(b"2"),
            Reply::Integer(1),
            Reply::Integer(1),
        ]);
        assert_eq!(
            cleanup_hdel_script(&mut s, KEYS, b"3", b"f").unwrap(),
            Reply::Integer(1)
        );
        assert!(
            !s.seen.iter().any(|c| c == "ZREM ._rem-set k"),
            "{:?}",
            s.seen
        );
    }
}
