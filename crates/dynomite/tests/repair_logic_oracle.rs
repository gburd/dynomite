//! Oracle for the read-repair script logic.
//!
//! Every script in `src/proto/dyn_proto_repair.h` of the C reference tree
//! (`$DYNOMITE_C_REF`, default `~/src/dynomite`) is run as `EVAL` on one
//! throwaway `valkey-server`, and the matching function of
//! `dynomite::proto::redis::repair::logic` runs against a second one with
//! the same arguments the C engine builds (`create_redis_prtcl_script`).
//! A property draws short operation sequences over a tiny key / field /
//! value / timestamp domain so ties, reorderings, duplicate fields, type
//! clashes and command errors all occur, and asserts identical replies per
//! operation and identical final datasets, metadata sets included.
//!
//! The test skips (and says why) when the C tree or `valkey-server` is
//! missing.

use std::{
    fmt::Write as _,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use dynomite::proto::redis::repair::logic::{self, RepairStore, Reply, ScriptKeys};
use hegel::{generators as gs, TestCase};

// ---------------------------------------------------------------- server

struct Server {
    child: Child,
    port: u16,
}

impl Server {
    fn start() -> Option<Server> {
        let port = TcpListener::bind("127.0.0.1:0")
            .ok()?
            .local_addr()
            .ok()?
            .port();
        let child = Command::new("valkey-server")
            .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
            .args(["--save", "", "--appendonly", "no"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        let server = Server { child, port };
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Some(server);
            }
            thread::sleep(Duration::from_millis(20));
        }
        None
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------- client

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    fn connect(port: u16) -> Client {
        let writer = TcpStream::connect(("127.0.0.1", port)).expect("connect to valkey");
        let reader = BufReader::new(writer.try_clone().expect("clone socket"));
        Client { reader, writer }
    }

    fn cmd<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Reply {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            let arg = arg.as_ref();
            out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            out.extend_from_slice(arg);
            out.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&out).expect("write command");
        self.read()
    }

    fn line(&mut self) -> Vec<u8> {
        let mut line = Vec::new();
        self.reader
            .read_until(b'\n', &mut line)
            .expect("read reply");
        assert!(line.ends_with(b"\r\n"), "truncated reply line {line:?}");
        line.truncate(line.len() - 2);
        line
    }

    fn read(&mut self) -> Reply {
        let line = self.line();
        let rest = line[1..].to_vec();
        let number = || -> i64 {
            String::from_utf8_lossy(&rest)
                .parse()
                .expect("numeric reply")
        };
        match line[0] {
            b'+' => Reply::Status(rest),
            b'-' => Reply::Error(rest),
            b':' => Reply::Integer(number()),
            b'$' => match usize::try_from(number()) {
                Ok(len) => {
                    let mut buf = vec![0; len + 2];
                    self.reader.read_exact(&mut buf).expect("read bulk");
                    buf.truncate(len);
                    Reply::Bulk(Some(buf))
                }
                Err(_) => Reply::Bulk(None),
            },
            b'*' => match usize::try_from(number()) {
                Ok(len) => Reply::Array((0..len).map(|_| self.read()).collect()),
                Err(_) => Reply::Bulk(None),
            },
            other => panic!("unexpected reply type {other}"),
        }
    }
}

impl RepairStore for Client {
    fn call(&mut self, args: &[&[u8]]) -> Reply {
        self.cmd(args)
    }
}

// ---------------------------------------------------------------- scripts

/// Script bodies from the C header, unescaped, without the `EVAL` shell.
struct Scripts {
    bodies: Vec<(String, Vec<u8>)>,
}

impl Scripts {
    fn load() -> Option<Scripts> {
        let root = std::env::var_os("DYNOMITE_C_REF").map_or_else(
            || PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join("src/dynomite"),
            PathBuf::from,
        );
        let header = std::fs::read_to_string(root.join("src/proto/dyn_proto_repair.h")).ok()?;
        let mut bodies = Vec::new();
        let mut lines = header.lines();
        while let Some(line) = lines.next() {
            let Some(rest) = line.strip_prefix("#define ") else {
                continue;
            };
            let Some((name, first)) = rest.split_once(' ') else {
                continue;
            };
            if !name.ends_with("_SCRIPT") {
                continue;
            }
            let mut raw = Vec::new();
            let mut cur = first;
            loop {
                let more = cur.trim_end().ends_with('\\');
                let lit = cur.trim().trim_end_matches('\\').trim();
                raw.extend(unescape(lit.trim_matches('"')));
                if !more {
                    break;
                }
                cur = lines.next().expect("continued #define");
            }
            bodies.push((name.to_string(), eval_body(&raw)));
        }
        Some(Scripts { bodies })
    }

    fn body(&self, name: &str) -> &[u8] {
        &self
            .bodies
            .iter()
            .find(|(n, _)| n == name)
            .unwrap_or_else(|| panic!("{name} not in header"))
            .1
    }
}

fn unescape(lit: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut chars = lit.bytes();
    while let Some(c) = chars.next() {
        if c != b'\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some(b'n') => out.push(b'\n'),
            Some(b'r') => out.push(b'\r'),
            Some(other) => out.push(other),
            None => panic!("dangling escape in {lit}"),
        }
    }
    out
}

/// Strip `$4\r\nEVAL\r\n$<n>\r\n` and the trailing CRLF, checking `<n>`.
fn eval_body(raw: &[u8]) -> Vec<u8> {
    let rest = raw
        .strip_prefix(b"$4\r\nEVAL\r\n$")
        .expect("EVAL shell prefix");
    let end = rest.iter().position(|&b| b == b'\r').expect("length line");
    let len: usize = std::str::from_utf8(&rest[..end])
        .expect("ascii length")
        .parse()
        .expect("declared length");
    let body = &rest[end + 2..];
    assert_eq!(body.len(), len + 2, "declared length matches the body");
    assert!(body.ends_with(b"\r\n"));
    body[..len].to_vec()
}

// ---------------------------------------------------------------- ops

const ADD: &[u8] = b"._add-set";
const REM: &[u8] = b"._rem-set";
const FAMILY_KEYS: [[&str; 3]; 4] = [
    ["s1", "s2", "x"],
    ["h1", "h2", "x"],
    ["t1", "t2", "x"],
    ["z1", "z2", "x"],
];
const FIELDS: [&str; 3] = ["f1", "f2", "f3"];
const VALUES: [&str; 3] = ["a", "b", "7"];
const SCORES: [&str; 3] = ["1", "2.5", "x"];
const ZOPTS: [&str; 4] = ["NX", "XX", "CH", "INCR"];

/// One command form and the family whose keys it draws from (`None`: any).
const FORMS: [(&str, Option<usize>); 15] = [
    ("SET", Some(0)),
    ("GET", Some(0)),
    ("DEL", None),
    ("HSET", Some(1)),
    ("HMSET", Some(1)),
    ("HDEL", Some(1)),
    ("SREM", Some(2)),
    ("ZREM", Some(3)),
    ("HGET", Some(1)),
    ("SISMEMBER", Some(2)),
    ("ZSCORE", Some(3)),
    ("ZADD", Some(3)),
    ("SADD", Some(2)),
    ("CLEANUP_DEL", None),
    ("CLEANUP_HDEL", None),
];

#[derive(Debug)]
struct Op {
    form: &'static str,
    key: &'static str,
    ts: String,
    opts: Vec<&'static str>,
    /// `(field, value)`; the value is empty for forms that carry none.
    pairs: Vec<(&'static str, &'static str)>,
}

fn pick<T: Copy>(tc: &TestCase, from: &[T]) -> T {
    from[tc.draw(gs::integers::<usize>().max_value(from.len() - 1))]
}

fn draw_op(tc: &TestCase) -> Op {
    let (form, family) = pick(tc, &FORMS);
    let family = family.unwrap_or_else(|| tc.draw(gs::integers::<usize>().max_value(3)));
    let key = pick(tc, &FAMILY_KEYS[family]);
    let ts = tc.draw(gs::integers::<u8>().max_value(6)).to_string();
    let count = match form {
        "HMSET" | "HDEL" | "SREM" | "ZREM" | "ZADD" | "SADD" => {
            tc.draw(gs::integers::<usize>().min_value(1).max_value(3))
        }
        "SET" | "HSET" | "HGET" | "SISMEMBER" | "ZSCORE" | "CLEANUP_HDEL" => 1,
        _ => 0,
    };
    let values: &[&str] = if form == "ZADD" { &SCORES } else { &VALUES };
    let pairs = (0..count)
        .map(|_| (pick(tc, &FIELDS), pick(tc, values)))
        .collect();
    let opts = if form == "ZADD" {
        let n = tc.draw(gs::integers::<usize>().max_value(3));
        (0..n).map(|_| pick(tc, &ZOPTS)).collect()
    } else {
        Vec::new()
    };
    Op {
        form,
        key,
        ts,
        opts,
        pairs,
    }
}

impl Op {
    fn script(&self) -> &'static str {
        match self.form {
            "SET" => "SET_SCRIPT",
            "GET" => "GET_SCRIPT",
            "DEL" => "DEL_SCRIPT",
            "HSET" | "HMSET" => "HSET_SCRIPT",
            "HDEL" | "SREM" | "ZREM" => "HDEL_SCRIPT",
            "HGET" | "SISMEMBER" | "ZSCORE" => "HGET_SCRIPT",
            "ZADD" => "ZADD_SCRIPT",
            "SADD" => "SADD_SCRIPT",
            "CLEANUP_DEL" => "CLEANUP_DEL_SCRIPT",
            _ => "CLEANUP_HDEL_SCRIPT",
        }
    }

    fn is_delete(&self) -> bool {
        matches!(self.form, "DEL" | "HDEL" | "SREM" | "ZREM")
    }

    fn keys(&self) -> ScriptKeys<'_> {
        let (add_set, rem_set) = if self.is_delete() {
            (REM, ADD)
        } else {
            (ADD, REM)
        };
        ScriptKeys {
            key: self.key.as_bytes(),
            add_set,
            rem_set,
        }
    }

    fn cmd(&self) -> &'static str {
        if self.form.starts_with("CLEANUP") {
            "UNKNOWN"
        } else {
            self.form
        }
    }

    fn has_values(&self) -> bool {
        matches!(self.form, "SET" | "HSET" | "HMSET" | "ZADD")
    }

    /// `EVAL body 3 key add rem cmd [nopts] nfields ts opts.. (field value)..`
    fn eval_args(&self, scripts: &Scripts) -> Vec<Vec<u8>> {
        let keys = self.keys();
        let mut args: Vec<Vec<u8>> = vec![
            b"EVAL".to_vec(),
            scripts.body(self.script()).to_vec(),
            b"3".to_vec(),
            keys.key.to_vec(),
            keys.add_set.to_vec(),
            keys.rem_set.to_vec(),
            self.cmd().into(),
        ];
        if self.form == "ZADD" {
            args.push(self.opts.len().to_string().into());
        }
        let num_fields = if self.form == "SET" {
            0
        } else {
            self.pairs.len()
        };
        args.push(num_fields.to_string().into());
        args.push(self.ts.clone().into());
        args.extend(self.opts.iter().map(|o| o.as_bytes().to_vec()));
        for (field, value) in &self.pairs {
            if self.form != "SET" {
                args.push(field.as_bytes().to_vec());
            }
            if self.has_values() {
                args.push(value.as_bytes().to_vec());
            }
        }
        args
    }

    fn run_rust(&self, store: &mut Client) -> Reply {
        let keys = self.keys();
        let cmd = self.cmd().as_bytes();
        let ts = self.ts.as_bytes();
        let fields: Vec<&[u8]> = self.pairs.iter().map(|(f, _)| f.as_bytes()).collect();
        let pairs: Vec<(&[u8], &[u8])> = self
            .pairs
            .iter()
            .map(|(f, v)| (f.as_bytes(), v.as_bytes()))
            .collect();
        let opts: Vec<&[u8]> = self.opts.iter().map(|o| o.as_bytes()).collect();
        let result = match self.script() {
            "SET_SCRIPT" => logic::set_script(store, keys, cmd, ts, pairs[0].1),
            "GET_SCRIPT" => logic::get_script(store, keys, cmd),
            "DEL_SCRIPT" => logic::del_script(store, keys, cmd, ts),
            "HSET_SCRIPT" => logic::hset_script(store, keys, cmd, ts, &pairs),
            "HDEL_SCRIPT" => logic::hdel_script(store, keys, cmd, ts, &fields),
            "HGET_SCRIPT" => logic::hget_script(store, keys, cmd, fields[0]),
            "ZADD_SCRIPT" => logic::zadd_script(store, keys, cmd, ts, &opts, &pairs),
            "SADD_SCRIPT" => logic::sadd_script(store, keys, cmd, ts, &fields),
            "CLEANUP_DEL_SCRIPT" => logic::cleanup_del_script(store, keys, ts),
            _ => logic::cleanup_hdel_script(store, keys, ts, fields[0]),
        };
        result.unwrap_or_else(|err| err)
    }
}

/// An error raised inside a script carries a ` script: ...` location
/// suffix; the rest of the message is the command's own error.
fn normalize(reply: Reply) -> Reply {
    match reply {
        Reply::Error(msg) => {
            let text = String::from_utf8_lossy(&msg).into_owned();
            let head = text.split(" script:").next().unwrap_or_default();
            Reply::Error(head.as_bytes().to_vec())
        }
        other => other,
    }
}

// ---------------------------------------------------------------- dataset

fn bulk_text(reply: &Reply) -> String {
    match reply {
        Reply::Bulk(Some(b)) | Reply::Status(b) => String::from_utf8_lossy(b).into_owned(),
        other => format!("{other:?}"),
    }
}

fn items(reply: Reply) -> Vec<String> {
    match reply {
        Reply::Array(items) => items.iter().map(bulk_text).collect(),
        other => panic!("expected array, got {other:?}"),
    }
}

/// Every key with its type and full contents, in a canonical order.
fn dump(client: &mut Client) -> String {
    let mut keys = items(client.cmd(&["KEYS", "*"]));
    keys.sort();
    let mut out = String::new();
    for key in keys {
        let kind = bulk_text(&client.cmd(&["TYPE", &key]));
        let contents = match kind.as_str() {
            "string" => vec![bulk_text(&client.cmd(&["GET", &key]))],
            "hash" => sorted_pairs(&items(client.cmd(&["HGETALL", &key]))),
            "set" => {
                let mut members = items(client.cmd(&["SMEMBERS", &key]));
                members.sort();
                members
            }
            "zset" => items(client.cmd(&["ZRANGE", &key, "0", "-1", "WITHSCORES"])),
            other => panic!("unexpected type {other} for {key}"),
        };
        let _ = writeln!(out, "{key} {kind} {contents:?}");
    }
    out
}

fn sorted_pairs(flat: &[String]) -> Vec<String> {
    let mut pairs: Vec<String> = flat.chunks(2).map(|p| p.join("=")).collect();
    pairs.sort();
    pairs
}

// ---------------------------------------------------------------- property

/// Run `ops` on both servers (flushed first); assert equal replies and
/// datasets. Returns the replies.
fn run_ops(
    ops: impl IntoIterator<Item = Op>,
    scripts: &Scripts,
    lua: &mut Client,
    rust: &mut Client,
    mut seen: impl FnMut(&Op, &Reply),
) {
    assert_eq!(lua.cmd(&["FLUSHALL"]), Reply::Status(b"OK".to_vec()));
    assert_eq!(rust.cmd(&["FLUSHALL"]), Reply::Status(b"OK".to_vec()));
    let mut history = Vec::new();
    for op in ops {
        let expected = normalize(lua.cmd(&op.eval_args(scripts)));
        let actual = normalize(op.run_rust(rust));
        history.push(format!("{op:?} -> {expected:?}"));
        assert_eq!(actual, expected, "reply differs; history:\n{history:#?}");
        seen(&op, &expected);
    }
    assert_eq!(
        dump(rust),
        dump(lua),
        "dataset differs; history:\n{history:#?}"
    );
}

fn op(
    form: &'static str,
    key: &'static str,
    ts: &str,
    opts: &[&'static str],
    pairs: &[(&'static str, &'static str)],
) -> Op {
    Op {
        form,
        key,
        ts: ts.to_string(),
        opts: opts.to_vec(),
        pairs: pairs.to_vec(),
    }
}

/// Cases a random draw reaches rarely, each with the reply the reference
/// gives, so a regression names the behaviour it broke.
fn directed(scripts: &Scripts, lua: &mut Client, rust: &mut Client) {
    let bulk = |s: &str| Reply::Bulk(Some(s.as_bytes().to_vec()));
    let cases: Vec<(Vec<Op>, Reply)> = vec![
        // A missing key reads as [X, :0, nil].
        (
            vec![op("GET", "s1", "1", &[], &[])],
            Reply::Array(vec![bulk("X"), Reply::Integer(0), Reply::Bulk(None)]),
        ),
        // The EXISTS reply :0 is truthy: a delete of a missing key is recorded.
        (vec![op("DEL", "s1", "3", &[], &[])], Reply::Integer(0)),
        // XX INCR on a missing member replies nil; no field write entry.
        (
            vec![op("ZADD", "z1", "2", &["XX", "INCR"], &[("f1", "1")])],
            Reply::Bulk(None),
        ),
        // NX INCR on an existing member replies nil.
        (
            vec![
                op("ZADD", "z1", "2", &[], &[("f1", "1")]),
                op("ZADD", "z1", "3", &["NX", "INCR"], &[("f1", "1")]),
            ],
            Reply::Bulk(None),
        ),
        // More than one pair: the bulk string OK, not a status.
        (
            vec![op("HMSET", "h1", "1", &[], &[("f1", "a"), ("f1", "b")])],
            bulk("OK"),
        ),
        // A recorded timestamp comes back as the ZSCORE bulk string.
        (
            vec![
                op("SET", "s1", "4", &[], &[("f1", "a")]),
                op("GET", "s1", "0", &[], &[]),
            ],
            Reply::Array(vec![bulk("E"), bulk("4"), bulk("a")]),
        ),
        // A stale write replies -1.
        (
            vec![
                op("SET", "s1", "4", &[], &[("f1", "a")]),
                op("SET", "s1", "3", &[], &[("f1", "b")]),
            ],
            Reply::Integer(-1),
        ),
        // A cleanup with no remove entry replies nil.
        (
            vec![op("CLEANUP_HDEL", "h1", "1", &[], &[("f1", "")])],
            Reply::Bulk(None),
        ),
        // A type clash aborts the script after the metadata write.
        (
            vec![
                op("SADD", "x", "1", &[], &[("f1", "")]),
                op("HSET", "x", "2", &[], &[("f1", "a")]),
            ],
            Reply::Error(
                b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec(),
            ),
        ),
    ];
    for (ops, want) in cases {
        let mut last = None;
        run_ops(ops, scripts, lua, rust, |_, r| last = Some(r.clone()));
        assert_eq!(last, Some(want));
    }
}

#[test]
fn every_repair_script_matches_the_lua_reference() {
    let Some(scripts) = Scripts::load() else {
        eprintln!("skip: C reference header dyn_proto_repair.h not found (set DYNOMITE_C_REF)");
        return;
    };
    assert_eq!(scripts.bodies.len(), 10, "ten scripts in the header");
    let (Some(lua_server), Some(rust_server)) = (Server::start(), Server::start()) else {
        eprintln!("skip: valkey-server not available on PATH");
        return;
    };
    let mut lua = Client::connect(lua_server.port);
    let mut rust = Client::connect(rust_server.port);
    directed(&scripts, &mut lua, &mut rust);

    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    hegel::Hegel::new(|tc: TestCase| {
        let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(12));
        let ops: Vec<Op> = (0..n).map(|_| draw_op(&tc)).collect();
        run_ops(ops, &scripts, &mut lua, &mut rust, |op, reply| {
            let class = match reply {
                Reply::Error(_) => "error",
                Reply::Bulk(None) => "nil",
                _ => "value",
            };
            *counts.entry(format!("{} {class}", op.form)).or_default() += 1;
        });
    })
    .settings(hegel::Settings::new().test_cases(200))
    .run();
    eprintln!("ops checked per form and reply class: {counts:#?}");
    for (form, _) in FORMS {
        assert!(
            counts.keys().any(|k| k.starts_with(&format!("{form} "))),
            "{form} never exercised"
        );
    }
}
