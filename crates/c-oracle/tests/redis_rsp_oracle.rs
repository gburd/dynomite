//! Oracle: C `redis_parse_rsp` (`src/proto/dyn_redis.c`) against the Rust
//! transliteration on single-buffer input.
//!
//! Inputs are well-formed responses of every RESP type (status, error,
//! integer, bulk, null bulk, multibulk with nested multibulk elements),
//! optionally followed by a second response, then optionally truncated and
//! byte-mutated. Both parsers must agree on the parse result, the response
//! type, the integer payload, the error flag, the recorded element
//! arguments, and (on success) the number of bytes consumed.

#![cfg(not(c_oracle_absent))]

use c_oracle::redis::{self, ParseResult};
use dynomite::msg::{Msg, MsgParseResult, MsgType};
use dynomite::proto::redis::redis_parse_rsp;
use hegel::generators as gs;
use hegel::TestCase;

const ERROR_WORDS: &[&[u8]] = &[
    b"-ERR",
    b"-OOM",
    b"-BUSY",
    b"-NOAUTH",
    b"-LOADING",
    b"-BUSYKEY",
    b"-MISCONF",
    b"-NOSCRIPT",
    b"-READONLY",
    b"-WRONGTYPE",
    b"-EXECABORT",
    b"-MASTERDOWN",
    b"-NOREPLICAS",
    b"-WRONG",
    b"-",
];

/// Bytes mutations draw from: framing bytes plus a few payload bytes.
const MUTATION_BYTES: &[u8] = b"\r\n-+:$*0123456789 aZ\x00\xff";

fn pick<'a, T>(tc: &TestCase, items: &'a [T]) -> &'a T {
    let i = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(items.len() - 1),
    );
    &items[i]
}

/// Line text with no CR or LF.
fn line_text(tc: &TestCase, max: usize) -> Vec<u8> {
    let alphabet: &[u8] = b"abcOK -+:$*09";
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(max));
    (0..n).map(|_| *pick(tc, alphabet)).collect()
}

/// Decimal length or count: usually the true value, sometimes a value that
/// overflows 32 bits.
fn decimal(tc: &TestCase, value: usize) -> Vec<u8> {
    if tc.draw(gs::booleans()) && tc.draw(gs::booleans()) && tc.draw(gs::booleans()) {
        let big = tc.draw(gs::integers::<u64>());
        return big.to_string().into_bytes();
    }
    value.to_string().into_bytes()
}

fn simple(tc: &TestCase) -> Vec<u8> {
    let mut out = Vec::new();
    match tc.draw(gs::integers::<u8>().min_value(0).max_value(2)) {
        0 => {
            out.push(b'+');
            out.extend(line_text(tc, 8));
        }
        1 => {
            out.extend_from_slice(*pick(tc, ERROR_WORDS));
            if tc.draw(gs::booleans()) {
                out.push(b' ');
                out.extend(line_text(tc, 8));
            }
        }
        _ => {
            out.push(b':');
            if tc.draw(gs::booleans()) {
                out.push(b'-');
            }
            let v = tc.draw(gs::integers::<u64>());
            out.extend(v.to_string().into_bytes());
        }
    }
    out.extend_from_slice(b"\r\n");
    out
}

fn bulk(tc: &TestCase) -> Vec<u8> {
    if tc.draw(gs::integers::<u8>().min_value(0).max_value(5)) == 0 {
        return b"$-1\r\n".to_vec();
    }
    let data = tc.draw(gs::vecs(gs::integers::<u8>()).max_size(12));
    let mut out = vec![b'$'];
    out.extend(decimal(tc, data.len()));
    out.extend_from_slice(b"\r\n");
    out.extend(data);
    out.extend_from_slice(b"\r\n");
    out
}

fn value(tc: &TestCase, depth: u32) -> Vec<u8> {
    let kinds = if depth == 0 { 2 } else { 3 };
    match tc.draw(gs::integers::<u8>().min_value(0).max_value(kinds)) {
        0 => simple(tc),
        1 | 2 => bulk(tc),
        _ => {
            if tc.draw(gs::integers::<u8>().min_value(0).max_value(7)) == 0 {
                return b"*-1\r\n".to_vec();
            }
            let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(4));
            let mut out = vec![b'*'];
            out.extend(decimal(tc, n));
            out.extend_from_slice(b"\r\n");
            for _ in 0..n {
                out.extend(value(tc, depth - 1));
            }
            out
        }
    }
}

fn response(tc: &TestCase) -> Vec<u8> {
    let mut input = value(tc, 3);
    if tc.draw(gs::booleans()) {
        input.extend(value(tc, 2));
    }
    if !input.is_empty() && tc.draw(gs::booleans()) {
        let cut = tc.draw(gs::integers::<usize>().min_value(1).max_value(input.len()));
        input.truncate(cut);
    }
    let mutations = tc.draw(gs::integers::<usize>().min_value(0).max_value(2));
    for _ in 0..mutations {
        if input.is_empty() {
            break;
        }
        let at = tc.draw(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(input.len() - 1),
        );
        input[at] = *pick(tc, MUTATION_BYTES);
    }
    input
}

fn result_from_rust(r: MsgParseResult) -> ParseResult {
    match r {
        MsgParseResult::Ok => ParseResult::Ok,
        MsgParseResult::Error => ParseResult::Error,
        MsgParseResult::Repair => ParseResult::Repair,
        MsgParseResult::Fragment => ParseResult::Fragment,
        MsgParseResult::Again => ParseResult::Again,
        MsgParseResult::Noop => ParseResult::Noop,
        MsgParseResult::DynoConfig => ParseResult::DynoConfig,
        MsgParseResult::OomError => ParseResult::Oom,
    }
}

/// Offset of the CR that ends a top-level error word with no message
/// (`-WORD\r\n`), the input class of finding DEV-P1.
fn bare_error_word_cr(input: &[u8]) -> Option<usize> {
    if input.first() != Some(&b'-') {
        return None;
    }
    let end = input
        .iter()
        .skip(1)
        .position(|&b| b == b' ' || b == b'\r')?
        + 1;
    (input[end] == b'\r').then_some(end)
}

fn assert_agree(input: &[u8]) {
    assert_agree_split(input, input.len());
}

/// Both parsers receive `input[..split]`, and on `Again` resume with the
/// whole of `input` from their saved state.
fn assert_agree_split(input: &[u8], split: usize) {
    // DEV-P1 (PROPOSED): C consumes the CR that ends a bare error word and
    // then runs to the next CR LF; Rust completes the error at that CR. The
    // Rust parse of `-WORD\r\n` equals the C parse of `-WORD \r\n` less the
    // inserted byte.
    let (c_input, shift) = match bare_error_word_cr(input) {
        Some(at) => {
            let mut v = input.to_vec();
            v.insert(at, b' ');
            (v, 1)
        }
        None => (input.to_vec(), 0),
    };
    let c_split = if shift == 1 && split > bare_error_word_cr(input).unwrap_or(usize::MAX) {
        split + 1
    } else {
        split
    };
    let mut c = redis::parse_rsp_split(&c_input, c_split);
    if c.result == ParseResult::Ok {
        c.pos -= shift;
    }
    let mut m = Msg::new(0, MsgType::Unknown, false);
    let mut rust = redis_parse_rsp(&mut m, &input[..split]);
    if rust == MsgParseResult::Again && split < input.len() {
        rust = redis_parse_rsp(&mut m, input);
    }
    let res = result_from_rust(rust);
    let ctx = format!("input {:?} split {split}", String::from_utf8_lossy(input));
    assert_eq!(res, c.result, "result; {ctx}");
    assert_eq!(m.ty().name(), c.type_name, "type; {ctx}");
    assert_eq!(m.integer(), i64::from(c.integer), "integer; {ctx}");
    assert_eq!(m.flags().is_error, c.is_error, "is_error; {ctx}");
    assert_eq!(m.ntokens(), c.ntokens, "ntokens; {ctx}");
    let c_args: Vec<&[u8]> = c.args.iter().map(|&(s, e)| &input[s..e]).collect();
    let r_args: Vec<&[u8]> = m.args().iter().map(dynomite::msg::ArgPos::bytes).collect();
    assert_eq!(r_args, c_args, "args; {ctx}");
    if c.result == ParseResult::Ok {
        assert_eq!(m.parser_pos(), c.pos, "consumed; {ctx}");
    }
}

#[hegel::test(test_cases = 3000)]
fn redis_parse_rsp_matches_c(tc: TestCase) {
    let input = response(&tc);
    assert_agree(&input);
}

/// The resumable-state path: a response split at every byte boundary.
#[hegel::test(test_cases = 400)]
fn redis_parse_rsp_resumed_matches_c(tc: TestCase) {
    let input = response(&tc);
    for split in 0..=input.len() {
        assert_agree_split(&input, split);
    }
}

#[test]
fn redis_parse_rsp_matches_c_on_fixed_cases() {
    for input in [
        &b"+OK\r\n"[..],
        b"-ERR no such key\r\n",
        b"-WRONGTYPE Operation against a key\r\n",
        b":42\r\n",
        b"$3\r\nfoo\r\n",
        b"$-1\r\n",
        b"*0\r\n",
        b"*-1\r\n",
        b"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        b"*2\r\n:1\r\n*1\r\n$1\r\nx\r\n",
        b"+OK",
        b"",
    ] {
        assert_agree(input);
    }
}

/// DEV-P1: C consumes the CR that ends a bare error word, so `-ERR\r\n`
/// does not complete and swallows the next reply up to its CR LF; Rust
/// completes the error at that CR.
#[test]
fn c_bare_error_word_runs_to_next_crlf() {
    let c = redis::parse_rsp(b"-ERR\r\n");
    assert_eq!(c.result, ParseResult::Again);
    let c = redis::parse_rsp(b"-ERR\r\n+OK\r\n");
    assert_eq!((c.result, c.pos), (ParseResult::Ok, 11));
    let mut m = Msg::new(0, MsgType::Unknown, false);
    assert_eq!(
        redis_parse_rsp(&mut m, b"-ERR\r\n+OK\r\n"),
        MsgParseResult::Ok
    );
    assert_eq!((m.ty(), m.parser_pos()), (MsgType::RspRedisErrorErr, 6));
}

/// DEV-P2: a nested multibulk replaces the outer element count, so only a
/// nested multibulk in last position parses as one reply. Rust and C
/// agree; the oracle property covers the general case.
#[test]
fn nested_multibulk_ends_at_inner_count() {
    let input = b"*2\r\n*1\r\n$1\r\na\r\n$1\r\nb\r\n";
    let c = redis::parse_rsp(input);
    assert_eq!((c.result, c.pos), (ParseResult::Ok, 15));
    assert_agree(input);
}
