//! FT.SUG* command-surface integration tests.
//!
//! Build argument vectors directly and call into the
//! suggestion-registry dispatcher; this skips the wire codec
//! and focuses on parser + execution semantics. Wire-level
//! coverage lives in `crates/dynomited/tests/ft_suggest_wire.rs`.

use dynomite_search::ft;
use dynomite_search::sugest_registry::SuggestionRegistry;

fn slices(v: &[Vec<u8>]) -> Vec<&[u8]> {
    v.iter().map(Vec::as_slice).collect()
}

fn add_args(key: &[u8], suggestion: &[u8], score: &str, extra: &[&[u8]]) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = vec![
        b"FT.SUGADD".to_vec(),
        key.to_vec(),
        suggestion.to_vec(),
        score.as_bytes().to_vec(),
    ];
    for b in extra {
        out.push(b.to_vec());
    }
    out
}

/// Decode the integer payload of a RESP `:<n>\r\n` reply.
fn parse_integer(bytes: &[u8]) -> i64 {
    let body = std::str::from_utf8(bytes).expect("utf8");
    assert!(
        body.starts_with(':'),
        "expected integer reply, got {body:?}"
    );
    body.trim_start_matches(':')
        .trim_end_matches("\r\n")
        .parse()
        .expect("parse i64")
}

/// Decode the elements of a flat RESP array reply into a
/// `Vec<Option<Vec<u8>>>`. `None` elements correspond to
/// `$-1\r\n` nil bulk strings; `Some(b)` elements correspond
/// to bulk strings.
fn parse_array_flat(bytes: &[u8]) -> Vec<Option<Vec<u8>>> {
    let mut cursor = 0;
    assert!(bytes[cursor] == b'*', "expected array, got {bytes:?}");
    cursor += 1;
    let nl = find_crlf(bytes, cursor);
    let count: usize = std::str::from_utf8(&bytes[cursor..nl])
        .unwrap()
        .parse()
        .unwrap();
    cursor = nl + 2;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if bytes[cursor] == b'$' {
            cursor += 1;
            let nl = find_crlf(bytes, cursor);
            let len_str = std::str::from_utf8(&bytes[cursor..nl]).unwrap();
            cursor = nl + 2;
            let len: i64 = len_str.parse().unwrap();
            if len < 0 {
                out.push(None);
            } else {
                let len = usize::try_from(len).unwrap();
                out.push(Some(bytes[cursor..cursor + len].to_vec()));
                cursor += len + 2;
            }
        } else {
            panic!(
                "unexpected RESP byte {:?} at offset {}",
                bytes[cursor], cursor
            );
        }
    }
    out
}

fn find_crlf(bytes: &[u8], start: usize) -> usize {
    bytes[start..]
        .iter()
        .position(|&b| b == b'\r')
        .map(|p| p + start)
        .expect("crlf")
}

#[test]
fn sugadd_increments_size() {
    let reg = SuggestionRegistry::new();
    let args = add_args(b"sk", b"alpha", "1.0", &[]);
    let bytes = ft::dispatch_sugest(&reg, &slices(&args));
    assert_eq!(parse_integer(&bytes), 1);
    let args = add_args(b"sk", b"beta", "2.0", &[]);
    let bytes = ft::dispatch_sugest(&reg, &slices(&args));
    assert_eq!(parse_integer(&bytes), 2);
    let args = add_args(b"sk", b"gamma", "3.0", &[]);
    let bytes = ft::dispatch_sugest(&reg, &slices(&args));
    assert_eq!(parse_integer(&bytes), 3);
}

#[test]
fn sugadd_replaces_score_without_incr() {
    let reg = SuggestionRegistry::new();
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"alpha", "1.0", &[])));
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"alpha", "5.0", &[])));
    // SUGGET WITHSCORES should report the latest score (5.0),
    // not the sum.
    let get: Vec<Vec<u8>> = vec![
        b"FT.SUGGET".to_vec(),
        b"sk".to_vec(),
        b"a".to_vec(),
        b"WITHSCORES".to_vec(),
    ];
    let bytes = ft::dispatch_sugest(&reg, &slices(&get));
    let arr = parse_array_flat(&bytes);
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_deref(), Some(&b"alpha"[..]));
    let score: f64 = std::str::from_utf8(arr[1].as_ref().unwrap())
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        (score - 5.0).abs() < 1e-9,
        "score should be 5.0, got {score}"
    );
}

#[test]
fn sugadd_with_incr_adds_to_score() {
    let reg = SuggestionRegistry::new();
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"alpha", "1.0", &[])));
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"alpha", "0.5", &[b"INCR"])));
    let get: Vec<Vec<u8>> = vec![
        b"FT.SUGGET".to_vec(),
        b"sk".to_vec(),
        b"a".to_vec(),
        b"WITHSCORES".to_vec(),
    ];
    let bytes = ft::dispatch_sugest(&reg, &slices(&get));
    let arr = parse_array_flat(&bytes);
    let score: f64 = std::str::from_utf8(arr[1].as_ref().unwrap())
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        (score - 1.5).abs() < 1e-9,
        "INCR score should be 1.5, got {score}"
    );
}

#[test]
fn sugget_returns_top_n_by_score() {
    let reg = SuggestionRegistry::new();
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"apple", "1.0", &[])));
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"apricot", "5.0", &[])));
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"avocado", "3.0", &[])));
    let get: Vec<Vec<u8>> = vec![b"FT.SUGGET".to_vec(), b"sk".to_vec(), b"a".to_vec()];
    let bytes = ft::dispatch_sugest(&reg, &slices(&get));
    let arr = parse_array_flat(&bytes);
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0].as_deref(), Some(&b"apricot"[..]));
    assert_eq!(arr[1].as_deref(), Some(&b"avocado"[..]));
    assert_eq!(arr[2].as_deref(), Some(&b"apple"[..]));
}

#[test]
fn sugget_lex_breaks_score_ties() {
    let reg = SuggestionRegistry::new();
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"banana", "1.0", &[])));
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"apple", "1.0", &[])));
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"cherry", "1.0", &[])));
    let get: Vec<Vec<u8>> = vec![b"FT.SUGGET".to_vec(), b"sk".to_vec(), b"".to_vec()];
    let bytes = ft::dispatch_sugest(&reg, &slices(&get));
    let arr = parse_array_flat(&bytes);
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0].as_deref(), Some(&b"apple"[..]));
    assert_eq!(arr[1].as_deref(), Some(&b"banana"[..]));
    assert_eq!(arr[2].as_deref(), Some(&b"cherry"[..]));
}

#[test]
fn sugget_with_max_caps_results() {
    let reg = SuggestionRegistry::new();
    for i in 0..10 {
        let s = format!("sugg{i}");
        let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", s.as_bytes(), "1.0", &[])));
    }
    let get: Vec<Vec<u8>> = vec![
        b"FT.SUGGET".to_vec(),
        b"sk".to_vec(),
        b"sugg".to_vec(),
        b"MAX".to_vec(),
        b"3".to_vec(),
    ];
    let bytes = ft::dispatch_sugest(&reg, &slices(&get));
    let arr = parse_array_flat(&bytes);
    assert_eq!(arr.len(), 3);
}

#[test]
fn sugget_with_withscores_returns_score_pairs() {
    let reg = SuggestionRegistry::new();
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"alpha", "2.5", &[])));
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"beta", "1.0", &[])));
    let get: Vec<Vec<u8>> = vec![
        b"FT.SUGGET".to_vec(),
        b"sk".to_vec(),
        b"".to_vec(),
        b"WITHSCORES".to_vec(),
    ];
    let bytes = ft::dispatch_sugest(&reg, &slices(&get));
    let arr = parse_array_flat(&bytes);
    // 2 hits * (value + score) = 4 elements.
    assert_eq!(arr.len(), 4);
    assert_eq!(arr[0].as_deref(), Some(&b"alpha"[..]));
    let s0: f64 = std::str::from_utf8(arr[1].as_ref().unwrap())
        .unwrap()
        .parse()
        .unwrap();
    assert!((s0 - 2.5).abs() < 1e-9);
    assert_eq!(arr[2].as_deref(), Some(&b"beta"[..]));
    let s1: f64 = std::str::from_utf8(arr[3].as_ref().unwrap())
        .unwrap()
        .parse()
        .unwrap();
    assert!((s1 - 1.0).abs() < 1e-9);
}

#[test]
fn sugget_with_withpayloads_round_trips_payload() {
    let reg = SuggestionRegistry::new();
    let with_payload = add_args(
        b"sk",
        b"alpha",
        "1.0",
        &[b"PAYLOAD", b"alpha-payload-bytes"],
    );
    let _ = ft::dispatch_sugest(&reg, &slices(&with_payload));
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"beta", "0.5", &[])));
    let get: Vec<Vec<u8>> = vec![
        b"FT.SUGGET".to_vec(),
        b"sk".to_vec(),
        b"".to_vec(),
        b"WITHPAYLOADS".to_vec(),
    ];
    let bytes = ft::dispatch_sugest(&reg, &slices(&get));
    let arr = parse_array_flat(&bytes);
    // 2 hits * (value + payload) = 4 elements.
    assert_eq!(arr.len(), 4);
    assert_eq!(arr[0].as_deref(), Some(&b"alpha"[..]));
    assert_eq!(arr[1].as_deref(), Some(&b"alpha-payload-bytes"[..]));
    assert_eq!(arr[2].as_deref(), Some(&b"beta"[..]));
    // beta has no payload -> nil.
    assert_eq!(arr[3], None);
}

#[test]
fn sugget_fuzzy_allows_one_edit() {
    let reg = SuggestionRegistry::new();
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"hello", "1.0", &[])));
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"world", "1.0", &[])));

    // Strict prefix on the typo "helo" finds nothing.
    let get_strict: Vec<Vec<u8>> = vec![b"FT.SUGGET".to_vec(), b"sk".to_vec(), b"helo".to_vec()];
    let bytes = ft::dispatch_sugest(&reg, &slices(&get_strict));
    let arr = parse_array_flat(&bytes);
    assert!(arr.is_empty(), "strict prefix on typo should miss");

    // FUZZY catches the single-edit typo.
    let get_fuzzy: Vec<Vec<u8>> = vec![
        b"FT.SUGGET".to_vec(),
        b"sk".to_vec(),
        b"helo".to_vec(),
        b"FUZZY".to_vec(),
    ];
    let bytes = ft::dispatch_sugest(&reg, &slices(&get_fuzzy));
    let arr = parse_array_flat(&bytes);
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0].as_deref(), Some(&b"hello"[..]));
}

#[test]
fn sugdel_returns_1_for_present_0_for_absent() {
    let reg = SuggestionRegistry::new();
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"alpha", "1.0", &[])));
    let del_present: Vec<Vec<u8>> = vec![b"FT.SUGDEL".to_vec(), b"sk".to_vec(), b"alpha".to_vec()];
    let bytes = ft::dispatch_sugest(&reg, &slices(&del_present));
    assert_eq!(parse_integer(&bytes), 1);
    let bytes = ft::dispatch_sugest(&reg, &slices(&del_present));
    assert_eq!(parse_integer(&bytes), 0);

    let del_missing_dict: Vec<Vec<u8>> = vec![
        b"FT.SUGDEL".to_vec(),
        b"missing".to_vec(),
        b"alpha".to_vec(),
    ];
    let bytes = ft::dispatch_sugest(&reg, &slices(&del_missing_dict));
    assert_eq!(parse_integer(&bytes), 0);
}

#[test]
fn suglen_reflects_size() {
    let reg = SuggestionRegistry::new();
    let suglen: Vec<Vec<u8>> = vec![b"FT.SUGLEN".to_vec(), b"sk".to_vec()];
    let bytes = ft::dispatch_sugest(&reg, &slices(&suglen));
    assert_eq!(parse_integer(&bytes), 0);

    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"a", "1.0", &[])));
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"b", "1.0", &[])));
    let _ = ft::dispatch_sugest(&reg, &slices(&add_args(b"sk", b"c", "1.0", &[])));
    let bytes = ft::dispatch_sugest(&reg, &slices(&suglen));
    assert_eq!(parse_integer(&bytes), 3);

    let del: Vec<Vec<u8>> = vec![b"FT.SUGDEL".to_vec(), b"sk".to_vec(), b"a".to_vec()];
    let _ = ft::dispatch_sugest(&reg, &slices(&del));
    let bytes = ft::dispatch_sugest(&reg, &slices(&suglen));
    assert_eq!(parse_integer(&bytes), 2);
}
