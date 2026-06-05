//! Transaction workload helpers for the Riak HTTP transaction
//! endpoint.
//!
//! dyniak extends Riak with a multi-key atomic transaction surface
//! exposed over HTTP at `POST /transactions` and
//! `POST /buckets/<bucket>/transactions`. This module builds the JSON
//! request bodies the benchmark sends and decides which batches
//! deliberately abort, so the workload exercises both the commit and
//! the rollback paths of the server.
//!
//! The logic lives here -- free of any transport dependency -- so it
//! compiles and is unit-tested under every feature set, while the
//! HTTP driver (feature `http`) is the only consumer that actually
//! sends the bodies.
//!
//! Transaction values must be UTF-8 for the JSON endpoint, so raw
//! value bytes are lowercase-hex encoded via [`to_hex`].

/// Default number of put operations packed into one transaction
/// batch. Multi-key by construction so the batch exercises the
/// atomic commit path.
pub const DEFAULT_TXN_BATCH_SIZE: usize = 3;

/// Default fraction of transaction batches that request a deliberate
/// abort (exercising the server's rollback path). A value in
/// `[0.0, 1.0]`.
pub const DEFAULT_TXN_ABORT_FRACTION: f64 = 0.1;

/// One put operation in a benchmark transaction batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TxnPut {
    /// Object key (UTF-8).
    pub key: String,
    /// Lowercase-hex-encoded object value.
    pub value_hex: String,
}

/// Build the JSON request body for the transaction endpoint.
///
/// `puts` becomes the ordered `operations` array; every op targets
/// `bucket` so the body is valid for both the cluster-wide
/// (`/transactions`) and the bucket-scoped
/// (`/buckets/<bucket>/transactions`) routes. When `abort` is true the
/// server applies every op and then rolls back.
///
/// # Examples
///
/// ```
/// use dyniak_bench::txn_workload::{build_txn_body, TxnPut};
/// let puts = vec![TxnPut { key: "alice".into(), value_hex: "6869".into() }];
/// let body = build_txn_body("users", &puts, false);
/// assert!(body.contains("\"abort\":false"));
/// assert!(body.contains("\"bucket\":\"users\""));
/// assert!(body.contains("\"key\":\"alice\""));
/// ```
#[must_use]
pub fn build_txn_body(bucket: &str, puts: &[TxnPut], abort: bool) -> String {
    let bucket_json = json_escape(bucket);
    let mut ops = String::new();
    for (i, p) in puts.iter().enumerate() {
        if i > 0 {
            ops.push(',');
        }
        ops.push_str("{\"op\":\"put\",\"bucket\":\"");
        ops.push_str(&bucket_json);
        ops.push_str("\",\"key\":\"");
        ops.push_str(&json_escape(&p.key));
        ops.push_str("\",\"value\":\"");
        ops.push_str(&json_escape(&p.value_hex));
        ops.push_str("\"}");
    }
    format!("{{\"abort\":{abort},\"operations\":[{ops}]}}")
}

/// Lowercase-hex encode `bytes`.
///
/// # Examples
///
/// ```
/// use dyniak_bench::txn_workload::to_hex;
/// assert_eq!(to_hex(b"hi"), "6869");
/// assert_eq!(to_hex(&[]), "");
/// ```
#[must_use]
pub fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(DIGITS[usize::from(b >> 4)] as char);
        out.push(DIGITS[usize::from(b & 0x0f)] as char);
    }
    out
}

/// Escape a string for embedding inside a JSON string literal.
///
/// Handles the two structural characters (`"` and `\`) plus the
/// control characters JSON forbids unescaped. Benchmark keys and
/// hex values never contain these, but the escaper keeps the body
/// well-formed for any bucket name an operator configures.
fn json_escape(s: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                // Control characters must be \u-escaped in JSON. The
                // write cannot fail on a String sink.
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_known_values() {
        assert_eq!(to_hex(b""), "");
        assert_eq!(to_hex(b"hi"), "6869");
        assert_eq!(to_hex(&[0x00, 0x0f, 0xff]), "000fff");
    }

    #[test]
    fn body_has_abort_flag_and_every_op() {
        let puts = vec![
            TxnPut {
                key: "alice".into(),
                value_hex: "6161".into(),
            },
            TxnPut {
                key: "bob".into(),
                value_hex: "6262".into(),
            },
            TxnPut {
                key: "carol".into(),
                value_hex: "6363".into(),
            },
        ];
        let body = build_txn_body("users", &puts, false);
        assert!(body.starts_with("{\"abort\":false,\"operations\":["));
        assert!(body.ends_with("]}"));
        // One op object per put.
        assert_eq!(body.matches("\"op\":\"put\"").count(), 3);
        // Every op carries the shared bucket and its own key.
        assert_eq!(body.matches("\"bucket\":\"users\"").count(), 3);
        for k in ["alice", "bob", "carol"] {
            assert!(body.contains(&format!("\"key\":\"{k}\"")), "missing {k}");
        }
    }

    #[test]
    fn abort_flag_is_emitted_true() {
        let puts = vec![TxnPut {
            key: "k".into(),
            value_hex: "00".into(),
        }];
        let body = build_txn_body("b", &puts, true);
        assert!(body.contains("\"abort\":true"));
    }

    #[test]
    fn empty_batch_renders_empty_array() {
        let body = build_txn_body("b", &[], false);
        assert_eq!(body, "{\"abort\":false,\"operations\":[]}");
    }

    #[test]
    fn json_escape_quotes_and_backslashes() {
        assert_eq!(json_escape("a\"b\\c"), "a\\\"b\\\\c");
        assert_eq!(json_escape("line\nbreak"), "line\\nbreak");
        assert_eq!(json_escape("\u{0001}"), "\\u0001");
    }

    #[test]
    fn abort_fraction_is_a_probability() {
        assert!((0.0..=1.0).contains(&DEFAULT_TXN_ABORT_FRACTION));
    }

    // Batches must be multi-key by construction (checked at compile
    // time so it cannot regress).
    const _: () = assert!(DEFAULT_TXN_BATCH_SIZE >= 2);
}
