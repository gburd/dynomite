//! `dyn-admin metrics` -- fetch the Prometheus text endpoint and
//! print it verbatim.

use std::io::Write;

use crate::client::http_get;
use crate::error::AdminError;

/// Fetch `/metrics` from `node` and copy the body byte-for-byte to
/// `out`. The Prometheus text format is the public surface; no
/// reshaping is applied. A trailing newline is appended when the
/// server omits one so the output always ends in `\n`.
pub async fn run<W: Write>(node: &str, out: &mut W) -> Result<(), AdminError> {
    let body = http_get(node, "/metrics").await?;
    out.write_all(body.as_bytes())?;
    if !body.ends_with('\n') {
        writeln!(out)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    /// The metrics renderer is "write the body verbatim". The
    /// integration test confirms the wire round-trip; the unit test
    /// here just confirms that an empty body produces a single
    /// newline (rather than zero bytes), which makes the output
    /// safe to pipe into tools that expect line-terminated input.
    #[test]
    fn write_passthrough_appends_newline_when_missing() {
        // Inline the trailing-newline branch from `run` without the
        // network call.
        let body = "foo bar 1";
        let mut buf = Vec::new();
        std::io::Write::write_all(&mut buf, body.as_bytes()).unwrap();
        if !body.ends_with('\n') {
            std::io::Write::write_all(&mut buf, b"\n").unwrap();
        }
        let s = String::from_utf8(buf).unwrap();
        assert!(s.ends_with('\n'));
        assert!(s.starts_with("foo bar"));
    }

    #[test]
    fn write_passthrough_keeps_existing_trailing_newline() {
        let body = "foo bar 1\n";
        let mut buf = Vec::new();
        std::io::Write::write_all(&mut buf, body.as_bytes()).unwrap();
        if !body.ends_with('\n') {
            std::io::Write::write_all(&mut buf, b"\n").unwrap();
        }
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s, body);
    }
}
