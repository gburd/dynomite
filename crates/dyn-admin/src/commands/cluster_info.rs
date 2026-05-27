//! `dyn-admin cluster-info` -- fetch the structured plaintext
//! diagnostic dump from a running node and print it to stdout
//! or write it to a file.
//!
//! The command issues `GET /cluster-info.txt` against the
//! supplied stats listener and treats the response body as
//! ASCII text. The output is suitable for attaching to a bug
//! report verbatim; redaction of secret material happens on
//! the server so the client need not parse the body.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use crate::client::http_get;
use crate::error::AdminError;

/// Run the cluster-info subcommand.
///
/// When `out_path` is `Some`, the dump is written to that path
/// (truncating an existing file). When it is `None`, the dump is
/// written to `stdout` via `out`.
///
/// The body is validated as ASCII before writing; the server
/// promises an ASCII-only payload, so a non-ASCII byte indicates
/// a misconfigured proxy and is rejected.
pub async fn run<W: Write>(
    node: &str,
    out_path: Option<&Path>,
    out: &mut W,
) -> Result<(), AdminError> {
    let body = http_get(node, "/cluster-info.txt").await?;
    if !body.is_ascii() {
        return Err(AdminError::Http(
            "cluster-info response was not ASCII".into(),
        ));
    }
    if let Some(path) = out_path {
        let mut f = File::create(path).map_err(AdminError::Io)?;
        f.write_all(body.as_bytes()).map_err(AdminError::Io)?;
        f.flush().map_err(AdminError::Io)?;
        // Tell the operator where the file was written so a
        // shell wrapper can pick the path out of stdout.
        writeln!(out, "wrote {} bytes to {}", body.len(), path.display())
            .map_err(AdminError::Io)?;
    } else {
        out.write_all(body.as_bytes()).map_err(AdminError::Io)?;
        if !body.ends_with('\n') {
            writeln!(out).map_err(AdminError::Io)?;
        }
    }
    Ok(())
}

/// Validate that `body` looks like a cluster-info dump: ASCII
/// only, every required header present.
///
/// Used by the integration test to assert response shape; pure
/// (no I/O) so it's safe to call from anywhere.
pub fn validate_shape(body: &str) -> Result<(), AdminError> {
    if !body.is_ascii() {
        return Err(AdminError::Http("non-ASCII cluster-info body".into()));
    }
    for header in REQUIRED_HEADERS {
        if !body.contains(header) {
            return Err(AdminError::Http(format!(
                "cluster-info body missing required header {header}"
            )));
        }
    }
    Ok(())
}

/// Section headers every well-formed dump contains. The list
/// must stay in sync with
/// [`dynomite::admin::cluster_info::format_text`]; the engine
/// integration test pins both ends.
pub const REQUIRED_HEADERS: &[&str] = &[
    "=== build ===",
    "=== config ===",
    "=== ring ===",
    "=== peers ===",
    "=== queues ===",
    "=== gossip ===",
    "=== recent_events ===",
    "=== memory ===",
    "=== fds ===",
];

/// Convenience shim: write the body to either stdout or the
/// supplied path. Shared with tests that drive the body through
/// the formatter rather than over the wire.
pub fn deliver<W: Write>(body: &str, out_path: Option<&Path>, out: &mut W) -> io::Result<()> {
    if let Some(p) = out_path {
        let mut f = File::create(p)?;
        f.write_all(body.as_bytes())?;
        f.flush()?;
        writeln!(out, "wrote {} bytes to {}", body.len(), p.display())?;
    } else {
        out.write_all(body.as_bytes())?;
        if !body.ends_with('\n') {
            writeln!(out)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_shape_passes_well_formed_body() {
        let mut body = String::new();
        for h in REQUIRED_HEADERS {
            body.push_str(h);
            body.push_str("\nk=v\n\n");
        }
        validate_shape(&body).expect("valid shape");
    }

    #[test]
    fn validate_shape_rejects_missing_section() {
        let body = "=== build ===\nk=v\n\n";
        let err = validate_shape(body).expect_err("missing");
        assert!(matches!(err, AdminError::Http(_)));
    }

    #[test]
    fn validate_shape_rejects_non_ascii() {
        let body = "=== build ===\nk=\u{2014}\n\n";
        let err = validate_shape(body).expect_err("non-ascii");
        assert!(matches!(err, AdminError::Http(_)));
    }

    #[test]
    fn deliver_to_buffer_appends_newline() {
        let mut buf = Vec::new();
        deliver("hello", None, &mut buf).unwrap();
        assert_eq!(buf, b"hello\n");
    }
}
