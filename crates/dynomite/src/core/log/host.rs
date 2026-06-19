//! Hostname helper used by the syslog event formatters.
//!
//! `nix::unistd::gethostname` is already in the workspace and is the
//! cheapest way to obtain the kernel-reported hostname on Linux. The
//! return type is an `OsString`; we lossy-decode to `String` so the
//! formatters can drop it into a UTF-8 line without further error
//! handling.

use nix::unistd;

/// Read the local hostname with a deterministic fallback.
///
/// On hosts where `gethostname(2)` succeeds the kernel-reported name is
/// returned (decoded with `OsString::to_string_lossy`). When the call
/// fails, or when the name is empty, we fall back to the literal string
/// `"localhost"` so the syslog `HOSTNAME` field is always a single
/// non-empty token. RFC 5424 section 6.2.4 requires HOSTNAME to consist
/// of one or more printable US-ASCII characters; to satisfy that we
/// also replace any whitespace or non-printable byte with `_`.
///
/// # Examples
///
/// ```
/// use dynomite::core::log::local_hostname;
/// let h = local_hostname();
/// assert!(!h.is_empty());
/// assert!(!h.contains(' '));
/// ```
#[must_use]
pub fn local_hostname() -> String {
    let raw = unistd::gethostname()
        .ok()
        .and_then(|os| os.into_string().ok())
        .unwrap_or_default();
    sanitize_hostname(&raw)
}

/// Sanitise a raw hostname into a single non-empty printable
/// US-ASCII token, replacing any non-graphic byte with `_` and
/// substituting `"localhost"` for an empty result.
fn sanitize_hostname(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|c| if c.is_ascii_graphic() { c } else { '_' })
        .collect();
    if cleaned.is_empty() {
        "localhost".to_string()
    } else {
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostname_is_non_empty_ascii() {
        let h = local_hostname();
        assert!(!h.is_empty());
        for c in h.chars() {
            assert!(
                c.is_ascii_graphic(),
                "non-ASCII-graphic char in hostname: {c:?}"
            );
        }
    }

    #[test]
    fn sanitize_replaces_non_graphic_and_falls_back_when_empty() {
        // Empty input falls back to the literal localhost token.
        assert_eq!(sanitize_hostname(""), "localhost");
        // Whitespace and control bytes become underscores.
        assert_eq!(sanitize_hostname("a b\tc"), "a_b_c");
        // An all-whitespace name still yields a non-empty token.
        assert_eq!(sanitize_hostname("   "), "___");
        // A clean name is returned verbatim.
        assert_eq!(sanitize_hostname("node-1.dc"), "node-1.dc");
    }
}
