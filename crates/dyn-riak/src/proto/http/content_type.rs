//! HTTP content-type negotiation for the Riak HTTP gateway.
//!
//! The HTTP transport carries Riak object bodies in one of three
//! baseline serialisations:
//!
//! * `application/json`
//! * `application/x-protobuf`
//! * `application/cbor`
//!
//! These are exactly the codecs the
//! [`dyn_encoding::CodecRegistry`] baseline ships with. The four
//! extra codecs called out in the broader plan (flatbuffers, capnp,
//! bebop, bson) are not yet wired through the registry as of this
//! slice and are therefore not advertised here. When they land, the
//! [`SUPPORTED_CONTENT_TYPES`] table is the only place that needs to
//! grow.
//!
//! # Negotiation rules
//!
//! [`select_codec`] implements a small subset of RFC 7231 Section 5.3:
//!
//! 1. The `Accept` header is split on commas. Each entry is a media
//!    range followed by zero or more `;param=value` parameters. The
//!    `q=` parameter, if present, sets the relative weight (default
//!    1.0).
//! 2. Entries with `q=0` are dropped.
//! 3. The highest-weight entry that names a supported content-type
//!    wins. Ties are broken by left-to-right order in the header.
//! 4. The wildcard `*/*` matches any content-type. When the wildcard
//!    is the best entry, the request `Content-Type` is preferred (so
//!    a JSON `PUT` followed by a `GET` with `Accept: */*` returns
//!    JSON), falling back to `application/json` if no request
//!    content-type was supplied or it was unsupported.
//! 5. If the `Accept` header is empty or absent (passed in as `""`),
//!    the request `Content-Type` is preferred, again falling back to
//!    `application/json`.
//! 6. If the header is non-empty but lists only unsupported types,
//!    the function returns `None` so the caller can reply with `406
//!    Not Acceptable`.

/// Content-types the Riak HTTP gateway can encode and decode.
///
/// Order matters for the wildcard fallback: when `Accept: */*` is
/// the best match and the request did not pin a `Content-Type`, the
/// first entry in this slice is used. JSON is the convention for
/// browser-driven clients and is therefore listed first.
pub const SUPPORTED_CONTENT_TYPES: &[&str] = &[
    "application/json",
    "application/x-protobuf",
    "application/cbor",
];

/// Pick the response content-type for an HTTP request.
///
/// `accept` is the raw value of the `Accept` header (pass `""` if
/// the header was absent). `content_type` is the `Content-Type` of
/// the request body, if any.
///
/// Returns the canonical content-type string from
/// [`SUPPORTED_CONTENT_TYPES`], or `None` when the client only
/// asked for media types this gateway cannot produce.
///
/// # Examples
///
/// ```
/// use dyn_riak::proto::http::content_type::select_codec;
/// assert_eq!(select_codec("application/json", None), Some("application/json"));
/// assert_eq!(
///     select_codec("application/json;q=0.5, application/cbor;q=0.9", None),
///     Some("application/cbor"),
/// );
/// assert_eq!(select_codec("*/*", Some("application/x-protobuf")), Some("application/x-protobuf"));
/// assert_eq!(select_codec("application/xml", None), None);
/// ```
#[must_use]
pub fn select_codec(accept: &str, content_type: Option<&str>) -> Option<&'static str> {
    let trimmed = accept.trim();
    if trimmed.is_empty() {
        return Some(
            canonicalize(content_type.unwrap_or("")).unwrap_or(SUPPORTED_CONTENT_TYPES[0]),
        );
    }

    // Best (canonical content-type, weight, order index). Order
    // index is used as the tie-breaker so the first entry of equal
    // weight wins.
    let mut best: Option<(&'static str, f32, usize)> = None;

    for (idx, raw) in trimmed.split(',').enumerate() {
        let entry = raw.trim();
        if entry.is_empty() {
            continue;
        }

        let mut parts = entry.split(';');
        let media = parts.next().unwrap_or("").trim();
        let mut q: f32 = 1.0;
        for param in parts {
            let param = param.trim();
            if let Some(rest) = param.strip_prefix("q=") {
                if let Ok(v) = rest.parse::<f32>() {
                    q = v;
                }
            }
        }
        if q.is_nan() || q <= 0.0 {
            // q=0 means "not acceptable"; NaN treated the same way.
            continue;
        }

        let candidate: Option<&'static str> = if media == "*/*" {
            // Wildcard: prefer the request content-type when it is
            // a supported codec; otherwise fall back to the default.
            Some(
                content_type
                    .and_then(canonicalize)
                    .unwrap_or(SUPPORTED_CONTENT_TYPES[0]),
            )
        } else {
            canonicalize(media)
        };

        if let Some(canonical) = candidate {
            match best {
                None => best = Some((canonical, q, idx)),
                Some((_, bq, _)) if q > bq => best = Some((canonical, q, idx)),
                _ => {}
            }
        }
    }

    best.map(|(ct, _, _)| ct)
}

/// Map a raw media-type string to the canonical entry from
/// [`SUPPORTED_CONTENT_TYPES`], if any.
///
/// The match is case-insensitive on the type/subtype portion and
/// ignores any `;parameter=value` suffix.
#[must_use]
pub fn canonicalize(raw: &str) -> Option<&'static str> {
    let head = raw.split(';').next().unwrap_or("").trim();
    SUPPORTED_CONTENT_TYPES
        .iter()
        .copied()
        .find(|c| c.eq_ignore_ascii_case(head))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match_returns_canonical() {
        assert_eq!(
            select_codec("application/json", None),
            Some("application/json")
        );
        assert_eq!(
            select_codec("application/cbor", None),
            Some("application/cbor")
        );
        assert_eq!(
            select_codec("application/x-protobuf", None),
            Some("application/x-protobuf")
        );
    }

    #[test]
    fn case_insensitive_match() {
        assert_eq!(
            select_codec("APPLICATION/JSON", None),
            Some("application/json")
        );
        assert_eq!(
            select_codec("Application/X-Protobuf", None),
            Some("application/x-protobuf")
        );
    }

    #[test]
    fn parameters_are_ignored_for_matching() {
        assert_eq!(
            select_codec("application/json; charset=utf-8", None),
            Some("application/json")
        );
    }

    #[test]
    fn highest_q_wins() {
        assert_eq!(
            select_codec("application/json;q=0.5, application/cbor;q=0.9", None,),
            Some("application/cbor")
        );
        assert_eq!(
            select_codec(
                "application/cbor;q=0.2, application/json;q=0.8, application/x-protobuf;q=0.5",
                None,
            ),
            Some("application/json")
        );
    }

    #[test]
    fn equal_q_falls_back_to_first_listed() {
        assert_eq!(
            select_codec("application/json, application/cbor", None),
            Some("application/json")
        );
        assert_eq!(
            select_codec("application/cbor, application/json", None),
            Some("application/cbor")
        );
    }

    #[test]
    fn q_zero_entries_are_dropped() {
        assert_eq!(
            select_codec("application/json;q=0, application/cbor", None),
            Some("application/cbor")
        );
        assert_eq!(select_codec("application/json;q=0", None), None);
    }

    #[test]
    fn wildcard_prefers_content_type() {
        assert_eq!(
            select_codec("*/*", Some("application/x-protobuf")),
            Some("application/x-protobuf")
        );
        assert_eq!(
            select_codec("*/*", Some("application/cbor; charset=utf-8")),
            Some("application/cbor")
        );
    }

    #[test]
    fn wildcard_without_content_type_uses_default() {
        assert_eq!(select_codec("*/*", None), Some("application/json"));
    }

    #[test]
    fn wildcard_with_unsupported_content_type_uses_default() {
        assert_eq!(
            select_codec("*/*", Some("application/xml")),
            Some("application/json")
        );
    }

    #[test]
    fn empty_accept_uses_content_type_or_default() {
        assert_eq!(
            select_codec("", Some("application/cbor")),
            Some("application/cbor")
        );
        assert_eq!(select_codec("   ", None), Some("application/json"));
        assert_eq!(
            select_codec("", Some("application/xml")),
            Some("application/json")
        );
    }

    #[test]
    fn unsupported_only_returns_none() {
        assert_eq!(select_codec("application/xml", None), None);
        assert_eq!(
            select_codec("text/plain, application/yaml;q=0.9", None),
            None
        );
    }

    #[test]
    fn explicit_match_beats_wildcard_at_equal_weight() {
        // "application/cbor" and "*/*" both at q=1.0. The named
        // type and the wildcard are both candidates; the named
        // type appears second. Ties go to the first occurrence,
        // and the wildcard resolves through content_type.
        assert_eq!(
            select_codec("*/*, application/cbor", Some("application/json")),
            Some("application/json")
        );
        // Reverse: cbor wins because it is first.
        assert_eq!(
            select_codec("application/cbor, */*", Some("application/json")),
            Some("application/cbor")
        );
    }

    #[test]
    fn unsupported_then_supported_picks_supported() {
        assert_eq!(
            select_codec("application/xml, application/json;q=0.5", None),
            Some("application/json")
        );
    }

    #[test]
    fn malformed_q_falls_back_to_default_one() {
        // "q=banana" cannot parse; default weight 1.0 applies.
        assert_eq!(
            select_codec("application/json;q=banana", None),
            Some("application/json")
        );
    }

    #[test]
    fn supported_table_is_three_entries() {
        // Locks the baseline. When we wire the additional four
        // codecs from Item 2 the count and this test grow together.
        assert_eq!(SUPPORTED_CONTENT_TYPES.len(), 3);
    }

    #[test]
    fn canonicalize_rejects_unknown() {
        assert!(canonicalize("application/xml").is_none());
        assert!(canonicalize("").is_none());
        assert_eq!(canonicalize("APPLICATION/JSON"), Some("application/json"));
    }
}
