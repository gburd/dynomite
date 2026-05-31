//! RediSearch FT.SEARCH filter expression parser and evaluator.
//!
//! Filter expressions are the LHS of the optional
//! `=>[KNN ...]` operator: a small DSL that combines
//! per-field predicates with boolean operators. The grammar
//! supports the four predicate shapes plus the four
//! combinators that the brief locks in:
//!
//! * `@<field>:[min max]`           - numeric range over a
//!   `NUMERIC` field. Bounds may be a literal number, `+inf`
//!   / `-inf`, or `(<n>` for an exclusive bound.
//! * `@<field>:<word>`              - text substring against
//!   a `TEXT` field's metadata bytes.
//! * `@<field>:{tag1|tag2|...}`     - set-membership against
//!   a `TAG` field's separator-split values.
//! * `*`                            - match every indexed
//!   document (the legacy LHS).
//! * `<a> <b>`                      - AND (whitespace
//!   juxtaposition).
//! * `<a> | <b>`                    - OR.
//! * `-<a>`                         - negation.
//! * `(<expr>)`                     - grouping.
//!
//! Expressions are evaluated against a [`VectorTable`]'s set
//! of indexed keys. The leaves consult either the row's
//! metadata (numeric / tag) or the table's per-field trigram
//! index (text). The combinators apply standard set algebra
//! over the resulting key sets.
//!
//! Out-of-scope shapes (geo filters, phrase queries with
//! `"..."` quoting, weighting modifiers) are surfaced as
//! [`FtError::Unsupported`]; consult the brief for the full
//! list.
//!
//! The module is intentionally self-contained: it consumes
//! raw bytes, returns either an AST or a key set, and never
//! touches the wire codec or the dispatcher.
//!
//! # Examples
//!
//! ```
//! use dynomite::proto::redis::ft_filter::{parse_expr, FilterExpr};
//!
//! let expr = parse_expr(b"@score:[100 200]").unwrap();
//! match expr {
//!     FilterExpr::NumericRange { field, .. } => assert_eq!(field, "score"),
//!     other => panic!("unexpected: {other:?}"),
//! }
//! ```
use std::collections::BTreeSet;

use crate::proto::redis::ft::FtError;
use crate::vector::registry::VectorTable;
use crate::vector::schema::MetadataFieldType;

/// One bound on a numeric range filter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NumericBound {
    /// `-inf` lower bound.
    NegInf,
    /// `+inf` upper bound.
    PosInf,
    /// Inclusive numeric value (`<n>`).
    Inclusive(f64),
    /// Exclusive numeric value (`(<n>`).
    Exclusive(f64),
}

impl NumericBound {
    /// True when `value` satisfies this bound on the given
    /// side of the range. `is_lower` selects the lower-bound
    /// semantics (the value must be `>=` / `>` than `self`);
    /// `false` selects the upper-bound semantics (`<=` / `<`).
    #[must_use]
    pub fn satisfies(&self, value: f64, is_lower: bool) -> bool {
        match (*self, is_lower) {
            (Self::NegInf, true) | (Self::PosInf, false) => true,
            (Self::NegInf, false) | (Self::PosInf, true) => false,
            (Self::Inclusive(b), true) => value >= b,
            (Self::Inclusive(b), false) => value <= b,
            (Self::Exclusive(b), true) => value > b,
            (Self::Exclusive(b), false) => value < b,
        }
    }
}

/// Filter expression AST. Built by [`parse_expr`] and
/// evaluated by [`evaluate`].
#[derive(Clone, Debug, PartialEq)]
pub enum FilterExpr {
    /// `*` literal: matches every indexed document.
    All,
    /// `@field:[min max]`.
    NumericRange {
        /// Schema field name (without the `@`).
        field: String,
        /// Lower bound.
        min: NumericBound,
        /// Upper bound.
        max: NumericBound,
    },
    /// `@field:<word>`. The query is the raw bytes after the
    /// colon, up to the next syntactic delimiter.
    TextSubstring {
        /// Schema field name.
        field: String,
        /// Substring bytes.
        query: Vec<u8>,
    },
    /// `@field:{tag1|tag2|...}`.
    TagSet {
        /// Schema field name.
        field: String,
        /// Member tags. Order is parse order; matching is by
        /// set membership against the row's separator-split
        /// values.
        tags: Vec<Vec<u8>>,
    },
    /// `<a> <b>` (whitespace) -> AND.
    And(Vec<FilterExpr>),
    /// `<a> | <b>` -> OR.
    Or(Vec<FilterExpr>),
    /// `-<expr>` -> negation.
    Not(Box<FilterExpr>),
}

/// Parse `query` as a filter expression. The query bytes
/// must be a complete, balanced expression; trailing
/// whitespace is permitted but extra non-whitespace tokens
/// trigger [`FtError::Syntax`].
///
/// # Errors
///
/// Returns [`FtError::Syntax`] for malformed shapes,
/// [`FtError::Unsupported`] for grammar in the
/// out-of-scope set (geo filters, quoted phrases,
/// weighting modifiers).
pub fn parse_expr(query: &[u8]) -> Result<FilterExpr, FtError> {
    let mut p = Parser::new(query);
    let expr = p.parse_or()?;
    p.skip_ws();
    if !p.at_end() {
        return Err(FtError::Syntax(format!(
            "FT.SEARCH filter: extra tokens at offset {} ({})",
            p.pos,
            String::from_utf8_lossy(&p.input[p.pos..]),
        )));
    }
    Ok(expr)
}

/// Evaluate the filter against `table` and return the set
/// of matching document keys.
///
/// `universe` is the candidate set. Pass
/// `table.indexed_keys()` to evaluate against every
/// observed key. Negation is computed relative to
/// `universe`; passing a smaller set therefore restricts
/// the meaning of `-<expr>` to that subset.
///
/// # Errors
///
/// Surfaces [`FtError::Syntax`] when a leaf references a
/// schema field that does not exist or has the wrong type
/// (e.g. a numeric-range leaf against a TEXT field).
pub fn evaluate(
    expr: &FilterExpr,
    table: &VectorTable,
    universe: &BTreeSet<Vec<u8>>,
) -> Result<BTreeSet<Vec<u8>>, FtError> {
    match expr {
        FilterExpr::All => Ok(universe.clone()),
        FilterExpr::NumericRange { field, min, max } => {
            ensure_field_kind(table, field, MetadataFieldType::Numeric)?;
            let mut out = BTreeSet::new();
            for key in universe {
                let Some(value) = lookup_numeric_metadata(table, key, field)? else {
                    continue;
                };
                if min.satisfies(value, true) && max.satisfies(value, false) {
                    out.insert(key.clone());
                }
            }
            Ok(out)
        }
        FilterExpr::TextSubstring { field, query } => {
            ensure_text_field(table, field)?;
            // Prefer the trigram + bloom index when one is
            // provisioned for this field. The fallback for
            // schema-declared TEXT fields without a runtime
            // index is a direct metadata substring scan;
            // both produce the same key set, the trigram
            // path just runs faster on large corpora.
            if let Some(hits) = table.search_text_substring(field, query) {
                let mut out = BTreeSet::new();
                for (key, _) in hits {
                    if universe.contains(&key) {
                        out.insert(key);
                    }
                }
                Ok(out)
            } else {
                let mut out = BTreeSet::new();
                for key in universe {
                    if let Some(bytes) = lookup_metadata_bytes(table, key, field)? {
                        if contains_subseq(&bytes, query) {
                            out.insert(key.clone());
                        }
                    }
                }
                Ok(out)
            }
        }
        FilterExpr::TagSet { field, tags } => {
            ensure_field_kind(table, field, MetadataFieldType::Tag)?;
            let separator = table
                .schema
                .metadata_fields
                .iter()
                .find(|f| f.name == *field)
                .map_or(
                    b',',
                    super::super::super::vector::schema::MetadataField::effective_tag_separator,
                );
            let mut out = BTreeSet::new();
            for key in universe {
                let Some(bytes) = lookup_metadata_bytes(table, key, field)? else {
                    continue;
                };
                if any_tag_present(&bytes, separator, tags) {
                    out.insert(key.clone());
                }
            }
            Ok(out)
        }
        FilterExpr::And(children) => {
            if children.is_empty() {
                return Ok(universe.clone());
            }
            let mut acc = evaluate(&children[0], table, universe)?;
            for child in &children[1..] {
                let next = evaluate(child, table, universe)?;
                acc = acc.intersection(&next).cloned().collect();
                if acc.is_empty() {
                    break;
                }
            }
            Ok(acc)
        }
        FilterExpr::Or(children) => {
            let mut acc = BTreeSet::new();
            for child in children {
                let next = evaluate(child, table, universe)?;
                acc.extend(next);
            }
            Ok(acc)
        }
        FilterExpr::Not(child) => {
            let inner = evaluate(child, table, universe)?;
            Ok(universe.difference(&inner).cloned().collect())
        }
    }
}

// ---- type-level helpers ------------------------------------------------

fn ensure_field_kind(
    table: &VectorTable,
    field: &str,
    kind: MetadataFieldType,
) -> Result<(), FtError> {
    let f = table
        .schema
        .metadata_fields
        .iter()
        .find(|f| f.name == field)
        .ok_or_else(|| FtError::Syntax(format!("FT.SEARCH filter: unknown field {field}")))?;
    if f.field_type != kind {
        return Err(FtError::Syntax(format!(
            "FT.SEARCH filter: field {field} is {actual:?}, not {kind:?}",
            actual = f.field_type,
        )));
    }
    Ok(())
}

fn ensure_text_field(table: &VectorTable, field: &str) -> Result<(), FtError> {
    if table.has_text_field(field) {
        return Ok(());
    }
    Err(FtError::Syntax(format!(
        "FT.SEARCH filter: field {field} is not declared TEXT",
    )))
}

fn lookup_metadata_bytes(
    table: &VectorTable,
    key: &[u8],
    field: &str,
) -> Result<Option<Vec<u8>>, FtError> {
    let row = table
        .engine
        .get(key)
        .map_err(|e| FtError::Engine(e.to_string()))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let Some(value) = row.metadata.get(field) else {
        return Ok(None);
    };
    let bytes = match value {
        serde_json::Value::String(s) => s.clone().into_bytes(),
        other => other.to_string().into_bytes(),
    };
    Ok(Some(bytes))
}

fn lookup_numeric_metadata(
    table: &VectorTable,
    key: &[u8],
    field: &str,
) -> Result<Option<f64>, FtError> {
    let Some(bytes) = lookup_metadata_bytes(table, key, field)? else {
        return Ok(None);
    };
    let s = match std::str::from_utf8(&bytes) {
        Ok(s) => s.trim(),
        Err(_) => return Ok(None),
    };
    Ok(s.parse::<f64>().ok().filter(|v| v.is_finite()))
}

fn any_tag_present(blob: &[u8], separator: u8, wanted: &[Vec<u8>]) -> bool {
    for chunk in blob.split(|b| *b == separator) {
        let trimmed = trim_ascii(chunk);
        for w in wanted {
            if trim_ascii(w) == trimmed {
                return true;
            }
        }
    }
    false
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

fn contains_subseq(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if haystack.len() < needle.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

// ---- parser ------------------------------------------------------------

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, pos: 0 }
    }

    fn at_end(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Some(b)
    }

    fn skip_ws(&mut self) {
        while let Some(b) = self.peek() {
            if b.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    /// `or_expr := and_expr ( '|' and_expr )*`.
    fn parse_or(&mut self) -> Result<FilterExpr, FtError> {
        let first = self.parse_and()?;
        let mut alts: Vec<FilterExpr> = vec![first];
        loop {
            self.skip_ws();
            if self.peek() == Some(b'|') {
                self.bump();
                self.skip_ws();
                alts.push(self.parse_and()?);
            } else {
                break;
            }
        }
        if alts.len() == 1 {
            Ok(alts.pop().expect("single child"))
        } else {
            Ok(FilterExpr::Or(alts))
        }
    }

    /// `and_expr := atom ( atom )*` (whitespace-separated).
    fn parse_and(&mut self) -> Result<FilterExpr, FtError> {
        self.skip_ws();
        let first = self.parse_atom()?;
        let mut conjs: Vec<FilterExpr> = vec![first];
        loop {
            self.skip_ws();
            match self.peek() {
                None | Some(b')' | b'|') => break,
                _ => {
                    let next = self.parse_atom()?;
                    conjs.push(next);
                }
            }
        }
        if conjs.len() == 1 {
            Ok(conjs.pop().expect("single child"))
        } else {
            Ok(FilterExpr::And(conjs))
        }
    }

    fn parse_atom(&mut self) -> Result<FilterExpr, FtError> {
        self.skip_ws();
        match self.peek() {
            None => Err(FtError::Syntax(
                "FT.SEARCH filter: unexpected end of expression".to_string(),
            )),
            Some(b'-') => {
                self.bump();
                // The brief grammar requires `-<atom>` to bind
                // tightly, not absorb a whole conjunction.
                let inner = self.parse_atom()?;
                Ok(FilterExpr::Not(Box::new(inner)))
            }
            Some(b'(') => {
                self.bump();
                let inner = self.parse_or()?;
                self.skip_ws();
                if self.peek() != Some(b')') {
                    return Err(FtError::Syntax(
                        "FT.SEARCH filter: missing ')' in group".to_string(),
                    ));
                }
                self.bump();
                Ok(inner)
            }
            Some(b'*') => {
                self.bump();
                Ok(FilterExpr::All)
            }
            Some(b'@') => self.parse_field_query(),
            Some(b'"') => Err(FtError::Unsupported(
                "FT.SEARCH filter: quoted phrases not supported in this build".to_string(),
            )),
            Some(b'{' | b'[' | b'|' | b')' | b':') => Err(FtError::Syntax(format!(
                "FT.SEARCH filter: unexpected character '{}'",
                self.peek().unwrap_or(0) as char,
            ))),
            Some(_) => Err(FtError::Syntax(format!(
                "FT.SEARCH filter: bare term '{}' must be qualified with '@<field>:'",
                String::from_utf8_lossy(self.read_word_preview()),
            ))),
        }
    }

    /// `field_query := '@' ident ':' value`.
    fn parse_field_query(&mut self) -> Result<FilterExpr, FtError> {
        debug_assert_eq!(self.peek(), Some(b'@'));
        self.bump();
        let field_start = self.pos;
        while let Some(b) = self.peek() {
            if b == b':' || b.is_ascii_whitespace() {
                break;
            }
            self.bump();
        }
        let field_bytes = &self.input[field_start..self.pos];
        if field_bytes.is_empty() {
            return Err(FtError::Syntax(
                "FT.SEARCH filter: empty field name after '@'".to_string(),
            ));
        }
        let field = std::str::from_utf8(field_bytes)
            .map(str::to_string)
            .map_err(|_| FtError::Syntax("FT.SEARCH filter: field is not UTF-8".to_string()))?;
        if self.peek() != Some(b':') {
            return Err(FtError::Syntax(format!(
                "FT.SEARCH filter: expected ':' after field {field}",
            )));
        }
        self.bump();
        match self.peek() {
            Some(b'[') => self.parse_numeric_range(field),
            Some(b'{') => self.parse_tag_set(field),
            Some(b'"') => Err(FtError::Unsupported(
                "FT.SEARCH filter: quoted phrases not supported in this build".to_string(),
            )),
            Some(b'(' | b'|' | b')') | None => Err(FtError::Syntax(format!(
                "FT.SEARCH filter: empty value for field {field}",
            ))),
            Some(b) if b.is_ascii_whitespace() => Err(FtError::Syntax(format!(
                "FT.SEARCH filter: empty value for field {field}",
            ))),
            Some(_) => self.parse_text_token(field),
        }
    }

    fn parse_numeric_range(&mut self, field: String) -> Result<FilterExpr, FtError> {
        debug_assert_eq!(self.peek(), Some(b'['));
        self.bump();
        self.skip_ws();
        let min = self.parse_numeric_bound()?;
        self.skip_ws();
        let max = self.parse_numeric_bound()?;
        self.skip_ws();
        // Reject geo-style trailing radius/unit modifiers
        // ([lon lat radius unit]) explicitly so a malformed
        // numeric range does not silently swallow extra
        // tokens.
        if self.peek() != Some(b']') {
            // Peek the next non-ws token to give a helpful
            // error: a third numeric value usually means the
            // operator tried to express a geo filter.
            let preview = self.read_word_preview();
            if !preview.is_empty() {
                return Err(FtError::Unsupported(format!(
                    "FT.SEARCH filter: extra token '{}' in numeric range (geo filters not supported)",
                    String::from_utf8_lossy(preview),
                )));
            }
            return Err(FtError::Syntax(
                "FT.SEARCH filter: missing ']' in numeric range".to_string(),
            ));
        }
        self.bump();
        Ok(FilterExpr::NumericRange { field, min, max })
    }

    fn parse_numeric_bound(&mut self) -> Result<NumericBound, FtError> {
        self.skip_ws();
        let mut exclusive = false;
        if self.peek() == Some(b'(') {
            self.bump();
            exclusive = true;
        }
        let start = self.pos;
        while let Some(b) = self.peek() {
            if b.is_ascii_whitespace() || b == b']' {
                break;
            }
            self.bump();
        }
        let token_bytes = &self.input[start..self.pos];
        if token_bytes.is_empty() {
            return Err(FtError::Syntax(
                "FT.SEARCH filter: empty numeric bound".to_string(),
            ));
        }
        let raw = std::str::from_utf8(token_bytes)
            .map_err(|_| FtError::Syntax("FT.SEARCH filter: bound is not UTF-8".to_string()))?;
        let lower = raw.to_ascii_lowercase();
        match lower.as_str() {
            "+inf" | "inf" => {
                if exclusive {
                    return Err(FtError::Syntax(
                        "FT.SEARCH filter: cannot apply '(' to infinity".to_string(),
                    ));
                }
                Ok(NumericBound::PosInf)
            }
            "-inf" => {
                if exclusive {
                    return Err(FtError::Syntax(
                        "FT.SEARCH filter: cannot apply '(' to infinity".to_string(),
                    ));
                }
                Ok(NumericBound::NegInf)
            }
            _ => {
                let parsed: f64 = raw.parse().map_err(|_| {
                    FtError::Syntax(format!("FT.SEARCH filter: invalid numeric bound {raw}"))
                })?;
                if !parsed.is_finite() {
                    return Err(FtError::Syntax(format!(
                        "FT.SEARCH filter: non-finite numeric bound {raw}",
                    )));
                }
                if exclusive {
                    Ok(NumericBound::Exclusive(parsed))
                } else {
                    Ok(NumericBound::Inclusive(parsed))
                }
            }
        }
    }

    fn parse_tag_set(&mut self, field: String) -> Result<FilterExpr, FtError> {
        debug_assert_eq!(self.peek(), Some(b'{'));
        self.bump();
        let mut tags: Vec<Vec<u8>> = Vec::new();
        loop {
            self.skip_ws();
            let start = self.pos;
            while let Some(b) = self.peek() {
                if b == b'|' || b == b'}' || b.is_ascii_whitespace() {
                    break;
                }
                self.bump();
            }
            let bytes = &self.input[start..self.pos];
            if bytes.is_empty() {
                return Err(FtError::Syntax(format!(
                    "FT.SEARCH filter: empty tag in @{field}:{{...}}"
                )));
            }
            tags.push(bytes.to_vec());
            self.skip_ws();
            match self.peek() {
                Some(b'|') => {
                    self.bump();
                }
                Some(b'}') => {
                    self.bump();
                    break;
                }
                _ => {
                    return Err(FtError::Syntax(format!(
                        "FT.SEARCH filter: expected '|' or '}}' in @{field}:{{...}}"
                    )));
                }
            }
        }
        if tags.is_empty() {
            return Err(FtError::Syntax(format!(
                "FT.SEARCH filter: @{field}:{{}} requires at least one tag",
            )));
        }
        Ok(FilterExpr::TagSet { field, tags })
    }

    fn parse_text_token(&mut self, field: String) -> Result<FilterExpr, FtError> {
        let start = self.pos;
        while let Some(b) = self.peek() {
            // Stop at any boolean/grouping operator. Hyphen
            // is fine inside a word (e.g. `errno-7`); a
            // leading `-` is only meaningful at the start
            // of an atom, which is handled above.
            if b.is_ascii_whitespace()
                || b == b'|'
                || b == b'('
                || b == b')'
                || b == b'{'
                || b == b'}'
                || b == b'['
                || b == b']'
            {
                break;
            }
            self.bump();
        }
        let bytes = &self.input[start..self.pos];
        if bytes.is_empty() {
            return Err(FtError::Syntax(format!(
                "FT.SEARCH filter: empty value for field {field}",
            )));
        }
        Ok(FilterExpr::TextSubstring {
            field,
            query: bytes.to_vec(),
        })
    }

    /// Snap a short preview of the current word for error
    /// messages. Does not advance `pos`.
    fn read_word_preview(&self) -> &[u8] {
        let start = self.pos;
        let mut end = start;
        while end < self.input.len() && !self.input[end].is_ascii_whitespace() {
            end += 1;
        }
        &self.input[start..end.min(start + 32)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_match_all_returns_all_variant() {
        let expr = parse_expr(b"*").unwrap();
        assert_eq!(expr, FilterExpr::All);
    }

    #[test]
    fn parse_numeric_range_inclusive() {
        let expr = parse_expr(b"@score:[100 200]").unwrap();
        match expr {
            FilterExpr::NumericRange { field, min, max } => {
                assert_eq!(field, "score");
                assert_eq!(min, NumericBound::Inclusive(100.0));
                assert_eq!(max, NumericBound::Inclusive(200.0));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn parse_numeric_range_exclusive_upper() {
        let expr = parse_expr(b"@score:[100 (200]").unwrap();
        let FilterExpr::NumericRange { min, max, .. } = expr else {
            panic!("expected numeric range");
        };
        assert_eq!(min, NumericBound::Inclusive(100.0));
        assert_eq!(max, NumericBound::Exclusive(200.0));
    }

    #[test]
    fn parse_numeric_range_inf_bounds() {
        let expr = parse_expr(b"@score:[-inf +inf]").unwrap();
        let FilterExpr::NumericRange { min, max, .. } = expr else {
            panic!("expected numeric range");
        };
        assert_eq!(min, NumericBound::NegInf);
        assert_eq!(max, NumericBound::PosInf);
    }

    #[test]
    fn parse_text_substring_token() {
        let expr = parse_expr(b"@body:errno").unwrap();
        match expr {
            FilterExpr::TextSubstring { field, query } => {
                assert_eq!(field, "body");
                assert_eq!(query, b"errno");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn parse_tag_set_multi_value() {
        let expr = parse_expr(b"@status:{ok|warn|error}").unwrap();
        let FilterExpr::TagSet { field, tags } = expr else {
            panic!("expected tag set");
        };
        assert_eq!(field, "status");
        assert_eq!(
            tags,
            vec![b"ok".to_vec(), b"warn".to_vec(), b"error".to_vec()]
        );
    }

    #[test]
    fn parse_and_two_clauses() {
        let expr = parse_expr(b"@score:[100 200] @body:errno").unwrap();
        let FilterExpr::And(children) = expr else {
            panic!("expected AND");
        };
        assert_eq!(children.len(), 2);
        assert!(matches!(&children[0], FilterExpr::NumericRange { .. }));
        assert!(matches!(&children[1], FilterExpr::TextSubstring { .. }));
    }

    #[test]
    fn parse_or_two_clauses() {
        let expr = parse_expr(b"@status:{ok} | @status:{warn}").unwrap();
        let FilterExpr::Or(children) = expr else {
            panic!("expected OR");
        };
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn parse_negation_unary() {
        let expr = parse_expr(b"-@status:{stale}").unwrap();
        let FilterExpr::Not(inner) = expr else {
            panic!("expected NOT");
        };
        assert!(matches!(*inner, FilterExpr::TagSet { .. }));
    }

    #[test]
    fn parse_grouping_with_parens() {
        let expr = parse_expr(b"(@status:{a} | @status:{b}) @body:foo").unwrap();
        let FilterExpr::And(children) = expr else {
            panic!("expected AND at the top");
        };
        assert_eq!(children.len(), 2);
        assert!(matches!(&children[0], FilterExpr::Or(_)));
        assert!(matches!(&children[1], FilterExpr::TextSubstring { .. }));
    }

    #[test]
    fn parse_empty_value_errors() {
        let err = parse_expr(b"@body:").unwrap_err();
        assert!(matches!(err, FtError::Syntax(_)));
    }

    #[test]
    fn parse_unbalanced_paren_errors() {
        let err = parse_expr(b"(@a:b").unwrap_err();
        assert!(matches!(err, FtError::Syntax(_)));
    }

    #[test]
    fn parse_geo_style_extra_token_returns_unsupported() {
        let err = parse_expr(b"@loc:[10 20 5 km]").unwrap_err();
        assert!(matches!(err, FtError::Unsupported(_)));
    }

    #[test]
    fn parse_quoted_phrase_returns_unsupported() {
        let err = parse_expr(b"@body:\"hello world\"").unwrap_err();
        assert!(matches!(err, FtError::Unsupported(_)));
    }

    #[test]
    fn numeric_bound_satisfies_inclusive() {
        let b = NumericBound::Inclusive(10.0);
        assert!(b.satisfies(10.0, true));
        assert!(b.satisfies(11.0, true));
        assert!(!b.satisfies(9.0, true));
        assert!(b.satisfies(9.0, false));
        assert!(!b.satisfies(11.0, false));
    }

    #[test]
    fn numeric_bound_satisfies_exclusive() {
        let b = NumericBound::Exclusive(10.0);
        assert!(!b.satisfies(10.0, true));
        assert!(b.satisfies(11.0, true));
        assert!(!b.satisfies(10.0, false));
    }

    #[test]
    fn numeric_bound_satisfies_inf() {
        assert!(NumericBound::NegInf.satisfies(-1e300, true));
        assert!(NumericBound::PosInf.satisfies(1e300, false));
        assert!(!NumericBound::NegInf.satisfies(0.0, false));
        assert!(!NumericBound::PosInf.satisfies(0.0, true));
    }

    #[test]
    fn any_tag_present_is_separator_aware() {
        assert!(any_tag_present(b"ok,warn", b',', &[b"warn".to_vec()]));
        assert!(!any_tag_present(b"ok,warn", b',', &[b"info".to_vec()]));
        assert!(any_tag_present(
            b"ok|warn",
            b'|',
            &[b"warn".to_vec(), b"foo".to_vec()]
        ));
        // Whitespace around tags is trimmed.
        assert!(any_tag_present(b" ok , warn", b',', &[b"ok".to_vec()]));
    }

    #[test]
    fn contains_subseq_is_byte_substring() {
        assert!(contains_subseq(b"hello world", b"lo wo"));
        assert!(!contains_subseq(b"hello world", b"xyz"));
        assert!(contains_subseq(b"abc", b""));
        assert!(!contains_subseq(b"a", b"abc"));
    }
}
