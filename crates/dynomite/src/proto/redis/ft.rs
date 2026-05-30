//! RediSearch FT.* command surface.
//!
//! This module is the in-process home of the
//! `FT.CREATE` / `FT.SEARCH` / `FT.INFO` / `FT.LIST` /
//! `FT.DROPINDEX` parsers and executors that route through the
//! [`crate::vector::registry::VectorRegistry`] landed in Phase B.
//! It also exposes [`maybe_index_hset`], the HSET interception
//! helper that the Redis dispatcher consults so that a write to
//! a key matching a registered prefix turns into a vector
//! upsert in the corresponding index.
//!
//! The module is intentionally self-contained: it consumes a
//! slice of byte strings (the parsed RESP arguments) and emits
//! a RESP-encoded response (`Vec<u8>`). It performs no I/O of
//! its own. That separation lets the FT.* surface be exercised
//! by integration tests directly, with the wire-format parser
//! and the dispatcher kept on the side.
//!
//! # Subset implemented today
//!
//! * `FT.CREATE <idx> ON HASH PREFIX <n> <prefix>... SCHEMA
//!   ( <field> TEXT | <field> VECTOR HNSW <m> TYPE FLOAT32
//!     DIM <d> DISTANCE_METRIC COSINE|EUCLIDEAN|DOTPRODUCT )+`
//! * `FT.SEARCH <idx> "*=>[KNN <k> @<field> $<param>]"
//!     PARAMS 2 <param> <bytes>`
//! * `FT.INFO <idx>`
//! * `FT.LIST` (alias `FT._LIST`)
//! * `FT.DROPINDEX <idx> [DD]`
//!
//! Out-of-scope shapes are rejected with
//! `-ERR not supported in this build`. The full grammar is
//! tracked under `docs/dynvec/fold-into-redis-path.md`.

use std::collections::HashMap;
use std::io::Write;

use thiserror::Error;

use crate::vector::registry::{VectorRegistry, VectorTable};
use crate::vector::schema::{
    DistanceMetric, IndexAlgorithm, MetadataField, MetadataFieldType, VectorSchema, VectorType,
};

/// Failure modes for the FT.* surface.
///
/// Each variant is mapped to a `-ERR ...` line by
/// [`render_error`]. The outward-facing wire shape is plain
/// RESP2 simple errors today; richer error replies (the
/// RediSearch `-WRONGTYPE` / `-NOINDEX` family) are a follow
/// up.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FtError {
    /// The first token is not a known `FT.*` keyword.
    #[error("unknown command: {0}")]
    UnknownCommand(String),
    /// The argument count or shape did not match the grammar.
    #[error("syntax error: {0}")]
    Syntax(String),
    /// The grammar shape is recognised but lands on a feature
    /// we do not implement in this build.
    #[error("not supported in this build: {0}")]
    Unsupported(String),
    /// No index registered under that name.
    #[error("index not found: {0}")]
    NotFound(String),
    /// An index already exists under that name.
    #[error("index already exists: {0}")]
    AlreadyExists(String),
    /// The vector dimension on a write or query does not match
    /// the index dimension.
    #[error("dimension mismatch: index={index_dim}, payload={payload_dim}")]
    DimensionMismatch {
        /// Index dimension.
        index_dim: usize,
        /// Payload dimension.
        payload_dim: usize,
    },
    /// Engine-level failure forwarded as a string.
    #[error("engine: {0}")]
    Engine(String),
}

/// Parsed FT.* command, ready for execution.
#[derive(Clone, Debug, PartialEq)]
pub enum FtCommand {
    /// `FT.CREATE` parsed payload.
    Create(CreateRequest),
    /// `FT.SEARCH` parsed payload.
    Search(SearchRequest),
    /// `FT.INFO <name>`.
    Info {
        /// Index name.
        name: String,
    },
    /// `FT.LIST` / `FT._LIST`.
    List,
    /// `FT.DROPINDEX <name> [DD]`.
    DropIndex {
        /// Index name.
        name: String,
        /// True when the `DD` keyword was present.
        delete_documents: bool,
    },
}

/// Document-source family. Today only `HASH` is recognised.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DocType {
    /// Hash documents (`ON HASH`).
    Hash,
}

/// Parsed `FT.CREATE` request.
#[derive(Clone, Debug, PartialEq)]
pub struct CreateRequest {
    /// Index name.
    pub name: String,
    /// Document family.
    pub doc_type: DocType,
    /// Compiled schema (prefixes + vector + metadata fields).
    pub schema: VectorSchema,
}

/// Parsed `FT.SEARCH` request restricted to the
/// `*=>[KNN k @field $param]` form.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchRequest {
    /// Index name.
    pub name: String,
    /// Number of results to return.
    pub k: usize,
    /// Vector field referenced in the query expression.
    pub vector_field: String,
    /// Raw query vector bytes (little-endian f32 stream).
    pub vector_bytes: Vec<u8>,
}

/// Outcome of an FT.* execution; the dispatch layer turns this
/// into RESP bytes via [`render_outcome`]. Tests can inspect
/// the structured result directly.
#[derive(Clone, Debug, PartialEq)]
pub enum FtOutcome {
    /// Simple status string (`+OK`).
    Ok,
    /// FT.LIST array of names.
    List(Vec<String>),
    /// FT.INFO key/value pairs.
    Info(Vec<(String, InfoValue)>),
    /// FT.SEARCH result set: total count plus per-doc
    /// (id, score) pairs.
    Search {
        /// Total number of matches returned.
        total: usize,
        /// Per-document (id, score) hits, sorted closest-first.
        hits: Vec<SearchHit>,
    },
    /// `+OK` with side note: drop also affected `<n>`
    /// underlying documents.
    DropOk {
        /// True when `DD` was present (and the underlying keys
        /// were enumerated).
        deleted_documents: bool,
        /// Number of underlying keys reported.
        document_count: usize,
    },
}

/// One hit in an FT.SEARCH response.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchHit {
    /// Document key.
    pub doc_id: Vec<u8>,
    /// Distance score (smaller is closer).
    pub score: f32,
    /// Selected metadata fields. Today the surface always
    /// emits the score under `__vec_score` plus every metadata
    /// field stored on the row.
    pub fields: Vec<(String, Vec<u8>)>,
}

/// Untyped FT.INFO value; mirrors the RESP shapes RediSearch
/// emits.
#[derive(Clone, Debug, PartialEq)]
pub enum InfoValue {
    /// Bulk string.
    String(String),
    /// Integer.
    Integer(i64),
    /// Nested array.
    Array(Vec<InfoValue>),
}

/// Parse a complete FT.* command (the keyword plus its args).
///
/// `args[0]` is the FT.* keyword (e.g. `FT.CREATE`). The rest
/// is the command body. Case is normalised to uppercase ASCII
/// for the keyword and per-clause tokens.
///
/// # Errors
///
/// [`FtError::UnknownCommand`] when `args[0]` is empty or not
/// an FT.* keyword; [`FtError::Syntax`] /
/// [`FtError::Unsupported`] for grammar issues.
pub fn parse_command(args: &[&[u8]]) -> Result<FtCommand, FtError> {
    let head = args
        .first()
        .ok_or_else(|| FtError::UnknownCommand(String::new()))?;
    let cmd = ascii_upper(head);
    let rest = &args[1..];
    match cmd.as_slice() {
        b"FT.CREATE" => parse_create(rest).map(FtCommand::Create),
        b"FT.SEARCH" => parse_search(rest).map(FtCommand::Search),
        b"FT.INFO" => parse_info(rest),
        b"FT.LIST" | b"FT._LIST" => parse_list(rest),
        b"FT.DROPINDEX" => parse_dropindex(rest),
        other => Err(FtError::UnknownCommand(
            String::from_utf8_lossy(other).into_owned(),
        )),
    }
}

/// Execute a parsed [`FtCommand`] against `registry`.
///
/// # Errors
///
/// Surfaces [`RegistryError`](crate::vector::registry::RegistryError)
/// translated into [`FtError`] for already-exists / not-found
/// outcomes, or [`FtError::Engine`] for engine-side failures.
pub fn execute(registry: &VectorRegistry, cmd: FtCommand) -> Result<FtOutcome, FtError> {
    match cmd {
        FtCommand::Create(req) => execute_create(registry, req),
        FtCommand::Search(req) => execute_search(registry, &req),
        FtCommand::Info { name } => execute_info(registry, name),
        FtCommand::List => Ok(FtOutcome::List(registry.list())),
        FtCommand::DropIndex {
            name,
            delete_documents,
        } => execute_dropindex(registry, name, delete_documents),
    }
}

/// Parse and execute `args` in one call, returning the RESP
/// bytes the wire wants to see.
///
/// This is the single public entry point the dispatcher reaches
/// for after recognising an FT.* keyword. Callers that want
/// structured results should call [`parse_command`] and
/// [`execute`] separately.
#[must_use]
pub fn dispatch(registry: &VectorRegistry, args: &[&[u8]]) -> Vec<u8> {
    match parse_command(args) {
        Ok(cmd) => match execute(registry, cmd) {
            Ok(outcome) => render_outcome(&outcome),
            Err(err) => render_error(&err),
        },
        Err(err) => render_error(&err),
    }
}

/// Inspect an HSET argument list and, if its key matches a
/// registered prefix, route it into the corresponding index.
///
/// `args` is the raw HSET argument vector after the command
/// keyword: `[key, f1, v1, f2, v2, ...]`. Returns the name of
/// the index that absorbed the write, or `None` when no
/// registered prefix matched.
///
/// # Errors
///
/// [`FtError::Syntax`] for malformed HSET argument lists,
/// [`FtError::DimensionMismatch`] when the vector field does
/// not carry the index's frozen dimension, and
/// [`FtError::Engine`] when the engine refuses the upsert.
pub fn maybe_index_hset(
    registry: &VectorRegistry,
    args: &[&[u8]],
) -> Result<Option<String>, FtError> {
    if args.is_empty() {
        return Err(FtError::Syntax("HSET requires a key".to_string()));
    }
    let key = args[0];
    let pairs = &args[1..];
    if pairs.is_empty() || !pairs.len().is_multiple_of(2) {
        return Err(FtError::Syntax(
            "HSET requires field/value pairs".to_string(),
        ));
    }
    for name in registry.list() {
        let Some(table) = registry.get(&name) else {
            continue;
        };
        if table.schema.prefixes.iter().any(|p| key.starts_with(p)) {
            insert_into_index(&table, key, pairs)?;
            return Ok(Some(name));
        }
    }
    Ok(None)
}

// ---- FT.CREATE ----------------------------------------------------------

fn parse_create(rest: &[&[u8]]) -> Result<CreateRequest, FtError> {
    // Layout: <name> ON <doc-type> [PREFIX <n> <p>...] SCHEMA <field>+
    let mut it = TokenCursor::new(rest);
    let name = it.next_string("FT.CREATE: missing index name")?;

    expect_keyword(it.next_required("FT.CREATE: expected ON")?, "ON")?;
    let doc_type_tok = it.next_required("FT.CREATE: expected doc type")?;
    let doc_type_up = ascii_upper(doc_type_tok);
    let doc_type = match doc_type_up.as_slice() {
        b"HASH" => DocType::Hash,
        _ => {
            return Err(FtError::Unsupported(format!(
                "FT.CREATE doc type {}",
                String::from_utf8_lossy(doc_type_tok)
            )));
        }
    };

    // PREFIX clause.
    let mut prefixes: Vec<Vec<u8>> = Vec::new();
    if matches_keyword(it.peek(), "PREFIX") {
        it.advance();
        let n_tok = it.next_required("FT.CREATE: PREFIX expects a count")?;
        let n = parse_unsigned(n_tok, "FT.CREATE: PREFIX count")?;
        for _ in 0..n {
            let p = it.next_required("FT.CREATE: missing PREFIX value")?;
            prefixes.push(p.to_vec());
        }
    }
    if prefixes.is_empty() {
        return Err(FtError::Syntax(
            "FT.CREATE requires at least one PREFIX value".to_string(),
        ));
    }

    expect_keyword(it.next_required("FT.CREATE: expected SCHEMA")?, "SCHEMA")?;

    let mut vector_field: Option<(String, VectorType, u16, DistanceMetric, IndexAlgorithm)> = None;
    let mut metadata_fields: Vec<MetadataField> = Vec::new();
    while let Some(field_tok) = it.next() {
        let field_name = utf8(field_tok, "FT.CREATE: field name")?;
        let kind_tok = it.next_required("FT.CREATE: missing field kind")?;
        let kind_up = ascii_upper(kind_tok);
        match kind_up.as_slice() {
            b"TEXT" => metadata_fields.push(MetadataField {
                name: field_name,
                field_type: MetadataFieldType::Text,
            }),
            b"NUMERIC" => metadata_fields.push(MetadataField {
                name: field_name,
                field_type: MetadataFieldType::Numeric,
            }),
            b"TAG" => metadata_fields.push(MetadataField {
                name: field_name,
                field_type: MetadataFieldType::Tag,
            }),
            b"GEO" => metadata_fields.push(MetadataField {
                name: field_name,
                field_type: MetadataFieldType::Geo,
            }),
            b"VECTOR" => {
                if vector_field.is_some() {
                    return Err(FtError::Unsupported(
                        "multiple VECTOR fields per index".to_string(),
                    ));
                }
                let parsed = parse_vector_clause(&mut it)?;
                vector_field = Some((field_name, parsed.0, parsed.1, parsed.2, parsed.3));
            }
            other => {
                return Err(FtError::Unsupported(format!(
                    "FT.CREATE field kind {}",
                    String::from_utf8_lossy(other)
                )));
            }
        }
    }
    let (vec_name, vec_type, dim, distance, algorithm) = vector_field.ok_or_else(|| {
        FtError::Syntax("FT.CREATE: SCHEMA must declare a VECTOR field".to_string())
    })?;

    let schema = VectorSchema {
        vector_field: vec_name,
        vector_type: vec_type,
        dim,
        distance,
        algorithm,
        prefixes,
        metadata_fields,
    };
    Ok(CreateRequest {
        name,
        doc_type,
        schema,
    })
}

fn parse_vector_clause(
    it: &mut TokenCursor<'_>,
) -> Result<(VectorType, u16, DistanceMetric, IndexAlgorithm), FtError> {
    let alg_tok = it.next_required("FT.CREATE: VECTOR missing algorithm")?;
    let alg_up = ascii_upper(alg_tok);
    let algorithm = match alg_up.as_slice() {
        b"HNSW" => IndexAlgorithm::Hnsw,
        b"FLAT" => {
            return Err(FtError::Unsupported(
                "FT.CREATE: FLAT vector index not supported in this build".to_string(),
            ));
        }
        other => {
            return Err(FtError::Unsupported(format!(
                "FT.CREATE VECTOR algorithm {}",
                String::from_utf8_lossy(other)
            )));
        }
    };
    let pair_count_tok = it.next_required("FT.CREATE: VECTOR missing parameter count")?;
    let pair_count = parse_unsigned(pair_count_tok, "FT.CREATE VECTOR parameter count")?;
    if !pair_count.is_multiple_of(2) {
        return Err(FtError::Syntax(
            "FT.CREATE VECTOR parameter count must be even".to_string(),
        ));
    }
    let mut vec_type: Option<VectorType> = None;
    let mut dim: Option<u16> = None;
    let mut distance: Option<DistanceMetric> = None;
    let pair_pairs = pair_count / 2;
    for _ in 0..pair_pairs {
        let key_tok = it.next_required("FT.CREATE: VECTOR missing parameter key")?;
        let val_tok = it.next_required("FT.CREATE: VECTOR missing parameter value")?;
        let key_up = ascii_upper(key_tok);
        let val_up = ascii_upper(val_tok);
        match key_up.as_slice() {
            b"TYPE" => match val_up.as_slice() {
                b"FLOAT32" => vec_type = Some(VectorType::Float32),
                b"FLOAT16" => {
                    return Err(FtError::Unsupported(
                        "FT.CREATE VECTOR TYPE FLOAT16 not supported in this build".to_string(),
                    ));
                }
                other => {
                    return Err(FtError::Unsupported(format!(
                        "FT.CREATE VECTOR TYPE {}",
                        String::from_utf8_lossy(other)
                    )));
                }
            },
            b"DIM" => {
                let d = parse_unsigned(val_tok, "FT.CREATE VECTOR DIM")?;
                if d == 0 || d > usize::from(u16::MAX) {
                    return Err(FtError::Syntax(
                        "FT.CREATE VECTOR DIM out of range".to_string(),
                    ));
                }
                dim = Some(u16::try_from(d).expect("dim fits u16"));
            }
            b"DISTANCE_METRIC" => {
                distance = Some(match val_up.as_slice() {
                    b"COSINE" => DistanceMetric::Cosine,
                    b"L2" | b"EUCLIDEAN" => DistanceMetric::L2,
                    b"IP" | b"DOTPRODUCT" | b"DOT_PRODUCT" => DistanceMetric::InnerProduct,
                    other => {
                        return Err(FtError::Unsupported(format!(
                            "FT.CREATE DISTANCE_METRIC {}",
                            String::from_utf8_lossy(other)
                        )));
                    }
                });
            }
            // Other RediSearch knobs (M, EF_CONSTRUCTION, ...).
            // Phase C ignores them (the engine uses defaults);
            // a future phase will plug them into HnswParams.
            _ => {}
        }
    }
    let vec_type =
        vec_type.ok_or_else(|| FtError::Syntax("FT.CREATE VECTOR missing TYPE".to_string()))?;
    let dim = dim.ok_or_else(|| FtError::Syntax("FT.CREATE VECTOR missing DIM".to_string()))?;
    let distance = distance
        .ok_or_else(|| FtError::Syntax("FT.CREATE VECTOR missing DISTANCE_METRIC".to_string()))?;
    Ok((vec_type, dim, distance, algorithm))
}

fn execute_create(registry: &VectorRegistry, req: CreateRequest) -> Result<FtOutcome, FtError> {
    use crate::vector::registry::RegistryError;
    match registry.create(req.name.clone(), req.schema) {
        Ok(()) => Ok(FtOutcome::Ok),
        Err(RegistryError::AlreadyExists(name)) => Err(FtError::AlreadyExists(name)),
        Err(RegistryError::UnsupportedAlgorithm(_)) => Err(FtError::Unsupported(
            "FT.CREATE: unsupported VECTOR algorithm".to_string(),
        )),
        Err(other) => Err(FtError::Engine(other.to_string())),
    }
}

// ---- FT.SEARCH ----------------------------------------------------------

fn parse_search(rest: &[&[u8]]) -> Result<SearchRequest, FtError> {
    let mut it = TokenCursor::new(rest);
    let name = it.next_string("FT.SEARCH: missing index name")?;
    let query = it.next_required("FT.SEARCH: missing query expression")?;
    let (k, vec_field, param_name) = parse_knn_query(query)?;

    // Optional clauses: PARAMS, RETURN, SORTBY, LIMIT, DIALECT.
    let mut params: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    loop {
        let Some(tok) = it.next() else { break };
        let up = ascii_upper(tok);
        match up.as_slice() {
            b"PARAMS" => {
                let n_tok = it.next_required("FT.SEARCH: PARAMS expects a count")?;
                let n = parse_unsigned(n_tok, "FT.SEARCH PARAMS count")?;
                if !n.is_multiple_of(2) {
                    return Err(FtError::Syntax(
                        "FT.SEARCH PARAMS count must be even".to_string(),
                    ));
                }
                for _ in 0..(n / 2) {
                    let k_tok = it.next_required("FT.SEARCH: PARAMS expects key/value pair")?;
                    let v_tok = it.next_required("FT.SEARCH: PARAMS expects key/value pair")?;
                    params.insert(k_tok.to_vec(), v_tok.to_vec());
                }
            }
            b"RETURN" => {
                let n_tok = it.next_required("FT.SEARCH: RETURN expects a count")?;
                let n = parse_unsigned(n_tok, "FT.SEARCH RETURN count")?;
                for _ in 0..n {
                    it.next_required("FT.SEARCH: RETURN expects field name")?;
                }
            }
            b"SORTBY" => {
                return Err(FtError::Unsupported(
                    "FT.SEARCH SORTBY not supported in this build".to_string(),
                ));
            }
            b"LIMIT" => {
                // Phase C ignores LIMIT and always honours the
                // implicit `0 k` from the KNN expression.
                it.next_required("FT.SEARCH: LIMIT expects offset")?;
                it.next_required("FT.SEARCH: LIMIT expects count")?;
            }
            b"DIALECT" => {
                it.next_required("FT.SEARCH: DIALECT expects a value")?;
            }
            b"NOCONTENT" | b"WITHSCORES" => {
                // Tolerated as no-ops; Phase C always returns
                // the score and the document key.
            }
            other => {
                return Err(FtError::Unsupported(format!(
                    "FT.SEARCH clause {}",
                    String::from_utf8_lossy(other)
                )));
            }
        }
    }

    let vector_bytes = params
        .remove(param_name.as_bytes())
        .ok_or_else(|| FtError::Syntax(format!("FT.SEARCH: PARAMS missing ${param_name}")))?;
    Ok(SearchRequest {
        name,
        k,
        vector_field: vec_field,
        vector_bytes,
    })
}

/// Parse the `*=>[KNN k @field $param]` query expression.
fn parse_knn_query(query: &[u8]) -> Result<(usize, String, String), FtError> {
    let s = std::str::from_utf8(query)
        .map_err(|_| FtError::Syntax("FT.SEARCH query is not UTF-8".to_string()))?;
    let trimmed = s.trim();
    let stripped = trimmed
        .strip_prefix("*=>")
        .ok_or_else(|| FtError::Unsupported(format!("FT.SEARCH query: {trimmed}")))?
        .trim_start();
    let inner = stripped
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| FtError::Syntax("FT.SEARCH query: missing brackets".to_string()))?;
    let mut parts = inner.split_ascii_whitespace();
    let knn_kw = parts
        .next()
        .ok_or_else(|| FtError::Syntax("FT.SEARCH query: empty".to_string()))?;
    if !knn_kw.eq_ignore_ascii_case("KNN") {
        return Err(FtError::Unsupported(format!(
            "FT.SEARCH query operator: {knn_kw}"
        )));
    }
    let k_str = parts
        .next()
        .ok_or_else(|| FtError::Syntax("FT.SEARCH query: KNN expects k".to_string()))?;
    let k: usize = k_str
        .parse()
        .map_err(|_| FtError::Syntax(format!("FT.SEARCH query: invalid k {k_str}")))?;
    let field_tok = parts
        .next()
        .ok_or_else(|| FtError::Syntax("FT.SEARCH query: KNN expects @field".to_string()))?;
    let field = field_tok
        .strip_prefix('@')
        .ok_or_else(|| FtError::Syntax("FT.SEARCH query: field must start with @".to_string()))?;
    let param_tok = parts
        .next()
        .ok_or_else(|| FtError::Syntax("FT.SEARCH query: KNN expects $param".to_string()))?;
    let param = param_tok
        .strip_prefix('$')
        .ok_or_else(|| FtError::Syntax("FT.SEARCH query: param must start with $".to_string()))?;
    if parts.next().is_some() {
        return Err(FtError::Unsupported(
            "FT.SEARCH query: extra tokens after KNN expression".to_string(),
        ));
    }
    if k == 0 {
        return Err(FtError::Syntax("FT.SEARCH KNN k must be > 0".to_string()));
    }
    Ok((k, field.to_string(), param.to_string()))
}

fn execute_search(registry: &VectorRegistry, req: &SearchRequest) -> Result<FtOutcome, FtError> {
    let table = registry
        .get(&req.name)
        .ok_or_else(|| FtError::NotFound(req.name.clone()))?;
    if table.schema.vector_field != req.vector_field {
        return Err(FtError::Syntax(format!(
            "FT.SEARCH: query references @{} but index vector field is {}",
            req.vector_field, table.schema.vector_field
        )));
    }
    let dim = usize::from(table.schema.dim);
    let query = decode_le_f32(&req.vector_bytes, dim)?;
    let hits = table
        .engine
        .search(&query, req.k, None)
        .map_err(|e| FtError::Engine(e.to_string()))?;

    let mut out = Vec::with_capacity(hits.len());
    for (row, score) in hits {
        let mut fields: Vec<(String, Vec<u8>)> = Vec::new();
        fields.push(("__vec_score".to_string(), format_float(score).into_bytes()));
        for (k, v) in &row.metadata {
            let value_bytes = match v {
                serde_json::Value::String(s) => s.clone().into_bytes(),
                other => other.to_string().into_bytes(),
            };
            fields.push((k.clone(), value_bytes));
        }
        out.push(SearchHit {
            doc_id: row.key,
            score,
            fields,
        });
    }
    let total = out.len();
    Ok(FtOutcome::Search { total, hits: out })
}

// ---- FT.INFO / FT.LIST / FT.DROPINDEX -----------------------------------

fn parse_info(rest: &[&[u8]]) -> Result<FtCommand, FtError> {
    let mut it = TokenCursor::new(rest);
    let name = it.next_string("FT.INFO: missing index name")?;
    if it.peek().is_some() {
        return Err(FtError::Syntax(
            "FT.INFO: unexpected trailing tokens".to_string(),
        ));
    }
    Ok(FtCommand::Info { name })
}

fn parse_list(rest: &[&[u8]]) -> Result<FtCommand, FtError> {
    if !rest.is_empty() {
        return Err(FtError::Syntax("FT.LIST: takes no arguments".to_string()));
    }
    Ok(FtCommand::List)
}

fn parse_dropindex(rest: &[&[u8]]) -> Result<FtCommand, FtError> {
    let mut it = TokenCursor::new(rest);
    let name = it.next_string("FT.DROPINDEX: missing index name")?;
    let mut delete_documents = false;
    loop {
        let Some(tok) = it.next() else { break };
        let up = ascii_upper(tok);
        match up.as_slice() {
            b"DD" => delete_documents = true,
            other => {
                return Err(FtError::Unsupported(format!(
                    "FT.DROPINDEX option {}",
                    String::from_utf8_lossy(other)
                )));
            }
        }
    }
    Ok(FtCommand::DropIndex {
        name,
        delete_documents,
    })
}

fn execute_info(registry: &VectorRegistry, name: String) -> Result<FtOutcome, FtError> {
    let info = registry
        .info(&name)
        .ok_or_else(|| FtError::NotFound(name.clone()))?;
    let table = registry.get(&name).ok_or(FtError::NotFound(name))?;
    let mut out: Vec<(String, InfoValue)> = Vec::new();
    out.push(("index_name".to_string(), InfoValue::String(info.name)));
    out.push((
        "algorithm".to_string(),
        InfoValue::String(format!("{:?}", info.algorithm).to_uppercase()),
    ));
    out.push((
        "distance_metric".to_string(),
        InfoValue::String(format!("{:?}", info.distance).to_uppercase()),
    ));
    out.push((
        "vector_field".to_string(),
        InfoValue::String(table.schema.vector_field.clone()),
    ));
    out.push((
        "vector_type".to_string(),
        InfoValue::String(format!("{:?}", table.schema.vector_type).to_uppercase()),
    ));
    out.push(("dim".to_string(), InfoValue::Integer(i64::from(info.dim))));
    let prefixes_value = InfoValue::Array(
        table
            .schema
            .prefixes
            .iter()
            .map(|p| InfoValue::String(String::from_utf8_lossy(p).into_owned()))
            .collect(),
    );
    out.push(("prefixes".to_string(), prefixes_value));
    let metadata_value = InfoValue::Array(
        table
            .schema
            .metadata_fields
            .iter()
            .map(|f| {
                InfoValue::Array(vec![
                    InfoValue::String(f.name.clone()),
                    InfoValue::String(format!("{:?}", f.field_type).to_uppercase()),
                ])
            })
            .collect(),
    );
    out.push(("schema_fields".to_string(), metadata_value));
    out.push((
        "num_docs".to_string(),
        InfoValue::Integer(i64::try_from(info.live_rows).unwrap_or(i64::MAX)),
    ));
    out.push((
        "tracked_rows".to_string(),
        InfoValue::Integer(i64::try_from(info.tracked_rows).unwrap_or(i64::MAX)),
    ));
    Ok(FtOutcome::Info(out))
}

fn execute_dropindex(
    registry: &VectorRegistry,
    name: String,
    delete_documents: bool,
) -> Result<FtOutcome, FtError> {
    use crate::vector::registry::RegistryError;
    if delete_documents {
        match registry.drop_with_dd(&name) {
            Ok(keys) => Ok(FtOutcome::DropOk {
                deleted_documents: true,
                document_count: keys.len(),
            }),
            Err(RegistryError::NotFound(_)) => Err(FtError::NotFound(name)),
            Err(other) => Err(FtError::Engine(other.to_string())),
        }
    } else {
        match registry.drop(&name) {
            Ok(_) => Ok(FtOutcome::DropOk {
                deleted_documents: false,
                document_count: 0,
            }),
            Err(RegistryError::NotFound(_)) => Err(FtError::NotFound(name)),
            Err(other) => Err(FtError::Engine(other.to_string())),
        }
    }
}

// ---- HSET interception --------------------------------------------------

fn insert_into_index(table: &VectorTable, key: &[u8], pairs: &[&[u8]]) -> Result<(), FtError> {
    let mut vector: Option<Vec<f32>> = None;
    let mut metadata: HashMap<String, serde_json::Value> = HashMap::new();
    let mut chunks = pairs.chunks_exact(2);
    for chunk in &mut chunks {
        let field = chunk[0];
        let value = chunk[1];
        let field_str = std::str::from_utf8(field)
            .map_err(|_| FtError::Syntax("HSET field name is not UTF-8".to_string()))?;
        if field_str == table.schema.vector_field {
            let dim = usize::from(table.schema.dim);
            vector = Some(decode_le_f32(value, dim)?);
        } else {
            let value_str = String::from_utf8_lossy(value).into_owned();
            metadata.insert(field_str.to_string(), serde_json::Value::String(value_str));
        }
    }
    if !chunks.remainder().is_empty() {
        return Err(FtError::Syntax(
            "HSET requires a value for every field".to_string(),
        ));
    }
    let v = vector.ok_or_else(|| {
        FtError::Syntax(format!(
            "HSET into indexed prefix is missing the vector field '{}'",
            table.schema.vector_field
        ))
    })?;
    table
        .engine
        .upsert(key.to_vec(), &v, metadata)
        .map_err(|e| FtError::Engine(e.to_string()))?;
    table.record_indexed_key(key.to_vec());
    Ok(())
}

// ---- RESP rendering -----------------------------------------------------

/// Render an [`FtOutcome`] as RESP2 bytes.
#[must_use]
pub fn render_outcome(outcome: &FtOutcome) -> Vec<u8> {
    let mut out = Vec::new();
    match outcome {
        FtOutcome::Ok | FtOutcome::DropOk { .. } => {
            out.extend_from_slice(b"+OK\r\n");
        }
        FtOutcome::List(names) => {
            write_array_header(&mut out, names.len());
            for name in names {
                write_bulk(&mut out, name.as_bytes());
            }
        }
        FtOutcome::Info(pairs) => {
            write_array_header(&mut out, pairs.len() * 2);
            for (k, v) in pairs {
                write_bulk(&mut out, k.as_bytes());
                write_info_value(&mut out, v);
            }
        }
        FtOutcome::Search { total, hits } => {
            // Layout: [total, doc_id_1, [field_pairs_1...], ...].
            let total_i64 = i64::try_from(*total).unwrap_or(i64::MAX);
            write_array_header(&mut out, 1 + hits.len() * 2);
            write_integer(&mut out, total_i64);
            for hit in hits {
                write_bulk(&mut out, &hit.doc_id);
                write_array_header(&mut out, hit.fields.len() * 2);
                for (fk, fv) in &hit.fields {
                    write_bulk(&mut out, fk.as_bytes());
                    write_bulk(&mut out, fv);
                }
            }
        }
    }
    out
}

/// Render an [`FtError`] as a RESP2 simple-error line.
#[must_use]
pub fn render_error(err: &FtError) -> Vec<u8> {
    let mut buf = Vec::new();
    // Every variant currently maps to the generic `-ERR`
    // prefix; richer RESP error families (`-WRONGTYPE`,
    // `-NOINDEX`, ...) are tracked in the brief's
    // out-of-scope list.
    let _ = write!(buf, "-ERR {err}\r\n");
    buf
}

fn write_array_header(out: &mut Vec<u8>, n: usize) {
    let _ = write!(out, "*{n}\r\n");
}

fn write_bulk(out: &mut Vec<u8>, payload: &[u8]) {
    let _ = write!(out, "${}\r\n", payload.len());
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n");
}

fn write_integer(out: &mut Vec<u8>, n: i64) {
    let _ = write!(out, ":{n}\r\n");
}

fn write_info_value(out: &mut Vec<u8>, value: &InfoValue) {
    match value {
        InfoValue::String(s) => write_bulk(out, s.as_bytes()),
        InfoValue::Integer(n) => write_integer(out, *n),
        InfoValue::Array(items) => {
            write_array_header(out, items.len());
            for it in items {
                write_info_value(out, it);
            }
        }
    }
}

// ---- helpers ------------------------------------------------------------

fn ascii_upper(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().map(u8::to_ascii_uppercase).collect()
}

fn matches_keyword(tok: Option<&[u8]>, kw: &str) -> bool {
    tok.is_some_and(|t| t.eq_ignore_ascii_case(kw.as_bytes()))
}

fn expect_keyword(tok: &[u8], kw: &str) -> Result<(), FtError> {
    if tok.eq_ignore_ascii_case(kw.as_bytes()) {
        Ok(())
    } else {
        Err(FtError::Syntax(format!(
            "expected {kw}, got {}",
            String::from_utf8_lossy(tok)
        )))
    }
}

fn parse_unsigned(tok: &[u8], context: &str) -> Result<usize, FtError> {
    let s =
        std::str::from_utf8(tok).map_err(|_| FtError::Syntax(format!("{context}: not UTF-8")))?;
    s.parse::<usize>()
        .map_err(|_| FtError::Syntax(format!("{context}: not a non-negative integer ({s})")))
}

fn utf8(tok: &[u8], context: &str) -> Result<String, FtError> {
    std::str::from_utf8(tok)
        .map(str::to_string)
        .map_err(|_| FtError::Syntax(format!("{context}: not UTF-8")))
}

fn decode_le_f32(bytes: &[u8], expected_dim: usize) -> Result<Vec<f32>, FtError> {
    if !bytes.len().is_multiple_of(4) {
        return Err(FtError::Syntax(
            "vector payload length is not a multiple of 4 bytes".to_string(),
        ));
    }
    let dim = bytes.len() / 4;
    if dim != expected_dim {
        return Err(FtError::DimensionMismatch {
            index_dim: expected_dim,
            payload_dim: dim,
        });
    }
    let mut out = Vec::with_capacity(dim);
    for chunk in bytes.chunks_exact(4) {
        let arr = [chunk[0], chunk[1], chunk[2], chunk[3]];
        out.push(f32::from_le_bytes(arr));
    }
    Ok(out)
}

fn format_float(f: f32) -> String {
    // RESP RediSearch traditionally renders scores in
    // shortest-round-trip form. `{:.6}` keeps the response
    // parseable as a float without trailing precision noise.
    format!("{f:.6}")
}

/// Walk-cursor over `&[&[u8]]` argument lists. Used by the
/// hand-written FT.* parsers in this module.
struct TokenCursor<'a> {
    args: &'a [&'a [u8]],
    idx: usize,
}

impl<'a> TokenCursor<'a> {
    fn new(args: &'a [&'a [u8]]) -> Self {
        Self { args, idx: 0 }
    }

    fn peek(&self) -> Option<&'a [u8]> {
        self.args.get(self.idx).copied()
    }

    fn advance(&mut self) {
        self.idx += 1;
    }

    fn next_required(&mut self, msg: &str) -> Result<&'a [u8], FtError> {
        let tok = self
            .args
            .get(self.idx)
            .copied()
            .ok_or_else(|| FtError::Syntax(msg.to_string()))?;
        self.idx += 1;
        Ok(tok)
    }

    fn next_string(&mut self, msg: &str) -> Result<String, FtError> {
        let tok = self.next_required(msg)?;
        utf8(tok, msg)
    }
}

impl<'a> Iterator for TokenCursor<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let tok = self.args.get(self.idx).copied()?;
        self.idx += 1;
        Some(tok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slices<'a>(v: &'a [&'a [u8]]) -> Vec<&'a [u8]> {
        v.to_vec()
    }

    #[test]
    fn ascii_upper_lowercases_to_uppercase() {
        assert_eq!(ascii_upper(b"ft.create"), b"FT.CREATE".to_vec());
    }

    #[test]
    fn parse_create_minimal() {
        let v: Vec<&[u8]> = vec![
            b"FT.CREATE",
            b"idx",
            b"ON",
            b"HASH",
            b"PREFIX",
            b"1",
            b"docs:",
            b"SCHEMA",
            b"vec",
            b"VECTOR",
            b"HNSW",
            b"6",
            b"TYPE",
            b"FLOAT32",
            b"DIM",
            b"4",
            b"DISTANCE_METRIC",
            b"COSINE",
        ];
        let cmd = parse_command(&slices(&v)).expect("parse ok");
        let FtCommand::Create(req) = cmd else {
            panic!("expected create");
        };
        assert_eq!(req.name, "idx");
        assert_eq!(req.doc_type, DocType::Hash);
        assert_eq!(req.schema.dim, 4);
        assert_eq!(req.schema.vector_field, "vec");
        assert_eq!(req.schema.distance, DistanceMetric::Cosine);
        assert_eq!(req.schema.algorithm, IndexAlgorithm::Hnsw);
        assert_eq!(req.schema.prefixes, vec![b"docs:".to_vec()]);
    }

    #[test]
    fn parse_knn_query_extracts_pieces() {
        let (k, field, param) = parse_knn_query(b"*=>[KNN 5 @vec $blob]").unwrap();
        assert_eq!(k, 5);
        assert_eq!(field, "vec");
        assert_eq!(param, "blob");
    }

    #[test]
    fn parse_knn_query_rejects_filter() {
        let err = parse_knn_query(b"@title:foo=>[KNN 5 @vec $blob]").unwrap_err();
        assert!(matches!(err, FtError::Unsupported(_)));
    }

    #[test]
    fn decode_le_f32_round_trips_a_short_vector() {
        let mut bytes = Vec::new();
        for v in [1.0_f32, -1.5, 2.25, 0.0] {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        let out = decode_le_f32(&bytes, 4).unwrap();
        assert_eq!(out, vec![1.0_f32, -1.5, 2.25, 0.0]);
    }

    #[test]
    fn decode_le_f32_rejects_dim_mismatch() {
        let bytes = vec![0u8; 8];
        let err = decode_le_f32(&bytes, 4).unwrap_err();
        assert!(matches!(err, FtError::DimensionMismatch { .. }));
    }
}
