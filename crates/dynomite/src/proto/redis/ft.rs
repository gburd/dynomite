//! RediSearch FT.* command surface.
//!
//! This module is the in-process home of the
//! `FT.CREATE` / `FT.SEARCH` / `FT.INFO` / `FT.LIST` /
//! `FT.DROPINDEX` / `FT.AGGREGATE` / `FT.EXPLAIN` /
//! `FT.ALTER` parsers and executors that route through the
//! [`crate::vector::registry::VectorRegistry`] landed in
//! Phase B. It also exposes [`maybe_index_hset`], the HSET
//! interception helper that the Redis dispatcher consults so
//! that a write to a key matching a registered prefix turns
//! into a vector upsert in the corresponding index.
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
//!     PARAMS 2 <param> <bytes>
//!     [RETURN <n> <field>...] [LIMIT <off> <cnt>]
//!     [SORTBY @<field> [ASC|DESC]] [NOCONTENT]`
//!   (vector k-NN form).
//! * `FT.SEARCH <idx> "@<field>:<substring>"` (text
//!   substring form; routed through the trigram +
//!   bloom-filter inverted index for any `TEXT`-typed schema
//!   field). Accepts the same `RETURN` / `LIMIT` / `SORTBY`
//!   / `NOCONTENT` projection clauses.
//! * `FT.AGGREGATE <idx> <query> GROUPBY <n> @<field>...
//!     ( REDUCE COUNT 0 AS <name>
//!     | REDUCE SUM 1 @<field> AS <name>
//!     | REDUCE AVG 1 @<field> AS <name> )+
//!     [LIMIT <off> <cnt>]` (the basic aggregation pipeline).
//! * `FT.EXPLAIN <idx> <query>` (textual query plan).
//! * `FT.ALTER <idx> ADD <field> ( TEXT | TAG )` (schema
//!   extension; rejects DROP / SCHEMA REPLACE and refuses
//!   to add VECTOR fields, which require an index rebuild).
//! * `FT.REGEX <idx> <field> <pattern> [K=<n>]` (Dynomite
//!   extension; not standard RediSearch). `K=0` (default) is
//!   the exact regex path; `K>=1` is the approximate regex
//!   path through the TRE C library, allowing up to `K` edit
//!   operations between the pattern and the matching text.
//! * `FT.INFO <idx>`
//! * `FT.LIST` (alias `FT._LIST`)
//! * `FT.DROPINDEX <idx> [DD]`
//!
//! Out-of-scope shapes are rejected with
//! `-ERR not supported in this build`. The full grammar is
//! tracked under `docs/dynvec/fold-into-redis-path.md`.

use std::collections::{BTreeMap, HashMap};
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
    /// `FT.SEARCH` k-NN form: `*=>[KNN k @field $param]`.
    Search(SearchRequest),
    /// `FT.SEARCH` substring form: `@field:<substring>`.
    SearchText(SearchTextRequest),
    /// `FT.AGGREGATE` GROUPBY pipeline.
    Aggregate(AggregateRequest),
    /// `FT.EXPLAIN <idx> <query>`.
    Explain(ExplainRequest),
    /// `FT.ALTER <idx> ADD <field> <type>`.
    Alter(AlterRequest),
    /// `FT.REGEX` (Dynomite extension): exact or approximate
    /// regex over a `TEXT`-indexed field.
    Regex(RegexRequest),
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

/// Sort direction for the `SORTBY` clause of `FT.SEARCH`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortDirection {
    /// Ascending order (smallest first).
    Asc,
    /// Descending order (largest first).
    Desc,
}

/// Parsed `FT.SEARCH` request restricted to the
/// `*=>[KNN k @field $param]` form. Carries the optional
/// projection clauses (`RETURN` / `LIMIT` / `SORTBY` /
/// `NOCONTENT`) so callers can pre-flight them at parse
/// time and the executor can apply them to the result set
/// before rendering.
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
    /// Optional `RETURN N field1 ... fieldN` projection. When
    /// `Some(list)` only those fields appear on each hit;
    /// when `None` every stored metadata field is emitted
    /// alongside the implicit `__vec_score`.
    pub return_fields: Option<Vec<String>>,
    /// Optional `LIMIT offset count` pagination window.
    /// `None` means "return up to `k` hits starting at 0".
    pub limit: Option<(usize, usize)>,
    /// Optional `SORTBY @field [ASC|DESC]` ordering. `None`
    /// preserves the engine's distance order.
    pub sortby: Option<(String, SortDirection)>,
    /// True when the client asked for `NOCONTENT`: hits are
    /// emitted as bare doc ids with no field arrays.
    pub nocontent: bool,
}

/// Parsed `FT.SEARCH` request for the text-substring form
/// `@field:<substring>`. The substring is matched against the
/// TEXT field's trigram + bloom inverted index. Carries the
/// same projection clauses as the k-NN variant.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchTextRequest {
    /// Index name.
    pub name: String,
    /// `TEXT` schema field referenced in the query.
    pub field: String,
    /// Raw substring bytes to search for.
    pub query: Vec<u8>,
    /// Optional `RETURN` projection (see [`SearchRequest`]).
    pub return_fields: Option<Vec<String>>,
    /// Optional `LIMIT offset count` window.
    pub limit: Option<(usize, usize)>,
    /// Optional `SORTBY` ordering.
    pub sortby: Option<(String, SortDirection)>,
    /// True when the client asked for `NOCONTENT`.
    pub nocontent: bool,
}

/// Reducer kind on an `FT.AGGREGATE` `REDUCE` clause.
#[derive(Clone, Debug, PartialEq)]
pub enum ReducerKind {
    /// `REDUCE COUNT 0 AS <name>` -- per-group row count.
    Count,
    /// `REDUCE SUM 1 @<field> AS <name>` -- per-group sum of
    /// the named numeric field.
    Sum {
        /// Field name (without the leading `@`).
        field: String,
    },
    /// `REDUCE AVG 1 @<field> AS <name>` -- per-group mean of
    /// the named numeric field.
    Avg {
        /// Field name (without the leading `@`).
        field: String,
    },
}

/// One reducer in an `FT.AGGREGATE` pipeline.
#[derive(Clone, Debug, PartialEq)]
pub struct ReducerSpec {
    /// Reducer kind (`COUNT` / `SUM` / `AVG`).
    pub kind: ReducerKind,
    /// Output column name (the `AS <name>` token).
    pub alias: String,
}

/// Parsed `FT.AGGREGATE` request.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregateRequest {
    /// Index name.
    pub name: String,
    /// Group-by fields (the `@<field>` tokens). At least
    /// one is required by this build's subset.
    pub group_by: Vec<String>,
    /// Pipeline of reducers, in declaration order.
    pub reducers: Vec<ReducerSpec>,
    /// Optional `LIMIT offset count` window applied after
    /// grouping.
    pub limit: Option<(usize, usize)>,
}

/// Parsed `FT.EXPLAIN` request.
#[derive(Clone, Debug, PartialEq)]
pub struct ExplainRequest {
    /// Index name.
    pub name: String,
    /// The raw query expression to explain.
    pub query: Vec<u8>,
}

/// Parsed `FT.ALTER ADD <field> <type>` request. Only ADD is
/// supported in this build; DROP and SCHEMA REPLACE are
/// rejected at parse time.
#[derive(Clone, Debug, PartialEq)]
pub struct AlterRequest {
    /// Index name.
    pub name: String,
    /// Field name to add.
    pub field: String,
    /// New field type. Limited to TEXT and TAG; VECTOR is
    /// explicitly rejected at parse time.
    pub field_type: MetadataFieldType,
}

/// Parsed `FT.REGEX` request. `max_errors` of `0` selects the
/// exact-regex path; values `>= 1` route through the TRE
/// approximate-regex matcher tolerating up to `max_errors`
/// edit operations.
#[derive(Clone, Debug, PartialEq)]
pub struct RegexRequest {
    /// Index name.
    pub name: String,
    /// `TEXT` schema field referenced in the query.
    pub field: String,
    /// Regular expression in POSIX-extended syntax.
    pub pattern: String,
    /// Maximum number of allowed edit operations.
    pub max_errors: u16,
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
    /// FT.SEARCH result set when the client passed
    /// `NOCONTENT`: only the matching doc ids are emitted,
    /// without per-hit metadata arrays.
    SearchNoContent {
        /// Total number of matches returned.
        total: usize,
        /// Doc keys, in the same order they would appear in
        /// the corresponding [`Self::Search`] response.
        doc_ids: Vec<Vec<u8>>,
    },
    /// FT.AGGREGATE result set: total group count plus one
    /// row of `(name, value)` pairs per group.
    Aggregate {
        /// Number of groups produced (before `LIMIT`).
        total_groups: usize,
        /// One row per group; each row is a flat list of
        /// `(name, bytes)` pairs that the renderer emits as
        /// a RESP array of bulk strings.
        rows: Vec<Vec<(String, Vec<u8>)>>,
    },
    /// FT.EXPLAIN textual query plan.
    Explain(String),
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
        b"FT.SEARCH" => parse_search(rest),
        b"FT.AGGREGATE" => parse_aggregate(rest).map(FtCommand::Aggregate),
        b"FT.EXPLAIN" => parse_explain(rest).map(FtCommand::Explain),
        b"FT.ALTER" => parse_alter(rest).map(FtCommand::Alter),
        b"FT.REGEX" => parse_regex(rest).map(FtCommand::Regex),
        b"FT.INFO" => parse_info(rest),
        b"FT.LIST" | b"FT._LIST" => parse_list(rest),
        b"FT.DROPINDEX" => parse_dropindex(rest),
        other => {
            // Unknown FT.* keywords surface with the
            // `not supported in this build` wording so the
            // dispatcher can use the same shared response
            // shape for genuinely-unimplemented commands.
            // Non-FT.* keywords still yield UnknownCommand;
            // the dispatcher only routes FT.* through here.
            if other.starts_with(b"FT.") {
                Err(FtError::Unsupported(
                    String::from_utf8_lossy(other).into_owned(),
                ))
            } else {
                Err(FtError::UnknownCommand(
                    String::from_utf8_lossy(other).into_owned(),
                ))
            }
        }
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
        FtCommand::SearchText(req) => execute_search_text(registry, &req),
        FtCommand::Aggregate(req) => execute_aggregate(registry, &req),
        FtCommand::Explain(req) => execute_explain(registry, &req),
        FtCommand::Alter(req) => execute_alter(registry, &req),
        FtCommand::Regex(req) => execute_regex(registry, &req),
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

/// Top-level FT.SEARCH parser. Routes between the k-NN form
/// (`*=>[KNN k @field $param]`) and the trigram-backed
/// substring form (`@field:substring`) based on the shape of
/// the query expression. The substring form is the
/// dyntext-powered text-search path; the k-NN form is the
/// dynvec ANN path.
fn parse_search(rest: &[&[u8]]) -> Result<FtCommand, FtError> {
    let mut it = TokenCursor::new(rest);
    let name = it.next_string("FT.SEARCH: missing index name")?;
    let query = it.next_required("FT.SEARCH: missing query expression")?;

    if let Some(parsed) = try_parse_text_field_query(query)? {
        let (field, substring) = parsed;
        let mut opts = SearchClauseOptions::default();
        consume_search_trailing_clauses(&mut it, false, &mut opts)?;
        return Ok(FtCommand::SearchText(SearchTextRequest {
            name,
            field,
            query: substring,
            return_fields: opts.return_fields,
            limit: opts.limit,
            sortby: opts.sortby,
            nocontent: opts.nocontent,
        }));
    }

    let (k, vec_field, param_name) = parse_knn_query(query)?;

    // Optional clauses: PARAMS plus the projection clauses
    // (RETURN / LIMIT / SORTBY / NOCONTENT / DIALECT).
    let mut params: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut opts = SearchClauseOptions::default();
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
            b"RETURN" => parse_return_clause(&mut it, &mut opts)?,
            b"SORTBY" => parse_sortby_clause(&mut it, &mut opts)?,
            b"LIMIT" => parse_limit_clause(&mut it, &mut opts)?,
            b"NOCONTENT" => opts.nocontent = true,
            b"DIALECT" => {
                it.next_required("FT.SEARCH: DIALECT expects a value")?;
            }
            b"WITHSCORES" => {
                // Tolerated as a no-op; this build always emits
                // the score under the implicit `__vec_score`
                // metadata field on each hit.
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
    Ok(FtCommand::Search(SearchRequest {
        name,
        k,
        vector_field: vec_field,
        vector_bytes,
        return_fields: opts.return_fields,
        limit: opts.limit,
        sortby: opts.sortby,
        nocontent: opts.nocontent,
    }))
}

/// Mutable accumulator for the `RETURN` / `LIMIT` / `SORTBY`
/// / `NOCONTENT` projection clauses common to both
/// `FT.SEARCH` shapes. Held inline by [`parse_search`] and
/// passed by reference to [`consume_search_trailing_clauses`]
/// so the substring path and the k-NN path stay in lockstep.
#[derive(Default)]
struct SearchClauseOptions {
    return_fields: Option<Vec<String>>,
    limit: Option<(usize, usize)>,
    sortby: Option<(String, SortDirection)>,
    nocontent: bool,
}

fn parse_return_clause(
    it: &mut TokenCursor<'_>,
    opts: &mut SearchClauseOptions,
) -> Result<(), FtError> {
    let n_tok = it.next_required("FT.SEARCH: RETURN expects a count")?;
    let n = parse_unsigned(n_tok, "FT.SEARCH RETURN count")?;
    let mut fields: Vec<String> = Vec::with_capacity(n);
    for _ in 0..n {
        let f_tok = it.next_required("FT.SEARCH: RETURN expects field name")?;
        // RediSearch lets clients prefix RETURN field names
        // with `@`. Treat both forms identically by stripping
        // the marker before storing.
        let trimmed: &[u8] = if f_tok.first() == Some(&b'@') {
            &f_tok[1..]
        } else {
            f_tok
        };
        fields.push(utf8(trimmed, "FT.SEARCH RETURN field name")?);
    }
    opts.return_fields = Some(fields);
    Ok(())
}

fn parse_limit_clause(
    it: &mut TokenCursor<'_>,
    opts: &mut SearchClauseOptions,
) -> Result<(), FtError> {
    let off_tok = it.next_required("FT.SEARCH: LIMIT expects offset")?;
    let cnt_tok = it.next_required("FT.SEARCH: LIMIT expects count")?;
    let off = parse_unsigned(off_tok, "FT.SEARCH LIMIT offset")?;
    let cnt = parse_unsigned(cnt_tok, "FT.SEARCH LIMIT count")?;
    opts.limit = Some((off, cnt));
    Ok(())
}

fn parse_sortby_clause(
    it: &mut TokenCursor<'_>,
    opts: &mut SearchClauseOptions,
) -> Result<(), FtError> {
    let f_tok = it.next_required("FT.SEARCH: SORTBY expects @field")?;
    let field_bytes: &[u8] = if f_tok.first() == Some(&b'@') {
        &f_tok[1..]
    } else {
        f_tok
    };
    let field = utf8(field_bytes, "FT.SEARCH SORTBY field")?;
    // The direction token is optional. Peek; if the next
    // token is ASC/DESC, consume it; otherwise default to
    // ASC and leave the cursor untouched so the caller's
    // outer loop can dispatch it.
    let direction = if let Some(next) = it.peek() {
        let up = ascii_upper(next);
        match up.as_slice() {
            b"ASC" => {
                it.advance();
                SortDirection::Asc
            }
            b"DESC" => {
                it.advance();
                SortDirection::Desc
            }
            _ => SortDirection::Asc,
        }
    } else {
        SortDirection::Asc
    };
    opts.sortby = Some((field, direction));
    Ok(())
}

/// Try to interpret the FT.SEARCH query expression as the
/// trigram-backed text form `@field:substring`. Returns
/// `Ok(Some((field, substring)))` when the shape matches,
/// `Ok(None)` to defer to the k-NN parser, and `Err` for
/// shapes that look like a text query but are malformed.
fn try_parse_text_field_query(query: &[u8]) -> Result<Option<(String, Vec<u8>)>, FtError> {
    if query.is_empty() || query[0] != b'@' {
        return Ok(None);
    }
    // The k-NN form `@title:foo=>[KNN k @vec $blob]` is not
    // implemented; surface the same `Unsupported` error the
    // legacy parser produced for that shape.
    if find_byte_subseq(query, b"=>").is_some() {
        return Err(FtError::Unsupported(format!(
            "FT.SEARCH query: {}",
            String::from_utf8_lossy(query)
        )));
    }
    let body = &query[1..];
    let colon = body
        .iter()
        .position(|&b| b == b':')
        .ok_or_else(|| FtError::Syntax("FT.SEARCH text query: missing ':'".to_string()))?;
    let field_bytes = &body[..colon];
    let substring = &body[colon + 1..];
    if field_bytes.is_empty() {
        return Err(FtError::Syntax(
            "FT.SEARCH text query: empty field name".to_string(),
        ));
    }
    let field = std::str::from_utf8(field_bytes)
        .map(str::to_string)
        .map_err(|_| FtError::Syntax("FT.SEARCH text query: field is not UTF-8".to_string()))?;
    Ok(Some((field, substring.to_vec())))
}

/// Find the first occurrence of `needle` in `haystack`, or
/// `None` when absent. Linear scan; the inputs are short
/// FT.SEARCH query expressions (a few dozen bytes at most).
fn find_byte_subseq(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Walk the trailing FT.SEARCH clauses (RETURN, LIMIT,
/// DIALECT, NOCONTENT, WITHSCORES) without consuming PARAMS,
/// which is k-NN-only. The flag controls whether PARAMS is
/// permitted; today the substring path forbids it because
/// the query body is already self-contained.
fn consume_search_trailing_clauses(
    it: &mut TokenCursor<'_>,
    allow_params: bool,
    opts: &mut SearchClauseOptions,
) -> Result<(), FtError> {
    loop {
        let Some(tok) = it.next() else { break };
        let up = ascii_upper(tok);
        match up.as_slice() {
            b"PARAMS" if allow_params => {
                let n_tok = it.next_required("FT.SEARCH: PARAMS expects a count")?;
                let n = parse_unsigned(n_tok, "FT.SEARCH PARAMS count")?;
                if !n.is_multiple_of(2) {
                    return Err(FtError::Syntax(
                        "FT.SEARCH PARAMS count must be even".to_string(),
                    ));
                }
                for _ in 0..n {
                    it.next_required("FT.SEARCH: PARAMS expects key/value pair")?;
                }
            }
            b"RETURN" => parse_return_clause(it, opts)?,
            b"SORTBY" => parse_sortby_clause(it, opts)?,
            b"LIMIT" => parse_limit_clause(it, opts)?,
            b"NOCONTENT" => opts.nocontent = true,
            b"DIALECT" => {
                it.next_required("FT.SEARCH: DIALECT expects a value")?;
            }
            b"WITHSCORES" => {}
            other => {
                return Err(FtError::Unsupported(format!(
                    "FT.SEARCH clause {}",
                    String::from_utf8_lossy(other)
                )));
            }
        }
    }
    Ok(())
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
    Ok(finalize_search_outcome(
        out,
        req.sortby.as_ref(),
        req.limit,
        req.return_fields.as_deref(),
        req.nocontent,
    ))
}

fn execute_search_text(
    registry: &VectorRegistry,
    req: &SearchTextRequest,
) -> Result<FtOutcome, FtError> {
    let table = registry
        .get(&req.name)
        .ok_or_else(|| FtError::NotFound(req.name.clone()))?;
    if !table.has_text_field(&req.field) {
        return Err(FtError::Syntax(format!(
            "FT.SEARCH: index {} has no TEXT field {}",
            req.name, req.field
        )));
    }
    let raw_hits = table
        .search_text_substring(&req.field, &req.query)
        .ok_or_else(|| {
            FtError::Engine(format!(
                "text index for field {} not provisioned",
                req.field
            ))
        })?;
    let mut out = Vec::with_capacity(raw_hits.len());
    for (key, text) in raw_hits {
        let mut fields: Vec<(String, Vec<u8>)> = vec![(req.field.clone(), text)];
        // Mirror any sortby field that the user named so the
        // ranking pass can find it on each hit. The trigram
        // search returns only the matched body; pull the
        // remaining metadata from the dynvec engine row when
        // SORTBY references a different field.
        if let Some((sort_field, _)) = &req.sortby {
            if sort_field != &req.field {
                if let Ok(Some(row)) = table.engine.get(&key) {
                    if let Some(v) = row.metadata.get(sort_field) {
                        let bytes = match v {
                            serde_json::Value::String(s) => s.clone().into_bytes(),
                            other => other.to_string().into_bytes(),
                        };
                        fields.push((sort_field.clone(), bytes));
                    }
                }
            }
        }
        out.push(SearchHit {
            doc_id: key,
            score: 0.0,
            fields,
        });
    }
    Ok(finalize_search_outcome(
        out,
        req.sortby.as_ref(),
        req.limit,
        req.return_fields.as_deref(),
        req.nocontent,
    ))
}

/// Apply the projection pipeline (`SORTBY` -> `LIMIT` ->
/// `RETURN` -> `NOCONTENT`) to a freshly-collected hit set
/// and pick the right [`FtOutcome`] variant.
///
/// `sortby` runs first because both `LIMIT` and `RETURN`
/// operate on the post-sort order. `LIMIT` then trims to the
/// (offset, count) window. `RETURN` projects each hit's
/// fields down to the requested subset. Finally, when
/// `nocontent` is set the field arrays are dropped entirely
/// and the renderer emits bare doc ids.
fn finalize_search_outcome(
    mut hits: Vec<SearchHit>,
    sortby: Option<&(String, SortDirection)>,
    limit: Option<(usize, usize)>,
    return_fields: Option<&[String]>,
    nocontent: bool,
) -> FtOutcome {
    if let Some((field, dir)) = sortby {
        sort_hits_by_field(&mut hits, field, *dir);
    }
    if let Some((offset, count)) = limit {
        if offset >= hits.len() {
            hits.clear();
        } else {
            let end = offset.saturating_add(count).min(hits.len());
            hits = hits.drain(offset..end).collect();
        }
    }
    if let Some(fields) = return_fields {
        for hit in &mut hits {
            hit.fields
                .retain(|(name, _)| fields.iter().any(|f| f == name));
        }
    }
    let total = hits.len();
    if nocontent {
        let doc_ids = hits.into_iter().map(|h| h.doc_id).collect();
        FtOutcome::SearchNoContent { total, doc_ids }
    } else {
        FtOutcome::Search { total, hits }
    }
}

/// Sort `hits` in place by the metadata field `field` in
/// direction `dir`. Hits that are missing the field sort
/// last (in either direction) so they do not pollute the
/// ordering of the hits that do declare it.
fn sort_hits_by_field(hits: &mut [SearchHit], field: &str, dir: SortDirection) {
    hits.sort_by(|a, b| {
        let av = lookup_field(a, field);
        let bv = lookup_field(b, field);
        // Push None to the back regardless of direction.
        match (av, bv) {
            (Some(a_bytes), Some(b_bytes)) => {
                match (parse_sort_key(a_bytes), parse_sort_key(b_bytes)) {
                    (Some(a_num), Some(b_num)) => match dir {
                        SortDirection::Asc => a_num
                            .partial_cmp(&b_num)
                            .unwrap_or(std::cmp::Ordering::Equal),
                        SortDirection::Desc => b_num
                            .partial_cmp(&a_num)
                            .unwrap_or(std::cmp::Ordering::Equal),
                    },
                    _ => match dir {
                        SortDirection::Asc => a_bytes.cmp(b_bytes),
                        SortDirection::Desc => b_bytes.cmp(a_bytes),
                    },
                }
            }
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
    });
}

fn lookup_field<'a>(hit: &'a SearchHit, name: &str) -> Option<&'a [u8]> {
    hit.fields
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.as_slice())
}

/// Try to parse a sort key as a finite f64. Used by
/// [`sort_hits_by_field`] so numeric fields sort by value
/// instead of by ASCII byte order.
fn parse_sort_key(bytes: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(bytes).ok()?;
    let parsed: f64 = s.trim().parse().ok()?;
    if parsed.is_finite() {
        Some(parsed)
    } else {
        None
    }
}

// ---- FT.AGGREGATE / FT.EXPLAIN / FT.ALTER ------------------------------

/// Parse `FT.AGGREGATE <idx> <query> GROUPBY <n> @field...`
/// `(REDUCE <kind> ...)+ [LIMIT <off> <cnt>]`. Other clauses
/// (`SORTBY`, `APPLY`, `FILTER`, `LOAD`, `WITHCURSOR`, ...) are
/// rejected with `not supported in this build` so the wire
/// surface keeps the brief's exit gate honest.
fn parse_aggregate(rest: &[&[u8]]) -> Result<AggregateRequest, FtError> {
    let mut it = TokenCursor::new(rest);
    let name = it.next_string("FT.AGGREGATE: missing index name")?;
    let _query = it.next_required("FT.AGGREGATE: missing query expression")?;

    let mut group_by: Vec<String> = Vec::new();
    let mut reducers: Vec<ReducerSpec> = Vec::new();
    let mut limit: Option<(usize, usize)> = None;
    let mut saw_groupby = false;

    while let Some(tok) = it.next() {
        let up = ascii_upper(tok);
        match up.as_slice() {
            b"GROUPBY" => {
                saw_groupby = true;
                parse_aggregate_groupby(&mut it, &mut group_by)?;
            }
            b"REDUCE" => {
                reducers.push(parse_aggregate_reduce(&mut it)?);
            }
            b"LIMIT" => {
                let off_tok = it.next_required("FT.AGGREGATE: LIMIT expects offset")?;
                let cnt_tok = it.next_required("FT.AGGREGATE: LIMIT expects count")?;
                let off = parse_unsigned(off_tok, "FT.AGGREGATE LIMIT offset")?;
                let cnt = parse_unsigned(cnt_tok, "FT.AGGREGATE LIMIT count")?;
                limit = Some((off, cnt));
            }
            other => {
                return Err(FtError::Unsupported(format!(
                    "FT.AGGREGATE clause {}",
                    String::from_utf8_lossy(other)
                )));
            }
        }
    }

    if !saw_groupby {
        return Err(FtError::Unsupported(
            "FT.AGGREGATE without GROUPBY".to_string(),
        ));
    }
    if reducers.is_empty() {
        return Err(FtError::Syntax(
            "FT.AGGREGATE: GROUPBY requires at least one REDUCE".to_string(),
        ));
    }
    Ok(AggregateRequest {
        name,
        group_by,
        reducers,
        limit,
    })
}

fn parse_aggregate_groupby(
    it: &mut TokenCursor<'_>,
    group_by: &mut Vec<String>,
) -> Result<(), FtError> {
    let n_tok = it.next_required("FT.AGGREGATE: GROUPBY expects a count")?;
    let n = parse_unsigned(n_tok, "FT.AGGREGATE GROUPBY count")?;
    if n == 0 {
        return Err(FtError::Syntax(
            "FT.AGGREGATE GROUPBY count must be > 0".to_string(),
        ));
    }
    for _ in 0..n {
        let f_tok = it.next_required("FT.AGGREGATE: GROUPBY expects @field")?;
        let bytes: &[u8] = if f_tok.first() == Some(&b'@') {
            &f_tok[1..]
        } else {
            f_tok
        };
        group_by.push(utf8(bytes, "FT.AGGREGATE GROUPBY field")?);
    }
    Ok(())
}

fn parse_aggregate_reduce(it: &mut TokenCursor<'_>) -> Result<ReducerSpec, FtError> {
    let kind_tok = it.next_required("FT.AGGREGATE: REDUCE expects a kind")?;
    let kind_up = ascii_upper(kind_tok);
    let arg_count_tok = it.next_required("FT.AGGREGATE: REDUCE expects an argument count")?;
    let arg_count = parse_unsigned(arg_count_tok, "FT.AGGREGATE REDUCE arg count")?;
    let kind = match kind_up.as_slice() {
        b"COUNT" => {
            if arg_count != 0 {
                return Err(FtError::Syntax(
                    "FT.AGGREGATE REDUCE COUNT expects 0 args".to_string(),
                ));
            }
            ReducerKind::Count
        }
        b"SUM" => {
            if arg_count != 1 {
                return Err(FtError::Syntax(
                    "FT.AGGREGATE REDUCE SUM expects 1 arg".to_string(),
                ));
            }
            ReducerKind::Sum {
                field: take_field_arg(it, "FT.AGGREGATE: SUM expects @field")?,
            }
        }
        b"AVG" => {
            if arg_count != 1 {
                return Err(FtError::Syntax(
                    "FT.AGGREGATE REDUCE AVG expects 1 arg".to_string(),
                ));
            }
            ReducerKind::Avg {
                field: take_field_arg(it, "FT.AGGREGATE: AVG expects @field")?,
            }
        }
        other => {
            // Skip the per-reducer arguments before surfacing
            // the error so the cursor stays well-formed for
            // any caller that wants to keep parsing after a
            // soft failure. Today we bail immediately so this
            // is just hygiene, but it keeps the helper usable
            // for future relaxations.
            for _ in 0..arg_count {
                let _ = it.next();
            }
            return Err(FtError::Unsupported(format!(
                "FT.AGGREGATE REDUCE {}",
                String::from_utf8_lossy(other)
            )));
        }
    };
    let as_tok = it.next_required("FT.AGGREGATE: REDUCE expects AS <name>")?;
    if !as_tok.eq_ignore_ascii_case(b"AS") {
        return Err(FtError::Syntax(
            "FT.AGGREGATE REDUCE clause missing AS".to_string(),
        ));
    }
    let alias = it.next_string("FT.AGGREGATE: REDUCE AS expects a name")?;
    Ok(ReducerSpec { kind, alias })
}

fn take_field_arg(it: &mut TokenCursor<'_>, msg: &str) -> Result<String, FtError> {
    let tok = it.next_required(msg)?;
    let bytes: &[u8] = if tok.first() == Some(&b'@') {
        &tok[1..]
    } else {
        tok
    };
    utf8(bytes, msg)
}

fn parse_explain(rest: &[&[u8]]) -> Result<ExplainRequest, FtError> {
    let mut it = TokenCursor::new(rest);
    let name = it.next_string("FT.EXPLAIN: missing index name")?;
    let query_tok = it.next_required("FT.EXPLAIN: missing query expression")?;
    // The DIALECT clause is the only trailing modifier we
    // tolerate today; consume its argument if present so
    // clients that always emit it stay green.
    while let Some(tok) = it.next() {
        let up = ascii_upper(tok);
        match up.as_slice() {
            b"DIALECT" => {
                it.next_required("FT.EXPLAIN: DIALECT expects a value")?;
            }
            other => {
                return Err(FtError::Unsupported(format!(
                    "FT.EXPLAIN clause {}",
                    String::from_utf8_lossy(other)
                )));
            }
        }
    }
    Ok(ExplainRequest {
        name,
        query: query_tok.to_vec(),
    })
}

fn parse_alter(rest: &[&[u8]]) -> Result<AlterRequest, FtError> {
    let mut it = TokenCursor::new(rest);
    let name = it.next_string("FT.ALTER: missing index name")?;
    let op_tok = it.next_required("FT.ALTER: expected ADD")?;
    let op_up = ascii_upper(op_tok);
    match op_up.as_slice() {
        b"ADD" => {}
        b"DROP" => {
            return Err(FtError::Unsupported("FT.ALTER DROP".to_string()));
        }
        b"SCHEMA" => {
            // FT.ALTER ... SCHEMA ADD <field> <type>...; the
            // SCHEMA-prefixed form is RediSearch 2.x syntax
            // and not implemented here, even when the inner
            // operation is ADD.
            return Err(FtError::Unsupported("FT.ALTER SCHEMA".to_string()));
        }
        other => {
            return Err(FtError::Syntax(format!(
                "FT.ALTER: expected ADD, got {}",
                String::from_utf8_lossy(other)
            )));
        }
    }
    let field = it.next_string("FT.ALTER ADD: missing field name")?;
    let type_tok = it.next_required("FT.ALTER ADD: missing field type")?;
    let type_up = ascii_upper(type_tok);
    let field_type = match type_up.as_slice() {
        b"TEXT" => MetadataFieldType::Text,
        b"TAG" => MetadataFieldType::Tag,
        b"VECTOR" => {
            return Err(FtError::Unsupported(
                "FT.ALTER ADD VECTOR (rebuild required)".to_string(),
            ));
        }
        b"NUMERIC" | b"GEO" => {
            return Err(FtError::Unsupported(format!(
                "FT.ALTER ADD {}",
                String::from_utf8_lossy(type_tok)
            )));
        }
        other => {
            return Err(FtError::Syntax(format!(
                "FT.ALTER ADD: unknown type {}",
                String::from_utf8_lossy(other)
            )));
        }
    };
    if it.peek().is_some() {
        return Err(FtError::Syntax(
            "FT.ALTER ADD: unexpected trailing tokens".to_string(),
        ));
    }
    Ok(AlterRequest {
        name,
        field,
        field_type,
    })
}

fn execute_aggregate(
    registry: &VectorRegistry,
    req: &AggregateRequest,
) -> Result<FtOutcome, FtError> {
    let table = registry
        .get(&req.name)
        .ok_or_else(|| FtError::NotFound(req.name.clone()))?;
    // Walk every key the FT.* surface absorbed via HSET
    // interception. dynvec's storage layer does not expose
    // a public iterator over rows, but the registry tracks
    // every key it indexed in `indexed_keys`, so we re-fetch
    // the row via `engine.get` for each one. The set is
    // typically small (matches the number of HSETs that
    // landed against the index's prefixes); the cost is
    // dominated by the metadata fan-out, not the lookup.
    let mut groups: BTreeMap<Vec<u8>, GroupAccumulator> = BTreeMap::new();
    for key in table.indexed_keys() {
        let row = match table.engine.get(&key) {
            Ok(Some(r)) => r,
            Ok(None) => continue,
            Err(e) => return Err(FtError::Engine(e.to_string())),
        };
        let group_key = build_group_key(&req.group_by, &row.metadata);
        let entry = groups.entry(group_key).or_insert_with(|| {
            GroupAccumulator::new(
                req.group_by
                    .iter()
                    .map(|f| {
                        (
                            f.clone(),
                            metadata_string(row.metadata.get(f)).unwrap_or_default(),
                        )
                    })
                    .collect(),
                req.reducers.len(),
            )
        });
        entry.observe(&req.reducers, &row.metadata);
    }

    // Emit groups in deterministic alphabetical order on
    // the composite group key so paginated callers see a
    // stable cursor without us having to expose one.
    let mut rows: Vec<Vec<(String, Vec<u8>)>> = Vec::with_capacity(groups.len());
    for (_, mut group) in groups {
        let mut row: Vec<(String, Vec<u8>)> = std::mem::take(&mut group.fields);
        for (i, reducer) in req.reducers.iter().enumerate() {
            let value = group.render_reducer(i, reducer);
            row.push((reducer.alias.clone(), value));
        }
        rows.push(row);
    }

    if let Some((offset, count)) = req.limit {
        if offset >= rows.len() {
            rows.clear();
        } else {
            let end = offset.saturating_add(count).min(rows.len());
            rows = rows.drain(offset..end).collect();
        }
    }

    let total_groups = rows.len();
    Ok(FtOutcome::Aggregate { total_groups, rows })
}

/// Per-group accumulator state: the raw `(name, value)`
/// pairs that identify the group, plus one parallel slot
/// per reducer to track count + running sum.
struct GroupAccumulator {
    fields: Vec<(String, Vec<u8>)>,
    slots: Vec<ReducerAccum>,
}

#[derive(Default)]
struct ReducerAccum {
    count: u64,
    sum: f64,
    /// True when at least one observed value parsed as a
    /// finite number; for non-numeric SUM/AVG fields this
    /// stays false and the renderer emits `0`.
    saw_numeric: bool,
}

impl GroupAccumulator {
    fn new(fields: Vec<(String, Vec<u8>)>, n_reducers: usize) -> Self {
        let mut slots = Vec::with_capacity(n_reducers);
        for _ in 0..n_reducers {
            slots.push(ReducerAccum::default());
        }
        Self { fields, slots }
    }

    fn observe(&mut self, reducers: &[ReducerSpec], metadata: &HashMap<String, serde_json::Value>) {
        for (i, reducer) in reducers.iter().enumerate() {
            let slot = &mut self.slots[i];
            slot.count = slot.count.saturating_add(1);
            match &reducer.kind {
                ReducerKind::Count => {}
                ReducerKind::Sum { field } | ReducerKind::Avg { field } => {
                    if let Some(v) = metadata.get(field) {
                        if let Some(n) = metadata_number(v) {
                            slot.sum += n;
                            slot.saw_numeric = true;
                        }
                    }
                }
            }
        }
    }

    fn render_reducer(&self, i: usize, reducer: &ReducerSpec) -> Vec<u8> {
        let slot = &self.slots[i];
        match &reducer.kind {
            ReducerKind::Count => slot.count.to_string().into_bytes(),
            ReducerKind::Sum { .. } => {
                if slot.saw_numeric {
                    format_float_f64(slot.sum).into_bytes()
                } else {
                    b"0".to_vec()
                }
            }
            ReducerKind::Avg { .. } => {
                if slot.saw_numeric && slot.count > 0 {
                    // `f64::from(u32)` is lossless, so trim
                    // the count to u32 before the cast.
                    // `saturating_as_f64` is not available
                    // until Rust 1.87, so we round-trip
                    // through `u32::try_from` to preserve
                    // precision; values larger than `u32::MAX`
                    // saturate at `u32::MAX` for the divisor,
                    // which keeps the mean within float
                    // resolution for sane group sizes.
                    let denom = u32::try_from(slot.count).unwrap_or(u32::MAX);
                    let mean = slot.sum / f64::from(denom);
                    format_float_f64(mean).into_bytes()
                } else {
                    b"0".to_vec()
                }
            }
        }
    }
}

fn build_group_key(group_by: &[String], metadata: &HashMap<String, serde_json::Value>) -> Vec<u8> {
    let mut key: Vec<u8> = Vec::new();
    for field in group_by {
        let v = metadata_string(metadata.get(field)).unwrap_or_default();
        // `\x1f` (Unit Separator) is a control byte that
        // never appears in field names or in stringified
        // numeric/JSON metadata, so it makes a safe
        // composite-key delimiter without escaping.
        if !key.is_empty() {
            key.push(0x1f);
        }
        key.extend_from_slice(&v);
    }
    key
}

fn metadata_string(value: Option<&serde_json::Value>) -> Option<Vec<u8>> {
    match value? {
        serde_json::Value::String(s) => Some(s.clone().into_bytes()),
        other => Some(other.to_string().into_bytes()),
    }
}

fn metadata_number(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn execute_explain(registry: &VectorRegistry, req: &ExplainRequest) -> Result<FtOutcome, FtError> {
    let table = registry
        .get(&req.name)
        .ok_or_else(|| FtError::NotFound(req.name.clone()))?;
    // The brief asks for "a textual representation of how the
    // query would be planned". We classify the query into one
    // of three buckets and emit a stable, multi-line plan.
    // Stable wording lets clients regex-test the output.
    let plan = if let Some((field, substring)) = try_parse_text_field_query(&req.query)? {
        // Text substring path: renders the trigram set we'd
        // probe and the bloom-filter lane.
        let trigrams = trigram_preview(&substring);
        format!(
            "@{field}: SUBSTRING\n  field: {field}\n  query: {query}\n  index: trigram+bloom\n  trigrams: {trigrams}\n",
            field = field,
            query = String::from_utf8_lossy(&substring),
            trigrams = trigrams,
        )
    } else if let Ok((k, vec_field, param_name)) = parse_knn_query(&req.query) {
        let dim = table.schema.dim;
        let metric = format!("{:?}", table.schema.distance).to_uppercase();
        let alg = format!("{:?}", table.schema.algorithm).to_uppercase();
        format!(
            "VECTOR KNN\n  index: {idx}\n  algorithm: {alg}\n  metric: {metric}\n  field: {field}\n  k: {k}\n  dim: {dim}\n  param: ${param}\n",
            idx = req.name,
            alg = alg,
            metric = metric,
            field = vec_field,
            k = k,
            dim = dim,
            param = param_name,
        )
    } else {
        // Anything else (e.g. `*` or a free-form expression
        // that neither parser accepts) lands here. Surface a
        // best-effort plan so callers do not get a wire
        // error for a debug-only command.
        format!(
            "UNKNOWN QUERY\n  index: {idx}\n  query: {q}\n  note: only @field:substring and *=>[KNN ...] are planned in this build\n",
            idx = req.name,
            q = String::from_utf8_lossy(&req.query),
        )
    };
    Ok(FtOutcome::Explain(plan))
}

/// Render the trigram set [`dyntext::TextIndex::search_substring`]
/// would probe for a query body. Used by FT.EXPLAIN to expose
/// the planner's view of a substring query without running it.
fn trigram_preview(query: &[u8]) -> String {
    if query.len() < 3 {
        return "<short-circuit: scan>".to_string();
    }
    let mut parts: Vec<String> = Vec::new();
    for window in query.windows(3).take(8) {
        parts.push(format!("\"{}\"", String::from_utf8_lossy(window)));
    }
    let suffix = if query.len() > 8 + 3 - 1 { ", ..." } else { "" };
    format!("[{}{}]", parts.join(", "), suffix)
}

fn execute_alter(registry: &VectorRegistry, req: &AlterRequest) -> Result<FtOutcome, FtError> {
    let table = registry
        .get(&req.name)
        .ok_or_else(|| FtError::NotFound(req.name.clone()))?;
    match req.field_type {
        MetadataFieldType::Text => {
            // Provision a per-field trigram index. Subsequent
            // HSETs that touch this field will be picked up
            // by `maybe_index_hset` because
            // `has_text_field` consults the runtime
            // text_indexes map alongside the frozen schema.
            let _new = table.add_text_field(&req.field);
            Ok(FtOutcome::Ok)
        }
        MetadataFieldType::Tag => {
            // TAG fields land in the row metadata blob without
            // a dedicated index; the registry has no separate
            // per-tag state to provision, so the schema-level
            // record is sufficient. We still acknowledge the
            // ADD so clients can keep their schema scripts
            // declarative.
            Ok(FtOutcome::Ok)
        }
        MetadataFieldType::Numeric | MetadataFieldType::Geo => {
            // Reachable only via the parser's sealed table; if
            // a future dialect lands a NUMERIC/GEO branch the
            // sealed match in `parse_alter` already rejects it
            // before we get here. Defensive guard.
            Err(FtError::Unsupported(format!(
                "FT.ALTER ADD type {:?}",
                req.field_type
            )))
        }
    }
}

fn format_float_f64(f: f64) -> String {
    // Shortest round-trip that still parses as a float when
    // the caller pipes the bulk string into `f64::parse`.
    // 6 fractional digits matches the existing
    // [`format_float`] used for vector scores.
    format!("{f:.6}")
}

// ---- FT.REGEX -----------------------------------------------------------

/// Parse `FT.REGEX <idx> <field> <pattern> [K=<n>]`.
///
/// `K=` is the only recognised option today; an unknown
/// option surfaces as [`FtError::Unsupported`]. The `n` value
/// must fit into a [`u16`]; pragmatically the TRE engine
/// rejects anything beyond the pattern length anyway, so the
/// upper bound here is purely a parser-level guard rail.
fn parse_regex(rest: &[&[u8]]) -> Result<RegexRequest, FtError> {
    let mut it = TokenCursor::new(rest);
    let name = it.next_string("FT.REGEX: missing index name")?;
    let field = it.next_string("FT.REGEX: missing field name")?;
    let pattern_tok = it.next_required("FT.REGEX: missing pattern")?;
    let pattern = std::str::from_utf8(pattern_tok)
        .map(str::to_string)
        .map_err(|_| FtError::Syntax("FT.REGEX: pattern is not UTF-8".to_string()))?;
    let mut max_errors: u16 = 0;
    for tok in &mut it {
        let up = ascii_upper(tok);
        if let Some(rest) = up.strip_prefix(b"K=") {
            let s = std::str::from_utf8(rest)
                .map_err(|_| FtError::Syntax("FT.REGEX: K= value is not UTF-8".to_string()))?;
            let n: u32 = s
                .parse()
                .map_err(|_| FtError::Syntax(format!("FT.REGEX: invalid K= value {s}")))?;
            max_errors = u16::try_from(n)
                .map_err(|_| FtError::Syntax(format!("FT.REGEX: K= value {n} exceeds u16")))?;
        } else {
            return Err(FtError::Unsupported(format!(
                "FT.REGEX option {}",
                String::from_utf8_lossy(tok)
            )));
        }
    }
    Ok(RegexRequest {
        name,
        field,
        pattern,
        max_errors,
    })
}

fn execute_regex(registry: &VectorRegistry, req: &RegexRequest) -> Result<FtOutcome, FtError> {
    let table = registry
        .get(&req.name)
        .ok_or_else(|| FtError::NotFound(req.name.clone()))?;
    if !table.has_text_field(&req.field) {
        return Err(FtError::Syntax(format!(
            "FT.REGEX: index {} has no TEXT field {}",
            req.name, req.field
        )));
    }
    let raw_hits = if req.max_errors == 0 {
        table
            .search_text_regex(&req.field, &req.pattern)
            .ok_or_else(|| {
                FtError::Engine(format!(
                    "text index for field {} not provisioned",
                    req.field
                ))
            })?
            .map_err(|e| FtError::Syntax(format!("FT.REGEX: invalid pattern: {e}")))?
    } else {
        table
            .search_text_regex_approx(&req.field, &req.pattern, req.max_errors)
            .ok_or_else(|| {
                FtError::Engine(format!(
                    "text index for field {} not provisioned",
                    req.field
                ))
            })?
            .map_err(|e| FtError::Engine(format!("FT.REGEX (approximate): {e}")))?
    };
    let mut out = Vec::with_capacity(raw_hits.len());
    for (key, text) in raw_hits {
        out.push(SearchHit {
            doc_id: key,
            score: 0.0,
            fields: vec![(req.field.clone(), text)],
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
    let mut text_writes: Vec<(String, Vec<u8>)> = Vec::new();
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
            // Mirror the value into the trigram index when the
            // schema declares this field as TEXT-indexed. The
            // dynvec metadata copy stays so FT.SEARCH KNN /
            // FT.INFO can echo the field; the dyntext copy is
            // what powers @field:substring and FT.REGEX.
            if table.has_text_field(field_str) {
                text_writes.push((field_str.to_string(), value.to_vec()));
            }
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
    for (field, bytes) in text_writes {
        table.upsert_text_field(&field, key, &bytes);
    }
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
        FtOutcome::SearchNoContent { total, doc_ids } => {
            // NOCONTENT layout: [total, doc_id_1, doc_id_2, ...].
            let total_i64 = i64::try_from(*total).unwrap_or(i64::MAX);
            write_array_header(&mut out, 1 + doc_ids.len());
            write_integer(&mut out, total_i64);
            for id in doc_ids {
                write_bulk(&mut out, id);
            }
        }
        FtOutcome::Aggregate { total_groups, rows } => {
            // FT.AGGREGATE layout (RediSearch shape):
            //   [total_groups, [k, v, k, v, ...], ...].
            let total_i64 = i64::try_from(*total_groups).unwrap_or(i64::MAX);
            write_array_header(&mut out, 1 + rows.len());
            write_integer(&mut out, total_i64);
            for row in rows {
                write_array_header(&mut out, row.len() * 2);
                for (name, value) in row {
                    write_bulk(&mut out, name.as_bytes());
                    write_bulk(&mut out, value);
                }
            }
        }
        FtOutcome::Explain(plan) => {
            write_bulk(&mut out, plan.as_bytes());
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
