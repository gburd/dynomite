//! `dyn-admin bucket-props get|set` -- inspect and update a bucket's
//! [`dyn_riak::proto::pb::RpbBucketProps`].
//!
//! Two subcommands:
//!
//! * `get <bucket>` issues an `RpbGetBucketReq` and pretty-prints the
//!   returned property bag. The default rendering is a `key: value`
//!   list; `--json` emits a single JSON object equivalent to
//!   `riak-admin bucket-type status <type> --json`.
//! * `set <bucket>` reads the bucket's current properties first, then
//!   overlays the values from the CLI flags and submits a
//!   `RpbSetBucketReq`. Fields the operator does not name are left
//!   at the bucket's existing value: the wire is a partial update
//!   exactly the way Riak documents.
//!
//! Quorum flags (`--read-consistency`, `--write-consistency`) accept
//! the symbolic values Riak ships out of the box (`one`, `quorum`,
//! `all`, `default`) plus a literal integer. The symbolic names map
//! to the magic uint32 values Riak uses on the wire so a Riak
//! client and `dyn-admin` agree on the semantics.

use std::fmt;
use std::io::Write;
use std::str::FromStr;

use dyn_riak::proto::pb::{
    MessageCode, RpbBucketProps, RpbGetBucketReq, RpbGetBucketResp, RpbSetBucketReq,
    RpbSetBucketResp, CHASH_KEYFUN_BUCKETONLY, CHASH_KEYFUN_CUSTOM, CHASH_KEYFUN_STD,
    REPLICATION_STRATEGY_SUCCESSORS, REPLICATION_STRATEGY_TOPOLOGY,
};
use serde::Serialize;

use crate::client::PbcClient;
use crate::error::AdminError;
use crate::output::{write_json, OutputFormat};

/// Riak-compatible "default" magic quorum value (use the bucket's
/// stored value).
pub const QUORUM_DEFAULT: u32 = u32::MAX - 4;
/// Riak-compatible "one" magic quorum value (any single replica).
pub const QUORUM_ONE: u32 = u32::MAX - 3;
/// Riak-compatible "quorum" magic value (`floor(n/2)+1`).
pub const QUORUM_QUORUM: u32 = u32::MAX - 2;
/// Riak-compatible "all" magic value (every replica).
pub const QUORUM_ALL: u32 = u32::MAX - 1;
/// "Unset" sentinel; never sent on the wire.
pub const QUORUM_UNSET: u32 = u32::MAX;

/// CLI shape for the keyfun selector.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyFunArg {
    /// Hash `<bucket>/<key>` (Riak's default).
    Std,
    /// Hash `<bucket>` only; every key in the bucket maps to the
    /// same partition.
    BucketOnly,
}

impl KeyFunArg {
    /// Encode as the wire selector consumed by
    /// [`RpbBucketProps::chash_keyfun`].
    #[must_use]
    pub fn to_wire(self) -> u32 {
        match self {
            Self::Std => CHASH_KEYFUN_STD,
            Self::BucketOnly => CHASH_KEYFUN_BUCKETONLY,
        }
    }
}

impl FromStr for KeyFunArg {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "std" => Ok(Self::Std),
            "bucketonly" | "bucket-only" | "bucket_only" => Ok(Self::BucketOnly),
            other => Err(format!(
                "unknown keyfun: {other:?} (expected one of: std, bucketonly)"
            )),
        }
    }
}

/// CLI shape for the replication-strategy selector.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationStrategyArg {
    /// Dynomite's per-DC, per-rack quorum fan-out.
    Topology,
    /// Riak-style walk-N-successors on the token ring.
    Successors,
}

impl ReplicationStrategyArg {
    /// Encode as the wire selector consumed by
    /// [`RpbBucketProps::replication_strategy`].
    #[must_use]
    pub fn to_wire(self) -> u32 {
        match self {
            Self::Topology => REPLICATION_STRATEGY_TOPOLOGY,
            Self::Successors => REPLICATION_STRATEGY_SUCCESSORS,
        }
    }
}

impl FromStr for ReplicationStrategyArg {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "topology" => Ok(Self::Topology),
            "successors" => Ok(Self::Successors),
            other => Err(format!(
                "unknown replication-strategy: {other:?} \
                 (expected one of: topology, successors)"
            )),
        }
    }
}

/// CLI shape for an `r`/`w` quorum knob. Accepts either a symbolic
/// Riak name or a literal non-negative integer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsistencyArg(u32);

impl ConsistencyArg {
    /// Wire value carried in `RpbBucketProps::r`/`w`.
    #[must_use]
    pub fn to_wire(self) -> u32 {
        self.0
    }
}

impl fmt::Display for ConsistencyArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            QUORUM_DEFAULT => f.write_str("default"),
            QUORUM_ONE => f.write_str("one"),
            QUORUM_QUORUM => f.write_str("quorum"),
            QUORUM_ALL => f.write_str("all"),
            QUORUM_UNSET => f.write_str("unset"),
            n => write!(f, "{n}"),
        }
    }
}

impl FromStr for ConsistencyArg {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "default" => Ok(Self(QUORUM_DEFAULT)),
            "one" => Ok(Self(QUORUM_ONE)),
            "quorum" => Ok(Self(QUORUM_QUORUM)),
            "all" => Ok(Self(QUORUM_ALL)),
            other => other
                .parse::<u32>()
                .map(Self)
                .map_err(|_| format!("invalid consistency: {other:?}")),
        }
    }
}

/// Decode a wire `r`/`w` value as a printable label. Magic values
/// surface as their Riak name; everything else as a decimal.
#[must_use]
pub fn render_quorum(v: u32) -> String {
    match v {
        QUORUM_DEFAULT => "default".into(),
        QUORUM_ONE => "one".into(),
        QUORUM_QUORUM => "quorum".into(),
        QUORUM_ALL => "all".into(),
        QUORUM_UNSET => "unset".into(),
        n => n.to_string(),
    }
}

/// Decode a wire `chash_keyfun` selector to a printable label.
#[must_use]
pub fn render_keyfun(v: u32) -> &'static str {
    match v {
        CHASH_KEYFUN_STD => "std",
        CHASH_KEYFUN_BUCKETONLY => "bucketonly",
        CHASH_KEYFUN_CUSTOM => "custom",
        _ => "unknown",
    }
}

/// Decode a wire `replication_strategy` selector to a printable
/// label.
#[must_use]
pub fn render_replication_strategy(v: u32) -> &'static str {
    match v {
        REPLICATION_STRATEGY_TOPOLOGY => "topology",
        REPLICATION_STRATEGY_SUCCESSORS => "successors",
        _ => "unknown",
    }
}

/// Caller-supplied overrides forwarded by `dyn-admin bucket-props
/// set`. Every field is `None` when the operator did not name it on
/// the command line; the run loop merges this struct on top of the
/// bucket's current properties before sending the update.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SetOptions {
    /// `--n-val`.
    pub n_val: Option<u32>,
    /// `--read-consistency`.
    pub read: Option<ConsistencyArg>,
    /// `--write-consistency`.
    pub write: Option<ConsistencyArg>,
    /// `--keyfun`.
    pub keyfun: Option<KeyFunArg>,
    /// `--replication-strategy`.
    pub replication_strategy: Option<ReplicationStrategyArg>,
}

impl SetOptions {
    /// Apply the overrides on top of `base` and return the result.
    /// `base` is the bucket's current props (as fetched via
    /// `RpbGetBucketReq`); only the fields the operator named are
    /// modified.
    #[must_use]
    pub fn overlay(&self, base: &RpbBucketProps) -> RpbBucketProps {
        let mut out = base.clone();
        if let Some(n) = self.n_val {
            out.n_val = Some(n);
        }
        if let Some(r) = self.read {
            out.r = Some(r.to_wire());
        }
        if let Some(w) = self.write {
            out.w = Some(w.to_wire());
        }
        if let Some(k) = self.keyfun {
            out.chash_keyfun = Some(k.to_wire());
        }
        if let Some(s) = self.replication_strategy {
            out.replication_strategy = Some(s.to_wire());
        }
        out
    }

    /// Whether at least one override was supplied.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.n_val.is_none()
            && self.read.is_none()
            && self.write.is_none()
            && self.keyfun.is_none()
            && self.replication_strategy.is_none()
    }
}

/// JSON-friendly view of `RpbBucketProps`. Only fields with a value
/// are serialised so a `riak-admin`-style consumer sees a stable
/// shape.
#[derive(Clone, Debug, Default, Serialize)]
pub struct BucketPropsView {
    /// Replication factor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n_val: Option<u32>,
    /// Allow concurrent siblings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_mult: Option<bool>,
    /// Last-write-wins resolution.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_write_wins: Option<bool>,
    /// Default replica-read quorum (Riak `r`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r: Option<String>,
    /// Default replica-write quorum (Riak `w`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub w: Option<String>,
    /// Default primary-read quorum (Riak `pr`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pr: Option<String>,
    /// Default primary-write quorum (Riak `pw`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pw: Option<String>,
    /// Default durable-write quorum (Riak `dw`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dw: Option<String>,
    /// Default replica-write-tombstone quorum (Riak `rw`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rw: Option<String>,
    /// Hash-key function (`std` / `bucketonly` / `custom`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keyfun: Option<&'static str>,
    /// Replication strategy (`topology` / `successors`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_strategy: Option<&'static str>,
    /// Strong-consistency mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistent: Option<bool>,
    /// Write-once mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_once: Option<bool>,
}

impl BucketPropsView {
    /// Project an `RpbBucketProps` into a printable view.
    #[must_use]
    pub fn from_pb(p: &RpbBucketProps) -> Self {
        Self {
            n_val: p.n_val,
            allow_mult: p.allow_mult,
            last_write_wins: p.last_write_wins,
            r: p.r.map(render_quorum),
            w: p.w.map(render_quorum),
            pr: p.pr.map(render_quorum),
            pw: p.pw.map(render_quorum),
            dw: p.dw.map(render_quorum),
            rw: p.rw.map(render_quorum),
            keyfun: p.chash_keyfun.map(render_keyfun),
            replication_strategy: p.replication_strategy.map(render_replication_strategy),
            consistent: p.consistent,
            write_once: p.write_once,
        }
    }
}

/// Result envelope emitted by `dyn-admin bucket-props get`.
#[derive(Clone, Debug, Serialize)]
pub struct BucketPropsReport {
    /// `host:port` the request was sent to.
    pub node: String,
    /// Bucket name.
    pub bucket: String,
    /// Property snapshot.
    pub props: BucketPropsView,
}

/// Run the `bucket-props get` subcommand.
///
/// # Errors
///
/// Surfaces every wire-level or server-side failure as an
/// [`AdminError`].
pub async fn run_get<W: Write>(
    node: &str,
    bucket: &str,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    let mut client = PbcClient::connect(node).await?;
    let report = fetch_props(&mut client, node, bucket).await?;
    render_get(&report, fmt, out)
}

/// Run the `bucket-props set` subcommand.
///
/// The bucket's current properties are fetched first, the operator's
/// flags are overlaid, and the resulting union is shipped back as
/// `RpbSetBucketReq`. After the update the post-set props are
/// re-fetched and printed so the operator can confirm the change
/// landed.
///
/// # Errors
///
/// Surfaces every wire-level or server-side failure as an
/// [`AdminError`]. An empty `--*` set is rejected up-front to prevent
/// a no-op round-trip.
pub async fn run_set<W: Write>(
    node: &str,
    bucket: &str,
    opts: &SetOptions,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    if opts.is_empty() {
        return Err(AdminError::Protocol(
            "bucket-props set requires at least one of \
             --n-val/--read-consistency/--write-consistency/\
             --keyfun/--replication-strategy"
                .into(),
        ));
    }
    let mut client = PbcClient::connect(node).await?;
    let current = fetch_props_pb(&mut client, bucket).await?;
    let merged = opts.overlay(&current);
    let set_req = RpbSetBucketReq {
        bucket: bucket.as_bytes().to_vec(),
        props: Some(merged),
        r#type: None,
    };
    let _: RpbSetBucketResp = client
        .call(
            MessageCode::SetBucketReq,
            MessageCode::SetBucketResp,
            &set_req,
        )
        .await?;
    let report = fetch_props(&mut client, node, bucket).await?;
    render_set(&report, fmt, out)
}

/// Helper: fetch and shape the bucket's current properties.
async fn fetch_props(
    client: &mut PbcClient,
    node: &str,
    bucket: &str,
) -> Result<BucketPropsReport, AdminError> {
    let pb = fetch_props_pb(client, bucket).await?;
    Ok(BucketPropsReport {
        node: node.to_string(),
        bucket: bucket.to_string(),
        props: BucketPropsView::from_pb(&pb),
    })
}

/// Helper: fetch the bucket's current `RpbBucketProps`.
async fn fetch_props_pb(
    client: &mut PbcClient,
    bucket: &str,
) -> Result<RpbBucketProps, AdminError> {
    let req = RpbGetBucketReq {
        bucket: bucket.as_bytes().to_vec(),
        r#type: None,
    };
    let resp: RpbGetBucketResp = client
        .call(MessageCode::GetBucketReq, MessageCode::GetBucketResp, &req)
        .await?;
    resp.props
        .ok_or_else(|| AdminError::Protocol("get-bucket response missing props".into()))
}

fn render_get<W: Write>(
    report: &BucketPropsReport,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    match fmt {
        OutputFormat::Json => write_json(out, report)?,
        OutputFormat::Human => {
            writeln!(
                out,
                "Bucket properties for {} via {}",
                report.bucket, report.node
            )?;
            render_view_human(&report.props, "  ", out)?;
            writeln!(out)?;
        }
    }
    Ok(())
}

fn render_set<W: Write>(
    report: &BucketPropsReport,
    fmt: OutputFormat,
    out: &mut W,
) -> Result<(), AdminError> {
    match fmt {
        OutputFormat::Json => write_json(out, report)?,
        OutputFormat::Human => {
            writeln!(
                out,
                "Updated bucket properties for {} via {}",
                report.bucket, report.node
            )?;
            render_view_human(&report.props, "  ", out)?;
            writeln!(out)?;
        }
    }
    Ok(())
}

fn render_view_human<W: Write>(
    view: &BucketPropsView,
    prefix: &str,
    out: &mut W,
) -> Result<(), AdminError> {
    if let Some(n) = view.n_val {
        writeln!(out, "{prefix}n_val: {n}")?;
    }
    if let Some(b) = view.allow_mult {
        writeln!(out, "{prefix}allow_mult: {b}")?;
    }
    if let Some(b) = view.last_write_wins {
        writeln!(out, "{prefix}last_write_wins: {b}")?;
    }
    if let Some(s) = view.r.as_deref() {
        writeln!(out, "{prefix}r: {s}")?;
    }
    if let Some(s) = view.w.as_deref() {
        writeln!(out, "{prefix}w: {s}")?;
    }
    if let Some(s) = view.pr.as_deref() {
        writeln!(out, "{prefix}pr: {s}")?;
    }
    if let Some(s) = view.pw.as_deref() {
        writeln!(out, "{prefix}pw: {s}")?;
    }
    if let Some(s) = view.dw.as_deref() {
        writeln!(out, "{prefix}dw: {s}")?;
    }
    if let Some(s) = view.rw.as_deref() {
        writeln!(out, "{prefix}rw: {s}")?;
    }
    if let Some(s) = view.keyfun {
        writeln!(out, "{prefix}keyfun: {s}")?;
    }
    if let Some(s) = view.replication_strategy {
        writeln!(out, "{prefix}replication_strategy: {s}")?;
    }
    if let Some(b) = view.consistent {
        writeln!(out, "{prefix}consistent: {b}")?;
    }
    if let Some(b) = view.write_once {
        writeln!(out, "{prefix}write_once: {b}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyfun_arg_parses_canonical_names() {
        assert_eq!(KeyFunArg::from_str("std").unwrap(), KeyFunArg::Std);
        assert_eq!(
            KeyFunArg::from_str("bucketonly").unwrap(),
            KeyFunArg::BucketOnly
        );
        assert_eq!(
            KeyFunArg::from_str("BUCKET-ONLY").unwrap(),
            KeyFunArg::BucketOnly
        );
        assert!(KeyFunArg::from_str("nope").is_err());
    }

    #[test]
    fn keyfun_arg_to_wire_round_trips() {
        assert_eq!(KeyFunArg::Std.to_wire(), CHASH_KEYFUN_STD);
        assert_eq!(KeyFunArg::BucketOnly.to_wire(), CHASH_KEYFUN_BUCKETONLY);
    }

    #[test]
    fn replication_strategy_arg_parses_canonical_names() {
        assert_eq!(
            ReplicationStrategyArg::from_str("Topology").unwrap(),
            ReplicationStrategyArg::Topology
        );
        assert_eq!(
            ReplicationStrategyArg::from_str("successors").unwrap(),
            ReplicationStrategyArg::Successors
        );
        assert!(ReplicationStrategyArg::from_str("eventual").is_err());
    }

    #[test]
    fn consistency_arg_parses_symbolic_and_numeric() {
        assert_eq!(
            ConsistencyArg::from_str("one").unwrap().to_wire(),
            QUORUM_ONE
        );
        assert_eq!(
            ConsistencyArg::from_str("quorum").unwrap().to_wire(),
            QUORUM_QUORUM
        );
        assert_eq!(
            ConsistencyArg::from_str("all").unwrap().to_wire(),
            QUORUM_ALL
        );
        assert_eq!(
            ConsistencyArg::from_str("default").unwrap().to_wire(),
            QUORUM_DEFAULT
        );
        assert_eq!(ConsistencyArg::from_str("3").unwrap().to_wire(), 3);
        assert!(ConsistencyArg::from_str("nope").is_err());
    }

    #[test]
    fn render_quorum_decodes_known_magics() {
        assert_eq!(render_quorum(QUORUM_ONE), "one");
        assert_eq!(render_quorum(QUORUM_QUORUM), "quorum");
        assert_eq!(render_quorum(QUORUM_ALL), "all");
        assert_eq!(render_quorum(QUORUM_DEFAULT), "default");
        assert_eq!(render_quorum(0), "0");
        assert_eq!(render_quorum(3), "3");
    }

    #[test]
    fn render_keyfun_covers_all_known_selectors() {
        assert_eq!(render_keyfun(CHASH_KEYFUN_STD), "std");
        assert_eq!(render_keyfun(CHASH_KEYFUN_BUCKETONLY), "bucketonly");
        assert_eq!(render_keyfun(CHASH_KEYFUN_CUSTOM), "custom");
        assert_eq!(render_keyfun(42), "unknown");
    }

    #[test]
    fn overlay_applies_only_named_fields() {
        let base = RpbBucketProps {
            n_val: Some(3),
            allow_mult: Some(false),
            chash_keyfun: Some(CHASH_KEYFUN_STD),
            ..RpbBucketProps::default()
        };
        let opts = SetOptions {
            n_val: Some(5),
            keyfun: Some(KeyFunArg::BucketOnly),
            ..SetOptions::default()
        };
        let merged = opts.overlay(&base);
        assert_eq!(merged.n_val, Some(5), "n_val overridden");
        assert_eq!(merged.allow_mult, Some(false), "allow_mult preserved");
        assert_eq!(merged.chash_keyfun, Some(CHASH_KEYFUN_BUCKETONLY));
        // Replication strategy was not named: stays absent.
        assert_eq!(merged.replication_strategy, None);
    }

    #[test]
    fn overlay_writes_quorum_magic_values() {
        let base = RpbBucketProps::default();
        let opts = SetOptions {
            read: Some(ConsistencyArg::from_str("quorum").unwrap()),
            write: Some(ConsistencyArg::from_str("3").unwrap()),
            ..SetOptions::default()
        };
        let merged = opts.overlay(&base);
        assert_eq!(merged.r, Some(QUORUM_QUORUM));
        assert_eq!(merged.w, Some(3));
    }

    #[test]
    fn empty_overrides_are_detected() {
        assert!(SetOptions::default().is_empty());
        assert!(!SetOptions {
            n_val: Some(1),
            ..SetOptions::default()
        }
        .is_empty());
    }

    #[test]
    fn view_from_pb_renders_quorum_and_keyfun() {
        let pb = RpbBucketProps {
            n_val: Some(5),
            r: Some(QUORUM_ALL),
            w: Some(2),
            chash_keyfun: Some(CHASH_KEYFUN_BUCKETONLY),
            replication_strategy: Some(REPLICATION_STRATEGY_SUCCESSORS),
            ..RpbBucketProps::default()
        };
        let view = BucketPropsView::from_pb(&pb);
        assert_eq!(view.n_val, Some(5));
        assert_eq!(view.r.as_deref(), Some("all"));
        assert_eq!(view.w.as_deref(), Some("2"));
        assert_eq!(view.keyfun, Some("bucketonly"));
        assert_eq!(view.replication_strategy, Some("successors"));
    }

    #[test]
    fn human_render_includes_overrides() {
        let report = BucketPropsReport {
            node: "127.0.0.1:8087".into(),
            bucket: "users".into(),
            props: BucketPropsView::from_pb(&RpbBucketProps {
                n_val: Some(5),
                allow_mult: Some(true),
                chash_keyfun: Some(CHASH_KEYFUN_BUCKETONLY),
                ..RpbBucketProps::default()
            }),
        };
        let mut buf = Vec::new();
        render_get(&report, OutputFormat::Human, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Bucket properties for users via 127.0.0.1:8087"));
        assert!(s.contains("n_val: 5"));
        assert!(s.contains("allow_mult: true"));
        assert!(s.contains("keyfun: bucketonly"));
    }

    #[test]
    fn json_render_emits_node_bucket_props_object() {
        let report = BucketPropsReport {
            node: "host:8087".into(),
            bucket: "users".into(),
            props: BucketPropsView::from_pb(&RpbBucketProps {
                n_val: Some(3),
                ..RpbBucketProps::default()
            }),
        };
        let mut buf = Vec::new();
        render_get(&report, OutputFormat::Json, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["node"], "host:8087");
        assert_eq!(v["bucket"], "users");
        assert_eq!(v["props"]["n_val"], 3);
        assert!(v["props"].get("allow_mult").is_none());
    }
}
