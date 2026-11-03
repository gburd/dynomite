//! Operation histories.
//!
//! A workload records each client operation as an invocation and a
//! completion (`ok`, `fail`, or `info` when the outcome is unknown, such
//! as a timeout). The format is the JSON-lines history that
//! `scripts/consistency/elle_check.py` consumes, with register operations
//! (`["w", k, v]`, `["r", k, v-or-null]`) alongside its list-append ones.

use std::io::Write;
use std::path::Path;
use std::time::Instant;

/// How an operation completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Acknowledged by the system.
    Ok,
    /// Definitely not applied (an error reply).
    Fail,
    /// Unknown: timed out or the connection dropped.
    Info,
}

/// One register operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// Write `value` to `key`.
    Write {
        /// Key.
        key: String,
        /// Unique value.
        value: String,
    },
    /// Read `key`; `value` is filled in on completion.
    Read {
        /// Key.
        key: String,
        /// Value observed, `None` for a missing key.
        value: Option<String>,
    },
}

/// A completed operation.
#[derive(Clone, Debug)]
pub struct Event {
    /// Client (process) id.
    pub process: usize,
    /// The operation and, for reads, what it observed.
    pub op: Op,
    /// How it completed.
    pub outcome: Outcome,
    /// Invocation time, nanoseconds since the history began.
    pub invoke_ns: u128,
    /// Completion time, nanoseconds since the history began.
    pub complete_ns: u128,
}

/// An append-only history shared by a workload's clients.
#[derive(Debug)]
pub struct History {
    start: Instant,
    events: Vec<Event>,
}

impl Default for History {
    fn default() -> Self {
        Self::new()
    }
}

impl History {
    /// An empty history whose clock starts now.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            events: Vec::new(),
        }
    }

    /// Nanoseconds since the history began.
    pub fn now(&self) -> u128 {
        self.start.elapsed().as_nanos()
    }

    /// Record a completed operation.
    pub fn record(&mut self, e: Event) {
        self.events.push(e);
    }

    /// All recorded events, in completion order.
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Write the history as JSON lines (invoke + completion records).
    pub fn write_jsonl(&self, path: &Path) -> std::io::Result<()> {
        let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
        for (i, e) in self.events.iter().enumerate() {
            let (inv, done) = match &e.op {
                Op::Write { key, value } => (
                    serde_json::json!([["w", key, value]]),
                    serde_json::json!([["w", key, value]]),
                ),
                Op::Read { key, value } => (
                    serde_json::json!([["r", key, null]]),
                    serde_json::json!([["r", key, value]]),
                ),
            };
            let ty = match e.outcome {
                Outcome::Ok => "ok",
                Outcome::Fail => "fail",
                Outcome::Info => "info",
            };
            writeln!(
                f,
                "{}",
                serde_json::json!({"index": 2 * i, "process": e.process, "type": "invoke",
                    "time_ns": e.invoke_ns, "value": inv})
            )?;
            writeln!(
                f,
                "{}",
                serde_json::json!({"index": 2 * i + 1, "process": e.process, "type": ty,
                    "time_ns": e.complete_ns, "value": done})
            )?;
        }
        Ok(())
    }
}
