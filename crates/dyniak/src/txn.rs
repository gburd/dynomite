//! Multi-key transaction surface for the Riak protocol layer.
//!
//! `dyniak` extends Riak's per-key, eventually-consistent model with
//! atomic multi-key transactions. A client groups several put and
//! delete operations into one [`TxnBatch`]; the backend applies all of
//! them or none of them. Riak itself has no equivalent: a multi-key
//! atomic write in Riak would need a PAXOS-style coordination layer.
//! The dyniak engine instead leans on the transactional storage engine
//! underneath it.
//!
//! Two layers stack on top of this module:
//!
//! * **Single-environment** (this module's [`TransactionalStore`]): all
//!   ops in a batch route to one node's storage engine and commit in a
//!   single engine transaction.
//! * **Cross-node** ([`crate::datastore::xa`], gated on the `noxu`
//!   feature): ops that span keys owned by different primary nodes are
//!   coordinated with X/Open XA two-phase commit.
//!
//! The wire-facing surfaces (HTTP `POST /transactions`, the PBC
//! `DynRpbTxn*` extension) decode a request into a [`TxnBatch`], hand it
//! to a [`TransactionalStore`], and encode the [`TxnOutcome`] back.
//!
//! # Examples
//!
//! ```
//! use dyniak::txn::{TxnBatch, TxnOp};
//!
//! let batch = TxnBatch {
//!     ops: vec![
//!         TxnOp::Put {
//!             bucket: b"users".to_vec(),
//!             key: b"alice".to_vec(),
//!             value: b"hello".to_vec(),
//!             indexes: vec![(b"age_int".to_vec(), b"42".to_vec())],
//!         },
//!         TxnOp::Delete {
//!             bucket: b"users".to_vec(),
//!             key: b"bob".to_vec(),
//!         },
//!     ],
//!     force_abort: false,
//! };
//! assert_eq!(batch.ops.len(), 2);
//! ```

use serde::{Deserialize, Serialize};

/// One operation in a multi-key transaction batch.
///
/// A batch is an ordered list of these; the backend replays them in
/// order inside a single engine transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TxnOp {
    /// Insert or overwrite the object at `(bucket, key)`, fanning the
    /// supplied `indexes` into the 2i layer (same semantics as
    /// [`crate::datastore::NoxuDatastore::put_object`] when the `noxu`
    /// feature is enabled).
    Put {
        /// Bucket name.
        bucket: Vec<u8>,
        /// Object key.
        key: Vec<u8>,
        /// Object value bytes.
        value: Vec<u8>,
        /// `(index_name, value)` 2i entries.
        indexes: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// Remove the object at `(bucket, key)` and its 2i entries.
    Delete {
        /// Bucket name.
        bucket: Vec<u8>,
        /// Object key.
        key: Vec<u8>,
    },
}

impl TxnOp {
    /// Bucket the operation targets.
    #[must_use]
    pub fn bucket(&self) -> &[u8] {
        match self {
            Self::Put { bucket, .. } | Self::Delete { bucket, .. } => bucket,
        }
    }

    /// Key the operation targets.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
        }
    }
}

/// A batch of operations to apply atomically.
///
/// `force_abort` exists for clients (and tests) that want to exercise
/// the rollback path: when set, the backend applies every operation
/// inside the transaction and then deliberately aborts, leaving the
/// keyspace untouched and reporting [`TxnOutcome::Aborted`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TxnBatch {
    /// Ordered list of operations.
    pub ops: Vec<TxnOp>,
    /// When true, apply every op and then roll back instead of
    /// committing.
    pub force_abort: bool,
}

/// Result of executing a [`TxnBatch`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TxnOutcome {
    /// The transaction committed; `operations` is the number of ops
    /// that were applied.
    Committed {
        /// Number of operations committed.
        operations: usize,
    },
    /// The transaction rolled back without changing the keyspace.
    Aborted {
        /// Human-readable abort reason.
        reason: String,
    },
}

/// Errors surfaced by a [`TransactionalStore`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TxnStoreError {
    /// The storage engine rejected the transaction. The inner string
    /// carries the backend's own error message.
    #[error("transaction backend: {0}")]
    Backend(String),
    /// The batch contained no operations.
    #[error("empty transaction batch")]
    EmptyBatch,
    /// A conflict / serialization failure was reported by the engine.
    /// The transaction was rolled back; the caller may retry.
    #[error("transaction conflict: {0}")]
    Conflict(String),
}

/// A backend that can apply a [`TxnBatch`] atomically.
///
/// Implemented by [`crate::datastore::NoxuDatastore`] under the `noxu`
/// feature. The trait is object-safe so the HTTP and PBC servers can
/// hold an `Arc<dyn TransactionalStore>` without naming the concrete
/// backend.
pub trait TransactionalStore: Send + Sync {
    /// Apply `batch` atomically.
    ///
    /// # Errors
    ///
    /// Returns [`TxnStoreError::EmptyBatch`] for an empty batch,
    /// [`TxnStoreError::Conflict`] when the engine reported a
    /// serialization failure (the batch rolled back), or
    /// [`TxnStoreError::Backend`] for any other engine error.
    fn execute_batch(&self, batch: &TxnBatch) -> Result<TxnOutcome, TxnStoreError>;
}

// ------------------------------------------------------------------
// HTTP / JSON data-transfer objects.
// ------------------------------------------------------------------

/// JSON request body for `POST /transactions`.
///
/// ```json
/// {
///   "abort": false,
///   "operations": [
///     {"op": "put", "bucket": "users", "key": "alice", "value": "hi",
///      "indexes": [{"name": "age_int", "value": "42"}]},
///     {"op": "delete", "bucket": "users", "key": "bob"}
///   ]
/// }
/// ```
///
/// Values and index values are UTF-8 strings; binary payloads are not
/// representable through the JSON endpoint (use the PBC extension for
/// arbitrary bytes).
#[derive(Clone, Debug, Deserialize)]
pub struct HttpTxnRequest {
    /// When true the transaction is rolled back after applying every
    /// op (exercises the abort path).
    #[serde(default)]
    pub abort: bool,
    /// Ordered operation list.
    pub operations: Vec<HttpTxnOp>,
}

/// One JSON operation in an [`HttpTxnRequest`].
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum HttpTxnOp {
    /// Store an object.
    Put {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Object value (UTF-8).
        value: String,
        /// Optional 2i entries.
        #[serde(default)]
        indexes: Vec<HttpIndexEntry>,
    },
    /// Delete an object.
    Delete {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
    },
}

/// One `(name, value)` 2i entry in an [`HttpTxnOp::Put`].
#[derive(Clone, Debug, Deserialize)]
pub struct HttpIndexEntry {
    /// Index name (for example `age_int`, `email_bin`).
    pub name: String,
    /// Index value (UTF-8).
    pub value: String,
}

/// JSON response body for `POST /transactions`.
#[derive(Clone, Debug, Serialize)]
pub struct HttpTxnResponse {
    /// Either `"committed"` or `"aborted"`.
    pub result: String,
    /// Number of operations committed (present on success).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operations: Option<usize>,
    /// Abort reason (present when `result == "aborted"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl HttpTxnRequest {
    /// Lower the JSON request into a [`TxnBatch`] the backend can run.
    #[must_use]
    pub fn into_batch(self) -> TxnBatch {
        let ops = self
            .operations
            .into_iter()
            .map(|op| match op {
                HttpTxnOp::Put {
                    bucket,
                    key,
                    value,
                    indexes,
                } => TxnOp::Put {
                    bucket: bucket.into_bytes(),
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                    indexes: indexes
                        .into_iter()
                        .map(|i| (i.name.into_bytes(), i.value.into_bytes()))
                        .collect(),
                },
                HttpTxnOp::Delete { bucket, key } => TxnOp::Delete {
                    bucket: bucket.into_bytes(),
                    key: key.into_bytes(),
                },
            })
            .collect();
        TxnBatch {
            ops,
            force_abort: self.abort,
        }
    }
}

impl HttpTxnResponse {
    /// Build a JSON response from a [`TxnOutcome`].
    #[must_use]
    pub fn from_outcome(outcome: &TxnOutcome) -> Self {
        match outcome {
            TxnOutcome::Committed { operations } => Self {
                result: "committed".to_string(),
                operations: Some(*operations),
                reason: None,
            },
            TxnOutcome::Aborted { reason } => Self {
                result: "aborted".to_string(),
                operations: None,
                reason: Some(reason.clone()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_request_lowers_to_batch() {
        let json = r#"{
            "operations": [
                {"op": "put", "bucket": "b", "key": "k", "value": "v",
                 "indexes": [{"name": "age_int", "value": "42"}]},
                {"op": "delete", "bucket": "b", "key": "old"}
            ]
        }"#;
        let req: HttpTxnRequest = serde_json::from_str(json).expect("decode");
        let batch = req.into_batch();
        assert!(!batch.force_abort);
        assert_eq!(batch.ops.len(), 2);
        assert_eq!(
            batch.ops[0],
            TxnOp::Put {
                bucket: b"b".to_vec(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                indexes: vec![(b"age_int".to_vec(), b"42".to_vec())],
            }
        );
        assert_eq!(
            batch.ops[1],
            TxnOp::Delete {
                bucket: b"b".to_vec(),
                key: b"old".to_vec(),
            }
        );
    }

    #[test]
    fn abort_flag_round_trips() {
        let json = r#"{"abort": true, "operations": []}"#;
        let req: HttpTxnRequest = serde_json::from_str(json).expect("decode");
        let batch = req.into_batch();
        assert!(batch.force_abort);
        assert!(batch.ops.is_empty());
    }

    #[test]
    fn response_from_outcome_committed() {
        let r = HttpTxnResponse::from_outcome(&TxnOutcome::Committed { operations: 3 });
        let s = serde_json::to_string(&r).expect("encode");
        assert!(s.contains("\"result\":\"committed\""));
        assert!(s.contains("\"operations\":3"));
        assert!(!s.contains("reason"));
    }

    #[test]
    fn response_from_outcome_aborted() {
        let r = HttpTxnResponse::from_outcome(&TxnOutcome::Aborted {
            reason: "client requested abort".to_string(),
        });
        let s = serde_json::to_string(&r).expect("encode");
        assert!(s.contains("\"result\":\"aborted\""));
        assert!(s.contains("client requested abort"));
        assert!(!s.contains("operations"));
    }

    #[test]
    fn txn_op_accessors() {
        let put = TxnOp::Put {
            bucket: b"bk".to_vec(),
            key: b"ky".to_vec(),
            value: b"v".to_vec(),
            indexes: vec![],
        };
        assert_eq!(put.bucket(), b"bk");
        assert_eq!(put.key(), b"ky");
    }
}
