//! Tictac active anti-entropy (AAE).
//!
//! Tictac AAE is Riak's continuously-running merkle-tree
//! synchroniser. Each node maintains a per-vnode rolling merkle
//! tree (the "Tictac" tree) over `(bucket, key, vclock)` tuples;
//! peers periodically exchange tree summaries and repair the
//! divergent leaves.
//!
//! This module layers Tictac AAE on top of the existing entropy
//! primitives in `crate::dynomite::entropy::*` (which already
//! ship a length-prefixed framing, AES-128-CBC reconciliation
//! channel, and snapshot source/sink traits). Tictac AAE keeps
//! the same wire shape (4-byte BE length + payload) but layers
//! a three-phase exchange protocol on top:
//!
//! * `ROOT-SYNC` exchanges the top-level (per-time-bucket) root
//!   vector.
//! * `TREE-SYNC` recurses into one diverging time bucket and
//!   exchanges the per-segment hash vector.
//! * `KEY-SYNC` enumerates the diverging keys in one
//!   `(time_bucket, segment)` pair so the repair scheduler can
//!   act.
//!
//! # Submodules
//!
//! * [`tictac`] -- the merkle tree itself.
//! * [`exchange`] -- per-pair three-phase wire protocol.
//! * [`scheduler`] -- the cadence driver (mockable clock).
//! * [`repair`] -- per-divergence repair-task dispatch.
//! * [`config`] -- operator-facing knobs.
//!
//! # Example
//!
//! ```
//! use dyniak::aae::config::ConfAae;
//! use dyniak::aae::tictac::{Tree, TreeShape};
//!
//! let cfg = ConfAae::default();
//! let shape = TreeShape {
//!     n_time_buckets: cfg.n_time_buckets,
//!     n_segments: cfg.n_segments,
//!     time_window_seconds: cfg.time_window_seconds,
//! };
//! let mut tree = Tree::new(shape);
//! tree.insert(b"users", b"alice", b"vc1", 0);
//! assert!(!tree.roots().is_empty());
//! ```

pub mod config;
pub mod delta_ship;
pub mod exchange;
pub mod exchange_fsm;
pub mod metrics;
#[cfg(feature = "noxu")]
pub mod mst_reconcile;
#[cfg(feature = "noxu")]
pub mod noxu_fold;
pub mod persist;
pub mod repair;
pub mod scheduler;
pub mod status;
pub mod tictac;

pub use crate::aae::config::ConfAae;
pub use crate::aae::config::ReconcileMode;
pub use crate::aae::delta_ship::{apply_shipment, plan_shipment, Shipment};
pub use crate::aae::exchange::{
    decode_key_sync, decode_root_sync, decode_tree_sync, encode_key_sync, encode_root_sync,
    encode_tree_sync, Divergence, Exchange, ExchangeError, ExchangeFrame, LocalPeerView, PeerView,
    EXCHANGE_MAGIC, FRAME_HEADER_LEN, MAX_PAYLOAD_LEN, PHASE_KEY_SYNC, PHASE_ROOT_SYNC,
    PHASE_TREE_SYNC,
};
pub use crate::aae::exchange_fsm::{
    exchange_with_peer, spawn_exchange, Event as ExchangeFsmEvent, ExchangeHandler,
    ExchangeOutcome, FsmRequest as ExchangeFsmRequest, PeerViewAsync, State as ExchangeFsmState,
    SyncPeerViewAdapter, DEFAULT_STATE_TIMEOUT,
};
pub use crate::aae::metrics::{
    load_snapshot_with_metrics, save_snapshot_with_metrics, AaeMetrics, AaeMetricsSnapshot,
    PeerEntry as AaePeerEntry, SimplePeerEntry as AaeSimplePeerEntry,
};
#[cfg(feature = "noxu")]
pub use crate::aae::mst_reconcile::{
    build_mst, composite_key, reconcile_pull, split_composite, value_digest, DatastoreSource,
    MstReconcileError, ObjectSource, ReconcileOutcome,
};
#[cfg(feature = "noxu")]
pub use crate::aae::noxu_fold::NoxuFoldError;
pub use crate::aae::persist::{PersistError, MAX_FIELD_LEN, SNAPSHOT_MAGIC, SNAPSHOT_VERSION};
pub use crate::aae::repair::{
    ClockOrder, ItcOrder, LexicographicOrder, MpscRepairSink, Outcome, RepairDirection,
    RepairOutcome, RepairScheduler, RepairSink, RepairTask,
};
pub use crate::aae::scheduler::{Clock, MockClock, Scheduler, SweepPlan, SweepTick, SystemClock};
pub use crate::aae::status::{
    AaePeerStatus, AaeStatusProvider, AaeStatusSnapshot, NoopAaeStatusProvider,
};
pub use crate::aae::tictac::{KeyEntry, Tree, TreeError, TreeShape};
