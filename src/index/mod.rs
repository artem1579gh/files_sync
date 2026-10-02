//! Per-replica index: entries, version vectors, the redb store and the intent
//! journal (design §3, §5.8).
pub mod entry;
pub mod journal;
pub mod store;
pub mod vv;

pub use entry::{Entry, Kind, LinkInfo, LocalMeta, UnmanagedReason, sync_mode};
pub use journal::{Intent, IntentId, IntentOp, IntentState, Journal};
pub use store::{Ack, DEFAULT_TOMBSTONE_RETENTION, IndexStore, PeerState, ReadTxn, WriteTxn};
pub use vv::{Ord4, VersionVector};
