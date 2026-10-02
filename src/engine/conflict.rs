//! Conflict bookkeeping for the executor (design §6.2, §6.3).
//!
//! What a conflict or resurrection does is decided by
//! [`reconcile`](crate::engine::reconcile) and laid out by
//! [`plan`](crate::engine::plan): the loser is renamed to its conflict name
//! (`Op::RenameToConflict`), then the winner's version is created on the
//! loser side, and the next round propagates the copy and the merged vector
//! like any other change. This module only logs the resolutions and records
//! what was done, for the [`SyncReport`](crate::engine::SyncReport).

use crate::config::ReplicaId;
use crate::engine::{Action, ActionKind, Side};
use crate::fs::RelPath;

/// A conflict copy made by this sync: the loser's version at `path` on
/// `side` now lives at `copy` (and reaches the other side in a later round).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictCopy {
    pub side: Side,
    pub path: RelPath,
    pub copy: RelPath,
}

/// Logs every conflict and resurrection among `actions`; `ids` are the
/// replica IDs of sides A and B.
pub(crate) fn log_resolutions(actions: &[Action], ids: [ReplicaId; 2]) {
    let id = |side: Side| match side {
        Side::A => ids[0],
        Side::B => ids[1],
    };
    for action in actions {
        let path = &action.path;
        match &action.kind {
            ActionKind::Conflict(r) => {
                let loser = id(r.winner.other());
                match &r.conflict_name {
                    Some(copy) => tracing::info!(
                        %path, %copy, winner = %id(r.winner), %loser,
                        "conflict: keeping the loser's version as a conflict copy"
                    ),
                    None => tracing::info!(
                        %path, winner = %id(r.winner), %loser,
                        "conflict: directory mode differs, the winner's mode applies"
                    ),
                }
            }
            ActionKind::Resurrect(r) => tracing::info!(
                %path, deleted_by = %id(r.winner.other()),
                "keeping a deleted directory: it holds changes the deleting side has not seen"
            ),
            _ => {}
        }
    }
}
