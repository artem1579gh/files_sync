//! inotify watcher and event debouncing for daemon mode (design §5.9).

use crate::fs::RelPath;

/// What a replica's watcher tells the sync loop (design §5.9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Hint {
    /// These paths may have changed: rescan them.
    Paths(Vec<RelPath>),
    /// Events were lost (inotify overflow): rescan everything.
    FullRescan,
}
