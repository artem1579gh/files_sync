//! The `Replica` trait and its local implementation (design §7).
//!
//! The engine sees a replica only through [`Replica`]. Every precondition
//! (compare-and-swap) check runs **inside** the replica, next to the files,
//! so a remote replica (T21) is race-free in exactly the same way: the engine
//! says what it expects in logical terms ([`Precondition`]), and the replica
//! maps that to its own physical fingerprint.
pub mod local;

pub use local::LocalReplica;

use std::io::Read;

use crossbeam_channel::Receiver;
use serde::{Deserialize, Serialize};

use crate::config::ReplicaId;
use crate::error::Result;
use crate::fs::RelPath;
use crate::fs::commit::FileMeta;
use crate::index::{Entry, Kind, LocalMeta, VersionVector};
use crate::scan::{ScanStats, Scope};
use crate::watch::Hint;

/// File content streamed out of a replica by [`Replica::open_read`].
///
/// Reaching EOF (`read` returning `Ok(0)`) means the content was verified
/// against the expected entry; a change during the read makes `read` fail
/// instead (with an `io::Error` wrapping
/// [`Error::Unstable`](crate::Error::Unstable), see
/// [`Error::from_stream`](crate::Error::from_stream)).
pub trait ContentReader: Read + Send {}

impl<T: Read + Send + ?Sized> ContentReader for T {}

/// One side of a sync pair (design §7).
pub trait Replica {
    fn id(&self) -> ReplicaId;

    /// Scans the replica into its own index, bumping version vectors of
    /// local changes.
    fn scan(&mut self, scope: Scope) -> Result<ScanStats>;

    /// Entries put since `seq` (exclusive), in seq order. [`LocalMeta`] is
    /// left out, as on the wire.
    fn changes_since(&self, seq: u64) -> Result<Vec<(RelPath, Entry)>>;

    /// Streams the content of the file `path`, which must be the file
    /// `expect` describes (kind, size and hash; checked at EOF).
    fn open_read(&self, path: &RelPath, expect: &Entry) -> Result<Box<dyn ContentReader>>;

    /// Applies `op` at `path` if, and only if, `pre` holds; on success the
    /// replica's index records the result.
    ///
    /// `content` is required by [`Op::WriteFile`] and ignored otherwise.
    /// Races give [`Outcome::PreconditionFailed`] (or `Err(Unstable)` when
    /// the path changed right after a commit); an `op` that cannot apply to
    /// the indexed state at all is [`Error::InvalidOp`](crate::Error::InvalidOp).
    fn apply(
        &mut self,
        path: &RelPath,
        op: Op,
        pre: Precondition,
        content: Option<&mut dyn Read>,
    ) -> Result<Outcome>;

    /// Change hints from a watcher, if the replica has one.
    fn watch(&mut self) -> Option<Receiver<Hint>>;
}

/// A change to apply at one path. Every op that leaves an entry at the path
/// carries that entry's version vector (from the engine).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    /// Writes a regular file from the content stream, which must hash to
    /// `hash`. Creates it (`Absent`), or replaces a file or symlink.
    WriteFile {
        meta: FileMeta,
        hash: [u8; 32],
        vv: VersionVector,
    },
    /// Creates a directory (`Absent` only).
    Mkdir {
        mode: u32,
        mtime_ns: i64,
        vv: VersionVector,
    },
    /// Creates a symlink to the canonical (unmunged) `target` (`Absent`), or
    /// replaces a file or symlink with it.
    Symlink {
        target: Vec<u8>,
        mtime_ns: i64,
        vv: VersionVector,
    },
    /// Deletes a file or symlink; the index keeps a tombstone with `vv`.
    Delete { vv: VersionVector },
    /// Removes an empty directory; the index keeps a tombstone with `vv`.
    Rmdir { vv: VersionVector },
    /// Renames a file or symlink to its sibling `to` (a conflict name, §6.2).
    /// The path's entry becomes a tombstone with a local bump; the copy at
    /// `to` is indexed with a fresh version vector, so it syncs like any new
    /// file.
    RenameToConflict { to: RelPath },
    /// Sets mode and mtime. A file is rewritten as a copy (content is never
    /// changed in place), a directory is `fchmod`ed. When nothing on disk
    /// needs to change (same mode and mtime, or a symlink, or a directory
    /// mtime, which is not synced), only the index is updated, which also
    /// serves to record a merged version vector.
    SetMeta {
        mode: u32,
        mtime_ns: i64,
        vv: VersionVector,
    },
}

/// What the engine expects at the path, in logical terms. The replica maps
/// it to its own physical fingerprint (§5.3 step 4(a)).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Precondition {
    /// Nothing live: no index entry, or a tombstone (any version vector).
    /// The commit itself checks that the name is free on disk.
    Absent,
    /// The index entry has exactly this kind (including hash or target) and
    /// version vector, and the object on disk still matches the entry's
    /// fingerprint.
    Matches { kind: Kind, vv: VersionVector },
}

impl Precondition {
    /// Expects exactly `entry` (a tombstone included).
    pub fn matching(entry: &Entry) -> Precondition {
        Precondition::Matches {
            kind: entry.kind.clone(),
            vv: entry.vv.clone(),
        }
    }
}

/// Result of [`Replica::apply`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Done; this is the path's index entry now (without [`LocalMeta`]).
    Applied(Entry),
    /// The precondition did not hold, or the path changed while we
    /// committed. Nothing changed. Carries the path's index entry, which may
    /// itself be stale: rescan the path (§6.4).
    PreconditionFailed(Option<Entry>),
    /// A concurrent change could not be undone cleanly, so a user object was
    /// kept under this conflict name. Nothing was lost; rescan the path.
    Preserved { conflict: RelPath },
}

/// The entry as it is sent to a peer: without [`LocalMeta`].
pub(crate) fn wire(mut entry: Entry) -> Entry {
    entry.local = LocalMeta::default();
    entry
}
