//! The `Replica` trait and its local implementation (design §7).
//!
//! The engine sees a replica only through [`Replica`]. Every precondition
//! (compare-and-swap) check runs **inside** the replica, next to the files,
//! so a remote replica (T21) is race-free in exactly the same way: the engine
//! says what it expects in logical terms ([`Precondition`]), and the replica
//! maps that to its own physical fingerprint.
pub mod delta;
pub mod local;
pub mod proto;
pub mod remote;

pub use delta::{BLOCK_SIZE, Blocks, Delta};
pub use local::LocalReplica;
pub use remote::RemoteReplica;

use std::io::Read;
use std::path::Path;
use std::time::{Duration, Instant};

use crossbeam_channel::Receiver;
use serde::{Deserialize, Serialize};

use crate::config::{PairConfig, ReplicaId};
use crate::error::Result;
use crate::fs::RelPath;
use crate::fs::commit::FileMeta;
use crate::index::{Entry, Kind, LocalMeta, PeerState, VersionVector};
use crate::scan::{ScanStats, Scope};
use crate::status::ReplicaStatus;
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

    /// `-K` (design §4.3): the peer holds a real directory at `path`, where
    /// this replica's index has a symlink. If this replica keeps dirlinks and
    /// the link points to a directory it may adopt, it indexes the link as
    /// that directory from now on (a local change). Returns whether `path`
    /// is an adopted directory now.
    fn adopt(&mut self, path: &RelPath) -> Result<bool> {
        let _ = path;
        Ok(false)
    }

    /// Ends a sync cycle with `peer`: records it (the last sync time) and
    /// collects tombstones (design §3). `tombstones` lists this replica's
    /// tombstones as the cycle ended, each with its version vector (checked
    /// against the index, like a precondition) and what the peer held at its
    /// path. A tombstone that every peer has acknowledged (by holding a
    /// tombstone or nothing there) for `retention` is removed. Returns the
    /// paths whose tombstones were removed.
    fn record_sync(
        &mut self,
        peer: ReplicaId,
        tombstones: Vec<(RelPath, VersionVector, PeerState)>,
        retention: Duration,
    ) -> Result<Vec<RelPath>> {
        let _ = (peer, tombstones, retention);
        Ok(Vec::new())
    }

    /// The replica's Lamport clock: the largest version counter its index
    /// has ever stored (design §3).
    fn clock(&self) -> Result<u64>;

    /// Raises the replica's clock to at least `floor` (the peer's clock), so
    /// local changes scanned from now on get counters above every counter
    /// the peer has seen, also from this replica (design §6.4, issue #2).
    fn witness(&mut self, floor: u64) -> Result<()>;

    /// Whether reaching this replica crosses a network, so that sending
    /// less content pays off (block-level delta transfer, design §7.1).
    fn is_remote(&self) -> bool {
        false
    }

    /// The block list of the file `path`, which must be the indexed file of
    /// kind `expect` (size and hash, checked at the end of a stable read).
    /// `None` if this replica takes part in no delta transfer of it (the
    /// default; a protocol v1 session; a file of too many blocks), so the
    /// file is sent whole.
    fn blocks(&self, path: &RelPath, expect: &Kind) -> Result<Option<Blocks>> {
        let _ = (path, expect);
        Ok(None)
    }

    /// Streams the blocks `blocks` (in this order) of the file `path`, which
    /// must be the indexed file of kind `expect` and stay unchanged while it
    /// is read (else the read fails with `Unstable`). Only called after
    /// [`Replica::blocks`] gave a list.
    fn read_blocks(
        &self,
        path: &RelPath,
        expect: &Kind,
        blocks: &[u32],
    ) -> Result<Box<dyn ContentReader>> {
        let _ = (expect, blocks);
        Err(delta_unsupported(path))
    }

    /// [`Replica::apply`] of an [`Op::WriteFile`] whose content is
    /// assembled from the file now at `path` and the blocks in `data`, as
    /// `delta` says. Every check of `apply` holds, plus: every block must
    /// hash as `delta` lists it, and the current file must not change
    /// during assembly (else `Err(Unstable)`, nothing committed). Only
    /// called after [`Replica::blocks`] gave a list.
    fn apply_delta(
        &mut self,
        path: &RelPath,
        op: Op,
        pre: Precondition,
        delta: &Delta,
        data: &mut dyn Read,
    ) -> Result<Outcome> {
        let _ = (op, pre, delta, data);
        Err(delta_unsupported(path))
    }
}

fn delta_unsupported(path: &RelPath) -> crate::Error {
    crate::Error::InvalidOp {
        path: path.as_bytes().to_vec(),
        reason: "this replica does not take part in delta transfers",
    }
}

/// What a process that runs sync cycles does with a replica besides the
/// [`Replica`] calls: housekeeping and status (the daemon, `sync --once`).
pub trait Housekeeping: Replica {
    /// When the next quarantined old inode is due to be swept (§5.3 step
    /// 4(f)); `None` if none is waiting here (a remote replica's server
    /// sweeps its own).
    fn next_sweep(&self) -> Option<Instant>;

    /// Sweeps the quarantine (see [`LocalReplica::sweep_quarantine`]).
    fn sweep(&mut self);

    /// The replica's state for `status`, with `peer` the other replica.
    fn status(&self, peer: ReplicaId) -> Result<ReplicaStatus>;
}

impl Housekeeping for LocalReplica {
    fn next_sweep(&self) -> Option<Instant> {
        self.quarantine().next_deadline()
    }

    fn sweep(&mut self) {
        self.sweep_quarantine();
    }

    fn status(&self, peer: ReplicaId) -> Result<ReplicaStatus> {
        ReplicaStatus::of(self, peer)
    }
}

impl Housekeeping for RemoteReplica {
    fn next_sweep(&self) -> Option<Instant> {
        None
    }

    fn sweep(&mut self) {}

    fn status(&self, _peer: ReplicaId) -> Result<ReplicaStatus> {
        Ok(ReplicaStatus::remote(self.addr()))
    }
}

/// One replica of a pair as a syncing process has it: opened here, or
/// reached at the `serve` process that runs it (design §7.1).
// A process holds two of them; boxing would buy nothing.
#[allow(clippy::large_enum_variant)]
pub enum PairReplica {
    Local(LocalReplica),
    Remote(RemoteReplica),
}

impl PairReplica {
    /// Opens replica `side` (0 = A, 1 = B) of `cfg`: locally, or, if the
    /// config gives it a remote address, by connecting to its server on
    /// behalf of the other replica.
    pub fn open(cfg: &PairConfig, side: usize, pair_dir: &Path) -> Result<PairReplica> {
        let r = &cfg.replicas[side];
        if r.is_remote() {
            let peer = &cfg.replicas[1 - side];
            Ok(PairReplica::Remote(RemoteReplica::connect(
                r, peer, pair_dir,
            )?))
        } else {
            Ok(PairReplica::Local(LocalReplica::open(r, pair_dir)?))
        }
    }

    fn get(&self) -> &dyn Housekeeping {
        match self {
            PairReplica::Local(r) => r,
            PairReplica::Remote(r) => r,
        }
    }

    fn get_mut(&mut self) -> &mut dyn Housekeeping {
        match self {
            PairReplica::Local(r) => r,
            PairReplica::Remote(r) => r,
        }
    }
}

impl Replica for PairReplica {
    fn id(&self) -> ReplicaId {
        self.get().id()
    }

    fn scan(&mut self, scope: Scope) -> Result<ScanStats> {
        self.get_mut().scan(scope)
    }

    fn changes_since(&self, seq: u64) -> Result<Vec<(RelPath, Entry)>> {
        self.get().changes_since(seq)
    }

    fn open_read(&self, path: &RelPath, expect: &Entry) -> Result<Box<dyn ContentReader>> {
        self.get().open_read(path, expect)
    }

    fn apply(
        &mut self,
        path: &RelPath,
        op: Op,
        pre: Precondition,
        content: Option<&mut dyn Read>,
    ) -> Result<Outcome> {
        self.get_mut().apply(path, op, pre, content)
    }

    fn watch(&mut self) -> Option<Receiver<Hint>> {
        self.get_mut().watch()
    }

    fn adopt(&mut self, path: &RelPath) -> Result<bool> {
        self.get_mut().adopt(path)
    }

    fn record_sync(
        &mut self,
        peer: ReplicaId,
        tombstones: Vec<(RelPath, VersionVector, PeerState)>,
        retention: Duration,
    ) -> Result<Vec<RelPath>> {
        self.get_mut().record_sync(peer, tombstones, retention)
    }

    fn clock(&self) -> Result<u64> {
        self.get().clock()
    }

    fn witness(&mut self, floor: u64) -> Result<()> {
        self.get_mut().witness(floor)
    }

    fn is_remote(&self) -> bool {
        self.get().is_remote()
    }

    fn blocks(&self, path: &RelPath, expect: &Kind) -> Result<Option<Blocks>> {
        self.get().blocks(path, expect)
    }

    fn read_blocks(
        &self,
        path: &RelPath,
        expect: &Kind,
        blocks: &[u32],
    ) -> Result<Box<dyn ContentReader>> {
        self.get().read_blocks(path, expect, blocks)
    }

    fn apply_delta(
        &mut self,
        path: &RelPath,
        op: Op,
        pre: Precondition,
        delta: &Delta,
        data: &mut dyn Read,
    ) -> Result<Outcome> {
        self.get_mut().apply_delta(path, op, pre, delta, data)
    }
}

impl Housekeeping for PairReplica {
    fn next_sweep(&self) -> Option<Instant> {
        self.get().next_sweep()
    }

    fn sweep(&mut self) {
        self.get_mut().sweep()
    }

    fn status(&self, peer: ReplicaId) -> Result<ReplicaStatus> {
        self.get().status(peer)
    }
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
