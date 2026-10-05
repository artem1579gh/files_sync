//! The executor: one sync cycle between two replicas (design §6.3, §6.4).
//!
//! [`Engine::sync`] scans both replicas, reconciles their indexes, and runs
//! the planned steps phase by phase through [`Replica::apply`], streaming a
//! file's content from [`Replica::open_read`] on the other side. It does no
//! I/O itself: every check that matters (the CAS precondition, the source
//! content's stability and hash) runs inside the replicas.
//!
//! A file that the destination already holds goes as a block-level delta
//! when one side is remote and both files are large enough (design §7.1):
//! the engine matches the two block lists and passes only the missing
//! blocks; the replicas check everything.
//!
//! A step that does not apply (`PreconditionFailed`, `Preserved`, or an
//! unstable source or destination) marks its path **dirty**: the path's
//! later steps in the round are skipped, and before the next round the dirty
//! paths are rescanned on both replicas. Rounds repeat while `reconcile`
//! still yields steps (a conflict or a resurrection always needs one more
//! round), up to [`MAX_ROUNDS`]. What is left after that waits for the next
//! cycle and is reported in [`SyncReport::unresolved`].
//!
//! Any other per-path error (e.g. `EACCES`) is logged and reported in
//! [`SyncReport::errors`], and the path and its subtree are left alone for
//! the rest of the cycle. Only an index or root failure aborts the cycle.
//!
//! **Mass-deletion guard** (design §6.4): when a round would delete more than
//! [`Engine::max_delete_percent`] of a replica's live entries (and more than
//! [`MASS_DELETE_MIN`]), no deletion is applied to that replica for the rest
//! of the cycle. They are reported in [`SyncReport::held_back`]; everything
//! else syncs as usual.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::time::Duration;

use crate::engine::conflict::{ConflictCopy, log_resolutions};
use crate::engine::{
    Action, ActionKind, IndexView, Side, SkipReason, Snapshot, Step, is_beneath, plan, reconcile,
};
use crate::error::{Error, RemoteKind, Result};
use crate::fs::RelPath;
use crate::index::{DEFAULT_TOMBSTONE_RETENTION, Entry, Kind, PeerState, UnmanagedReason};
use crate::replica::{Delta, Op, Outcome, Precondition, Replica};
use crate::scan::{ScanStats, Scope};

/// Rounds per sync cycle (§6.4).
pub const MAX_ROUNDS: usize = 5;

/// Files smaller than this (old or new) are always sent whole (§7.1).
pub const DELTA_MIN_SIZE: u64 = 1 << 20;

/// Default of [`Engine::max_delete_percent`].
pub const DEFAULT_MAX_DELETE_PERCENT: u8 = 50;

/// The mass-deletion guard never holds back this many deletions or fewer,
/// so deleting most of a small tree needs no confirmation.
pub const MASS_DELETE_MIN: usize = 10;

/// Runs sync cycles between two replicas.
#[derive(Clone, Debug)]
pub struct Engine {
    max_rounds: usize,
    tombstone_retention: Duration,
    delta_min_size: u64,
    max_delete_percent: u8,
}

impl Default for Engine {
    fn default() -> Engine {
        Engine {
            max_rounds: MAX_ROUNDS,
            tombstone_retention: DEFAULT_TOMBSTONE_RETENTION,
            delta_min_size: DELTA_MIN_SIZE,
            max_delete_percent: DEFAULT_MAX_DELETE_PERCENT,
        }
    }
}

/// Deletions on one replica held back by the mass-deletion guard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldBack {
    /// The replica the deletions were for.
    pub side: Side,
    /// Its live entries when the guard tripped.
    pub live: usize,
    /// The paths that were not deleted, in path order.
    pub paths: Vec<RelPath>,
}

/// What a sync cycle did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncReport {
    /// Rounds that ran steps.
    pub rounds: usize,
    /// Steps applied.
    pub applied: usize,
    /// Steps that did not apply because a path changed under us (the path
    /// was rescanned and retried).
    pub retried: usize,
    /// Conflict copies made.
    pub conflicts: Vec<ConflictCopy>,
    /// Directories kept although one side deleted them (§6.3).
    pub resurrected: Vec<RelPath>,
    /// Paths not synced because one side's object is `Unmanaged` (§3).
    pub unmanaged: Vec<RelPath>,
    /// Paths that failed with an error other than a race, with the error.
    /// They were left alone.
    pub errors: Vec<(RelPath, String)>,
    /// Paths with work left after the last round (still changing, or still
    /// failing their precondition). The next cycle picks them up.
    pub unresolved: Vec<RelPath>,
    /// Tombstones garbage-collected at the end of the cycle (design §3), on
    /// A and on B.
    pub collected: [usize; 2],
    /// Files written as a block-level delta (§7.1), among `applied`.
    pub deltas: usize,
    /// Deletions not applied by the mass-deletion guard, per replica (at
    /// most one entry each).
    pub held_back: Vec<HeldBack>,
}

impl SyncReport {
    /// Both replicas hold the same synced state (nothing unresolved, no
    /// errors, no deletion held back).
    pub fn is_converged(&self) -> bool {
        self.unresolved.is_empty() && self.errors.is_empty() && self.held_back.is_empty()
    }
}

impl Engine {
    pub fn new() -> Engine {
        Engine::default()
    }

    /// Overrides [`MAX_ROUNDS`] (at least 1).
    pub fn max_rounds(mut self, rounds: usize) -> Engine {
        self.max_rounds = rounds.max(1);
        self
    }

    /// Overrides [`DEFAULT_TOMBSTONE_RETENTION`]: how long both sides must
    /// have held a deletion before its tombstones are collected.
    pub fn tombstone_retention(mut self, retention: Duration) -> Engine {
        self.tombstone_retention = retention;
        self
    }

    /// Overrides [`DELTA_MIN_SIZE`]: a file is sent as a block-level delta
    /// only if its old and new sizes are both at least this (and one side
    /// is remote).
    pub fn delta_min_size(mut self, size: u64) -> Engine {
        self.delta_min_size = size;
        self
    }

    /// Overrides [`DEFAULT_MAX_DELETE_PERCENT`]: the share of a replica's
    /// live entries (in percent) that one cycle may delete there before the
    /// mass-deletion guard holds the deletions back. 100 or more turns the
    /// guard off.
    pub fn max_delete_percent(mut self, percent: u8) -> Engine {
        self.max_delete_percent = percent;
        self
    }

    /// A full sync cycle: scans both replicas completely, then syncs.
    pub fn sync_once(&self, a: &mut dyn Replica, b: &mut dyn Replica) -> Result<SyncReport> {
        self.sync(a, b, Scope::Full)
    }

    /// A sync cycle that first scans `scope` on both replicas. The whole
    /// indexes are reconciled, whatever the scope.
    ///
    /// Fails only if a replica's index or root fails; everything per path is
    /// in the report.
    pub fn sync(
        &self,
        a: &mut dyn Replica,
        b: &mut dyn Replica,
        scope: Scope,
    ) -> Result<SyncReport> {
        let mut cycle = Cycle {
            a,
            b,
            report: SyncReport::default(),
            dirty: BTreeSet::new(),
            errors: BTreeMap::new(),
            unmanaged: BTreeSet::new(),
            adopt_asked: BTreeSet::new(),
            last: None,
            delta_min_size: self.delta_min_size,
            max_delete_percent: self.max_delete_percent,
            held: [None, None],
        };
        cycle.sync_clocks()?;
        cycle.scan(&scope)?;
        let mut round = 0;
        loop {
            if !cycle.dirty.is_empty() {
                let dirty = std::mem::take(&mut cycle.dirty);
                cycle.scan(&Scope::Paths(dirty.into_iter().collect()))?;
            }
            let todo = cycle.reconcile()?;
            if todo.is_empty() && cycle.dirty.is_empty() {
                break;
            }
            if round == self.max_rounds {
                let left: BTreeSet<RelPath> = todo
                    .into_iter()
                    .map(|a| a.path)
                    .chain(std::mem::take(&mut cycle.dirty))
                    .collect();
                tracing::warn!(
                    rounds = round,
                    paths = left.len(),
                    "sync did not converge; the rest waits for the next cycle"
                );
                cycle.report.unresolved = left.into_iter().collect();
                break;
            }
            round += 1;
            if !todo.is_empty() {
                cycle.report.rounds += 1;
                cycle.execute(&todo)?;
            }
        }
        if let Some((a, b)) = cycle.last.take() {
            cycle.record_sync(&a, &b, self.tombstone_retention)?;
        }
        let mut report = cycle.report;
        report.errors = cycle.errors.into_iter().collect();
        report.unmanaged = cycle.unmanaged.into_iter().collect();
        report.held_back = cycle.held.into_iter().flatten().collect();
        if report.rounds == 0 && report.is_converged() {
            // Nothing to do (e.g. a daemon cycle triggered by our own writes).
            tracing::debug!("sync cycle done: nothing to do");
        } else {
            tracing::info!(
                rounds = report.rounds,
                applied = report.applied,
                deltas = report.deltas,
                retried = report.retried,
                conflicts = report.conflicts.len(),
                errors = report.errors.len(),
                unresolved = report.unresolved.len(),
                held_back = report
                    .held_back
                    .iter()
                    .map(|h| h.paths.len())
                    .sum::<usize>(),
                "sync cycle done"
            );
        }
        Ok(report)
    }
}

/// The state of one sync cycle.
struct Cycle<'r> {
    a: &'r mut dyn Replica,
    b: &'r mut dyn Replica,
    report: SyncReport,
    /// Paths to rescan before the next round.
    dirty: BTreeSet<RelPath>,
    /// Paths that failed with a non-race error; they and their subtrees are
    /// left alone for the rest of the cycle.
    errors: BTreeMap<RelPath, String>,
    /// Paths skipped as `Unmanaged` (warned about once).
    unmanaged: BTreeSet<RelPath>,
    /// Symlinks a replica was asked to adopt (`-K`) this cycle.
    adopt_asked: BTreeSet<(Side, RelPath)>,
    /// The snapshots the last reconcile compared: the state the cycle ends
    /// in, since nothing ran after it.
    last: Option<(Snapshot, Snapshot)>,
    /// See [`Engine::delta_min_size`].
    delta_min_size: u64,
    /// See [`Engine::max_delete_percent`].
    max_delete_percent: u8,
    /// Per side (A, B): set once the mass-deletion guard tripped for it this
    /// cycle; from then on every deletion there is held back.
    held: [Option<HeldBack>; 2],
}

/// Whether `action` deletes something live on `side`: a pushed tombstone,
/// or a directory replaced by a file or symlink (with everything beneath).
fn deletes_on(action: &Action, side: Side) -> bool {
    let ActionKind::Push {
        from,
        entry,
        target: Some(target),
    } = &action.kind
    else {
        return false;
    };
    *from == side.other()
        && target.is_live()
        && (entry.is_tombstone() || (target.kind == Kind::Dir && entry.kind != Kind::Dir))
}

/// How a step went.
enum Done {
    /// Whether it went as a delta.
    Applied { delta: bool },
    /// Raced with a change: rescan and retry.
    Dirty,
    /// Another error; leave the path alone.
    Failed(String),
}

impl Cycle<'_> {
    /// Lamport clock exchange, before anything is scanned (design §6.4,
    /// issue #2): the replica with the lower clock raises it to the
    /// other's. A replica whose index was lost or restored from a backup
    /// would otherwise hand out counters the peer has already seen from it,
    /// and the peer's older versions would dominate its new changes.
    fn sync_clocks(&mut self) -> Result<()> {
        let (ca, cb) = (self.a.clock()?, self.b.clock()?);
        if ca < cb {
            self.a.witness(cb)?;
        } else if cb < ca {
            self.b.witness(ca)?;
        }
        Ok(())
    }

    fn scan(&mut self, scope: &Scope) -> Result<()> {
        let stats = self.a.scan(scope.clone())?;
        self.note_scan(Side::A, stats);
        let stats = self.b.scan(scope.clone())?;
        self.note_scan(Side::B, stats);
        Ok(())
    }

    fn note_scan(&mut self, side: Side, stats: ScanStats) {
        tracing::debug!(?side, ?stats, "scanned");
        self.dirty.extend(stats.dirty);
        for path in stats.errors {
            // The scanner logged the cause and left the entries unchanged.
            self.errors
                .entry(path)
                .or_insert_with(|| format!("cannot scan on side {side:?} (see log)"));
        }
    }

    /// Reconciles the current indexes. Rescans go to `dirty`; returns the
    /// actions that have steps, without paths that already failed.
    fn reconcile(&mut self) -> Result<Vec<Action>> {
        let mut a = Snapshot::new(self.a.id(), self.a.changes_since(0)?);
        let mut b = Snapshot::new(self.b.id(), self.b.changes_since(0)?);
        let [adopted_a, adopted_b] = self.adopt_dirlinks(&a, &b)?;
        if adopted_a {
            a = Snapshot::new(self.a.id(), self.a.changes_since(0)?);
        }
        if adopted_b {
            b = Snapshot::new(self.b.id(), self.b.changes_since(0)?);
        }
        let now = jiff::Zoned::now().datetime();
        let mut todo = Vec::new();
        for action in reconcile(&a, &b, now) {
            let path = &action.path;
            if self.has_failed(path) {
                continue;
            }
            match &action.kind {
                ActionKind::Rescan => {
                    tracing::debug!(%path, "equal version vectors, different content: rescanning");
                    self.dirty.insert(action.path);
                }
                ActionKind::Skip(SkipReason::Unmanaged(side)) => {
                    if self.unmanaged.insert(path.clone()) {
                        tracing::warn!(%path, ?side, "not synced: unmanaged object (special file or ignored symlink)");
                    }
                }
                ActionKind::Skip(SkipReason::BeneathUnmanaged(_)) => {}
                _ => todo.push(action),
            }
        }
        self.hold_mass_deletes(&mut todo, &a, &b);
        log_resolutions(&todo, [self.a.id(), self.b.id()]);
        self.last = Some((a, b));
        Ok(todo)
    }

    /// The mass-deletion guard: takes out of `todo` the deletions on a side
    /// for which it tripped, this round or earlier in the cycle (so the
    /// share cannot sneak under the limit once other changes applied).
    fn hold_mass_deletes(&mut self, todo: &mut Vec<Action>, a: &Snapshot, b: &Snapshot) {
        for (i, side, snap) in [(0, Side::A, a), (1, Side::B, b)] {
            if self.held[i].is_none() {
                let deletes = todo.iter().filter(|x| deletes_on(x, side)).count();
                let live = snap
                    .entries()
                    .filter(|(p, e)| !p.is_root() && e.is_live())
                    .count();
                if deletes > MASS_DELETE_MIN
                    && deletes * 100 > live * usize::from(self.max_delete_percent)
                {
                    tracing::warn!(
                        ?side,
                        replica = %snap.replica(),
                        deletes,
                        live,
                        max_percent = self.max_delete_percent,
                        "mass deletion held back: too many of the replica's entries would be deleted"
                    );
                    self.held[i] = Some(HeldBack {
                        side,
                        live,
                        paths: Vec::new(),
                    });
                }
            }
            let Some(held) = &mut self.held[i] else {
                continue;
            };
            todo.retain(|x| {
                if !deletes_on(x, side) {
                    return true;
                }
                if let Err(at) = held.paths.binary_search(&x.path) {
                    held.paths.insert(at, x.path.clone());
                }
                false
            });
        }
    }

    /// Tells each replica how the cycle ended at its tombstones, so it can
    /// collect them (design §3).
    fn record_sync(&mut self, a: &Snapshot, b: &Snapshot, retention: Duration) -> Result<()> {
        fn seen(
            ours: &Snapshot,
            peer: &Snapshot,
        ) -> Vec<(RelPath, crate::index::VersionVector, PeerState)> {
            ours.entries()
                .filter(|(_, e)| e.kind == Kind::Tombstone)
                .map(|(path, e)| {
                    let state = match peer.get(path) {
                        None => PeerState::Absent,
                        Some(p) if p.kind == Kind::Tombstone => PeerState::Tombstone(p.vv.clone()),
                        Some(_) => PeerState::Live,
                    };
                    (path.clone(), e.vv.clone(), state)
                })
                .collect()
        }
        let removed = self.a.record_sync(b.replica(), seen(a, b), retention)?;
        self.report.collected[0] = removed.len();
        let removed = self.b.record_sync(a.replica(), seen(b, a), retention)?;
        self.report.collected[1] = removed.len();
        Ok(())
    }

    /// `-K` (design §4.3): where one side has a real directory and the other
    /// a symlink (synced or ignored), the symlink's replica may adopt it as
    /// that directory. Asked once per path and cycle. Returns which sides
    /// adopted something.
    fn adopt_dirlinks(&mut self, a: &Snapshot, b: &Snapshot) -> Result<[bool; 2]> {
        let linkish = |e: &Entry| {
            matches!(
                e.kind,
                Kind::Symlink { .. } | Kind::Unmanaged(UnmanagedReason::IgnoredLink)
            )
        };
        let mut asks = Vec::new();
        for (path, ea) in a.entries() {
            let Some(eb) = b.get(path) else { continue };
            if linkish(ea) && eb.kind == Kind::Dir {
                asks.push((Side::A, path.clone()));
            } else if linkish(eb) && ea.kind == Kind::Dir {
                asks.push((Side::B, path.clone()));
            }
        }
        let mut adopted = [false; 2];
        for (side, path) in asks {
            if self.has_failed(&path) || !self.adopt_asked.insert((side, path.clone())) {
                continue;
            }
            let replica: &mut dyn Replica = match side {
                Side::A => &mut *self.a,
                Side::B => &mut *self.b,
            };
            match replica.adopt(&path) {
                Ok(true) => adopted[side as usize] = true,
                Ok(false) => {}
                Err(e) if is_fatal(&e) => return Err(e),
                Err(e) => {
                    tracing::debug!(%path, ?side, error = %e, "cannot adopt symlink; rescanning");
                    self.dirty.insert(path);
                }
            }
        }
        Ok(adopted)
    }

    /// `path` or one of its ancestors failed with an error this cycle.
    fn has_failed(&self, path: &RelPath) -> bool {
        self.errors.keys().any(|p| p == path || is_beneath(path, p))
    }

    /// Runs the steps for `todo`, phase by phase.
    fn execute(&mut self, todo: &[Action]) -> Result<()> {
        for action in todo {
            if let ActionKind::Resurrect(_) = action.kind {
                self.report.resurrected.push(action.path.clone());
            }
        }
        // Paths with a step that did not apply: their later steps are skipped.
        let mut stopped: BTreeSet<RelPath> = BTreeSet::new();
        for phase in plan(todo) {
            tracing::debug!(phase = ?phase.kind, steps = phase.steps.len(), "running phase");
            for step in phase.steps {
                if stopped.contains(&step.path) || self.has_failed(&step.path) {
                    continue;
                }
                let path = step.path.clone();
                let rename = match &step.op {
                    Op::RenameToConflict { to } => Some((step.side, to.clone())),
                    _ => None,
                };
                match self.run(step)? {
                    Done::Applied { delta } => {
                        self.report.applied += 1;
                        self.report.deltas += usize::from(delta);
                        if let Some((side, copy)) = rename {
                            self.report.conflicts.push(ConflictCopy {
                                side,
                                path: path.clone(),
                                copy: copy.clone(),
                            });
                        }
                    }
                    Done::Dirty => {
                        self.report.retried += 1;
                        stopped.insert(path.clone());
                        self.dirty.insert(path);
                    }
                    Done::Failed(why) => {
                        stopped.insert(path.clone());
                        self.errors.insert(path, why);
                    }
                }
            }
        }
        Ok(())
    }

    /// Applies one step on its side; a file's content is streamed from the
    /// other side.
    fn run(&mut self, step: Step) -> Result<Done> {
        let Step {
            side,
            path,
            op,
            pre,
            source,
        } = step;
        let (dst, src): (&mut dyn Replica, &dyn Replica) = match side {
            Side::A => (&mut *self.a, &*self.b),
            Side::B => (&mut *self.b, &*self.a),
        };
        tracing::debug!(%path, ?side, ?op, "apply");
        let mut delta = false;
        let result = match (&op, source) {
            (Op::WriteFile { .. }, Some(source)) => write_file(
                dst,
                src,
                &path,
                op,
                pre,
                &source,
                self.delta_min_size,
                &mut delta,
            ),
            _ => dst.apply(&path, op, pre, None),
        };
        match result {
            Ok(Outcome::Applied(_)) => Ok(Done::Applied { delta }),
            Ok(Outcome::PreconditionFailed(_)) => {
                tracing::debug!(%path, ?side, "precondition failed; rescanning");
                Ok(Done::Dirty)
            }
            Ok(Outcome::Preserved { conflict }) => {
                tracing::warn!(%path, ?side, %conflict, "a concurrent change was kept as a conflict copy");
                Ok(Done::Dirty)
            }
            Err(e) if e.is_unstable() || e.is_not_found() => {
                tracing::debug!(%path, ?side, error = %e, "path changed during the step; rescanning");
                Ok(Done::Dirty)
            }
            Err(e) if is_fatal(&e) => Err(e),
            Err(e) => {
                tracing::error!(%path, ?side, error = %e, "cannot sync path");
                Ok(Done::Failed(e.to_string()))
            }
        }
    }
}

/// Writes the file `source` of `src` at `path` on `dst`: as a block-level
/// delta if that can pay off (design §7.1; sets `delta`), else whole.
#[allow(clippy::too_many_arguments)]
fn write_file(
    dst: &mut dyn Replica,
    src: &dyn Replica,
    path: &RelPath,
    op: Op,
    pre: Precondition,
    source: &Entry,
    min_size: u64,
    delta: &mut bool,
) -> Result<Outcome> {
    if let Some(plan) = plan_delta(dst, src, path, &pre, source, min_size)? {
        let needed = plan.needed();
        tracing::debug!(
            %path,
            blocks = plan.reuse.len(),
            sent = needed.len(),
            "delta transfer"
        );
        *delta = true;
        // Every block is at the destination already: the source need not
        // be read (the destination checks each block and the whole hash).
        let mut data: Box<dyn Read> = if needed.is_empty() {
            Box::new(std::io::empty())
        } else {
            src.read_blocks(path, &source.kind, &needed)?
        };
        return dst.apply_delta(path, op, pre, &plan, &mut data);
    }
    let mut reader = src.open_read(path, source)?;
    dst.apply(path, op, pre, Some(&mut reader as &mut dyn Read))
}

/// The delta for writing `source` over `dst`'s file at `path`, if one side
/// is remote, both files are at least `min_size`, and both replicas give a
/// block list.
fn plan_delta(
    dst: &dyn Replica,
    src: &dyn Replica,
    path: &RelPath,
    pre: &Precondition,
    source: &Entry,
    min_size: u64,
) -> Result<Option<Delta>> {
    if !(dst.is_remote() || src.is_remote()) {
        return Ok(None);
    }
    let Precondition::Matches {
        kind: old @ Kind::File { size: old_size, .. },
        ..
    } = pre
    else {
        return Ok(None);
    };
    let Kind::File { size, .. } = source.kind else {
        return Ok(None);
    };
    if size < min_size || *old_size < min_size {
        return Ok(None);
    }
    let Some(new) = src.blocks(path, &source.kind)? else {
        return Ok(None);
    };
    let Some(have) = dst.blocks(path, old)? else {
        return Ok(None);
    };
    // Even if no block can be reused: the block lists cost 0.025 % of the
    // file, and every replaced file takes the same path.
    Ok(Some(Delta::plan(&have, new)))
}

/// Errors that are not about one path: the replica itself, or the
/// connection to it, is unusable.
fn is_fatal(e: &Error) -> bool {
    // Db, BadIndex and Protocol, local or reported by a remote replica.
    matches!(e.remote_kind(), RemoteKind::Index | RemoteKind::Protocol)
}
