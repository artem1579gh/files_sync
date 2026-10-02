//! The executor: one sync cycle between two replicas (design §6.3, §6.4).
//!
//! [`Engine::sync`] scans both replicas, reconciles their indexes, and runs
//! the planned steps phase by phase through [`Replica::apply`], streaming a
//! file's content from [`Replica::open_read`] on the other side. It does no
//! I/O itself: every check that matters (the CAS precondition, the source
//! content's stability and hash) runs inside the replicas.
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

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

use crate::engine::conflict::{ConflictCopy, log_resolutions};
use crate::engine::{
    Action, ActionKind, Side, SkipReason, Snapshot, Step, is_beneath, plan, reconcile,
};
use crate::error::{Error, Result};
use crate::fs::RelPath;
use crate::replica::{Op, Outcome, Replica};
use crate::scan::{ScanStats, Scope};

/// Rounds per sync cycle (§6.4).
pub const MAX_ROUNDS: usize = 5;

/// Runs sync cycles between two replicas.
#[derive(Clone, Debug)]
pub struct Engine {
    max_rounds: usize,
}

impl Default for Engine {
    fn default() -> Engine {
        Engine {
            max_rounds: MAX_ROUNDS,
        }
    }
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
}

impl SyncReport {
    /// Both replicas hold the same synced state (nothing unresolved, no
    /// errors).
    pub fn is_converged(&self) -> bool {
        self.unresolved.is_empty() && self.errors.is_empty()
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
        };
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
        let mut report = cycle.report;
        report.errors = cycle.errors.into_iter().collect();
        report.unmanaged = cycle.unmanaged.into_iter().collect();
        if report.rounds == 0 && report.is_converged() {
            // Nothing to do (e.g. a daemon cycle triggered by our own writes).
            tracing::debug!("sync cycle done: nothing to do");
        } else {
            tracing::info!(
                rounds = report.rounds,
                applied = report.applied,
                retried = report.retried,
                conflicts = report.conflicts.len(),
                errors = report.errors.len(),
                unresolved = report.unresolved.len(),
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
}

/// How a step went.
enum Done {
    Applied,
    /// Raced with a change: rescan and retry.
    Dirty,
    /// Another error; leave the path alone.
    Failed(String),
}

impl Cycle<'_> {
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
        let a = Snapshot::new(self.a.id(), self.a.changes_since(0)?);
        let b = Snapshot::new(self.b.id(), self.b.changes_since(0)?);
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
        log_resolutions(&todo, [self.a.id(), self.b.id()]);
        Ok(todo)
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
                    Done::Applied => {
                        self.report.applied += 1;
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
        let result = match (&op, source) {
            (Op::WriteFile { .. }, Some(source)) => match src.open_read(&path, &source) {
                Ok(mut reader) => dst.apply(&path, op, pre, Some(&mut reader as &mut dyn Read)),
                Err(e) => Err(e),
            },
            _ => dst.apply(&path, op, pre, None),
        };
        match result {
            Ok(Outcome::Applied(_)) => Ok(Done::Applied),
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

/// Errors that are not about one path: the replica itself is unusable.
fn is_fatal(e: &Error) -> bool {
    matches!(e, Error::Db(_) | Error::BadIndex { .. })
}
