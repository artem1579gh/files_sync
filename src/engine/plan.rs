//! The planner: reconciler actions → ordered replica operations (design §6.3).
//!
//! [`plan`] is pure. Each [`Step`] is one [`Replica::apply`] call: an [`Op`]
//! with the [`Precondition`] that guards it, on one side. Phases run in
//! order:
//!
//! 1. [`PhaseKind::Conflicts`]: losing files and symlinks are renamed to
//!    their conflict names (`Op::RenameToConflict`).
//! 2. [`PhaseKind::Deletes`]: deletes, depth descending (children before
//!    their directory), including the delete half of a type change to or
//!    from a directory.
//! 3. [`PhaseKind::Creates`]: creates and updates, depth ascending
//!    (directories before their children), including the create half of a
//!    type change and every `SetMeta`.
//!
//! A path has at most one step per phase and side, and its steps only make
//! sense in order: when one does not apply, the executor should mark the
//! path dirty and skip its later steps. (Skipping is not needed for safety:
//! a later step's precondition fails anyway, e.g. a create expects `Absent`
//! where a rename or delete did not happen.)
//!
//! [`Replica::apply`]: crate::replica::Replica::apply

use std::cmp::Reverse;

use crate::engine::Side;
use crate::engine::reconcile::{Action, ActionKind};
use crate::fs::RelPath;
use crate::fs::commit::FileMeta;
use crate::index::{Entry, Kind, VersionVector};
use crate::replica::{Op, Precondition};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PhaseKind {
    Conflicts,
    Deletes,
    Creates,
}

/// Steps that may run in the listed order once the previous phase is done.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Phase {
    pub kind: PhaseKind,
    pub steps: Vec<Step>,
}

/// One `apply` on one side.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    /// The replica to apply to.
    pub side: Side,
    pub path: RelPath,
    pub op: Op,
    pub pre: Precondition,
    /// For `Op::WriteFile`: the content is read from the other side
    /// (`side.other()`) at the same path, which must still be this entry
    /// (`Replica::open_read`).
    pub source: Option<Entry>,
}

/// Orders `actions` into phases (§6.3). Only non-empty phases are returned.
/// `Skip` and `Rescan` actions produce no steps; the caller handles them.
pub fn plan(actions: &[Action]) -> Vec<Phase> {
    let mut p = Planner::default();
    for action in actions {
        let path = &action.path;
        match &action.kind {
            ActionKind::Push {
                from,
                entry,
                target,
            } => p.push(path, from.other(), entry, &entry.vv, target.as_ref()),
            ActionKind::MergeVv { vv, mtime_ns, a, b } => {
                for (side, e) in [(Side::A, a), (Side::B, b)] {
                    p.creates
                        .push(set_meta(side, path, e, e.mode, *mtime_ns, vv));
                }
            }
            ActionKind::Conflict(r) | ActionKind::Resurrect(r) => {
                let side = r.winner.other();
                let target = match &r.conflict_name {
                    Some(to) => {
                        p.conflicts.push(Step {
                            side,
                            path: path.clone(),
                            op: Op::RenameToConflict { to: to.clone() },
                            pre: Precondition::matching(&r.loser),
                            source: None,
                        });
                        None
                    }
                    None => Some(&r.loser),
                };
                p.push(path, side, &r.entry, &r.vv, target);
            }
            ActionKind::Skip(_) | ActionKind::Rescan => {}
        }
    }
    p.finish()
}

#[derive(Default)]
struct Planner {
    conflicts: Vec<Step>,
    deletes: Vec<Step>,
    creates: Vec<Step>,
}

impl Planner {
    /// Makes side `to` hold `entry` with version vector `vv`, where it now
    /// holds `target`.
    fn push(
        &mut self,
        path: &RelPath,
        to: Side,
        entry: &Entry,
        vv: &VersionVector,
        target: Option<&Entry>,
    ) {
        let target = target.filter(|t| !t.is_tombstone());
        if entry.is_unmanaged() || target.is_some_and(Entry::is_unmanaged) {
            debug_assert!(false, "unmanaged entries are never pushed: {path}");
            return;
        }
        match target {
            None if entry.is_tombstone() => {}
            Some(t) if entry.is_tombstone() => self.deletes.push(remove(to, path, t, vv)),
            None => self
                .creates
                .push(create(to, path, entry, vv, Precondition::Absent)),
            Some(t) => {
                let (dir, was_dir) = (entry.kind == Kind::Dir, t.kind == Kind::Dir);
                if dir != was_dir {
                    // A type change to or from a directory: delete, then
                    // create. The tombstone keeps the old vector, so if the
                    // create does not happen the pushed entry still dominates.
                    self.deletes.push(remove(to, path, t, &t.vv));
                    self.creates
                        .push(create(to, path, entry, vv, Precondition::Absent));
                } else if entry.kind == t.kind {
                    // Same content (or both directories): metadata and vv.
                    self.creates
                        .push(set_meta(to, path, t, entry.mode, entry.mtime_ns, vv));
                } else {
                    // File ↔ symlink, or new content: replaced in one commit.
                    let pre = Precondition::matching(t);
                    self.creates.push(create(to, path, entry, vv, pre));
                }
            }
        }
    }

    fn finish(mut self) -> Vec<Phase> {
        self.conflicts.sort_by(|x, y| x.path.cmp(&y.path));
        self.deletes
            .sort_by_key(|s| (Reverse(s.path.depth()), s.path.clone(), s.side));
        self.creates
            .sort_by_key(|s| (s.path.depth(), s.path.clone(), s.side));
        [
            (PhaseKind::Conflicts, self.conflicts),
            (PhaseKind::Deletes, self.deletes),
            (PhaseKind::Creates, self.creates),
        ]
        .into_iter()
        .filter(|(_, steps)| !steps.is_empty())
        .map(|(kind, steps)| Phase { kind, steps })
        .collect()
    }
}

/// Deletes the file, symlink or directory `t`, leaving a tombstone with `vv`.
fn remove(side: Side, path: &RelPath, t: &Entry, vv: &VersionVector) -> Step {
    let vv = vv.clone();
    let op = match t.kind {
        Kind::Dir => Op::Rmdir { vv },
        _ => Op::Delete { vv },
    };
    Step {
        side,
        path: path.clone(),
        op,
        pre: Precondition::matching(t),
        source: None,
    }
}

/// Writes `entry`'s object with `vv`; a file's content comes from the other side.
fn create(
    side: Side,
    path: &RelPath,
    entry: &Entry,
    vv: &VersionVector,
    pre: Precondition,
) -> Step {
    let vv = vv.clone();
    let (op, source) = match &entry.kind {
        Kind::File { hash, .. } => (
            Op::WriteFile {
                meta: FileMeta {
                    mode: entry.mode,
                    mtime_ns: entry.mtime_ns,
                },
                hash: *hash,
                vv,
            },
            Some(entry.clone()),
        ),
        Kind::Dir => (
            Op::Mkdir {
                mode: entry.mode,
                mtime_ns: entry.mtime_ns,
                vv,
            },
            None,
        ),
        Kind::Symlink { target } => (
            Op::Symlink {
                target: target.clone(),
                mtime_ns: entry.mtime_ns,
                vv,
            },
            None,
        ),
        Kind::Tombstone | Kind::Unmanaged(_) => unreachable!("not a live entry: {path}"),
    };
    Step {
        side,
        path: path.clone(),
        op,
        pre,
        source,
    }
}

/// Sets mode, mtime and vv on `t`, whose content stays.
fn set_meta(
    side: Side,
    path: &RelPath,
    t: &Entry,
    mode: u32,
    mtime_ns: i64,
    vv: &VersionVector,
) -> Step {
    Step {
        side,
        path: path.clone(),
        op: Op::SetMeta {
            mode,
            mtime_ns,
            vv: vv.clone(),
        },
        pre: Precondition::matching(t),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReplicaId;
    use crate::engine::{Snapshot, reconcile};
    use jiff::civil::date;

    const IA: ReplicaId = ReplicaId(0x1111_2222_3333_4444);
    const IB: ReplicaId = ReplicaId(0xaaaa_bbbb_cccc_dddd);

    fn rp(s: &str) -> RelPath {
        RelPath::new(s).unwrap()
    }

    fn vv(a: u64, b: u64) -> VersionVector {
        [(IA, a), (IB, b)].into_iter().collect()
    }

    fn file(hash: u8, vv: VersionVector) -> Entry {
        Entry::new(
            Kind::File {
                size: 1,
                hash: [hash; 32],
            },
            0o644,
            100,
            vv,
        )
    }

    fn dir(vv: VersionVector) -> Entry {
        Entry::new(Kind::Dir, 0o755, 0, vv)
    }

    fn link(t: &str, vv: VersionVector) -> Entry {
        Entry::new(
            Kind::Symlink {
                target: t.as_bytes().to_vec(),
            },
            0o777,
            0,
            vv,
        )
    }

    fn tomb(vv: VersionVector) -> Entry {
        Entry::new(Kind::Tombstone, 0, 0, vv)
    }

    fn run(a: &[(&str, Entry)], b: &[(&str, Entry)]) -> (Vec<Action>, Vec<Phase>) {
        let snap = |id, es: &[(&str, Entry)]| {
            Snapshot::new(id, es.iter().map(|(p, e)| (rp(p), e.clone())))
        };
        let actions = reconcile(&snap(IA, a), &snap(IB, b), date(2026, 10, 2).at(9, 5, 7, 0));
        let phases = plan(&actions);
        (actions, phases)
    }

    /// `(phase, side, path, op name, precondition is Absent)` per step.
    fn summary(phases: &[Phase]) -> Vec<(PhaseKind, Side, String, &'static str, bool)> {
        phases
            .iter()
            .flat_map(|ph| {
                ph.steps.iter().map(move |s| {
                    let op = match s.op {
                        Op::WriteFile { .. } => "write",
                        Op::Mkdir { .. } => "mkdir",
                        Op::Symlink { .. } => "symlink",
                        Op::Delete { .. } => "delete",
                        Op::Rmdir { .. } => "rmdir",
                        Op::RenameToConflict { .. } => "rename",
                        Op::SetMeta { .. } => "setmeta",
                    };
                    let path = String::from_utf8(s.path.as_bytes().to_vec()).unwrap();
                    (ph.kind, s.side, path, op, s.pre == Precondition::Absent)
                })
            })
            .collect()
    }

    /// A newer version on A over each kind of B entry: which steps.
    /// `(phase, op name, precondition is Absent)`.
    type Want = Vec<(PhaseKind, &'static str, bool)>;

    #[test]
    fn push_matrix() {
        use PhaseKind::{Creates as C, Deletes as D};
        let new = vv(2, 1);
        let old = vv(1, 1);
        let cases: Vec<(Entry, Entry, Want)> = vec![
            // Same kind.
            (
                file(1, new.clone()),
                file(2, old.clone()),
                vec![(C, "write", false)],
            ),
            (
                file(1, new.clone()),
                file(1, old.clone()),
                vec![(C, "setmeta", false)],
            ),
            (
                link("x", new.clone()),
                link("y", old.clone()),
                vec![(C, "symlink", false)],
            ),
            (
                link("x", new.clone()),
                link("x", old.clone()),
                vec![(C, "setmeta", false)],
            ),
            (
                dir(new.clone()),
                dir(old.clone()),
                vec![(C, "setmeta", false)],
            ),
            // File ↔ symlink: one replacing commit.
            (
                file(1, new.clone()),
                link("x", old.clone()),
                vec![(C, "write", false)],
            ),
            (
                link("x", new.clone()),
                file(1, old.clone()),
                vec![(C, "symlink", false)],
            ),
            // To or from a directory: split.
            (
                file(1, new.clone()),
                dir(old.clone()),
                vec![(D, "rmdir", false), (C, "write", true)],
            ),
            (
                link("x", new.clone()),
                dir(old.clone()),
                vec![(D, "rmdir", false), (C, "symlink", true)],
            ),
            (
                dir(new.clone()),
                file(1, old.clone()),
                vec![(D, "delete", false), (C, "mkdir", true)],
            ),
            (
                dir(new.clone()),
                link("x", old.clone()),
                vec![(D, "delete", false), (C, "mkdir", true)],
            ),
            // Over a tombstone: a create.
            (
                file(1, new.clone()),
                tomb(old.clone()),
                vec![(C, "write", true)],
            ),
            (
                dir(new.clone()),
                tomb(old.clone()),
                vec![(C, "mkdir", true)],
            ),
            (
                link("x", new.clone()),
                tomb(old.clone()),
                vec![(C, "symlink", true)],
            ),
            // Deletes.
            (
                tomb(new.clone()),
                file(1, old.clone()),
                vec![(D, "delete", false)],
            ),
            (
                tomb(new.clone()),
                link("x", old.clone()),
                vec![(D, "delete", false)],
            ),
            (
                tomb(new.clone()),
                dir(old.clone()),
                vec![(D, "rmdir", false)],
            ),
        ];
        for (ea, eb, want) in cases {
            let (_, phases) = run(&[("p", ea.clone())], &[("p", eb.clone())]);
            let got: Vec<_> = summary(&phases)
                .into_iter()
                .map(|(ph, side, path, op, absent)| {
                    assert_eq!((side, path.as_str()), (Side::B, "p"));
                    (ph, op, absent)
                })
                .collect();
            assert_eq!(got, want, "{ea:?} over {eb:?}");
            for step in phases.iter().flat_map(|p| &p.steps) {
                // The delete half of a type change keeps B's old vv; every
                // other step carries A's.
                let step_vv = match &step.op {
                    Op::WriteFile { vv, .. }
                    | Op::Mkdir { vv, .. }
                    | Op::Symlink { vv, .. }
                    | Op::Delete { vv }
                    | Op::Rmdir { vv }
                    | Op::SetMeta { vv, .. } => vv,
                    Op::RenameToConflict { .. } => unreachable!(),
                };
                let split = ea.is_live() && (ea.kind == Kind::Dir) != (eb.kind == Kind::Dir);
                let deleting = matches!(step.op, Op::Delete { .. } | Op::Rmdir { .. });
                let want_vv = if split && deleting { &old } else { &new };
                assert_eq!(step_vv, want_vv, "{ea:?} over {eb:?}");
                if step.pre != Precondition::Absent {
                    assert_eq!(step.pre, Precondition::matching(&eb));
                }
                if let Op::WriteFile { meta, hash, .. } = &step.op {
                    assert_eq!(step.source.as_ref(), Some(&ea));
                    assert_eq!((meta.mode, meta.mtime_ns), (ea.mode, ea.mtime_ns));
                    assert!(matches!(ea.kind, Kind::File { hash: h, .. } if h == *hash));
                } else {
                    assert_eq!(step.source, None);
                }
            }
        }
    }

    #[test]
    fn ordering() {
        use PhaseKind::*;
        let (new, old) = (vv(2, 1), vv(1, 1));
        let a = [
            // New nested tree.
            ("n", dir(new.clone())),
            ("n/m", dir(new.clone())),
            ("n/m/f", file(1, new.clone())),
            // Dir → file: its child is deleted first.
            ("t", file(1, new.clone())),
            ("t/c", tomb(new.clone())),
            ("t/c/g", tomb(new.clone())),
            // Deleted tree.
            ("x", tomb(new.clone())),
            ("x/y", tomb(new.clone())),
            // Concurrent edit: B wins by mtime, A's file is renamed.
            ("c", file(1, vv(2, 1))),
            // Concurrent, same content: merged on both sides.
            ("s", file(7, vv(2, 1))),
        ];
        let mut cb = file(2, vv(1, 2));
        cb.mtime_ns = 200;
        let b = [
            ("t", dir(old.clone())),
            ("t/c", dir(old.clone())),
            ("t/c/g", file(3, old.clone())),
            ("x", dir(old.clone())),
            ("x/y", file(3, old.clone())),
            ("c", cb),
            ("s", file(7, vv(1, 2))),
        ];
        let (_, phases) = run(&a, &b);
        let got = summary(&phases);
        let want = [
            (Conflicts, Side::A, "c", "rename", false),
            (Deletes, Side::B, "t/c/g", "delete", false),
            (Deletes, Side::B, "t/c", "rmdir", false),
            (Deletes, Side::B, "x/y", "delete", false),
            (Deletes, Side::B, "t", "rmdir", false),
            (Deletes, Side::B, "x", "rmdir", false),
            (Creates, Side::A, "c", "write", true),
            (Creates, Side::B, "n", "mkdir", true),
            (Creates, Side::A, "s", "setmeta", false),
            (Creates, Side::B, "s", "setmeta", false),
            (Creates, Side::B, "t", "write", true),
            (Creates, Side::B, "n/m", "mkdir", true),
            (Creates, Side::B, "n/m/f", "write", true),
        ];
        let got: Vec<_> = got
            .iter()
            .map(|(k, s, p, o, ab)| (*k, *s, p.as_str(), *o, *ab))
            .collect();
        assert_eq!(got, want);

        // The conflict: A's loser is renamed, then gets B's content with the
        // merged vector plus B's bump.
        let rename = &phases[0].steps[0];
        let Op::RenameToConflict { to } = &rename.op else {
            panic!()
        };
        assert!(
            to.as_bytes()
                .starts_with(b"c.sync-conflict-20261002-090507-1111222")
        );
        assert_eq!(rename.pre, Precondition::matching(&file(1, vv(2, 1))));
        let write = &phases[2].steps[0];
        assert!(matches!(&write.op, Op::WriteFile { vv: v, .. } if *v == vv(2, 3)));
        assert_eq!(write.source.as_ref().map(|e| e.vv.clone()), Some(vv(1, 2)));
    }

    #[test]
    fn resurrection_plan() {
        use PhaseKind::*;
        // A deleted `d`; B added `d/e/new` and B's `d/old` was deleted on A.
        // A replaced dir `k` with a file; B added `k/new`.
        let a = [
            ("d", tomb(vv(3, 1))),
            ("d/old", tomb(vv(3, 1))),
            ("d/e", tomb(vv(3, 1))),
            ("k", file(1, vv(3, 1))),
        ];
        let b = [
            ("d", dir(vv(1, 1))),
            ("d/old", file(1, vv(1, 1))),
            ("d/e", dir(vv(1, 1))),
            ("d/e/new", file(2, vv(1, 4))),
            ("k", dir(vv(1, 1))),
            ("k/new", file(2, vv(1, 4))),
        ];
        let (_, phases) = run(&a, &b);
        let got = summary(&phases);
        let got: Vec<_> = got
            .iter()
            .map(|(k, s, p, o, ab)| (*k, *s, p.as_str(), *o, *ab))
            .collect();
        assert_eq!(
            got,
            [
                (Conflicts, Side::A, "k", "rename", false),
                (Deletes, Side::B, "d/old", "delete", false),
                (Creates, Side::A, "d", "mkdir", true),
                (Creates, Side::A, "k", "mkdir", true),
                (Creates, Side::A, "d/e", "mkdir", true),
                (Creates, Side::A, "k/new", "write", true),
                (Creates, Side::A, "d/e/new", "write", true),
            ]
        );
        // The resurrected directories dominate A's tombstones.
        for step in &phases[2].steps {
            if let Op::Mkdir { vv: v, .. } = &step.op {
                assert_eq!(*v, vv(3, 4));
            }
        }
    }
}
