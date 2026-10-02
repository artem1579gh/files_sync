//! The reconciler: what to do for each path (design §6.1, §6.2, §6.3).
//!
//! [`reconcile`] is pure. It compares two [`IndexView`]s path by path and
//! returns one [`Action`] for every path where something needs doing (or
//! where something is deliberately not done: [`ActionKind::Skip`],
//! [`ActionKind::Rescan`]). Paths that are in sync get no action.
//!
//! Every resolution is safe by construction: the side that changes is always
//! guarded by its own current entry (the replica checks it, §7), and nothing
//! is ever recorded as "seen" on a side that does not hold the content. In
//! particular a conflict or a resurrection writes only to the losing side:
//! the winner learns the merged version vector in the next round, through an
//! ordinary dominating push of identical content (an index-only `SetMeta`).

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Bound;

use jiff::ToSpan;
use jiff::civil::DateTime;

use crate::config::ReplicaId;
use crate::engine::{IndexView, Side, is_beneath};
use crate::fs::{RelPath, conflict_name};
use crate::index::{Entry, Kind, Ord4, VersionVector};

/// What to do at one path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Action {
    pub path: RelPath,
    pub kind: ActionKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActionKind {
    /// Make the other side (`from.other()`) match `entry`, which is `from`'s
    /// current entry; a tombstone propagates a delete. `target` is the other
    /// side's current entry (`None` or a tombstone when it has nothing live),
    /// the precondition of the change.
    Push {
        from: Side,
        entry: Entry,
        target: Option<Entry>,
    },
    /// Concurrent versions with the same content (kind, hash or target,
    /// mode): both sides record `vv`, the merged vector, and `mtime_ns`, the
    /// newer of the two mtimes. `a` and `b` are the current entries.
    MergeVv {
        vv: VersionVector,
        mtime_ns: i64,
        a: Entry,
        b: Entry,
    },
    /// Concurrent versions that differ (§6.2).
    Conflict(Resolution),
    /// A directory delete that would destroy something the deleting side
    /// has not seen (§6.3): the directory is kept and recreated on the side
    /// that deleted it (or replaced it with a file or symlink, which is
    /// renamed to a conflict copy first).
    Resurrect(Resolution),
    /// Deliberately not synced.
    Skip(SkipReason),
    /// Equal version vectors but different content: the indexes are
    /// inconsistent, rescan the path on both sides (§6.1).
    Rescan,
}

/// How a conflict or resurrection is resolved: the loser side
/// (`winner.other()`) ends up holding `entry` with version vector `vv`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolution {
    pub winner: Side,
    /// The winner's current entry (its own version vector).
    pub entry: Entry,
    /// The merged version vector plus a bump by the winner's replica: it
    /// dominates both sides' entries.
    pub vv: VersionVector,
    /// The loser side's current entry.
    pub loser: Entry,
    /// For a file or symlink loser: the sibling it is renamed to before the
    /// winner's version is written. `None` for a directory loser (only a
    /// mode differs; it is overwritten in place, §5.4) and for a tombstone.
    pub conflict_name: Option<RelPath>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkipReason {
    /// That side's entry is `Unmanaged` while the other side has a live
    /// one; the unmanaged object is never overwritten, and it is not a
    /// deletion either (§3). Worth a warning.
    Unmanaged(Side),
    /// The path lies beneath this path, which is `Unmanaged` on a side: the
    /// whole subtree is left alone.
    BeneathUnmanaged(RelPath),
}

/// Compares the two indexes and decides what to do for every path.
///
/// `now` (local wall-clock time) is only used for conflict names
/// (`fs::conflict_name`); a name that is live on either side, or already
/// chosen in this call, is retried one second later. Actions are in path
/// order.
pub fn reconcile(a: &dyn IndexView, b: &dyn IndexView, now: DateTime) -> Vec<Action> {
    let views = Views { a, b };
    let paths: BTreeSet<&RelPath> = a.entries().chain(b.entries()).map(|(p, _)| p).collect();
    let mut names = Names {
        views,
        now,
        chosen: HashSet::new(),
    };
    let mut actions = BTreeMap::new();
    let mut unmanaged: HashSet<&RelPath> = HashSet::new();
    for path in paths {
        if path.is_root() {
            continue;
        }
        let (ea, eb) = (a.get(path), b.get(path));
        if ea.is_some_and(Entry::is_unmanaged) || eb.is_some_and(Entry::is_unmanaged) {
            unmanaged.insert(path);
        }
        let Some(decision) = decide(ea, eb, a.replica(), b.replica()) else {
            continue;
        };
        let kind = match ancestors(path).find(|p| unmanaged.contains(p)) {
            Some(anc) => ActionKind::Skip(SkipReason::BeneathUnmanaged(anc)),
            None => decision.into_action(path, &views, &mut names),
        };
        actions.insert(path.clone(), kind);
    }
    resurrect(&views, &mut actions, &mut names);
    actions
        .into_iter()
        .map(|(path, kind)| Action { path, kind })
        .collect()
}

#[derive(Clone, Copy)]
struct Views<'v> {
    a: &'v dyn IndexView,
    b: &'v dyn IndexView,
}

impl<'v> Views<'v> {
    fn side(&self, side: Side) -> &'v dyn IndexView {
        match side {
            Side::A => self.a,
            Side::B => self.b,
        }
    }

    fn id(&self, side: Side) -> ReplicaId {
        self.side(side).replica()
    }

    fn entry(&self, side: Side, path: &RelPath) -> Entry {
        self.side(side)
            .get(path)
            .cloned()
            .expect("decided paths have an entry on this side")
    }
}

/// The §6.1 table for one path, before entries are attached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decision {
    Push(Side),
    MergeVv,
    Conflict { winner: Side },
    Skip(Side),
    Rescan,
}

fn decide(
    ea: Option<&Entry>,
    eb: Option<&Entry>,
    ia: ReplicaId,
    ib: ReplicaId,
) -> Option<Decision> {
    use Decision::*;
    let live = |e: Option<&Entry>| e.is_some_and(Entry::is_live);
    match (ea, eb) {
        (Some(e), _) if e.is_unmanaged() => live(eb).then_some(Skip(Side::A)),
        (_, Some(e)) if e.is_unmanaged() => live(ea).then_some(Skip(Side::B)),
        // Present on one side only. A tombstone there needs no push.
        (Some(e), None) => e.is_live().then_some(Push(Side::A)),
        (None, Some(e)) => e.is_live().then_some(Push(Side::B)),
        (None, None) => None,
        // Deleted on both sides. Differing vectors are left alone: neither
        // side has anything to change, and `SetMeta` cannot apply to a
        // tombstone (see the T12 notes on tombstone GC).
        (Some(ea), Some(eb)) if ea.is_tombstone() && eb.is_tombstone() => None,
        (Some(ea), Some(eb)) => match ea.vv.compare(&eb.vv) {
            Ord4::Equal => (!same_version(ea, eb)).then_some(Rescan),
            Ord4::Dominates => Some(Push(Side::A)),
            Ord4::Dominated => Some(Push(Side::B)),
            Ord4::Concurrent if same_content(ea, eb) => Some(MergeVv),
            // The modification wins over a concurrent delete.
            Ord4::Concurrent if ea.is_tombstone() => Some(Push(Side::B)),
            Ord4::Concurrent if eb.is_tombstone() => Some(Push(Side::A)),
            Ord4::Concurrent => Some(Conflict {
                winner: winner(ea, eb, ia, ib),
            }),
        },
    }
}

/// Same content for §6.1: kind (with hash or target) and mode. The mtime is
/// not part of it.
fn same_content(a: &Entry, b: &Entry) -> bool {
    a.kind == b.kind && a.mode == b.mode
}

/// What equal version vectors promise: the same content and, for a file,
/// the same mtime (the only kind whose mtime is synced, §3).
fn same_version(a: &Entry, b: &Entry) -> bool {
    same_content(a, b) && (!matches!(a.kind, Kind::File { .. }) || a.mtime_ns == b.mtime_ns)
}

/// §6.2: Dir > File > Symlink for a type conflict; otherwise the newer
/// mtime, and on a tie the higher replica ID.
fn winner(a: &Entry, b: &Entry, ia: ReplicaId, ib: ReplicaId) -> Side {
    fn rank(k: &Kind) -> u8 {
        match k {
            Kind::Dir => 2,
            Kind::File { .. } => 1,
            _ => 0,
        }
    }
    let key = |e: &Entry, id| (rank(&e.kind), e.mtime_ns, id);
    if key(a, ia) >= key(b, ib) {
        Side::A
    } else {
        Side::B
    }
}

impl Decision {
    fn into_action(self, path: &RelPath, views: &Views, names: &mut Names) -> ActionKind {
        let target = |side: Side| views.side(side).get(path).cloned();
        match self {
            Decision::Push(from) => ActionKind::Push {
                from,
                entry: views.entry(from, path),
                target: target(from.other()),
            },
            Decision::MergeVv => {
                let (a, b) = (views.entry(Side::A, path), views.entry(Side::B, path));
                ActionKind::MergeVv {
                    vv: a.vv.merge(&b.vv),
                    mtime_ns: a.mtime_ns.max(b.mtime_ns),
                    a,
                    b,
                }
            }
            Decision::Conflict { winner } => {
                ActionKind::Conflict(resolve(path, winner, views, names))
            }
            Decision::Skip(side) => ActionKind::Skip(SkipReason::Unmanaged(side)),
            Decision::Rescan => ActionKind::Rescan,
        }
    }
}

/// `winner`'s version stays; the loser side gets it with the merged vector
/// plus a bump, after its own file or symlink is moved to a conflict name.
fn resolve(path: &RelPath, winner: Side, views: &Views, names: &mut Names) -> Resolution {
    let (entry, loser) = (views.entry(winner, path), views.entry(winner.other(), path));
    let mut vv = entry.vv.merge(&loser.vv);
    vv.bump(views.id(winner));
    let conflict_name = matches!(loser.kind, Kind::File { .. } | Kind::Symlink { .. })
        .then(|| names.choose(path, &loser, views.id(winner.other())));
    Resolution {
        winner,
        entry,
        vv,
        loser,
        conflict_name,
    }
}

/// Picks conflict names that are free on both sides.
struct Names<'v> {
    views: Views<'v>,
    now: DateTime,
    chosen: HashSet<RelPath>,
}

impl Names<'_> {
    /// `stem.sync-conflict-…-<ID7>.ext` for `loser`, a file or symlink of
    /// replica `replica`, next to `path`.
    fn choose(&mut self, path: &RelPath, loser: &Entry, replica: ReplicaId) -> RelPath {
        let parent = path.parent().expect("never the root");
        let name = path.name().expect("never the root");
        let split_ext = matches!(loser.kind, Kind::File { .. });
        let mut t = self.now;
        loop {
            let c = parent
                .join(conflict_name(name, split_ext, t, replica))
                .expect("a conflict name is a valid component");
            let live = |v: &dyn IndexView| v.get(&c).is_some_and(|e| !e.is_tombstone());
            if !live(self.views.a) && !live(self.views.b) && !self.chosen.contains(&c) {
                self.chosen.insert(c.clone());
                return c;
            }
            t = t
                .checked_add(1.second())
                .expect("conflict name timestamp overflow");
        }
    }
}

/// The proper ancestors of `p`, nearest first, without the root.
fn ancestors(p: &RelPath) -> impl Iterator<Item = RelPath> {
    std::iter::successors(p.parent(), RelPath::parent).filter(|a| !a.is_root())
}

/// §6.3: a directory delete is skipped, and the directory resurrected, if
/// the deleting side would still hold something beneath it afterwards: a
/// live or unmanaged object it is not told to delete, or one it is told to
/// create. Deepest first, so a resurrected directory keeps its parent too.
fn resurrect(views: &Views, actions: &mut BTreeMap<RelPath, ActionKind>, names: &mut Names) {
    let mut rmdirs: Vec<(RelPath, Side)> = actions
        .iter()
        .filter_map(|(p, k)| match k {
            ActionKind::Push {
                from,
                entry,
                target: Some(t),
            } if t.kind == Kind::Dir && entry.kind != Kind::Dir => Some((p.clone(), from.other())),
            _ => None,
        })
        .collect();
    rmdirs.sort_by_key(|(p, _)| (Reverse(p.depth()), p.clone()));

    for (dir, side) in rmdirs {
        let view = views.side(side);
        let mut beneath: BTreeSet<&RelPath> = view.descendants(&dir).map(|(p, _)| p).collect();
        let mut end = dir.as_bytes().to_vec();
        end.push(b'0');
        beneath.extend(
            actions
                .range((Bound::Excluded(&dir), Bound::Unbounded))
                .map(|(p, _)| p)
                .take_while(|p| p.as_bytes() < end.as_slice())
                .filter(|p| is_beneath(p, &dir)),
        );
        let blocked = beneath
            .into_iter()
            .any(|q| present_after(actions.get(q), side, view.get(q)));
        if !blocked {
            continue;
        }
        tracing::debug!(path = %dir, ?side, "directory delete skipped: resurrecting");
        let resolution = resolve(&dir, side, views, names);
        actions.insert(dir, ActionKind::Resurrect(resolution));
    }
}

/// Whether `side` holds something at a path after this round, given the
/// path's action and `side`'s current entry there.
fn present_after(action: Option<&ActionKind>, side: Side, current: Option<&Entry>) -> bool {
    match action {
        Some(ActionKind::Push { from, entry, .. }) if *from != side => entry.is_live(),
        // The winner's (live) version ends up on both sides.
        Some(ActionKind::Conflict(_) | ActionKind::Resurrect(_)) => true,
        _ => current.is_some_and(|e| !e.is_tombstone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Snapshot;
    use crate::index::UnmanagedReason;
    use jiff::civil::date;

    const IA: ReplicaId = ReplicaId(0x1111_2222_3333_4444);
    const IB: ReplicaId = ReplicaId(0xaaaa_bbbb_cccc_dddd);

    fn rp(s: &str) -> RelPath {
        RelPath::new(s).unwrap()
    }

    fn now() -> DateTime {
        date(2026, 10, 2).at(9, 5, 7, 0)
    }

    fn vv(a: u64, b: u64) -> VersionVector {
        [(IA, a), (IB, b)].into_iter().collect()
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum K {
        Absent,
        File,
        Dir,
        Symlink,
        Tomb,
        Unmanaged,
    }

    const KINDS: [K; 6] = [
        K::Absent,
        K::File,
        K::Dir,
        K::Symlink,
        K::Tomb,
        K::Unmanaged,
    ];

    fn is_live(k: K) -> bool {
        matches!(k, K::File | K::Dir | K::Symlink)
    }

    /// Variant 0 and 1 differ in content (hash, mode or target) and mtime;
    /// variant 0 is newer.
    fn mk(k: K, variant: u8, vv: VersionVector) -> Option<Entry> {
        let mtime = 2_000 - 1_000 * i64::from(variant);
        let (kind, mode) = match k {
            K::Absent => return None,
            K::File => (
                Kind::File {
                    size: 1,
                    hash: [variant; 32],
                },
                0o644,
            ),
            K::Dir => (Kind::Dir, if variant == 0 { 0o755 } else { 0o700 }),
            K::Symlink => (
                Kind::Symlink {
                    target: vec![b't', variant],
                },
                0o777,
            ),
            K::Tomb => return Some(Entry::new(Kind::Tombstone, 0, 0, vv)),
            K::Unmanaged => (Kind::Unmanaged(UnmanagedReason::Special), 0o644),
        };
        Some(Entry::new(kind, mode, mtime, vv))
    }

    fn snap(id: ReplicaId, entries: &[(&str, Option<Entry>)]) -> Snapshot {
        Snapshot::new(
            id,
            entries
                .iter()
                .filter_map(|(p, e)| Some((rp(p), e.clone()?))),
        )
    }

    fn run(a: &Snapshot, b: &Snapshot) -> Vec<Action> {
        reconcile(a, b, now())
    }

    /// The action's shape, for the table test.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Exp {
        None,
        Push(Side),
        Merge,
        Conflict { winner: Side, renamed: bool },
        Skip(Side),
        Rescan,
    }

    /// The §6.1/§6.2 table, written out case by case.
    fn expected(ka: K, kb: K, ord: Ord4, same: bool) -> Exp {
        use K::*;
        match (ka, kb) {
            (Absent, Absent) => Exp::None,
            (Unmanaged, _) => {
                if is_live(kb) {
                    Exp::Skip(Side::A)
                } else {
                    Exp::None
                }
            }
            (_, Unmanaged) => {
                if is_live(ka) {
                    Exp::Skip(Side::B)
                } else {
                    Exp::None
                }
            }
            (Absent, _) => {
                if is_live(kb) {
                    Exp::Push(Side::B)
                } else {
                    Exp::None
                }
            }
            (_, Absent) => {
                if is_live(ka) {
                    Exp::Push(Side::A)
                } else {
                    Exp::None
                }
            }
            (Tomb, Tomb) => Exp::None,
            _ => match ord {
                Ord4::Equal if ka == kb && same => Exp::None,
                Ord4::Equal => Exp::Rescan,
                Ord4::Dominates => Exp::Push(Side::A),
                Ord4::Dominated => Exp::Push(Side::B),
                Ord4::Concurrent if ka == kb && same => Exp::Merge,
                Ord4::Concurrent if ka == Tomb => Exp::Push(Side::B),
                Ord4::Concurrent if kb == Tomb => Exp::Push(Side::A),
                Ord4::Concurrent => {
                    let rank = |k| match k {
                        Dir => 2,
                        File => 1,
                        _ => 0,
                    };
                    // Same kind: variant 0 (A) has the newer mtime.
                    let winner = if rank(ka) >= rank(kb) {
                        Side::A
                    } else {
                        Side::B
                    };
                    let loser = if winner == Side::A { kb } else { ka };
                    Exp::Conflict {
                        winner,
                        renamed: loser != Dir,
                    }
                }
            },
        }
    }

    #[test]
    fn decision_table_is_exhaustive() {
        let vvs = [
            (Ord4::Equal, vv(1, 1), vv(1, 1)),
            (Ord4::Dominates, vv(2, 1), vv(1, 1)),
            (Ord4::Dominated, vv(1, 1), vv(1, 2)),
            (Ord4::Concurrent, vv(2, 1), vv(1, 2)),
        ];
        let mut cases = 0;
        for ka in KINDS {
            for kb in KINDS {
                for (ord, va, vb) in &vvs {
                    assert_eq!(va.compare(vb), *ord);
                    for same in [true, false] {
                        cases += 1;
                        let ea = mk(ka, 0, va.clone());
                        let eb = mk(kb, if same { 0 } else { 1 }, vb.clone());
                        let a = snap(IA, &[("dir/x", ea.clone())]);
                        let b = snap(IB, &[("dir/x", eb.clone())]);
                        let actions = run(&a, &b);
                        let ctx = format!("{ka:?} vs {kb:?}, {ord:?}, same={same}: {actions:#?}");
                        assert!(actions.len() <= 1, "{ctx}");
                        let got = match actions.first() {
                            None => Exp::None,
                            Some(act) => {
                                assert_eq!(act.path, rp("dir/x"), "{ctx}");
                                shape(&act.kind, ea.as_ref(), eb.as_ref(), &ctx)
                            }
                        };
                        assert_eq!(got, expected(ka, kb, *ord, same), "{ctx}");
                        // Every decision can be planned.
                        crate::engine::plan(&actions);
                    }
                }
            }
        }
        assert_eq!(cases, 6 * 6 * 4 * 2);
    }

    /// Checks the entries an action carries and returns its shape.
    fn shape(kind: &ActionKind, ea: Option<&Entry>, eb: Option<&Entry>, ctx: &str) -> Exp {
        let of = |s: Side| if s == Side::A { ea } else { eb };
        match kind {
            ActionKind::Push {
                from,
                entry,
                target,
            } => {
                assert_eq!(Some(entry), of(*from), "{ctx}");
                assert_eq!(target.as_ref(), of(from.other()), "{ctx}");
                Exp::Push(*from)
            }
            ActionKind::MergeVv { vv, mtime_ns, a, b } => {
                assert_eq!((Some(a), Some(b)), (ea, eb), "{ctx}");
                assert_eq!(*vv, a.vv.merge(&b.vv), "{ctx}");
                assert_eq!(*mtime_ns, a.mtime_ns.max(b.mtime_ns), "{ctx}");
                Exp::Merge
            }
            ActionKind::Conflict(r) => {
                assert_eq!(Some(&r.entry), of(r.winner), "{ctx}");
                assert_eq!(Some(&r.loser), of(r.winner.other()), "{ctx}");
                assert_eq!(r.vv.compare(&r.entry.vv), Ord4::Dominates, "{ctx}");
                assert_eq!(r.vv.compare(&r.loser.vv), Ord4::Dominates, "{ctx}");
                if let Some(c) = &r.conflict_name {
                    let id = if r.winner == Side::A {
                        "aaaabbb"
                    } else {
                        "1111222"
                    };
                    let name = String::from_utf8(c.as_bytes().to_vec()).unwrap();
                    assert!(
                        name.starts_with("dir/x.sync-conflict-20261002-090507-"),
                        "{name}"
                    );
                    assert!(name.ends_with(id), "{name}");
                }
                Exp::Conflict {
                    winner: r.winner,
                    renamed: r.conflict_name.is_some(),
                }
            }
            ActionKind::Skip(SkipReason::Unmanaged(s)) => Exp::Skip(*s),
            ActionKind::Rescan => Exp::Rescan,
            other => panic!("unexpected {other:?}: {ctx}"),
        }
    }

    fn file(hash: u8, mtime: i64, vv: VersionVector) -> Entry {
        Entry::new(
            Kind::File {
                size: 1,
                hash: [hash; 32],
            },
            0o644,
            mtime,
            vv,
        )
    }

    fn dir(vv: VersionVector) -> Entry {
        Entry::new(Kind::Dir, 0o755, 0, vv)
    }

    fn tomb(vv: VersionVector) -> Entry {
        Entry::new(Kind::Tombstone, 0, 0, vv)
    }

    fn only(actions: Vec<Action>) -> ActionKind {
        assert_eq!(actions.len(), 1, "{actions:#?}");
        actions.into_iter().next().unwrap().kind
    }

    #[test]
    fn mtime_rules() {
        // Equal vectors, a file mtime differs: inconsistent.
        let a = snap(IA, &[("f", Some(file(1, 5, vv(1, 1))))]);
        let b = snap(IB, &[("f", Some(file(1, 6, vv(1, 1))))]);
        assert_eq!(only(run(&a, &b)), ActionKind::Rescan);
        // A directory's mtime is not synced.
        let mut d = dir(vv(1, 1));
        d.mtime_ns = 9;
        let a = snap(IA, &[("d", Some(d))]);
        let b = snap(IB, &[("d", Some(dir(vv(1, 1))))]);
        assert!(run(&a, &b).is_empty());
        // Concurrent, same content, different mtime: merged, newer mtime.
        let a = snap(IA, &[("f", Some(file(1, 5, vv(2, 1))))]);
        let b = snap(IB, &[("f", Some(file(1, 7, vv(1, 2))))]);
        let ActionKind::MergeVv {
            vv: m, mtime_ns, ..
        } = only(run(&a, &b))
        else {
            panic!()
        };
        assert_eq!((m, mtime_ns), (vv(2, 2), 7));
    }

    #[test]
    fn conflict_winner_rules() {
        let winner = |ea: Entry, eb: Entry| {
            let a = snap(IA, &[("f", Some(ea))]);
            let b = snap(IB, &[("f", Some(eb))]);
            match only(run(&a, &b)) {
                ActionKind::Conflict(r) => r.winner,
                other => panic!("{other:?}"),
            }
        };
        // Newer mtime wins, whichever side.
        assert_eq!(winner(file(1, 5, vv(2, 1)), file(2, 6, vv(1, 2))), Side::B);
        assert_eq!(winner(file(1, 6, vv(2, 1)), file(2, 5, vv(1, 2))), Side::A);
        // A tie goes to the higher replica ID (B here).
        assert_eq!(winner(file(1, 5, vv(2, 1)), file(2, 5, vv(1, 2))), Side::B);
        // Type conflicts: Dir > File > Symlink, mtime notwithstanding.
        let link = |mtime, vv| {
            Entry::new(
                Kind::Symlink {
                    target: b"t".to_vec(),
                },
                0o777,
                mtime,
                vv,
            )
        };
        let mut d = dir(vv(2, 1));
        d.mtime_ns = 1;
        assert_eq!(winner(d, file(1, 99, vv(1, 2))), Side::A);
        assert_eq!(winner(link(99, vv(2, 1)), file(1, 1, vv(1, 2))), Side::B);
        // The vv the loser gets: merged plus a bump by the winner.
        let a = snap(IA, &[("f", Some(file(1, 5, vv(3, 1))))]);
        let b = snap(IB, &[("f", Some(file(2, 6, vv(1, 2))))]);
        let ActionKind::Conflict(r) = only(run(&a, &b)) else {
            panic!()
        };
        assert_eq!(r.vv, vv(3, 4));
        // A mode-only directory conflict is resolved without a rename.
        let mut d700 = dir(vv(1, 2));
        d700.mode = 0o700;
        let a = snap(IA, &[("d", Some(dir(vv(2, 1))))]);
        let b = snap(IB, &[("d", Some(d700))]);
        let ActionKind::Conflict(r) = only(run(&a, &b)) else {
            panic!()
        };
        assert_eq!((r.winner, r.conflict_name), (Side::B, None));
    }

    #[test]
    fn conflict_names_avoid_taken_names() {
        let cn = |n: &[u8], t: DateTime| conflict_name(n, true, t, IA);
        let s = |n: Vec<u8>| String::from_utf8(n).unwrap();
        let later = now().checked_add(1.second()).unwrap();
        // B (newer) wins, so A's versions are renamed. A live entry under
        // the first choice for `f.txt` pushes it a second later; a tombstone
        // under the first choice for `g.txt` does not.
        let a = snap(
            IA,
            &[
                ("f.txt", Some(file(1, 5, vv(2, 1)))),
                ("g.txt", Some(file(1, 5, vv(2, 1)))),
            ],
        );
        let b = snap(
            IB,
            &[
                ("f.txt", Some(file(2, 6, vv(1, 2)))),
                ("g.txt", Some(file(2, 6, vv(1, 2)))),
                (&s(cn(b"f.txt", now())), Some(file(3, 1, vv(0, 1)))),
                (&s(cn(b"g.txt", now())), Some(tomb(vv(0, 1)))),
            ],
        );
        let names = |actions: Vec<Action>| -> Vec<Vec<u8>> {
            actions
                .into_iter()
                .filter_map(|act| match act.kind {
                    ActionKind::Conflict(r) => Some(r.conflict_name?.as_bytes().to_vec()),
                    _ => None,
                })
                .collect()
        };
        assert_eq!(
            names(run(&a, &b)),
            [cn(b"f.txt", later), cn(b"g.txt", now())]
        );

        // Two long names that shorten to the same conflict name: the second
        // one chosen in this call moves on too.
        let (n1, n2) = (
            format!("{}1", "a".repeat(250)),
            format!("{}2", "a".repeat(250)),
        );
        assert_eq!(cn(n1.as_bytes(), now()), cn(n2.as_bytes(), now()));
        let a = snap(
            IA,
            &[
                (&n1, Some(file(1, 5, vv(2, 1)))),
                (&n2, Some(file(1, 5, vv(2, 1)))),
            ],
        );
        let b = snap(
            IB,
            &[
                (&n1, Some(file(2, 6, vv(1, 2)))),
                (&n2, Some(file(2, 6, vv(1, 2)))),
            ],
        );
        assert_eq!(
            names(run(&a, &b)),
            [cn(n1.as_bytes(), now()), cn(n2.as_bytes(), later)]
        );
    }

    #[test]
    fn unmanaged_subtree_is_left_alone() {
        let special = Entry::new(Kind::Unmanaged(UnmanagedReason::Loop), 0o755, 0, vv(1, 0));
        let a = snap(IA, &[("d", Some(special)), ("d/old", Some(tomb(vv(2, 1))))]);
        let b = snap(
            IB,
            &[
                ("d", Some(dir(vv(0, 1)))),
                ("d/old", Some(file(1, 1, vv(1, 1)))),
                ("d/new", Some(file(1, 1, vv(0, 2)))),
                ("e", Some(file(1, 1, vv(0, 2)))),
            ],
        );
        let kinds: Vec<_> = run(&a, &b).into_iter().map(|a| (a.path, a.kind)).collect();
        let beneath = ActionKind::Skip(SkipReason::BeneathUnmanaged(rp("d")));
        assert_eq!(
            kinds[0],
            (rp("d"), ActionKind::Skip(SkipReason::Unmanaged(Side::A)))
        );
        assert_eq!(kinds[1], (rp("d/new"), beneath.clone()));
        assert_eq!(kinds[2], (rp("d/old"), beneath));
        assert!(
            matches!(kinds[3], (ref p, ActionKind::Push { from: Side::B, .. }) if *p == rp("e"))
        );
        assert_eq!(kinds.len(), 4);
    }

    /// Resurrection, §6.3: A deleted `d`, B created something inside it.
    #[test]
    fn resurrection() {
        let a = snap(
            IA,
            &[
                ("d", Some(tomb(vv(3, 1)))),
                ("d/old", Some(tomb(vv(3, 1)))),
                ("d/e", Some(tomb(vv(3, 1)))),
                ("d/e/new", None),
                ("d/gone", Some(tomb(vv(3, 1)))),
                ("d/gone/x", Some(tomb(vv(3, 1)))),
                ("plain", Some(tomb(vv(3, 1)))),
                ("plain/x", Some(tomb(vv(3, 1)))),
            ],
        );
        let b = snap(
            IB,
            &[
                ("d", Some(dir(vv(1, 1)))),
                ("d/old", Some(file(1, 1, vv(1, 1)))),
                ("d/e", Some(dir(vv(1, 1)))),
                ("d/e/new", Some(file(2, 1, vv(1, 4)))),
                ("d/gone", Some(dir(vv(1, 1)))),
                ("d/gone/x", Some(file(1, 1, vv(1, 1)))),
                ("plain", Some(dir(vv(1, 1)))),
                ("plain/x", Some(file(1, 1, vv(1, 1)))),
            ],
        );
        let actions: BTreeMap<_, _> = run(&a, &b).into_iter().map(|a| (a.path, a.kind)).collect();
        let resurrected = |p: &str| match &actions[&rp(p)] {
            ActionKind::Resurrect(r) => {
                assert_eq!(r.winner, Side::B);
                assert_eq!(r.loser, tomb(vv(3, 1)));
                assert_eq!(r.vv, vv(3, 4));
                assert_eq!(r.conflict_name, None);
            }
            other => panic!("{p}: {other:?}"),
        };
        // `d/e` holds a new file, so it stays, and so does `d`.
        resurrected("d");
        resurrected("d/e");
        let pushed = |p: &str, from| {
            assert!(
                matches!(&actions[&rp(p)], ActionKind::Push { from: f, .. } if *f == from),
                "{p}: {:?}",
                actions[&rp(p)]
            );
        };
        pushed("d/e/new", Side::B);
        // What A deleted and B never touched is still deleted.
        pushed("d/old", Side::A);
        pushed("d/gone", Side::A);
        pushed("d/gone/x", Side::A);
        pushed("plain", Side::A);
        pushed("plain/x", Side::A);
        assert_eq!(actions.len(), 8);
    }

    #[test]
    fn resurrection_over_a_type_change_and_unmanaged() {
        // A replaced dir `d` by a file; B added `d/new`. A's file becomes a
        // conflict copy. `u` holds an unmanaged object on B: it stays too.
        let a = snap(
            IA,
            &[
                ("d", Some(file(1, 1, vv(3, 1)))),
                ("u", Some(tomb(vv(3, 1)))),
                ("u/fifo", None),
            ],
        );
        let fifo = Entry::new(
            Kind::Unmanaged(UnmanagedReason::Special),
            0o644,
            0,
            vv(0, 2),
        );
        let b = snap(
            IB,
            &[
                ("d", Some(dir(vv(1, 1)))),
                ("d/new", Some(file(2, 1, vv(0, 2)))),
                ("u", Some(dir(vv(1, 1)))),
                ("u/fifo", Some(fifo)),
            ],
        );
        let actions: BTreeMap<_, _> = run(&a, &b).into_iter().map(|a| (a.path, a.kind)).collect();
        let ActionKind::Resurrect(r) = &actions[&rp("d")] else {
            panic!("{actions:#?}")
        };
        assert_eq!(r.winner, Side::B);
        let name = r.conflict_name.as_ref().unwrap().as_bytes().to_vec();
        assert!(name.starts_with(b"d.sync-conflict-") && name.ends_with(b"-1111222"));
        assert!(
            matches!(actions[&rp("u")], ActionKind::Resurrect(ref r) if r.conflict_name.is_none())
        );
        assert!(matches!(
            actions[&rp("d/new")],
            ActionKind::Push { from: Side::B, .. }
        ));
        assert_eq!(actions.len(), 3);
    }

    #[test]
    fn root_is_ignored() {
        let a = snap(IA, &[("", Some(dir(vv(2, 0))))]);
        let b = snap(IB, &[("", Some(dir(vv(0, 2))))]);
        assert!(run(&a, &b).is_empty());
    }
}
