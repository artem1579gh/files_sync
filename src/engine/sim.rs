//! Model-based test of [`reconcile`] + [`plan`]: two in-memory replicas that
//! follow `LocalReplica`'s index semantics (§7), random user edits, then
//! sync rounds until nothing is left to do.
//!
//! Checks that a sync converges within the §6.4 round limit, that both sides
//! end up with the same live entries, that no step is an invalid op (an
//! engine bug), and that no file version is lost unless the other side's
//! version causally supersedes it.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use jiff::civil::date;
use proptest::prelude::*;

use crate::config::ReplicaId;
use crate::engine::{ActionKind, MAX_ROUNDS, Side, Snapshot, Step, is_beneath, plan, reconcile};
use crate::fs::RelPath;
use crate::index::{Entry, Kind, VersionVector};
use crate::replica::{Op, Precondition};

const IA: ReplicaId = ReplicaId(0x1111_2222_3333_4444);
const IB: ReplicaId = ReplicaId(0xaaaa_bbbb_cccc_dddd);

struct Model {
    id: ReplicaId,
    /// The Lamport floor (`IndexStore::max_counter`).
    max: u64,
    map: BTreeMap<RelPath, Entry>,
}

impl Model {
    fn new(id: ReplicaId) -> Model {
        Model {
            id,
            max: 0,
            map: BTreeMap::new(),
        }
    }

    fn put(&mut self, p: &RelPath, e: Entry) {
        self.max = self.max.max(e.vv.max_counter());
        self.map.insert(p.clone(), e);
    }

    fn live(&self, p: &RelPath) -> Option<&Entry> {
        self.map.get(p).filter(|e| e.is_live())
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot::new(self.id, self.map.clone())
    }

    // User edits, as the scanner records them: a bumped vv per change.

    fn change(&mut self, p: &RelPath, kind: Kind, mode: u32, mtime: i64) {
        let mut vv = self.map.get(p).map(|e| e.vv.clone()).unwrap_or_default();
        vv.bump_after(self.id, self.max);
        self.put(p, Entry::new(kind, mode, mtime, vv));
    }

    /// Removes `p` and everything beneath it.
    fn remove(&mut self, p: &RelPath) {
        let doomed: Vec<RelPath> = self
            .map
            .iter()
            .filter(|(q, e)| (*q == p || is_beneath(q, p)) && !e.is_tombstone())
            .map(|(q, _)| q.clone())
            .collect();
        for q in doomed.into_iter().rev() {
            self.change(&q, Kind::Tombstone, 0, 0);
        }
    }

    /// Makes every ancestor of `p` a directory.
    fn parents(&mut self, p: &RelPath, mtime: i64) {
        let mut ancestors: Vec<RelPath> = std::iter::successors(p.parent(), RelPath::parent)
            .filter(|a| !a.is_root())
            .collect();
        ancestors.reverse();
        for a in ancestors {
            match self.live(&a).map(|e| e.kind.clone()) {
                Some(Kind::Dir) => {}
                Some(_) => {
                    self.remove(&a);
                    self.change(&a, Kind::Dir, 0o755, mtime);
                }
                None => self.change(&a, Kind::Dir, 0o755, mtime),
            }
        }
    }

    fn edit(&mut self, edit: &Edit, mtime: i64) {
        let p = &edit.path;
        self.parents(p, mtime);
        match edit.what {
            What::Write(h) => {
                if self.live(p).is_some_and(|e| e.kind == Kind::Dir) {
                    self.remove(p);
                }
                self.change(
                    p,
                    Kind::File {
                        size: 1,
                        hash: [h; 32],
                    },
                    0o644,
                    mtime,
                );
            }
            What::Link(t) => {
                if self.live(p).is_some_and(|e| e.kind == Kind::Dir) {
                    self.remove(p);
                }
                self.change(p, Kind::Symlink { target: vec![t] }, 0o777, mtime);
            }
            What::Mkdir => match self.live(p).map(|e| e.kind.clone()) {
                Some(Kind::Dir) => {}
                Some(_) => {
                    self.remove(p);
                    self.change(p, Kind::Dir, 0o755, mtime);
                }
                None => self.change(p, Kind::Dir, 0o755, mtime),
            },
            What::Chmod(mode) => {
                if let Some(e) = self
                    .live(p)
                    .filter(|e| e.kind == Kind::Dir && e.mode != mode)
                {
                    let e = e.clone();
                    self.change(p, Kind::Dir, mode, e.mtime_ns);
                }
            }
            What::Remove => self.remove(p),
        }
    }

    // `LocalReplica::apply`, on the index alone.

    /// `Ok(true)` if applied, `Ok(false)` for `PreconditionFailed`, `Err`
    /// for what `LocalReplica` reports as `InvalidOp` (an engine bug).
    fn apply(&mut self, s: &Step, peer: &Model) -> Result<bool, String> {
        let p = &s.path;
        let bug = |why: &str| Err(format!("invalid op at {p}: {why}: {s:?}"));
        let ancestors_ok = std::iter::successors(p.parent(), RelPath::parent)
            .filter(|a| !a.is_root())
            .all(|a| self.map.get(&a).is_some_and(|e| e.kind == Kind::Dir));
        if !ancestors_ok {
            return Ok(false);
        }
        let cur = self.map.get(p).cloned();
        let ok = match &s.pre {
            Precondition::Absent => cur.as_ref().is_none_or(Entry::is_tombstone),
            Precondition::Matches { kind, vv } => cur
                .as_ref()
                .is_some_and(|e| !e.is_unmanaged() && e.kind == *kind && e.vv == *vv),
        };
        if !ok {
            return Ok(false);
        }
        let live = cur.clone().filter(Entry::is_live);
        let is_dir = live.as_ref().is_some_and(|e| e.kind == Kind::Dir);
        match &s.op {
            Op::WriteFile { meta, hash, vv } => {
                if is_dir {
                    return bug("WriteFile over a directory");
                }
                // `open_read` on the other side: it must still hold the source.
                let src = s.source.as_ref().ok_or("WriteFile without a source")?;
                if peer.map.get(p).map(|e| &e.kind) != Some(&src.kind) {
                    return Ok(false);
                }
                let kind = Kind::File {
                    size: 1,
                    hash: *hash,
                };
                self.put(p, Entry::new(kind, meta.mode, meta.mtime_ns, vv.clone()));
            }
            Op::Symlink {
                target,
                mtime_ns,
                vv,
            } => {
                if is_dir {
                    return bug("Symlink over a directory");
                }
                let kind = Kind::Symlink {
                    target: target.clone(),
                };
                self.put(p, Entry::new(kind, 0o777, *mtime_ns, vv.clone()));
            }
            Op::Mkdir { mode, mtime_ns, vv } => {
                if live.is_some() {
                    return bug("Mkdir over an existing object");
                }
                self.put(p, Entry::new(Kind::Dir, *mode, *mtime_ns, vv.clone()));
            }
            Op::Delete { vv } => {
                if live.is_none() || is_dir {
                    return bug("Delete needs a file or symlink");
                }
                self.put(p, Entry::new(Kind::Tombstone, 0, 0, vv.clone()));
            }
            Op::Rmdir { vv } => {
                if !is_dir {
                    return bug("Rmdir needs a directory");
                }
                if self
                    .map
                    .iter()
                    .any(|(q, e)| is_beneath(q, p) && !e.is_tombstone())
                {
                    return Ok(false);
                }
                self.put(p, Entry::new(Kind::Tombstone, 0, 0, vv.clone()));
            }
            Op::RenameToConflict { to } => {
                let Some(e) = live.filter(|_| !is_dir) else {
                    return bug("RenameToConflict needs a file or symlink");
                };
                if to.parent() != p.parent() {
                    return bug("conflict name is not a sibling");
                }
                if self.map.get(to).is_some_and(|t| !t.is_tombstone()) {
                    return Ok(false);
                }
                let mut gone = e.vv.clone();
                gone.bump_after(self.id, self.max);
                let mut copy_vv = VersionVector::new();
                copy_vv.bump_after(self.id, gone.max_counter());
                self.put(p, Entry::new(Kind::Tombstone, 0, 0, gone));
                self.put(to, Entry { vv: copy_vv, ..e });
            }
            Op::SetMeta { mode, mtime_ns, vv } => {
                let Some(mut e) = live else {
                    return bug("SetMeta needs a live entry");
                };
                if !matches!(e.kind, Kind::Symlink { .. }) {
                    e.mode = *mode;
                }
                e.mtime_ns = *mtime_ns;
                e.vv = vv.clone();
                self.put(p, e);
            }
        }
        Ok(true)
    }
}

/// Runs sync rounds; returns how many had something to do.
fn sync(a: &mut Model, b: &mut Model) -> Result<usize, String> {
    let now = date(2026, 10, 2).at(9, 5, 7, 0);
    for round in 0..=MAX_ROUNDS {
        let actions = reconcile(&a.snapshot(), &b.snapshot(), now);
        if let Some(act) = actions
            .iter()
            .find(|a| matches!(a.kind, ActionKind::Rescan))
        {
            return Err(format!("round {round}: unexpected rescan: {act:?}"));
        }
        let todo: Vec<_> = actions
            .into_iter()
            .filter(|a| !matches!(a.kind, ActionKind::Skip(_)))
            .collect();
        if todo.is_empty() {
            return Ok(round);
        }
        if round == MAX_ROUNDS {
            return Err(format!(
                "not converged after {MAX_ROUNDS} rounds: {todo:#?}"
            ));
        }
        let mut failed = BTreeSet::new();
        for phase in plan(&todo) {
            for step in &phase.steps {
                if failed.contains(&step.path) {
                    continue;
                }
                let applied = match step.side {
                    Side::A => a.apply(step, b)?,
                    Side::B => b.apply(step, a)?,
                };
                if !applied {
                    failed.insert(step.path.clone());
                }
            }
        }
        // Without concurrent user edits, every step must apply.
        if !failed.is_empty() {
            return Err(format!("round {round}: steps failed at {failed:?}"));
        }
        // No counter in a replica's name is above its clock, or its next
        // local change could reuse it (issue #3).
        for (m, peer) in [(&*a, &*b), (&*b, &*a)] {
            if let Some((p, e)) = peer.map.iter().find(|(_, e)| e.vv.get(m.id) > m.max) {
                return Err(format!(
                    "round {round}: {p} holds {:?}, above {}'s clock {}",
                    e.vv, m.id, m.max
                ));
            }
        }
    }
    unreachable!()
}

#[derive(Clone, Debug)]
enum What {
    Write(u8),
    Link(u8),
    Mkdir,
    Chmod(u32),
    Remove,
}

#[derive(Clone, Debug)]
struct Edit {
    path: RelPath,
    what: What,
}

fn arb_edit() -> impl Strategy<Value = Edit> {
    let path = prop::collection::vec(prop::sample::select(vec!["a", "b", "c"]), 1..4)
        .prop_map(|c| RelPath::new(c.join("/")).unwrap());
    let what = prop_oneof![
        4 => any::<u8>().prop_map(What::Write),
        1 => (0u8..3).prop_map(What::Link),
        2 => Just(What::Mkdir),
        1 => prop::sample::select(vec![0o700u32, 0o750, 0o755]).prop_map(What::Chmod),
        3 => Just(What::Remove),
    ];
    (path, what).prop_map(|(path, what)| Edit { path, what })
}

/// Live entries, compared on what is synced.
fn synced(m: &Model) -> BTreeMap<RelPath, (Kind, u32, Option<i64>, VersionVector)> {
    m.map
        .iter()
        .filter(|(_, e)| e.is_live())
        .map(|(p, e)| {
            let mtime = matches!(e.kind, Kind::File { .. }).then_some(e.mtime_ns);
            (p.clone(), (e.kind.clone(), e.mode, mtime, e.vv.clone()))
        })
        .collect()
}

/// Every file version on `m` that `peer` does not supersede at its path.
fn must_survive(m: &Model, peer: &Model) -> Vec<(RelPath, Kind)> {
    m.map
        .iter()
        .filter(|(_, e)| matches!(e.kind, Kind::File { .. }))
        .filter(|(p, e)| {
            !peer
                .map
                .get(*p)
                .is_some_and(|o| o.vv.compare(&e.vv) == crate::index::Ord4::Dominates)
        })
        .map(|(p, e)| (p.clone(), e.kind.clone()))
        .collect()
}

/// Gives every write a unique hash, so a surviving version is identifiable.
fn uniquify(edits: &mut [Edit], salt: u8) {
    for (i, e) in edits.iter_mut().enumerate() {
        if let What::Write(h) = &mut e.what {
            *h = salt.wrapping_add(i as u8 * 3);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn sync_converges_without_losing_versions(
        mut base in prop::collection::vec(arb_edit(), 0..8),
        mut ea in prop::collection::vec(arb_edit(), 0..6),
        mut eb in prop::collection::vec(arb_edit(), 0..6),
    ) {
        uniquify(&mut base, 0);
        uniquify(&mut ea, 1);
        uniquify(&mut eb, 2);
        let (mut a, mut b) = (Model::new(IA), Model::new(IB));
        let mut clock = 1_000;
        for e in &base {
            clock += 1;
            a.edit(e, clock);
        }
        sync(&mut a, &mut b).map_err(TestCaseError::fail)?;
        prop_assert_eq!(synced(&a), synced(&b));

        // Concurrent edits; B's clock runs a little ahead now and then, so
        // either side can win by mtime.
        for (i, e) in ea.iter().enumerate() {
            a.edit(e, clock + 2 * i as i64);
        }
        for (i, e) in eb.iter().enumerate() {
            b.edit(e, clock + 3 * i as i64);
        }
        let mut wanted = must_survive(&a, &b);
        wanted.extend(must_survive(&b, &a));

        let rounds = sync(&mut a, &mut b).map_err(TestCaseError::fail)?;
        prop_assert!(rounds <= 3, "{} rounds", rounds);
        prop_assert_eq!(synced(&a), synced(&b));
        let kinds: HashSet<Kind> = a.map.values().filter(|e| e.is_live()).map(|e| e.kind.clone()).collect();
        for (p, k) in wanted {
            prop_assert!(kinds.contains(&k), "lost {:?} from {}", k, p);
        }
    }
}
