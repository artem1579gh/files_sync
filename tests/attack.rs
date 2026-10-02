//! Race-injection attack suite (design §5.1, §9). Needs the `hooks` feature:
//! `cargo test --features hooks --test attack`.
//!
//! Every commit operation is first run without interference while its hook
//! points are traced. Then, for every position in that trace and every attack
//! below, the operation is run again on a fresh tree with the attack injected
//! at exactly that point:
//!
//! - an ancestor directory (`a` or `a/b`) is replaced by a symlink to an
//!   outside directory;
//! - an ancestor directory is moved out of the root;
//! - the target is moved aside and replaced by a symlink to an outside file or
//!   directory.
//!
//! Each attack first writes a uniquely named marker file into the target's
//! directory. After each case:
//!
//! - the outside directory is exactly as before (every entry's kind, mode,
//!   size, mtime, ctime and content);
//! - every piece of user content written during the case (markers, the
//!   attacker's symlinks, edits made through a held fd) still exists in the
//!   root, at its name or as a conflict copy. Only for "moved out of the root"
//!   may it be in the directory the user moved it to;
//! - an `Applied` or `Removed` outcome is true when the operation returns
//!   (unless the attack ran after the final check);
//! - no `.~fsync.*` names remain after a `Quarantine::sweep` with zero grace.
//!
//! Some operations also run in variants that reach the undo, restore and
//! conflict paths: the user edits the target through a held fd just before
//! the exchange or move-aside, or writes through it after the old inode is
//! quarantined. A coverage test checks that the traces reach every hook point
//! in `src/fs/commit.rs`.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::fs;
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, SystemTime};

use files_sync::Result;
use files_sync::config::ReplicaId;
use files_sync::fs::caps::Caps;
use files_sync::fs::commit::{self, Ctx, Expected, FileMeta, Outcome, Quarantine};
use files_sync::fs::{RelPath, Root, hooks, is_reserved};
use files_sync::index::Journal;

const REPLICA: ReplicaId = ReplicaId(0x0123456789abcdef);
const TARGET: &str = "a/b/t";
/// The file deleted before the `rmdir` in [`Op::DeleteThenRmdir`].
const CHILD: &str = "a/b/t/c";
/// Where [`Op::RenameFile`] and [`Op::RenameSymlink`] move the target.
const CONFLICT: &str = "a/b/t.conflict";
const NEW: &[u8] = b"new content from the peer";
const META: FileMeta = FileMeta {
    mode: 0o644,
    mtime_ns: 1_700_000_000_000_000_000,
};
/// An mtime far in the past, so any write changes it.
const OLD_MTIME: Duration = Duration::from_secs(1_600_000_000);

// ---------------------------------------------------------------------------
// Operations, variants and attacks
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    CreateFile,
    CreateSymlink,
    Mkdir,
    ReplaceFile,
    ReplaceSymlink,
    DeleteFile,
    DeleteSymlink,
    Rmdir,
    /// Deletes `a/b/t/c`, then removes `a/b/t` (its quarantined child is
    /// settled by the rmdir).
    DeleteThenRmdir,
    /// Renames the target to a conflict name.
    RenameFile,
    RenameSymlink,
    /// Changes a directory's mode in place.
    ChmodDir,
}

const ALL_OPS: [Op; 12] = [
    Op::CreateFile,
    Op::CreateSymlink,
    Op::Mkdir,
    Op::ReplaceFile,
    Op::ReplaceSymlink,
    Op::DeleteFile,
    Op::DeleteSymlink,
    Op::Rmdir,
    Op::DeleteThenRmdir,
    Op::RenameFile,
    Op::RenameSymlink,
    Op::ChmodDir,
];

impl Op {
    /// Whether the operation starts from a user file we hold an fd on.
    fn holds_file(self) -> bool {
        matches!(
            self,
            Op::ReplaceFile | Op::DeleteFile | Op::DeleteThenRmdir | Op::RenameFile
        )
    }

    fn variants(self) -> &'static [Variant] {
        match self {
            // Nothing is quarantined, so there is no late write to catch.
            Op::RenameFile => &[Variant::Plain, Variant::EditBefore],
            _ if self.holds_file() => &[Variant::Plain, Variant::EditBefore, Variant::LateWrite],
            _ => &[Variant::Plain],
        }
    }

    /// Whether the temp-file strategy matters (O_TMPFILE vs a named file).
    fn stages_file(self) -> bool {
        matches!(self, Op::CreateFile | Op::ReplaceFile)
    }

    /// Where [`Variant::EditBefore`] edits and [`Variant::LateWrite`] writes.
    fn trigger(self, v: Variant) -> Option<&'static str> {
        match (self, v) {
            (_, Variant::Plain) => None,
            (Op::ReplaceFile, Variant::EditBefore) => Some("replace.before_exchange"),
            (Op::ReplaceFile, Variant::LateWrite) => Some("replace.quarantined"),
            (Op::RenameFile, Variant::EditBefore) => Some("rename.before_rename"),
            (_, Variant::EditBefore) => Some("delete.before_rename"),
            (_, Variant::LateWrite) => Some("delete.quarantined"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Variant {
    /// No interference besides the attack.
    Plain,
    /// The user rewrites the target through a held fd after it was checked
    /// but before it is exchanged or moved aside: undo / restore paths.
    EditBefore,
    /// The user writes through a held fd after the old inode was quarantined:
    /// the quarantine turns it into a conflict copy.
    LateWrite,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Attack {
    /// Replace this ancestor with a symlink to the outside directory.
    SymlinkAncestor(&'static str),
    /// Move this ancestor out of the root.
    MoveAncestorOut(&'static str),
    /// Move the target aside and put a symlink to an outside file
    /// (`false`) or directory (`true`) in its place.
    SwapTargetForSymlink { to_dir: bool },
}

const ALL_ATTACKS: [Attack; 6] = [
    Attack::SymlinkAncestor("a"),
    Attack::SymlinkAncestor("a/b"),
    Attack::MoveAncestorOut("a"),
    Attack::MoveAncestorOut("a/b"),
    Attack::SwapTargetForSymlink { to_dir: false },
    Attack::SwapTargetForSymlink { to_dir: true },
];

/// One attack at the `nth` (0-based) time `point` is reached.
#[derive(Clone, Copy, Debug)]
struct Injection {
    point: &'static str,
    nth: usize,
    attack: Attack,
}

/// Content that must survive somewhere.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Tracked {
    File(Vec<u8>),
    Symlink(Vec<u8>),
}

// ---------------------------------------------------------------------------
// One case
// ---------------------------------------------------------------------------

struct World {
    dir: tempfile::TempDir,
    /// Must never change.
    outside: tempfile::TempDir,
    /// Where `MoveAncestorOut` moves directories (the residual of §5.1).
    away: tempfile::TempDir,
    root: Root,
    caps: Caps,
    journal: Journal,
    expected: Option<Expected>,
    /// An fd on the user file (`t`, or `c` for `DeleteThenRmdir`).
    held: Option<fs::File>,
    tracked: Rc<RefCell<Vec<Tracked>>>,
}

impl World {
    fn new(op: Op, caps: Caps) -> World {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let away = tempfile::tempdir().unwrap();
        let root = Root::open(dir.path()).unwrap();

        // Outside: decoys at every path a followed symlink could lead to.
        let o = outside.path();
        fs::write(o.join("victim"), b"outside victim").unwrap();
        fs::create_dir(o.join("vdir")).unwrap();
        fs::write(o.join("vdir/x"), b"outside vdir/x").unwrap();
        fs::write(o.join("t"), b"outside t").unwrap();
        fs::create_dir(o.join("b")).unwrap();
        fs::write(o.join("b/t"), b"outside b/t").unwrap();

        fs::create_dir_all(dir.path().join("a/b")).unwrap();
        let mut w = World {
            dir,
            outside,
            away,
            root,
            caps,
            journal: Journal::in_memory().unwrap(),
            expected: None,
            held: None,
            tracked: Rc::default(),
        };
        match op {
            Op::CreateFile | Op::CreateSymlink | Op::Mkdir => {}
            Op::ReplaceFile | Op::DeleteFile | Op::RenameFile => w.user_file(TARGET),
            Op::ReplaceSymlink | Op::DeleteSymlink | Op::RenameSymlink => {
                symlink("orig-target", w.p(TARGET)).unwrap();
                w.expected = Some(Expected::from(w.root.stat(&rp(TARGET)).unwrap()));
            }
            Op::Rmdir => fs::create_dir(w.p(TARGET)).unwrap(),
            Op::DeleteThenRmdir => {
                fs::create_dir(w.p(TARGET)).unwrap();
                w.user_file(CHILD);
            }
            Op::ChmodDir => {
                fs::create_dir(w.p(TARGET)).unwrap();
                fs::set_permissions(w.p(TARGET), fs::Permissions::from_mode(0o755)).unwrap();
                w.expected = Some(Expected::from(w.root.stat(&rp(TARGET)).unwrap()));
            }
        }
        w
    }

    fn p(&self, rel: &str) -> PathBuf {
        self.dir.path().join(rel)
    }

    /// A user file with an old mtime; sets `expected` and `held`.
    fn user_file(&mut self, rel: &str) {
        let data = format!("original {rel}").into_bytes();
        fs::write(self.p(rel), &data).unwrap();
        let f = fs::File::options()
            .read(true)
            .write(true)
            .open(self.p(rel))
            .unwrap();
        f.set_modified(SystemTime::UNIX_EPOCH + OLD_MTIME).unwrap();
        self.expected = Some(Expected {
            fp: self.root.stat(&rp(rel)).unwrap(),
            hash: Some(hash(&data)),
            racy: false,
        });
        self.held = Some(f);
    }

    fn ctx(&self) -> Ctx<'_> {
        Ctx {
            root: &self.root,
            caps: &self.caps,
            replica: REPLICA,
            journal: &self.journal,
        }
    }

    /// Registers the variant's user write through the held fd.
    fn arm_trigger(&self, op: Op, v: Variant) -> Option<hooks::Guard> {
        let point = op.trigger(v)?;
        let held = self.held.as_ref().unwrap().try_clone().unwrap();
        let tracked = self.tracked.clone();
        let data = format!("user write {v:?}").into_bytes();
        Some(hooks::once(point, move || {
            held.set_len(0).unwrap();
            held.write_all_at(&data, 0).unwrap();
            tracked.borrow_mut().push(Tracked::File(data));
        }))
    }

    /// Registers `inj`; the returned flag says whether it has fired.
    fn arm_attack(&self, inj: Injection) -> (hooks::Guard, Rc<Cell<bool>>) {
        let fired = Rc::new(Cell::new(false));
        let (root, outside, away) = (
            self.dir.path().to_path_buf(),
            self.outside.path().to_path_buf(),
            self.away.path().to_path_buf(),
        );
        let tracked = self.tracked.clone();
        let mut hits = 0;
        let flag = fired.clone();
        let guard = hooks::on(inj.point, move || {
            hits += 1;
            if hits - 1 == inj.nth {
                attack(&root, &outside, &away, inj, &tracked);
                flag.set(true);
            }
        });
        (guard, fired)
    }
}

/// Carries out an attack (as the user would, with plain path syscalls).
fn attack(
    root: &Path,
    outside: &Path,
    away: &Path,
    inj: Injection,
    tracked: &RefCell<Vec<Tracked>>,
) {
    let marker = format!("marker {} #{} {:?}", inj.point, inj.nth, inj.attack).into_bytes();
    fs::write(root.join("a/b/marker"), &marker).unwrap();
    tracked.borrow_mut().push(Tracked::File(marker));
    match inj.attack {
        Attack::SymlinkAncestor(anc) => {
            fs::rename(root.join(anc), root.join("stash")).unwrap();
            symlink(outside, root.join(anc)).unwrap();
        }
        Attack::MoveAncestorOut(anc) => {
            fs::rename(root.join(anc), away.join("moved")).unwrap();
        }
        Attack::SwapTargetForSymlink { to_dir } => {
            let t = root.join(TARGET);
            if fs::symlink_metadata(&t).is_ok() {
                fs::rename(&t, root.join("a/b/stash")).unwrap();
            }
            let to = outside.join(if to_dir { "vdir" } else { "victim" });
            symlink(&to, &t).unwrap();
            let bytes = to.into_os_string().into_encoded_bytes();
            tracked.borrow_mut().push(Tracked::Symlink(bytes));
        }
    }
}

/// What happened in one run.
struct Run {
    trace: Vec<&'static str>,
    /// Per call: `(path, outcome, whether the outcome was true right after
    /// the call, whether the attack fired during the call)`.
    results: Vec<(&'static str, Result<Outcome>, bool, bool)>,
    /// The outside directory before anything ran.
    outside_before: Snapshot,
}

fn run(op: Op, v: Variant, caps: Caps, inj: Option<Injection>) -> (World, Run) {
    let w = World::new(op, caps);
    let outside_before = snapshot(w.outside.path());
    let _trigger = w.arm_trigger(op, v);
    let (guard, flag) = match inj {
        Some(inj) => {
            let (g, f) = w.arm_attack(inj);
            (Some(g), f)
        }
        None => (None, Rc::default()),
    };
    let mut q = Quarantine::new(REPLICA, Duration::ZERO);
    let ctx = w.ctx();
    let exp = w.expected.as_ref();
    let exp = || exp.unwrap();
    let t = rp(TARGET);
    let mut results = Vec::new();
    let mut call = |path: &'static str, f: &mut dyn FnMut(&mut Quarantine) -> Result<Outcome>| {
        let before = flag.get();
        let out = f(&mut q);
        let claim = claim_holds(&w.root, path, &out);
        results.push((path, out, claim, flag.get() && !before));
    };

    hooks::start_trace();
    match op {
        Op::CreateFile => call(TARGET, &mut |_| {
            commit::create_file(&ctx, &t, &mut &NEW[..], META, &hash(NEW))
        }),
        Op::CreateSymlink => call(TARGET, &mut |_| {
            commit::create_symlink(&ctx, &t, b"new-target")
        }),
        Op::Mkdir => call(TARGET, &mut |_| commit::mkdir(&ctx, &t, 0o755)),
        Op::ReplaceFile => call(TARGET, &mut |q| {
            commit::replace_file(&ctx, q, &t, exp(), &mut &NEW[..], META, &hash(NEW))
        }),
        Op::ReplaceSymlink => call(TARGET, &mut |q| {
            commit::replace_symlink(&ctx, q, &t, exp(), b"new-target")
        }),
        Op::DeleteFile | Op::DeleteSymlink => {
            call(TARGET, &mut |q| commit::delete(&ctx, q, &t, exp()))
        }
        Op::Rmdir => call(TARGET, &mut |q| commit::rmdir(&ctx, q, &t)),
        Op::DeleteThenRmdir => {
            call(CHILD, &mut |q| commit::delete(&ctx, q, &rp(CHILD), exp()));
            call(TARGET, &mut |q| commit::rmdir(&ctx, q, &t));
        }
        Op::RenameFile | Op::RenameSymlink => call(CONFLICT, &mut |_| {
            commit::rename_to(&ctx, &t, exp(), &rp(CONFLICT))
        }),
        Op::ChmodDir => call(TARGET, &mut |_| {
            commit::set_dir_mode(&ctx, &t, &exp().fp, 0o700)
        }),
    }
    q.sweep();
    let trace = hooks::take_trace();
    drop(guard);
    // The sweep with zero grace the suite requires, outside the injection.
    q.sweep();
    if let Some(inj) = inj {
        assert!(
            flag.get(),
            "{op:?}/{v:?}: injection {inj:?} never fired; trace {trace:?}"
        );
    }
    let run = Run {
        trace,
        results,
        outside_before,
    };
    (w, run)
}

/// Whether the outcome is true right after the call: `Applied` means the
/// object is at `path` (resolved inside the root), `Removed` that nothing is.
fn claim_holds(root: &Root, path: &str, out: &Result<Outcome>) -> bool {
    match out {
        Ok(Outcome::Applied(fp)) => root
            .stat(&rp(path))
            .is_ok_and(|now| now.same_file(fp) && now.kind == fp.kind),
        Ok(Outcome::Removed) => root.stat(&rp(path)).is_err(),
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// Invariants
// ---------------------------------------------------------------------------

/// Everything observable about a tree, without following symlinks.
#[derive(PartialEq, Eq)]
struct Snapshot(BTreeMap<PathBuf, String>);

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(&self.0).finish()
    }
}

fn snapshot(dir: &Path) -> Snapshot {
    fn walk(base: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, String>) {
        let md = fs::symlink_metadata(dir).unwrap();
        let rel = dir.strip_prefix(base).unwrap().to_path_buf();
        let stamp = format!(
            "mode={:o} size={} mtime={}.{} ctime={}.{}",
            md.mode(),
            md.size(),
            md.mtime(),
            md.mtime_nsec(),
            md.ctime(),
            md.ctime_nsec()
        );
        let what = if md.is_symlink() {
            format!("link -> {:?}", fs::read_link(dir).unwrap())
        } else if md.is_file() {
            format!(
                "file {:?}",
                fs::read(dir).unwrap().escape_ascii().to_string()
            )
        } else {
            for e in fs::read_dir(dir).unwrap() {
                walk(base, &e.unwrap().path(), out);
            }
            "dir".to_owned()
        };
        out.insert(rel, format!("{what} {stamp}"));
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    Snapshot(out)
}

/// Every object under `dir`, without following symlinks.
fn walk(dir: &Path, f: &mut dyn FnMut(&Path, &fs::Metadata)) {
    for e in fs::read_dir(dir).unwrap() {
        let path = e.unwrap().path();
        let md = fs::symlink_metadata(&path).unwrap();
        f(&path, &md);
        if md.is_dir() {
            walk(&path, f);
        }
    }
}

fn contents(dirs: &[&Path]) -> HashSet<Tracked> {
    let mut out = HashSet::new();
    for dir in dirs {
        walk(dir, &mut |path, md| {
            if md.is_file() {
                out.insert(Tracked::File(fs::read(path).unwrap()));
            } else if md.is_symlink() {
                let target = fs::read_link(path).unwrap();
                out.insert(Tracked::Symlink(
                    target.into_os_string().into_encoded_bytes(),
                ));
            }
        });
    }
    out
}

fn reserved_leftovers(dirs: &[&Path]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for dir in dirs {
        walk(dir, &mut |path, _| {
            if is_reserved(path.file_name().unwrap().as_encoded_bytes()) {
                out.push(path.to_path_buf());
            }
        });
    }
    out
}

/// Runs one case and checks every invariant.
fn check_case(op: Op, v: Variant, caps: Caps, inj: Injection) {
    let ctx = format!("{op:?}/{v:?} tmpfile={} {inj:?}", caps.o_tmpfile);
    let (w, r) = run(op, v, caps, Some(inj));

    // Nothing outside the root was created or changed.
    assert_eq!(
        snapshot(w.outside.path()),
        r.outside_before,
        "{ctx}: outside directory changed; results {:?}",
        r.results
    );

    // User content survives in the root, or where the user moved it.
    let mut dirs = vec![w.dir.path()];
    if matches!(inj.attack, Attack::MoveAncestorOut(_)) {
        dirs.push(w.away.path());
    }
    let have = contents(&dirs);
    for t in w.tracked.borrow().iter() {
        assert!(
            have.contains(t),
            "{ctx}: user content {t:?} lost; results {:?}",
            r.results
        );
    }

    // Outcomes are true when returned, unless the attack came after the
    // final check.
    for (path, out, claim, fired_here) in &r.results {
        let after_final_check = *fired_here && inj.point == "commit.verified";
        assert!(
            *claim || after_final_check,
            "{ctx}: {path}: {out:?} is not true afterwards"
        );
    }

    // No reserved leftovers, in the root or in a moved-out directory.
    let left = reserved_leftovers(&[w.dir.path(), w.away.path()]);
    assert!(
        left.is_empty(),
        "{ctx}: leftovers {left:?}; results {:?}",
        r.results
    );
}

// ---------------------------------------------------------------------------
// Driving
// ---------------------------------------------------------------------------

fn rp(s: &str) -> RelPath {
    RelPath::new(s).unwrap()
}

fn hash(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

fn caps() -> Caps {
    let dir = tempfile::tempdir().unwrap();
    let caps = Caps::probe(Root::open(dir.path()).unwrap().fd()).unwrap();
    caps.require_minimum().unwrap();
    caps
}

/// The capability sets to run `op` under: both temp-file strategies when it
/// stages a file.
fn caps_variants(op: Op) -> Vec<Caps> {
    let caps = caps();
    let mut out = vec![caps];
    if op.stages_file() && caps.o_tmpfile {
        out.push(Caps {
            o_tmpfile: false,
            ..caps
        });
    }
    out
}

/// Runs every injection for `op`; returns the number of cases.
fn attack_op(op: Op) -> usize {
    let mut cases = 0;
    for caps in caps_variants(op) {
        for &v in op.variants() {
            let (_, clean) = run(op, v, caps, None);
            for (i, &point) in clean.trace.iter().enumerate() {
                let nth = clean.trace[..i].iter().filter(|p| **p == point).count();
                for attack in ALL_ATTACKS {
                    check_case(op, v, caps, Injection { point, nth, attack });
                    cases += 1;
                }
            }
        }
    }
    cases
}

/// Every hook point named in `src/fs/commit.rs` (outside its tests): the
/// `point("…")` calls plus the rehash points passed to `verify_old`.
fn commit_points() -> BTreeSet<&'static str> {
    const SRC: &str = include_str!("../src/fs/commit.rs");
    // `recover.*` points are reached by replay only (tests/crash.rs).
    const PREFIXES: [&str; 10] = [
        "commit",
        "journal",
        "stage",
        "create",
        "replace",
        "delete",
        "rmdir",
        "rename",
        "chmod",
        "quarantine",
    ];
    let code = &SRC[..SRC.find("#[cfg(test)]").unwrap_or(SRC.len())];
    code.split('"')
        .skip(1)
        .step_by(2)
        .filter(|lit| {
            lit.split_once('.').is_some_and(|(prefix, rest)| {
                PREFIXES.contains(&prefix)
                    && !rest.is_empty()
                    && rest.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
            })
        })
        .collect()
}

/// The clean runs reach every hook point in commit.rs, and do what they
/// should.
#[test]
fn every_hook_point_is_attacked() {
    let mut reached = BTreeSet::new();
    for op in ALL_OPS {
        for caps in caps_variants(op) {
            for &v in op.variants() {
                let (w, r) = run(op, v, caps, None);
                reached.extend(r.trace.iter().copied());
                let outcomes: Vec<_> = r.results.iter().map(|(_, o, _, _)| o).collect();
                let ok = match v {
                    Variant::Plain | Variant::LateWrite if op == Op::DeleteThenRmdir => {
                        let rmdir_ok = if v == Variant::Plain {
                            matches!(outcomes[1], Ok(Outcome::Removed))
                        } else {
                            // The late write becomes a conflict copy in the
                            // directory, which keeps it alive.
                            matches!(outcomes[1], Ok(Outcome::PreconditionFailed(_)))
                        };
                        matches!(outcomes[0], Ok(Outcome::Removed)) && rmdir_ok
                    }
                    Variant::Plain | Variant::LateWrite => outcomes
                        .iter()
                        .all(|o| matches!(o, Ok(Outcome::Applied(_) | Outcome::Removed))),
                    Variant::EditBefore => {
                        matches!(outcomes[0], Ok(Outcome::PreconditionFailed(_)))
                    }
                };
                assert!(ok, "{op:?}/{v:?}: unexpected outcomes {outcomes:?}");
                let left = reserved_leftovers(&[w.dir.path()]);
                assert!(left.is_empty(), "{op:?}/{v:?}: leftovers {left:?}");
                for t in w.tracked.borrow().iter() {
                    assert!(
                        contents(&[w.dir.path()]).contains(t),
                        "{op:?}/{v:?}: lost {t:?}"
                    );
                }
            }
        }
    }
    let all = commit_points();
    assert!(all.len() >= 30, "found only {all:?}");
    let missed: Vec<_> = all.difference(&reached).collect();
    assert!(missed.is_empty(), "hook points never reached: {missed:?}");
    let unknown: Vec<_> = reached.difference(&all).collect();
    assert!(
        unknown.is_empty(),
        "points not found in commit.rs: {unknown:?}"
    );
}

macro_rules! attack_tests {
    ($($name:ident => $op:expr,)*) => {$(
        #[test]
        fn $name() {
            let cases = attack_op($op);
            assert!(cases > 0);
            eprintln!("{:?}: {cases} cases", $op);
        }
    )*};
}

attack_tests! {
    attack_create_file => Op::CreateFile,
    attack_create_symlink => Op::CreateSymlink,
    attack_mkdir => Op::Mkdir,
    attack_replace_file => Op::ReplaceFile,
    attack_replace_symlink => Op::ReplaceSymlink,
    attack_delete_file => Op::DeleteFile,
    attack_delete_symlink => Op::DeleteSymlink,
    attack_rmdir => Op::Rmdir,
    attack_delete_then_rmdir => Op::DeleteThenRmdir,
    attack_rename_file => Op::RenameFile,
    attack_rename_symlink => Op::RenameSymlink,
    attack_chmod_dir => Op::ChmodDir,
}
