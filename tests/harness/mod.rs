//! Sync test harness (design §9): two tempdir replicas with their state
//! directory, a small scenario DSL for user edits, and [`Pair::assert_converged`].
//!
//! The DSL acts as the *user*: it edits the trees with plain `std::fs`, as
//! any other program would. Only the replicas go through `fs::Root`.
#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crossbeam_channel::Receiver;
use files_sync::config::{ReplicaConfig, ReplicaId, SymlinkPolicy};
use files_sync::engine::{Engine, Side, SyncReport};
use files_sync::fs::{RelPath, is_reserved};
use files_sync::index::{Entry, Kind, VersionVector};
use files_sync::replica::{ContentReader, LocalReplica, Op, Outcome, Precondition, Replica};
use files_sync::scan::{ScanStats, Scope};
use files_sync::symlink::unmunge;
use files_sync::watch::Hint;

/// Fixed replica IDs, so conflict names are predictable.
pub const ID_A: ReplicaId = ReplicaId(0x1111_aaaa_0000_0001);
pub const ID_B: ReplicaId = ReplicaId(0x2222_bbbb_0000_0002);

/// One replica: its directory (the user's view) and the `LocalReplica`.
pub struct Tree {
    dir: tempfile::TempDir,
    pub replica: LocalReplica,
}

/// Two replicas of one pair, sharing a state directory.
pub struct Pair {
    pub a: Tree,
    pub b: Tree,
    pub engine: Engine,
    policy: SymlinkPolicy,
    _state: tempfile::TempDir,
}

impl Pair {
    /// Two empty replicas syncing symlinks under `policy`.
    pub fn new(policy: SymlinkPolicy) -> Pair {
        let state = tempfile::tempdir().unwrap();
        let tree = |id| {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = ReplicaConfig::new(dir.path().canonicalize().unwrap()).unwrap();
            cfg.id = id;
            cfg.symlinks = policy;
            let replica = LocalReplica::open(&cfg, state.path())
                .unwrap()
                // Old inodes are unlinked by the first sweep, so
                // `assert_converged` can demand a tree without leftovers.
                .quarantine_grace(Duration::ZERO);
            Tree { dir, replica }
        };
        Pair {
            a: tree(ID_A),
            b: tree(ID_B),
            engine: Engine::new(),
            policy,
            _state: state,
        }
    }

    /// One sync cycle; it must converge without errors.
    pub fn sync(&mut self) -> SyncReport {
        let report = self.try_sync();
        assert!(report.is_converged(), "sync did not converge: {report:#?}");
        report
    }

    /// One sync cycle, whatever its outcome; then sweeps the quarantines.
    pub fn try_sync(&mut self) -> SyncReport {
        let report = self
            .engine
            .sync_once(&mut self.a.replica, &mut self.b.replica)
            .unwrap();
        self.a.replica.sweep_quarantine();
        self.b.replica.sweep_quarantine();
        report
    }

    /// One sync cycle in which `edit` (a user edit) runs once, right before
    /// the engine's first `when` call at `path` on `side`. It must converge.
    pub fn sync_racing(
        &mut self,
        side: Side,
        when: When,
        path: &str,
        edit: impl FnOnce() + 'static,
    ) -> SyncReport {
        let race = Race {
            when,
            path: rp(path),
            edit: RefCell::new(Some(Box::new(edit))),
        };
        let (mut a, mut b) = (
            Racing::new(&mut self.a.replica),
            Racing::new(&mut self.b.replica),
        );
        match side {
            Side::A => a.race = Some(race),
            Side::B => b.race = Some(race),
        }
        let report = self.engine.sync_once(&mut a, &mut b).unwrap();
        let fired = [&a, &b]
            .iter()
            .all(|r| r.race.as_ref().is_none_or(|r| r.edit.borrow().is_none()));
        assert!(fired, "the racing edit never ran");
        self.a.replica.sweep_quarantine();
        self.b.replica.sweep_quarantine();
        assert!(report.is_converged(), "sync did not converge: {report:#?}");
        report
    }

    /// Checks that the pair is in sync:
    /// - the trees are equal after policy normalization (file content,
    ///   mode and mtime; directory mode; symlink targets, unmunged);
    /// - the live index entries are equal, version vectors included;
    /// - no reserved (`.~fsync.`) names are left, and nothing is quarantined;
    /// - a full rescan of either replica changes nothing (no echo).
    pub fn assert_converged(&mut self) {
        for t in [&mut self.a, &mut self.b] {
            t.replica.sweep_quarantine();
            assert!(t.replica.quarantine().is_empty(), "quarantine not empty");
        }
        let (ta, tb) = (self.a.tree(self.policy), self.b.tree(self.policy));
        assert_eq!(ta, tb, "trees differ (left: A, right: B)");
        assert_eq!(
            self.a.live_entries(),
            self.b.live_entries(),
            "indexes differ (left: A, right: B)"
        );
        for (name, t) in [("A", &mut self.a), ("B", &mut self.b)] {
            let stats = t.replica.scan(Scope::Full).unwrap();
            assert_eq!(
                (stats.changed, stats.dirty.len(), stats.errors.len()),
                (0, 0, 0),
                "rescan of {name} found changes: {stats:?}"
            );
        }
    }
}

/// Where [`Pair::sync_racing`] injects its edit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum When {
    /// Before `apply` on this side (the destination changes after the scan).
    Apply,
    /// Before `open_read` on this side (the source changes after the scan).
    Read,
}

struct Race {
    when: When,
    path: RelPath,
    edit: RefCell<Option<Box<dyn FnOnce()>>>,
}

impl Race {
    fn fire(&self, when: When, path: &RelPath) {
        if self.when == when
            && self.path == *path
            && let Some(edit) = self.edit.borrow_mut().take()
        {
            edit();
        }
    }
}

/// A replica that runs a user edit at one chosen moment of a sync.
struct Racing<'r> {
    inner: &'r mut LocalReplica,
    race: Option<Race>,
}

impl<'r> Racing<'r> {
    fn new(inner: &'r mut LocalReplica) -> Racing<'r> {
        Racing { inner, race: None }
    }

    fn fire(&self, when: When, path: &RelPath) {
        if let Some(r) = &self.race {
            r.fire(when, path);
        }
    }
}

impl Replica for Racing<'_> {
    fn id(&self) -> ReplicaId {
        self.inner.id()
    }

    fn scan(&mut self, scope: Scope) -> files_sync::Result<ScanStats> {
        self.inner.scan(scope)
    }

    fn changes_since(&self, seq: u64) -> files_sync::Result<Vec<(RelPath, Entry)>> {
        self.inner.changes_since(seq)
    }

    fn open_read(
        &self,
        path: &RelPath,
        expect: &Entry,
    ) -> files_sync::Result<Box<dyn ContentReader>> {
        self.fire(When::Read, path);
        self.inner.open_read(path, expect)
    }

    fn apply(
        &mut self,
        path: &RelPath,
        op: Op,
        pre: Precondition,
        content: Option<&mut dyn Read>,
    ) -> files_sync::Result<Outcome> {
        self.fire(When::Apply, path);
        self.inner.apply(path, op, pre, content)
    }

    fn watch(&mut self) -> Option<Receiver<Hint>> {
        self.inner.watch()
    }
}

/// An object in a tree, as compared by [`Pair::assert_converged`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Node {
    File {
        content: String,
        mode: u32,
        mtime_ns: i64,
    },
    Dir {
        mode: u32,
    },
    Symlink(String),
}

impl Tree {
    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    pub fn path(&self, rel: &str) -> PathBuf {
        self.dir.path().join(rel)
    }

    pub fn id(&self) -> ReplicaId {
        self.replica.id()
    }

    // --- User edits -------------------------------------------------------

    /// Writes a file (replacing whatever is there), creating its parents.
    pub fn write(&self, rel: &str, content: impl AsRef<[u8]>) {
        let p = self.path(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        if p.symlink_metadata().is_ok_and(|m| !m.is_file()) {
            self.rm(rel);
        }
        fs::write(p, content).unwrap();
    }

    /// Writes a file and sets its mtime to `secs` after the epoch.
    pub fn write_at(&self, rel: &str, content: impl AsRef<[u8]>, secs: u64) {
        self.write(rel, content);
        self.set_mtime(rel, secs);
    }

    pub fn set_mtime(&self, rel: &str, secs: u64) {
        let f = fs::File::options()
            .write(true)
            .open(self.path(rel))
            .unwrap();
        f.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
            .unwrap();
    }

    pub fn mkdir(&self, rel: &str) {
        fs::create_dir_all(self.path(rel)).unwrap();
    }

    pub fn symlink(&self, rel: &str, target: &str) {
        let p = self.path(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        if p.symlink_metadata().is_ok() {
            self.rm(rel);
        }
        symlink(target, p).unwrap();
    }

    pub fn chmod(&self, rel: &str, mode: u32) {
        fs::set_permissions(self.path(rel), fs::Permissions::from_mode(mode)).unwrap();
    }

    /// Removes a file, symlink or whole directory tree.
    pub fn rm(&self, rel: &str) {
        let p = self.path(rel);
        if p.symlink_metadata().unwrap().is_dir() {
            fs::remove_dir_all(p).unwrap();
        } else {
            fs::remove_file(p).unwrap();
        }
    }

    // --- Observations -----------------------------------------------------

    pub fn read(&self, rel: &str) -> String {
        String::from_utf8(fs::read(self.path(rel)).unwrap()).unwrap()
    }

    pub fn readlink(&self, rel: &str) -> String {
        let t = fs::read_link(self.path(rel)).unwrap();
        String::from_utf8(t.into_os_string().into_vec()).unwrap()
    }

    pub fn exists(&self, rel: &str) -> bool {
        self.path(rel).symlink_metadata().is_ok()
    }

    pub fn is_dir(&self, rel: &str) -> bool {
        self.path(rel).symlink_metadata().is_ok_and(|m| m.is_dir())
    }

    pub fn is_file(&self, rel: &str) -> bool {
        self.path(rel).symlink_metadata().is_ok_and(|m| m.is_file())
    }

    pub fn is_symlink(&self, rel: &str) -> bool {
        self.path(rel)
            .symlink_metadata()
            .is_ok_and(|m| m.is_symlink())
    }

    pub fn mode(&self, rel: &str) -> u32 {
        self.path(rel).symlink_metadata().unwrap().mode() & 0o7777
    }

    /// Every path in the tree, sorted.
    pub fn ls(&self) -> Vec<String> {
        self.tree(SymlinkPolicy::Links).into_keys().collect()
    }

    /// The names in directory `rel` (`""` for the root) that are conflict
    /// copies, sorted.
    pub fn conflicts(&self, rel: &str) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(self.path(rel))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.contains(".sync-conflict-"))
            .collect();
        names.sort();
        names
    }

    /// The index entry at `rel`.
    pub fn entry(&self, rel: &str) -> Option<Entry> {
        self.replica.index().get(&rp(rel)).unwrap()
    }

    /// The tree on disk, normalized for `policy`. Panics on a reserved name.
    pub fn tree(&self, policy: SymlinkPolicy) -> BTreeMap<String, Node> {
        let mut out = BTreeMap::new();
        self.walk(Path::new(""), policy, &mut out);
        out
    }

    fn walk(&self, rel: &Path, policy: SymlinkPolicy, out: &mut BTreeMap<String, Node>) {
        for e in fs::read_dir(self.root().join(rel)).unwrap() {
            let e = e.unwrap();
            let name = e.file_name();
            assert!(
                !is_reserved(name.as_bytes()),
                "reserved name left in {}: {}",
                self.root().display(),
                rel.join(&name).display()
            );
            let rel = rel.join(&name);
            let key = rel.to_str().unwrap().to_owned();
            let md = e.path().symlink_metadata().unwrap();
            let ft = md.file_type();
            if ft.is_dir() {
                out.insert(
                    key,
                    Node::Dir {
                        mode: md.mode() & 0o1777,
                    },
                );
                self.walk(&rel, policy, out);
            } else if ft.is_file() {
                let content = String::from_utf8(fs::read(e.path()).unwrap()).unwrap();
                let mtime_ns = md.mtime() * 1_000_000_000 + md.mtime_nsec();
                let mode = md.mode() & 0o1777;
                out.insert(
                    key,
                    Node::File {
                        content,
                        mode,
                        mtime_ns,
                    },
                );
            } else if ft.is_symlink() {
                let raw = fs::read_link(e.path()).unwrap().into_os_string().into_vec();
                let target = if self.replica.config().munge_links {
                    unmunge(&raw)
                } else {
                    raw
                };
                match policy {
                    SymlinkPolicy::Links => {
                        out.insert(key, Node::Symlink(String::from_utf8(target).unwrap()));
                    }
                    // Never synced: each side keeps its own.
                    SymlinkPolicy::Skip => {}
                    other => unimplemented!("normalization for {other:?} (T17)"),
                }
            }
            // FIFOs, sockets and devices are never synced.
        }
    }

    /// The live index entries, compared on what is synced (a file's mtime;
    /// not a directory's or a symlink's).
    pub fn live_entries(&self) -> BTreeMap<RelPath, (Kind, u32, Option<i64>, VersionVector)> {
        self.replica
            .changes_since(0)
            .unwrap()
            .into_iter()
            .filter(|(_, e)| e.is_live())
            .map(|(p, e)| {
                let mtime = matches!(e.kind, Kind::File { .. }).then_some(e.mtime_ns);
                (p, (e.kind, e.mode, mtime, e.vv))
            })
            .collect()
    }
}

pub fn rp(s: &str) -> RelPath {
    RelPath::new(s).unwrap()
}

/// The first 7 hex digits of a replica ID, as in conflict names.
pub fn id7(id: ReplicaId) -> String {
    id.to_string()[..7].to_owned()
}
