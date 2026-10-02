//! The tree scanner: walks a replica with fds only, and brings its index up to
//! date (design §3, §4.5, §5.2).
//!
//! - **Traversal:** each child directory is opened from its parent's fd with
//!   `openat2(RESOLVE_BENEATH | NO_SYMLINKS | NO_MAGICLINKS | NO_XDEV)` and
//!   checked against the `statx` taken from the listing. Reserved `.~fsync.`
//!   names are skipped. The (dev, ino) of every directory on the current
//!   traversal stack is kept, so a followed link back to an ancestor becomes
//!   `Unmanaged(Loop)` instead of recursing forever.
//! - **Symlinks:** classified by [`classify`] on the canonical (unmunged)
//!   target. A followed link is opened beneath the root
//!   (`RESOLVE_BENEATH | NO_MAGICLINKS | NO_XDEV`, from the root fd and the
//!   link's own path) or, when that escapes, from the link's directory with
//!   `RESOLVE_NO_MAGICLINKS` only, as an out-of-tree referent (§4.5). The link
//!   must be unchanged (same inode, so same target) after its referent is
//!   read or opened.
//! - **Files:** not rehashed when (ino, size, mtime, ctime) match the index and
//!   the entry is not `racy`; otherwise hashed with a stable read (§5.2). A
//!   file whose ctime is within the racy window of the scan start is marked
//!   `racy` and is rehashed by the next scan.
//! - **Index updates:** a logical change (kind, content, target, mode; mtime
//!   for files only) is written with `vv.bump_after(replica, max_counter)` and
//!   a new seq. A change to [`LocalMeta`] alone is written in place, keeping
//!   the seq. Indexed paths in the scanned region that are gone from disk
//!   become tombstones. Every write checks that the entry still has the seq
//!   the scan started from; otherwise the path is reported dirty.
//! - **Unstable paths** (a file changing while it is read, a symlink on a
//!   path, a name replaced between two steps) leave their entry, and their
//!   whole subtree, unchanged and are reported in [`ScanStats::dirty`]. Other
//!   per-path errors (e.g. `EACCES`, a mount point) do the same and are
//!   reported in [`ScanStats::errors`].

use std::collections::HashSet;
use std::io;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustix::fs::{Dir, Mode, OFlags, ResolveFlags};
use rustix::io::Errno;

use crate::config::ReplicaId;
use crate::error::{Error, Result};
use crate::fs::hooks;
use crate::fs::root::{open_beneath, unstable_reason};
use crate::fs::{FileKind, Fingerprint, RelPath, Root, is_reserved};
use crate::index::{
    Entry, IndexStore, Kind, LinkInfo, LocalMeta, ReadTxn, UnmanagedReason, sync_mode,
};
use crate::scan::hasher::Hasher;
use crate::symlink::{SymlinkPolicy, Treatment, classify, needs_referent, unmunge};

/// Default racy window: a file whose ctime is this close to the scan start
/// (or later) is rehashed by the next scan. Generous next to a kernel
/// timestamp tick (1–10 ms), to absorb the coarse clock's lag.
pub const DEFAULT_RACY_WINDOW: Duration = Duration::from_secs(1);

/// Pending index updates are committed in batches of this size.
const FLUSH_EVERY: usize = 1024;

/// Resolution for following a symlink to an in-tree referent (§4.5).
const IN_TREE: ResolveFlags = ResolveFlags::BENEATH
    .union(ResolveFlags::NO_MAGICLINKS)
    .union(ResolveFlags::NO_XDEV);

/// What to scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
    /// The whole tree.
    Full,
    /// These paths and everything beneath them. The ancestors of each path
    /// are observed too (not recursively), so they are indexed.
    Paths(Vec<RelPath>),
}

/// What a scan did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScanStats {
    /// Objects examined (each name `statx`ed).
    pub scanned: u64,
    /// Files whose content was read and hashed.
    pub hashed: u64,
    /// Entries written with a version bump (tombstones included).
    pub changed: u64,
    /// Of `changed`, the new tombstones.
    pub tombstoned: u64,
    /// Paths that changed while being scanned, or whose index entry changed
    /// during the scan. Their entries (and subtrees) were left as they were;
    /// rescan them later.
    pub dirty: Vec<RelPath>,
    /// Paths that could not be scanned for another reason (logged). Their
    /// entries and subtrees were left as they were.
    pub errors: Vec<RelPath>,
}

/// Scans one replica into its index.
#[derive(Debug)]
pub struct Scanner<'a> {
    root: &'a Root,
    index: &'a IndexStore,
    policy: SymlinkPolicy,
    munge_links: bool,
    racy_window: Duration,
}

impl<'a> Scanner<'a> {
    /// A scanner for `root` that records into `index` (whose replica ID is the
    /// one bumped), under the symlink `policy`.
    pub fn new(root: &'a Root, index: &'a IndexStore, policy: SymlinkPolicy) -> Scanner<'a> {
        Scanner {
            root,
            index,
            policy,
            munge_links: false,
            racy_window: DEFAULT_RACY_WINDOW,
        }
    }

    /// rsync `--munge-links` on this replica: targets on disk are munged
    /// and are indexed unmunged (design §4.4).
    pub fn munge_links(mut self, on: bool) -> Self {
        self.munge_links = on;
        self
    }

    /// Overrides [`DEFAULT_RACY_WINDOW`].
    pub fn racy_window(mut self, window: Duration) -> Self {
        self.racy_window = window;
        self
    }

    /// Scans `scope` and updates the index. Fails only if the root or the
    /// index fails; per-path problems are reported in [`ScanStats`].
    pub fn scan(&self, scope: &Scope) -> Result<ScanStats> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
        let window = i64::try_from(self.racy_window.as_nanos()).unwrap_or(i64::MAX);
        let mut walk = Walk {
            root: self.root,
            index: self.index,
            policy: self.policy,
            munge_links: self.munge_links,
            replica: self.index.replica(),
            racy_cutoff: now.saturating_sub(window),
            snap: self.index.read()?,
            hasher: Hasher::new(),
            stack: Vec::new(),
            seen: HashSet::new(),
            protected: HashSet::new(),
            regions: Vec::new(),
            pending: Vec::new(),
            stats: ScanStats::default(),
        };
        match scope {
            Scope::Full => walk.scan_path(&RelPath::root())?,
            Scope::Paths(paths) => {
                for p in outermost(paths) {
                    walk.scan_path(&p)?;
                }
            }
        }
        walk.tombstones()?;
        walk.flush()?;
        let mut stats = walk.stats;
        stats.hashed = walk.hasher.hashed();
        stats.dirty.sort();
        stats.dirty.dedup();
        stats.errors.sort();
        stats.errors.dedup();
        Ok(stats)
    }
}

/// `paths` without duplicates and without paths beneath another one.
fn outermost(paths: &[RelPath]) -> Vec<RelPath> {
    let mut sorted: Vec<&RelPath> = paths.iter().collect();
    sorted.sort_by_key(|p| (p.depth(), p.as_bytes()));
    let mut kept: HashSet<&[u8]> = HashSet::new();
    let mut out = Vec::new();
    for p in sorted {
        if !prefixes(p).any(|pre| kept.contains(pre)) {
            kept.insert(p.as_bytes());
            out.push(p.clone());
        }
    }
    out
}

/// The root (empty), every ancestor of `p`, and `p` itself, as raw bytes.
fn prefixes(p: &RelPath) -> impl Iterator<Item = &[u8]> {
    let b = p.as_bytes();
    std::iter::once(&b[..0])
        .chain(
            b.iter()
                .enumerate()
                .filter(|&(_, &c)| c == b'/')
                .map(move |(i, _)| &b[..i]),
        )
        .chain((!b.is_empty()).then_some(b))
}

/// What one name turned out to be.
enum Obs {
    /// Not there (or removed before we could look at it).
    Absent,
    /// Changed while we looked; leave its entry alone and rescan later.
    Dirty(&'static str),
    /// Could not be scanned; leave its entry alone.
    Failed(Error),
    Found(Found),
}

/// An observed object, in index terms.
struct Found {
    kind: Kind,
    mode: u32,
    mtime_ns: i64,
    local: LocalMeta,
    /// For a directory: an open fd to descend into.
    dir: Option<OpenDir>,
}

struct OpenDir {
    fd: OwnedFd,
    id: (u64, u64),
    /// Reached through a followed symlink.
    followed: bool,
}

/// A queued index write; `base` is the seq the decision was made from.
struct Update {
    path: RelPath,
    base: Option<u64>,
    change: Change,
}

enum Change {
    /// A logical change: a new entry with a bumped version vector.
    New {
        kind: Kind,
        mode: u32,
        mtime_ns: i64,
        local: LocalMeta,
    },
    /// Only `LocalMeta` changed: rewrite in place, same seq.
    Local(LocalMeta),
}

/// The state of one scan.
struct Walk<'a> {
    root: &'a Root,
    index: &'a IndexStore,
    policy: SymlinkPolicy,
    munge_links: bool,
    replica: ReplicaId,
    /// Files with a ctime at or after this are `racy`.
    racy_cutoff: i64,
    /// The index as of the scan start.
    snap: ReadTxn,
    hasher: Hasher,
    /// (dev, ino) of the directories on the current traversal path.
    stack: Vec<(u64, u64)>,
    /// Paths found on disk.
    seen: HashSet<Vec<u8>>,
    /// Dirty or failed paths: they and their subtrees keep their entries.
    protected: HashSet<Vec<u8>>,
    /// Roots of the subtrees scanned recursively; tombstones are limited to these.
    regions: Vec<RelPath>,
    pending: Vec<Update>,
    stats: ScanStats,
}

impl Walk<'_> {
    /// Observes the ancestors of `p` one by one from the root, then `p`
    /// recursively. If an ancestor is not a real directory (gone, a file, a
    /// symlink), that ancestor is scanned recursively instead.
    fn scan_path(&mut self, p: &RelPath) -> Result<()> {
        if self
            .regions
            .iter()
            .any(|r| prefixes(p).any(|pre| pre == r.as_bytes()))
        {
            return Ok(());
        }
        let mut dir = self.root.open_dir(&RelPath::root())?;
        let root_fp = Fingerprint::of_fd(dir.as_fd())?;
        self.stack = vec![(root_fp.dev, root_fp.ino)];
        if p.is_root() {
            self.regions.push(RelPath::root());
            return self.walk_dir(dir, &RelPath::root());
        }
        let names: Vec<&[u8]> = p.components().collect();
        let mut cur = RelPath::root();
        for (i, name) in names.iter().enumerate() {
            if is_reserved(name) {
                return Ok(());
            }
            cur = cur.join(name)?;
            let last = i + 1 == names.len();
            match self.visit(dir.as_fd(), &cur, name, last)? {
                Some(d) => {
                    self.stack.push(d.id);
                    dir = d.fd;
                }
                None => break,
            }
        }
        self.regions.push(cur);
        Ok(())
    }

    /// Lists the directory `fd` (at `path`) and visits each child.
    fn walk_dir(&mut self, fd: OwnedFd, path: &RelPath) -> Result<()> {
        let names = match list_dir(&fd) {
            Ok(names) => names,
            Err(e) => {
                self.fail(path, Error::io(format!("read directory {path}"), e));
                return Ok(());
            }
        };
        for name in names {
            let child = path.join(&name)?;
            self.visit(fd.as_fd(), &child, &name, true)?;
            if self.pending.len() >= FLUSH_EVERY {
                self.flush()?;
            }
        }
        Ok(())
    }

    /// Observes `parent/name` (at `path`) and queues its index update. A
    /// directory is descended into, except a real (not followed) one when
    /// `recurse` is false: its open fd is returned instead.
    fn visit(
        &mut self,
        parent: BorrowedFd<'_>,
        path: &RelPath,
        name: &[u8],
        recurse: bool,
    ) -> Result<Option<OpenDir>> {
        if self.protected.contains(path.as_bytes()) {
            return Ok(None);
        }
        self.stats.scanned += 1;
        let old = self.snap.get(path)?;
        match self.observe(parent, path, name, old.as_ref())? {
            Obs::Absent => Ok(None),
            Obs::Dirty(reason) => {
                tracing::debug!(%path, reason, "unstable during scan; left for a rescan");
                self.stats.dirty.push(path.clone());
                self.protected.insert(path.as_bytes().to_vec());
                Ok(None)
            }
            Obs::Failed(e) => {
                self.fail(path, e);
                Ok(None)
            }
            Obs::Found(mut found) => {
                let dir = found.dir.take();
                // An ancestor shared by several scoped paths is recorded once.
                if self.seen.insert(path.as_bytes().to_vec()) {
                    self.record(path, old, found);
                }
                match dir {
                    Some(d) if !recurse && !d.followed => Ok(Some(d)),
                    Some(d) => {
                        self.stack.push(d.id);
                        let res = self.walk_dir(d.fd, path);
                        self.stack.pop();
                        res.map(|()| None)
                    }
                    None => Ok(None),
                }
            }
        }
    }

    fn fail(&mut self, path: &RelPath, e: Error) {
        tracing::warn!(%path, error = %e, "cannot scan; index entry left unchanged");
        self.stats.errors.push(path.clone());
        self.protected.insert(path.as_bytes().to_vec());
    }

    fn observe(
        &mut self,
        parent: BorrowedFd<'_>,
        path: &RelPath,
        name: &[u8],
        old: Option<&Entry>,
    ) -> Result<Obs> {
        let fp = match Fingerprint::at(parent, name) {
            Ok(fp) => fp,
            Err(e) => return per_path(e),
        };
        hooks::point("scan.stat");
        match fp.kind {
            FileKind::File => self.observe_file(parent, name, &fp, old),
            FileKind::Dir => self.observe_dir(parent, path, name, &fp),
            FileKind::Symlink => self.observe_link(parent, path, name, &fp, old),
            FileKind::Special => Ok(Obs::Found(Found {
                kind: Kind::Unmanaged(UnmanagedReason::Special),
                mode: sync_mode(fp.mode),
                mtime_ns: fp.mtime_ns,
                local: local_of(&fp),
                dir: None,
            })),
        }
    }

    fn observe_file(
        &mut self,
        parent: BorrowedFd<'_>,
        name: &[u8],
        fp: &Fingerprint,
        old: Option<&Entry>,
    ) -> Result<Obs> {
        if let Some(old) = old.filter(|old| unchanged_file(old, fp, None)) {
            return Ok(Obs::Found(found_as(old)));
        }
        match self.hasher.hash_at(parent, name) {
            Ok((f2, hash)) => Ok(Obs::Found(self.file_found(&f2, hash, None))),
            Err(e) => per_path(e),
        }
    }

    fn file_found(&self, fp: &Fingerprint, hash: [u8; 32], via_link: Option<LinkInfo>) -> Found {
        Found {
            kind: Kind::File {
                size: fp.size,
                hash,
            },
            mode: sync_mode(fp.mode),
            mtime_ns: fp.mtime_ns,
            local: LocalMeta {
                via_link,
                racy: fp.ctime_ns >= self.racy_cutoff,
                ..local_of(fp)
            },
            dir: None,
        }
    }

    /// A real directory: opened beneath its parent, so a symlink swapped in
    /// since the `statx` is refused.
    fn observe_dir(
        &mut self,
        parent: BorrowedFd<'_>,
        path: &RelPath,
        name: &[u8],
        fp: &Fingerprint,
    ) -> Result<Obs> {
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
        let fd = match open_beneath(parent, name, flags) {
            Ok(fd) => fd,
            // Opening the name itself can only cross a mount point.
            Err(Errno::XDEV) => {
                return Ok(Obs::Failed(Error::io(
                    format!("open directory {path}"),
                    io::Error::other("mount point inside the replica; not synced"),
                )));
            }
            Err(Errno::NOTDIR) => return Ok(Obs::Dirty("directory replaced")),
            Err(e) => return Ok(errno_obs(e, path, "open directory")),
        };
        self.dir_found(fd, fp, None)
    }

    /// The directory open at `fd`, which must be the object `expect`
    /// describes. `via` is the followed link it was reached through, with
    /// the link's own local metadata (used if it turns out to be a loop).
    fn dir_found(
        &self,
        fd: OwnedFd,
        expect: &Fingerprint,
        via: Option<(LinkInfo, LocalMeta)>,
    ) -> Result<Obs> {
        let dfp = match Fingerprint::of_fd(fd.as_fd()) {
            Ok(dfp) => dfp,
            Err(e) => return per_path(e),
        };
        if !dfp.same_file(expect) || dfp.kind != FileKind::Dir {
            return Ok(Obs::Dirty("directory replaced"));
        }
        let id = (dfp.dev, dfp.ino);
        let (via_link, link_local) = via.unzip();
        if self.stack.contains(&id) {
            return Ok(Obs::Found(Found {
                kind: Kind::Unmanaged(UnmanagedReason::Loop),
                mode: sync_mode(dfp.mode),
                mtime_ns: dfp.mtime_ns,
                local: link_local.unwrap_or_else(|| local_of(&dfp)),
                dir: None,
            }));
        }
        Ok(Obs::Found(Found {
            kind: Kind::Dir,
            mode: sync_mode(dfp.mode),
            mtime_ns: dfp.mtime_ns,
            dir: Some(OpenDir {
                fd,
                id,
                followed: via_link.is_some(),
            }),
            local: LocalMeta {
                via_link,
                ..local_of(&dfp)
            },
        }))
    }

    fn observe_link(
        &mut self,
        parent: BorrowedFd<'_>,
        path: &RelPath,
        name: &[u8],
        fp: &Fingerprint,
        old: Option<&Entry>,
    ) -> Result<Obs> {
        let raw = match rustix::fs::readlinkat(parent, name, Vec::new()) {
            Ok(t) => t.into_bytes(),
            Err(Errno::INVAL) => return Ok(Obs::Dirty("symlink replaced")),
            Err(e) => return Ok(errno_obs(e, path, "read symlink")),
        };
        // Symlink targets are immutable: the same inode has the same target.
        match link_unchanged(parent, name, fp)? {
            None => {}
            Some(why) => return Ok(Obs::Dirty(why)),
        }
        let target = if self.munge_links {
            unmunge(&raw)
        } else {
            raw.clone()
        };
        let link_local = LocalMeta {
            raw_target: (target != raw).then(|| raw.clone()),
            ..local_of(fp)
        };

        let mut referent = None;
        let mut looped = false;
        let referent_kind = if needs_referent(self.policy, path, &target) {
            match open_referent(
                self.root,
                parent,
                path,
                name,
                OFlags::PATH | OFlags::CLOEXEC,
            ) {
                Ok((fd, out_of_tree)) => match Fingerprint::of_fd(fd.as_fd()) {
                    Ok(rfp) => {
                        referent = Some((fd, rfp, out_of_tree));
                        Some(rfp.kind)
                    }
                    Err(e) => return per_path(e),
                },
                Err(Errno::NOENT | Errno::NOTDIR) => None,
                // Too many levels of symlinks: a chain that loops.
                Err(Errno::LOOP) => {
                    looped = true;
                    None
                }
                Err(e) => return Ok(errno_obs(e, path, "follow symlink")),
            }
        } else {
            None
        };

        let link_found = |kind| {
            Obs::Found(Found {
                kind,
                mode: sync_mode(fp.mode),
                mtime_ns: fp.mtime_ns,
                local: link_local.clone(),
                dir: None,
            })
        };
        match classify(self.policy, path, &target, referent_kind) {
            Treatment::AsSymlink => Ok(link_found(Kind::Symlink { target })),
            Treatment::Unmanaged(UnmanagedReason::Dangling) if looped => {
                Ok(link_found(Kind::Unmanaged(UnmanagedReason::Loop)))
            }
            Treatment::Unmanaged(reason) => Ok(link_found(Kind::Unmanaged(reason))),
            Treatment::Follow => {
                let Some((rfd, rfp, out_of_tree)) = referent else {
                    return Ok(Obs::Dirty("symlink referent vanished"));
                };
                let via = LinkInfo {
                    ino: fp.ino,
                    ctime_ns: fp.ctime_ns,
                    raw_target: raw,
                    out_of_tree,
                };
                match rfp.kind {
                    FileKind::File => self.follow_file(parent, path, name, fp, &rfp, via, old),
                    FileKind::Dir => {
                        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
                        let fd = match open_beneath(rfd.as_fd(), b".", flags) {
                            Ok(fd) => fd,
                            Err(e) => return Ok(errno_obs(e, path, "open symlink referent")),
                        };
                        if let Some(why) = link_unchanged(parent, name, fp)? {
                            return Ok(Obs::Dirty(why));
                        }
                        self.dir_found(fd, &rfp, Some((via, link_local)))
                    }
                    // `classify` follows only files and directories.
                    _ => Ok(Obs::Dirty("symlink referent changed type")),
                }
            }
        }
    }

    /// Hashes the file a followed link points to. The link must still be
    /// the same inode after the read, and the referent the one we classified.
    #[allow(clippy::too_many_arguments)]
    fn follow_file(
        &mut self,
        parent: BorrowedFd<'_>,
        path: &RelPath,
        name: &[u8],
        link_fp: &Fingerprint,
        rfp: &Fingerprint,
        via: LinkInfo,
        old: Option<&Entry>,
    ) -> Result<Obs> {
        if let Some(old) = old.filter(|old| unchanged_file(old, rfp, Some(&via))) {
            return Ok(Obs::Found(found_as(old)));
        }
        let root = self.root;
        let res = self.hasher.hash_with(
            path.as_bytes(),
            || {
                let flags = OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC;
                match open_referent(root, parent, path, name, flags | OFlags::NOATIME) {
                    Err(Errno::PERM) => open_referent(root, parent, path, name, flags),
                    res => res,
                }
                .map(|(fd, _)| fd)
            },
            |f2| {
                if !f2.same_file(rfp) {
                    return Ok(Some("symlink referent replaced"));
                }
                link_unchanged(parent, name, link_fp)
            },
        );
        match res {
            Ok((f2, hash)) => Ok(Obs::Found(self.file_found(&f2, hash, Some(via)))),
            Err(e) => per_path(e),
        }
    }

    /// Queues the index update that turns `old` into what was found.
    fn record(&mut self, path: &RelPath, old: Option<Entry>, f: Found) {
        let change = match &old {
            Some(old) if same_logical(old, &f) => {
                if old.local == f.local {
                    return;
                }
                Change::Local(f.local)
            }
            _ => Change::New {
                kind: f.kind,
                mode: f.mode,
                mtime_ns: f.mtime_ns,
                local: f.local,
            },
        };
        self.pending.push(Update {
            path: path.clone(),
            base: old.map(|e| e.seq),
            change,
        });
    }

    /// Queues tombstones for indexed paths in the scanned regions that were
    /// not found (and are not protected).
    fn tombstones(&mut self) -> Result<()> {
        for region in std::mem::take(&mut self.regions) {
            for (path, e) in self.snap.iter_prefix(&region)? {
                if e.is_tombstone()
                    || self.seen.contains(path.as_bytes())
                    || prefixes(&path).any(|pre| self.protected.contains(pre))
                {
                    continue;
                }
                self.pending.push(Update {
                    path,
                    base: Some(e.seq),
                    change: Change::New {
                        kind: Kind::Tombstone,
                        mode: 0,
                        mtime_ns: 0,
                        local: LocalMeta::default(),
                    },
                });
            }
        }
        Ok(())
    }

    /// Commits the pending updates in one transaction.
    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut txn = self.index.write()?;
        for u in std::mem::take(&mut self.pending) {
            let cur = txn.get(&u.path)?;
            if cur.as_ref().map(|e| e.seq) != u.base {
                tracing::debug!(path = %u.path, "index entry changed during the scan");
                self.stats.dirty.push(u.path);
                continue;
            }
            match u.change {
                Change::New {
                    kind,
                    mode,
                    mtime_ns,
                    local,
                } => {
                    let mut vv = cur.map(|e| e.vv).unwrap_or_default();
                    vv.bump_after(self.replica, txn.max_counter());
                    let tombstone = kind == Kind::Tombstone;
                    let mut entry = Entry {
                        kind,
                        mode,
                        mtime_ns,
                        vv,
                        seq: 0,
                        local,
                    };
                    txn.put(&u.path, &mut entry)?;
                    self.stats.changed += 1;
                    self.stats.tombstoned += u64::from(tombstone);
                }
                Change::Local(local) => {
                    let mut entry = cur.expect("base seq matched an entry");
                    entry.local = local;
                    txn.put_local(&u.path, &entry)?;
                }
            }
        }
        txn.commit()
    }
}

/// Names in the directory `fd`, sorted, without `.`, `..` and reserved names.
fn list_dir(fd: &OwnedFd) -> io::Result<Vec<Vec<u8>>> {
    let mut names = Vec::new();
    for entry in Dir::new(fd.try_clone()?)? {
        let name = entry?.file_name().to_bytes().to_vec();
        if name != b"." && name != b".." && !is_reserved(&name) {
            names.push(name);
        }
    }
    names.sort();
    Ok(names)
}

/// Opens what the symlink `parent/name` (at `path`) points to, following
/// links (design §4.5). First beneath the root, from the root fd and the
/// link's own path; if that escapes the root (`EXDEV`), from the link's
/// directory with only `RESOLVE_NO_MAGICLINKS`, as an out-of-tree referent.
/// Returns the fd and whether the referent is out of tree.
///
/// If the in-tree attempt fails for another reason, the out-of-tree route
/// must fail too (its errno is returned); if it succeeds instead, the two
/// views of the path disagree, and the result is `EAGAIN` (unstable).
fn open_referent(
    root: &Root,
    parent: BorrowedFd<'_>,
    path: &RelPath,
    name: &[u8],
    flags: OFlags,
) -> std::result::Result<(OwnedFd, bool), Errno> {
    let direct = || {
        rustix::fs::openat2(
            parent,
            name,
            flags,
            Mode::empty(),
            ResolveFlags::NO_MAGICLINKS,
        )
    };
    match rustix::fs::openat2(root.fd(), path.as_bytes(), flags, Mode::empty(), IN_TREE) {
        Ok(fd) => Ok((fd, false)),
        Err(Errno::XDEV) => direct().map(|fd| (fd, true)),
        Err(_) => match direct() {
            Ok(_) => Err(Errno::AGAIN),
            Err(e) => Err(e),
        },
    }
}

/// `None` if `parent/name` is still the symlink `fp`, else why not.
fn link_unchanged(
    parent: BorrowedFd<'_>,
    name: &[u8],
    fp: &Fingerprint,
) -> Result<Option<&'static str>> {
    match Fingerprint::at(parent, name) {
        Ok(now) if now.unchanged(fp) => Ok(None),
        Ok(_) => Ok(Some("symlink replaced")),
        Err(Error::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
            Ok(Some("symlink removed"))
        }
        Err(e) => Err(e),
    }
}

/// The rehash shortcut (design §3): the indexed file has the same inode,
/// size, mtime and ctime (and was reached the same way) and is not racy.
fn unchanged_file(old: &Entry, fp: &Fingerprint, via: Option<&LinkInfo>) -> bool {
    matches!(old.kind, Kind::File { size, .. } if size == fp.size)
        && !old.local.racy
        && old.local.dev == fp.dev
        && old.local.ino == fp.ino
        && old.local.ctime_ns == fp.ctime_ns
        && old.mtime_ns == fp.mtime_ns
        && old.local.via_link.as_ref() == via
}

/// Whether `f` is the same as `old` for the peer. Directory and symlink
/// mtimes are not synced state: a directory's mtime moves with every child,
/// and a symlink's is set when it is created.
fn same_logical(old: &Entry, f: &Found) -> bool {
    old.kind == f.kind
        && match f.kind {
            Kind::File { .. } => old.mode == f.mode && old.mtime_ns == f.mtime_ns,
            Kind::Dir => old.mode == f.mode,
            Kind::Symlink { .. } | Kind::Unmanaged(_) | Kind::Tombstone => true,
        }
}

/// The indexed entry, unchanged.
fn found_as(old: &Entry) -> Found {
    Found {
        kind: old.kind.clone(),
        mode: old.mode,
        mtime_ns: old.mtime_ns,
        local: old.local.clone(),
        dir: None,
    }
}

fn local_of(fp: &Fingerprint) -> LocalMeta {
    LocalMeta {
        dev: fp.dev,
        ino: fp.ino,
        ctime_ns: fp.ctime_ns,
        ..LocalMeta::default()
    }
}

/// Sorts a per-path library error: unstable → dirty, gone → absent, other
/// I/O → failed. Index and other errors abort the scan.
fn per_path(e: Error) -> Result<Obs> {
    match e {
        Error::Unstable { reason, .. } => Ok(Obs::Dirty(reason)),
        Error::Io { ref source, .. } if source.kind() == io::ErrorKind::NotFound => Ok(Obs::Absent),
        Error::Io { .. } | Error::InvalidPath { .. } => Ok(Obs::Failed(e)),
        e => Err(e),
    }
}

/// Sorts a per-path errno like [`per_path`].
fn errno_obs(e: Errno, path: &RelPath, what: &str) -> Obs {
    if e == Errno::NOENT {
        return Obs::Absent;
    }
    match unstable_reason(e) {
        Some(reason) => Obs::Dirty(reason),
        None => Obs::Failed(Error::io(format!("{what} {path}"), e.into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::fs;
    use std::io::Write;
    use std::rc::Rc;

    const ME: ReplicaId = ReplicaId(0xfeed);

    struct Fixture {
        dir: tempfile::TempDir,
        _state: tempfile::TempDir,
        root: Root,
        index: Rc<IndexStore>,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let root = Root::open(dir.path()).unwrap();
        let index = Rc::new(IndexStore::open(&state.path().join("i.redb"), ME).unwrap());
        Fixture {
            dir,
            _state: state,
            root,
            index,
        }
    }

    impl Fixture {
        fn scan(&self) -> ScanStats {
            Scanner::new(&self.root, &self.index, SymlinkPolicy::Links)
                .scan(&Scope::Full)
                .unwrap()
        }

        fn get(&self, p: &str) -> Entry {
            self.index.get(&RelPath::new(p).unwrap()).unwrap().unwrap()
        }
    }

    fn rp(s: &str) -> RelPath {
        RelPath::new(s).unwrap()
    }

    #[test]
    fn prefixes_and_outermost() {
        let p = rp("a/bb/c");
        let pre: Vec<&[u8]> = prefixes(&p).collect();
        assert_eq!(pre, [&b""[..], b"a", b"a/bb", b"a/bb/c"]);
        assert_eq!(prefixes(&RelPath::root()).collect::<Vec<_>>(), [&b""[..]]);
        let paths = ["a/b", "a.b", "a", "c/d", "c/d", "a/b/c"].map(rp);
        assert_eq!(outermost(&paths), ["a", "a.b", "c/d"].map(rp));
        assert_eq!(outermost(&[rp("x"), RelPath::root()]), [RelPath::root()]);
    }

    /// A file that keeps changing while it is read stays as indexed, and is
    /// reported dirty.
    #[test]
    fn unstable_read_is_dirty_and_unchanged() {
        let f = fixture();
        let path = f.dir.path().join("f");
        fs::write(&path, b"version one").unwrap();
        f.scan();
        let before = f.get("f");
        fs::write(&path, b"version two").unwrap();

        let appends = Rc::new(Cell::new(0));
        let n = appends.clone();
        let _g = hooks::on("scan.read_chunk", move || {
            let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"+").unwrap();
            n.set(n.get() + 1);
        });
        let stats = f.scan();
        assert_eq!(stats.dirty, [rp("f")]);
        assert_eq!((stats.changed, stats.hashed), (0, 1));
        assert_eq!(appends.get(), 1 + crate::fs::stat::STABLE_READ_RETRIES);
        assert_eq!(f.get("f"), before);
    }

    /// A directory swapped for a symlink between its `statx` and its open is
    /// dirty; its entry and subtree are kept, and nothing outside is read.
    #[test]
    fn directory_swapped_for_symlink_is_dirty() {
        let f = fixture();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("secret"), b"s").unwrap();
        fs::create_dir(f.dir.path().join("d")).unwrap();
        fs::write(f.dir.path().join("d/child"), b"c").unwrap();
        f.scan();
        let (d, child) = (f.get("d"), f.get("d/child"));

        let root = f.dir.path().to_owned();
        let out = outside.path().to_owned();
        let _g = hooks::once("scan.stat", move || {
            fs::rename(root.join("d"), root.join("moved")).unwrap();
            std::os::unix::fs::symlink(&out, root.join("d")).unwrap();
        });
        let stats = f.scan();
        assert_eq!(stats.dirty, [rp("d")], "{stats:?}");
        assert_eq!((f.get("d"), f.get("d/child")), (d, child.clone()));
        assert!(f.index.get(&rp("d/secret")).unwrap().is_none());

        // The next scan sees the symlink, and the directory under its new
        // name (listed after the rename).
        let stats = f.scan();
        assert!(stats.dirty.is_empty());
        assert!(matches!(f.get("d").kind, Kind::Symlink { .. }));
        assert_eq!(f.get("d/child").kind, Kind::Tombstone);
        assert_eq!(f.get("moved/child").kind, child.kind);
    }

    /// An index entry written by someone else while the scan runs is not
    /// overwritten; the path is reported dirty.
    #[test]
    fn index_changed_during_scan_is_dirty() {
        let f = fixture();
        fs::write(f.dir.path().join("f"), b"one").unwrap();
        f.scan();
        fs::write(f.dir.path().join("f"), b"two").unwrap();

        let index = f.index.clone();
        let mut theirs = f.get("f");
        theirs.vv.set(ReplicaId(7), 99);
        let written = Rc::new(Cell::new(0));
        let w = written.clone();
        let mut t = theirs.clone();
        let _g = hooks::once("scan.read_chunk", move || {
            w.set(index.put(&RelPath::new("f").unwrap(), &mut t).unwrap());
        });
        let stats = f.scan();
        assert_eq!(stats.dirty, [rp("f")]);
        assert_eq!(stats.changed, 0);
        let now = f.get("f");
        assert_eq!((now.vv, now.seq), (theirs.vv, written.get()));
    }
}
