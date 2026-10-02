//! Compare-and-swap commits: the **only** code that modifies a replica
//! (design §5.3–§5.5).
//!
//! Every mutation stages the new object under a reserved temp name in the
//! target's own directory, then moves it into place with one atomic
//! `renameat2`:
//!
//! - **create** (expected: absent) uses `RENAME_NOREPLACE`, so a concurrent
//!   create makes the rename fail instead of overwriting anything;
//! - **replace** (expected: the indexed fingerprint) pins and checks the old
//!   inode, swaps it out with `RENAME_EXCHANGE`, verifies what came out, and
//!   swaps back if it was modified in between. A verified old inode goes into
//!   [`Quarantine`] rather than being unlinked at once;
//! - **delete** (§5.6) pins and checks the object, moves it aside to a
//!   `.~fsync.del.` name with `RENAME_NOREPLACE`, verifies it, and quarantines
//!   it, or moves it back if it was modified in between;
//! - **rmdir** (§5.7) relies on `unlinkat(AT_REMOVEDIR)` as an atomic
//!   emptiness check;
//! - **rename to a conflict name** (§6.2) moves a checked file or symlink
//!   aside like a delete, verifies it, and then renames it on to the new name
//!   with `RENAME_NOREPLACE`;
//! - **directory mode** (§5.4) is the one in-place change: a directory
//!   cannot be replaced by a copy, so it is pinned, checked and `fchmod`ed
//!   through the pinning fd.
//!
//! After the rename, the parent is fsynced, the name must hold the inode we
//! staged (or nothing, after a delete), and the parent must still resolve to
//! the same directory (§5.3 step 5). Nothing is ever written in place, and nothing is unlinked unless
//! it is verified to be ours or a verified, quarantined old inode.
//!
//! Before a commit creates a reserved name, or moves a user object to one, it
//! records an intent in the [`Journal`] (§5.3 step 1, §5.8). [`recover`]
//! replays an intent left over from a crash.
//!
//! Every step boundary calls [`point`](crate::fs::hooks::point) so tests can inject races.

use std::io::{self, Read};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::time::{Duration, Instant};

use jiff::ToSpan;
use rustix::fs::{AtFlags, CWD, Mode, OFlags, RenameFlags, Timespec, Timestamps, UTIME_OMIT};
use rustix::io::Errno;

use crate::config::ReplicaId;
use crate::error::{Error, Result};
use crate::fs::caps::Caps;
use crate::fs::hooks::point;
use crate::fs::root::{RESOLVE, RelPath, Root, path_err};
use crate::fs::stat::{Discard, FileKind, Fingerprint, stable_read};
use crate::fs::tmpname::{self, TmpId, TmpKind};
use crate::index::journal::{Intent, IntentId, IntentOp, IntentState, Journal};

/// Everything a commit needs to know about the replica it modifies.
#[derive(Clone, Copy, Debug)]
pub struct Ctx<'a> {
    pub root: &'a Root,
    pub caps: &'a Caps,
    /// Used in conflict-copy names (§6.2).
    pub replica: ReplicaId,
    /// Where commits record their intents (§5.8). The caller finishes them
    /// ([`Journal::take_open`]).
    pub journal: &'a Journal,
}

/// Metadata set on a new file before it is committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FileMeta {
    /// Permission bits; setuid and setgid are stripped.
    pub mode: u32,
    pub mtime_ns: i64,
}

/// The state a replace expects to find at the target name (§5.3 step 4(a)).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Expected {
    /// The indexed fingerprint: ino, kind, size, mtime and ctime must match.
    pub fp: Fingerprint,
    /// The indexed content hash of a file. When present, the old content is
    /// rehashed after the exchange if the file is small or `racy`.
    pub hash: Option<[u8; 32]>,
    /// The index marked the entry racily clean (§3).
    pub racy: bool,
}

impl From<Fingerprint> for Expected {
    fn from(fp: Fingerprint) -> Expected {
        Expected {
            fp,
            hash: None,
            racy: false,
        }
    }
}

/// Result of a commit. I/O failures and unstable paths are `Err` instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The new object is at the name; this is its fingerprint after commit.
    Applied(Fingerprint),
    /// The target was not in the expected state (or changed while we
    /// committed). Nothing changed except possibly an undo; the caller should
    /// rescan the path.
    PreconditionFailed(&'static str),
    /// A concurrent change could not be undone cleanly, so one of two user
    /// objects was kept under this conflict name. Nothing was lost.
    Preserved { conflict: RelPath },
    /// The object was deleted ([`delete`], [`rmdir`]).
    Removed,
}

/// Files below this size are always rehashed when verifying a replace
/// (§5.3 step 4(d)).
pub const REHASH_BELOW: u64 = 1 << 20;

/// Permission bits we set: setuid and setgid are stripped (§3).
const MODE_MASK: u32 = 0o1777;
/// Attempts at finding a free conflict name.
const NAME_RETRIES: i64 = 16;

// ---------------------------------------------------------------------------
// Public operations
// ---------------------------------------------------------------------------

/// Creates the regular file `path` with `content` (expected state: absent;
/// §5.3 steps 2, 3 and 5).
///
/// The content is hashed while it is written, and a hash other than
/// `expected_hash` gives [`Outcome::PreconditionFailed`] without committing.
pub fn create_file(
    ctx: &Ctx<'_>,
    path: &RelPath,
    content: &mut dyn Read,
    meta: FileMeta,
    expected_hash: &[u8; 32],
) -> Result<Outcome> {
    let t = Target::resolve(ctx.root, path)?;
    if t.stat(t.name)?.is_some() {
        return Ok(Outcome::PreconditionFailed("name exists"));
    }
    let mut rec = Record::new(ctx.journal, IntentOp::Create, &t, None)?;
    let staged = match stage_file(&t, ctx.caps, &mut rec, content, meta, expected_hash)? {
        Ok(staged) => staged,
        Err(reason) => return Ok(Outcome::PreconditionFailed(reason)),
    };
    commit_create(&t, staged)
}

/// Creates the symlink `path` pointing at `target` (expected state: absent;
/// §5.5).
pub fn create_symlink(ctx: &Ctx<'_>, path: &RelPath, target: &[u8]) -> Result<Outcome> {
    let t = Target::resolve(ctx.root, path)?;
    if t.stat(t.name)?.is_some() {
        return Ok(Outcome::PreconditionFailed("name exists"));
    }
    let mut rec = Record::new(ctx.journal, IntentOp::Create, &t, None)?;
    let staged = stage_symlink(&t, &mut rec, target)?;
    commit_create(&t, staged)
}

/// Creates the directory `path` with permission bits `mode` (expected state:
/// absent; §5.4).
///
/// Like files, the directory is made under a temp name, given its mode
/// through a pinned fd (so the umask does not apply) and moved into place
/// with `RENAME_NOREPLACE`.
pub fn mkdir(ctx: &Ctx<'_>, path: &RelPath, mode: u32) -> Result<Outcome> {
    let t = Target::resolve(ctx.root, path)?;
    if t.stat(t.name)?.is_some() {
        return Ok(Outcome::PreconditionFailed("name exists"));
    }
    let mut rec = Record::new(ctx.journal, IntentOp::Create, &t, None)?;
    let staged = stage_dir(&t, &mut rec, mode)?;
    commit_create(&t, staged)
}

/// Replaces the file or symlink at `path`, which must match `expected`, with
/// a regular file holding `content` (§5.3 step 4). The old inode goes into
/// `quarantine`.
pub fn replace_file(
    ctx: &Ctx<'_>,
    quarantine: &mut Quarantine,
    path: &RelPath,
    expected: &Expected,
    content: &mut dyn Read,
    meta: FileMeta,
    expected_hash: &[u8; 32],
) -> Result<Outcome> {
    let t = Target::resolve(ctx.root, path)?;
    if let Some(reason) = precheck(&t, expected)? {
        return Ok(Outcome::PreconditionFailed(reason));
    }
    let mut rec = Record::new(ctx.journal, IntentOp::Replace, &t, Some(expected))?;
    let staged = match stage_file(&t, ctx.caps, &mut rec, content, meta, expected_hash)? {
        Ok(staged) => staged,
        Err(reason) => return Ok(Outcome::PreconditionFailed(reason)),
    };
    commit_replace(ctx, quarantine, &t, staged, expected, &mut rec)
}

/// Replaces the file or symlink at `path`, which must match `expected`, with
/// a symlink to `target` (§5.5). The old inode goes into `quarantine`.
pub fn replace_symlink(
    ctx: &Ctx<'_>,
    quarantine: &mut Quarantine,
    path: &RelPath,
    expected: &Expected,
    target: &[u8],
) -> Result<Outcome> {
    let t = Target::resolve(ctx.root, path)?;
    if let Some(reason) = precheck(&t, expected)? {
        return Ok(Outcome::PreconditionFailed(reason));
    }
    let mut rec = Record::new(ctx.journal, IntentOp::Replace, &t, Some(expected))?;
    let staged = stage_symlink(&t, &mut rec, target)?;
    commit_replace(ctx, quarantine, &t, staged, expected, &mut rec)
}

/// Deletes the file or symlink at `path`, which must match `expected` (§5.6).
///
/// The object is moved aside to a `.~fsync.del.` name, verified, and then
/// quarantined like a replaced inode, so a write through an fd held across
/// the delete becomes a conflict copy rather than being lost. If it was
/// modified before it was moved aside, it goes back to `path` (or, if the
/// name was taken meanwhile, to a conflict name).
pub fn delete(
    ctx: &Ctx<'_>,
    quarantine: &mut Quarantine,
    path: &RelPath,
    expected: &Expected,
) -> Result<Outcome> {
    let t = Target::resolve(ctx.root, path)?;
    if let Some(reason) = precheck(&t, expected)? {
        return Ok(Outcome::PreconditionFailed(reason));
    }
    let mut rec = Record::new(ctx.journal, IntentOp::Delete, &t, Some(expected))?;
    commit_delete(ctx, quarantine, &t, expected, &mut rec)
}

/// Removes the empty directory at `path` (§5.7).
///
/// `unlinkat(AT_REMOVEDIR)` is the atomic emptiness check: a child created
/// concurrently gives [`Outcome::PreconditionFailed`] and the directory stays.
/// Children still in `quarantine` would keep the directory non-empty, so they
/// are settled first, without waiting for their grace period (see
/// [`Quarantine::settle_dir`]).
pub fn rmdir(ctx: &Ctx<'_>, quarantine: &mut Quarantine, path: &RelPath) -> Result<Outcome> {
    let t = Target::resolve(ctx.root, path)?;
    let dir = match t.stat(t.name)? {
        None => return Ok(Outcome::PreconditionFailed("name missing")),
        Some(fp) if fp.kind != FileKind::Dir => {
            return Ok(Outcome::PreconditionFailed("not a directory"));
        }
        Some(fp) => fp,
    };
    point("rmdir.checked");
    let settled = quarantine.settle_dir(&dir);
    ctx.journal.forget(&settled.finished)?;
    if !settled.conflicts.is_empty() {
        tracing::warn!(path = %t.path, conflicts = ?settled.conflicts, "directory being removed holds conflict copies");
    }
    point("rmdir.before_rmdir");
    match rustix::fs::unlinkat(t.parent(), t.name, AtFlags::REMOVEDIR) {
        Ok(()) => {}
        Err(Errno::NOTEMPTY | Errno::EXIST) => {
            return Ok(Outcome::PreconditionFailed("directory not empty"));
        }
        Err(Errno::NOENT) => return Ok(Outcome::PreconditionFailed("name removed")),
        // Replaced by a file or symlink, which rmdir never removes.
        Err(Errno::NOTDIR) => return Ok(Outcome::PreconditionFailed("not a directory")),
        Err(e) => return Err(t.err("rmdir", e)),
    }
    point("rmdir.after_rmdir");
    t.finish_removed()
}

/// Renames the file or symlink at `path`, which must match `expected`, to
/// its sibling `to`, which must be free (§6.2: the losing side of a conflict
/// is renamed to a conflict name).
///
/// Like a delete, the object is first moved aside to a reserved name and
/// verified; if it was modified before that, it goes back to `path` (or, if
/// the name was taken meanwhile, to a conflict name of our choosing). Then it
/// is renamed to `to` with `RENAME_NOREPLACE`; if `to` was taken meanwhile it
/// goes back too. `Applied` carries its fingerprint at `to`.
pub fn rename_to(
    ctx: &Ctx<'_>,
    path: &RelPath,
    expected: &Expected,
    to: &RelPath,
) -> Result<Outcome> {
    let to_name = match to.name() {
        Some(name) if to.parent() == path.parent() && to != path => name,
        _ => {
            return Err(Error::InvalidPath {
                path: to.as_bytes().to_vec(),
                reason: "rename target is not a sibling of the renamed path",
            });
        }
    };
    if tmpname::is_reserved(to_name) {
        return Err(Error::InvalidPath {
            path: to.as_bytes().to_vec(),
            reason: "reserved `.~fsync.` name",
        });
    }
    let t = Target::resolve(ctx.root, path)?;
    if let Some(reason) = precheck(&t, expected)? {
        return Ok(Outcome::PreconditionFailed(reason));
    }
    if t.stat(to_name)?.is_some() {
        return Ok(Outcome::PreconditionFailed("new name exists"));
    }
    let mut rec = Record::new(ctx.journal, IntentOp::Rename, &t, Some(expected))?;
    commit_rename(ctx, &t, expected, to_name, &mut rec)
}

/// Sets the permission bits of the directory at `path` to `mode` (§5.4).
///
/// The directory must be `expected`'s inode with `expected`'s mode. It is
/// pinned with an `O_RDONLY|O_DIRECTORY` fd, checked, and changed through
/// that fd, so the chmod cannot hit another object swapped in at `path`.
pub fn set_dir_mode(
    ctx: &Ctx<'_>,
    path: &RelPath,
    expected: &Fingerprint,
    mode: u32,
) -> Result<Outcome> {
    let t = Target::resolve(ctx.root, path)?;
    point("chmod.before_pin");
    let pin = match rustix::fs::openat2(
        t.parent(),
        t.name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
        RESOLVE,
    ) {
        Ok(pin) => pin,
        Err(Errno::NOENT) => return Ok(Outcome::PreconditionFailed("name missing")),
        // A symlink (ENOTDIR with O_NOFOLLOW|O_DIRECTORY, or ELOOP) or a file.
        Err(Errno::NOTDIR | Errno::LOOP) => {
            return Ok(Outcome::PreconditionFailed("not a directory"));
        }
        Err(e) => return Err(t.err("pin directory", e)),
    };
    let fp = Fingerprint::of_fd(pin.as_fd())?;
    if !fp.same_file(expected)
        || fp.kind != FileKind::Dir
        || fp.mode & MODE_MASK != expected.mode & MODE_MASK
    {
        return Ok(Outcome::PreconditionFailed("changed since scan"));
    }
    point("chmod.pinned");
    rustix::fs::fchmod(&pin, Mode::from_raw_mode(mode & MODE_MASK))
        .map_err(|e| t.err("chmod", e))?;
    point("chmod.after_chmod");
    t.finish(&Fingerprint::of_fd(pin.as_fd())?)
}

// ---------------------------------------------------------------------------
// The target of a commit
// ---------------------------------------------------------------------------

/// A resolved commit target: the pinned parent directory and the final name.
struct Target<'a> {
    root: &'a Root,
    path: &'a RelPath,
    parent: OwnedFd,
    /// Identity of `parent` at resolution, re-checked after commit (§5.1).
    parent_fp: Fingerprint,
    name: &'a [u8],
}

impl<'a> Target<'a> {
    fn resolve(root: &'a Root, path: &'a RelPath) -> Result<Target<'a>> {
        let t = Target::open(root, path)?;
        point("commit.resolved");
        Ok(t)
    }

    fn open(root: &'a Root, path: &'a RelPath) -> Result<Target<'a>> {
        let (parent, name) = root.resolve_parent(path)?;
        if tmpname::is_reserved(name) {
            return Err(Error::InvalidPath {
                path: path.as_bytes().to_vec(),
                reason: "reserved `.~fsync.` name",
            });
        }
        let parent_fp = Fingerprint::of_fd(parent.as_fd())?;
        Ok(Target {
            root,
            path,
            parent,
            parent_fp,
            name,
        })
    }

    fn parent(&self) -> BorrowedFd<'_> {
        self.parent.as_fd()
    }

    /// Fingerprint of `name` in the parent, `None` if absent.
    fn stat(&self, name: &[u8]) -> Result<Option<Fingerprint>> {
        Fingerprint::at_opt(self.parent(), name)
    }

    fn err(&self, what: &str, e: Errno) -> Error {
        path_err(e, self.path, what)
    }

    fn unstable(&self, reason: &'static str) -> Error {
        Error::Unstable {
            path: self.path.as_bytes().to_vec(),
            reason,
        }
    }

    /// §5.3 step 5: fsync the parent, check that the name holds the object we
    /// staged and that the parent still resolves to the same directory.
    fn finish(&self, staged: &Fingerprint) -> Result<Outcome> {
        self.sync_parent()?;
        let now = match self.stat(self.name)? {
            Some(now) if same_object(staged, &now) => now,
            _ => return Err(self.unstable("name replaced right after commit")),
        };
        self.check_parent()?;
        point("commit.verified");
        Ok(Outcome::Applied(now))
    }

    /// §5.3 step 5 after a delete: the name must be free.
    fn finish_removed(&self) -> Result<Outcome> {
        self.sync_parent()?;
        if self.stat(self.name)?.is_some() {
            return Err(self.unstable("name re-created right after delete"));
        }
        self.check_parent()?;
        point("commit.verified");
        Ok(Outcome::Removed)
    }

    fn sync_parent(&self) -> Result<()> {
        let dir = rustix::fs::openat2(
            self.parent(),
            ".",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            RESOLVE,
        )
        .map_err(|e| self.err("open parent for fsync", e))?;
        rustix::fs::fsync(&dir).map_err(|e| self.err("fsync parent of", e))?;
        point("commit.synced");
        Ok(())
    }

    /// The parent must still resolve to the directory we committed in (§5.1).
    fn check_parent(&self) -> Result<()> {
        let parent_now = self
            .root
            .resolve_parent(self.path)
            .and_then(|(fd, _)| Fingerprint::of_fd(fd.as_fd()));
        match parent_now {
            Ok(p) if p.same_file(&self.parent_fp) => Ok(()),
            _ => Err(self.unstable("parent directory moved during commit")),
        }
    }

    /// Renames `from` (in the parent) to a free conflict name for this
    /// target, and returns the conflict path.
    fn preserve(&self, replica: ReplicaId, from: &[u8], kind: FileKind) -> Result<RelPath> {
        let name = rename_to_conflict(self.parent(), from, self.name, kind, replica)
            .map_err(|e| self.err("keep conflict copy of", e))?;
        let dir = self.path.parent().unwrap_or_default();
        let conflict = dir.join(&name)?;
        tracing::warn!(path = %self.path, %conflict, "concurrent change kept as a conflict copy");
        Ok(conflict)
    }
}

/// Same inode and kind; for non-directories also the same size and mtime.
/// ctime is not compared: rename bumps it.
fn same_object(a: &Fingerprint, b: &Fingerprint) -> bool {
    a.same_file(b)
        && a.kind == b.kind
        && (a.kind == FileKind::Dir || (a.size == b.size && a.mtime_ns == b.mtime_ns))
}

/// §5.3 step 4(a), done early and cheaply so we don't stage content for a
/// replace that cannot succeed. The binding check comes after staging.
fn precheck(t: &Target<'_>, expected: &Expected) -> Result<Option<&'static str>> {
    Ok(match t.stat(t.name)? {
        None => Some("name missing"),
        Some(fp) if !expected.fp.unchanged(&fp) => Some("changed since scan"),
        Some(fp) if !matches!(fp.kind, FileKind::File | FileKind::Symlink) => {
            Some("not a file or symlink")
        }
        Some(_) => None,
    })
}

// ---------------------------------------------------------------------------
// Journal (§5.3 step 1, §5.8)
// ---------------------------------------------------------------------------

/// The journal record of one commit. Every reserved name the commit may use
/// is chosen here, so it is in the journal before it exists on disk.
struct Record<'j> {
    journal: &'j Journal,
    /// `None` until first written.
    id: Option<IntentId>,
    intent: Intent,
}

impl<'j> Record<'j> {
    fn new(
        journal: &'j Journal,
        op: IntentOp,
        t: &Target<'_>,
        expected: Option<&Expected>,
    ) -> Result<Record<'j>> {
        let fresh = |kind| TmpId::random().map(|id| tmpname::name(kind, id));
        let tmp = fresh(match op {
            IntentOp::Delete => TmpKind::Del,
            _ => TmpKind::Temp,
        })?;
        let old = match op {
            IntentOp::Replace | IntentOp::Delete => Some(fresh(TmpKind::Old)?),
            IntentOp::Create | IntentOp::Rename => None,
        };
        Ok(Record {
            journal,
            id: None,
            intent: Intent {
                op,
                path: t.path.clone(),
                parent: t.parent_fp,
                tmp,
                old,
                expected: expected.copied(),
                staged: None,
                state: IntentState::Started,
            },
        })
    }

    /// The staging or move-aside name.
    fn tmp(&self) -> &[u8] {
        &self.intent.tmp
    }

    /// The quarantine name (replace, delete).
    fn old(&self) -> &[u8] {
        self.intent
            .old
            .as_deref()
            .expect("replace and delete have a quarantine name")
    }

    fn id(&self) -> IntentId {
        self.id.expect("intent recorded")
    }

    /// Records the intent durably. Called before the first reserved name is
    /// created or a user object is moved to one.
    fn begin(&mut self) -> Result<()> {
        if self.id.is_none() {
            self.id = Some(self.journal.begin(&self.intent)?);
            point("journal.started");
        }
        Ok(())
    }

    /// Records the staged object N durably, before anything can be
    /// exchanged with it. For an `O_TMPFILE` this happens before it is
    /// linked in, and is the first record. A create never exchanges, so once
    /// begun it needs no update.
    fn staged(&mut self, fp: &Fingerprint) -> Result<()> {
        self.intent.staged = Some(*fp);
        if self.id.is_some() && self.intent.op == IntentOp::Create {
            return Ok(());
        }
        self.intent.state = IntentState::TempWritten;
        match self.id {
            None => self.id = Some(self.journal.begin(&self.intent)?),
            Some(id) => self.journal.update(id, &self.intent, true)?,
        }
        point("journal.temp_written");
        Ok(())
    }

    /// The user's object is at our reserved name now. Not durable, and a
    /// failure is only logged: replay inspects the names whatever the state
    /// says, and the commit must go on to put the object where it belongs.
    fn exchanged(&mut self) {
        self.intent.state = IntentState::Exchanged;
        if let Err(e) = self.journal.update(self.id(), &self.intent, false) {
            tracing::warn!(path = %self.intent.path, error = %e, "cannot update intent");
        }
    }
}

// ---------------------------------------------------------------------------
// Staging (§5.3 step 2)
// ---------------------------------------------------------------------------

/// A new file being written in a replica directory, not yet visible.
///
/// It is an unnamed `O_TMPFILE` inode when [`Caps::tmpfile_usable`], else an
/// `O_CREAT|O_EXCL` file under its reserved temp name. Content is hashed as
/// it is written. Dropping it before [`TempFile::finish`] removes it.
pub struct TempFile<'p> {
    parent: BorrowedFd<'p>,
    /// Always `Some` until `finish` moves it out.
    fd: Option<OwnedFd>,
    /// The reserved name it is (or will be) linked in under.
    name: Vec<u8>,
    /// Whether `name` exists: from the start for a named temp file; for
    /// `O_TMPFILE` only once linked.
    named: bool,
    link_empty_path: bool,
    hasher: blake3::Hasher,
    size: u64,
}

impl<'p> TempFile<'p> {
    /// Creates the temp file for the reserved name `name`, which must be
    /// free (it was chosen at random and journaled before).
    pub fn create(parent: BorrowedFd<'p>, caps: &Caps, name: &[u8]) -> Result<TempFile<'p>> {
        let mode = Mode::from_raw_mode(0o600);
        let named = !caps.tmpfile_usable();
        let fd = if named {
            let flags =
                OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            rustix::fs::openat2(parent, name, flags, mode, RESOLVE)
                .map_err(|e| sys_err("create temp file", e))?
        } else {
            rustix::fs::openat2(
                parent,
                ".",
                OFlags::TMPFILE | OFlags::WRONLY | OFlags::CLOEXEC,
                mode,
                RESOLVE,
            )
            .map_err(|e| sys_err("create O_TMPFILE", e))?
        };
        point("stage.created");
        Ok(TempFile {
            parent,
            fd: Some(fd),
            name: name.to_vec(),
            named,
            link_empty_path: caps.linkat_empty_path,
            hasher: blake3::Hasher::new(),
            size: 0,
        })
    }

    fn fd(&self) -> BorrowedFd<'_> {
        self.fd
            .as_ref()
            .expect("TempFile used after finish")
            .as_fd()
    }

    pub fn write_all(&mut self, mut data: &[u8]) -> Result<()> {
        self.hasher.update(data);
        self.size += data.len() as u64;
        while !data.is_empty() {
            match rustix::io::write(self.fd(), data) {
                Ok(0) => return Err(sys_err("write temp file", Errno::IO)),
                Ok(n) => data = &data[n..],
                Err(Errno::INTR) => {}
                Err(e) => return Err(sys_err("write temp file", e)),
            }
        }
        Ok(())
    }

    /// Writes everything `content` yields.
    pub fn copy_from(&mut self, content: &mut dyn Read) -> Result<()> {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = match content.read(&mut buf) {
                Ok(0) => return Ok(()),
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Error::from_stream("read content to commit", e)),
            };
            self.write_all(&buf[..n])?;
        }
    }

    /// blake3 of everything written so far.
    pub fn hash(&self) -> [u8; 32] {
        *self.hasher.finalize().as_bytes()
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Sets mode and mtime, fsyncs, calls `record` with the file's
    /// fingerprint (to journal it) and links the file in under its reserved
    /// name. The returned [`Staged`] keeps the fd open, pinning the inode.
    pub fn finish(
        mut self,
        meta: FileMeta,
        record: impl FnOnce(&Fingerprint) -> Result<()>,
    ) -> Result<Staged<'p>> {
        // On failure before the hand-over below, `self`'s Drop removes a
        // named temp file.
        let fd = self
            .fd
            .as_ref()
            .expect("TempFile used after finish")
            .as_fd();
        rustix::fs::fchmod(fd, Mode::from_raw_mode(meta.mode & MODE_MASK))
            .map_err(|e| sys_err("chmod temp file", e))?;
        let times = Timestamps {
            last_access: Timespec {
                tv_sec: 0,
                tv_nsec: UTIME_OMIT,
            },
            last_modification: timespec(meta.mtime_ns),
        };
        rustix::fs::futimens(fd, &times).map_err(|e| sys_err("set temp file mtime", e))?;
        rustix::fs::fsync(fd).map_err(|e| sys_err("fsync temp file", e))?;
        point("stage.synced");
        record(&Fingerprint::of_fd(fd)?)?;

        if !self.named {
            let name = self.name.as_slice();
            let res = if self.link_empty_path {
                rustix::fs::linkat(fd, "", self.parent, name, AtFlags::EMPTY_PATH)
            } else {
                let proc_path = format!("/proc/self/fd/{}", fd.as_raw_fd());
                rustix::fs::linkat(
                    CWD,
                    proc_path.as_str(),
                    self.parent,
                    name,
                    AtFlags::SYMLINK_FOLLOW,
                )
            };
            res.map_err(|e| sys_err("link temp file", e))?;
            self.named = true;
        }
        let fp = Fingerprint::of_fd(fd)?;
        point("stage.linked");
        // Hand over: from here on `Staged` cleans up, and `self`'s Drop does nothing.
        self.named = false;
        Ok(Staged {
            parent: self.parent,
            name: std::mem::take(&mut self.name),
            fp,
            pin: self.fd.take().expect("TempFile used after finish"),
            live: true,
        })
    }
}

impl Drop for TempFile<'_> {
    fn drop(&mut self) {
        // An O_TMPFILE inode vanishes with its fd; a named one must be removed.
        if let (true, Some(fd)) = (self.named, self.fd.as_ref()) {
            let name = &self.name;
            let res = Fingerprint::of_fd(fd.as_fd())
                .and_then(|ours| remove_if(self.parent, name, |f| f.same_file(&ours)));
            if let Err(e) = res {
                tracing::warn!(name = %name.escape_ascii(), error = %e, "cannot remove temp file");
            }
        }
    }
}

fn timespec(ns: i64) -> Timespec {
    Timespec {
        tv_sec: ns.div_euclid(1_000_000_000),
        tv_nsec: ns.rem_euclid(1_000_000_000) as _,
    }
}

/// A new object linked in under a reserved temp name, pinned by an open fd.
///
/// While `live`, the temp name is ours to remove; dropping a live `Staged`
/// removes it if it still holds the staged object.
pub struct Staged<'p> {
    parent: BorrowedFd<'p>,
    /// The temp name. After an exchange it holds the old occupant instead
    /// (and `live` is false).
    name: Vec<u8>,
    /// The staged object (N) as linked in.
    fp: Fingerprint,
    pin: OwnedFd,
    live: bool,
}

impl Staged<'_> {
    pub fn fingerprint(&self) -> &Fingerprint {
        &self.fp
    }

    fn is_ours(&self, fp: &Fingerprint) -> bool {
        same_object(&self.fp, fp)
    }

    /// Removes the temp name if it still holds the staged object.
    fn discard(&mut self) -> Result<()> {
        if !std::mem::take(&mut self.live) {
            return Ok(());
        }
        let fp = self.fp;
        remove_if(self.parent, &self.name, |f| same_object(&fp, f)).map(drop)
    }
}

impl Drop for Staged<'_> {
    fn drop(&mut self) {
        if let Err(e) = self.discard() {
            tracing::warn!(name = %self.name.escape_ascii(), error = %e, "cannot remove temp object");
        }
    }
}

/// Unlinks `parent/name` if it exists and `ours` accepts its fingerprint.
/// Returns whether it was removed. Something else at `name` is left alone.
fn remove_if(
    parent: BorrowedFd<'_>,
    name: &[u8],
    ours: impl Fn(&Fingerprint) -> bool,
) -> Result<bool> {
    let Some(fp) = Fingerprint::at_opt(parent, name)? else {
        return Ok(false);
    };
    if !ours(&fp) {
        tracing::warn!(name = %name.escape_ascii(), "reserved name holds a foreign object; left alone");
        return Ok(false);
    }
    let flags = if fp.kind == FileKind::Dir {
        AtFlags::REMOVEDIR
    } else {
        AtFlags::empty()
    };
    match rustix::fs::unlinkat(parent, name, flags) {
        Ok(()) | Err(Errno::NOENT) => Ok(true),
        Err(e) => Err(Error::io(
            format!("remove {}", name.escape_ascii()),
            e.into(),
        )),
    }
}

/// Writes `content` to a temp file in the target's directory and links it
/// in. `Ok(Err(reason))` when the content does not hash to `expected_hash`.
///
/// A named temp file exists from the start, so its intent is recorded first;
/// an `O_TMPFILE` is recorded, with its inode, just before it is linked in.
fn stage_file<'p>(
    t: &'p Target<'_>,
    caps: &Caps,
    rec: &mut Record<'_>,
    content: &mut dyn Read,
    meta: FileMeta,
    expected_hash: &[u8; 32],
) -> Result<std::result::Result<Staged<'p>, &'static str>> {
    if !caps.tmpfile_usable() {
        rec.begin()?;
    }
    let mut tmp = TempFile::create(t.parent(), caps, rec.tmp())?;
    tmp.copy_from(content)?;
    point("stage.written");
    if tmp.hash() != *expected_hash {
        return Ok(Err("content hash mismatch"));
    }
    tmp.finish(meta, |fp| rec.staged(fp)).map(Ok)
}

/// `symlinkat(target, parent, tmp)`, then pins the link and checks it is the
/// one we made.
fn stage_symlink<'p>(t: &'p Target<'_>, rec: &mut Record<'_>, target: &[u8]) -> Result<Staged<'p>> {
    if target.is_empty() || target.contains(&0) {
        return Err(Error::io(
            format!("symlink {}", t.path),
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty or NUL-containing target",
            ),
        ));
    }
    let parent = t.parent();
    rec.begin()?;
    let name = rec.tmp().to_vec();
    rustix::fs::symlinkat(target, parent, name.as_slice())
        .map_err(|e| t.err("create temp symlink for", e))?;
    point("stage.linked");
    // O_PATH|O_NOFOLLOW opens the link itself, despite RESOLVE_NO_SYMLINKS.
    let pin = rustix::fs::openat2(
        parent,
        name.as_slice(),
        OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
        RESOLVE,
    )
    .map_err(|e| t.err("pin temp symlink for", e))?;
    let fp = Fingerprint::of_fd(pin.as_fd())?;
    let ours = fp.kind == FileKind::Symlink
        && rustix::fs::readlinkat(&pin, "", Vec::new()).is_ok_and(|got| got.as_bytes() == target);
    if !ours {
        return Err(Error::Unstable {
            path: name,
            reason: "temp symlink replaced before it was pinned",
        });
    }
    let staged = Staged {
        parent,
        name,
        fp,
        pin,
        live: true,
    };
    rec.staged(&fp)?;
    Ok(staged)
}

/// `mkdirat(parent, tmp, 0700)`, then pins the directory and sets its mode.
fn stage_dir<'p>(t: &'p Target<'_>, rec: &mut Record<'_>, mode: u32) -> Result<Staged<'p>> {
    let parent = t.parent();
    rec.begin()?;
    let name = rec.tmp().to_vec();
    rustix::fs::mkdirat(parent, name.as_slice(), Mode::from_raw_mode(0o700))
        .map_err(|e| t.err("create temp directory for", e))?;
    point("stage.linked");
    let pin = rustix::fs::openat2(
        parent,
        name.as_slice(),
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
        RESOLVE,
    )
    .map_err(|e| t.err("pin temp directory for", e))?;
    let mut staged = Staged {
        parent,
        name,
        fp: Fingerprint::of_fd(pin.as_fd())?,
        pin,
        live: true,
    };
    rustix::fs::fchmod(&staged.pin, Mode::from_raw_mode(mode & MODE_MASK))
        .map_err(|e| t.err("chmod temp directory for", e))?;
    staged.fp = Fingerprint::of_fd(staged.pin.as_fd())?;
    Ok(staged)
}

// ---------------------------------------------------------------------------
// Create (§5.3 step 3)
// ---------------------------------------------------------------------------

fn commit_create(t: &Target<'_>, mut staged: Staged<'_>) -> Result<Outcome> {
    point("create.before_rename");
    match rustix::fs::renameat_with(
        t.parent(),
        staged.name.as_slice(),
        t.parent(),
        t.name,
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => staged.live = false,
        Err(Errno::EXIST) => {
            staged.discard()?;
            return Ok(Outcome::PreconditionFailed("name created concurrently"));
        }
        // `staged` is dropped, which removes the temp name.
        Err(e) => return Err(t.err("commit", e)),
    }
    point("create.after_rename");
    t.finish(&staged.fp)
}

// ---------------------------------------------------------------------------
// Replace (§5.3 step 4)
// ---------------------------------------------------------------------------

fn commit_replace(
    ctx: &Ctx<'_>,
    quarantine: &mut Quarantine,
    t: &Target<'_>,
    mut staged: Staged<'_>,
    expected: &Expected,
    rec: &mut Record<'_>,
) -> Result<Outcome> {
    // (a) Pin the old inode and check it against the index.
    point("replace.before_pin");
    let (pin, old) = match pin_expected(t, expected)? {
        Ok(pinned) => pinned,
        Err(reason) => {
            staged.discard()?;
            return Ok(Outcome::PreconditionFailed(reason));
        }
    };
    point("replace.pinned");

    // (b) T18: take an F_WRLCK lease on an O_RDONLY fd for `old` here when
    // `ctx.caps.leases`, and give up with PreconditionFailed if it is refused.
    point("replace.before_exchange");

    // (c) Exchange: N goes to `name`, the old occupant to the temp name.
    match rustix::fs::renameat_with(
        t.parent(),
        staged.name.as_slice(),
        t.parent(),
        t.name,
        RenameFlags::EXCHANGE,
    ) {
        Ok(()) => staged.live = false,
        Err(Errno::NOENT) => {
            // `name` was removed after the pin (or our temp name was).
            staged.discard()?;
            return Ok(Outcome::PreconditionFailed("name removed"));
        }
        Err(e) => return Err(t.err("exchange", e)),
    }
    rec.exchanged();
    point("replace.after_exchange");

    // (d) Verify what came out.
    match verify_old(t, &staged.name, &old, expected, "replace.before_rehash") {
        Verified::Unchanged => {
            point("replace.verified");
            // (f) Quarantine the old inode.
            quarantine.add(t, &staged.name, rec.old(), old, pin, rec.id())?;
            point("replace.quarantined");
        }
        Verified::Gone => {
            tracing::warn!(path = %t.path, "replaced object vanished from its temp name");
        }
        Verified::Changed => return undo(ctx, t, staged),
    }
    t.finish(&staged.fp)
}

/// §5.3 step 4(a): pins the object at the target name with `O_PATH|O_NOFOLLOW`
/// and checks it against `expected`. `Ok(Err(reason))` if it does not match.
fn pin_expected(
    t: &Target<'_>,
    expected: &Expected,
) -> Result<std::result::Result<(OwnedFd, Fingerprint), &'static str>> {
    let pin = match rustix::fs::openat2(
        t.parent(),
        t.name,
        OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
        RESOLVE,
    ) {
        Ok(pin) => pin,
        Err(Errno::NOENT) => return Ok(Err("name missing")),
        Err(e) => return Err(t.err("pin", e)),
    };
    let old = Fingerprint::of_fd(pin.as_fd())?;
    if !expected.fp.unchanged(&old) || !matches!(old.kind, FileKind::File | FileKind::Symlink) {
        return Ok(Err("changed since scan"));
    }
    Ok(Ok((pin, old)))
}

enum Verified {
    /// The old inode, unmodified since the precondition check.
    Unchanged,
    /// Something else, or the old inode modified.
    Changed,
    /// Nothing at the temp name any more.
    Gone,
}

/// §5.3 step 4(d): is the object at `tmp` still the old inode, unmodified?
/// ctime is not compared, since the rename itself bumps it. Errors count as
/// "changed", so the caller undoes the rename. `rehash_point` is the hook
/// point reached before a rehash.
fn verify_old(
    t: &Target<'_>,
    tmp: &[u8],
    old: &Fingerprint,
    expected: &Expected,
    rehash_point: &'static str,
) -> Verified {
    let out = match t.stat(tmp) {
        Ok(Some(out)) => out,
        Ok(None) => return Verified::Gone,
        Err(e) => {
            tracing::warn!(path = %t.path, error = %e, "cannot verify replaced object");
            return Verified::Changed;
        }
    };
    if !same_object(old, &out) {
        return Verified::Changed;
    }
    if old.kind == FileKind::File
        && let Some(hash) = expected.hash
        && (expected.racy || old.size < REHASH_BELOW)
    {
        point(rehash_point);
        match stable_read(t.parent(), tmp, &mut Discard) {
            Ok((fp, h)) if same_object(old, &fp) && h == hash => {}
            Ok(_) => return Verified::Changed,
            Err(e) => {
                tracing::debug!(path = %t.path, error = %e, "rehash of replaced object failed");
                return Verified::Changed;
            }
        }
    }
    // T18: if a lease was taken, F_GETLEASE must still return F_WRLCK.
    Verified::Unchanged
}

/// §5.3 step 4(e): the old object was modified or replaced between the check
/// and the exchange. Swap back so the user's version is at `name` again, and
/// remove ours, unless that too was modified: then both are user data and the
/// one at the temp name is kept as a conflict copy.
fn undo(ctx: &Ctx<'_>, t: &Target<'_>, mut staged: Staged<'_>) -> Result<Outcome> {
    let failed = Outcome::PreconditionFailed("changed during commit");
    point("replace.before_undo");
    match rustix::fs::renameat_with(
        t.parent(),
        staged.name.as_slice(),
        t.parent(),
        t.name,
        RenameFlags::EXCHANGE,
    ) {
        Ok(()) => {
            point("replace.after_undo");
            match t.stat(&staged.name)? {
                Some(back) if staged.is_ours(&back) => {
                    staged.live = true;
                    staged.discard()?;
                    Ok(failed)
                }
                Some(back) => {
                    let conflict = t.preserve(ctx.replica, &staged.name, back.kind)?;
                    Ok(Outcome::Preserved { conflict })
                }
                None => Ok(failed),
            }
        }
        Err(Errno::NOENT) => {
            // `name` was removed after the exchange (or the temp name was):
            // move the user's object back to `name`.
            point("replace.after_undo");
            restore(ctx, t, &staged.name)
        }
        Err(e) => Err(t.err("undo exchange of", e)),
    }
}

/// Moves a user object from our reserved name `from` back to the target name
/// with `RENAME_NOREPLACE`. If the name was taken meanwhile, the object is
/// kept under a conflict name instead; nothing is overwritten.
fn restore(ctx: &Ctx<'_>, t: &Target<'_>, from: &[u8]) -> Result<Outcome> {
    match rustix::fs::renameat_with(t.parent(), from, t.parent(), t.name, RenameFlags::NOREPLACE) {
        // NOENT: it vanished from our name, so there is nothing to restore.
        Ok(()) | Err(Errno::NOENT) => Ok(Outcome::PreconditionFailed("changed during commit")),
        Err(Errno::EXIST) => {
            let kind = t.stat(from)?.map_or(FileKind::File, |f| f.kind);
            let conflict = t.preserve(ctx.replica, from, kind)?;
            Ok(Outcome::Preserved { conflict })
        }
        Err(e) => Err(t.err("restore", e)),
    }
}

// ---------------------------------------------------------------------------
// Delete (§5.6)
// ---------------------------------------------------------------------------

fn commit_delete(
    ctx: &Ctx<'_>,
    quarantine: &mut Quarantine,
    t: &Target<'_>,
    expected: &Expected,
    rec: &mut Record<'_>,
) -> Result<Outcome> {
    // 1. Pin the object and check it against the index.
    point("delete.before_pin");
    let (pin, old) = match pin_expected(t, expected)? {
        Ok(pinned) => pinned,
        Err(reason) => return Ok(Outcome::PreconditionFailed(reason)),
    };
    point("delete.pinned");

    rec.begin()?;
    // T18: take an F_WRLCK lease here when `ctx.caps.leases`.
    point("delete.before_rename");

    // 2. Move it aside; NOREPLACE so a reserved name is never overwritten.
    let del = rec.tmp().to_vec();
    match rustix::fs::renameat_with(
        t.parent(),
        t.name,
        t.parent(),
        del.as_slice(),
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => {}
        Err(Errno::NOENT) => return Ok(Outcome::PreconditionFailed("name removed")),
        Err(e) => return Err(t.err("move aside for delete", e)),
    }
    rec.exchanged();
    point("delete.after_rename");

    // 3. Verify what was moved.
    match verify_old(t, &del, &old, expected, "delete.before_rehash") {
        Verified::Unchanged => {
            point("delete.verified");
            // 4. Quarantine rather than unlink, for writers holding an fd.
            quarantine.add(t, &del, rec.old(), old, pin, rec.id())?;
            point("delete.quarantined");
        }
        Verified::Gone => {
            tracing::warn!(path = %t.path, "deleted object vanished from its temp name");
        }
        Verified::Changed => {
            // 5. Modified or replaced before it was moved: put it back.
            point("delete.before_restore");
            return restore(ctx, t, &del);
        }
    }
    t.finish_removed()
}

// ---------------------------------------------------------------------------
// Rename to a conflict name (§6.2)
// ---------------------------------------------------------------------------

fn commit_rename(
    ctx: &Ctx<'_>,
    t: &Target<'_>,
    expected: &Expected,
    to: &[u8],
    rec: &mut Record<'_>,
) -> Result<Outcome> {
    // 1. Pin the object and check it against the index.
    point("rename.before_pin");
    let (_pin, old) = match pin_expected(t, expected)? {
        Ok(pinned) => pinned,
        Err(reason) => return Ok(Outcome::PreconditionFailed(reason)),
    };
    point("rename.pinned");

    rec.begin()?;
    // T18: take an F_WRLCK lease here when `ctx.caps.leases`.
    point("rename.before_rename");

    // 2. Move it aside to a reserved name, so what we verify next can only
    //    be the object we moved.
    let tmp = rec.tmp().to_vec();
    match rustix::fs::renameat_with(
        t.parent(),
        t.name,
        t.parent(),
        tmp.as_slice(),
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => {}
        Err(Errno::NOENT) => return Ok(Outcome::PreconditionFailed("name removed")),
        Err(e) => return Err(t.err("move aside for rename", e)),
    }
    rec.exchanged();
    point("rename.after_rename");

    // 3. Verify what was moved; put it back if it was modified.
    match verify_old(t, &tmp, &old, expected, "rename.before_rehash") {
        Verified::Unchanged => point("rename.verified"),
        Verified::Gone => return Err(t.unstable("renamed object vanished from its temp name")),
        Verified::Changed => {
            point("rename.before_restore");
            return restore(ctx, t, &tmp);
        }
    }

    // 4. On to the new name; never over something that appeared there.
    match rustix::fs::renameat_with(
        t.parent(),
        tmp.as_slice(),
        t.parent(),
        to,
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => {}
        Err(Errno::EXIST) => {
            point("rename.before_restore");
            return Ok(match restore(ctx, t, &tmp)? {
                Outcome::PreconditionFailed(_) => Outcome::PreconditionFailed("new name exists"),
                other => other,
            });
        }
        Err(Errno::NOENT) => return Err(t.unstable("renamed object vanished from its temp name")),
        Err(e) => return Err(t.err("rename", e)),
    }
    point("rename.moved");

    // 5. Step 5 checks at the new name.
    t.sync_parent()?;
    let now = match t.stat(to)? {
        Some(now) if same_object(&old, &now) => now,
        _ => return Err(t.unstable("renamed object replaced right after commit")),
    };
    t.check_parent()?;
    point("commit.verified");
    Ok(Outcome::Applied(now))
}

/// Renames `parent/from` to a free conflict name for `orig` (§6.2), trying
/// later timestamps if the name is taken. Returns the new name.
fn rename_to_conflict(
    parent: BorrowedFd<'_>,
    from: &[u8],
    orig: &[u8],
    kind: FileKind,
    replica: ReplicaId,
) -> std::result::Result<Vec<u8>, Errno> {
    let now = jiff::Zoned::now().datetime();
    for i in 0..NAME_RETRIES {
        let when = now.checked_add(i.seconds()).unwrap_or(now);
        let name = tmpname::conflict_name(orig, kind == FileKind::File, when, replica);
        match rustix::fs::renameat_with(
            parent,
            from,
            parent,
            name.as_slice(),
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) => return Ok(name),
            Err(Errno::EXIST) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(Errno::EXIST)
}

fn sys_err(what: &str, e: Errno) -> Error {
    Error::io(what, e.into())
}

// ---------------------------------------------------------------------------
// Recovery (§5.8)
// ---------------------------------------------------------------------------

/// What [`recover`] did with one intent.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Recovered {
    /// Our own staged objects removed.
    pub removed: usize,
    /// Old inodes put (back) into the quarantine.
    pub quarantined: usize,
    /// User objects moved back to their name.
    pub restored: usize,
    /// User objects kept under these conflict names instead.
    pub conflicts: Vec<RelPath>,
    /// Foreign objects found at the intent's reserved names, left alone.
    pub foreign: usize,
    /// Why the intent was not replayed: its directory was moved, removed or
    /// replaced, so its names are out of reach. Nothing was touched.
    pub skipped: Option<&'static str>,
}

/// Replays the journal intent `id` of a commit that did not finish (a crash,
/// or an error on the way), as §5.8 describes. It inspects every reserved
/// name the intent recorded, whatever its state:
///
/// - our staged object N is removed. Before N is recorded
///   ([`IntentState::Started`]) nothing of the user's can be at the staging
///   name yet, so whatever is there is ours;
/// - the expected old object O at the staging name (after an exchange) or
///   the `.del.` name is verified as in §5.3 step 4(d): unchanged, it is
///   quarantined as the commit would have done (roll forward); modified, it
///   goes back to the name, by exchanging back if the name still holds N
///   (step (e)), else with `RENAME_NOREPLACE` or under a conflict name;
/// - any other object there is user data and goes back the same way. A
///   rename's moved-aside object always goes back to its name;
/// - O at the quarantine name is quarantined again, with a fresh grace
///   period (whoever wrote to it may still hold an fd).
///
/// Replaying is idempotent: an intent that already finished finds its names
/// empty. Afterwards the intent is done unless `quarantine` [holds] it.
///
/// [holds]: Quarantine::holds
pub fn recover(
    ctx: &Ctx<'_>,
    quarantine: &mut Quarantine,
    id: IntentId,
    intent: &Intent,
) -> Result<Recovered> {
    let mut rep = Recovered::default();
    if quarantine.holds(id) {
        // The commit got as far as quarantining; nothing else is left.
        return Ok(rep);
    }
    let t = match Target::open(ctx.root, &intent.path) {
        Ok(t) if t.parent_fp.same_file(&intent.parent) => t,
        Ok(_) => {
            rep.skipped = Some("directory replaced");
            return Ok(rep);
        }
        Err(e) if e.is_unstable() || e.is_not_found() => {
            rep.skipped = Some("directory moved or removed");
            return Ok(rep);
        }
        Err(e) => return Err(e),
    };
    point("recover.resolved");
    let tmp = intent.tmp.as_slice();
    match (intent.op, inspect(&t, tmp, intent)?) {
        (_, Found::Absent) => {}
        (IntentOp::Create | IntentOp::Replace, Found::Staged) => remove_ours(&t, tmp, &mut rep)?,
        // Recorded before anything was exchanged: only ours can be there.
        (IntentOp::Create | IntentOp::Replace, Found::Other) if intent.staged.is_none() => {
            remove_ours(&t, tmp, &mut rep)?
        }
        (IntentOp::Create, _) => {
            tracing::warn!(path = %t.path, name = %tmp.escape_ascii(), "foreign object at a reserved name; left alone");
            rep.foreign += 1;
        }
        (IntentOp::Replace | IntentOp::Delete, Found::Expected(pin)) => {
            let exp = intent.expected.as_ref().expect("found the expected object");
            match verify_old(&t, tmp, &exp.fp, exp, "recover.before_rehash") {
                Verified::Unchanged => {
                    quarantine.add(&t, tmp, quarantine_name(intent), exp.fp, pin, id)?;
                    rep.quarantined += 1;
                }
                Verified::Changed => put_back(ctx, &t, tmp, intent, &mut rep)?,
                Verified::Gone => {}
            }
        }
        _ => put_back(ctx, &t, tmp, intent, &mut rep)?,
    }
    // The quarantine name, unless the old inode just went there.
    if let Some(old) = intent.old.as_deref().filter(|_| !quarantine.holds(id)) {
        match inspect(&t, old, intent)? {
            Found::Absent => {}
            Found::Expected(pin) => {
                let exp = intent.expected.as_ref().expect("found the expected object");
                quarantine.add(&t, old, old, exp.fp, pin, id)?;
                rep.quarantined += 1;
            }
            Found::Staged | Found::Other => {
                tracing::warn!(path = %t.path, name = %old.escape_ascii(), "foreign object at a quarantine name; left alone");
                rep.foreign += 1;
            }
        }
    }
    point("recover.done");
    Ok(rep)
}

/// What a reserved name of an intent holds.
enum Found {
    Absent,
    /// The staged object N, unchanged.
    Staged,
    /// The expected old object O (any size or mtime), pinned.
    Expected(OwnedFd),
    Other,
}

fn inspect(t: &Target<'_>, name: &[u8], intent: &Intent) -> Result<Found> {
    let Some(fp) = t.stat(name)? else {
        return Ok(Found::Absent);
    };
    if intent.staged.is_some_and(|n| same_object(&n, &fp)) {
        return Ok(Found::Staged);
    }
    if let Some(exp) = intent.expected.filter(|e| e.fp.same_file(&fp)) {
        let pin = rustix::fs::openat2(
            t.parent(),
            name,
            OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
            RESOLVE,
        );
        // Re-checked through the pin, so the inode cannot change under us.
        if let Ok(pin) = pin
            && Fingerprint::of_fd(pin.as_fd())?.same_file(&exp.fp)
        {
            return Ok(Found::Expected(pin));
        }
    }
    Ok(Found::Other)
}

fn quarantine_name(intent: &Intent) -> &[u8] {
    intent.old.as_deref().unwrap_or(&intent.tmp)
}

/// Removes our own object at `name` (a directory only if empty).
fn remove_ours(t: &Target<'_>, name: &[u8], rep: &mut Recovered) -> Result<()> {
    match remove_if(t.parent(), name, |_| true) {
        Ok(true) => rep.removed += 1,
        Ok(false) => {}
        Err(e) => {
            // A directory of ours that someone filled: keep it.
            tracing::warn!(path = %t.path, name = %name.escape_ascii(), error = %e, "cannot remove staged object");
            rep.foreign += 1;
        }
    }
    Ok(())
}

/// The user object at our reserved name `from` goes back to the target name:
/// by exchanging back if the name still holds our staged object (which then
/// is removed), else with `RENAME_NOREPLACE`, or under a conflict name.
fn put_back(
    ctx: &Ctx<'_>,
    t: &Target<'_>,
    from: &[u8],
    intent: &Intent,
    rep: &mut Recovered,
) -> Result<()> {
    if let Some(n) = intent.staged
        && t.stat(t.name)?.is_some_and(|fp| same_object(&n, &fp))
    {
        rustix::fs::renameat_with(t.parent(), from, t.parent(), t.name, RenameFlags::EXCHANGE)
            .map_err(|e| t.err("undo exchange of", e))?;
        point("recover.undone");
        rep.restored += 1;
        return match t.stat(from)? {
            Some(back) if same_object(&n, &back) => remove_ours(t, from, rep),
            Some(back) => {
                rep.conflicts
                    .push(t.preserve(ctx.replica, from, back.kind)?);
                Ok(())
            }
            None => Ok(()),
        };
    }
    match restore(ctx, t, from)? {
        Outcome::Preserved { conflict } => rep.conflicts.push(conflict),
        _ => rep.restored += 1,
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Quarantine (§5.3 step 4(f))
// ---------------------------------------------------------------------------

/// Old inodes replaced by a commit, kept as `.~fsync.old.<id>` until it is
/// safe to unlink them.
///
/// Someone may still hold an fd (or a writable mmap) on a replaced file and
/// write through it after the exchange. So an old inode is only unlinked once
/// its grace period has passed with mtime and size unchanged; if it changed,
/// [`Quarantine::sweep`] renames it to a conflict copy instead. (T18 adds the
/// faster path: unlink as soon as a write lease can be taken.)
///
/// Each entry belongs to an intent in the journal, which outlives a crash
/// (§5.8): the caller keeps it while [`Quarantine::holds`] it, and forgets it
/// once a sweep reports it [`finished`](SweepReport::finished).
pub struct Quarantine {
    replica: ReplicaId,
    grace: Duration,
    pending: Vec<Pending>,
}

/// One quarantined inode.
struct Pending {
    /// The directory holding it (an `O_PATH` fd, so it is found even if the
    /// directory is renamed).
    parent: OwnedFd,
    /// That directory's identity, for [`Quarantine::settle_dir`].
    dir_fp: Fingerprint,
    /// That directory's path when the entry was made, for conflict paths.
    dir: RelPath,
    /// The name the inode was replaced at, for conflict names.
    orig: Vec<u8>,
    /// Its current `.~fsync.old.<id>` name.
    name: Vec<u8>,
    /// The old inode as verified; size and mtime must stay the same.
    fp: Fingerprint,
    /// Keeps the inode number from being reused while we compare it.
    _pin: OwnedFd,
    /// When the grace period started.
    since: Instant,
    /// The journal intent that records this entry.
    intent: IntentId,
}

/// What a [`Quarantine::sweep`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Old inodes unlinked.
    pub removed: usize,
    /// Old inodes modified after the replace, now kept under these names.
    pub conflicts: Vec<RelPath>,
    /// Entries forgotten because their name vanished or holds a foreign
    /// object (which is left alone).
    pub dropped: usize,
    /// The journal intents of every entry that was settled (removed, made a
    /// conflict copy or dropped); their records can go.
    pub finished: Vec<IntentId>,
}

enum Verdict {
    Keep,
    Unlink,
    Conflict,
    Drop,
}

impl Quarantine {
    /// Twice the watcher's debounce interval (§5.3 step 4(f), §5.9).
    pub const DEFAULT_GRACE: Duration = Duration::from_millis(400);

    pub fn new(replica: ReplicaId, grace: Duration) -> Quarantine {
        Quarantine {
            replica,
            grace,
            pending: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn grace(&self) -> Duration {
        self.grace
    }

    /// Changes the grace period, for the entries already waiting too.
    pub fn set_grace(&mut self, grace: Duration) {
        self.grace = grace;
    }

    /// When the earliest entry becomes due for unlinking.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.pending.iter().map(|p| p.since + self.grace).min()
    }

    /// Whether an entry of the journal intent `id` is waiting.
    pub fn holds(&self, id: IntentId) -> bool {
        self.pending.iter().any(|p| p.intent == id)
    }

    /// Moves the old inode at the target's reserved name `from` to its
    /// quarantine name `to` (unless it is there already) and starts its grace
    /// period.
    fn add(
        &mut self,
        t: &Target<'_>,
        from: &[u8],
        to: &[u8],
        old: Fingerprint,
        pin: OwnedFd,
        intent: IntentId,
    ) -> Result<()> {
        let name = if from == to {
            to.to_vec()
        } else {
            match rustix::fs::renameat_with(
                t.parent(),
                from,
                t.parent(),
                to,
                RenameFlags::NOREPLACE,
            ) {
                Ok(()) => to.to_vec(),
                Err(e) => {
                    // Keep it where it is; the sweep handles it all the same.
                    tracing::warn!(path = %t.path, error = %e, "cannot rename old inode to quarantine name");
                    from.to_vec()
                }
            }
        };
        self.pending.push(Pending {
            parent: t
                .parent
                .try_clone()
                .map_err(|e| Error::io("duplicate parent fd", e))?,
            dir_fp: t.parent_fp,
            dir: t.path.parent().unwrap_or_default(),
            orig: t.name.to_vec(),
            name,
            fp: old,
            _pin: pin,
            since: Instant::now(),
            intent,
        });
        Ok(())
    }

    /// Processes every entry: one modified since the replace becomes a
    /// conflict copy; one unchanged past its deadline is unlinked; the rest
    /// wait. Errors are logged and the entry is retried on the next sweep.
    pub fn sweep(&mut self) -> SweepReport {
        let now = Instant::now();
        let grace = self.grace;
        self.process(|p| Some(now >= p.since + grace))
    }

    /// Settles every entry in the directory `dir` now, without waiting for
    /// its grace period: unchanged ones are unlinked, modified ones become
    /// conflict copies (in `dir`). [`rmdir`] calls this so quarantined
    /// children don't keep a deleted directory non-empty (§5.7).
    pub fn settle_dir(&mut self, dir: &Fingerprint) -> SweepReport {
        self.process(|p| p.dir_fp.same_file(dir).then_some(true))
    }

    /// Sweeps the entries `due` selects (`Some(due)`), leaving the others.
    fn process(&mut self, due: impl Fn(&Pending) -> Option<bool>) -> SweepReport {
        let mut report = SweepReport::default();
        let replica = self.replica;
        self.pending.retain(|p| {
            let Some(due) = due(p) else { return true };
            match p.sweep(due, replica, &mut report) {
                Ok(done) => {
                    if done {
                        report.finished.push(p.intent);
                    }
                    !done
                }
                Err(e) => {
                    tracing::warn!(dir = %p.dir, name = %p.name.escape_ascii(), error = %e, "quarantine sweep failed");
                    true
                }
            }
        });
        report
    }
}

impl Pending {
    fn verdict(&self, due: bool) -> Result<Verdict> {
        let Some(fp) = Fingerprint::at_opt(self.parent.as_fd(), &self.name)? else {
            return Ok(Verdict::Drop);
        };
        if !fp.same_file(&self.fp) {
            return Ok(Verdict::Drop);
        }
        if !same_object(&self.fp, &fp) {
            return Ok(Verdict::Conflict);
        }
        // T18: also Unlink before the deadline once a write lease can be taken.
        Ok(if due { Verdict::Unlink } else { Verdict::Keep })
    }

    /// Returns whether the entry is finished.
    fn sweep(&self, due: bool, replica: ReplicaId, report: &mut SweepReport) -> Result<bool> {
        let parent = self.parent.as_fd();
        let shown = || format!("{}/{}", self.dir, self.name.escape_ascii());
        match self.verdict(due)? {
            Verdict::Keep => Ok(false),
            Verdict::Drop => {
                tracing::warn!(name = %shown(), "quarantined object vanished or was replaced; forgetting it");
                report.dropped += 1;
                Ok(true)
            }
            Verdict::Unlink => {
                point("quarantine.before_unlink");
                match rustix::fs::unlinkat(parent, self.name.as_slice(), AtFlags::empty()) {
                    Ok(()) | Err(Errno::NOENT) => {}
                    Err(e) => return Err(Error::io(format!("unlink {}", shown()), e.into())),
                }
                report.removed += 1;
                Ok(true)
            }
            Verdict::Conflict => {
                point("quarantine.before_conflict");
                let name =
                    rename_to_conflict(parent, &self.name, &self.orig, self.fp.kind, replica)
                        .map_err(|e| {
                            Error::io(format!("keep conflict copy of {}", shown()), e.into())
                        })?;
                let conflict = self.dir.join(&name)?;
                tracing::warn!(%conflict, "replaced file was written to after the replace; kept as a conflict copy");
                report.conflicts.push(conflict);
                Ok(true)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::hooks;
    use std::cell::RefCell;
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    use std::path::{Path, PathBuf};
    use std::rc::Rc;
    use std::time::SystemTime;

    const REPLICA: ReplicaId = ReplicaId(0xabcdef0123456789);
    /// An mtime far in the past, so any write gives a different one.
    const OLD_MTIME: Duration = Duration::from_secs(1_600_000_000);

    struct Fx {
        dir: tempfile::TempDir,
        root: Root,
        caps: Caps,
        journal: Rc<Journal>,
    }

    impl Fx {
        fn new() -> Fx {
            let dir = tempfile::tempdir().unwrap();
            let root = Root::open(dir.path()).unwrap();
            let caps = Caps::probe(root.fd()).unwrap();
            let journal = Rc::new(Journal::in_memory().unwrap());
            Fx {
                dir,
                root,
                caps,
                journal,
            }
        }

        /// Without O_TMPFILE: named temp files.
        fn named() -> Fx {
            let mut fx = Fx::new();
            fx.caps.o_tmpfile = false;
            fx
        }

        fn ctx(&self) -> Ctx<'_> {
            Ctx {
                root: &self.root,
                caps: &self.caps,
                replica: REPLICA,
                journal: &self.journal,
            }
        }

        fn p(&self, rel: &str) -> PathBuf {
            self.dir.path().join(rel)
        }

        /// A user file with an old mtime.
        fn user_file(&self, rel: &str, data: &[u8]) -> Expected {
            let p = self.p(rel);
            fs::write(&p, data).unwrap();
            fs::File::options()
                .write(true)
                .open(&p)
                .unwrap()
                .set_modified(SystemTime::UNIX_EPOCH + OLD_MTIME)
                .unwrap();
            Expected {
                fp: self.root.stat(&rp(rel)).unwrap(),
                hash: Some(hash(data)),
                racy: false,
            }
        }

        /// Every reserved name under the root.
        fn leftovers(&self) -> Vec<PathBuf> {
            fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
                for e in fs::read_dir(dir).unwrap() {
                    let e = e.unwrap();
                    let name = e.file_name();
                    if tmpname::is_reserved(name.as_encoded_bytes()) {
                        out.push(e.path());
                    }
                    if e.file_type().unwrap().is_dir() {
                        walk(&e.path(), out);
                    }
                }
            }
            let mut out = Vec::new();
            walk(self.dir.path(), &mut out);
            out
        }
    }

    fn rp(s: &str) -> RelPath {
        RelPath::new(s).unwrap()
    }

    fn hash(data: &[u8]) -> [u8; 32] {
        *blake3::hash(data).as_bytes()
    }

    fn meta() -> FileMeta {
        FileMeta {
            mode: 0o640,
            mtime_ns: 1_700_000_000_123_456_789,
        }
    }

    fn create(fx: &Fx, rel: &str, data: &[u8]) -> Result<Outcome> {
        create_file(&fx.ctx(), &rp(rel), &mut &data[..], meta(), &hash(data))
    }

    fn replace(
        fx: &Fx,
        q: &mut Quarantine,
        rel: &str,
        exp: &Expected,
        data: &[u8],
    ) -> Result<Outcome> {
        replace_file(
            &fx.ctx(),
            q,
            &rp(rel),
            exp,
            &mut &data[..],
            meta(),
            &hash(data),
        )
    }

    fn quarantine() -> Quarantine {
        Quarantine::new(REPLICA, Duration::ZERO)
    }

    fn applied(o: Outcome) -> Fingerprint {
        match o {
            Outcome::Applied(fp) => fp,
            o => panic!("expected Applied, got {o:?}"),
        }
    }

    fn append(path: &Path, data: &[u8]) {
        fs::File::options()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(data)
            .unwrap();
    }

    // ----- T04: create-type commits -------------------------------------

    #[test]
    fn create_file_on_empty_name() {
        for fx in [Fx::new(), Fx::named()] {
            fs::create_dir(fx.p("d")).unwrap();
            let fp = applied(create(&fx, "d/f", b"hello").unwrap());
            let md = fs::symlink_metadata(fx.p("d/f")).unwrap();
            assert_eq!(fs::read(fx.p("d/f")).unwrap(), b"hello");
            assert_eq!(md.permissions().mode() & 0o7777, 0o640);
            assert_eq!(
                md.mtime() * 1_000_000_000 + md.mtime_nsec(),
                meta().mtime_ns
            );
            assert_eq!((fp.ino, fp.size, fp.kind), (md.ino(), 5, FileKind::File));
            assert_eq!(fp, fx.root.stat(&rp("d/f")).unwrap());
            assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
        }
    }

    #[test]
    fn create_file_strips_setuid_and_handles_large_content() {
        let fx = Fx::new();
        let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let m = FileMeta {
            mode: 0o6755,
            mtime_ns: -1_500_000_000,
        };
        let out = create_file(&fx.ctx(), &rp("big"), &mut &data[..], m, &hash(&data)).unwrap();
        applied(out);
        let md = fs::symlink_metadata(fx.p("big")).unwrap();
        assert_eq!(md.permissions().mode() & 0o7777, 0o755);
        assert_eq!((md.mtime(), md.mtime_nsec()), (-2, 500_000_000));
        assert_eq!(fs::read(fx.p("big")).unwrap(), data);
    }

    #[test]
    fn create_file_on_existing_name_fails() {
        for fx in [Fx::new(), Fx::named()] {
            fs::write(fx.p("f"), b"user").unwrap();
            let before = fx.root.stat(&rp("f")).unwrap();
            let out = create(&fx, "f", b"ours").unwrap();
            assert!(matches!(out, Outcome::PreconditionFailed(_)), "{out:?}");
            assert_eq!(fs::read(fx.p("f")).unwrap(), b"user");
            assert!(before.unchanged(&fx.root.stat(&rp("f")).unwrap()));
            assert!(fx.leftovers().is_empty());
        }
    }

    #[test]
    fn create_file_rejects_hash_mismatch() {
        for fx in [Fx::new(), Fx::named()] {
            let out = create_file(&fx.ctx(), &rp("f"), &mut &b"torn"[..], meta(), &hash(b"x"));
            assert_eq!(
                out.unwrap(),
                Outcome::PreconditionFailed("content hash mismatch")
            );
            assert!(!fx.p("f").exists());
            assert!(fx.leftovers().is_empty());
        }
    }

    #[test]
    fn concurrent_create_before_rename_wins() {
        for fx in [Fx::new(), Fx::named()] {
            let user = fx.p("f");
            let _g = hooks::once("create.before_rename", move || {
                fs::write(&user, b"user").unwrap()
            });
            let out = create(&fx, "f", b"ours").unwrap();
            assert_eq!(
                out,
                Outcome::PreconditionFailed("name created concurrently")
            );
            assert_eq!(fs::read(fx.p("f")).unwrap(), b"user");
            assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
        }
    }

    #[test]
    fn create_symlink_cases() {
        let fx = Fx::new();
        // Empty name.
        let fp = applied(create_symlink(&fx.ctx(), &rp("l"), b"../some/\xfftarget").unwrap());
        assert_eq!(fp.kind, FileKind::Symlink);
        let target = fs::read_link(fx.p("l")).unwrap();
        assert_eq!(target.as_os_str().as_encoded_bytes(), b"../some/\xfftarget");

        // Existing name.
        fs::write(fx.p("f"), b"user").unwrap();
        let out = create_symlink(&fx.ctx(), &rp("f"), b"x").unwrap();
        assert!(matches!(out, Outcome::PreconditionFailed(_)));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"user");

        // Concurrent create just before the rename.
        let user = fx.p("c");
        let _g = hooks::once("create.before_rename", move || {
            symlink("user", &user).unwrap()
        });
        let out = create_symlink(&fx.ctx(), &rp("c"), b"ours").unwrap();
        assert_eq!(
            out,
            Outcome::PreconditionFailed("name created concurrently")
        );
        assert_eq!(fs::read_link(fx.p("c")).unwrap(), Path::new("user"));

        // Invalid targets.
        assert!(create_symlink(&fx.ctx(), &rp("e"), b"").is_err());
        assert!(create_symlink(&fx.ctx(), &rp("e"), b"a\0b").is_err());
        assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
    }

    #[test]
    fn mkdir_cases() {
        let fx = Fx::new();
        // Empty name: the exact mode, regardless of the umask.
        let fp = applied(mkdir(&fx.ctx(), &rp("d"), 0o2777).unwrap());
        let md = fs::symlink_metadata(fx.p("d")).unwrap();
        assert!(md.is_dir());
        assert_eq!(md.permissions().mode() & 0o7777, 0o777);
        assert_eq!(fp.ino, md.ino());
        applied(mkdir(&fx.ctx(), &rp("d/e"), 0o700).unwrap());

        // Existing name, of either type.
        fs::write(fx.p("f"), b"user").unwrap();
        for p in ["d", "f"] {
            let out = mkdir(&fx.ctx(), &rp(p), 0o755).unwrap();
            assert!(matches!(out, Outcome::PreconditionFailed(_)));
        }
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"user");
        assert!(fx.p("d/e").is_dir());

        // Concurrent create just before the rename: a directory with a child.
        let user = fx.p("c");
        let _g = hooks::once("create.before_rename", move || {
            fs::create_dir(&user).unwrap();
            fs::write(user.join("child"), b"user").unwrap();
        });
        let out = mkdir(&fx.ctx(), &rp("c"), 0o755).unwrap();
        assert_eq!(
            out,
            Outcome::PreconditionFailed("name created concurrently")
        );
        assert_eq!(fs::read(fx.p("c/child")).unwrap(), b"user");
        assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
    }

    #[test]
    fn create_hits_every_step_in_order() {
        let fx = Fx::new();
        hooks::start_trace();
        applied(create(&fx, "f", b"x").unwrap());
        assert_eq!(
            hooks::take_trace(),
            [
                "commit.resolved",
                "stage.created",
                "stage.written",
                "stage.synced",
                "journal.temp_written",
                "stage.linked",
                "create.before_rename",
                "create.after_rename",
                "commit.synced",
                "commit.verified",
            ]
        );
    }

    #[test]
    fn symlinked_ancestor_is_unstable_and_writes_nothing_outside() {
        let fx = Fx::new();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), fx.p("out")).unwrap();
        for res in [
            create(&fx, "out/f", b"x"),
            create_symlink(&fx.ctx(), &rp("out/l"), b"x"),
            mkdir(&fx.ctx(), &rp("out/d"), 0o755),
        ] {
            assert!(matches!(res, Err(Error::Unstable { .. })), "{res:?}");
        }
        // The ancestor is swapped for a symlink after resolution: the commit
        // lands in the pinned directory, never outside.
        fs::create_dir(fx.p("a")).unwrap();
        let (a, out) = (fx.p("a"), outside.path().to_path_buf());
        let moved = fx.p("moved");
        let _g = hooks::once("stage.created", move || {
            fs::rename(&a, &moved).unwrap();
            symlink(&out, &a).unwrap();
        });
        let res = create(&fx, "a/f", b"x");
        assert!(matches!(res, Err(Error::Unstable { .. })), "{res:?}");
        assert_eq!(fs::read(fx.p("moved/f")).unwrap(), b"x");
        assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
    }

    #[test]
    fn reserved_names_and_root_are_refused() {
        let fx = Fx::new();
        assert!(matches!(
            create(&fx, ".~fsync.0123456789abcdef", b"x"),
            Err(Error::InvalidPath { .. })
        ));
        assert!(matches!(
            mkdir(&fx.ctx(), &RelPath::root(), 0o755),
            Err(Error::InvalidPath { .. })
        ));
    }

    #[test]
    fn name_replaced_right_after_commit_is_unstable() {
        let fx = Fx::new();
        let user = fx.p("f");
        let _g = hooks::once("create.after_rename", move || {
            fs::remove_file(&user).unwrap();
            fs::write(&user, b"user").unwrap();
        });
        let res = create(&fx, "f", b"ours");
        assert!(matches!(res, Err(Error::Unstable { .. })), "{res:?}");
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"user");
        assert!(fx.leftovers().is_empty());
    }

    // ----- T05: replace via RENAME_EXCHANGE ---------------------------------

    #[test]
    fn replace_normal() {
        for fx in [Fx::new(), Fx::named()] {
            let exp = fx.user_file("f", b"old");
            let mut q = quarantine();
            let fp = applied(replace(&fx, &mut q, "f", &exp, b"new content").unwrap());
            assert_eq!(fs::read(fx.p("f")).unwrap(), b"new content");
            assert_eq!(fp, fx.root.stat(&rp("f")).unwrap());
            assert_ne!(fp.ino, exp.fp.ino);
            // The old inode waits in quarantine until swept.
            assert_eq!(q.len(), 1);
            assert_eq!(fx.leftovers().len(), 1);
            let left = fx.leftovers()[0].file_name().unwrap().to_owned();
            assert_eq!(
                tmpname::parse(left.as_encoded_bytes()).map(|(k, _)| k),
                Some(TmpKind::Old)
            );
            let report = q.sweep();
            assert_eq!(report.removed, 1);
            assert!(q.is_empty());
            assert!(fx.leftovers().is_empty());
        }
    }

    #[test]
    fn replace_hits_every_step_in_order() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let mut q = quarantine();
        hooks::start_trace();
        applied(replace(&fx, &mut q, "f", &exp, b"new").unwrap());
        assert_eq!(
            hooks::take_trace(),
            [
                "commit.resolved",
                "stage.created",
                "stage.written",
                "stage.synced",
                "journal.temp_written",
                "stage.linked",
                "replace.before_pin",
                "replace.pinned",
                "replace.before_exchange",
                "replace.after_exchange",
                "replace.before_rehash",
                "replace.verified",
                "replace.quarantined",
                "commit.synced",
                "commit.verified",
            ]
        );
    }

    #[test]
    fn replace_with_stale_expectation_fails() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        append(&fx.p("f"), b" edited");
        let before = fx.root.stat(&rp("f")).unwrap();
        let mut q = quarantine();
        let out = replace(&fx, &mut q, "f", &exp, b"new").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed since scan"));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"old edited");
        assert!(before.unchanged(&fx.root.stat(&rp("f")).unwrap()));
        assert!(q.is_empty());
        assert!(fx.leftovers().is_empty());

        // A missing name fails too.
        fs::remove_file(fx.p("f")).unwrap();
        let out = replace(&fx, &mut q, "f", &exp, b"new").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("name missing"));
        assert!(!fx.p("f").exists());
    }

    /// The check at (a) happens after staging; a change between the early
    /// precheck and (a) must be caught there.
    #[test]
    fn replace_change_before_pin_fails() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let user = fx.p("f");
        let _g = hooks::once("stage.linked", move || append(&user, b" edited"));
        let mut q = quarantine();
        let out = replace(&fx, &mut q, "f", &exp, b"new").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed since scan"));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"old edited");
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn write_between_check_and_exchange_is_undone() {
        for fx in [Fx::new(), Fx::named()] {
            let exp = fx.user_file("f", b"old");
            let user = fx.p("f");
            let _g = hooks::once("replace.before_exchange", move || append(&user, b" edited"));
            let mut q = quarantine();
            let out = replace(&fx, &mut q, "f", &exp, b"new").unwrap();
            assert_eq!(out, Outcome::PreconditionFailed("changed during commit"));
            assert_eq!(fs::read(fx.p("f")).unwrap(), b"old edited");
            assert_eq!(fs::symlink_metadata(fx.p("f")).unwrap().ino(), exp.fp.ino);
            assert!(q.is_empty());
            assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
        }
    }

    /// Same size, mtime restored: only the rehash of small files catches it.
    #[test]
    fn mtime_preserving_write_is_caught_by_rehash() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let user = fx.p("f");
        let _g = hooks::once("replace.before_exchange", move || {
            let f = fs::File::options().write(true).open(&user).unwrap();
            (&f).write_all(b"OLD").unwrap();
            f.set_modified(SystemTime::UNIX_EPOCH + OLD_MTIME).unwrap();
        });
        let mut q = quarantine();
        let out = replace(&fx, &mut q, "f", &exp, b"new").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed during commit"));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"OLD");
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn new_inode_between_check_and_exchange_is_undone() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let user = fx.p("f");
        let _g = hooks::once("replace.before_exchange", move || {
            let tmp = user.with_file_name("user-tmp");
            fs::write(&tmp, b"user's new file").unwrap();
            fs::rename(&tmp, &user).unwrap();
        });
        let mut q = quarantine();
        let out = replace(&fx, &mut q, "f", &exp, b"new").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed during commit"));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"user's new file");
        assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());

        // Replaced by a directory: swapped back just the same.
        let exp = fx.user_file("g", b"old");
        let user = fx.p("g");
        let _g = hooks::once("replace.before_exchange", move || {
            fs::remove_file(&user).unwrap();
            fs::create_dir(&user).unwrap();
            fs::write(user.join("child"), b"user").unwrap();
        });
        let out = replace(&fx, &mut q, "g", &exp, b"new").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed during commit"));
        assert_eq!(fs::read(fx.p("g/child")).unwrap(), b"user");
        assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
    }

    #[test]
    fn name_removed_before_exchange_fails_cleanly() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let user = fx.p("f");
        let _g = hooks::once("replace.before_exchange", move || {
            fs::remove_file(&user).unwrap()
        });
        let mut q = quarantine();
        let out = replace(&fx, &mut q, "f", &exp, b"new").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("name removed"));
        assert!(!fx.p("f").exists());
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn write_through_held_fd_after_exchange_becomes_conflict_copy() {
        let fx = Fx::new();
        let exp = fx.user_file("f.txt", b"old");
        let held: Rc<RefCell<Option<fs::File>>> = Rc::default();
        let (user, slot) = (fx.p("f.txt"), held.clone());
        let _g = hooks::once("replace.before_exchange", move || {
            *slot.borrow_mut() = Some(fs::File::options().append(true).open(&user).unwrap());
        });
        let mut q = quarantine();
        applied(replace(&fx, &mut q, "f.txt", &exp, b"new").unwrap());
        // The writer did not notice the replace and keeps writing.
        held.borrow_mut()
            .as_mut()
            .unwrap()
            .write_all(b" late write")
            .unwrap();

        let report = q.sweep();
        assert_eq!(report.removed, 0);
        assert_eq!(report.conflicts.len(), 1);
        let conflict = &report.conflicts[0];
        let name = conflict.as_os_str().to_string_lossy().into_owned();
        assert!(
            name.starts_with("f.sync-conflict-") && name.ends_with("-abcdef0.txt"),
            "{name}"
        );
        assert_eq!(fs::read(fx.p(&name)).unwrap(), b"old late write");
        assert_eq!(fs::read(fx.p("f.txt")).unwrap(), b"new");
        // The writer's fd now writes into the conflict copy.
        held.borrow_mut().as_mut().unwrap().write_all(b"!").unwrap();
        assert_eq!(fs::read(fx.p(&name)).unwrap(), b"old late write!");
        assert!(q.is_empty());
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn quarantine_waits_for_grace_period() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let mut q = Quarantine::new(REPLICA, Duration::from_secs(3600));
        applied(replace(&fx, &mut q, "f", &exp, b"new").unwrap());
        assert_eq!(q.sweep(), SweepReport::default());
        assert_eq!(q.len(), 1);
        assert!(q.next_deadline().unwrap() > Instant::now());
        assert_eq!(fx.leftovers().len(), 1);

        // A foreign object at the quarantine name is left alone.
        let old = fx.leftovers().remove(0);
        fs::remove_file(&old).unwrap();
        fs::write(&old, b"someone else's").unwrap();
        let report = q.sweep();
        assert_eq!(report.dropped, 1);
        assert!(q.is_empty());
        assert_eq!(fs::read(&old).unwrap(), b"someone else's");
    }

    /// Both the old object and ours change around the exchange: the undo
    /// cannot discard ours, so it is kept as a conflict copy.
    #[test]
    fn undo_keeps_both_when_ours_was_modified_too() {
        let fx = Fx::new();
        let exp = fx.user_file("f.txt", b"old");
        let user = fx.p("f.txt");
        let _g1 = hooks::once("replace.before_exchange", move || append(&user, b" edited"));
        let user = fx.p("f.txt");
        let _g2 = hooks::once("replace.after_exchange", move || append(&user, b" + more"));
        let mut q = quarantine();
        let out = replace(&fx, &mut q, "f.txt", &exp, b"new").unwrap();
        let Outcome::Preserved { conflict } = out else {
            panic!("expected Preserved, got {out:?}");
        };
        assert_eq!(fs::read(fx.p("f.txt")).unwrap(), b"old edited");
        let name = conflict.as_os_str().to_str().unwrap();
        assert!(name.ends_with(".txt"));
        assert_eq!(fs::read(fx.p(name)).unwrap(), b"new + more");
        assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
    }

    /// The name is deleted after the exchange and the old object was
    /// modified: the user's object goes back to its name.
    #[test]
    fn undo_restores_when_name_was_removed() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let user = fx.p("f");
        let _g1 = hooks::once("replace.before_exchange", move || append(&user, b" edited"));
        let user = fx.p("f");
        let _g2 = hooks::once("replace.after_exchange", move || {
            fs::remove_file(&user).unwrap()
        });
        let mut q = quarantine();
        let out = replace(&fx, &mut q, "f", &exp, b"new").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed during commit"));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"old edited");
        assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
    }

    #[test]
    fn replace_symlink_cases() {
        let fx = Fx::new();
        symlink("old-target", fx.p("l")).unwrap();
        let exp = Expected::from(fx.root.stat(&rp("l")).unwrap());
        let mut q = quarantine();

        // Normal.
        let fp =
            applied(replace_symlink(&fx.ctx(), &mut q, &rp("l"), &exp, b"new-target").unwrap());
        assert_eq!(fp.kind, FileKind::Symlink);
        assert_eq!(fs::read_link(fx.p("l")).unwrap(), Path::new("new-target"));
        assert_eq!(q.sweep().removed, 1);

        // Stale expectation.
        let out = replace_symlink(&fx.ctx(), &mut q, &rp("l"), &exp, b"x").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed since scan"));
        assert_eq!(fs::read_link(fx.p("l")).unwrap(), Path::new("new-target"));

        // Replaced by the user between check and exchange.
        let exp = Expected::from(fp);
        let user = fx.p("l");
        let _g = hooks::once("replace.before_exchange", move || {
            fs::remove_file(&user).unwrap();
            symlink("user-target", &user).unwrap();
        });
        let out = replace_symlink(&fx.ctx(), &mut q, &rp("l"), &exp, b"ours").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed during commit"));
        assert_eq!(fs::read_link(fx.p("l")).unwrap(), Path::new("user-target"));

        // A file can replace a symlink, and the other way round.
        let exp = Expected::from(fx.root.stat(&rp("l")).unwrap());
        applied(replace(&fx, &mut q, "l", &exp, b"now a file").unwrap());
        assert_eq!(fs::read(fx.p("l")).unwrap(), b"now a file");
        let exp = Expected::from(fx.root.stat(&rp("l")).unwrap());
        applied(replace_symlink(&fx.ctx(), &mut q, &rp("l"), &exp, b"t").unwrap());
        assert_eq!(fs::read_link(fx.p("l")).unwrap(), Path::new("t"));
        q.sweep();
        assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
    }

    #[test]
    fn replace_refuses_directories() {
        let fx = Fx::new();
        fs::create_dir(fx.p("d")).unwrap();
        let exp = Expected::from(fx.root.stat(&rp("d")).unwrap());
        let mut q = quarantine();
        let out = replace(&fx, &mut q, "d", &exp, b"x").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("not a file or symlink"));
        assert!(fx.p("d").is_dir());
    }

    // ----- T06: delete and rmdir ---------------------------------------------

    fn del(fx: &Fx, q: &mut Quarantine, rel: &str, exp: &Expected) -> Result<Outcome> {
        delete(&fx.ctx(), q, &rp(rel), exp)
    }

    #[test]
    fn delete_normal() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        symlink("target", fx.p("l")).unwrap();
        let lexp = Expected::from(fx.root.stat(&rp("l")).unwrap());
        let mut q = quarantine();
        hooks::start_trace();
        assert_eq!(del(&fx, &mut q, "f", &exp).unwrap(), Outcome::Removed);
        assert_eq!(
            hooks::take_trace(),
            [
                "commit.resolved",
                "delete.before_pin",
                "delete.pinned",
                "journal.started",
                "delete.before_rename",
                "delete.after_rename",
                "delete.before_rehash",
                "delete.verified",
                "delete.quarantined",
                "commit.synced",
                "commit.verified",
            ]
        );
        assert_eq!(del(&fx, &mut q, "l", &lexp).unwrap(), Outcome::Removed);
        assert!(fs::symlink_metadata(fx.p("f")).is_err());
        assert!(fs::symlink_metadata(fx.p("l")).is_err());
        // The deleted inodes wait in quarantine until swept.
        assert_eq!(q.len(), 2);
        assert_eq!(fx.leftovers().len(), 2);
        assert_eq!(q.sweep().removed, 2);
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn delete_with_stale_expectation_fails() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        append(&fx.p("f"), b" edited");
        let mut q = quarantine();
        let out = del(&fx, &mut q, "f", &exp).unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed since scan"));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"old edited");

        fs::remove_file(fx.p("f")).unwrap();
        let out = del(&fx, &mut q, "f", &exp).unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("name missing"));

        // Directories are rmdir's job.
        fs::create_dir(fx.p("d")).unwrap();
        let exp = Expected::from(fx.root.stat(&rp("d")).unwrap());
        let out = del(&fx, &mut q, "d", &exp).unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("not a file or symlink"));
        assert!(fx.p("d").is_dir());
        assert!(q.is_empty());
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn delete_modification_before_rename_is_restored() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let user = fx.p("f");
        let _g = hooks::once("delete.before_rename", move || append(&user, b" edited"));
        let mut q = quarantine();
        let out = del(&fx, &mut q, "f", &exp).unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed during commit"));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"old edited");
        assert_eq!(fs::symlink_metadata(fx.p("f")).unwrap().ino(), exp.fp.ino);
        assert!(q.is_empty());
        assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
    }

    /// Same size, mtime restored: only the rehash catches it.
    #[test]
    fn delete_mtime_preserving_write_is_restored() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let user = fx.p("f");
        let _g = hooks::once("delete.before_rename", move || {
            let f = fs::File::options().write(true).open(&user).unwrap();
            (&f).write_all(b"OLD").unwrap();
            f.set_modified(SystemTime::UNIX_EPOCH + OLD_MTIME).unwrap();
        });
        let mut q = quarantine();
        let out = del(&fx, &mut q, "f", &exp).unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed during commit"));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"OLD");
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn delete_new_inode_before_rename_is_restored() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let user = fx.p("f");
        let _g = hooks::once("delete.before_rename", move || {
            fs::remove_file(&user).unwrap();
            fs::create_dir(&user).unwrap();
            fs::write(user.join("child"), b"user").unwrap();
        });
        let mut q = quarantine();
        let out = del(&fx, &mut q, "f", &exp).unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed during commit"));
        assert_eq!(fs::read(fx.p("f/child")).unwrap(), b"user");
        assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
    }

    /// The modified file cannot go back because the user re-created the name
    /// after our rename: it is kept under a conflict name, and both survive.
    #[test]
    fn delete_restore_onto_recreated_name_keeps_conflict_copy() {
        let fx = Fx::new();
        let exp = fx.user_file("f.txt", b"old");
        let user = fx.p("f.txt");
        let _g1 = hooks::once("delete.before_rename", move || append(&user, b" edited"));
        let user = fx.p("f.txt");
        let _g2 = hooks::once("delete.after_rename", move || {
            fs::write(&user, b"re-created").unwrap()
        });
        let mut q = quarantine();
        let out = del(&fx, &mut q, "f.txt", &exp).unwrap();
        let Outcome::Preserved { conflict } = out else {
            panic!("expected Preserved, got {out:?}");
        };
        let name = conflict.as_os_str().to_str().unwrap();
        assert!(
            name.starts_with("f.sync-conflict-") && name.ends_with("-abcdef0.txt"),
            "{name}"
        );
        assert_eq!(fs::read(fx.p(name)).unwrap(), b"old edited");
        assert_eq!(fs::read(fx.p("f.txt")).unwrap(), b"re-created");
        assert!(q.is_empty());
        assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
    }

    /// The delete itself was valid, but the name is taken again right after:
    /// reported as unstable so the path is rescanned.
    #[test]
    fn delete_then_recreated_name_is_unstable() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let user = fx.p("f");
        let _g = hooks::once("delete.quarantined", move || {
            fs::write(&user, b"re-created").unwrap()
        });
        let mut q = quarantine();
        let res = del(&fx, &mut q, "f", &exp);
        assert!(matches!(res, Err(Error::Unstable { .. })), "{res:?}");
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"re-created");
        assert_eq!(q.sweep().removed, 1);
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn delete_write_through_held_fd_becomes_conflict_copy() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let mut held = fs::File::options().append(true).open(fx.p("f")).unwrap();
        let mut q = quarantine();
        assert_eq!(del(&fx, &mut q, "f", &exp).unwrap(), Outcome::Removed);
        held.write_all(b" late write").unwrap();
        let report = q.sweep();
        assert_eq!((report.removed, report.conflicts.len()), (0, 1));
        let name = report.conflicts[0].as_os_str().to_str().unwrap().to_owned();
        assert!(name.starts_with("f.sync-conflict-"), "{name}");
        assert_eq!(fs::read(fx.p(&name)).unwrap(), b"old late write");
        assert!(fs::symlink_metadata(fx.p("f")).is_err());
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn rmdir_cases() {
        let fx = Fx::new();
        let mut q = quarantine();
        fs::create_dir_all(fx.p("d/e")).unwrap();
        fs::write(fx.p("f"), b"user").unwrap();
        symlink("d", fx.p("l")).unwrap();

        // Normal, with the exact trace.
        hooks::start_trace();
        assert_eq!(
            rmdir(&fx.ctx(), &mut q, &rp("d/e")).unwrap(),
            Outcome::Removed
        );
        assert_eq!(
            hooks::take_trace(),
            [
                "commit.resolved",
                "rmdir.checked",
                "rmdir.before_rmdir",
                "rmdir.after_rmdir",
                "commit.synced",
                "commit.verified",
            ]
        );
        assert!(!fx.p("d/e").exists());

        // Not a directory (a symlink to one is not followed), missing, not empty.
        for (p, why) in [
            ("f", "not a directory"),
            ("l", "not a directory"),
            ("nope", "name missing"),
        ] {
            let out = rmdir(&fx.ctx(), &mut q, &rp(p)).unwrap();
            assert_eq!(out, Outcome::PreconditionFailed(why), "{p}");
        }
        assert!(fx.p("d").is_dir() && fx.p("f").is_file());
        fs::write(fx.p("d/child"), b"x").unwrap();
        let out = rmdir(&fx.ctx(), &mut q, &rp("d")).unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("directory not empty"));
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn rmdir_with_child_created_just_before_fails() {
        let fx = Fx::new();
        fs::create_dir(fx.p("d")).unwrap();
        let user = fx.p("d/child");
        let _g = hooks::once("rmdir.before_rmdir", move || {
            fs::write(&user, b"user").unwrap()
        });
        let mut q = quarantine();
        let out = rmdir(&fx.ctx(), &mut q, &rp("d")).unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("directory not empty"));
        assert_eq!(fs::read(fx.p("d/child")).unwrap(), b"user");

        // Replaced by a file just before: rmdir never removes it.
        fs::create_dir(fx.p("e")).unwrap();
        let user = fx.p("e");
        let _g = hooks::once("rmdir.before_rmdir", move || {
            fs::remove_dir(&user).unwrap();
            fs::write(&user, b"user").unwrap();
        });
        let out = rmdir(&fx.ctx(), &mut q, &rp("e")).unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("not a directory"));
        assert_eq!(fs::read(fx.p("e")).unwrap(), b"user");
    }

    /// Children deleted just before are still in quarantine (a long grace
    /// period here); rmdir settles them first.
    #[test]
    fn rmdir_settles_quarantined_children() {
        let fx = Fx::new();
        fs::create_dir_all(fx.p("d/sub")).unwrap();
        fs::create_dir(fx.p("other")).unwrap();
        let a = fx.user_file("d/a", b"a");
        let b = fx.user_file("d/b.txt", b"b");
        let o = fx.user_file("other/o", b"o");
        let mut q = Quarantine::new(REPLICA, Duration::from_secs(3600));
        let mut held = fs::File::options()
            .append(true)
            .open(fx.p("d/b.txt"))
            .unwrap();
        for (p, exp) in [("d/a", &a), ("d/b.txt", &b), ("other/o", &o)] {
            assert_eq!(del(&fx, &mut q, p, exp).unwrap(), Outcome::Removed);
        }
        assert_eq!(
            rmdir(&fx.ctx(), &mut q, &rp("d/sub")).unwrap(),
            Outcome::Removed
        );

        // A write through a held fd after the delete: the settled child
        // becomes a conflict copy, which keeps the directory alive.
        held.write_all(b" late").unwrap();
        let out = rmdir(&fx.ctx(), &mut q, &rp("d")).unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("directory not empty"));
        let names: Vec<_> = fs::read_dir(fx.p("d"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names.len(), 1, "{names:?}");
        assert!(names[0].starts_with("b.sync-conflict-"), "{names:?}");
        assert_eq!(fs::read(fx.p("d").join(&names[0])).unwrap(), b"b late");
        // Entries in other directories keep waiting.
        assert_eq!(q.len(), 1);

        // Unchanged children are unlinked at once, and the rmdir succeeds.
        fs::remove_file(fx.p("d").join(&names[0])).unwrap();
        let c = fx.user_file("d/c", b"c");
        assert_eq!(del(&fx, &mut q, "d/c", &c).unwrap(), Outcome::Removed);
        assert_eq!(
            rmdir(&fx.ctx(), &mut q, &rp("d")).unwrap(),
            Outcome::Removed
        );
        assert!(!fx.p("d").exists());
        assert_eq!(q.len(), 1);
        assert_eq!(fx.leftovers().len(), 1);
    }

    // ----- T11: rename to a conflict name, directory mode -------------------

    fn ren(fx: &Fx, rel: &str, exp: &Expected, to: &str) -> Result<Outcome> {
        rename_to(&fx.ctx(), &rp(rel), exp, &rp(to))
    }

    #[test]
    fn rename_to_normal() {
        let fx = Fx::new();
        let exp = fx.user_file("f.txt", b"loser");
        hooks::start_trace();
        let fp = applied(ren(&fx, "f.txt", &exp, "f.c.txt").unwrap());
        assert_eq!(
            hooks::take_trace(),
            [
                "commit.resolved",
                "rename.before_pin",
                "rename.pinned",
                "journal.started",
                "rename.before_rename",
                "rename.after_rename",
                "rename.before_rehash",
                "rename.verified",
                "rename.moved",
                "commit.synced",
                "commit.verified",
            ]
        );
        assert_eq!(fp.ino, exp.fp.ino);
        assert_eq!(fs::read(fx.p("f.c.txt")).unwrap(), b"loser");
        assert!(!fx.p("f.txt").exists());

        symlink("t", fx.p("l")).unwrap();
        let lexp = Expected::from(fx.root.stat(&rp("l")).unwrap());
        applied(ren(&fx, "l", &lexp, "l.c").unwrap());
        assert_eq!(fs::read_link(fx.p("l.c")).unwrap(), Path::new("t"));
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn rename_to_refusals() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"data");
        fs::create_dir(fx.p("d")).unwrap();
        for to in ["d/f", "f", ".~fsync.x"] {
            let out = ren(&fx, "f", &exp, to);
            assert!(
                matches!(out, Err(Error::InvalidPath { .. })),
                "{to}: {out:?}"
            );
        }
        fs::write(fx.p("taken"), b"user").unwrap();
        let out = ren(&fx, "f", &exp, "taken").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("new name exists"));
        append(&fx.p("f"), b" edited");
        let out = ren(&fx, "f", &exp, "c").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed since scan"));
        let dexp = Expected::from(fx.root.stat(&rp("d")).unwrap());
        let out = ren(&fx, "d", &dexp, "c").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("not a file or symlink"));
        assert_eq!(fs::read(fx.p("taken")).unwrap(), b"user");
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"data edited");
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn rename_to_modification_before_move_is_restored() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let user = fx.p("f");
        let _g = hooks::once("rename.before_rename", move || append(&user, b" edited"));
        let out = ren(&fx, "f", &exp, "c").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed during commit"));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"old edited");
        assert_eq!(fs::symlink_metadata(fx.p("f")).unwrap().ino(), exp.fp.ino);
        assert!(!fx.p("c").exists());
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn rename_to_name_taken_after_verify_is_restored() {
        let fx = Fx::new();
        let exp = fx.user_file("f", b"old");
        let taken = fx.p("c");
        let _g = hooks::once("rename.verified", move || {
            fs::write(&taken, b"user c").unwrap()
        });
        let out = ren(&fx, "f", &exp, "c").unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("new name exists"));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"old");
        assert_eq!(fs::read(fx.p("c")).unwrap(), b"user c");
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn set_dir_mode_cases() {
        let fx = Fx::new();
        fs::create_dir(fx.p("d")).unwrap();
        fs::set_permissions(fx.p("d"), fs::Permissions::from_mode(0o755)).unwrap();
        let exp = fx.root.stat(&rp("d")).unwrap();
        hooks::start_trace();
        let fp = applied(set_dir_mode(&fx.ctx(), &rp("d"), &exp, 0o6700).unwrap());
        assert_eq!(
            hooks::take_trace(),
            [
                "commit.resolved",
                "chmod.before_pin",
                "chmod.pinned",
                "chmod.after_chmod",
                "commit.synced",
                "commit.verified",
            ]
        );
        let md = fs::symlink_metadata(fx.p("d")).unwrap();
        assert_eq!(md.permissions().mode() & 0o7777, 0o700, "setgid stripped");
        assert_eq!((fp.ino, fp.mode), (exp.ino, 0o700));

        // The expected mode is stale now.
        let out = set_dir_mode(&fx.ctx(), &rp("d"), &exp, 0o750).unwrap();
        assert_eq!(out, Outcome::PreconditionFailed("changed since scan"));
        // Missing, a file, a symlink to a directory.
        fs::write(fx.p("f"), b"x").unwrap();
        symlink("d", fx.p("l")).unwrap();
        for (name, why) in [
            ("x", "name missing"),
            ("f", "not a directory"),
            ("l", "not a directory"),
        ] {
            let out = set_dir_mode(&fx.ctx(), &rp(name), &fp, 0o750).unwrap();
            assert_eq!(out, Outcome::PreconditionFailed(why), "{name}");
        }
        assert_eq!(
            fs::symlink_metadata(fx.p("d"))
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o700
        );
    }

    /// The directory is swapped for another after the pin: only the pinned
    /// (verified) one is changed.
    #[test]
    fn set_dir_mode_changes_only_the_pinned_directory() {
        let fx = Fx::new();
        fs::create_dir(fx.p("d")).unwrap();
        fs::set_permissions(fx.p("d"), fs::Permissions::from_mode(0o755)).unwrap();
        let exp = fx.root.stat(&rp("d")).unwrap();
        let (d, moved) = (fx.p("d"), fx.p("moved"));
        let _g = hooks::once("chmod.pinned", move || {
            fs::rename(&d, &moved).unwrap();
            fs::create_dir(&d).unwrap();
            fs::set_permissions(&d, fs::Permissions::from_mode(0o755)).unwrap();
        });
        let out = set_dir_mode(&fx.ctx(), &rp("d"), &exp, 0o700);
        assert!(matches!(out, Err(Error::Unstable { .. })), "{out:?}");
        let mode = |p: &str| fs::symlink_metadata(fx.p(p)).unwrap().permissions().mode() & 0o7777;
        assert_eq!((mode("moved"), mode("d")), (0o700, 0o755));
    }

    // ----- T15: journal and recovery ------------------------------------------

    /// The single intent the last commit recorded.
    fn last_intent(fx: &Fx) -> Intent {
        let open = fx.journal.take_open();
        assert_eq!(open.len(), 1, "{open:?}");
        let all = fx.journal.pending().unwrap();
        all.into_iter().find(|(id, _)| *id == open[0]).unwrap().1
    }

    #[test]
    fn commits_record_their_names_before_creating_them() {
        for fx in [Fx::new(), Fx::named()] {
            // Every reserved name in the directory, at every step, is one
            // the journal already holds.
            let checked = Rc::new(RefCell::new(0));
            let mut guards = Vec::new();
            for p in [
                "stage.created",
                "stage.written",
                "stage.synced",
                "stage.linked",
                "replace.after_exchange",
                "replace.verified",
                "replace.quarantined",
                "delete.after_rename",
                "delete.quarantined",
                "rename.after_rename",
                "create.before_rename",
            ] {
                let (dir, j, n) = (
                    fx.dir.path().to_path_buf(),
                    fx.journal.clone(),
                    checked.clone(),
                );
                guards.push(hooks::on(p, move || {
                    let pending = j.pending().unwrap();
                    for e in fs::read_dir(&dir).unwrap() {
                        let name = e.unwrap().file_name();
                        let name = name.as_encoded_bytes();
                        if tmpname::is_reserved(name) {
                            let known = pending
                                .iter()
                                .any(|(_, i)| i.tmp == name || i.old.as_deref() == Some(name));
                            assert!(known, "{p}: {} not journaled", name.escape_ascii());
                            *n.borrow_mut() += 1;
                        }
                    }
                }));
            }
            let exp = fx.user_file("f", b"old");
            let mut q = Quarantine::new(REPLICA, Duration::from_secs(3600));
            let fp = applied(replace(&fx, &mut q, "f", &exp, b"new").unwrap());
            let i = last_intent(&fx);
            assert_eq!(
                (i.op, i.state, i.path.clone()),
                (IntentOp::Replace, IntentState::Exchanged, rp("f"))
            );
            assert_eq!(i.expected, Some(exp));
            assert!(i.staged.is_some_and(|n| same_object(&n, &fp)));
            assert!(q.holds(1));

            applied(create(&fx, "g", b"g").unwrap());
            let i = last_intent(&fx);
            assert_eq!(i.op, IntentOp::Create);
            // A named temp file is recorded before it exists; an O_TMPFILE
            // with its inode, just before it is linked in.
            let want = if fx.caps.tmpfile_usable() {
                IntentState::TempWritten
            } else {
                IntentState::Started
            };
            assert_eq!(i.state, want);

            let exp = fx.user_file("h", b"h");
            assert_eq!(del(&fx, &mut q, "h", &exp).unwrap(), Outcome::Removed);
            let i = last_intent(&fx);
            assert_eq!((i.op, i.state), (IntentOp::Delete, IntentState::Exchanged));
            assert_eq!(tmpname::parse(&i.tmp).map(|(k, _)| k), Some(TmpKind::Del));

            let exp = fx.user_file("r", b"r");
            applied(ren(&fx, "r", &exp, "r2").unwrap());
            assert_eq!(last_intent(&fx).op, IntentOp::Rename);
            applied(mkdir(&fx.ctx(), &rp("d"), 0o755).unwrap());
            applied(create_symlink(&fx.ctx(), &rp("l"), b"t").unwrap());
            assert_eq!(fx.journal.take_open().len(), 2);
            drop(guards);
            assert!(*checked.borrow() > 0);
        }
    }

    const TMP: TmpId = TmpId(0x1111);

    /// An intent for `path` as a crash would leave it, with fixed names.
    fn crashed(
        fx: &Fx,
        op: IntentOp,
        path: &str,
        expected: Option<Expected>,
        staged: Option<Fingerprint>,
    ) -> Intent {
        let path = rp(path);
        Intent {
            op,
            parent: fx.root.stat(&path.parent().unwrap()).unwrap(),
            path,
            tmp: tmpname::name(
                if op == IntentOp::Delete {
                    TmpKind::Del
                } else {
                    TmpKind::Temp
                },
                TMP,
            ),
            old: matches!(op, IntentOp::Replace | IntentOp::Delete).then(|| tmpname::old(TMP)),
            expected,
            staged,
            state: IntentState::Exchanged,
        }
    }

    fn tmp_path(fx: &Fx, i: &Intent) -> PathBuf {
        fx.dir.path().join(std::ffi::OsStr::from_bytes(&i.tmp))
    }

    fn recov(fx: &Fx, q: &mut Quarantine, i: &Intent) -> Recovered {
        recover(&fx.ctx(), q, 7, i).unwrap()
    }

    use std::os::unix::ffi::OsStrExt;

    #[test]
    fn recover_removes_staged_objects() {
        let fx = Fx::new();
        let mut q = quarantine();
        // Started: whatever is at the staging name is ours (a partial file,
        // a symlink, an empty directory).
        let mut i = crashed(&fx, IntentOp::Create, "f", None, None);
        i.state = IntentState::Started;
        for make in [
            &(|p: &Path| fs::write(p, b"partial").unwrap()) as &dyn Fn(&Path),
            &|p: &Path| symlink("t", p).unwrap(),
            &|p: &Path| fs::create_dir(p).unwrap(),
        ] {
            make(&tmp_path(&fx, &i));
            assert_eq!(recov(&fx, &mut q, &i).removed, 1);
            assert!(fx.leftovers().is_empty());
        }
        // TempWritten: N is removed; anything else is foreign and stays.
        fs::write(tmp_path(&fx, &i), b"staged").unwrap();
        let n = Fingerprint::at(fx.root.fd(), &i.tmp).unwrap();
        i.staged = Some(n);
        i.state = IntentState::TempWritten;
        assert_eq!(recov(&fx, &mut q, &i).removed, 1);
        fs::write(tmp_path(&fx, &i), b"someone's").unwrap();
        let rep = recov(&fx, &mut q, &i);
        assert_eq!((rep.removed, rep.foreign), (0, 1));
        assert_eq!(fs::read(tmp_path(&fx, &i)).unwrap(), b"someone's");
        fs::remove_file(tmp_path(&fx, &i)).unwrap();
        // Finished: nothing to do.
        assert_eq!(recov(&fx, &mut q, &i), Recovered::default());

        // A replace that never got to the exchange.
        let exp = fx.user_file("g", b"user");
        let mut i = crashed(&fx, IntentOp::Replace, "g", Some(exp), None);
        i.state = IntentState::Started;
        fs::write(tmp_path(&fx, &i), b"part").unwrap();
        assert_eq!(recov(&fx, &mut q, &i).removed, 1);
        assert_eq!(fs::read(fx.p("g")).unwrap(), b"user");
        assert!(fx.leftovers().is_empty() && q.is_empty());
    }

    /// Sets up a replace that crashed right after the exchange: N at `name`,
    /// the user's old file at the staging name.
    fn exchanged(fx: &Fx, name: &str) -> Intent {
        let exp = fx.user_file(name, b"old");
        fs::write(fx.p("n"), b"new").unwrap();
        let i = crashed(fx, IntentOp::Replace, name, Some(exp), None);
        fs::rename(fx.p(name), tmp_path(fx, &i)).unwrap();
        fs::rename(fx.p("n"), fx.p(name)).unwrap();
        let n = fx.root.stat(&rp(name)).unwrap();
        Intent {
            staged: Some(n),
            ..i
        }
    }

    #[test]
    fn recover_after_exchange_quarantines_unchanged_old_file() {
        let fx = Fx::new();
        let mut q = Quarantine::new(REPLICA, Duration::from_secs(3600));
        let i = exchanged(&fx, "f");
        let rep = recov(&fx, &mut q, &i);
        assert_eq!((rep.quarantined, rep.removed), (1, 0));
        assert!(q.holds(7));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"new");
        let old = fx
            .dir
            .path()
            .join(std::ffi::OsStr::from_bytes(i.old.as_ref().unwrap()));
        assert_eq!(fs::read(&old).unwrap(), b"old");
        // Replaying again (another crash) changes nothing.
        assert_eq!(recov(&fx, &mut q, &i), Recovered::default());
        q.set_grace(Duration::ZERO);
        let report = q.sweep();
        assert_eq!((report.removed, report.finished.clone()), (1, vec![7]));
        assert!(fx.leftovers().is_empty());

        // A quarantined old inode (state Quarantined) after a restart: a
        // fresh entry, and a write since becomes a conflict copy.
        let i = exchanged(&fx, "g.txt");
        fs::rename(
            tmp_path(&fx, &i),
            fx.dir
                .path()
                .join(std::ffi::OsStr::from_bytes(i.old.as_ref().unwrap())),
        )
        .unwrap();
        let i = Intent {
            state: IntentState::Quarantined,
            ..i
        };
        let mut q = Quarantine::new(REPLICA, Duration::from_secs(3600));
        assert_eq!(recov(&fx, &mut q, &i).quarantined, 1);
        append(
            &fx.dir
                .path()
                .join(std::ffi::OsStr::from_bytes(i.old.as_ref().unwrap())),
            b" late",
        );
        let report = q.sweep();
        assert_eq!(report.conflicts.len(), 1);
        let name = report.conflicts[0].as_os_str().to_str().unwrap().to_owned();
        assert!(name.starts_with("g.sync-conflict-"), "{name}");
        assert_eq!(fs::read(fx.p(&name)).unwrap(), b"old late");
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn recover_after_exchange_undoes_a_modified_old_file() {
        let fx = Fx::new();
        let mut q = quarantine();
        // Modified after the check: exchanged back, N removed.
        let i = exchanged(&fx, "f");
        append(&tmp_path(&fx, &i), b" edited");
        let rep = recov(&fx, &mut q, &i);
        assert_eq!((rep.restored, rep.removed, rep.quarantined), (1, 1, 0));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"old edited");
        assert!(fx.leftovers().is_empty() && q.is_empty());

        // The same, but the user replaced N since: both are kept.
        let i = exchanged(&fx, "g.txt");
        append(&tmp_path(&fx, &i), b" edited");
        fs::remove_file(fx.p("g.txt")).unwrap();
        fs::write(fx.p("g.txt"), b"user's newer").unwrap();
        let rep = recov(&fx, &mut q, &i);
        assert_eq!(rep.conflicts.len(), 1);
        assert_eq!(fs::read(fx.p("g.txt")).unwrap(), b"user's newer");
        assert_eq!(
            fs::read(fx.p(rep.conflicts[0].as_os_str().to_str().unwrap())).unwrap(),
            b"old edited"
        );

        // Another object swapped in before the exchange: it is user data.
        let i = exchanged(&fx, "h");
        fs::remove_file(tmp_path(&fx, &i)).unwrap();
        fs::write(tmp_path(&fx, &i), b"swapped in").unwrap();
        assert_eq!(recov(&fx, &mut q, &i).restored, 1);
        assert_eq!(fs::read(fx.p("h")).unwrap(), b"swapped in");
        assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
    }

    #[test]
    fn recover_moved_aside_objects() {
        let fx = Fx::new();
        let mut q = Quarantine::new(REPLICA, Duration::from_secs(3600));
        // A delete: unchanged → quarantined, as the delete would have.
        let exp = fx.user_file("f", b"old");
        let i = crashed(&fx, IntentOp::Delete, "f", Some(exp), None);
        fs::rename(fx.p("f"), tmp_path(&fx, &i)).unwrap();
        assert_eq!(recov(&fx, &mut q, &i).quarantined, 1);
        assert!(!fx.p("f").exists());
        q.set_grace(Duration::ZERO);
        assert_eq!(q.sweep().removed, 1);

        // Modified → back at its name; the name taken → a conflict copy.
        let exp = fx.user_file("g.txt", b"old");
        let i = crashed(&fx, IntentOp::Delete, "g.txt", Some(exp), None);
        fs::rename(fx.p("g.txt"), tmp_path(&fx, &i)).unwrap();
        append(&tmp_path(&fx, &i), b" edited");
        assert_eq!(recov(&fx, &mut q, &i).restored, 1);
        assert_eq!(fs::read(fx.p("g.txt")).unwrap(), b"old edited");
        let exp = Expected::from(fx.root.stat(&rp("g.txt")).unwrap());
        let i = crashed(&fx, IntentOp::Delete, "g.txt", Some(exp), None);
        fs::rename(fx.p("g.txt"), tmp_path(&fx, &i)).unwrap();
        append(&tmp_path(&fx, &i), b" again");
        fs::write(fx.p("g.txt"), b"re-created").unwrap();
        let rep = recov(&fx, &mut q, &i);
        assert_eq!(rep.conflicts.len(), 1);
        assert_eq!(fs::read(fx.p("g.txt")).unwrap(), b"re-created");

        // A rename always goes back.
        let exp = fx.user_file("r", b"loser");
        let i = crashed(&fx, IntentOp::Rename, "r", Some(exp), None);
        fs::rename(fx.p("r"), tmp_path(&fx, &i)).unwrap();
        assert_eq!(recov(&fx, &mut q, &i).restored, 1);
        assert_eq!(fs::read(fx.p("r")).unwrap(), b"loser");
        assert!(fx.leftovers().is_empty(), "{:?}", fx.leftovers());
    }

    #[test]
    fn recover_skips_a_moved_directory_and_foreign_quarantine_names() {
        let fx = Fx::new();
        let mut q = quarantine();
        fs::create_dir(fx.p("d")).unwrap();
        let exp = fx.user_file("d/f", b"old");
        let i = crashed(&fx, IntentOp::Delete, "d/f", Some(exp), None);
        let del = fx.p("d").join(std::ffi::OsStr::from_bytes(&i.tmp));
        fs::rename(fx.p("d/f"), &del).unwrap();
        // Moved away, and replaced by another directory.
        fs::rename(fx.p("d"), fx.p("moved")).unwrap();
        fs::create_dir(fx.p("d")).unwrap();
        assert_eq!(recov(&fx, &mut q, &i).skipped, Some("directory replaced"));
        fs::remove_dir(fx.p("d")).unwrap();
        assert_eq!(
            recov(&fx, &mut q, &i).skipped,
            Some("directory moved or removed")
        );
        assert_eq!(fx.leftovers().len(), 1, "left alone");

        // Something else at the quarantine name is not ours.
        let exp = fx.user_file("g", b"old");
        let i = crashed(&fx, IntentOp::Replace, "g", Some(exp), None);
        let old = fx
            .dir
            .path()
            .join(std::ffi::OsStr::from_bytes(i.old.as_ref().unwrap()));
        fs::write(&old, b"foreign").unwrap();
        assert_eq!(recov(&fx, &mut q, &i).foreign, 1);
        assert_eq!(fs::read(&old).unwrap(), b"foreign");
        assert_eq!(fs::read(fx.p("g")).unwrap(), b"old");
    }
}
