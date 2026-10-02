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
//!   [`Quarantine`] rather than being unlinked at once.
//!
//! After the rename, the parent is fsynced, the name must hold the inode we
//! staged, and the parent must still resolve to the same directory (§5.3
//! step 5). Nothing is ever written in place, and nothing is unlinked unless
//! it is verified to be ours or a verified, quarantined old inode.
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

/// Everything a commit needs to know about the replica it modifies.
#[derive(Clone, Copy, Debug)]
pub struct Ctx<'a> {
    pub root: &'a Root,
    pub caps: &'a Caps,
    /// Used in conflict-copy names (§6.2).
    pub replica: ReplicaId,
}

/// Metadata set on a new file before it is committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileMeta {
    /// Permission bits; setuid and setgid are stripped.
    pub mode: u32,
    pub mtime_ns: i64,
}

/// The state a replace expects to find at the target name (§5.3 step 4(a)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
}

/// Files below this size are always rehashed when verifying a replace
/// (§5.3 step 4(d)).
pub const REHASH_BELOW: u64 = 1 << 20;

/// Permission bits we set: setuid and setgid are stripped (§3).
const MODE_MASK: u32 = 0o1777;
/// Attempts at finding a free random reserved name, or a free conflict name.
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
    let staged = match stage_file(&t, ctx.caps, content, meta, expected_hash)? {
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
    let staged = stage_symlink(&t, target)?;
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
    let staged = stage_dir(&t, mode)?;
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
    let staged = match stage_file(&t, ctx.caps, content, meta, expected_hash)? {
        Ok(staged) => staged,
        Err(reason) => return Ok(Outcome::PreconditionFailed(reason)),
    };
    commit_replace(ctx, quarantine, &t, staged, expected)
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
    let staged = stage_symlink(&t, target)?;
    commit_replace(ctx, quarantine, &t, staged, expected)
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
        let (parent, name) = root.resolve_parent(path)?;
        if tmpname::is_reserved(name) {
            return Err(Error::InvalidPath {
                path: path.as_bytes().to_vec(),
                reason: "reserved `.~fsync.` name",
            });
        }
        let parent_fp = Fingerprint::of_fd(parent.as_fd())?;
        point("commit.resolved");
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

        let now = match self.stat(self.name)? {
            Some(now) if same_object(staged, &now) => now,
            _ => return Err(self.unstable("name replaced right after commit")),
        };
        let parent_now = self
            .root
            .resolve_parent(self.path)
            .and_then(|(fd, _)| Fingerprint::of_fd(fd.as_fd()));
        match parent_now {
            Ok(p) if p.same_file(&self.parent_fp) => {}
            _ => return Err(self.unstable("parent directory moved during commit")),
        }
        point("commit.verified");
        Ok(Outcome::Applied(now))
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
// Staging (§5.3 step 2)
// ---------------------------------------------------------------------------

/// Calls `f` with fresh random reserved names of `kind` until one is free.
fn with_fresh_name<T>(
    kind: TmpKind,
    mut f: impl FnMut(&[u8]) -> rustix::io::Result<T>,
) -> std::result::Result<(T, Vec<u8>), Errno> {
    for _ in 0..NAME_RETRIES {
        let id = TmpId::random().map_err(|_| Errno::IO)?;
        let name = tmpname::name(kind, id);
        match f(&name) {
            Ok(v) => return Ok((v, name)),
            Err(Errno::EXIST) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(Errno::EXIST)
}

/// A new file being written in a replica directory, not yet visible.
///
/// It is an unnamed `O_TMPFILE` inode when [`Caps::tmpfile_usable`], else an
/// `O_CREAT|O_EXCL` file under a reserved temp name. Content is hashed as it
/// is written. Dropping it before [`TempFile::finish`] removes it.
pub struct TempFile<'p> {
    parent: BorrowedFd<'p>,
    /// Always `Some` until `finish` moves it out.
    fd: Option<OwnedFd>,
    /// The temp name of a named temp file; `None` for `O_TMPFILE` (until
    /// linked) and after `finish`.
    named: Option<Vec<u8>>,
    link_empty_path: bool,
    hasher: blake3::Hasher,
    size: u64,
}

impl<'p> TempFile<'p> {
    pub fn create(parent: BorrowedFd<'p>, caps: &Caps) -> Result<TempFile<'p>> {
        let mode = Mode::from_raw_mode(0o600);
        let (fd, named) = if caps.tmpfile_usable() {
            let fd = rustix::fs::openat2(
                parent,
                ".",
                OFlags::TMPFILE | OFlags::WRONLY | OFlags::CLOEXEC,
                mode,
                RESOLVE,
            )
            .map_err(|e| sys_err("create O_TMPFILE", e))?;
            (fd, None)
        } else {
            let flags =
                OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let (fd, name) = with_fresh_name(TmpKind::Temp, |name| {
                rustix::fs::openat2(parent, name, flags, mode, RESOLVE)
            })
            .map_err(|e| sys_err("create temp file", e))?;
            (fd, Some(name))
        };
        point("stage.created");
        Ok(TempFile {
            parent,
            fd: Some(fd),
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
                Err(e) => return Err(Error::io("read content to commit", e)),
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

    /// Sets mode and mtime, fsyncs, and links the file in under a reserved
    /// temp name. The returned [`Staged`] keeps the fd open, pinning the inode.
    pub fn finish(mut self, meta: FileMeta) -> Result<Staged<'p>> {
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

        if self.named.is_none() {
            let proc_path = format!("/proc/self/fd/{}", fd.as_raw_fd());
            let ((), name) = with_fresh_name(TmpKind::Temp, |name| {
                if self.link_empty_path {
                    rustix::fs::linkat(fd, "", self.parent, name, AtFlags::EMPTY_PATH)
                } else {
                    rustix::fs::linkat(
                        CWD,
                        proc_path.as_str(),
                        self.parent,
                        name,
                        AtFlags::SYMLINK_FOLLOW,
                    )
                }
            })
            .map_err(|e| sys_err("link temp file", e))?;
            self.named = Some(name);
        }
        let fp = Fingerprint::of_fd(fd)?;
        point("stage.linked");
        // Hand over: from here on `Staged` cleans up, and `self`'s Drop does nothing.
        Ok(Staged {
            parent: self.parent,
            name: self.named.take().expect("linked above"),
            fp,
            pin: self.fd.take().expect("TempFile used after finish"),
            live: true,
        })
    }
}

impl Drop for TempFile<'_> {
    fn drop(&mut self) {
        // An O_TMPFILE inode vanishes with its fd; a named one must be removed.
        if let (Some(name), Some(fd)) = (self.named.take(), self.fd.as_ref()) {
            let res = Fingerprint::of_fd(fd.as_fd())
                .and_then(|ours| remove_if(self.parent, &name, |f| f.same_file(&ours)));
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
fn stage_file<'p>(
    t: &'p Target<'_>,
    caps: &Caps,
    content: &mut dyn Read,
    meta: FileMeta,
    expected_hash: &[u8; 32],
) -> Result<std::result::Result<Staged<'p>, &'static str>> {
    let mut tmp = TempFile::create(t.parent(), caps)?;
    tmp.copy_from(content)?;
    point("stage.written");
    if tmp.hash() != *expected_hash {
        return Ok(Err("content hash mismatch"));
    }
    tmp.finish(meta).map(Ok)
}

/// `symlinkat(target, parent, tmp)`, then pins the link and checks it is the
/// one we made.
fn stage_symlink<'p>(t: &'p Target<'_>, target: &[u8]) -> Result<Staged<'p>> {
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
    let ((), name) = with_fresh_name(TmpKind::Temp, |name| {
        rustix::fs::symlinkat(target, parent, name)
    })
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
    Ok(Staged {
        parent,
        name,
        fp,
        pin,
        live: true,
    })
}

/// `mkdirat(parent, tmp, 0700)`, then pins the directory and sets its mode.
fn stage_dir<'p>(t: &'p Target<'_>, mode: u32) -> Result<Staged<'p>> {
    let parent = t.parent();
    let ((), name) = with_fresh_name(TmpKind::Temp, |name| {
        rustix::fs::mkdirat(parent, name, Mode::from_raw_mode(0o700))
    })
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
) -> Result<Outcome> {
    // (a) Pin the old inode and check it against the index.
    point("replace.before_pin");
    let pin = match rustix::fs::openat2(
        t.parent(),
        t.name,
        OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
        RESOLVE,
    ) {
        Ok(pin) => pin,
        Err(Errno::NOENT) => {
            staged.discard()?;
            return Ok(Outcome::PreconditionFailed("name missing"));
        }
        Err(e) => return Err(t.err("pin", e)),
    };
    let old = Fingerprint::of_fd(pin.as_fd())?;
    if !expected.fp.unchanged(&old) || !matches!(old.kind, FileKind::File | FileKind::Symlink) {
        staged.discard()?;
        return Ok(Outcome::PreconditionFailed("changed since scan"));
    }
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
    point("replace.after_exchange");

    // (d) Verify what came out.
    match verify_old(t, &staged.name, &old, expected) {
        Verified::Unchanged => {
            point("replace.verified");
            // (f) Quarantine the old inode.
            quarantine.add(t, &staged.name, old, pin)?;
            point("replace.quarantined");
        }
        Verified::Gone => {
            tracing::warn!(path = %t.path, "replaced object vanished from its temp name");
        }
        Verified::Changed => return undo(ctx, t, staged),
    }
    t.finish(&staged.fp)
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
/// ctime is not compared, since the exchange itself bumps it. Errors count as
/// "changed", so the caller undoes the exchange.
fn verify_old(t: &Target<'_>, tmp: &[u8], old: &Fingerprint, expected: &Expected) -> Verified {
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
        point("replace.before_rehash");
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
            match rustix::fs::renameat_with(
                t.parent(),
                staged.name.as_slice(),
                t.parent(),
                t.name,
                RenameFlags::NOREPLACE,
            ) {
                Ok(()) | Err(Errno::NOENT) => Ok(failed),
                Err(Errno::EXIST) => {
                    let kind = t.stat(&staged.name)?.map_or(FileKind::File, |f| f.kind);
                    let conflict = t.preserve(ctx.replica, &staged.name, kind)?;
                    Ok(Outcome::Preserved { conflict })
                }
                Err(e) => Err(t.err("restore after failed replace of", e)),
            }
        }
        Err(e) => Err(t.err("undo exchange of", e)),
    }
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
    deadline: Instant,
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

    /// When the earliest entry becomes due for unlinking.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.pending.iter().map(|p| p.deadline).min()
    }

    /// Moves the old inode at the target's temp name `tmp` to a quarantine
    /// name and starts its grace period.
    fn add(&mut self, t: &Target<'_>, tmp: &[u8], old: Fingerprint, pin: OwnedFd) -> Result<()> {
        let name = match with_fresh_name(TmpKind::Old, |name| {
            rustix::fs::renameat_with(t.parent(), tmp, t.parent(), name, RenameFlags::NOREPLACE)
        }) {
            Ok(((), name)) => name,
            Err(e) => {
                // Keep it under the temp name; the sweep handles it all the same.
                tracing::warn!(path = %t.path, error = %e, "cannot rename old inode to quarantine name");
                tmp.to_vec()
            }
        };
        self.pending.push(Pending {
            parent: t
                .parent
                .try_clone()
                .map_err(|e| Error::io("duplicate parent fd", e))?,
            dir: t.path.parent().unwrap_or_default(),
            orig: t.name.to_vec(),
            name,
            fp: old,
            _pin: pin,
            deadline: Instant::now() + self.grace,
        });
        Ok(())
    }

    /// Processes every entry: one modified since the replace becomes a
    /// conflict copy; one unchanged past its deadline is unlinked; the rest
    /// wait. Errors are logged and the entry is retried on the next sweep.
    pub fn sweep(&mut self) -> SweepReport {
        let now = Instant::now();
        let mut report = SweepReport::default();
        let replica = self.replica;
        self.pending.retain(|p| match p.sweep(now, replica, &mut report) {
            Ok(done) => !done,
            Err(e) => {
                tracing::warn!(dir = %p.dir, name = %p.name.escape_ascii(), error = %e, "quarantine sweep failed");
                true
            }
        });
        report
    }
}

impl Pending {
    fn verdict(&self, now: Instant) -> Result<Verdict> {
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
        Ok(if now >= self.deadline {
            Verdict::Unlink
        } else {
            Verdict::Keep
        })
    }

    /// Returns whether the entry is finished.
    fn sweep(&self, now: Instant, replica: ReplicaId, report: &mut SweepReport) -> Result<bool> {
        let parent = self.parent.as_fd();
        let shown = || format!("{}/{}", self.dir, self.name.escape_ascii());
        match self.verdict(now)? {
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
    }

    impl Fx {
        fn new() -> Fx {
            let dir = tempfile::tempdir().unwrap();
            let root = Root::open(dir.path()).unwrap();
            let caps = Caps::probe(root.fd()).unwrap();
            Fx { dir, root, caps }
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
}
