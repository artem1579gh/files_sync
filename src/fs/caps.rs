//! Filesystem feature checks (design §1 environment caveat, §5.3).
//!
//! [`Caps::probe`] runs each check as a real, harmless operation inside the
//! replica root, using `.~fsync.probe.<tag>.*` names that are removed again
//! afterwards. Only names this probe created, still holding an inode it
//! created, are ever unlinked.

use std::fmt;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::path::Path;

use rustix::fs::{AtFlags, CWD, Mode, OFlags, RenameFlags, ResolveFlags, StatxFlags};
use rustix::io::Errno;

use crate::error::{Error, Result};
use crate::fs::lease::Lease;
use crate::fs::root::Root;
use crate::fs::stat::Fingerprint;

/// Name prefix of probe files; inside the reserved `.~fsync.` namespace.
pub const PROBE_PREFIX: &str = ".~fsync.probe.";

/// What the filesystem under a replica root supports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Caps {
    /// `openat2` with `RESOLVE_BENEATH` enforced (required, §5.1).
    pub openat2: bool,
    /// `renameat2(RENAME_EXCHANGE)` (required, §5.3 step 4).
    pub rename_exchange: bool,
    /// `renameat2(RENAME_NOREPLACE)` (required, §5.3 step 3).
    pub rename_noreplace: bool,
    /// `O_TMPFILE` (optional; falls back to a named temp file).
    pub o_tmpfile: bool,
    /// `linkat(fd, "", .., AT_EMPTY_PATH)` on an `O_TMPFILE` fd.
    pub linkat_empty_path: bool,
    /// `linkat(AT_FDCWD, "/proc/self/fd/N", .., AT_SYMLINK_FOLLOW)`, the
    /// fallback for linking an `O_TMPFILE` in (§5.3 step 2).
    pub linkat_proc_fd: bool,
    /// `statx` reports a birth time.
    pub statx_btime: bool,
    /// `F_SETLEASE F_WRLCK` can be taken on a file we own (§5.3 step 4(b)).
    pub leases: bool,
    pub fs_type: FsType,
}

impl Caps {
    /// Probes the filesystem of the directory `root`, which may be an
    /// `O_PATH` fd.
    pub fn probe(root: BorrowedFd<'_>) -> Result<Caps> {
        let st = rustix::fs::fstatfs(root).map_err(|e| io_err("fstatfs on replica root", e))?;
        // Magic numbers are 32-bit; mask off sign extension where f_type is signed.
        #[allow(clippy::unnecessary_cast)]
        let fs_type = FsType::from_magic(st.f_type as u64 & 0xffff_ffff);

        let openat2 = probe_openat2(root)?;
        let mut p = Probe::new(root)?;
        let result = p.run(fs_type, openat2);
        p.cleanup();
        result
    }

    /// Opens the directory at `path` and probes it.
    pub fn probe_path(path: &Path) -> Result<Caps> {
        Self::probe(Root::open(path)?.fd())
    }

    /// `O_TMPFILE` works and its result can be linked into the tree.
    pub fn tmpfile_usable(&self) -> bool {
        self.o_tmpfile && (self.linkat_empty_path || self.linkat_proc_fd)
    }

    /// Fails unless the features race-freedom depends on are present.
    pub fn require_minimum(&self) -> Result<()> {
        let missing: Vec<&str> = [
            (self.openat2, "openat2(RESOLVE_BENEATH)"),
            (self.rename_noreplace, "renameat2(RENAME_NOREPLACE)"),
            (self.rename_exchange, "renameat2(RENAME_EXCHANGE)"),
        ]
        .into_iter()
        .filter_map(|(ok, name)| (!ok).then_some(name))
        .collect();
        if missing.is_empty() {
            return Ok(());
        }
        Err(Error::MissingCapabilities {
            fs_type: self.fs_type.to_string(),
            missing: missing.join(", "),
        })
    }

    /// Logs the probe result for the replica at `root`.
    pub fn log(&self, root: &Path) {
        tracing::info!(
            root = %root.display(),
            fs = %self.fs_type,
            openat2 = self.openat2,
            rename_exchange = self.rename_exchange,
            rename_noreplace = self.rename_noreplace,
            o_tmpfile = self.o_tmpfile,
            linkat_empty_path = self.linkat_empty_path,
            linkat_proc_fd = self.linkat_proc_fd,
            statx_btime = self.statx_btime,
            leases = self.leases,
            "filesystem capabilities"
        );
        if !self.fs_type.is_recommended() {
            tracing::warn!(
                root = %root.display(),
                fs = %self.fs_type,
                "replica root is not on ext4, xfs, btrfs or tmpfs; this is untested"
            );
        }
        if !self.tmpfile_usable() {
            tracing::info!(root = %root.display(), "O_TMPFILE unusable; using named temp files");
        }
    }
}

/// Filesystem type, from `statfs.f_type`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsType {
    /// ext2, ext3 and ext4 share one magic number.
    Ext4,
    Xfs,
    Btrfs,
    Tmpfs,
    /// 9p: WSL2 drvfs (`/mnt/c`) and other virtual-machine shares.
    V9fs,
    /// FUSE, including virtiofs.
    Fuse,
    Nfs,
    Overlay,
    Other(u64),
}

impl FsType {
    pub fn from_magic(magic: u64) -> FsType {
        match magic {
            0xEF53 => FsType::Ext4,
            0x5846_5342 => FsType::Xfs,
            0x9123_683E => FsType::Btrfs,
            0x0102_1994 => FsType::Tmpfs,
            0x0102_1997 => FsType::V9fs,
            0x6573_5546 => FsType::Fuse,
            0x6969 => FsType::Nfs,
            0x794C_7630 => FsType::Overlay,
            m => FsType::Other(m),
        }
    }

    /// The filesystems the design supports (§1).
    pub fn is_recommended(&self) -> bool {
        matches!(
            self,
            FsType::Ext4 | FsType::Xfs | FsType::Btrfs | FsType::Tmpfs
        )
    }
}

impl fmt::Display for FsType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FsType::Ext4 => f.write_str("ext2/3/4"),
            FsType::Xfs => f.write_str("xfs"),
            FsType::Btrfs => f.write_str("btrfs"),
            FsType::Tmpfs => f.write_str("tmpfs"),
            FsType::V9fs => f.write_str("9p (e.g. WSL drvfs)"),
            FsType::Fuse => f.write_str("fuse"),
            FsType::Nfs => f.write_str("nfs"),
            FsType::Overlay => f.write_str("overlayfs"),
            FsType::Other(m) => write!(f, "unknown (magic {m:#x})"),
        }
    }
}

fn io_err(context: impl Into<String>, e: Errno) -> Error {
    Error::io(context, e.into())
}

/// `Ok(false)` for errnos that mean "not supported here", `Err` otherwise.
fn unsupported_if(e: Errno, unsupported: &[Errno], what: &str) -> Result<bool> {
    if unsupported.contains(&e) {
        tracing::debug!(%e, "{what} unsupported");
        Ok(false)
    } else {
        Err(io_err(format!("probe {what}"), e))
    }
}

/// openat2 must exist and actually enforce `RESOLVE_BENEATH`.
fn probe_openat2(root: BorrowedFd<'_>) -> Result<bool> {
    let resolve = ResolveFlags::BENEATH
        | ResolveFlags::NO_SYMLINKS
        | ResolveFlags::NO_MAGICLINKS
        | ResolveFlags::NO_XDEV;
    let flags = OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC;
    let open = |name: &str| rustix::fs::openat2(root, name, flags, Mode::empty(), resolve);
    match open(".") {
        Ok(_) => {}
        Err(e) => {
            return unsupported_if(
                e,
                &[Errno::NOSYS, Errno::PERM, Errno::INVAL, Errno::TOOBIG],
                "openat2",
            );
        }
    }
    match open("..") {
        Err(Errno::XDEV) => Ok(true),
        Ok(_) => {
            tracing::warn!("openat2 ignored RESOLVE_BENEATH");
            Ok(false)
        }
        Err(e) => unsupported_if(e, &[Errno::NOSYS, Errno::PERM, Errno::INVAL], "openat2"),
    }
}

/// Probe files created under the root, removed again by [`Probe::cleanup`].
struct Probe<'a> {
    root: BorrowedFd<'a>,
    tag: String,
    /// Every name a probe step may have created.
    names: Vec<Vec<u8>>,
    /// (dev, ino) of every inode this probe created; only names holding one
    /// of these are unlinked.
    inodes: Vec<FileId>,
    /// Open fds on those inodes, so their numbers cannot be reused by someone
    /// else's file before cleanup. `O_PATH` fds do not block write leases.
    pins: Vec<OwnedFd>,
}

type FileId = (u32, u32, u64);

fn file_id(st: &rustix::fs::Statx) -> FileId {
    (st.stx_dev_major, st.stx_dev_minor, st.stx_ino)
}

impl<'a> Probe<'a> {
    fn new(root: BorrowedFd<'a>) -> Result<Self> {
        let mut buf = [0u8; 8];
        rustix::rand::getrandom(&mut buf, rustix::rand::GetRandomFlags::empty())
            .map_err(|e| io_err("getrandom", e))?;
        Ok(Probe {
            root,
            tag: format!("{:016x}", u64::from_le_bytes(buf)),
            names: Vec::new(),
            inodes: Vec::new(),
            pins: Vec::new(),
        })
    }

    /// Returns a fresh probe name and remembers it for cleanup.
    fn name(&mut self, suffix: &str) -> Vec<u8> {
        let name = format!("{PROBE_PREFIX}{}.{suffix}", self.tag).into_bytes();
        self.names.push(name.clone());
        name
    }

    fn id_at(&self, name: &[u8]) -> Result<FileId> {
        rustix::fs::statx(self.root, name, AtFlags::SYMLINK_NOFOLLOW, StatxFlags::INO)
            .map(|st| file_id(&st))
            .map_err(|e| io_err("statx probe file", e))
    }

    fn ino_at(&self, name: &[u8]) -> Result<u64> {
        self.id_at(name).map(|id| id.2)
    }

    /// Records `fd`'s inode as ours and keeps `fd` open until cleanup.
    fn pin(&mut self, fd: OwnedFd) -> Result<FileId> {
        let st = rustix::fs::statx(&fd, "", AtFlags::EMPTY_PATH, StatxFlags::INO)
            .map_err(|e| io_err("statx probe fd", e))?;
        self.inodes.push(file_id(&st));
        self.pins.push(fd);
        Ok(file_id(&st))
    }

    /// Creates an empty regular file with `O_EXCL`. Only an `O_PATH` fd stays
    /// open, so the file can still be leased.
    fn create(&mut self, suffix: &str) -> Result<Vec<u8>> {
        let name = self.name(suffix);
        let fd = rustix::fs::openat(
            self.root,
            name.as_slice(),
            OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )
        .map_err(|e| io_err("create probe file in replica root", e))?;
        let path_fd = rustix::fs::openat(
            self.root,
            name.as_slice(),
            OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| io_err("reopen probe file", e))?;
        let id = self.pin(path_fd)?;
        let created = rustix::fs::statx(&fd, "", AtFlags::EMPTY_PATH, StatxFlags::INO)
            .map_err(|e| io_err("statx probe fd", e))?;
        if file_id(&created) != id {
            return Err(Error::io(
                "create probe file in replica root",
                std::io::Error::other("probe file replaced concurrently"),
            ));
        }
        Ok(name)
    }

    fn run(&mut self, fs_type: FsType, openat2: bool) -> Result<Caps> {
        let (o_tmpfile, linkat_empty_path, linkat_proc_fd) = self.probe_tmpfile()?;
        let a = self.create("a")?;
        let b = self.create("b")?;
        let (rename_noreplace, a) = self.probe_noreplace(a, &b)?;
        let rename_exchange = self.probe_exchange(&a, &b)?;
        let statx_btime = self.probe_btime(&b)?;
        let leases = self.probe_leases(&b);
        Ok(Caps {
            openat2,
            rename_exchange,
            rename_noreplace,
            o_tmpfile,
            linkat_empty_path,
            linkat_proc_fd,
            statx_btime,
            leases,
            fs_type,
        })
    }

    fn probe_tmpfile(&mut self) -> Result<(bool, bool, bool)> {
        let fd = match rustix::fs::openat(
            self.root,
            ".",
            OFlags::TMPFILE | OFlags::WRONLY | OFlags::CLOEXEC,
            Mode::from_bits_truncate(0o600),
        ) {
            Ok(fd) => fd,
            Err(e) => {
                let ok = unsupported_if(
                    e,
                    &[Errno::OPNOTSUPP, Errno::ISDIR, Errno::INVAL],
                    "O_TMPFILE",
                )?;
                return Ok((ok, false, false));
            }
        };
        let tmp_name = self.name("tmp");
        let proc_name = self.name("proc");
        // Pin before linking, so cleanup recognises the linked names as ours.
        self.pin(fd)?;
        let fd = self.pins.last().expect("just pinned").as_fd();
        // Linking fails with ENOENT when unprivileged AT_EMPTY_PATH is not
        // allowed (kernels before 6.10) or /proc is not mounted.
        let not_allowed = [
            Errno::NOENT,
            Errno::PERM,
            Errno::INVAL,
            Errno::XDEV,
            Errno::OPNOTSUPP,
        ];

        let empty_path =
            match rustix::fs::linkat(fd, "", self.root, tmp_name.as_slice(), AtFlags::EMPTY_PATH) {
                Ok(()) => true,
                Err(e) => unsupported_if(e, &not_allowed, "linkat(AT_EMPTY_PATH)")?,
            };

        let proc_path = format!("/proc/self/fd/{}", fd.as_raw_fd());
        let proc_fd = match rustix::fs::linkat(
            CWD,
            proc_path.as_str(),
            self.root,
            proc_name.as_slice(),
            AtFlags::SYMLINK_FOLLOW,
        ) {
            Ok(()) => true,
            Err(e) => unsupported_if(e, &not_allowed, "linkat(/proc/self/fd)")?,
        };
        Ok((true, empty_path, proc_fd))
    }

    /// Renaming onto an existing name must fail with EEXIST, and onto a free
    /// name must succeed. Returns the current name of `a`.
    fn probe_noreplace(&mut self, a: Vec<u8>, b: &[u8]) -> Result<(bool, Vec<u8>)> {
        let unsupported = [Errno::INVAL, Errno::NOSYS];
        match rustix::fs::renameat_with(
            self.root,
            a.as_slice(),
            self.root,
            b,
            RenameFlags::NOREPLACE,
        ) {
            Err(Errno::EXIST) => {}
            Ok(()) => {
                tracing::warn!("renameat2 ignored RENAME_NOREPLACE");
                return Ok((false, a));
            }
            Err(e) => return Ok((unsupported_if(e, &unsupported, "RENAME_NOREPLACE")?, a)),
        }
        let c = self.name("c");
        match rustix::fs::renameat_with(
            self.root,
            a.as_slice(),
            self.root,
            c.as_slice(),
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) => Ok((true, c)),
            Err(e) => Ok((unsupported_if(e, &unsupported, "RENAME_NOREPLACE")?, a)),
        }
    }

    /// Exchanging two names must swap their inodes.
    fn probe_exchange(&mut self, a: &[u8], b: &[u8]) -> Result<bool> {
        let (ino_a, ino_b) = (self.ino_at(a)?, self.ino_at(b)?);
        match rustix::fs::renameat_with(self.root, a, self.root, b, RenameFlags::EXCHANGE) {
            Ok(()) => {
                let swapped = self.ino_at(a)? == ino_b && self.ino_at(b)? == ino_a;
                if !swapped {
                    tracing::warn!("renameat2(RENAME_EXCHANGE) did not swap the files");
                }
                Ok(swapped)
            }
            Err(e) => unsupported_if(e, &[Errno::INVAL, Errno::NOSYS], "RENAME_EXCHANGE"),
        }
    }

    fn probe_btime(&self, name: &[u8]) -> Result<bool> {
        match rustix::fs::statx(
            self.root,
            name,
            AtFlags::SYMLINK_NOFOLLOW,
            StatxFlags::BTIME,
        ) {
            Ok(st) => Ok(st.stx_mask & StatxFlags::BTIME.bits() != 0),
            Err(e) => unsupported_if(e, &[Errno::NOSYS], "statx"),
        }
    }

    /// Takes and releases a write lease. Any failure means "no leases": they
    /// can be disabled (`fs.leases-enable`), unsupported by the filesystem, or
    /// refused because some other fd has the file open.
    fn probe_leases(&self, name: &[u8]) -> bool {
        let Ok(fp) = Fingerprint::at(self.root, name) else {
            return false;
        };
        Lease::take(self.root, name, &fp).is_some_and(|lease| lease.held())
    }

    /// Unlinks every probe name that still holds an inode we created. Anything
    /// else found under a probe name is left alone.
    fn cleanup(&mut self) {
        for name in std::mem::take(&mut self.names) {
            let shown = String::from_utf8_lossy(&name);
            match self.id_at(&name) {
                Ok(id) if self.inodes.contains(&id) => {
                    if let Err(e) =
                        rustix::fs::unlinkat(self.root, name.as_slice(), AtFlags::empty())
                    {
                        tracing::warn!(name = %shown, %e, "cannot remove probe file");
                    }
                }
                Ok(_) => {
                    tracing::warn!(name = %shown, "probe name holds a foreign file; leaving it")
                }
                Err(Error::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!(name = %shown, %e, "cannot check probe file"),
            }
        }
        self.pins.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn root_fd(dir: &Path) -> OwnedFd {
        rustix::fs::open(
            dir,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .unwrap()
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut v: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn tmp_supports_everything_and_leaves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("user"), b"data").unwrap();

        let caps = Caps::probe_path(dir.path()).unwrap();
        caps.log(dir.path());
        assert!(caps.openat2, "{caps:?}");
        assert!(caps.rename_exchange, "{caps:?}");
        assert!(caps.rename_noreplace, "{caps:?}");
        assert!(caps.o_tmpfile, "{caps:?}");
        assert!(caps.linkat_empty_path || caps.linkat_proc_fd, "{caps:?}");
        assert!(caps.tmpfile_usable());
        assert!(caps.statx_btime, "{caps:?}");
        assert!(caps.fs_type.is_recommended(), "{caps:?}");
        caps.require_minimum().unwrap();

        assert_eq!(names(dir.path()), ["user"]);
        assert_eq!(fs::read(dir.path().join("user")).unwrap(), b"data");
    }

    #[test]
    fn probe_is_repeatable() {
        let dir = tempfile::tempdir().unwrap();
        let fd = root_fd(dir.path());
        let first = Caps::probe(fd.as_fd()).unwrap();
        assert_eq!(Caps::probe(fd.as_fd()).unwrap(), first);
        assert!(names(dir.path()).is_empty());
    }

    #[test]
    fn require_minimum_lists_missing_features() {
        let dir = tempfile::tempdir().unwrap();
        let mut caps = Caps::probe_path(dir.path()).unwrap();
        caps.openat2 = false;
        caps.rename_exchange = false;
        caps.fs_type = FsType::V9fs;
        let msg = caps.require_minimum().unwrap_err().to_string();
        assert!(msg.contains("openat2"), "{msg}");
        assert!(msg.contains("RENAME_EXCHANGE"), "{msg}");
        assert!(!msg.contains("RENAME_NOREPLACE"), "{msg}");
        assert!(msg.contains("9p"), "{msg}");

        // Optional features do not matter.
        let mut caps = Caps::probe_path(dir.path()).unwrap();
        caps.o_tmpfile = false;
        caps.leases = false;
        caps.statx_btime = false;
        caps.require_minimum().unwrap();
    }

    #[test]
    fn cleanup_leaves_foreign_files_alone() {
        let dir = tempfile::tempdir().unwrap();
        let fd = root_fd(dir.path());
        let mut p = Probe::new(fd.as_fd()).unwrap();
        let ours = p.create("a").unwrap();
        // Someone else's file under a name the probe registered but never created.
        let foreign = p.name("b");
        let foreign_path = dir.path().join(String::from_utf8(foreign).unwrap());
        fs::write(&foreign_path, b"theirs").unwrap();
        // And our name replaced by a foreign inode.
        let ours_path = dir.path().join(String::from_utf8(ours).unwrap());
        fs::remove_file(&ours_path).unwrap();
        fs::write(&ours_path, b"also theirs").unwrap();

        p.cleanup();
        assert_eq!(fs::read(&foreign_path).unwrap(), b"theirs");
        assert_eq!(fs::read(&ours_path).unwrap(), b"also theirs");
    }

    #[test]
    fn fs_magic() {
        assert_eq!(FsType::from_magic(0xEF53), FsType::Ext4);
        assert_eq!(FsType::from_magic(0x0102_1997), FsType::V9fs);
        assert_eq!(FsType::from_magic(0x1234), FsType::Other(0x1234));
        assert!(!FsType::V9fs.is_recommended());
        assert!(FsType::Tmpfs.is_recommended());
    }
}
