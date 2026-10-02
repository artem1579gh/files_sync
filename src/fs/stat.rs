//! `statx` fingerprints and stable reads (design §5.2).

use std::io::{self, Read};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::time::Duration;

use rustix::fs::{AtFlags, FileType, OFlags, Statx, StatxFlags, StatxTimestamp};
use rustix::io::Errno;

use crate::error::{Error, Result};
use crate::fs::root::{open_beneath, unstable_reason, validate_name};

/// Object type, from `st_mode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum FileKind {
    File,
    Dir,
    Symlink,
    /// FIFO, socket or device node: never synced.
    Special,
}

impl FileKind {
    /// `None` for `DT_UNKNOWN`.
    pub fn from_file_type(t: FileType) -> Option<FileKind> {
        Some(match t {
            FileType::RegularFile => FileKind::File,
            FileType::Directory => FileKind::Dir,
            FileType::Symlink => FileKind::Symlink,
            FileType::Unknown => return None,
            _ => FileKind::Special,
        })
    }
}

/// What `statx` says about one inode, enough to notice any change to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Fingerprint {
    pub dev: u64,
    pub ino: u64,
    pub size: u64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    /// Permission bits (`st_mode & 0o7777`).
    pub mode: u32,
    pub kind: FileKind,
}

/// The `statx` fields a fingerprint needs.
const NEEDED: StatxFlags = StatxFlags::TYPE
    .union(StatxFlags::MODE)
    .union(StatxFlags::INO)
    .union(StatxFlags::SIZE)
    .union(StatxFlags::MTIME)
    .union(StatxFlags::CTIME);

impl Fingerprint {
    /// Fingerprint of the inode `fd` refers to (an `O_PATH` fd is fine).
    pub fn of_fd(fd: BorrowedFd<'_>) -> Result<Fingerprint> {
        rustix::fs::statx(fd, "", AtFlags::EMPTY_PATH, NEEDED)
            .map_err(|e| Error::io("statx fd", e.into()))
            .and_then(|st| Self::from_statx(&st))
    }

    /// Fingerprint of `dirfd/name`, not following a symlink at `name`.
    pub fn at(dirfd: BorrowedFd<'_>, name: &[u8]) -> Result<Fingerprint> {
        validate_name(name)?;
        rustix::fs::statx(dirfd, name, AtFlags::SYMLINK_NOFOLLOW, NEEDED)
            .map_err(|e| Error::io(format!("statx {}", name.escape_ascii()), e.into()))
            .and_then(|st| Self::from_statx(&st))
    }

    /// Like [`Fingerprint::at`], but `Ok(None)` when `name` does not exist.
    pub fn at_opt(dirfd: BorrowedFd<'_>, name: &[u8]) -> Result<Option<Fingerprint>> {
        match Self::at(dirfd, name) {
            Ok(fp) => Ok(Some(fp)),
            Err(Error::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn from_statx(st: &Statx) -> Result<Fingerprint> {
        if st.stx_mask & NEEDED.bits() != NEEDED.bits() {
            return Err(Error::io(
                "statx",
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("filesystem did not report all of {NEEDED:?}"),
                ),
            ));
        }
        let mode = u32::from(st.stx_mode);
        let kind =
            FileKind::from_file_type(FileType::from_raw_mode(mode)).unwrap_or(FileKind::Special);
        Ok(Fingerprint {
            dev: rustix::fs::makedev(st.stx_dev_major, st.stx_dev_minor),
            ino: st.stx_ino,
            size: st.stx_size,
            mtime_ns: timestamp_ns(&st.stx_mtime),
            ctime_ns: timestamp_ns(&st.stx_ctime),
            mode: mode & 0o7777,
            kind,
        })
    }

    /// Same inode: (dev, ino) are equal.
    pub fn same_file(&self, other: &Fingerprint) -> bool {
        self.dev == other.dev && self.ino == other.ino
    }

    /// Same inode with the same kind, size, mtime and ctime: nothing about its
    /// content or metadata changed in between (§5.2 step 5).
    pub fn unchanged(&self, other: &Fingerprint) -> bool {
        self.same_file(other)
            && self.kind == other.kind
            && self.size == other.size
            && self.mtime_ns == other.mtime_ns
            && self.ctime_ns == other.ctime_ns
    }
}

fn timestamp_ns(t: &StatxTimestamp) -> i64 {
    t.tv_sec
        .saturating_mul(1_000_000_000)
        .saturating_add(i64::from(t.tv_nsec))
}

/// Where [`stable_read`] sends the content it reads.
///
/// A read can be retried, so a sink must be able to drop what an earlier,
/// unstable attempt wrote.
pub trait Sink {
    /// Discards everything written so far. Called before each retry, not
    /// before the first attempt.
    fn restart(&mut self) -> io::Result<()>;
    fn write(&mut self, data: &[u8]) -> io::Result<()>;
}

/// Hash only; the content is dropped.
pub struct Discard;

impl Sink for Discard {
    fn restart(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn write(&mut self, _: &[u8]) -> io::Result<()> {
        Ok(())
    }
}

impl Sink for Vec<u8> {
    fn restart(&mut self) -> io::Result<()> {
        self.clear();
        Ok(())
    }
    fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.extend_from_slice(data);
        Ok(())
    }
}

/// Retries after the first unstable attempt (§5.2 step 5).
pub const STABLE_READ_RETRIES: u32 = 3;
const BACKOFF: Duration = Duration::from_millis(5);
const CHUNK: usize = 64 * 1024;

/// Reads the regular file `parent/name` through blake3 into `sink`, and
/// returns its fingerprint and hash only if nothing changed during the read
/// (design §5.2).
///
/// The fingerprint before and after reading must agree on ino, size, mtime
/// and ctime, the byte count must equal the size, and `name` must still be
/// the same inode afterwards. Otherwise the read is retried
/// [`STABLE_READ_RETRIES`] times with backoff and then fails with
/// [`Error::Unstable`]. A symlink or non-regular file at `name` is also
/// unstable: the caller expected a file. Changes that leave mtime, ctime and
/// size all identical within one timestamp tick are not detectable here;
/// that is what the scanner's `racy` flag is for (§3).
pub fn stable_read<S: Sink + ?Sized>(
    parent: BorrowedFd<'_>,
    name: &[u8],
    sink: &mut S,
) -> Result<(Fingerprint, [u8; 32])> {
    validate_name(name)?;
    stable_read_with(
        name,
        || open_for_read(parent, name),
        |f2| match Fingerprint::at(parent, name) {
            Ok(f3) if f2.same_file(&f3) => Ok(None),
            Ok(_) => Ok(Some("name replaced during read")),
            Err(Error::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
                Ok(Some("name removed during read"))
            }
            Err(e) => Err(e),
        },
        sink,
    )
}

/// The general form of [`stable_read`], for files reached some other way
/// (the scanner reads followed symlink referents with it, design §4.5).
///
/// Each attempt calls `open` to get a fresh read fd, then applies the same
/// checks as [`stable_read`]: a regular file, F1 and F2 unchanged, byte count
/// equal to the size. `recheck` runs last with F2 and returns why the file is
/// no longer reachable the way it was opened (`stable_read`'s F3 check), if
/// it isn't. Errors name `what`.
pub fn stable_read_with<S, O, C>(
    what: &[u8],
    mut open: O,
    mut recheck: C,
    sink: &mut S,
) -> Result<(Fingerprint, [u8; 32])>
where
    S: Sink + ?Sized,
    O: FnMut() -> std::result::Result<OwnedFd, Errno>,
    C: FnMut(&Fingerprint) -> Result<Option<&'static str>>,
{
    let mut buf = vec![0u8; CHUNK];
    let mut reason = "";
    for attempt in 0..=STABLE_READ_RETRIES {
        if attempt > 0 {
            std::thread::sleep(BACKOFF * (1 << (attempt - 1)));
            sink.restart()
                .map_err(|e| Error::io("restart read sink", e))?;
        }
        match read_once(what, &mut open, &mut recheck, sink, &mut buf)? {
            Ok(done) => return Ok(done),
            Err(why) => {
                tracing::debug!(name = %what.escape_ascii(), attempt, why, "unstable read");
                reason = why;
            }
        }
    }
    Err(Error::Unstable {
        path: what.to_vec(),
        reason,
    })
}

/// One attempt: `Ok(Err(reason))` when the file changed under us.
fn read_once<S, O, C>(
    what: &[u8],
    open: &mut O,
    recheck: &mut C,
    sink: &mut S,
    buf: &mut [u8],
) -> Result<std::result::Result<(Fingerprint, [u8; 32]), &'static str>>
where
    S: Sink + ?Sized,
    O: FnMut() -> std::result::Result<OwnedFd, Errno>,
    C: FnMut(&Fingerprint) -> Result<Option<&'static str>>,
{
    let shown = || what.escape_ascii();
    let fd = match open() {
        Ok(fd) => fd,
        Err(Errno::LOOP) => return Ok(Err("name is a symlink")),
        // Someone holds a lease on it (O_NONBLOCK turns the wait into EAGAIN).
        Err(Errno::AGAIN) => return Ok(Err("file is leased")),
        // A socket.
        Err(Errno::NXIO) => return Ok(Err("not a regular file")),
        Err(e) => {
            return Err(match unstable_reason(e) {
                Some(reason) => Error::Unstable {
                    path: what.to_vec(),
                    reason,
                },
                None => Error::io(format!("open {} for reading", shown()), e.into()),
            });
        }
    };
    let f1 = Fingerprint::of_fd(fd.as_fd())?;
    if f1.kind != FileKind::File {
        return Ok(Err("not a regular file"));
    }

    let mut hasher = blake3::Hasher::new();
    let mut total: u64 = 0;
    loop {
        let n = match rustix::io::read(&fd, &mut *buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(Errno::INTR) => continue,
            Err(e) => return Err(Error::io(format!("read {}", shown()), e.into())),
        };
        total += n as u64;
        if total > f1.size {
            return Ok(Err("file grew during read"));
        }
        hasher.update(&buf[..n]);
        sink.write(&buf[..n])
            .map_err(|e| Error::io(format!("send content of {}", shown()), e))?;
    }

    let f2 = Fingerprint::of_fd(fd.as_fd())?;
    if !f1.unchanged(&f2) {
        return Ok(Err("file changed during read"));
    }
    if total != f2.size {
        return Ok(Err("byte count differs from size"));
    }
    if let Some(why) = recheck(&f2)? {
        return Ok(Err(why));
    }
    Ok(Ok((f2, *hasher.finalize().as_bytes())))
}

/// The check a [`StableReader`] runs at EOF with the final fingerprint: why
/// the file is no longer reachable the way it was opened, if it isn't (see
/// [`stable_read_with`]).
pub type Recheck = Box<dyn FnMut(&Fingerprint) -> Result<Option<&'static str>> + Send>;

/// A streaming [`stable_read`] for content that is passed on as it is read
/// (`Replica::open_read`, design §7): one attempt, no retries, since the
/// bytes already handed out cannot be taken back.
///
/// It applies the same checks as `stable_read`, plus the expected size and
/// hash. The file must be a regular file of the expected size when opened.
/// At EOF its fingerprint must be unchanged, the byte count must equal the
/// size, `recheck` must pass, and the content must hash to the expected
/// hash. Only then does `read` return `Ok(0)`; so a consumer that reads to
/// EOF has verified content. Otherwise `read` fails with an `io::Error` that
/// wraps [`Error::Unstable`] (unwrap it with [`Error::from_stream`]), and so
/// does every later call.
pub struct StableReader {
    what: Vec<u8>,
    /// Closed once the read is verified or failed, so the file is no longer
    /// open while the content is committed (a lease on it can be taken,
    /// §5.3 step 4(b)).
    fd: Option<OwnedFd>,
    f1: Fingerprint,
    hash: [u8; 32],
    hasher: blake3::Hasher,
    total: u64,
    recheck: Recheck,
    state: ReaderState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReaderState {
    Reading,
    Verified,
    Failed(&'static str),
}

impl StableReader {
    /// Reads the regular file `parent/name`; `name` must still be the same
    /// inode at EOF. `what` names the file in errors.
    pub fn open_at(
        what: &[u8],
        parent: OwnedFd,
        name: &[u8],
        size: u64,
        hash: [u8; 32],
    ) -> Result<StableReader> {
        validate_name(name)?;
        let fd = open_for_read(parent.as_fd(), name).map_err(|e| open_err(what, e))?;
        let name = name.to_vec();
        let recheck: Recheck =
            Box::new(
                move |f2| match Fingerprint::at_opt(parent.as_fd(), &name)? {
                    Some(f3) if f2.same_file(&f3) => Ok(None),
                    Some(_) => Ok(Some("name replaced during read")),
                    None => Ok(Some("name removed during read")),
                },
            );
        StableReader::new(what, fd, size, hash, recheck)
    }

    /// Reads the file open at `fd` (for files reached some other way, such
    /// as a followed symlink's referent).
    pub fn new(
        what: &[u8],
        fd: OwnedFd,
        size: u64,
        hash: [u8; 32],
        recheck: Recheck,
    ) -> Result<StableReader> {
        let unstable = |reason| Error::Unstable {
            path: what.to_vec(),
            reason,
        };
        let f1 = Fingerprint::of_fd(fd.as_fd())?;
        if f1.kind != FileKind::File {
            return Err(unstable("not a regular file"));
        }
        if f1.size != size {
            return Err(unstable("size differs from the expected entry"));
        }
        Ok(StableReader {
            what: what.to_vec(),
            fd: Some(fd),
            f1,
            hash,
            hasher: blake3::Hasher::new(),
            total: 0,
            recheck,
            state: ReaderState::Reading,
        })
    }

    /// The fingerprint when the file was opened.
    pub fn fingerprint(&self) -> &Fingerprint {
        &self.f1
    }

    fn fail(&mut self, reason: &'static str) -> io::Error {
        tracing::debug!(name = %self.what.escape_ascii(), reason, "unstable streaming read");
        self.state = ReaderState::Failed(reason);
        self.fd = None;
        self.unstable(reason)
    }

    fn unstable(&self, reason: &'static str) -> io::Error {
        io::Error::other(Error::Unstable {
            path: self.what.clone(),
            reason,
        })
    }

    /// The EOF checks; `Ok(Some(reason))` if one fails.
    fn verify(&mut self) -> Result<Option<&'static str>> {
        let fd = self.fd.as_ref().expect("open while reading");
        let f2 = Fingerprint::of_fd(fd.as_fd())?;
        if !self.f1.unchanged(&f2) {
            return Ok(Some("file changed during read"));
        }
        if self.total != f2.size {
            return Ok(Some("byte count differs from size"));
        }
        if let Some(why) = (self.recheck)(&f2)? {
            return Ok(Some(why));
        }
        if *self.hasher.finalize().as_bytes() != self.hash {
            return Ok(Some("content differs from the expected hash"));
        }
        Ok(None)
    }
}

impl Read for StableReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.state {
            ReaderState::Reading => {}
            ReaderState::Verified => return Ok(0),
            ReaderState::Failed(reason) => return Err(self.unstable(reason)),
        }
        if buf.is_empty() {
            return Ok(0);
        }
        let fd = self.fd.as_ref().expect("open while reading");
        let n = loop {
            match rustix::io::read(fd, &mut *buf) {
                Ok(n) => break n,
                Err(Errno::INTR) => continue,
                Err(e) => return Err(e.into()),
            }
        };
        if n == 0 {
            return match self.verify() {
                Ok(None) => {
                    self.state = ReaderState::Verified;
                    self.fd = None;
                    Ok(0)
                }
                Ok(Some(reason)) => Err(self.fail(reason)),
                Err(e) => Err(io::Error::other(e)),
            };
        }
        self.total += n as u64;
        if self.total > self.f1.size {
            return Err(self.fail("file grew during read"));
        }
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

/// Maps a failed open for reading like [`read_once`] does, but as an error.
fn open_err(what: &[u8], e: Errno) -> Error {
    let reason = match e {
        Errno::LOOP => "name is a symlink",
        Errno::AGAIN => "file is leased",
        Errno::NXIO => "not a regular file",
        e => match unstable_reason(e) {
            Some(reason) => reason,
            None => {
                return Error::io(
                    format!("open {} for reading", what.escape_ascii()),
                    e.into(),
                );
            }
        },
    };
    Error::Unstable {
        path: what.to_vec(),
        reason,
    }
}

/// `O_RDONLY | O_NOFOLLOW | O_NOATIME`, retrying without `O_NOATIME` on EPERM
/// (we don't own the file). `O_NONBLOCK` keeps a FIFO or a leased file from
/// blocking us; it has no effect on reads from regular files.
pub(crate) fn open_for_read(
    parent: BorrowedFd<'_>,
    name: &[u8],
) -> std::result::Result<OwnedFd, Errno> {
    let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
    match open_beneath(parent, name, flags | OFlags::NOATIME) {
        Err(Errno::PERM) => open_beneath(parent, name, flags),
        res => res,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::root::{RelPath, Root};
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    fn hash(data: &[u8]) -> [u8; 32] {
        *blake3::hash(data).as_bytes()
    }

    fn root_fd(dir: &Path) -> Root {
        Root::open(dir).unwrap()
    }

    /// A sink that runs `hook` on the first chunk of each attempt, standing in
    /// for a hook point between F1 and F2.
    struct HookSink<F: FnMut(u32)> {
        data: Vec<u8>,
        attempt: u32,
        fired: bool,
        hook: F,
    }

    impl<F: FnMut(u32)> HookSink<F> {
        fn new(hook: F) -> Self {
            HookSink {
                data: Vec::new(),
                attempt: 0,
                fired: false,
                hook,
            }
        }
    }

    impl<F: FnMut(u32)> Sink for HookSink<F> {
        fn restart(&mut self) -> io::Result<()> {
            self.data.clear();
            self.attempt += 1;
            self.fired = false;
            Ok(())
        }
        fn write(&mut self, data: &[u8]) -> io::Result<()> {
            self.data.extend_from_slice(data);
            if !self.fired {
                self.fired = true;
                (self.hook)(self.attempt);
            }
            Ok(())
        }
    }

    fn append(path: &PathBuf, data: &[u8]) {
        fs::OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(data)
            .unwrap();
    }

    #[test]
    fn normal_read_returns_blake3() {
        let dir = tempfile::tempdir().unwrap();
        // Several chunks plus a partial one.
        let content: Vec<u8> = (0..CHUNK * 3 + 123).map(|i| (i * 7 % 251) as u8).collect();
        fs::write(dir.path().join("f"), &content).unwrap();
        fs::write(dir.path().join("empty"), b"").unwrap();
        let root = root_fd(dir.path());

        let mut out = Vec::new();
        let (fp, h) = stable_read(root.fd(), b"f", &mut out).unwrap();
        assert_eq!(h, hash(&content));
        assert_eq!(out, content);
        assert_eq!(fp.size, content.len() as u64);
        assert_eq!(fp.kind, FileKind::File);
        assert!(fp.unchanged(&root.stat(&RelPath::new("f").unwrap()).unwrap()));

        let (fp, h) = stable_read(root.fd(), b"empty", &mut Discard).unwrap();
        assert_eq!((fp.size, h), (0, hash(b"")));
    }

    #[test]
    fn read_in_subdir_via_resolve_parent() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("a/b")).unwrap();
        fs::write(dir.path().join("a/b/f"), b"hello").unwrap();
        let root = root_fd(dir.path());
        let p = RelPath::new("a/b/f").unwrap();
        let (parent, name) = root.resolve_parent(&p).unwrap();
        let (_, h) = stable_read(parent.as_fd(), name, &mut Discard).unwrap();
        assert_eq!(h, hash(b"hello"));
    }

    #[test]
    fn append_during_every_attempt_is_unstable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        fs::write(&path, b"initial").unwrap();
        let root = root_fd(dir.path());

        let mut attempts = 0;
        let mut sink = HookSink::new(|_| {
            attempts += 1;
            append(&path, b"+more");
        });
        let err = stable_read(root.fd(), b"f", &mut sink).unwrap_err();
        assert!(
            matches!(err, Error::Unstable { ref path, .. } if path == b"f"),
            "{err}"
        );
        assert_eq!(attempts, 1 + STABLE_READ_RETRIES);
    }

    #[test]
    fn append_during_first_attempt_is_retried() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        fs::write(&path, b"initial").unwrap();
        let root = root_fd(dir.path());

        let mut sink = HookSink::new(|attempt| {
            if attempt == 0 {
                append(&path, b"+more");
            }
        });
        let (fp, h) = stable_read(root.fd(), b"f", &mut sink).unwrap();
        assert_eq!(sink.attempt, 1);
        assert_eq!(h, hash(b"initial+more"));
        assert_eq!(sink.data, b"initial+more");
        assert_eq!(fp.size, 12);
    }

    #[test]
    fn appending_thread_is_unstable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        // Large enough that each attempt takes a while.
        fs::write(&path, vec![b'x'; 8 << 20]).unwrap();
        let root = root_fd(dir.path());

        let stop = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|s| {
            s.spawn(|| {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    append(&path, b"y");
                }
            });
            let res = stable_read(root.fd(), b"f", &mut Discard);
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            assert!(matches!(res, Err(Error::Unstable { .. })), "{res:?}");
        });
    }

    #[test]
    fn name_replaced_during_read_is_unstable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        fs::write(&path, b"old").unwrap();
        let root = root_fd(dir.path());

        // F1 == F2 on the open fd, but `f` now names a different inode (F3).
        let tmp = dir.path().join("t");
        let mut sink = HookSink::new(|_| {
            fs::write(&tmp, b"new").unwrap();
            fs::rename(&tmp, &path).unwrap();
        });
        let err = stable_read(root.fd(), b"f", &mut sink).unwrap_err();
        assert!(matches!(err, Error::Unstable { .. }), "{err}");

        // Removed during the read: unstable, then NotFound on the retry.
        let mut sink = HookSink::new(|_| fs::remove_file(&path).unwrap());
        match stable_read(root.fd(), b"f", &mut sink) {
            Err(Error::Io { source, .. }) => assert_eq!(source.kind(), io::ErrorKind::NotFound),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn non_regular_files_are_unstable() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("f"), b"x").unwrap();
        std::os::unix::fs::symlink("f", dir.path().join("link")).unwrap();
        fs::create_dir(dir.path().join("d")).unwrap();
        rustix::fs::mknodat(
            rustix::fs::CWD,
            dir.path().join("fifo").as_path(),
            FileType::Fifo,
            rustix::fs::Mode::from_bits_truncate(0o600),
            0,
        )
        .unwrap();
        let root = root_fd(dir.path());
        for name in [&b"link"[..], b"d", b"fifo"] {
            let res = stable_read(root.fd(), name, &mut Discard);
            assert!(
                matches!(res, Err(Error::Unstable { .. })),
                "{}: {res:?}",
                name.escape_ascii()
            );
        }
        assert!(matches!(
            stable_read(root.fd(), b"a/f", &mut Discard),
            Err(Error::InvalidPath { .. })
        ));
    }

    /// T18: the streaming reader closes the file once it has verified it, so
    /// a commit of what it read can take a lease on the file.
    #[test]
    fn stable_reader_closes_the_file_at_eof() {
        use crate::fs::lease::Lease;
        use std::io::Read;
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("f"), b"data").unwrap();
        let root = root_fd(dir.path());
        let fp = Fingerprint::at(root.fd(), b"f").unwrap();
        let parent = rustix::io::fcntl_dupfd_cloexec(root.fd(), 0).unwrap();
        let mut r = StableReader::open_at(b"f", parent, b"f", 4, hash(b"data")).unwrap();
        assert!(
            Lease::take(root.fd(), b"f", &fp).is_none(),
            "open while reading"
        );
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"data");
        assert!(Lease::take(root.fd(), b"f", &fp).is_some(), "closed at EOF");
        assert_eq!(r.read(&mut [0; 8]).unwrap(), 0);
    }

    #[test]
    fn fingerprint_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        fs::write(&path, b"12345").unwrap();
        fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o640)).unwrap();
        let root = root_fd(dir.path());
        let fp = root.stat(&RelPath::new("f").unwrap()).unwrap();
        let std_md = fs::symlink_metadata(&path).unwrap();
        use std::os::unix::fs::MetadataExt;
        assert_eq!(fp.ino, std_md.ino());
        assert_eq!(fp.dev, std_md.dev());
        assert_eq!(fp.size, 5);
        assert_eq!(fp.mode, 0o640);
        assert_eq!(
            fp.mtime_ns,
            std_md.mtime() * 1_000_000_000 + std_md.mtime_nsec()
        );
        assert_eq!(
            fp.ctime_ns,
            std_md.ctime() * 1_000_000_000 + std_md.ctime_nsec()
        );
        assert_eq!(root.stat(&RelPath::root()).unwrap().kind, FileKind::Dir);
    }
}
