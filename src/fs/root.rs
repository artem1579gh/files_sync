//! Replica root handle, validated relative paths and race-free path
//! resolution (design §5.1).
//!
//! Every lookup inside a replica starts at the root dirfd. The parent of a
//! path is opened with `openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS |
//! RESOLVE_NO_MAGICLINKS | RESOLVE_NO_XDEV)`; callers then act on the single
//! final name with `*at(parentfd, name)`. A symlink or mount point anywhere on
//! the way makes resolution fail with [`Error::Unstable`].

use std::ffi::OsStr;
use std::fmt;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use rustix::fs::{Dir, Mode, OFlags, ResolveFlags};
use rustix::io::Errno;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::fs::stat::{FileKind, Fingerprint};

/// `openat2` resolve flags used for every lookup inside a replica.
pub const RESOLVE: ResolveFlags = ResolveFlags::BENEATH
    .union(ResolveFlags::NO_SYMLINKS)
    .union(ResolveFlags::NO_MAGICLINKS)
    .union(ResolveFlags::NO_XDEV);

/// Resolution for following a symlink to an in-tree referent (§4.5).
const IN_TREE: ResolveFlags = ResolveFlags::BENEATH
    .union(ResolveFlags::NO_MAGICLINKS)
    .union(ResolveFlags::NO_XDEV);

/// A path relative to a replica root, as raw bytes.
///
/// It is either the root itself (empty) or a sequence of components joined by
/// single `/`s. Components are non-empty and are never `.` or `..`. No path
/// contains a NUL byte or starts or ends with `/`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
#[serde(try_from = "Vec<u8>", into = "Vec<u8>")]
pub struct RelPath(Vec<u8>);

impl RelPath {
    /// The root itself.
    pub fn root() -> RelPath {
        RelPath(Vec::new())
    }

    /// Validates `path`. The empty path is the root.
    pub fn new(path: impl Into<Vec<u8>>) -> Result<RelPath> {
        let path = path.into();
        if !path.is_empty() {
            for c in path.split(|&b| b == b'/') {
                check_component(c).map_err(|reason| Error::InvalidPath {
                    path: path.clone(),
                    reason,
                })?;
            }
        }
        Ok(RelPath(path))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn as_os_str(&self) -> &OsStr {
        OsStr::from_bytes(&self.0)
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    /// Number of components; 0 for the root.
    pub fn depth(&self) -> usize {
        self.components().count()
    }

    pub fn components(&self) -> impl Iterator<Item = &[u8]> {
        self.0.split(|&b| b == b'/').filter(|c| !c.is_empty())
    }

    /// The last component, or `None` for the root.
    pub fn name(&self) -> Option<&[u8]> {
        self.split().map(|(_, name)| name)
    }

    /// The path without its last component, or `None` for the root.
    pub fn parent(&self) -> Option<RelPath> {
        self.split().map(|(parent, _)| RelPath(parent.to_vec()))
    }

    /// Appends `rel`, which is validated like [`RelPath::new`] and must not be
    /// empty.
    pub fn join(&self, rel: impl AsRef<[u8]>) -> Result<RelPath> {
        let rel = rel.as_ref();
        if rel.is_empty() {
            return Err(Error::InvalidPath {
                path: rel.to_vec(),
                reason: "empty component",
            });
        }
        let rel = RelPath::new(rel)?;
        if self.is_root() {
            return Ok(rel);
        }
        let mut path = Vec::with_capacity(self.0.len() + 1 + rel.0.len());
        path.extend_from_slice(&self.0);
        path.push(b'/');
        path.extend_from_slice(&rel.0);
        Ok(RelPath(path))
    }

    /// `(parent bytes, last component)`, or `None` for the root.
    fn split(&self) -> Option<(&[u8], &[u8])> {
        if self.is_root() {
            return None;
        }
        Some(match self.0.iter().rposition(|&b| b == b'/') {
            Some(i) => (&self.0[..i], &self.0[i + 1..]),
            None => (&[][..], &self.0[..]),
        })
    }
}

/// Why `c` is not a valid single path component, if it isn't.
fn check_component(c: &[u8]) -> std::result::Result<(), &'static str> {
    match c {
        [] => Err("empty component (absolute path, `//` or trailing `/`)"),
        b"." => Err("`.` component"),
        b".." => Err("`..` component"),
        _ if c.contains(&b'/') => Err("component contains `/`"),
        _ if c.contains(&0) => Err("contains a NUL byte"),
        _ => Ok(()),
    }
}

/// Fails unless `name` is a single valid path component.
pub fn validate_name(name: &[u8]) -> Result<()> {
    check_component(name).map_err(|reason| Error::InvalidPath {
        path: name.to_vec(),
        reason,
    })
}

impl TryFrom<Vec<u8>> for RelPath {
    type Error = Error;
    fn try_from(path: Vec<u8>) -> Result<RelPath> {
        RelPath::new(path)
    }
}

impl TryFrom<&[u8]> for RelPath {
    type Error = Error;
    fn try_from(path: &[u8]) -> Result<RelPath> {
        RelPath::new(path)
    }
}

impl From<RelPath> for Vec<u8> {
    fn from(p: RelPath) -> Vec<u8> {
        p.0
    }
}

impl fmt::Display for RelPath {
    /// Escapes non-printable and non-ASCII bytes; the root shows as `.`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_root() {
            f.write_str(".")
        } else {
            write!(f, "{}", self.0.escape_ascii())
        }
    }
}

impl fmt::Debug for RelPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RelPath(\"{}\")", self.0.escape_ascii())
    }
}

/// An entry returned by [`Root::read_dir`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    pub name: Vec<u8>,
    /// From `d_type`; `None` when the filesystem reports `DT_UNKNOWN` and the
    /// caller has to `statx` the name.
    pub kind: Option<FileKind>,
}

/// An open replica root: an `O_PATH | O_DIRECTORY` fd plus the path it was
/// opened from (for messages only; it is never used for access).
#[derive(Debug)]
pub struct Root {
    fd: OwnedFd,
    path: PathBuf,
}

impl Root {
    /// Opens the directory at `path`. Symlinks in `path` itself are followed;
    /// nothing below the root ever is.
    pub fn open(path: &Path) -> Result<Root> {
        let fd = rustix::fs::open(
            path,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| Error::io(format!("open replica root {}", path.display()), e.into()))?;
        Ok(Root {
            fd,
            path: path.to_path_buf(),
        })
    }

    pub fn fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Opens the parent directory of `p` as an `O_PATH` fd and returns it with
    /// `p`'s last component. A symlink, magic link or mount point on the way
    /// gives [`Error::Unstable`]; the root itself has no parent.
    pub fn resolve_parent<'p>(&self, p: &'p RelPath) -> Result<(OwnedFd, &'p [u8])> {
        let Some((parent, name)) = p.split() else {
            return Err(Error::InvalidPath {
                path: Vec::new(),
                reason: "the root has no parent",
            });
        };
        // No O_NOFOLLOW: with O_PATH it would let RESOLVE_NO_SYMLINKS accept a
        // trailing symlink (opening the link itself, then failing with
        // ENOTDIR). Without it, any symlink on the way is ELOOP.
        let flags = OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC;
        let fd = if parent.is_empty() {
            self.fd
                .try_clone()
                .map_err(|e| Error::io("duplicate root fd", e))?
        } else {
            open_beneath(self.fd(), parent, flags).map_err(|e| path_err(e, p, "resolve parent"))?
        };
        Ok((fd, name))
    }

    /// Opens the directory `p` (the root included) for reading.
    pub fn open_dir(&self, p: &RelPath) -> Result<OwnedFd> {
        // No O_NOFOLLOW, as in `resolve_parent`: with O_DIRECTORY it turns a
        // symlink into ENOTDIR instead of RESOLVE_NO_SYMLINKS's ELOOP.
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
        let res = match p.split() {
            None => open_beneath(self.fd(), b".", flags),
            Some(_) => {
                let (parent, name) = self.resolve_parent(p)?;
                open_beneath(parent.as_fd(), name, flags)
            }
        };
        res.map_err(|e| path_err(e, p, "open directory"))
    }

    /// Lists the directory `p` (the root included), sorted by name, without
    /// `.` and `..`. Reserved `.~fsync.` names are included; callers filter.
    pub fn read_dir(&self, p: &RelPath) -> Result<Vec<DirEntry>> {
        let dir = self.open_dir(p)?;
        let mut entries = Vec::new();
        for entry in Dir::new(dir).map_err(|e| path_err(e, p, "read directory"))? {
            let entry = entry.map_err(|e| path_err(e, p, "read directory"))?;
            let name = entry.file_name().to_bytes();
            if name == b"." || name == b".." {
                continue;
            }
            entries.push(DirEntry {
                name: name.to_vec(),
                kind: FileKind::from_file_type(entry.file_type()),
            });
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(entries)
    }

    /// Fingerprint of `p` itself, not following a final symlink.
    pub fn stat(&self, p: &RelPath) -> Result<Fingerprint> {
        let res = match p.split() {
            None => Fingerprint::of_fd(self.fd()),
            Some(_) => {
                let (parent, name) = self.resolve_parent(p)?;
                Fingerprint::at(parent.as_fd(), name)
            }
        };
        res.map_err(|e| match e {
            Error::Io { source, .. } => Error::io(format!("stat {p}"), source),
            e => e,
        })
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
    pub(crate) fn open_referent(
        &self,
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
        match rustix::fs::openat2(self.fd(), path.as_bytes(), flags, Mode::empty(), IN_TREE) {
            Ok(fd) => Ok((fd, false)),
            Err(Errno::XDEV) => direct().map(|fd| (fd, true)),
            Err(_) => match direct() {
                Ok(_) => Err(Errno::AGAIN),
                Err(e) => Err(e),
            },
        }
    }
}

/// `openat2(dirfd, path, flags, RESOLVE)`.
pub(crate) fn open_beneath(
    dirfd: BorrowedFd<'_>,
    path: &[u8],
    flags: OFlags,
) -> std::result::Result<OwnedFd, Errno> {
    rustix::fs::openat2(dirfd, path, flags, Mode::empty(), RESOLVE)
}

/// Maps the errnos that mean "the path changed under us" to
/// [`Error::Unstable`], everything else to [`Error::Io`].
pub(crate) fn path_err(e: Errno, p: &RelPath, what: &str) -> Error {
    match unstable_reason(e) {
        Some(reason) => Error::Unstable {
            path: p.as_bytes().to_vec(),
            reason,
        },
        None => Error::io(format!("{what} {p}"), e.into()),
    }
}

pub(crate) fn unstable_reason(e: Errno) -> Option<&'static str> {
    match e {
        Errno::LOOP => Some("symlink in path"),
        Errno::XDEV => Some("path crosses a mount point or leaves the root"),
        // openat2 could not rule out a concurrent rename escaping the root.
        Errno::AGAIN => Some("concurrent rename during resolution"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;

    fn rp(s: &str) -> RelPath {
        RelPath::new(s).unwrap()
    }

    #[test]
    fn relpath_accepts_normal_paths() {
        for ok in [
            "",
            "a",
            "a/b",
            "a/b/c",
            ".hidden",
            "..x",
            "x..",
            "a/.~fsync.1",
            "a b",
        ] {
            RelPath::new(ok).unwrap_or_else(|e| panic!("{ok:?}: {e}"));
        }
        // Non-UTF-8 bytes are fine.
        let p = RelPath::new(&b"d\xff/f\x80"[..]).unwrap();
        assert_eq!(p.depth(), 2);
        assert_eq!(p.to_string(), "d\\xff/f\\x80");
    }

    #[test]
    fn relpath_rejects_bad_paths() {
        for bad in [
            "/",
            "/a",
            "/etc/passwd",
            "a/",
            "a//b",
            "//",
            ".",
            "..",
            "./a",
            "a/.",
            "a/./b",
            "../a",
            "a/..",
            "a/../b",
            "a/../../x",
            "a\0b",
        ] {
            assert!(
                matches!(RelPath::new(bad), Err(Error::InvalidPath { .. })),
                "{bad:?} accepted"
            );
        }
    }

    #[test]
    fn relpath_parts() {
        let root = RelPath::root();
        assert!(root.is_root());
        assert_eq!(root.depth(), 0);
        assert_eq!(root.name(), None);
        assert_eq!(root.parent(), None);
        assert_eq!(root.to_string(), ".");

        let a = rp("a");
        assert_eq!(a.depth(), 1);
        assert_eq!(a.name(), Some(&b"a"[..]));
        assert_eq!(a.parent(), Some(RelPath::root()));

        let abc = rp("a/b/c");
        assert_eq!(abc.depth(), 3);
        assert_eq!(abc.name(), Some(&b"c"[..]));
        assert_eq!(abc.parent(), Some(rp("a/b")));
        assert_eq!(abc.components().collect::<Vec<_>>(), [b"a", b"b", b"c"]);

        assert_eq!(root.join("a").unwrap(), a);
        assert_eq!(a.join("b").unwrap().join("c").unwrap(), abc);
        assert_eq!(a.join("b/c").unwrap(), abc);
        for bad in ["", "..", ".", "/x", "x/", "b/../c"] {
            assert!(a.join(bad).is_err(), "join {bad:?} accepted");
        }
    }

    #[test]
    fn relpath_serde_validates() {
        let p = rp("a/b");
        let bytes = postcard::to_stdvec(&p).unwrap();
        assert_eq!(postcard::from_bytes::<RelPath>(&bytes).unwrap(), p);
        let bad = postcard::to_stdvec(&b"a/../b".to_vec()).unwrap();
        assert!(postcard::from_bytes::<RelPath>(&bad).is_err());
    }

    #[test]
    fn resolve_parent_normal() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("a/b")).unwrap();
        fs::write(dir.path().join("a/b/f"), b"x").unwrap();
        let root = Root::open(dir.path()).unwrap();

        let p = rp("a/b/f");
        let (parent, name) = root.resolve_parent(&p).unwrap();
        assert_eq!(name, b"f");
        let parent_fp = Fingerprint::of_fd(parent.as_fd()).unwrap();
        assert_eq!(parent_fp, root.stat(&rp("a/b")).unwrap());

        // Depth 1: the parent is the root.
        let a = rp("a");
        let (parent, name) = root.resolve_parent(&a).unwrap();
        assert_eq!(name, b"a");
        assert!(
            Fingerprint::of_fd(parent.as_fd())
                .unwrap()
                .same_file(&root.stat(&RelPath::root()).unwrap())
        );

        assert!(matches!(
            root.resolve_parent(&RelPath::root()),
            Err(Error::InvalidPath { .. })
        ));
    }

    #[test]
    fn symlinked_intermediate_dir_is_unstable() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("secret"), b"s").unwrap();
        fs::create_dir(dir.path().join("real")).unwrap();
        fs::write(dir.path().join("real/f"), b"x").unwrap();
        // One link inside the tree, one leading out of it.
        symlink("real", dir.path().join("inside")).unwrap();
        symlink(outside.path(), dir.path().join("out")).unwrap();
        fs::create_dir(dir.path().join("real/sub")).unwrap();
        let root = Root::open(dir.path()).unwrap();

        for p in ["inside/f", "out/secret", "inside/sub/x", "out/x/y"] {
            let p = rp(p);
            match root.resolve_parent(&p) {
                Err(Error::Unstable { path, .. }) => assert_eq!(path, p.as_bytes()),
                other => panic!("{p}: expected Unstable, got {other:?}"),
            }
            assert!(matches!(
                root.read_dir(&p.parent().unwrap()),
                Err(Error::Unstable { .. })
            ));
            assert!(matches!(root.stat(&p), Err(Error::Unstable { .. })));
        }
        // The links themselves are fine as final components: stat sees the link.
        assert_eq!(root.stat(&rp("inside")).unwrap().kind, FileKind::Symlink);
        // Opening a symlink as a directory is unstable too.
        assert!(matches!(
            root.read_dir(&rp("out")),
            Err(Error::Unstable { .. })
        ));
    }

    #[test]
    fn missing_and_non_dir_parents_are_io_errors() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("file"), b"x").unwrap();
        let root = Root::open(dir.path()).unwrap();
        match root.resolve_parent(&rp("nope/x")) {
            Err(Error::Io { source, .. }) => {
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound)
            }
            other => panic!("{other:?}"),
        }
        match root.resolve_parent(&rp("file/x")) {
            Err(Error::Io { source, .. }) => {
                assert_eq!(source.raw_os_error(), Some(libc::ENOTDIR))
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn read_dir_lists_names_and_types() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("d/e")).unwrap();
        fs::write(dir.path().join("d/f"), b"x").unwrap();
        symlink("f", dir.path().join("d/l")).unwrap();
        let odd = OsStr::from_bytes(b"n\xff");
        fs::write(dir.path().join("d").join(odd), b"").unwrap();
        let root = Root::open(dir.path()).unwrap();

        let got: Vec<_> = root
            .read_dir(&rp("d"))
            .unwrap()
            .into_iter()
            .map(|e| (e.name, e.kind))
            .collect();
        assert_eq!(
            got,
            [
                (b"e".to_vec(), Some(FileKind::Dir)),
                (b"f".to_vec(), Some(FileKind::File)),
                (b"l".to_vec(), Some(FileKind::Symlink)),
                (b"n\xff".to_vec(), Some(FileKind::File)),
            ]
        );
        let top: Vec<_> = root
            .read_dir(&RelPath::root())
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(top, [b"d".to_vec()]);
        assert!(root.read_dir(&rp("d/e")).unwrap().is_empty());
        assert!(root.read_dir(&rp("d/f")).is_err());
    }
}
