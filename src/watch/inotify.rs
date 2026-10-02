//! The inotify [`EventSource`] (design §5.9).
//!
//! - **Adding watches:** each directory is opened beneath the root (via
//!   [`Root::open_dir`], so no symlink is followed), and the watch is added
//!   on `/proc/self/fd/<dirfd>`: the magic link resolves to the inode we
//!   pinned, not to whatever the path names by then.
//! - **Bookkeeping:** watch descriptor → (dev, ino) → path. inotify gives one
//!   watch per inode, so adding a watch for a directory that is already
//!   watched (it moved) only updates its path.
//! - **New directories** (created or moved in): the watch is added first,
//!   then the directory is listed and its subdirectories are watched the same
//!   way, so children created before the watch existed are not missed. The
//!   event itself makes the whole new subtree dirty, and the sync loop scans
//!   it after the watches are in place.
//! - **Moves:** a directory moved away loses the watches of its whole subtree
//!   (they would report under stale paths); if it moved within the tree, the
//!   matching `MOVED_TO` watches it again. `MOVE_SELF` on a directory still
//!   mapped (its parent's `MOVED_FROM` was missed) makes its parent, and so
//!   the whole subtree, dirty. A moved root asks for a full rescan.
//! - **Limits:** `IN_Q_OVERFLOW` becomes [`Event::Overflow`]. If the watch
//!   limit (`fs.inotify.max_user_watches`) is hit, some directories go
//!   unwatched, so the source falls back to polling: an [`Event::Overflow`]
//!   every [`InotifySource::LIMIT_POLL`].
//! - Reserved `.~fsync.` names are ignored. Symlinks are never followed
//!   while watching the tree.
//! - **Followed links** (design §4.5): the replica hands over its followed
//!   links with an fd on each referent ([`EventSource::follow`]). The
//!   referent directory and every directory beneath it (or the referent
//!   file) are watched as **aliases** of the link's path: an event there
//!   also dirties the matching path beneath the link. An in-tree referent
//!   shares its inode's watch with its own path; an out-of-tree one gets a
//!   watch of its own. Aliases are rebuilt whenever the replica sends a new
//!   set (after a scan that changed something); a directory created inside
//!   an out-of-tree referent in between is reported, but its own content is
//!   only seen by the next rescan.

use std::collections::{HashMap, VecDeque};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::time::{Duration, Instant};

use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::fs::inotify::{self, CreateFlags, ReadFlags, WatchFlags};
use rustix::fs::{Dir, FileType, OFlags};
use rustix::io::Errno;

use crate::error::{Error, Result};
use crate::fs::root::open_beneath;
use crate::fs::{Fingerprint, RelPath, Root, is_reserved};
use crate::watch::{Event, EventSource, Followed};

/// The events we watch for on a followed file (design §4.5).
const FILE_MASK: WatchFlags = WatchFlags::MODIFY
    .union(WatchFlags::CLOSE_WRITE)
    .union(WatchFlags::ATTRIB)
    .union(WatchFlags::DELETE_SELF)
    .union(WatchFlags::MOVE_SELF);

/// The events we watch for (design §5.9).
const MASK: WatchFlags = WatchFlags::CREATE
    .union(WatchFlags::DELETE)
    .union(WatchFlags::MODIFY)
    .union(WatchFlags::CLOSE_WRITE)
    .union(WatchFlags::MOVED_FROM)
    .union(WatchFlags::MOVED_TO)
    .union(WatchFlags::ATTRIB)
    .union(WatchFlags::DELETE_SELF)
    .union(WatchFlags::MOVE_SELF)
    .union(WatchFlags::ONLYDIR)
    .union(WatchFlags::EXCL_UNLINK);

type Wd = i32;
type Id = (u64, u64);

/// One inotify instance watching every directory of a replica.
pub struct InotifySource {
    root: Root,
    inotify: OwnedFd,
    buf: Vec<MaybeUninit<u8>>,
    by_wd: HashMap<Wd, Id>,
    by_id: HashMap<Id, (Wd, RelPath)>,
    /// Watches of followed referents, with the paths they stand for.
    aliases: HashMap<Wd, Vec<RelPath>>,
    /// Events produced outside `wait` (e.g. while adding watches).
    pending: Vec<Event>,
    /// Set once the watch limit was hit: when to poll next.
    poll_at: Option<Instant>,
}

/// An event as read, before the buffer is released.
struct Raw {
    wd: Wd,
    mask: ReadFlags,
    name: Option<Vec<u8>>,
}

impl InotifySource {
    /// While the watch limit keeps some directories unwatched, a full rescan
    /// is requested this often.
    pub const LIMIT_POLL: Duration = Duration::from_secs(60);

    /// Watches every directory beneath `root`. Changes from the moment this
    /// returns are reported.
    pub fn new(root: Root) -> Result<InotifySource> {
        let inotify = inotify::init(CreateFlags::CLOEXEC | CreateFlags::NONBLOCK)
            .map_err(|e| Error::io("inotify_init1", e.into()))?;
        let mut source = InotifySource {
            root,
            inotify,
            buf: vec![MaybeUninit::uninit(); 64 * 1024],
            by_wd: HashMap::new(),
            by_id: HashMap::new(),
            aliases: HashMap::new(),
            pending: Vec::new(),
            poll_at: None,
        };
        source.watch_tree(RelPath::root());
        if !source.by_id.contains_key(&source.root_id()?) {
            return Err(Error::io(
                format!("watch replica root {}", source.root.path().display()),
                std::io::Error::other("cannot add an inotify watch on the root"),
            ));
        }
        // The caller scans everything after this anyway.
        source.pending.clear();
        Ok(source)
    }

    /// Number of watches: the tree's directories, plus followed referents
    /// outside it.
    pub fn watches(&self) -> usize {
        self.by_wd.len()
            + self
                .aliases
                .keys()
                .filter(|wd| !self.by_wd.contains_key(wd))
                .count()
    }

    fn root_id(&self) -> Result<Id> {
        let fp = Fingerprint::of_fd(self.root.fd())?;
        Ok((fp.dev, fp.ino))
    }

    /// Watches the directory at `top` and every directory beneath it: each
    /// watch is added before the directory is listed.
    fn watch_tree(&mut self, top: RelPath) {
        let mut todo = VecDeque::from([top]);
        while let Some(path) = todo.pop_front() {
            let dir = match self.root.open_dir(&path) {
                Ok(dir) => dir,
                Err(e) => {
                    // Gone, or replaced by a non-directory: whoever did it
                    // caused an event in the parent.
                    tracing::debug!(%path, error = %e, "cannot watch directory");
                    continue;
                }
            };
            if !self.add_watch(&path, &dir) {
                continue;
            }
            let entries = match Dir::read_from(&dir) {
                Ok(entries) => entries,
                Err(e) => {
                    tracing::debug!(%path, error = %e, "cannot list directory to watch");
                    continue;
                }
            };
            for entry in entries {
                let Ok(entry) = entry else { break };
                let name = entry.file_name().to_bytes();
                if name == b"." || name == b".." || is_reserved(name) {
                    continue;
                }
                if matches!(entry.file_type(), FileType::Directory | FileType::Unknown)
                    && let Ok(child) = path.join(name)
                {
                    todo.push_back(child);
                }
            }
        }
    }

    /// Adds (or refreshes) the watch on the open directory `dir` at `path`.
    fn add_watch(&mut self, path: &RelPath, dir: &OwnedFd) -> bool {
        let proc = format!("/proc/self/fd/{}", dir.as_raw_fd());
        let wd = match inotify::add_watch(&self.inotify, proc.as_str(), MASK) {
            Ok(wd) => wd,
            Err(Errno::NOSPC) => {
                if self.poll_at.is_none() {
                    tracing::warn!(
                        root = %self.root.path().display(),
                        every = ?Self::LIMIT_POLL,
                        "inotify watch limit reached (fs.inotify.max_user_watches): \
                         some directories are not watched; rescanning periodically"
                    );
                    self.poll_at = Some(Instant::now() + Self::LIMIT_POLL);
                    self.pending.push(Event::Overflow);
                }
                return false;
            }
            Err(e) => {
                tracing::debug!(%path, error = %e, "cannot add inotify watch");
                return false;
            }
        };
        let fp = match Fingerprint::of_fd(dir.as_fd()) {
            Ok(fp) => fp,
            Err(e) => {
                tracing::debug!(%path, error = %e, "cannot stat watched directory");
                let _ = inotify::remove_watch(&self.inotify, wd);
                return false;
            }
        };
        let id = (fp.dev, fp.ino);
        if let Some(old) = self.by_wd.insert(wd, id)
            && old != id
        {
            self.by_id.remove(&old);
        }
        self.by_id.insert(id, (wd, path.clone()));
        true
    }

    /// Watches what the followed links point to, as aliases of their paths;
    /// replaces the previous aliases.
    fn follow_links(&mut self, links: Vec<Followed>) {
        // New watches first: a referent watched before keeps its watch
        // (same inode, same wd), so no event is missed in between.
        let old = std::mem::take(&mut self.aliases);
        for link in links {
            if link.dir {
                self.alias_tree(link.path, link.fd);
            } else {
                self.alias(&link.path, &link.fd, FILE_MASK);
            }
        }
        for wd in old.into_keys() {
            // A watch of the tree itself stays.
            if !self.aliases.contains_key(&wd) && !self.by_wd.contains_key(&wd) {
                let _ = inotify::remove_watch(&self.inotify, wd);
            }
        }
    }

    /// Watches the directory open at `fd` and every directory beneath it
    /// (opened without following symlinks) as aliases of `top` and its paths.
    fn alias_tree(&mut self, top: RelPath, fd: OwnedFd) {
        let mut todo = VecDeque::from([(top, fd)]);
        while let Some((path, fd)) = todo.pop_front() {
            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
            let Ok(dir) = open_beneath(fd.as_fd(), b".", flags) else {
                continue;
            };
            if !self.alias(&path, &dir, MASK) {
                continue;
            }
            let Ok(entries) = Dir::read_from(&dir) else {
                continue;
            };
            for entry in entries {
                let Ok(entry) = entry else { break };
                let name = entry.file_name().to_bytes();
                if name == b"." || name == b".." || is_reserved(name) {
                    continue;
                }
                if matches!(entry.file_type(), FileType::Directory | FileType::Unknown)
                    && let Ok(child) = path.join(name)
                    && let Ok(fd) = open_beneath(dir.as_fd(), name, OFlags::PATH | flags)
                {
                    todo.push_back((child, fd));
                }
            }
        }
    }

    /// Adds a watch on what `fd` refers to, as an alias of `path`.
    fn alias(&mut self, path: &RelPath, fd: &OwnedFd, mask: WatchFlags) -> bool {
        let proc = format!("/proc/self/fd/{}", fd.as_raw_fd());
        // Re-adding the watch of a watched directory keeps its wd and mask.
        match inotify::add_watch(&self.inotify, proc.as_str(), mask) {
            Ok(wd) => {
                self.aliases.entry(wd).or_default().push(path.clone());
                true
            }
            Err(e) => {
                tracing::debug!(%path, error = %e, "cannot watch followed referent");
                false
            }
        }
    }

    /// Removes the watches of `top` and everything beneath it.
    fn unwatch_tree(&mut self, top: &RelPath) {
        let gone: Vec<(Id, Wd)> = self
            .by_id
            .iter()
            .filter(|(_, (_, p))| within(p, top))
            .map(|(id, (wd, _))| (*id, *wd))
            .collect();
        for (id, wd) in gone {
            self.by_id.remove(&id);
            self.by_wd.remove(&wd);
            // A referent's watch stays for its aliases.
            if !self.aliases.contains_key(&wd) {
                // EINVAL if the kernel already dropped it (deleted directory).
                let _ = inotify::remove_watch(&self.inotify, wd);
            }
        }
    }

    fn path_of(&self, wd: Wd) -> Option<&RelPath> {
        let id = self.by_wd.get(&wd)?;
        self.by_id.get(id).map(|(_, p)| p)
    }

    /// Translates one inotify event, updating the watches.
    fn handle(&mut self, raw: Raw, out: &mut Vec<Event>) {
        let Raw { wd, mask, name } = raw;
        if mask.contains(ReadFlags::QUEUE_OVERFLOW) {
            tracing::warn!(root = %self.root.path().display(), "inotify queue overflow; rescanning everything");
            out.push(Event::Overflow);
            return;
        }
        if mask.contains(ReadFlags::IGNORED) {
            // The watch is gone (directory deleted, or removed by us).
            if let Some(id) = self.by_wd.remove(&wd)
                && self.by_id.get(&id).is_some_and(|(w, _)| *w == wd)
            {
                self.by_id.remove(&id);
            }
            self.aliases.remove(&wd);
            return;
        }
        let aliases = self.aliases.get(&wd).cloned().unwrap_or_default();
        let name = name.filter(|n| !n.is_empty());
        if name.as_deref().is_some_and(is_reserved) {
            return;
        }
        // The same change, seen through each followed link to this inode.
        for alias in &aliases {
            match &name {
                Some(name) => {
                    if let Ok(child) = alias.join(name) {
                        out.push(Event::Dirty(child));
                    }
                }
                None => out.push(Event::Dirty(alias.clone())),
            }
        }
        let Some(path) = self.path_of(wd).cloned() else {
            return;
        };
        match name {
            Some(name) => {
                let Ok(child) = path.join(&name) else { return };
                if mask.contains(ReadFlags::ISDIR) {
                    if mask.intersects(ReadFlags::MOVED_FROM | ReadFlags::DELETE) {
                        self.unwatch_tree(&child);
                    }
                    if mask.intersects(ReadFlags::CREATE | ReadFlags::MOVED_TO) {
                        self.watch_tree(child.clone());
                    }
                }
                out.push(Event::Dirty(child));
            }
            // An event on the watched directory itself.
            None if mask.contains(ReadFlags::MOVE_SELF) => match path.parent() {
                // Our root fd follows the directory; its content is unknown.
                None => out.push(Event::Overflow),
                Some(parent) => {
                    self.unwatch_tree(&path);
                    out.push(Event::Dirty(parent));
                }
            },
            None => out.push(Event::Dirty(path)),
        }
    }

    /// Reads every queued event (the fd is non-blocking).
    fn read_all(&mut self) -> Result<Vec<Raw>> {
        let mut raws = Vec::new();
        let mut reader = inotify::Reader::new(&self.inotify, &mut self.buf);
        loop {
            match reader.next() {
                Ok(ev) => raws.push(Raw {
                    wd: ev.wd(),
                    mask: ev.events(),
                    name: ev.file_name().map(|n| n.to_bytes().to_vec()),
                }),
                Err(Errno::AGAIN) => return Ok(raws),
                Err(Errno::INTR) => {}
                Err(e) => return Err(Error::io("read inotify events", e.into())),
            }
        }
    }
}

impl EventSource for InotifySource {
    fn follow(&mut self, links: Vec<Followed>) {
        self.follow_links(links);
    }

    fn wait(&mut self, timeout: Duration) -> Result<Vec<Event>> {
        let mut out = std::mem::take(&mut self.pending);
        let now = Instant::now();
        if let Some(at) = self.poll_at.filter(|at| *at <= now) {
            out.push(Event::Overflow);
            self.poll_at = Some(at.max(now) + Self::LIMIT_POLL);
        }
        let timeout = if out.is_empty() {
            timeout
        } else {
            Duration::ZERO
        };
        let ts = Timespec {
            tv_sec: i64::try_from(timeout.as_secs()).unwrap_or(i64::MAX),
            tv_nsec: i64::from(timeout.subsec_nanos()),
        };
        let mut fds = [PollFd::new(&self.inotify, PollFlags::IN)];
        match poll(&mut fds, Some(&ts)) {
            Ok(0) | Err(Errno::INTR) => return Ok(out),
            Ok(_) => {}
            Err(e) => return Err(Error::io("poll inotify", e.into())),
        }
        for raw in self.read_all()? {
            self.handle(raw, &mut out);
        }
        // Watches added while handling may have hit the limit.
        out.append(&mut self.pending);
        Ok(out)
    }
}

/// `p` is `top` or lies beneath it.
fn within(p: &RelPath, top: &RelPath) -> bool {
    top.is_root()
        || p == top
        || p.as_bytes()
            .strip_prefix(top.as_bytes())
            .is_some_and(|rest| rest.first() == Some(&b'/'))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::path::Path;

    use super::*;

    fn rp(s: &str) -> RelPath {
        RelPath::new(s).unwrap()
    }

    fn source(dir: &Path) -> InotifySource {
        InotifySource::new(Root::open(dir).unwrap()).unwrap()
    }

    /// Collects the dirty paths until `want` are all seen (or 5 s pass).
    fn collect(src: &mut InotifySource, want: &[&str]) -> Vec<Event> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut all = Vec::new();
        while Instant::now() < deadline {
            all.extend(src.wait(Duration::from_millis(50)).unwrap());
            if want.iter().all(|w| all.contains(&Event::Dirty(rp(w)))) {
                // Let stragglers of the same burst in.
                all.extend(src.wait(Duration::from_millis(50)).unwrap());
                return all;
            }
        }
        panic!("missing events for {want:?}; got {all:?}");
    }

    fn paths(src: &InotifySource) -> Vec<String> {
        let mut v: Vec<String> = src
            .by_id
            .values()
            .map(|(_, p)| String::from_utf8(p.as_bytes().to_vec()).unwrap())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn watches_existing_tree_and_reports_changes() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        fs::create_dir_all(d.join("a/b")).unwrap();
        fs::write(d.join("a/b/f"), "x").unwrap();
        fs::create_dir(d.join(".~fsync.tmp")).unwrap();
        symlink("a", d.join("link")).unwrap();
        let mut src = source(d);
        // The root, a, a/b; never the reserved directory or the symlink.
        assert_eq!(paths(&src), ["", "a", "a/b"]);
        assert_eq!(src.wait(Duration::ZERO).unwrap(), []);

        fs::write(d.join("a/b/f"), "y").unwrap();
        fs::write(d.join("top"), "t").unwrap();
        fs::write(d.join("a/.~fsync.0123"), "ignored").unwrap();
        let ev = collect(&mut src, &["a/b/f", "top"]);
        assert!(
            !ev.iter()
                .any(|e| matches!(e, Event::Dirty(p) if is_reserved(p.name().unwrap())))
        );

        fs::remove_file(d.join("a/b/f")).unwrap();
        collect(&mut src, &["a/b/f"]);
    }

    /// Followed links' referents report under the links' paths too: in the
    /// tree (sharing the watch), outside it, and a followed file.
    #[test]
    fn followed_referents_are_watched_as_aliases() {
        let dir = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        let d = dir.path();
        fs::create_dir_all(d.join("d/sub")).unwrap();
        fs::create_dir_all(out.path().join("o")).unwrap();
        fs::write(out.path().join("of"), "x").unwrap();
        symlink("d", d.join("l")).unwrap();
        symlink(out.path(), d.join("lo")).unwrap();
        symlink(out.path().join("of"), d.join("lf")).unwrap();
        let mut src = source(d);
        let tree = src.watches();
        let open = |p: &Path| {
            rustix::fs::open(p, OFlags::PATH | OFlags::CLOEXEC, rustix::fs::Mode::empty()).unwrap()
        };
        let follow = |path: &str, target: &Path, dir| Followed {
            path: rp(path),
            fd: open(target),
            dir,
        };
        src.follow(vec![
            follow("l", &d.join("d"), true),
            follow("lo", out.path(), true),
            follow("lf", &out.path().join("of"), false),
        ]);
        // The outside directory, its subdirectory and the file.
        assert_eq!(src.watches(), tree + 3);

        fs::write(d.join("d/sub/f"), "x").unwrap();
        collect(&mut src, &["d/sub/f", "l/sub/f"]);
        fs::write(out.path().join("o/g"), "x").unwrap();
        collect(&mut src, &["lo/o/g"]);
        fs::write(out.path().join("of"), "y").unwrap();
        collect(&mut src, &["lf"]);

        // A new set replaces the old: the outside watches go, the tree's stay.
        src.follow(vec![follow("l", &d.join("d"), true)]);
        assert_eq!(src.watches(), tree);
        fs::write(out.path().join("o/h"), "x").unwrap();
        fs::write(d.join("d/k"), "x").unwrap();
        let ev = collect(&mut src, &["d/k", "l/k"]);
        assert!(!ev.contains(&Event::Dirty(rp("lo/o/h"))), "{ev:?}");
        // The in-tree directory is still watched for itself once unfollowed.
        src.follow(Vec::new());
        fs::write(d.join("d/sub/f2"), "x").unwrap();
        let ev = collect(&mut src, &["d/sub/f2"]);
        assert!(!ev.contains(&Event::Dirty(rp("l/sub/f2"))), "{ev:?}");
    }

    #[test]
    fn new_directories_are_watched_before_they_are_listed() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let mut src = source(d);
        fs::create_dir_all(d.join("n/m/k")).unwrap();
        fs::write(d.join("n/m/k/f"), "x").unwrap();
        collect(&mut src, &["n"]);
        // However fast the tree was made, every level is watched now.
        assert_eq!(paths(&src), ["", "n", "n/m", "n/m/k"]);
        fs::write(d.join("n/m/k/g"), "x").unwrap();
        collect(&mut src, &["n/m/k/g"]);
    }

    #[test]
    fn moved_directories_follow_their_new_path() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        fs::create_dir_all(d.join("a/b")).unwrap();
        fs::create_dir(d.join("dst")).unwrap();
        let out = tempfile::tempdir().unwrap();
        let mut src = source(d);

        fs::rename(d.join("a"), d.join("dst/a2")).unwrap();
        collect(&mut src, &["a", "dst/a2"]);
        assert_eq!(paths(&src), ["", "dst", "dst/a2", "dst/a2/b"]);
        fs::write(d.join("dst/a2/b/f"), "x").unwrap();
        collect(&mut src, &["dst/a2/b/f"]);

        // Moved out of the tree: no longer watched.
        fs::rename(d.join("dst/a2"), out.path().join("gone")).unwrap();
        collect(&mut src, &["dst/a2"]);
        assert_eq!(paths(&src), ["", "dst"]);
        fs::write(out.path().join("gone/b/f"), "y").unwrap();
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(src.wait(Duration::ZERO).unwrap(), []);

        // Deleted: the watch goes away with it.
        fs::remove_dir(d.join("dst")).unwrap();
        collect(&mut src, &["dst"]);
        src.wait(Duration::from_millis(50)).unwrap();
        assert_eq!(paths(&src), [""]);
        assert_eq!(src.watches(), 1);
    }

    #[test]
    fn move_self_without_moved_from_dirties_the_parent() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        fs::create_dir_all(d.join("p/c")).unwrap();
        let mut src = source(d);
        let wd = src.by_id.values().find(|(_, p)| *p == rp("p/c")).unwrap().0;
        let mut out = Vec::new();
        src.handle(
            Raw {
                wd,
                mask: ReadFlags::MOVE_SELF,
                name: None,
            },
            &mut out,
        );
        assert_eq!(out, [Event::Dirty(rp("p"))]);
        assert_eq!(paths(&src), ["", "p"]);

        let root_wd = src.by_id.values().find(|(_, p)| p.is_root()).unwrap().0;
        let mut out = Vec::new();
        src.handle(
            Raw {
                wd: root_wd,
                mask: ReadFlags::MOVE_SELF,
                name: None,
            },
            &mut out,
        );
        assert_eq!(out, [Event::Overflow]);
        src.handle(
            Raw {
                wd: -1,
                mask: ReadFlags::QUEUE_OVERFLOW,
                name: None,
            },
            &mut out,
        );
        assert_eq!(out, [Event::Overflow, Event::Overflow]);
    }

    #[test]
    fn within_is_component_wise() {
        assert!(within(&rp("a/b"), &rp("a")));
        assert!(within(&rp("a"), &rp("a")));
        assert!(!within(&rp("ab"), &rp("a")));
        assert!(within(&rp("x"), &RelPath::root()));
    }
}
