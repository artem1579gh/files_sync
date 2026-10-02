//! Scanner integration tests on real tempdirs (T10).

use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::time::Duration;

use files_sync::config::{ReplicaId, SymlinkPolicy};
use files_sync::fs::{RelPath, Root};
use files_sync::index::{Entry, IndexStore, Kind, Ord4, UnmanagedReason};
use files_sync::scan::{ScanStats, Scanner, Scope};

const ME: ReplicaId = ReplicaId(0x1234_5678_9abc_def0);
/// Short racy window so tests can wait it out.
const RACY: Duration = Duration::from_millis(200);

struct Replica {
    dir: tempfile::TempDir,
    _state: tempfile::TempDir,
    root: Root,
    index: IndexStore,
}

impl Replica {
    fn new() -> Replica {
        let dir = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let root = Root::open(dir.path()).unwrap();
        let index = IndexStore::open(&state.path().join("index.redb"), ME).unwrap();
        Replica {
            dir,
            _state: state,
            root,
            index,
        }
    }

    fn path(&self, rel: &str) -> std::path::PathBuf {
        self.dir.path().join(rel)
    }

    fn write(&self, rel: &str, content: &[u8]) {
        fs::write(self.path(rel), content).unwrap();
    }

    fn mkdir(&self, rel: &str) {
        fs::create_dir_all(self.path(rel)).unwrap();
    }

    fn link(&self, rel: &str, target: impl AsRef<Path>) {
        symlink(target, self.path(rel)).unwrap();
    }

    fn scanner(&self, policy: SymlinkPolicy) -> Scanner<'_> {
        Scanner::new(&self.root, &self.index, policy).racy_window(RACY)
    }

    fn scan(&self, policy: SymlinkPolicy) -> ScanStats {
        self.scanner(policy).scan(&Scope::Full).unwrap()
    }

    fn scan_paths(&self, policy: SymlinkPolicy, paths: &[&str]) -> ScanStats {
        let paths = paths.iter().map(|p| rp(p)).collect();
        self.scanner(policy).scan(&Scope::Paths(paths)).unwrap()
    }

    fn get(&self, rel: &str) -> Entry {
        self.index
            .get(&rp(rel))
            .unwrap()
            .unwrap_or_else(|| panic!("{rel} not indexed"))
    }

    fn kind(&self, rel: &str) -> Kind {
        self.get(rel).kind
    }

    /// Every indexed path, sorted.
    fn paths(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .index
            .iter_prefix(&RelPath::root())
            .unwrap()
            .into_iter()
            .map(|(p, _)| p.to_string())
            .collect();
        v.sort();
        v
    }
}

fn rp(s: &str) -> RelPath {
    RelPath::new(s).unwrap()
}

fn file_kind(content: &[u8]) -> Kind {
    Kind::File {
        size: content.len() as u64,
        hash: *blake3::hash(content).as_bytes(),
    }
}

fn link_kind(target: &[u8]) -> Kind {
    Kind::Symlink {
        target: target.to_vec(),
    }
}

/// Lets every file created so far fall out of the racy window.
fn settle() {
    std::thread::sleep(RACY + Duration::from_millis(100));
}

#[test]
fn first_scan_under_links() {
    let r = Replica::new();
    r.mkdir("d/e");
    r.write("top", b"top content");
    r.write("d/f", b"");
    r.write("d/e/g", &vec![7u8; 200_000]);
    fs::set_permissions(r.path("d/f"), fs::Permissions::from_mode(0o4751)).unwrap();
    fs::set_permissions(r.path("d/e"), fs::Permissions::from_mode(0o1750)).unwrap();
    r.link("ln", "d/f");
    r.link("abs", "/etc/passwd");
    r.link("dangling", "nowhere");
    // A non-UTF-8 name with a non-UTF-8 target.
    let odd = std::ffi::OsStr::from_bytes(b"odd\xff");
    symlink(
        std::ffi::OsStr::from_bytes(b"t\xfe"),
        r.dir.path().join(odd),
    )
    .unwrap();
    let fifo = std::ffi::CString::new(r.path("fifo").as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o644) }, 0);
    // Reserved names are ignored, even directories full of stuff.
    r.write(".~fsync.0123456789abcdef", b"temp");
    r.mkdir(".~fsync.old.x/sub");

    let stats = r.scan(SymlinkPolicy::Links);
    assert!(
        stats.dirty.is_empty() && stats.errors.is_empty(),
        "{stats:?}"
    );
    assert_eq!(
        r.paths(),
        [
            "abs", "d", "d/e", "d/e/g", "d/f", "dangling", "fifo", "ln", "odd\\xff", "top"
        ]
    );
    assert_eq!(stats.changed, 10);
    assert_eq!(stats.hashed, 3);

    let top = r.get("top");
    assert_eq!(top.kind, file_kind(b"top content"));
    let meta = fs::symlink_metadata(r.path("top")).unwrap();
    assert_eq!(
        top.mode,
        std::os::unix::fs::MetadataExt::mode(&meta) & 0o777
    );
    assert_eq!(top.mtime_ns, meta_mtime_ns(&meta));
    assert_eq!(top.local.ino, std::os::unix::fs::MetadataExt::ino(&meta));
    assert!(top.local.racy, "just written, so racy");
    assert!(top.local.via_link.is_none());
    assert_eq!(r.kind("d/e/g"), file_kind(&vec![7u8; 200_000]));
    // setuid is stripped, sticky kept.
    assert_eq!(r.get("d/f").mode, 0o751);
    assert_eq!(r.kind("d/e"), Kind::Dir);
    assert_eq!(r.get("d/e").mode, 0o1750);
    assert_eq!(r.kind("ln"), link_kind(b"d/f"));
    assert_eq!(r.kind("abs"), link_kind(b"/etc/passwd"));
    assert_eq!(r.kind("dangling"), link_kind(b"nowhere"));
    assert_eq!(
        r.index
            .get(&RelPath::new(&b"odd\xff"[..]).unwrap())
            .unwrap()
            .unwrap()
            .kind,
        link_kind(b"t\xfe")
    );
    assert_eq!(r.kind("fifo"), Kind::Unmanaged(UnmanagedReason::Special));

    // Each entry has its own counter for this replica, and nothing else.
    let mut counters: Vec<u64> = r
        .index
        .iter_prefix(&RelPath::root())
        .unwrap()
        .iter()
        .map(|(_, e)| {
            assert_eq!(e.vv.iter().count(), 1);
            e.vv.get(ME)
        })
        .collect();
    counters.sort();
    counters.dedup();
    assert_eq!(counters.len(), 10);
}

#[test]
fn first_scan_under_skip() {
    let r = Replica::new();
    r.mkdir("d");
    r.write("d/f", b"x");
    r.link("l", "d/f");
    r.link("dl", "d");
    let stats = r.scan(SymlinkPolicy::Skip);
    assert_eq!(stats.changed, 4);
    assert_eq!(r.kind("d/f"), file_kind(b"x"));
    assert_eq!(r.kind("l"), Kind::Unmanaged(UnmanagedReason::IgnoredLink));
    assert_eq!(r.kind("dl"), Kind::Unmanaged(UnmanagedReason::IgnoredLink));
    // Never followed: nothing beneath the directory link.
    assert_eq!(r.paths(), ["d", "d/f", "dl", "l"]);
}

#[test]
fn modification_bumps_the_vv_once() {
    let r = Replica::new();
    r.mkdir("d");
    r.write("d/f", b"one");
    r.write("g", b"other");
    settle();
    r.scan(SymlinkPolicy::Links);
    let before = r.get("d/f");
    let other = r.get("g");

    r.write("d/f", b"two!");
    let stats = r.scan(SymlinkPolicy::Links);
    assert_eq!(stats.changed, 1, "{stats:?}");
    let after = r.get("d/f");
    assert_eq!(after.kind, file_kind(b"two!"));
    assert_eq!(after.vv.compare(&before.vv), Ord4::Dominates);
    // One bump: above every counter seen so far (the Lamport floor).
    assert_eq!(after.vv.get(ME), r.index.max_counter().unwrap());
    assert!(after.seq > before.seq);
    assert_eq!(r.get("g"), other);

    // Rescans leave it alone (the racy one is rehashed but unchanged).
    settle();
    for _ in 0..2 {
        let stats = r.scan(SymlinkPolicy::Links);
        assert_eq!(stats.changed, 0, "{stats:?}");
        let again = r.get("d/f");
        assert_eq!((again.vv, again.seq), (after.vv.clone(), after.seq));
    }

    // Same content, new mtime: a change (mtime is synced for files).
    let f = fs::File::options().write(true).open(r.path("d/f")).unwrap();
    f.set_modified(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000))
        .unwrap();
    drop(f);
    assert_eq!(r.scan(SymlinkPolicy::Links).changed, 1);
    assert_eq!(r.get("d/f").mtime_ns, 1_000_000 * 1_000_000_000);
    assert_eq!(r.get("d/f").kind, file_kind(b"two!"));

    // chmod: a change. A directory mtime moving with its children is not.
    let d = r.get("d");
    fs::set_permissions(r.path("d/f"), fs::Permissions::from_mode(0o600)).unwrap();
    r.write("d/new", b"n");
    let stats = r.scan(SymlinkPolicy::Links);
    assert_eq!(stats.changed, 2, "{stats:?}");
    assert_eq!(r.get("d/f").mode, 0o600);
    assert_eq!(r.get("d").vv, d.vv);
    assert_eq!(r.get("d").seq, d.seq);

    // A retargeted symlink is a change; a re-created identical one is not.
    r.link("l", "a");
    r.scan(SymlinkPolicy::Links);
    let l = r.get("l");
    fs::remove_file(r.path("l")).unwrap();
    r.link("l", "a");
    assert_eq!(r.scan(SymlinkPolicy::Links).changed, 0);
    assert_eq!(r.get("l").vv, l.vv);
    fs::remove_file(r.path("l")).unwrap();
    r.link("l", "b");
    assert_eq!(r.scan(SymlinkPolicy::Links).changed, 1);
    assert_eq!(r.kind("l"), link_kind(b"b"));
    assert_eq!(r.get("l").vv.compare(&l.vv), Ord4::Dominates);
}

#[test]
fn untouched_rescan_rehashes_only_racy_entries() {
    let r = Replica::new();
    r.mkdir("a/b");
    for i in 0..20 {
        r.write(&format!("a/b/f{i}"), format!("content {i}").as_bytes());
    }
    r.write("top", b"top");
    settle();
    let stats = r.scan(SymlinkPolicy::Links);
    assert_eq!(stats.hashed, 21);
    assert!(
        r.index
            .iter_prefix(&RelPath::root())
            .unwrap()
            .iter()
            .all(|(_, e)| !e.local.racy)
    );
    let snapshot = r.index.iter_prefix(&RelPath::root()).unwrap();

    let stats = r.scan(SymlinkPolicy::Links);
    assert_eq!((stats.hashed, stats.changed), (0, 0), "{stats:?}");
    assert_eq!(stats.scanned, 23);
    assert_eq!(r.index.iter_prefix(&RelPath::root()).unwrap(), snapshot);

    // A file written just before a scan is racy: rehashed by the next scan
    // even though its fingerprint is unchanged, until it is old enough.
    r.write("fresh", b"fresh");
    let stats = r.scan(SymlinkPolicy::Links);
    assert_eq!((stats.hashed, stats.changed), (1, 1));
    assert!(r.get("fresh").local.racy);
    let seq = r.get("fresh").seq;
    settle();
    let stats = r.scan(SymlinkPolicy::Links);
    assert_eq!((stats.hashed, stats.changed), (1, 0));
    // The racy flag is local: cleared in place, same seq, nothing for peers.
    let fresh = r.get("fresh");
    assert!(!fresh.local.racy);
    assert_eq!(fresh.seq, seq);
    assert!(r.index.changes_since(seq).unwrap().is_empty());
    let stats = r.scan(SymlinkPolicy::Links);
    assert_eq!((stats.hashed, stats.changed), (0, 0));
}

#[test]
fn deletion_produces_tombstones() {
    let r = Replica::new();
    r.mkdir("d/sub");
    r.write("d/sub/f", b"f");
    r.write("d/g", b"g");
    r.write("keep", b"k");
    r.link("l", "keep");
    settle();
    r.scan(SymlinkPolicy::Links);
    let before: Vec<(RelPath, Entry)> = r.index.iter_prefix(&RelPath::root()).unwrap();

    fs::remove_dir_all(r.path("d")).unwrap();
    fs::remove_file(r.path("l")).unwrap();
    let stats = r.scan(SymlinkPolicy::Links);
    assert_eq!((stats.changed, stats.tombstoned), (5, 5), "{stats:?}");
    for (p, old) in &before {
        let now = r.index.get(p).unwrap().unwrap();
        if p.as_bytes() == b"keep" {
            assert_eq!(&now, old);
        } else {
            assert_eq!(now.kind, Kind::Tombstone, "{p}");
            assert_eq!(now.vv.compare(&old.vv), Ord4::Dominates, "{p}");
        }
    }

    // Already a tombstone: no further bump.
    let tomb = r.get("d/g");
    let stats = r.scan(SymlinkPolicy::Links);
    assert_eq!(stats.changed, 0);
    assert_eq!(r.get("d/g"), tomb);

    // Re-created: live again, descending from the tombstone.
    r.mkdir("d");
    r.write("d/g", b"g2");
    let stats = r.scan(SymlinkPolicy::Links);
    assert_eq!(stats.changed, 2);
    assert_eq!(r.kind("d/g"), file_kind(b"g2"));
    assert_eq!(r.get("d/g").vv.compare(&tomb.vv), Ord4::Dominates);
    assert_eq!(r.kind("d/sub"), Kind::Tombstone);

    // A file replaced by a directory: one change at the name, children new.
    fs::remove_file(r.path("keep")).unwrap();
    r.mkdir("keep");
    r.write("keep/x", b"x");
    let stats = r.scan(SymlinkPolicy::Links);
    assert_eq!((stats.changed, stats.tombstoned), (2, 0));
    assert_eq!(r.kind("keep"), Kind::Dir);
}

#[test]
fn copy_links_loops() {
    let r = Replica::new();
    r.mkdir("a/b");
    r.write("a/b/f", b"f");
    r.link("a/b/up", "..");
    r.link("self", ".");
    r.link("x", "y");
    r.link("y", "x");
    let stats = r.scan(SymlinkPolicy::CopyLinks);
    assert!(
        stats.dirty.is_empty() && stats.errors.is_empty(),
        "{stats:?}"
    );
    let lp = Kind::Unmanaged(UnmanagedReason::Loop);
    assert_eq!(r.kind("a/b/up"), lp);
    assert_eq!(r.kind("self"), lp);
    // A chain of links that never ends (ELOOP).
    assert_eq!(r.kind("x"), lp);
    assert_eq!(r.kind("y"), lp);
    assert_eq!(r.paths(), ["a", "a/b", "a/b/f", "a/b/up", "self", "x", "y"]);
    // The link's own inode is recorded.
    let ino = std::os::unix::fs::MetadataExt::ino(&fs::symlink_metadata(r.path("self")).unwrap());
    assert_eq!(r.get("self").local.ino, ino);

    // Under `Links` the same links are plain symlinks.
    let r2 = Replica::new();
    r2.mkdir("a");
    r2.link("a/up", "..");
    r2.scan(SymlinkPolicy::Links);
    assert_eq!(r2.kind("a/up"), link_kind(b".."));
}

#[test]
fn copy_links_dangling() {
    let r = Replica::new();
    r.mkdir("d");
    r.link("d/gone", "missing");
    r.link("d/abs_gone", "/nonexistent/path/x");
    r.link("d/through_file", "../f/x");
    r.write("f", b"f");
    let stats = r.scan(SymlinkPolicy::CopyLinks);
    assert!(
        stats.dirty.is_empty() && stats.errors.is_empty(),
        "{stats:?}"
    );
    let dg = Kind::Unmanaged(UnmanagedReason::Dangling);
    assert_eq!(r.kind("d/gone"), dg);
    assert_eq!(r.kind("d/abs_gone"), dg);
    assert_eq!(r.kind("d/through_file"), dg);
    // Not a tombstone, and stable across rescans.
    assert_eq!(r.scan(SymlinkPolicy::CopyLinks).changed, 0);

    // The referent appears: now it is the file.
    r.write("d/missing", b"here");
    let stats = r.scan(SymlinkPolicy::CopyLinks);
    assert_eq!(stats.changed, 2);
    assert_eq!(r.kind("d/gone"), file_kind(b"here"));
}

#[test]
fn copy_links_follows_files_and_dirs() {
    let r = Replica::new();
    let outside = tempfile::tempdir().unwrap();
    fs::create_dir(outside.path().join("od")).unwrap();
    fs::write(outside.path().join("od/x"), b"outside x").unwrap();
    fs::write(outside.path().join("of"), b"outside f").unwrap();
    r.mkdir("real");
    r.write("real/inner", b"inner");
    r.write("file", b"file");
    r.link("lf", "file");
    r.link("ld", "real");
    r.link("out_f", outside.path().join("of"));
    r.link("out_d", outside.path().join("od"));
    // Two links to the same directory (a diamond) are both copied.
    r.link("ld2", "./real/");
    settle();

    let stats = r.scan(SymlinkPolicy::CopyLinks);
    assert!(
        stats.dirty.is_empty() && stats.errors.is_empty(),
        "{stats:?}"
    );
    assert_eq!(
        r.paths(),
        [
            "file",
            "ld",
            "ld/inner",
            "ld2",
            "ld2/inner",
            "lf",
            "out_d",
            "out_d/x",
            "out_f",
            "real",
            "real/inner"
        ]
    );
    let lf = r.get("lf");
    assert_eq!(lf.kind, file_kind(b"file"));
    assert_eq!(lf.local.ino, r.get("file").local.ino);
    let via = lf.local.via_link.as_ref().unwrap();
    assert_eq!(via.raw_target, b"file");
    assert!(!via.out_of_tree);
    assert_eq!(
        via.ino,
        std::os::unix::fs::MetadataExt::ino(&fs::symlink_metadata(r.path("lf")).unwrap())
    );
    assert_eq!(r.kind("ld"), Kind::Dir);
    assert!(!r.get("ld").local.via_link.as_ref().unwrap().out_of_tree);
    assert_eq!(r.kind("ld/inner"), file_kind(b"inner"));
    assert!(r.get("ld/inner").local.via_link.is_none());
    assert_eq!(r.kind("out_f"), file_kind(b"outside f"));
    assert!(r.get("out_f").local.via_link.as_ref().unwrap().out_of_tree);
    assert_eq!(r.kind("out_d"), Kind::Dir);
    assert!(r.get("out_d").local.via_link.as_ref().unwrap().out_of_tree);
    assert_eq!(r.kind("out_d/x"), file_kind(b"outside x"));

    // The rehash shortcut applies to followed files too.
    let stats = r.scan(SymlinkPolicy::CopyLinks);
    assert_eq!((stats.hashed, stats.changed), (0, 0), "{stats:?}");

    // Retargeting a followed link to identical content is local only; to
    // different content it is a change.
    fs::remove_file(r.path("lf")).unwrap();
    r.link("lf", "real/../file");
    let stats = r.scan(SymlinkPolicy::CopyLinks);
    assert_eq!((stats.hashed, stats.changed), (1, 0));
    assert_eq!(
        r.get("lf").local.via_link.as_ref().unwrap().raw_target,
        b"real/../file"
    );
    fs::remove_file(r.path("lf")).unwrap();
    r.link("lf", "real/inner");
    assert_eq!(r.scan(SymlinkPolicy::CopyLinks).changed, 1);
    assert_eq!(r.kind("lf"), file_kind(b"inner"));

    // The referent's content changes: a change at the link's path too.
    fs::write(outside.path().join("of"), b"outside f v2").unwrap();
    let stats = r.scan(SymlinkPolicy::CopyLinks);
    assert_eq!(stats.changed, 1, "{stats:?}");
    assert_eq!(r.kind("out_f"), file_kind(b"outside f v2"));
    // Nothing was written outside.
    assert_eq!(fs::read(outside.path().join("od/x")).unwrap(), b"outside x");
}

#[test]
fn copy_dirlinks_and_unsafe_policies() {
    let r = Replica::new();
    r.mkdir("d");
    r.write("d/f", b"f");
    r.write("f", b"top");
    r.link("dl", "d");
    r.link("fl", "f");
    r.link("d/esc", "../../no-such-fsync-dir");
    r.scan(SymlinkPolicy::CopyDirlinks);
    assert_eq!(r.kind("dl"), Kind::Dir);
    assert_eq!(r.kind("dl/f"), file_kind(b"f"));
    assert_eq!(r.kind("fl"), link_kind(b"f"));
    assert_eq!(r.kind("d/esc"), link_kind(b"../../no-such-fsync-dir"));
    // `dl/esc` is the same link seen through `dl`.
    assert_eq!(r.kind("dl/esc"), link_kind(b"../../no-such-fsync-dir"));

    let r = Replica::new();
    r.mkdir("d");
    r.write("x", b"x");
    r.link("d/safe", "../x");
    r.link("d/unsafe", "../../no-such-fsync-file");
    r.scan(SymlinkPolicy::SafeLinks);
    assert_eq!(r.kind("d/safe"), link_kind(b"../x"));
    assert_eq!(
        r.kind("d/unsafe"),
        Kind::Unmanaged(UnmanagedReason::IgnoredLink)
    );

    // Switching policy reclassifies on the next scan.
    r.scan(SymlinkPolicy::CopyUnsafeLinks);
    assert_eq!(r.kind("d/safe"), link_kind(b"../x"));
    assert_eq!(
        r.kind("d/unsafe"),
        Kind::Unmanaged(UnmanagedReason::Dangling)
    );
}

#[test]
fn munged_targets_are_indexed_canonical() {
    let r = Replica::new();
    r.link("m", "/rsyncd-munged/../x");
    r.link("twice", "/rsyncd-munged//rsyncd-munged/y");
    let stats = r
        .scanner(SymlinkPolicy::Links)
        .munge_links(true)
        .scan(&Scope::Full)
        .unwrap();
    assert_eq!(stats.changed, 2);
    let m = r.get("m");
    assert_eq!(m.kind, link_kind(b"../x"));
    assert_eq!(
        m.local.raw_target.as_deref(),
        Some(&b"/rsyncd-munged/../x"[..])
    );
    assert_eq!(r.kind("twice"), link_kind(b"/rsyncd-munged/y"));
    // Without munging the same link is taken verbatim.
    let r2 = Replica::new();
    r2.link("m", "/rsyncd-munged/../x");
    r2.scan(SymlinkPolicy::Links);
    assert_eq!(r2.kind("m"), link_kind(b"/rsyncd-munged/../x"));
    assert_eq!(r2.get("m").local.raw_target, None);
}

#[test]
fn scoped_scan() {
    let r = Replica::new();
    r.mkdir("a/b");
    r.mkdir("c");
    r.write("a/b/f", b"f");
    r.write("c/g", b"g");
    r.scan(SymlinkPolicy::Links);
    let c_g = r.get("c/g");

    // Changes in and out of scope: only the scoped subtree is updated.
    fs::remove_file(r.path("c/g")).unwrap();
    r.write("a/b/f", b"f2");
    r.write("a/b/new", b"new");
    let stats = r.scan_paths(SymlinkPolicy::Links, &["a/b/f", "a/b/new", "a/b/f"]);
    assert_eq!(stats.changed, 2, "{stats:?}");
    assert_eq!(r.kind("a/b/f"), file_kind(b"f2"));
    assert_eq!(r.kind("a/b/new"), file_kind(b"new"));
    assert_eq!(r.get("c/g"), c_g);

    // A new directory reported only through its child: the ancestors are
    // indexed as well.
    r.mkdir("n1/n2");
    r.write("n1/n2/leaf", b"leaf");
    let stats = r.scan_paths(SymlinkPolicy::Links, &["n1/n2/leaf"]);
    assert_eq!(stats.changed, 3);
    assert_eq!(r.kind("n1"), Kind::Dir);
    assert_eq!(r.kind("n1/n2"), Kind::Dir);

    // A scoped path whose ancestor is gone: the whole missing subtree is
    // tombstoned, from the ancestor down.
    fs::remove_dir_all(r.path("a")).unwrap();
    let stats = r.scan_paths(SymlinkPolicy::Links, &["a/b/f"]);
    assert_eq!(stats.tombstoned, 4, "{stats:?}");
    for p in ["a", "a/b", "a/b/f", "a/b/new"] {
        assert_eq!(r.kind(p), Kind::Tombstone, "{p}");
    }
    assert_eq!(r.get("c/g"), c_g);

    // An ancestor replaced by a symlink: scanned as that symlink.
    r.mkdir("real");
    r.write("real/z", b"z");
    r.scan(SymlinkPolicy::Links);
    fs::rename(r.path("real"), r.path("moved")).unwrap();
    r.link("real", "moved");
    let stats = r.scan_paths(SymlinkPolicy::Links, &["real/z"]);
    assert!(stats.dirty.is_empty(), "{stats:?}");
    assert_eq!(r.kind("real"), link_kind(b"moved"));
    assert_eq!(r.kind("real/z"), Kind::Tombstone);

    // The root as a scoped path is a full scan.
    fs::remove_file(r.path("n1/n2/leaf")).unwrap();
    let stats = r.scan_paths(SymlinkPolicy::Links, &["c", ""]);
    assert_eq!(stats.tombstoned, 1, "{stats:?}");
    assert_eq!(r.kind("n1/n2/leaf"), Kind::Tombstone);
    assert_eq!(r.kind("c/g"), Kind::Tombstone);
    assert_eq!(r.kind("moved/z"), file_kind(b"z"));
}

#[test]
fn unreadable_directory_keeps_its_subtree() {
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipped: root ignores directory permissions");
        return;
    }
    let r = Replica::new();
    r.mkdir("locked/sub");
    r.write("locked/sub/f", b"f");
    r.write("other", b"o");
    r.scan(SymlinkPolicy::Links);
    let f = r.get("locked/sub/f");
    let locked = r.get("locked");
    fs::set_permissions(r.path("locked"), fs::Permissions::from_mode(0o000)).unwrap();
    let stats = r.scan(SymlinkPolicy::Links);
    fs::set_permissions(r.path("locked"), fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(stats.errors, [rp("locked")], "{stats:?}");
    assert_eq!(stats.tombstoned, 0);
    assert_eq!(r.get("locked/sub/f"), f);
    // The directory's own entry is left alone too.
    assert_eq!(r.get("locked"), locked);
}

fn meta_mtime_ns(meta: &fs::Metadata) -> i64 {
    use std::os::unix::fs::MetadataExt;
    meta.mtime() * 1_000_000_000 + meta.mtime_nsec()
}
