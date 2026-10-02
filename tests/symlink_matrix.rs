//! The symlink policy matrix (design §4.1, §4.3, §9): every policy × every
//! kind of link × direction (A→B, B→A, both changed), each with the
//! expected tree on both sides. Plus the differential test: when `rsync` is
//! installed, each one-directional case is also run as `rsync -a <opts>
//! src/ C/` into an empty `C`, which must equal our destination (contents,
//! modes and file mtimes; reserved names and conflict copies never occur in
//! one-directional cases).
//!
//! Every case runs in its own directory `P` holding the roots `a/`, `b/`,
//! rsync's `c/`, the state directory and `outside/`, so relative links that
//! leave a root (`../outside/…`) and absolute ones resolve alike everywhere.

mod harness;

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use files_sync::config::SymlinkPolicy::{self, *};
use harness::*;

/// Base files' mtime (and the outside referents'): newer than [`OLD`].
const NEW: u64 = 1_500_000_000;
/// The mtime of the file the other side writes concurrently.
const OLD: u64 = 1_000_000_000;

/// The links of the matrix. The source side holds `f` ("F"), `d/x` ("X")
/// and an empty `s/`; `outside/of` ("OF") and `outside/od/ox` ("OX") are
/// outside both roots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    /// `s/l -> ../d`: safe, relative, up one level, to a directory.
    SafeRel,
    /// `l -> ../outside/od`: unsafe, relative, to a directory outside.
    UnsafeRel,
    /// `l -> <P>/outside/of`: absolute, to a file outside.
    Absolute,
    /// `l -> nope`
    Dangling,
    /// `l -> f`
    File,
    /// `l -> d`
    Dir,
    /// `d/loop -> ..`: a directory link to an ancestor (the root).
    Loop,
    /// `l -> /rsyncd-munged/f`: absolute and dangling, in a replica that
    /// does not munge.
    Munged,
}

const CASES: [Case; 8] = [
    Case::SafeRel,
    Case::UnsafeRel,
    Case::Absolute,
    Case::Dangling,
    Case::File,
    Case::Dir,
    Case::Loop,
    Case::Munged,
];

/// What the destination gets for the link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Out {
    /// The symlink, target bytes verbatim.
    Link,
    /// A real copy of what it points to.
    Copy,
    /// Nothing: not synced (ignored, unsafe, dangling or a loop).
    Nothing,
}

impl Case {
    fn link(self) -> &'static str {
        match self {
            Case::SafeRel => "s/l",
            Case::Loop => "d/loop",
            _ => "l",
        }
    }

    fn target(self, p: &Path) -> String {
        match self {
            Case::SafeRel => "../d".into(),
            Case::UnsafeRel => "../outside/od".into(),
            Case::Absolute => format!("{}/outside/of", p.display()),
            Case::Dangling => "nope".into(),
            Case::File => "f".into(),
            Case::Dir => "d".into(),
            Case::Loop => "..".into(),
            Case::Munged => "/rsyncd-munged/f".into(),
        }
    }

    /// What a copy of the referent looks like, relative to the link.
    fn copy(self) -> Vec<(&'static str, &'static str)> {
        match self {
            Case::SafeRel | Case::Dir => vec![("", "D"), ("x", "F:X")],
            Case::UnsafeRel => vec![("", "D"), ("ox", "F:OX")],
            Case::Absolute => vec![("", "F:OF")],
            Case::File => vec![("", "F:F")],
            other => panic!("{other:?} has no referent to copy"),
        }
    }

    /// Whether rsync can be compared with: it recurses through a loop
    /// under a following policy until the path gets too long, where we stop
    /// at `Unmanaged(Loop)` (design §4.5).
    fn rsync_comparable(self, policy: SymlinkPolicy) -> bool {
        !(self == Case::Loop && matches!(policy, CopyLinks | CopyDirlinks))
    }
}

/// The matrix: what the destination gets, per policy and case. Checked
/// against rsync by the differential test.
fn expected(policy: SymlinkPolicy, case: Case) -> Out {
    use Case::*;
    use Out::*;
    let row: [Out; 8] = match policy {
        //            SafeRel  UnsafeRel Absolute Dangling File  Dir   Loop     Munged
        Skip => [
            Nothing, Nothing, Nothing, Nothing, Nothing, Nothing, Nothing, Nothing,
        ],
        Links => [Link, Link, Link, Link, Link, Link, Link, Link],
        CopyLinks => [Copy, Copy, Copy, Nothing, Copy, Copy, Nothing, Nothing],
        CopyUnsafeLinks => [Link, Copy, Copy, Link, Link, Link, Link, Nothing],
        SafeLinks => [Link, Nothing, Nothing, Link, Link, Link, Link, Nothing],
        CopyDirlinks => [Copy, Copy, Link, Link, Link, Copy, Nothing, Link],
    };
    let i = match case {
        SafeRel => 0,
        UnsafeRel => 1,
        Absolute => 2,
        Dangling => 3,
        File => 4,
        Dir => 5,
        Loop => 6,
        Munged => 7,
    };
    row[i]
}

/// rsync's options for `policy` (on top of `-a`).
fn rsync_opts(policy: SymlinkPolicy) -> &'static [&'static str] {
    match policy {
        Skip => &["--no-links"],
        Links => &[],
        CopyLinks => &["--copy-links"],
        CopyUnsafeLinks => &["--copy-unsafe-links"],
        SafeLinks => &["--safe-links"],
        CopyDirlinks => &["--copy-dirlinks"],
    }
}

/// One case's directory `P` with the pair in it.
struct World {
    dir: tempfile::TempDir,
    pair: Pair,
}

impl World {
    fn new(m: Mode, a: impl Into<Opts>, b: impl Into<Opts>) -> World {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        for d in ["a", "b", "state", "outside/od"] {
            fs::create_dir_all(p.join(d)).unwrap();
        }
        fs::write(p.join("outside/of"), "OF").unwrap();
        fs::write(p.join("outside/od/ox"), "OX").unwrap();
        for f in ["outside/of", "outside/od/ox"] {
            set_mtime(&p.join(f), NEW);
        }
        let pair = Pair::open_at_with(&p.join("a"), &p.join("b"), &p.join("state"), a, b).over(m);
        World { dir, pair }
    }

    fn p(&self) -> PathBuf {
        self.dir.path().canonicalize().unwrap()
    }

    fn side(&self, side: Side) -> &Tree {
        match side {
            Side::A => &self.pair.a,
            Side::B => &self.pair.b,
        }
    }

    /// The source side's base tree, synced to the other side.
    fn base(&mut self, side: Side) {
        let t = self.side(side);
        t.write_at("f", "F", NEW);
        t.write_at("d/x", "X", NEW);
        t.mkdir("s");
        self.pair.sync();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    A,
    B,
}

fn set_mtime(path: &Path, secs: u64) {
    let f = fs::File::options().write(true).open(path).unwrap();
    f.set_modified(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(secs))
        .unwrap();
}

/// A tree as `path → "D" | "F:content" | "L:target"`, with conflict-copy
/// names shortened to `name.CONFLICT-<ID7>`.
type View = BTreeMap<String, String>;

fn view(t: &Tree) -> View {
    t.raw()
        .into_iter()
        .map(|(path, node)| {
            let v = match node {
                Node::Dir { .. } => "D".to_owned(),
                Node::File { content, .. } => format!("F:{content}"),
                Node::Symlink(target) => format!("L:{target}"),
            };
            (short_conflict(&path), v)
        })
        .collect()
}

/// `stem.sync-conflict-YYYYMMDD-HHMMSS-<ID7>` → `stem.CONFLICT-<ID7>`.
fn short_conflict(path: &str) -> String {
    const MARK: &str = ".sync-conflict-";
    match path.find(MARK) {
        Some(i) => {
            let rest = &path[i + MARK.len()..];
            format!(
                "{}.CONFLICT-{}",
                &path[..i],
                &rest["YYYYMMDD-HHMMSS-".len()..]
            )
        }
        None => path.to_owned(),
    }
}

/// The base tree, plus `extra`.
fn base_view(extra: &[(String, String)]) -> View {
    let mut v: View = [("d", "D"), ("d/x", "F:X"), ("f", "F:F"), ("s", "D")]
        .into_iter()
        .map(|(p, k)| (p.to_owned(), k.to_owned()))
        .collect();
    v.extend(extra.iter().cloned());
    v
}

/// The entries the destination gets at `at` for the source's link.
fn received(case: Case, out: Out, at: &str, target: &str) -> Vec<(String, String)> {
    match out {
        Out::Link => vec![(at.to_owned(), format!("L:{target}"))],
        Out::Copy => case
            .copy()
            .into_iter()
            .map(|(rel, k)| {
                let p = if rel.is_empty() {
                    at.to_owned()
                } else {
                    format!("{at}/{rel}")
                };
                (p, k.to_owned())
            })
            .collect(),
        Out::Nothing => vec![],
    }
}

/// `Some(path to rsync)`, or `None` (reported once) if it is not installed.
fn rsync() -> Option<&'static Path> {
    static RSYNC: OnceLock<Option<PathBuf>> = OnceLock::new();
    RSYNC
        .get_or_init(|| {
            let found = std::env::var_os("PATH").and_then(|paths| {
                std::env::split_paths(&paths)
                    .map(|d| d.join("rsync"))
                    .find(|p| p.is_file())
            });
            if found.is_none() {
                eprintln!("rsync not found in PATH: skipping the rsync differential checks");
            }
            found
        })
        .as_deref()
}

/// Runs `rsync -a <opts> src/ <P>/c/` into a fresh `c`, and returns `c`'s
/// tree, or `None` without rsync.
fn rsync_into_c(w: &World, src: Side, opts: &[&str]) -> Option<BTreeMap<String, Node>> {
    let rsync = rsync()?;
    let c = w.p().join("c");
    let _ = fs::remove_dir_all(&c);
    let src = w.side(src).root().to_owned();
    let out = Command::new(rsync)
        .arg("-a")
        .args(opts)
        .arg(format!("{}/", src.display()))
        .arg(format!("{}/", c.display()))
        .output()
        .unwrap();
    // 23: partial transfer (e.g. "symlink has no referent"), as expected.
    assert!(
        matches!(out.status.code(), Some(0 | 23)),
        "rsync {opts:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Some(raw_tree(&c))
}

/// The link is made on `from`; one sync must give the other side the
/// matrix's result, and rsync must agree.
fn one_way(m: Mode, policy: SymlinkPolicy, case: Case, from: Side) {
    let ctx = format!("{policy:?} {case:?} from {from:?}");
    let mut w = World::new(m, policy, policy);
    w.base(from);
    let to = if from == Side::A { Side::B } else { Side::A };
    let target = case.target(&w.p());
    w.side(from).symlink(case.link(), &target);
    let report = w.pair.sync();

    let link = vec![(case.link().to_owned(), format!("L:{target}"))];
    let got = received(case, expected(policy, case), case.link(), &target);
    assert_eq!(view(w.side(from)), base_view(&link), "{ctx}: source");
    assert_eq!(view(w.side(to)), base_view(&got), "{ctx}: destination");
    assert!(report.conflicts.is_empty(), "{ctx}: {report:?}");
    w.pair.assert_converged();

    if case.rsync_comparable(policy)
        && let Some(c) = rsync_into_c(&w, from, rsync_opts(policy))
    {
        assert_eq!(
            w.side(to).raw(),
            c,
            "{ctx}: differs from rsync (left: ours, right: rsync)"
        );
    }
}

/// A makes the link while B writes an older plain file at the same path.
fn both_changed(m: Mode, policy: SymlinkPolicy, case: Case) {
    let ctx = format!("{policy:?} {case:?} both changed");
    let mut w = World::new(m, policy, policy);
    w.base(Side::A);
    let (at, target) = (case.link(), case.target(&w.p()));
    w.pair.a.symlink(at, &target);
    w.pair.b.write_at(at, "B", OLD);
    w.pair.sync();

    let link = (at.to_owned(), format!("L:{target}"));
    let theirs = (at.to_owned(), "F:B".to_owned());
    let copy_of = |id, k: &str| (format!("{at}.CONFLICT-{}", id7(id)), k.to_owned());
    let out = expected(policy, case);
    let (a, b) = match out {
        // Not synced: each side keeps its own.
        Out::Nothing => (base_view(&[link]), base_view(&[theirs])),
        // File beats symlink (§6.2): A's link becomes the conflict copy.
        Out::Link => {
            let both = base_view(&[theirs, copy_of(ID_A, &link.1)]);
            (both.clone(), both)
        }
        // A's copy is newer, or a directory: B's file becomes the copy. A
        // keeps its link; B gets the real thing.
        Out::Copy => {
            let copy = copy_of(ID_B, "F:B");
            let mut b = received(case, out, at, &target);
            b.push(copy.clone());
            (base_view(&[link, copy]), base_view(&b))
        }
    };
    assert_eq!(view(&w.pair.a), a, "{ctx}: A");
    assert_eq!(view(&w.pair.b), b, "{ctx}: B");
    if out != Out::Nothing {
        w.pair.assert_converged();
    }
}

fn matrix(m: Mode, policy: SymlinkPolicy) {
    for case in CASES {
        one_way(m, policy, case, Side::A);
        one_way(m, policy, case, Side::B);
        both_changed(m, policy, case);
    }
}

fn matrix_skip(m: Mode) {
    matrix(m, Skip);
}

fn matrix_links(m: Mode) {
    matrix(m, Links);
}

fn matrix_copy_links(m: Mode) {
    matrix(m, CopyLinks);
}

fn matrix_copy_unsafe_links(m: Mode) {
    matrix(m, CopyUnsafeLinks);
}

fn matrix_safe_links(m: Mode) {
    matrix(m, SafeLinks);
}

fn matrix_copy_dirlinks(m: Mode) {
    matrix(m, CopyDirlinks);
}

/// `--munge-links` on the sending replica: what it stores munged arrives
/// unmunged, and what the user made unmunged arrives as it is. rsync's
/// sender unmunges the same way.
fn munged_sender(m: Mode) {
    for policy in [Links, SafeLinks, CopyUnsafeLinks] {
        let munged = Opts {
            policy,
            munge_links: true,
            ..Opts::default()
        };
        let mut w = World::new(m, munged, policy);
        w.base(Side::A);
        let a = &w.pair.a;
        symlink("/rsyncd-munged/f", a.path("m")).unwrap();
        symlink("/rsyncd-munged//rsyncd-munged/x", a.path("mm")).unwrap();
        symlink("f", a.path("plain")).unwrap();
        w.pair.sync();
        let b = view(&w.pair.b);
        assert_eq!(b.get("m").map(String::as_str), Some("L:f"), "{policy:?}");
        assert_eq!(
            b.get("plain").map(String::as_str),
            Some("L:f"),
            "{policy:?}"
        );
        // Unmunged once: absolute, so unsafe. `--copy-unsafe-links`
        // follows what is on disk, which leads nowhere.
        let mm = b.get("mm").map(String::as_str);
        match policy {
            Links => assert_eq!(mm, Some("L:/rsyncd-munged/x"), "{policy:?}"),
            _ => assert_eq!(mm, None, "{policy:?}"),
        }
        w.pair.assert_converged();
        // rsync's `--copy-unsafe-links` judges the munged (absolute) target
        // and follows it; we classify the canonical one (design §4.2).
        if policy != CopyUnsafeLinks
            && let Some(c) = rsync_into_c(
                &w,
                Side::A,
                &[rsync_opts(policy), &["--munge-links"]].concat(),
            )
        {
            assert_eq!(w.pair.b.raw(), c, "{policy:?}: differs from rsync");
        }

        // Back from B: munged on A.
        w.pair.b.symlink("s/n", "../d/x");
        w.pair.sync();
        let n = fs::read_link(w.pair.a.path("s/n")).unwrap();
        assert_eq!(n, Path::new("/rsyncd-munged/../d/x"), "{policy:?}");
        w.pair.assert_converged();
    }
}

// ---------------------------------------------------------------------------
// Incoming changes to followed links (design §4.3)
// ---------------------------------------------------------------------------

/// `-L`: a change from the peer replaces the followed link by a real object,
/// as rsync's receiver does; a change beneath a followed directory first
/// turns it into a real copy. What the links pointed to is never written.
fn copy_links_write_back(m: Mode) {
    let mut w = World::new(m, CopyLinks, CopyLinks);
    let a = &w.pair.a;
    a.write_at("f", "F", NEW);
    a.write_at("d/x", "X", NEW);
    a.write_at("d/e/y", "Y", NEW);
    a.symlink("lf", "f");
    a.symlink("l", "d");
    a.symlink("m", "d");
    w.pair.sync();
    assert!(w.pair.b.is_dir("l") && w.pair.b.is_file("lf"));

    let b = &w.pair.b;
    b.write_at("lf", "B lf", NEW + 1);
    b.write_at("l/x", "B x", NEW + 1);
    b.rm("m");
    w.pair.sync();
    let a = &w.pair.a;
    assert!(a.is_file("lf") && a.is_dir("l"), "{:?}", a.ls());
    assert_eq!(a.read("lf"), "B lf");
    assert_eq!(a.read("l/x"), "B x");
    assert_eq!(a.read("l/e/y"), "Y");
    assert!(!a.exists("m"));
    // Untouched: what the links pointed to.
    assert_eq!(
        (a.read("f"), a.read("d/x"), a.read("d/e/y")),
        ("F".into(), "X".into(), "Y".into())
    );
    w.pair.assert_converged();
}

/// `followed_write = conflict`: the links stay; incoming versions become
/// conflict copies beside them, and the local version goes back.
fn followed_write_conflict_keeps_links(m: Mode) {
    let opts = Opts {
        policy: CopyLinks,
        followed_write: files_sync::config::FollowedWrite::Conflict,
        ..Opts::default()
    };
    let mut w = World::new(m, opts, opts);
    let a = &w.pair.a;
    a.write_at("f", "F", NEW);
    a.write_at("d/x", "X", NEW);
    a.symlink("lf", "f");
    a.symlink("l", "d");
    w.pair.sync();

    let b = &w.pair.b;
    b.write_at("lf", "B lf", NEW + 1);
    b.write_at("l/x", "B x", NEW + 1);
    let report = w.pair.sync();
    assert!(report.rounds >= 2, "{report:?}");
    let a = view(&w.pair.a);
    let b7 = id7(ID_B);
    for (path, want) in [
        ("lf", "L:f".to_owned()),
        ("l", "L:d".to_owned()),
        ("f", "F:F".to_owned()),
        ("d/x", "F:X".to_owned()),
        (&format!("lf.CONFLICT-{b7}"), "F:B lf".to_owned()),
        // `x` lies beneath the followed `l`: its copy goes beside `l`.
        (&format!("x.CONFLICT-{b7}"), "F:B x".to_owned()),
    ] {
        assert_eq!(a.get(path), Some(&want), "A: {path} in {a:?}");
    }
    let b = view(&w.pair.b);
    assert_eq!(b.get("lf").map(String::as_str), Some("F:F"));
    assert_eq!(b.get("l/x").map(String::as_str), Some("F:X"));
    w.pair.assert_converged();
}

/// `-K`: B's link to a directory, where A has a real directory, is adopted;
/// A's files are written through it. rsync `-K` agrees.
fn keep_dirlinks_adopts_and_writes_through(m: Mode) {
    let kept = Opts {
        keep_dirlinks: true,
        ..Opts::default()
    };
    let mut w = World::new(m, Links, kept);
    w.pair.a.mkdir("real");
    w.pair.sync();
    w.pair.b.symlink("k", "real");
    w.pair.a.write_at("k/y", "Y", NEW);
    w.pair.a.write_at("k/sub/z", "Z", NEW);
    let report = w.pair.sync();
    assert!(report.conflicts.is_empty(), "{report:?}");
    assert!(w.pair.b.adopted("k"));
    let b = view(&w.pair.b);
    assert_eq!(b.get("k").map(String::as_str), Some("L:real"));
    assert_eq!(b.get("real/y").map(String::as_str), Some("F:Y"));
    assert_eq!(b.get("real/sub/z").map(String::as_str), Some("F:Z"));
    if let Some(rsync) = rsync() {
        let c = w.p().join("c");
        let _ = fs::remove_dir_all(&c);
        fs::create_dir_all(c.join("real")).unwrap();
        symlink("real", c.join("k")).unwrap();
        let st = Command::new(rsync)
            .args(["-a", "-K"])
            .arg(format!("{}/", w.pair.a.root().display()))
            .arg(format!("{}/", c.display()))
            .status()
            .unwrap();
        assert!(st.success());
        assert_eq!(w.pair.b.raw(), raw_tree(&c), "differs from rsync -K");
    }
    // What arrived through `k` is in `real` too, which syncs as itself.
    w.pair.sync();
    assert_eq!(w.pair.a.read("real/y"), "Y");
    w.pair.assert_converged();

    // Changes both ways, and a delete through the link.
    w.pair.b.write_at("real/y", "Y2", NEW + 5);
    w.pair.a.rm("k/sub");
    w.pair.sync();
    w.pair.sync();
    assert_eq!(w.pair.a.read("k/y"), "Y2");
    assert!(!w.pair.b.exists("real/sub"));
    assert!(w.pair.b.is_symlink("k"));
    w.pair.assert_converged();
}

/// `-K` with a directory outside the root: adopted only with
/// `--keep-dirlinks-unsafe`; otherwise A's directory wins the type conflict.
fn keep_dirlinks_outside_the_root(m: Mode) {
    for unsafe_ok in [false, true] {
        let kept = Opts {
            keep_dirlinks: true,
            keep_dirlinks_unsafe: unsafe_ok,
            ..Opts::default()
        };
        let mut w = World::new(m, Links, kept);
        w.pair.b.symlink("k", "../outside/od");
        w.pair.a.write_at("k/y", "Y", NEW);
        w.pair.sync();
        let od = w.p().join("outside/od");
        if unsafe_ok {
            assert!(w.pair.b.adopted("k"));
            assert_eq!(fs::read(od.join("y")).unwrap(), b"Y");
            assert!(w.pair.b.is_symlink("k"));
            // The outside directory's file arrives on A.
            assert_eq!(w.pair.a.read("k/ox"), "OX");
        } else {
            assert!(!od.join("y").exists());
            assert!(w.pair.b.is_dir("k"));
            assert_eq!(w.pair.b.conflicts("").len(), 1);
        }
        w.pair.assert_converged();
    }
}

both_modes! {
    matrix_skip,
    matrix_links,
    matrix_copy_links,
    matrix_copy_unsafe_links,
    matrix_safe_links,
    matrix_copy_dirlinks,
    munged_sender,
    copy_links_write_back,
    followed_write_conflict_keeps_links,
    keep_dirlinks_adopts_and_writes_through,
    keep_dirlinks_outside_the_root,
}
