//! Model-based property test (design §9, T14): random user operations on
//! both replicas of a real [`Pair`], interleaved with sync cycles, checked
//! after every cycle against an in-memory reference model.
//!
//! The model is written from the user's point of view and does not reuse the
//! engine: a path's causal history is the **set** of edit events it has seen
//! (not a version vector), and a sync resolves each path on its own:
//! - one history contains the other: the newer state wins (last writer wins
//!   per causal history), a delete included;
//! - concurrent, same content: merged, the newer mtime kept;
//! - concurrent, a delete vs a live object: the live object wins;
//! - concurrent, otherwise: the winner (Dir > File > Symlink, then the newer
//!   mtime, then the higher replica ID) stays, the loser becomes a conflict
//!   copy next to it;
//! - then, deepest first, a path that is not a directory but has something
//!   live beneath it becomes a directory again (resurrection); a file or
//!   symlink that was there becomes a conflict copy.
//!
//! Like the scanner, the model records an event only when a path differs
//! from what was recorded at the previous sync (§3: kind, content, target;
//! the mtime for files only); edits in between that cancel out are invisible.
//!
//! Conflict-copy names carry the wall-clock time, so the model predicts each
//! copy as (directory, original name, loser's ID7, content) and adopts the
//! real name found on disk. Every other path must match exactly: kind, file
//! content and mtime, symlink target. [`Pair::assert_converged`] checks the
//! rest (both trees and indexes equal, no leftovers, no echo).
//!
//! Every write is unique (content and mtime), and every symlink the user
//! creates or moves gets a fresh mtime, so the model can always tell which
//! version wins a conflict.

mod harness;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write as _;

use files_sync::config::{ReplicaId, SymlinkPolicy};
use files_sync::engine::{Engine, Side};
use harness::{ID_A, ID_B, Mode, Pair, Tree, id7};
use proptest::prelude::*;
use rustix::fs::{AtFlags, CWD, Timespec, Timestamps, utimensat};

/// The first user mtime (seconds); each timestamp handed out is one more.
const EPOCH: i64 = 1_500_000_000;

// --- Operations --------------------------------------------------------------

#[derive(Clone, Debug)]
enum Op {
    /// Writes a new file (replacing a directory or symlink), creating missing
    /// parent directories.
    Write(Side, String),
    /// Appends to an existing file.
    Append(Side, String),
    /// Removes a file, symlink or whole directory tree.
    Delete(Side, String),
    /// Creates a directory (replacing a file or symlink) and its parents.
    Mkdir(Side, String),
    /// Removes an empty directory.
    Rmdir(Side, String),
    /// Creates a symlink (replacing whatever is there) and its parents.
    Symlink(Side, String, &'static str),
    /// `rename(2)` to a path that does not exist, in an existing directory.
    Rename(Side, String, String),
    Sync,
}

/// Targets: relative, nested, escaping, absolute (dangling) and the link's
/// own directory. Under `Links` all are synced verbatim and never followed.
const TARGETS: [&str; 5] = ["a", "b/a", "../a", "/nonexistent/fsync-model", "."];

fn arb_path() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(vec!["a", "b"]), 1..=3).prop_map(|c| c.join("/"))
}

fn arb_op() -> impl Strategy<Value = Op> {
    let side = prop_oneof![Just(Side::A), Just(Side::B)];
    prop_oneof![
        5 => (side.clone(), arb_path()).prop_map(|(s, p)| Op::Write(s, p)),
        2 => (side.clone(), arb_path()).prop_map(|(s, p)| Op::Append(s, p)),
        3 => (side.clone(), arb_path()).prop_map(|(s, p)| Op::Delete(s, p)),
        3 => (side.clone(), arb_path()).prop_map(|(s, p)| Op::Mkdir(s, p)),
        1 => (side.clone(), arb_path()).prop_map(|(s, p)| Op::Rmdir(s, p)),
        2 => (side.clone(), arb_path(), prop::sample::select(TARGETS.to_vec()))
            .prop_map(|(s, p, t)| Op::Symlink(s, p, t)),
        2 => (side, arb_path(), arb_path()).prop_map(|(s, f, t)| Op::Rename(s, f, t)),
        3 => Just(Op::Sync),
    ]
}

// --- Paths -------------------------------------------------------------------

fn is_beneath(q: &str, p: &str) -> bool {
    q.len() > p.len() && q.starts_with(p) && q.as_bytes()[p.len()] == b'/'
}

/// The parent directory (`""` for the root) and the name.
fn split(p: &str) -> (&str, &str) {
    p.rsplit_once('/').unwrap_or(("", p))
}

/// The proper ancestors of `p`, shallowest first.
fn ancestors(p: &str) -> impl Iterator<Item = &str> {
    p.match_indices('/').map(|(i, _)| &p[..i])
}

fn depth(p: &str) -> usize {
    p.matches('/').count()
}

fn side_name(s: Side) -> char {
    match s {
        Side::A => 'A',
        Side::B => 'B',
    }
}

fn replica_id(s: Side) -> ReplicaId {
    match s {
        Side::A => ID_A,
        Side::B => ID_B,
    }
}

// --- The model ---------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
enum Node {
    File {
        content: String,
        mtime: i64,
    },
    Dir,
    /// `mtime` is the one the replica records: the link's own mtime when the
    /// change was scanned, or the peer's when it was synced.
    Link {
        target: String,
        mtime: i64,
    },
}

impl Node {
    /// What the scanner counts as no change (§3).
    fn same_version(&self, o: &Node) -> bool {
        match (self, o) {
            (Node::Link { target: a, .. }, Node::Link { target: b, .. }) => a == b,
            _ => self == o,
        }
    }

    /// Same content for a concurrent change (§6.1): the mtime aside.
    fn same_content(&self, o: &Node) -> bool {
        match (self, o) {
            (Node::File { content: a, .. }, Node::File { content: b, .. }) => a == b,
            _ => self.same_version(o),
        }
    }

    fn rank(&self) -> u8 {
        match self {
            Node::Dir => 2,
            Node::File { .. } => 1,
            Node::Link { .. } => 0,
        }
    }

    fn mtime(&self) -> i64 {
        match self {
            Node::File { mtime, .. } | Node::Link { mtime, .. } => *mtime,
            Node::Dir => 0,
        }
    }

    /// Whether the real object is this node: file content and mtime,
    /// symlink target.
    fn matches(&self, real: &harness::Node) -> bool {
        match (self, real) {
            (
                Node::File { content, mtime },
                harness::Node::File {
                    content: c,
                    mtime_ns,
                    ..
                },
            ) => content == c && mtime * 1_000_000_000 == *mtime_ns,
            (Node::Dir, harness::Node::Dir { .. }) => true,
            (Node::Link { target, .. }, harness::Node::Symlink(t)) => target == t,
            _ => false,
        }
    }
}

/// What one replica has recorded for a path at the last sync.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Rec {
    /// `None`: deleted.
    node: Option<Node>,
    /// Every edit event this state descends from.
    hist: BTreeSet<u32>,
}

#[derive(Default)]
struct ModelTree {
    /// The tree as the user sees it now.
    disk: BTreeMap<String, Node>,
    /// The state recorded at the last sync.
    recs: BTreeMap<String, Rec>,
}

impl ModelTree {
    fn remove_tree(&mut self, p: &str) {
        self.disk.retain(|q, _| q != p && !is_beneath(q, p));
    }

    /// No ancestor of `p` is a file or symlink.
    fn ancestors_ok(&self, p: &str) -> bool {
        ancestors(p).all(|a| matches!(self.disk.get(a), None | Some(Node::Dir)))
    }

    fn make_parents(&mut self, p: &str) {
        for a in ancestors(p) {
            self.disk.entry(a.to_owned()).or_insert(Node::Dir);
        }
    }

    fn has_children(&self, p: &str) -> bool {
        self.disk.keys().any(|q| is_beneath(q, p))
    }
}

/// A conflict copy a sync must create: the loser's version, next to `path`.
#[derive(Debug)]
struct Copy {
    path: String,
    node: Node,
    loser: Side,
}

#[derive(Default)]
struct Model {
    a: ModelTree,
    b: ModelTree,
    events: u32,
}

impl Model {
    fn tree(&mut self, s: Side) -> &mut ModelTree {
        match s {
            Side::A => &mut self.a,
            Side::B => &mut self.b,
        }
    }

    fn event(&mut self) -> u32 {
        self.events += 1;
        self.events
    }

    /// Records every path that differs from its record with a new event.
    fn scan(&mut self, s: Side) {
        let t = self.tree(s);
        let paths: BTreeSet<String> = t
            .disk
            .keys()
            .chain(
                t.recs
                    .iter()
                    .filter(|(_, r)| r.node.is_some())
                    .map(|(p, _)| p),
            )
            .cloned()
            .collect();
        let mut changed = Vec::new();
        for p in paths {
            let now = t.disk.get(&p);
            let was = t.recs.get(&p).and_then(|r| r.node.as_ref());
            let same = match (now, was) {
                (None, None) => true,
                (Some(n), Some(w)) => n.same_version(w),
                _ => false,
            };
            if !same {
                changed.push((p, now.cloned()));
            }
        }
        for (p, node) in changed {
            let e = self.event();
            let t = self.tree(s);
            let rec = t.recs.entry(p).or_insert(Rec {
                node: None,
                hist: BTreeSet::new(),
            });
            rec.node = node;
            rec.hist.insert(e);
        }
    }

    /// Predicts a sync: updates both trees and returns the conflict copies
    /// it creates (not yet in the trees; see [`Model::adopt`]).
    fn sync(&mut self) -> Vec<Copy> {
        self.scan(Side::A);
        self.scan(Side::B);
        let paths: BTreeSet<String> = self
            .a
            .recs
            .keys()
            .chain(self.b.recs.keys())
            .cloned()
            .collect();
        let none = Rec {
            node: None,
            hist: BTreeSet::new(),
        };
        // The resolved state of every path, and the records to write.
        let mut state: BTreeMap<String, Option<Node>> = BTreeMap::new();
        let mut updates: BTreeMap<String, Rec> = BTreeMap::new();
        let mut copies = Vec::new();
        for p in &paths {
            let ra = self.a.recs.get(p).unwrap_or(&none).clone();
            let rb = self.b.recs.get(p).unwrap_or(&none).clone();
            let union: BTreeSet<u32> = ra.hist.union(&rb.hist).copied().collect();
            let rec = match (&ra.node, &rb.node) {
                // Nothing to propagate, whatever the histories.
                (None, None) => None,
                _ if ra.hist == rb.hist => {
                    assert_eq!(
                        ra.node, rb.node,
                        "model: same history, different state at {p}"
                    );
                    None
                }
                _ if ra.hist.is_superset(&rb.hist) => Some(ra.clone()),
                _ if rb.hist.is_superset(&ra.hist) => Some(rb.clone()),
                (Some(x), Some(y)) if x.same_content(y) => Some(Rec {
                    node: Some(if x.mtime() >= y.mtime() { x } else { y }.clone()),
                    hist: union,
                }),
                (Some(x), None) | (None, Some(x)) => Some(Rec {
                    node: Some(x.clone()),
                    hist: self.bumped(&union),
                }),
                (Some(x), Some(y)) => {
                    let a_wins = (x.rank(), x.mtime(), ID_A) >= (y.rank(), y.mtime(), ID_B);
                    let (w, l, loser) = if a_wins {
                        (x, y, Side::B)
                    } else {
                        (y, x, Side::A)
                    };
                    copies.push(Copy {
                        path: p.clone(),
                        node: l.clone(),
                        loser,
                    });
                    Some(Rec {
                        node: Some(w.clone()),
                        hist: self.bumped(&union),
                    })
                }
            };
            let resolved = rec.as_ref().map_or(&ra.node, |r| &r.node).clone();
            state.insert(p.clone(), resolved);
            if let Some(rec) = rec {
                updates.insert(p.clone(), rec);
            }
        }

        // Resurrection, deepest first: a directory stays while anything
        // beneath it is live.
        let mut by_depth: Vec<&String> = paths.iter().collect();
        by_depth.sort_by_key(|p| std::cmp::Reverse(depth(p)));
        for p in by_depth {
            if state[p] == Some(Node::Dir)
                || !state.iter().any(|(q, n)| n.is_some() && is_beneath(q, p))
            {
                continue;
            }
            let ra = self.a.recs.get(p).unwrap_or(&none).clone();
            let rb = self.b.recs.get(p).unwrap_or(&none).clone();
            if let Some(n) = &state[p] {
                // The side that removed the directory loses its object.
                let loser = if ra.node == Some(Node::Dir) {
                    Side::B
                } else {
                    Side::A
                };
                copies.push(Copy {
                    path: p.clone(),
                    node: n.clone(),
                    loser,
                });
            }
            let union: BTreeSet<u32> = ra.hist.union(&rb.hist).copied().collect();
            let rec = Rec {
                node: Some(Node::Dir),
                hist: self.bumped(&union),
            };
            state.insert(p.clone(), Some(Node::Dir));
            updates.insert(p.clone(), rec);
        }

        for t in [&mut self.a, &mut self.b] {
            for (p, rec) in &updates {
                match &rec.node {
                    Some(n) => t.disk.insert(p.clone(), n.clone()),
                    None => t.disk.remove(p),
                };
                t.recs.insert(p.clone(), rec.clone());
            }
        }
        let same = |x: &BTreeMap<String, Node>, y: &BTreeMap<String, Node>| {
            x.len() == y.len()
                && x.iter()
                    .zip(y)
                    .all(|((p, n), (q, m))| p == q && n.same_version(m))
        };
        assert!(
            same(&self.a.disk, &self.b.disk),
            "model: trees differ after a sync"
        );
        for p in self.a.disk.keys() {
            assert!(
                self.a.ancestors_ok(p) && ancestors(p).all(|a| self.a.disk.contains_key(a)),
                "model: {p} has no parent directory"
            );
        }
        copies
    }

    fn bumped(&mut self, hist: &BTreeSet<u32>) -> BTreeSet<u32> {
        let mut h = hist.clone();
        h.insert(self.event());
        h
    }

    /// Matches each predicted copy to a new path on disk, adopts it as a
    /// fresh object on both sides, and checks that nothing else is new.
    fn adopt(
        &mut self,
        copies: Vec<Copy>,
        real: &BTreeMap<String, harness::Node>,
    ) -> Result<(), String> {
        let mut unknown: BTreeMap<&String, &harness::Node> = real
            .iter()
            .filter(|(p, _)| !self.a.disk.contains_key(*p))
            .collect();
        for c in copies {
            let (dir, name) = split(&c.path);
            let prefix = if dir.is_empty() {
                format!("{name}.sync-conflict-")
            } else {
                format!("{dir}/{name}.sync-conflict-")
            };
            let suffix = format!("-{}", id7(replica_id(c.loser)));
            let found = unknown
                .iter()
                .find(|(p, n)| {
                    p.starts_with(&prefix)
                        && p.ends_with(&suffix)
                        && !p[prefix.len()..].contains('/')
                        && c.node.matches(n)
                })
                .map(|(p, _)| (*p).clone());
            let Some(path) = found else {
                return Err(format!(
                    "missing conflict copy {c:?}; new paths: {unknown:#?}"
                ));
            };
            unknown.remove(&path);
            let rec = Rec {
                node: Some(c.node.clone()),
                hist: BTreeSet::from([self.event()]),
            };
            for t in [&mut self.a, &mut self.b] {
                t.disk.insert(path.clone(), c.node.clone());
                t.recs.insert(path.clone(), rec.clone());
            }
        }
        if unknown.is_empty() {
            Ok(())
        } else {
            Err(format!("unexpected paths: {unknown:#?}"))
        }
    }
}

// --- Running a scenario ------------------------------------------------------

struct Run {
    pair: Pair,
    model: Model,
    /// The last timestamp handed out.
    clock: i64,
    /// The last unique write number.
    writes: u32,
}

impl Run {
    fn new(m: Mode) -> Run {
        let mut pair = Pair::new(SymlinkPolicy::Links).over(m);
        // The model applies every deletion: no mass-deletion guard (§6.4).
        pair.engine = Engine::new().max_delete_percent(100);
        Run {
            pair,
            model: Model::default(),
            clock: EPOCH,
            writes: 0,
        }
    }

    fn tick(&mut self) -> i64 {
        self.clock += 1;
        self.clock
    }

    fn unique(&mut self, s: Side) -> String {
        self.writes += 1;
        format!("{}{}", side_name(s), self.writes)
    }

    fn real(&self, s: Side) -> &Tree {
        match s {
            Side::A => &self.pair.a,
            Side::B => &self.pair.b,
        }
    }

    /// Sets a symlink's own mtime (`utimensat` with `AT_SYMLINK_NOFOLLOW`).
    fn touch_link(&self, s: Side, p: &str, secs: i64) {
        let ts = Timespec {
            tv_sec: secs,
            tv_nsec: 0,
        };
        let times = Timestamps {
            last_access: ts,
            last_modification: ts,
        };
        utimensat(CWD, self.real(s).path(p), &times, AtFlags::SYMLINK_NOFOLLOW).unwrap();
    }

    fn step(&mut self, op: &Op) -> Result<(), String> {
        match op {
            Op::Write(s, p) => {
                if !self.model.tree(*s).ancestors_ok(p) {
                    return Ok(());
                }
                let (content, mtime) = (self.unique(*s), self.tick());
                let t = self.model.tree(*s);
                t.remove_tree(p);
                t.make_parents(p);
                let node = Node::File {
                    content: content.clone(),
                    mtime,
                };
                t.disk.insert(p.clone(), node);
                self.real(*s).write_at(p, content, mtime as u64);
            }
            Op::Append(s, p) => {
                let (more, mtime) = (format!("+{}", self.unique(*s)), self.tick());
                let Some(Node::File { content, mtime: m }) = self.model.tree(*s).disk.get_mut(p)
                else {
                    return Ok(());
                };
                content.push_str(&more);
                *m = mtime;
                let real = self.real(*s);
                let mut f = fs::File::options().append(true).open(real.path(p)).unwrap();
                f.write_all(more.as_bytes()).unwrap();
                drop(f);
                real.set_mtime(p, mtime as u64);
            }
            Op::Delete(s, p) => {
                let t = self.model.tree(*s);
                if t.disk.contains_key(p) {
                    t.remove_tree(p);
                    self.real(*s).rm(p);
                }
            }
            Op::Mkdir(s, p) => {
                let t = self.model.tree(*s);
                if !t.ancestors_ok(p) || t.disk.get(p) == Some(&Node::Dir) {
                    return Ok(());
                }
                let existed = t.disk.contains_key(p);
                t.remove_tree(p);
                t.make_parents(p);
                t.disk.insert(p.clone(), Node::Dir);
                let real = self.real(*s);
                if existed {
                    real.rm(p);
                }
                real.mkdir(p);
            }
            Op::Rmdir(s, p) => {
                let t = self.model.tree(*s);
                if t.disk.get(p) == Some(&Node::Dir) && !t.has_children(p) {
                    t.disk.remove(p);
                    fs::remove_dir(self.real(*s).path(p)).unwrap();
                }
            }
            Op::Symlink(s, p, target) => {
                if !self.model.tree(*s).ancestors_ok(p) {
                    return Ok(());
                }
                let mtime = self.tick();
                let t = self.model.tree(*s);
                t.remove_tree(p);
                t.make_parents(p);
                let node = Node::Link {
                    target: target.to_string(),
                    mtime,
                };
                t.disk.insert(p.clone(), node);
                self.real(*s).symlink(p, target);
                self.touch_link(*s, p, mtime);
            }
            Op::Rename(s, from, to) => {
                let t = self.model.tree(*s);
                let (dir, _) = split(to);
                let ok = t.disk.contains_key(from)
                    && !t.disk.contains_key(to)
                    && !is_beneath(to, from)
                    && (dir.is_empty() || t.disk.get(dir) == Some(&Node::Dir));
                if !ok {
                    return Ok(());
                }
                let moved: Vec<(String, Node)> = t
                    .disk
                    .iter()
                    .filter(|(q, _)| *q == from || is_beneath(q, from))
                    .map(|(q, n)| (format!("{to}{}", &q[from.len()..]), n.clone()))
                    .collect();
                t.remove_tree(from);
                t.disk.extend(moved.iter().cloned());
                let real = self.real(*s);
                fs::rename(real.path(from), real.path(to)).unwrap();
                // A moved symlink keeps its inode's mtime, which the model
                // does not know for a synced one: the user touches it.
                for (q, n) in moved {
                    if let Node::Link { target, .. } = n {
                        let mtime = self.tick();
                        self.touch_link(*s, &q, mtime);
                        let link = Node::Link { target, mtime };
                        self.model.tree(*s).disk.insert(q, link);
                    }
                }
            }
            Op::Sync => self.sync()?,
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<(), String> {
        let copies = self.model.sync();
        self.pair.sync();
        self.pair.assert_converged();
        let real = self.pair.a.tree(SymlinkPolicy::Links);
        self.model.adopt(copies, &real)?;
        let want = &self.model.a.disk;
        let same = want.len() == real.len()
            && want
                .iter()
                .zip(&real)
                .all(|((p, n), (q, r))| p == q && n.matches(r));
        if same {
            Ok(())
        } else {
            Err(format!(
                "model and real trees differ\nmodel: {want:#?}\nreal: {real:#?}"
            ))
        }
    }
}

/// Runs `ops`, then a final sync, with the replicas reached in `m`.
fn run(m: Mode, ops: &[Op]) -> Result<(), String> {
    let mut r = Run::new(m);
    for op in ops.iter().chain([&Op::Sync]) {
        r.step(op)?;
    }
    Ok(())
}

mod local {
    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn sync_matches_reference_model(ops in prop::collection::vec(arb_op(), 1..40)) {
            run(Mode::Local, &ops).map_err(TestCaseError::fail)?;
        }
    }
}

/// The same over loopback-remote replicas (design §9).
mod remote {
    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn sync_matches_reference_model(ops in prop::collection::vec(arb_op(), 1..40)) {
            run(Mode::Remote, &ops).map_err(TestCaseError::fail)?;
        }
    }
}
