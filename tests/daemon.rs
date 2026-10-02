//! The daemon on two real tempdir replicas, driven by inotify (T16).
//!
//! Each test runs `Daemon::run` on a thread while the test edits the trees
//! with plain `std::fs`, then stops it and checks `assert_converged`.

mod harness;

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crossbeam_channel::Receiver;
use files_sync::config::SymlinkPolicy;
use files_sync::daemon::{CycleReport, Daemon, DaemonStats};
use files_sync::fs::is_reserved;
use files_sync::watch::{ChannelSource, Event};
use harness::Pair;

/// What a test sees while the daemon runs.
struct Ctl {
    a: PathBuf,
    b: PathBuf,
    reports: Receiver<CycleReport>,
}

impl Ctl {
    /// Waits for the next cycle report.
    fn next_cycle(&self, timeout: Duration) -> CycleReport {
        self.reports
            .recv_timeout(timeout)
            .unwrap_or_else(|_| panic!("no sync cycle within {timeout:?}"))
    }

    /// The cycle reports that arrive within `d`.
    fn cycles_for(&self, d: Duration) -> Vec<CycleReport> {
        let end = Instant::now() + d;
        let mut out = Vec::new();
        while let Ok(r) = self
            .reports
            .recv_timeout(end.saturating_duration_since(Instant::now()))
        {
            out.push(r);
        }
        out
    }

    /// Waits until no cycle has run for `quiet`, returning the cycles seen.
    fn settle(&self, quiet: Duration) -> Vec<CycleReport> {
        let mut out = Vec::new();
        while let Ok(r) = self.reports.recv_timeout(quiet) {
            out.push(r);
        }
        out
    }
}

/// Runs the daemon on `p` while `f` runs; returns `f`'s result and the
/// daemon's totals. The daemon stops when `f` returns or panics.
fn with_daemon<R>(p: &mut Pair, daemon: Daemon, f: impl FnOnce(&Ctl) -> R) -> (R, DaemonStats) {
    let (tx, reports) = crossbeam_channel::unbounded();
    let ctl = Ctl {
        a: p.a.root().to_path_buf(),
        b: p.b.root().to_path_buf(),
        reports,
    };
    let daemon = daemon.reports(tx);
    let (ra, rb) = (&mut p.a.replica, &mut p.b.replica);
    std::thread::scope(|s| {
        // Dropped on return or unwind, which stops the daemon.
        let (stop, stopped) = crossbeam_channel::bounded::<()>(1);
        let running = s.spawn(move || daemon.run(ra, rb, &stopped));
        let out = f(&ctl);
        drop(stop);
        let stats = running.join().unwrap().expect("daemon failed");
        (out, stats)
    })
}

/// Polls `cond` until it holds; panics after `timeout`. Returns the time it
/// took.
fn wait_until(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) -> Duration {
    let t0 = Instant::now();
    loop {
        if cond() {
            return t0.elapsed();
        }
        assert!(t0.elapsed() < timeout, "{what}: not within {timeout:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The tree under `root` as path → content (`/` for a directory), ignoring
/// reserved names. Tolerates a tree that changes while it is read (an object
/// that vanishes is left out).
fn snapshot(root: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, rel: &Path, out: &mut BTreeMap<String, String>) {
        let Ok(dir) = fs::read_dir(root.join(rel)) else {
            return;
        };
        for e in dir.flatten() {
            if is_reserved(e.file_name().as_bytes()) {
                continue;
            }
            let rel = rel.join(e.file_name());
            let key = rel.to_str().unwrap().to_owned();
            let Ok(md) = e.path().symlink_metadata() else {
                continue;
            };
            if md.is_dir() {
                out.insert(key, "/".into());
                walk(root, &rel, out);
            } else if let Ok(content) = fs::read(e.path()) {
                out.insert(key, String::from_utf8_lossy(&content).into_owned());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, Path::new(""), &mut out);
    out
}

fn read(path: impl AsRef<Path>) -> Option<String> {
    fs::read(path).ok().map(|c| String::from_utf8(c).unwrap())
}

fn applied(cycles: &[CycleReport]) -> usize {
    cycles.iter().map(|c| c.report.applied).sum()
}

const START: Duration = Duration::from_secs(10);

#[test]
fn edit_in_a_appears_in_b_within_3s_without_echo() {
    let mut p = Pair::new(SymlinkPolicy::Links);
    p.a.write("doc.txt", "v1");
    p.a.write("dir/other.txt", "o");
    let ((), stats) = with_daemon(&mut p, Daemon::new(), |c| {
        // The first cycle is a full one and copies the initial tree.
        let first = c.next_cycle(START);
        assert!(first.full);
        assert_eq!(first.report.applied, 3);
        // Our own writes on B cause scoped cycles that do nothing.
        let echo = c.settle(Duration::from_secs(1));
        assert_eq!(applied(&echo), 0, "echo of the first cycle: {echo:#?}");

        fs::write(c.a.join("doc.txt"), "v2").unwrap();
        let took = wait_until("edit reaches B", Duration::from_secs(3), || {
            read(c.b.join("doc.txt")).as_deref() == Some("v2")
        });
        eprintln!("edit reached B after {took:?}");

        // Exactly one action: the write to B. The cycles that follow it
        // (inotify reporting our write on B) apply nothing.
        let after = c.settle(Duration::from_secs(1));
        assert_eq!(applied(&after), 1, "{after:#?}");
        assert!(after.iter().all(|c| !c.full), "{after:#?}");
        assert!(
            after.len() >= 2 && after.last().unwrap().report.applied == 0,
            "expected an echo cycle that applies nothing: {after:#?}"
        );
    });
    assert_eq!(stats.applied, 4);
    assert_eq!(stats.full_cycles, 1);
    p.assert_converged();
}

#[test]
fn burst_of_a_three_level_tree_arrives_in_full() {
    let mut p = Pair::new(SymlinkPolicy::Links);
    with_daemon(&mut p, Daemon::new(), |c| {
        c.next_cycle(START);
        // As fast as possible: directories are created and filled before
        // the watcher can add their watches.
        for i in 0..4 {
            for j in 0..4 {
                for k in 0..4 {
                    let d = c.a.join(format!("d{i}/e{j}/f{k}"));
                    fs::create_dir_all(&d).unwrap();
                    fs::write(d.join("leaf.txt"), format!("{i}{j}{k}")).unwrap();
                }
                fs::write(c.a.join(format!("d{i}/e{j}/mid.txt")), "m").unwrap();
            }
            fs::write(c.a.join(format!("d{i}/top.txt")), "t").unwrap();
        }
        let want = snapshot(&c.a);
        assert_eq!(want.len(), 4 + 16 + 64 + 64 + 16 + 4);
        wait_until("the whole tree reaches B", Duration::from_secs(10), || {
            snapshot(&c.b) == want
        });
        // Later edits deep inside the new tree are watched too (after the
        // echo cycles, whose scans would otherwise pick them up anyway).
        c.settle(Duration::from_secs(1));
        fs::write(c.a.join("d3/e3/f3/late.txt"), "late").unwrap();
        wait_until("an edit in the new tree", Duration::from_secs(3), || {
            read(c.b.join("d3/e3/f3/late.txt")).is_some()
        });
        c.settle(Duration::from_secs(1));
    });
    p.assert_converged();
}

#[test]
fn injected_overflow_triggers_a_full_rescan() {
    let mut p = Pair::new(SymlinkPolicy::Links);
    // A's watcher reads only what the test injects: its edits go unseen.
    let (inject, source) = ChannelSource::new();
    p.a.replica.local_mut().set_event_source(Box::new(source));
    p.a.write("old.txt", "old");
    let ((), stats) = with_daemon(&mut p, Daemon::new(), |c| {
        assert!(c.next_cycle(START).full);
        c.settle(Duration::from_millis(500));

        fs::write(c.a.join("missed.txt"), "missed").unwrap();
        fs::remove_file(c.a.join("old.txt")).unwrap();
        let none = c.cycles_for(Duration::from_secs(1));
        assert!(none.is_empty(), "nothing should notice the edit: {none:#?}");
        assert!(c.b.join("old.txt").exists() && !c.b.join("missed.txt").exists());

        inject.send(Event::Overflow).unwrap();
        let cycle = c.next_cycle(Duration::from_secs(3));
        assert!(cycle.full, "{cycle:#?}");
        assert_eq!(cycle.report.applied, 2, "{cycle:#?}");
        assert_eq!(read(c.b.join("missed.txt")).as_deref(), Some("missed"));
        assert!(!c.b.join("old.txt").exists());
        c.settle(Duration::from_millis(500));
    });
    assert_eq!(stats.full_cycles, 2);
    p.assert_converged();
}

#[test]
fn periodic_rescan_catches_what_the_watchers_miss() {
    let mut p = Pair::new(SymlinkPolicy::Links);
    let (_quiet_a, source) = ChannelSource::new();
    p.a.replica.local_mut().set_event_source(Box::new(source));
    let (_quiet_b, source) = ChannelSource::new();
    p.b.replica.local_mut().set_event_source(Box::new(source));
    let daemon = Daemon::new().rescan_every(Duration::from_secs(1));
    with_daemon(&mut p, daemon, |c| {
        assert!(c.next_cycle(START).full);
        fs::write(c.b.join("from_b.txt"), "b").unwrap();
        wait_until("the timer's rescan", Duration::from_secs(3), || {
            c.a.join("from_b.txt").exists()
        });
        assert!(c.next_cycle(Duration::from_secs(3)).full);
    });
    p.assert_converged();
}

#[test]
fn quiet_tree_causes_no_actions() {
    let mut p = Pair::new(SymlinkPolicy::Links);
    for i in 0..10 {
        p.a.write(&format!("a{i}/f.txt"), format!("a{i}"));
        p.b.write(&format!("b{i}.txt"), format!("b{i}"));
    }
    with_daemon(&mut p, Daemon::new(), |c| {
        let first = c.next_cycle(START);
        assert_eq!(first.report.applied, 30);
        assert!(first.report.is_converged());
        c.settle(Duration::from_secs(1));
        assert_eq!(snapshot(&c.a), snapshot(&c.b));

        let quiet = c.cycles_for(Duration::from_secs(5));
        assert!(quiet.is_empty(), "cycles in a quiet tree: {quiet:#?}");
    });
    p.assert_converged();
}

#[test]
fn edits_on_both_sides_converge_with_a_conflict_copy() {
    let mut p = Pair::new(SymlinkPolicy::Links);
    p.a.write("shared.txt", "base");
    p.sync();
    with_daemon(&mut p, Daemon::new(), |c| {
        c.next_cycle(START);
        c.settle(Duration::from_millis(500));
        fs::write(c.a.join("shared.txt"), "from a").unwrap();
        fs::write(c.b.join("shared.txt"), "from b, newer").unwrap();
        wait_until("both sides agree", Duration::from_secs(5), || {
            let (a, b) = (snapshot(&c.a), snapshot(&c.b));
            a == b && a.len() == 2
        });
        c.settle(Duration::from_secs(1));
    });
    p.assert_converged();
    assert_eq!(p.a.read("shared.txt"), "from b, newer");
    assert_eq!(p.a.conflicts("").len(), 1);
}

/// `-L`: changes to what followed links point to arrive under the links'
/// paths, from the watcher alone (no periodic rescan): in the tree (the
/// referent's own path is watched too) and outside it (extra watches).
#[test]
fn followed_referents_are_watched() {
    let outside = tempfile::tempdir().unwrap();
    fs::create_dir(outside.path().join("o")).unwrap();
    fs::write(outside.path().join("o/f"), "out v1").unwrap();
    let mut p = Pair::new(SymlinkPolicy::CopyLinks);
    p.a.write("d/x", "in v1");
    p.a.symlink("l", "d");
    p.a.symlink("lo", outside.path().to_str().unwrap());
    let ((), _) = with_daemon(&mut p, Daemon::new(), |c| {
        c.next_cycle(START);
        c.settle(Duration::from_secs(1));
        assert_eq!(read(c.b.join("lo/o/f")).as_deref(), Some("out v1"));

        fs::write(outside.path().join("o/f"), "out v2").unwrap();
        wait_until("outside edit reaches B", Duration::from_secs(3), || {
            read(c.b.join("lo/o/f")).as_deref() == Some("out v2")
        });
        fs::write(c.a.join("d/x"), "in v2").unwrap();
        wait_until("edit through l reaches B", Duration::from_secs(3), || {
            read(c.b.join("l/x")).as_deref() == Some("in v2")
                && read(c.b.join("d/x")).as_deref() == Some("in v2")
        });
        // A new directory outside, then a file in it (watched after the
        // scan that found the directory).
        fs::create_dir(outside.path().join("new")).unwrap();
        wait_until("new outside dir reaches B", Duration::from_secs(3), || {
            c.b.join("lo/new").is_dir()
        });
        c.settle(Duration::from_millis(500));
        fs::write(outside.path().join("new/g"), "g").unwrap();
        wait_until("file in it reaches B", Duration::from_secs(3), || {
            read(c.b.join("lo/new/g")).as_deref() == Some("g")
        });
        c.settle(Duration::from_millis(500));
    });
    p.assert_converged();
}
