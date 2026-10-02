//! Crash recovery (design §5.8, §9). Needs the `hooks` feature:
//! `cargo test --features hooks --test crash`.
//!
//! Each scenario syncs a base tree, makes a user change, and then runs one
//! sync cycle (and a sweep of both quarantines) in which the receiving side
//! applies the change. That cycle is first run in-process with its hook
//! points traced. Then, for **every position** in the trace (the nth hit of a
//! point), a child process (this test binary, re-run as [`crash_child`])
//! runs the same cycle on a fresh copy of the scenario and calls
//! `std::process::abort()` at exactly that point: no destructors, no
//! cleanup, like a crash. Afterwards the parent checks that:
//!
//! - every reserved (`.~fsync.*`) name left on disk is named by an intent in
//!   the journal of its replica (the journal is always written first);
//! - reopening the replicas (which replays the journals) and sweeping the
//!   quarantines leaves no reserved name at all;
//! - a following sync converges, and the result is the one of the cycle
//!   without a crash: the same tree, conflict copies compared without their
//!   timestamp. So no user data was lost, and nothing was made up.
//!
//! In the late-write variant, the user holds an fd on the receiving side's
//! file and writes through it at the crash point, i.e. into whatever inode
//! the commit has moved where. That write must survive somewhere (at the
//! name or in a conflict copy). Points from the first quarantine sweep on are
//! left out: a sweep that has decided to unlink may lose a write made in that
//! instant (design §5.10).
//!
//! [`crashes_during_recovery`] crashes a second time, during the replay.
//!
//! The child is a re-run of the test binary rather than a `fork()`: the
//! parent runs cases on several threads, and only an exec gives the child a
//! clean process.

mod harness;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use files_sync::config::SymlinkPolicy;
use files_sync::fs::{hooks, is_reserved};
use files_sync::index::IndexStore;
use harness::{ID_A, ID_B, Node, Pair};

/// Tells the test binary it runs as a crash child, and what to do.
const CHILD_ENV: &str = "FSYNC_CRASH_CHILD";
/// The child's exit code when its hook point was never reached.
const NOT_REACHED: i32 = 3;
/// The hook point at which [`crashes_during_recovery`] crashes the replay:
/// after the first intent's names are dealt with, before its record is.
const RECOVERY_POINT: &str = "recover.done";

const T0: u64 = 1_600_000_000;
const T1: u64 = 1_700_000_000;
const T2: u64 = 1_700_000_100;

struct Scenario {
    name: &'static str,
    /// Both replicas' symlink policy.
    policy: SymlinkPolicy,
    /// User edits synced before the crashing cycle.
    base: fn(&Pair),
    /// The user edits the crashing cycle syncs.
    change: fn(&Pair),
    /// Whether it stages files, so it also runs with named temp files.
    files: bool,
    /// A file on B the user keeps open for the late-write variant.
    held: Option<&'static str>,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "create_file",
        base: |p| p.a.mkdir("d"),
        change: |p| p.a.write_at("d/f", "new file", T1),
        files: true,
        held: None,
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "replace_file",
        base: |p| p.a.write_at("f", "old content", T0),
        change: |p| p.a.write_at("f", "new content", T1),
        files: true,
        held: Some("f"),
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "create_symlink",
        base: |_| {},
        change: |p| p.a.symlink("l", "some/target"),
        files: false,
        held: None,
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "replace_symlink",
        base: |p| p.a.symlink("l", "old-target"),
        change: |p| p.a.symlink("l", "new-target"),
        files: false,
        held: None,
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "file_to_symlink",
        base: |p| p.a.write_at("f", "a file", T0),
        change: |p| p.a.symlink("f", "now-a-link"),
        files: false,
        held: None,
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "symlink_to_file",
        base: |p| p.a.symlink("f", "a-link"),
        change: |p| p.a.write_at("f", "now a file", T1),
        files: true,
        held: None,
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "mkdir_with_child",
        base: |_| {},
        change: |p| p.a.write_at("d/e/f", "nested", T1),
        files: true,
        held: None,
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "delete_file",
        base: |p| p.a.write_at("f", "doomed", T0),
        change: |p| p.a.rm("f"),
        files: false,
        held: Some("f"),
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "delete_symlink",
        base: |p| p.a.symlink("l", "t"),
        change: |p| p.a.rm("l"),
        files: false,
        held: None,
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "delete_tree",
        base: |p| {
            p.a.write_at("d/f", "f", T0);
            p.a.write_at("d/e/g", "g", T0);
            p.a.symlink("d/l", "f");
        },
        change: |p| p.a.rm("d"),
        files: false,
        held: Some("d/f"),
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "conflict",
        base: |p| p.a.write_at("f.txt", "base", T0),
        change: |p| {
            p.a.write_at("f.txt", "from a, newer", T2);
            p.b.write_at("f.txt", "from b, older", T1);
        },
        files: true,
        held: Some("f.txt"),
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "chmod_file",
        base: |p| {
            p.a.write_at("f", "same content", T0);
            p.a.chmod("f", 0o644);
        },
        change: |p| p.a.chmod("f", 0o600),
        files: true,
        held: Some("f"),
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "chmod_dir",
        base: |p| {
            p.a.mkdir("d");
            p.a.chmod("d", 0o755);
        },
        change: |p| p.a.chmod("d", 0o700),
        files: false,
        held: None,
    },
    Scenario {
        policy: SymlinkPolicy::Links,
        name: "dir_to_file",
        base: |p| p.a.write_at("d/x", "child", T0),
        change: |p| {
            p.a.rm("d");
            p.a.write_at("d", "now a file", T1);
        },
        files: true,
        held: Some("d/x"),
    },
    // `-L` write-back: A's followed directory becomes a real copy first.
    Scenario {
        policy: SymlinkPolicy::CopyLinks,
        name: "materialize",
        base: |p| {
            p.a.write_at("d/x", "x", T0);
            p.a.write_at("d/e/y", "y", T0);
            p.a.symlink("d/e/l", "../x");
            p.a.symlink("l", "d");
        },
        change: |p| p.b.write_at("l/x", "changed on b", T1),
        files: true,
        held: None,
    },
];

// ---------------------------------------------------------------------------
// The child
// ---------------------------------------------------------------------------

/// What a child process does.
struct ChildSpec {
    dirs: [PathBuf; 3],
    policy: SymlinkPolicy,
    named: bool,
    /// Run the cycle (true), or only open the replicas, i.e. replay.
    cycle: bool,
    point: String,
    nth: usize,
    /// The file on B to write to at the crash point.
    late: Option<String>,
}

/// What the late-write variant writes.
const LATE: &str = " + late write";

impl ChildSpec {
    fn encode(&self) -> String {
        let [a, b, s] = &self.dirs;
        [
            a.to_str().unwrap(),
            b.to_str().unwrap(),
            s.to_str().unwrap(),
            if self.named { "named" } else { "tmpfile" },
            if self.cycle { "cycle" } else { "open" },
            &self.point,
            &self.nth.to_string(),
            self.late.as_deref().unwrap_or(""),
            &format!("{:?}", self.policy),
        ]
        .join("\n")
    }

    fn decode(s: &str) -> ChildSpec {
        let f: Vec<&str> = s.split('\n').collect();
        ChildSpec {
            dirs: [f[0].into(), f[1].into(), f[2].into()],
            named: f[3] == "named",
            cycle: f[4] == "cycle",
            point: f[5].to_owned(),
            nth: f[6].parse().unwrap(),
            late: Some(f[7]).filter(|l| !l.is_empty()).map(str::to_owned),
            policy: [SymlinkPolicy::Links, SymlinkPolicy::CopyLinks]
                .into_iter()
                .find(|p| format!("{p:?}") == f[8])
                .expect("a scenario policy"),
        }
    }

    /// Runs the child; `Ok(true)` if it crashed, `Ok(false)` if the point
    /// was never reached.
    fn run(&self) -> Result<bool, String> {
        let out = Command::new(std::env::current_exe().unwrap())
            .args(["crash_child", "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD_ENV, self.encode())
            .output()
            .unwrap();
        match (out.status.signal(), out.status.code()) {
            (Some(libc::SIGABRT), _) => Ok(true),
            (_, Some(NOT_REACHED)) => Ok(false),
            _ => Err(format!(
                "child did not crash: {}\n{}{}",
                out.status,
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            )),
        }
    }
}

/// Entry point of the child process; does nothing in a normal test run.
#[test]
fn crash_child() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let spec = ChildSpec::decode(&spec);
    // A crash, not a core dump.
    let no_core = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    unsafe { libc::setrlimit(libc::RLIMIT_CORE, &no_core) };
    let point: &'static str = Box::leak(spec.point.into_boxed_str());
    let [a, b, state] = &spec.dirs;
    let mut held = spec
        .late
        .as_ref()
        .map(|rel| fs::File::options().append(true).open(b.join(rel)).unwrap());
    let mut hits = 0;
    let _g = hooks::on(point, move || {
        if hits == spec.nth {
            if let Some(f) = &mut held {
                f.write_all(LATE.as_bytes()).unwrap();
            }
            std::process::abort();
        }
        hits += 1;
    });
    let mut pair = Pair::open_at(a, b, state, spec.policy);
    if spec.cycle {
        if spec.named {
            force_named(&mut pair);
        }
        cycle(&mut pair);
    }
    std::process::exit(NOT_REACHED);
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

fn force_named(p: &mut Pair) {
    p.a.replica.local_mut().caps_mut().o_tmpfile = false;
    p.b.replica.local_mut().caps_mut().o_tmpfile = false;
}

/// The cycle that crashes: one sync, then a sweep of both quarantines.
fn cycle(p: &mut Pair) {
    let report = p
        .engine
        .sync_once(&mut p.a.replica, &mut p.b.replica)
        .unwrap();
    assert!(report.is_converged(), "{report:#?}");
    p.a.replica.sweep_quarantine();
    p.b.replica.sweep_quarantine();
}

fn prepare(sc: &Scenario) -> Pair {
    let mut p = Pair::new(sc.policy);
    (sc.base)(&p);
    p.sync();
    (sc.change)(&p);
    p
}

/// The tree after a sync (symlinks as symlinks), conflict copies keyed
/// without their timestamp.
type Outcome = BTreeMap<String, Node>;

fn outcome(p: &Pair) -> Outcome {
    p.a.tree(SymlinkPolicy::Links)
        .into_iter()
        .map(|(path, node)| {
            let key = match path.find(".sync-conflict-") {
                // ".sync-conflict-" + "YYYYMMDD-HHMMSS"
                Some(i) => format!("{}{}", &path[..i + 15], &path[i + 30..]),
                None => path,
            };
            (key, node)
        })
        .collect()
}

/// One scenario run without a crash: its trace and its outcome.
struct Clean {
    trace: Vec<&'static str>,
    outcome: Outcome,
}

fn clean_run(sc: &Scenario, named: bool) -> Clean {
    let mut p = prepare(sc);
    if named {
        force_named(&mut p);
    }
    hooks::start_trace();
    cycle(&mut p);
    let trace = hooks::take_trace();
    p.assert_converged();
    Clean {
        trace,
        outcome: outcome(&p),
    }
}

/// Reserved names under `root`, as (directory, name) relative to it.
fn reserved(root: &Path) -> BTreeSet<(PathBuf, Vec<u8>)> {
    fn walk(root: &Path, rel: &Path, out: &mut BTreeSet<(PathBuf, Vec<u8>)>) {
        for e in fs::read_dir(root.join(rel)).unwrap() {
            let e = e.unwrap();
            let name = e.file_name();
            if is_reserved(name.as_bytes()) {
                out.insert((rel.to_path_buf(), name.as_bytes().to_vec()));
            }
            if e.file_type().unwrap().is_dir() {
                walk(root, &rel.join(&name), out);
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(root, Path::new(""), &mut out);
    out
}

/// Every reserved name left by the crash is named by an intent in the
/// journal of its replica.
fn check_explained(dirs: &[PathBuf; 3]) -> Result<(), String> {
    for (root, id) in [(&dirs[0], ID_A), (&dirs[1], ID_B)] {
        let found = reserved(root);
        if found.is_empty() {
            continue;
        }
        let store = IndexStore::open(&IndexStore::path_for(&dirs[2], id), id).unwrap();
        let mut known = BTreeSet::new();
        for (_, i) in store.journal().pending().unwrap() {
            let dir = PathBuf::from(std::ffi::OsStr::from_bytes(
                i.path.parent().unwrap().as_bytes(),
            ));
            known.insert((dir.clone(), i.tmp.clone()));
            if let Some(old) = i.old {
                known.insert((dir, old));
            }
        }
        let unexplained: Vec<_> = found.difference(&known).collect();
        if !unexplained.is_empty() {
            return Err(format!(
                "{id}: reserved names not in the journal: {unexplained:?}"
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct Case {
    scenario: usize,
    named: bool,
    point: &'static str,
    nth: usize,
    /// Crash the replay too.
    twice: bool,
    /// The late-write variant.
    late: bool,
}

/// Runs one case; returns whether the replay crashed too.
fn run_case(case: Case, want: &Outcome) -> Result<bool, String> {
    let sc = &SCENARIOS[case.scenario];
    let pair = prepare(sc);
    let dirs = pair.dirs();
    let mut crashed_replay = false;
    let mut res = Ok(());
    let mut pair = pair.reopen_after(|| {
        let mut child = ChildSpec {
            dirs: dirs.clone(),
            policy: sc.policy,
            named: case.named,
            cycle: true,
            point: case.point.to_owned(),
            nth: case.nth,
            late: sc.held.filter(|_| case.late).map(str::to_owned),
        };
        res = match child.run() {
            Ok(true) => check_explained(&dirs),
            Ok(false) => Err("the hook point was never reached".to_owned()),
            Err(e) => Err(e),
        };
        if res.is_ok() && case.twice {
            child.cycle = false;
            child.point = RECOVERY_POINT.to_owned();
            child.nth = 0;
            child.late = None;
            res = child.run().map(|crashed| crashed_replay = crashed);
            if crashed_replay {
                res = check_explained(&dirs);
            }
        }
    });
    res?;
    // Replayed by the reopen; a zero-grace sweep settles the quarantines.
    pair.a.replica.sweep_quarantine();
    pair.b.replica.sweep_quarantine();
    for root in &dirs[..2] {
        let left = reserved(root);
        if !left.is_empty() {
            return Err(format!("reserved names left after replay: {left:?}"));
        }
    }
    let report = pair.try_sync();
    if !report.is_converged() {
        return Err(format!("no convergence: {report:#?}"));
    }
    pair.assert_converged();
    let got = outcome(&pair);
    if case.late {
        // The tree differs from the clean run by the late write, which
        // must be in some file.
        let kept = got
            .values()
            .any(|n| matches!(n, Node::File { content, .. } if content.ends_with(LATE)));
        return if kept {
            Ok(crashed_replay)
        } else {
            Err(format!("the late write is lost: {got:?}"))
        };
    }
    if got != *want {
        return Err(format!(
            "outcome differs from the run without a crash\n  got:  {got:?}\n  want: {want:?}"
        ));
    }
    Ok(crashed_replay)
}

/// Runs every position of the selected scenarios' traces, on all cores.
/// `twice` crashes the replay too. Returns the points covered and how many
/// replays crashed.
fn sweep(select: impl Fn(&Scenario) -> bool, twice: bool) -> (BTreeSet<&'static str>, usize) {
    let mut cases = Vec::new();
    let mut wants = BTreeMap::new();
    let mut covered = BTreeSet::new();
    for (i, sc) in SCENARIOS.iter().enumerate() {
        if !select(sc) {
            continue;
        }
        for named in [false, true] {
            if named && !sc.files {
                continue;
            }
            let clean = clean_run(sc, named);
            assert!(!clean.trace.is_empty(), "{}: empty trace", sc.name);
            let mut seen: BTreeMap<&str, usize> = BTreeMap::new();
            let mut swept = false;
            for &point in &clean.trace {
                let nth = seen.entry(point).or_default();
                swept |= point.starts_with("quarantine.");
                for late in [false, true] {
                    if late && (sc.held.is_none() || swept) {
                        continue;
                    }
                    cases.push(Case {
                        scenario: i,
                        named,
                        point,
                        nth: *nth,
                        twice,
                        late,
                    });
                }
                *nth += 1;
                covered.insert(point);
            }
            wants.insert((i, named), clean.outcome);
        }
    }
    let next = AtomicUsize::new(0);
    let failures = Mutex::new(Vec::new());
    let replays = AtomicUsize::new(0);
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(&case) = cases.get(i) else { break };
                    let want = &wants[&(case.scenario, case.named)];
                    match std::panic::catch_unwind(|| run_case(case, want)) {
                        Ok(Ok(crashed)) => {
                            replays.fetch_add(usize::from(crashed), Ordering::Relaxed);
                        }
                        Ok(Err(e)) => failures
                            .lock()
                            .unwrap()
                            .push(format!("{} {case:?}: {e}", SCENARIOS[case.scenario].name)),
                        Err(_) => failures.lock().unwrap().push(format!(
                            "{} {case:?}: panicked",
                            SCENARIOS[case.scenario].name
                        )),
                    }
                }
            });
        }
    });
    let failures = failures.into_inner().unwrap();
    assert!(
        failures.is_empty(),
        "{} of {} cases failed:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
    eprintln!("{} crash cases passed", cases.len());
    (covered, replays.into_inner())
}

/// A crash at every hook point of every operation.
#[test]
fn crashes_at_every_hook_point() {
    let (covered, _) = sweep(|_| true, false);
    // Every kind of commit step is crashed at.
    for point in [
        "journal.started",
        "journal.temp_written",
        "stage.created",
        "stage.linked",
        "create.before_rename",
        "create.after_rename",
        "replace.after_exchange",
        "replace.quarantined",
        "delete.after_rename",
        "delete.quarantined",
        "rmdir.before_rmdir",
        "rename.after_rename",
        "rename.moved",
        "chmod.after_chmod",
        "materialize.copied",
        "materialize.filled",
        "quarantine.before_unlink",
        "commit.verified",
    ] {
        assert!(covered.contains(point), "{point} never crashed at");
    }
}

/// A second crash during the replay of the first: replaying is idempotent.
#[test]
fn crashes_during_recovery() {
    let (_, replays) = sweep(
        |sc| {
            matches!(
                sc.name,
                "replace_file" | "file_to_symlink" | "delete_tree" | "conflict"
            )
        },
        true,
    );
    assert!(replays > 10, "only {replays} replays crashed");
}
