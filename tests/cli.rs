//! End-to-end tests of the `files_sync` binary.

use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use files_sync::config::PairConfig;

fn run(state_home: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_files_sync"))
        .args(args)
        .env("XDG_STATE_HOME", state_home)
        .env_remove("RUST_LOG")
        .output()
        .unwrap()
}

#[test]
fn help_lists_subcommands() {
    let state = tempfile::tempdir().unwrap();
    let out = run(state.path(), &["--help"]);
    assert!(out.status.success());
    let help = String::from_utf8(out.stdout).unwrap();
    for cmd in ["init", "sync", "daemon", "status"] {
        assert!(help.contains(cmd), "--help lacks {cmd}:\n{help}");
    }
}

#[test]
fn init_writes_config_that_loads_back() {
    let (state, a, b) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    let (a_str, b_str) = (a.path().to_str().unwrap(), b.path().to_str().unwrap());
    let out = run(state.path(), &["init", "docs", "--a", a_str, "--b", b_str]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let cfg = PairConfig::load(state.path(), "docs").unwrap();
    assert_eq!(cfg.name, "docs");
    assert_eq!(cfg.replicas[0].root, a.path().canonicalize().unwrap());
    assert_eq!(cfg.replicas[1].root, b.path().canonicalize().unwrap());
    assert_ne!(cfg.replicas[0].id, cfg.replicas[1].id);

    // A second init must not overwrite the pair.
    let out = run(state.path(), &["init", "docs", "--a", a_str, "--b", b_str]);
    assert!(!out.status.success());
    assert_eq!(PairConfig::load(state.path(), "docs").unwrap(), cfg);
}

/// Initialises pair `name` over two fresh tempdirs.
fn init_pair(state: &Path, name: &str) -> (tempfile::TempDir, tempfile::TempDir) {
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a_str, b_str) = (a.path().to_str().unwrap(), b.path().to_str().unwrap());
    let out = run(state, &["init", name, "--a", a_str, "--b", b_str]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    (a, b)
}

/// Runs `args`, which must succeed; returns stdout.
fn ok(state: &Path, args: &[&str]) -> String {
    let out = run(state, args);
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn status_shows_index_conflicts_quarantine_and_last_sync() {
    let state = tempfile::tempdir().unwrap();
    let (a, _b) = init_pair(state.path(), "p");
    let st = ok(state.path(), &["status", "p"]);
    assert!(st.contains("0 entries (0 tombstones)"), "{st}");
    assert!(st.contains("last sync:   never"), "{st}");

    std::fs::write(a.path().join("f"), "x").unwrap();
    std::fs::write(a.path().join("gone"), "x").unwrap();
    let copy = "f.sync-conflict-20260101-120000-abcdef0";
    std::fs::write(a.path().join(copy), "y").unwrap();
    ok(state.path(), &["sync", "--once", "p"]);
    std::fs::remove_file(a.path().join("gone")).unwrap();
    ok(state.path(), &["sync", "--once", "p"]);

    let st = ok(state.path(), &["status", "p"]);
    assert_eq!(st.matches("3 entries (1 tombstones)").count(), 2, "{st}");
    assert_eq!(st.matches("conflicts:   1").count(), 2, "{st}");
    assert_eq!(st.matches(copy).count(), 2, "{st}");
    assert_eq!(st.matches("quarantined: 0").count(), 2, "{st}");
    assert!(!st.contains("never") && !st.contains("daemon"), "{st}");
}

/// `--sandbox` (landlock) still lets a sync write the roots and the state.
#[test]
fn sandboxed_sync_and_status() {
    if files_sync::sandbox::abi().is_none() {
        eprintln!("landlock unsupported here; skipping");
        return;
    }
    let state = tempfile::tempdir().unwrap();
    let (a, b) = init_pair(state.path(), "p");
    std::fs::create_dir(a.path().join("d")).unwrap();
    std::fs::write(a.path().join("d/f"), "x").unwrap();
    let out = run(state.path(), &["--sandbox", "sync", "--once", "p"]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    assert!(stderr.contains("landlock sandbox"), "{stderr}");
    assert_eq!(std::fs::read(b.path().join("d/f")).unwrap(), b"x");
    // A replace (quarantine), a delete and an rmdir.
    std::fs::write(b.path().join("d/f"), "y").unwrap();
    ok(state.path(), &["sync", "--sandbox", "--once", "p"]);
    assert_eq!(std::fs::read(a.path().join("d/f")).unwrap(), b"y");
    std::fs::remove_dir_all(a.path().join("d")).unwrap();
    ok(state.path(), &["--sandbox", "sync", "--once", "p"]);
    assert!(!b.path().join("d").exists());
    assert_eq!(names(b.path()), Vec::<String>::new());
    let st = ok(state.path(), &["--sandbox", "status", "p"]);
    assert!(st.contains("2 entries (2 tombstones)"), "{st}");
}

/// The names in `dir`, sorted.
fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

/// Polls `cond` for up to 10 s.
fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while !cond() {
        assert!(t0.elapsed() < Duration::from_secs(10), "{what}: timed out");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn daemon_syncs_until_a_signal_stops_it() {
    for signal in [libc::SIGTERM, libc::SIGINT] {
        let state = tempfile::tempdir().unwrap();
        let (a, b) = init_pair(state.path(), "p");
        std::fs::write(a.path().join("before"), "1").unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_files_sync"))
            .args(["daemon", "p"])
            .env("XDG_STATE_HOME", state.path())
            .env_remove("RUST_LOG")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let (a, b) = (a.path(), b.path());
        wait_for("initial sync", || b.join("before").exists());
        // A replace leaves an old inode in quarantine, a create does not.
        std::fs::write(a.join("before"), "2").unwrap();
        std::fs::create_dir(b.join("dir")).unwrap();
        std::fs::write(b.join("dir/after"), "x").unwrap();
        wait_for("edits sync", || {
            std::fs::read(b.join("before")).is_ok_and(|c| c == b"2") && a.join("dir/after").exists()
        });
        // The daemon holds the indexes; `status` shows its report.
        wait_for("status report", || {
            ok(state.path(), &["status", "p"])
                .matches("3 entries")
                .count()
                == 2
        });
        let st = ok(state.path(), &["status", "p"]);
        assert!(st.contains("daemon running"), "{st}");

        let pid = i32::try_from(child.id()).unwrap();
        // SAFETY: sending a signal to our own child process.
        assert_eq!(unsafe { libc::kill(pid, signal) }, 0);
        let t0 = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if t0.elapsed() > Duration::from_secs(10) {
                child.kill().unwrap();
                panic!("daemon did not stop on signal {signal}");
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let out = child.wait_with_output().unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(status.success(), "signal {signal}: {status:?}\n{stderr}");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains("daemon for \"p\" stopped"), "{stdout}");
        assert!(stderr.contains("shutting down"), "{stderr}");
        // Shut down cleanly: nothing reserved is left behind.
        for root in [a, b] {
            assert_eq!(names(root), ["before", "dir"]);
            assert_eq!(names(&root.join("dir")), ["after"]);
        }
    }
}

#[test]
fn sync_once_syncs_two_directories() {
    let state = tempfile::tempdir().unwrap();
    let (a, b) = init_pair(state.path(), "p");
    std::fs::create_dir(a.path().join("dir")).unwrap();
    std::fs::write(a.path().join("dir/from_a"), "a").unwrap();
    std::fs::write(b.path().join("from_b"), "b").unwrap();
    std::fs::write(b.path().join("replaced"), "old").unwrap();

    let out = run(state.path(), &["sync", "--once", "p"]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    assert!(stderr.contains("filesystem capabilities"), "{stderr}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("4 change(s) applied"), "{stdout}");
    assert_eq!(std::fs::read(b.path().join("dir/from_a")).unwrap(), b"a");
    assert_eq!(std::fs::read(a.path().join("from_b")).unwrap(), b"b");

    // A replace leaves an old inode in quarantine; the one-shot sync waits
    // for it, so nothing reserved is left behind.
    std::fs::write(a.path().join("replaced"), "new").unwrap();
    let out = run(state.path(), &["sync", "--once", "p"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(std::fs::read(b.path().join("replaced")).unwrap(), b"new");
    for root in [&a, &b] {
        let mut names: Vec<_> = std::fs::read_dir(root.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, ["dir", "from_b", "replaced"]);
    }
}

/// A local pair sends whole files, so its summary never mentions deltas
/// (T28), even for a replaced file above the delta threshold.
#[test]
fn local_sync_prints_no_delta() {
    let state = tempfile::tempdir().unwrap();
    let (a, b) = init_pair(state.path(), "p");
    let mut data = vec![1u8; 2 << 20];
    std::fs::write(a.path().join("big"), &data).unwrap();
    ok(state.path(), &["sync", "--once", "p"]);
    data[1000] = 2;
    std::fs::write(a.path().join("big"), &data).unwrap();
    let out = ok(state.path(), &["sync", "--once", "p"]);
    assert!(out.contains("1 change(s) applied in 1 round(s)\n"), "{out}");
    assert!(!out.contains("delta"), "{out}");
    assert_eq!(std::fs::read(b.path().join("big")).unwrap(), data);
}

#[test]
fn sync_requires_initialised_pair() {
    let state = tempfile::tempdir().unwrap();
    let out = run(state.path(), &["sync", "--once", "nope"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("not initialised"), "{stderr}");
}

/// Runs `args` with stdout on a pipe whose reader is already closed.
fn run_closed_stdout(state: &Path, args: &[&str]) -> Output {
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    Command::new(env!("CARGO_BIN_EXE_files_sync"))
        .args(args)
        .env("XDG_STATE_HOME", state)
        .env_remove("RUST_LOG")
        .stdout(writer)
        .stderr(Stdio::piped())
        .output()
        .unwrap()
}

/// A closed stdout (`status p | head -0`) ends the command quietly with
/// 141 (128 + SIGPIPE), after it did its work; it does not panic.
#[test]
fn closed_stdout_exits_quietly() {
    let state = tempfile::tempdir().unwrap();
    let (a, b) = init_pair(state.path(), "p");
    std::fs::write(a.path().join("f"), "x").unwrap();
    for args in [&["sync", "--once", "p"][..], &["status", "p"]] {
        let out = run_closed_stdout(state.path(), args);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(!stderr.contains("panicked"), "{args:?}: {stderr}");
        assert!(!stderr.contains("Error"), "{args:?}: {stderr}");
        assert_eq!(out.status.code(), Some(141), "{args:?}: {stderr}");
    }
    assert_eq!(std::fs::read(b.path().join("f")).unwrap(), b"x");

    // A daemon keeps syncing; only its stop summary is lost.
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    let mut child = Command::new(env!("CARGO_BIN_EXE_files_sync"))
        .args(["daemon", "p"])
        .env("XDG_STATE_HOME", state.path())
        .env_remove("RUST_LOG")
        .stdout(writer)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::fs::write(a.path().join("g"), "y").unwrap();
    wait_for("daemon sync", || b.path().join("g").exists());
    let pid = i32::try_from(child.id()).unwrap();
    // SAFETY: sending a signal to our own child process.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    wait_for("daemon stop", || child.try_wait().unwrap().is_some());
    let out = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert!(stderr.contains("daemon stopped"), "{stderr}");
    assert_eq!(out.status.code(), Some(141), "{stderr}");
}
