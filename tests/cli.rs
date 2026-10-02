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

#[test]
fn status_is_a_stub() {
    let state = tempfile::tempdir().unwrap();
    init_pair(state.path(), "p");
    let out = run(state.path(), &["status", "p"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("not implemented"), "{stderr}");
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

#[test]
fn sync_requires_initialised_pair() {
    let state = tempfile::tempdir().unwrap();
    let out = run(state.path(), &["sync", "--once", "nope"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("not initialised"), "{stderr}");
}
