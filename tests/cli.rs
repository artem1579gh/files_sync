//! End-to-end tests of the `files_sync` binary.

use std::path::Path;
use std::process::{Command, Output};

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
fn stubs_probe_roots_and_report_not_implemented() {
    let state = tempfile::tempdir().unwrap();
    let (a, b) = init_pair(state.path(), "p");

    for args in [&["daemon", "p"][..], &["status", "p"]] {
        let out = run(state.path(), args);
        assert!(!out.status.success(), "{args:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("not implemented"), "{args:?}: {stderr}");
        if args[0] != "status" {
            assert!(
                stderr.contains("filesystem capabilities"),
                "{args:?}: {stderr}"
            );
        }
    }
    // Probing leaves nothing behind in the roots.
    for root in [&a, &b] {
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
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
