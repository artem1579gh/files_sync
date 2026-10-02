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

#[test]
fn stubs_probe_roots_and_report_not_implemented() {
    let (state, a, b) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    let (a_str, b_str) = (a.path().to_str().unwrap(), b.path().to_str().unwrap());
    let out = run(state.path(), &["init", "p", "--a", a_str, "--b", b_str]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    for args in [
        &["sync", "--once", "p"][..],
        &["daemon", "p"],
        &["status", "p"],
    ] {
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
fn sync_requires_initialised_pair() {
    let state = tempfile::tempdir().unwrap();
    let out = run(state.path(), &["sync", "--once", "nope"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("not initialised"), "{stderr}");
}
