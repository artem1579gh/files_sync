//! A replica whose index was lost or restored from a backup must not hand out
//! version counters its peer has already seen from it (issue #2): its
//! unsynced changes would be dominated by the peer's older versions and
//! overwritten or deleted, silently.

mod harness;

use std::fs;
use std::path::{Path, PathBuf};

use files_sync::config::SymlinkPolicy;
use files_sync::index::IndexStore;
use files_sync::replica::proto::PROTOCOL_VERSION;
use harness::{ID_A, ID_B, Mode, Pair};

fn pair(m: Mode) -> Pair {
    Pair::new(SymlinkPolicy::Links).over(m)
}

fn index_of(p: &Pair, side_b: bool) -> PathBuf {
    let state = p.dirs()[2].clone();
    IndexStore::path_for(&state, if side_b { ID_B } else { ID_A })
}

/// Closes both replicas, deletes B's index, and opens them again.
fn lose_index_b(p: Pair) -> Pair {
    let index = index_of(&p, true);
    p.reopen_after(|| fs::remove_file(&index).unwrap())
}

/// B makes three versions of `f.txt`, each synced to A.
fn three_versions_from_b(p: &mut Pair) {
    for v in ["v1", "v2", "v3"] {
        p.b.write("f.txt", v);
        p.sync();
    }
    assert_eq!(p.a.read("f.txt"), "v3");
}

/// The reproduction from the issue.
fn lost_index_keeps_an_unsynced_edit(m: Mode) {
    let mut p = pair(m);
    three_versions_from_b(&mut p);
    p.b.write("f.txt", "unsynced edit on B");
    let mut p = lose_index_b(p);
    p.sync();
    p.assert_converged();
    for t in [&p.a, &p.b] {
        assert_eq!(t.read("f.txt"), "unsynced edit on B");
    }
}

/// Both sides changed the file since the last sync: neither change is lost.
fn lost_index_with_edits_on_both_sides_keeps_both(m: Mode) {
    let mut p = pair(m);
    three_versions_from_b(&mut p);
    p.a.write_at("f.txt", "edit on A", 2_000_000_000);
    p.b.write_at("f.txt", "edit on B", 1_000_000_000);
    let mut p = lose_index_b(p);
    let r = p.sync();
    p.assert_converged();
    assert_eq!(r.conflicts.len(), 1, "{r:#?}");
    for t in [&p.a, &p.b] {
        assert_eq!(t.read("f.txt"), "edit on A");
        let copies = t.conflicts("");
        assert_eq!(copies.len(), 1, "{copies:?}");
        assert_eq!(t.read(&copies[0]), "edit on B");
    }
}

/// A file B deleted (A keeps the tombstone) and then created again is not
/// deleted by that tombstone.
fn lost_index_keeps_a_recreated_file(m: Mode) {
    let mut p = pair(m);
    p.b.write("g", "old");
    p.sync();
    p.b.rm("g");
    p.sync();
    assert!(!p.a.exists("g"));
    p.b.write("g", "new");
    let mut p = lose_index_b(p);
    p.sync();
    p.assert_converged();
    for t in [&p.a, &p.b] {
        assert_eq!(t.read("g"), "new");
    }
}

/// The index is the one from after the first sync (a backup): the changes
/// since then are kept.
fn restored_index_keeps_an_unsynced_edit(m: Mode) {
    let mut p = pair(m);
    p.b.write("f.txt", "v1");
    p.b.write("h.txt", "h1");
    p.sync();
    let index = index_of(&p, true);
    let backup = p.dirs()[2].join("backup.redb");
    let mut p = p.reopen_after(|| {
        fs::copy(&index, &backup).unwrap();
    });
    p.b.write("f.txt", "v2");
    p.sync();
    p.b.write("f.txt", "v3");
    p.a.write("h.txt", "h2 from A");
    p.sync();
    p.b.write("f.txt", "unsynced edit on B");
    let mut p = p.reopen_after(|| {
        fs::copy(&backup, &index).unwrap();
    });
    p.sync();
    p.assert_converged();
    for t in [&p.a, &p.b] {
        assert_eq!(t.read("f.txt"), "unsynced edit on B");
        assert_eq!(t.read("h.txt"), "h2 from A");
    }
}

both_modes! {
    lost_index_keeps_an_unsynced_edit,
    lost_index_with_edits_on_both_sides_keeps_both,
    lost_index_keeps_a_recreated_file,
    restored_index_keeps_an_unsynced_edit,
}

/// A served by a protocol v2 server, which has no clock exchange: the reset
/// replica B (local) still learns A's clock, from A's index.
#[test]
fn lost_index_against_a_v2_server() {
    let dirs = tempfile::tempdir().unwrap();
    let [a, b, state] = ["a", "b", "state"].map(|d| {
        let path = dirs.path().join(d);
        fs::create_dir(&path).unwrap();
        path
    });
    let open = |a: &Path, b: &Path, state: &Path| {
        let v = PROTOCOL_VERSION;
        Pair::open_at(a, b, state, SymlinkPolicy::Links)
            .over_each([Mode::Remote, Mode::Local], [(2, v), (v, v)])
    };
    let mut p = open(&a, &b, &state);
    three_versions_from_b(&mut p);
    p.b.write("f.txt", "unsynced edit on B");
    drop(p);
    fs::remove_file(IndexStore::path_for(&state, ID_B)).unwrap();
    let mut p = open(&a, &b, &state);
    p.sync();
    p.assert_converged();
    for t in [&p.a, &p.b] {
        assert_eq!(t.read("f.txt"), "unsynced edit on B");
    }
}
