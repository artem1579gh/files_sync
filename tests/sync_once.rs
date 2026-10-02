//! Scenario tests of `Engine::sync_once` on two real tempdir replicas (T13).

mod harness;

use files_sync::config::SymlinkPolicy;
use files_sync::engine::Side;
use files_sync::index::Kind;
use harness::{ID_A, ID_B, Pair, When, id7};

fn pair() -> Pair {
    Pair::new(SymlinkPolicy::Links)
}

#[test]
fn create_on_each_side() {
    let mut p = pair();
    p.a.write("a.txt", "from a");
    p.b.write("b.txt", "from b");
    p.a.mkdir("empty_a");
    p.b.mkdir("empty_b");
    let r = p.sync();
    assert_eq!(r.applied, 4);
    assert_eq!(r.rounds, 1);
    p.assert_converged();
    for t in [&p.a, &p.b] {
        assert_eq!(t.read("a.txt"), "from a");
        assert_eq!(t.read("b.txt"), "from b");
        assert!(t.is_dir("empty_a") && t.is_dir("empty_b"));
    }
    // Nothing left to do: a second cycle applies nothing.
    let r = p.sync();
    assert_eq!((r.applied, r.rounds), (0, 0));
}

#[test]
fn modify_on_each_side() {
    let mut p = pair();
    p.a.write("x", "1");
    p.sync();
    p.a.write("x", "2 from a");
    p.sync();
    p.assert_converged();
    assert_eq!(p.b.read("x"), "2 from a");

    p.b.write("x", "3 from b");
    p.b.chmod("x", 0o600);
    p.sync();
    p.assert_converged();
    assert_eq!(p.a.read("x"), "3 from b");
    assert_eq!(p.a.mode("x"), 0o600);

    // A mode change alone.
    p.a.chmod("x", 0o640);
    p.sync();
    p.assert_converged();
    assert_eq!(p.b.mode("x"), 0o640);
    assert_eq!(p.b.read("x"), "3 from b");
}

#[test]
fn delete_on_each_side() {
    let mut p = pair();
    p.a.write("x", "x");
    p.b.write("y", "y");
    p.sync();
    p.a.rm("x");
    p.b.rm("y");
    p.sync();
    p.assert_converged();
    for t in [&p.a, &p.b] {
        assert!(!t.exists("x") && !t.exists("y"));
        assert_eq!(t.entry("x").unwrap().kind, Kind::Tombstone);
    }
}

#[test]
fn concurrent_modification_makes_conflict_copies() {
    // Either side can win: the newer mtime does.
    for winner in [Side::A, Side::B] {
        let mut p = pair();
        p.a.write("doc.txt", "base");
        p.sync();
        let (ta, tb) = match winner {
            Side::A => (2_000_000_000, 1_000_000_000),
            Side::B => (1_000_000_000, 2_000_000_000),
        };
        p.a.write_at("doc.txt", "from a", ta);
        p.b.write_at("doc.txt", "from b", tb);
        let r = p.sync();
        p.assert_converged();

        let (won, lost, loser_id) = match winner {
            Side::A => ("from a", "from b", ID_B),
            Side::B => ("from b", "from a", ID_A),
        };
        assert_eq!(r.conflicts.len(), 1, "{r:#?}");
        assert_eq!(r.conflicts[0].side, winner.other());
        for t in [&p.a, &p.b] {
            assert_eq!(t.read("doc.txt"), won);
            let copies = t.conflicts("");
            assert_eq!(copies.len(), 1, "{copies:?}");
            let copy = &copies[0];
            assert!(copy.starts_with("doc.sync-conflict-"), "{copy}");
            assert!(copy.ends_with(&format!("-{}.txt", id7(loser_id))), "{copy}");
            assert_eq!(t.read(copy), lost);
        }
        assert_eq!(p.a.ls().len(), 2);
    }
}

#[test]
fn concurrent_identical_change_is_not_a_conflict() {
    let mut p = pair();
    p.a.write("x", "base");
    p.sync();
    p.a.write_at("x", "same", 1_000_000_000);
    p.b.write_at("x", "same", 1_000_000_005);
    let r = p.sync();
    p.assert_converged();
    assert!(r.conflicts.is_empty());
    assert_eq!(p.a.ls(), ["x"]);
    // The newer mtime is kept.
    assert_eq!(
        p.a.tree(SymlinkPolicy::Links),
        p.b.tree(SymlinkPolicy::Links)
    );
}

#[test]
fn concurrent_delete_vs_modification_keeps_modification() {
    for deleter in [Side::A, Side::B] {
        let mut p = pair();
        p.a.write("x", "base");
        p.sync();
        let (del, edit) = match deleter {
            Side::A => (&p.a, &p.b),
            Side::B => (&p.b, &p.a),
        };
        del.rm("x");
        edit.write("x", "edited");
        p.sync();
        p.assert_converged();
        for t in [&p.a, &p.b] {
            assert_eq!(t.read("x"), "edited");
            assert!(t.conflicts("").is_empty());
        }
    }
}

#[test]
fn directory_delete_vs_new_child_resurrects() {
    let mut p = pair();
    p.a.write("d/old", "old");
    p.sync();
    p.a.rm("d");
    p.b.write("d/new", "new");
    let r = p.sync();
    p.assert_converged();
    assert_eq!(r.resurrected.len(), 1, "{r:#?}");
    for t in [&p.a, &p.b] {
        assert_eq!(t.read("d/new"), "new");
        // The delete of the child A had seen still goes through.
        assert!(!t.exists("d/old"));
    }
}

#[test]
fn type_changes() {
    let mut p = pair();
    p.a.write("t", "file");
    p.sync();

    // File → directory on A.
    p.a.rm("t");
    p.a.write("t/inner", "inner");
    p.sync();
    p.assert_converged();
    assert_eq!(p.b.read("t/inner"), "inner");

    // Directory → symlink on B.
    p.b.rm("t");
    p.b.symlink("t", "elsewhere");
    p.sync();
    p.assert_converged();
    assert!(p.a.is_symlink("t"));
    assert_eq!(p.a.readlink("t"), "elsewhere");

    // Symlink → file on A.
    p.a.write("t", "file again");
    p.sync();
    p.assert_converged();
    assert!(p.b.is_file("t"));
    assert_eq!(p.b.read("t"), "file again");

    // File → directory on B, then back to a file on A.
    p.b.rm("t");
    p.b.mkdir("t");
    p.sync();
    p.assert_converged();
    assert!(p.a.is_dir("t"));
    p.a.rm("t");
    p.a.write("t", "last");
    p.sync();
    p.assert_converged();
    assert_eq!(p.b.read("t"), "last");
}

#[test]
fn nested_directories() {
    let mut p = pair();
    p.a.write("d1/d2/d3/f", "deep");
    p.a.write("d1/g", "g");
    p.a.mkdir("d1/d2/empty");
    p.a.chmod("d1/d2", 0o750);
    p.sync();
    p.assert_converged();
    assert_eq!(p.b.read("d1/d2/d3/f"), "deep");
    assert_eq!(p.b.mode("d1/d2"), 0o750);
    assert!(p.b.is_dir("d1/d2/empty"));

    // A directory mode change alone.
    p.b.chmod("d1/d2", 0o700);
    p.sync();
    p.assert_converged();
    assert_eq!(p.a.mode("d1/d2"), 0o700);

    // A whole subtree deleted on B.
    p.b.rm("d1");
    p.sync();
    p.assert_converged();
    assert!(!p.a.exists("d1"));
    assert!(p.a.ls().is_empty());
}

#[test]
fn symlinks_under_links() {
    let mut p = pair();
    p.a.write("dir/file", "content");
    p.a.symlink("rel", "dir/file");
    p.a.symlink("dirlink", "dir");
    p.a.symlink("abs", "/tmp/outside/somewhere");
    p.a.symlink("dangling", "no/such/thing");
    p.a.symlink("up", "../../escapes");
    p.sync();
    p.assert_converged();
    assert_eq!(p.b.readlink("rel"), "dir/file");
    assert_eq!(p.b.read("rel"), "content");
    assert_eq!(p.b.readlink("dirlink"), "dir");
    assert_eq!(p.b.readlink("abs"), "/tmp/outside/somewhere");
    assert_eq!(p.b.readlink("dangling"), "no/such/thing");
    assert_eq!(p.b.readlink("up"), "../../escapes");
    // The link to a directory is synced as a link: nothing beneath it.
    assert!(p.b.entry("dirlink/file").is_none());

    // Retarget on B, delete on A.
    p.b.symlink("rel", "dir/other");
    p.a.rm("abs");
    p.sync();
    p.assert_converged();
    assert_eq!(p.a.readlink("rel"), "dir/other");
    assert!(!p.b.exists("abs"));

    // Concurrent retargets: a conflict copy of the losing link.
    p.a.symlink("dangling", "from-a");
    p.b.symlink("dangling", "from-b");
    let r = p.sync();
    p.assert_converged();
    assert_eq!(r.conflicts.len(), 1);
    let copies = p.a.conflicts("");
    assert_eq!(copies.len(), 1);
    let kept = [p.a.readlink("dangling"), p.a.readlink(&copies[0])];
    assert!(kept.contains(&"from-a".to_owned()) && kept.contains(&"from-b".to_owned()));
}

/// Found by `tests/model.rs` (T14), where the model got it wrong: a symlink
/// re-created with the same target (new inode and mtime) is no change, so a
/// concurrent retarget on the other side wins without a conflict.
#[test]
fn recreated_symlink_with_same_target_is_not_a_change() {
    let mut p = pair();
    p.a.symlink("l", "t");
    p.sync();
    p.a.symlink("l", "t");
    p.b.symlink("l", "u");
    let r = p.sync();
    p.assert_converged();
    assert!(r.conflicts.is_empty(), "{r:#?}");
    assert_eq!(p.a.readlink("l"), "u");
    assert_eq!(p.a.ls(), ["l"]);
}

#[test]
fn symlinks_under_skip_are_left_alone() {
    let mut p = Pair::new(SymlinkPolicy::Skip);
    p.a.write("f", "f");
    p.a.symlink("l", "f");
    p.b.symlink("l", "other");
    let r = p.sync();
    p.assert_converged();
    assert!(
        r.unmanaged.is_empty(),
        "no live entry on either side: {r:#?}"
    );
    assert_eq!(p.a.readlink("l"), "f");
    assert_eq!(p.b.readlink("l"), "other");
    assert_eq!(p.b.read("f"), "f");

    // A file on one side, an ignored link on the other: skipped, both kept.
    p.a.write("g", "g");
    p.b.symlink("g", "elsewhere");
    let r = p.sync();
    assert_eq!(r.unmanaged.len(), 1, "{r:#?}");
    assert_eq!(p.a.read("g"), "g");
    assert_eq!(p.b.readlink("g"), "elsewhere");
}

#[test]
fn version_vectors_record_both_replicas() {
    let mut p = pair();
    p.a.write("x", "1");
    p.sync();
    p.b.write("x", "2");
    p.sync();
    let vv = p.a.entry("x").unwrap().vv;
    assert!(vv.get(ID_A) > 0 && vv.get(ID_B) > 0, "{vv:?}");
    assert_eq!(vv, p.b.entry("x").unwrap().vv);
}

#[test]
fn destination_edited_during_sync_becomes_conflict() {
    let mut p = pair();
    p.a.write("x", "base");
    p.sync();
    p.a.write_at("x", "from a", 1_000_000_000);
    // B's user writes right before the push lands: the CAS fails, the path
    // is rescanned, and the next round sees a conflict. B's edit is newer.
    let bx = p.b.path("x");
    let r = p.sync_racing(Side::B, When::Apply, "x", move || {
        std::fs::write(&bx, "from b, mid-sync").unwrap();
    });
    p.assert_converged();
    assert!(r.retried >= 1, "{r:#?}");
    assert!(r.rounds >= 2, "{r:#?}");
    for t in [&p.a, &p.b] {
        assert_eq!(t.read("x"), "from b, mid-sync");
        let copies = t.conflicts("");
        assert_eq!(copies.len(), 1, "{copies:?}");
        assert_eq!(t.read(&copies[0]), "from a");
    }
}

#[test]
fn source_edited_during_sync_is_resent() {
    let mut p = pair();
    p.a.write("x", "first");
    // A's user rewrites the file just as B reads it from A: the read fails
    // its stability check, the path is rescanned, and the new content goes.
    let ax = p.a.path("x");
    let r = p.sync_racing(Side::A, When::Read, "x", move || {
        std::fs::write(&ax, "second, longer").unwrap();
    });
    p.assert_converged();
    assert!(r.retried >= 1, "{r:#?}");
    assert_eq!(p.b.read("x"), "second, longer");
    assert!(p.b.conflicts("").is_empty());
}

#[test]
fn directory_filled_during_delete_is_kept() {
    let mut p = pair();
    p.a.write("d/f", "f");
    p.sync();
    p.a.rm("d");
    // B's user drops a new file into d just before B removes it.
    let new = p.b.path("d/new");
    let r = p.sync_racing(Side::B, When::Apply, "d", move || {
        std::fs::write(&new, "new").unwrap();
    });
    p.assert_converged();
    assert!(r.retried >= 1, "{r:#?}");
    for t in [&p.a, &p.b] {
        assert_eq!(t.read("d/new"), "new");
        assert!(!t.exists("d/f"));
    }
}

/// T18: a tombstone is collected on both sides once both held the deletion
/// for the retention period; a later create syncs as a new file, and
/// nothing deleted comes back.
#[test]
fn tombstones_are_collected_after_the_retention_period() {
    let mut p = pair();
    p.a.write("f", "x");
    p.a.mkdir("d");
    p.a.write("d/g", "y");
    p.a.write("keep", "z");
    p.sync();
    p.a.rm("f");
    p.b.rm("d");
    // Default retention (30 days): the tombstones stay.
    let r = p.sync();
    assert_eq!(r.collected, [0, 0]);
    for t in [&p.a, &p.b] {
        for path in ["f", "d", "d/g"] {
            assert_eq!(t.entry(path).unwrap().kind, Kind::Tombstone, "{path}");
        }
    }

    p.engine = p
        .engine
        .clone()
        .tombstone_retention(std::time::Duration::ZERO);
    let r = p.sync();
    assert_eq!((r.applied, r.collected), (0, [3, 3]));
    for t in [&p.a, &p.b] {
        for path in ["f", "d", "d/g"] {
            assert_eq!(t.entry(path), None, "{path}");
        }
        assert_eq!(t.read("keep"), "z");
    }
    let r = p.sync();
    assert_eq!((r.applied, r.collected), (0, [0, 0]));
    p.assert_converged();
    assert!(!p.a.exists("f") && !p.b.exists("d"));

    // Re-created after the GC: an ordinary new file.
    p.b.write("f", "again");
    let r = p.sync();
    assert_eq!(r.applied, 1);
    assert_eq!(p.a.read("f"), "again");
    p.assert_converged();
}

/// Independent deletes on both sides leave concurrent tombstones, which
/// reconcile never equalises (§6.1); they are collected all the same.
#[test]
fn concurrent_tombstones_are_collected() {
    let mut p = pair();
    p.a.write("f", "x");
    p.sync();
    p.a.write("f", "a's edit");
    p.a.rm("f");
    p.b.rm("f");
    p.engine = p
        .engine
        .clone()
        .tombstone_retention(std::time::Duration::ZERO);
    let r = p.sync();
    assert_eq!(r.collected, [1, 1]);
    assert_eq!((p.a.entry("f"), p.b.entry("f")), (None, None));
    p.assert_converged();
}
