//! A conflict winner's change made before it learns the merged version
//! vector must not be overwritten by its own older version (issue #3). The
//! merged vector must not carry a counter that the winner's clock has not
//! seen, or the winner's next local change can reuse it and be dominated.

mod harness;

use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime};

use files_sync::config::SymlinkPolicy;
use files_sync::engine::{Engine, Side};
use files_sync::replica::Replica;
use files_sync::replica::proto::PROTOCOL_VERSION;
use files_sync::scan::Scope;
use harness::{Mode, Pair, When};

fn pair(m: Mode) -> Pair {
    Pair::new(SymlinkPolicy::Links).over(m)
}

/// Concurrent edits of `f.txt`; A's is newer, so A wins the conflict.
fn file_conflict_won_by_a(p: &mut Pair) {
    p.a.write("f.txt", "base");
    p.sync();
    p.a.write_at("f.txt", "older edit on A", 2_000_000_000);
    p.b.write_at("f.txt", "edit on B", 1_000_000_000);
}

/// What A's user saves last, newer than both edits.
fn save_newest(path: &Path) {
    fs::write(path, "newest edit on A").unwrap();
    let f = fs::File::options().write(true).open(path).unwrap();
    f.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(2_100_000_000))
        .unwrap();
}

/// Both sides hold A's newest edit, and both edits it raced with survive as
/// conflict copies.
fn assert_newest_kept(p: &mut Pair) {
    p.assert_converged();
    for t in [&p.a, &p.b] {
        assert_eq!(t.read("f.txt"), "newest edit on A");
        let mut copies: Vec<String> = t.conflicts("").iter().map(|c| t.read(c)).collect();
        copies.sort();
        assert_eq!(copies, ["edit on B", "older edit on A"]);
    }
}

/// Scans both sides; returns the higher clock.
fn scan_both(p: &mut Pair) -> u64 {
    for t in [&mut p.a, &mut p.b] {
        t.replica.scan(Scope::Full).unwrap();
    }
    p.a.replica.clock().unwrap().max(p.b.replica.clock().unwrap())
}

/// The trace from the issue. Round 1 renames B's version to a conflict
/// copy and gives B A's version with the merged vector. In round 2, as A
/// reads B's copy, B's user touches the copy (so it does not reach A this
/// round, and does not raise A's clock) and A's user saves `f.txt` again
/// (so A's `SetMeta` fails and A rescans it).
fn winner_edit_before_it_learns_the_merged_vector_is_kept(m: Mode) {
    let mut p = pair(m);
    file_conflict_won_by_a(&mut p);
    let clock = scan_both(&mut p);
    let (af, broot) = (p.a.path("f.txt"), p.b.root().to_path_buf());
    let r = p.sync_racing_at(
        Side::B,
        When::Read,
        |p| p.as_bytes().starts_with(b"f.sync-conflict-"),
        move || {
            save_newest(&af);
            for e in fs::read_dir(&broot).unwrap() {
                let path = e.unwrap().path();
                if path.to_str().unwrap().contains(".sync-conflict-") {
                    fs::write(&path, "edit on B, touched").unwrap();
                }
            }
        },
    );
    assert!(r.retried >= 2, "{r:#?}");
    p.assert_converged();
    for t in [&p.a, &p.b] {
        assert_eq!(t.read("f.txt"), "newest edit on A");
        let mut copies: Vec<String> = t.conflicts("").iter().map(|c| t.read(c)).collect();
        copies.sort();
        assert_eq!(copies, ["edit on B, touched", "older edit on A"]);
    }
    // Round 1 issued counters up to `clock + 2` on B (the merged vector,
    // B's tombstone and its copy). A rescans after a clock exchange, so its
    // edit's counter is above all of them.
    let vv = p.a.entry("f.txt").unwrap().vv;
    assert!(vv.get(p.a.id()) > clock + 2, "{vv:?}, clock {clock}");
}

/// The cycle stops right after round 1; A's user saves before the next one.
fn winner_edit_after_an_interrupted_cycle_is_kept(m: Mode) {
    let mut p = pair(m);
    file_conflict_won_by_a(&mut p);
    p.engine = Engine::new().max_rounds(1);
    let r = p.try_sync();
    assert!(!r.unresolved.is_empty(), "{r:#?}");
    assert_eq!(p.b.read("f.txt"), "older edit on A");
    save_newest(&p.a.path("f.txt"));
    p.engine = Engine::new();
    p.sync();
    assert_newest_kept(&mut p);
}

both_modes! {
    winner_edit_before_it_learns_the_merged_vector_is_kept,
    winner_edit_after_an_interrupted_cycle_is_kept,
}

/// As above, with A served by a protocol v2 server: it cannot take part in
/// the clock exchange, so only the merged vector itself can protect it.
#[test]
fn winner_edit_after_an_interrupted_cycle_on_a_v2_server_is_kept() {
    let v = PROTOCOL_VERSION;
    let mut p =
        Pair::new(SymlinkPolicy::Links).over_each([Mode::Remote, Mode::Local], [(2, v), (v, v)]);
    file_conflict_won_by_a(&mut p);
    p.engine = Engine::new().max_rounds(1);
    p.try_sync();
    save_newest(&p.a.path("f.txt"));
    p.engine = Engine::new();
    p.sync();
    assert_newest_kept(&mut p);
}
