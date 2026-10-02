//! Block-level delta transfer over loopback-remote replicas (T24, design
//! §7.1): what crosses the connection, protocol v1 sessions, and a
//! destination that changes while the new file is assembled.

mod harness;

use std::fs;
use std::os::unix::fs::FileExt;
use std::path::Path;

use files_sync::config::SymlinkPolicy;
use files_sync::engine::Engine;
use files_sync::replica::BLOCK_SIZE;
use files_sync::replica::proto::PROTOCOL_VERSION;
use harness::{Mode, Pair};

const MIB: usize = 1 << 20;

/// `n` pseudo-random lowercase letters (the harness compares trees as
/// text): every block differs.
fn random(n: usize, seed: u8) -> Vec<u8> {
    let mut out = vec![0u8; n];
    blake3::Hasher::new()
        .update(&[seed])
        .finalize_xof()
        .fill(&mut out);
    out.iter_mut().for_each(|b| *b = b'a' + *b % 26);
    out
}

/// Changes the letter at `offset` in place (a new mtime, the same size).
fn flip(path: &Path, offset: u64) {
    let f = fs::File::options()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut b = [0u8; 1];
    f.read_exact_at(&mut b, offset).unwrap();
    let other = if b[0] == b'z' { b'a' } else { b[0] + 1 };
    f.write_all_at(&[other], offset).unwrap();
}

/// Both replicas served, with the engine's default delta threshold.
fn remote_pair(versions: [(u32, u32); 2]) -> Pair {
    let mut p = Pair::new(SymlinkPolicy::Links).over_each([Mode::Remote; 2], versions);
    p.engine = Engine::new();
    p
}

fn same_content(p: &Pair, rel: &str) -> bool {
    fs::read(p.a.path(rel)).unwrap() == fs::read(p.b.path(rel)).unwrap()
}

const V2: (u32, u32) = (PROTOCOL_VERSION, PROTOCOL_VERSION);

#[test]
fn one_byte_in_64_mib_moves_a_few_blocks() {
    let mut p = remote_pair([V2, V2]);
    let size = 64 * MIB;
    p.a.write("big", random(size, 1));
    let before = p.traffic();
    let r = p.sync();
    assert_eq!((r.applied, r.deltas), (1, 0), "a new file goes whole");
    let whole = p.traffic() - before;
    // From A's server to the engine, and on to B's: the counter sees it all.
    assert!(whole > 2 * size as u64, "{whole} bytes for a new file");

    flip(&p.a.path("big"), 40 * MIB as u64 + 12_345);
    let before = p.traffic();
    let r = p.sync();
    let moved = p.traffic() - before;
    assert_eq!((r.applied, r.deltas), (1, 1), "{r:?}");
    assert!(same_content(&p, "big"));
    // The changed block crosses twice (A's server to the engine, the
    // engine to B's server); the rest is three block lists of 16 KiB, the
    // delta, scans and index exchange.
    let limit = 2 * BLOCK_SIZE + 192 * 1024;
    eprintln!("one byte changed: {moved} bytes on the connections (limit {limit})");
    assert!(moved <= limit, "{moved} bytes moved, limit {limit}");
    p.assert_converged();

    // The other way, with a block appended and one dropped: still deltas.
    let mut grown = fs::read(p.b.path("big")).unwrap();
    grown.drain(..BLOCK_SIZE as usize);
    grown.extend(random(BLOCK_SIZE as usize, 2));
    p.b.write("big", &grown);
    let before = p.traffic();
    let r = p.sync();
    let moved = p.traffic() - before;
    assert_eq!((r.applied, r.deltas), (1, 1), "{r:?}");
    assert_eq!(fs::read(p.a.path("big")).unwrap(), grown);
    assert!(moved <= limit, "{moved} bytes moved, limit {limit}");
    p.assert_converged();
}

/// A v1 server, a v1 client, or one v1 replica of two: every file goes
/// whole, and the pair syncs as before. Two v2 sides send the same change
/// as a delta.
#[test]
fn v1_sessions_send_whole_files() {
    let v1_server = (1, PROTOCOL_VERSION);
    let v1_client = (PROTOCOL_VERSION, 1);
    let size = 4 * MIB;
    for (versions, delta) in [
        ([v1_server, v1_server], false),
        ([v1_client, v1_client], false),
        ([V2, v1_server], false),
        ([v1_client, V2], false),
        ([V2, V2], true),
    ] {
        let mut p = remote_pair(versions);
        p.a.write("f", random(size, 3));
        p.b.write("g", random(size, 4));
        p.sync();
        flip(&p.a.path("f"), 5);
        flip(&p.b.path("g"), size as u64 - 1);
        let before = p.traffic();
        let r = p.sync();
        let moved = p.traffic() - before;
        assert_eq!(r.applied, 2, "{versions:?}: {r:?}");
        assert!(same_content(&p, "f") && same_content(&p, "g"));
        p.assert_converged();
        if delta {
            assert_eq!(r.deltas, 2, "{versions:?}");
            assert!(moved < MIB as u64, "{versions:?}: {moved} bytes");
        } else {
            assert_eq!(r.deltas, 0, "{versions:?}");
            // Each file from one server to the other, whole.
            assert!(moved > 4 * size as u64, "{versions:?}: {moved} bytes");
        }
    }
}

/// The destination's file changes while the new content is assembled from
/// it: nothing is committed, no temp file is left, and the next round keeps
/// both versions (a conflict). B is local, so its commit runs on this
/// thread, where the hook is; A is served, so the transfer is a delta.
#[cfg(feature = "hooks")]
#[test]
fn destination_changed_mid_assembly_commits_nothing() {
    use files_sync::fs::hooks;

    let mut p = Pair::new(SymlinkPolicy::Links).over_each([Mode::Remote, Mode::Local], [V2, V2]);
    p.engine = Engine::new();
    let size = 3 * MIB;
    let old = random(size, 5);
    p.a.write("f", &old);
    p.sync();
    flip(&p.a.path("f"), 7);
    let new = fs::read(p.a.path("f")).unwrap();

    // While B assembles block 0, the user writes into a later block.
    let path = p.b.path("f");
    let at = 2 * MIB as u64 + 3;
    let _hook = hooks::once("delta.block", move || {
        let f = fs::File::options().write(true).open(&path).unwrap();
        f.write_all_at(b"user", at).unwrap();
    });
    let r = p.sync();
    assert!(r.retried >= 1, "{r:?}");
    assert_eq!(r.conflicts.len(), 1, "{r:?}");
    p.assert_converged();

    // Both versions survive: A's change and the user's edit on B.
    let mut user = old.clone();
    user[at as usize..at as usize + 4].copy_from_slice(b"user");
    let copy = &p.b.conflicts("")[0];
    let mut versions = [
        fs::read(p.b.path("f")).unwrap(),
        fs::read(p.b.path(copy)).unwrap(),
    ];
    versions.sort();
    let mut expected = [new, user];
    expected.sort();
    assert!(versions == expected, "both versions are kept");
}
