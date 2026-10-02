//! A pair whose replica B is served over TLS on 127.0.0.1, in one process
//! (T21): `RemoteReplica` against `Server`, through the engine and the
//! daemon. The two-process CLI test is `tests/net.rs`.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::io::Read;
use std::net::TcpListener;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use files_sync::Error;
use files_sync::config::{self, PairConfig, ReplicaId};
use files_sync::daemon::Daemon;
use files_sync::engine::Engine;
use files_sync::fs::RelPath;
use files_sync::index::Kind;
use files_sync::replica::{LocalReplica, RemoteReplica, Replica};
use files_sync::server::{Server, ServerHandle};
use files_sync::tls::{DeviceId, Identity};
use files_sync::watch::Hint;
use tempfile::TempDir;

struct Net {
    _state: TempDir,
    ra: TempDir,
    rb: TempDir,
    cfg: PairConfig,
    pair_dir: PathBuf,
    server: Option<ServerHandle>,
}

impl Net {
    fn new() -> Net {
        let (state, ra, rb) = (tmp(), tmp(), tmp());
        let (cfg, _) = config::init(state.path(), "p", ra.path(), rb.path()).unwrap();
        let pair_dir = config::pair_dir(state.path(), "p").unwrap();
        let mut net = Net {
            _state: state,
            ra,
            rb,
            cfg,
            pair_dir,
            server: None,
        };
        let replica = LocalReplica::open(&net.cfg.replicas[1], &net.pair_dir)
            .unwrap()
            .quarantine_grace(Duration::ZERO);
        net.serve(replica, "127.0.0.1:0");
        net
    }

    fn serve(&mut self, replica: LocalReplica, addr: &str) {
        let [a, b] = &self.cfg.replicas;
        let identity = Identity::load(&self.pair_dir, b.id, b.device.unwrap()).unwrap();
        let tls = identity.server_config(a.device.unwrap()).unwrap();
        let handle = Server::new(replica, a.id, tls)
            .spawn(TcpListener::bind(addr).unwrap())
            .unwrap();
        self.cfg.replicas[1].remote = Some(handle.local_addr().to_string());
        self.server = Some(handle);
    }

    fn server(&self) -> &ServerHandle {
        self.server.as_ref().unwrap()
    }

    fn a(&self) -> LocalReplica {
        LocalReplica::open(&self.cfg.replicas[0], &self.pair_dir)
            .unwrap()
            .quarantine_grace(Duration::ZERO)
    }

    fn b(&self) -> RemoteReplica {
        RemoteReplica::connect(&self.cfg.replicas[1], &self.cfg.replicas[0], &self.pair_dir)
            .unwrap()
    }

    fn identity(&self, side: usize) -> Identity {
        let r = &self.cfg.replicas[side];
        Identity::load(&self.pair_dir, r.id, r.device.unwrap()).unwrap()
    }

    fn ids(&self) -> (ReplicaId, ReplicaId) {
        (self.cfg.replicas[0].id, self.cfg.replicas[1].id)
    }
}

fn tmp() -> TempDir {
    tempfile::tempdir().unwrap()
}

#[derive(Debug, PartialEq, Eq)]
enum Node {
    File(Vec<u8>),
    Dir,
    Link(Vec<u8>),
}

/// The tree under `root`, without reserved names.
fn tree(root: &Path) -> BTreeMap<Vec<u8>, Node> {
    fn walk(dir: &Path, rel: &[u8], out: &mut BTreeMap<Vec<u8>, Node>) {
        for e in fs::read_dir(dir).unwrap() {
            let e = e.unwrap();
            let name = e.file_name();
            if name.as_bytes().starts_with(b".~fsync.") {
                continue;
            }
            let mut path = rel.to_vec();
            if !path.is_empty() {
                path.push(b'/');
            }
            path.extend_from_slice(name.as_bytes());
            let ft = e.file_type().unwrap();
            if ft.is_symlink() {
                let target = fs::read_link(e.path()).unwrap();
                out.insert(path, Node::Link(target.as_os_str().as_bytes().to_vec()));
            } else if ft.is_dir() {
                walk(&e.path(), &path, out);
                out.insert(path, Node::Dir);
            } else {
                out.insert(path, Node::File(fs::read(e.path()).unwrap()));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, b"", &mut out);
    out
}

fn assert_same(net: &Net) {
    assert_eq!(tree(net.ra.path()), tree(net.rb.path()));
}

fn big(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| (i as u8).wrapping_mul(31) ^ seed).collect()
}

#[test]
fn sync_once_both_ways_over_tls() {
    let net = Net::new();
    let (ra, rb) = (net.ra.path(), net.rb.path());
    fs::create_dir_all(ra.join("d/e")).unwrap();
    fs::write(ra.join("d/e/f"), b"from a").unwrap();
    fs::write(ra.join("big"), big(300_000, 1)).unwrap();
    std::os::unix::fs::symlink("d/e/f", ra.join("link")).unwrap();
    fs::write(rb.join(OsStr::from_bytes(b"non-utf8-\xff")), b"from b").unwrap();
    fs::write(rb.join("empty"), b"").unwrap();

    let (mut a, mut b) = (net.a(), net.b());
    let report = Engine::new().sync_once(&mut a, &mut b).unwrap();
    assert!(report.is_converged(), "{report:?}");
    assert_same(&net);
    let full = b.entries_received();
    assert!(full >= 7, "{full}");

    // Nothing changed: no entry crosses the wire again.
    let report = Engine::new().sync_once(&mut a, &mut b).unwrap();
    assert_eq!(report.applied, 0);
    assert_eq!(b.entries_received(), full);

    // Edits, a delete and a conflict.
    fs::write(rb.join("big"), big(200_000, 2)).unwrap();
    fs::remove_file(ra.join("link")).unwrap();
    fs::write(ra.join("d/e/f"), b"a's edit").unwrap();
    fs::write(rb.join("d/e/f"), b"b's edit").unwrap();
    let report = Engine::new().sync_once(&mut a, &mut b).unwrap();
    assert!(report.is_converged(), "{report:?}");
    assert_eq!(report.conflicts.len(), 1, "{report:?}");
    assert_same(&net);
    assert_eq!(fs::read(ra.join("big")).unwrap(), big(200_000, 2));
    assert!(!rb.join("link").exists());
    // Only what changed came over (the changed entries and the conflict).
    let delta = b.entries_received() - full;
    assert!((1..=8).contains(&delta), "{delta}");
    // A single connection served it all.
    assert_eq!(b.connections(), 1);
}

#[test]
fn remote_index_and_reads() {
    let net = Net::new();
    let rb = net.rb.path();
    fs::write(rb.join("small"), b"0123456789").unwrap();
    fs::write(rb.join("large"), big(3 << 20, 3)).unwrap();
    let mut b = net.b();
    b.scan(files_sync::scan::Scope::Full).unwrap();
    let changes = b.changes_since(0).unwrap();
    let seqs: Vec<u64> = changes.iter().map(|(_, e)| e.seq).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "{seqs:?}");
    let entry = |name: &str| {
        changes
            .iter()
            .find(|(p, _)| p.as_bytes() == name.as_bytes())
            .map(|(_, e)| e.clone())
            .unwrap()
    };
    // Since a seq: only the later ones.
    let mid = entry("large").seq.min(entry("small").seq);
    assert!(
        b.changes_since(mid)
            .unwrap()
            .iter()
            .all(|(_, e)| e.seq > mid)
    );

    let small = RelPath::new("small").unwrap();
    let large = RelPath::new("large").unwrap();
    let mut buf = Vec::new();
    b.open_read(&small, &entry("small"))
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();
    assert_eq!(buf, b"0123456789");
    // A small read dropped early is drained: the connection stays.
    let mut r = b.open_read(&small, &entry("small")).unwrap();
    r.read_exact(&mut [0u8; 3]).unwrap();
    drop(r);
    b.changes_since(0).unwrap();
    assert_eq!(b.connections(), 1);
    // A large one is closed instead; the next call connects again.
    let mut r = b.open_read(&large, &entry("large")).unwrap();
    r.read_exact(&mut [0u8; 100]).unwrap();
    drop(r);
    b.changes_since(0).unwrap();
    assert_eq!(b.connections(), 2);
    // A request while a reader is open uses another connection.
    let mut r = b.open_read(&large, &entry("large")).unwrap();
    b.changes_since(0).unwrap();
    let mut all = Vec::new();
    r.read_to_end(&mut all).unwrap();
    assert_eq!(all, big(3 << 20, 3));
    drop(r);
    assert_eq!(b.connections(), 3);

    // Errors keep their class.
    let mut stale = entry("small");
    stale.kind = Kind::File {
        size: 10,
        hash: [7; 32],
    };
    let err = b.open_read(&small, &stale).err().unwrap();
    assert!(err.is_unstable(), "{err}");
    let err = b
        .open_read(
            &small,
            &files_sync::index::Entry::new(Kind::Dir, 0o755, 0, Default::default()),
        )
        .err()
        .unwrap();
    assert!(matches!(err, Error::Remote { .. }), "{err}");
    // And the connection is still the same.
    b.changes_since(0).unwrap();
    assert_eq!(b.connections(), 3);
}

#[test]
fn wrong_certificates_are_rejected() {
    let net = Net::new();
    let addr = net.server().local_addr().to_string();
    let (a_id, b_id) = net.ids();
    let b_device = net.cfg.replicas[1].device.unwrap();

    // A client whose certificate is not the pinned one.
    let stranger = Identity::generate(a_id).unwrap();
    let err = RemoteReplica::with_tls(&addr, a_id, b_id, stranger.client_config(b_device).unwrap())
        .unwrap_err();
    assert!(err.is_disconnected(), "{err}");

    // A server whose certificate is not the one we pin.
    let other = DeviceId::of(b"some other certificate");
    let err = RemoteReplica::with_tls(
        &addr,
        a_id,
        b_id,
        net.identity(0).client_config(other).unwrap(),
    )
    .unwrap_err();
    assert!(err.is_disconnected(), "{err}");
    assert!(err.to_string().contains("pinned"), "{err}");

    // Right certificates, wrong replica.
    let tls = net.identity(0).client_config(b_device).unwrap();
    let err = RemoteReplica::with_tls(&addr, a_id, ReplicaId(42), tls.clone()).unwrap_err();
    assert!(err.to_string().contains("expected"), "{err}");

    // And the right one still works.
    RemoteReplica::with_tls(&addr, a_id, b_id, tls).unwrap();
}

/// Waits for a hint matching `want`.
fn wait_hint(rx: &crossbeam_channel::Receiver<Hint>, want: impl Fn(&Hint) -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let left = end.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(h) if want(&h) => return,
            Ok(_) => {}
            Err(e) => panic!("no matching hint: {e}"),
        }
    }
}

#[test]
fn hints_are_pushed_and_survive_reconnects() {
    let net = Net::new();
    let mut b = net.b();
    let rx = b.watch().expect("the server watches");
    fs::write(net.rb.path().join("new"), b"x").unwrap();
    wait_hint(
        &rx,
        |h| matches!(h, Hint::Paths(p) if p.iter().any(|p| p.as_bytes() == b"new")),
    );

    // The connections drop: the hint thread reconnects and asks for a full
    // rescan; the next call reconnects too.
    net.server().close_connections();
    wait_hint(&rx, |h| *h == Hint::FullRescan);
    fs::write(net.rb.path().join("newer"), b"y").unwrap();
    wait_hint(
        &rx,
        |h| matches!(h, Hint::Paths(p) if p.iter().any(|p| p.as_bytes() == b"newer")),
    );
    b.scan(files_sync::scan::Scope::Full).unwrap();
    assert_eq!(b.connections(), 2);
}

#[test]
fn server_restart_and_lost_connections() {
    let mut net = Net::new();
    let rb = net.rb.path().to_path_buf();
    fs::write(rb.join("f"), b"1").unwrap();
    let (mut a, mut b) = (net.a(), net.b());
    Engine::new().sync_once(&mut a, &mut b).unwrap();
    let addr = net.server().local_addr().to_string();

    // The server goes away: the cycle fails as a whole, with a connection
    // error.
    let replica = net.server.take().unwrap().into_replica();
    let err = Engine::new().sync_once(&mut a, &mut b).unwrap_err();
    assert!(err.is_disconnected(), "{err}");

    // Back at the same address: the next cycle reconnects, and the mirror
    // starts over.
    let before = b.entries_received();
    net.serve(replica, &addr);
    fs::write(rb.join("g"), b"2").unwrap();
    let report = Engine::new().sync_once(&mut a, &mut b).unwrap();
    assert!(report.is_converged(), "{report:?}");
    assert_same(&net);
    assert!(b.entries_received() - before >= 2);
}

#[test]
fn daemon_with_a_remote_replica() {
    let net = Net::new();
    let (ra, rb) = (net.ra.path().to_path_buf(), net.rb.path().to_path_buf());
    let (mut a, mut b) = (net.a(), net.b());
    let (stop_tx, stop) = crossbeam_channel::bounded(1);
    let (reports_tx, reports) = crossbeam_channel::unbounded();
    let daemon = std::thread::spawn(move || {
        Daemon::new()
            .reports(reports_tx)
            .run(&mut a, &mut b, &stop)
            .unwrap()
    });
    reports.recv_timeout(Duration::from_secs(10)).unwrap();

    let wait_for = |path: &Path, content: &[u8]| {
        let end = Instant::now() + Duration::from_secs(10);
        while fs::read(path).ok().as_deref() != Some(content) {
            assert!(Instant::now() < end, "{} never arrived", path.display());
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    // B's change reaches A through the server's pushed hints alone (the
    // rescan timer is 10 minutes).
    fs::write(rb.join("from_b"), b"b").unwrap();
    wait_for(&ra.join("from_b"), b"b");
    fs::write(ra.join("from_a"), b"a").unwrap();
    wait_for(&rb.join("from_a"), b"a");

    // A lost connection does not stop the daemon.
    net.server().close_connections();
    fs::write(rb.join("after"), b"c").unwrap();
    wait_for(&ra.join("after"), b"c");

    stop_tx.send(()).unwrap();
    // (The drop may have cut the cycle that applied `from_a` short, in its
    // last request: then that cycle failed and was retried.)
    let stats = daemon.join().unwrap();
    assert!(stats.applied >= 2, "{stats:?}");
    assert_same(&net);
}
