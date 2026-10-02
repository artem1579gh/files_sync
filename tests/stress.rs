//! Concurrent stress test (design §9, T19). Ignored by default:
//! `cargo test --release --test stress -- --ignored --nocapture`.
//!
//! Writer threads edit both trees of a pair with every kind of user change
//! (read-modify-write, append, blind overwrite, delete, rename, directory ↔
//! symlink swaps, symlinks out of the tree, deleting conflict copies) while
//! the daemon syncs the pair in-process, sandboxed with landlock. Every write
//! is unique and logged to a ledger. Once the writers stop and the daemon is
//! quiet, three invariants are checked:
//!
//! - **No loss.** Every written content that no later user operation read
//!   and replaced (an in-place write's predecessor) or removed exists at the
//!   end, at a path it was written or moved to, or as a conflict copy of one.
//! - **No escape.** The daemon thread may only write beneath the roots and
//!   the state directory (landlock), and the sentinel directory that the
//!   writers' symlinks point into is unchanged, down to inode numbers and
//!   mtimes.
//! - **Convergence.** At most 3 full sync cycles until one applies nothing;
//!   then [`Pair::assert_converged`]: equal trees and version vectors, no
//!   echo, and no `.~fsync.*` names once the quarantine grace has passed.
//!
//! **The user model.** A writer resolves the parent of each path beneath its
//! tree's root without following symlinks (`openat2`), as a careful program
//! would, so the user never writes through a symlink themselves. It removes
//! objects by moving them out of the tree, atomically, into its side's
//! scratch directory (outside the root, same filesystem): a delete is a
//! rename out, a replacement an exchange with an object prepared there.
//! What came out is then hashed and logged as removed, so the ledger knows
//! exactly what the user removed, even when the daemon changed it just
//! before. Writers on one tree lock the slots (top-level names) they touch,
//! only so that the ledger is exact: the daemon takes no such lock.
//!
//! An in-place write into an inode that is already unlinked (`nlink == 0`
//! right after the write) is logged but not required to survive: that is the
//! documented window of design §5.10 (a quarantined child settled by an
//! rmdir while a writer holds an fd to it).
//!
//! Settings (environment variables):
//! - `FSYNC_STRESS_SECS` (30): how long the writers run;
//! - `FSYNC_STRESS_THREADS` (4): writer threads per tree;
//! - `FSYNC_STRESS_SLOTS` (8): top-level names the writers use;
//! - `FSYNC_STRESS_PAUSE_MS` (10): the longest pause between two operations
//!   of a writer (uniformly random);
//! - `FSYNC_STRESS_SEED` (from the clock): the writers' random seed;
//! - `FSYNC_STRESS_SANDBOX` (1): 0 runs the daemon without landlock.
//!
//! The test runs in both harness modes (T22): `local::writers_race_the_daemon`
//! syncs the `LocalReplica`s directly, `remote::writers_race_the_daemon`
//! through loopback servers. A served replica commits on its server's
//! connection threads, so in remote mode the servers are started from a
//! sandboxed thread too, and inherit its landlock domain. The two runs take
//! turns (a lock), so each has the machine to itself.

mod harness;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ffi::CString;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::{FileExt, MetadataExt, symlink};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use crossbeam_channel::RecvTimeoutError;
use files_sync::config::SymlinkPolicy;
use files_sync::daemon::{CycleReport, Daemon};
use files_sync::fs::commit::Quarantine;
use files_sync::fs::is_conflict_name;
use files_sync::sandbox;
use harness::Pair;
use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, RenameFlags, ResolveFlags};
use rustix::io::Errno;

/// How long the daemon must stay without a cycle to count as quiet: longer
/// than the debouncer's maximum delay (2 s) plus its retry delay (1 s).
const QUIET: Duration = Duration::from_secs(4);
/// How long the daemon may take to go quiet after the writers stop.
const SETTLE_LIMIT: Duration = Duration::from_secs(300);
/// How often (percent) a writer that picked another side's slot switches to
/// a slot of its own.
const HOME: u64 = 60;

#[derive(Debug)]
struct Config {
    secs: u64,
    threads: usize,
    slots: usize,
    pause_ms: u64,
    seed: u64,
    sandbox: bool,
}

fn env<T: FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(v) => v
            .parse()
            .unwrap_or_else(|_| panic!("{name}: cannot parse {v:?}")),
        Err(_) => default,
    }
}

impl Config {
    fn from_env() -> Config {
        let clock = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Config {
            secs: env("FSYNC_STRESS_SECS", 30),
            threads: env("FSYNC_STRESS_THREADS", 4),
            slots: env("FSYNC_STRESS_SLOTS", 8),
            pause_ms: env("FSYNC_STRESS_PAUSE_MS", 10),
            seed: env("FSYNC_STRESS_SEED", clock),
            sandbox: env::<u8>("FSYNC_STRESS_SANDBOX", 1) != 0,
        }
    }
}

/// splitmix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    /// True with probability `pct` percent.
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Hash([u8; 32]);

impl Hash {
    fn of(content: &[u8]) -> Hash {
        Hash(*blake3::hash(content).as_bytes())
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0[..6] {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// One ledger record. (Some fields are only read through `Debug`, when a
/// loss is reported.)
#[derive(Debug)]
#[allow(dead_code)]
enum Rec {
    /// Unique content `new` written at `path`. An in-place write names the
    /// content it read and replaced as `pred`. `unlinked`: the inode had no
    /// name left right after the write (design §5.10).
    Write {
        side: char,
        op: &'static str,
        path: String,
        pred: Option<Hash>,
        new: Hash,
        unlinked: bool,
    },
    /// Contents an operation took out of the tree (by path in the removed
    /// object).
    Removed {
        side: char,
        op: &'static str,
        path: String,
        hashes: Vec<(String, Hash)>,
    },
    /// Contents found beneath `to` right after a rename from `from`.
    Moved {
        side: char,
        from: String,
        to: String,
        found: Vec<(String, Hash)>,
    },
}

/// Microseconds since the first call: when a record was logged.
fn clock_us() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_micros() as u64
}

/// A writer's ledger and operation counts.
#[derive(Default)]
struct Log {
    /// Records with the time they were logged ([`clock_us`]).
    recs: Vec<(u64, Rec)>,
    /// Per operation: (done, skipped because the tree changed under it).
    ops: BTreeMap<&'static str, (u64, u64)>,
    /// Per operation and errno: how often it was skipped.
    skips: BTreeMap<(&'static str, i32), u64>,
}

/// One tree as the writers see it.
struct User {
    side: char,
    root: OwnedFd,
    scratch: OwnedFd,
    scratch_path: PathBuf,
    /// The sentinel directory the writers' absolute symlinks point into.
    outside: PathBuf,
    /// One lock per slot, among this tree's writers only.
    locks: Vec<Mutex<()>>,
}

fn open_dir(path: &Path) -> OwnedFd {
    rustix::fs::open(
        path,
        OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .unwrap()
}

const BENEATH: ResolveFlags = ResolveFlags::BENEATH
    .union(ResolveFlags::NO_SYMLINKS)
    .union(ResolveFlags::NO_MAGICLINKS);

impl User {
    fn new(side: char, root: &Path, scratch: &Path, outside: &Path, slots: usize) -> User {
        User {
            side,
            root: open_dir(root),
            scratch: open_dir(scratch),
            scratch_path: scratch.to_path_buf(),
            outside: outside.to_path_buf(),
            locks: (0..slots).map(|_| Mutex::new(())).collect(),
        }
    }

    /// The parent directory of `path` (beneath the root, no symlinks
    /// followed) and the last component.
    fn parent<'p>(&self, path: &'p str) -> io::Result<(OwnedFd, &'p str)> {
        let (dir, name) = match path.rfind('/') {
            Some(i) => (&path[..i], &path[i + 1..]),
            None => (".", path),
        };
        let fd = rustix::fs::openat2(
            &self.root,
            dir,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            BENEATH,
        )?;
        Ok((fd, name))
    }

    /// Removes the object at `name` in the scratch directory.
    fn discard(&self, name: &str) {
        let path = self.scratch_path.join(name);
        for _ in 0..100 {
            let res = match path.symlink_metadata() {
                Err(e) if e.kind() == io::ErrorKind::NotFound => return,
                Err(e) => Err(e),
                Ok(md) if md.is_dir() => fs::remove_dir_all(&path),
                Ok(_) => fs::remove_file(&path),
            };
            if res.is_ok() {
                return;
            }
            // Something is still being created inside (an unsandboxed
            // daemon writing into a directory moved out of the root).
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("cannot remove {}", path.display());
    }
}

/// Every regular file at or beneath `name` in `dir`, as (path, hash), with
/// paths starting at `prefix`. Never follows symlinks; objects that vanish
/// or change type meanwhile are left out.
fn hashes(dir: BorrowedFd, name: &CString, prefix: &str, out: &mut Vec<(String, Hash)>) {
    let Ok(st) = rustix::fs::statat(dir, name.as_c_str(), AtFlags::SYMLINK_NOFOLLOW) else {
        return;
    };
    match FileType::from_raw_mode(st.st_mode) {
        FileType::RegularFile => {
            let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let Ok(fd) = rustix::fs::openat(dir, name.as_c_str(), flags, Mode::empty()) else {
                return;
            };
            let mut content = Vec::new();
            if File::from(fd).read_to_end(&mut content).is_ok() {
                out.push((prefix.to_owned(), Hash::of(&content)));
            }
        }
        FileType::Directory => {
            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let Ok(fd) = rustix::fs::openat(dir, name.as_c_str(), flags, Mode::empty()) else {
                return;
            };
            let Ok(list) = Dir::read_from(&fd) else {
                return;
            };
            let names: Vec<CString> = list
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_owned())
                .filter(|n| n.as_bytes() != b"." && n.as_bytes() != b"..")
                .collect();
            for n in names {
                let p = format!("{prefix}/{}", String::from_utf8_lossy(n.as_bytes()));
                hashes(fd.as_fd(), &n, &p, out);
            }
        }
        _ => {}
    }
}

fn cstr(s: &str) -> CString {
    CString::new(s).unwrap()
}

/// Errors that only mean the tree changed under the operation.
fn skipped(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(
            libc::ENOENT
                | libc::ENOTDIR
                | libc::ELOOP
                | libc::EISDIR
                | libc::EEXIST
                | libc::EAGAIN
                | libc::ENOTEMPTY
        )
    )
}

/// A writer thread: random operations on random paths until `until`.
struct Writer<'u> {
    user: &'u User,
    id: usize,
    rng: Rng,
    seq: u64,
    slots: usize,
    log: Log,
}

impl Writer<'_> {
    fn run(mut self, until: Instant, pause_ms: u64) -> Log {
        while Instant::now() < until {
            self.step();
            let pause = self.rng.below(pause_ms as usize + 1);
            std::thread::sleep(Duration::from_millis(pause as u64));
        }
        self.log
    }

    /// Counts the outcome of `op`; panics on an unexpected error.
    fn count(&mut self, op: &'static str, res: &io::Result<()>) {
        let n = self.log.ops.entry(op).or_default();
        match res {
            Ok(()) => n.0 += 1,
            Err(e) if skipped(e) => {
                n.1 += 1;
                let errno = e.raw_os_error().unwrap_or(0);
                *self.log.skips.entry((op, errno)).or_default() += 1;
            }
            Err(e) => panic!("writer {}{}: {op}: {e}", self.user.side, self.id),
        }
    }

    /// One random operation on a random slot.
    fn step(&mut self) {
        let k = self.slot();
        let op = match self.rng.below(100) {
            0..20 => "rmw",
            20..32 => "append",
            32..50 => "overwrite",
            50..56 => "delete",
            56..62 => "swap",
            62..66 => "link",
            66..76 => "rename",
            76..82 => "mkdir",
            _ => "resolve",
        };
        if op == "rename" {
            let res = self.rename(k);
            return self.count(op, &res);
        }
        let _slot = self.user.locks[k].lock().unwrap();
        let path = match op {
            "rmw" | "append" | "overwrite" => self.file_path(k),
            "delete" | "link" => self.any_path(k),
            "swap" | "mkdir" => self.dir_path(k),
            _ => String::new(),
        };
        let res = match op {
            "rmw" => self.rmw(&path),
            "append" => self.append(&path),
            "overwrite" => self.overwrite(&path),
            "delete" => self.delete(&path, op),
            "swap" => self.swap(&path),
            "link" => self.link(&path),
            "mkdir" => self.mkdir(&path),
            _ => self.resolve(k),
        };
        self.count(op, &res);
        // A user whose directory is gone (or is a file or symlink now)
        // usually makes it again.
        let parent_gone = matches!(
            res.as_ref().map_err(|e| e.raw_os_error()),
            Err(Some(libc::ENOENT | libc::ENOTDIR | libc::ELOOP))
        );
        if parent_gone && path.contains('/') && self.rng.chance(60) {
            let res = self.repair(&path);
            self.count("repair", &res);
        }
    }

    /// Makes the first ancestor of `path` that is not a directory (the
    /// slot or its `sub`) one: created if absent, else swapped for a new
    /// directory.
    fn repair(&mut self, path: &str) -> io::Result<()> {
        for (i, _) in path.match_indices('/') {
            let dir = &path[..i];
            let (parent, name) = self.user.parent(dir)?;
            match rustix::fs::statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW) {
                Ok(st) if FileType::from_raw_mode(st.st_mode) == FileType::Directory => continue,
                Ok(_) => return self.put_dir("repair", dir),
                Err(Errno::NOENT) => return self.mkdir(dir),
                Err(e) => return Err(e.into()),
            }
        }
        Err(Errno::EXIST.into())
    }

    // --- Paths --------------------------------------------------------------

    /// A random slot, usually one of this side's own (even on A, odd on B):
    /// a slot busy on one side only is replaced on the other, and the other
    /// side's occasional edit then races that replace.
    fn slot(&mut self) -> usize {
        let own = usize::from(self.user.side == 'B');
        let k = self.rng.below(self.slots);
        if k % 2 != own && k ^ 1 < self.slots && self.rng.chance(HOME) {
            k ^ 1
        } else {
            k
        }
    }

    /// A path where files live: the slot itself, `f0..f2` in it, or
    /// `f0..f1` in its `sub`.
    fn file_path(&mut self, k: usize) -> String {
        match self.rng.below(100) {
            0..8 => format!("s{k}"),
            8..60 => format!("s{k}/f{}", self.rng.below(3)),
            _ => format!("s{k}/sub/f{}", self.rng.below(2)),
        }
    }

    /// A path where directories live: the slot or its `sub`.
    fn dir_path(&mut self, k: usize) -> String {
        if self.rng.chance(35) {
            format!("s{k}")
        } else {
            format!("s{k}/sub")
        }
    }

    fn any_path(&mut self, k: usize) -> String {
        if self.rng.chance(30) {
            self.dir_path(k)
        } else {
            self.file_path(k)
        }
    }

    // --- Content ------------------------------------------------------------

    /// Unique content: a header naming the writer, its sequence number and
    /// the operation, then `body`, then padding of random length (now and
    /// then over 1 MiB, past the size below which replaced files are always
    /// rehashed).
    fn content(&mut self, op: &str, body: &[u8]) -> Vec<u8> {
        self.seq += 1;
        let mut out = format!("{}{}-{} {op}\n", self.user.side, self.id, self.seq).into_bytes();
        out.extend_from_slice(body);
        let pad = if self.rng.chance(3) {
            (1 << 20) + self.rng.below(1 << 19)
        } else {
            self.rng.below(300)
        };
        let fill = b'a' + (self.seq % 26) as u8;
        out.extend(std::iter::repeat_n(fill, pad));
        out.push(b'\n');
        out
    }

    fn write_rec(
        &mut self,
        op: &'static str,
        path: &str,
        pred: Option<Hash>,
        new: &[u8],
        unlinked: bool,
    ) {
        self.log.recs.push((
            clock_us(),
            Rec::Write {
                side: self.user.side,
                op,
                path: path.to_owned(),
                pred,
                new: Hash::of(new),
                unlinked,
            },
        ));
    }

    fn removed_rec(&mut self, op: &'static str, path: &str, hashes: Vec<(String, Hash)>) {
        if !hashes.is_empty() {
            self.log.recs.push((
                clock_us(),
                Rec::Removed {
                    side: self.user.side,
                    op,
                    path: path.to_owned(),
                    hashes,
                },
            ));
        }
    }

    /// A fresh name in the scratch directory.
    fn scratch_name(&mut self) -> String {
        self.seq += 1;
        format!("{}{}-{}", self.user.side, self.id, self.seq)
    }

    // --- Operations ---------------------------------------------------------

    /// Opens the regular file at `path` for writing, in place, or creates
    /// it if nothing is there (then `false`).
    fn open_file(&self, path: &str, flags: OFlags) -> io::Result<(File, bool)> {
        let (dir, name) = self.user.parent(path)?;
        let flags = flags | OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let (fd, existed) = match rustix::fs::openat(&dir, name, flags, Mode::empty()) {
            Err(Errno::NOENT) => {
                let flags = flags | OFlags::CREATE | OFlags::EXCL;
                let mode = Mode::from_raw_mode(0o644);
                (rustix::fs::openat(&dir, name, flags, mode)?, false)
            }
            res => (res?, true),
        };
        let st = rustix::fs::fstat(&fd)?;
        if FileType::from_raw_mode(st.st_mode) != FileType::RegularFile {
            return Err(Errno::ISDIR.into());
        }
        Ok((File::from(fd), existed))
    }

    /// Reads the file and rewrites it in place with new content derived
    /// from the old.
    fn rmw(&mut self, path: &str) -> io::Result<()> {
        let (mut f, existed) = self.open_file(path, OFlags::empty())?;
        let mut old = Vec::new();
        f.read_to_end(&mut old)?;
        let new = self.content("rmw", &old[..old.len().min(64)]);
        f.write_all_at(&new, 0)?;
        f.set_len(new.len() as u64)?;
        let unlinked = f.metadata()?.nlink() == 0;
        let pred = existed.then(|| Hash::of(&old));
        self.write_rec("rmw", path, pred, &new, unlinked);
        Ok(())
    }

    /// Reads the file, then appends to it.
    fn append(&mut self, path: &str) -> io::Result<()> {
        let (mut f, existed) = self.open_file(path, OFlags::APPEND)?;
        let mut old = Vec::new();
        f.read_to_end(&mut old)?;
        let tail = self.content("append", b"");
        f.write_all(&tail)?;
        let unlinked = f.metadata()?.nlink() == 0;
        let mut new = old.clone();
        new.extend_from_slice(&tail);
        let pred = existed.then(|| Hash::of(&old));
        self.write_rec("append", path, pred, &new, unlinked);
        Ok(())
    }

    /// Moves the object prepared at `tmp` in the scratch directory to
    /// `path`: exchanged with whatever is there (which comes out, is hashed
    /// and logged as removed by `op`), or created.
    fn put(&mut self, op: &'static str, path: &str, tmp: &str) -> io::Result<()> {
        let scratch = &self.user.scratch;
        let res = self.user.parent(path).and_then(|(dir, name)| {
            match rustix::fs::renameat_with(scratch, tmp, &dir, name, RenameFlags::EXCHANGE) {
                Ok(()) => Ok(true),
                Err(Errno::NOENT) => {
                    rustix::fs::renameat_with(scratch, tmp, &dir, name, RenameFlags::NOREPLACE)?;
                    Ok(false)
                }
                Err(e) => Err(e.into()),
            }
        });
        if res.as_ref().is_ok_and(|&swapped| swapped) {
            let mut out = Vec::new();
            hashes(scratch.as_fd(), &cstr(tmp), path, &mut out);
            self.removed_rec(op, path, out);
        }
        self.user.discard(tmp);
        res.map(|_| ())
    }

    /// Replaces whatever is at `path` with a new file, atomically, without
    /// reading it first.
    fn overwrite(&mut self, path: &str) -> io::Result<()> {
        let new = self.content("overwrite", b"");
        let tmp = self.scratch_name();
        let fd = rustix::fs::openat(
            &self.user.scratch,
            tmp.as_str(),
            OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o644),
        )?;
        File::from(fd).write_all(&new)?;
        self.put("overwrite", path, &tmp)?;
        self.write_rec("overwrite", path, None, &new, false);
        Ok(())
    }

    /// Moves the object at `path` out of the tree and discards it.
    fn delete(&mut self, path: &str, op: &'static str) -> io::Result<()> {
        let (dir, name) = self.user.parent(path)?;
        let tmp = self.scratch_name();
        rustix::fs::renameat_with(
            &dir,
            name,
            &self.user.scratch,
            tmp.as_str(),
            RenameFlags::NOREPLACE,
        )?;
        let mut out = Vec::new();
        hashes(self.user.scratch.as_fd(), &cstr(&tmp), path, &mut out);
        self.removed_rec(op, path, out);
        self.user.discard(&tmp);
        Ok(())
    }

    /// A symlink target: into the sentinel directory (absolute, or relative
    /// and climbing out of the root), within the tree, or nowhere.
    fn target(&mut self, path: &str) -> String {
        let outside = self.user.outside.to_str().unwrap();
        let up = "../".repeat(path.matches('/').count() + 1);
        match self.rng.below(7) {
            0 => outside.to_owned(),
            1 => format!("{outside}/f0"),
            2 => format!("{outside}/sub"),
            3 => format!("{up}outside"),
            4 => "f0".to_owned(),
            5 => "..".to_owned(),
            _ => "nowhere".to_owned(),
        }
    }

    /// Swaps the directory at `path` for a symlink, or the symlink (or file)
    /// there for a new directory holding a file.
    fn swap(&mut self, path: &str) -> io::Result<()> {
        let (dir, name) = self.user.parent(path)?;
        let to_dir = match rustix::fs::statat(&dir, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(st) => FileType::from_raw_mode(st.st_mode) != FileType::Directory,
            Err(Errno::NOENT) => self.rng.chance(50),
            Err(e) => return Err(e.into()),
        };
        if to_dir {
            self.put_dir("swap", path)
        } else {
            let tmp = self.scratch_name();
            let target = self.target(path);
            symlink(target, self.user.scratch_path.join(&tmp))?;
            self.put("swap", path, &tmp)
        }
    }

    /// Replaces whatever is at `path` with a new directory holding a file.
    fn put_dir(&mut self, op: &'static str, path: &str) -> io::Result<()> {
        let tmp = self.scratch_name();
        let mode = Mode::from_raw_mode(0o755);
        rustix::fs::mkdirat(&self.user.scratch, tmp.as_str(), mode)?;
        let new = self.content(op, b"");
        fs::write(self.user.scratch_path.join(&tmp).join("f0"), &new)?;
        self.put(op, path, &tmp)?;
        self.write_rec(op, &format!("{path}/f0"), None, &new, false);
        Ok(())
    }

    /// Replaces whatever is at `path` with a symlink.
    fn link(&mut self, path: &str) -> io::Result<()> {
        let tmp = self.scratch_name();
        let target = self.target(path);
        symlink(target, self.user.scratch_path.join(&tmp))?;
        self.put("link", path, &tmp)
    }

    fn mkdir(&mut self, path: &str) -> io::Result<()> {
        let (dir, name) = self.user.parent(path)?;
        rustix::fs::mkdirat(&dir, name, Mode::from_raw_mode(0o755))?;
        Ok(())
    }

    /// Renames a slot to another free slot name, or a file or symlink to
    /// another free file path; logs what arrived.
    fn rename(&mut self, k: usize) -> io::Result<()> {
        let j = self.rng.below(self.slots);
        let whole = self.rng.chance(30);
        if whole && j == k {
            return Err(Errno::EXIST.into());
        }
        let (from, to) = if whole {
            (format!("s{k}"), format!("s{j}"))
        } else {
            (self.file_path(k), self.file_path(j))
        };
        if from == to || to.starts_with(&format!("{from}/")) {
            return Err(Errno::EXIST.into());
        }
        let _locks = if k == j {
            (self.user.locks[k].lock().unwrap(), None)
        } else {
            let (lo, hi) = (k.min(j), k.max(j));
            let lo = self.user.locks[lo].lock().unwrap();
            (lo, Some(self.user.locks[hi].lock().unwrap()))
        };
        let (d1, n1) = self.user.parent(&from)?;
        if !whole {
            let st = rustix::fs::statat(&d1, n1, AtFlags::SYMLINK_NOFOLLOW)?;
            if FileType::from_raw_mode(st.st_mode) == FileType::Directory {
                return Err(Errno::ISDIR.into());
            }
        }
        let (d2, n2) = self.user.parent(&to)?;
        match rustix::fs::renameat_with(&d1, n1, &d2, n2, RenameFlags::NOREPLACE) {
            Ok(()) => {}
            // Into itself: it became a directory meanwhile.
            Err(Errno::INVAL) => return Err(Errno::ISDIR.into()),
            Err(e) => return Err(e.into()),
        }
        let mut found = Vec::new();
        hashes(d2.as_fd(), &cstr(n2), &to, &mut found);
        self.log.recs.push((
            clock_us(),
            Rec::Moved {
                side: self.user.side,
                from,
                to,
                found,
            },
        ));
        Ok(())
    }

    /// Deletes a conflict copy of slot `k` (beside it, or in it).
    fn resolve(&mut self, k: usize) -> io::Result<()> {
        let dir = match self.rng.below(3) {
            0 => ".".to_owned(),
            1 => format!("s{k}"),
            _ => format!("s{k}/sub"),
        };
        let fd = rustix::fs::openat2(
            &self.user.root,
            dir.as_str(),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
            BENEATH,
        )?;
        let mine = format!("s{k}.");
        let names: Vec<String> = Dir::read_from(&fd)?
            .filter_map(|e| e.ok())
            .map(|e| String::from_utf8_lossy(e.file_name().to_bytes()).into_owned())
            .filter(|n| is_conflict_name(n.as_bytes()))
            .filter(|n| dir != "." || n.starts_with(&mine))
            .collect();
        if names.is_empty() {
            return Err(Errno::NOENT.into());
        }
        let name = &names[self.rng.below(names.len())];
        let path = if dir == "." {
            name.clone()
        } else {
            format!("{dir}/{name}")
        };
        self.delete(&path, "resolve")
    }
}

/// The initial tree, written on A by a writer: per slot a directory with
/// files and a `sub` directory, except that every fourth slot is a file and
/// every fourth slot's `sub` is a symlink out of the tree.
fn seed(user: &User, slots: usize) -> Log {
    let mut w = Writer {
        user,
        // Not a writer thread's: its content must differ from theirs.
        id: 99,
        rng: Rng(0),
        seq: 0,
        slots,
        log: Log::default(),
    };
    for k in 0..slots {
        if k % 4 == 3 {
            w.overwrite(&format!("s{k}")).unwrap();
            continue;
        }
        w.mkdir(&format!("s{k}")).unwrap();
        for f in ["f0", "f1"] {
            w.overwrite(&format!("s{k}/{f}")).unwrap();
        }
        if k % 4 == 2 {
            let tmp = w.scratch_name();
            symlink(&user.outside, user.scratch_path.join(&tmp)).unwrap();
            w.put("seed", &format!("s{k}/sub"), &tmp).unwrap();
        } else {
            w.mkdir(&format!("s{k}/sub")).unwrap();
            w.overwrite(&format!("s{k}/sub/f0")).unwrap();
        }
    }
    w.log
}

/// The sentinel directory's content, to compare before and after.
#[derive(Debug, PartialEq, Eq)]
struct Meta {
    kind: &'static str,
    ino: u64,
    mode: u32,
    nlink: u64,
    mtime_ns: i64,
    ctime_ns: i64,
    content: Option<Hash>,
}

fn snapshot(root: &Path) -> BTreeMap<String, Meta> {
    fn walk(dir: &Path, rel: &str, out: &mut BTreeMap<String, Meta>) {
        let md = dir.symlink_metadata().unwrap();
        let meta = |md: &fs::Metadata, kind, content| Meta {
            kind,
            ino: md.ino(),
            mode: md.mode(),
            nlink: md.nlink(),
            mtime_ns: md.mtime() * 1_000_000_000 + md.mtime_nsec(),
            ctime_ns: md.ctime() * 1_000_000_000 + md.ctime_nsec(),
            content,
        };
        out.insert(rel.to_owned(), meta(&md, "dir", None));
        let mut names: Vec<_> = fs::read_dir(dir).unwrap().map(|e| e.unwrap()).collect();
        names.sort_by_key(|e| e.file_name());
        for e in names {
            let rel = format!("{rel}/{}", e.file_name().to_str().unwrap());
            let md = e.path().symlink_metadata().unwrap();
            if md.is_dir() {
                walk(&e.path(), &rel, out);
            } else if md.is_file() {
                let h = Hash::of(&fs::read(e.path()).unwrap());
                out.insert(rel, meta(&md, "file", Some(h)));
            } else {
                out.insert(rel, meta(&md, "other", None));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, "", &mut out);
    out
}

/// Every regular file in the tree at `root` (after the run; nothing changes
/// any more), as hash → paths.
fn contents(root: &Path, side: char, out: &mut HashMap<Hash, Vec<String>>) {
    fn walk(dir: &Path, rel: &str, side: char, out: &mut HashMap<Hash, Vec<String>>) {
        for e in fs::read_dir(dir).unwrap() {
            let e = e.unwrap();
            let name = e.file_name().to_str().unwrap().to_owned();
            let rel = if rel.is_empty() {
                name
            } else {
                format!("{rel}/{name}")
            };
            let md = e.path().symlink_metadata().unwrap();
            if md.is_dir() {
                walk(&e.path(), &rel, side, out);
            } else if md.is_file() {
                let h = Hash::of(&fs::read(e.path()).unwrap());
                out.entry(h).or_default().push(format!("{side}:{rel}"));
            }
        }
    }
    walk(root, "", side, out);
}

/// `path` with every conflict marker (`.sync-conflict-YYYYMMDD-HHMMSS-ID7`)
/// taken out: the path it is a conflict copy of.
fn unconflict(path: &str) -> String {
    const MARKER: &str = ".sync-conflict-";
    const LEN: usize = MARKER.len() + 23;
    let mut p = path.to_owned();
    while let Some(i) = p.find(MARKER) {
        let end = (i + LEN).min(p.len());
        p.replace_range(i..end, "");
    }
    p
}

fn writers_race_the_daemon(mode: harness::Mode) {
    static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());
    let _turn = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let cfg = Config::from_env();
    eprintln!("stress: {mode:?}, {cfg:?}");
    let base = tempfile::tempdir().unwrap();
    let dir = |name: &str| {
        let p = base.path().join(name);
        fs::create_dir(&p).unwrap();
        p.canonicalize().unwrap()
    };
    let (a, b, state) = (dir("a"), dir("b"), dir("state"));
    let outside = dir("outside");
    let scratch = [dir("scratch-a"), dir("scratch-b")];

    // The sentinel: names the writers use, so a write through one of their
    // links would land on something.
    for f in ["f0", "f1", "sub/f0", "sub/f1", "s0/f0", "s0/sub/f0"] {
        let p = outside.join(f);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, format!("sentinel {f}\n")).unwrap();
    }
    let sentinel = snapshot(&outside);

    let users = [
        User::new('A', &a, &scratch[0], &outside, cfg.slots),
        User::new('B', &b, &scratch[1], &outside, cfg.slots),
    ];
    let mut logs = vec![seed(&users[0], cfg.slots)];
    let sandboxed = cfg.sandbox && sandbox::abi().is_some();
    if cfg.sandbox && !sandboxed {
        eprintln!("stress: landlock is not supported here; the daemon runs without a sandbox");
    }
    let writable = [a.clone(), b.clone(), state.clone()];
    let restrict = move || {
        if sandboxed {
            let dirs: Vec<&Path> = writable.iter().map(|p| p.as_path()).collect();
            sandbox::restrict(&dirs).expect("landlock");
        }
    };
    let pair = Pair::open_at(&a, &b, &state, SymlinkPolicy::Links)
        .quarantine_grace(Quarantine::DEFAULT_GRACE);
    // Served replicas commit on their servers' threads: start the servers
    // from a sandboxed thread.
    let restrict_servers = restrict.clone();
    let mut pair = std::thread::spawn(move || {
        restrict_servers();
        pair.over(mode)
    })
    .join()
    .unwrap();
    pair.sync();
    let (tx, reports) = crossbeam_channel::unbounded();
    let daemon = Daemon::new().reports(tx);
    let t0 = Instant::now();
    let until = t0 + Duration::from_secs(cfg.secs);
    let mut cycles: Vec<CycleReport> = Vec::new();
    let stats = std::thread::scope(|s| {
        let (stop, stopped) = crossbeam_channel::bounded::<()>(1);
        let (ra, rb) = (&mut pair.a.replica, &mut pair.b.replica);
        let running = s.spawn(move || {
            restrict();
            daemon.run(ra, rb, &stopped)
        });
        let pause_ms = cfg.pause_ms;
        let writers: Vec<_> = users
            .iter()
            .flat_map(|user| (0..cfg.threads).map(move |id| (user, id)))
            .map(|(user, id)| {
                let w = Writer {
                    user,
                    id,
                    rng: Rng(cfg.seed ^ (((user.side as u64) << 32) | id as u64)),
                    seq: 0,
                    slots: cfg.slots,
                    log: Log::default(),
                };
                s.spawn(move || w.run(until, pause_ms))
            })
            .collect();
        for w in writers {
            logs.push(w.join().unwrap());
        }
        let stopped_at = Instant::now();
        eprintln!("stress: writers stopped after {:?}", stopped_at - t0);
        cycles.extend(reports.try_iter());
        loop {
            match reports.recv_timeout(QUIET) {
                Ok(r) => cycles.push(r),
                Err(RecvTimeoutError::Timeout) => break,
                // The daemon failed; its error is reported below.
                Err(RecvTimeoutError::Disconnected) => break,
            }
            assert!(
                stopped_at.elapsed() < SETTLE_LIMIT,
                "the daemon did not go quiet within {SETTLE_LIMIT:?}"
            );
        }
        eprintln!(
            "stress: daemon quiet {:?} after the writers stopped",
            stopped_at.elapsed() - QUIET
        );
        drop(stop);
        running.join().unwrap().expect("daemon failed")
    });

    // What happened.
    let mut ops: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    for log in &logs {
        for (op, (done, skip)) in &log.ops {
            let n = ops.entry(op).or_default();
            n.0 += done;
            n.1 += skip;
        }
    }
    eprintln!("stress: operations (done, skipped): {ops:?}");
    let mut skips: BTreeMap<(&str, i32), u64> = BTreeMap::new();
    for log in &logs {
        for (k, n) in &log.skips {
            *skips.entry(*k).or_default() += n;
        }
    }
    eprintln!("stress: skips by errno: {skips:?}");
    let retried: usize = cycles.iter().map(|c| c.report.retried).sum();
    eprintln!("stress: daemon {stats:?}, {retried} steps retried");
    let mut errors: BTreeMap<String, usize> = BTreeMap::new();
    for c in &cycles {
        for (_, e) in &c.report.errors {
            // The error kind, without the path.
            let kind = e.rsplit(": ").next().unwrap_or(e).to_owned();
            *errors.entry(kind).or_default() += 1;
        }
    }
    eprintln!("stress: per-path errors in daemon cycles: {errors:?}");

    // Convergence: at most 3 full cycles until one applies nothing.
    let mut quiet = None;
    for round in 1..=3 {
        let r = pair.try_sync();
        eprintln!(
            "stress: final cycle {round}: applied {}, conflicts {}, errors {:?}, unresolved {:?}",
            r.applied,
            r.conflicts.len(),
            r.errors,
            r.unresolved
        );
        if r.applied == 0 && r.is_converged() {
            quiet = Some(round);
            break;
        }
    }
    assert!(quiet.is_some(), "no quiet cycle within 3 full cycles");
    // The quarantine expires; then no reserved name may be left.
    std::thread::sleep(Quarantine::DEFAULT_GRACE + Duration::from_millis(100));
    pair.assert_converged();

    // No escape.
    let now = snapshot(&outside);
    let changed: Vec<_> = sentinel
        .keys()
        .chain(now.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|k| sentinel.get(*k) != now.get(*k))
        .map(|k| (k, sentinel.get(k), now.get(k)))
        .collect();
    assert!(
        changed.is_empty(),
        "the sentinel directory changed (path, before, after): {changed:#?}"
    );
    for s in &scratch {
        let left: Vec<_> = fs::read_dir(s).unwrap().collect();
        assert!(left.is_empty(), "left in {}: {left:?}", s.display());
    }

    // No loss.
    let mut timed: Vec<&(u64, Rec)> = logs.iter().flat_map(|l| &l.recs).collect();
    timed.sort_by_key(|(t, _)| *t);
    let recs: Vec<&Rec> = timed.iter().map(|(_, r)| r).collect();
    let mut gone: HashSet<Hash> = HashSet::new();
    let mut places: HashMap<Hash, BTreeSet<String>> = HashMap::new();
    for r in &recs {
        match r {
            Rec::Write {
                path, pred, new, ..
            } => {
                gone.extend(*pred);
                places.entry(*new).or_default().insert(path.clone());
            }
            Rec::Removed { hashes, .. } => gone.extend(hashes.iter().map(|(_, h)| *h)),
            Rec::Moved { found, .. } => {
                for (p, h) in found {
                    places.entry(*h).or_default().insert(p.clone());
                }
            }
        }
    }
    let mut found: HashMap<Hash, Vec<String>> = HashMap::new();
    contents(&a, 'A', &mut found);
    contents(&b, 'B', &mut found);
    let (mut writes, mut live, mut windows) = (0, 0, 0);
    let mut lost = Vec::new();
    for r in &recs {
        let Rec::Write { new, unlinked, .. } = r else {
            continue;
        };
        writes += 1;
        if gone.contains(new) {
            continue;
        }
        if *unlinked {
            windows += 1;
            continue;
        }
        live += 1;
        let ok = found.get(new).is_some_and(|at| {
            at.iter()
                .any(|p| places[new].contains(&unconflict(&p[2..])))
        });
        if !ok {
            lost.push((r, found.get(new)));
        }
    }
    let copies = found
        .values()
        .flatten()
        .filter(|p| p.contains(".sync-conflict-"))
        .count();
    eprintln!(
        "stress: {writes} writes, {live} still live at the end, {windows} into unlinked inodes \
         (design §5.10, exempt); {copies} conflict copies in the trees"
    );
    if !lost.is_empty() {
        for (r, at) in lost.iter().take(20) {
            eprintln!("LOST: {r:?}, found at {at:?}; related records (µs, record):");
            if let Rec::Write { path, new, .. } = r {
                for (t, h) in &timed {
                    let related = match h {
                        Rec::Write {
                            path: p,
                            pred,
                            new: n,
                            ..
                        } => p == path || n == new || *pred == Some(*new),
                        Rec::Removed {
                            path: p, hashes, ..
                        } => path.starts_with(p.as_str()) || hashes.iter().any(|(_, x)| x == new),
                        Rec::Moved {
                            from, to, found, ..
                        } => {
                            path.starts_with(from.as_str())
                                || path.starts_with(to.as_str())
                                || found.iter().any(|(_, x)| x == new)
                        }
                    };
                    if related {
                        eprintln!("    {t:>10} {h:?}");
                    }
                }
            }
        }
        panic!("{} of {live} live writes were lost", lost.len());
    }
}

both_modes! {
    #[ignore = "long; cargo test --release --test stress -- --ignored"]
    writers_race_the_daemon,
}
