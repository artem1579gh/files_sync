# files_sync: design

A race-free, two-way file synchronizer for Linux, written in Rust. It aims to be "syncthing-style sync with rsync symlink semantics".

Task breakdown: [`tasks.md`](tasks.md). Section numbers (§N.M) are referenced from there.

---

## 1. Problem and requirements

**Two variants:**
- **Simplified:** two-way sync between two local directories.
- **Production:** two-way sync between two directories over the network.

**Requirements for both:**
- **Full symlink support:** every symlink handling mode that rsync offers (§4).
- **No races:** files may be modified, created, deleted or renamed by other processes while sync is running. Concurrent activity must never cause:
  - silent data loss;
  - a write outside the replica root;
  - a corrupted (torn) file.
- **Conflict resolution:** automatic and simple. Both versions are kept, and the loser is saved as a `sync-conflict` copy.

**Prior art:**
- **unison** and **syncthing** are the closest, but have no real symlink support.
- **rsync** supports symlinks fully, but is one-way and still has TOCTOU races (a long CVE history around symlink and directory swaps).

**Decisions made:**
- **Platform:** Linux only. We rely on `openat2`, `renameat2`, `O_TMPFILE`, `statx`, `inotify` and `F_SETLEASE`.
- **Scope:** local first, network-ready. The engine talks to replicas only through the `Replica` trait (§7), and all compare-and-swap (CAS) logic lives inside the replica. A remote replica is then just an RPC stub.
- **Run modes:** a continuous `daemon` driven by inotify, and a one-shot `sync --once`. Both share the same engine.

**Environment caveat:** replica roots must be on ext4, xfs, btrfs or tmpfs. WSL `/mnt/c` (drvfs/9p) does not support `O_TMPFILE`, `RENAME_EXCHANGE` or leases, and its inotify is unreliable. `fs/caps.rs` checks for these features at startup and either falls back or refuses to run.

---

## 2. Architecture

**Layout:** one crate with both a library and a binary. Split into a workspace only in the network phase.

```
src/main.rs        binary entry, calls into lib
src/lib.rs
src/cli.rs         clap: init, sync --once, daemon, status  [serve, --remote later]
src/config.rs      pair config: roots, ReplicaIds, per-replica symlink policy flags
src/daemon.rs
src/fs/      root.rs     Root + openat2 parent resolution, fd-based readdir
             stat.rs     statx Fingerprint
             commit.rs   CAS create/replace/delete/symlink/mkdir/rmdir   <- the ONLY code that mutates replicas
             tmpname.rs  .~fsync.<id> naming
             lease.rs    F_SETLEASE helpers
             caps.rs     filesystem feature checks (creates and removes its own .~fsync.probe.* files:
                         the one sanctioned mutation outside commit.rs)
             hooks.rs    cfg(test) race-injection points
src/index/   entry.rs, vv.rs (version vectors), store.rs (redb), journal.rs (intent log)
src/symlink/ policy.rs, safety.rs (port of rsync unsafe_symlink), munge.rs
src/scan/    scanner.rs, hasher.rs
src/watch/   inotify.rs, debounce.rs
src/replica/ mod.rs (trait), local.rs     [remote.rs, proto/ in the network phase]
src/engine/  reconcile.rs, plan.rs, executor.rs, conflict.rs
tests/       harness/, attack.rs, symlink_matrix.rs, stress.rs, crash.rs
```

**Crates (one choice for each need):**

| Need | Crate |
|---|---|
| syscalls (openat2, renameat2, statx, linkat, symlinkat, inotify) | `rustix` (feature `fs`; inotify is `rustix::fs::inotify`) |
| F_SETLEASE / F_GETLEASE | `libc` |
| hashing | `blake3` |
| index and journal store | `redb` (pure Rust, ACID) |
| serialization | `serde` + `postcard` |
| config file | `toml` |
| CLI | `clap` (derive) |
| threads and channels | std threads + `crossbeam-channel`. **No tokio**: the work is blocking syscalls and hashing. |
| logging | `tracing` + `tracing-subscriber` |
| errors | `thiserror` (lib), `anyhow` (bin) |
| timestamps in conflict names | `jiff` |
| self-sandbox | `landlock` (optional) |
| network (later) | `rustls` (blocking, over std TcpStream) |
| dev-only | `tempfile`, `proptest` |

**State directory:** `$XDG_STATE_HOME/fsync/<pair>/<replica>.redb` holds the index and journal. It lives outside the replica roots.

**Temp files:** they always live in the same directory as their target (same filesystem, same parent dirfd). They use the reserved prefix `.~fsync.`, which the scanner and watcher always ignore.

---

## 3. Data model

```rust
struct Entry {
    kind: Kind,
    mode: u32,          // permission bits only; setuid/setgid stripped
    mtime_ns: i64,      // synced; set with futimens on the temp fd before commit
    vv: VersionVector,  // SmallVec<(ReplicaId u64, counter u64)>
    seq: u64,           // local change sequence number, for incremental index exchange
    local: LocalMeta,   // NEVER sent to peers
}
enum Kind {
    File { size: u64, hash: [u8; 32] },
    Dir,
    Symlink { target: Vec<u8> },          // canonical (unmunged) target bytes
    Tombstone,
    Unmanaged(UnmanagedReason),           // IgnoredLink | Dangling | Loop | Special
}
struct LocalMeta { dev, ino, ctime_ns, mnt_id, raw_target: Option<Vec<u8>>, via_link: Option<LinkInfo>, racy: bool }
```

- **Paths:** index keys are raw path bytes relative to the root, never UTF-8 `String`.
- **Local change:** the replica's counter becomes `max(all counters) + 1`, as in syncthing.
- **Rehash shortcut:** a file is not rehashed if (ino, size, mtime, ctime) are unchanged.
- **Racily clean entries:** if ctime is within one timestamp tick of the scan start, the entry is marked `racy` and is always rehashed on the next scan. Git uses the same rule.
- **Tombstones:** a deletion keeps its version vector. A tombstone is garbage-collected when every known replica has an equal version vector and a retention period has passed (default 30 days).
- **`Unmanaged` entries:** these are **not** deletions. The peer keeps its copy, and an incoming change to that path is skipped with a warning. We never overwrite an unmanaged object.

---

## 4. Symlink semantics

### 4.1 Guiding principle

For each path, the two-way result must equal running `rsync <opts>` in the direction the version vectors choose. On quiet trees that means A→B, then B→A.

### 4.2 "Unsafe" links

This is a lexical check, ported from rsync's `unsafe_symlink()`. A link is unsafe if:
- its target is absolute, or
- walking its `..` components, starting from the link's own directory (expressed relative to the root), ever goes above depth 0.

Both sides use the same relative paths, so the classification is symmetric. **Security never depends on this check.** Escapes are prevented by the kernel through `RESOLVE_BENEATH` (§5.1).

### 4.3 Policy for each rsync option

| rsync option | Indexed as | Applied on the peer | Incoming change for this path |
|---|---|---|---|
| default (no `-l`) | `Unmanaged(IgnoredLink)` | nothing | skipped with a warning; the path is blocked |
| `--links` / `-l` | `Symlink{target}`, bytes copied verbatim (relative or absolute), dangling allowed | symlinkat to a temp name, then rename (§5.5) | normal version-vector handling |
| `--copy-links` / `-L` | the referent as File or Dir, with `via_link` set | a real file or directory | **the link is replaced by a real object**, as rsync's receiver does. We never write through a followed link. The option `followed_write=conflict` keeps the link and writes a conflict copy instead. |
| `--copy-unsafe-links` | unsafe links as with `-L`, safe links as with `--links` | same | same |
| `--safe-links` | unsafe links → `Unmanaged(IgnoredLink)`, safe links → `Symlink` | same | same |
| `--munge-links` | **per-replica setting.** The index stores the canonical, unmunged target; `local.raw_target` holds the bytes on disk. | written as `/rsyncd-munged/` + target | — |
| `--copy-dirlinks` / `-k` | links to directories followed as Dir, other links as with `--links` | same | same |
| `--keep-dirlinks` / `-K` | **per-replica setting.** A link to a directory whose peer entry is a real Dir is "adopted": indexed as Dir and traversed. | — | **the only write-through case.** The link target is opened once as a dirfd and its (dev, ino) is pinned at adoption; children are resolved beneath it. In-tree targets are allowed by default; out-of-tree targets need `--keep-dirlinks-unsafe`. |

### 4.4 Munge round trip

`"/rsyncd-munged/x"` on disk ↔ `x` canonical. Munging always prepends the prefix, and unmunging strips exactly one prefix. A canonical target that already starts with `/rsyncd-munged/` is therefore munged twice, so the mapping is a bijection.

### 4.5 Edge cases

- **Reading through a followed link:**
  - Run `readlinkat` before and after reading the referent. If the link was retargeted in between, the read is unstable (§5.2).
  - In-tree referents are opened with `RESOLVE_BENEATH`. Out-of-tree referents are opened with `RESOLVE_NO_MAGICLINKS` and are **read-only**: we never write to them.
- **A followed referent changes:**
  - In-tree referents are already watched.
  - Out-of-tree referents get extra inotify watches.
  - A periodic full rescan (default every 10 minutes) is the backstop.
  - If a referent is itself a synced path, both paths propagate (aliasing).
- **Loops:** keep the set of directory (dev, ino) pairs on the current traversal stack. A followed directory link that resolves to an ancestor becomes `Unmanaged(Loop)`. A diamond (two links to the same non-ancestor directory) is allowed and copied twice.
- **Dangling links under a following policy:** `Unmanaged(Dangling)`, like rsync's "symlink has no referent". This is not a tombstone.
- **Symlink-to-directory vs real directory** (without `-k`/`-K`):
  - If one version vector dominates, it is a plain type change (§6.3).
  - If they are concurrent, precedence for keeping the name is Dir > File > Symlink, and the loser is renamed to a conflict name.

---

## 5. Race-freedom (the core)

### 5.1 Root and path resolution

- `Root` holds the root dirfd (`O_PATH | O_DIRECTORY`).
- Every operation re-resolves its **parent** directory with:
  `openat2(rootfd, parent, O_PATH|O_DIRECTORY, RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_XDEV)`.
  No `O_NOFOLLOW` on directory opens: with `O_PATH` (or `O_DIRECTORY`) it makes the kernel open a trailing symlink as itself and fail with `ENOTDIR`, whereas without it `RESOLVE_NO_SYMLINKS` rejects every symlink on the way, the trailing one included, with `ELOOP`. Nothing is followed either way.
  It then acts with `*at(parentfd, name)`, where `name` is a single path component. **No multi-component path string is ever passed to the kernel.**
- A directory swapped for a symlink makes the resolution fail with `ELOOP` (or `EXDEV`). That counts as an unstable path and triggers a rescan. This closes the whole rsync CVE class of "swap a dir for a symlink mid-transfer".
- **Residual case: a directory moved out of the root.** `mv root/a /tmp/x` while we hold a dirfd for `a` puts our write into the moved directory. This is not privilege escalation: an attacker can only redirect writes into a directory they could already move. Mitigation: after each commit, re-resolve the parent and compare (dev, ino); a mismatch triggers a rescan. The test invariant is **"never create anything in a directory that was never inside the root."**

### 5.2 Read path: stable reads

1. `openat2(parentfd, name, O_RDONLY|O_NOFOLLOW|O_NOATIME)`. Retry without `O_NOATIME` on EPERM.
2. `statx` the fd → fingerprint F1.
3. Stream the content through blake3, and to the peer if it is being sent.
4. `statx` the fd again → F2. Then `fstatat(parentfd, name, AT_SYMLINK_NOFOLLOW)` → F3.
5. All of these must match: ino, size, mtime and ctime between F1 and F2, and ino between F2 and F3. Otherwise the read is **unstable**: retry three times with backoff, then postpone and mark the path dirty.
6. The receiving side recomputes the hash while writing and rejects a mismatch, so a torn copy is never committed.

### 5.3 Write path: CAS commit (`fs/commit.rs`)

**Step 1. Journal.** Append `Intent{id, op, parent, name, tmp, expected}` and commit the redb transaction (§5.8).

**Step 2. Temp file.**
- `openat(parentfd, ".", O_TMPFILE|O_WRONLY, mode)`.
- Write the content while hashing it, then `fchmod`, `futimens` (the remote mtime) and `fsync`.
- Link the inode in as `.~fsync.<id>`:
  - first try `linkat(fd, "", parentfd, tmp, AT_EMPTY_PATH)`;
  - fall back to `linkat(AT_FDCWD, "/proc/self/fd/N", parentfd, tmp, AT_SYMLINK_FOLLOW)`;
  - on filesystems without `O_TMPFILE`, use an `O_CREAT|O_EXCL` named temp file.
- Keep the fd open; this pins the new inode **N**.

**Step 3. Create** (expected state: absent).
- `renameat2(parentfd, tmp, parentfd, name, RENAME_NOREPLACE)`.
- `EEXIST` means a concurrent create happened. Unlink the temp file (after checking it is still N) and return `PreconditionFailed`. The content still exists on the source side, and the next round resolves it as a conflict.

**Step 4. Replace** (expected state: the indexed entry).
- **(a) Pin the old inode.** `openat2(parentfd, name, O_PATH|O_NOFOLLOW)` pins the old inode **O**. Check `statx(O)` against the index: ino, ctime, mtime, size and kind must match exactly. If they don't, unlink the temp file and return `PreconditionFailed`.
- **(b) Optional lease** (T18). Take `F_SETLEASE F_WRLCK` on an `O_RDONLY` fd for O. It is granted only if nobody else has the file open, including writable mmaps.
- **(c) Exchange.** `renameat2(parentfd, tmp, parentfd, name, RENAME_EXCHANGE)`. In one atomic step, N goes to `name` and the old occupant goes to `tmp`.
- **(d) Verify what came out at `tmp`.**
  - Its ino must equal O, and mtime and size must equal the expected values.
  - Also rehash it if the entry was `racy`, or if it is small (under 1 MiB).
  - **Do not compare ctime here: rename itself bumps ctime** on ext4, xfs, btrfs and tmpfs.
  - Any write between (a) and (c) sets mtime to the current time, which differs from the indexed value. Any other inode at `tmp` means the file was replaced concurrently.
  - If a lease was taken, `F_GETLEASE` must still return `F_WRLCK`.
- **(e) If verification fails, undo.**
  - Exchange again, which puts the user's version back at `name` and ours at `tmp`.
  - Confirm that the object coming out is N with unchanged mtime and size, then discard it.
  - If even that check fails, both objects are user data: keep the outsider as `name.sync-conflict-…` using `RENAME_NOREPLACE`.
  - In every case, **nothing is lost**, and the outcome is `PreconditionFailed` or `Preserved{conflict}`.
- **(f) If verification succeeds, quarantine the old inode.**
  - Rename `.~fsync.<id>` to `.~fsync.old.<id>`.
  - Unlink it only once a write lease can be taken (no fds or mmaps remain) or a grace period (2 × debounce) has passed with mtime and size unchanged.
  - If it changed meanwhile (a writer still held an fd to it), turn it into a conflict copy.

**Step 5. After commit.**
- `fsync(parentfd)`.
- `statx` the name and require ino == N with the same mtime and size. ctime will have changed because of the rename; we accept that and record it.
- Re-resolve the parent and compare its (dev, ino) (§5.1).
- In **one** redb transaction, write the index entry (local meta = the fingerprint we produced) and mark the journal intent Done.
- The resulting inotify event then finds the index already up to date, so **there is no echo**.

### 5.4 Directories

`mkdirat(parentfd, name, mode)` directly on the name; `EEXIST` means `PreconditionFailed`.

### 5.5 Symlinks

`symlinkat(target, parentfd, tmp)`, then the same `NOREPLACE` or `EXCHANGE` commit. Verification compares the readlink bytes plus ino.

### 5.6 Deletes

1. Pin the object with `O_PATH|O_NOFOLLOW` and check it against the index.
2. `renameat2(name → .~fsync.del.<id>, RENAME_NOREPLACE)`.
3. Verify ino, mtime and size, as in §5.3 step 4(d).
4. If verification succeeds: unlink, or quarantine as in §5.3 step 4(f).
5. If verification fails: rename it back with `NOREPLACE`. If the name was taken meanwhile, use a conflict name.

### 5.7 Directory deletes

- Delete children first, deepest first, each with its own CAS.
- Then run `unlinkat(parentfd, name, AT_REMOVEDIR)`. **rmdir is an atomic emptiness check.**
- `ENOTEMPTY` means a child appeared concurrently. Abort and bump the directory's version vector, which resurrects it, so the new child and its directory propagate to the peer.

### 5.8 Journal and crash recovery

The journal is an intent log in the same redb file. At startup, replay unfinished intents:
- `TempWritten`: unlink the temp file if it still matches the recorded ino.
- `Exchanged`: run the §5.3 step 4(d) verification on whatever is at `tmp`, then take step (e) or (f).
- `.~fsync.*` names that are not in the journal: leave them alone and log them. The journal is always written first, so they are not ours.

A crash between the filesystem commit and the index update is harmless. The rescan sees the same content on both sides with concurrent version vectors, and those merge silently (false-conflict suppression).

### 5.9 Watcher (daemon mode)

- **Adding watches:** `inotify_add_watch("/proc/self/fd/<dirfd>")`. The magic link resolves to the pinned inode, so there is no path race.
- **Mask:** `CREATE, DELETE, MODIFY, CLOSE_WRITE, MOVED_FROM, MOVED_TO, ATTRIB, DELETE_SELF, MOVE_SELF, ONLYDIR, EXCL_UNLINK`.
- **New directories:** add the watch **first**, then scan the directory recursively, so children created before the watch existed are still found.
- **Bookkeeping:** keep a map from watch descriptor → (dev, ino) → path. `MOVE_SELF` marks the parent and the whole subtree dirty.
- **Queue overflow:** `IN_Q_OVERFLOW` triggers a full rescan. If the watch limit is hit, fall back to periodic polling.
- **Debounce:** collect dirty paths, then flush after 200 ms of quiet or 2 s at most.
- **Backstop:** a periodic full rescan (default 10 minutes).
- **Testability:** the event source is a trait, so tests can inject synthetic events, including overflow.

### 5.10 Remaining windows, stated honestly

| Window | Mitigation |
|---|---|
| A write that preserves mtime (`utimensat`) between the precondition check and the exchange | Rehash racy and small files. Large non-racy files have a theoretical gap. |
| A writer holding an fd or mmap to the old inode after the exchange | Lease check plus quarantine with a grace period; changes become a conflict copy. |
| Leases need the caller to own the file (or CAP_LEASE) and a local filesystem | Fall back to the grace timer. |
| Hardlinks | We never write in place, so other links keep the old content. Matches rsync without `-H`; `-H` may come later. |
| A directory moved out of the root while we hold its fd | Post-commit (dev, ino) check of the parent (§5.1). |

---

## 6. Reconciliation

### 6.1 Decision table

Take the union of paths over both indexes. For each path, given entries `ea` and `eb`:

| Case | Action |
|---|---|
| present on one side only | push it (a tombstone only if the peer has a live entry) |
| `Unmanaged` on either side | skip with a warning |
| version vectors equal | no-op. If the content differs anyway, the index is inconsistent: rescan the path. |
| `a` dominates `b` | apply a → B with precondition = B's indexed entry |
| concurrent, same content (kind, hash or target, mode) | merge the version vectors, no-op |
| concurrent, tombstone vs modification | the modification wins (resurrect) |
| concurrent, otherwise | **conflict** |

### 6.2 Conflicts

- **Winner:** the newer mtime; a tie goes to the higher ReplicaId. For type conflicts, Dir > File > Symlink.
- **Loser:** renamed on its own replica to `stem.sync-conflict-YYYYMMDD-HHMMSS-<ID7>.ext`, where `<ID7>` is the first 7 characters of the replica ID. Directories, symlinks and names without an extension get no extension split.
- The conflict copy gets a fresh version vector and syncs to both sides like any other file. The winner gets the merged version vector plus a bump.

### 6.3 Ordering

- Creates and updates: depth ascending (directories before their children).
- Deletes: depth descending (children before their directory).
- A type change becomes a delete in the delete phase plus a create in the create phase.
- A directory delete is skipped (the directory is resurrected) if the deleting side has any live descendant not dominated by the tombstone.

### 6.4 Convergence loop

Any `PreconditionFailed` marks the path dirty. The loop then rescans the dirty paths on both replicas and runs another round, up to 5 rounds per sync cycle. Paths still unresolved wait for the next cycle.

Rename detection by inode or hash is an optional later optimization.

---

## 7. Replica trait (network-ready)

```rust
trait Replica {
    fn id(&self) -> ReplicaId;
    fn scan(&mut self, scope: Scope) -> Result<ScanStats>;               // replica-side: updates own index, bumps VVs
    fn changes_since(&self, seq: u64) -> Result<Vec<(RelPath, Entry)>>;  // Entry without LocalMeta on the wire
    fn open_read(&self, p: &RelPath, expect: &Entry) -> Result<Box<dyn ContentReader>>; // stability checked at EOF
    fn apply(&mut self, op: Op, pre: Precondition, content: Option<&mut dyn Read>) -> Result<Outcome>;
    fn watch(&mut self) -> Option<Receiver<Hint>>;
}
enum Op { WriteFile{meta, hash}, Mkdir{mode}, Symlink{target}, Delete, Rmdir, RenameToConflict{to}, SetMeta{mode, mtime} }
enum Precondition { Absent, Matches{ kind, hash_or_target, vv } }  // the replica maps it to its own physical fingerprint
enum Outcome { Applied(Entry), PreconditionFailed(Option<Entry>), Preserved{ conflict: RelPath } }
```

**All CAS logic runs inside `LocalReplica::apply`, next to the files.** The engine never touches the filesystem. A `RemoteReplica` is a postcard-framed RPC stub around the same calls, so the network version inherits race-freedom unchanged.

### 7.1 Network phase (sketch)

- **Transport:** TCP plus `rustls` with mutual TLS. Device ID = hash of the certificate, as in syncthing.
- **Server:** a `serve` process wraps a `LocalReplica`.
- **Index exchange:** incremental, by `seq`.
- **Content:** streamed in chunks. Block lists (128 KiB blake3 blocks) for delta transfer come later.

---

## 8. Milestones

| M | Content | Tasks |
|---|---|---|
| M0 | scaffolding, CLI, config, feature checks | T01–T02 |
| M1 | `fs/` primitives plus race injection (riskiest, so first) | T03–T07 |
| M2 | index, version vectors, symlink classification, scanner | T08–T10 |
| M3 | replica, reconciler, executor, `sync --once`: **simplified variant done** | T11–T14 |
| M4 | journal and crash recovery | T15 |
| M5 | daemon | T16 |
| M6 | full symlink matrix | T17 |
| M7 | hardening: leases, GC, landlock, stress test | T18–T19 |
| M8 | network: **production variant** | T20–T22 |

---

## 9. Verification strategy

- **Unit tests:** every commit path in `fs/commit.rs`.
- **Deterministic race injection.** `fs::hooks::point("before_exchange")` and similar exist only under `cfg(test)` or the `hooks` feature. Tests register closures that mutate the filesystem at exactly that moment:
  - swap a directory for a symlink to `/tmp/outside`;
  - write between the precondition check and the exchange;
  - replace the file between the precondition check and the exchange;
  - hold an fd open and write after the exchange;
  - create a child just before rmdir.

  Each test asserts no loss and no escape.
- **proptest:**
  - the rsync `unsafe_symlink` table plus random targets;
  - munge/unmunge is a bijection;
  - version-vector comparison is a partial order;
  - model-based random operation sequences on A and B, interleaved with syncs, checked against a reference model.
- **Harness (`tests/harness/`):** two tempdirs, a scenario DSL, and `assert_converged` (trees equal after policy normalization, version vectors equal). It is generic over `Replica`, so the network phase reruns it over loopback.
- **Symlink matrix:**
  - policy × {safe relative, unsafe relative, absolute, dangling, directory link, loop, munged} × direction;
  - a differential test against real `rsync` (when installed) for the one-directional cases.
- **Crash tests:** a forked child calls `abort()` at hook point N; restart and check the invariants. Sweep over every N.
- **Stress test** (`tests/stress.rs`, `#[ignore]`).
  - *Setup:* N writer threads per tree perform read-modify-write, blind overwrite, append, delete, rename, directory↔symlink swaps and symlinks to `/tmp/outside`, while the daemon syncs. Every write is unique and logged to a ledger `(replica, path, pred_hash, new_hash)`.
  - **Invariant: no loss.** Every `new_hash` that was not superseded (never another entry's `pred_hash`, and not deleted by a logged delete) exists at the end in a tree, either at its path or as a conflict copy of it.
  - **Invariant: no escape.** Landlock limits writes to the roots and the state directory, and sentinel directories outside the roots are snapshotted and must be unchanged.
  - **Invariant: convergence.** Once the writers stop, at most 3 rounds produce zero actions, the trees are identical, the version vectors are equal, and no `.~fsync.*` files remain after the quarantine expires.
