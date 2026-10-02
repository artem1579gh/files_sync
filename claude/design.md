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
- **Wire vs. stored form:** `Entry`'s serde form is the wire form; `local` is `#[serde(skip)]`. The store persists `(Entry, LocalMeta)` together. `LinkInfo { ino, ctime_ns, raw_target, out_of_tree }` describes the followed link behind a `via_link` entry. `mode` keeps the sticky bit (`st_mode & 0o1777`, the same mask the commit code uses).
- **Version vectors** are canonical: sorted by replica, no zero counters, every counter ≤ 2^62 (enforced when decoding, so `bump` cannot overflow). `compare` gives `Ord4 { Equal, Dominates, Dominated, Concurrent }`.
- **Local change:** the replica's counter becomes `max(all counters) + 1`, as in syncthing. `bump_after(id, floor)` also lifts it above `floor`; the store keeps the largest counter ever stored (`max_counter`, a Lamport clock) for that purpose.
- **Store (redb):** `entries` (path → postcard `(Entry, LocalMeta)`), `by_seq` (seq → path, exactly one row per entry at its current seq), `meta` (schema version, replica ID, `next_seq`, `max_counter`), `intents` (the commit journal, id → postcard `Intent`, §5.8). Every put gets a fresh seq, starting at 1, so `changes_since(0)` returns everything.
- **Rehash shortcut:** a file is not rehashed if (ino, size, mtime, ctime) are unchanged (and, for a followed link, the link is the same inode with the same target).
- **Racily clean entries:** if ctime is within the racy window of the scan start (or later), the entry is marked `racy` and is always rehashed on the next scan. Git uses the same rule. The window defaults to 1 s (`scan::DEFAULT_RACY_WINDOW`), far above a kernel timestamp tick, to absorb the coarse clock's lag.
- **What counts as a change (scanner):** kind, content hash or target, and mode; plus mtime for **files only**. A directory's mtime moves with every child and a symlink's is set when it is created, so for those an mtime difference alone is not a change; their stored `mtime_ns` is the one seen at the last logical change (used only to pick a conflict winner). A logical change gets `vv.bump_after(local, max_counter)` and a new seq. A change to `LocalMeta` alone (new inode with the same content, ctime, `racy`) is written **in place with the same seq** (`WriteTxn::put_local`), so peers never re-fetch it.
- **Scanner errors:** a path that changes while it is scanned (unstable read, symlink swapped into a path, name replaced between two steps) keeps its entry and its whole subtree unchanged and is reported dirty. Other per-path errors (`EACCES`, a mount point inside the root, which `RESOLVE_NO_XDEV` refuses) do the same and are reported as errors. Every index write checks that the entry still has the seq the scan read; otherwise the path is reported dirty.
- **Tombstones:** a deletion keeps its version vector. A tombstone is garbage-collected when every known replica has an equal version vector and a retention period has passed (default 30 days).
- **`Unmanaged` entries:** these are **not** deletions. The peer keeps its copy, and an incoming change to that path is skipped with a warning. We never overwrite an unmanaged object.

---

## 4. Symlink semantics

### 4.1 Guiding principle

For each path, the two-way result must equal running `rsync <opts>` in the direction the version vectors choose. On quiet trees that means A→B, then B→A.

### 4.2 "Unsafe" links

This is a lexical check, ported from rsync's `unsafe_symlink()`. A link is unsafe if:
- its target is absolute or empty, or
- walking its `..` components, starting from the link's own directory (expressed relative to the root), ever goes above depth 0, or
- *(rsync ≥ 3.4.0, CVE-2024-12088, also backported by distributions)* its target contains a `..` component anywhere after the leading run of `../` (e.g. `a/../x`), or ends in `/..`. An intermediate component could later be replaced by a symlink.

We port rsync 3.4.1, quirks included (`symlink/safety.rs`), and check it against the installed rsync. The check runs on the canonical (unmunged) target. Both sides use the same relative paths, so the classification is symmetric. **Security never depends on this check.** Escapes are prevented by the kernel through `RESOLVE_BENEATH` (§5.1).

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
  - Run `readlinkat` before and after reading the referent. If the link was retargeted in between, the read is unstable (§5.2). In practice the "after" check is a `statx` of the link: a symlink's target cannot change without a new inode, so an unchanged (ino, ctime) means an unchanged target.
  - In-tree referents are opened with `RESOLVE_BENEATH` (plus `NO_MAGICLINKS | NO_XDEV`), from the root fd and the link's own root-relative path, so the kernel follows the link but cannot leave the root. If that fails with `EXDEV` (the link, or a link it leads to, escapes), the referent is opened from the link's own directory fd with `RESOLVE_NO_MAGICLINKS` only and marked `out_of_tree`. If the in-tree attempt fails otherwise but the direct one succeeds, the two views of the path disagree and the link is unstable. Out-of-tree referents are **read-only**: we never write to them.
  - A referent is first opened `O_PATH` to learn its kind, so a FIFO or device is never opened for reading. A file referent is then read with `fs::stable_read_with` (the §5.2 checks, plus: still the same inode, link unchanged). A directory referent is opened as `"."` beneath its `O_PATH` fd, so it is exactly the classified inode.
  - A chain of links that never resolves (`ELOOP`) is `Unmanaged(Loop)` under a following policy.
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

**Step 1. Journal.** Before the first reserved name is created, or a user object is moved to one, record `Intent{op, path, parent (dev, ino), tmp, old, expected, staged, state}` and commit it durably (§5.8). Every reserved name the commit may use (`tmp`: the staging or move-aside name; `old`: the quarantine name) is chosen at random **up front** and recorded, so a name exists on disk only once the journal knows it (an `EEXIST` on one is an error, not a retry). A named temp file, symlink or directory is recorded before it is created; an `O_TMPFILE` only just before it is linked in, already with its inode (one commit). A replace records the staged inode N durably (`TempWritten`) before the exchange.

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
- If either check fails, the commit already happened but the name or its directory changed right after: report `Unstable` so the path is rescanned.
- In **one** redb transaction, write the index entry (local meta = the fingerprint we produced) and mark the journal intent Done (delete it, or keep it as `Quarantined` while its old inode waits in quarantine).
- The resulting inotify event then finds the index already up to date, so **there is no echo**.

### 5.4 Directories

**Mode change** (`commit::set_dir_mode`, T11): a directory can't be replaced by a copy, so this is the one in-place change. Pin it with `openat2(O_RDONLY|O_DIRECTORY|O_NOFOLLOW)`, require the expected inode **and** the indexed mode, `fchmod` through the pin, then the step 5 checks. A concurrent swap only means the pinned (verified) directory is changed; only metadata is at stake.

**Create:** `mkdirat(parentfd, tmp, 0700)` under a temp name, pin it with an `O_RDONLY|O_DIRECTORY` fd, `fchmod` it to the wanted mode (so the umask does not apply and the chmod cannot hit someone else's directory), then `renameat2(tmp → name, RENAME_NOREPLACE)` and the usual step 5 checks. `EEXIST` means `PreconditionFailed`. (Originally `mkdirat` directly on the name; the temp name gives the same atomic emptiness of the name plus a verified inode and exact mode.)

### 5.5 Symlinks

`symlinkat(target, parentfd, tmp)`, then the same `NOREPLACE` or `EXCHANGE` commit. Verification compares the readlink bytes plus ino. A file and a symlink may replace each other in one exchange (the old object only has to be a file or symlink matching the index); directories are never exchanged.

### 5.6 Deletes

1. Pin the object with `O_PATH|O_NOFOLLOW` and check it against the index.
2. `renameat2(name → .~fsync.del.<id>, RENAME_NOREPLACE)`.
3. Verify ino, mtime and size, as in §5.3 step 4(d).
4. If verification succeeds: unlink, or quarantine as in §5.3 step 4(f). (Implemented: always quarantine, renamed on to `.~fsync.old.<id>`, so a write through an fd held across the delete becomes a conflict copy.)
5. If verification fails: rename it back with `NOREPLACE`. If the name was taken meanwhile, use a conflict name.
6. Step 5 checks as for a commit, except that the name must be **free**: a name re-created right after the delete gives `Unstable`, so the path is rescanned.

### 5.7 Directory deletes

- Delete children first, deepest first, each with its own CAS.
- Then run `unlinkat(parentfd, name, AT_REMOVEDIR)`. **rmdir is an atomic emptiness check.**
- `ENOTEMPTY` means a child appeared concurrently. Abort and bump the directory's version vector, which resurrects it, so the new child and its directory propagate to the peer.
- Deleted children wait in the quarantine **inside** the directory, which would keep it non-empty. So rmdir first settles the quarantine entries of that directory without waiting for their grace period (`Quarantine::settle_dir`): unchanged ones are unlinked; ones written to since the delete become conflict copies in the directory, which then fails the rmdir and resurrects it with the conflict copy.
- `ENOTDIR` (the name was replaced by a file or a symlink) is `PreconditionFailed` as well: `AT_REMOVEDIR` never removes or follows a non-directory.
- rmdir has no fingerprint precondition: a directory swapped for another **empty** one just before is removed. No data is lost, only that directory's mode.

### 5.8 Journal and crash recovery

The journal is an intent log in the same redb file (`index/journal.rs`, table `intents`). One intent per commit that uses reserved names (create, replace, delete, rename to a conflict name; `rmdir` and `set_dir_mode` need none). States: `Started` (names chosen; nothing of the user's is at a reserved name yet), `TempWritten` (N recorded), `Exchanged` (the user's object is at `tmp`: after the exchange or a move-aside), `Quarantined` (committed; the old inode waits under `old`). Done means the record is deleted.

- **When records are written.** The first record and `TempWritten` are durable (`Durability::Immediate`); `Exchanged`, Done for a commit that did not apply, and Done for a settled quarantine entry are not (`Durability::None`). That is safe because replay inspects the names instead of trusting the state, and replaying an intent that already finished finds its names empty (they are unique).
- **Index transaction.** `LocalReplica::apply` checks preconditions against a read snapshot (redb has one writer, and the commit writes the journal), then writes the entries and finishes the commit's intents in one write transaction. Nothing else writes the index in between (`&mut self`, and redb locks the file to one process).
- **Replay** (`commit::recover`, at `LocalReplica::open`, and right after a commit that returned an error) works on the recorded names, in the recorded parent (re-resolved; a parent moved, removed or replaced means the intent is skipped and its names left alone):
  - our staged object N at `tmp` is removed. In state `Started` (N unknown) only our own object can be at `tmp`, so whatever is there is removed (a directory only if empty);
  - the expected old object O at `tmp` (replace, after the exchange) or at the `.del.` name (delete) gets the §5.3 step 4(d) verification: unchanged → (f) quarantine it (roll forward); changed → (e) put it back: exchange back if the name still holds N unchanged (then remove N), else `RENAME_NOREPLACE` to the name, else a conflict name;
  - any other object at `tmp` is user data and goes back the same way. A rename's moved-aside object always goes back (roll back; the next sync redoes the conflict);
  - O at `old` is quarantined again with a fresh grace period (a writer may still hold an fd); a foreign object there is left alone;
  - afterwards the intent is done, or kept as `Quarantined` while the quarantine holds it. The quarantine reports settled entries (`SweepReport::finished`) so their records go.
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

As implemented (T16, `src/watch/`):
- **Layers:** an `EventSource` (`wait(timeout) -> Vec<Event>`, `Event { Dirty(path), Overflow }`) is read by a `Watcher` thread, which runs it through a pure `Debouncer` and sends `Hint`s. `InotifySource` is the production source; `ChannelSource` takes injected events (`LocalReplica::set_event_source`). `Replica::watch()` starts the watcher once (its initial watches are in place when it returns) and returns `None` if inotify cannot be used.
- **Moves:** a directory's `MOVED_FROM` (or `DELETE`) removes the watches of its whole subtree, and `MOVED_TO` (or `CREATE`) watches the new subtree afresh (watch, then list, breadth first, each directory opened beneath the root). So no watch ever reports under a stale path. `MOVE_SELF` therefore only matters when the parent's `MOVED_FROM` was missed: it dirties the parent (and so the whole subtree). A moved root becomes `Overflow`.
- Events on a watched directory itself (no name) dirty that directory; with a name, they dirty `parent/name`. A dirty path is always rescanned recursively (`Scope::Paths`).
- **Limits:** a batch of more than 10 000 dirty paths becomes a full rescan. When `inotify_add_watch` hits the watch limit (`ENOSPC`), the source emits `Overflow` at once and then every 60 s (polling).

### 5.10 Remaining windows, stated honestly

| Window | Mitigation |
|---|---|
| A write that preserves mtime (`utimensat`) between the precondition check and the exchange | Rehash racy and small files. Large non-racy files have a theoretical gap. |
| A writer holding an fd or mmap to the old inode after the exchange | Lease check plus quarantine with a grace period; changes become a conflict copy. |
| Leases need the caller to own the file (or CAP_LEASE) and a local filesystem | Fall back to the grace timer. |
| Hardlinks | We never write in place, so other links keep the old content. Matches rsync without `-H`; `-H` may come later. |
| A directory moved out of the root while we hold its fd | Post-commit (dev, ino) check of the parent (§5.1). |
| A writer holding an fd to a deleted child when its directory is removed | rmdir settles the directory's quarantine early (§5.7), so the grace period is cut short; the stat → unlink window of the sweep remains (T18 leases close it). |

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

Details of the table, as implemented by `engine::reconcile` (T12):
- **Purity:** `reconcile(a: &dyn IndexView, b: &dyn IndexView, now)` does no I/O. `now`, the local wall-clock time, is only used for conflict names. `engine::Snapshot` is the in-memory `IndexView` (e.g. from `changes_since(0)`). The root path is never an action.
- **Same content** means the same kind (with hash or target) and mode, as above. **Equal vectors** also promise the same mtime for a file (the only synced mtime, §3); a file differing only in mtime is therefore also "inconsistent → rescan".
- **Concurrent, same content:** both sides record the merged vector and the newer mtime (`SetMeta`, which rewrites the older file as a copy).
- **Tombstone vs tombstone:** never an action, even when the vectors differ (`SetMeta` cannot apply to a tombstone). Tombstone GC (T18) has to handle such pairs.
- **`Unmanaged`:** a `Skip` action (worth a warning) only when the other side has a live entry; otherwise no action. Every action beneath a path that is `Unmanaged` on either side is skipped too: the peer keeps the whole subtree.
- **A push of the same content** (a dominating vector, but the same hash or target, or two directories) is a `SetMeta`: metadata and vector only, no transfer.

### 6.2 Conflicts

- **Winner:** the newer mtime; a tie goes to the higher ReplicaId. For type conflicts, Dir > File > Symlink.
- **Loser:** renamed on its own replica to `stem.sync-conflict-YYYYMMDD-HHMMSS-<ID7>.ext`, where `<ID7>` is the first 7 characters of the loser's replica ID. Directories, symlinks and names without an extension get no extension split. If the name is taken (live on either side's index, or already chosen in this reconcile), the timestamp is bumped by a second and retried; a name taken on disk only makes the rename fail (`PreconditionFailed`, rescan). The helper is `fs::tmpname::conflict_name` (in `fs/`, because `fs::commit` needs it too).
- The conflict copy gets a fresh version vector and syncs to both sides like any other file (in the next round: the rename assigns its vector).
- **The winner's version** gets the merged version vector plus a bump by the winner's replica. It is written **only to the loser side**: first the loser is renamed away, then the winner's content is created there with that vector (`Absent`). The winner side learns the vector in the next round, through an ordinary dominating push of identical content (an index-only `SetMeta`). This way no side ever records "has seen the loser's change" without holding the result: if the rename fails, the create fails too (`Absent`), and nothing dominates the loser.
- **Dir-vs-dir conflicts** (only the mode differs): no rename. The loser directory gets the winner's mode (`SetMeta`, a `fchmod`, §5.4) with the bumped vector.
- **Renaming the loser** (`commit::rename_to`, `Op::RenameToConflict`, T11): pin and check as for a delete, move aside to a reserved name with `NOREPLACE`, verify (restore on failure, as in §5.6), then rename on to the conflict name with `NOREPLACE` (if it was taken, restore → `PreconditionFailed`), then the step 5 checks at the new name. Files and symlinks only. Since Dir wins every type conflict, a directory loses only a dir-vs-dir conflict (mode), which is resolved without a rename (above).

### 6.3 Ordering

- Creates and updates: depth ascending (directories before their children).
- Deletes: depth descending (children before their directory).
- A type change becomes a delete in the delete phase plus a create in the create phase.
- A type change to or from a directory: the delete leaves a tombstone with the **target's old** version vector, so if the create does not happen, the pushed entry still dominates and the next round retries it.
- A directory delete is skipped (the directory is resurrected) if the deleting side has any live descendant not dominated by the tombstone. Precisely (T12, decided in `reconcile`): a directory removal on side S (a pushed tombstone, or a type change to a file or symlink) is blocked if S would still hold anything beneath the directory after this round: a live or `Unmanaged` object it is not told to delete, or one it is told to create. Checked deepest first, so a kept directory keeps its ancestors too. A blocked removal becomes a `Resurrect` action, resolved like a conflict that S's directory wins: the merged vector plus S's bump goes to the other side, whose tombstone is replaced by the directory, or whose file or symlink is first renamed to a conflict copy.
- **Plan** (`engine::plan`): three phases, run in order: `Conflicts` (renames of losers), `Deletes` (depth descending), `Creates` (creates, updates and `SetMeta`, depth ascending). A path has at most one step per phase and side. When a step does not apply, the executor marks the path dirty and skips its later steps (a later step's precondition would fail anyway).

### 6.4 Convergence loop

Any `PreconditionFailed` marks the path dirty. The loop then rescans the dirty paths on both replicas and runs another round, up to 5 rounds per sync cycle. Paths still unresolved wait for the next cycle. A round with a conflict or a resurrection needs a following round even when nothing failed: the conflict copies and the winner's merged vector propagate then. So the loop runs while `reconcile` still yields steps (the T12 model test converges in at most 3 rounds without concurrent edits).

**Executor** (`engine::Engine`, T13): `sync(a, b, scope)` scans `scope` on both replicas (`sync_once` = `Scope::Full`), then loops: rescan the dirty paths on both sides, reconcile **snapshots of the whole indexes** (`changes_since(0)`), and run the plan's phases. A `WriteFile` streams `open_read` on the source side straight into `apply` on the destination. Dirty paths come from `PreconditionFailed`, `Preserved`, `Err(Unstable)` or not-found (from either `open_read` or `apply`), `Rescan` actions and scan-reported dirty paths. After a step fails, that path's later steps in the round are skipped. Any other per-path error (e.g. `EACCES`, or an `InvalidOp`, which is an engine bug) is logged and reported, and the path and its subtree are left alone for the rest of the cycle. Only index failures (`Db`, `BadIndex`) and scan failures (root or index) abort the cycle. The loop stops when a reconcile yields no steps and nothing is dirty. After `MAX_ROUNDS` (5) rounds (a round that only rescans counts too), what is left is reported as `unresolved`. `SyncReport` lists rounds, applied/retried steps, conflict copies (`engine::conflict::ConflictCopy`, recorded when the rename applied), resurrections, `Unmanaged` skips (warned once per cycle), errors and unresolved paths.

**Daemon** (`daemon::Daemon`, T16): `run(a, b, stop)` starts both watchers, then runs a full cycle (watch first, then scan). It then waits for a hint (a cycle scoped to the hinted paths on both replicas, or a full one after an overflow), the rescan timer (full cycle, default 10 minutes), a retry (paths a cycle left `unresolved` are synced again after 1 s), a quarantine deadline (`sweep_quarantine`), or `stop`. Hints that arrive during a cycle join the next one. Our own writes are reported by inotify too; the cycle they cause finds the index already up to date and applies nothing, so each applied change costs one extra, empty scoped cycle. A failing cycle (index or root failure) ends the run with the error. The CLI blocks `SIGINT`/`SIGTERM` before any thread starts and turns them into the stop message (a `sigwait` thread; a second signal exits at once), then drains the quarantines as `sync --once` does.

`sync --once` (CLI) runs one cycle, then waits for both quarantines to drain (sweeping until empty, at most 10 grace periods), so a one-shot run leaves no `.~fsync.old.*` files behind. It exits non-zero if the cycle did not converge.

Rename detection by inode or hash is an optional later optimization.

---

## 7. Replica trait (network-ready)

```rust
trait Replica {
    fn id(&self) -> ReplicaId;
    fn scan(&mut self, scope: Scope) -> Result<ScanStats>;               // replica-side: updates own index, bumps VVs
    fn changes_since(&self, seq: u64) -> Result<Vec<(RelPath, Entry)>>;  // Entry without LocalMeta on the wire
    fn open_read(&self, p: &RelPath, expect: &Entry) -> Result<Box<dyn ContentReader>>; // stability + hash checked at EOF
    fn apply(&mut self, p: &RelPath, op: Op, pre: Precondition, content: Option<&mut dyn Read>) -> Result<Outcome>;
    fn watch(&mut self) -> Option<Receiver<Hint>>;
}
enum Op {                                   // every op that leaves an entry carries its vv (computed by the engine)
    WriteFile{meta: FileMeta, hash, vv}, Mkdir{mode, mtime_ns, vv}, Symlink{target, mtime_ns, vv},
    Delete{vv}, Rmdir{vv}, RenameToConflict{to: RelPath}, SetMeta{mode, mtime_ns, vv},
}
enum Precondition { Absent, Matches{ kind: Kind /* incl. hash or target */, vv } }  // mapped to the replica's own physical fingerprint
enum Outcome { Applied(Entry), PreconditionFailed(Option<Entry>), Preserved{ conflict: RelPath } }
```

**All CAS logic runs inside `LocalReplica::apply`, next to the files.** The engine never touches the filesystem. A `RemoteReplica` is a postcard-framed RPC stub around the same calls, so the network version inherits race-freedom unchanged.

`LocalReplica::apply` (T11) runs in one index write transaction:
- **Logical check** against the index. `Absent` = no entry or a tombstone; `Matches` = the entry's kind and vv are exactly these, and the entry is not `Unmanaged`. Every ancestor must be an indexed real directory, and neither the path nor an ancestor may be a followed link (`via_link`; writing through one is T17). Failure → `PreconditionFailed(current entry)`.
- **Physical check:** the entry is mapped to an `Expected` fingerprint (files: from the index; symlinks: indexed (dev, ino, ctime) checked against a fresh `statx`, since the index keeps a symlink's mtime from its last logical change), and the matching `fs::commit` function does the binding CAS.
- **Index:** on success the entry is written with the op's vv and the fingerprint the commit produced (files are marked `racy`, so the next scan rehashes them once). The next scan therefore finds nothing to do (no echo).
- **Ops:** `WriteFile`/`Symlink` create (`Absent`) or replace a file or symlink. `Delete`/`Rmdir` leave a tombstone with the op's vv; `Rmdir` also requires every indexed descendant to be a tombstone. `RenameToConflict{to}` (files and symlinks; `to` must be a sibling) makes the path a tombstone with a **local bump** and indexes the copy at `to` with a fresh vv. `SetMeta` rewrites a file as a copy (via `replace_file`, never in place), `fchmod`s a directory (§5.4), and only updates the index when nothing on disk changes (same file mode and mtime, a directory's mtime, any symlink). So it also serves to record a merged vv.
- An op that cannot apply to the indexed kind (e.g. `Rmdir` on a file, `WriteFile` over a directory, `WriteFile` without content) is `Error::InvalidOp`, a caller bug. Type changes from or to a directory are a delete plus a create (§6.3).
- **`open_read`** streams through `fs::StableReader`: one attempt (bytes already sent can't be retried), the §5.2 checks, plus the expected size up front and hash at EOF. `read` returns `Ok(0)` only once all checks pass; otherwise it fails with an `io::Error` wrapping `Error::Unstable` (`Error::from_stream` unwraps it; `TempFile::copy_from` does so). A followed link's referent is read via `Root::open_referent`, and the link must be unchanged at EOF.

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
- **Deterministic race injection.** `fs::hooks::point("replace.before_exchange")` and similar exist only under `cfg(test)` or the `hooks` feature. Tests register closures (per thread) that mutate the filesystem at exactly that moment:
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
  - model-based random operation sequences on A and B, interleaved with syncs, checked against a reference model (`tests/model.rs`, T14). The model is independent of the engine: per path, a causal history is a *set* of edit events, recorded (like the scanner) only when the path differs from the last sync. A sync resolves each path on its own (superset wins; concurrent: same content merges, live beats deleted, otherwise the §6.2 winner stays and the loser becomes a conflict copy), then resurrects, deepest first, every non-directory with something live beneath it (its file or symlink becomes a conflict copy). Conflict copies are predicted as (directory, original name, loser's ID7, content); the real name is adopted from disk. After every sync the real tree must match the model exactly (kind, file content and mtime, symlink target), on top of `assert_converged`. Every write is unique in content and mtime, and the user gives every symlink it creates or moves a fresh mtime, so the model always knows the winner.
- **Harness (`tests/harness/`):** two tempdirs, a scenario DSL, and `assert_converged` (trees equal after policy normalization, version vectors equal). The network phase must be able to rerun it over loopback (T22 makes it generic over `Replica`; T13's version holds two `LocalReplica`s).
  - `Pair::new(policy)`: fixed replica IDs (predictable conflict names) and quarantine grace 0.
  - DSL on `pair.a` / `pair.b`: user edits through plain `std::fs` (`write`, `write_at` (sets the mtime), `mkdir`, `symlink`, `chmod`, `rm`) and observations (`read`, `readlink`, `ls`, `conflicts`, `entry`, …). `sync()` must converge; `try_sync()` returns the report whatever it says.
  - `sync_racing(side, Apply|Read, path, edit)` wraps the replicas so a user edit runs right before the engine's first `apply` or `open_read` at `path`. It is the integration-level test of the dirty → rescan → retry loop.
  - `assert_converged`: sweeps the quarantines; the trees match (file content, mode and mtime; directory mode; symlink targets, unmunged; symlinks dropped under `Skip`; other policies are T17's job); the live index entries match, vectors included; there are no reserved names; a full rescan of either side changes nothing (no echo).
- **Symlink matrix:**
  - policy × {safe relative, unsafe relative, absolute, dangling, directory link, loop, munged} × direction;
  - a differential test against real `rsync` (when installed) for the one-directional cases.
- **Crash tests** (`tests/crash.rs`, T15): a child process (the test binary re-run as `crash_child`, rather than a `fork()` of the multi-threaded test process) runs one sync cycle of a scenario and calls `abort()` at the nth hit of hook point N; sweep over every position of the cycle's trace, for 14 scenarios covering every commit op (file-staging ones under both temp strategies). The parent checks that every reserved name left is named by the journal, that reopening (replay) and a zero-grace sweep leave none, and that the next sync converges to exactly the crash-free outcome (conflict copies compared without timestamps). A late-write variant writes through a held fd at the crash point; that write must survive. A second suite crashes the replay again (`recover.done`). A process crash keeps redb's non-durable commits, so these tests cannot tell `Immediate` from `None`; the durability rules of §5.8 are argued, not tested (power loss).
- **Stress test** (`tests/stress.rs`, `#[ignore]`).
  - *Setup:* N writer threads per tree perform read-modify-write, blind overwrite, append, delete, rename, directory↔symlink swaps and symlinks to `/tmp/outside`, while the daemon syncs. Every write is unique and logged to a ledger `(replica, path, pred_hash, new_hash)`.
  - **Invariant: no loss.** Every `new_hash` that was not superseded (never another entry's `pred_hash`, and not deleted by a logged delete) exists at the end in a tree, either at its path or as a conflict copy of it.
  - **Invariant: no escape.** Landlock limits writes to the roots and the state directory, and sentinel directories outside the roots are snapshotted and must be unchanged.
  - **Invariant: convergence.** Once the writers stop, at most 3 rounds produce zero actions, the trees are identical, the version vectors are equal, and no `.~fsync.*` files remain after the quarantine expires.
