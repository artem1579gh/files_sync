# files_sync: tasks

The design lives in [`design.md`](design.md); §N.M below refers to its sections.

## How to work on a task (read this first)

1. Pick the **first unchecked task** whose dependencies are all checked.
2. Read only the design sections listed in that task's **Read** line. Then read the existing code it touches.
3. Keep the scope tight: do this task only. If you need a small stub from a later task, add a stub with a `todo!()` and say so in Notes.
4. Make the **Done when** criteria pass, plus `cargo build`, `cargo test` and `cargo clippy --all-targets -- -D warnings`.
5. Tick the box. Fill in **Notes** with deviations from the design, open issues and follow-ups. If you deviated from the design, update `design.md` too.
6. Commit with the message `T<NN>: <title>`.

---

## M0: foundations

### [x] T01: Scaffolding
- **Depends on:** —
- **Read:** §1, §2
- **Files:** `Cargo.toml`, `src/lib.rs`, `src/main.rs`, `src/cli.rs`, `src/config.rs`, `src/error.rs`, empty `mod.rs` stubs for every module in §2
- **Do:**
  - Add the §2 dependencies that are needed now: rustix, libc, blake3, redb, serde, postcard, toml, clap, crossbeam-channel, tracing, tracing-subscriber, thiserror, anyhow, jiff. Dev dependencies: tempfile, proptest. Leave out landlock and rustls for now.
  - Add a lib + bin layout, with `main.rs` calling `files_sync::cli::run()`.
  - Add a clap CLI with these subcommands: `init <pair> --a <dir> --b <dir>`, `sync --once <pair>`, `daemon <pair>`, `status <pair>`. They are stubs that print "not implemented".
  - `config.rs`:
    - `PairConfig { name, replicas: [ReplicaConfig; 2] }`;
    - `ReplicaConfig { id: ReplicaId(u64), root: PathBuf, symlinks: SymlinkPolicy, munge_links: bool, keep_dirlinks: bool }`;
    - a placeholder `SymlinkPolicy` enum: `Skip | Links | CopyLinks | CopyUnsafeLinks | SafeLinks | CopyDirlinks`;
    - TOML load and save under `$XDG_STATE_HOME/fsync/<pair>/config.toml`;
    - `init` generates random replica IDs.
  - Add an `error.rs` `Error` enum with `thiserror`.
- **Done when:** `cargo run -- --help` lists the subcommands, `init` writes a config that loads back, and clippy is clean.
- **Notes:**
  - Deps as listed; `rustix` has features `fs` + `rand` (`rand` is used for `getrandom`, so no `rand` crate is needed). `postcard` uses `use-std`, `tracing-subscriber` uses `env-filter`. Edition 2024.
  - Module stubs: `fs`, `index`, `symlink`, `scan`, `watch`, `replica`, `engine` (`mod.rs` holding only a doc comment) plus `src/daemon.rs`. Submodule files (`root.rs` etc.) are left to their own tasks.
  - `cli::run()` returns `anyhow::Result` because `cli.rs` is the binary's front end; the rest of the library uses `crate::Error`. Stub subcommands exit 1 with "Error: `<cmd>`: not implemented". `sync` without `--once` is rejected and points to `daemon`. Logging goes to stderr via `RUST_LOG` (default `info`).
  - Config: `ReplicaId` is serialized as a 16-digit hex **string** in TOML, because TOML integers are i64 and random u64 IDs would not round-trip. `ReplicaId` itself is `serde(transparent)` u64, so postcard/index stay compact. `Display` is also 16-digit hex, so ID7 = the first 7 chars.
  - `SymlinkPolicy` is serialized in kebab-case (`links`, `copy-unsafe-links`, …) and defaults to `Links` (rsync `-a`). `symlinks`, `munge_links` and `keep_dirlinks` are optional in the file. `deny_unknown_fields` is on.
  - The config API takes `state_home` explicitly (`PairConfig::load/create/save`, `config::init`); `config::state_home()` reads `$XDG_STATE_HOME`, else `$HOME/.local/state` (relative values are ignored, per the XDG spec). This keeps tests independent of the environment.
  - `config::init` canonicalises both roots and rejects: a missing root or a non-directory, overlapping or nested roots, a state dir inside a root, an existing pair (installed via `link(2)`, so no overwrite even under a race), and non-UTF-8 root paths (TOML can't store them; follow-up if needed). Pair names are restricted to `[A-Za-z0-9._-]`, ≤64 bytes, no leading `.`.
  - Config writes are atomic (temp file + fsync + rename/link + dir fsync), with dir mode 0700 and file mode 0600. These use `std::fs`, which is fine because the state dir is never inside a replica.
  - Tests: 8 unit tests in `config.rs`, plus `tests/cli.rs` (help lists the subcommands, `init` round-trips through the real binary, stubs report "not implemented").

### [x] T02: Filesystem feature checks (`fs/caps.rs`)
- **Depends on:** T01
- **Read:** §1 (environment caveat), §5.3 step 2, §5.3 step 4(b)
- **Files:** `src/fs/caps.rs`
- **Do:** implement `Caps::probe(root_dirfd) -> Caps { openat2, rename_exchange, rename_noreplace, o_tmpfile, linkat_empty_path, statx_btime, leases, fs_type }`. Each check is a real, harmless operation inside the root using `.~fsync.probe.*` names, cleaned up afterwards. Add a `require_minimum()` check that fails clearly when openat2 or the renameat2 flags are missing.
- **Done when:** a test on a tempdir in `/tmp` reports everything supported (except leases, which depend on the environment). The result is logged at startup.
- **Notes:**
  - Extra field `linkat_proc_fd` (the `/proc/self/fd/N` + `AT_SYMLINK_FOLLOW` fallback from §5.3 step 2), plus `Caps::tmpfile_usable()` = `o_tmpfile && (linkat_empty_path || linkat_proc_fd)`. T04 should use these to choose between O_TMPFILE and a named temp file. Unprivileged `AT_EMPTY_PATH` only works on kernels ≥ 6.10; on older kernels it reports false (ENOENT).
  - `fs_type` is an `FsType` enum built from the `statfs` magic (ext2/3/4, xfs, btrfs, tmpfs, 9p, fuse, nfs, overlay, `Other(magic)`). `Caps::log` warns when the type is not one of the four supported filesystems; `require_minimum` checks only features, not the type.
  - `openat2` counts as supported only if `..` really fails with EXDEV under `RESOLVE_BENEATH`. `rename_noreplace` must give EEXIST on an existing name and succeed on a free one. `rename_exchange` must actually swap the inodes. `leases` takes, checks (`F_GETLEASE`) and releases an `F_WRLCK` lease; any failure counts as false.
  - Errnos that mean "unsupported" give `false`. Anything else (e.g. EACCES/EROFS on the root) is an `Err`, since such a root couldn't be synced anyway.
  - Cleanup safety: probe names are `.~fsync.probe.<random tag>.<suffix>`. Every probe inode stays pinned by an fd until cleanup, so its inode number can't be reused; `O_PATH` pins don't block write leases. Cleanup unlinks a name only if it still holds one of our (dev, ino); anything else is left and logged. There is a residual statx→unlink window on a random reserved name, which is accepted. Test: `cleanup_leaves_foreign_files_alone`.
  - Mutating outside `commit.rs`: `caps.rs` creates and removes its own probe files. CLAUDE.md anticipates this (the "probe files" in the reserved prefix), and design §2 now says so.
  - `Error::MissingCapabilities { fs_type, missing }` was added. Until `Root` (T03) exists, `Caps::probe_path` opens the root directly with `O_PATH|O_DIRECTORY`; T03/T11 can switch to `Root`.
  - CLI: `init` probes both roots and refuses one missing the required features. `sync --once` and `daemon` load the config, probe and log both roots ("filesystem capabilities" at INFO), then say "not implemented". `status` is still a plain stub. In T11, `LocalReplica::open` should own the probe.
  - Measured on this WSL2 machine (kernel 6.18): ext4 `/tmp` and tmpfs `/dev/shm` report every capability as true, leases included. The drvfs path is unit-tested only through forced flags, because tests must not touch `/mnt/c`.

## M1: race-safe filesystem primitives

### [x] T03: `Root`, `RelPath`, fingerprints
- **Depends on:** T01
- **Read:** §2, §3 (paths), §5.1, §5.2
- **Files:** `src/fs/root.rs`, `src/fs/stat.rs`, `src/fs/mod.rs`
- **Do:**
  - `RelPath`: raw bytes, validated; components are non-empty and contain no `/`, `.` or `..`. Provide `parent()`, `name()`, `depth()` and `join()`.
  - `Root::open(path)`: an `O_PATH|O_DIRECTORY` fd.
  - `Root::resolve_parent(&RelPath) -> Result<(OwnedFd, &[u8] name)>` using openat2 with `RESOLVE_BENEATH|NO_SYMLINKS|NO_MAGICLINKS|NO_XDEV`. ELOOP and EXDEV map to `Error::Unstable`.
  - `Root::read_dir(&RelPath)`: fd-based, returns names and d_type.
  - `Fingerprint { dev, ino, size, mtime_ns, ctime_ns, mode, kind }` from `statx` (fd or `AT_SYMLINK_NOFOLLOW`).
  - `stable_read(parentfd, name, sink) -> Result<(Fingerprint, [u8;32])>` implementing §5.2: hash while reading, compare the before and after fingerprints, retry three times, then return `Unstable`.
- **Done when:** unit tests show:
  - a symlinked intermediate directory gives `Unstable`;
  - `..` and absolute paths are rejected by `RelPath`;
  - a file appended to during `stable_read` (from a hook or a thread) gives `Unstable`;
  - the normal case returns the correct blake3 hash.
- **Notes:**
  - `RelPath` (in `fs/root.rs`, re-exported as `fs::RelPath`) wraps `Vec<u8>`; the empty path is the root (`RelPath::root()`, depth 0, `parent()`/`name()` are `None`). Rejected: empty components (so absolute paths, `//` and a trailing `/`), `.`, `..` and NUL bytes. `join()` takes one or more components, validated the same way. Serde goes through `try_from = "Vec<u8>"`, so deserialised paths are validated too. `Display` uses `escape_ascii` (root shows as `.`). Reserved `.~fsync.` names are *not* rejected; that is the scanner's job (T04 adds `is_reserved`).
  - `Root { fd, path }`: `open`, `fd`, `resolve_parent`, plus `open_dir` (an `O_RDONLY` fd for the directory itself, root included), `read_dir` (sorted, without `.`/`..`, reserved names included; `kind: Option<FileKind>`, `None` for `DT_UNKNOWN`) and `stat` (a `Fingerprint` of the path itself). For a depth-1 path, `resolve_parent` returns a dup of the root fd. `Caps::probe_path` now uses `Root::open`.
  - **Deviation (design §5.1 updated):** directory opens use `O_PATH|O_DIRECTORY` without `O_NOFOLLOW`. With it, a trailing symlink in the parent path is opened as the link itself and fails with ENOTDIR instead of ELOOP. `RESOLVE_NO_SYMLINKS` alone rejects every symlink with ELOOP. The file open in `stable_read` keeps `O_NOFOLLOW` (no `O_DIRECTORY`, so it gives ELOOP).
  - Errors: new `Error::InvalidPath { path, reason }` and `Error::Unstable { path: Vec<u8>, reason: &'static str }`. ELOOP, EXDEV and EAGAIN (openat2's "possible concurrent rename") map to `Unstable`. ENOENT and ENOTDIR stay `Error::Io`, so callers can tell "gone" from "changed". In `stable_read`, `Unstable.path` is the single `name`; callers holding the `RelPath` should rewrap it.
  - `Fingerprint { dev (makedev), ino, size, mtime_ns, ctime_ns, mode (& 0o7777), kind: FileKind { File, Dir, Symlink, Special } }`, with `of_fd`, `at` (`AT_SYMLINK_NOFOLLOW`), `same_file` (dev+ino) and `unchanged` (same_file + kind, size, mtime, ctime: the §5.2 check). A statx result missing any needed field is an `Io` error (Unsupported). `mnt_id` from §3 `LocalMeta` is not in the fingerprint; T10 can add it if needed.
  - `stable_read(parent, name, &mut impl Sink)`. `Sink` has `restart()` (called before each retry, so a sink can drop a torn attempt) and `write()`; `Vec<u8>` and `Discard` implement it. There are 1 + 3 attempts with 5/10/20 ms backoff. Unstable when: the name is a symlink or not a regular file; F1≠F2 (`unchanged`); more bytes are read than F1.size (stops early on a growing file); the byte count ≠ size; the name is removed or replaced (F3); or the file is leased (EAGAIN). The open uses `O_NONBLOCK`, so a FIFO or a foreign lease can't block us; FIFOs then fail the regular-file check. `O_NOATIME` falls back on EPERM. The returned fingerprint is F2.
  - Tests (16 new): RelPath validation, parts, serde; resolve_parent (normal, symlinked intermediate inside and outside the tree → `Unstable`, missing/ENOTDIR → `Io`); read_dir types incl. non-UTF-8 names; stable_read normal multi-chunk + empty (blake3 checked), append on every attempt → `Unstable` after exactly 4 attempts, append on the first attempt only → success with the new content, a concurrent appending thread → `Unstable`, name replaced/removed mid-read, symlink/dir/FIFO → `Unstable`. Hooks don't exist yet (T04), so the "hook" is a `Sink` that mutates the file on its first write of each attempt. T04 could add a `hooks::point("stable_read.after_read")`.
  - For T04: `resolve_parent` returns an `O_PATH` fd, which works as a dirfd for every `*at` call and for `O_TMPFILE`, but `fsync(parentfd)` (§5.3 step 5) needs a separate `O_RDONLY` open of the directory (`Root::open_dir`, or `openat2(parentfd, ".", O_RDONLY|O_DIRECTORY)`).

### [x] T04: Hooks, temp files, create-type commits
- **Depends on:** T03, T02
- **Read:** §5.3 (steps 1–3 and 5), §5.4, §5.5, §9 (race injection)
- **Files:** `src/fs/hooks.rs`, `src/fs/tmpname.rs`, `src/fs/commit.rs`
- **Do:**
  - `hooks::point(&'static str)` does nothing unless `cfg(any(test, feature = "hooks"))`. Tests use it to register closures per point. Add a `hooks` feature to Cargo.toml.
  - `tmpname`: `.~fsync.<id>`, `.~fsync.old.<id>`, `.~fsync.del.<id>`, and `is_reserved(name)`.
  - `TempFile::create(parentfd, caps)`: O_TMPFILE, falling back to a named temp file. Then `write_all` with hashing, `finish(mode, mtime)` (fchmod, futimens, fsync), then link in. The fd is kept open.
  - `commit::create_file(root, relpath, content, meta, expected_hash) -> Outcome` using `RENAME_NOREPLACE`. Reject on hash mismatch.
  - `commit::create_symlink(root, relpath, target)` via a temp name and NOREPLACE.
  - `commit::mkdir(root, relpath, mode)`.
  - Post-commit verification (§5.3 step 5, without the index or journal for now). `Outcome` here is a local enum: `Applied(Fingerprint) | PreconditionFailed`.
  - Hook points at every step boundary.
- **Done when:**
  - Unit tests: create on an empty name succeeds; create on an existing name gives `PreconditionFailed` and leaves no temp file behind; symlink and mkdir behave the same way.
  - A hook-injected concurrent create (just before the rename) results in `PreconditionFailed` with the user's file untouched.
- **Notes:**
  - **Hooks** (`fs/hooks.rs`, feature `hooks`): `point(name)` is an `#[inline(always)]` no-op unless `cfg(any(test, feature = "hooks"))`. The registry is **thread-local**, so parallel tests don't see each other's hooks; commit code runs on the caller's thread. API: `on(name, FnMut) -> Guard` (removed on drop), `once(name, FnOnce)`, `start_trace()`/`take_trace()` (the ordered list of points reached, for T07/T15 to enumerate every point per op). Closures are taken out while running, so they may re-enter `point`.
  - **Hook points** (shared names across ops; use the trace to see which an op hits): `commit.resolved`; `stage.created`, `stage.written`, `stage.synced`, `stage.linked`; `create.before_rename`, `create.after_rename`; `commit.synced`, `commit.verified`. Files hit all `stage.*`; symlinks and dirs only `stage.linked`.
  - **tmpname**: `.~fsync.<16 hex>`, `.~fsync.old.<id>`, `.~fsync.del.<id>`, plus `is_reserved`, `parse` (for journal replay) and `TmpId::random`. `conflict_name` lives here too (see T05).
  - **API** (`fs/commit.rs`): functions take `&Ctx { root, caps, replica }` instead of a bare root, since they need the caps (temp-file strategy) and, from T05, the replica ID for conflict names. `create_file(ctx, path, &mut dyn Read, FileMeta { mode, mtime_ns }, &expected_hash)`, `create_symlink(ctx, path, target)`, `mkdir(ctx, path, mode)`. `Outcome::PreconditionFailed(&'static str)` carries a reason for logs/tests.
  - **TempFile**: O_TMPFILE when `Caps::tmpfile_usable()`, linked with `AT_EMPTY_PATH` or `/proc/self/fd` per caps; else `O_CREAT|O_EXCL` under a temp name. `finish(meta)` = fchmod (setuid/setgid stripped: `& 0o1777`), futimens (atime `UTIME_OMIT`), fsync, link in, and returns a `Staged` that keeps the fd (pins N). Dropping a `TempFile` or a live `Staged` removes the temp name only if it still holds our inode (and, for `Staged`, the same size/mtime).
  - Hash mismatch → `PreconditionFailed("content hash mismatch")`, nothing linked. A cheap "name exists" precheck runs before staging so we don't write content for a create that can't succeed; `RENAME_NOREPLACE` is still the binding check.
  - **Deviation (design §5.4 updated):** `mkdir` stages a `.~fsync.<id>` dir (0700), pins it, fchmods it to the exact mode (no umask), then renames with NOREPLACE; same step 5 checks.
  - **Step 5:** fsync the parent (separate `O_RDONLY` open of `.`), name must hold N (ino+kind, plus size+mtime for non-dirs), parent must re-resolve to the same (dev, ino). A failure here means the commit happened but something changed right after: `Err(Unstable)`, so the caller rescans (design §5.3 step 5 updated). Reserved target names and the root are refused with `InvalidPath`.
  - Tests (11 in commit.rs + 3 hooks + 4 tmpname): create file (O_TMPFILE and named fallback), mode/mtime incl. negative mtime and 300 KB content, existing name, hash mismatch, hook-injected concurrent create before the rename (file, symlink, dir-with-child) → `PreconditionFailed`, user object untouched, no leftovers; exact hook trace; symlinked ancestor → `Unstable` with nothing written outside; ancestor swapped for an outside symlink after resolution → write lands in the pinned (moved) dir, outside dir empty, `Unstable`; name replaced right after the rename → `Unstable`.

### [x] T05: CAS replace via RENAME_EXCHANGE
- **Depends on:** T04
- **Read:** §5.3 step 4, §5.10
- **Files:** `src/fs/commit.rs`
- **Do:**
  - `commit::replace_file(root, relpath, expected: &Fingerprint, content, meta)` and `replace_symlink(...)` implementing (a) pin and check, (c) exchange, (d) verify, (e) undo or conflict copy, and (f) quarantine with a grace timer. Leases come in T18; leave a hook for them.
  - `Outcome` gains `Preserved { conflict: RelPath }`.
  - Add a `Quarantine` struct listing pending `.old.` files with deadlines, and `Quarantine::sweep()` to process them.
  - Conflict-copy naming helper: `conflict_name(name, now, replica_id)` (§6.2). Put it in `engine/conflict.rs`, or in `fs/` if that is simpler, and note where.
- **Done when:** these tests pass:
  - normal replace;
  - expected fingerprint mismatch → `PreconditionFailed`, file unchanged;
  - hook writes to the file between (a) and (c) → user content survives at `name` (after the undo);
  - hook replaces the file with a new inode between (a) and (c) → user content survives;
  - hook holds an fd, and the test writes through it after (c) → the quarantine sweep produces a conflict copy containing that write.
- **Notes:**
  - `replace_file(ctx, &mut Quarantine, path, &Expected, content, meta, &expected_hash)` and `replace_symlink(ctx, &mut Quarantine, path, &Expected, target)`. `Expected { fp, hash: Option<[u8;32]>, racy }` (`From<Fingerprint>`): `hash` enables the (d) rehash, done when `racy` or size < 1 MiB (`REHASH_BELOW`). T11 should fill it from the index entry.
  - The old object may be a file or a symlink for either function (file↔symlink type change in one exchange; design §5.5 updated); a directory gives `PreconditionFailed("not a file or symlink")`.
  - Flow: precheck (cheap), stage, (a) pin with `O_PATH|O_NOFOLLOW` + `Fingerprint::unchanged` against `expected.fp`, (b) placeholder comment for the T18 lease, (c) `RENAME_EXCHANGE`, (d) `same_object` (ino, kind, size, mtime; no ctime) + optional rehash via `stable_read`, (f) rename to `.~fsync.old.<id>` and add to the quarantine, then step 5. ENOENT on the exchange (name deleted after the pin) → `PreconditionFailed("name removed")`. Errors during (d) count as "changed", so they trigger the undo rather than leaving the user's file under a temp name.
  - (e) undo: exchange back; if the object coming out is N unchanged, unlink it → `PreconditionFailed("changed during commit")`; if not, it is user data too → renamed to a conflict name → `Preserved { conflict }`. If the name was deleted after (c) (ENOENT on the second exchange), the user's object is renamed back with NOREPLACE (conflict name on EEXIST).
  - **Quarantine**: `Quarantine::new(replica, grace)` (`DEFAULT_GRACE` = 400 ms = 2 × debounce), `len`, `is_empty`, `next_deadline`, `sweep() -> SweepReport { removed, conflicts, dropped }`. Each entry holds an `O_PATH` fd for the parent dir (found even if the dir is renamed) and one pinning the old inode (so its ino can't be reused). Sweep: changed size/mtime → conflict copy right away; unchanged and past the deadline → unlink; name gone or holding a foreign inode → forgotten and left alone. Per-entry errors are logged and retried next sweep. Not persisted: T15's journal must cover `.old.` files across restarts.
  - Residual: the stat → unlink window in the sweep (a write through a still-open fd in that instant is lost). T18's lease check closes it; already listed in design §5.10.
  - **Conflict names**: `fs::tmpname::conflict_name(name, split_ext, now: civil::DateTime, replica)` (in `fs/` because `fs::commit` needs it and `fs` must not depend on `engine`; T13's `engine/conflict.rs` should reuse it). Extension = after the last `.` that is neither first nor last; names are shortened to fit 255 bytes. On EEXIST the timestamp is bumped by 1 s (up to 16 tries). Local time from `jiff::Zoned::now()`.
  - New hook points: `replace.before_pin`, `replace.pinned`, `replace.before_exchange`, `replace.after_exchange`, `replace.before_rehash`, `replace.verified`, `replace.quarantined`, `replace.before_undo`, `replace.after_undo`, `quarantine.before_unlink`, `quarantine.before_conflict`.
  - Tests (14): normal replace (both temp strategies) + sweep; exact hook trace; stale expectation and missing name; change between precheck and (a); append between (a) and (c) → undone, same inode back at `name`; same-size write with mtime restored → caught only by the rehash; new inode (and a directory) swapped in between (a) and (c) → user object back at `name`; name deleted before (c); fd held from before (c) and written after → sweep makes `f.sync-conflict-…-abcdef0.txt` with the late write, and the fd keeps writing into it; grace period respected and a foreign object at the `.old.` name left alone; both objects modified around (c) → `Preserved`; name deleted after (c) → user object restored; symlink replace normal/stale/raced, file↔symlink; directory refused.

### [x] T06: CAS delete and rmdir
- **Depends on:** T05
- **Read:** §5.6, §5.7
- **Files:** `src/fs/commit.rs`
- **Do:** `commit::delete(root, relpath, expected)` for files and symlinks: pin, rename to `.del.`, verify, then unlink or quarantine, with restore and conflict fallbacks. `commit::rmdir(root, relpath)`: ENOTEMPTY → `PreconditionFailed`.
- **Done when:** tests cover:
  - a normal delete;
  - a modification just before the rename (hook) → the file is restored at `name`;
  - the name re-created by the user after our rename → the restored file goes to a conflict name and both survive;
  - rmdir with a child created by a hook just before → `PreconditionFailed`, child intact.
- **Notes:**
  - **API:** `delete(ctx, &mut Quarantine, path, &Expected)` (file or symlink; a directory gives `PreconditionFailed("not a file or symlink")`) and `rmdir(ctx, &mut Quarantine, path)`. New `Outcome::Removed` for a successful delete/rmdir (`Applied(Fingerprint)` has nothing to carry); T11 maps it to `Applied(tombstone)`.
  - **Delete flow:** same precheck as replace, then (1) pin + check (`pin_expected`, now shared with replace), (2) `renameat2(name → .~fsync.del.<id>, NOREPLACE)` (ENOENT → `PreconditionFailed("name removed")`), (3) `verify_old` (shared with replace; it takes the rehash hook point as an argument), (4) quarantine (renamed on to `.~fsync.old.<id>`) instead of unlinking, so a write through a held fd becomes a conflict copy. (5) On a failed verify, `restore` (shared with replace's undo) moves it back with NOREPLACE; on EEXIST it keeps it under a conflict name → `Preserved`. Step 5: fsync the parent, the name must be **free**, and the parent must re-resolve to the same (dev, ino); otherwise `Err(Unstable)`.
  - **rmdir:** checks the name is a directory, **settles** the quarantine entries of that directory (`Quarantine::settle_dir`: unchanged → unlink now, written since → conflict copy in the dir), then `unlinkat(AT_REMOVEDIR)`. ENOTEMPTY/EEXIST → `PreconditionFailed("directory not empty")`; ENOTDIR (replaced by a file/symlink) → `"not a directory"`; ENOENT → `"name removed"`. Without settling, deleting children and then their directory (§5.7) would always fail on the quarantined `.old.` files. Design §5.6, §5.7 and §5.10 updated.
  - `Pending` now records its directory's identity (`dir_fp`); `Quarantine::sweep` and `settle_dir` share `process()`.
  - Residuals (design §5.7, §5.10): rmdir has no fingerprint precondition, so an empty directory swapped for another empty one is removed (no data lost); settling cuts the grace period short for children of a removed directory.
  - New hook points: `delete.before_pin`, `delete.pinned`, `delete.before_rename`, `delete.after_rename`, `delete.before_rehash`, `delete.verified`, `delete.quarantined`, `delete.before_restore`; `rmdir.checked`, `rmdir.before_rmdir`, `rmdir.after_rmdir`. T18 lease placeholders are marked at `delete.before_rename`.
  - Tests (11): normal delete of a file and a symlink with the exact trace; stale/missing/directory; a modification before the rename → restored at `name` (same inode); a same-size, mtime-preserving write → caught by the rehash and restored; a new inode (a directory with a child) before the rename → restored; a modification before the rename plus the name re-created after it → `Preserved`, conflict copy `f.sync-conflict-…-abcdef0.txt` and the new file both survive; name re-created after a valid delete → `Unstable`; a write through a held fd after the delete → conflict copy on sweep; rmdir normal (trace), file, symlink-to-dir, missing, non-empty; a child created by a hook just before the rmdir → `PreconditionFailed`, child intact; a directory replaced by a file just before → file untouched; rmdir settling quarantined children (grace 1 h): a late-written child becomes a conflict copy and keeps the directory, while unchanged children are unlinked, entries in other directories wait, and the rmdir succeeds.

### [x] T07: Race-injection attack suite
- **Depends on:** T06
- **Read:** §5.1, §9
- **Files:** `tests/attack.rs` (integration; needs the `hooks` feature: `cargo test --features hooks`)
- **Do:**
  - For **every** hook point in commit.rs, and for each of create, replace, delete, mkdir and symlink:
    - replace an ancestor directory with a symlink to an outside tempdir;
    - move an ancestor directory out of the root;
    - swap the target for a symlink.
  - After each case, assert:
    - nothing was created or changed in the outside dir (snapshot it before and after);
    - every piece of user content written by the hook still exists somewhere in the root (at its name or a conflict copy);
    - no `.~fsync.*` leftovers remain after `Quarantine::sweep` with a zero grace period.
- **Done when:** the whole suite passes. Record any accepted residual (§5.1, a directory moved out) in Notes.
- **Notes:**
  - `tests/attack.rs`, gated with `[[test]] required-features = ["hooks"]` in Cargo.toml, so a plain `cargo test` skips it. Run it with `cargo test --features hooks --test attack` (~3.5 s).
  - **Operations:** create file, create symlink, mkdir, replace file, replace symlink, delete file, delete symlink, rmdir, and delete-then-rmdir (delete `a/b/t/c`, then rmdir `a/b/t`, which covers the quarantine settle). The target is always `a/b/t`. File-staging ops run under both temp strategies (O_TMPFILE and named).
  - **Variants** reach the undo, restore and conflict paths: `EditBefore` (the user rewrites the file through a held fd at `replace.before_exchange` / `delete.before_rename`) and `LateWrite` (through the held fd at `*.quarantined`, so the sweep or the rmdir settle makes a conflict copy).
  - **Enumeration:** each op × variant × caps runs once clean with `hooks::start_trace`. Then, for **every position** in the trace (the nth hit of a point, so the two `commit.*` passes of delete-then-rmdir are both attacked; the trace includes the quarantine sweep), each of the 6 attacks is injected on a fresh tree: ancestor `a` or `a/b` → symlink to the outside dir; `a` or `a/b` moved out of the root (into a separate "away" tempdir); the target moved aside within the root and replaced by a symlink to an outside file or directory. 1242 cases. Each attack first writes a unique marker file in `a/b`. The outside dir holds decoys at every path a followed link would reach (`t`, `b/t`, `victim`, `vdir/`).
  - **Checks per case:** the outside dir snapshot (kind, mode, size, mtime, ctime and content of every entry, the dir itself included) is unchanged; every tracked piece of user content (markers, the attacker's symlink, held-fd writes) exists as a file or symlink somewhere in the root; an `Applied`/`Removed` outcome holds right after the call (skipped only when the attack fired at that call's `commit.verified`); no `.~fsync.*` names remain in the root or the away dir after a final zero-grace `Quarantine::sweep`; the injection actually fired.
  - **Coverage test** `every_hook_point_is_attacked`: it parses every hook point name out of `src/fs/commit.rs` (outside its tests) and requires the clean traces to reach all of them (and nothing else). It also checks that the clean runs do what they should (success, `PreconditionFailed` for `EditBefore`, a resurrected dir for a late write before rmdir) with nothing lost and no leftovers. New hook points therefore fail this test until a scenario reaches them.
  - **Mutation check** (done by hand, not committed): disabling the parent re-resolution in step 5 fails 5 suites through the outcome check; making `restore` overwrite (no NOREPLACE) fails delete through the lost-content check.
  - **Accepted residual (§5.1, directory moved out):** when `a` or `a/b` is moved out of the root mid-commit, the commit completes inside the moved directory, through the pinned parent fd. The new file/dir/symlink, a restored file, or a conflict copy can then end up in the away dir, and our own temp and `.old.` names there are cleaned up through the same fds. The op reports `Err(Unstable)` (or `PreconditionFailed`), never `Applied`. Nothing is ever created in a directory that was never inside the root, and the "user content survives" check counts the away dir only for this attack. No other residuals were found; the suite passed without changes to `commit.rs`.

## M2: index, symlinks, scanner

### [x] T08: Index entries, version vectors, redb store
- **Depends on:** T01
- **Read:** §3, §6.1
- **Files:** `src/index/{mod,entry,vv,store}.rs`
- **Do:**
  - `Entry`, `Kind`, `UnmanagedReason` and `LocalMeta` exactly as in §3, with serde derives. On the wire, `LocalMeta` is `#[serde(skip)]`, or use a separate wire type.
  - `VersionVector`:
    - `bump(id)` (max + 1);
    - `merge`;
    - `compare -> Ord4 { Equal, Dominates, Dominated, Concurrent }`.
  - `IndexStore` (redb). Tables:
    - `entries: &[u8] -> Entry`;
    - `by_seq: u64 -> &[u8]`;
    - `meta` (replica id, next_seq, max counter).
    - API: `get`, `put` (assigns seq), `changes_since(seq)`, `iter_prefix`, and transactions.
- **Done when:**
  - proptest: version-vector compare is reflexive, antisymmetric and transitive (a partial order); merge is commutative, associative and idempotent, and dominates both inputs.
  - Store round-trip tests pass.
  - `changes_since` returns every entry put after the given seq.
- **Notes:**
  - **Entry** (`index/entry.rs`): `Entry`, `Kind`, `UnmanagedReason`, `LocalMeta` as in §3. `Entry`'s serde form is the **wire** form (`local` is `#[serde(skip)]`, tested: the raw target bytes don't appear in the encoding); the store persists `(Entry, LocalMeta)`. `LinkInfo { ino, ctime_ns, raw_target, out_of_tree }` was left open by the design (T10/T17 may extend it). Also `Entry::new` (seq 0, default local), `is_live`/`is_tombstone`/`is_unmanaged`, and `sync_mode(st_mode) = & 0o1777` (same mask as `commit.rs`).
  - **VersionVector** (`index/vv.rs`): `SmallVec<[(ReplicaId, u64); 2]>` (new dep `smallvec` with `serde`), canonical (sorted, no zeros), so derived `==` agrees with `compare == Equal`; `PartialOrd` follows `compare`. API: `bump(id)` (max + 1), `bump_after(id, floor)` (above a Lamport floor too), `get`, `set`, `max_counter`, `iter`, `merge` (returns a new vector), `compare -> Ord4`, `descends`, `FromIterator`. Deserialisation rejects unsorted, duplicate, zero or > `MAX_COUNTER` (2^62) counters, so `bump` can't overflow and needs no error path.
  - **IndexStore** (`index/store.rs`): `open(path, replica)` creates or checks `meta` (schema 1, replica ID; mismatch → `Error::BadIndex`); `path_for(pair_dir, id)` = `<pair>/<id>.redb`. Values are stored as `&[u8]` and decoded by us, so a corrupt record is `BadIndex`, not a panic inside redb's `Value::from_bytes`. `by_seq` keeps exactly one row per entry (the old one is removed on overwrite). Transactions: `read() -> ReadTxn` (snapshot: `get`, `changes_since`, `iter_prefix`, `len`, `next_seq`, `max_counter`) and `write() -> WriteTxn` (`get` sees own puts, `put(&path, &mut Entry) -> seq` sets `entry.seq`, `remove` for T18's GC, `commit`; drop = abort). The single-op helpers on `IndexStore` each use one transaction.
  - `iter_prefix` is component-wise (`a` gives `a`, `a/…`, not `ab` or `a.txt`; root gives everything), via the key range `["a/", "a0")`. `changes_since` and `iter_prefix` return `Vec`s; fine for now, a streaming/callback form can come with T21 if large indexes need it.
  - `meta.max_counter` is the largest vv counter ever put (a Lamport clock), for `bump_after`. T10 should bump with `vv.bump_after(local_id, store.max_counter())` so a re-created path outranks anything seen before.
  - New errors: `Error::Db(redb::Error)` (every redb sub-error goes through `redb::Error`) and `Error::BadIndex { reason }`.
  - Tests (16): vv basics, compare cases, non-canonical serde rejected, proptests (partial order: reflexive, swap-consistent, antisymmetric, transitive; merge commutative, associative, idempotent, upper and least upper bound; bump strictly dominates; serde round trip); entry wire form; store round-trip of every kind with full `LocalMeta`, non-UTF-8 key and the root key, across reopen; overwrite reassigns seq; `changes_since` against a model over 50 puts with overwrites, at every cut; `iter_prefix`; transaction abort/snapshot isolation/remove; another replica's index rejected; corrupt record → `BadIndex`.

### [x] T09: Symlink safety, munging, policy
- **Depends on:** T01
- **Read:** §4 (all)
- **Files:** `src/symlink/{mod,safety,munge,policy}.rs`, `src/config.rs` (finalize `SymlinkPolicy`)
- **Do:**
  - `safety::is_unsafe(link_relpath, target: &[u8]) -> bool`, a faithful port of rsync `util1.c:unsafe_symlink()`. Copy its semantics, including how `.` and repeated `/` are handled.
  - `munge`: `munge(&[u8]) -> Vec<u8>` and `unmunge(&[u8]) -> Vec<u8>` with the prefix `/rsyncd-munged/` (§4.4).
  - `policy::classify(policy, link_relpath, target, referent_kind: Option<Kind>) -> Treatment { AsSymlink, Follow, Unmanaged(reason) }` covering every row of §4.3 except the `-K` adoption (T17).
- **Done when:**
  - a table test with at least 20 cases (absolute, `..` escapes at various depths, `a/../..`, `./x`, trailing slash, empty target);
  - proptest: `unmunge(munge(x)) == x` for all x, and `munge` is injective;
  - a classification table test for each policy × {safe, unsafe, dangling, dir referent}.
- **Notes:**
  - **Deviation (design §4.2 updated):** `safety::unsafe_symlink(dest, src)` ports rsync **3.4.1**, not the classic version. The classic algorithm (identical in upstream 3.2.7) disagreed with the installed `rsync 3.2.7` on 8 of 41 cases: Ubuntu backports the CVE-2024-12088 fix, which also rejects a `/../` after the leading `../` run (`a/../x`, `./../x`) and a trailing `/..` (`x/..`, even `../..` at depth 2). T17's differential test compares against the installed rsync, so we follow the current upstream. Quirks kept: `..` in `src` resets the margin, a leading `/` in `src` adds depth, repeated `/` are skipped, only exact `.`/`..` are special, input ends at the first NUL. `is_unsafe(&RelPath, target)` wraps it.
  - The 40-row table (plus raw-`src` quirks) was checked one by one against `rsync -a --safe-links` on this machine (scratch script, not committed; 45/45 agree including the policy-test targets). T17 can turn this into a permanent differential test.
  - **munge** (`symlink/munge.rs`): `munge`, `unmunge` (strips exactly one prefix; a target without it is returned unchanged), `is_munged`, `PREFIX`. Unlike rsync's sender (which unmunges only when `len > prefix len`), the exact-prefix target `/rsyncd-munged/` unmunges to the empty target, so `unmunge(munge(x)) == x` holds for all `x`; empty targets can't exist on Linux anyway. T17 should use `is_munged` to spot user-made unprefixed links in a munged replica.
  - **policy** (`symlink/policy.rs`): `classify(policy, link, canonical_target, referent: Option<FileKind>) -> Treatment`. **Deviation from the task text:** the referent is an `fs::FileKind` (what `stat` says), not an index `Kind`, because it describes the on-disk object, not an entry. `None` = dangling; `Some(Special)` under following → `Unmanaged(Special)`; `Some(Symlink)` (impossible after `stat`) is treated as dangling. Loops (ancestor dev/ino, `ELOOP`) are for the scanner (T10). `needs_referent(policy, link, target)` tells the scanner when it can skip the `stat`. Rows: Skip → `IgnoredLink`; Links → `AsSymlink`; CopyLinks → `Follow`/`Dangling`; CopyUnsafeLinks → unsafe as `-L`, safe as `-l`; SafeLinks → unsafe `IgnoredLink`, safe `AsSymlink`; CopyDirlinks → `Follow` only for a dir referent, else `AsSymlink` (dangling stays a link, as in rsync's `link_stat`). `-K` and munging are per-replica and not part of `classify`.
  - `config::SymlinkPolicy` is final: docs describe each policy's §4.3 behaviour, `Hash` was added, and it is re-exported as `symlink::SymlinkPolicy`.
  - Tests (7): safety table (40 rows) + raw-`src` quirks; munge examples + proptests (round trip, injective; inputs biased towards repeated and partial prefixes); policy matrix of 6 policies × {safe, unsafe} × {file, dir, dangling, special}, each with 3 target spellings, plus a check that the referent is ignored when `needs_referent` is false; depth-dependent safety.

### [x] T10: Scanner
- **Depends on:** T03, T08, T09
- **Read:** §3, §4.5, §5.2
- **Files:** `src/scan/{mod,scanner,hasher}.rs`
- **Do:**
  - `Scanner::scan(root, index, policy, scope: Full | Paths(Vec<RelPath>))`:
    - recursive, fd-based (open each child directory via openat2 from the parent fd);
    - never follow symlinks except as `classify` says, with following done as in §4.5;
    - skip reserved names;
    - track the (dev, ino) stack for loops;
    - rehash shortcut plus the racy flag;
    - on change, write the new Entry with `vv.bump(local_id)`;
    - paths missing from disk become a Tombstone (with a vv bump) unless already a tombstone;
    - an unstable read leaves the entry unchanged and reports the path as dirty.
  - Return `ScanStats { changed, dirty: Vec<RelPath>, ... }`.
- **Done when:** integration tests on a tempdir show:
  - a first scan indexes files, dirs and symlinks correctly under `Links` and `Skip`;
  - a modification bumps the vv once;
  - an untouched rescan rehashes nothing (count via a hasher counter) except racy entries;
  - a deletion produces a tombstone;
  - a loop link becomes `Unmanaged(Loop)` under `CopyLinks`;
  - a dangling link becomes `Unmanaged(Dangling)` under `CopyLinks`.
- **Notes:**
  - **API** (`scan/scanner.rs`): `Scanner::new(&Root, &IndexStore, SymlinkPolicy)` with builder options `.munge_links(bool)` and `.racy_window(Duration)`, then `.scan(&Scope) -> ScanStats`. This is a builder rather than the `scan(root, index, policy, scope)` form in the task text. The replica ID that gets bumped is `index.replica()`. `Scope::{Full, Paths(Vec<RelPath>)}`. `ScanStats { scanned, hashed, changed, tombstoned, dirty, errors }`: `changed` counts version-vector bumps (tombstones included), and `errors` lists paths skipped for non-race reasons (EACCES, a mount point). Only root and index failures abort the scan.
  - **Traversal:** depth-first. Each child directory is opened with `openat2(parentfd, name, O_RDONLY|O_DIRECTORY, RESOLVE)` and must be the same inode as its `statx`. Children are listed before recursing, so one fd stays open per level. Reserved names are skipped at every level. Every directory (real or followed) is checked against the (dev, ino) stack, so a loop reached through a followed parent's real subdirectory also becomes `Unmanaged(Loop)`.
  - **Following (design §4.5 updated):** the referent is opened beneath the root via the link's own root-relative path (`BENEATH|NO_MAGICLINKS|NO_XDEV`). On `EXDEV` it is opened from the link's dirfd with only `NO_MAGICLINKS` and marked `out_of_tree`. If the two routes disagree, the link is unstable. The first open is `O_PATH`, to get the referent's kind. Files are read through the new `fs::stable_read_with` (`stable_read` was refactored onto it; same checks, with the open and the F3 check made pluggable), with link-unchanged and same-referent rechecks. Directories are reopened as `"."` beneath the `O_PATH` fd. ENOENT/ENOTDIR mean dangling, and ELOOP means `Unmanaged(Loop)`. Children of a followed directory are indexed under the link's path (`l/x`), and only the link's own entry carries `via_link`. T11/T17 must check ancestors for `via_link` before writing beneath a followed link.
  - **Change rules (design §3 updated):** a logical change is kind, hash/target, mode, and mtime for files only. Directory and symlink mtimes alone are not changes, and the stored value is the one from the last logical change. Logical changes use `vv.bump_after(id, txn.max_counter())`. A change to `LocalMeta` alone goes through the new `WriteTxn::put_local` (same seq, so `changes_since` doesn't re-report it). Unmanaged entries compare by reason only. Tombstones get mode 0, mtime 0 and default `LocalMeta`. An `Unmanaged` entry whose object disappears becomes a tombstone too, as the task text asks. T12 should check that this is what it wants (see the file→ignored-link→deleted case).
  - **Racy:** a file is `racy` when its ctime ≥ scan start − window. `DEFAULT_RACY_WINDOW` = 1 s; tests use 200 ms and sleep past it. Only files (incl. followed files) are ever racy.
  - **Tombstones and protection:** the scan records every path it found. Afterwards, indexed non-tombstone entries in the scanned regions that weren't found become tombstones, except dirty or failed paths and their subtrees. Updates are batched (1024 per write txn), and every write checks that the entry still has the seq read at the scan start; otherwise the path is reported dirty.
  - **Scoped scans:** paths are deduplicated to the outermost ones. Each one is reached by walking down from the root, recording every ancestor (so a new `n1/n2/leaf` also indexes `n1` and `n1/n2`). If an ancestor is gone, or is not a real directory (a file, or a symlink, followed or not), that ancestor is scanned recursively instead and becomes the tombstone region.
  - **Hasher** (`scan/hasher.rs`): `Hasher { hash_at, hash_with, hashed() }`. The sink reaches the hook point `scan.read_chunk` on every chunk. The scanner also has `scan.stat` after each `statx`. Neither is in `commit.rs`, so `tests/attack.rs` is unaffected.
  - **Not done / follow-ups:** `LocalMeta.mnt_id` is still 0, because `Fingerprint` has no mount ID. A mount point inside a root is reported in `errors` on every scan and never indexed. A directory that can't be opened (EACCES) keeps its old entry, even its mode. Out-of-tree watches and `-K` are T16/T17. A munged replica unmunges every target; spotting user-made unprefixed links is T17. Recursion depth is unbounded (one fd and one stack frame per level).
  - Tests (19 new): `tests/scan.rs` (12): first scan under `Links` (files, modes with setuid stripped, sticky dir, links incl. absolute, dangling and non-UTF-8, FIFO → `Special`, reserved names skipped, distinct counters) and `Skip`; modify → one bump above the Lamport floor, quiet rescans, mtime-only change, chmod, dir mtime not a change, symlink re-created identical vs retargeted; untouched rescan hashes 0 and racy entries are rehashed exactly until settled (cleared in place, same seq); deletion of a tree → tombstones, no re-bump, re-creation dominates the tombstone, file→dir; `CopyLinks` loops (`..`, `.`, an ELOOP chain); dangling (missing, absolute, through a file) and then resolved; followed files/dirs in and out of tree, a diamond, the shortcut for followed files, local-only retarget, referent content change; `CopyDirlinks`, `SafeLinks`, `CopyUnsafeLinks`; munged targets; scoped scans (dedupe, ancestors indexed, missing ancestor, ancestor replaced by a symlink, root in scope); an unreadable dir keeps its subtree. Unit tests (4): `prefixes`/`outermost`; a read that changes on every attempt → dirty, entry unchanged, exactly 4 attempts; a dir swapped for an outside symlink between `statx` and open → dirty, nothing outside indexed; an index entry rewritten mid-scan → dirty, not overwritten. Plus `put_local_keeps_seq` in `store.rs`.

## M3: engine → simplified variant complete

### [x] T11: Replica trait and LocalReplica
- **Depends on:** T06, T10
- **Read:** §7, §5.3 step 5, §5.8 (only the "no echo" idea; the journal comes in T15)
- **Files:** `src/replica/{mod,local}.rs`
- **Do:**
  - Implement the trait, `Op`, `Precondition` and `Outcome` exactly as in §7.
  - `LocalReplica` owns a `Root`, an `IndexStore`, the `Caps`, a `Quarantine` and its config.
  - `apply` maps a logical `Precondition::Matches{..}` to the indexed `LocalMeta` fingerprint, calls the right `fs::commit` function, and on success writes the resulting Entry into the index (the vv comes from the op).
  - `open_read` wraps `stable_read`, streaming, and verifies the expected hash at EOF.
  - `watch()` returns `None` for now.
- **Done when:**
  - unit tests apply each `Op` on a tempdir replica and check both the disk and the index;
  - a stale precondition gives `PreconditionFailed`.
- **Notes:**
  - **API deviations (design §7 updated):**
    - `apply` takes the `path` (§7's signature had none).
    - Every `Op` that leaves an entry carries its `vv` (and `Mkdir`/`Symlink` their `mtime_ns`), since "the vv comes from the op": `WriteFile{meta: FileMeta, hash, vv}`, `Mkdir{mode, mtime_ns, vv}`, `Symlink{target, mtime_ns, vv}`, `Delete{vv}`, `Rmdir{vv}`, `RenameToConflict{to: RelPath}`, `SetMeta{mode, mtime_ns, vv}`.
    - `Precondition::Matches{kind: Kind, vv}`: the `Kind` carries the hash or target, and `Precondition::matching(&entry)` builds one.
    - `Outcome::Applied(Entry)` is the path's new entry and `PreconditionFailed` carries the current index entry, both without `LocalMeta`, like `changes_since`.
    - `ContentReader` = `Read + Send` (blanket impl). `Op`, `Precondition` and `FileMeta` derive serde for T20.
  - **`LocalReplica`** (`replica/local.rs`): `open(&ReplicaConfig, pair_dir)` owns the capability probe (log + `require_minimum`, as T02 asked). Builder options `racy_window` and `quarantine_grace`. Accessors `root`, `caps`, `index`, `config`, `quarantine`, plus `sweep_quarantine()` for T16's timer (not in the trait). `scan` builds the T10 `Scanner` with the replica's policy and `munge_links`. `watch()` returns `None`. A minimal `watch::Hint { Paths, FullRescan }` exists for the trait; T16 owns it.
  - **apply** is one redb write txn: logical check against the index, then `Expected` from the entry (files from the index; symlinks by checking the indexed (dev, ino, ctime) against a fresh `statx`, because the index keeps a symlink's mtime from its last *logical* change), then the `fs::commit` call, then the put(s) and the commit. A failed commit or an `Err` aborts the txn, so the index is untouched.
    - Refused with `PreconditionFailed`: an `Unmanaged` entry; a path that is, or lies beneath, a followed link (`via_link`; T17's write-back); a parent that is not an indexed real directory; `Rmdir` while an indexed descendant is still live.
    - `Error::InvalidOp` (new) for an op that can't fit the indexed kind (caller bug).
    - Files we write are stored `racy`: the next scan rehashes them once (and, being within that scan's racy window, once more). It finds nothing to bump: no echo, tested after every op.
  - **New commit functions** (each gets hook points and attack-suite coverage):
    - `commit::rename_to(ctx, path, &Expected, to)` for `RenameToConflict`: pin and check, move aside to a reserved name, verify (rehash), restore on change, then `NOREPLACE` onto `to` (restore → `PreconditionFailed("new name exists")` if it was taken), then step 5 at `to`.
    - `commit::set_dir_mode(ctx, path, &Fingerprint, mode)`: pin the dir, require the same inode **and** the indexed mode, `fchmod` through the pin, then step 5. This is the one in-place change (design §5.4).
    - Hook points: `rename.{before_pin,pinned,before_rename,after_rename,before_rehash,verified,before_restore,moved}` and `chmod.{before_pin,pinned,after_chmod}`.
    - `tests/attack.rs` gained `RenameFile` (Plain + EditBefore at `rename.before_rename`), `RenameSymlink` and `ChmodDir`: 1464 cases (was 1242), all passing with no change to existing commit code.
  - **RenameToConflict index update:** the path becomes a tombstone with the old vv plus a **local bump**. With the old vv unchanged, it would equal the peer's view of the loser while the content differs, and §6.1 would loop on "rescan". The copy at `to` gets a fresh vv `{local: tombstone counter + 1}`. Files and symlinks only; `to` must be a sibling (`InvalidPath` otherwise). T12 must resolve dir-vs-dir (mode) conflicts without renaming.
  - **SetMeta:**
    - A file whose mode or mtime differs is rewritten as a **copy** (a `StableReader` on itself → `replace_file`; the hash check guards against a change mid-copy), never `fchmod`ed in place. In-place metadata changes would hide a concurrent write from the rehash shortcut.
    - A directory whose mode differs → `set_dir_mode`.
    - Otherwise the index alone is updated after a `statx` check: same file mode/mtime, a directory's mtime, any symlink. So T12/T13 can use `SetMeta` with unchanged metadata to record a **merged vv** (`MergeVv`).
  - **open_read** uses the new `fs::StableReader` (in `stat.rs`): one attempt, §5.2 checks, size checked at open, hash at EOF. Failures are `io::Error`s wrapping `Error::Unstable`; new `Error::from_stream` unwraps them, and `TempFile::copy_from` now uses it, so a torn source makes `apply` return `Err(Unstable)` with nothing committed. Also new: `Error::is_unstable`, `Error::is_not_found`.
    - Our index entry must have the expected kind (else `Unstable`), and `expect` must be a file (else `InvalidOp`).
    - Followed links (`-L`) are read via `Root::open_referent`, moved there from the scanner. At EOF the link must still be the indexed inode with the same ctime; the referent path is not re-resolved.
  - **CLI:** `sync --once` and `daemon` now open both `LocalReplica`s (probe, log, open the index), then still say "not implemented" (T13/T16).
  - **For T13/T17/T18:**
    - `apply` doesn't check whether this replica's policy would manage an incoming symlink (e.g. an unsafe target under `SafeLinks` becomes `IgnoredLink` on the next scan, which bumps it). T17 should refuse or adapt that.
    - The `SetMeta` file copy keeps its read fd open across the exchange, which would block T18's lease. T18 should close it first (e.g. stage, then drop the reader).
    - `Preserved` leaves the index untouched (rescan).
  - Tests: 12 in `replica/local.rs`:
    - create/replace/file↔symlink, with no echo;
    - stale vv, kind or `Absent`, a disk change since the scan for every CAS op, a hook-injected edit mid-commit, unindexed name, unmanaged FIFO;
    - mkdir, nested write, parent-not-indexed, rmdir with live child, delete, rmdir, recreate over a tombstone;
    - munged symlinks;
    - rename to conflict (non-sibling, taken name, vv shapes, `changes_since`, winner create);
    - `SetMeta` (file copy, index-only merge, dir chmod, dir mtime, symlink, user chmod → `PreconditionFailed`);
    - `open_read` (normal 200 KB, foreign entry, non-file, rewrite mid-read → stays failed, size changed, torn source → apply `Err` with nothing committed);
    - `-L` read and retarget mid-read, write refused;
    - invalid ops; `changes_since` without `LocalMeta`.
  - Plus 6 in `commit.rs`: rename trace and refusals, an edit before the move → restored, the name taken after verify → restored; chmod trace and refusals, a dir swapped after the pin → only the pinned one changed.

### [x] T12: Reconciler and planner (pure)
- **Depends on:** T08
- **Read:** §6 (all)
- **Files:** `src/engine/{mod,reconcile,plan}.rs`
- **Do:**
  - `reconcile(a: &dyn IndexView, b: &dyn IndexView) -> Vec<Action>` with no I/O, implementing the §6.1 table and §6.2 winner choice.
  - `Action { path, kind: Push{from, to, entry} | MergeVv | Conflict{winner, loser_side, conflict_name} | Skip(reason) | Rescan }`.
  - `plan(actions) -> Vec<Phase>`, ordered as in §6.3, including type-change splitting and directory-delete resurrection.
- **Done when:**
  - an exhaustive table test over {absent, file, dir, symlink, tombstone, unmanaged}² × {vv equal, a>b, b>a, concurrent} × {same content, different content} produces the expected action;
  - an ordering test passes;
  - a resurrection test passes.
- **Notes:**
  - **API deviations (design §6.1–§6.4 updated):**
    - `reconcile(a, b, now: jiff::civil::DateTime)`: `now` (local wall-clock time) is only for conflict names, so the function stays pure. T13 passes `jiff::Zoned::now().datetime()`.
    - `IndexView` (new trait in `engine/mod.rs`): `replica`, `get`, `entries` (path order), `descendants`. `engine::Snapshot::new(id, entries)` is the in-memory one; T13 builds it from `changes_since(0)`. `Side { A, B }` names the replicas.
    - `ActionKind`: `Push{from, entry, target}` (`to` is `from.other()`; `target` is the precondition entry), `MergeVv{vv, mtime_ns, a, b}`, `Conflict(Resolution)`, `Resurrect(Resolution)`, `Skip(SkipReason)`, `Rescan`. `Resolution{winner, entry, vv, loser, conflict_name: Option<RelPath>}` replaces the task's `Conflict{winner, loser_side, conflict_name}`. `SkipReason::{Unmanaged(Side), BeneathUnmanaged(RelPath)}`.
    - **Resurrection is decided in `reconcile`, not `plan`:** a resurrected type change needs a conflict name, and the check needs the views. `plan(&[Action]) -> Vec<Phase>` only orders and splits. `Phase{kind: Conflicts|Deletes|Creates, steps}` (non-empty phases only); `Step{side, path, op: Op, pre: Precondition, source: Option<Entry>}`, where `source` is the other side's entry to `open_read` for a `WriteFile`.
  - **Semantics** (all in design §6):
    - **Conflicts write only to the loser side:** rename it away, then create the winner's version there with merged + bump (`Absent`). The winner records that vector in the **next** round (a dominating push of identical content → index-only `SetMeta`). So nothing ever dominates the loser unless the loser side really holds the winner's version, whatever the executor does after a failure. Dir-vs-dir (mode) conflicts: the loser gets the winner's mode via `SetMeta`.
    - Equal vectors with a different file mtime → `Rescan`. Concurrent with the same content → `MergeVv` with the newer mtime. Tombstone vs tombstone → no action, even with different vectors (**T18:** GC must cope with such pairs). A push of the same content → `SetMeta`, no transfer.
    - `Unmanaged` vs live → `Skip`; vs absent or tombstone → nothing. Every action beneath an unmanaged path → `Skip(BeneathUnmanaged)`. The root path is ignored.
    - The delete half of a type change keeps the **target's** vector, so a failed create is retried next round.
    - Resurrection: a dir removal on side S is blocked if S would still hold anything beneath it after the round (present and not deleted, `Unmanaged` included, or created). Deepest first, so ancestors follow. Over a type change, the other side's file or symlink is renamed to a conflict copy first.
    - Conflict names: `<ID7>` of the loser's replica; a name live in either index, or already chosen in this call, moves on by one second.
  - **For T13:**
    - Run phases in order. After a step fails, skip that path's later steps and mark it dirty.
    - `Rescan` actions are dirty paths too.
    - Keep looping while `reconcile` yields non-`Skip` actions, not only while paths are dirty: conflicts and resurrections need a second round.
    - `Skip(Unmanaged(_))` deserves a warning.
  - **Tests** (13 in `engine/`):
    - `decision_table_is_exhaustive`: {absent, file, dir, symlink, tombstone, unmanaged}² × {equal, a>b, b>a, concurrent} × {same, different content} = 288 cases, checked against a separate written-out oracle. Each case also checks the entries the action carries (merged + bumped vv dominates both, conflict-name format) and that it can be planned.
    - Unit tests: mtime rules; winner rules (mtime, ID tie-break, type rank, bumped vv, dir-vs-dir); conflict names that dodge live names (but not tombstones) and each other (long names that truncate to the same stem); an unmanaged subtree; resurrection (nested, untouched siblings still deleted); resurrection over a type change and over an unmanaged child; root ignored; `descendants` with siblings that sort inside a prefix (`d-x`, `d.x`).
    - `plan`: a push matrix (every target kind under every pushed kind → ops, phases, preconditions, vectors, source); an ordering test (rename → deletes depth-descending → creates depth-ascending, including a dir→file type change and `MergeVv`); a resurrection plan.
    - `engine/sim.rs` (proptest, 512 cases): two in-memory replicas that mirror `LocalReplica`'s index semantics (incl. `InvalidOp` as a failure). It runs random edits on a synced base, then concurrent random edits on both sides, then syncs. It asserts convergence in ≤ 3 rounds (limit 5), equal live entries, no failed steps, and that every file version survives unless the peer's vector dominates it. Mutation-checked: disabling resurrection or the conflict rename makes it fail.

### [x] T13: Executor, conflicts, `sync --once`, test harness
- **Depends on:** T11, T12
- **Read:** §6.2, §6.4, §7, §9 (harness)
- **Files:** `src/engine/{executor,conflict}.rs`, `src/cli.rs`, `tests/harness/mod.rs`, `tests/sync_once.rs`
- **Do:**
  - `Engine::sync_once(a: &mut dyn Replica, b: &mut dyn Replica)`:
    - scan both replicas;
    - reconcile;
    - execute the phases, streaming `open_read` → `apply`;
    - collect dirty paths from `PreconditionFailed` and unstable reads;
    - rescan those paths and repeat, up to 5 rounds.
  - Conflict handling: `RenameToConflict` on the loser side, then normal propagation.
  - Wire up the `sync --once <pair>` CLI.
  - Harness:
    - `Pair::new(policy)` creates two tempdir replicas plus state directories;
    - a scenario DSL (`a.write("x", "1")`, `b.symlink(...)`, `sync()`, …);
    - `assert_converged()` checks the trees are equal after policy normalization, the version vectors are equal, and there are no reserved leftovers.
- **Done when:** scenario tests pass for:
  - create, modify and delete on each side;
  - concurrent modification → conflict copy on both sides with both contents;
  - concurrent delete vs modification → modification survives;
  - type change;
  - nested directory create and delete;
  - symlinks under `Links`.
  - `cargo run -- sync --once` works on two real directories.
- **Notes:**
  - **API (design §6.4 updated):**
    - `engine::Engine` (builder `max_rounds`): `sync_once(a, b)` = `sync(a, b, Scope::Full)`. `sync` takes the initial scan scope, for T16's watcher-driven cycles. Each round reconciles the **whole** indexes (`changes_since(0)`); incremental exchange by `seq` is left for T16/T20.
    - It returns `SyncReport { rounds, applied, retried, conflicts: Vec<ConflictCopy>, resurrected, unmanaged, errors: Vec<(RelPath, String)>, unresolved }` and `is_converged()`. `engine::MAX_ROUNDS = 5`; `sim.rs` now uses it.
    - `engine/conflict.rs` is bookkeeping only (`ConflictCopy {side, path, copy}`, logging of conflicts and resurrections). The resolution itself is already in reconcile/plan (T12): `RenameToConflict` on the loser side, then normal propagation.
  - **Executor behaviour:**
    - These mark a path dirty (skip its later steps, rescan it on both sides before the next round): `PreconditionFailed`, `Preserved`, `Err` that `is_unstable()`/`is_not_found()` from either `open_read` or `apply`, `Rescan` actions, and `ScanStats::dirty`.
    - Other per-path errors (also `InvalidOp`, an engine bug) are logged and reported, and that path and its subtree are left alone for the rest of the cycle. Only `Db`/`BadIndex` errors and scan errors (root or index) abort.
    - `ScanStats::errors` paths are reported, not blocked: the scanner keeps their entries unchanged, which is safe to reconcile.
    - The loop stops when a reconcile yields no steps and nothing is dirty. A round that only rescans counts toward the 5.
  - **CLI:** `sync --once` runs one cycle, prints a summary (applied, conflict copies, resurrections, unmanaged, errors, unresolved) and exits non-zero if not converged.
    - Before exiting it drains both quarantines (sweeps until empty, at most 10 × `DEFAULT_GRACE`). Otherwise every replaced file would leave a `.~fsync.old.*` behind when the process exits. **T15** should still sweep stale quarantine names at startup (a crash or a give-up leaves them).
    - The `stubs_…` CLI test now covers only `daemon`/`status`. New test `sync_once_syncs_two_directories` (also checks that no reserved names are left after a replace).
    - Checked by hand with `cargo run -- init` + `sync --once` on two scratch dirs: initial sync, then a concurrent edit → conflict copy on both sides.
  - **Harness** (`tests/harness/mod.rs`, design §9 updated):
    - Holds two `LocalReplica`s, not generic `Replica`s: **T22** has to abstract over replica construction.
    - The `Racing` wrapper passes every `Replica` call through to its `LocalReplica` and injects a user edit before the first `apply`/`open_read` at a path. It covers the retry loop without the `hooks` feature.
    - `assert_converged`'s normalization handles only `Links` and `Skip` (plus unmunging). Other policies hit `unimplemented!` (**T17**).
  - **Tests** (`tests/sync_once.rs`, 15):
    - create, modify (content, file mode), delete on each side, plus a no-op second cycle;
    - concurrent modification with each side winning by mtime → one conflict copy with the loser's ID7, both contents on both sides;
    - concurrent identical change → `MergeVv`, no copy;
    - delete vs modification on each side → the modification survives;
    - dir delete vs new child → resurrection;
    - type changes file→dir→symlink→file→dir→file;
    - nested dirs (create, dir mode, recursive delete);
    - symlinks under `Links` (relative, dir link not descended, absolute, dangling, escaping, retarget, delete, concurrent retarget → conflict copy of the link);
    - `Skip` leaves links alone and reports a link-vs-file `Unmanaged`;
    - vv records both replicas;
    - races: destination edited before `apply` → retry → conflict; source rewritten before `open_read` → `Unstable` → resent; directory filled just before `Rmdir` → kept.
  - Mutation-checked: skipping conflict renames, or not rescanning dirty paths, makes tests fail.

### [x] T14: Model-based property test
- **Depends on:** T13
- **Read:** §9
- **Files:** `tests/model.rs`
- **Do:**
  - proptest generates sequences of ops (write, append, delete, mkdir, rmdir, symlink, rename) on A or B, interleaved with `sync()`.
  - A simple in-memory reference model predicts the outcome: last writer wins per causal history, and concurrent edits produce a conflict copy.
  - After a final sync, compare the model with the real trees: equal paths, and the content sets including conflict copies.
- **Done when:** 256 cases pass. Every shrunk failure found along the way gets fixed and added as a regression scenario in `tests/sync_once.rs`.
- **Notes:**
  - **Model** (`tests/model.rs`, design §9 updated): independent of the engine. Causal history per path is a set of event IDs, not a vv. Events are recorded at sync time, like the scanner: only when a path differs from its record (kind, content, target; mtime for files only). Per-path resolution, then a deepest-first resurrection pass. Conflict copies are predicted as (dir, origin name, loser's ID7, content) and their real names adopted from disk; everything else must match exactly after **every** sync, on top of `Pair::assert_converged`.
  - **Ops** (on A or B, paths of 1–3 components over `{a, b}`): write (unique content and mtime; replaces a dir or link, creates parents), append, recursive delete, mkdir (replaces a file or link), rmdir (empty only), symlink (5 targets: relative, nested, `../`, absolute dangling, `.`), rename (to a free path in an existing dir), sync. An op whose precondition does not hold (e.g. a file ancestor) is a no-op in both model and tree. Mode changes are not generated (dir-vs-dir mode conflicts stay covered by `engine/sim.rs`).
  - **Symlink mtimes:** a symlink's recorded mtime decides symlink-vs-symlink conflicts. A synced link's on-disk mtime is the commit time, so the user op gives every created or moved (renamed, or inside a renamed dir) link a fresh mtime via `utimensat(AT_SYMLINK_NOFOLLOW)`.
  - **Results:** 256 cases (~35 s debug) pass; 5000 cases in release passed too. Per 256 cases about 200 conflict copies and 100 resurrections (a third over a type change) are predicted and checked.
  - **Shrunk failures:** one, a model bug (a symlink re-created with the same target is no change; the model compared its mtime). Added as `recreated_symlink_with_same_target_is_not_a_change` in `tests/sync_once.rs`. No engine bug found.
  - Mutation-checked: inverting the §6.2 winner or disabling resurrection in `reconcile` fails within a few cases.
  - Not covered: user edits *during* a sync (the `Racing` scenarios in `sync_once.rs`, and T19's stress test), policies other than `Links` (T17).

## M4: durability

### [x] T15: Journal and crash recovery
- **Depends on:** T13
- **Read:** §5.3 step 1, §5.3 step 5, §5.8
- **Files:** `src/index/journal.rs`, `src/fs/commit.rs`, `src/replica/local.rs`, `tests/crash.rs`
- **Do:**
  - Add an `Intent` table to redb.
  - `commit` functions take a `&Journal`: write the intent before the temp file and update its state at each step (`TempWritten`, `Exchanged`, `Done`). Mark it Done in the same transaction as the index update.
  - `LocalReplica::open` replays unfinished intents as described in §5.8.
- **Done when:** `tests/crash.rs` passes. It forks a child that runs one apply with a hook calling `std::process::abort()` at point N. The parent reopens the replica, checks that no user data was lost and no unexplained `.~fsync.*` files remain, and checks that a following `sync_once` converges. The test sweeps N over every hook point and op type.
- **Notes:**
  - **API (design §3, §5.3 step 1, §5.8 updated):**
    - `index::journal`: `Journal` (table `intents` in the index's redb file; `IndexStore::journal()`, sharing the `Database` through an `Arc`; `Journal::in_memory()` for tests of `fs::commit` alone), `Intent { op, path, parent: Fingerprint, tmp, old, expected: Option<Expected>, staged: Option<Fingerprint>, state }`, `IntentOp { Create, Replace, Delete, Rename }`, `IntentState { Started, TempWritten, Exchanged, Quarantined }`. Methods `begin` (durable), `update(durable)`, `take_open`, `pending`, `finish(done, quarantined)`, `forget`; `WriteTxn::finish_intents` does the same inside the index transaction. `Fingerprint`, `FileKind` and `Expected` derive serde.
    - The journal reaches the commit functions through **`Ctx { journal: &Journal }`** rather than an extra parameter. The ids a commit begins collect in the journal until `take_open`, so the caller can finish them.
    - `commit::recover(ctx, &mut Quarantine, id, &Intent) -> Recovered { removed, quarantined, restored, conflicts, foreign, skipped }`.
    - `Quarantine`: entries carry their intent id; `holds(id)`, `grace`, `set_grace` (deadlines are `since + grace`, so it applies to waiting entries too); `SweepReport::finished` lists settled intents. `rmdir` forgets those it settles; `LocalReplica::sweep_quarantine` forgets the rest.
  - **Commit changes:** every reserved name is chosen at random in `Record::new` and recorded before it exists (`with_fresh_name` is gone; `EEXIST` on a reserved name is an error). `TempFile::create` takes the name; `TempFile::finish(meta, record)` calls `record(&fp)` after the fsync and before linking. Durable redb commits per op (about 2.4 ms each on this WSL2 ext4): create 1, replace 1 with `O_TMPFILE` or 2 (named temp file, symlink), delete 1, rename 1, mkdir/symlink 1; `Exchanged` and all Done records for failed commits and quarantine settles are non-durable. The model test went from ~35 s to ~37 s.
  - **LocalReplica:** `apply` no longer holds a write transaction across the commit (redb has one writer and the commit writes the journal): preconditions are checked on a read snapshot, then one write transaction stores the entries and finishes the intents. `open` replays every pending intent; after a commit that returned `Err`, its intents are replayed at once (an error path may leave reserved names). An intent whose replay fails with an I/O error is kept for the next start. `quarantine_grace` no longer requires an empty quarantine (replay may fill it); new `caps_mut()` for tests (e.g. forcing named temp files).
  - **Replay** (design §5.8): inspection-based, idempotent. Started → anything at `tmp` is ours. N → removed. O at `tmp`/`.del.` → verify: unchanged → quarantine (roll forward), changed → exchange back if the name holds N, else NOREPLACE, else conflict copy. Other objects → put back the same way. A rename's moved-aside object always goes back (roll back). O at `old` → quarantined again with a fresh grace. A moved/replaced parent → skipped, names left alone (logged).
  - **Hook points:** `journal.started`, `journal.temp_written` (in `commit.rs`; the attack suite reaches them; `commit_points` now includes the `journal` prefix; 1578 cases (was 1464), all passing) and `recover.resolved`, `recover.undone`, `recover.done`, `recover.before_rehash` (replay only; excluded from the attack suite's coverage check).
  - **Tests:**
    - `commit.rs` (+6): every reserved name seen at any step is already journaled (both temp strategies), with the right op/state per commit; replay of hand-built crash states: staged objects removed (Started: file, symlink, dir; TempWritten: N removed, foreign left), exchange with O unchanged → quarantined then re-quarantined after a "restart" (late write → conflict copy), O modified → exchanged back / conflict copy when N was replaced / swapped-in object restored, delete and rename move-asides, moved or replaced directory skipped, foreign object at `old` left alone. `journal.rs` (1), `local.rs` (1: intents finish with the index, kept while quarantined, replayed on reopen).
    - `tests/crash.rs` (`required-features = ["hooks"]`, ~22 s): 14 scenarios (create/replace file, create/replace symlink, file↔symlink, mkdir with child, delete file/symlink/tree, conflict, chmod file/dir, dir→file), file-staging ones under both temp strategies. **Deviation:** the child is the test binary re-run as `crash_child` (env `FSYNC_CRASH_CHILD`), not a `fork()`, because the parent runs cases on all cores; core dumps are disabled with `setrlimit`. Each case crashes one sync cycle (plus sweeps) at the nth hit of a traced point, then checks: every reserved name left is in its replica's journal; reopen (replay) + zero-grace sweep leaves none; the next sync converges (`assert_converged`) to exactly the crash-free outcome. Late-write variant (held fd on B, written at the crash point, points before the first quarantine sweep): the write must survive. 684 cases. `crashes_during_recovery` crashes the replay again at `recover.done` (311 cases, more than 10 replays crashed).
    - Harness: `Tree` keeps a root path (owning the tempdir optionally), `open_replica`, `Pair::open_at` (existing dirs, not removed on drop), `Pair::dirs`, `Pair::reopen_after(f)` (closes both replicas while `f` runs, then reopens them, which replays).
  - **Mutation checks** (by hand, not committed): recovery disabled → 210/467 cases fail (reserved names left); a delete journaled after its move-aside → `crash.rs` reports unexplained names; replay discarding a modified old file instead of putting it back → late-write cases fail. A non-durable `TempWritten` is **not** caught: a process crash keeps redb's non-durable commits, so durability against power loss is argued (design §5.8), not tested.
  - **Residuals / follow-ups:** a reserved name created by someone else at one of our random names would be removed in state `Started` (only possible deliberately). `.~fsync.old.*` names from before T15 (no journal) are left alone. **T16:** the daemon's quarantine timer must use `LocalReplica::sweep_quarantine` (it forgets settled intents). **T18:** the lease checks hook into the same commit steps; tombstone GC is unaffected.

## M5: daemon

### [x] T16: Watcher and daemon
- **Depends on:** T13 (T15 recommended)
- **Read:** §5.9, §6.4
- **Files:** `src/watch/{mod,inotify,debounce}.rs`, `src/daemon.rs`, `src/replica/local.rs` (`watch()`), `src/cli.rs`, `tests/daemon.rs`
- **Do:**
  - Watcher behavior:
    - inotify via `/proc/self/fd/<dirfd>`;
    - a map wd → (dev, ino) → path;
    - watch-then-scan for new directories;
    - `MOVE_SELF`/`DELETE_SELF` handling;
    - overflow → `Hint::FullRescan`.
  - Debouncer: 200 ms quiet, 2 s max.
  - `EventSource` trait for test injection.
  - Daemon loop: wait for hints from both replicas or the periodic timer (10 min), then `sync_once` scoped to dirty paths; full scan on overflow or timer. Also run `Quarantine::sweep` on a timer.
  - Our own writes must not trigger extra sync actions (verify with an action counter).
  - Clean shutdown on SIGINT/SIGTERM.
- **Done when:** tests pass for:
  - an edit in A appears in B within 3 s;
  - a burst creation of a 3-level directory tree with files appears in full;
  - an injected overflow → full rescan and convergence;
  - a quiet tree → no actions for 5 s after convergence.
- **Notes:**
  - **API (design §5.9, §6.4 updated):**
    - `watch`: `Event { Dirty(RelPath), Overflow }`; `EventSource: Send` with `wait(timeout) -> Result<Vec<Event>>`; `ChannelSource::new() -> (Sender<Event>, ChannelSource)` for injection; `Debouncer` (pure; `QUIET` 200 ms, `MAX_DELAY` 2 s, `MAX_PATHS` 10 000 → `FullRescan`; `push(event, now)`, `deadline`, `take_due(now)`, `take`); `Watcher::spawn(name, source, debouncer)` (thread, 100 ms tick, stopped and joined on drop; a source error counts as an overflow); `InotifySource::new(Root)` (`watches()`, `LIMIT_POLL` 60 s).
    - `LocalReplica`: `watch()` starts the watcher once (later calls return the same hints; `None` with a warning if inotify fails); `set_event_source(Box<dyn EventSource>)` replaces inotify (a `&mut self` setter rather than a builder, so tests can set it on a harness replica). The watcher is owned by the replica and stops with it.
    - `fs::Root::try_clone` (a `dup`; the watcher gets its own root fd without resolving the path again).
    - `daemon::Daemon` (builders `engine`, `rescan_every`, `retry_delay`, `reports(Sender<CycleReport>)`): `run(&mut LocalReplica, &mut LocalReplica, &Receiver<()>) -> Result<DaemonStats { cycles, full_cycles, applied, conflicts, unconverged }>`. `CycleReport { full, paths, report }`. `daemon::shutdown_signals()` blocks SIGINT/SIGTERM and turns them into the stop message from a `sigwait` thread (a second signal exits at once with 128+sig).
  - **Inotify details:** watches go on `/proc/self/fd/<fd>` of a directory opened with `Root::open_dir` (beneath, no symlinks). New directories are watched breadth first, each watch added before its directory is listed; the event itself makes the whole subtree dirty, scanned by the next cycle. A directory's `MOVED_FROM`/`DELETE` removes the watches of its subtree and `MOVED_TO`/`CREATE` re-adds them, so no watch reports under a stale path; `MOVE_SELF` on a still-mapped directory dirties its parent (design), on the root it is an overflow. `IN_IGNORED` drops the mapping. Reserved names are ignored.
  - **Daemon:** watch first, then the initial full cycle. Scoped cycles scan the hinted paths of **both** sides on both replicas (cheap; keeps `Engine::sync` unchanged). Unresolved paths are retried after 1 s even without a new event. The quarantine is swept at its next deadline. Our own writes cause one empty scoped cycle each (no action; tested). A cycle that fails as a whole (`Db`, `BadIndex`, root) ends `run` with the error, no backoff. The engine now logs a cycle with nothing to do at `debug` instead of `info`.
  - **CLI:** `daemon <pair>` blocks the signals before opening the replicas (no thread exists yet), runs until SIGINT/SIGTERM, drains the quarantines, prints totals and exits 0.
  - **Tests:** unit: debouncer (3), watcher thread with injected events (1), inotify (5: existing tree, reserved names and symlinks skipped, burst `mkdir -p` fully watched, moves within/out of the tree, delete, synthetic `MOVE_SELF`/overflow), `LocalReplica::watch` (1). `tests/daemon.rs` (6, ~7 s): edit A→B (≈220 ms; exactly one action, followed by an empty echo cycle), burst of a 3-level tree (4×4×4 dirs, 84 files) plus a later edit deep inside it, injected overflow (A's watcher is a `ChannelSource`; an unseen edit and delete arrive only after the injected `Overflow`, via a full cycle), the periodic timer (1 s) with no watcher events, a quiet tree (no cycle at all for 5 s after convergence), concurrent edits → conflict copy. Each ends with `assert_converged`. `tests/cli.rs`: `daemon` syncs, then stops cleanly on SIGTERM and on SIGINT with no reserved names left (the old "daemon is a stub" test now only covers `status`). Stable over 8 repeated runs and under concurrent CPU load.
  - **Mutation check** (by hand): not watching new directories → the burst test fails (the later deep edit never arrives). This needed the test to wait out the echo cycles first, whose scans would otherwise pick up the edit anyway.
  - **Follow-ups:**
    - **T17:** no watches for followed links' referents. Changes beneath a followed directory link are indexed under the link's path (`l/x`), but in-tree events name the real path. So they are only picked up by a full rescan. Out-of-tree referents are not watched at all.
    - Every cycle reconciles the whole indexes (`changes_since(0)`), so each empty echo cycle still costs O(index). Incremental exchange by `seq` is T20.
    - The root's fd follows the directory: a root that is moved away keeps syncing at its new place (the `MOVE_SELF` only triggers a full rescan). A deleted root scans as empty after its contents were deleted (as with `sync --once`).
    - **T18** `status` could report `DaemonStats` and the last cycle time; the daemon has no state file yet.

## M6: symlinks complete

### [x] T17: Full symlink policy matrix and -K adoption
- **Depends on:** T13, T09
- **Read:** §4 (all)
- **Files:** `src/symlink/policy.rs`, `src/scan/scanner.rs`, `src/replica/local.rs`, `tests/symlink_matrix.rs`
- **Do:**
  - Implement the remaining policy behaviors:
    - `-L` write-back (incoming change replaces the link with a real object), plus the `followed_write=conflict` option;
    - `-K` adoption with a pinned dirfd, plus `--keep-dirlinks-unsafe`;
    - munge per replica;
    - extra watches for out-of-tree referents.
  - Matrix test: policy × {safe relative, unsafe relative, absolute, dangling, link to a file, link to a directory, loop, munged-looking target} × direction (A→B, B→A, both changed). Each case has the expected tree on both sides.
  - Differential test: when `rsync` is in PATH, for each one-directional case run `rsync -a <opts> A/ C/` and assert that our B equals C, ignoring our reserved names and conflict copies.
- **Done when:** the matrix passes; the rsync differential passes or is skipped with a message when rsync is absent.
- **Notes:**
  - **Design:** new §4.3.1 describes what was built. §4.2, §4.4, §4.5, §5.8, §5.9, §7 and §9 are updated too.
  - **API:**
    - `config`: `FollowedWrite { Replace (default), Conflict }` as `followed_write`, and `keep_dirlinks_unsafe` (both optional in TOML).
    - `LinkInfo::adopted`. **Index schema 2**, so a T16 index is refused with `BadIndex`.
    - `Replica::adopt(path) -> Result<bool>`, with a default of `Ok(false)` (T21 forwards it).
    - `Scanner::keep_dirlinks(on, unsafe_ok)` and `Scanner::adopt(paths)`.
    - `Root::at_fd` and `fs::root::follow_at`.
    - `commit::materialize` + `CopyNode`, `commit::set_referent_mode`, `IntentOp::Materialize`.
    - `watch::Followed`, `EventSource::follow` (default no-op), `Watcher::follow`.
    - Harness: `Opts`, `Pair::with`, `Pair::open_at_with`, `Tree::{synced, raw, opts, adopted}`, `raw_tree`.
  - **Deviations:**
    - **`-K` adoption is requested by the engine.** The scanner cannot see the peer's entry. So before each reconcile, the engine calls `Replica::adopt` once per path and cycle where one side has a directory and the other a symlink or `IgnoredLink`. The replica rescans that path with the scanner's `adopt` set. The adoption is a local change (bumped vv).
    - **`followed_write = conflict` declines changes:** the local version re-asserts itself with `merge + bump`, and incoming files and symlinks become conflict copies beside the followed link. Beneath a followed directory a copy is flat, named after the path's last component. A `RenameToConflict` there is a no-op `Applied`.
    - **Loops and rsync:** loops under `-L`/`-k` are `Unmanaged(Loop)`, while rsync recurses until the path is too long. They are excluded from the differential test.
    - **Munging sender and rsync:** with a munging sender, rsync's `--copy-unsafe-links` judges the munged target; we classify the canonical one (§4.2). That combination is excluded from the differential test.
  - **Route** (`replica/local.rs`, `Disk`): replaces T11's "refuse anything at or beneath `via_link`".
    - Reads (`open_read`, materialize) go through every followed directory. Each is opened through its link and must be the indexed link and the indexed directory. Before T17, files beneath a followed directory could not be read (sync looped until `unresolved`).
    - At a followed link itself, the commit acts on the link: `Expected` is the link's fingerprint, and the referent must be unchanged.
    - An index-only `SetMeta` never materializes or declines.
  - **Materialize:**
    - Built from the index: unmanaged entries are not copied (they become tombstones on the rescan, which is harmless), symlinks get their raw targets, and file hashes are checked.
    - After it, a scoped rescan of the link (no bump) and a retry, at most once per directory level.
    - A peer that deletes a whole followed directory makes us materialize it first, then delete it. Correct, but wasteful.
  - **Watcher:**
    - After every scan that changed something, `LocalReplica` hands the watcher all followed and adopted links (an O(index) walk).
    - Aliases are wd → link paths. In-tree referents share the tree's watch; out-of-tree ones get their own. Refreshes add the new watches first, then drop the stale ones.
  - **Tests:**
    - `tests/symlink_matrix.rs` (11):
      - The matrix: 6 policies × 8 cases × {A→B, B→A, both changed} = 144 scenarios, each against a written-out table and the expected tree on both sides.
      - rsync differential on all 96 one-directional cases except the 4 loops under `-L`/`-k`; with rsync 3.2.7 installed, all agree. Without rsync the checks are skipped with a message (checked with `PATH=/nonexistent`).
      - `munged_sender` (3 policies, plus the rsync `--munge-links` differential).
      - `copy_links_write_back`, `followed_write_conflict_keeps_links`.
      - `keep_dirlinks_adopts_and_writes_through` (plus the rsync `-K` differential), `keep_dirlinks_outside_the_root`.
    - `local.rs` (4, replacing `followed_links_are_read_but_not_written`):
      - followed file write-back (refusals included);
      - materialize, including a referent changed mid-copy → nothing committed, no leftovers;
      - conflict mode;
      - `-K` adopt, write-through, chmod, retarget, out of tree.
    - Other unit tests: inotify aliases. `tests/daemon.rs` (+1): in-tree and out-of-tree referents reach B through the watcher alone.
    - Attack suite: `Materialize` (150 cases) and `ChmodReferent` (30), with the `materialize` hook prefix in the coverage check. 1758 cases in all, all passing.
    - Crash suite: per-scenario policy, plus a `materialize` scenario under `-L`. 853 cases (was 684), all passing.
  - **Follow-ups / residuals:**
    - **Crash replay beneath a `-K` link:** an intent of a commit written through a `-K` link records its path relative to the pinned directory. Replay resolves it from the replica root, finds a different parent, and skips the intent: its reserved names are left in the referent, and logged. The crash tests don't cover this. A fix would record the link in the intent.
    - **Mixed policies:** `apply` still writes whatever symlink comes in. With mixed policies (e.g. A `Links`, B `SafeLinks`), an unsafe link written to B becomes `IgnoredLink` on B's next scan, and then stays `Skip(Unmanaged)`. That is stable, but not rsync. With the same policy on both sides, classification is symmetric.
    - **Out-of-tree directories:** a directory created inside an out-of-tree referent is watched only after the scan that its own event triggers.
    - **CLI:** no flags for the new settings yet (config file only). T18's `status` could show adopted links.

## M7: hardening

### [x] T18: Leases, tombstone GC, landlock, status
- **Depends on:** T15, T16
- **Read:** §5.3 step 4(b), (d) and (f), §3 (tombstones), §5.10
- **Files:** `src/fs/lease.rs`, `src/fs/commit.rs`, `src/index/store.rs`, `src/daemon.rs`, `src/cli.rs`
- **Do:**
  - Use `F_SETLEASE F_WRLCK` in replace and delete when `Caps.leases` is true; check `F_GETLEASE` during verify; quarantine unlinks once a lease can be taken.
  - Tombstone GC:
    - track the peer's last-seen vv per replica;
    - GC when equal and older than the retention period (configurable).
  - Optional `landlock` self-sandbox (`--sandbox`): write access to the roots and the state directory only.
  - `status <pair>` prints the index size, pending conflicts, quarantine count and last sync time.
- **Done when:**
  - lease tests pass: an open fd prevents the immediate quarantine unlink, and after close the next sweep unlinks;
  - a GC test passes;
  - a landlock test passes (a write outside the root fails with EACCES), or is skipped when unsupported.
- **Notes:**
  - **Leases** (`fs/lease.rs`): `Lease::take(parent, name, &expect)` opens the file by name (`O_RDONLY|O_NONBLOCK|O_NOFOLLOW`, checked to be the expected inode) and takes `F_WRLCK`; any failure is `None`. `held()` = `F_GETLEASE == F_WRLCK`; `hash()` rehashes through a dup of the lease fd (opening the file again would break our own lease); dropping releases it. The first `take` sets `SIGIO` to ignored unless it has a handler (a lease break sends `SIGIO`, which kills by default). `Caps::probe` now uses `Lease` too.
  - **Commit:** replace takes the lease between `replace.pinned` and `replace.before_exchange` (step (b)), delete before `delete.before_rename`, only when `ctx.caps.leases` and the old object is a regular file. A refused lease is not a failure (falls back to the grace timer). `verify_old` takes `Option<&Lease>`: rehash through it, and a broken lease = changed → undo/restore. The lease is dropped right after (d). `rename_to` takes none (its object stays user data). `StableReader` now closes its fd once the read is verified (or failed), so `SetMeta`'s copy of a file no longer holds it open across the replace (the T11 follow-up; the lease used to be refused there, falling back to the grace timer); test `stable_reader_closes_the_file_at_eof`.
  - **Quarantine:** `Pending.leases` (from the commit's caps). `verdict` stats; if unchanged and leasable, takes the lease, hits the new hook point `quarantine.leased`, re-checks size/mtime through the lease fd (changed → conflict) and returns `Unlink` with the lease, held across the unlink, before the deadline too. Otherwise the old deadline rule. `rmdir`'s settle goes through the same code. The daemon now also sweeps right after each cycle.
  - **Tests (leases):** `fs::lease` (taken only without other fds; an open breaks it and waits for the release); `fs::commit` (5): leased replace + delete unlinked at the first sweep (grace 1 h, trace has `quarantine.leased`); **an open fd keeps the inode quarantined and the next sweep after close unlinks it** (Done-when), a writer's fd still yields a conflict copy; an open during the leased replace / delete (a thread blocked in `open`, detected through `/proc/locks` "BREAKING") → undone/restored with the late write at `name`; symlinks wait for the grace period. The existing commit tests use `Fx::new()` with `leases = false` (their hooks open the old file, which would now wait 45 s for the lease break), `Fx::leased()` for the new ones; likewise one hook in `local.rs`'s `stale_precondition_fails`. `tests/attack.rs`: the `Plain` variant drops the held fd so leases are taken and `quarantine.leased` is attacked (coverage test passes, still ~9 s).
  - **Tombstone GC** (`index/store.rs`): new tables `peers` (peer ID → last sync, ns) and `acks` (peer ID BE + path → postcard `Ack { ours, theirs, since_ns }`), created on open (no schema bump: existing tables unchanged). `IndexStore::record_sync(peer, &[(path, vv, PeerState)], now_ns, retention)` (one `Durability::None` transaction, `WriteTxn::record_sync`). New `Replica::record_sync` (default no-op; `LocalReplica` uses the wall clock), called by `Engine::sync` after every cycle with the last reconcile's snapshots. `PeerState::{Absent, Tombstone(vv), Live}`; an absent peer entry counts as acknowledged (never had it, or already collected it), and any peer tombstone does, whatever its vector, because §6.1 never equalises concurrent tombstones. Retention: `PairConfig::tombstone_retention_days` (serde default 30), `Engine::tombstone_retention`, `DEFAULT_TOMBSTONE_RETENTION`. `SyncReport::collected: [usize; 2]`. Design §3 updated.
  - **Tests (GC):** store (3): collected exactly at `since + retention` (a peer that dropped its tombstone keeps the record's time), a live peer never acks; a changed peer vector restarts the period, a changed own entry is ignored, a recreated path drops its record; every known peer must ack. `tests/sync_once.rs` (2): with the default retention tombstones stay; with zero both sides collect file + dir + child (`collected == [3, 3]`), nothing comes back, a re-created file syncs as new; concurrent tombstones are collected.
  - **Landlock** (`src/sandbox.rs`, raw syscalls via `libc`, no new crate): `sandbox::abi()`, `sandbox::restrict(&[dirs])` handles every write right (+`REFER` ABI≥2, `TRUNCATE` ABI≥3) and allows them beneath the given dirs; `PR_SET_NO_NEW_PRIVS` then `landlock_restrict_self`. Fails if landlock is unavailable. CLI: global `--sandbox`, applied after loading the config and **before** the signal thread starts (roots + pair dir). Test (runs in its own thread, since landlock restricts the calling thread): writes inside work, create/overwrite/unlink/mkdir/symlink/rename-out outside fail with `EACCES`; skipped if unsupported (this WSL2 kernel has ABI 7, so it ran). `tests/cli.rs`: sandboxed `sync --once` (create, replace, delete, rmdir) and `status` work.
  - **Status** (`src/status.rs`): `PairStatus::load(cfg, pair_dir)` reads the indexes (`IndexStore::open`, not `LocalReplica::open`, which would probe and replay); a missing index counts as empty; on `DatabaseAlreadyOpen` (a daemon holds it) it reads `status.toml`, which `Daemon::status_dir(pair_dir)` saves after each cycle and sweep (`PairStatus::of`, live quarantine count). Per replica: entries, tombstones, conflict copies (`fs::is_conflict_name`, new in `tmpname.rs`), quarantined and other unfinished intents, last sync. `config::write_atomic` is now `pub(crate)`. The old `status_is_a_stub` CLI test was replaced by a real one; the daemon CLI test checks `status` while it runs ("daemon running").

### [x] T19: Concurrent stress test
- **Depends on:** T16, T18
- **Read:** §9 (stress test)
- **Files:** `tests/stress.rs` (`#[ignore]`)
- **Do:** follow §9 exactly:
  - writer threads on both trees performing every op type, including directory↔symlink swaps and outside symlinks;
  - a ledger of `(replica, path, pred_hash, new_hash)`;
  - the daemon running in-process;
  - after the writers stop, wait for convergence and check the three invariants.
  - Make the duration and thread count configurable through environment variables.
- **Done when:** `cargo test --release --test stress -- --ignored` passes at the default 30 s / 4 threads per side, and passes 10 consecutive times. Record any flake and its fix in Notes.
- **Notes:**
  - **Design:** §9 "As implemented (T19)" describes the user model, the ledger, the invariants and the reach.
  - **Test:** `tests/stress.rs`, one test `writers_race_the_daemon` (`#[ignore]`, ~37 s). Environment: `FSYNC_STRESS_SECS` (30), `FSYNC_STRESS_THREADS` (4 per tree), `FSYNC_STRESS_SLOTS` (8), `FSYNC_STRESS_PAUSE_MS` (10), `FSYNC_STRESS_SEED` (printed), `FSYNC_STRESS_SANDBOX` (1). Run with `--nocapture` to see the summary (operations done/skipped by errno, daemon totals, retried steps, per-path daemon errors, final cycles, live writes, conflict copies).
  - **Harness:** `Pair::quarantine_grace(d)` (the stress test uses the default 400 ms grace instead of the harness's zero).
  - **Deviations from §9:**
    - The ledger is `Write { path, pred, new }` plus `Removed { hashes }` and `Moved { found }`, instead of only `(replica, path, pred_hash, new_hash)`. A blind overwrite, delete or swap has no single predecessor: it removes whatever was there (a whole directory, maybe just changed by the daemon), so the writer moves it out of the tree atomically and logs what came out. A rename logs what arrived at the new name, which is where the content may then be found.
    - `/tmp/outside` is a per-run sentinel directory `outside/` beside the roots.
    - "At most 3 rounds produce zero actions" is read as: after the writers stop and the daemon goes quiet, at most 3 full `sync_once` cycles until one applies nothing. In every run so far the first one did.
    - In-place writes into an inode that has no name left right afterwards (the rmdir-settle window, §5.10) are exempt and counted (0 in every unmutated run).
  - **Results:** 10 consecutive runs at the defaults passed, no flake (35–36 s each; per run ~18 700 writes, 40 daemon cycles, ~3 150 applied steps, ~1 000 conflict copies made, ~4 200 retried steps, 1–5 blocked `EACCES` commits; the daemon went quiet 0.5–1.4 s after the writers stopped; the first final full cycle always applied nothing; 24–45 live writes checked at the end, 0 exempt). After a last fix to the test (`slot()` could pick a slot past the end with an odd slot count), passes again at the defaults, with `FSYNC_STRESS_SANDBOX=0`, and with 7 slots / 8 threads per tree (~37 000 writes).
  - **Mutation checks** (by hand, not committed): create without `RENAME_NOREPLACE` → 5–7 lost writes, 3 of 3 runs; parent resolution following symlinks → the sentinel changes without the sandbox, `EACCES` (blocked) with it. Replace without its pin check, or without its verification, is not caught: the other one (the rehash against the expected hash), or the quarantine's fingerprint check, still saves the data. Both together are reached about once per 15 s run (the engine's scan → commit gap for a path is a few ms) and were caught in 1 of 4 runs. Narrow windows like that one are the attack suite's job.
  - **Workload tuning** (no engine change was needed): the first version mostly hit absent or wrong-type parents (`ENOTDIR`, `ELOOP`) and left few live writes; in-place writes now create a missing file, a writer recreates a missing slot or `sub` 60% of the time, and slot-level files/links are rarer. A home bias per side (60%) makes replaces under a rare edit of the other side more frequent: the create mutant went from about 2 to 10 hits per run.
  - **Follow-ups:** none required. Possible: a `-L`/`-K` variant (followed links are not exercised here, policy `Links` only); timing-free coverage of the replace window would need hooks, which the attack suite has.

## M8: network → production variant

### [x] T20: Wire protocol
- **Depends on:** T13
- **Read:** §7, §7.1
- **Files:** `src/replica/proto/{mod,messages,framing}.rs`
- **Do:**
  - Request and response enums mirror each `Replica` method.
  - Length-prefixed postcard framing over any `Read + Write`.
  - Content streams as a sequence of `Chunk(Vec<u8>)` messages followed by `End{hash}`.
  - Version handshake.
- **Done when:** a round-trip test of every message over an in-memory pipe passes, and malformed frames produce errors, not panics.
- **Notes:**
  - **Design:** §7.1 "Wire protocol as implemented (T20)" describes framing, handshake, messages, batches, content streams and errors.
  - **Module** `replica::proto`: `mod.rs` (constants `MAGIC`, `PROTOCOL_VERSION`/`MIN_PROTOCOL_VERSION` = 1, `MAX_FRAME` 32 MiB, `CHUNK_SIZE` 64 KiB, `BATCH_BYTES` 1 MiB; `client_handshake`/`server_handshake` → `Session { version, peer }`), `messages.rs` (`Hello`, `HelloReply`, `Request`, `Response`, `Content`, `WireError`, `batches`), `framing.rs` (`write_frame`, `read_frame`, `send_content`, `ContentStream`).
  - **Deviations:** no `Id` request (the handshake's `Welcome` carries the server's replica ID; both sides check the other's ID). Content is a nested `Content { Chunk, End { hash }, Abort(WireError) }` inside `Request::Content`/`Response::Content`; `Abort` was added so a source failing mid-stream (the `StableReader` on the server, or the client's reader from the other replica) ends the stream in step and reaches the receiver as the same error class. `Changes`, `RecordSync` and `Collected` are batched (`more` flag).
  - **Changes outside `proto/`:** `Error::Remote { kind: RemoteKind, message }` and `Error::Protocol { reason }`, `RemoteKind`, `Error::remote_kind()`; `is_unstable`/`is_not_found` recognise remote errors; the executor's `is_fatal` is now `remote_kind() ∈ {Index, Protocol}` (same set as before for local errors, plus protocol and remote index errors). Serde derives on `Scope`, `ScanStats`, `Hint`, `Outcome`. `VersionVector` decodes through its own visitor, which caps the up-front reservation (smallvec's impl reserves the claimed length).
  - **Tests** (15, in `proto/mod.rs`): every `Request`/`Response`/`Op`/`Outcome`/`Kind`/`Content`/`Hello`/`HelloReply` variant round-trips over `std::io::pipe` (exhaustive matches on `Request`, `Response`, `Op` and `Content`, so a new variant won't compile until the test covers it) with a clean EOF after; `LocalMeta` never crosses; the frame layout; handshake (success, version choice, no common version, wrong replica either way, server picking an unknown version, bad magic either way, hang-ups); content (0 / 1 / one chunk / 3 chunks + 17 bytes, in step for the next frame), a torn source → `Abort` → `is_unstable()` on the receiver and stays failed, hash mismatch, an unexpected message, hints set aside by the filter, connection closed at and inside a frame, `drain`; 17 hand-made malformed frames (incl. `..` and NUL in paths, unsorted/zero vv, 2^60-element and 4 GiB length claims, bad bool, bad UTF-8, overflowing `Duration`) are `Protocol` errors and I/O errors stay `Io`; oversized messages are not sent; proptest: random bytes and corrupted/truncated valid messages never panic (2000 cases each); `batches`; error classes survive the round trip and are not prefixed twice when passed on.
  - Mutation-checked by hand: skipping the hash check, the trailing-bytes check, or `drain`'s loop each fails tests.
  - **Pre-existing flake (not fixed, not caused by T20):** `cargo test --features hooks --test crash` fails about 1 run in 6–10, on this branch and on a clean T19 checkout alike (1 of 10 there). Always scenario 14 ("materialize"), at varying hook points (`stage.synced`, `commit.synced`, `scan.stat`): the harness's final rescan of A reports `d/e/l` dirty (`dirty: [RelPath("d/e/l")]`, no changes). Looks timing-dependent; worth a look by whoever touches T15/T17 code next. → **T23**.
  - **For T21:** the server must answer requests in order and drain an `Apply`'s content before answering when `apply` did not consume it. A client that drops an `open_read` reader before EOF must `drain` it (or close the connection); there is no cancel message. The client side of hints needs a reader that demultiplexes `Response::Hint` from answers (`ContentStream`'s filter does this inside a stream). Map `Request::RecordSync` batches back to one `record_sync` call (same `peer` and `retention`).

### [x] T21: RemoteReplica, server, TLS
- **Depends on:** T20
- **Read:** §7.1
- **Files:** `src/replica/remote.rs`, `src/server.rs`, `src/cli.rs`, `src/config.rs`
- **Do:**
  - `serve --listen addr <pair-side>` wraps a `LocalReplica`.
  - `RemoteReplica` implements `Replica` over the protocol.
  - Security:
    - rustls with mutual TLS using self-signed certificates generated at `init`;
    - device ID = blake3 of the certificate;
    - the peer's ID is pinned in the config.
  - Watch hints are forwarded as server-push messages.
  - Incremental `changes_since(seq)` exchange.
- **Done when:** `sync --once` and `daemon` work between two processes over 127.0.0.1, and a wrong peer certificate is rejected.
- **Notes:**
  - **Design:** §7.1 "Network as implemented (T21)": identities and pinning, deployment, server, hint connections, client connections, errors, incremental index exchange, daemon/status. §2 layout and crates updated.
  - **New crates:** `rustls` 0.23.45 (`ring` + `std` only; TLS 1.3 only) and `rcgen` 0.14.10 (`crypto` + `ring`); `rustix` gained the `net` feature (keepalive, `TCP_NODELAY`).
  - **Modules:**
    - `src/tls.rs`: `DeviceId` (blake3 of the cert DER, 64 hex digits, serde as a string), `Identity` (`generate`, `save`/`load` as `<pair>/<replica>.crt`/`.key`, `load` checks the pin, `server_config`/`client_config`), the `Pinned` verifier for both directions.
    - `src/server.rs`: `Server::{new, open(cfg, side, pair_dir), spawn(listener)}` → `ServerHandle::{local_addr, replica, sweep, close_connections, shutdown, into_replica}`.
    - `src/replica/remote.rs`: `RemoteReplica::{connect(remote_cfg, local_cfg, pair_dir), with_tls(addr, local, remote, tls), addr, entries_received, connections}`.
    - `src/replica/mod.rs`: `Housekeeping` trait (`next_sweep`, `sweep`, `status`) for `LocalReplica`, `RemoteReplica`, `PairReplica`; `PairReplica::open(cfg, side, pair_dir)` (local or remote by config).
  - **Config:** `ReplicaConfig.device: Option<DeviceId>` and `.remote: Option<String>` (both omitted from the TOML when unset, so older configs load). `init` now writes both identities (config first, so an existing pair is never touched). `config::init_with(state_home, pair, [NewReplica; 2])` for remote replicas; a remote root must be absolute and is not checked locally; the overlap check only applies when both replicas are local; `ReplicaConfig::device()` errors with `Error::Tls` for a pre-T21 pair.
  - **CLI:** `init --a-remote/--b-remote HOST:PORT` (prints device IDs and what to copy to the server host); `serve <pair> <a|b> [--listen ADDR]` (default address: the replica's `remote`; prints `serving replica … on ADDR`; sweeps the quarantine; SIGINT/SIGTERM stop it cleanly); `sync --once`/`daemon` open `PairReplica`s; `status` shows a remote replica as "served at ADDR"; `--sandbox` covers `serve` (the served root is allowed even though the shared config marks it remote — a bug found by hand and covered by `sandboxed_serve_and_sync`).
  - **Errors:** new `Error::Connection { peer, reason }` (any transport failure; `RemoteKind::Protocol`, so fatal for the cycle; `is_disconnected()`) and `Error::Tls { reason }`. `Daemon` takes `&mut dyn Housekeeping` and retries a disconnected cycle after `RECONNECT_DELAY` (5 s) with a full cycle; existing callers with `LocalReplica`s compile unchanged.
  - **Deviations:**
    - Hints use a separate connection: `Watch` turns its connection into a hint stream (blocking rustls cannot read and write one TLS connection from two threads, and a client idle in the daemon would not read pushed hints on its request connection). Request connections are strictly request/response. Compatible with the T20 messages.
    - The task text's `serve --listen addr <pair-side>` is `serve <pair> <a|b> [--listen addr]`.
    - Each replica (not each host) has a certificate: a client presents the identity of the replica it acts for, so the server pins exactly one device (the peer's). Moving a replica to another host means copying its `.crt`/`.key` there.
  - **Tests:**
    - `tests/remote.rs` (6, in-process over 127.0.0.1): sync both ways (dirs, symlink, non-UTF-8 name, empty and 300 KB files, edits, delete, conflict) with a no-op cycle transferring no entries and the delta bounded, all over one connection; remote index order and `changes_since(seq)`, reads (small reader dropped early → drained, connection kept; large → closed; a request during a read uses a second connection; error classes `Unstable`/`Remote` survive); wrong client certificate, wrong pinned server device, wrong replica ID rejected; hints pushed, then `FullRescan` after the connections were cut; server restart at the same address (cycle fails with `Connection`, the next reconnects, mirror starts over); the daemon with a remote B (hints alone carry B's change; a cut connection is retried).
    - `tests/net.rs` (3, two processes, separate state homes as two hosts): `serve` + `sync --once` + `status` + `daemon` (both directions, SIGTERM) + server certificate not the pinned one (`is not the pinned`) + client certificate not pinned (fails; the server logs `rejected: TLS handshake failed`) + `serve` stops cleanly on SIGTERM; `serve` without an address or identity, sync with nothing listening; `--sandbox` on both sides (skipped without landlock).
    - Unit: `tls` (device ID text, identity save/load/pin/permissions), `config::init_with_a_remote_replica`; `init_round_trips` now expects the identity files.
    - Mutation-checked by hand: a verifier that accepts any certificate fails both `wrong_certificates_are_rejected` and the two-process test. 15 consecutive runs of `remote` + `net`: no flake.
  - **Follow-ups (T22 / later):** the harness can serve a replica in-process with `Server::new(..).spawn(TcpListener::bind("127.0.0.1:0"))` and `ServerHandle::into_replica`; the crash/attack hooks are not reachable through a remote replica's process boundary unless the server runs in-process. No retry of a request on a connection that dies mid-request (the cycle fails and is retried as a whole). Block-level delta transfer (§7.1) → **T24**. The pre-existing `crash` flake noted under T20 was not looked at → **T23**.

### [ ] T22: Network test parity
- **Depends on:** T21, T14, T17, T19
- **Read:** §9
- **Files:** `tests/harness/mod.rs`, the existing test files
- **Do:** make the harness generic over a `ReplicaFactory` (local or loopback-remote), then run the sync_once, model, symlink_matrix and stress suites in both modes.
- **Done when:** every suite passes in both modes.
- **Notes:**

## M9: follow-ups

### [ ] T23: Fix the `crash` suite flake
- **Depends on:** T15, T17
- **Read:** §4.3.1, §4.5, §5.8, §9 (crash suite)
- **Files:** `tests/crash.rs`, plus whatever the root cause turns out to be (likely `src/replica/local.rs`, `src/scan/scanner.rs` or `src/fs/commit.rs`)
- **Do:**
  - Reproduce the flake recorded in T20's Notes: `cargo test --features hooks --test crash` fails about 1 run in 6–10, always in scenario 14 ("materialize", under `-L`), at varying hook points (`stage.synced`, `commit.synced`, `scan.stat`). The harness's final rescan of A reports `d/e/l` dirty (`dirty: [RelPath("d/e/l")]`) with no changes. Run the suite in a loop, with `--release` if that makes it more frequent, until it fails, and capture the logs (`RUST_LOG=files_sync=debug`).
  - Find the root cause. Is it a real defect (a recovery or rescan that leaves the index unstable after a crash during materialize), or a test that is wrong (e.g. a timestamp/racy-window assumption in the harness)?
  - Fix the cause. Do not paper over it with retries, sleeps or a looser assertion, unless the analysis shows the "dirty" report is correct behaviour; then explain why in Notes and assert the correct behaviour instead.
- **Done when:**
  - 50 consecutive runs of `cargo test --features hooks --test crash` pass;
  - the root cause is written up in Notes;
  - if it was a product bug, a deterministic regression test (a hook point, not timing) fails without the fix and passes with it;
  - `cargo test --features hooks` and clippy pass.
- **Notes:**

### [ ] T24: Block-level delta transfer
- **Depends on:** T22
- **Read:** §3, §5.2, §5.3, §7, §7.1
- **Files:** `src/replica/proto/messages.rs`, `src/replica/proto/mod.rs`, `src/replica/{mod,local,remote}.rs`, `src/server.rs`, `src/engine/executor.rs`, `src/scan/hasher.rs`, and the index if block lists are stored
- **Do:**
  - Extend design §7.1 first with the concrete scheme, then implement it. The starting point is the sketch: fixed, aligned 128 KiB blake3 blocks, as in syncthing (no rolling hash). The source sends the new file's block list. The destination reuses the blocks it already holds in its current file at that path and receives only the missing ones.
  - **Race-freedom stays intact.**
    - The destination assembles a new temp file from stable reads of its own indexed file plus the received blocks. It never writes in place.
    - The temp file is committed by the usual CAS replace against the expected fingerprint.
    - The whole-file hash is checked against `Op::WriteFile`'s hash before commit.
    - If the old file changes during assembly, the result is `Unstable` with nothing committed, and the path is rescanned.
    - All checks run inside the destination replica. The engine still does no I/O, and only orchestrates through `Replica` methods. Extend the trait as needed (e.g. a block list from the source, reads of selected blocks, and an `apply` content that is either a full stream or a delta), with a default that falls back to the full stream.
  - **Protocol:** bump `PROTOCOL_VERSION` to 2 for the new messages. A v1 peer must still work: both sides negotiate down and use full transfers.
  - Use delta only when it can pay off: the destination holds a live file at the path, and the size is above a threshold. Local pairs keep streaming whole files (same host, no network to save).
  - Block hashes may be computed on demand, or stored with the entry (computed by the hasher in the same pass as the file hash). Decide, and note the trade-off in the design.
- **Done when:**
  - changing 1 byte in a 64 MiB file and syncing over the loopback-remote mode transfers at most a few blocks plus protocol overhead. Assert this with a byte counter on the connection.
  - a destination file modified mid-assembly (hook point) commits nothing and leaves no temp files.
  - a forced v1 session still syncs correctly with full transfers.
  - the T22 suites pass in both modes, and so do the attack suite and clippy.
- **Notes:**
