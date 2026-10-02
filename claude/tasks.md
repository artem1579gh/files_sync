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

### [ ] T03: `Root`, `RelPath`, fingerprints
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

### [ ] T04: Hooks, temp files, create-type commits
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

### [ ] T05: CAS replace via RENAME_EXCHANGE
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

### [ ] T06: CAS delete and rmdir
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

### [ ] T07: Race-injection attack suite
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

## M2: index, symlinks, scanner

### [ ] T08: Index entries, version vectors, redb store
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

### [ ] T09: Symlink safety, munging, policy
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

### [ ] T10: Scanner
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

## M3: engine → simplified variant complete

### [ ] T11: Replica trait and LocalReplica
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

### [ ] T12: Reconciler and planner (pure)
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

### [ ] T13: Executor, conflicts, `sync --once`, test harness
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

### [ ] T14: Model-based property test
- **Depends on:** T13
- **Read:** §9
- **Files:** `tests/model.rs`
- **Do:**
  - proptest generates sequences of ops (write, append, delete, mkdir, rmdir, symlink, rename) on A or B, interleaved with `sync()`.
  - A simple in-memory reference model predicts the outcome: last writer wins per causal history, and concurrent edits produce a conflict copy.
  - After a final sync, compare the model with the real trees: equal paths, and the content sets including conflict copies.
- **Done when:** 256 cases pass. Every shrunk failure found along the way gets fixed and added as a regression scenario in `tests/sync_once.rs`.
- **Notes:**

## M4: durability

### [ ] T15: Journal and crash recovery
- **Depends on:** T13
- **Read:** §5.3 step 1, §5.3 step 5, §5.8
- **Files:** `src/index/journal.rs`, `src/fs/commit.rs`, `src/replica/local.rs`, `tests/crash.rs`
- **Do:**
  - Add an `Intent` table to redb.
  - `commit` functions take a `&Journal`: write the intent before the temp file and update its state at each step (`TempWritten`, `Exchanged`, `Done`). Mark it Done in the same transaction as the index update.
  - `LocalReplica::open` replays unfinished intents as described in §5.8.
- **Done when:** `tests/crash.rs` passes. It forks a child that runs one apply with a hook calling `std::process::abort()` at point N. The parent reopens the replica, checks that no user data was lost and no unexplained `.~fsync.*` files remain, and checks that a following `sync_once` converges. The test sweeps N over every hook point and op type.
- **Notes:**

## M5: daemon

### [ ] T16: Watcher and daemon
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

## M6: symlinks complete

### [ ] T17: Full symlink policy matrix and -K adoption
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

## M7: hardening

### [ ] T18: Leases, tombstone GC, landlock, status
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

### [ ] T19: Concurrent stress test
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

## M8: network → production variant

### [ ] T20: Wire protocol
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

### [ ] T21: RemoteReplica, server, TLS
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

### [ ] T22: Network test parity
- **Depends on:** T21, T14, T17, T19
- **Read:** §9
- **Files:** `tests/harness/mod.rs`, the existing test files
- **Do:** make the harness generic over a `ReplicaFactory` (local or loopback-remote), then run the sync_once, model, symlink_matrix and stress suites in both modes.
- **Done when:** every suite passes in both modes.
- **Notes:**
