# CLAUDE.md

`files_sync` is a race-free, two-way file synchronizer for Linux, written in Rust: "syncthing-style sync with rsync symlink semantics". It syncs two local directories, or two directories over the network (`serve`, mutual TLS, block-level deltas). Milestones M0–M11 are implemented, and every task in `claude/tasks.md` is done.

- **Design:** [`claude/design.md`](claude/design.md). It is the source of truth; keep it updated when you deviate from it.
- **Task list:** [`claude/tasks.md`](claude/tasks.md). Its tasks are designed to be done one per session, without prior context.
- **User guide:** [`docs/usage.md`](docs/usage.md). It describes the CLI, the config file and the user-visible behaviour. Keep it in sync when you change any of these.
- **Release notes:** [`release_notes.md`](release_notes.md). One note per released version (see "Versions and releases").

## How to work in a session

1. Open `claude/tasks.md` and take the first unchecked task whose dependencies are checked, unless the user names another task.
2. Read only the `design.md` sections that task lists, then the code it touches.
3. Implement the task, make its "Done when" criteria pass, tick the box and fill in Notes, then commit as `T<NN>: <title>`.
4. If the task changed the CLI, the config or the output, update `docs/usage.md` and `README.md` too.

## Commands

```sh
cargo build
cargo test                                   # unit + integration
cargo test --features hooks                  # includes race-injection suites (tests/attack.rs, crash.rs)
cargo clippy --all-targets -- -D warnings
cargo test --release --test stress -- --ignored   # long concurrent stress test
cargo build --release                             # binary for manual checks
```

To try the binary by hand, follow `docs/usage.md`. Its ```` ```sh ```` blocks form one runnable demo; ```` ```bash ```` blocks are illustrative and not run. Run it under `/tmp`, with `XDG_STATE_HOME` pointing there too, so `~/.local/state` is not touched.

## Versions and releases

- **Format:** `MAJOR.MIDDLE.MINOR`, as in `version` in `Cargo.toml`. `0.1.1` was the initial release.
- **MINOR:** goes up for bug fixes and optimisations.
- **MIDDLE:** goes up for new features. MINOR starts again at 1.
- **MAJOR:** only for special occasions. Never change it without discussing it with the user. Major `0` is a special case (the current pre-1.0 series).
- **Numbering:** MIDDLE and MINOR start at 1, never 0. A new middle version is `X.Y.1`.
- **Releasing:** only when the user asks.
  - Set `version` in `Cargo.toml`, and run `cargo build` so `Cargo.lock` follows.
  - Add a note at the **top** of `release_notes.md`: a `## <version> — YYYY.MM.DD HH:MM:SS` heading with the date and time the note is added, **in UTC** (`date -u '+%Y.%m.%d %H:%M:%S'`), then briefly what the release does.
  - Keep one empty line between notes. Never rewrite older notes.

## Hard rules (these protect race-freedom; do not break them)

- **Path handling:**
  - All filesystem access to a replica goes through `fs::Root`.
  - Never pass a multi-component path string to the kernel. Resolve the parent with `openat2(RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_XDEV)`, then use `*at(parentfd, single_name)`.
  - No `std::fs` calls on paths inside a replica.
  - Never follow symlinks during traversal unless the symlink policy explicitly says to (design §4).
- **Mutation:**
  - Only `src/fs/commit.rs` may modify a replica.
  - Every mutation is a compare-and-swap against an expected fingerprint (temp file + `renameat2` with `RENAME_NOREPLACE`/`RENAME_EXCHANGE` + verify; design §5.3).
  - Never write in place.
  - When in doubt, **preserve user data as a conflict copy**. Never unlink something you haven't verified.
- **Data handling:**
  - Paths and symlink targets are raw bytes (`[u8]`/`OsStr`), never `String`/`str`.
  - The `.~fsync.` name prefix is reserved for temp, quarantine and probe files. The scanner and watcher must ignore it.
  - The engine (`src/engine/`) does no I/O itself; it talks only to the `Replica` trait. Precondition (CAS) checks run inside the replica, so the network version stays race-free.
- **Code and tests:**
  - No tokio: use std threads plus `crossbeam-channel`.
  - Race tests use `fs::hooks::point("…")` injection points (enabled under `cfg(test)` or the `hooks` feature). Add a hook at every new step boundary in commit code.

## Environment notes

- Linux only (kernel ≥ 5.6 for `openat2`). Development happens on WSL2.
- Test and replica directories must be on ext4 or tmpfs (`tempfile` in `/tmp` is fine), **never under `/mnt/c`**. drvfs lacks `O_TMPFILE`, `RENAME_EXCHANGE` and leases, and has broken inotify.
- `rsync` is used for differential tests if it is installed (optional).

## Conventions

- Library and binary in one crate: `src/lib.rs` + `src/main.rs`. Errors use `thiserror` in the library and `anyhow` in the binary. Logging uses `tracing`.
- Conflict copy naming: `stem.sync-conflict-YYYYMMDD-HHMMSS-<ID7>.ext`.
- State (index, journal, config) lives in `$XDG_STATE_HOME/fsync/<pair>/`, never inside a replica root.
