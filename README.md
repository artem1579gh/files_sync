# files_sync

A two-way file synchronizer for Linux that **stays correct while files are being modified**, and that **handles symlinks the way rsync does**.

> Status: **in design and early development.** Nothing below works yet. See [`claude/tasks.md`](claude/tasks.md) for progress and [`claude/design.md`](claude/design.md) for the full design.

## Why

| Tool | Two-way | Symlinks | Safe against concurrent changes |
|---|---|---|---|
| rsync | no (one-way) | yes, all modes | no (TOCTOU races, symlink-swap CVEs) |
| unison | yes | limited | partially |
| syncthing | yes | not really | mostly |
| **files_sync** (goal) | yes | yes, every rsync mode | yes, by design |

## How it stays race-free (short version)

- **Path resolution:** every filesystem operation resolves the parent directory with `openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)` and acts on a single name relative to that directory fd. A directory swapped for a symlink mid-sync cannot redirect a write outside the tree.
- **Reads:** a file is hashed while it is read, and its metadata is checked before and after. Torn reads are retried, never propagated.
- **Writes:** nothing is written in place.
  1. New content goes to a temp file and is fsynced.
  2. The temp file is committed with `renameat2(RENAME_NOREPLACE)` or `renameat2(RENAME_EXCHANGE)`.
  3. The file that was swapped out is checked against what we expected. If someone changed it meanwhile, it is put back or kept as a conflict copy.
- **Conflicts:** both versions are kept. The losing one is renamed to `name.sync-conflict-YYYYMMDD-HHMMSS-<id>.ext`.
- **Causality:** version vectors per file (like syncthing) tell real conflicts from ordinary updates.
- **Crash safety:** an intent journal recovers half-finished operations.

## Symlink modes

Each replica can be configured with an rsync-equivalent symlink policy:

| Mode | Behaviour |
|---|---|
| `skip` (rsync default) | symlinks are ignored and never touched |
| `links` (`-l`) | symlinks are synced as symlinks, target bytes verbatim |
| `copy-links` (`-L`) | symlinks are followed; the referent is synced as a real file or directory |
| `copy-unsafe-links` | only links that point outside the tree are followed |
| `safe-links` | links that point outside the tree are ignored |
| `copy-dirlinks` (`-k`) | only symlinks to directories are followed |
| `keep-dirlinks` (`-K`) | a local symlink-to-directory is kept and treated as the directory |
| `munge-links` | targets are stored on disk prefixed with `/rsyncd-munged/` |

## Requirements

- Linux with kernel ≥ 5.6 (`openat2`).
- Replica directories on a local filesystem such as ext4, xfs, btrfs or tmpfs. Network filesystems and WSL `/mnt/c` are not supported.
- Rust (edition 2024) to build.

## Usage (planned)

```sh
cargo build --release

# create a sync pair
files_sync init docs --a ~/docs --b /data/docs-mirror

# one-shot sync (like unison)
files_sync sync --once docs

# continuous sync (like syncthing)
files_sync daemon docs

# later: across the network
files_sync serve --listen 0.0.0.0:7777 docs
files_sync daemon docs --remote host:7777
```

## Development

```sh
cargo test
cargo test --features hooks        # race-injection tests
cargo test --release --test stress -- --ignored
```

Development is organised as a sequence of self-contained tasks in [`claude/tasks.md`](claude/tasks.md).
