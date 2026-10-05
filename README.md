# files_sync

A two-way file synchronizer for Linux that **stays correct while files are being modified**, and that **handles symlinks the way rsync does**.

> Status: **feature-complete for its planned scope.** It covers local and network sync, a one-shot and a daemon mode, every rsync symlink mode, crash recovery and block-level delta transfer. **User guide: [`docs/usage.md`](docs/usage.md).** The design is in [`claude/design.md`](claude/design.md), and progress is tracked in [`claude/tasks.md`](claude/tasks.md).

## Why

| Tool | Two-way | Symlinks | Safe against concurrent changes |
|---|---|---|---|
| rsync | no (one-way) | yes, all modes | no (TOCTOU races, symlink-swap CVEs) |
| unison | yes | limited | partially |
| syncthing | yes | not really | mostly |
| **files_sync** | yes | yes, every rsync mode | yes, by design |

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
- **Root marker:** each root holds a `.~fsync.root.<replica-id>` file, as syncthing's `.stfolder`. A root without it, such as the empty mount point of a disk that is not mounted, is refused, not synced as "everything deleted".
- **Trash (optional):** with `trash_days` set, files a sync replaces or deletes are kept in `.~fsync.trash` for that many days.
- **Lost or restored index:** a replica whose index was deleted or restored from a backup keeps its unsynced changes. They are pushed to the peer or kept as conflict copies, never overwritten by the peer's older versions.
- **Mass-deletion guard:** a sync that would delete more than half of a replica (configurable) holds those deletions back until you confirm them with `sync --once --allow-mass-delete`.
- **Network:** mutual TLS 1.3 with pinned self-signed certificates, as in syncthing. A changed file of 1 MiB or more is sent as a block-level delta: only its changed 128 KiB blocks cross the network.
- **Sandbox (optional):** with `--sandbox`, Landlock confines the process's writes to the replica roots and its state directory.

## Symlink modes

Each replica can be configured with an rsync-equivalent symlink policy (`symlinks = …` in the pair's `config.toml`; see [the guide](docs/usage.md#6-symlinks)):

| Setting | Behaviour |
|---|---|
| `skip` (rsync default) | symlinks are ignored and never touched |
| `links` (`-l`) | symlinks are synced as symlinks, target bytes verbatim |
| `copy-links` (`-L`) | symlinks are followed; the referent is synced as a real file or directory |
| `copy-unsafe-links` | only links that point outside the tree are followed |
| `safe-links` | links that point outside the tree are ignored |
| `copy-dirlinks` (`-k`) | only symlinks to directories are followed |
| `keep_dirlinks = true` (`-K`) | a local symlink-to-directory is kept and treated as the directory |
| `munge_links = true` | targets are stored on disk prefixed with `/rsyncd-munged/` |

## Requirements

- Linux with kernel ≥ 5.6 (`openat2`).
- Replica directories on a local filesystem such as ext4, xfs, btrfs or tmpfs. Network filesystems and WSL `/mnt/c` are not supported.
- Rust (edition 2024) to build.

## Usage

```sh
cargo build --release              # binary: target/release/files_sync

# create a sync pair (state and config go to $XDG_STATE_HOME/fsync/docs/)
files_sync init docs --a ~/docs --b /data/docs-mirror

# one-shot sync (like unison)
files_sync sync --once docs

# continuous sync, driven by inotify (like syncthing)
files_sync daemon docs

# what is going on: index size, conflict copies, last sync
files_sync status docs

# across the network: B lives on another host and is served there
files_sync init paper --a ~/paper --b /srv/paper --b-remote server:7777
#   copy config.toml and B's .crt/.key to the server, then on the server:
files_sync serve paper b
files_sync status paper            # there: the served replica's state
#   and on this host, as usual:
files_sync daemon paper
```

See **[`docs/usage.md`](docs/usage.md)** for a walk-through, with examples you can run in `/tmp`. It covers conflicts, symlink policies, network setup, the config file reference and troubleshooting.

## Development

```sh
cargo test                                        # unit + integration
cargo test --features hooks                       # adds the race-injection and crash suites
cargo clippy --all-targets -- -D warnings
cargo test --release --test stress -- --ignored   # long concurrent stress test
```

Development is organised as a sequence of self-contained tasks in [`claude/tasks.md`](claude/tasks.md).
