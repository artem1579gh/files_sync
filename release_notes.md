# Release notes

All times are UTC.

## 0.1.2 — 2026.10.05 10:45:26

Fixes [issue #1](https://github.com/artem1579gh/files_sync/issues/1): an empty or unmounted replica root deleted every file on the other replica.

- **Root marker:** each replica root holds a `.~fsync.root.<replica-id>` file. A root without it is refused instead of being synced as "everything deleted". This covers, for example, the empty mount point of a disk that is not mounted. Existing pairs get their markers on their next sync.
- **Mass-deletion guard:** a sync that would delete more than `max_delete_percent` (default 50%) of a replica's entries, and more than 10 of them, holds those deletions back. `sync --once --allow-mass-delete` applies them; a daemon warns and keeps running.
- **Trash (optional):** with `trash_days` set on a replica, files its syncs replace or delete are kept in `.~fsync.trash/` for that many days instead of being removed.

## 0.1.1 — 2026.10.02 22:15:00

Initial release.

- Two-way sync between two local directories (`sync --once`, or the inotify-driven `daemon`), or over the network (`serve`, mutual TLS with pinned certificates, block-level deltas).
- Race-free by design: every change is a compare-and-swap commit, and nothing is written in place. Concurrent edits never cause silent data loss, torn files or writes outside a root.
- Version vectors per file. Real conflicts keep both versions, the loser as a `sync-conflict` copy.
- Every rsync symlink mode (`links`, `skip`, `copy-links`, `copy-unsafe-links`, `safe-links`, `copy-dirlinks`, `--munge-links`, `-K`).
- Crash recovery through an intent journal, tombstone garbage collection, an optional Landlock sandbox and `status`.
