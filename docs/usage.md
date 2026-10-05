# files_sync user guide

`files_sync` keeps two directories identical in both directions: edit either one, and the change shows up in the other. It is built to stay correct while other programs are changing files during a sync, and it handles symlinks the way `rsync` does.

This guide covers:

1. [Install](#1-install)
2. [Concepts](#2-concepts)
3. [Quick start](#3-quick-start): a pair, a first sync, edits on both sides, deletions
4. [Conflicts](#4-conflicts)
5. [Continuous sync with the daemon](#5-continuous-sync-with-the-daemon)
6. [Symlinks](#6-symlinks)
7. [Syncing over the network](#7-syncing-over-the-network)
8. [Sandboxing and logging](#8-sandboxing-and-logging)
9. [Command reference](#9-command-reference)
10. [Config file reference](#10-config-file-reference)
11. [What is synced, and what is not](#11-what-is-synced-and-what-is-not)
12. [Troubleshooting](#12-troubleshooting)

Every example in sections 3–8 is part of one runnable demo. It works in a throwaway directory and keeps its state there too, so it does not touch your home directory. You can paste the blocks into a shell in order.

## 1. Install

You need:
- Linux with kernel 5.6 or newer.
- A Rust toolchain that supports edition 2024 (Rust 1.85 or newer).

```bash
git clone <this repository> files_sync
cd files_sync
cargo build --release
# The binary is target/release/files_sync. Put it on your PATH, e.g.:
cargo install --path .
```

Both replica directories must be on a local filesystem: ext4, xfs, btrfs or tmpfs. Network filesystems and WSL's `/mnt/c` (drvfs) are not supported, because they lack the kernel features the safe write path needs (`O_TMPFILE`, `RENAME_EXCHANGE`, leases, reliable inotify). `files_sync` checks this when it opens a replica and refuses to run if a required feature is missing.

## 2. Concepts

- **Pair:** two directories that are kept in sync. A pair has a name, such as `docs`.
- **Replica:** one of the two directories, called **A** and **B**. They are equal peers: neither is the "source". Each has a random 16-hex-digit **replica ID**.
- **State directory:** `$XDG_STATE_HOME/fsync/<pair>/`, or `~/.local/state/fsync/<pair>/` if `XDG_STATE_HOME` is unset. It holds:
  - `config.toml`, the pair's settings ([§10](#10-config-file-reference));
  - `<replica-id>.redb`, the index (what was last seen and synced) and the crash-recovery journal of each local replica;
  - `<replica-id>.crt` / `.key`, a TLS identity per replica (used only for network sync);
  - `status.toml`, written by a running daemon, and `status-<replica-id>.toml`, written by a running `serve`.

  The state directory never lives inside a replica.
- **Version vectors:** per-file causal history, as in syncthing. They tell an ordinary update ("B has seen A's last change and edited it again") from a real conflict ("both sides changed the file independently").
- **Reserved names:** names starting with `.~fsync.` are temp, quarantine and probe files. You may briefly see them during a sync. They are never synced, and they are cleaned up, also after a crash. The root marker and the trash below are the ones that stay.
- **Root marker:** the first sync puts a small file `.~fsync.root.<replica-id>` at the top of each local root, saying which replica the directory is. Like syncthing's `.stfolder`, it guards against an empty directory standing in for the replica. That happens, for example, when the disk behind a root is not mounted and its empty mount point is left. Without the marker, every file in the index would look deleted, and the deletions would be synced to the other side. If the marker is missing, `sync`, `daemon` and `serve` refuse to run for the pair (a daemon whose root loses its marker stops): ``replica root …: its root marker is missing; it may not be the replica's directory (is its disk mounted?)``. To fix it:
  - if the disk is not mounted, or the path is wrong, mount it or fix the path, and run again;
  - if the directory really is the replica (say, you restored it from a backup that skipped the marker), create the marker yourself, e.g. `touch '/data/backup/.~fsync.root.b8585899415ed538'`. Its content does not matter. The next sync then treats whatever is missing from the directory as deleted, and deletes it on the other side too;
  - to fill an empty directory from the other side instead, delete this replica's index `<replica-id>.redb` from the state directory (only while nothing runs for the pair). The replica then starts over: the next sync copies the other side's files into it and deletes nothing.

  A pair made before root markers gets its markers on its next sync, unless a root is empty while its index lists files: that is refused in the same way.
  Back up the marker with the rest of the directory, and keep it there. Since there is one marker per replica ID, a directory can be a replica of several pairs.
- **Trash (optional):** by default, a file that a sync replaces or deletes is gone shortly afterwards. It is unlinked once nothing writes to it any more, or after a short grace period. If someone writes to it in the meantime, it is kept as a conflict copy instead. Set `trash_days` on a replica ([§10](#10-config-file-reference)) to keep such files in its trash, `.~fsync.trash` at the top of its root, for that many days. They keep their directory path there, with the time they were moved in added to the name, e.g. `.~fsync.trash/reports/q3~20261005-091139.pdf`. A sync never looks inside the trash. Files are removed once older than `trash_days`, checked about once an hour while a `sync`, `daemon` or `serve` runs. Restore a file by copying or moving it back. The trash is on the replica's own disk, so it takes space there. A file beneath a `keep_dirlinks` link to another filesystem cannot be moved there, and is removed as without a trash.
- **Mass-deletion guard:** a second safety net, like unison's `confirmbigdeletes`. If one sync would delete more than half of a replica's entries (`max_delete_percent` in [§10](#10-config-file-reference)), and more than 10 of them, none of those deletions is applied there. Everything else is still synced. The deleting side keeps its deletions, so nothing comes back either:

  ```text
  synced "docs": 0 change(s) applied in 0 round(s)
    held back 41 deletion(s) in /data/backup: more than 50% of its 42 entries
      photos
      photos/img1.jpg
      photos/img10.jpg
      photos/img11.jpg
      photos/img12.jpg
      … and 36 more
      if these deletions are intended, apply them with: files_sync sync --once --allow-mass-delete docs
  Error: 41 path(s) not synced; see above
  ```

  `sync --once` then exits with status 1, and every later sync holds them back again. If you meant to delete them, run `files_sync sync --once --allow-mass-delete docs`. If not, restore the files on the side that lost them, from the other side for example. A `daemon` holds them back too, logs a warning, and keeps running; stop it to run `sync --once --allow-mass-delete`, then start it again. Deleting at most 10 entries, or at most half of them, is never held back.

## 3. Quick start

Set up a scratch area. `XDG_STATE_HOME` points into it, so this demo keeps its pair configs out of `~/.local/state`. `RUST_LOG=warn` hides the informational log lines; see [§8](#8-sandboxing-and-logging).

```sh
export DEMO=/tmp/fsync-demo
rm -rf "$DEMO" && mkdir -p "$DEMO" && cd "$DEMO"
export XDG_STATE_HOME="$DEMO/state"
export RUST_LOG=warn
```

Create two directories, with some files in the first one:

```sh
mkdir laptop backup
echo "hello" > laptop/notes.txt
mkdir laptop/photos
printf 'jpegdata' > laptop/photos/cat.jpg
```

Create the pair. Roots must be existing directories, and must not be nested in each other:

```sh
files_sync init docs --a "$DEMO/laptop" --b "$DEMO/backup"
```

```text
initialised pair "docs": /tmp/fsync-demo/state/fsync/docs/config.toml
  a: /tmp/fsync-demo/laptop (replica f563ef689fcf91de)
     device cdfacff3cbc46bbb14e403cc328caae6c0551ae189a13a70ddd8cbd1c24c90d3
  b: /tmp/fsync-demo/backup (replica b8585899415ed538)
     device 53e86ac3ee77db37c8a3ed6cd1240148e5388cf974fdbc7e5e6315843857131a
```

(Your IDs will differ.) Run a one-shot sync:

```sh
files_sync sync --once docs
find backup -not -name '.~fsync.*' | sort
```

```text
synced "docs": 3 change(s) applied in 1 round(s)
backup
backup/notes.txt
backup/photos
backup/photos/cat.jpg
```

`find` leaves out the root marker `.~fsync.root.<replica-id>` that the sync put in each root (see [§2](#2-concepts)).

Now change things on **both** sides: append to a file in `backup`, and delete a file in `laptop`. One sync carries each change to the other side.

```sh
echo "edited on backup" >> backup/notes.txt
rm laptop/photos/cat.jpg
files_sync sync --once docs
cat laptop/notes.txt
ls backup/photos
```

```text
synced "docs": 2 change(s) applied in 1 round(s)
hello
edited on backup
```

(`backup/photos` is now empty.) A sync with nothing to do applies nothing:

```sh
files_sync sync --once docs
files_sync status docs
```

```text
synced "docs": 0 change(s) applied in 0 round(s)
pair "docs"
  a: /tmp/fsync-demo/laptop (replica f563ef689fcf91de)
    index:       3 entries (1 tombstones)
    conflicts:   0
    quarantined: 0
    last sync:   2026-10-02 23:54:16 CEST
  b: /tmp/fsync-demo/backup (replica b8585899415ed538)
    index:       3 entries (1 tombstones)
    conflicts:   0
    quarantined: 0
    last sync:   2026-10-02 23:54:16 CEST
```

A **tombstone** is how a replica remembers a deletion. It keeps a deleted file from being "resurrected" by the other side. Tombstones are dropped once both replicas agree on them and the retention period has passed (30 days by default; see `tombstone_retention_days` in [§10](#10-config-file-reference)).

`sync --once` exits with status 0 when both sides converged. It exits non-zero if some path could not be synced, such as a permission error or a file that kept changing during every retry; such paths are listed in the output. Run it again later, or use the daemon.

## 4. Conflicts

A conflict happens when the same file was changed on both sides since the last sync. Nothing is lost:
- The version with the **newer modification time wins** and keeps the name. On a tie, the replica with the higher ID wins.
- The other version is renamed to `stem.sync-conflict-YYYYMMDD-HHMMSS-<ID7>.ext`, where `<ID7>` is the first 7 hex digits of the losing replica's ID.
- Both files then exist on **both** sides.

```sh
mkdir a b
echo v1 > a/report.txt
files_sync init conflict-demo --a "$DEMO/a" --b "$DEMO/b"
files_sync sync --once conflict-demo

echo "edit from A" > a/report.txt
sleep 1                          # make B's edit clearly newer
echo "edit from B" > b/report.txt
files_sync sync --once conflict-demo
ls a b
cat a/report.txt a/report.sync-conflict-*
```

```text
synced "conflict-demo": 4 change(s) applied in 2 round(s)
  conflict at report.txt: the version in /tmp/fsync-demo/a is kept as report.sync-conflict-20261002-235427-3d37213.txt
a:
report.sync-conflict-20261002-235427-3d37213.txt
report.txt

b:
report.sync-conflict-20261002-235427-3d37213.txt
report.txt
edit from B
edit from A
```

`files_sync status` lists the conflict copies that exist in each replica. To resolve a conflict, merge the content by hand and delete the conflict copy; the deletion syncs like any other.

Other conflict rules:
- **Edit vs. delete:** the edit wins. The deleted file comes back with the new content.
- **File vs. directory vs. symlink at the same name:** the directory keeps the name, then the file; the loser becomes a conflict copy.
- **Deleting a directory on one side while the other side adds files to it:** the directory is kept, with the new files in it.

## 5. Continuous sync with the daemon

`files_sync daemon <pair>` watches both replicas with inotify and syncs each change within a fraction of a second. It also runs a full rescan every 10 minutes as a backstop. It stops cleanly on Ctrl-C (SIGINT) or SIGTERM.

Usually you run it in its own terminal, or as a service. Here it runs in the background:

```sh
mkdir -p d/a d/b
files_sync init live --a "$DEMO/d/a" --b "$DEMO/d/b"
files_sync daemon live &
sleep 1

echo "hello from A" > d/a/todo.txt
mkdir -p d/b/projects/x
echo code > d/b/projects/x/main.rs
sleep 1
cat d/b/todo.txt d/a/projects/x/main.rs
```

```text
hello from A
code
```

While the daemon runs, it holds the replicas' indexes, so `status` shows the report the daemon saved after its latest cycle:

```sh
files_sync status live
```

```text
pair "live" (daemon running; its report from 2026-10-02 23:55:02 CEST)
  a: /tmp/fsync-demo/d/a (replica ae9d6d20ded92206)
    index:       4 entries (0 tombstones)
  ...
```

Stop it:

```sh
kill -INT %1
wait
```

```text
daemon for "live" stopped: 4 cycle(s), 5 change(s) applied, 0 conflict(s)
```

If a change would delete most of a replica, the daemon holds those deletions back and logs `mass deletion held back` (see **Mass-deletion guard** in [§2](#2-concepts)).

You can use `sync --once` and the daemon on the same pair, one at a time. Running both at once fails, because each replica's index is locked by the process that has it open.

## 6. Symlinks

Each replica has a **symlink policy**, named after the matching rsync option. It decides how the symlinks found **in that replica** are treated. Set it in `config.toml` ([§10](#10-config-file-reference)), normally to the same value on both replicas, and preferably right after `init`, before the first sync.

A link is **unsafe** when its target is absolute, or climbs out of the replica root through `..`. This is the same check rsync makes.

| `symlinks =` | rsync option | Effect |
|---|---|---|
| `links` (default) | `-l` / `-a` | Every symlink is synced as a symlink. Its target bytes are copied verbatim. Dangling links are synced too. |
| `skip` | (no `-l`) | Symlinks are ignored and never touched. The other replica's object at that path is left alone too. |
| `copy-links` | `-L` | Every symlink is followed: the other side gets a real file or directory with the referent's content. Dangling links are skipped. |
| `copy-unsafe-links` | `--copy-unsafe-links` | Unsafe links are followed as with `copy-links`; safe ones are synced as links. |
| `safe-links` | `--safe-links` | Unsafe links are ignored; safe ones are synced as links. |
| `copy-dirlinks` | `-k` | Links to directories are followed; all other links are synced as links. |

Two more per-replica switches:

| Setting | rsync option | Effect |
|---|---|---|
| `munge_links = true` | `--munge-links` | Link targets are stored on this replica's disk with a `/rsyncd-munged/` prefix, so they cannot be followed there. The other replica sees the original target. |
| `keep_dirlinks = true` | `-K` | If this replica has a symlink to a directory where the other has a real directory, the link is kept and the directory's contents are synced *through* it. Links to directories outside the root also need `keep_dirlinks_unsafe = true`. |

Under the following policies, an incoming change to a followed link replaces the link with a real file, as rsync's receiver does. The file the link pointed to is never written. Set `followed_write = "conflict"` to keep the link instead and store the incoming version as a conflict copy beside it.

The `files_sync` process can never be tricked into writing outside a replica root, whatever the links point to. This is enforced by the kernel (`openat2` with `RESOLVE_BENEATH`), not by the policy.

### Example: what each policy does

Replica A of each pair below holds these entries:

```sh
mkdir -p outside && echo "outside data" > outside/secret.txt

make_tree() {     # $1 = directory for replica A
  mkdir -p "$1/docs"
  echo readme > "$1/docs/readme.txt"
  ln -s docs/readme.txt "$1/safe-link"                # safe: stays inside
  ln -s "$DEMO/outside/secret.txt" "$1/abs-link"      # unsafe: absolute
  ln -s ../../outside "$1/up-link"                    # unsafe: climbs out (a directory)
  ln -s nowhere "$1/dangling"                         # dangling
}
```

For each policy, make a pair, set the policy on both replicas, sync, and list what arrives in B:

```sh
for policy in links skip copy-links copy-unsafe-links safe-links copy-dirlinks; do
  mkdir -p "sl-$policy/b"
  make_tree "$DEMO/sl-$policy/a"
  files_sync init "sl-$policy" --a "$DEMO/sl-$policy/a" --b "$DEMO/sl-$policy/b" > /dev/null
  sed -i "s/^symlinks = .*/symlinks = \"$policy\"/" "$XDG_STATE_HOME/fsync/sl-$policy/config.toml"
  files_sync sync --once "sl-$policy" > /dev/null
  echo "== $policy"
  (cd "sl-$policy/b" && find . -mindepth 1 -not -name '.~fsync.*' -printf '%y %p %l\n' | sort)
done
```

The result (`d` directory, `f` file, `l` symlink with its target):

| entry in A | `links` | `skip` | `copy-links` | `copy-unsafe-links` | `safe-links` | `copy-dirlinks` |
|---|---|---|---|---|---|---|
| `safe-link -> docs/readme.txt` | l | — | f (copy) | l | l | l |
| `abs-link -> /…/outside/secret.txt` | l | — | f (copy) | f (copy) | — | l |
| `up-link -> ../../outside` | l | — | d, with `secret.txt` | d, with `secret.txt` | — | d, with `secret.txt` |
| `dangling -> nowhere` | l | — | — | l | l | l |

`docs/readme.txt` arrives in every case.

### Example: munged links

```sh
mkdir -p m/a m/b
ln -s /etc/hostname m/a/host
files_sync init munge --a "$DEMO/m/a" --b "$DEMO/m/b" > /dev/null
# set munge_links = true on replica B (the second [[replicas]] table)
awk '/^\[\[replicas\]\]/ { n++ } n == 2 && /^munge_links/ { $0 = "munge_links = true" } { print }' \
  "$XDG_STATE_HOME/fsync/munge/config.toml" > cfg.tmp && mv cfg.tmp "$XDG_STATE_HOME/fsync/munge/config.toml"
files_sync sync --once munge
readlink m/a/host m/b/host
```

```text
synced "munge": 1 change(s) applied in 1 round(s)
/etc/hostname
/rsyncd-munged//etc/hostname
```

### Example: writing to a followed link

Under `copy-links`, B got `safe-link` as a plain file. If B edits it, A's link is replaced by a real file with the new content, and the file the link pointed to stays unchanged:

```sh
echo "new content" > sl-copy-links/b/safe-link
files_sync sync --once sl-copy-links
ls -l sl-copy-links/a/safe-link
cat sl-copy-links/a/safe-link sl-copy-links/a/docs/readme.txt
```

```text
synced "sl-copy-links": 1 change(s) applied in 1 round(s)
-rw-r--r-- 1 you you 12 Oct  2 23:55 sl-copy-links/a/safe-link
new content
readme
```

## 7. Syncing over the network

A replica can live on another host. There, `files_sync serve` runs it and answers its peer over TLS 1.3. Both sides authenticate with certificates generated by `init` and pinned in the config, as in syncthing: no CA, no passwords. Each host keeps its own replica's index in its own state directory. When a file that both sides already have changes, and it is at least 1 MiB, only the changed 128 KiB blocks are sent.

### Deployment

On the host that will run `sync`/`daemon` (here "laptop"), create the pair. Give B's address with `--b-remote`; `--b` is then the path **on the server**:

```bash
files_sync init paper --a ~/work --b /srv/mirror --b-remote server.example:7777
```

`init` prints which files to copy. Copy `config.toml` and B's `<id>.crt` and `<id>.key` to the same state directory on the server (`~/.local/state/fsync/paper/`, or the one under `$XDG_STATE_HOME`). Keep the `.key` private.

On the server:

```bash
mkdir -p /srv/mirror
files_sync serve paper b                    # listens on the address from the config
# or: files_sync serve paper b --listen 0.0.0.0:7777
```

On the laptop, use the pair as usual. `sync --once` and `daemon` connect to the server for replica B:

```bash
files_sync sync --once paper
files_sync daemon paper
```

The daemon reconnects by itself if the connection drops. Either replica can be remote, or both.

### Trying it on one machine

This simulates the two hosts with two state directories, and serves B on `127.0.0.1`:

```sh
mkdir -p net/laptop/work net/server/mirror net/laptop-state net/server-state
echo "draft" > net/laptop/work/paper.txt
dd if=/dev/urandom of=net/laptop/work/data.bin bs=1M count=3 status=none

# "laptop": create the pair, with B served at 127.0.0.1:47771
XDG_STATE_HOME="$DEMO/net/laptop-state" \
  files_sync init paper --a "$DEMO/net/laptop/work" --b "$DEMO/net/server/mirror" \
  --b-remote 127.0.0.1:47771

# copy the config and B's identity to the "server"
B_ID=$(grep '^id = ' net/laptop-state/fsync/paper/config.toml | tail -1 | cut -d'"' -f2)
mkdir -p net/server-state/fsync/paper
cp net/laptop-state/fsync/paper/{config.toml,$B_ID.crt,$B_ID.key} net/server-state/fsync/paper/

# "server": serve replica B
XDG_STATE_HOME="$DEMO/net/server-state" files_sync serve paper b &
sleep 1

# "laptop": sync
XDG_STATE_HOME="$DEMO/net/laptop-state" files_sync sync --once paper
cmp net/laptop/work/data.bin net/server/mirror/data.bin && echo identical
```

```text
serving replica c9c5677860a0d25b of "paper" (/tmp/fsync-demo/net/server/mirror) on 127.0.0.1:47771
synced "paper": 2 change(s) applied in 1 round(s)
identical
```

Change one byte of the 3 MiB file and sync again. This time only one block of it crosses the connection:

```sh
printf 'X' | dd of=net/laptop/work/data.bin bs=1 seek=1000000 conv=notrunc status=none
XDG_STATE_HOME="$DEMO/net/laptop-state" files_sync sync --once paper
cmp net/laptop/work/data.bin net/server/mirror/data.bin && echo identical
```

```text
synced "paper": 1 change(s) applied in 1 round(s), 1 sent as delta
identical
```

`1 sent as delta` means that the file was rebuilt on the server from the blocks it already had plus the changed one. Files sent whole (new files, files under 1 MiB, and every file of a local pair) are not mentioned.

### Status on each host

On the laptop, `status` shows the remote replica as ``served at 127.0.0.1:47771 (run `status` there)``: the server's index is not readable from the laptop.

On the server, `status` shows the served replica. While `serve` runs, it holds the index, so `status` reads the report that `serve` saves at start and after each sync cycle:

```sh
XDG_STATE_HOME="$DEMO/net/server-state" files_sync status paper
kill -INT %1
wait
```

```text
pair "paper"
  a: /tmp/fsync-demo/net/laptop/work (replica 5d0b6a3b8c9e1f27)
    on the client host, not here (run `status` there)
  b: /tmp/fsync-demo/net/server/mirror (replica c9c5677860a0d25b)
    served here on 127.0.0.1:47771 (`serve` running; its report from 2026-10-03 20:31:07 CEST)
    index:       2 entries (0 tombstones)
    conflicts:   0
    quarantined: 0
    last sync:   2026-10-03 20:31:07 CEST
stopped serving "paper"
```

After `serve` stopped, `status` reads the index itself and says ``served here at 127.0.0.1:47771 (`serve` not running)``. `status` knows that this host serves B because B's index is in this host's state directory: the first `serve` created it. A replica that the config marks remote, and whose index is not here, is served elsewhere.

## 8. Sandboxing and logging

**`--sandbox`** confines the process with Landlock (Linux 5.13 or newer). It may then write only beneath the pair's local replica roots and its state directory, which adds a second, kernel-enforced guarantee to the one in [§6](#6-symlinks). It works with `sync`, `daemon`, `serve` and `status`, and fails rather than run unconfined if Landlock is unavailable.

```sh
echo sandboxed > d/a/s.txt
files_sync --sandbox sync --once live
cat d/b/s.txt
```

```text
synced "live": 1 change(s) applied in 1 round(s)
sandboxed
```

**Logging** goes to stderr and is controlled by `RUST_LOG`. The default is `info`, which also prints each replica's filesystem feature check and a one-line summary per sync cycle. Some useful values:
- `RUST_LOG=warn`: warnings and errors only.
- `RUST_LOG=files_sync=debug`: detailed per-path decisions, for bug reports, and the bytes sent and received over the network.

## 9. Command reference

```text
files_sync [--sandbox] <COMMAND>

init <PAIR> --a <DIR> --b <DIR> [--a-remote HOST:PORT] [--b-remote HOST:PORT]
    Create a pair. Writes the config and a TLS identity per replica into
    $XDG_STATE_HOME/fsync/<PAIR>/. Refuses to overwrite an existing pair.
    PAIR may contain ASCII letters, digits, '.', '_' and '-' (max 64 bytes,
    not starting with '.').

sync --once [--allow-mass-delete] <PAIR>
    One sync pass, then exit. Exit status 0 when converged. Deletions of more
    than max_delete_percent of a replica's entries are held back (exit
    status 1) unless --allow-mass-delete is given.

daemon <PAIR>
    Continuous sync driven by inotify (plus a full rescan every 10 minutes).
    Stops cleanly on SIGINT/SIGTERM; a second signal exits at once.

status <PAIR>
    Index size, tombstones, conflict copies, quarantined files, last sync time.
    Reads the daemon's latest report while a daemon runs. On the host that
    serves a replica, shows that replica (from the report of `serve` while it
    runs) and the other one as being on the client host.

serve <PAIR> <a|b> [--listen HOST:PORT]
    Serve one replica to its peer over mutual TLS. --listen defaults to the
    replica's `remote` address; port 0 picks a free port (the address is printed).
```

`sync --once` and `daemon` add `, N sent as delta` to their summary when files were sent as block-level deltas.

If the reader of stdout goes away (`files_sync status docs | head -3`), a command stops printing and exits with status 141 (128 + SIGPIPE, as a shell shows for a program killed by the signal) after finishing its work. A `daemon` or `serve` keeps running; only its output is lost.

## 10. Config file reference

`$XDG_STATE_HOME/fsync/<pair>/config.toml`, as written by `init`:

```toml
name = "docs"
tombstone_retention_days = 30      # how long deletions are remembered after both sides agree
max_delete_percent = 50            # hold back a sync that deletes more of a replica (100: never)

[[replicas]]                       # replica A
id = "f563ef689fcf91de"            # do not change
root = "/home/you/docs"            # absolute path
symlinks = "links"                 # links | skip | copy-links | copy-unsafe-links | safe-links | copy-dirlinks
munge_links = false                # rsync --munge-links
keep_dirlinks = false              # rsync -K
keep_dirlinks_unsafe = false       # with keep_dirlinks: also links to directories outside the root
followed_write = "replace"         # replace | conflict (incoming change to a followed link)
trash_days = 0                     # keep replaced/deleted files in .~fsync.trash this many days (0: no trash)
device = "cdfacff3…"               # pinned TLS certificate hash; do not change
# remote = "host:7777"             # set by init --a-remote: replica is served there

[[replicas]]                       # replica B, same keys
id = "b8585899415ed538"
root = "/data/docs-mirror"
# ...
```

Edit it while no `sync`, `daemon` or `serve` for the pair is running. Unknown keys are rejected, so a typo produces an error instead of being silently ignored. For a network pair, keep the copies on both hosts identical.

## 11. What is synced, and what is not

**Synced:**
- regular files: content, permission bits (setuid/setgid are stripped) and modification time;
- directories: their existence and permission bits;
- symlinks, according to the policy ([§6](#6-symlinks)).

**Not synced:**
- owner and group, ACLs, extended attributes;
- hard links: each name is synced as a separate file;
- FIFOs, sockets and device files: they are skipped, and the same path on the other side is left alone;
- names starting with `.~fsync.` (temp files, and the root marker of [§2](#2-concepts));
- anything on another filesystem mounted inside a replica root: the mount point is not crossed and is reported as an error.

**Not supported yet:**
- ignore/exclude patterns: everything in a root is synced;
- rename detection: a renamed file is synced as a delete plus a create (with the network, a create sends the whole file);
- more than two replicas per pair;
- non-UTF-8 replica **root** paths. Non-UTF-8 file names *inside* a root are fine.

## 12. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `pair not initialised: …/config.toml not found` | Wrong pair name, or `XDG_STATE_HOME` differs from when you ran `init`. |
| `` `sync` requires --once `` | `sync` is one-shot only; use `daemon` for continuous sync. |
| `roots … overlap` | One root is inside the other. Pick two separate directories. |
| `pair already initialised` | `init` never overwrites. Delete the pair's state directory to start over: this forgets the sync history, not your files. |
| A required filesystem feature is missing | The root is on an unsupported filesystem (e.g. `/mnt/c` under WSL, NFS). Move it to ext4, xfs, btrfs or tmpfs. |
| `index database: Database already open. Cannot acquire lock.` | Another `sync`, `daemon` or `serve` for the same pair holds the index. Run one at a time. |
| `status`: `index in use (a daemon?), and no report at …` (or ``index in use (by `serve`?)``) | Another process holds the index but has not saved a status report, e.g. a `sync --once` that is still running. Run `status` again when it has finished. |
| A command exits with status 141 | Its stdout was closed early (e.g. `files_sync status docs \| head -3`). Its work was done; only the rest of its output was dropped. See [§9](#9-command-reference). |
| `not settled (changed during the sync), retry: <path>` | The file kept changing while it was being synced. Nothing was lost; the next run (or the daemon, after 1 s) retries it. |
| `.sync-conflict-` files appear | Both sides changed the file between syncs. See [§4](#4-conflicts). |
| `replica root …: its root marker is missing` (or `it is empty, but its index lists N live entries`, or `its root marker is gone`) | The root does not hold the replica's root marker, so it may be the wrong directory, such as the empty mount point of a disk that is not mounted. Nothing was synced. See **Root marker** in [§2](#2-concepts). |
| `held back N deletion(s) in …: more than 50% of its M entries` | One side lost most of its files since the last sync, and the mass-deletion guard kept the other side from following. If that was intended, run `sync --once --allow-mass-delete`. See [§2](#2-concepts). |
| `.~fsync.*` files remain after a crash | They are recovered or removed automatically by the next `sync`, `daemon` or `serve` of the pair. `.~fsync.root.<replica-id>` at the top of a root is the root marker: keep it. `.~fsync.trash` is the trash, if `trash_days` is set. |
| I need a file back that a sync replaced or deleted | Look in `.~fsync.trash` at the top of that replica's root, if `trash_days` is set there (see **Trash** in [§2](#2-concepts)). Otherwise it is gone, unless the other replica still has it. Consider turning the trash on. |
