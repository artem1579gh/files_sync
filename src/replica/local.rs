//! [`LocalReplica`]: a replica in a local directory (design §7).
//!
//! `apply` checks the logical [`Precondition`] against an index snapshot,
//! maps the indexed entry to the physical [`Expected`] fingerprint, runs the
//! matching `fs::commit` operation (which does the binding compare-and-swap
//! on disk, journaling its intent first), and on success writes the
//! resulting entry, with the version vector from the op, in one transaction
//! that also marks the intent done. The index is then already up to date when
//! the next scan (or inotify event) sees our own write, so it causes no echo
//! (§5.3 step 5, §5.8). Nothing else writes the index in between: `apply`
//! and `scan` take `&mut self`, and redb locks the file to one process.
//!
//! Followed and adopted links (§4.3.1): an op's path is resolved through the
//! index (`Disk::route`). Beneath a followed directory it is read through the
//! link; a write either goes through it (`-K`, with a `Root` at the pinned
//! directory), or first turns the link into a real copy of the directory
//! (`-L` write-back), or is declined (`followed_write = conflict`). At a
//! followed link itself, a commit acts on the link, never on what it points
//! to.
//!
//! [`LocalReplica::open`] replays the intents a crash left behind (§5.8).

use std::collections::HashMap;
use std::io::Read;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::path::Path;
use std::time::{Duration, Instant};

use crossbeam_channel::Receiver;
use jiff::ToSpan;
use rustix::fs::OFlags;
use rustix::io::Errno;

use crate::config::{FollowedWrite, ReplicaConfig, ReplicaId};
use crate::error::{Error, Result};
use crate::fs::caps::Caps;
use crate::fs::commit::{
    self, CopyNode, Ctx, Expected, FileMeta, Quarantine, Recovered, SweepReport,
};
use crate::fs::root::{follow_at, path_err};
use crate::fs::{
    FileKind, Fingerprint, PinnedFile, Recheck, RelPath, Root, StableReader, open_checked, tmpname,
};
use crate::index::{
    Entry, IndexStore, Intent, IntentId, Kind, LinkInfo, LocalMeta, PeerState, ReadTxn,
    UnmanagedReason, VersionVector, sync_mode,
};
use crate::replica::delta::{Assembler, BlockReader, Blocks, Delta};
use crate::replica::{ContentReader, Op, Outcome, Precondition, Replica, wire};
use crate::scan::{DEFAULT_RACY_WINDOW, ScanStats, Scanner, Scope};
use crate::symlink::munge;
use crate::watch::{Debouncer, EventSource, Followed, Hint, InotifySource, Watcher};

/// A replica rooted at a local directory, with its index in the pair's
/// state directory.
pub struct LocalReplica {
    config: ReplicaConfig,
    root: Root,
    caps: Caps,
    index: IndexStore,
    quarantine: Quarantine,
    racy_window: Duration,
    /// Replaces inotify as the watcher's event source (tests).
    event_source: Option<Box<dyn EventSource>>,
    /// Started by the first [`Replica::watch`]; stopped on drop.
    watcher: Option<Watcher>,
    /// How many followed links the watcher was last given.
    followed: usize,
    /// How long the trash keeps what it holds, if there is one (T31).
    trash: Option<Duration>,
    /// When the trash was last purged.
    purged: Option<Instant>,
}

/// How often [`LocalReplica::sweep_quarantine`] purges the trash.
const PURGE_EVERY: Duration = Duration::from_secs(3600);

impl LocalReplica {
    /// Opens the replica `config`: its root, its capabilities (probed,
    /// logged, and required, §1) and its index under `pair_dir`
    /// (`$XDG_STATE_HOME/fsync/<pair>/`, which must exist). Checks the root
    /// marker (§5.1, [`ensure_root_marker`]). Then replays the journal
    /// (§5.8): commits a crash interrupted are finished or undone, and
    /// quarantined old inodes are quarantined again.
    pub fn open(config: &ReplicaConfig, pair_dir: &Path) -> Result<LocalReplica> {
        let root = Root::open(&config.root)?;
        let caps = Caps::probe(root.fd())?;
        caps.log(root.path());
        caps.require_minimum()?;
        let index = IndexStore::open(&IndexStore::path_for(pair_dir, config.id), config.id)?;
        ensure_root_marker(&root, &index, config.id)?;
        let mut replica = LocalReplica {
            config: config.clone(),
            root,
            caps,
            index,
            quarantine: Quarantine::new(config.id, Quarantine::DEFAULT_GRACE),
            racy_window: DEFAULT_RACY_WINDOW,
            event_source: None,
            watcher: None,
            followed: 0,
            trash: config.trash_retention(),
            purged: None,
        };
        if replica.trash.is_some() {
            replica.quarantine.set_trash(&replica.root)?;
        }
        let pending = replica.index.journal().pending()?;
        if !pending.is_empty() {
            tracing::info!(root = %replica.root.path().display(), intents = pending.len(), "replaying the journal");
            let ids = pending.into_iter().map(|(id, intent)| (id, Some(intent)));
            replica.replay(ids.collect())?;
        }
        Ok(replica)
    }

    /// Overrides the scanner's racy window ([`DEFAULT_RACY_WINDOW`]).
    pub fn racy_window(mut self, window: Duration) -> Self {
        self.racy_window = window;
        self
    }

    /// Overrides the quarantine grace period ([`Quarantine::DEFAULT_GRACE`]),
    /// for entries already waiting (replayed by `open`) too.
    pub fn quarantine_grace(mut self, grace: Duration) -> Self {
        self.quarantine.set_grace(grace);
        self
    }

    /// Makes [`Replica::watch`] read `source` instead of watching the root
    /// with inotify (to inject events in tests). No effect once the watcher
    /// runs.
    pub fn set_event_source(&mut self, source: Box<dyn EventSource>) {
        self.event_source = Some(source);
    }

    /// The probed capabilities, to override in tests (e.g. to force the
    /// named temp-file fallback by clearing `o_tmpfile`).
    pub fn caps_mut(&mut self) -> &mut Caps {
        &mut self.caps
    }

    pub fn config(&self) -> &ReplicaConfig {
        &self.config
    }

    pub fn root(&self) -> &Root {
        &self.root
    }

    pub fn caps(&self) -> &Caps {
        &self.caps
    }

    pub fn index(&self) -> &IndexStore {
        &self.index
    }

    pub fn quarantine(&self) -> &Quarantine {
        &self.quarantine
    }

    /// Unlinks quarantined old inodes whose grace period is over, or turns
    /// them into conflict copies if they were written to (§5.3 step 4(f)).
    /// Their journal intents are done then.
    pub fn sweep_quarantine(&mut self) -> SweepReport {
        let report = self.quarantine.sweep();
        if let Err(e) = self.index.journal().forget(&report.finished) {
            // Replaying them later finds nothing to do.
            tracing::warn!(error = %e, "cannot forget settled quarantine intents");
        }
        if let Some(keep) = self.trash
            && self.purged.is_none_or(|t| t.elapsed() >= PURGE_EVERY)
        {
            self.purged = Some(Instant::now());
            self.purge_trash(keep);
        }
        report
    }

    /// Removes what the trash has held for longer than `keep` (T31). A
    /// failure is logged; the next purge tries again.
    pub fn purge_trash(&mut self, keep: Duration) {
        match commit::purge_trash(&self.root, keep) {
            Ok(0) => {}
            Ok(n) => {
                tracing::info!(root = %self.root.path().display(), removed = n, "purged the trash")
            }
            Err(e) => tracing::warn!(error = %e, "cannot purge the trash"),
        }
    }

    /// Replays the intents `ids` (§5.8), reading those given without one
    /// from the journal, then finishes them: done, or kept while their old
    /// inode is quarantined. An intent that cannot be replayed (an I/O error)
    /// is kept for the next time.
    fn replay(&mut self, mut ids: Vec<(IntentId, Option<Intent>)>) -> Result<()> {
        if ids.iter().any(|(_, i)| i.is_none()) {
            let pending = self.index.journal().pending()?;
            for (id, intent) in &mut ids {
                if intent.is_none() {
                    *intent = pending
                        .iter()
                        .find(|(p, _)| p == id)
                        .map(|(_, i)| i.clone());
                }
            }
        }
        let top = RelPath::root();
        let ctx = Ctx {
            root: &self.root,
            base: &top,
            caps: &self.caps,
            replica: self.config.id,
            journal: self.index.journal(),
        };
        let (mut done, mut kept) = (Vec::new(), Vec::new());
        for (id, intent) in ids {
            if let Some(intent) = intent {
                match commit::recover(&ctx, &mut self.quarantine, id, &intent) {
                    Ok(rep) => log_recovered(&intent, &rep),
                    Err(e) => {
                        tracing::warn!(path = %intent.path, error = %e, "cannot replay intent; retrying at the next start");
                        continue;
                    }
                }
            }
            if self.quarantine.holds(id) {
                kept.push(id);
            } else {
                done.push(id);
            }
        }
        ctx.journal.finish(&done, &kept)
    }

    /// After a scan that changed something (a new, retargeted or removed
    /// followed link, or a new directory beneath one), the watcher's view of
    /// the followed links is refreshed.
    fn after_scan(&mut self, stats: &ScanStats) {
        if stats.changed > 0 {
            self.watch_followed();
        }
    }

    /// Hands the followed (and adopted) links, with their referents, to the
    /// watcher (design §4.5), unless there are none now and there were none
    /// the last time. Links that cannot be opened now are left out until the
    /// next time.
    fn watch_followed(&mut self) {
        let Some(watcher) = &self.watcher else {
            return;
        };
        let disk = Disk {
            root: &self.root,
            config: &self.config,
        };
        let found = (|| -> Result<Vec<Followed>> {
            let snap = self.index.read()?;
            let mut links = Vec::new();
            for (path, e) in snap.iter_prefix(&RelPath::root())? {
                if e.local.via_link.is_none() || !e.is_live() {
                    continue;
                }
                let dir = e.kind == Kind::Dir;
                let base = match disk.route(&snap, &path, false)? {
                    Route::At(base) => base,
                    _ => continue,
                };
                if let Some(fd) = disk.open_followed(&base, &path, &e)? {
                    links.push(Followed { path, fd, dir });
                }
            }
            Ok(links)
        })();
        let links = match found {
            Ok(links) => links,
            Err(e) => {
                tracing::warn!(error = %e, "cannot list followed links to watch");
                return;
            }
        };
        if !links.is_empty() || self.followed > 0 {
            tracing::debug!(links = links.len(), "watching followed links");
            self.followed = links.len();
            watcher.follow(links);
        }
    }

    /// The scanner, with this replica's settings.
    fn scanner(&self) -> Scanner<'_> {
        Scanner::new(&self.root, &self.index, self.config.symlinks)
            .munge_links(self.config.munge_links)
            .keep_dirlinks(self.config.keep_dirlinks, self.config.keep_dirlinks_unsafe)
            .racy_window(self.racy_window)
    }

    fn disk(&self) -> Disk<'_> {
        Disk {
            root: &self.root,
            config: &self.config,
        }
    }

    /// Finishes the intents of the commits since the last call: done, or
    /// kept while their old inode is quarantined. After a commit that
    /// returned an error (`failed`) they are replayed at once instead, since
    /// an error path may leave reserved names behind.
    fn settle(&mut self, failed: bool) -> Result<()> {
        let open = self.index.journal().take_open();
        if failed {
            return self.replay(open.into_iter().map(|id| (id, None)).collect());
        }
        let (kept, done): (Vec<_>, Vec<_>) =
            open.into_iter().partition(|id| self.quarantine.holds(*id));
        self.index.journal().finish(&done, &kept)
    }

    /// One try at `apply`: its outcome, or `None` if a followed directory on
    /// the way was turned into a real one first (try again). The
    /// precondition is checked on a snapshot, not in a write transaction:
    /// the commit writes the journal in transactions of its own, and redb has
    /// one writer at a time.
    fn attempt(
        &mut self,
        path: &RelPath,
        op: Op,
        pre: &Precondition,
        content: Option<&mut dyn Read>,
    ) -> Result<Option<Outcome>> {
        let snap = self.index.read()?;
        let cur = snap.get(path)?;
        let failed = |reason, cur: Option<Entry>| {
            tracing::debug!(%path, reason, "precondition failed");
            Ok(Some(Outcome::PreconditionFailed(cur.map(wire))))
        };
        let conflict_mode = self.config.followed_write == FollowedWrite::Conflict;
        let disk_change = changes_disk(&op, cur.as_ref());
        // Only checking and recording needs no write access: it may look
        // through any followed link.
        let route = self.disk().route(&snap, path, disk_change)?;
        let (base, step) = match route {
            Route::Refused(reason) => return failed(reason, cur),
            // Beneath a followed directory that is not written through
            // (design §4.3).
            Route::WriteBack { base, link } if conflict_mode => {
                let step = self.decline(&snap, &base, &link, path, op, cur.as_ref(), content);
                (base, step)
            }
            Route::WriteBack { base, link } => {
                if let Some(reason) = check(cur.as_ref(), pre) {
                    return failed(reason, cur);
                }
                return self.materialize(&snap, &base, &link, path, cur);
            }
            Route::At(base) => {
                let followed = cur
                    .as_ref()
                    .filter(|e| e.local.via_link.is_some() && !self.disk().writes_through(e));
                let step = if conflict_mode
                    && followed.is_some()
                    && disk_change
                    && !matches!(op, Op::RenameToConflict { .. })
                {
                    self.decline(&snap, &base, path, path, op, cur.as_ref(), content)
                } else if let Some(reason) = check(cur.as_ref(), pre) {
                    return failed(reason, cur);
                } else {
                    self.run(&snap, &base, path, op, cur.as_ref(), content)
                };
                (base, step)
            }
        };
        if let Ok(Step::WriteBack) = step {
            return self.materialize(&snap, &base, path, path, cur);
        }
        drop(snap);
        let step = match step {
            Ok(Step::Preserved(conflict)) => base.full(&conflict).map(Step::Preserved),
            other => other,
        };
        self.finish(path, step, cur).map(Some)
    }

    /// Records what a step did: its entries go into the index in the same
    /// transaction that finishes the commit's intents.
    fn finish(
        &mut self,
        path: &RelPath,
        step: Result<Step>,
        cur: Option<Entry>,
    ) -> Result<Outcome> {
        let entries = match step {
            Ok(Step::Put(entries)) => entries,
            other => {
                // Nothing to index.
                let finished = self.settle(other.is_err());
                if let (Err(e), Err(_)) = (&finished, &other) {
                    // Report the commit's error; the next start replays.
                    tracing::warn!(%path, error = %e, "cannot finish intents");
                }
                let step = other?;
                finished?;
                return Ok(match step {
                    Step::Failed(reason) => {
                        tracing::debug!(%path, reason, "precondition failed");
                        Outcome::PreconditionFailed(cur.map(wire))
                    }
                    Step::Preserved(conflict) => Outcome::Preserved { conflict },
                    Step::Keep => match cur {
                        Some(e) => Outcome::Applied(wire(e)),
                        None => Outcome::PreconditionFailed(None),
                    },
                    Step::Put(_) | Step::WriteBack => unreachable!("handled before"),
                });
            }
        };
        let open = self.index.journal().take_open();
        let mut txn = self.index.write()?;
        let mut first = None;
        for (p, mut e) in entries {
            txn.put(&p, &mut e)?;
            first.get_or_insert(e);
        }
        let (kept, done): (Vec<_>, Vec<_>) =
            open.into_iter().partition(|id| self.quarantine.holds(*id));
        txn.finish_intents(&done, &kept)?;
        txn.commit()?;
        Ok(Outcome::Applied(wire(first.expect("at least one entry"))))
    }

    /// Turns the followed directory link `link` (in `base`) into a real copy
    /// of the directory it shows, so a change at `path` (at or beneath it)
    /// can be applied without writing through the link (design §4.3). The
    /// copy comes from the index: every file must still hash as indexed.
    /// Then the link's subtree is rescanned, which records the new inodes
    /// (a local change only).
    fn materialize(
        &mut self,
        snap: &ReadTxn,
        base: &Base,
        link: &RelPath,
        path: &RelPath,
        cur: Option<Entry>,
    ) -> Result<Option<Outcome>> {
        let failed = |reason| {
            tracing::debug!(%path, %link, reason, "cannot turn the followed directory into a real one");
            Ok(Some(Outcome::PreconditionFailed(cur.clone().map(wire))))
        };
        let disk = Disk {
            root: &self.root,
            config: &self.config,
        };
        let Some(e) = snap
            .get(link)?
            .filter(|e| e.kind == Kind::Dir && e.local.via_link.is_some())
        else {
            return failed("not a followed directory");
        };
        let Some(exp) = disk.expected(base, link, &e)? else {
            return failed("changed since scan");
        };
        let mut tree = Vec::new();
        let mut sources = HashMap::new();
        for (p, ent) in snap.iter_prefix(link)? {
            if p == *link {
                continue;
            }
            let rel = RelPath::new(&p.as_bytes()[link.as_bytes().len() + 1..])?;
            let node = match &ent.kind {
                Kind::Dir => CopyNode::Dir { mode: ent.mode },
                Kind::File { hash, .. } => CopyNode::File {
                    meta: FileMeta {
                        mode: ent.mode,
                        mtime_ns: ent.mtime_ns,
                    },
                    hash: *hash,
                },
                Kind::Symlink { target } => CopyNode::Symlink {
                    target: ent
                        .local
                        .raw_target
                        .clone()
                        .unwrap_or_else(|| target.clone()),
                },
                // Not synced, so not copied (rsync would skip them too).
                Kind::Tombstone | Kind::Unmanaged(_) => continue,
            };
            if matches!(node, CopyNode::File { .. }) {
                sources.insert(rel.clone(), (p, ent));
            }
            tree.push((rel, node));
        }
        let mut open = |rel: &RelPath| -> Result<Box<dyn Read>> {
            let (full, ent) = &sources[rel];
            Ok(Box::new(disk.open_file(snap, full, ent)?))
        };
        let ctx = Ctx {
            root: base.root(&self.root),
            base: &base.link,
            caps: &self.caps,
            replica: self.config.id,
            journal: self.index.journal(),
        };
        let res = commit::materialize(
            &ctx,
            &mut self.quarantine,
            &base.rel(link),
            &exp,
            e.mode,
            &tree,
            &mut open,
        );
        let settled = self.settle(res.is_err());
        let out = res?;
        settled?;
        match out {
            commit::Outcome::Applied(_) => {
                tracing::info!(%link, %path, "followed directory turned into a real one for an incoming change");
                self.scan(Scope::Paths(vec![link.clone()]))?;
                Ok(None)
            }
            commit::Outcome::PreconditionFailed(reason) => failed(reason),
            commit::Outcome::Preserved { conflict } => Ok(Some(Outcome::Preserved {
                conflict: base.full(&conflict)?,
            })),
            commit::Outcome::Removed => unreachable!("materialize does not remove"),
        }
    }

    /// `followed_write = conflict` (design §4.3): the followed link `near`
    /// stays as it is. The local version at `path` (at or beneath `near`)
    /// wins: its version vector absorbs the incoming one plus a local bump,
    /// so it goes back to the peer. An incoming file or symlink is kept as a
    /// conflict copy beside `near`.
    #[allow(clippy::too_many_arguments)]
    fn decline(
        &mut self,
        txn: &ReadTxn,
        base: &Base,
        near: &RelPath,
        path: &RelPath,
        op: Op,
        cur: Option<&Entry>,
        content: Option<&mut dyn Read>,
    ) -> Result<Step> {
        let id = self.config.id;
        let theirs = match &op {
            Op::WriteFile { vv, .. }
            | Op::Mkdir { vv, .. }
            | Op::Symlink { vv, .. }
            | Op::Delete { vv }
            | Op::Rmdir { vv }
            | Op::SetMeta { vv, .. } => vv.clone(),
            // The local version stays where it is; the winner's version
            // comes next, and is declined then.
            Op::RenameToConflict { .. } => return Ok(Step::Keep),
        };
        let mut vv = theirs.merge(&cur.map(|e| e.vv.clone()).unwrap_or_default());
        vv.bump_after(id, txn.max_counter()?);
        let kept = match cur {
            Some(e) if !e.is_tombstone() => Entry {
                vv: vv.clone(),
                ..e.clone()
            },
            _ => tombstone(vv.clone()),
        };
        let mut entries = vec![(path.clone(), kept)];
        let is_file = matches!(op, Op::WriteFile { .. });
        if is_file || matches!(op, Op::Symlink { .. }) {
            // Named after the replica that most likely wrote it.
            let writer = theirs
                .iter()
                .filter(|(r, _)| *r != id)
                .max_by_key(|(_, c)| *c)
                .map_or(id, |(r, _)| r);
            let Some(copy) = self.copy_name(txn, base, near, path, is_file, writer)? else {
                return Ok(Step::Failed("no free conflict name"));
            };
            let ctx = Ctx {
                root: base.root(&self.root),
                base: &base.link,
                caps: &self.caps,
                replica: id,
                journal: self.index.journal(),
            };
            let rel = base.rel(&copy);
            let mut copy_vv = VersionVector::new();
            copy_vv.bump_after(id, vv.max_counter());
            let entry = match op {
                Op::WriteFile { meta, hash, .. } => {
                    let Some(content) = content else {
                        return Err(invalid(path, "WriteFile needs content"));
                    };
                    match applied(commit::create_file(&ctx, &rel, content, meta, &hash)?) {
                        Ok(fp) => file_entry(&fp, hash, copy_vv),
                        Err(step) => return Ok(step),
                    }
                }
                Op::Symlink {
                    target, mtime_ns, ..
                } => {
                    let raw = if self.config.munge_links {
                        munge(&target)
                    } else {
                        target.clone()
                    };
                    match applied(commit::create_symlink(&ctx, &rel, &raw)?) {
                        Ok(fp) => {
                            let local = LocalMeta {
                                raw_target: (raw != target).then_some(raw),
                                ..local_of(&fp)
                            };
                            let kind = Kind::Symlink { target };
                            entry(kind, sync_mode(fp.mode), mtime_ns, copy_vv, local)
                        }
                        Err(step) => return Ok(step),
                    }
                }
                _ => unreachable!("files and symlinks only"),
            };
            tracing::warn!(%path, link = %near, %copy, "incoming change to a followed symlink kept as a conflict copy (followed_write = conflict)");
            entries.push((copy, entry));
        } else {
            tracing::warn!(%path, link = %near, "incoming change to a followed symlink declined (followed_write = conflict)");
        }
        Ok(Step::Put(entries))
    }

    /// A free conflict name for the incoming version of `path`, beside the
    /// followed link `near` (§6.2 naming, with the writer's ID7).
    fn copy_name(
        &self,
        txn: &ReadTxn,
        base: &Base,
        near: &RelPath,
        path: &RelPath,
        is_file: bool,
        writer: ReplicaId,
    ) -> Result<Option<RelPath>> {
        let dir = near.parent().unwrap_or_default();
        let name = path.name().expect("never the root");
        let root = base.root(&self.root);
        let now = jiff::Zoned::now().datetime();
        for i in 0..16 {
            let when = now.checked_add(i.seconds()).unwrap_or(now);
            let copy = dir.join(tmpname::conflict_name(name, is_file, when, writer))?;
            let taken = txn.get(&copy)?.is_some_and(|e| !e.is_tombstone())
                || match root.stat(&base.rel(&copy)) {
                    Ok(_) => true,
                    Err(e) if e.is_not_found() => false,
                    Err(e) => return Err(e),
                };
            if !taken {
                return Ok(Some(copy));
            }
        }
        Ok(None)
    }

    /// Runs `op` at `path` (beneath `base`) after the precondition passed;
    /// `cur` is the path's entry.
    fn run(
        &mut self,
        txn: &ReadTxn,
        base: &Base,
        path: &RelPath,
        op: Op,
        cur: Option<&Entry>,
        content: Option<&mut dyn Read>,
    ) -> Result<Step> {
        let id = self.config.id;
        let munge_links = self.config.munge_links;
        let disk = Disk {
            root: &self.root,
            config: &self.config,
        };
        let rel = base.rel(path);
        let rel = &rel;
        let ctx = Ctx {
            root: base.root(&self.root),
            base: &base.link,
            caps: &self.caps,
            replica: id,
            journal: self.index.journal(),
        };
        let q = &mut self.quarantine;
        let live = cur.filter(|e| e.is_live());
        let file_or_link =
            live.filter(|e| matches!(e.kind, Kind::File { .. } | Kind::Symlink { .. }));
        let expect = |e: &Entry| disk.expected(base, path, e);
        let invalid = |reason| Err(invalid(path, reason));

        match op {
            Op::WriteFile { meta, hash, vv } => {
                let Some(content) = content else {
                    return invalid("WriteFile needs content");
                };
                // Over a followed link, the link itself is replaced (§4.3).
                let out = match (live, file_or_link) {
                    (None, _) => commit::create_file(&ctx, rel, content, meta, &hash)?,
                    (Some(_), Some(e)) => {
                        let Some(exp) = expect(e)? else {
                            return Ok(Step::Failed("changed since scan"));
                        };
                        commit::replace_file(&ctx, q, rel, &exp, content, meta, &hash)?
                    }
                    (Some(_), None) => {
                        return invalid("WriteFile over a directory (rmdir it first)");
                    }
                };
                Ok(match applied(out) {
                    Ok(fp) => Step::put(path, file_entry(&fp, hash, vv)),
                    Err(step) => step,
                })
            }

            Op::Symlink {
                target,
                mtime_ns,
                vv,
            } => {
                let raw = if munge_links {
                    munge(&target)
                } else {
                    target.clone()
                };
                let out = match (live, file_or_link) {
                    (None, _) => commit::create_symlink(&ctx, rel, &raw)?,
                    (Some(_), Some(e)) => {
                        let Some(exp) = expect(e)? else {
                            return Ok(Step::Failed("changed since scan"));
                        };
                        commit::replace_symlink(&ctx, q, rel, &exp, &raw)?
                    }
                    (Some(_), None) => return invalid("Symlink over a directory (rmdir it first)"),
                };
                Ok(match applied(out) {
                    Ok(fp) => {
                        let local = LocalMeta {
                            raw_target: (raw != target).then_some(raw),
                            ..local_of(&fp)
                        };
                        let kind = Kind::Symlink { target };
                        Step::put(path, entry(kind, sync_mode(fp.mode), mtime_ns, vv, local))
                    }
                    Err(step) => step,
                })
            }

            Op::Mkdir { mode, mtime_ns, vv } => {
                if live.is_some() {
                    return invalid("Mkdir over an existing object");
                }
                Ok(match applied(commit::mkdir(&ctx, rel, mode)?) {
                    Ok(fp) => Step::put(
                        path,
                        entry(Kind::Dir, sync_mode(fp.mode), mtime_ns, vv, local_of(&fp)),
                    ),
                    Err(step) => step,
                })
            }

            // Of a followed file, the link is deleted.
            Op::Delete { vv } => {
                let Some(e) = file_or_link else {
                    return invalid("Delete needs a file or symlink");
                };
                let Some(exp) = expect(e)? else {
                    return Ok(Step::Failed("changed since scan"));
                };
                Ok(match removed(commit::delete(&ctx, q, rel, &exp)?) {
                    Ok(()) => Step::put(path, tombstone(vv)),
                    Err(step) => step,
                })
            }

            Op::Rmdir { vv } => {
                let Some(e) = live.filter(|e| e.kind == Kind::Dir) else {
                    return invalid("Rmdir needs a directory");
                };
                // Its children must be deleted first (§5.7), in the index too.
                let live_child = txn
                    .iter_prefix(path)?
                    .into_iter()
                    .any(|(p, e)| p != *path && !e.is_tombstone());
                if live_child {
                    return Ok(Step::Failed("directory has live entries in the index"));
                }
                let out = if e.local.via_link.is_some() {
                    // A followed or adopted directory: its link is deleted,
                    // as rsync deletes a symlink; the directory it pointed
                    // to stays (empty, as far as this path showed it).
                    let Some(exp) = expect(e)? else {
                        return Ok(Step::Failed("changed since scan"));
                    };
                    commit::delete(&ctx, q, rel, &exp)?
                } else {
                    commit::rmdir(&ctx, q, rel)?
                };
                Ok(match removed(out) {
                    Ok(()) => Step::put(path, tombstone(vv)),
                    Err(step) => step,
                })
            }

            // Of a followed file, the link is renamed.
            Op::RenameToConflict { to } => {
                let Some(e) = file_or_link else {
                    return invalid("RenameToConflict needs a file or symlink");
                };
                if txn.get(&to)?.is_some_and(|t| !t.is_tombstone()) {
                    return Ok(Step::Failed("conflict name is indexed"));
                }
                if to.parent() != path.parent() {
                    return Err(Error::InvalidPath {
                        path: to.as_bytes().to_vec(),
                        reason: "rename target is not a sibling of the renamed path",
                    });
                }
                let Some(exp) = expect(e)? else {
                    return Ok(Step::Failed("changed since scan"));
                };
                let fp = match applied(commit::rename_to(&ctx, rel, &exp, &base.rel(&to))?) {
                    Ok(fp) => fp,
                    Err(step) => return Ok(step),
                };
                // The path: a local deletion. The copy: a new local object,
                // whose counter comes after the tombstone's.
                let mut gone_vv = e.vv.clone();
                gone_vv.bump_after(id, txn.max_counter()?);
                let mut copy_vv = VersionVector::new();
                copy_vv.bump_after(id, gone_vv.max_counter());
                let local = LocalMeta {
                    raw_target: e.local.raw_target.clone(),
                    racy: e.local.racy,
                    ..local_of(&fp)
                };
                let copy = entry(e.kind.clone(), e.mode, e.mtime_ns, copy_vv, local);
                Ok(Step::Put(vec![
                    (path.clone(), tombstone(gone_vv)),
                    (to, copy),
                ]))
            }

            Op::SetMeta { mode, mtime_ns, vv } => {
                let Some(e) = live else {
                    return invalid("SetMeta needs a file, directory or symlink");
                };
                let Some(exp) = expect(e)? else {
                    return Ok(Step::Failed("changed since scan"));
                };
                let mode = sync_mode(mode);
                // Nothing to change on disk: only check it still matches.
                let index_only = |kept: &Entry, mtime_ns| -> Result<Step> {
                    let still = match ctx.root.stat(rel) {
                        Ok(now) => {
                            now.kind == exp.fp.kind
                                && if now.kind == FileKind::File {
                                    exp.fp.unchanged(&now)
                                } else {
                                    now.same_file(&exp.fp)
                                }
                        }
                        Err(e) if e.is_not_found() => false,
                        Err(e) => return Err(e),
                    };
                    if !still {
                        return Ok(Step::Failed("changed since scan"));
                    }
                    let mut kept = kept.clone();
                    kept.mtime_ns = mtime_ns;
                    kept.vv = vv.clone();
                    Ok(Step::put(path, kept))
                };
                match &e.kind {
                    Kind::File { hash, .. } => {
                        if mode == e.mode && mtime_ns == e.mtime_ns {
                            return index_only(e, mtime_ns);
                        }
                        // Rewrite as a copy with the new metadata (of a
                        // followed file: the link is replaced by the copy);
                        // the content hash guards against a change mid-copy.
                        let mut copy = match disk.open_file(txn, path, e) {
                            Ok(r) => r,
                            Err(err) if err.is_unstable() || err.is_not_found() => {
                                return Ok(Step::Failed("changed since scan"));
                            }
                            Err(err) => return Err(err),
                        };
                        let meta = FileMeta { mode, mtime_ns };
                        let out = commit::replace_file(&ctx, q, rel, &exp, &mut copy, meta, hash)?;
                        Ok(match applied(out) {
                            Ok(fp) => Step::put(path, file_entry(&fp, *hash, vv)),
                            Err(step) => step,
                        })
                    }
                    Kind::Dir if mode != e.mode => match e.local.via_link {
                        None => Ok(
                            match applied(commit::set_dir_mode(&ctx, rel, &exp.fp, mode)?) {
                                Ok(fp) => Step::put(
                                    path,
                                    entry(
                                        Kind::Dir,
                                        sync_mode(fp.mode),
                                        mtime_ns,
                                        vv,
                                        local_of(&fp),
                                    ),
                                ),
                                Err(step) => step,
                            },
                        ),
                        // `-K`: changed through the link (§4.3).
                        Some(_) if disk.writes_through(e) => {
                            let Some((link, dir)) = disk.followed(base, path, e)? else {
                                return Ok(Step::Failed("changed since scan"));
                            };
                            let out = commit::set_referent_mode(&ctx, rel, &link, &dir, mode)?;
                            Ok(match applied(out) {
                                Ok(fp) => {
                                    let local = LocalMeta {
                                        via_link: e.local.via_link.clone(),
                                        ..local_of(&fp)
                                    };
                                    let kind = Kind::Dir;
                                    Step::put(
                                        path,
                                        entry(kind, sync_mode(fp.mode), mtime_ns, vv, local),
                                    )
                                }
                                Err(step) => step,
                            })
                        }
                        // Turned into a real directory first.
                        Some(_) => Ok(Step::WriteBack),
                    },
                    // A directory's mtime, and a symlink's mode and mtime,
                    // are not synced state (§3).
                    _ => index_only(e, mtime_ns),
                }
            }
        }
    }

    /// Indexes the symlink at `path` as the directory it points to (`-K`),
    /// when it does point to one that may be adopted. Returns whether it is
    /// adopted now.
    fn adopt_link(&mut self, path: &RelPath) -> Result<bool> {
        let adopted =
            |e: &Entry| e.kind == Kind::Dir && e.local.via_link.as_ref().is_some_and(|v| v.adopted);
        if !self.config.keep_dirlinks || path.is_root() {
            return Ok(false);
        }
        match self.index.get(path)? {
            Some(e) if adopted(&e) => return Ok(true),
            Some(e)
                if matches!(
                    e.kind,
                    Kind::Symlink { .. } | Kind::Unmanaged(UnmanagedReason::IgnoredLink)
                ) => {}
            _ => return Ok(false),
        }
        let stats = self
            .scanner()
            .adopt([path.clone()])
            .scan(&Scope::Paths(vec![path.clone()]))?;
        let done = self.index.get(path)?.is_some_and(|e| adopted(&e));
        if done {
            tracing::info!(%path, "symlink to a directory adopted as that directory (keep_dirlinks)");
            self.after_scan(&stats);
        }
        Ok(done)
    }
}

impl Replica for LocalReplica {
    fn id(&self) -> ReplicaId {
        self.config.id
    }

    fn scan(&mut self, scope: Scope) -> Result<ScanStats> {
        // Before and after: a root removed during the scan (`rm -rf`, the
        // marker with it) fails the cycle before its deletions propagate.
        check_root_marker(&self.root, self.config.id)?;
        let stats = self.scanner().scan(&scope)?;
        check_root_marker(&self.root, self.config.id)?;
        self.after_scan(&stats);
        Ok(stats)
    }

    fn changes_since(&self, seq: u64) -> Result<Vec<(RelPath, Entry)>> {
        Ok(self
            .index
            .changes_since(seq)?
            .into_iter()
            .map(|(p, e)| (p, wire(e)))
            .collect())
    }

    fn open_read(&self, path: &RelPath, expect: &Entry) -> Result<Box<dyn ContentReader>> {
        if !matches!(expect.kind, Kind::File { .. }) {
            return Err(invalid(path, "open_read of an entry that is not a file"));
        }
        let snap = self.index.read()?;
        let cur = indexed_as(&snap, path, &expect.kind)?;
        Ok(Box::new(self.disk().open_file(&snap, path, &cur)?))
    }

    fn blocks(&self, path: &RelPath, expect: &Kind) -> Result<Option<Blocks>> {
        let Kind::File { size, .. } = *expect else {
            return Err(invalid(path, "blocks of an entry that is not a file"));
        };
        if !Blocks::fits(size) {
            return Ok(None);
        }
        let snap = self.index.read()?;
        let cur = indexed_as(&snap, path, expect)?;
        let mut reader = self.disk().open_file(&snap, path, &cur)?;
        drop(snap);
        Blocks::read(&mut reader, size).map(Some)
    }

    fn read_blocks(
        &self,
        path: &RelPath,
        expect: &Kind,
        blocks: &[u32],
    ) -> Result<Box<dyn ContentReader>> {
        let Kind::File { size, .. } = *expect else {
            return Err(invalid(path, "read_blocks of an entry that is not a file"));
        };
        if blocks.iter().any(|&i| u64::from(i) >= Blocks::count(size)) {
            return Err(invalid(
                path,
                "read_blocks of a block the file does not have",
            ));
        }
        let snap = self.index.read()?;
        let cur = indexed_as(&snap, path, expect)?;
        let Some(file) = self.disk().open_pinned(&snap, path, &cur)? else {
            return Err(Error::Unstable {
                path: path.as_bytes().to_vec(),
                reason: "file changed since scan",
            });
        };
        Ok(Box::new(BlockReader::new(
            path.as_bytes(),
            file,
            size,
            blocks.to_vec(),
        )))
    }

    /// Checks the precondition and the delta, pins the current file, and
    /// then applies `op` with the assembled content (design §7.1): the
    /// commit is an ordinary `WriteFile`, so the whole-file hash and the CAS
    /// replace are checked as for any other content.
    fn apply_delta(
        &mut self,
        path: &RelPath,
        op: Op,
        pre: Precondition,
        delta: &Delta,
        data: &mut dyn Read,
    ) -> Result<Outcome> {
        if !matches!(op, Op::WriteFile { .. }) {
            return Err(invalid(path, "apply_delta needs WriteFile"));
        }
        let snap = self.index.read()?;
        let cur = snap.get(path)?;
        if let Some(reason) = check(cur.as_ref(), &pre) {
            tracing::debug!(%path, reason, "precondition failed");
            return Ok(Outcome::PreconditionFailed(cur.map(wire)));
        }
        let Some(cur) = cur else {
            return Err(invalid(path, "apply_delta needs a file at the path"));
        };
        let Kind::File { size, .. } = cur.kind else {
            return Err(invalid(path, "apply_delta needs a file at the path"));
        };
        if let Some(reason) = delta.misfit(size) {
            return Err(invalid(path, reason));
        }
        let Some(old) = self.disk().open_pinned(&snap, path, &cur)? else {
            tracing::debug!(%path, "precondition failed: file changed since scan");
            return Ok(Outcome::PreconditionFailed(Some(wire(cur))));
        };
        drop(snap);
        let mut content = Assembler::new(path.as_bytes(), old, delta, data);
        self.apply(path, op, pre, Some(&mut content))
    }

    fn apply(
        &mut self,
        path: &RelPath,
        op: Op,
        pre: Precondition,
        mut content: Option<&mut dyn Read>,
    ) -> Result<Outcome> {
        if path.is_root() {
            return Err(invalid(path, "cannot apply to the replica root"));
        }
        // A change beneath a followed directory first turns that directory
        // into a real one; once per followed directory on the way.
        for _ in 0..path.depth() {
            let content = content.as_mut().map(|c| &mut **c as &mut dyn Read);
            if let Some(outcome) = self.attempt(path, op.clone(), &pre, content)? {
                return Ok(outcome);
            }
        }
        Ok(Outcome::PreconditionFailed(self.index.get(path)?.map(wire)))
    }

    /// Starts watching (once; later calls return the same hints). The
    /// watches are in place when this returns, so a scan started afterwards
    /// misses nothing. `None` if inotify cannot be used; the caller then
    /// relies on periodic full rescans.
    fn watch(&mut self) -> Option<Receiver<Hint>> {
        if self.watcher.is_none() {
            let root = self.root.path().display().to_string();
            let source = match self.event_source.take() {
                Some(source) => source,
                None => match self.root.try_clone().and_then(InotifySource::new) {
                    Ok(source) => {
                        tracing::info!(%root, dirs = source.watches(), "watching");
                        Box::new(source)
                    }
                    Err(e) => {
                        tracing::warn!(%root, error = %e, "cannot watch; relying on periodic rescans");
                        return None;
                    }
                },
            };
            let name = format!("{:.7}", self.config.id.to_string());
            match Watcher::spawn(&name, source, Debouncer::default()) {
                Ok(w) => self.watcher = Some(w),
                Err(e) => {
                    tracing::warn!(%root, error = %e, "cannot watch; relying on periodic rescans");
                    return None;
                }
            }
            self.watch_followed();
        }
        self.watcher.as_ref().map(Watcher::hints)
    }

    fn adopt(&mut self, path: &RelPath) -> Result<bool> {
        self.adopt_link(path)
    }

    fn record_sync(
        &mut self,
        peer: ReplicaId,
        tombstones: Vec<(RelPath, VersionVector, PeerState)>,
        retention: Duration,
    ) -> Result<Vec<RelPath>> {
        let removed = self
            .index
            .record_sync(peer, &tombstones, now_ns(), retention)?;
        if !removed.is_empty() {
            tracing::debug!(root = %self.root.path().display(), count = removed.len(), "tombstones collected");
        }
        Ok(removed)
    }
}

/// Wall-clock time in ns since the Unix epoch.
fn now_ns() -> i64 {
    jiff::Timestamp::now().as_nanosecond() as i64
}

/// What a commit led to.
enum Step {
    /// Committed: write these entries, the path's own first.
    Put(Vec<(RelPath, Entry)>),
    Failed(&'static str),
    Preserved(RelPath),
    /// Nothing changed, by design: the path keeps its entry.
    Keep,
    /// The path is a followed directory that must become a real one first.
    WriteBack,
}

impl Step {
    fn put(path: &RelPath, entry: Entry) -> Step {
        Step::Put(vec![(path.clone(), entry)])
    }
}

/// Where a path lies on disk (design §4.3).
enum Route {
    /// Beneath `base`.
    At(Base),
    /// Beneath the followed directory link `link` (in `base`), which is not
    /// written through: it must become a real directory first.
    WriteBack { base: Base, link: RelPath },
    /// An ancestor is not an indexed directory, or a followed one changed.
    Refused(&'static str),
}

/// The directory a path is resolved from: the replica root, or the
/// directory the link at `link` points to (reading through a followed link,
/// or writing through a `-K` one).
struct Base {
    root: Option<Root>,
    link: RelPath,
}

impl Base {
    fn top() -> Base {
        Base {
            root: None,
            link: RelPath::root(),
        }
    }

    fn root<'a>(&'a self, replica: &'a Root) -> &'a Root {
        self.root.as_ref().unwrap_or(replica)
    }

    /// `path` (the link or beneath it) relative to the base.
    fn rel(&self, path: &RelPath) -> RelPath {
        let (p, l) = (path.as_bytes(), self.link.as_bytes());
        if l.is_empty() {
            return path.clone();
        }
        match p.strip_prefix(l) {
            Some([]) => RelPath::root(),
            Some([b'/', rest @ ..]) => RelPath::new(rest).expect("a suffix of a valid path"),
            _ => unreachable!("{path} is not beneath {}", self.link),
        }
    }

    /// The replica path of `rel`, a path relative to the base.
    fn full(&self, rel: &RelPath) -> Result<RelPath> {
        if self.link.is_root() {
            Ok(rel.clone())
        } else if rel.is_root() {
            Ok(self.link.clone())
        } else {
            self.link.join(rel.as_bytes())
        }
    }
}

/// Read access to a replica's tree, for resolving paths through followed
/// and adopted links.
struct Disk<'a> {
    root: &'a Root,
    config: &'a ReplicaConfig,
}

impl Disk<'_> {
    /// Whether changes beneath the followed directory `e` are written
    /// through its link (`-K`, design §4.3) rather than turning it into a
    /// real directory first.
    fn writes_through(&self, e: &Entry) -> bool {
        e.kind == Kind::Dir
            && self.config.keep_dirlinks
            && e.local
                .via_link
                .as_ref()
                .is_some_and(|v| !v.out_of_tree || self.config.keep_dirlinks_unsafe)
    }

    /// Resolves the ancestors of `path` as the index describes them: real
    /// directories, or followed and adopted ones, which are opened through
    /// their links and must still be the indexed directories. For `write`,
    /// a followed directory that is not written through ends the walk.
    fn route(&self, snap: &ReadTxn, path: &RelPath, write: bool) -> Result<Route> {
        let mut base = Base::top();
        let names: Vec<&[u8]> = path.components().collect();
        let mut a = RelPath::root();
        for name in &names[..names.len().saturating_sub(1)] {
            a = a.join(name)?;
            let Some(e) = snap.get(&a)?.filter(|e| e.kind == Kind::Dir) else {
                return Ok(Route::Refused("parent is not an indexed directory"));
            };
            if e.local.via_link.is_none() {
                continue;
            }
            if write && !self.writes_through(&e) {
                return Ok(Route::WriteBack { base, link: a });
            }
            let Some(fd) = self.open_followed(&base, &a, &e)? else {
                return Ok(Route::Refused("followed directory changed since scan"));
            };
            let shown = self.root.path().join(a.as_os_str());
            base = Base {
                root: Some(Root::at_fd(fd, shown)),
                link: a.clone(),
            };
        }
        Ok(Route::At(base))
    }

    /// Opens (`O_PATH`) the file or directory the followed link `e` at
    /// `path` (in `base`) points to, if it is still the indexed link and
    /// object.
    fn open_followed(&self, base: &Base, path: &RelPath, e: &Entry) -> Result<Option<OwnedFd>> {
        let Some(v) = &e.local.via_link else {
            return Ok(None);
        };
        let kind = match e.kind {
            Kind::File { .. } => FileKind::File,
            Kind::Dir => FileKind::Dir,
            _ => return Ok(None),
        };
        let rel = base.rel(path);
        let (parent, name) = base.root(self.root).resolve_parent(&rel)?;
        if !link_is(parent.as_fd(), name, v)? {
            return Ok(None);
        }
        let fd = match follow_at(parent.as_fd(), name, OFlags::PATH | OFlags::CLOEXEC) {
            Ok(fd) => fd,
            Err(Errno::NOENT | Errno::NOTDIR | Errno::LOOP) => return Ok(None),
            Err(err) => return Err(path_err(err, path, "follow symlink")),
        };
        let fp = Fingerprint::of_fd(fd.as_fd())?;
        Ok((fp.kind == kind && (fp.dev, fp.ino) == (e.local.dev, e.local.ino)).then_some(fd))
    }

    /// For an entry indexed through a followed or adopted link at `path`:
    /// the fingerprints of the link and of the object it points to, if both
    /// are still what the index says (the link the same inode with the same
    /// ctime, so the same target; the referent the indexed inode, and an
    /// unchanged one for a file).
    fn followed(
        &self,
        base: &Base,
        path: &RelPath,
        e: &Entry,
    ) -> Result<Option<(Fingerprint, Fingerprint)>> {
        let Some(v) = &e.local.via_link else {
            return Ok(None);
        };
        let rel = base.rel(path);
        let (parent, name) = base.root(self.root).resolve_parent(&rel)?;
        let link = match Fingerprint::at_opt(parent.as_fd(), name)? {
            Some(fp)
                if fp.kind == FileKind::Symlink && fp.ino == v.ino && fp.ctime_ns == v.ctime_ns =>
            {
                fp
            }
            _ => return Ok(None),
        };
        let fd = match follow_at(parent.as_fd(), name, OFlags::PATH | OFlags::CLOEXEC) {
            Ok(fd) => fd,
            Err(Errno::NOENT | Errno::NOTDIR | Errno::LOOP) => return Ok(None),
            Err(err) => return Err(path_err(err, path, "follow symlink")),
        };
        let r = Fingerprint::of_fd(fd.as_fd())?;
        let l = &e.local;
        let same = match e.kind {
            Kind::File { size, .. } => r.unchanged(&Fingerprint {
                dev: l.dev,
                ino: l.ino,
                size,
                mtime_ns: e.mtime_ns,
                ctime_ns: l.ctime_ns,
                mode: e.mode,
                kind: FileKind::File,
            }),
            Kind::Dir => r.kind == FileKind::Dir && (r.dev, r.ino) == (l.dev, l.ino),
            _ => false,
        };
        Ok(same.then_some((link, r)))
    }

    /// The physical state the indexed `entry` at `path` (in `base`)
    /// describes, for the commit's compare-and-swap; `None` if the object on
    /// disk is known not to match. For an entry indexed through a followed
    /// link, that is the link itself: the link is what a commit replaces,
    /// deletes or renames, never the object it points to.
    fn expected(&self, base: &Base, path: &RelPath, entry: &Entry) -> Result<Option<Expected>> {
        if entry.local.via_link.is_some() {
            return Ok(self
                .followed(base, path, entry)?
                .map(|(link, _)| link.into()));
        }
        let l = &entry.local;
        let indexed = |size, kind| Fingerprint {
            dev: l.dev,
            ino: l.ino,
            size,
            mtime_ns: entry.mtime_ns,
            ctime_ns: l.ctime_ns,
            mode: entry.mode,
            kind,
        };
        Ok(Some(match &entry.kind {
            Kind::File { size, hash } => Expected {
                fp: indexed(*size, FileKind::File),
                hash: Some(*hash),
                racy: l.racy,
            },
            // Only the inode and mode matter (`set_dir_mode`).
            Kind::Dir => indexed(0, FileKind::Dir).into(),
            // The index keeps a symlink's mtime from its last logical change, not
            // the one on disk. A symlink cannot change without a new ctime, so the
            // indexed (dev, ino, ctime) identify it; the rest comes from disk.
            Kind::Symlink { .. } => match base.root(self.root).stat(&base.rel(path)) {
                Ok(fp)
                    if fp.kind == FileKind::Symlink
                        && (fp.dev, fp.ino, fp.ctime_ns) == (l.dev, l.ino, l.ctime_ns) =>
                {
                    fp.into()
                }
                Ok(_) => return Ok(None),
                Err(e) if e.is_not_found() => return Ok(None),
                Err(e) => return Err(e),
            },
            Kind::Tombstone | Kind::Unmanaged(_) => return Ok(None),
        }))
    }

    /// A verified streaming read of the indexed file `e` at `path`, through
    /// any followed links on the way (and of a followed file, its referent).
    fn open_file(&self, snap: &ReadTxn, path: &RelPath, e: &Entry) -> Result<StableReader> {
        let Kind::File { size, hash } = e.kind else {
            return Err(invalid(path, "not a file"));
        };
        let (fd, recheck) = self.open_indexed(snap, path, e)?;
        StableReader::new(path.as_bytes(), fd, size, hash, recheck)
    }

    /// The indexed file `e` at `path`, pinned for reads at chosen offsets
    /// (delta transfer, design §7.1); `None` if it is not the indexed inode
    /// as indexed (ino, size, mtime, ctime) any more.
    fn open_pinned(&self, snap: &ReadTxn, path: &RelPath, e: &Entry) -> Result<Option<PinnedFile>> {
        let Kind::File { size, .. } = e.kind else {
            return Err(invalid(path, "not a file"));
        };
        let (fd, recheck) = self.open_indexed(snap, path, e)?;
        let file = PinnedFile::new(path.as_bytes(), fd, recheck)?;
        let l = &e.local;
        let indexed = Fingerprint {
            dev: l.dev,
            ino: l.ino,
            size,
            mtime_ns: e.mtime_ns,
            ctime_ns: l.ctime_ns,
            mode: e.mode,
            kind: FileKind::File,
        };
        Ok(file.fingerprint().unchanged(&indexed).then_some(file))
    }

    /// Opens the indexed file `e` at `path` for reading, through any
    /// followed links on the way (and of a followed file, its referent),
    /// with the check that it is still reachable that way at the end.
    fn open_indexed(
        &self,
        snap: &ReadTxn,
        path: &RelPath,
        e: &Entry,
    ) -> Result<(OwnedFd, Recheck)> {
        let base = match self.route(snap, path, false)? {
            Route::At(base) => base,
            Route::Refused(reason) => {
                return Err(Error::Unstable {
                    path: path.as_bytes().to_vec(),
                    reason,
                });
            }
            Route::WriteBack { .. } => unreachable!("reads follow every followed link"),
        };
        let root = base.root(self.root);
        let rel = base.rel(path);
        let (parent, name) = root.resolve_parent(&rel)?;
        match &e.local.via_link {
            None => open_checked(path.as_bytes(), parent, name),
            Some(link) => {
                let flags = OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC;
                let fd =
                    match root.open_referent(parent.as_fd(), &rel, name, flags | OFlags::NOATIME) {
                        Err(Errno::PERM) => root.open_referent(parent.as_fd(), &rel, name, flags),
                        res => res,
                    }
                    .map_err(|e| path_err(e, path, "open symlink referent"))?
                    .0;
                Ok((fd, link_recheck(parent, name, link.clone())))
            }
        }
    }
}

/// Whether `parent/name` is still the followed link `v` (same inode and
/// ctime, so the same target).
fn link_is(parent: BorrowedFd<'_>, name: &[u8], v: &LinkInfo) -> Result<bool> {
    Ok(Fingerprint::at_opt(parent, name)?.is_some_and(|fp| {
        fp.kind == FileKind::Symlink && fp.ino == v.ino && fp.ctime_ns == v.ctime_ns
    }))
}

/// The index entry at `path`, which must be of kind `expect` (a read of
/// content the peer saw); `Unstable` if it is not (any more).
fn indexed_as(snap: &ReadTxn, path: &RelPath, expect: &Kind) -> Result<Entry> {
    snap.get(path)?
        .filter(|cur| cur.kind == *expect)
        .ok_or_else(|| Error::Unstable {
            path: path.as_bytes().to_vec(),
            reason: "index entry differs from the expected entry",
        })
}

/// Whether `op` changes anything on disk at the path whose entry is `cur`.
/// The other ops only record (e.g. a merged version vector) after checking
/// the object still matches.
fn changes_disk(op: &Op, cur: Option<&Entry>) -> bool {
    match (op, cur) {
        (Op::SetMeta { mode, mtime_ns, .. }, Some(e)) => match e.kind {
            Kind::File { .. } => sync_mode(*mode) != e.mode || *mtime_ns != e.mtime_ns,
            Kind::Dir => sync_mode(*mode) != e.mode,
            _ => false,
        },
        _ => true,
    }
}

/// The logical precondition, against the index. `Some(reason)` if it fails.
/// (Where the path's parent is, and whether it can be written, is
/// [`Disk::route`]'s.)
fn check(cur: Option<&Entry>, pre: &Precondition) -> Option<&'static str> {
    match pre {
        Precondition::Absent => match cur {
            Some(e) if !e.is_tombstone() => Some("path is indexed"),
            _ => None,
        },
        Precondition::Matches { kind, vv } => match cur {
            // Never overwrite an unmanaged object (§3).
            Some(e) if e.is_unmanaged() => Some("unmanaged"),
            Some(e) if e.kind == *kind && e.vv == *vv => None,
            _ => Some("index entry differs"),
        },
    }
}

/// Whether `root` holds `replica`'s root marker, as a regular file.
fn has_root_marker(root: &Root, replica: ReplicaId) -> Result<bool> {
    match root.stat(&RelPath::new(tmpname::root_marker(replica))?) {
        Ok(fp) => Ok(fp.kind == FileKind::File),
        Err(e) if e.is_not_found() => Ok(false),
        Err(e) => Err(e),
    }
}

fn marker_missing(root: &Root, replica: ReplicaId, reason: String) -> Error {
    Error::RootMarkerMissing {
        path: root.path().to_owned(),
        marker: String::from_utf8_lossy(&tmpname::root_marker(replica)).into_owned(),
        reason,
    }
}

/// Fails with [`Error::RootMarkerMissing`] unless `root` holds `replica`'s
/// root marker (§5.1).
fn check_root_marker(root: &Root, replica: ReplicaId) -> Result<()> {
    if has_root_marker(root, replica)? {
        Ok(())
    } else {
        Err(marker_missing(
            root,
            replica,
            "its root marker is gone".into(),
        ))
    }
}

/// Makes sure `root` is `replica`'s root (design §5.1): it must hold the
/// replica's root marker once `index` has recorded one. An index that has
/// not (a new one, or one from before markers) gets the marker made, unless
/// the root is empty while the index lists live entries: that is what a disk
/// that is not mounted looks like, and syncing it would delete every one of
/// them on the peer.
fn ensure_root_marker(root: &Root, index: &IndexStore, replica: ReplicaId) -> Result<()> {
    if has_root_marker(root, replica)? {
        if !index.root_marked()? {
            index.set_root_marked()?;
        }
        return Ok(());
    }
    if index.root_marked()? {
        return Err(marker_missing(
            root,
            replica,
            "its root marker is missing".into(),
        ));
    }
    let live = index
        .iter_prefix(&RelPath::root())?
        .iter()
        .filter(|(p, e)| !p.is_root() && e.is_live())
        .count();
    let empty = root
        .read_dir(&RelPath::root())?
        .iter()
        .all(|e| tmpname::is_reserved(&e.name));
    if live > 0 && empty {
        return Err(marker_missing(
            root,
            replica,
            format!(
                "it is empty, but its index lists {live} live entries, and it has no root marker"
            ),
        ));
    }
    commit::create_root_marker(root, replica)?;
    // Something else may have had the name.
    if !has_root_marker(root, replica)? {
        return Err(marker_missing(
            root,
            replica,
            "its root marker is not a regular file".into(),
        ));
    }
    index.set_root_marked()?;
    tracing::info!(root = %root.path().display(), "created the root marker");
    Ok(())
}

fn log_recovered(intent: &Intent, rep: &Recovered) {
    if let Some(why) = rep.skipped {
        tracing::warn!(path = %intent.path, why, "cannot replay intent; its reserved names are left alone");
    } else if *rep != Recovered::default() {
        tracing::info!(path = %intent.path, op = ?intent.op, ?rep, "replayed intent");
    }
}

/// For a streaming read of a followed link's referent: the link must still
/// be the indexed one at EOF.
fn link_recheck(parent: OwnedFd, name: &[u8], link: LinkInfo) -> Recheck {
    let name = name.to_vec();
    Box::new(move |_| {
        Ok(match Fingerprint::at_opt(parent.as_fd(), &name)? {
            Some(fp)
                if fp.kind == FileKind::Symlink
                    && fp.ino == link.ino
                    && fp.ctime_ns == link.ctime_ns =>
            {
                None
            }
            Some(_) => Some("symlink replaced during read"),
            None => Some("symlink removed during read"),
        })
    })
}

/// `Ok(fingerprint)` for `Applied`, else the step to report.
fn applied(out: commit::Outcome) -> std::result::Result<Fingerprint, Step> {
    match out {
        commit::Outcome::Applied(fp) => Ok(fp),
        other => Err(not_done(other)),
    }
}

/// `Ok(())` for `Removed`, else the step to report.
fn removed(out: commit::Outcome) -> std::result::Result<(), Step> {
    match out {
        commit::Outcome::Removed => Ok(()),
        other => Err(not_done(other)),
    }
}

fn not_done(out: commit::Outcome) -> Step {
    match out {
        commit::Outcome::PreconditionFailed(reason) => Step::Failed(reason),
        commit::Outcome::Preserved { conflict } => Step::Preserved(conflict),
        commit::Outcome::Applied(_) | commit::Outcome::Removed => {
            unreachable!("commit outcome does not fit the operation")
        }
    }
}

fn entry(kind: Kind, mode: u32, mtime_ns: i64, vv: VersionVector, local: LocalMeta) -> Entry {
    Entry {
        kind,
        mode,
        mtime_ns,
        vv,
        seq: 0,
        local,
    }
}

/// A file we just committed. It is `racy`: its ctime is from the commit, so
/// the next scan rehashes it once (§3), finding no change.
fn file_entry(fp: &Fingerprint, hash: [u8; 32], vv: VersionVector) -> Entry {
    let kind = Kind::File {
        size: fp.size,
        hash,
    };
    let local = LocalMeta {
        racy: true,
        ..local_of(fp)
    };
    entry(kind, sync_mode(fp.mode), fp.mtime_ns, vv, local)
}

fn tombstone(vv: VersionVector) -> Entry {
    entry(Kind::Tombstone, 0, 0, vv, LocalMeta::default())
}

fn local_of(fp: &Fingerprint) -> LocalMeta {
    LocalMeta {
        dev: fp.dev,
        ino: fp.ino,
        ctime_ns: fp.ctime_ns,
        ..LocalMeta::default()
    }
}

fn invalid(path: &RelPath, reason: &'static str) -> Error {
    Error::InvalidOp {
        path: path.as_bytes().to_vec(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SymlinkPolicy;
    use crate::fs::{hooks, is_reserved, is_root_marker};
    use crate::index::{Ord4, UnmanagedReason};
    use std::fs;
    use std::io::Write;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt, symlink};
    use std::path::PathBuf;

    const ME: ReplicaId = ReplicaId(0x1111_2222_3333_4444);
    const PEER: ReplicaId = ReplicaId(0x9999_8888_7777_6666);
    const META: FileMeta = FileMeta {
        mode: 0o640,
        mtime_ns: 1_700_000_000_123_456_789,
    };
    const RACY: Duration = Duration::from_millis(200);

    struct Fx {
        dir: tempfile::TempDir,
        _state: tempfile::TempDir,
        r: LocalReplica,
    }

    impl Fx {
        fn new() -> Fx {
            Fx::with(SymlinkPolicy::Links, false)
        }

        fn with(symlinks: SymlinkPolicy, munge_links: bool) -> Fx {
            let dir = tempfile::tempdir().unwrap();
            let state = tempfile::tempdir().unwrap();
            let cfg = ReplicaConfig {
                id: ME,
                root: dir.path().to_path_buf(),
                symlinks,
                munge_links,
                keep_dirlinks: false,
                keep_dirlinks_unsafe: false,
                followed_write: Default::default(),
                trash_days: 0,
                device: None,
                remote: None,
            };
            let r = LocalReplica::open(&cfg, state.path())
                .unwrap()
                .racy_window(RACY)
                .quarantine_grace(Duration::ZERO);
            Fx {
                dir,
                _state: state,
                r,
            }
        }

        fn p(&self, rel: &str) -> PathBuf {
            self.dir.path().join(rel)
        }

        /// The stored entry, `LocalMeta` included.
        fn get(&self, rel: &str) -> Entry {
            self.r.index().get(&rp(rel)).unwrap().unwrap()
        }

        fn scan(&mut self) -> ScanStats {
            let stats = self.r.scan(Scope::Full).unwrap();
            assert!(
                stats.dirty.is_empty() && stats.errors.is_empty(),
                "{stats:?}"
            );
            stats
        }

        /// Scans after the racy window, so racy entries settle.
        fn settled_scan(&mut self) -> ScanStats {
            std::thread::sleep(RACY + Duration::from_millis(50));
            self.scan()
        }

        fn apply(&mut self, rel: &str, op: Op, pre: Precondition) -> Result<Outcome> {
            self.r.apply(&rp(rel), op, pre, None)
        }

        fn write(
            &mut self,
            rel: &str,
            data: &[u8],
            pre: Precondition,
            vv: VersionVector,
        ) -> Result<Outcome> {
            let op = Op::WriteFile {
                meta: META,
                hash: hash(data),
                vv,
            };
            self.r.apply(&rp(rel), op, pre, Some(&mut &data[..]))
        }

        /// Reserved names left anywhere under the root (but its root
        /// marker), after a sweep.
        fn leftovers(&mut self) -> Vec<PathBuf> {
            fn walk(dir: &Path, top: bool, out: &mut Vec<PathBuf>) {
                for e in fs::read_dir(dir).unwrap() {
                    let e = e.unwrap();
                    let name = e.file_name();
                    let marker = top && is_root_marker(name.as_encoded_bytes());
                    if is_reserved(name.as_encoded_bytes()) && !marker {
                        out.push(e.path());
                    }
                    if e.file_type().unwrap().is_dir() {
                        walk(&e.path(), false, out);
                    }
                }
            }
            self.r.sweep_quarantine();
            let mut out = Vec::new();
            walk(self.dir.path(), true, &mut out);
            out
        }
    }

    fn rp(s: &str) -> RelPath {
        RelPath::new(s).unwrap()
    }

    fn hash(data: &[u8]) -> [u8; 32] {
        *blake3::hash(data).as_bytes()
    }

    fn peer(n: u64) -> VersionVector {
        let mut vv = VersionVector::new();
        vv.set(PEER, n);
        vv
    }

    fn applied(out: Result<Outcome>) -> Entry {
        match out {
            Ok(Outcome::Applied(e)) => e,
            other => panic!("expected Applied, got {other:?}"),
        }
    }

    fn failed(out: Result<Outcome>) -> Option<Entry> {
        match out {
            Ok(Outcome::PreconditionFailed(e)) => e,
            other => panic!("expected PreconditionFailed, got {other:?}"),
        }
    }

    fn invalid_op(out: Result<Outcome>) {
        assert!(matches!(out, Err(Error::InvalidOp { .. })), "{out:?}");
    }

    fn mtime_ns(md: &fs::Metadata) -> i64 {
        md.mtime() * 1_000_000_000 + md.mtime_nsec()
    }

    fn mode(md: &fs::Metadata) -> u32 {
        md.permissions().mode() & 0o7777
    }

    /// A user file with an old mtime, so any later write changes it.
    fn user_file(fx: &Fx, rel: &str, data: &[u8]) {
        fs::write(fx.p(rel), data).unwrap();
        let f = fs::File::options().write(true).open(fx.p(rel)).unwrap();
        f.set_modified(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000))
            .unwrap();
        fs::set_permissions(fx.p(rel), fs::Permissions::from_mode(0o644)).unwrap();
    }

    fn append(path: &Path, data: &[u8]) {
        fs::File::options()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(data)
            .unwrap();
    }

    fn config(root: &Path) -> ReplicaConfig {
        ReplicaConfig {
            id: ME,
            root: root.to_path_buf(),
            symlinks: SymlinkPolicy::Links,
            munge_links: false,
            keep_dirlinks: false,
            keep_dirlinks_unsafe: false,
            followed_write: Default::default(),
            trash_days: 0,
            device: None,
            remote: None,
        }
    }

    fn marker_path(root: &Path) -> PathBuf {
        root.join(std::ffi::OsStr::from_bytes(&tmpname::root_marker(ME)))
    }

    /// Issue #1: the root marker is made at the first open; once it is gone
    /// (the root replaced by an empty directory, e.g. a disk that is not
    /// mounted), neither a scan nor a new open runs, and nothing is indexed
    /// as deleted.
    #[test]
    fn root_marker_is_made_then_required() {
        let mut fx = Fx::new();
        let marker = marker_path(fx.dir.path());
        assert!(marker.is_file());
        assert!(fx.r.index().root_marked().unwrap());
        fs::write(fx.p("f"), "x").unwrap();
        fx.scan();

        fs::rename(fx.p("f"), fx._state.path().join("f")).unwrap();
        fs::remove_file(&marker).unwrap();
        let err = fx.r.scan(Scope::Full).unwrap_err();
        assert!(matches!(err, Error::RootMarkerMissing { .. }), "{err}");
        assert!(fx.get("f").is_live(), "a refused scan indexes nothing");

        let cfg = fx.r.config().clone();
        drop(fx.r);
        let err = LocalReplica::open(&cfg, fx._state.path()).err().unwrap();
        assert!(matches!(err, Error::RootMarkerMissing { .. }), "{err}");
        assert!(err.to_string().contains("is missing"), "{err}");
        assert!(!marker.exists(), "a refused open makes no marker");

        // The user says it is the right directory.
        fs::write(&marker, "").unwrap();
        let mut r = LocalReplica::open(&cfg, fx._state.path()).unwrap();
        r.scan(Scope::Full).unwrap();
        assert!(r.index().get(&rp("f")).unwrap().unwrap().is_tombstone());
    }

    /// A marker that is not a regular file is no marker.
    #[test]
    fn root_marker_must_be_a_file() {
        let (dir, state) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        fs::create_dir(marker_path(dir.path())).unwrap();
        let err = LocalReplica::open(&config(dir.path()), state.path())
            .err()
            .unwrap();
        assert!(err.to_string().contains("not a regular file"), "{err}");
    }

    /// An index from before root markers gets one made, unless the root is
    /// empty while the index lists live entries.
    #[test]
    fn legacy_index_gets_a_marker_unless_the_root_is_empty() {
        let (dir, state) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        fs::create_dir(dir.path().join("d")).unwrap();
        fs::write(dir.path().join("d/f"), "x").unwrap();
        {
            let root = Root::open(dir.path()).unwrap();
            let index = IndexStore::open(&IndexStore::path_for(state.path(), ME), ME).unwrap();
            Scanner::new(&root, &index, SymlinkPolicy::Links)
                .scan(&Scope::Full)
                .unwrap();
            assert!(!index.root_marked().unwrap());
        }
        let away = state.path().join("d");
        fs::rename(dir.path().join("d"), &away).unwrap();
        let cfg = config(dir.path());
        let err = LocalReplica::open(&cfg, state.path()).err().unwrap();
        assert!(matches!(err, Error::RootMarkerMissing { .. }), "{err}");
        assert!(err.to_string().contains("it is empty"), "{err}");
        assert!(!marker_path(dir.path()).exists());

        fs::rename(&away, dir.path().join("d")).unwrap();
        let r = LocalReplica::open(&cfg, state.path()).unwrap();
        assert!(marker_path(dir.path()).is_file());
        assert!(r.index().root_marked().unwrap());
    }

    #[test]
    fn write_file_creates_and_replaces() {
        let mut fx = Fx::new();
        let e = applied(fx.write("f", b"hello", Precondition::Absent, peer(1)));
        let file = Kind::File {
            size: 5,
            hash: hash(b"hello"),
        };
        assert_eq!(
            (&e.kind, &e.vv, e.mode, e.mtime_ns),
            (&file, &peer(1), 0o640, META.mtime_ns)
        );
        assert_eq!(e.local, LocalMeta::default(), "LocalMeta is not returned");
        let md = fs::symlink_metadata(fx.p("f")).unwrap();
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"hello");
        assert_eq!((mode(&md), mtime_ns(&md)), (0o640, META.mtime_ns));
        let stored = fx.get("f");
        assert_eq!(
            (stored.seq, stored.local.ino, stored.local.racy),
            (e.seq, md.ino(), true)
        );
        assert_eq!(
            stored.local.ctime_ns,
            md.ctime() * 1_000_000_000 + md.ctime_nsec()
        );

        // No echo: the scan finds the index up to date.
        let stats = fx.scan();
        assert_eq!(
            (stats.changed, stats.hashed),
            (0, 1),
            "racy entry rehashed once"
        );
        // Still within the racy window of that scan; settled after it.
        assert_eq!(fx.settled_scan().changed, 0);
        assert_eq!(fx.settled_scan().hashed, 0);
        assert_eq!(fx.get("f").seq, e.seq);

        // Replace, with the precondition the engine would send.
        let pre = Precondition::matching(&e);
        let e2 = applied(fx.write("f", b"world!", pre, peer(2)));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"world!");
        assert_ne!(fs::symlink_metadata(fx.p("f")).unwrap().ino(), md.ino());
        assert_eq!(e2.vv, peer(2));
        assert!(e2.seq > e.seq);
        assert_eq!(fx.r.quarantine().len(), 1);

        // File → symlink → file, each in one exchange.
        let op = Op::Symlink {
            target: b"t".to_vec(),
            mtime_ns: 5,
            vv: peer(3),
        };
        let e3 = applied(fx.apply("f", op, Precondition::matching(&e2)));
        assert_eq!(
            e3.kind,
            Kind::Symlink {
                target: b"t".to_vec()
            }
        );
        assert_eq!(e3.mtime_ns, 5);
        assert_eq!(fs::read_link(fx.p("f")).unwrap(), Path::new("t"));
        let e4 = applied(fx.write("f", b"file again", Precondition::matching(&e3), peer(4)));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"file again");
        assert_eq!(fx.settled_scan().changed, 0);
        assert_eq!(fx.get("f").seq, e4.seq);
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn stale_precondition_fails() {
        let mut fx = Fx::new();
        user_file(&fx, "f", b"user data");
        fx.scan();
        let e = fx.get("f");
        let stale_vv = Precondition::Matches {
            kind: e.kind.clone(),
            vv: peer(9),
        };
        let stale_kind = Precondition::Matches {
            kind: Kind::File {
                size: 9,
                hash: hash(b"something"),
            },
            vv: e.vv.clone(),
        };
        for pre in [stale_vv, stale_kind, Precondition::Absent] {
            let cur = failed(fx.write("f", b"ours", pre.clone(), peer(1)));
            assert_eq!(cur, Some(wire(e.clone())));
            let cur = failed(fx.apply("f", Op::Delete { vv: peer(1) }, pre));
            assert_eq!(cur, Some(wire(e.clone())));
        }

        // The index still matches, but the file changed since the scan: the
        // commit's fingerprint check catches it.
        append(&fx.p("f"), b" edited");
        let pre = Precondition::matching(&e);
        failed(fx.write("f", b"ours", pre.clone(), peer(1)));
        failed(fx.apply("f", Op::Delete { vv: peer(1) }, pre.clone()));
        let to = Op::RenameToConflict {
            to: rp("f.conflict"),
        };
        failed(fx.apply("f", to, pre.clone()));
        let set = Op::SetMeta {
            mode: 0o600,
            mtime_ns: 1,
            vv: peer(1),
        };
        failed(fx.apply("f", set, pre));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"user data edited");
        assert_eq!(fx.get("f"), e, "index untouched");

        // Modified while we commit (between the check and the exchange).
        // Without a lease, which would make the hook's open wait for the
        // lease break time (fs::commit tests that case).
        fx.r.caps_mut().leases = false;
        fx.scan();
        let e = fx.get("f");
        let user = fx.p("f");
        let _g = hooks::once("replace.before_exchange", move || append(&user, b" again"));
        let cur = failed(fx.write("f", b"ours", Precondition::matching(&e), peer(1)));
        assert_eq!(cur, Some(wire(e.clone())));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"user data edited again");
        assert_eq!(fx.get("f"), e);

        // On disk but not yet indexed: the create's RENAME_NOREPLACE refuses.
        fs::write(fx.p("g"), b"new user file").unwrap();
        assert_eq!(
            failed(fx.write("g", b"ours", Precondition::Absent, peer(1))),
            None
        );
        assert_eq!(fs::read(fx.p("g")).unwrap(), b"new user file");

        // Unmanaged objects are never touched.
        rustix::fs::mknodat(
            rustix::fs::CWD,
            fx.p("fifo").as_path(),
            rustix::fs::FileType::Fifo,
            rustix::fs::Mode::from_raw_mode(0o644),
            0,
        )
        .unwrap();
        fx.scan();
        let fifo = fx.get("fifo");
        assert_eq!(fifo.kind, Kind::Unmanaged(UnmanagedReason::Special));
        failed(fx.write("fifo", b"ours", Precondition::matching(&fifo), peer(1)));
        assert!(
            fs::symlink_metadata(fx.p("fifo"))
                .unwrap()
                .file_type()
                .is_fifo()
        );
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn mkdir_nested_writes_delete_and_rmdir() {
        let mut fx = Fx::new();
        // The parent must be an indexed directory.
        assert_eq!(
            failed(fx.write("d/f", b"x", Precondition::Absent, peer(1))),
            None
        );
        assert!(!fx.p("d").exists());

        let mkdir = Op::Mkdir {
            mode: 0o750,
            mtime_ns: 42,
            vv: peer(1),
        };
        let d = applied(fx.apply("d", mkdir, Precondition::Absent));
        assert_eq!(
            (&d.kind, d.mode, d.mtime_ns, &d.vv),
            (&Kind::Dir, 0o750, 42, &peer(1))
        );
        assert_eq!(mode(&fs::symlink_metadata(fx.p("d")).unwrap()), 0o750);
        let f = applied(fx.write("d/f", b"child", Precondition::Absent, peer(2)));

        // Mkdir only creates.
        let again = Op::Mkdir {
            mode: 0o750,
            mtime_ns: 42,
            vv: peer(3),
        };
        invalid_op(fx.apply("d", again, Precondition::matching(&d)));

        // Children first (§5.7), in the index too.
        failed(fx.apply("d", Op::Rmdir { vv: peer(3) }, Precondition::matching(&d)));
        assert!(fx.p("d/f").exists());
        let gone = applied(fx.apply(
            "d/f",
            Op::Delete { vv: peer(3) },
            Precondition::matching(&f),
        ));
        assert_eq!((&gone.kind, &gone.vv), (&Kind::Tombstone, &peer(3)));
        assert!(!fx.p("d/f").exists());
        assert_eq!(fx.r.quarantine().len(), 1);
        let gone = applied(fx.apply("d", Op::Rmdir { vv: peer(4) }, Precondition::matching(&d)));
        assert_eq!((&gone.kind, &gone.vv), (&Kind::Tombstone, &peer(4)));
        assert!(!fx.p("d").exists());
        assert!(fx.r.quarantine().is_empty(), "settled by rmdir");
        assert_eq!(fx.scan().changed, 0);

        // A tombstone can be expected explicitly, too.
        let mkdir = Op::Mkdir {
            mode: 0o700,
            mtime_ns: 43,
            vv: peer(5),
        };
        applied(fx.apply("d", mkdir, Precondition::matching(&gone)));
        assert!(fx.p("d").is_dir());
        assert_eq!(fx.settled_scan().changed, 0);
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn symlinks_with_munging() {
        let mut fx = Fx::with(SymlinkPolicy::Links, true);
        let op = Op::Symlink {
            target: b"../escape".to_vec(),
            mtime_ns: 7,
            vv: peer(1),
        };
        let e = applied(fx.apply("l", op, Precondition::Absent));
        let munged = munge(b"../escape");
        assert_eq!(
            e.kind,
            Kind::Symlink {
                target: b"../escape".to_vec()
            }
        );
        assert_eq!(
            fs::read_link(fx.p("l"))
                .unwrap()
                .as_os_str()
                .as_encoded_bytes(),
            munged.as_slice()
        );
        assert_eq!(fx.get("l").local.raw_target, Some(munged));
        assert_eq!(fx.scan().changed, 0);

        let op = Op::Symlink {
            target: b"other".to_vec(),
            mtime_ns: 8,
            vv: peer(2),
        };
        let e = applied(fx.apply("l", op, Precondition::matching(&e)));
        assert_eq!(
            fs::read_link(fx.p("l")).unwrap(),
            Path::new("/rsyncd-munged/other")
        );
        assert_eq!(fx.scan().changed, 0);

        applied(fx.apply("l", Op::Delete { vv: peer(3) }, Precondition::matching(&e)));
        assert!(fs::symlink_metadata(fx.p("l")).is_err());
        assert_eq!(fx.scan().changed, 0);
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn rename_to_conflict() {
        let mut fx = Fx::new();
        user_file(&fx, "f.txt", b"loser");
        fx.scan();
        let e = fx.get("f.txt");
        let to = rp("f.sync-conflict-20260101-000000-1111222.txt");

        // Not a sibling.
        fs::create_dir(fx.p("d")).unwrap();
        let op = Op::RenameToConflict { to: rp("d/x") };
        let out = fx.apply("f.txt", op, Precondition::matching(&e));
        assert!(matches!(out, Err(Error::InvalidPath { .. })), "{out:?}");
        fs::remove_dir(fx.p("d")).unwrap();

        // The new name is taken on disk.
        fs::write(fx.p(to.as_os_str().to_str().unwrap()), b"user's").unwrap();
        let op = Op::RenameToConflict { to: to.clone() };
        failed(fx.apply("f.txt", op.clone(), Precondition::matching(&e)));
        assert_eq!(fs::read(fx.p("f.txt")).unwrap(), b"loser");
        fs::remove_file(fx.p(to.as_os_str().to_str().unwrap())).unwrap();

        let gone = applied(fx.apply("f.txt", op, Precondition::matching(&e)));
        assert_eq!(gone.kind, Kind::Tombstone);
        assert_eq!(gone.vv.compare(&e.vv), Ord4::Dominates);
        assert!(!fx.p("f.txt").exists());
        let copy_path = fx.p(to.as_os_str().to_str().unwrap());
        assert_eq!(fs::read(&copy_path).unwrap(), b"loser");
        assert_eq!(fs::symlink_metadata(&copy_path).unwrap().ino(), e.local.ino);
        let copy = fx.r.index().get(&to).unwrap().unwrap();
        assert_eq!(
            (&copy.kind, copy.mode, copy.mtime_ns),
            (&e.kind, e.mode, e.mtime_ns)
        );
        let mut fresh = VersionVector::new();
        fresh.set(ME, gone.vv.get(ME) + 1);
        assert_eq!(copy.vv, fresh);
        // Both show up as changes for the peer.
        let changes = fx.r.changes_since(e.seq).unwrap();
        let paths: Vec<_> = changes.iter().map(|(p, _)| p.clone()).collect();
        assert_eq!(paths, [rp("f.txt"), to.clone()]);

        // Then the winner propagates as a create.
        applied(fx.write("f.txt", b"winner", Precondition::Absent, peer(7)));
        assert_eq!(fs::read(fx.p("f.txt")).unwrap(), b"winner");
        assert_eq!(fx.settled_scan().changed, 0);
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn set_meta() {
        let mut fx = Fx::new();
        user_file(&fx, "f", b"same content");
        fs::create_dir(fx.p("d")).unwrap();
        fs::set_permissions(fx.p("d"), fs::Permissions::from_mode(0o755)).unwrap();
        symlink("t", fx.p("l")).unwrap();
        fx.settled_scan();

        // A file is rewritten as a copy with the new mode and mtime.
        let e = fx.get("f");
        let op = Op::SetMeta {
            mode: 0o600,
            mtime_ns: 99,
            vv: peer(1),
        };
        let e2 = applied(fx.apply("f", op, Precondition::matching(&e)));
        let md = fs::symlink_metadata(fx.p("f")).unwrap();
        assert_eq!((mode(&md), mtime_ns(&md)), (0o600, 99));
        assert_ne!(md.ino(), e.local.ino);
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"same content");
        assert_eq!(
            (&e2.kind, e2.mode, e2.mtime_ns, &e2.vv),
            (&e.kind, 0o600, 99, &peer(1))
        );

        // Same metadata: only the index changes (a merged version vector).
        let mut merged = peer(1);
        merged.set(ME, 5);
        let op = Op::SetMeta {
            mode: 0o600,
            mtime_ns: 99,
            vv: merged.clone(),
        };
        let e3 = applied(fx.apply("f", op, Precondition::matching(&e2)));
        assert_eq!(e3.vv, merged);
        assert_eq!(fs::symlink_metadata(fx.p("f")).unwrap().ino(), md.ino());

        // A directory is chmodded in place; its mtime alone is index-only.
        let d = fx.get("d");
        let op = Op::SetMeta {
            mode: 0o700,
            mtime_ns: 1,
            vv: peer(2),
        };
        let d2 = applied(fx.apply("d", op, Precondition::matching(&d)));
        let dmd = fs::symlink_metadata(fx.p("d")).unwrap();
        assert_eq!((mode(&dmd), dmd.ino()), (0o700, d.local.ino));
        assert_eq!((d2.mode, d2.mtime_ns), (0o700, 1));
        let op = Op::SetMeta {
            mode: 0o700,
            mtime_ns: 2,
            vv: peer(3),
        };
        let d3 = applied(fx.apply("d", op, Precondition::matching(&d2)));
        assert_eq!((d3.mode, d3.mtime_ns, &d3.vv), (0o700, 2, &peer(3)));

        // A symlink: index only.
        let l = fx.get("l");
        let op = Op::SetMeta {
            mode: 0o777,
            mtime_ns: 3,
            vv: peer(4),
        };
        let l2 = applied(fx.apply("l", op, Precondition::matching(&l)));
        assert_eq!((l2.mtime_ns, &l2.vv), (3, &peer(4)));
        assert_eq!(fs::symlink_metadata(fx.p("l")).unwrap().ino(), l.local.ino);

        // A user chmod since the scan.
        fs::set_permissions(fx.p("d"), fs::Permissions::from_mode(0o750)).unwrap();
        let op = Op::SetMeta {
            mode: 0o755,
            mtime_ns: 2,
            vv: peer(5),
        };
        failed(fx.apply("d", op, Precondition::matching(&fx.get("d"))));
        assert_eq!(mode(&fs::symlink_metadata(fx.p("d")).unwrap()), 0o750);

        assert_eq!(fx.settled_scan().changed, 1, "only the user's chmod");
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn open_read_streams_and_verifies() {
        let mut fx = Fx::new();
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        user_file(&fx, "f", &data);
        fx.scan();
        let e = wire(fx.get("f"));
        let mut got = Vec::new();
        fx.r.open_read(&rp("f"), &e)
            .unwrap()
            .read_to_end(&mut got)
            .unwrap();
        assert_eq!(got, data);

        // The engine's entry is not ours.
        let mut other = e.clone();
        other.kind = Kind::File {
            size: 3,
            hash: hash(b"abc"),
        };
        let res = fx.r.open_read(&rp("f"), &other);
        assert!(
            res.as_ref().is_err_and(Error::is_unstable),
            "{:?}",
            res.err()
        );
        let mut dir = e.clone();
        dir.kind = Kind::Dir;
        let res = fx.r.open_read(&rp("f"), &dir);
        assert!(
            matches!(res, Err(Error::InvalidOp { .. })),
            "{:?}",
            res.err()
        );

        // Rewritten (same size) during the read: EOF fails.
        let mut r = fx.r.open_read(&rp("f"), &e).unwrap();
        let mut buf = vec![0; 1000];
        r.read_exact(&mut buf).unwrap();
        let held = fs::File::options().write(true).open(fx.p("f")).unwrap();
        std::os::unix::fs::FileExt::write_all_at(&held, b"XYZ", 0).unwrap();
        let err = r.read_to_end(&mut Vec::new()).unwrap_err();
        let err = Error::from_stream("read", err);
        assert!(err.is_unstable(), "{err}");
        // ... and stays failed.
        assert!(r.read(&mut buf).is_err());

        // Changed since the scan: the size no longer matches.
        append(&fx.p("f"), b"more");
        let res = fx.r.open_read(&rp("f"), &e);
        assert!(
            res.as_ref().is_err_and(Error::is_unstable),
            "{:?}",
            res.err()
        );

        // A content stream that fails makes the apply fail, committing nothing.
        fx.scan();
        let e = wire(fx.get("f"));
        let mut src = fx.r.open_read(&rp("f"), &e).unwrap();
        append(&fx.p("f"), b"!");
        let op = Op::WriteFile {
            meta: META,
            hash: match e.kind {
                Kind::File { hash, .. } => hash,
                _ => unreachable!(),
            },
            vv: peer(1),
        };
        let out =
            fx.r.apply(&rp("copy"), op, Precondition::Absent, Some(&mut src));
        assert!(out.as_ref().is_err_and(Error::is_unstable), "{out:?}");
        assert!(!fx.p("copy").exists());
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn followed_file_is_read_and_written_back() {
        let mut fx = Fx::with(SymlinkPolicy::CopyLinks, false);
        user_file(&fx, "target", b"referent");
        symlink("target", fx.p("l")).unwrap();
        fx.scan();
        let e = fx.get("l");
        assert!(e.local.via_link.is_some());
        let mut got = Vec::new();
        fx.r.open_read(&rp("l"), &wire(e.clone()))
            .unwrap()
            .read_to_end(&mut got)
            .unwrap();
        assert_eq!(got, b"referent");

        // Retargeted during the read.
        let mut r = fx.r.open_read(&rp("l"), &wire(e.clone())).unwrap();
        fs::remove_file(fx.p("l")).unwrap();
        symlink("target", fx.p("l")).unwrap();
        let err = Error::from_stream("read", r.read_to_end(&mut Vec::new()).unwrap_err());
        assert!(err.is_unstable(), "{err}");

        // A stale link (retargeted since the scan) is not replaced.
        let e = fx.get("l");
        fs::remove_file(fx.p("l")).unwrap();
        symlink("target", fx.p("l")).unwrap();
        failed(fx.write("l", b"ours", Precondition::matching(&e), peer(1)));
        // Nor one whose referent changed since the scan.
        fx.scan();
        let e = fx.get("l");
        append(&fx.p("target"), b"+");
        failed(fx.write("l", b"ours", Precondition::matching(&e), peer(1)));

        // A metadata change replaces the link with a real copy (§4.3), and
        // so does a write. The referent is never written.
        fx.settled_scan();
        let e = fx.get("l");
        let op = Op::SetMeta {
            mode: 0o600,
            mtime_ns: 7,
            vv: peer(2),
        };
        let e2 = applied(fx.apply("l", op, Precondition::matching(&e)));
        let md = fs::symlink_metadata(fx.p("l")).unwrap();
        assert!(md.is_file());
        assert_eq!((mode(&md), mtime_ns(&md)), (0o600, 7));
        assert_eq!(fs::read(fx.p("l")).unwrap(), b"referent+");
        assert_eq!(fx.get("l").local.via_link, None);
        assert_eq!(fx.settled_scan().changed, 0);
        symlink("target", fx.p("l2")).unwrap();
        fx.scan();
        let e3 = fx.get("l2");
        applied(fx.write("l2", b"theirs", Precondition::matching(&e3), peer(3)));
        assert_eq!(fs::read(fx.p("l2")).unwrap(), b"theirs");
        assert!(!fs::symlink_metadata(fx.p("l2")).unwrap().is_symlink());
        assert_eq!(fs::read(fx.p("target")).unwrap(), b"referent+");
        assert_eq!(fx.settled_scan().changed, 0);

        // A delete removes the link, a conflict rename moves it.
        symlink("target", fx.p("l3")).unwrap();
        symlink("target", fx.p("l4")).unwrap();
        fx.scan();
        let (e3, e4) = (fx.get("l3"), fx.get("l4"));
        applied(fx.apply(
            "l3",
            Op::Delete { vv: peer(4) },
            Precondition::matching(&e3),
        ));
        assert!(!fx.p("l3").exists());
        let to = rp("l4.conflict");
        let op = Op::RenameToConflict { to: to.clone() };
        applied(fx.apply("l4", op, Precondition::matching(&e4)));
        assert!(
            fs::symlink_metadata(fx.p("l4.conflict"))
                .unwrap()
                .is_symlink()
        );
        assert_eq!(fs::read(fx.p("target")).unwrap(), b"referent+");
        assert_eq!(fx.settled_scan().changed, 0, "the moved link is no change");
        let _ = e2;
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn followed_directory_is_turned_into_a_real_one() {
        let mut fx = Fx::with(SymlinkPolicy::CopyDirlinks, false);
        fs::create_dir_all(fx.p("d/sub")).unwrap();
        user_file(&fx, "d/x", b"x");
        user_file(&fx, "d/sub/y", b"y");
        fs::set_permissions(fx.p("d/sub"), fs::Permissions::from_mode(0o550)).unwrap();
        symlink("x", fx.p("d/lx")).unwrap();
        symlink("d", fx.p("l")).unwrap();
        fx.settled_scan();
        let before = |p: &str| fx.get(p);
        let (l, lx, ly) = (before("l"), before("l/x"), before("l/sub/y"));
        assert!(l.local.via_link.is_some());
        // Read through the followed directory.
        let mut got = Vec::new();
        fx.r.open_read(&rp("l/sub/y"), &wire(ly.clone()))
            .unwrap()
            .read_to_end(&mut got)
            .unwrap();
        assert_eq!(got, b"y");

        // A write beneath it: the link becomes a real copy first.
        applied(fx.write("l/x", b"new x", Precondition::matching(&lx), peer(1)));
        let md = fs::symlink_metadata(fx.p("l")).unwrap();
        assert!(md.is_dir());
        assert_eq!(fs::read(fx.p("l/x")).unwrap(), b"new x");
        assert_eq!(fs::read(fx.p("l/sub/y")).unwrap(), b"y");
        assert_eq!(mode(&fs::metadata(fx.p("l/sub")).unwrap()), 0o550);
        assert_eq!(fs::read_link(fx.p("l/lx")).unwrap(), Path::new("x"));
        // The directory it pointed to is untouched.
        assert_eq!(fs::read(fx.p("d/x")).unwrap(), b"x");
        // The copy is the same tree for the peer: nothing to bump.
        assert_eq!(fx.get("l").vv, l.vv);
        assert_eq!(fx.get("l").local.via_link, None);
        assert_eq!(fx.get("l/sub/y").vv, ly.vv);
        assert_eq!(fx.settled_scan().changed, 0);
        assert!(fx.leftovers().is_empty());

        // A referent file changed after the scan: nothing is committed.
        fs::set_permissions(fx.p("d/sub"), fs::Permissions::from_mode(0o750)).unwrap();
        symlink("d", fx.p("m")).unwrap();
        fx.settled_scan();
        let my = fx.get("m/sub/y");
        let path = fx.p("d/sub/y");
        let _g = hooks::once("materialize.copied", move || append(&path, b"+"));
        let out = fx.write(
            "m/x",
            b"new",
            Precondition::matching(&fx.get("m/x")),
            peer(2),
        );
        assert!(
            matches!(out, Ok(Outcome::PreconditionFailed(_)))
                || out.as_ref().is_err_and(Error::is_unstable),
            "{out:?}"
        );
        assert!(fs::symlink_metadata(fx.p("m")).unwrap().is_symlink());
        assert_eq!(fx.get("m/sub/y"), my);
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn followed_write_conflict_keeps_the_link() {
        let mut fx = Fx::with(SymlinkPolicy::CopyLinks, false);
        fx.r.config.followed_write = FollowedWrite::Conflict;
        user_file(&fx, "target", b"referent");
        fs::create_dir(fx.p("d")).unwrap();
        user_file(&fx, "d/x", b"x");
        symlink("target", fx.p("l")).unwrap();
        symlink("d", fx.p("ld")).unwrap();
        fx.scan();
        let e = fx.get("l");

        // The incoming file is kept beside the link; ours dominates both.
        let out = applied(fx.write("l", b"theirs", Precondition::matching(&e), peer(5)));
        assert!(fs::symlink_metadata(fx.p("l")).unwrap().is_symlink());
        assert_eq!(fs::read(fx.p("target")).unwrap(), b"referent");
        assert_eq!(out.vv.compare(&peer(5)), Ord4::Dominates);
        assert_eq!(out.vv.compare(&e.vv), Ord4::Dominates);
        let copies: Vec<_> = fs::read_dir(fx.dir.path())
            .unwrap()
            .map(|d| d.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.starts_with("l.sync-conflict-"))
            .collect();
        assert_eq!(copies.len(), 1, "{copies:?}");
        assert!(copies[0].ends_with(&PEER.to_string()[..7]), "{copies:?}");
        assert_eq!(fs::read(fx.p(&copies[0])).unwrap(), b"theirs");

        // Beneath a followed directory: declined, kept beside the link.
        let ex = fx.get("ld/x");
        let out = applied(fx.write("ld/x", b"new x", Precondition::matching(&ex), peer(6)));
        assert_eq!(out.vv.compare(&peer(6)), Ord4::Dominates);
        assert!(fs::symlink_metadata(fx.p("ld")).unwrap().is_symlink());
        assert_eq!(fs::read(fx.p("d/x")).unwrap(), b"x");
        let copies = fs::read_dir(fx.dir.path())
            .unwrap()
            .filter(|d| {
                d.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("x.sync-conflict-")
            })
            .count();
        assert_eq!(copies, 1);
        // A delete is declined too.
        let e = fx.get("l");
        let out = applied(fx.apply("l", Op::Delete { vv: peer(9) }, Precondition::matching(&e)));
        assert_eq!(out.vv.compare(&peer(9)), Ord4::Dominates);
        assert!(fs::symlink_metadata(fx.p("l")).unwrap().is_symlink());
        assert_eq!(fx.settled_scan().changed, 0);
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn keep_dirlinks_adopts_and_writes_through() {
        let outside = tempfile::tempdir().unwrap();
        let mut fx = Fx::new();
        fx.r.config.keep_dirlinks = true;
        fs::create_dir(fx.p("real")).unwrap();
        user_file(&fx, "real/y", b"y");
        symlink("real", fx.p("k")).unwrap();
        symlink(outside.path(), fx.p("out")).unwrap();
        user_file(&fx, "f", b"f");
        symlink("f", fx.p("lf")).unwrap();
        fx.scan();
        let k = fx.get("k");
        assert!(matches!(k.kind, Kind::Symlink { .. }));

        // Only links to directories, and out of tree only when unsafe is on.
        assert!(!fx.r.adopt(&rp("lf")).unwrap());
        assert!(!fx.r.adopt(&rp("out")).unwrap());
        assert!(!fx.r.adopt(&rp("f")).unwrap());
        assert!(fx.r.adopt(&rp("k")).unwrap());
        let k2 = fx.get("k");
        assert_eq!(k2.kind, Kind::Dir);
        assert!(k2.local.via_link.as_ref().unwrap().adopted);
        assert_eq!(k2.vv.compare(&k.vv), Ord4::Dominates, "a local change");
        assert!(matches!(fx.get("k/y").kind, Kind::File { .. }));
        // It stays adopted.
        assert_eq!(fx.settled_scan().changed, 0);
        assert!(fx.r.adopt(&rp("k")).unwrap());

        // Writes beneath it go through the link.
        applied(fx.write("k/new", b"n", Precondition::Absent, peer(1)));
        assert_eq!(fs::read(fx.p("real/new")).unwrap(), b"n");
        assert!(fs::symlink_metadata(fx.p("k")).unwrap().is_symlink());
        let ky = fx.get("k/y");
        applied(fx.apply(
            "k/y",
            Op::Delete { vv: peer(2) },
            Precondition::matching(&ky),
        ));
        assert!(!fx.p("real/y").exists());
        // A mode change goes to the directory.
        let k = fx.get("k");
        let op = Op::SetMeta {
            mode: 0o700,
            mtime_ns: k.mtime_ns,
            vv: peer(3),
        };
        applied(fx.apply("k", op, Precondition::matching(&k)));
        assert_eq!(mode(&fs::metadata(fx.p("real")).unwrap()), 0o700);
        assert!(fx.leftovers().is_empty());

        // Retargeted to another directory: no longer adopted.
        fs::create_dir(fx.p("other")).unwrap();
        fs::remove_file(fx.p("k")).unwrap();
        symlink("other", fx.p("k")).unwrap();
        fx.settled_scan();
        assert!(matches!(fx.get("k").kind, Kind::Symlink { .. }));

        // Out of tree with keep_dirlinks_unsafe.
        fx.r.config.keep_dirlinks_unsafe = true;
        assert!(fx.r.adopt(&rp("out")).unwrap());
        applied(fx.write("out/o", b"o", Precondition::Absent, peer(4)));
        assert_eq!(fs::read(outside.path().join("o")).unwrap(), b"o");
    }

    #[test]
    fn invalid_ops() {
        let mut fx = Fx::new();
        user_file(&fx, "f", b"x");
        fs::create_dir(fx.p("d")).unwrap();
        fx.scan();
        let (f, d) = (fx.get("f"), fx.get("d"));
        let out = fx.r.apply(
            &RelPath::root(),
            Op::Rmdir { vv: peer(1) },
            Precondition::Absent,
            None,
        );
        assert!(matches!(out, Err(Error::InvalidOp { .. })));
        let op = Op::WriteFile {
            meta: META,
            hash: hash(b"y"),
            vv: peer(1),
        };
        invalid_op(fx.apply("f", op.clone(), Precondition::matching(&f)));
        invalid_op(fx.r.apply(
            &rp("d"),
            op,
            Precondition::matching(&d),
            Some(&mut &b"y"[..]),
        ));
        invalid_op(fx.apply("new", Op::Delete { vv: peer(1) }, Precondition::Absent));
        invalid_op(fx.apply("d", Op::Delete { vv: peer(1) }, Precondition::matching(&d)));
        invalid_op(fx.apply("f", Op::Rmdir { vv: peer(1) }, Precondition::matching(&f)));
        let mkdir = Op::Mkdir {
            mode: 0o755,
            mtime_ns: 0,
            vv: peer(1),
        };
        invalid_op(fx.apply("f", mkdir, Precondition::matching(&f)));
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"x");
        assert!(fx.p("d").is_dir());
    }

    #[test]
    fn intents_finish_with_the_index_and_replay_on_open() {
        use crate::index::IntentState;
        let mut fx = Fx::new();
        let mut q = Quarantine::new(ME, Duration::from_secs(3600));
        std::mem::swap(&mut fx.r.quarantine, &mut q);
        let pending = |fx: &Fx| fx.r.index().journal().pending().unwrap();

        // A create is done with its index update.
        let e = applied(fx.write("f", b"one", Precondition::Absent, peer(1)));
        assert!(pending(&fx).is_empty());
        // A replace keeps its intent while the old inode is quarantined.
        applied(fx.write("f", b"two", Precondition::matching(&e), peer(2)));
        let p = pending(&fx);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].1.state, IntentState::Quarantined);
        assert_eq!(fx.r.quarantine().len(), 1);

        // After a restart the old inode is quarantined again.
        let (dir, state) = (fx.dir.path().to_path_buf(), fx._state.path().to_path_buf());
        let cfg = fx.r.config().clone();
        drop(std::mem::replace(
            &mut fx.r,
            LocalReplica::open(&cfg, tempfile::tempdir().unwrap().path()).unwrap(),
        ));
        // A crashed commit's staged file, recorded but never finished.
        let staged = dir.join(".~fsync.00000000000000aa");
        fs::write(&staged, b"partial").unwrap();
        {
            let index = IndexStore::open(&IndexStore::path_for(&state, ME), ME).unwrap();
            let root = Root::open(&dir).unwrap();
            index
                .journal()
                .begin(&Intent {
                    op: crate::index::IntentOp::Create,
                    path: rp("g"),
                    parent: Fingerprint::of_fd(root.fd()).unwrap(),
                    tmp: b".~fsync.00000000000000aa".to_vec(),
                    old: None,
                    expected: None,
                    staged: None,
                    state: IntentState::Started,
                })
                .unwrap();
        }
        fx.r = LocalReplica::open(&cfg, &state)
            .unwrap()
            .quarantine_grace(Duration::ZERO);
        assert!(!staged.exists());
        assert_eq!(fx.r.quarantine().len(), 1);
        assert_eq!(pending(&fx).len(), 1);
        let report = fx.r.sweep_quarantine();
        assert_eq!((report.removed, report.finished.len()), (1, 1));
        assert!(pending(&fx).is_empty());
        assert_eq!(fs::read(fx.p("f")).unwrap(), b"two");
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn changes_since_leaves_out_local_meta() {
        let mut fx = Fx::new();
        user_file(&fx, "f", b"x");
        fx.scan();
        let changes = fx.r.changes_since(0).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].1.local, LocalMeta::default());
        assert_eq!(changes[0].1.kind, fx.get("f").kind);
        assert_eq!(fx.r.id(), ME);
    }

    #[test]
    fn watch_reports_changed_paths() {
        let mut fx = Fx::new();
        let hints = fx.r.watch().expect("inotify works on tmpfs and ext4");
        user_file(&fx, "g", b"y");
        let hint = hints.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(hint, Hint::Paths(vec![RelPath::new("g").unwrap()]));
        // The same watcher: a second call does not start another one.
        let again = fx.r.watch().unwrap();
        user_file(&fx, "h", b"z");
        let hint = again.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(hint, Hint::Paths(vec![RelPath::new("h").unwrap()]));
        assert!(hints.is_empty());
    }

    // ---- block-level delta transfer (§7.1) ----

    use crate::replica::BLOCK_SIZE;

    /// `n` bytes that differ in every block.
    fn blob(n: usize, seed: u8) -> Vec<u8> {
        (0..n)
            .map(|i| (i as u8).wrapping_mul(29).wrapping_add(seed) ^ (i >> 12) as u8)
            .collect()
    }

    fn blocks_of(data: &[u8]) -> Blocks {
        Blocks::read(&mut &data[..], data.len() as u64).unwrap()
    }

    /// The delta from `old` to `new`, and the bytes it needs sent.
    fn delta(old: &[u8], new: &[u8]) -> (Delta, Vec<u8>) {
        let d = Delta::plan(&blocks_of(old), blocks_of(new));
        let bs = BLOCK_SIZE as usize;
        let data = d
            .needed()
            .iter()
            .flat_map(|&i| new[i as usize * bs..new.len().min((i as usize + 1) * bs)].to_vec())
            .collect();
        (d, data)
    }

    /// An indexed user file `f` holding `old` (settled, so not racy).
    fn indexed_file(fx: &mut Fx, old: &[u8]) -> Entry {
        user_file(fx, "f", old);
        fx.settled_scan();
        fx.get("f")
    }

    /// `vv` after a change on the peer.
    fn bumped(vv: &VersionVector) -> VersionVector {
        let mut vv = vv.clone();
        vv.bump(PEER);
        vv
    }

    fn write_op(new: &[u8], vv: VersionVector) -> Op {
        Op::WriteFile {
            meta: META,
            hash: hash(new),
            vv,
        }
    }

    #[test]
    fn blocks_and_read_blocks_of_the_indexed_file() {
        let mut fx = Fx::new();
        let bs = BLOCK_SIZE as usize;
        let data = blob(3 * bs + 100, 1);
        let e = indexed_file(&mut fx, &data);
        assert_eq!(
            fx.r.blocks(&rp("f"), &e.kind).unwrap(),
            Some(blocks_of(&data))
        );

        let mut out = Vec::new();
        fx.r.read_blocks(&rp("f"), &e.kind, &[3, 1])
            .unwrap()
            .read_to_end(&mut out)
            .unwrap();
        assert_eq!(out, [&data[3 * bs..], &data[bs..2 * bs]].concat());

        // Not a block of the file; not the indexed kind.
        let err = fx.r.read_blocks(&rp("f"), &e.kind, &[4]).err().unwrap();
        assert!(matches!(err, Error::InvalidOp { .. }), "{err:?}");
        let other = Kind::File {
            size: 1,
            hash: [0; 32],
        };
        assert!(fx.r.blocks(&rp("f"), &other).unwrap_err().is_unstable());

        // A file that changes while its blocks are read: the stream fails.
        let mut reader = fx.r.read_blocks(&rp("f"), &e.kind, &[0, 1]).unwrap();
        let mut first = vec![0u8; bs];
        reader.read_exact(&mut first).unwrap();
        fs::File::options()
            .write(true)
            .open(fx.p("f"))
            .unwrap()
            .write_all(b"X")
            .unwrap();
        let err = reader.read_to_end(&mut Vec::new()).unwrap_err();
        assert!(Error::from_stream("read", err).is_unstable());
        // Changed since the scan: refused up front, both reads.
        assert!(
            fx.r.read_blocks(&rp("f"), &e.kind, &[0])
                .err()
                .unwrap()
                .is_unstable()
        );
        assert!(fx.r.blocks(&rp("f"), &e.kind).unwrap_err().is_unstable());
    }

    #[test]
    fn apply_delta_rebuilds_from_the_current_file() {
        let mut fx = Fx::new();
        let bs = BLOCK_SIZE as usize;
        let old = blob(4 * bs + 7, 1);
        let e = indexed_file(&mut fx, &old);
        // One byte changed in block 1, a new block inserted after block 2
        // (aligned blocks: an insertion of a whole block shifts nothing).
        let mut new = old[..3 * bs].to_vec();
        new[bs + 5] ^= 0xFF;
        new.extend(blob(bs, 9));
        new.extend(&old[3 * bs..]);
        let (d, data) = delta(&old, &new);
        assert_eq!(d.reuse, [Some(0), None, Some(2), None, Some(3), Some(4)]);
        let vv = bumped(&e.vv);
        let pre = Precondition::matching(&e);
        let out = fx.r.apply_delta(
            &rp("f"),
            write_op(&new, vv.clone()),
            pre,
            &d,
            &mut &data[..],
        );
        let got = applied(out);
        assert_eq!(fs::read(fx.p("f")).unwrap(), new);
        assert_eq!(
            got.kind,
            Kind::File {
                size: new.len() as u64,
                hash: hash(&new)
            }
        );
        assert_eq!(got.vv, vv);
        assert!(fx.leftovers().is_empty());
        // Nothing to rescan: the index already has it.
        fx.settled_scan();
        assert_eq!(fx.get("f").vv, vv);
    }

    #[test]
    fn apply_delta_with_the_current_file_changed_mid_assembly_commits_nothing() {
        let mut fx = Fx::new();
        let bs = BLOCK_SIZE as usize;
        let old = blob(3 * bs, 1);
        let e = indexed_file(&mut fx, &old);
        let mut new = old.clone();
        new[0] ^= 1;
        let (d, data) = delta(&old, &new);
        assert_eq!(d.needed(), [0]);

        // The user writes into block 2 (still to be reused) while block 0
        // is assembled: the reused block no longer hashes as listed.
        let path = fx.p("f");
        let _hook = hooks::once("delta.block", move || {
            let f = fs::File::options().write(true).open(&path).unwrap();
            std::os::unix::fs::FileExt::write_at(&f, b"user", 2 * BLOCK_SIZE + 3).unwrap();
        });
        let pre = Precondition::matching(&e);
        let out = fx.r.apply_delta(
            &rp("f"),
            write_op(&new, bumped(&e.vv)),
            pre.clone(),
            &d,
            &mut &data[..],
        );
        assert!(matches!(&out, Err(e) if e.is_unstable()), "{out:?}");
        let mut user = old.clone();
        user[2 * bs + 3..2 * bs + 7].copy_from_slice(b"user");
        assert_eq!(fs::read(fx.p("f")).unwrap(), user);
        assert_eq!(fx.get("f"), e, "the index is unchanged");
        assert!(fx.leftovers().is_empty());
        assert!(fx.r.index().journal().take_open().is_empty());

        // A change to a block that is not reused, after it was read: only
        // the final check of the current file sees it.
        let mut fx = Fx::new();
        let e = indexed_file(&mut fx, &old);
        let path = fx.p("f");
        let _hook = hooks::once("delta.assembled", move || {
            let f = fs::File::options().write(true).open(&path).unwrap();
            std::os::unix::fs::FileExt::write_at(&f, b"late", 1).unwrap();
        });
        let out = fx.r.apply_delta(
            &rp("f"),
            write_op(&new, bumped(&e.vv)),
            Precondition::matching(&e),
            &d,
            &mut &data[..],
        );
        assert!(matches!(&out, Err(e) if e.is_unstable()), "{out:?}");
        assert_eq!(&fs::read(fx.p("f")).unwrap()[1..5], b"late");
        assert_eq!(fx.get("f"), e);
        assert!(fx.leftovers().is_empty());
    }

    #[test]
    fn apply_delta_checks_the_precondition_and_the_delta() {
        let mut fx = Fx::new();
        let bs = BLOCK_SIZE as usize;
        let old = blob(2 * bs, 1);
        let e = indexed_file(&mut fx, &old);
        let mut new = old.clone();
        new[bs] ^= 1;
        let (d, data) = delta(&old, &new);
        let op = || write_op(&new, bumped(&e.vv));
        let pre = Precondition::matching(&e);
        let apply = |fx: &mut Fx, pre: Precondition, d: &Delta, data: &[u8]| {
            fx.r.apply_delta(&rp("f"), op(), pre, d, &mut &data[..])
        };

        // A stale precondition.
        let stale = Precondition::Matches {
            kind: e.kind.clone(),
            vv: peer(9),
        };
        assert_eq!(
            failed(apply(&mut fx, stale, &d, &data)),
            Some(wire(e.clone()))
        );
        // Deltas that do not fit.
        let mut far = d.clone();
        far.reuse[0] = Some(2);
        invalid_op(apply(&mut fx, pre.clone(), &far, &data));
        let mut short = d.clone();
        short.reuse.pop();
        invalid_op(apply(&mut fx, pre.clone(), &short, &data));
        invalid_op(fx.r.apply_delta(
            &rp("f"),
            Op::Delete { vv: peer(1) },
            pre.clone(),
            &d,
            &mut &data[..],
        ));
        // A reused block that does not hash as listed (the file did not
        // change: the delta is wrong).
        let mut wrong = d.clone();
        wrong.reuse[0] = Some(1);
        let out = apply(&mut fx, pre.clone(), &wrong, &data);
        assert!(matches!(&out, Err(e) if e.is_unstable()), "{out:?}");
        // A received block that is not the listed one.
        let mut bad = data.clone();
        bad[0] ^= 1;
        let out = apply(&mut fx, pre.clone(), &d, &bad);
        assert!(matches!(&out, Err(e) if e.is_unstable()), "{out:?}");
        // Too little or too much data.
        let out = apply(&mut fx, pre.clone(), &d, &data[1..]);
        assert!(matches!(&out, Err(e) if e.is_unstable()), "{out:?}");
        let out = apply(&mut fx, pre.clone(), &d, &[&data[..], b"+"].concat());
        assert!(matches!(&out, Err(e) if e.is_unstable()), "{out:?}");
        assert_eq!(fs::read(fx.p("f")).unwrap(), old);
        assert!(fx.leftovers().is_empty());

        // The file changed since the scan (same size, new mtime).
        let mut user = old.clone();
        user[0] ^= 1;
        fs::write(fx.p("f"), &user).unwrap();
        assert_eq!(
            failed(apply(&mut fx, pre.clone(), &d, &data)),
            Some(wire(e.clone()))
        );
        assert_eq!(fs::read(fx.p("f")).unwrap(), user);
        assert!(fx.leftovers().is_empty());
    }
}
