//! The per-replica index store, in redb (design §2, §3).
//!
//! Tables:
//! - `entries`: path bytes → postcard `(Entry, LocalMeta)`. Values are stored as
//!   bytes and decoded by this module, so a corrupt record is an error rather
//!   than a panic inside redb's `Value::from_bytes`.
//! - `by_seq`: seq → path bytes. Holds exactly one row per entry, at its
//!   current seq, so `changes_since` is a range scan.
//! - `meta`: schema version, replica ID, next seq, max counter, and
//!   `root_marker` (1 once the replica's root marker was made, design §5.1;
//!   absent in an index from before markers, so the schema is unchanged).
//! - `intents`: the commit journal ([`Journal`], design §5.8).
//! - `peers`: peer replica ID → wall-clock time (ns) of the last sync cycle
//!   with it ([`IndexStore::record_sync`]).
//! - `acks`: peer ID (8 bytes, big-endian) + path bytes → postcard [`Ack`]:
//!   what the peer held at one of our tombstones, and since when (tombstone
//!   GC, design §3).
//!
//! Every write goes through a [`WriteTxn`]; the single-op helpers on
//! [`IndexStore`] open one per call.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use redb::{
    Database, Durability, ReadOnlyTable, ReadTransaction, ReadableDatabase, ReadableTable,
    ReadableTableMetadata, Table, TableDefinition, WriteTransaction,
};
use serde::{Deserialize, Serialize};

use crate::config::ReplicaId;
use crate::error::{Error, Result};
use crate::fs::RelPath;
use crate::index::entry::{Entry, Kind, LocalMeta};
use crate::index::journal::{INTENTS, Journal};
use crate::index::vv::VersionVector;

const ENTRIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("entries");
const BY_SEQ: TableDefinition<u64, &[u8]> = TableDefinition::new("by_seq");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
const PEERS: TableDefinition<u64, i64> = TableDefinition::new("peers");
const ACKS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("acks");

const META_SCHEMA: &str = "schema";
const META_REPLICA: &str = "replica_id";
const META_NEXT_SEQ: &str = "next_seq";
const META_MAX_COUNTER: &str = "max_counter";
const META_ROOT_MARKER: &str = "root_marker";

/// Bump when the encoding of any table changes.
/// 2: `LinkInfo::adopted` (T17).
const SCHEMA_VERSION: u64 = 2;

/// The first seq handed out; `changes_since(0)` returns everything.
const FIRST_SEQ: u64 = 1;

/// How long a tombstone is kept after every peer acknowledged it (design §3).
pub const DEFAULT_TOMBSTONE_RETENTION: Duration = Duration::from_secs(30 * 24 * 3600);

/// What a peer's index holds at a path where this index has a tombstone
/// (tombstone GC, design §3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerState {
    /// No entry: it never had one, or it collected its tombstone already.
    Absent,
    /// A tombstone with this version vector.
    Tombstone(VersionVector),
    /// A live or `Unmanaged` entry: the deletion is not acknowledged.
    Live,
}

/// A peer's acknowledgement of one of our tombstones: our tombstone's version
/// vector and the peer's (`None`: no entry) when first seen together, and
/// when that was.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub ours: VersionVector,
    pub theirs: Option<VersionVector>,
    /// Wall-clock time in ns since the Unix epoch.
    pub since_ns: i64,
}

fn ack_key(peer: ReplicaId, path: &[u8]) -> Vec<u8> {
    let mut key = peer.0.to_be_bytes().to_vec();
    key.extend_from_slice(path);
    key
}

fn decode_ack(key: &[u8], value: &[u8]) -> Result<Ack> {
    postcard::from_bytes(value).map_err(|e| bad(format!("ack \"{}\": {e}", key.escape_ascii())))
}

fn dberr(e: impl Into<redb::Error>) -> Error {
    Error::Db(e.into())
}

fn bad(reason: impl Into<String>) -> Error {
    Error::BadIndex {
        reason: reason.into(),
    }
}

fn encode(entry: &Entry) -> Result<Vec<u8>> {
    postcard::to_stdvec(&(entry, &entry.local)).map_err(|e| bad(format!("encode entry: {e}")))
}

fn decode(key: &[u8], value: &[u8]) -> Result<(RelPath, Entry)> {
    let path = RelPath::new(key).map_err(|e| bad(format!("bad key: {e}")))?;
    let (mut entry, local): (Entry, LocalMeta) = postcard::from_bytes(value)
        .map_err(|e| bad(format!("entry \"{}\": {e}", key.escape_ascii())))?;
    entry.local = local;
    Ok((path, entry))
}

/// The index of one replica.
pub struct IndexStore {
    db: Arc<Database>,
    journal: Journal,
    path: PathBuf,
    replica: ReplicaId,
}

impl std::fmt::Debug for IndexStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexStore")
            .field("path", &self.path)
            .field("replica", &self.replica)
            .finish_non_exhaustive()
    }
}

impl IndexStore {
    /// Where a replica's index lives: `<pair state dir>/<replica>.redb`.
    pub fn path_for(pair_dir: &Path, replica: ReplicaId) -> PathBuf {
        pair_dir.join(format!("{replica}.redb"))
    }

    /// Opens the index at `path`, creating it if missing. An existing index
    /// must belong to `replica` and use the current schema.
    pub fn open(path: &Path, replica: ReplicaId) -> Result<IndexStore> {
        let db = Database::create(path).map_err(dberr)?;
        let txn = db.begin_write().map_err(dberr)?;
        {
            let mut meta = txn.open_table(META).map_err(dberr)?;
            let entries = txn.open_table(ENTRIES).map_err(dberr)?;
            txn.open_table(BY_SEQ).map_err(dberr)?;
            txn.open_table(INTENTS).map_err(dberr)?;
            txn.open_table(PEERS).map_err(dberr)?;
            txn.open_table(ACKS).map_err(dberr)?;
            match get_meta(&meta, META_SCHEMA)? {
                None => {
                    if !entries.is_empty().map_err(dberr)? {
                        return Err(bad(format!("{}: entries without a schema", path.display())));
                    }
                    for (k, v) in [
                        (META_SCHEMA, SCHEMA_VERSION),
                        (META_REPLICA, replica.0),
                        (META_NEXT_SEQ, FIRST_SEQ),
                        (META_MAX_COUNTER, 0),
                    ] {
                        meta.insert(k, v).map_err(dberr)?;
                    }
                }
                Some(SCHEMA_VERSION) => {
                    let stored = get_meta(&meta, META_REPLICA)?.map(ReplicaId);
                    if stored != Some(replica) {
                        return Err(bad(format!(
                            "{} belongs to replica {}, not {replica}",
                            path.display(),
                            stored.map_or("<none>".into(), |r| r.to_string()),
                        )));
                    }
                }
                Some(v) => {
                    return Err(bad(format!(
                        "{}: schema version {v}, expected {SCHEMA_VERSION}",
                        path.display()
                    )));
                }
            }
        }
        txn.commit().map_err(dberr)?;
        let db = Arc::new(db);
        Ok(IndexStore {
            journal: Journal::new(db.clone()),
            db,
            path: path.to_owned(),
            replica,
        })
    }

    pub fn replica(&self) -> ReplicaId {
        self.replica
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The commit journal, in the same database.
    pub fn journal(&self) -> &Journal {
        &self.journal
    }

    /// A consistent read-only snapshot.
    pub fn read(&self) -> Result<ReadTxn> {
        let txn = self.db.begin_read().map_err(dberr)?;
        Ok(ReadTxn { txn })
    }

    /// A write transaction; nothing is visible until [`WriteTxn::commit`].
    pub fn write(&self) -> Result<WriteTxn> {
        WriteTxn::begin(self.db.begin_write().map_err(dberr)?)
    }

    pub fn get(&self, path: &RelPath) -> Result<Option<Entry>> {
        self.read()?.get(path)
    }

    /// Stores `entry` at `path` in its own transaction and sets `entry.seq`.
    pub fn put(&self, path: &RelPath, entry: &mut Entry) -> Result<u64> {
        let mut txn = self.write()?;
        let seq = txn.put(path, entry)?;
        txn.commit()?;
        Ok(seq)
    }

    /// Removes the entry at `path` in its own transaction. Returns whether
    /// there was one.
    pub fn remove(&self, path: &RelPath) -> Result<bool> {
        let mut txn = self.write()?;
        let removed = txn.remove(path)?;
        txn.commit()?;
        Ok(removed)
    }

    pub fn changes_since(&self, seq: u64) -> Result<Vec<(RelPath, Entry)>> {
        self.read()?.changes_since(seq)
    }

    pub fn iter_prefix(&self, prefix: &RelPath) -> Result<Vec<(RelPath, Entry)>> {
        self.read()?.iter_prefix(prefix)
    }

    /// The seq the next put will get. Every stored entry has a smaller seq.
    pub fn next_seq(&self) -> Result<u64> {
        self.read()?.next_seq()
    }

    /// The largest version counter ever stored: the replica's Lamport clock,
    /// for [`VersionVector::bump_after`](crate::index::VersionVector::bump_after).
    pub fn max_counter(&self) -> Result<u64> {
        self.read()?.max_counter()
    }

    /// Whether the replica's root marker was made (or found) for this index
    /// ([`IndexStore::set_root_marked`]). From then on, a root without it is
    /// refused (design §5.1).
    pub fn root_marked(&self) -> Result<bool> {
        let txn = self.db.begin_read().map_err(dberr)?;
        let meta = txn.open_table(META).map_err(dberr)?;
        Ok(get_meta(&meta, META_ROOT_MARKER)? == Some(1))
    }

    /// Records, durably, that the root marker exists.
    pub fn set_root_marked(&self) -> Result<()> {
        let txn = self.db.begin_write().map_err(dberr)?;
        txn.open_table(META)
            .map_err(dberr)?
            .insert(META_ROOT_MARKER, 1)
            .map_err(dberr)?;
        txn.commit().map_err(dberr)
    }

    /// Records a sync cycle with `peer` at `now_ns` and collects tombstones,
    /// in one (non-durable) transaction; see [`WriteTxn::record_sync`].
    pub fn record_sync(
        &self,
        peer: ReplicaId,
        tombstones: &[(RelPath, VersionVector, PeerState)],
        now_ns: i64,
        retention: Duration,
    ) -> Result<Vec<RelPath>> {
        let mut txn = self.write()?;
        txn.txn.set_durability(Durability::None).map_err(dberr)?;
        let removed = txn.record_sync(peer, tombstones, now_ns, retention)?;
        txn.commit()?;
        Ok(removed)
    }
}

/// A read-only snapshot of the index.
pub struct ReadTxn {
    txn: ReadTransaction,
}

impl ReadTxn {
    fn entries(&self) -> Result<ReadOnlyTable<&'static [u8], &'static [u8]>> {
        self.txn.open_table(ENTRIES).map_err(dberr)
    }

    fn meta(&self, key: &str) -> Result<u64> {
        let meta = self.txn.open_table(META).map_err(dberr)?;
        get_meta(&meta, key)?.ok_or_else(|| bad(format!("missing meta key {key}")))
    }

    pub fn get(&self, path: &RelPath) -> Result<Option<Entry>> {
        get_entry(&self.entries()?, path)
    }

    /// Every entry whose seq is above `seq`, in seq order.
    pub fn changes_since(&self, seq: u64) -> Result<Vec<(RelPath, Entry)>> {
        let by_seq = self.txn.open_table(BY_SEQ).map_err(dberr)?;
        changes_since(&by_seq, &self.entries()?, seq)
    }

    /// `prefix` itself and every entry beneath it, in key order. The root
    /// prefix returns the whole index.
    pub fn iter_prefix(&self, prefix: &RelPath) -> Result<Vec<(RelPath, Entry)>> {
        iter_prefix(&self.entries()?, prefix)
    }

    /// Number of entries (tombstones included).
    pub fn len(&self) -> Result<u64> {
        self.entries()?.len().map_err(dberr)
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn next_seq(&self) -> Result<u64> {
        self.meta(META_NEXT_SEQ)
    }

    pub fn max_counter(&self) -> Result<u64> {
        self.meta(META_MAX_COUNTER)
    }

    /// Every peer synced with, and the wall-clock time (ns) of the last
    /// sync cycle with it.
    pub fn peers(&self) -> Result<Vec<(ReplicaId, i64)>> {
        let peers = self.txn.open_table(PEERS).map_err(dberr)?;
        let mut out = Vec::new();
        for row in peers.iter().map_err(dberr)? {
            let (k, v) = row.map_err(dberr)?;
            out.push((ReplicaId(k.value()), v.value()));
        }
        Ok(out)
    }

    /// `peer`'s acknowledgement of our tombstone at `path`, if recorded.
    pub fn ack(&self, peer: ReplicaId, path: &RelPath) -> Result<Option<Ack>> {
        let acks = self.txn.open_table(ACKS).map_err(dberr)?;
        let key = ack_key(peer, path.as_bytes());
        match acks.get(key.as_slice()).map_err(dberr)? {
            Some(v) => Ok(Some(decode_ack(&key, v.value())?)),
            None => Ok(None),
        }
    }
}

/// A write transaction. Dropping it without [`commit`](Self::commit) discards
/// every change.
pub struct WriteTxn {
    txn: WriteTransaction,
    next_seq: u64,
    max_counter: u64,
}

impl WriteTxn {
    fn begin(txn: WriteTransaction) -> Result<WriteTxn> {
        let (next_seq, max_counter) = {
            let meta = txn.open_table(META).map_err(dberr)?;
            let get = |k| get_meta(&meta, k)?.ok_or_else(|| bad(format!("missing meta key {k}")));
            (get(META_NEXT_SEQ)?, get(META_MAX_COUNTER)?)
        };
        Ok(WriteTxn {
            txn,
            next_seq,
            max_counter,
        })
    }

    pub(crate) fn raw(&self) -> &WriteTransaction {
        &self.txn
    }

    fn entries(&self) -> Result<Table<'_, &'static [u8], &'static [u8]>> {
        self.txn.open_table(ENTRIES).map_err(dberr)
    }

    fn by_seq(&self) -> Result<Table<'_, u64, &'static [u8]>> {
        self.txn.open_table(BY_SEQ).map_err(dberr)
    }

    /// Sees this transaction's own uncommitted puts.
    pub fn get(&self, path: &RelPath) -> Result<Option<Entry>> {
        get_entry(&self.entries()?, path)
    }

    /// Stores `entry` at `path`, replacing any previous entry, under a fresh
    /// seq. Sets `entry.seq` and returns it.
    pub fn put(&mut self, path: &RelPath, entry: &mut Entry) -> Result<u64> {
        let seq = self.next_seq;
        entry.seq = seq;
        let value = encode(entry)?;
        let key = path.as_bytes();
        let old_seq = {
            let mut entries = self.entries()?;
            let old = entries.insert(key, value.as_slice()).map_err(dberr)?;
            old.map(|old| decode(key, old.value()).map(|(_, e)| e.seq))
                .transpose()?
        };
        {
            let mut by_seq = self.by_seq()?;
            if let Some(old_seq) = old_seq {
                by_seq.remove(old_seq).map_err(dberr)?;
            }
            by_seq.insert(seq, key).map_err(dberr)?;
        }
        self.next_seq += 1;
        self.max_counter = self.max_counter.max(entry.vv.max_counter());
        Ok(seq)
    }

    /// Rewrites the entry at `path` **without** a new seq: for changes to
    /// [`LocalMeta`] only, which peers never see, so `changes_since` does not
    /// report the entry again. `entry.seq` must be the stored entry's seq.
    pub fn put_local(&mut self, path: &RelPath, entry: &Entry) -> Result<()> {
        let key = path.as_bytes();
        let value = encode(entry)?;
        let mut entries = self.entries()?;
        let stored = entries
            .get(key)
            .map_err(dberr)?
            .map(|old| decode(key, old.value()).map(|(_, e)| e.seq))
            .transpose()?;
        if stored != Some(entry.seq) {
            return Err(bad(format!(
                "put_local \"{}\": seq {} does not match the stored entry ({stored:?})",
                key.escape_ascii(),
                entry.seq
            )));
        }
        entries.insert(key, value.as_slice()).map_err(dberr)?;
        Ok(())
    }

    /// Removes the entry at `path` (tombstone GC). Returns whether there was one.
    pub fn remove(&mut self, path: &RelPath) -> Result<bool> {
        let key = path.as_bytes();
        let old_seq = {
            let mut entries = self.entries()?;
            let old = entries.remove(key).map_err(dberr)?;
            old.map(|old| decode(key, old.value()).map(|(_, e)| e.seq))
                .transpose()?
        };
        match old_seq {
            Some(seq) => {
                self.by_seq()?.remove(seq).map_err(dberr)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub fn changes_since(&self, seq: u64) -> Result<Vec<(RelPath, Entry)>> {
        changes_since(&self.by_seq()?, &self.entries()?, seq)
    }

    pub fn iter_prefix(&self, prefix: &RelPath) -> Result<Vec<(RelPath, Entry)>> {
        iter_prefix(&self.entries()?, prefix)
    }

    pub fn max_counter(&self) -> u64 {
        self.max_counter
    }

    /// Tombstone GC (design §3), after a sync cycle with `peer` that ended
    /// at `now_ns` (wall clock).
    ///
    /// `tombstones` lists every tombstone of this index as the cycle ended,
    /// with its version vector and what `peer` held at its path. One that is
    /// no longer the stored entry (another entry, or another vector) is
    /// ignored. For the others, the peer's acknowledgement is recorded: a
    /// tombstone or no entry there acknowledges the deletion (from now on,
    /// or since the earlier record if the peer's vector and ours are
    /// unchanged; a peer that dropped its tombstone keeps its record); a live
    /// entry withdraws it. Records of paths that are no tombstone any more go.
    ///
    /// A tombstone is then removed if every peer synced with so far has
    /// acknowledged it for at least `retention`. Neither side can resurrect it
    /// then: the peer has nothing live there, and a later change on either
    /// side gets a version vector that dominates or is concurrent with the
    /// other side's tombstone, which a modification wins (§6.1). Concurrent
    /// tombstones never become equal (§6.1 never pushes one over another), so
    /// any peer tombstone counts, whatever its vector.
    ///
    /// Also records `now_ns` as the last sync with `peer`. Returns the paths
    /// whose tombstones were removed.
    pub fn record_sync(
        &mut self,
        peer: ReplicaId,
        tombstones: &[(RelPath, VersionVector, PeerState)],
        now_ns: i64,
        retention: Duration,
    ) -> Result<Vec<RelPath>> {
        let mut current: Vec<(&RelPath, &VersionVector)> = Vec::new();
        {
            let entries = self.entries()?;
            let mut acks = self.txn.open_table(ACKS).map_err(dberr)?;
            for (path, vv, state) in tombstones {
                let key = ack_key(peer, path.as_bytes());
                let is_current = get_entry(&entries, path)?
                    .is_some_and(|e| e.kind == Kind::Tombstone && &e.vv == vv);
                if !is_current {
                    acks.remove(key.as_slice()).map_err(dberr)?;
                    continue;
                }
                current.push((path, vv));
                let old = match acks.get(key.as_slice()).map_err(dberr)? {
                    Some(v) => Some(decode_ack(&key, v.value())?),
                    None => None,
                };
                let old = old.filter(|a| &a.ours == vv);
                let theirs = match state {
                    PeerState::Live => {
                        acks.remove(key.as_slice()).map_err(dberr)?;
                        continue;
                    }
                    PeerState::Absent => match old {
                        Some(_) => continue,
                        None => None,
                    },
                    PeerState::Tombstone(t) => {
                        if old.as_ref().is_some_and(|a| a.theirs.as_ref() == Some(t)) {
                            continue;
                        }
                        Some(t.clone())
                    }
                };
                let ack = Ack {
                    ours: vv.clone(),
                    theirs,
                    since_ns: now_ns,
                };
                let value =
                    postcard::to_stdvec(&ack).map_err(|e| bad(format!("encode ack: {e}")))?;
                acks.insert(key.as_slice(), value.as_slice())
                    .map_err(dberr)?;
            }

            // Forget records of paths that are no tombstone any more.
            let keep: HashSet<&[u8]> = current.iter().map(|(p, _)| p.as_bytes()).collect();
            let prefix = peer.0.to_be_bytes();
            let mut stale = Vec::new();
            for row in acks.range(prefix.as_slice()..).map_err(dberr)? {
                let (k, _) = row.map_err(dberr)?;
                let Some(path) = k.value().strip_prefix(prefix.as_slice()) else {
                    break;
                };
                if !keep.contains(path) {
                    stale.push(k.value().to_vec());
                }
            }
            for k in stale {
                acks.remove(k.as_slice()).map_err(dberr)?;
            }
        }
        let peers: Vec<ReplicaId> = {
            let mut table = self.txn.open_table(PEERS).map_err(dberr)?;
            table.insert(peer.0, now_ns).map_err(dberr)?;
            let mut peers = Vec::new();
            for row in table.iter().map_err(dberr)? {
                peers.push(ReplicaId(row.map_err(dberr)?.0.value()));
            }
            peers
        };

        let retention = i64::try_from(retention.as_nanos()).unwrap_or(i64::MAX);
        let mut removed = Vec::new();
        for (path, vv) in current {
            let mut keys = Vec::new();
            let mut acked = true;
            {
                let acks = self.txn.open_table(ACKS).map_err(dberr)?;
                for &p in &peers {
                    let key = ack_key(p, path.as_bytes());
                    let ack = match acks.get(key.as_slice()).map_err(dberr)? {
                        Some(v) => decode_ack(&key, v.value())?,
                        None => {
                            acked = false;
                            break;
                        }
                    };
                    if &ack.ours != vv || ack.since_ns.saturating_add(retention) > now_ns {
                        acked = false;
                        break;
                    }
                    keys.push(key);
                }
            }
            if !acked {
                continue;
            }
            self.remove(path)?;
            let mut acks = self.txn.open_table(ACKS).map_err(dberr)?;
            for key in keys {
                acks.remove(key.as_slice()).map_err(dberr)?;
            }
            removed.push(path.clone());
        }
        Ok(removed)
    }

    /// Makes every change durable and visible, atomically.
    pub fn commit(self) -> Result<()> {
        {
            let mut meta = self.txn.open_table(META).map_err(dberr)?;
            meta.insert(META_NEXT_SEQ, self.next_seq).map_err(dberr)?;
            meta.insert(META_MAX_COUNTER, self.max_counter)
                .map_err(dberr)?;
        }
        self.txn.commit().map_err(dberr)
    }
}

fn get_meta(meta: &impl ReadableTable<&'static str, u64>, key: &str) -> Result<Option<u64>> {
    Ok(meta.get(key).map_err(dberr)?.map(|v| v.value()))
}

fn get_entry(
    entries: &impl ReadableTable<&'static [u8], &'static [u8]>,
    path: &RelPath,
) -> Result<Option<Entry>> {
    let key = path.as_bytes();
    match entries.get(key).map_err(dberr)? {
        Some(v) => Ok(Some(decode(key, v.value())?.1)),
        None => Ok(None),
    }
}

fn changes_since(
    by_seq: &impl ReadableTable<u64, &'static [u8]>,
    entries: &impl ReadableTable<&'static [u8], &'static [u8]>,
    seq: u64,
) -> Result<Vec<(RelPath, Entry)>> {
    let Some(from) = seq.checked_add(1) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for row in by_seq.range(from..).map_err(dberr)? {
        let (s, key) = row.map_err(dberr)?;
        let (s, key) = (s.value(), key.value());
        let value = entries.get(key).map_err(dberr)?.ok_or_else(|| {
            bad(format!(
                "seq {s} points to missing \"{}\"",
                key.escape_ascii()
            ))
        })?;
        let (path, entry) = decode(key, value.value())?;
        if entry.seq != s {
            return Err(bad(format!(
                "seq {s} points to \"{path}\", which has seq {}",
                entry.seq
            )));
        }
        out.push((path, entry));
    }
    Ok(out)
}

fn iter_prefix(
    entries: &impl ReadableTable<&'static [u8], &'static [u8]>,
    prefix: &RelPath,
) -> Result<Vec<(RelPath, Entry)>> {
    let mut out = Vec::new();
    let push_range = |out: &mut Vec<_>, range: redb::Range<'_, &'static [u8], &'static [u8]>| {
        for row in range {
            let (k, v) = row.map_err(dberr)?;
            out.push(decode(k.value(), v.value())?);
        }
        Ok::<_, Error>(())
    };
    if prefix.is_root() {
        push_range(&mut out, entries.iter().map_err(dberr)?)?;
        return Ok(out);
    }
    if let Some(e) = get_entry(entries, prefix)? {
        out.push((prefix.clone(), e));
    }
    // Descendants are exactly the keys in ["prefix/", "prefix0"): '0' follows
    // '/' in byte order. A sibling like "prefix.txt" sorts before them.
    let mut lo = prefix.as_bytes().to_vec();
    lo.push(b'/');
    let mut hi = prefix.as_bytes().to_vec();
    hi.push(b'/' + 1);
    push_range(
        &mut out,
        entries
            .range::<&[u8]>(lo.as_slice()..hi.as_slice())
            .map_err(dberr)?,
    )?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::entry::{Kind, LinkInfo, UnmanagedReason};
    use crate::index::vv::VersionVector;

    const ME: ReplicaId = ReplicaId(0xabcd_ef01_2345_6789);

    fn p(s: &[u8]) -> RelPath {
        RelPath::new(s).unwrap()
    }

    fn file(content: &[u8], counter: u64) -> Entry {
        let mut vv = VersionVector::new();
        vv.set(ME, counter);
        Entry::new(
            Kind::File {
                size: content.len() as u64,
                hash: *blake3::hash(content).as_bytes(),
            },
            0o644,
            1_700_000_000_123_456_789,
            vv,
        )
    }

    fn open() -> (tempfile::TempDir, IndexStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = IndexStore::open(&IndexStore::path_for(dir.path(), ME), ME).unwrap();
        (dir, store)
    }

    fn paths(v: &[(RelPath, Entry)]) -> Vec<Vec<u8>> {
        v.iter().map(|(p, _)| p.as_bytes().to_vec()).collect()
    }

    #[test]
    fn round_trip_every_kind_with_local_meta() {
        let (dir, store) = open();
        let mut vv = VersionVector::new();
        vv.set(ReplicaId(1), 3);
        vv.set(ME, 7);
        let local = LocalMeta {
            dev: 0x803,
            ino: 12345,
            ctime_ns: -1,
            mnt_id: 99,
            raw_target: Some(b"/rsyncd-munged/t".to_vec()),
            via_link: Some(LinkInfo {
                ino: 5,
                ctime_ns: 6,
                raw_target: b"/abs/\xfe".to_vec(),
                out_of_tree: true,
                adopted: true,
            }),
            racy: true,
        };
        let kinds = [
            Kind::File {
                size: 3,
                hash: [9; 32],
            },
            Kind::Dir,
            Kind::Symlink {
                target: b"../t\x80".to_vec(),
            },
            Kind::Tombstone,
            Kind::Unmanaged(UnmanagedReason::IgnoredLink),
            Kind::Unmanaged(UnmanagedReason::Dangling),
            Kind::Unmanaged(UnmanagedReason::Loop),
            Kind::Unmanaged(UnmanagedReason::Special),
        ];
        let mut stored = Vec::new();
        for (i, kind) in kinds.into_iter().enumerate() {
            let path = p(format!("d/n\u{e9}{i}").as_bytes());
            let mut e = Entry::new(kind, 0o1755, -(i as i64), vv.clone());
            e.local = local.clone();
            let seq = store.put(&path, &mut e).unwrap();
            assert_eq!(e.seq, seq);
            stored.push((path, e));
        }
        // Non-UTF-8 key and the root itself.
        for path in [p(b"\xff\xfe"), RelPath::root()] {
            let mut e = file(b"x", 1);
            store.put(&path, &mut e).unwrap();
            stored.push((path, e));
        }
        let check = |store: &IndexStore| {
            for (path, e) in &stored {
                assert_eq!(store.get(path).unwrap().as_ref(), Some(e), "{path}");
            }
            assert_eq!(store.get(&p(b"missing")).unwrap(), None);
            assert_eq!(store.read().unwrap().len().unwrap(), stored.len() as u64);
            assert_eq!(store.max_counter().unwrap(), 7);
            assert_eq!(store.next_seq().unwrap(), FIRST_SEQ + stored.len() as u64);
        };
        check(&store);
        let path = store.path().to_owned();
        drop(store);
        let store = IndexStore::open(&path, ME).unwrap();
        check(&store);
        drop(dir);
    }

    #[test]
    fn put_replaces_and_reassigns_seq() {
        let (_dir, store) = open();
        let (a, b) = (p(b"a"), p(b"b"));
        let s1 = store.put(&a, &mut file(b"1", 1)).unwrap();
        let s2 = store.put(&b, &mut file(b"2", 2)).unwrap();
        let s3 = store.put(&a, &mut file(b"3", 3)).unwrap();
        assert!(s1 < s2 && s2 < s3);
        assert_eq!(store.get(&a).unwrap().unwrap().seq, s3);
        // One by_seq row per entry: `a` shows up once, at its new seq.
        let all = store.changes_since(0).unwrap();
        assert_eq!(paths(&all), [b"b".to_vec(), b"a".to_vec()]);
        assert_eq!(all[1].1, file(b"3", 3).with_seq(s3));
        assert_eq!(paths(&store.changes_since(s2).unwrap()), [b"a".to_vec()]);
        assert!(store.changes_since(s3).unwrap().is_empty());
        assert!(store.changes_since(u64::MAX).unwrap().is_empty());
    }

    #[test]
    fn put_local_keeps_seq() {
        let (_dir, store) = open();
        let a = p(b"a");
        let mut e = file(b"1", 1);
        let seq = store.put(&a, &mut e).unwrap();
        e.local.ino = 77;
        e.local.racy = true;
        let mut txn = store.write().unwrap();
        txn.put_local(&a, &e).unwrap();
        // A stale seq or a missing entry is refused.
        let stale = e.clone().with_seq(seq + 1);
        assert!(matches!(
            txn.put_local(&a, &stale),
            Err(Error::BadIndex { .. })
        ));
        assert!(matches!(
            txn.put_local(&p(b"b"), &e),
            Err(Error::BadIndex { .. })
        ));
        txn.commit().unwrap();
        let back = store.get(&a).unwrap().unwrap();
        assert_eq!(back, e);
        assert_eq!((back.seq, back.local.ino), (seq, 77));
        assert!(store.changes_since(seq).unwrap().is_empty());
        assert_eq!(store.next_seq().unwrap(), seq + 1);
    }

    #[test]
    fn changes_since_returns_every_later_put() {
        let (_dir, store) = open();
        let mut seqs = Vec::new();
        for i in 0..50u64 {
            // Revisit paths so some puts replace earlier ones.
            let path = p(format!("f{}", i % 17).as_bytes());
            seqs.push((
                store
                    .put(&path, &mut file(&i.to_le_bytes(), i + 1))
                    .unwrap(),
                path,
            ));
        }
        for (cut, _) in &seqs {
            let changes = store.changes_since(*cut).unwrap();
            // Expected: the last put of each path, if it came after `cut`.
            let mut want: Vec<(u64, RelPath)> = Vec::new();
            for (s, path) in &seqs {
                want.retain(|(_, q)| q != path);
                want.push((*s, path.clone()));
            }
            want.retain(|(s, _)| s > cut);
            let got: Vec<(u64, RelPath)> = changes.into_iter().map(|(p, e)| (e.seq, p)).collect();
            assert_eq!(got, want, "since {cut}");
        }
    }

    #[test]
    fn iter_prefix_is_component_wise() {
        let (_dir, store) = open();
        for name in [
            &b"a"[..],
            b"a/b",
            b"a/b/c",
            b"a.txt",
            b"a0",
            b"ab",
            b"a/\xff",
            b"b",
            b"b/a",
        ] {
            store.put(&p(name), &mut file(name, 1)).unwrap();
        }
        let got = |s: &[u8]| paths(&store.iter_prefix(&p(s)).unwrap());
        assert_eq!(got(b"a"), [&b"a"[..], b"a/b", b"a/b/c", b"a/\xff"]);
        assert_eq!(got(b"a/b"), [&b"a/b"[..], b"a/b/c"]);
        assert_eq!(got(b"b"), [&b"b"[..], b"b/a"]);
        assert_eq!(got(b"c"), Vec::<Vec<u8>>::new());
        // Descendants are returned even when the prefix itself has no entry.
        store.remove(&p(b"a")).unwrap();
        assert_eq!(got(b"a"), [&b"a/b"[..], b"a/b/c", b"a/\xff"]);
        assert_eq!(store.iter_prefix(&RelPath::root()).unwrap().len(), 8);
    }

    #[test]
    fn transactions_are_atomic() {
        let (_dir, store) = open();
        let base = store.put(&p(b"keep"), &mut file(b"k", 1)).unwrap();
        {
            let mut txn = store.write().unwrap();
            txn.put(&p(b"x"), &mut file(b"x", 5)).unwrap();
            assert!(txn.get(&p(b"x")).unwrap().is_some());
            assert!(txn.remove(&p(b"keep")).unwrap());
            assert_eq!(txn.max_counter(), 5);
            // Dropped without commit.
        }
        assert!(store.get(&p(b"x")).unwrap().is_none());
        assert!(store.get(&p(b"keep")).unwrap().is_some());
        assert_eq!(store.max_counter().unwrap(), 1);
        assert_eq!(store.next_seq().unwrap(), base + 1);

        // A read snapshot does not see a later commit.
        let snap = store.read().unwrap();
        let mut txn = store.write().unwrap();
        let s1 = txn.put(&p(b"x"), &mut file(b"x", 5)).unwrap();
        let s2 = txn.put(&p(b"y"), &mut file(b"y", 6)).unwrap();
        assert_eq!(
            paths(&txn.changes_since(base).unwrap()),
            [b"x".to_vec(), b"y".to_vec()]
        );
        txn.commit().unwrap();
        assert!(snap.get(&p(b"x")).unwrap().is_none());
        assert_eq!(
            paths(&store.changes_since(base).unwrap()),
            [b"x".to_vec(), b"y".to_vec()]
        );
        assert_eq!((s1, s2), (base + 1, base + 2));
        assert_eq!(store.max_counter().unwrap(), 6);

        assert!(store.remove(&p(b"x")).unwrap());
        assert!(!store.remove(&p(b"x")).unwrap());
        assert_eq!(paths(&store.changes_since(base).unwrap()), [b"y".to_vec()]);
    }

    #[test]
    fn open_rejects_another_replica() {
        let (_dir, store) = open();
        let path = store.path().to_owned();
        drop(store);
        let err = IndexStore::open(&path, ReplicaId(1)).unwrap_err();
        assert!(matches!(err, Error::BadIndex { .. }), "{err}");
        IndexStore::open(&path, ME).unwrap();
    }

    #[test]
    fn root_marked_persists() {
        let (_dir, store) = open();
        assert!(!store.root_marked().unwrap());
        store.set_root_marked().unwrap();
        assert!(store.root_marked().unwrap());
        let path = store.path().to_owned();
        drop(store);
        assert!(IndexStore::open(&path, ME).unwrap().root_marked().unwrap());
    }

    #[test]
    fn corrupt_record_is_an_error() {
        let (_dir, store) = open();
        store.put(&p(b"ok"), &mut file(b"ok", 1)).unwrap();
        {
            let txn = store.db.begin_write().unwrap();
            {
                let mut t = txn.open_table(ENTRIES).unwrap();
                t.insert(&b"bad"[..], &b"\xff\xff\xff"[..]).unwrap();
                t.insert(&b"/abs"[..], encode(&file(b"", 1)).unwrap().as_slice())
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        assert!(matches!(store.get(&p(b"bad")), Err(Error::BadIndex { .. })));
        assert!(matches!(
            store.iter_prefix(&RelPath::root()),
            Err(Error::BadIndex { .. })
        ));
        assert!(store.get(&p(b"ok")).unwrap().is_some());
    }

    // ----- T18: tombstone GC ---------------------------------------------

    const PEER: ReplicaId = ReplicaId(0x77);
    const H: Duration = Duration::from_nanos(100);

    fn tomb(store: &IndexStore, path: &[u8], counter: u64) -> VersionVector {
        let mut vv = VersionVector::new();
        vv.set(PEER, counter);
        store
            .put(&p(path), &mut Entry::new(Kind::Tombstone, 0, 0, vv.clone()))
            .unwrap();
        vv
    }

    fn gc(
        store: &IndexStore,
        peer: ReplicaId,
        seen: &[(&[u8], &VersionVector, PeerState)],
        now: i64,
    ) -> Vec<Vec<u8>> {
        let seen: Vec<_> = seen
            .iter()
            .map(|(path, vv, st)| (p(path), (*vv).clone(), st.clone()))
            .collect();
        let removed = store.record_sync(peer, &seen, now, H).unwrap();
        removed.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    #[test]
    fn tombstones_are_collected_after_retention() {
        let (_dir, store) = open();
        let (t, a, l) = (
            tomb(&store, b"t", 1),
            tomb(&store, b"a", 2),
            tomb(&store, b"l", 3),
        );
        let seen = |st_t| {
            [
                (&b"t"[..], &t, st_t),
                (&b"a"[..], &a, PeerState::Absent),
                (&b"l"[..], &l, PeerState::Live),
            ]
        };
        // Acknowledged at 1000 (tombstone or nothing at the peer); live: not.
        assert!(gc(&store, PEER, &seen(PeerState::Tombstone(t.clone())), 1000).is_empty());
        let r = store.read().unwrap();
        let ack = r.ack(PEER, &p(b"t")).unwrap().unwrap();
        assert_eq!(
            (ack.ours, ack.theirs, ack.since_ns),
            (t.clone(), Some(t.clone()), 1000)
        );
        assert_eq!(r.ack(PEER, &p(b"a")).unwrap().unwrap().theirs, None);
        assert_eq!(r.ack(PEER, &p(b"l")).unwrap(), None);
        assert_eq!(r.peers().unwrap(), [(PEER, 1000)]);
        drop(r);

        // The peer collected its tombstone at "t": the record keeps its time.
        assert!(gc(&store, PEER, &seen(PeerState::Absent), 1099).is_empty());
        assert_eq!(
            gc(&store, PEER, &seen(PeerState::Absent), 1100),
            [b"t", b"a"]
        );
        let r = store.read().unwrap();
        assert_eq!(r.get(&p(b"t")).unwrap(), None);
        assert_eq!(r.get(&p(b"a")).unwrap(), None);
        assert_eq!(r.ack(PEER, &p(b"t")).unwrap(), None);
        assert_eq!(r.get(&p(b"l")).unwrap().unwrap().kind, Kind::Tombstone);
        assert_eq!(r.peers().unwrap(), [(PEER, 1100)]);
    }

    #[test]
    fn a_changed_vector_or_entry_restarts_or_drops_the_ack() {
        let (_dir, store) = open();
        let t = tomb(&store, b"t", 1);
        let theirs = |n| {
            let mut vv = VersionVector::new();
            vv.set(ReplicaId(9), n);
            vv
        };
        gc(
            &store,
            PEER,
            &[(b"t", &t, PeerState::Tombstone(theirs(1)))],
            1000,
        );
        // A different (concurrent) peer tombstone restarts the period.
        gc(
            &store,
            PEER,
            &[(b"t", &t, PeerState::Tombstone(theirs(2)))],
            1050,
        );
        assert!(gc(&store, PEER, &[(b"t", &t, PeerState::Absent)], 1149).is_empty());
        // Our tombstone changed since the list was made: ignored.
        let t2 = tomb(&store, b"t", 5);
        assert!(gc(&store, PEER, &[(b"t", &t, PeerState::Absent)], 5000).is_empty());
        assert_eq!(store.read().unwrap().ack(PEER, &p(b"t")).unwrap(), None);
        gc(&store, PEER, &[(b"t", &t2, PeerState::Absent)], 6000);
        // The path is live again: its record goes.
        store.put(&p(b"t"), &mut file(b"back", 6)).unwrap();
        assert!(gc(&store, PEER, &[], 7000).is_empty());
        assert_eq!(store.read().unwrap().ack(PEER, &p(b"t")).unwrap(), None);
        assert!(store.get(&p(b"t")).unwrap().is_some());
    }

    #[test]
    fn every_known_peer_must_acknowledge() {
        let (_dir, store) = open();
        let t = tomb(&store, b"t", 1);
        let other = ReplicaId(0x88);
        gc(&store, other, &[(b"t", &t, PeerState::Live)], 1000);
        assert!(gc(&store, PEER, &[(b"t", &t, PeerState::Absent)], 1000).is_empty());
        assert!(gc(&store, PEER, &[(b"t", &t, PeerState::Absent)], 9000).is_empty());
        gc(&store, other, &[(b"t", &t, PeerState::Absent)], 9000);
        assert_eq!(
            gc(&store, PEER, &[(b"t", &t, PeerState::Absent)], 9100),
            [b"t"]
        );
        assert_eq!(store.read().unwrap().ack(other, &p(b"t")).unwrap(), None);
    }

    impl Entry {
        fn with_seq(mut self, seq: u64) -> Entry {
            self.seq = seq;
            self
        }
    }
}
