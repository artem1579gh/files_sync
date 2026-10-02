//! The per-replica index store, in redb (design §2, §3).
//!
//! Tables:
//! - `entries`: path bytes → postcard `(Entry, LocalMeta)`. Values are stored as
//!   bytes and decoded by this module, so a corrupt record is an error rather
//!   than a panic inside redb's `Value::from_bytes`.
//! - `by_seq`: seq → path bytes. Holds exactly one row per entry, at its
//!   current seq, so `changes_since` is a range scan.
//! - `meta`: schema version, replica ID, next seq, max counter.
//!
//! Every write goes through a [`WriteTxn`]; the single-op helpers on
//! [`IndexStore`] open one per call.

use std::path::{Path, PathBuf};

use redb::{
    Database, ReadOnlyTable, ReadTransaction, ReadableDatabase, ReadableTable,
    ReadableTableMetadata, Table,
    TableDefinition, WriteTransaction,
};

use crate::config::ReplicaId;
use crate::error::{Error, Result};
use crate::fs::RelPath;
use crate::index::entry::{Entry, LocalMeta};

const ENTRIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("entries");
const BY_SEQ: TableDefinition<u64, &[u8]> = TableDefinition::new("by_seq");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

const META_SCHEMA: &str = "schema";
const META_REPLICA: &str = "replica_id";
const META_NEXT_SEQ: &str = "next_seq";
const META_MAX_COUNTER: &str = "max_counter";

/// Bump when the encoding of any table changes.
const SCHEMA_VERSION: u64 = 1;

/// The first seq handed out; `changes_since(0)` returns everything.
const FIRST_SEQ: u64 = 1;

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
    db: Database,
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
        Ok(IndexStore {
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

    /// Makes every change durable and visible, atomically.
    pub fn commit(self) -> Result<()> {
        {
            let mut meta = self.txn.open_table(META).map_err(dberr)?;
            meta.insert(META_NEXT_SEQ, self.next_seq).map_err(dberr)?;
            meta.insert(META_MAX_COUNTER, self.max_counter).map_err(dberr)?;
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
        let value = entries
            .get(key)
            .map_err(dberr)?
            .ok_or_else(|| bad(format!("seq {s} points to missing \"{}\"", key.escape_ascii())))?;
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
    push_range(&mut out, entries.range::<&[u8]>(lo.as_slice()..hi.as_slice()).map_err(dberr)?)?;
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
            }),
            racy: true,
        };
        let kinds = [
            Kind::File { size: 3, hash: [9; 32] },
            Kind::Dir,
            Kind::Symlink { target: b"../t\x80".to_vec() },
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
    fn changes_since_returns_every_later_put() {
        let (_dir, store) = open();
        let mut seqs = Vec::new();
        for i in 0..50u64 {
            // Revisit paths so some puts replace earlier ones.
            let path = p(format!("f{}", i % 17).as_bytes());
            seqs.push((store.put(&path, &mut file(&i.to_le_bytes(), i + 1)).unwrap(), path));
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
            &b"a"[..], b"a/b", b"a/b/c", b"a.txt", b"a0", b"ab", b"a/\xff", b"b", b"b/a",
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
        assert_eq!(paths(&txn.changes_since(base).unwrap()), [b"x".to_vec(), b"y".to_vec()]);
        txn.commit().unwrap();
        assert!(snap.get(&p(b"x")).unwrap().is_none());
        assert_eq!(paths(&store.changes_since(base).unwrap()), [b"x".to_vec(), b"y".to_vec()]);
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
    fn corrupt_record_is_an_error() {
        let (_dir, store) = open();
        store.put(&p(b"ok"), &mut file(b"ok", 1)).unwrap();
        {
            let txn = store.db.begin_write().unwrap();
            {
                let mut t = txn.open_table(ENTRIES).unwrap();
                t.insert(&b"bad"[..], &b"\xff\xff\xff"[..]).unwrap();
                t.insert(&b"/abs"[..], encode(&file(b"", 1)).unwrap().as_slice()).unwrap();
            }
            txn.commit().unwrap();
        }
        assert!(matches!(store.get(&p(b"bad")), Err(Error::BadIndex { .. })));
        assert!(matches!(store.iter_prefix(&RelPath::root()), Err(Error::BadIndex { .. })));
        assert!(store.get(&p(b"ok")).unwrap().is_some());
    }

    impl Entry {
        fn with_seq(mut self, seq: u64) -> Entry {
            self.seq = seq;
            self
        }
    }
}
