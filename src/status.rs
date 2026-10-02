//! The state of a pair, as `status <pair>` shows it: index size, pending
//! conflict copies, quarantined old inodes and the last sync time (T18).
//!
//! It is read from the indexes in the pair's state directory, without
//! opening the replicas (which would probe and recover them). A running
//! daemon holds both indexes open, and redb locks them to one process, so
//! the daemon writes the same report to `<pair>/status.toml` after every
//! cycle ([`PairStatus::save`]); [`PairStatus::load`] falls back to it.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::{PairConfig, ReplicaId};
use crate::error::{Error, Result};
use crate::fs::{RelPath, is_conflict_name};
use crate::index::{IndexStore, IntentState, Kind};
use crate::replica::{Housekeeping, LocalReplica};

/// File name of the daemon's status report in the pair's state directory.
pub const STATUS_FILE: &str = "status.toml";

/// One replica's state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaStatus {
    /// Index entries, tombstones included.
    pub entries: u64,
    pub tombstones: u64,
    /// Conflict copies in the index (live entries with a conflict name,
    /// design §6.2) that nobody has removed or renamed yet.
    pub conflicts: Vec<String>,
    /// Old inodes waiting in quarantine (§5.3 step 4(f)); without the
    /// daemon, the intents a run left in that state (swept by the next one).
    pub quarantined: u64,
    /// Other unfinished commit intents (left by a crash; replayed when the
    /// replica is opened next).
    pub unfinished: u64,
    /// End of the last sync cycle with the peer (ns since the Unix epoch).
    pub last_sync_ns: Option<i64>,
    /// The replica runs in a `serve` process at this address; its state is
    /// not known here (see `status` on its host).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
}

impl ReplicaStatus {
    /// Reads `index`. `quarantined`, when given, replaces the count of
    /// quarantined intents (the live quarantine of a running replica).
    pub fn from_index(
        index: &IndexStore,
        peer: ReplicaId,
        quarantined: Option<u64>,
    ) -> Result<ReplicaStatus> {
        let mut st = ReplicaStatus::default();
        let txn = index.read()?;
        for (path, entry) in txn.iter_prefix(&RelPath::root())? {
            st.entries += 1;
            match entry.kind {
                Kind::Tombstone => st.tombstones += 1,
                Kind::Unmanaged(_) => {}
                _ if path.name().is_some_and(is_conflict_name) => {
                    st.conflicts.push(path.to_string());
                }
                _ => {}
            }
        }
        st.last_sync_ns = txn
            .peers()?
            .into_iter()
            .find_map(|(id, at)| (id == peer).then_some(at));
        for (_, intent) in index.journal().pending()? {
            if intent.state == IntentState::Quarantined {
                st.quarantined += 1;
            } else {
                st.unfinished += 1;
            }
        }
        if let Some(q) = quarantined {
            st.quarantined = q;
        }
        Ok(st)
    }

    /// A replica served elsewhere, at `addr`.
    pub fn remote(addr: &str) -> ReplicaStatus {
        ReplicaStatus {
            remote: Some(addr.to_owned()),
            ..ReplicaStatus::default()
        }
    }

    /// The state of an open replica, with its live quarantine.
    pub fn of(replica: &LocalReplica, peer: ReplicaId) -> Result<ReplicaStatus> {
        let q = replica.quarantine().len() as u64;
        ReplicaStatus::from_index(replica.index(), peer, Some(q))
    }
}

/// A pair's state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairStatus {
    /// When it was taken (ns since the Unix epoch).
    pub taken_ns: i64,
    /// Read from a running daemon's report: its indexes were locked.
    #[serde(skip)]
    pub from_daemon: bool,
    /// Replicas A and B, in config order.
    pub replicas: [ReplicaStatus; 2],
}

impl PairStatus {
    /// The state of two open replicas.
    pub fn of(a: &dyn Housekeeping, b: &dyn Housekeeping) -> Result<PairStatus> {
        Ok(PairStatus {
            taken_ns: now_ns(),
            from_daemon: false,
            replicas: [a.status(b.id())?, b.status(a.id())?],
        })
    }

    /// The state of the pair `cfg`, from its indexes in `pair_dir`; or, if
    /// a daemon holds them, from the report it saved. A missing index (never
    /// synced) counts as empty.
    pub fn load(cfg: &PairConfig, pair_dir: &Path) -> Result<PairStatus> {
        let ids = [cfg.replicas[0].id, cfg.replicas[1].id];
        let mut replicas: [ReplicaStatus; 2] = Default::default();
        for (i, st) in replicas.iter_mut().enumerate() {
            if let Some(addr) = &cfg.replicas[i].remote {
                *st = ReplicaStatus::remote(addr);
                continue;
            }
            let path = IndexStore::path_for(pair_dir, ids[i]);
            if !path.exists() {
                continue;
            }
            match IndexStore::open(&path, ids[i]) {
                Ok(index) => *st = ReplicaStatus::from_index(&index, ids[1 - i], None)?,
                Err(Error::Db(redb::Error::DatabaseAlreadyOpen)) => {
                    return Self::load_report(pair_dir);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(PairStatus {
            taken_ns: now_ns(),
            from_daemon: false,
            replicas,
        })
    }

    fn load_report(pair_dir: &Path) -> Result<PairStatus> {
        let path = pair_dir.join(STATUS_FILE);
        let text = std::fs::read_to_string(&path).map_err(|e| {
            Error::io(
                format!(
                    "index in use (a daemon?), and no report at {}",
                    path.display()
                ),
                e,
            )
        })?;
        let mut st: PairStatus = toml::from_str(&text).map_err(|e| Error::InvalidConfig {
            path: path.clone(),
            reason: e.to_string(),
        })?;
        st.from_daemon = true;
        Ok(st)
    }

    /// Saves the report to `<pair_dir>/status.toml` (atomically).
    pub fn save(&self, pair_dir: &Path) -> Result<()> {
        let text = toml::to_string(self)?;
        crate::config::write_atomic(&pair_dir.join(STATUS_FILE), text.as_bytes(), true)
    }
}

fn now_ns() -> i64 {
    jiff::Timestamp::now().as_nanosecond() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;

    #[test]
    fn counts_and_falls_back_to_the_daemon_report() {
        let (da, db, state) = (
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        );
        let cfg = PairConfig::new("p", [da.path().to_path_buf(), db.path().to_path_buf()]).unwrap();
        // Never synced: empty.
        let st = PairStatus::load(&cfg, state.path()).unwrap();
        assert_eq!(
            st.replicas,
            [ReplicaStatus::default(), ReplicaStatus::default()]
        );

        std::fs::write(da.path().join("f"), b"x").unwrap();
        std::fs::write(da.path().join("g"), b"y").unwrap();
        std::fs::write(
            da.path().join("f.sync-conflict-20260101-000000-aaaa000"),
            b"z",
        )
        .unwrap();
        let open = |i: usize| {
            LocalReplica::open(&cfg.replicas[i], state.path())
                .unwrap()
                .quarantine_grace(std::time::Duration::from_secs(3600))
        };
        let (mut a, mut b) = (open(0), open(1));
        Engine::new().sync_once(&mut a, &mut b).unwrap();
        std::fs::remove_file(db.path().join("g")).unwrap();
        std::fs::write(db.path().join("f"), b"new").unwrap();
        Engine::new().sync_once(&mut a, &mut b).unwrap();

        let live = PairStatus::of(&a, &b).unwrap();
        for (i, st) in live.replicas.iter().enumerate() {
            assert_eq!((st.entries, st.tombstones), (3, 1), "{i}");
            assert_eq!(st.conflicts, ["f.sync-conflict-20260101-000000-aaaa000"]);
            assert!(st.last_sync_ns.is_some());
        }
        // A's replaced and deleted old inodes (nothing swept them yet).
        assert_eq!(
            (live.replicas[0].quarantined, live.replicas[1].quarantined),
            (2, 0)
        );

        // The indexes are locked while the replicas are open.
        assert!(PairStatus::load(&cfg, state.path()).is_err());
        live.save(state.path()).unwrap();
        let saved = PairStatus::load(&cfg, state.path()).unwrap();
        assert!(saved.from_daemon);
        assert_eq!(saved.replicas, live.replicas);

        // Closed: read from the indexes; the quarantined intents are left.
        let quarantined = [a.quarantine().len() as u64, b.quarantine().len() as u64];
        drop((a, b));
        let st = PairStatus::load(&cfg, state.path()).unwrap();
        assert!(!st.from_daemon);
        for ((st, live), q) in st.replicas.iter().zip(&live.replicas).zip(quarantined) {
            assert_eq!(st.quarantined, q);
            assert_eq!(st.last_sync_ns, live.last_sync_ns);
            assert_eq!(st.entries, 3);
        }
    }
}
