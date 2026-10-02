//! Index entries (design §3).
//!
//! [`Entry`]'s serde form is the **wire** form: it leaves out [`LocalMeta`],
//! which must never reach a peer. The index store persists `LocalMeta`
//! separately, next to the entry.

use serde::{Deserialize, Serialize};

use crate::index::vv::VersionVector;

/// One path's synced state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub kind: Kind,
    /// Permission bits only; setuid/setgid stripped (see [`sync_mode`]).
    pub mode: u32,
    /// Synced; set with `futimens` on the temp fd before the commit.
    pub mtime_ns: i64,
    pub vv: VersionVector,
    /// Local change sequence number, for incremental index exchange. Assigned
    /// by the store on every put.
    pub seq: u64,
    /// Local-only facts about the object on disk. Never sent to peers.
    #[serde(skip)]
    pub local: LocalMeta,
}

/// What a path holds.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Kind {
    File { size: u64, hash: [u8; 32] },
    Dir,
    /// Canonical (unmunged) target bytes.
    Symlink { target: Vec<u8> },
    /// Deleted; the entry keeps its version vector.
    Tombstone,
    /// Present but not synced. Not a deletion: the peer keeps its copy and
    /// incoming changes to the path are skipped (design §3).
    Unmanaged(UnmanagedReason),
}

/// Why a path is [`Kind::Unmanaged`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnmanagedReason {
    /// A symlink the policy does not sync (no `-l`, or unsafe under `--safe-links`).
    IgnoredLink,
    /// A followed symlink with no referent.
    Dangling,
    /// A followed directory symlink that resolves to one of its ancestors.
    Loop,
    /// A FIFO, socket or device node.
    Special,
}

/// Local-only metadata of the object on disk (design §3).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalMeta {
    pub dev: u64,
    pub ino: u64,
    pub ctime_ns: i64,
    /// Mount ID (`statx` `stx_mnt_id`); 0 if unknown.
    pub mnt_id: u64,
    /// The symlink target as stored on disk, if it differs from the canonical
    /// target (a munged link, design §4.4).
    pub raw_target: Option<Vec<u8>>,
    /// Set when the entry describes the referent of a followed symlink.
    pub via_link: Option<LinkInfo>,
    /// The ctime was within one timestamp tick of the scan start: always
    /// rehash on the next scan.
    pub racy: bool,
}

/// The followed symlink behind an entry indexed as its referent (design §4.3, §4.5).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkInfo {
    /// The link inode itself.
    pub ino: u64,
    pub ctime_ns: i64,
    /// The link's target bytes as read from disk.
    pub raw_target: Vec<u8>,
    /// The referent lies outside the replica root (read-only unless adopted
    /// with `keep_dirlinks_unsafe`; watched with extra watches).
    pub out_of_tree: bool,
    /// A link to a directory kept as that directory by `-K` (design §4.3),
    /// not followed by the symlink policy. The entry's (dev, ino) pin the
    /// directory: the adoption holds only while the link resolves to it.
    pub adopted: bool,
}

/// The mode bits that are synced: permissions plus sticky, never setuid/setgid.
pub fn sync_mode(st_mode: u32) -> u32 {
    st_mode & 0o1777
}

impl Kind {
    /// File, Dir or Symlink: an object that is synced.
    pub fn is_live(&self) -> bool {
        matches!(self, Kind::File { .. } | Kind::Dir | Kind::Symlink { .. })
    }
}

impl Entry {
    /// A new entry with default local metadata and seq 0 (the store assigns it).
    pub fn new(kind: Kind, mode: u32, mtime_ns: i64, vv: VersionVector) -> Entry {
        Entry {
            kind,
            mode,
            mtime_ns,
            vv,
            seq: 0,
            local: LocalMeta::default(),
        }
    }

    pub fn is_live(&self) -> bool {
        self.kind.is_live()
    }

    pub fn is_tombstone(&self) -> bool {
        self.kind == Kind::Tombstone
    }

    pub fn is_unmanaged(&self) -> bool {
        matches!(self.kind, Kind::Unmanaged(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReplicaId;

    fn sample() -> Entry {
        let mut vv = VersionVector::new();
        vv.bump(ReplicaId(42));
        let mut e = Entry::new(
            Kind::Symlink {
                target: b"../x\xff".to_vec(),
            },
            0o777,
            -5,
            vv,
        );
        e.seq = 9;
        e.local = LocalMeta {
            dev: 1,
            ino: 2,
            ctime_ns: 3,
            mnt_id: 4,
            raw_target: Some(b"/rsyncd-munged/../x\xff".to_vec()),
            via_link: None,
            racy: true,
        };
        e
    }

    #[test]
    fn wire_form_drops_local_meta() {
        let e = sample();
        let bytes = postcard::to_stdvec(&e).unwrap();
        let back: Entry = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back.local, LocalMeta::default());
        assert_eq!(Entry { local: e.local.clone(), ..back }, e);
        // The raw target bytes are not in the encoding at all.
        assert!(!bytes.windows(14).any(|w| w == b"/rsyncd-munged"));
    }

    #[test]
    fn kinds() {
        let vv = VersionVector::new();
        let f = Entry::new(Kind::File { size: 1, hash: [7; 32] }, 0o644, 0, vv.clone());
        assert!(f.is_live() && !f.is_tombstone() && !f.is_unmanaged());
        let t = Entry::new(Kind::Tombstone, 0, 0, vv.clone());
        assert!(!t.is_live() && t.is_tombstone());
        let u = Entry::new(Kind::Unmanaged(UnmanagedReason::Loop), 0, 0, vv);
        assert!(!u.is_live() && u.is_unmanaged());
        assert_eq!(sync_mode(0o106755), 0o755);
        assert_eq!(sync_mode(0o41777), 0o1777);
    }
}
