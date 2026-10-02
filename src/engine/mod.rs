//! Reconciliation, planning, execution and conflict handling (design §6).
//!
//! The engine performs no I/O itself; it talks only to the `Replica` trait.
//! [`reconcile`] compares two index views and decides what to do for each
//! path; [`plan`] turns those decisions into ordered per-replica steps.
pub mod plan;
pub mod reconcile;
#[cfg(test)]
mod sim;

pub use plan::{Phase, PhaseKind, Step, plan};
pub use reconcile::{Action, ActionKind, Resolution, SkipReason, reconcile};

use std::collections::BTreeMap;
use std::ops::Bound;

use crate::config::ReplicaId;
use crate::fs::RelPath;
use crate::index::Entry;

/// One of the two replicas of a pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Side {
    A,
    B,
}

impl Side {
    pub fn other(self) -> Side {
        match self {
            Side::A => Side::B,
            Side::B => Side::A,
        }
    }
}

/// A read-only view of one replica's index, as the engine sees it: entries
/// in the wire form (no [`LocalMeta`](crate::index::LocalMeta) needed).
pub trait IndexView {
    /// The replica the entries belong to.
    fn replica(&self) -> ReplicaId;

    fn get(&self, path: &RelPath) -> Option<&Entry>;

    /// Every entry, in path (byte) order.
    fn entries(&self) -> Box<dyn Iterator<Item = (&RelPath, &Entry)> + '_>;

    /// The entries strictly beneath `path` (all of them for the root), in
    /// path order.
    fn descendants(&self, path: &RelPath) -> Box<dyn Iterator<Item = (&RelPath, &Entry)> + '_>;
}

/// An in-memory [`IndexView`], e.g. built from
/// [`Replica::changes_since(0)`](crate::replica::Replica::changes_since).
#[derive(Clone, Debug)]
pub struct Snapshot {
    replica: ReplicaId,
    entries: BTreeMap<RelPath, Entry>,
}

impl Snapshot {
    /// A later entry for the same path replaces an earlier one.
    pub fn new(
        replica: ReplicaId,
        entries: impl IntoIterator<Item = (RelPath, Entry)>,
    ) -> Snapshot {
        Snapshot {
            replica,
            entries: entries.into_iter().collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl IndexView for Snapshot {
    fn replica(&self) -> ReplicaId {
        self.replica
    }

    fn get(&self, path: &RelPath) -> Option<&Entry> {
        self.entries.get(path)
    }

    fn entries(&self) -> Box<dyn Iterator<Item = (&RelPath, &Entry)> + '_> {
        Box::new(self.entries.iter())
    }

    fn descendants(&self, path: &RelPath) -> Box<dyn Iterator<Item = (&RelPath, &Entry)> + '_> {
        if path.is_root() {
            return Box::new(self.entries.iter().filter(|(p, _)| !p.is_root()));
        }
        // Everything beneath `p` sorts between `p` and `p0` (`'0'` follows
        // `'/'`), mixed with siblings such as `p-x` and `p.x`.
        let mut end = path.as_bytes().to_vec();
        end.push(b'0');
        let path = path.clone();
        Box::new(
            self.entries
                .range((Bound::Excluded(path.clone()), Bound::Unbounded))
                .take_while(move |(p, _)| p.as_bytes() < end.as_slice())
                .filter(move |(p, _)| is_beneath(p, &path)),
        )
    }
}

/// `p` lies strictly beneath `ancestor`.
pub fn is_beneath(p: &RelPath, ancestor: &RelPath) -> bool {
    if ancestor.is_root() {
        return !p.is_root();
    }
    p.as_bytes()
        .strip_prefix(ancestor.as_bytes())
        .is_some_and(|rest| rest.first() == Some(&b'/'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{Kind, VersionVector};

    fn rp(s: &str) -> RelPath {
        RelPath::new(s).unwrap()
    }

    #[test]
    fn descendants_skip_siblings_that_sort_inside() {
        let e = Entry::new(Kind::Dir, 0o755, 0, VersionVector::new());
        let paths = [
            "d", "d!", "d-x", "d.x", "d/a", "d/a/b", "d/z", "d0", "e", "dd/x",
        ];
        let s = Snapshot::new(ReplicaId(1), paths.iter().map(|p| (rp(p), e.clone())));
        let under = |p: &str| {
            s.descendants(&rp(p))
                .map(|(p, _)| String::from_utf8(p.as_bytes().to_vec()).unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(under("d"), ["d/a", "d/a/b", "d/z"]);
        assert_eq!(under("d/a"), ["d/a/b"]);
        assert_eq!(under("e"), Vec::<String>::new());
        assert_eq!(under("").len(), paths.len());
        assert!(is_beneath(&rp("d/a"), &rp("d")));
        assert!(!is_beneath(&rp("d"), &rp("d")));
        assert!(!is_beneath(&rp("dd/x"), &rp("d")));
    }
}
