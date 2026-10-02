//! Version vectors (design §3, §6.1).
//!
//! A vector maps replica IDs to counters. It is kept canonical: sorted by
//! replica ID, one entry per replica, no zero counters. Two vectors are
//! therefore equal as values exactly when they compare [`Ord4::Equal`].

use std::cmp::Ordering;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smallvec::SmallVec;

use crate::config::ReplicaId;

/// The largest counter accepted from storage or the wire.
///
/// Local bumps add 1 to the largest counter seen, so with every input at most
/// 2^62 a counter cannot overflow `u64` within any realistic number of bumps,
/// and [`VersionVector::bump`] needs no error path.
pub const MAX_COUNTER: u64 = 1 << 62;

/// Result of comparing two version vectors: they are partially ordered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Ord4 {
    Equal,
    /// `self` has seen everything `other` has, and more.
    Dominates,
    /// `other` has seen everything `self` has, and more.
    Dominated,
    /// Each side has seen something the other has not.
    Concurrent,
}

impl Ord4 {
    /// The result of the comparison with the operands swapped.
    pub fn reverse(self) -> Ord4 {
        match self {
            Ord4::Dominates => Ord4::Dominated,
            Ord4::Dominated => Ord4::Dominates,
            o => o,
        }
    }
}

/// Inline capacity: one counter per replica of a pair.
type Counters = SmallVec<[(ReplicaId, u64); 2]>;

/// A version vector: `(replica, counter)` pairs, sorted by replica, with no
/// zero counters (a missing replica counts as 0).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct VersionVector(Counters);

impl VersionVector {
    /// The empty vector (nothing seen).
    pub fn new() -> VersionVector {
        VersionVector::default()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The counter for `id` (0 if absent).
    pub fn get(&self, id: ReplicaId) -> u64 {
        match self.0.binary_search_by_key(&id, |&(r, _)| r) {
            Ok(i) => self.0[i].1,
            Err(_) => 0,
        }
    }

    /// The largest counter in the vector (0 if empty).
    pub fn max_counter(&self) -> u64 {
        self.0.iter().map(|&(_, c)| c).max().unwrap_or(0)
    }

    /// The `(replica, counter)` pairs, sorted by replica.
    pub fn iter(&self) -> impl Iterator<Item = (ReplicaId, u64)> + '_ {
        self.0.iter().copied()
    }

    /// Records a local change on replica `id`: its counter becomes
    /// `max(all counters) + 1` (design §3). Returns the new counter. The
    /// result strictly dominates the old vector.
    pub fn bump(&mut self, id: ReplicaId) -> u64 {
        self.bump_after(id, 0)
    }

    /// Like [`bump`](Self::bump), but the new counter is also above `floor`.
    /// Pass the replica's Lamport clock (the store's `max_counter`) so a local
    /// change also outranks every counter the replica has ever seen.
    pub fn bump_after(&mut self, id: ReplicaId, floor: u64) -> u64 {
        let c = self
            .max_counter()
            .max(floor)
            .checked_add(1)
            .expect("version counter overflow");
        self.set(id, c);
        c
    }

    /// Sets the counter for `id`; 0 removes it.
    pub fn set(&mut self, id: ReplicaId, counter: u64) {
        match self.0.binary_search_by_key(&id, |&(r, _)| r) {
            Ok(i) if counter == 0 => {
                self.0.remove(i);
            }
            Ok(i) => self.0[i].1 = counter,
            Err(_) if counter == 0 => {}
            Err(i) => self.0.insert(i, (id, counter)),
        }
    }

    /// The pointwise maximum of `self` and `other` (the least upper bound).
    pub fn merge(&self, other: &VersionVector) -> VersionVector {
        let mut out = Counters::with_capacity(self.0.len().max(other.0.len()));
        let (mut a, mut b) = (self.0.iter().peekable(), other.0.iter().peekable());
        loop {
            let next = match (a.peek(), b.peek()) {
                (None, None) => break,
                (Some(_), None) => *a.next().unwrap(),
                (None, Some(_)) => *b.next().unwrap(),
                (Some(&&(ra, ca)), Some(&&(rb, cb))) => match ra.cmp(&rb) {
                    Ordering::Less => *a.next().unwrap(),
                    Ordering::Greater => *b.next().unwrap(),
                    Ordering::Equal => {
                        a.next();
                        b.next();
                        (ra, ca.max(cb))
                    }
                },
            };
            out.push(next);
        }
        VersionVector(out)
    }

    /// Compares `self` with `other` in the causal partial order.
    pub fn compare(&self, other: &VersionVector) -> Ord4 {
        // `greater`: some counter of self is above other's; `less`: vice versa.
        let (mut greater, mut less) = (false, false);
        let (mut a, mut b) = (self.0.iter().peekable(), other.0.iter().peekable());
        loop {
            match (a.peek(), b.peek()) {
                (None, None) => break,
                (Some(_), None) => {
                    greater = true;
                    break;
                }
                (None, Some(_)) => {
                    less = true;
                    break;
                }
                (Some(&&(ra, ca)), Some(&&(rb, cb))) => match ra.cmp(&rb) {
                    Ordering::Less => {
                        greater = true;
                        a.next();
                    }
                    Ordering::Greater => {
                        less = true;
                        b.next();
                    }
                    Ordering::Equal => {
                        greater |= ca > cb;
                        less |= ca < cb;
                        a.next();
                        b.next();
                    }
                },
            }
            if greater && less {
                break;
            }
        }
        match (greater, less) {
            (false, false) => Ord4::Equal,
            (true, false) => Ord4::Dominates,
            (false, true) => Ord4::Dominated,
            (true, true) => Ord4::Concurrent,
        }
    }

    /// `self` has seen everything `other` has (`Equal` or `Dominates`).
    pub fn descends(&self, other: &VersionVector) -> bool {
        matches!(self.compare(other), Ord4::Equal | Ord4::Dominates)
    }

    /// Checks the canonical form; used when decoding.
    fn validate(counters: &[(ReplicaId, u64)]) -> Result<(), &'static str> {
        if counters.windows(2).any(|w| w[0].0 >= w[1].0) {
            return Err("replicas not strictly sorted");
        }
        if counters.iter().any(|&(_, c)| c == 0) {
            return Err("zero counter");
        }
        if counters.iter().any(|&(_, c)| c > MAX_COUNTER) {
            return Err("counter too large");
        }
        Ok(())
    }
}

impl PartialOrd for VersionVector {
    /// `Dominates` is `Greater`; concurrent vectors are unordered.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.compare(other) {
            Ord4::Equal => Some(Ordering::Equal),
            Ord4::Dominates => Some(Ordering::Greater),
            Ord4::Dominated => Some(Ordering::Less),
            Ord4::Concurrent => None,
        }
    }
}

impl FromIterator<(ReplicaId, u64)> for VersionVector {
    /// Builds a canonical vector; duplicate replicas keep their largest counter.
    fn from_iter<I: IntoIterator<Item = (ReplicaId, u64)>>(iter: I) -> Self {
        let mut vv = VersionVector::new();
        for (id, c) in iter {
            let c = c.max(vv.get(id));
            vv.set(id, c);
        }
        vv
    }
}

impl Serialize for VersionVector {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.as_slice().serialize(s)
    }
}

impl<'de> Deserialize<'de> for VersionVector {
    /// Rejects non-canonical input (unsorted, duplicate, zero or oversized
    /// counters), so every decoded vector upholds the invariants.
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let counters = Counters::deserialize(d)?;
        VersionVector::validate(&counters).map_err(serde::de::Error::custom)?;
        Ok(VersionVector(counters))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn r(n: u64) -> ReplicaId {
        ReplicaId(n)
    }

    fn vv(pairs: &[(u64, u64)]) -> VersionVector {
        pairs.iter().map(|&(id, c)| (r(id), c)).collect()
    }

    /// Small IDs and counters, so equal and comparable vectors come up often.
    fn arb_vv() -> impl Strategy<Value = VersionVector> {
        prop::collection::vec((0u64..4, 0u64..4), 0..5)
            .prop_map(|v| v.into_iter().map(|(id, c)| (r(id), c)).collect())
    }

    #[test]
    fn basics() {
        let mut a = VersionVector::new();
        assert!(a.is_empty());
        assert_eq!(a.bump(r(7)), 1);
        assert_eq!(a.bump(r(3)), 2);
        assert_eq!(a.bump(r(7)), 3);
        assert_eq!(a.iter().collect::<Vec<_>>(), [(r(3), 2), (r(7), 3)]);
        assert_eq!(a.get(r(1)), 0);
        assert_eq!(a.max_counter(), 3);
        assert_eq!(a.bump_after(r(1), 10), 11);
        assert_eq!(a, vv(&[(1, 11), (3, 2), (7, 3)]));
        a.set(r(3), 0);
        assert_eq!(a, vv(&[(1, 11), (7, 3)]));
    }

    #[test]
    fn compare_cases() {
        let e = VersionVector::new();
        assert_eq!(e.compare(&e), Ord4::Equal);
        assert_eq!(vv(&[(1, 1)]).compare(&e), Ord4::Dominates);
        assert_eq!(e.compare(&vv(&[(1, 1)])), Ord4::Dominated);
        assert_eq!(vv(&[(1, 2), (2, 1)]).compare(&vv(&[(1, 1), (2, 1)])), Ord4::Dominates);
        assert_eq!(vv(&[(1, 1)]).compare(&vv(&[(2, 1)])), Ord4::Concurrent);
        assert_eq!(vv(&[(1, 2), (2, 1)]).compare(&vv(&[(1, 1), (2, 2)])), Ord4::Concurrent);
        assert_eq!(vv(&[(1, 1), (3, 1)]).compare(&vv(&[(1, 1), (2, 1)])), Ord4::Concurrent);
        assert!(vv(&[(1, 1)]) < vv(&[(1, 1), (2, 1)]));
        assert_eq!(vv(&[(1, 1)]).partial_cmp(&vv(&[(2, 1)])), None);
    }

    #[test]
    fn serde_rejects_non_canonical() {
        let ok = vv(&[(1, 5), (2, 3)]);
        let bytes = postcard::to_stdvec(&ok).unwrap();
        assert_eq!(postcard::from_bytes::<VersionVector>(&bytes).unwrap(), ok);
        for bad in [
            vec![(r(2), 1u64), (r(1), 1)],
            vec![(r(1), 1), (r(1), 2)],
            vec![(r(1), 0)],
            vec![(r(1), MAX_COUNTER + 1)],
        ] {
            let bytes = postcard::to_stdvec(&bad).unwrap();
            assert!(postcard::from_bytes::<VersionVector>(&bytes).is_err(), "{bad:?}");
        }
    }

    proptest! {
        #[test]
        fn compare_is_a_partial_order(a in arb_vv(), b in arb_vv(), c in arb_vv()) {
            // Reflexive.
            prop_assert_eq!(a.compare(&a), Ord4::Equal);
            // Consistent with swapping the operands.
            prop_assert_eq!(a.compare(&b), b.compare(&a).reverse());
            // Antisymmetric: a ≤ b and b ≤ a imply a == b.
            if b.descends(&a) && a.descends(&b) {
                prop_assert_eq!(&a, &b);
            }
            prop_assert_eq!(a.compare(&b) == Ord4::Equal, a == b);
            // Transitive.
            if b.descends(&a) && c.descends(&b) {
                prop_assert!(c.descends(&a));
            }
            if a.compare(&b) == Ord4::Dominated && b.compare(&c) == Ord4::Dominated {
                prop_assert_eq!(a.compare(&c), Ord4::Dominated);
            }
        }

        #[test]
        fn merge_is_a_join(a in arb_vv(), b in arb_vv(), c in arb_vv()) {
            let ab = a.merge(&b);
            prop_assert_eq!(&ab, &b.merge(&a));
            prop_assert_eq!(a.merge(&b).merge(&c), a.merge(&b.merge(&c)));
            prop_assert_eq!(&a.merge(&a), &a);
            prop_assert!(ab.descends(&a));
            prop_assert!(ab.descends(&b));
            // Least upper bound: anything above both is above the merge.
            if c.descends(&a) && c.descends(&b) {
                prop_assert!(c.descends(&ab));
            }
            // Canonical form survives.
            prop_assert!(VersionVector::validate(&ab.0).is_ok());
        }

        #[test]
        fn bump_dominates(a in arb_vv(), b in arb_vv(), id in 0u64..6) {
            let mut x = a.clone();
            let c = x.bump(r(id));
            prop_assert_eq!(x.compare(&a), Ord4::Dominates);
            prop_assert_eq!(c, a.max_counter() + 1);
            prop_assert_eq!(x.max_counter(), c);
            // A bump after merging dominates both inputs.
            let mut m = a.merge(&b);
            m.bump(r(id));
            prop_assert_eq!(m.compare(&a), Ord4::Dominates);
            prop_assert_eq!(m.compare(&b), Ord4::Dominates);
        }

        #[test]
        fn serde_round_trip(a in arb_vv()) {
            let bytes = postcard::to_stdvec(&a).unwrap();
            prop_assert_eq!(postcard::from_bytes::<VersionVector>(&bytes).unwrap(), a);
        }
    }
}
