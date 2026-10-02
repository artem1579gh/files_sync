//! rsync `--munge-links` (design §4.4).
//!
//! A replica with munging on stores `/rsyncd-munged/` + target on disk; the
//! index always holds the canonical (unmunged) target. Munging always
//! prepends the prefix and unmunging strips exactly one, so a canonical target
//! that already starts with the prefix is munged twice and the mapping is a
//! bijection between canonical targets and munged on-disk targets.

/// rsync's `SYMLINK_PREFIX`.
pub const PREFIX: &[u8] = b"/rsyncd-munged/";

/// The on-disk form of the canonical `target`.
pub fn munge(target: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PREFIX.len() + target.len());
    out.extend_from_slice(PREFIX);
    out.extend_from_slice(target);
    out
}

/// Whether an on-disk target carries the munge prefix.
pub fn is_munged(raw: &[u8]) -> bool {
    raw.starts_with(PREFIX)
}

/// The canonical form of the on-disk `raw` target: one prefix stripped. A
/// target without the prefix (a link the user created directly in a munged
/// replica) is returned unchanged; callers that care check [`is_munged`].
pub fn unmunge(raw: &[u8]) -> Vec<u8> {
    raw.strip_prefix(PREFIX).unwrap_or(raw).to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn examples() {
        assert_eq!(munge(b"x"), b"/rsyncd-munged/x");
        assert_eq!(munge(b"/etc/passwd"), b"/rsyncd-munged//etc/passwd");
        assert_eq!(unmunge(b"/rsyncd-munged/../x"), b"../x");
        // Exactly one prefix is stripped.
        assert_eq!(munge(b"/rsyncd-munged/x"), b"/rsyncd-munged//rsyncd-munged/x");
        assert_eq!(unmunge(b"/rsyncd-munged//rsyncd-munged/x"), b"/rsyncd-munged/x");
        // Not munged: unchanged.
        assert!(!is_munged(b"/rsyncd-munge/x"));
        assert_eq!(unmunge(b"/rsyncd-munge/x"), b"/rsyncd-munge/x");
        assert_eq!(unmunge(b"x"), b"x");
        assert!(is_munged(&munge(b"")));
    }

    /// Bytes biased towards prefixes of the munge prefix.
    fn arb_target() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            prop::collection::vec(any::<u8>(), 0..40),
            (0usize..3, prop::collection::vec(any::<u8>(), 0..10)).prop_map(|(n, tail)| {
                let mut v = PREFIX.repeat(n);
                v.extend(tail);
                v
            }),
            (0..PREFIX.len()).prop_map(|n| PREFIX[..n].to_vec()),
        ]
    }

    proptest! {
        #[test]
        fn round_trip(x in arb_target()) {
            let m = munge(&x);
            prop_assert!(is_munged(&m));
            prop_assert_eq!(unmunge(&m), x);
        }

        #[test]
        fn munge_is_injective(x in arb_target(), y in arb_target()) {
            prop_assert_eq!(munge(&x) == munge(&y), x == y);
        }
    }
}
