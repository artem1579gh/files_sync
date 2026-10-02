//! How a replica's symlink policy treats one symlink (design §4.3).
//!
//! [`classify`] covers every row of the §4.3 table except the per-replica
//! settings: `--munge-links` changes only how targets are stored
//! ([`super::munge`]), and `-K` adoption needs the peer's entry (T17).

pub use crate::config::SymlinkPolicy;
use crate::fs::{FileKind, RelPath};
use crate::index::UnmanagedReason;
use crate::symlink::safety::is_unsafe;

/// What the scanner does with a symlink.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Treatment {
    /// Index it as `Kind::Symlink` with its canonical target.
    AsSymlink,
    /// Index the referent (File or Dir) in its place, with `via_link` set.
    Follow,
    /// Index it as `Kind::Unmanaged(reason)`.
    Unmanaged(UnmanagedReason),
}

/// Whether [`classify`] needs the referent's kind for this link, so the
/// scanner can skip the extra `stat` otherwise.
pub fn needs_referent(policy: SymlinkPolicy, link: &RelPath, target: &[u8]) -> bool {
    match policy {
        SymlinkPolicy::Skip | SymlinkPolicy::Links | SymlinkPolicy::SafeLinks => false,
        SymlinkPolicy::CopyLinks | SymlinkPolicy::CopyDirlinks => true,
        SymlinkPolicy::CopyUnsafeLinks => is_unsafe(link, target),
    }
}

/// Classifies the symlink at `link` (relative to the root) under `policy`.
///
/// - `target` is the **canonical** (unmunged) target, so both replicas classify
///   a link the same way whatever their munge settings.
/// - `referent` is the kind of the object the on-disk link resolves to after
///   following every link (`stat`, not `lstat`); `None` if it does not resolve
///   (dangling). It is ignored when [`needs_referent`] is false. A referent
///   that resolves to an ancestor directory (a loop) is the scanner's to
///   detect, from its (dev, ino) stack (design §4.5), as is `ELOOP`.
pub fn classify(
    policy: SymlinkPolicy,
    link: &RelPath,
    target: &[u8],
    referent: Option<FileKind>,
) -> Treatment {
    use SymlinkPolicy::*;
    match policy {
        // No `-l`: rsync skips every symlink.
        Skip => Treatment::Unmanaged(UnmanagedReason::IgnoredLink),
        // `-l`: verbatim, dangling allowed.
        Links => Treatment::AsSymlink,
        // `-L`: always the referent.
        CopyLinks => follow(referent),
        // `--copy-unsafe-links`: unsafe links like `-L`, safe ones like `-l`.
        CopyUnsafeLinks if is_unsafe(link, target) => follow(referent),
        CopyUnsafeLinks => Treatment::AsSymlink,
        // `--safe-links`: unsafe links are ignored.
        SafeLinks if is_unsafe(link, target) => Treatment::Unmanaged(UnmanagedReason::IgnoredLink),
        SafeLinks => Treatment::AsSymlink,
        // `-k`: links to directories become the directory; the rest like `-l`
        // (rsync's `link_stat` keeps a dangling link as a link).
        CopyDirlinks if referent == Some(FileKind::Dir) => Treatment::Follow,
        CopyDirlinks => Treatment::AsSymlink,
    }
}

/// Following the link: rsync copies the referent, or reports "symlink has no
/// referent" and skips it. FIFOs, sockets and devices are never synced.
fn follow(referent: Option<FileKind>) -> Treatment {
    match referent {
        Some(FileKind::File | FileKind::Dir) => Treatment::Follow,
        Some(FileKind::Special) => Treatment::Unmanaged(UnmanagedReason::Special),
        // `stat` follows every link, so a symlink referent cannot happen;
        // treat it as unresolvable.
        Some(FileKind::Symlink) | None => Treatment::Unmanaged(UnmanagedReason::Dangling),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use SymlinkPolicy::*;

    const S: Treatment = Treatment::AsSymlink;
    const F: Treatment = Treatment::Follow;
    const I: Treatment = Treatment::Unmanaged(UnmanagedReason::IgnoredLink);
    const D: Treatment = Treatment::Unmanaged(UnmanagedReason::Dangling);
    const X: Treatment = Treatment::Unmanaged(UnmanagedReason::Special);

    /// Columns: safe target → {file, dir, dangling, special}, then unsafe
    /// target → {file, dir, dangling, special}.
    const MATRIX: &[(SymlinkPolicy, [Treatment; 8])] = &[
        (Skip, [I, I, I, I, I, I, I, I]),
        (Links, [S, S, S, S, S, S, S, S]),
        (CopyLinks, [F, F, D, X, F, F, D, X]),
        (CopyUnsafeLinks, [S, S, S, S, F, F, D, X]),
        (SafeLinks, [S, S, S, S, I, I, I, I]),
        (CopyDirlinks, [S, F, S, S, S, F, S, S]),
    ];

    const REFERENTS: [Option<FileKind>; 4] = [
        Some(FileKind::File),
        Some(FileKind::Dir),
        None,
        Some(FileKind::Special),
    ];

    #[test]
    fn matrix() {
        let link = RelPath::new("a/l").unwrap();
        // Several spellings of safe and unsafe targets for a link in `a/`.
        let safe: [&[u8]; 3] = [b"x", b"../b/x", b"./y//z"];
        let unsafe_: [&[u8]; 3] = [b"/abs", b"../../x", b"y/../z"];
        let all = [
            SymlinkPolicy::Skip,
            Links,
            CopyLinks,
            CopyUnsafeLinks,
            SafeLinks,
            CopyDirlinks,
        ];
        assert_eq!(MATRIX.len(), all.len());
        for &(policy, row) in MATRIX {
            for (col, &want) in row.iter().enumerate() {
                let targets = if col < 4 { safe } else { unsafe_ };
                let referent = REFERENTS[col % 4];
                for target in targets {
                    let got = classify(policy, &link, target, referent);
                    assert_eq!(
                        got,
                        want,
                        "{policy:?} {} -> {referent:?}",
                        target.escape_ascii()
                    );
                    // The referent only matters when `needs_referent` says so.
                    if !needs_referent(policy, &link, target) {
                        for other in REFERENTS {
                            assert_eq!(classify(policy, &link, target, other), want);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn safety_depends_on_link_depth() {
        let target = b"../x";
        let top = RelPath::new("l").unwrap();
        let nested = RelPath::new("a/l").unwrap();
        let file = Some(FileKind::File);
        assert_eq!(classify(SafeLinks, &top, target, file), I);
        assert_eq!(classify(SafeLinks, &nested, target, file), S);
        assert_eq!(classify(CopyUnsafeLinks, &top, target, file), F);
        assert_eq!(classify(CopyUnsafeLinks, &nested, target, file), S);
        assert!(needs_referent(CopyUnsafeLinks, &top, target));
        assert!(!needs_referent(CopyUnsafeLinks, &nested, target));
    }
}
