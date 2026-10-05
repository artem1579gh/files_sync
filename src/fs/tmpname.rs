//! Reserved names and conflict-copy names (design §2, §5.3, §6.2).
//!
//! Every name starting with [`RESERVED_PREFIX`] belongs to us: temp files
//! (`.~fsync.<id>`), quarantined old inodes (`.~fsync.old.<id>`), files
//! being deleted (`.~fsync.del.<id>`), capability probes
//! (`.~fsync.probe.*`), the root marker (`.~fsync.root.<replica>`,
//! [`root_marker`]) and the trash directory ([`TRASH_DIR`]). The scanner and
//! watcher ignore them all.
//!
//! [`conflict_name`] lives here rather than in `engine/conflict.rs` because
//! `fs::commit` needs it too and `fs` must not depend on the engine.

use std::fmt;

use jiff::civil::DateTime;

use crate::config::ReplicaId;
use crate::error::{Error, Result};

/// Prefix of every name we create for our own bookkeeping.
pub const RESERVED_PREFIX: &[u8] = b".~fsync.";

/// Longest file name Linux filesystems accept.
pub const NAME_MAX: usize = 255;

/// True for names in the reserved `.~fsync.` namespace.
pub fn is_reserved(name: &[u8]) -> bool {
    name.starts_with(RESERVED_PREFIX)
}

/// Prefix of the root marker's name ([`root_marker`]).
pub const ROOT_MARKER_PREFIX: &[u8] = b".~fsync.root.";

/// `.~fsync.root.<replica>`: the file at the top of a replica root that
/// shows it is that replica's root (design §5.1). A replica refuses to sync
/// a root without it once it has been made, so an empty directory in its
/// place (a disk that is not mounted) is not taken for a replica whose
/// files were all deleted. One per replica ID, so one directory can be a
/// replica of several pairs.
pub fn root_marker(replica: ReplicaId) -> Vec<u8> {
    let mut name = ROOT_MARKER_PREFIX.to_vec();
    name.extend_from_slice(replica.to_string().as_bytes());
    name
}

/// True for the name of any replica's root marker ([`root_marker`]).
pub fn is_root_marker(name: &[u8]) -> bool {
    name.starts_with(ROOT_MARKER_PREFIX)
}

/// The trash directory at the top of a root (design §5.3 step 4(f), T31):
/// with a replica's `trash_days` set, the old files its commits replace or
/// delete are moved here instead of being unlinked, as [`trash_name`].
pub const TRASH_DIR: &[u8] = b".~fsync.trash";

/// `stem~YYYYMMDD-HHMMSS.ext`: the name a replaced or deleted object `name`
/// gets in the trash, `now` being the local time it was moved there. The
/// extension is split as in [`conflict_name`].
pub fn trash_name(name: &[u8], split_ext: bool, now: DateTime) -> Vec<u8> {
    let marker = format!(
        "~{:04}{:02}{:02}-{:02}{:02}{:02}",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    );
    with_marker(name, split_ext, &marker)
}

/// Random identifier of one temp, quarantine or delete name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TmpId(pub u64);

impl TmpId {
    pub fn random() -> Result<TmpId> {
        let mut buf = [0u8; 8];
        let mut filled = 0;
        while filled < buf.len() {
            match rustix::rand::getrandom(&mut buf[filled..], rustix::rand::GetRandomFlags::empty())
            {
                Ok(n) => filled += n,
                Err(rustix::io::Errno::INTR) => {}
                Err(e) => return Err(Error::io("getrandom", e.into())),
            }
        }
        Ok(TmpId(u64::from_le_bytes(buf)))
    }
}

impl fmt::Display for TmpId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// What a reserved name is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TmpKind {
    /// `.~fsync.<id>`: new content staged for a commit.
    Temp,
    /// `.~fsync.old.<id>`: a replaced inode in quarantine (§5.3 step 4(f)).
    Old,
    /// `.~fsync.del.<id>`: an object being deleted (§5.6).
    Del,
}

impl TmpKind {
    fn infix(self) -> &'static str {
        match self {
            TmpKind::Temp => "",
            TmpKind::Old => "old.",
            TmpKind::Del => "del.",
        }
    }
}

/// `.~fsync.<id>`, `.~fsync.old.<id>` or `.~fsync.del.<id>`.
pub fn name(kind: TmpKind, id: TmpId) -> Vec<u8> {
    format!(".~fsync.{}{id}", kind.infix()).into_bytes()
}

pub fn tmp(id: TmpId) -> Vec<u8> {
    name(TmpKind::Temp, id)
}

pub fn old(id: TmpId) -> Vec<u8> {
    name(TmpKind::Old, id)
}

pub fn del(id: TmpId) -> Vec<u8> {
    name(TmpKind::Del, id)
}

/// The inverse of [`name`]; `None` for any other name (probe names included).
pub fn parse(name: &[u8]) -> Option<(TmpKind, TmpId)> {
    let rest = name.strip_prefix(RESERVED_PREFIX)?;
    let (kind, hex) = if let Some(hex) = rest.strip_prefix(b"old.") {
        (TmpKind::Old, hex)
    } else if let Some(hex) = rest.strip_prefix(b"del.") {
        (TmpKind::Del, hex)
    } else {
        (TmpKind::Temp, rest)
    };
    if hex.len() != 16 || !hex.iter().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let id = u64::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?;
    Some((kind, TmpId(id)))
}

/// Whether `name` is a conflict copy's name ([`conflict_name`]): it holds
/// `.sync-conflict-YYYYMMDD-HHMMSS-<ID7>`, followed by its end or a `.`.
pub fn is_conflict_name(name: &[u8]) -> bool {
    const MARKER: &[u8] = b".sync-conflict-";
    let digits = |b: &[u8]| b.iter().all(u8::is_ascii_digit);
    let hex = |b: &[u8]| b.iter().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'));
    (0..name.len()).any(|i| {
        let Some(rest) = name[i..].strip_prefix(MARKER) else {
            return false;
        };
        rest.len() >= 23
            && digits(&rest[..8])
            && rest[8] == b'-'
            && digits(&rest[9..15])
            && rest[15] == b'-'
            && hex(&rest[16..23])
            && matches!(rest.get(23), None | Some(b'.'))
    })
}

/// `stem.sync-conflict-YYYYMMDD-HHMMSS-<ID7>.ext` (design §6.2).
///
/// `split_ext` is false for directories and symlinks, which never get an
/// extension split. A name has an extension when it has a `.` that is neither
/// its first nor its last byte; the extension is what follows the last one.
/// `now` is the local wall-clock time. If the result would exceed
/// [`NAME_MAX`], the stem is shortened.
pub fn conflict_name(name: &[u8], split_ext: bool, now: DateTime, replica: ReplicaId) -> Vec<u8> {
    let id = replica.to_string();
    let marker = format!(
        ".sync-conflict-{:04}{:02}{:02}-{:02}{:02}{:02}-{}",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        &id[..7]
    );
    with_marker(name, split_ext, &marker)
}

/// `name` with `marker` inserted before its extension (if `split_ext`),
/// shortened to fit [`NAME_MAX`].
fn with_marker(name: &[u8], split_ext: bool, marker: &str) -> Vec<u8> {
    let dot = name
        .iter()
        .rposition(|&b| b == b'.')
        .filter(|&i| split_ext && i > 0 && i + 1 < name.len());
    let (mut stem, mut ext) = match dot {
        Some(i) => (&name[..i], &name[i..]),
        None => (name, &[][..]),
    };
    // An extension too long to keep alongside the marker is not one.
    if ext.len() + marker.len() + 1 > NAME_MAX {
        (stem, ext) = (name, &[][..]);
    }
    let room = NAME_MAX - marker.len() - ext.len();
    stem = &stem[..stem.len().min(room)];

    let mut out = Vec::with_capacity(stem.len() + marker.len() + ext.len());
    out.extend_from_slice(stem);
    out.extend_from_slice(marker.as_bytes());
    out.extend_from_slice(ext);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::civil::date;

    #[test]
    fn reserved_names_round_trip() {
        let id = TmpId(0x0123_4567_89ab_cdef);
        assert_eq!(tmp(id), b".~fsync.0123456789abcdef");
        assert_eq!(old(id), b".~fsync.old.0123456789abcdef");
        assert_eq!(del(id), b".~fsync.del.0123456789abcdef");
        for kind in [TmpKind::Temp, TmpKind::Old, TmpKind::Del] {
            let n = name(kind, id);
            assert!(is_reserved(&n));
            assert_eq!(parse(&n), Some((kind, id)));
        }
        assert_ne!(TmpId::random().unwrap(), TmpId::random().unwrap());
    }

    #[test]
    fn parse_rejects_other_names() {
        for n in [
            &b"file"[..],
            b".~fsync.",
            b".~fsync.probe.0123456789abcdef.a",
            b".~fsync.old.0123",
            b".~fsync.0123456789ABCDEF",
            b".~fsync.0123456789abcdef0",
            b".~fsync.xyz.0123456789abcdef",
        ] {
            assert_eq!(parse(n), None, "{}", n.escape_ascii());
        }
        assert!(is_reserved(b".~fsync.probe.x"));
        assert!(!is_reserved(b".~fsync"));
        assert!(!is_reserved(b"a.~fsync.x"));
    }

    #[test]
    fn trash_names() {
        let now = date(2026, 10, 2).at(9, 5, 7, 0);
        let t = |n: &[u8], split| String::from_utf8(trash_name(n, split, now)).unwrap();
        assert_eq!(t(b"report.txt", true), "report~20261002-090507.txt");
        assert_eq!(t(b"link.d", false), "link.d~20261002-090507");
        assert_eq!(t(b".bashrc", true), ".bashrc~20261002-090507");
        assert_eq!(trash_name(&[b'x'; 300], true, now).len(), NAME_MAX);
        assert!(is_reserved(TRASH_DIR));
    }

    #[test]
    fn root_marker_names() {
        let name = root_marker(ReplicaId(0xabcdef0123456789));
        assert_eq!(name, b".~fsync.root.abcdef0123456789");
        assert!(is_reserved(&name) && is_root_marker(&name));
        assert_eq!(parse(&name), None);
        assert!(!is_root_marker(b".~fsync.0123456789abcdef"));
    }

    #[test]
    fn conflict_names() {
        let now = date(2026, 10, 2).at(9, 5, 7, 0);
        let id = ReplicaId(0xabcdef0123456789);
        let c = |n: &[u8], split| String::from_utf8(conflict_name(n, split, now, id)).unwrap();
        let m = ".sync-conflict-20261002-090507-abcdef0";
        assert_eq!(c(b"report.txt", true), format!("report{m}.txt"));
        assert_eq!(c(b"a.tar.gz", true), format!("a.tar{m}.gz"));
        assert_eq!(c(b"Makefile", true), format!("Makefile{m}"));
        assert_eq!(c(b".bashrc", true), format!(".bashrc{m}"));
        assert_eq!(c(b"trailing.", true), format!("trailing.{m}"));
        assert_eq!(c(b"dir.d", false), format!("dir.d{m}"));
        // Non-UTF-8 bytes pass through.
        assert_eq!(
            conflict_name(b"\xff.x", true, now, id),
            [&b"\xff"[..], m.as_bytes(), b".x"].concat()
        );
    }

    #[test]
    fn conflict_name_fits_name_max() {
        let now = date(2026, 1, 1).at(0, 0, 0, 0);
        let id = ReplicaId(1);
        let long = [b'a'; 250];
        let n = conflict_name(&[&long[..], b".txt"].concat(), true, now, id);
        assert_eq!(n.len(), NAME_MAX);
        assert!(n.ends_with(b"-0000000.txt"));
        // An absurd extension is not split off.
        let n = conflict_name(&[&b"a."[..], &long[..]].concat(), true, now, id);
        assert_eq!(n.len(), NAME_MAX);
        assert!(n.ends_with(b"-0000000"));
    }
    #[test]
    fn conflict_names_are_recognised() {
        let now = DateTime::constant(2026, 10, 2, 13, 4, 5, 0);
        let id = ReplicaId(0xabcdef0123456789);
        for (name, split) in [(&b"f.txt"[..], true), (b"dir", false), (b".hidden", true)] {
            assert!(is_conflict_name(&conflict_name(name, split, now, id)));
        }
        for name in [
            &b"f.txt"[..],
            b"f.sync-conflict-.txt",
            b"f.sync-conflict-20261002-130405-abcdef.txt",
            b"f.sync-conflict-20261002-130405-abcdef0x",
            b"f.sync-conflict-2026100a-130405-abcdef0",
        ] {
            assert!(!is_conflict_name(name), "{}", name.escape_ascii());
        }
    }
}
