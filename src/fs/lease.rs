//! Write leases (`F_SETLEASE F_WRLCK`, design §5.3 step 4(b), (d) and (f)).
//!
//! The kernel grants a write lease on a file only while no other open file
//! description refers to it, writable mmaps included (`O_PATH` fds and the
//! lease's own fd do not count). While we hold it, anyone else's `open`
//! breaks it: the opener waits until we release the lease (at most
//! `/proc/sys/fs/lease-break-time`), and `F_GETLEASE` stops returning
//! `F_WRLCK`. So a lease that is still [`held`](Lease::held) proves that
//! nobody could have written to the file since it was taken, and nobody can
//! until it is released (by dropping the [`Lease`]).
//!
//! Leases only ever shorten waits or close windows: they need a local
//! filesystem and the caller to own the file (or `CAP_LEASE`), so every
//! failure to take one just means "no lease" and the caller falls back to
//! the grace timer (§5.10).
//!
//! A lease break also sends `SIGIO` to the holder, and `SIGIO` kills a
//! process by default. [`Lease::take`] therefore makes the process ignore
//! `SIGIO` unless it already handles it.

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::sync::Once;

use rustix::fs::SeekFrom;

use crate::error::Result;
use crate::fs::stat::{Discard, FileKind, Fingerprint, open_for_read, stable_read_with};

/// A write lease on one file, held until dropped.
#[derive(Debug)]
pub struct Lease {
    /// `O_RDONLY`; the lease belongs to this open file description.
    fd: OwnedFd,
}

impl Lease {
    /// Takes a write lease on the regular file `parent/name`, which must be
    /// the inode `expect` describes (dev and ino). `None` if it is not, if
    /// someone else has the file open, or if leases are unavailable here.
    /// Opening the file breaks someone else's lease on it, if there is one.
    pub fn take(parent: BorrowedFd<'_>, name: &[u8], expect: &Fingerprint) -> Option<Lease> {
        if expect.kind != FileKind::File {
            return None;
        }
        ignore_sigio();
        let fd = match open_for_read(parent, name) {
            Ok(fd) => fd,
            Err(e) => {
                tracing::debug!(name = %name.escape_ascii(), error = %e, "cannot open file to lease");
                return None;
            }
        };
        match Fingerprint::of_fd(fd.as_fd()) {
            Ok(fp) if fp.same_file(expect) && fp.kind == FileKind::File => {}
            _ => return None,
        }
        // SAFETY: fcntl on an fd we own, with integer arguments only.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETLEASE, libc::F_WRLCK) } != 0 {
            let e = std::io::Error::last_os_error();
            tracing::debug!(name = %name.escape_ascii(), error = %e, "no write lease");
            return None;
        }
        Some(Lease { fd })
    }

    /// The lease is unbroken: nobody has opened the file since it was taken.
    pub fn held(&self) -> bool {
        // SAFETY: fcntl on an fd we own, with integer arguments only.
        unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_GETLEASE) == libc::F_WRLCK }
    }

    /// The leased file's current fingerprint.
    pub fn fingerprint(&self) -> Result<Fingerprint> {
        Fingerprint::of_fd(self.fd.as_fd())
    }

    /// Hashes the leased file through the lease's own open file description
    /// (opening the file again would break the lease), with the checks of
    /// [`stable_read`](crate::fs::stable_read): fingerprint unchanged during
    /// the read, byte count equal to the size, and `parent/name` still the
    /// same inode at the end.
    pub fn hash(&self, parent: BorrowedFd<'_>, name: &[u8]) -> Result<(Fingerprint, [u8; 32])> {
        stable_read_with(
            name,
            || {
                let fd = rustix::io::fcntl_dupfd_cloexec(&self.fd, 0)?;
                rustix::fs::seek(&fd, SeekFrom::Start(0))?;
                Ok(fd)
            },
            |f2| match Fingerprint::at_opt(parent, name)? {
                Some(f3) if f2.same_file(&f3) => Ok(None),
                Some(_) => Ok(Some("name replaced during read")),
                None => Ok(Some("name removed during read")),
            },
            &mut Discard,
        )
    }
}

/// Makes the process ignore `SIGIO` (sent to a lease holder when its lease
/// is broken), unless a handler is installed already.
pub fn ignore_sigio() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // SAFETY: sigaction with a zeroed (valid) struct; only the
        // disposition of SIGIO is changed, and only from the default.
        unsafe {
            let mut old: libc::sigaction = std::mem::zeroed();
            if libc::sigaction(libc::SIGIO, std::ptr::null(), &mut old) != 0 {
                return;
            }
            if old.sa_sigaction == libc::SIG_DFL {
                let mut ign: libc::sigaction = std::mem::zeroed();
                ign.sa_sigaction = libc::SIG_IGN;
                libc::sigemptyset(&mut ign.sa_mask);
                libc::sigaction(libc::SIGIO, &ign, std::ptr::null_mut());
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::Root;
    use std::fs;
    use std::io::Write;

    fn fx(data: &[u8]) -> (tempfile::TempDir, Root, Fingerprint) {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("f"), data).unwrap();
        let root = Root::open(dir.path()).unwrap();
        let fp = Fingerprint::at(root.fd(), b"f").unwrap();
        (dir, root, fp)
    }

    #[test]
    fn taken_only_without_other_open_fds() {
        let (dir, root, fp) = fx(b"data");
        let lease = Lease::take(root.fd(), b"f", &fp).expect("tmpfs supports leases");
        assert!(lease.held());
        let (now, hash) = lease.hash(root.fd(), b"f").unwrap();
        assert!(now.unchanged(&fp));
        assert_eq!(hash, *blake3::hash(b"data").as_bytes());
        assert!(lease.held(), "reading through the lease keeps it");
        drop(lease);

        let held = fs::File::open(dir.path().join("f")).unwrap();
        assert!(
            Lease::take(root.fd(), b"f", &fp).is_none(),
            "an open fd refuses it"
        );
        drop(held);
        assert!(Lease::take(root.fd(), b"f", &fp).is_some());

        // Another inode at the name, or a directory: no lease.
        fs::write(dir.path().join("g"), b"other").unwrap();
        assert!(Lease::take(root.fd(), b"g", &fp).is_none());
        fs::create_dir(dir.path().join("d")).unwrap();
        let d = Fingerprint::at(root.fd(), b"d").unwrap();
        assert!(Lease::take(root.fd(), b"d", &d).is_none());
    }

    #[test]
    fn an_open_breaks_it() {
        let (dir, root, fp) = fx(b"data");
        let lease = Lease::take(root.fd(), b"f", &fp).unwrap();
        let path = dir.path().join("f");
        // The opener blocks until the lease is released.
        let writer = std::thread::spawn(move || {
            let mut f = fs::File::options().append(true).open(path).unwrap();
            f.write_all(b"+late").unwrap();
        });
        let t0 = std::time::Instant::now();
        while lease.held() {
            assert!(t0.elapsed().as_secs() < 10, "lease never broken");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(!writer.is_finished(), "the opener waits for the release");
        drop(lease);
        writer.join().unwrap();
        assert_eq!(fs::read(dir.path().join("f")).unwrap(), b"data+late");
    }
}
