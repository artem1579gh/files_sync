//! Optional landlock self-sandbox (`--sandbox`, T18).
//!
//! [`restrict`] limits the calling thread, and every thread or process it
//! starts afterwards, to writing beneath the given directories: the replica
//! roots and the pair's state directory. Reads stay unrestricted, since
//! followed symlinks may point anywhere (design §4). So a bug, or a race we
//! did not foresee, cannot write outside the pair. Writing through a `-K`
//! link to a directory outside the root (`keep_dirlinks_unsafe`) then fails
//! with `EACCES`.
//!
//! Landlock restricts only what is opened, created, removed or renamed after
//! the call; fds already open (stderr, an open index) keep working. Call it
//! before any other thread starts, so that all of them are restricted.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;

use rustix::fs::{CWD, Mode, OFlags};

use crate::error::{Error, Result};

// From <linux/landlock.h>.
const CREATE_RULESET_VERSION: libc::c_ulong = 1;
const RULE_PATH_BENEATH: libc::c_int = 1;

const ACCESS_FS_WRITE_FILE: u64 = 1 << 1;
const ACCESS_FS_REMOVE_DIR: u64 = 1 << 4;
const ACCESS_FS_REMOVE_FILE: u64 = 1 << 5;
const ACCESS_FS_MAKE_CHAR: u64 = 1 << 6;
const ACCESS_FS_MAKE_DIR: u64 = 1 << 7;
const ACCESS_FS_MAKE_REG: u64 = 1 << 8;
const ACCESS_FS_MAKE_SOCK: u64 = 1 << 9;
const ACCESS_FS_MAKE_FIFO: u64 = 1 << 10;
const ACCESS_FS_MAKE_BLOCK: u64 = 1 << 11;
const ACCESS_FS_MAKE_SYM: u64 = 1 << 12;
/// ABI 2: linking or renaming a file into another directory.
const ACCESS_FS_REFER: u64 = 1 << 13;
/// ABI 3: `truncate`, `ftruncate` and `O_TRUNC`.
const ACCESS_FS_TRUNCATE: u64 = 1 << 14;

/// `struct landlock_ruleset_attr`, ABI 1 part (the kernel accepts the
/// shorter struct from newer ABIs).
#[repr(C)]
struct RulesetAttr {
    handled_access_fs: u64,
}

/// `struct landlock_path_beneath_attr` (packed in the uapi header).
#[repr(C, packed)]
struct PathBeneathAttr {
    allowed_access: u64,
    parent_fd: i32,
}

/// The landlock ABI version the kernel supports, or `None` if landlock is
/// not built in or not enabled.
pub fn abi() -> Option<u32> {
    // SAFETY: the version query takes no pointer.
    let v = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<RulesetAttr>(),
            0usize,
            CREATE_RULESET_VERSION,
        )
    };
    u32::try_from(v).ok().filter(|&v| v > 0)
}

/// Every right that writes, for the given ABI version.
fn write_access(abi: u32) -> u64 {
    let mut access = ACCESS_FS_WRITE_FILE
        | ACCESS_FS_REMOVE_DIR
        | ACCESS_FS_REMOVE_FILE
        | ACCESS_FS_MAKE_CHAR
        | ACCESS_FS_MAKE_DIR
        | ACCESS_FS_MAKE_REG
        | ACCESS_FS_MAKE_SOCK
        | ACCESS_FS_MAKE_FIFO
        | ACCESS_FS_MAKE_BLOCK
        | ACCESS_FS_MAKE_SYM;
    if abi >= 2 {
        access |= ACCESS_FS_REFER;
    }
    if abi >= 3 {
        access |= ACCESS_FS_TRUNCATE;
    }
    access
}

fn last_err(what: &str) -> Error {
    Error::io(format!("landlock: {what}"), io::Error::last_os_error())
}

/// Restricts writes by this thread, and the threads and processes it starts
/// from now on, to the directories `writable` and what lies beneath them.
/// Returns the landlock ABI version used. Fails if landlock is unavailable:
/// a sandbox that was asked for is never silently skipped.
pub fn restrict(writable: &[&Path]) -> Result<u32> {
    let abi = abi().ok_or_else(|| {
        Error::io(
            "landlock",
            io::Error::new(
                io::ErrorKind::Unsupported,
                "not supported by this kernel (needs CONFIG_SECURITY_LANDLOCK and lsm=landlock)",
            ),
        )
    })?;
    let access = write_access(abi);
    let attr = RulesetAttr {
        handled_access_fs: access,
    };
    // SAFETY: `attr` is a valid ruleset attribute of the size passed.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            &attr as *const RulesetAttr,
            std::mem::size_of::<RulesetAttr>(),
            0u32,
        )
    };
    if fd < 0 {
        return Err(last_err("create ruleset"));
    }
    // SAFETY: the syscall returned a new fd that nothing else owns.
    let ruleset = unsafe { OwnedFd::from_raw_fd(fd as i32) };

    for dir in writable {
        let dirfd = rustix::fs::openat(
            CWD,
            *dir,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| Error::io(format!("landlock: open {}", dir.display()), e.into()))?;
        let rule = PathBeneathAttr {
            allowed_access: access,
            parent_fd: dirfd.as_raw_fd(),
        };
        // SAFETY: `rule` is a valid path-beneath attribute; both fds are open.
        let rc = unsafe {
            libc::syscall(
                libc::SYS_landlock_add_rule,
                ruleset.as_raw_fd(),
                RULE_PATH_BENEATH,
                &rule as *const PathBeneathAttr,
                0u32,
            )
        };
        if rc != 0 {
            return Err(last_err(&format!("allow writes beneath {}", dir.display())));
        }
    }

    // SAFETY: plain prctl and syscall with integer arguments.
    unsafe {
        if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
            return Err(last_err("set no_new_privs"));
        }
        if libc::syscall(libc::SYS_landlock_restrict_self, ruleset.as_raw_fd(), 0u32) != 0 {
            return Err(last_err("restrict self"));
        }
    }
    Ok(abi)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Done-when: a write outside the allowed directories fails with
    /// EACCES. Landlock restricts only the calling thread (and its
    /// children), so the test runs in a thread of its own.
    #[test]
    fn writes_outside_the_roots_fail() {
        if abi().is_none() {
            eprintln!("landlock unsupported here; skipping");
            return;
        }
        let (root, state, outside) = (
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        );
        fs::write(outside.path().join("victim"), b"x").unwrap();
        fs::write(root.path().join("old"), b"x").unwrap();
        let (r, s, o) = (
            root.path().to_path_buf(),
            state.path().to_path_buf(),
            outside.path().to_path_buf(),
        );
        std::thread::spawn(move || {
            restrict(&[&r, &s]).unwrap();
            let denied = |res: io::Result<()>, what: &str| {
                let e = res.expect_err(what);
                assert_eq!(e.raw_os_error(), Some(libc::EACCES), "{what}: {e}");
            };
            // Inside: everything works.
            fs::write(r.join("new"), b"y").unwrap();
            fs::create_dir(r.join("d")).unwrap();
            fs::rename(r.join("new"), r.join("d/moved")).unwrap();
            fs::remove_file(r.join("old")).unwrap();
            fs::write(s.join("index"), b"z").unwrap();
            // Outside: no writes, but reads.
            denied(fs::write(o.join("new"), b"y"), "create");
            denied(fs::write(o.join("victim"), b"y"), "overwrite");
            denied(fs::remove_file(o.join("victim")), "unlink");
            denied(fs::create_dir(o.join("d")), "mkdir");
            denied(std::os::unix::fs::symlink("t", o.join("l")), "symlink");
            denied(
                fs::rename(r.join("d/moved"), o.join("stolen")),
                "rename out",
            );
            assert_eq!(fs::read(o.join("victim")).unwrap(), b"x");
        })
        .join()
        .unwrap();
        // The test's own thread is not restricted.
        fs::write(outside.path().join("after"), b"ok").unwrap();
    }
}
