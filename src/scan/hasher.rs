//! Content hashing for the scanner: [`stable_read`] into blake3 with a counter,
//! so tests can check the rehash shortcut (design §3) actually skips reads.

use std::io;
use std::os::fd::{BorrowedFd, OwnedFd};

use rustix::io::Errno;

use crate::error::Result;
use crate::fs::hooks;
use crate::fs::{Fingerprint, Sink, stable_read, stable_read_with};

/// Hashes files and counts how many it hashed.
#[derive(Debug, Default)]
pub struct Hasher {
    hashed: u64,
}

/// Drops the content; marks each chunk with the `scan.read_chunk` hook point,
/// so tests can modify a file while it is being read.
struct ChunkHook;

impl Sink for ChunkHook {
    fn restart(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn write(&mut self, _: &[u8]) -> io::Result<()> {
        hooks::point("scan.read_chunk");
        Ok(())
    }
}

impl Hasher {
    pub fn new() -> Hasher {
        Hasher::default()
    }

    /// Files hashed so far (each call counts once, whatever its retries).
    pub fn hashed(&self) -> u64 {
        self.hashed
    }

    /// Stable read of the regular file `parent/name` (design §5.2).
    pub fn hash_at(
        &mut self,
        parent: BorrowedFd<'_>,
        name: &[u8],
    ) -> Result<(Fingerprint, [u8; 32])> {
        self.hashed += 1;
        stable_read(parent, name, &mut ChunkHook)
    }

    /// Stable read of a file reached some other way (a followed symlink);
    /// see [`stable_read_with`].
    pub fn hash_with(
        &mut self,
        what: &[u8],
        open: impl FnMut() -> std::result::Result<OwnedFd, Errno>,
        recheck: impl FnMut(&Fingerprint) -> Result<Option<&'static str>>,
    ) -> Result<(Fingerprint, [u8; 32])> {
        self.hashed += 1;
        stable_read_with(what, open, recheck, &mut ChunkHook)
    }
}
