//! Block-level delta transfer (design §7.1, T24).
//!
//! A file is cut into fixed, aligned blocks of [`BLOCK_SIZE`] bytes (the
//! last one shorter), each hashed with blake3: its [`Blocks`]. When the
//! destination of a `WriteFile` already holds a file at the path, the engine
//! matches the new file's blocks against the current file's
//! ([`Delta::plan`]) and only the blocks the destination lacks travel.
//!
//! Both ends of a transfer verify, inside their replica:
//! - the source reads the chosen blocks with a [`BlockReader`], which fails
//!   with `Unstable` unless the file stays unchanged throughout;
//! - the destination assembles the new content with an [`Assembler`] from
//!   its own current file (pinned and checked the same way) and the received
//!   blocks, checking every block against the new block list. The assembled
//!   content is then committed like any other (`commit::replace_file`: a new
//!   temp file, the whole-file hash, the CAS replace).

use std::io::{self, Read};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::fs::PinnedFile;
use crate::fs::hooks::point;

/// The size of a block (the last block of a file may be shorter).
pub const BLOCK_SIZE: u64 = 128 << 10;

/// Files with more blocks are always sent whole: their block list (32 bytes
/// a block) and their delta must fit one frame.
pub const MAX_BLOCKS: u64 = 1 << 19;

/// A file's block list: the blake3 hash of each [`BLOCK_SIZE`] block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blocks {
    pub size: u64,
    pub hashes: Vec<[u8; 32]>,
}

impl Blocks {
    /// How many blocks a file of `size` bytes has.
    pub fn count(size: u64) -> u64 {
        size.div_ceil(BLOCK_SIZE)
    }

    /// Whether a file of `size` bytes may be sent as a delta at all.
    pub fn fits(size: u64) -> bool {
        Blocks::count(size) <= MAX_BLOCKS
    }

    /// The length of block `i` of a file of `size` bytes.
    pub fn len_of(size: u64, i: u64) -> usize {
        size.saturating_sub(i * BLOCK_SIZE).min(BLOCK_SIZE) as usize
    }

    /// The list has one hash per block of `size`, and not too many.
    pub fn is_consistent(&self) -> bool {
        Blocks::fits(self.size) && self.hashes.len() as u64 == Blocks::count(self.size)
    }

    /// Hashes the blocks of everything `content` yields, which must be
    /// `size` bytes (for a verifying reader such as a `StableReader`, that
    /// is checked at its EOF).
    pub fn read(content: &mut dyn Read, size: u64) -> Result<Blocks> {
        let mut hashes = Vec::with_capacity(Blocks::count(size).min(MAX_BLOCKS) as usize);
        let mut hasher = blake3::Hasher::new();
        let mut in_block: u64 = 0;
        let mut total: u64 = 0;
        let mut buf = vec![0u8; 64 << 10];
        loop {
            let n = match content.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Error::from_stream("hash blocks", e)),
            };
            total += n as u64;
            let mut data = &buf[..n];
            while !data.is_empty() {
                let take = data.len().min((BLOCK_SIZE - in_block) as usize);
                hasher.update(&data[..take]);
                in_block += take as u64;
                data = &data[take..];
                if in_block == BLOCK_SIZE {
                    hashes.push(*hasher.finalize().as_bytes());
                    hasher.reset();
                    in_block = 0;
                }
            }
        }
        if in_block > 0 {
            hashes.push(*hasher.finalize().as_bytes());
        }
        if total != size {
            return Err(Error::Protocol {
                reason: format!("block list of {total} bytes, expected {size}"),
            });
        }
        Ok(Blocks { size, hashes })
    }
}

/// How the destination builds a new file: the new file's block list, and for
/// each of its blocks either the index of a block of the destination's
/// current file with the same hash, or `None` (the block's bytes are sent,
/// in order).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delta {
    pub blocks: Blocks,
    pub reuse: Vec<Option<u32>>,
}

impl Delta {
    /// Matches the blocks of `new` against those of the current file `old`:
    /// the block at the same index if it has the same hash, else the first
    /// block with that hash.
    pub fn plan(old: &Blocks, new: Blocks) -> Delta {
        let mut first = std::collections::HashMap::with_capacity(old.hashes.len());
        for (j, h) in old.hashes.iter().enumerate().rev() {
            first.insert(h, j as u32);
        }
        let reuse = new
            .hashes
            .iter()
            .enumerate()
            .map(|(i, h)| match old.hashes.get(i) {
                Some(o) if o == h => Some(i as u32),
                _ => first.get(h).copied(),
            })
            .collect();
        Delta { blocks: new, reuse }
    }

    /// The new file's blocks whose bytes must be sent, in order.
    pub fn needed(&self) -> Vec<u32> {
        (0..self.reuse.len() as u32)
            .filter(|&i| self.reuse[i as usize].is_none())
            .collect()
    }

    /// How many blocks the destination takes from its current file.
    pub fn reused(&self) -> usize {
        self.reuse.iter().filter(|r| r.is_some()).count()
    }

    /// The delta fits a new file of its own block list and a current file of
    /// `old_size` bytes. `Some(reason)` if not.
    pub fn misfit(&self, old_size: u64) -> Option<&'static str> {
        if !self.blocks.is_consistent() {
            Some("block list does not fit the file size")
        } else if self.reuse.len() != self.blocks.hashes.len() {
            Some("delta does not cover every block")
        } else if self
            .reuse
            .iter()
            .flatten()
            .any(|&j| u64::from(j) >= Blocks::count(old_size))
        {
            Some("delta reuses a block the current file does not have")
        } else {
            None
        }
    }
}

fn unstable(what: &[u8], reason: &'static str) -> io::Error {
    io::Error::other(Error::Unstable {
        path: what.to_vec(),
        reason,
    })
}

/// The chosen blocks of a file, in order, as one stream (the source side of
/// a delta transfer). The file must stay unchanged: `read` returns `Ok(0)`
/// only once [`PinnedFile::verify`] passed, and fails with `Unstable`
/// otherwise.
pub struct BlockReader {
    what: Vec<u8>,
    /// Closed once the read is verified or failed.
    file: Option<PinnedFile>,
    size: u64,
    blocks: Vec<u32>,
    next: usize,
    buf: Vec<u8>,
    pos: usize,
    failed: Option<&'static str>,
}

impl BlockReader {
    /// Reads `blocks` of `file` (a file of `size` bytes, as indexed); every
    /// index must be a block of it.
    pub fn new(what: &[u8], file: PinnedFile, size: u64, blocks: Vec<u32>) -> BlockReader {
        BlockReader {
            what: what.to_vec(),
            file: Some(file),
            size,
            blocks,
            next: 0,
            buf: Vec::new(),
            pos: 0,
            failed: None,
        }
    }

    fn fail(&mut self, reason: &'static str) -> io::Error {
        tracing::debug!(name = %self.what.escape_ascii(), reason, "unstable block read");
        self.failed = Some(reason);
        self.file = None;
        unstable(&self.what, reason)
    }

    /// Loads the next block into `buf`; `Ok(false)` at the end (verified).
    fn fill(&mut self) -> io::Result<bool> {
        let Some(file) = self.file.as_ref() else {
            return Ok(false);
        };
        let Some(&i) = self.blocks.get(self.next) else {
            let file = self.file.take().expect("checked above");
            return match file.verify().map_err(io::Error::other)? {
                None => Ok(false),
                Some(reason) => Err(self.fail(reason)),
            };
        };
        self.next += 1;
        let len = Blocks::len_of(self.size, i.into());
        self.buf.resize(len, 0);
        let offset = u64::from(i) * BLOCK_SIZE;
        if !file
            .read_at(&mut self.buf, offset)
            .map_err(io::Error::other)?
        {
            return Err(self.fail("file shrank during read"));
        }
        self.pos = 0;
        Ok(true)
    }
}

impl Read for BlockReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if let Some(reason) = self.failed {
            return Err(unstable(&self.what, reason));
        }
        while self.pos == self.buf.len() {
            if !self.fill()? {
                return Ok(0);
            }
        }
        let n = out.len().min(self.buf.len() - self.pos);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// The new content of a delta transfer, assembled on the destination from
/// its current file `old` (pinned, as indexed) and the received blocks
/// `data`, as a plain `Read` to commit like any other content.
///
/// Every block must hash to its hash in the delta's block list: a reused one
/// read from `old`, a received one read from `data`. A mismatch, a short
/// read, or an `old` that is not unchanged (and still at its name) once the
/// last block is done fails the read with `Unstable`; nothing has been
/// committed then. `old` is closed before the content ends, so the commit
/// can lease the old inode (§5.3 step 4(b)). An error from `data` is passed
/// on as it is (e.g. the source's `Unstable`).
pub struct Assembler<'a> {
    what: Vec<u8>,
    old: Option<PinnedFile>,
    delta: &'a Delta,
    data: &'a mut dyn Read,
    next: usize,
    buf: Vec<u8>,
    pos: usize,
    done: bool,
    failed: Option<&'static str>,
}

impl<'a> Assembler<'a> {
    /// `delta` must fit `old` ([`Delta::misfit`]).
    pub fn new(
        what: &[u8],
        old: PinnedFile,
        delta: &'a Delta,
        data: &'a mut dyn Read,
    ) -> Assembler<'a> {
        Assembler {
            what: what.to_vec(),
            old: Some(old),
            delta,
            data,
            next: 0,
            buf: Vec::with_capacity(BLOCK_SIZE as usize),
            pos: 0,
            done: false,
            failed: None,
        }
    }

    fn fail(&mut self, reason: &'static str) -> io::Error {
        tracing::debug!(name = %self.what.escape_ascii(), reason, "delta assembly failed");
        self.failed = Some(reason);
        self.old = None;
        unstable(&self.what, reason)
    }

    /// Loads the next block into `buf`; `Ok(false)` at the end (verified).
    fn fill(&mut self) -> io::Result<bool> {
        if self.done {
            return Ok(false);
        }
        let delta = self.delta;
        let blocks = &delta.blocks;
        let i = self.next;
        if i == blocks.hashes.len() {
            // `data` must end here, with its own checks (a content stream
            // checks its hash at its end).
            let mut probe = [0u8; 1];
            loop {
                match self.data.read(&mut probe) {
                    Ok(0) => break,
                    Ok(_) => return Err(self.fail("more data than the delta needs")),
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => {
                        self.old = None;
                        return Err(e);
                    }
                }
            }
            point("delta.assembled");
            let old = self.old.take().expect("open until done");
            return match old.verify().map_err(io::Error::other)? {
                None => {
                    self.done = true;
                    Ok(false)
                }
                Some(_) => Err(self.fail("current file changed during assembly")),
            };
        }
        let len = Blocks::len_of(blocks.size, i as u64);
        self.buf.resize(len, 0);
        match delta.reuse[i] {
            Some(j) => {
                let old = self.old.as_ref().expect("open until done");
                let offset = u64::from(j) * BLOCK_SIZE;
                if !old
                    .read_at(&mut self.buf, offset)
                    .map_err(io::Error::other)?
                {
                    return Err(self.fail("current file shrank during assembly"));
                }
                if *blake3::hash(&self.buf).as_bytes() != blocks.hashes[i] {
                    return Err(self.fail("current file changed during assembly"));
                }
            }
            None => {
                if let Err(e) = self.data.read_exact(&mut self.buf) {
                    self.old = None;
                    return Err(if e.kind() == io::ErrorKind::UnexpectedEof {
                        self.fail("delta data ended early")
                    } else {
                        e
                    });
                }
                if *blake3::hash(&self.buf).as_bytes() != blocks.hashes[i] {
                    return Err(self.fail("received block differs from the block list"));
                }
            }
        }
        self.next += 1;
        self.pos = 0;
        point("delta.block");
        Ok(true)
    }
}

impl Read for Assembler<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if let Some(reason) = self.failed {
            return Err(unstable(&self.what, reason));
        }
        while self.pos == self.buf.len() {
            if !self.fill()? {
                return Ok(0);
            }
        }
        let n = out.len().min(self.buf.len() - self.pos);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn blocks_of(data: &[u8]) -> Blocks {
        Blocks::read(&mut Cursor::new(data), data.len() as u64).unwrap()
    }

    fn data(n: usize, seed: u8) -> Vec<u8> {
        (0..n)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed) ^ (i >> 11) as u8)
            .collect()
    }

    #[test]
    fn block_lists_cut_aligned_blocks() {
        let bs = BLOCK_SIZE as usize;
        assert_eq!(blocks_of(b"").hashes.len(), 0);
        let d = data(2 * bs + 5, 1);
        let b = blocks_of(&d);
        assert_eq!(b.size, d.len() as u64);
        assert_eq!(
            b.hashes,
            [
                *blake3::hash(&d[..bs]).as_bytes(),
                *blake3::hash(&d[bs..2 * bs]).as_bytes(),
                *blake3::hash(&d[2 * bs..]).as_bytes(),
            ]
        );
        assert!(b.is_consistent());
        assert_eq!(Blocks::len_of(b.size, 2), 5);
        assert_eq!(Blocks::count(2 * BLOCK_SIZE), 2);
        // The size must be what the reader yields.
        assert!(Blocks::read(&mut Cursor::new(&d[..]), 7).is_err());
        let mut short = b.clone();
        short.hashes.pop();
        assert!(!short.is_consistent());
    }

    #[test]
    fn plans_reuse_by_hash() {
        let bs = BLOCK_SIZE as usize;
        let old = data(4 * bs, 1);
        // Block 0 changed, blocks 1 and 2 swapped, block 3 kept, one new
        // block appended.
        let mut new = Vec::new();
        new.extend(data(bs, 9));
        new.extend(&old[2 * bs..3 * bs]);
        new.extend(&old[bs..2 * bs]);
        new.extend(&old[3 * bs..]);
        new.extend(data(10, 7));
        let delta = Delta::plan(&blocks_of(&old), blocks_of(&new));
        assert_eq!(delta.reuse, [None, Some(2), Some(1), Some(3), None]);
        assert_eq!(delta.needed(), [0, 4]);
        assert_eq!(delta.reused(), 3);
        assert_eq!(delta.misfit(old.len() as u64), None);
        assert!(delta.misfit(2 * BLOCK_SIZE).is_some());
    }
}
