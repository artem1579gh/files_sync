//! Length-prefixed postcard frames over any byte stream, and file content
//! as a stream of [`Content`] messages (design §7.1).
//!
//! A frame is a 4-byte big-endian length followed by that many bytes: one
//! postcard-encoded message, with nothing left over. Everything read is
//! untrusted: a malformed frame is an [`Error::Protocol`], never a panic, and
//! no allocation is sized by the peer beyond [`MAX_FRAME`].

use std::io::{self, Read, Write};
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::messages::{Content, WireError};
use super::{CHUNK_SIZE, MAX_FRAME};
use crate::error::{Error, Result};

fn protocol(reason: impl Into<String>) -> Error {
    Error::Protocol {
        reason: reason.into(),
    }
}

/// Writes `msg` as one frame (a single `write_all`) and flushes `w`.
pub fn write_frame<W: Write + ?Sized, T: Serialize>(w: &mut W, msg: &T) -> Result<()> {
    let mut buf = postcard::to_extend(msg, vec![0u8; 4])
        .map_err(|e| protocol(format!("cannot encode message: {e}")))?;
    let len = buf.len() - 4;
    if len > MAX_FRAME {
        return Err(protocol(format!(
            "message of {len} bytes exceeds the frame limit of {MAX_FRAME}"
        )));
    }
    buf[..4].copy_from_slice(&(len as u32).to_be_bytes());
    w.write_all(&buf)
        .and_then(|()| w.flush())
        .map_err(|e| Error::io("write frame", e))
}

/// Reads one frame and decodes it. `Ok(None)` is a clean end of stream (the
/// connection closed between frames); an end of stream inside a frame is an
/// error.
pub fn read_frame<R: Read + ?Sized, T: DeserializeOwned>(r: &mut R) -> Result<Option<T>> {
    let mut header = [0u8; 4];
    let mut got = 0;
    while got < header.len() {
        match r.read(&mut header[got..]) {
            Ok(0) if got == 0 => return Ok(None),
            Ok(0) => return Err(protocol("connection closed inside a frame header")),
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(Error::io("read frame", e)),
        }
    }
    let len = u32::from_be_bytes(header) as usize;
    if len == 0 {
        return Err(protocol("empty frame"));
    }
    if len > MAX_FRAME {
        return Err(protocol(format!(
            "frame of {len} bytes exceeds the limit of {MAX_FRAME}"
        )));
    }
    // Grows with the bytes that actually arrive, not with the claimed length.
    let mut body = Vec::with_capacity(len.min(CHUNK_SIZE + 64));
    r.take(len as u64)
        .read_to_end(&mut body)
        .map_err(|e| Error::io("read frame", e))?;
    if body.len() < len {
        return Err(protocol(format!(
            "connection closed inside a frame ({} of {len} bytes)",
            body.len()
        )));
    }
    let (msg, rest) = postcard::take_from_bytes::<T>(&body)
        .map_err(|e| protocol(format!("malformed message: {e}")))?;
    if !rest.is_empty() {
        return Err(protocol(format!(
            "{} trailing bytes after the message",
            rest.len()
        )));
    }
    Ok(Some(msg))
}

/// Sends all of `src` as content messages (each wrapped by `wrap`): chunks of
/// at most [`CHUNK_SIZE`], then [`Content::End`] with the blake3 of the bytes
/// sent. If `src` fails, [`Content::Abort`] is sent instead of `End`.
///
/// The outer error is the connection's (unusable now); the inner one is the
/// source's, after the stream was ended with `Abort` (the connection stays in
/// step). On success, returns the hash sent.
pub fn send_content<W, M>(
    w: &mut W,
    src: &mut dyn Read,
    wrap: impl Fn(Content) -> M,
) -> Result<Result<[u8; 32]>>
where
    W: Write + ?Sized,
    M: Serialize,
{
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        let n = match src.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => {
                let err = Error::from_stream("read content", e);
                write_frame(w, &wrap(Content::Abort(WireError::from(&err))))?;
                return Ok(Err(err));
            }
        };
        hasher.update(&buf[..n]);
        write_frame(w, &wrap(Content::Chunk(buf[..n].to_vec())))?;
    }
    let hash = *hasher.finalize().as_bytes();
    write_frame(w, &wrap(Content::End { hash }))?;
    Ok(Ok(hash))
}

/// How a [`ContentStream`] stands.
#[derive(Clone, Debug, PartialEq, Eq)]
enum State {
    /// More content may follow.
    Open,
    /// `End` arrived and the hash matched.
    Done,
    /// The sender ended the stream with `Abort`.
    Aborted(WireError),
    /// `End` arrived, but the content does not hash to it.
    Mismatch,
    /// The connection failed or broke the protocol mid-stream: why.
    Broken(String),
}

/// Reads a content stream (sent by [`send_content`]) from frames of message
/// type `M`, as a plain [`Read`].
///
/// `filter` picks the content out of each message: `Ok(Some(_))` for a
/// content message, `Ok(None)` to skip a message it handled itself (such as
/// a pushed [`Hint`](super::Response::Hint)), `Err` for one that does not
/// belong here.
///
/// Reaching EOF (`read` returning `Ok(0)`) means the whole content arrived
/// and its hash matched. Otherwise `read` fails with an `io::Error` wrapping
/// an [`Error`] ([`Error::from_stream`] unwraps it): the sender's
/// [`Error::Remote`] for an `Abort`, else [`Error::Protocol`]. A failed
/// stream stays failed.
pub struct ContentStream<R, M, F> {
    src: R,
    filter: F,
    hasher: blake3::Hasher,
    chunk: Vec<u8>,
    pos: usize,
    state: State,
    _msg: PhantomData<fn() -> M>,
}

impl<R, M, F> ContentStream<R, M, F>
where
    R: Read,
    M: DeserializeOwned,
    F: FnMut(M) -> Result<Option<Content>>,
{
    pub fn new(src: R, filter: F) -> Self {
        ContentStream {
            src,
            filter,
            hasher: blake3::Hasher::new(),
            chunk: Vec::new(),
            pos: 0,
            state: State::Open,
            _msg: PhantomData,
        }
    }

    /// The stream was ended by the sender (`End` or `Abort`), so the next
    /// frame on the connection is the next message.
    pub fn is_finished(&self) -> bool {
        matches!(
            self.state,
            State::Done | State::Aborted(_) | State::Mismatch
        )
    }

    /// Reads and discards the rest of the stream, so the connection can be
    /// used for the next message. Fails only if the connection is broken.
    pub fn drain(&mut self) -> Result<()> {
        self.chunk.clear();
        self.pos = 0;
        while self.state == State::Open {
            self.next_message();
            self.chunk.clear();
        }
        match &self.state {
            State::Broken(reason) => Err(protocol(reason.clone())),
            _ => Ok(()),
        }
    }

    /// Gives back the underlying reader.
    pub fn into_inner(self) -> R {
        self.src
    }

    /// Takes one message into `chunk` or `state` (which is `Open` on entry).
    fn next_message(&mut self) {
        let content = match read_frame::<_, M>(&mut self.src) {
            Ok(Some(msg)) => (self.filter)(msg),
            Ok(None) => Err(protocol("connection closed inside a content stream")),
            Err(e) => Err(e),
        };
        self.state = match content {
            Ok(None) => State::Open,
            Ok(Some(Content::Chunk(data))) => {
                self.hasher.update(&data);
                self.chunk = data;
                self.pos = 0;
                State::Open
            }
            Ok(Some(Content::End { hash })) if hash == *self.hasher.finalize().as_bytes() => {
                State::Done
            }
            Ok(Some(Content::End { .. })) => State::Mismatch,
            Ok(Some(Content::Abort(e))) => State::Aborted(e),
            Err(Error::Protocol { reason }) => State::Broken(reason),
            Err(e) => State::Broken(e.to_string()),
        };
    }

    fn error(&self) -> io::Error {
        let err = match &self.state {
            State::Aborted(e) => Error::from(e.clone()),
            State::Mismatch => protocol("content hash mismatch"),
            State::Broken(reason) => protocol(reason.clone()),
            State::Open | State::Done => unreachable!("not a failure"),
        };
        io::Error::other(err)
    }
}

impl<R, M, F> Read for ContentStream<R, M, F>
where
    R: Read,
    M: DeserializeOwned,
    F: FnMut(M) -> Result<Option<Content>>,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.pos < self.chunk.len() {
                let n = buf.len().min(self.chunk.len() - self.pos);
                buf[..n].copy_from_slice(&self.chunk[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            match self.state {
                State::Open => self.next_message(),
                State::Done => return Ok(0),
                State::Aborted(_) | State::Mismatch | State::Broken(_) => {
                    return Err(self.error());
                }
            }
        }
    }
}
