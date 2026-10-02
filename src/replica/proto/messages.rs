//! The messages of the wire protocol (design §7.1).
//!
//! Each direction has one message type: [`Request`] (client to server) and
//! [`Response`] (server to client), with one variant per [`Replica`] call.
//! Content travels inside them as [`Content`] messages. The handshake uses
//! its own types ([`Hello`], [`HelloReply`]), whose encoding must never
//! change, so any two versions can at least agree that they disagree.
//!
//! [`Replica`]: crate::replica::Replica

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config::ReplicaId;
use crate::error::{Error, RemoteKind};
use crate::fs::RelPath;
use crate::index::{Entry, Kind, PeerState, VersionVector};
use crate::replica::{Blocks, Delta, Op, Outcome, Precondition};
use crate::scan::{ScanStats, Scope};
use crate::watch::Hint;

/// First message, client to server. Its encoding is fixed for all versions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// [`MAGIC`](super::MAGIC).
    pub magic: [u8; 8],
    /// The protocol versions the client speaks, inclusive.
    pub min_version: u32,
    pub max_version: u32,
    /// The client's own replica: the server's peer.
    pub replica: ReplicaId,
}

/// The server's answer to [`Hello`]. Its encoding is fixed for all versions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HelloReply {
    /// The session speaks `version`; the server's replica is `replica`
    /// (the answer to [`Replica::id`](crate::replica::Replica::id)).
    Welcome {
        magic: [u8; 8],
        version: u32,
        replica: ReplicaId,
    },
    /// The server will not talk to this client; it closes the connection.
    Refused { magic: [u8; 8], reason: String },
}

/// Client to server: one variant per [`Replica`](crate::replica::Replica)
/// call (`id` and `is_remote` need none), plus the content of an
/// [`Request::Apply`] or [`Request::ApplyDelta`].
///
/// Variants are only ever appended, so that the messages of an older
/// version encode as they did; those marked "v2" need a session of protocol
/// version 2 or later (a server refuses them on an older session).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
    /// → [`Response::Scanned`].
    Scan { scope: Scope },
    /// → one or more [`Response::Changes`].
    ChangesSince { seq: u64 },
    /// → [`Response::Reading`] followed by the content, or an error.
    OpenRead { path: RelPath, expect: Entry },
    /// With `content`, the request is followed by the content stream (which
    /// the server consumes in full, even when it does not need it).
    /// → [`Response::Applied`].
    Apply {
        path: RelPath,
        op: Op,
        pre: Precondition,
        content: bool,
    },
    /// → [`Response::Watching`], then [`Response::Hint`]s at any time.
    Watch,
    /// → [`Response::Adopted`].
    Adopt { path: RelPath },
    /// The tombstones come in batches ([`batches`]); every batch but the
    /// last has `more` set and the same `peer` and `retention`.
    /// → one or more [`Response::Collected`].
    RecordSync {
        peer: ReplicaId,
        tombstones: Vec<(RelPath, VersionVector, PeerState)>,
        retention: Duration,
        more: bool,
    },
    /// Part of an [`Request::Apply`]'s or [`Request::ApplyDelta`]'s content.
    Content(Content),
    /// v2. → [`Response::Blocks`].
    Blocks { path: RelPath, expect: Kind },
    /// v2. → [`Response::Reading`] followed by the content (the blocks, in
    /// this order), or an error.
    ReadBlocks {
        path: RelPath,
        expect: Kind,
        blocks: Vec<u32>,
    },
    /// v2. Always followed by a content stream: the blocks `delta` does not
    /// reuse, in order (which the server consumes in full, even when it
    /// does not need it). → [`Response::Applied`].
    ApplyDelta {
        path: RelPath,
        op: Op,
        pre: Precondition,
        delta: Delta,
    },
}

/// Server to client: the answer to each [`Request`], in order, plus watch
/// hints pushed at any time after [`Response::Watching`]`(true)`. Variants
/// are only ever appended (see [`Request`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    Scanned(ScanStats),
    /// A batch of [`Request::ChangesSince`]'s answer, in seq order; every
    /// batch but the last has `more` set.
    Changes {
        entries: Vec<(RelPath, Entry)>,
        more: bool,
    },
    /// The file is open; its content follows.
    Reading,
    Applied(Outcome),
    /// Whether the replica has a watcher, whose hints follow.
    Watching(bool),
    Adopted(bool),
    /// A batch of the paths whose tombstones [`Request::RecordSync`]
    /// removed; every batch but the last has `more` set.
    Collected {
        paths: Vec<RelPath>,
        more: bool,
    },
    /// The request failed.
    Error(WireError),
    /// Part of an [`Response::Reading`]'s content.
    Content(Content),
    /// Pushed by the server's watcher (not an answer to a request).
    Hint(Hint),
    /// v2. The answer to [`Request::Blocks`].
    Blocks(Option<Blocks>),
}

/// File content on the wire: any number of chunks, then `End` or `Abort`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Content {
    Chunk(#[serde(with = "bytes")] Vec<u8>),
    /// The stream is complete; `hash` is the blake3 of all chunks.
    End {
        hash: [u8; 32],
    },
    /// The sender's source failed mid-stream (e.g. the file changed while it
    /// was read); the content is incomplete and must not be used.
    Abort(WireError),
}

/// An [`Error`] as sent over the wire: its class and its message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireError {
    pub kind: RemoteKind,
    pub message: String,
}

impl From<&Error> for WireError {
    fn from(e: &Error) -> WireError {
        let message = match e {
            // Not "remote replica: …" twice when passed on.
            Error::Remote { message, .. } => message.clone(),
            e => e.to_string(),
        };
        WireError {
            kind: e.remote_kind(),
            message,
        }
    }
}

impl From<WireError> for Error {
    /// The error on the receiving side: [`Error::Remote`] with the same class.
    fn from(e: WireError) -> Error {
        Error::Remote {
            kind: e.kind,
            message: e.message,
        }
    }
}

/// Splits `items` into batches of at most `max_bytes` encoded bytes each (a
/// larger item gets a batch of its own). Always returns at least one batch,
/// so an empty list is still answered.
pub fn batches<T: Serialize>(items: Vec<T>, max_bytes: usize) -> Vec<Vec<T>> {
    let mut out = vec![Vec::new()];
    let mut size: usize = 0;
    for item in items {
        // Encoding into a counter cannot fail for our types; count a failure
        // as "too big" so the item still goes out (and fails visibly there).
        let n = postcard::experimental::serialized_size(&item).unwrap_or(usize::MAX);
        let started = out.last().is_some_and(|batch| !batch.is_empty());
        if started && size.saturating_add(n) > max_bytes {
            out.push(Vec::new());
            size = 0;
        }
        out.last_mut().expect("never empty").push(item);
        size = size.saturating_add(n);
    }
    out
}

/// Serde helper: a byte vector as one byte string (postcard: length plus raw
/// bytes, the same encoding as a `Vec<u8>` sequence, but decoded in one copy
/// instead of byte by byte).
mod bytes {
    use std::fmt;

    use serde::de::{SeqAccess, Visitor};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(v)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        d.deserialize_byte_buf(BytesVisitor)
    }

    struct BytesVisitor;

    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a byte string")
        }

        fn visit_bytes<E>(self, v: &[u8]) -> Result<Vec<u8>, E> {
            Ok(v.to_vec())
        }

        fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
            Ok(v)
        }

        // Self-describing formats may send a sequence instead.
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
            let mut v = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(64 * 1024));
            while let Some(b) = seq.next_element()? {
                v.push(b);
            }
            Ok(v)
        }
    }
}
