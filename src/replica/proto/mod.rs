//! The wire protocol between a client and a server replica (design §7.1).
//!
//! A connection is any `Read + Write` byte stream (TLS over TCP in T21; a
//! socket pair in tests). It starts with a version handshake
//! ([`client_handshake`], [`server_handshake`]). After that, the client
//! sends [`Request`]s and the server answers each one, in order, with
//! [`Response`]s ([`Request`] documents which). Every message is one
//! length-prefixed postcard frame ([`framing`]).
//!
//! File content (the answer to [`Request::OpenRead`], the payload of a
//! [`Request::Apply`]) follows its message as [`Content::Chunk`]s and a final
//! [`Content::End`] carrying the blake3 hash of the bytes sent, or
//! [`Content::Abort`] when the sender's source fails mid-stream
//! ([`send_content`], [`ContentStream`]). The receiver checks the hash, so a
//! stream is only complete once it ended cleanly.
//!
//! The protocol carries no CAS logic: preconditions travel as data and are
//! checked by the replica on the server, next to its files (design §7).

pub mod framing;
pub mod messages;

pub use framing::{ContentStream, read_frame, send_content, write_frame};
pub use messages::{Content, Hello, HelloReply, Request, Response, WireError, batches};

use std::io::{Read, Write};

use crate::config::ReplicaId;
use crate::error::{Error, Result};

/// The first bytes of every [`Hello`] and [`HelloReply`].
pub const MAGIC: [u8; 8] = *b"fsync\x00wp";

/// The protocol version this build speaks best. Version 2 added the
/// block-level delta transfer (`Blocks`, `ReadBlocks`, `ApplyDelta`);
/// version 3 the clock exchange (`Clock`, `Witness`).
pub const PROTOCOL_VERSION: u32 = 3;

/// The first version with block-level delta transfer.
pub const DELTA_VERSION: u32 = 2;

/// The first version with the clock exchange (issue #2).
pub const CLOCK_VERSION: u32 = 3;

/// The oldest protocol version this build still speaks.
pub const MIN_PROTOCOL_VERSION: u32 = 1;

/// The largest frame accepted or sent: bounds the memory a peer can make us
/// allocate for one message.
pub const MAX_FRAME: usize = 32 << 20;

/// The content bytes per [`Content::Chunk`].
pub const CHUNK_SIZE: usize = 64 << 10;

/// The encoded size a batch of [`Response::Changes`], [`Request::RecordSync`]
/// or [`Response::Collected`] aims for (see [`batches`]); well below
/// [`MAX_FRAME`], so one huge path cannot push a batch over it.
pub const BATCH_BYTES: usize = 1 << 20;

/// An established session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Session {
    /// The protocol version both sides speak.
    pub version: u32,
    /// The other side's replica.
    pub peer: ReplicaId,
}

fn refused(reason: impl Into<String>) -> Error {
    Error::Protocol {
        reason: reason.into(),
    }
}

/// Opens a session as the client: `local` is our replica, `expect_peer` the
/// replica we must reach. Fails if the server refuses, speaks no common
/// version or serves another replica.
pub fn client_handshake<S: Read + Write + ?Sized>(
    stream: &mut S,
    local: ReplicaId,
    expect_peer: ReplicaId,
) -> Result<Session> {
    client_handshake_upto(stream, local, expect_peer, PROTOCOL_VERSION)
}

/// [`client_handshake`], offering versions up to `max_version` only (at
/// most [`PROTOCOL_VERSION`]; tests force older sessions with it).
pub fn client_handshake_upto<S: Read + Write + ?Sized>(
    stream: &mut S,
    local: ReplicaId,
    expect_peer: ReplicaId,
    max_version: u32,
) -> Result<Session> {
    let max_version = max_version.clamp(MIN_PROTOCOL_VERSION, PROTOCOL_VERSION);
    let hello = Hello {
        magic: MAGIC,
        min_version: MIN_PROTOCOL_VERSION,
        max_version,
        replica: local,
    };
    write_frame(stream, &hello)?;
    let reply = read_frame::<_, HelloReply>(stream)?
        .ok_or_else(|| refused("connection closed during the handshake"))?;
    match reply {
        HelloReply::Welcome { magic, .. } | HelloReply::Refused { magic, .. } if magic != MAGIC => {
            Err(refused("not a files_sync server (bad magic)"))
        }
        HelloReply::Refused { reason, .. } => {
            Err(refused(format!("server refused the session: {reason}")))
        }
        HelloReply::Welcome { version, .. }
            if !(MIN_PROTOCOL_VERSION..=max_version).contains(&version) =>
        {
            Err(refused(format!(
                "server chose protocol version {version}, we speak \
                 {MIN_PROTOCOL_VERSION}..={max_version}"
            )))
        }
        HelloReply::Welcome { replica, .. } if replica != expect_peer => Err(refused(format!(
            "server serves replica {}, expected {}",
            replica.0, expect_peer.0
        ))),
        HelloReply::Welcome {
            version, replica, ..
        } => Ok(Session {
            version,
            peer: replica,
        }),
    }
}

/// Accepts a session as the server of replica `local`, for the client
/// replica `expect_peer`. Picks the highest version both sides speak. A
/// client that cannot be served gets a [`HelloReply::Refused`] (unless it
/// is not speaking this protocol at all) and an error is returned.
pub fn server_handshake<S: Read + Write + ?Sized>(
    stream: &mut S,
    local: ReplicaId,
    expect_peer: ReplicaId,
) -> Result<Session> {
    server_handshake_upto(stream, local, expect_peer, PROTOCOL_VERSION)
}

/// [`server_handshake`], speaking versions up to `max_version` only (at
/// most [`PROTOCOL_VERSION`]; tests force older sessions with it).
pub fn server_handshake_upto<S: Read + Write + ?Sized>(
    stream: &mut S,
    local: ReplicaId,
    expect_peer: ReplicaId,
    max_version: u32,
) -> Result<Session> {
    let max_version = max_version.clamp(MIN_PROTOCOL_VERSION, PROTOCOL_VERSION);
    let hello = read_frame::<_, Hello>(stream)?
        .ok_or_else(|| refused("connection closed before the handshake"))?;
    if hello.magic != MAGIC {
        return Err(refused("not a files_sync client (bad magic)"));
    }
    let version = max_version.min(hello.max_version);
    let problem = if hello.min_version > hello.max_version {
        Some(format!(
            "empty version range {}..={}",
            hello.min_version, hello.max_version
        ))
    } else if version < MIN_PROTOCOL_VERSION.max(hello.min_version) {
        Some(format!(
            "no common protocol version: client speaks {}..={}, server \
             {MIN_PROTOCOL_VERSION}..={max_version}",
            hello.min_version, hello.max_version
        ))
    } else if hello.replica != expect_peer {
        Some(format!(
            "client is replica {}, expected {}",
            hello.replica.0, expect_peer.0
        ))
    } else {
        None
    };
    if let Some(reason) = problem {
        let reply = HelloReply::Refused {
            magic: MAGIC,
            reason: reason.clone(),
        };
        // Best effort: we fail either way.
        let _ = write_frame(stream, &reply);
        return Err(refused(format!("refused the client: {reason}")));
    }
    write_frame(
        stream,
        &HelloReply::Welcome {
            magic: MAGIC,
            version,
            replica: local,
        },
    )?;
    Ok(Session {
        version,
        peer: hello.replica,
    })
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};
    use std::os::unix::net::UnixStream;
    use std::thread;
    use std::time::Duration;

    use proptest::prelude::*;

    use super::*;
    use crate::error::RemoteKind;
    use crate::fs::RelPath;
    use crate::fs::commit::FileMeta;
    use crate::index::{Entry, Kind, LocalMeta, PeerState, UnmanagedReason, VersionVector};
    use crate::replica::{Blocks, Delta, Op, Outcome, Precondition};
    use crate::scan::{ScanStats, Scope};
    use crate::watch::Hint;

    const A: ReplicaId = ReplicaId(0xA1);
    const B: ReplicaId = ReplicaId(0xB2);

    fn p(s: &[u8]) -> RelPath {
        RelPath::new(s).unwrap()
    }

    fn vv(pairs: &[(u64, u64)]) -> VersionVector {
        pairs.iter().map(|&(id, c)| (ReplicaId(id), c)).collect()
    }

    fn entry(kind: Kind) -> Entry {
        Entry {
            kind,
            mode: 0o1755,
            mtime_ns: -1_234_567_890,
            vv: vv(&[(0xA1, 3), (0xB2, u64::MAX >> 2)]),
            seq: 42,
            local: LocalMeta::default(),
        }
    }

    /// Every [`Kind`], with non-UTF-8 bytes where bytes are allowed.
    fn kinds() -> Vec<Kind> {
        let mut kinds = vec![
            Kind::File {
                size: u64::MAX,
                hash: [0xEE; 32],
            },
            Kind::Dir,
            Kind::Symlink {
                target: b"../\xff\xfe/x".to_vec(),
            },
            Kind::Tombstone,
        ];
        for reason in [
            UnmanagedReason::IgnoredLink,
            UnmanagedReason::Dangling,
            UnmanagedReason::Loop,
            UnmanagedReason::Special,
        ] {
            kinds.push(Kind::Unmanaged(reason));
        }
        kinds
    }

    fn ops() -> Vec<Op> {
        vec![
            Op::WriteFile {
                meta: FileMeta {
                    mode: 0o644,
                    mtime_ns: 1,
                },
                hash: [7; 32],
                vv: vv(&[(1, 1)]),
            },
            Op::Mkdir {
                mode: 0o700,
                mtime_ns: i64::MIN,
                vv: vv(&[(1, 2)]),
            },
            Op::Symlink {
                target: b"/abs/\x80".to_vec(),
                mtime_ns: i64::MAX,
                vv: vv(&[(2, 1)]),
            },
            Op::Delete { vv: vv(&[(1, 5)]) },
            Op::Rmdir {
                vv: VersionVector::new(),
            },
            Op::RenameToConflict {
                to: p(b"a/f.sync-conflict-20260101-000000-ABCDEFG.txt"),
            },
            Op::SetMeta {
                mode: 0o1777,
                mtime_ns: 0,
                vv: vv(&[(1, 1), (2, 2)]),
            },
        ]
    }

    fn wire_errors() -> Vec<WireError> {
        [
            RemoteKind::Unstable,
            RemoteKind::NotFound,
            RemoteKind::InvalidOp,
            RemoteKind::InvalidPath,
            RemoteKind::Index,
            RemoteKind::Protocol,
            RemoteKind::Other,
        ]
        .into_iter()
        .map(|kind| WireError {
            kind,
            message: format!("{kind:?} happened"),
        })
        .collect()
    }

    fn contents() -> Vec<Content> {
        vec![
            Content::Chunk(Vec::new()),
            Content::Chunk((0..=255).collect()),
            Content::End { hash: [3; 32] },
            Content::Abort(wire_errors().remove(0)),
        ]
    }

    fn requests() -> Vec<Request> {
        let mut out = vec![
            Request::Scan { scope: Scope::Full },
            Request::Scan {
                scope: Scope::Paths(vec![p(b"a"), p(b"b/\xc3\x28")]),
            },
            Request::ChangesSince { seq: u64::MAX },
            Request::Watch,
            Request::Adopt {
                path: p(b"dirlink"),
            },
            Request::RecordSync {
                peer: B,
                tombstones: vec![
                    (p(b"x"), vv(&[(1, 1)]), PeerState::Absent),
                    (p(b"y"), vv(&[(1, 2)]), PeerState::Tombstone(vv(&[(2, 9)]))),
                    (p(b"z"), vv(&[(2, 3)]), PeerState::Live),
                ],
                retention: Duration::new(30 * 24 * 3600, 999_999_999),
                more: true,
            },
        ];
        out.extend(kinds().into_iter().map(|kind| Request::OpenRead {
            path: p(b"dir/f"),
            expect: entry(kind),
        }));
        for (i, op) in ops().into_iter().enumerate() {
            let pre = if i % 2 == 0 {
                Precondition::Absent
            } else {
                Precondition::Matches {
                    kind: kinds()[i].clone(),
                    vv: vv(&[(1, i as u64 + 1)]),
                }
            };
            out.push(Request::Apply {
                path: p(b"f"),
                op,
                pre,
                content: i == 0,
            });
        }
        out.extend(contents().into_iter().map(Request::Content));
        out.push(Request::Blocks {
            path: p(b"big"),
            expect: kinds().remove(0),
        });
        out.push(Request::ReadBlocks {
            path: p(b"big"),
            expect: kinds().remove(0),
            blocks: vec![0, 7, u32::MAX],
        });
        out.push(Request::ApplyDelta {
            path: p(b"big"),
            op: ops().remove(0),
            pre: Precondition::Matches {
                kind: kinds().remove(0),
                vv: vv(&[(1, 1)]),
            },
            delta: Delta {
                blocks: block_list(),
                reuse: vec![Some(1), None, Some(u32::MAX)],
            },
        });
        out.push(Request::Clock);
        out.push(Request::Witness { floor: 0 });
        out.push(Request::Witness { floor: u64::MAX });
        out
    }

    fn block_list() -> Blocks {
        Blocks {
            size: 300 << 10,
            hashes: vec![[1; 32], [2; 32], [0xFF; 32]],
        }
    }

    fn responses() -> Vec<Response> {
        let mut out = vec![
            Response::Scanned(ScanStats::default()),
            Response::Scanned(ScanStats {
                scanned: 1,
                hashed: 2,
                changed: 3,
                tombstoned: 4,
                dirty: vec![p(b"d")],
                errors: vec![p(b"e/\xff")],
            }),
            Response::Changes {
                entries: Vec::new(),
                more: false,
            },
            Response::Changes {
                entries: kinds()
                    .into_iter()
                    .enumerate()
                    .map(|(i, k)| (p(format!("p{i}").as_bytes()), entry(k)))
                    .collect(),
                more: true,
            },
            Response::Reading,
            Response::Applied(Outcome::Applied(entry(Kind::Dir))),
            Response::Applied(Outcome::PreconditionFailed(None)),
            Response::Applied(Outcome::PreconditionFailed(Some(entry(Kind::Tombstone)))),
            Response::Applied(Outcome::Preserved {
                conflict: p(b"f.sync-conflict-20260101-000000-ABCDEFG"),
            }),
            Response::Watching(true),
            Response::Watching(false),
            Response::Adopted(true),
            Response::Collected {
                paths: vec![p(b"gone"), p(b"also/gone")],
                more: false,
            },
            Response::Hint(Hint::Paths(vec![p(b"h")])),
            Response::Hint(Hint::FullRescan),
        ];
        out.extend(wire_errors().into_iter().map(Response::Error));
        out.extend(contents().into_iter().map(Response::Content));
        out.push(Response::Blocks(None));
        out.push(Response::Blocks(Some(block_list())));
        out.push(Response::Clock(0));
        out.push(Response::Clock(u64::MAX));
        out
    }

    /// The variant names covered by `requests()`; the match has no wildcard,
    /// so a new variant does not compile until it is listed here.
    fn request_variant(r: &Request) -> &'static str {
        match r {
            Request::Scan { .. } => "Scan",
            Request::ChangesSince { .. } => "ChangesSince",
            Request::OpenRead { .. } => "OpenRead",
            Request::Apply { .. } => "Apply",
            Request::Watch => "Watch",
            Request::Adopt { .. } => "Adopt",
            Request::RecordSync { .. } => "RecordSync",
            Request::Content(_) => "Content",
            Request::Blocks { .. } => "Blocks",
            Request::ReadBlocks { .. } => "ReadBlocks",
            Request::ApplyDelta { .. } => "ApplyDelta",
            Request::Clock => "Clock",
            Request::Witness { .. } => "Witness",
        }
    }

    fn response_variant(r: &Response) -> &'static str {
        match r {
            Response::Scanned(_) => "Scanned",
            Response::Changes { .. } => "Changes",
            Response::Reading => "Reading",
            Response::Applied(Outcome::Applied(_)) => "Applied(Applied)",
            Response::Applied(Outcome::PreconditionFailed(_)) => "Applied(PreconditionFailed)",
            Response::Applied(Outcome::Preserved { .. }) => "Applied(Preserved)",
            Response::Watching(_) => "Watching",
            Response::Adopted(_) => "Adopted",
            Response::Collected { .. } => "Collected",
            Response::Error(_) => "Error",
            Response::Content(_) => "Content",
            Response::Hint(_) => "Hint",
            Response::Blocks(_) => "Blocks",
            Response::Clock(_) => "Clock",
        }
    }

    fn op_variant(op: &Op) -> &'static str {
        match op {
            Op::WriteFile { .. } => "WriteFile",
            Op::Mkdir { .. } => "Mkdir",
            Op::Symlink { .. } => "Symlink",
            Op::Delete { .. } => "Delete",
            Op::Rmdir { .. } => "Rmdir",
            Op::RenameToConflict { .. } => "RenameToConflict",
            Op::SetMeta { .. } => "SetMeta",
        }
    }

    fn content_variant(c: &Content) -> &'static str {
        match c {
            Content::Chunk(_) => "Chunk",
            Content::End { .. } => "End",
            Content::Abort(_) => "Abort",
        }
    }

    fn variants<T>(items: &[T], name: fn(&T) -> &'static str) -> Vec<&'static str> {
        let mut names: Vec<_> = items.iter().map(name).collect();
        names.sort();
        names.dedup();
        names
    }

    /// Writes `msgs` into one end of a pipe from another thread and reads
    /// them back from the other end, then expects a clean end of stream.
    fn through_pipe<T>(msgs: Vec<T>) -> Vec<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
    {
        let (mut rx, mut tx) = io::pipe().unwrap();
        let n = msgs.len();
        let writer = thread::spawn(move || {
            for m in &msgs {
                write_frame(&mut tx, m).unwrap();
            }
        });
        let back: Vec<T> = (0..n)
            .map(|_| read_frame(&mut rx).unwrap().expect("a message"))
            .collect();
        writer.join().unwrap();
        assert!(read_frame::<_, T>(&mut rx).unwrap().is_none(), "clean EOF");
        back
    }

    #[test]
    fn every_message_round_trips_over_a_pipe() {
        let reqs = requests();
        assert_eq!(
            variants(&reqs, request_variant),
            [
                "Adopt",
                "Apply",
                "ApplyDelta",
                "Blocks",
                "ChangesSince",
                "Clock",
                "Content",
                "OpenRead",
                "ReadBlocks",
                "RecordSync",
                "Scan",
                "Watch",
                "Witness"
            ]
        );
        let ops: Vec<Op> = reqs
            .iter()
            .filter_map(|r| match r {
                Request::Apply { op, .. } => Some(op.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(variants(&ops, op_variant).len(), 7, "every Op");
        assert_eq!(through_pipe(reqs.clone()), reqs);

        let resps = responses();
        assert_eq!(
            variants(&resps, response_variant).len(),
            14,
            "every Response"
        );
        assert_eq!(through_pipe(resps.clone()), resps);
        assert_eq!(variants(&contents(), content_variant).len(), 3);

        let hellos = vec![Hello {
            magic: MAGIC,
            min_version: 1,
            max_version: u32::MAX,
            replica: A,
        }];
        assert_eq!(through_pipe(hellos.clone()), hellos);
        let replies = vec![
            HelloReply::Welcome {
                magic: MAGIC,
                version: 1,
                replica: B,
            },
            HelloReply::Refused {
                magic: MAGIC,
                reason: "no".into(),
            },
        ];
        assert_eq!(through_pipe(replies.clone()), replies);
    }

    /// Versions 2 and 3 only appended variants, so every older message
    /// encodes as it did (postcard: the variant index comes first).
    #[test]
    fn newer_variants_are_appended() {
        let tag = |body: Vec<u8>| body[0];
        let req = |r: &Request| tag(postcard::to_stdvec(r).unwrap());
        let resp = |r: &Response| tag(postcard::to_stdvec(r).unwrap());
        assert_eq!(req(&Request::Scan { scope: Scope::Full }), 0);
        assert_eq!(req(&Request::Content(Content::Chunk(Vec::new()))), 7);
        for (r, n) in requests().iter().filter_map(|r| match r {
            Request::Blocks { .. } => Some((r, 8)),
            Request::ReadBlocks { .. } => Some((r, 9)),
            Request::ApplyDelta { .. } => Some((r, 10)),
            Request::Clock => Some((r, 11)),
            Request::Witness { .. } => Some((r, 12)),
            _ => None,
        }) {
            assert_eq!(req(r), n);
        }
        assert_eq!(resp(&Response::Scanned(ScanStats::default())), 0);
        assert_eq!(resp(&Response::Hint(Hint::FullRescan)), 9);
        assert_eq!(resp(&Response::Blocks(None)), 10);
        assert_eq!(resp(&Response::Clock(0)), 11);
    }

    #[test]
    fn local_meta_never_crosses_the_wire() {
        let mut e = entry(Kind::Symlink {
            target: b"t".to_vec(),
        });
        e.local = LocalMeta {
            dev: 1,
            ino: 2,
            ctime_ns: 3,
            mnt_id: 4,
            raw_target: Some(b"munged".to_vec()),
            via_link: None,
            racy: true,
        };
        let msg = Response::Applied(Outcome::Applied(e.clone()));
        let back = through_pipe(vec![msg]).remove(0);
        e.local = LocalMeta::default();
        assert_eq!(back, Response::Applied(Outcome::Applied(e)));
    }

    #[test]
    fn frame_layout_is_length_prefixed_postcard() {
        let mut buf = Vec::new();
        write_frame(&mut buf, &Request::ChangesSince { seq: 300 }).unwrap();
        let body = postcard::to_stdvec(&Request::ChangesSince { seq: 300 }).unwrap();
        assert_eq!(&buf[..4], &(body.len() as u32).to_be_bytes());
        assert_eq!(&buf[4..], &body[..]);
        // A chunk is a byte string: length plus raw bytes, no per-byte cost.
        let mut buf = Vec::new();
        write_frame(&mut buf, &Content::Chunk(vec![0xFF; 1000])).unwrap();
        assert_eq!(buf.len(), 4 + 1 + 2 + 1000);
    }

    // ---- handshake ----

    /// Runs `server` and `client` on the two ends of a socket pair.
    fn handshake<C, S, RC, RS>(client: C, server: S) -> (RC, RS)
    where
        C: FnOnce(&mut UnixStream) -> RC + Send + 'static,
        S: FnOnce(&mut UnixStream) -> RS + Send + 'static,
        RC: Send + 'static,
        RS: Send + 'static,
    {
        let (mut c, mut s) = UnixStream::pair().unwrap();
        let server = thread::spawn(move || {
            let r = server(&mut s);
            drop(s);
            r
        });
        let rc = client(&mut c);
        drop(c);
        (rc, server.join().unwrap())
    }

    fn reason(e: Error) -> String {
        match e {
            Error::Protocol { reason } => reason,
            e => panic!("expected a protocol error, got {e:?}"),
        }
    }

    #[test]
    fn handshake_agrees_on_version_and_replicas() {
        let (c, s) = handshake(|c| client_handshake(c, A, B), |s| server_handshake(s, B, A));
        let expected = |peer| Session {
            version: PROTOCOL_VERSION,
            peer,
        };
        assert_eq!(c.unwrap(), expected(B));
        assert_eq!(s.unwrap(), expected(A));

        // A newer client that still speaks our version gets ours.
        let (c, s) = handshake(
            |c| {
                let hello = Hello {
                    magic: MAGIC,
                    min_version: 1,
                    max_version: 9,
                    replica: A,
                };
                write_frame(c, &hello).unwrap();
                read_frame::<_, HelloReply>(c).unwrap().unwrap()
            },
            |s| server_handshake(s, B, A),
        );
        assert_eq!(
            c,
            HelloReply::Welcome {
                magic: MAGIC,
                version: PROTOCOL_VERSION,
                replica: B
            }
        );
        assert_eq!(s.unwrap().version, PROTOCOL_VERSION);
    }

    #[test]
    fn handshake_versions_can_be_capped() {
        // An old client (or one capped at v1) gets a v1 session.
        let (c, s) = handshake(
            |c| client_handshake_upto(c, A, B, 1),
            |s| server_handshake(s, B, A),
        );
        assert_eq!(c.unwrap().version, 1);
        assert_eq!(s.unwrap().version, 1);
        // So does any client of a server capped at v1.
        let (c, s) = handshake(
            |c| client_handshake(c, A, B),
            |s| server_handshake_upto(s, B, A, 1),
        );
        assert_eq!(c.unwrap().version, 1);
        assert_eq!(s.unwrap().version, 1);
        // A client capped at v1 refuses a server that answers v2.
        let (c, _) = handshake(
            |c| client_handshake_upto(c, A, B, 1),
            |s| {
                read_frame::<_, Hello>(s).unwrap().unwrap();
                let reply = HelloReply::Welcome {
                    magic: MAGIC,
                    version: 2,
                    replica: B,
                };
                write_frame(s, &reply).unwrap();
            },
        );
        assert!(reason(c.unwrap_err()).contains("server chose protocol version 2"));
    }

    #[test]
    fn handshake_refusals() {
        // No common version: the server refuses, the client sees why.
        let (c, s) = handshake(
            |c| {
                let hello = Hello {
                    magic: MAGIC,
                    min_version: PROTOCOL_VERSION + 1,
                    max_version: PROTOCOL_VERSION + 3,
                    replica: A,
                };
                write_frame(c, &hello).unwrap();
                read_frame::<_, HelloReply>(c).unwrap().unwrap()
            },
            |s| server_handshake(s, B, A),
        );
        assert!(
            matches!(c, HelloReply::Refused { ref reason, .. } if reason.contains("no common"))
        );
        assert!(reason(s.unwrap_err()).contains("no common protocol version"));

        // Each side checks the other's replica.
        let (c, s) = handshake(
            |c| client_handshake(c, ReplicaId(7), B),
            |s| server_handshake(s, B, A),
        );
        assert!(reason(c.unwrap_err()).contains("client is replica 7, expected 161"));
        assert!(reason(s.unwrap_err()).contains("client is replica 7"));
        let (c, s) = handshake(
            |c| client_handshake(c, A, ReplicaId(9)),
            |s| server_handshake(s, B, A),
        );
        assert!(reason(c.unwrap_err()).contains("server serves replica 178, expected 9"));
        // The server is done before the client checks.
        assert!(s.is_ok());

        // A server answering with a version we don't speak.
        let (c, _) = handshake(
            |c| client_handshake(c, A, B),
            |s| {
                read_frame::<_, Hello>(s).unwrap().unwrap();
                let reply = HelloReply::Welcome {
                    magic: MAGIC,
                    version: PROTOCOL_VERSION + 1,
                    replica: B,
                };
                write_frame(s, &reply).unwrap();
            },
        );
        assert!(reason(c.unwrap_err()).contains("server chose protocol version"));

        // Bad magic either way; a peer that hangs up.
        let (c, s) = handshake(
            |c| {
                let hello = Hello {
                    magic: *b"HTTP/1.1",
                    min_version: 1,
                    max_version: 1,
                    replica: A,
                };
                write_frame(c, &hello).unwrap();
                read_frame::<_, HelloReply>(c).unwrap()
            },
            |s| server_handshake(s, B, A),
        );
        assert_eq!(c, None, "no reply to a stranger");
        assert!(reason(s.unwrap_err()).contains("bad magic"));
        let (c, _) = handshake(
            |c| client_handshake(c, A, B),
            |s| {
                read_frame::<_, Hello>(s).unwrap().unwrap();
                let reply = HelloReply::Refused {
                    magic: [0; 8],
                    reason: String::new(),
                };
                write_frame(s, &reply).unwrap();
            },
        );
        assert!(reason(c.unwrap_err()).contains("bad magic"));
        let (c, s) = handshake(
            |c| client_handshake(c, A, B),
            |s| read_frame::<_, Hello>(s).unwrap().map(|_| ()),
        );
        assert!(reason(c.unwrap_err()).contains("closed during the handshake"));
        assert_eq!(s, Some(()));
        let (_, s) = handshake(|_| (), |s| server_handshake(s, B, A));
        assert!(reason(s.unwrap_err()).contains("closed before the handshake"));
    }

    // ---- content streams ----

    /// A content stream of `Response` frames, with `Hint`s set aside.
    type Stream<'a> = ContentStream<
        &'a mut Cursor<Vec<u8>>,
        Response,
        Box<dyn FnMut(Response) -> Result<Option<Content>> + 'a>,
    >;

    fn response_content<'a>(
        hints: &'a mut Vec<Hint>,
    ) -> Box<dyn FnMut(Response) -> Result<Option<Content>> + 'a> {
        Box::new(move |msg| match msg {
            Response::Content(c) => Ok(Some(c)),
            Response::Hint(h) => {
                hints.push(h);
                Ok(None)
            }
            other => Err(Error::Protocol {
                reason: format!("unexpected {}", response_variant(&other)),
            }),
        })
    }

    fn data(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 7 + i / 251) as u8).collect()
    }

    fn sent(src: &[u8]) -> (Vec<u8>, [u8; 32]) {
        let mut buf = Vec::new();
        let hash = send_content(&mut buf, &mut &src[..], Response::Content)
            .unwrap()
            .unwrap();
        (buf, hash)
    }

    #[test]
    fn content_streams_in_chunks_and_checks_the_hash() {
        for n in [0, 1, CHUNK_SIZE, 3 * CHUNK_SIZE + 17] {
            let src = data(n);
            let (mut buf, hash) = sent(&src);
            assert_eq!(hash, *blake3::hash(&src).as_bytes());
            write_frame(&mut buf, &Response::Watching(false)).unwrap();
            let frames = n.div_ceil(CHUNK_SIZE) + 1;
            let mut cur = Cursor::new(buf);
            let mut hints = Vec::new();
            let mut stream: Stream = ContentStream::new(&mut cur, response_content(&mut hints));
            let mut got = Vec::new();
            stream.read_to_end(&mut got).unwrap();
            assert_eq!(got, src, "{n} bytes");
            assert!(stream.is_finished());
            assert_eq!(stream.read(&mut [0; 8]).unwrap(), 0, "stays at EOF");
            stream.drain().unwrap();
            drop(stream);
            assert_eq!(
                read_frame::<_, Response>(&mut cur).unwrap(),
                Some(Response::Watching(false)),
                "in step after {frames} content frames"
            );
        }
    }

    /// Yields `ok` bytes, then fails with an unstable-path error.
    struct Torn {
        ok: usize,
    }

    impl Read for Torn {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.ok == 0 {
                return Err(io::Error::other(Error::Unstable {
                    path: b"f".to_vec(),
                    reason: "file changed while it was read",
                }));
            }
            let n = buf.len().min(self.ok);
            buf[..n].fill(1);
            self.ok -= n;
            Ok(n)
        }
    }

    #[test]
    fn a_failing_source_aborts_the_stream() {
        let mut buf = Vec::new();
        let source_err = send_content(&mut buf, &mut Torn { ok: CHUNK_SIZE + 5 }, Request::Content)
            .unwrap()
            .unwrap_err();
        assert!(source_err.is_unstable(), "{source_err:?}");
        write_frame(&mut buf, &Request::Watch).unwrap();

        let mut cur = Cursor::new(buf);
        let filter = |msg| match msg {
            Request::Content(c) => Ok(Some(c)),
            _ => Err(Error::Protocol {
                reason: "not content".into(),
            }),
        };
        let mut stream = ContentStream::new(&mut cur, filter);
        let mut got = Vec::new();
        let e = Error::from_stream("read", stream.read_to_end(&mut got).unwrap_err());
        assert_eq!(got.len(), CHUNK_SIZE + 5, "the chunks before the abort");
        assert!(e.is_unstable(), "{e:?}");
        assert!(matches!(&e, Error::Remote { message, .. } if message.contains("file changed")));
        assert!(stream.is_finished());
        let again = Error::from_stream("read", stream.read(&mut [0; 4]).unwrap_err());
        assert!(again.is_unstable(), "stays failed");
        stream.drain().unwrap();
        drop(stream);
        assert_eq!(read_frame(&mut cur).unwrap(), Some(Request::Watch));
    }

    #[test]
    fn bad_content_streams_fail() {
        // Wrong hash.
        let mut buf = Vec::new();
        write_frame(
            &mut buf,
            &Response::Content(Content::Chunk(b"abc".to_vec())),
        )
        .unwrap();
        write_frame(&mut buf, &Response::Content(Content::End { hash: [0; 32] })).unwrap();
        let mut cur = Cursor::new(buf);
        let mut hints = Vec::new();
        let mut stream: Stream = ContentStream::new(&mut cur, response_content(&mut hints));
        let e = Error::from_stream("read", stream.read_to_end(&mut Vec::new()).unwrap_err());
        assert!(reason(e).contains("hash mismatch"));
        assert!(stream.is_finished());
        stream.drain().unwrap();

        // A message that does not belong in the stream breaks it; a pushed
        // hint is handed to the filter and skipped.
        let (mut buf, _) = sent(&data(10));
        buf.splice(0..0, frame(&Response::Hint(Hint::FullRescan)));
        let last = buf.len() - frame(&Response::Content(Content::End { hash: [0; 32] })).len();
        buf.splice(last..last, frame(&Response::Reading));
        let mut cur = Cursor::new(buf);
        let mut hints = Vec::new();
        let mut stream: Stream = ContentStream::new(&mut cur, response_content(&mut hints));
        let mut got = Vec::new();
        let e = Error::from_stream("read", stream.read_to_end(&mut got).unwrap_err());
        assert_eq!(got, data(10));
        assert!(reason(e).contains("unexpected Reading"));
        assert!(!stream.is_finished());
        assert!(reason(stream.drain().unwrap_err()).contains("unexpected Reading"));
        drop(stream);
        assert_eq!(hints, [Hint::FullRescan]);

        // The connection closes mid-stream, at a frame boundary or inside one.
        let (buf, _) = sent(&data(3 * CHUNK_SIZE));
        for cut in [
            frame(&Response::Content(Content::Chunk(Vec::new()))).len() + 7,
            buf.len() - 5,
        ] {
            let mut cur = Cursor::new(buf[..cut].to_vec());
            let mut hints = Vec::new();
            let mut stream: Stream = ContentStream::new(&mut cur, response_content(&mut hints));
            let e = Error::from_stream("read", stream.read_to_end(&mut Vec::new()).unwrap_err());
            assert!(reason(e).contains("connection closed"), "cut at {cut}");
        }
        let mut cur = Cursor::new(Vec::new());
        let mut hints = Vec::new();
        let mut stream: Stream = ContentStream::new(&mut cur, response_content(&mut hints));
        assert!(reason(stream.drain().unwrap_err()).contains("closed inside a content stream"));
    }

    #[test]
    fn drain_skips_an_unread_stream() {
        let (mut buf, _) = sent(&data(5 * CHUNK_SIZE));
        write_frame(&mut buf, &Response::Adopted(true)).unwrap();
        let mut cur = Cursor::new(buf);
        let mut hints = Vec::new();
        let mut stream: Stream = ContentStream::new(&mut cur, response_content(&mut hints));
        let mut first = [0u8; 100];
        stream.read_exact(&mut first).unwrap();
        assert_eq!(first[..], data(100)[..]);
        assert!(!stream.is_finished());
        stream.drain().unwrap();
        assert!(stream.is_finished());
        assert_eq!(
            stream.read(&mut first).unwrap(),
            0,
            "drained to a verified end"
        );
        drop(stream);
        assert_eq!(read_frame(&mut cur).unwrap(), Some(Response::Adopted(true)));
    }

    // ---- malformed input ----

    fn frame<T: serde::Serialize>(msg: &T) -> Vec<u8> {
        let mut buf = Vec::new();
        write_frame(&mut buf, msg).unwrap();
        buf
    }

    /// A frame with `body` as its payload, whatever it is.
    fn raw(body: &[u8]) -> Vec<u8> {
        let mut buf = (body.len() as u32).to_be_bytes().to_vec();
        buf.extend_from_slice(body);
        buf
    }

    fn read_err<T: serde::de::DeserializeOwned + std::fmt::Debug>(bytes: &[u8]) -> String {
        reason(read_frame::<_, T>(&mut &bytes[..]).unwrap_err())
    }

    /// The body of `Request::Apply { path, op: Delete { vv }, .. }` with the
    /// raw encodings of the path and the vector spliced in.
    fn apply_delete(path: &[u8], vv: &[u8]) -> Vec<u8> {
        let mut body = vec![3]; // Request::Apply
        body.extend_from_slice(path);
        body.push(3); // Op::Delete
        body.extend_from_slice(vv);
        body.push(0); // Precondition::Absent
        body.push(0); // content: false
        body
    }

    #[test]
    fn malformed_frames_are_errors() {
        let good = frame(&Request::ChangesSince { seq: 1 });
        let cases: Vec<(&str, Vec<u8>, &str)> = vec![
            ("short header", vec![0, 0], "inside a frame header"),
            (
                "short body",
                good[..good.len() - 1].to_vec(),
                "inside a frame",
            ),
            ("empty frame", raw(&[]), "empty frame"),
            (
                "oversized",
                ((MAX_FRAME + 1) as u32).to_be_bytes().to_vec(),
                "exceeds the limit",
            ),
            ("huge length", vec![0xFF; 4], "exceeds the limit"),
            ("unknown variant", raw(&[0x7F]), "malformed message"),
            (
                "variant tag overflow",
                raw(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]),
                "malformed",
            ),
            ("truncated message", raw(&[1]), "malformed message"),
            ("trailing bytes", raw(&[1, 1, 0]), "trailing bytes"),
            (
                "bool out of range",
                raw(&[3, 1, b'f', 3, 0, 0, 2]),
                "malformed message",
            ),
            (
                "path with ..",
                raw(&apply_delete(b"\x04a/..", &[0])),
                "malformed message",
            ),
            (
                "absolute path",
                raw(&apply_delete(b"\x02/a", &[0])),
                "malformed",
            ),
            (
                "NUL in path",
                raw(&apply_delete(b"\x03a\x00b", &[0])),
                "malformed",
            ),
            (
                "unsorted vv",
                raw(&apply_delete(b"\x01f", &[2, 5, 1, 1, 1])),
                "malformed message",
            ),
            (
                "zero counter",
                raw(&apply_delete(b"\x01f", &[1, 1, 0])),
                "malformed",
            ),
            (
                "vv claims 2^60 counters",
                raw(&apply_delete(
                    b"\x01f",
                    &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x10],
                )),
                "malformed",
            ),
            (
                "path claims 4 GiB",
                raw(&apply_delete(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F], &[0])),
                "malformed",
            ),
        ];
        for (what, bytes, expect) in cases {
            let msg = read_err::<Request>(&bytes);
            assert!(msg.contains(expect), "{what}: {msg}");
        }
        // Sanity: the hand-made Apply encoding is right when valid.
        let ok = read_frame::<_, Request>(&mut &raw(&apply_delete(b"\x01f", &[1, 1, 1]))[..]);
        assert_eq!(
            ok.unwrap(),
            Some(Request::Apply {
                path: p(b"f"),
                op: Op::Delete { vv: vv(&[(1, 1)]) },
                pre: Precondition::Absent,
                content: false
            })
        );

        // Invalid UTF-8 in an error message.
        let body = [7, 0, 2, 0xC3, 0x28]; // Response::Error(Unstable, "\xc3\x28")
        assert!(read_err::<Response>(&raw(&body)).contains("malformed"));
        let body = [7, 0, 2, 0xC3, 0xA9]; // "é"
        assert!(read_frame::<_, Response>(&mut &raw(&body)[..]).is_ok());

        // A retention whose nanoseconds carry the seconds past u64::MAX.
        let sync = |nanos| Request::RecordSync {
            peer: A,
            tombstones: Vec::new(),
            retention: Duration::new(u64::MAX, nanos),
            more: false,
        };
        let mut body = postcard::to_stdvec(&sync(0)).unwrap();
        assert_eq!(body[body.len() - 2..], [0, 0], "nanos = 0, more = false");
        body.truncate(body.len() - 2);
        body.extend(postcard::to_stdvec(&1_000_000_000u32).unwrap());
        body.push(0);
        assert!(read_err::<Request>(&raw(&body)).contains("malformed"));

        // I/O errors stay I/O errors.
        struct Failing;
        impl Read for Failing {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::from(io::ErrorKind::ConnectionReset))
            }
        }
        assert!(matches!(
            read_frame::<_, Request>(&mut Failing),
            Err(Error::Io { .. })
        ));
    }

    #[test]
    fn oversized_messages_are_not_sent() {
        let mut buf = Vec::new();
        let e = write_frame(&mut buf, &Content::Chunk(vec![0; MAX_FRAME])).unwrap_err();
        assert!(reason(e).contains("exceeds the frame limit"));
        assert!(buf.is_empty(), "nothing written");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        /// Arbitrary bytes never panic the decoder.
        #[test]
        fn random_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
            let _ = read_frame::<_, Request>(&mut &bytes[..]);
            let _ = read_frame::<_, Response>(&mut &raw(&bytes)[..]);
            let _ = read_frame::<_, Request>(&mut &raw(&bytes)[..]);
            let _ = read_frame::<_, Hello>(&mut &raw(&bytes)[..]);
            let _ = read_frame::<_, HelloReply>(&mut &raw(&bytes)[..]);
        }

        /// Neither do corrupted or truncated valid messages.
        #[test]
        fn corrupted_messages_never_panic(
            i in 0usize..64,
            flips in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 1..4),
            cut in any::<prop::sample::Index>(),
        ) {
            let msgs: Vec<Vec<u8>> = requests()
                .iter()
                .map(|m| postcard::to_stdvec(m).unwrap())
                .chain(responses().iter().map(|m| postcard::to_stdvec(m).unwrap()))
                .collect();
            let mut body = msgs[i % msgs.len()].clone();
            for (at, b) in flips {
                let at = at.index(body.len());
                body[at] ^= b;
            }
            body.truncate(1 + cut.index(body.len()));
            let _ = read_frame::<_, Request>(&mut &raw(&body)[..]);
            let _ = read_frame::<_, Response>(&mut &raw(&body)[..]);
        }
    }

    // ---- batches and errors ----

    #[test]
    fn batches_split_by_encoded_size() {
        assert_eq!(
            batches(Vec::<RelPath>::new(), 10),
            vec![Vec::<RelPath>::new()]
        );
        let paths: Vec<RelPath> = (0..10).map(|i| p(format!("{i:03}").as_bytes())).collect();
        // Each path encodes to 4 bytes.
        let b = batches(paths.clone(), 9);
        assert_eq!(b.iter().map(Vec::len).collect::<Vec<_>>(), [2, 2, 2, 2, 2]);
        assert_eq!(b.concat(), paths);
        let b = batches(paths.clone(), 3);
        assert_eq!(b.len(), 10, "an item larger than the limit goes alone");
        assert_eq!(batches(paths.clone(), BATCH_BYTES), vec![paths]);
    }

    #[test]
    fn errors_keep_their_class_across_the_wire() {
        let cases = [
            (
                Error::Unstable {
                    path: b"f".to_vec(),
                    reason: "changed",
                },
                RemoteKind::Unstable,
            ),
            (
                Error::io("open", io::Error::from(io::ErrorKind::NotFound)),
                RemoteKind::NotFound,
            ),
            (
                Error::io("open", io::Error::from(io::ErrorKind::NotADirectory)),
                RemoteKind::NotFound,
            ),
            (
                Error::io("open", io::Error::from(io::ErrorKind::PermissionDenied)),
                RemoteKind::Other,
            ),
            (
                Error::InvalidOp {
                    path: b"f".to_vec(),
                    reason: "no content",
                },
                RemoteKind::InvalidOp,
            ),
            (
                Error::InvalidPath {
                    path: b"/".to_vec(),
                    reason: "absolute",
                },
                RemoteKind::InvalidPath,
            ),
            (
                Error::BadIndex {
                    reason: "schema".into(),
                },
                RemoteKind::Index,
            ),
            (
                Error::Protocol {
                    reason: "bad".into(),
                },
                RemoteKind::Protocol,
            ),
        ];
        for (e, kind) in cases {
            let wire = WireError::from(&e);
            assert_eq!(wire.kind, kind, "{e:?}");
            assert_eq!(wire.message, e.to_string());
            let back = Error::from(through_pipe(vec![wire.clone()]).remove(0));
            assert_eq!(back.is_unstable(), e.is_unstable(), "{e:?}");
            assert_eq!(back.is_not_found(), e.is_not_found(), "{e:?}");
            assert_eq!(back.remote_kind(), kind);
            // Passed on again, the message is not prefixed twice.
            assert_eq!(WireError::from(&back), wire);
        }
    }
}
