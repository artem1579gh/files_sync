//! Library error type.

use std::io;
use std::path::PathBuf;

/// Errors returned by the `files_sync` library.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error, with a short description of what was being attempted.
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: io::Error,
    },

    /// The pair name cannot be used as a state directory name.
    #[error("invalid pair name {name:?}: {reason}")]
    InvalidPairName { name: String, reason: &'static str },

    /// A replica root is missing, not a directory, or overlaps another path it must not.
    #[error("invalid replica root {}: {reason}", path.display())]
    InvalidRoot { path: PathBuf, reason: String },

    /// A replica root lacks its root marker (design §5.1), so it may not be
    /// the replica's directory at all: most likely a disk that is not
    /// mounted, leaving its empty mount point. Nothing is synced, since every
    /// file the index knows would look deleted.
    #[error(
        "replica root {}: {reason}; it may not be the replica's directory \
         (is its disk mounted?), so it is not synced. If it is the right \
         directory, create the marker: touch '{}/{marker}' \
         (see \"Root marker\" in docs/usage.md)",
        path.display(),
        path.display()
    )]
    RootMarkerMissing {
        path: PathBuf,
        marker: String,
        reason: String,
    },

    /// The configuration file parsed but describes an invalid pair.
    #[error("invalid config {}: {reason}", path.display())]
    InvalidConfig { path: PathBuf, reason: String },

    /// `init` would overwrite an existing pair.
    #[error("pair already initialised: {} exists", path.display())]
    ConfigExists { path: PathBuf },

    /// The pair has not been initialised.
    #[error("pair not initialised: {} not found", path.display())]
    ConfigNotFound { path: PathBuf },

    /// The configuration file is not valid TOML or does not match the schema.
    #[error("cannot parse config {}: {source}", path.display())]
    ConfigParse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    /// The configuration could not be serialised (e.g. a root path that is not UTF-8).
    #[error("cannot serialise config: {0}")]
    ConfigSerialize(#[from] toml::ser::Error),

    /// The filesystem under a replica root lacks features race-freedom depends on.
    #[error(
        "filesystem {fs_type} lacks required features: {missing}; \
         replica roots must be on ext4, xfs, btrfs or tmpfs (not WSL /mnt/c)"
    )]
    MissingCapabilities { fs_type: String, missing: String },

    /// A replica-relative path is malformed (absolute, `.`/`..`, empty or
    /// NUL-containing components).
    #[error("invalid relative path \"{}\": {reason}", path.escape_ascii())]
    InvalidPath { path: Vec<u8>, reason: &'static str },

    /// The path or file changed while we were resolving or reading it (a
    /// symlink or mount point on the way, or a concurrent modification). The
    /// caller should mark the path dirty and rescan it later (design §5.1, §5.2).
    #[error("unstable path \"{}\": {reason}", path.escape_ascii())]
    Unstable { path: Vec<u8>, reason: &'static str },

    /// A replica was asked to do something that cannot apply to the path's
    /// indexed state (e.g. `Rmdir` on a file, `WriteFile` without content).
    /// This is a caller bug, not a race: races give `PreconditionFailed`.
    #[error("invalid operation on \"{}\": {reason}", path.escape_ascii())]
    InvalidOp { path: Vec<u8>, reason: &'static str },

    /// The index database failed (I/O, corruption detected by redb, or a
    /// transaction error).
    #[error("index database: {0}")]
    Db(#[from] redb::Error),

    /// The index database opened but its contents are unusable: a schema or
    /// replica mismatch, or a record that does not decode.
    #[error("bad index: {reason}")]
    BadIndex { reason: String },

    /// An error reported by a remote replica (design §7.1). `kind` keeps the
    /// local error's class, so callers react to it as to the local error
    /// (see [`Error::is_unstable`], [`Error::is_not_found`]).
    #[error("remote replica: {message}")]
    Remote { kind: RemoteKind, message: String },

    /// The peer broke the wire protocol: a malformed or unexpected message, a
    /// failed handshake, or a connection closed mid-message. The connection
    /// cannot be used any more.
    #[error("protocol error: {reason}")]
    Protocol { reason: String },

    /// The connection to a remote replica could not be made (refused, TLS
    /// or handshake failure, a peer certificate that is not the pinned one)
    /// or was lost. Nothing about one path: the cycle fails, and a daemon
    /// tries again later (design §7.1).
    #[error("connection to {peer}: {reason}")]
    Connection { peer: String, reason: String },

    /// A TLS identity (certificate and key) cannot be made, read or used.
    #[error("TLS identity: {reason}")]
    Tls { reason: String },

    /// Neither `$XDG_STATE_HOME` nor `$HOME` gives a usable state directory.
    #[error("cannot determine state directory: set XDG_STATE_HOME or HOME to an absolute path")]
    NoStateHome,
}

/// The class of an error sent over the wire ([`Error::Remote`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum RemoteKind {
    /// [`Error::Unstable`]: rescan the path.
    Unstable,
    /// An I/O error for which [`Error::is_not_found`] holds.
    NotFound,
    /// [`Error::InvalidOp`].
    InvalidOp,
    /// [`Error::InvalidPath`].
    InvalidPath,
    /// [`Error::Db`] or [`Error::BadIndex`]: the remote index is unusable.
    Index,
    /// [`Error::Protocol`]: the remote side saw us break the protocol.
    Protocol,
    /// Anything else: a per-path failure.
    Other,
}

impl Error {
    /// Wraps an [`io::Error`] with context.
    pub fn io(context: impl Into<String>, source: io::Error) -> Self {
        Error::Io {
            context: context.into(),
            source,
        }
    }
}

impl Error {
    /// Wraps an [`io::Error`] from a content stream. If it carries a library
    /// error (a [`StableReader`](crate::fs::stat::StableReader) reports
    /// [`Error::Unstable`] that way), that error is returned unwrapped.
    pub fn from_stream(context: impl Into<String>, e: io::Error) -> Self {
        if e.get_ref().is_some_and(|inner| inner.is::<Error>()) {
            let inner = e.into_inner().expect("checked above");
            return *inner.downcast::<Error>().expect("checked above");
        }
        Error::io(context, e)
    }

    /// The path changed under us; rescan it ([`Error::Unstable`]).
    pub fn is_unstable(&self) -> bool {
        matches!(
            self,
            Error::Unstable { .. }
                | Error::Remote {
                    kind: RemoteKind::Unstable,
                    ..
                }
        )
    }

    /// An I/O error meaning the path does not exist (or a parent is not a
    /// directory).
    pub fn is_not_found(&self) -> bool {
        match self {
            Error::Io { source, .. } => matches!(
                source.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ),
            Error::Remote { kind, .. } => *kind == RemoteKind::NotFound,
            _ => false,
        }
    }

    /// The connection to a remote replica failed ([`Error::Connection`]).
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Error::Connection { .. })
    }

    /// The class this error is sent over the wire with.
    pub fn remote_kind(&self) -> RemoteKind {
        match self {
            Error::Unstable { .. } => RemoteKind::Unstable,
            Error::InvalidOp { .. } => RemoteKind::InvalidOp,
            Error::InvalidPath { .. } => RemoteKind::InvalidPath,
            Error::Db(_) | Error::BadIndex { .. } => RemoteKind::Index,
            Error::Protocol { .. } | Error::Connection { .. } => RemoteKind::Protocol,
            Error::Remote { kind, .. } => *kind,
            e if e.is_not_found() => RemoteKind::NotFound,
            _ => RemoteKind::Other,
        }
    }
}

/// Library result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;
