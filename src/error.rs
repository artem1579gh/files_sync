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

    /// The index database failed (I/O, corruption detected by redb, or a
    /// transaction error).
    #[error("index database: {0}")]
    Db(#[from] redb::Error),

    /// The index database opened but its contents are unusable: a schema or
    /// replica mismatch, or a record that does not decode.
    #[error("bad index: {reason}")]
    BadIndex { reason: String },

    /// Neither `$XDG_STATE_HOME` nor `$HOME` gives a usable state directory.
    #[error("cannot determine state directory: set XDG_STATE_HOME or HOME to an absolute path")]
    NoStateHome,
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

/// Library result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;
