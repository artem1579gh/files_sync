//! `files_sync`: a race-free, two-way file synchronizer for Linux with rsync
//! symlink semantics. See `claude/design.md` for the design.

pub mod cli;
pub mod config;
pub mod daemon;
pub mod engine;
pub mod error;
pub mod fs;
pub mod index;
pub mod replica;
pub mod sandbox;
pub mod scan;
pub mod server;
pub mod status;
pub mod symlink;
pub mod tls;
pub mod watch;

pub use error::{Error, RemoteKind, Result};
