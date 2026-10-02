//! Symlink handling: rsync-compatible safety check, munging and policy
//! classification (design §4).
pub mod munge;
pub mod policy;
pub mod safety;

pub use munge::{is_munged, munge, unmunge};
pub use policy::{SymlinkPolicy, Treatment, classify, needs_referent};
pub use safety::is_unsafe;
