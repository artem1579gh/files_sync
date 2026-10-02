//! Race-safe filesystem primitives: `Root`, fingerprints, CAS commits, temp names,
//! leases, capability probes and race-injection hooks (design §2, §5).
//!
//! `commit.rs` (T04) will be the only code allowed to mutate a replica.
pub mod caps;
pub mod root;
pub mod stat;

pub use root::{DirEntry, RelPath, Root};
pub use stat::{FileKind, Fingerprint, Sink, stable_read};
