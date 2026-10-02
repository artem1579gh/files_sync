//! Race-safe filesystem primitives: `Root`, fingerprints, CAS commits, temp names,
//! leases, capability probes and race-injection hooks (design §2, §5).
//!
//! `commit.rs` is the only code allowed to mutate a replica (plus `caps.rs`,
//! which creates and removes its own probe files).
pub mod caps;
pub mod commit;
pub mod hooks;
pub mod root;
pub mod stat;
pub mod tmpname;

pub use root::{DirEntry, RelPath, Root};
pub use stat::{Discard, FileKind, Fingerprint, Sink, stable_read, stable_read_with};
pub use tmpname::{conflict_name, is_reserved};
