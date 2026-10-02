//! Race-safe filesystem primitives: `Root`, fingerprints, CAS commits, temp names,
//! leases, capability probes and race-injection hooks (design §2, §5).
//!
//! `commit.rs` (T04) will be the only code allowed to mutate a replica.
