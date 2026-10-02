//! Fd-based tree scanner and content hasher (design §3, §4.5, §5.2).
pub mod hasher;
pub mod scanner;

pub use hasher::Hasher;
pub use scanner::{DEFAULT_RACY_WINDOW, ScanStats, Scanner, Scope};
