//! Command-line interface. This is the binary's front end, so it reports
//! errors with `anyhow` rather than the library [`Error`](crate::Error).

use std::path::PathBuf;

use anyhow::bail;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::config;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Race-free two-way file synchronizer with rsync symlink semantics"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create a new sync pair between two directories.
    Init {
        /// Name of the pair; its state lives in $XDG_STATE_HOME/fsync/<PAIR>/.
        pair: String,
        /// Root directory of replica A.
        #[arg(long = "a", value_name = "DIR")]
        a: PathBuf,
        /// Root directory of replica B.
        #[arg(long = "b", value_name = "DIR")]
        b: PathBuf,
    },
    /// Synchronise a pair.
    Sync {
        /// Run a single sync pass and exit (use `daemon` for continuous sync).
        #[arg(long)]
        once: bool,
        pair: String,
    },
    /// Keep a pair in sync continuously, driven by inotify.
    Daemon { pair: String },
    /// Show the state of a pair.
    Status { pair: String },
}

/// Parses the command line and runs the selected subcommand.
pub fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_logging();
    match cli.command {
        Command::Init { pair, a, b } => {
            let state_home = config::state_home()?;
            let (cfg, path) = config::init(&state_home, &pair, &a, &b)?;
            println!("initialised pair {:?}: {}", cfg.name, path.display());
            for (side, r) in ["a", "b"].iter().zip(&cfg.replicas) {
                println!("  {side}: {} (replica {})", r.root.display(), r.id);
            }
            Ok(())
        }
        Command::Sync { once: true, .. } => not_implemented("sync --once"),
        Command::Sync { once: false, .. } => {
            bail!("`sync` requires --once; use `daemon` for continuous sync")
        }
        Command::Daemon { .. } => not_implemented("daemon"),
        Command::Status { .. } => not_implemented("status"),
    }
}

fn not_implemented(what: &str) -> anyhow::Result<()> {
    bail!("`{what}`: not implemented")
}

/// Logs to stderr, filtered by `RUST_LOG` (default `info`).
fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}
