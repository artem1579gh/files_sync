//! Command-line interface. This is the binary's front end, so it reports
//! errors with `anyhow` rather than the library [`Error`](crate::Error).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::config::{self, PairConfig};
use crate::daemon::{self, Daemon};
use crate::engine::{Engine, Side, SyncReport};
use crate::fs::caps::Caps;
use crate::fs::commit::Quarantine;
use crate::replica::LocalReplica;

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
    /// Keep a pair in sync continuously, driven by inotify; stops cleanly on
    /// SIGINT or SIGTERM.
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
            // Missing or non-directory roots are reported by `config::init`.
            for dir in [&a, &b] {
                if dir.is_dir() {
                    probe_root(dir)?;
                }
            }
            let state_home = config::state_home()?;
            let (cfg, path) = config::init(&state_home, &pair, &a, &b)?;
            println!("initialised pair {:?}: {}", cfg.name, path.display());
            for (side, r) in ["a", "b"].iter().zip(&cfg.replicas) {
                println!("  {side}: {} (replica {})", r.root.display(), r.id);
            }
            Ok(())
        }
        Command::Sync { once: true, pair } => {
            let (cfg, [mut a, mut b]) = open_pair(&pair)?;
            let report = Engine::new().sync_once(&mut a, &mut b)?;
            drain_quarantine(&mut [&mut a, &mut b]);
            print_report(&cfg, &report);
            if !report.is_converged() {
                bail!(
                    "{} path(s) not synced; see above",
                    report.errors.len() + report.unresolved.len()
                );
            }
            Ok(())
        }
        Command::Sync { once: false, .. } => {
            bail!("`sync` requires --once; use `daemon` for continuous sync")
        }
        Command::Daemon { pair } => {
            // Before any thread starts, so every thread inherits the mask.
            let stop = daemon::shutdown_signals()?;
            let (cfg, [mut a, mut b]) = open_pair(&pair)?;
            tracing::info!(pair = %cfg.name, "daemon started");
            let stats = Daemon::new().run(&mut a, &mut b, &stop)?;
            drain_quarantine(&mut [&mut a, &mut b]);
            println!(
                "daemon for {:?} stopped: {} cycle(s), {} change(s) applied, {} conflict(s)",
                cfg.name, stats.cycles, stats.applied, stats.conflicts
            );
            Ok(())
        }
        Command::Status { .. } => not_implemented("status"),
    }
}

/// Loads a pair's config and opens both replicas (which probes, logs and
/// checks their roots' capabilities).
fn open_pair(pair: &str) -> anyhow::Result<(PairConfig, [LocalReplica; 2])> {
    let state_home = config::state_home()?;
    let cfg = PairConfig::load(&state_home, pair)?;
    let pair_dir = config::pair_dir(&state_home, pair)?;
    let open = |r: &config::ReplicaConfig| {
        LocalReplica::open(r, &pair_dir)
            .with_context(|| format!("replica root {}", r.root.display()))
    };
    let replicas = [open(&cfg.replicas[0])?, open(&cfg.replicas[1])?];
    Ok((cfg, replicas))
}

/// Waits until the replaced old inodes in quarantine can be unlinked
/// (§5.3 step 4(f)), so a one-shot sync leaves no `.~fsync.old.*` files
/// behind. Gives up, with a warning, after a few grace periods.
fn drain_quarantine(replicas: &mut [&mut LocalReplica]) {
    let give_up = Instant::now() + 10 * Quarantine::DEFAULT_GRACE;
    loop {
        for r in replicas.iter_mut() {
            r.sweep_quarantine();
        }
        let next = replicas
            .iter()
            .filter_map(|r| r.quarantine().next_deadline())
            .min();
        let Some(next) = next else { return };
        let now = Instant::now();
        if now >= give_up {
            let left: usize = replicas.iter().map(|r| r.quarantine().len()).sum();
            tracing::warn!(
                left,
                "quarantined files left behind; they are swept by the next run"
            );
            return;
        }
        std::thread::sleep(
            next.saturating_duration_since(now)
                .max(Duration::from_millis(10)),
        );
    }
}

fn print_report(cfg: &PairConfig, report: &SyncReport) {
    let root = |side: Side| match side {
        Side::A => &cfg.replicas[0].root,
        Side::B => &cfg.replicas[1].root,
    };
    println!(
        "synced {:?}: {} change(s) applied in {} round(s)",
        cfg.name, report.applied, report.rounds
    );
    for c in &report.conflicts {
        println!(
            "  conflict at {}: the version in {} is kept as {}",
            c.path,
            root(c.side).display(),
            c.copy
        );
    }
    for p in &report.resurrected {
        println!("  kept deleted directory {p}: it holds unseen changes");
    }
    for p in &report.unmanaged {
        println!("  not synced (unmanaged): {p}");
    }
    for (p, e) in &report.errors {
        println!("  error: {p}: {e}");
    }
    for p in &report.unresolved {
        println!("  not settled (changed during the sync), retry: {p}");
    }
}

/// Probes a replica root, logs the result and checks the required features.
fn probe_root(root: &Path) -> anyhow::Result<Caps> {
    let caps = Caps::probe_path(root)?;
    caps.log(root);
    caps.require_minimum()
        .with_context(|| format!("replica root {}", root.display()))?;
    Ok(caps)
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
