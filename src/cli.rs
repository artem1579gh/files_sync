//! Command-line interface. This is the binary's front end, so it reports
//! errors with `anyhow` rather than the library [`Error`](crate::Error).

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand, ValueEnum};
use crossbeam_channel::select;
use tracing_subscriber::EnvFilter;

use crate::config::{self, NewReplica, PairConfig};
use crate::daemon::{self, Daemon};
use crate::engine::{Engine, Side, SyncReport};
use crate::fs::caps::Caps;
use crate::fs::commit::Quarantine;
use crate::replica::{Housekeeping, PairReplica};
use crate::sandbox;
use crate::server::Server;
use crate::status::PairStatus;
use crate::tls::Identity;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Race-free two-way file synchronizer with rsync symlink semantics"
)]
struct Cli {
    /// With sync, daemon, serve and status: confine the process with
    /// landlock, so it may write only beneath the pair's local replica roots
    /// and its state directory (Linux ≥ 5.13).
    #[arg(long, global = true)]
    sandbox: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create a new sync pair between two directories.
    ///
    /// Also generates a TLS certificate per replica (in the state directory)
    /// and pins both device IDs in the config. For a replica on another
    /// host, give its address with --a-remote/--b-remote, then copy the
    /// config and that replica's .crt and .key to the same state directory
    /// there and run `serve` for it.
    Init {
        /// Name of the pair; its state lives in $XDG_STATE_HOME/fsync/<PAIR>/.
        pair: String,
        /// Root directory of replica A (on its server's host if remote).
        #[arg(long = "a", value_name = "DIR")]
        a: PathBuf,
        /// Root directory of replica B (on its server's host if remote).
        #[arg(long = "b", value_name = "DIR")]
        b: PathBuf,
        /// Replica A is run by `serve` at this address.
        #[arg(long = "a-remote", value_name = "HOST:PORT")]
        a_remote: Option<String>,
        /// Replica B is run by `serve` at this address.
        #[arg(long = "b-remote", value_name = "HOST:PORT")]
        b_remote: Option<String>,
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
    /// Run one replica of a pair for its peer, over TLS (mutual, pinned
    /// certificates); stops cleanly on SIGINT or SIGTERM.
    Serve {
        pair: String,
        /// The replica to serve.
        side: SideArg,
        /// Address to listen on; defaults to the replica's remote address
        /// from the config. Port 0 picks a free port; the address in use is
        /// printed.
        #[arg(long, value_name = "HOST:PORT")]
        listen: Option<String>,
    },
}

/// A replica of a pair, by its place in the config.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum SideArg {
    A,
    B,
}

impl SideArg {
    fn index(self) -> usize {
        match self {
            SideArg::A => 0,
            SideArg::B => 1,
        }
    }
}

/// Parses the command line and runs the selected subcommand.
pub fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_logging();
    match cli.command {
        Command::Init {
            pair,
            a,
            b,
            a_remote,
            b_remote,
        } => {
            // Missing or non-directory roots are reported by `config::init`.
            for (dir, remote) in [(&a, &a_remote), (&b, &b_remote)] {
                if remote.is_none() && dir.is_dir() {
                    probe_root(dir)?;
                }
            }
            let state_home = config::state_home()?;
            let replicas = [
                NewReplica {
                    root: &a,
                    remote: a_remote.as_deref(),
                },
                NewReplica {
                    root: &b,
                    remote: b_remote.as_deref(),
                },
            ];
            let (cfg, path) = config::init_with(&state_home, &pair, replicas)?;
            println!("initialised pair {:?}: {}", cfg.name, path.display());
            let pair_dir = config::pair_dir(&state_home, &pair)?;
            for (side, r) in ["a", "b"].iter().zip(&cfg.replicas) {
                println!("  {side}: {} (replica {})", r.root.display(), r.id);
                if let Some(device) = r.device {
                    println!("     device {device}");
                }
                if let Some(addr) = &r.remote {
                    let (crt, key) = Identity::paths(&pair_dir, r.id);
                    println!(
                        "     served at {addr}: copy {}, {} and {} to {} on that host, \
                         then run `serve {pair} {side}` there",
                        path.display(),
                        crt.display(),
                        key.display(),
                        pair_dir.display()
                    );
                }
            }
            Ok(())
        }
        Command::Sync { once: true, pair } => {
            let (cfg, pair_dir) = load_pair(&pair)?;
            if cli.sandbox {
                sandbox(
                    &cfg,
                    &pair_dir,
                    cfg.replicas.each_ref().map(|r| !r.is_remote()),
                )?;
            }
            let [mut a, mut b] = open_pair(&cfg, &pair_dir)?;
            let engine = Engine::new().tombstone_retention(cfg.tombstone_retention());
            let report = engine.sync_once(&mut a, &mut b)?;
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
            let (cfg, pair_dir) = load_pair(&pair)?;
            // Before any thread starts, so every thread is confined and
            // inherits the signal mask.
            if cli.sandbox {
                sandbox(
                    &cfg,
                    &pair_dir,
                    cfg.replicas.each_ref().map(|r| !r.is_remote()),
                )?;
            }
            let stop = daemon::shutdown_signals()?;
            let [mut a, mut b] = open_pair(&cfg, &pair_dir)?;
            tracing::info!(pair = %cfg.name, "daemon started");
            let engine = Engine::new().tombstone_retention(cfg.tombstone_retention());
            let stats = Daemon::new()
                .engine(engine)
                .status_dir(pair_dir.clone())
                .run(&mut a, &mut b, &stop)?;
            drain_quarantine(&mut [&mut a, &mut b]);
            println!(
                "daemon for {:?} stopped: {} cycle(s), {} change(s) applied, {} conflict(s)",
                cfg.name, stats.cycles, stats.applied, stats.conflicts
            );
            Ok(())
        }
        Command::Status { pair } => {
            let (cfg, pair_dir) = load_pair(&pair)?;
            if cli.sandbox {
                sandbox(
                    &cfg,
                    &pair_dir,
                    cfg.replicas.each_ref().map(|r| !r.is_remote()),
                )?;
            }
            let st = PairStatus::load(&cfg, &pair_dir)?;
            print_status(&cfg, &st);
            Ok(())
        }
        Command::Serve { pair, side, listen } => {
            let (cfg, pair_dir) = load_pair(&pair)?;
            let i = side.index();
            let r = &cfg.replicas[i];
            let Some(addr) = listen.or_else(|| r.remote.clone()) else {
                bail!(
                    "no address to listen on: pass --listen or set `remote` for replica {} in the config",
                    r.id
                );
            };
            if cli.sandbox {
                sandbox(&cfg, &pair_dir, [i == 0, i == 1])?;
            }
            let stop = daemon::shutdown_signals()?;
            let server = Server::open(&cfg, i, &pair_dir)
                .with_context(|| format!("replica root {}", r.root.display()))?;
            let listener = TcpListener::bind(&addr).with_context(|| format!("listen on {addr}"))?;
            let mut handle = server.spawn(listener)?;
            println!(
                "serving replica {} of {:?} ({}) on {}",
                r.id,
                cfg.name,
                r.root.display(),
                handle.local_addr()
            );
            loop {
                let next = handle.sweep();
                let timeout = next
                    .map(|d| d.saturating_duration_since(Instant::now()))
                    .unwrap_or(Duration::from_secs(1))
                    .clamp(Duration::from_millis(10), Duration::from_secs(1));
                select! {
                    recv(stop) -> _ => break,
                    default(timeout) => {}
                }
            }
            handle.shutdown();
            // Waits for a request still being answered.
            let mut replica = handle.replica();
            drain_quarantine(&mut [&mut *replica]);
            println!("stopped serving {:?}", cfg.name);
            Ok(())
        }
    }
}

/// Loads a pair's config; returns it with the pair's state directory.
fn load_pair(pair: &str) -> anyhow::Result<(PairConfig, PathBuf)> {
    let state_home = config::state_home()?;
    let cfg = PairConfig::load(&state_home, pair)?;
    let pair_dir = config::pair_dir(&state_home, pair)?;
    Ok((cfg, pair_dir))
}

/// Opens both replicas: a local one is probed (its root's capabilities are
/// logged and checked) and its journal replayed; a remote one is connected
/// to.
fn open_pair(cfg: &PairConfig, pair_dir: &Path) -> anyhow::Result<[PairReplica; 2]> {
    let open = |i: usize| {
        let r = &cfg.replicas[i];
        PairReplica::open(cfg, i, pair_dir).with_context(|| match &r.remote {
            Some(addr) => format!("replica {} at {addr}", r.id),
            None => format!("replica root {}", r.root.display()),
        })
    };
    Ok([open(0)?, open(1)?])
}

/// `--sandbox`: from now on, writes only beneath `pair_dir` and the roots of
/// the replicas `which` selects (those this process opens itself).
fn sandbox(cfg: &PairConfig, pair_dir: &Path, which: [bool; 2]) -> anyhow::Result<()> {
    let mut dirs = vec![pair_dir];
    for (r, on) in cfg.replicas.iter().zip(which) {
        if on {
            dirs.push(r.root.as_path());
        }
    }
    let abi = sandbox::restrict(&dirs).context("--sandbox")?;
    tracing::info!(
        abi,
        "landlock sandbox: writes confined to the roots and the state directory"
    );
    Ok(())
}

fn print_status(cfg: &PairConfig, st: &PairStatus) {
    let when = |ns: i64| {
        jiff::Timestamp::from_nanosecond(i128::from(ns))
            .map(|t| {
                t.to_zoned(jiff::tz::TimeZone::system())
                    .strftime("%Y-%m-%d %H:%M:%S %Z")
                    .to_string()
            })
            .unwrap_or_else(|_| format!("{ns} ns"))
    };
    print!("pair {:?}", cfg.name);
    if st.from_daemon {
        print!(" (daemon running; its report from {})", when(st.taken_ns));
    }
    println!();
    for ((side, r), s) in ["a", "b"].iter().zip(&cfg.replicas).zip(&st.replicas) {
        println!("  {side}: {} (replica {})", r.root.display(), r.id);
        if let Some(addr) = &s.remote {
            println!("    served at {addr} (run `status` there)");
            continue;
        }
        println!(
            "    index:       {} entries ({} tombstones)",
            s.entries, s.tombstones
        );
        println!("    conflicts:   {}", s.conflicts.len());
        for c in &s.conflicts {
            println!("      {c}");
        }
        println!("    quarantined: {}", s.quarantined);
        if s.unfinished > 0 {
            println!(
                "    unfinished:  {} (replayed on the next run)",
                s.unfinished
            );
        }
        match s.last_sync_ns {
            Some(ns) => println!("    last sync:   {}", when(ns)),
            None => println!("    last sync:   never"),
        }
    }
}

/// Waits until the replaced old inodes in quarantine can be unlinked
/// (§5.3 step 4(f)), so a one-shot sync leaves no `.~fsync.old.*` files
/// behind. Gives up, with a warning, after a few grace periods.
fn drain_quarantine(replicas: &mut [&mut dyn Housekeeping]) {
    let give_up = Instant::now() + 10 * Quarantine::DEFAULT_GRACE;
    loop {
        for r in replicas.iter_mut() {
            r.sweep();
        }
        let next = replicas.iter().filter_map(|r| r.next_sweep()).min();
        let Some(next) = next else { return };
        let now = Instant::now();
        if now >= give_up {
            tracing::warn!("quarantined files left behind; they are swept by the next run");
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

/// Logs to stderr, filtered by `RUST_LOG` (default `info`).
fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}
