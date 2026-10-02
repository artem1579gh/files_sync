//! Continuous sync loop driven by watcher hints and a periodic timer (design §5.9, §6.4).
//!
//! [`Daemon::run`] starts both replicas' watchers, then runs a full sync
//! cycle (watch first, then scan, so nothing changed in between is missed).
//! After that it waits for:
//! - **hints** from either watcher: a cycle scoped to the dirty paths (both
//!   replicas scan them), or a full one after an overflow;
//! - the **rescan timer** ([`Daemon::DEFAULT_RESCAN`]): a full cycle, the
//!   backstop for anything the watchers missed;
//! - a **retry**: paths a cycle left unresolved are synced again after
//!   [`Daemon::RETRY_DELAY`], even if no new event names them;
//! - a **quarantine deadline**: replaced old inodes are swept when their
//!   grace period ends (§5.3 step 4(f));
//! - the **stop** channel (a message, or its sender dropped).
//!
//! Our own writes produce inotify events too. They cause one more cycle
//! scoped to the written paths, whose scan finds the index already up to date
//! (§5.3 step 5), so it applies nothing: no echo.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, select};

use crate::engine::{Engine, SyncReport};
use crate::error::{Error, Result};
use crate::fs::RelPath;
use crate::replica::{LocalReplica, Replica};
use crate::scan::Scope;
use crate::watch::Hint;

/// Runs a pair of replicas continuously.
#[derive(Clone, Debug)]
pub struct Daemon {
    engine: Engine,
    rescan_every: Duration,
    retry_delay: Duration,
    reports: Option<Sender<CycleReport>>,
}

impl Default for Daemon {
    fn default() -> Daemon {
        Daemon {
            engine: Engine::new(),
            rescan_every: Daemon::DEFAULT_RESCAN,
            retry_delay: Daemon::RETRY_DELAY,
            reports: None,
        }
    }
}

/// One sync cycle the daemon ran.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CycleReport {
    /// A full scan (the first cycle, the timer, or a watcher overflow), as
    /// opposed to one scoped to dirty paths.
    pub full: bool,
    /// The dirty paths a scoped cycle scanned (empty for a full one).
    pub paths: Vec<RelPath>,
    pub report: SyncReport,
}

/// Totals over a daemon run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DaemonStats {
    pub cycles: u64,
    pub full_cycles: u64,
    /// Steps applied (the action counter).
    pub applied: u64,
    pub conflicts: u64,
    /// Cycles that left paths unresolved or failed.
    pub unconverged: u64,
}

/// What the next cycle has to scan.
#[derive(Default)]
struct Todo {
    full: bool,
    paths: BTreeSet<RelPath>,
}

impl Todo {
    fn add(&mut self, hint: Hint) {
        match hint {
            Hint::FullRescan => self.full = true,
            Hint::Paths(paths) => self.paths.extend(paths),
        }
    }

    fn is_empty(&self) -> bool {
        !self.full && self.paths.is_empty()
    }
}

impl Daemon {
    /// Period of the backstop full rescan (design §5.9).
    pub const DEFAULT_RESCAN: Duration = Duration::from_secs(10 * 60);
    /// Delay before paths a cycle left unresolved are tried again.
    pub const RETRY_DELAY: Duration = Duration::from_secs(1);

    pub fn new() -> Daemon {
        Daemon::default()
    }

    /// Overrides the engine (e.g. its round limit).
    pub fn engine(mut self, engine: Engine) -> Daemon {
        self.engine = engine;
        self
    }

    /// Overrides [`Daemon::DEFAULT_RESCAN`].
    pub fn rescan_every(mut self, period: Duration) -> Daemon {
        self.rescan_every = period;
        self
    }

    /// Overrides [`Daemon::RETRY_DELAY`].
    pub fn retry_delay(mut self, delay: Duration) -> Daemon {
        self.retry_delay = delay;
        self
    }

    /// Sends a [`CycleReport`] after every cycle (for tests and monitoring).
    pub fn reports(mut self, tx: Sender<CycleReport>) -> Daemon {
        self.reports = Some(tx);
        self
    }

    /// Syncs `a` and `b` until `stop` receives a message or is closed.
    ///
    /// Fails only when a cycle fails as a whole (an index or root failure);
    /// per-path problems are logged and retried. Quarantined files may be
    /// left when it returns: sweep them before exiting.
    pub fn run(
        &self,
        a: &mut LocalReplica,
        b: &mut LocalReplica,
        stop: &Receiver<()>,
    ) -> Result<DaemonStats> {
        let mut hints = [a.watch(), b.watch()].map(|h| h.unwrap_or_else(crossbeam_channel::never));
        let mut stats = DaemonStats::default();
        let mut todo = Todo {
            full: true,
            paths: BTreeSet::new(),
        };
        let mut next_full = Instant::now() + self.rescan_every;
        let mut retry: Option<(Instant, Vec<RelPath>)> = None;
        loop {
            let now = Instant::now();
            if now >= next_full {
                todo.full = true;
            }
            if retry.as_ref().is_some_and(|(at, _)| *at <= now) {
                let (_, paths) = retry.take().expect("checked above");
                todo.paths.extend(paths);
            }
            if !todo.is_empty() {
                let cycle = std::mem::take(&mut todo);
                let report = self.cycle(a, b, cycle, &mut stats)?;
                if report.full {
                    next_full = Instant::now() + self.rescan_every;
                }
                let unresolved = &report.report.unresolved;
                if !unresolved.is_empty() {
                    let at = Instant::now() + self.retry_delay;
                    let mut paths = retry.take().map(|(_, p)| p).unwrap_or_default();
                    paths.extend(unresolved.iter().cloned());
                    retry = Some((at, paths));
                }
                if let Some(tx) = &self.reports {
                    let _ = tx.send(report);
                }
            }
            for r in [&mut *a, &mut *b] {
                if r.quarantine()
                    .next_deadline()
                    .is_some_and(|d| d <= Instant::now())
                {
                    r.sweep_quarantine();
                }
            }

            let wake = [
                Some(next_full),
                retry.as_ref().map(|(at, _)| *at),
                a.quarantine().next_deadline(),
                b.quarantine().next_deadline(),
            ]
            .into_iter()
            .flatten()
            .min()
            .expect("the rescan timer is always set");
            let timeout = wake.saturating_duration_since(Instant::now());
            let (mut closed, mut stopping) = (None, false);
            select! {
                recv(stop) -> _ => stopping = true,
                recv(hints[0]) -> h => match h {
                    Ok(h) => todo.add(h),
                    Err(_) => closed = Some(0),
                },
                recv(hints[1]) -> h => match h {
                    Ok(h) => todo.add(h),
                    Err(_) => closed = Some(1),
                },
                default(timeout) => {}
            }
            if stopping {
                break;
            }
            if let Some(i) = closed {
                tracing::warn!(
                    side = ["A", "B"][i],
                    "watcher stopped; relying on periodic rescans"
                );
                hints[i] = crossbeam_channel::never();
            }
            // Hints that arrived meanwhile join the same cycle.
            for h in &hints {
                for hint in h.try_iter() {
                    todo.add(hint);
                }
            }
        }
        tracing::info!(?stats, "daemon stopped");
        Ok(stats)
    }

    /// Runs one cycle for `todo`.
    fn cycle(
        &self,
        a: &mut LocalReplica,
        b: &mut LocalReplica,
        todo: Todo,
        stats: &mut DaemonStats,
    ) -> Result<CycleReport> {
        let (full, paths) = if todo.full {
            (true, Vec::new())
        } else {
            (false, todo.paths.into_iter().collect::<Vec<_>>())
        };
        let scope = if full {
            Scope::Full
        } else {
            Scope::Paths(paths.clone())
        };
        tracing::debug!(full, paths = paths.len(), "sync cycle");
        let report = self.engine.sync(a, b, scope)?;
        stats.cycles += 1;
        stats.full_cycles += u64::from(full);
        stats.applied += report.applied as u64;
        stats.conflicts += report.conflicts.len() as u64;
        if !report.is_converged() {
            stats.unconverged += 1;
            for (p, e) in &report.errors {
                tracing::warn!(path = %p, error = %e, "not synced");
            }
        }
        Ok(CycleReport {
            full,
            paths,
            report,
        })
    }
}

/// Blocks `SIGINT` and `SIGTERM` in this thread and every thread it starts
/// from now on, and turns them into a message on the returned channel, sent
/// by a dedicated `sigwait` thread. A second signal exits the process at
/// once.
///
/// Call it before any other thread is started, so no thread runs the
/// default handler (which would kill the process mid-commit).
pub fn shutdown_signals() -> Result<Receiver<()>> {
    // SAFETY: plain libc calls on a local, initialized signal set.
    let set = unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGINT);
        libc::sigaddset(&mut set, libc::SIGTERM);
        let rc = libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
        if rc != 0 {
            return Err(Error::io(
                "block SIGINT and SIGTERM",
                std::io::Error::from_raw_os_error(rc),
            ));
        }
        set
    };
    let (tx, rx) = crossbeam_channel::bounded(1);
    std::thread::Builder::new()
        .name("signals".into())
        .spawn(move || {
            let mut first = true;
            loop {
                let mut sig = 0;
                // SAFETY: `set` is a valid signal set; `sig` is a valid out pointer.
                if unsafe { libc::sigwait(&set, &mut sig) } != 0 {
                    continue;
                }
                if first {
                    tracing::info!(signal = sig, "shutting down (signal again to exit at once)");
                    let _ = tx.try_send(());
                    first = false;
                } else {
                    tracing::warn!(signal = sig, "exiting at once");
                    std::process::exit(128 + sig);
                }
            }
        })
        .map_err(|e| Error::io("spawn signal thread", e))?;
    Ok(rx)
}
