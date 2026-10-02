//! inotify watcher and event debouncing for daemon mode (design §5.9).
//!
//! An [`EventSource`] yields raw change [`Event`]s: [`InotifySource`] in
//! production, [`ChannelSource`] when a test injects them. A [`Watcher`]
//! thread feeds them through a [`Debouncer`] and sends the resulting
//! [`Hint`]s to the sync loop.
//!
//! Hints are only hints: the scanner decides what changed. A missed event
//! delays a change until the next event nearby or the periodic full rescan;
//! a spurious one costs a scan that finds nothing.
pub mod debounce;
pub mod inotify;

pub use debounce::Debouncer;
pub use inotify::InotifySource;

use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};

use crate::error::{Error, Result};
use crate::fs::RelPath;

/// What a replica's watcher tells the sync loop (design §5.9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Hint {
    /// These paths (and everything beneath them) may have changed: rescan them.
    Paths(Vec<RelPath>),
    /// Events were lost (inotify overflow): rescan everything.
    FullRescan,
}

/// One change seen by an [`EventSource`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The object at this path, or something beneath it, may have changed.
    Dirty(RelPath),
    /// Events were lost; anything may have changed.
    Overflow,
}

/// A stream of change events for one replica (design §5.9: a trait, so tests
/// can inject synthetic events, overflow included).
pub trait EventSource: Send {
    /// Waits up to `timeout` for events and returns those available (none
    /// on timeout). Errors are logged by the caller and treated as lost
    /// events.
    fn wait(&mut self, timeout: Duration) -> Result<Vec<Event>>;

    /// The replica's followed links now are `links` (design §4.5): changes
    /// to what they point to are reported under the links' paths. Replaces
    /// the previous set. Ignored by sources that cannot watch.
    fn follow(&mut self, links: Vec<Followed>) {
        let _ = links;
    }
}

/// A followed (or adopted) symlink, with an open fd on what it points to.
#[derive(Debug)]
pub struct Followed {
    /// The link's path.
    pub path: RelPath,
    /// The referent, opened through the link (`O_PATH` is enough).
    pub fd: OwnedFd,
    /// The referent is a directory (watched with everything beneath it), not
    /// a file.
    pub dir: bool,
}

/// An [`EventSource`] fed through a channel: events are whatever the sender
/// sends. Once the sender is gone, it yields nothing.
pub struct ChannelSource(Receiver<Event>);

impl ChannelSource {
    /// A source and the sender that feeds it.
    pub fn new() -> (Sender<Event>, ChannelSource) {
        let (tx, rx) = crossbeam_channel::unbounded();
        (tx, ChannelSource(rx))
    }
}

impl EventSource for ChannelSource {
    fn wait(&mut self, timeout: Duration) -> Result<Vec<Event>> {
        match self.0.recv_timeout(timeout) {
            Ok(first) => Ok(std::iter::once(first).chain(self.0.try_iter()).collect()),
            Err(RecvTimeoutError::Timeout) => Ok(Vec::new()),
            Err(RecvTimeoutError::Disconnected) => {
                std::thread::sleep(timeout);
                Ok(Vec::new())
            }
        }
    }
}

/// A thread that reads an [`EventSource`], debounces its events and sends
/// [`Hint`]s. Stopped and joined on drop.
pub struct Watcher {
    hints: Receiver<Hint>,
    follows: Sender<Vec<Followed>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Watcher {
    /// How long the thread waits for events before it checks for a stop.
    const TICK: Duration = Duration::from_millis(100);

    /// Starts a thread watching `source` (whose initial watches must already
    /// be in place: changes from then on are reported).
    pub fn spawn(
        name: &str,
        mut source: Box<dyn EventSource>,
        mut debouncer: Debouncer,
    ) -> Result<Watcher> {
        let (tx, hints) = crossbeam_channel::unbounded();
        let (follows, new_follows) = crossbeam_channel::unbounded::<Vec<Followed>>();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name(format!("watch-{name}"))
            .spawn(move || {
                while !stopped.load(Ordering::Relaxed) {
                    if let Some(links) = new_follows.try_iter().last() {
                        source.follow(links);
                    }
                    let now = Instant::now();
                    let timeout = debouncer
                        .deadline()
                        .map_or(Self::TICK, |d| d.saturating_duration_since(now))
                        .min(Self::TICK);
                    match source.wait(timeout) {
                        Ok(events) => {
                            let now = Instant::now();
                            for e in events {
                                debouncer.push(e, now);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "watcher failed; rescanning everything");
                            debouncer.push(Event::Overflow, Instant::now());
                            std::thread::sleep(Self::TICK);
                        }
                    }
                    if let Some(hint) = debouncer.take_due(Instant::now()) {
                        tracing::trace!(?hint, "watch hint");
                        if tx.send(hint).is_err() {
                            break;
                        }
                    }
                }
            })
            .map_err(|e| Error::io("spawn watcher thread", e))?;
        Ok(Watcher {
            hints,
            follows,
            stop,
            thread: Some(thread),
        })
    }

    /// The hints, for as long as the watcher runs.
    pub fn hints(&self) -> Receiver<Hint> {
        self.hints.clone()
    }

    /// Hands the replica's current followed links to the source
    /// ([`EventSource::follow`]), before its next wait.
    pub fn follow(&self, links: Vec<Followed>) {
        // Only fails once the thread is gone.
        let _ = self.follows.send(links);
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watcher_debounces_injected_events() {
        let (tx, source) = ChannelSource::new();
        let w = Watcher::spawn("test", Box::new(source), Debouncer::default()).unwrap();
        let hints = w.hints();
        let t0 = Instant::now();
        tx.send(Event::Dirty(RelPath::new("a").unwrap())).unwrap();
        tx.send(Event::Dirty(RelPath::new("b").unwrap())).unwrap();
        let h = hints.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(
            h,
            Hint::Paths(vec![RelPath::new("a").unwrap(), RelPath::new("b").unwrap()])
        );
        assert!(t0.elapsed() >= Debouncer::QUIET);
        tx.send(Event::Overflow).unwrap();
        assert_eq!(
            hints.recv_timeout(Duration::from_secs(2)).unwrap(),
            Hint::FullRescan
        );
        drop(w);
        // The thread is gone: the channel is closed.
        assert!(hints.recv().is_err());
    }
}
