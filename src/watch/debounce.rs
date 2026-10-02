//! Debouncing of watcher events into [`Hint`]s (design §5.9).
//!
//! Dirty paths are collected until no new event has arrived for
//! [`Debouncer::QUIET`], or until [`Debouncer::MAX_DELAY`] has passed since
//! the first event of the batch, whichever comes first. A lost-events signal
//! (overflow), or too many paths, turns the batch into a full rescan.
//!
//! The debouncer is pure: the caller passes the current time in.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::fs::RelPath;
use crate::watch::{Event, Hint};

/// Collects events and says when to flush them as one [`Hint`].
#[derive(Clone, Debug)]
pub struct Debouncer {
    quiet: Duration,
    max_delay: Duration,
    max_paths: usize,
    paths: BTreeSet<RelPath>,
    full: bool,
    /// Arrival of the batch's first and latest event.
    first: Option<Instant>,
    last: Option<Instant>,
}

impl Default for Debouncer {
    fn default() -> Debouncer {
        Debouncer::new(Debouncer::QUIET, Debouncer::MAX_DELAY)
    }
}

impl Debouncer {
    /// Flush after this long without a new event.
    pub const QUIET: Duration = Duration::from_millis(200);
    /// Flush at the latest this long after the batch's first event.
    pub const MAX_DELAY: Duration = Duration::from_secs(2);
    /// More dirty paths than this in one batch become a full rescan.
    pub const MAX_PATHS: usize = 10_000;

    pub fn new(quiet: Duration, max_delay: Duration) -> Debouncer {
        Debouncer {
            quiet,
            max_delay: max_delay.max(quiet),
            max_paths: Debouncer::MAX_PATHS,
            paths: BTreeSet::new(),
            full: false,
            first: None,
            last: None,
        }
    }

    /// Overrides [`Debouncer::MAX_PATHS`].
    pub fn max_paths(mut self, n: usize) -> Debouncer {
        self.max_paths = n;
        self
    }

    /// Adds an event that arrived at `now`.
    pub fn push(&mut self, event: Event, now: Instant) {
        match event {
            Event::Dirty(_) if self.full => {}
            Event::Dirty(path) => {
                self.paths.insert(path);
                if self.paths.len() > self.max_paths {
                    self.full = true;
                    self.paths.clear();
                }
            }
            Event::Overflow => {
                self.full = true;
                self.paths.clear();
            }
        }
        self.first.get_or_insert(now);
        self.last = Some(now);
    }

    /// Nothing is waiting to be flushed.
    pub fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    /// When the pending batch is due, if there is one.
    pub fn deadline(&self) -> Option<Instant> {
        let (first, last) = (self.first?, self.last?);
        Some((last + self.quiet).min(first + self.max_delay))
    }

    /// Takes the pending batch if it is due at `now`.
    pub fn take_due(&mut self, now: Instant) -> Option<Hint> {
        if self.deadline()? > now {
            return None;
        }
        self.take()
    }

    /// Takes the pending batch, due or not.
    pub fn take(&mut self) -> Option<Hint> {
        self.first.take()?;
        self.last = None;
        if std::mem::take(&mut self.full) {
            return Some(Hint::FullRescan);
        }
        Some(Hint::Paths(
            std::mem::take(&mut self.paths).into_iter().collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rp(s: &str) -> RelPath {
        RelPath::new(s).unwrap()
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn flushes_after_quiet_period() {
        let t0 = Instant::now();
        let mut d = Debouncer::default();
        assert!(d.is_empty());
        assert_eq!(d.deadline(), None);
        assert_eq!(d.take_due(t0), None);

        d.push(Event::Dirty(rp("b")), t0);
        d.push(Event::Dirty(rp("a")), t0 + ms(50));
        d.push(Event::Dirty(rp("a")), t0 + ms(100));
        assert_eq!(d.deadline(), Some(t0 + ms(300)));
        assert_eq!(d.take_due(t0 + ms(299)), None);
        assert_eq!(
            d.take_due(t0 + ms(300)),
            Some(Hint::Paths(vec![rp("a"), rp("b")]))
        );
        assert!(d.is_empty());
        assert_eq!(d.take_due(t0 + ms(1000)), None);
    }

    #[test]
    fn flushes_a_steady_stream_after_max_delay() {
        let t0 = Instant::now();
        let mut d = Debouncer::default();
        // An event every 100 ms never leaves 200 ms of quiet.
        let mut t = t0;
        while t < t0 + ms(1950) {
            d.push(Event::Dirty(rp("busy")), t);
            assert_eq!(d.take_due(t), None);
            t += ms(100);
        }
        assert_eq!(d.deadline(), Some(t0 + Debouncer::MAX_DELAY));
        assert_eq!(
            d.take_due(t0 + ms(2000)),
            Some(Hint::Paths(vec![rp("busy")]))
        );
        // A new batch starts its own clock.
        d.push(Event::Dirty(rp("x")), t0 + ms(2050));
        assert_eq!(d.deadline(), Some(t0 + ms(2250)));
    }

    #[test]
    fn overflow_and_too_many_paths_become_a_full_rescan() {
        let t0 = Instant::now();
        let mut d = Debouncer::default();
        d.push(Event::Dirty(rp("a")), t0);
        d.push(Event::Overflow, t0);
        d.push(Event::Dirty(rp("b")), t0);
        assert_eq!(d.take(), Some(Hint::FullRescan));
        assert_eq!(d.take(), None);

        let mut d = Debouncer::default().max_paths(2);
        for p in ["a", "b", "c"] {
            d.push(Event::Dirty(rp(p)), t0);
        }
        assert_eq!(d.take(), Some(Hint::FullRescan));
        d.push(Event::Dirty(rp("a")), t0);
        assert_eq!(d.take(), Some(Hint::Paths(vec![rp("a")])));
    }
}
