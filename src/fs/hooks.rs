//! Deterministic race-injection points (design §9).
//!
//! Commit code calls [`point`] at every step boundary. In normal builds it
//! compiles to nothing. Under `cfg(test)` or the `hooks` feature, tests can
//! register closures with [`on`] that run when a point is reached, and so
//! mutate the filesystem at exactly that moment.
//!
//! Hooks are **per thread**: a closure only fires on the thread that
//! registered it, which keeps parallel tests from seeing each other's hooks.
//! The commit functions run on the caller's thread, so this is enough for unit,
//! attack (T07) and crash (T15) tests.

#[cfg(any(test, feature = "hooks"))]
pub use imp::*;

/// Marks a step boundary. Runs every closure registered for `name` on this
/// thread, in registration order.
#[cfg(not(any(test, feature = "hooks")))]
#[inline(always)]
pub fn point(_name: &'static str) {}

#[cfg(any(test, feature = "hooks"))]
mod imp {
    use std::cell::RefCell;

    type Hook = Box<dyn FnMut()>;

    #[derive(Default)]
    struct Registry {
        next_id: u64,
        /// `None` while the hook is running (taken out so it may re-enter).
        hooks: Vec<(u64, &'static str, Option<Hook>)>,
        trace: Option<Vec<&'static str>>,
    }

    thread_local! {
        static REGISTRY: RefCell<Registry> = RefCell::new(Registry::default());
    }

    /// Marks a step boundary. Runs every closure registered for `name` on
    /// this thread, in registration order.
    pub fn point(name: &'static str) {
        let ids: Vec<u64> = REGISTRY.with_borrow_mut(|r| {
            if let Some(trace) = &mut r.trace {
                trace.push(name);
            }
            r.hooks
                .iter()
                .filter(|(_, n, _)| *n == name)
                .map(|(id, _, _)| *id)
                .collect()
        });
        for id in ids {
            // Take the closure out so it can register hooks or reach points
            // itself without a double borrow.
            let hook = REGISTRY.with_borrow_mut(|r| {
                r.hooks
                    .iter_mut()
                    .find(|(i, _, _)| *i == id)
                    .and_then(|(_, _, h)| h.take())
            });
            if let Some(mut hook) = hook {
                hook();
                REGISTRY.with_borrow_mut(|r| {
                    if let Some((_, _, slot)) = r.hooks.iter_mut().find(|(i, _, _)| *i == id) {
                        *slot = Some(hook);
                    }
                });
            }
        }
    }

    /// Unregisters its hook when dropped.
    #[must_use = "the hook is removed when this guard is dropped"]
    pub struct Guard(u64);

    impl Drop for Guard {
        fn drop(&mut self) {
            let id = self.0;
            // Ignore a destroyed thread-local (guard dropped during thread exit).
            let _ = REGISTRY.try_with(|r| r.borrow_mut().hooks.retain(|(i, _, _)| *i != id));
        }
    }

    /// Runs `f` every time this thread reaches `name`, until the returned
    /// guard is dropped.
    pub fn on(name: &'static str, f: impl FnMut() + 'static) -> Guard {
        REGISTRY.with_borrow_mut(|r| {
            r.next_id += 1;
            r.hooks.push((r.next_id, name, Some(Box::new(f))));
            Guard(r.next_id)
        })
    }

    /// Runs `f` the first time this thread reaches `name`.
    pub fn once(name: &'static str, f: impl FnOnce() + 'static) -> Guard {
        let mut f = Some(f);
        on(name, move || {
            if let Some(f) = f.take() {
                f()
            }
        })
    }

    /// Starts recording every point this thread reaches (clearing any earlier
    /// recording).
    pub fn start_trace() {
        REGISTRY.with_borrow_mut(|r| r.trace = Some(Vec::new()));
    }

    /// Stops recording and returns the points reached since [`start_trace`],
    /// in order.
    pub fn take_trace() -> Vec<&'static str> {
        REGISTRY.with_borrow_mut(|r| r.trace.take().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    #[test]
    fn hooks_fire_per_point_until_dropped() {
        let hits = Rc::new(Cell::new(0));
        let h = hits.clone();
        let guard = on("t.a", move || h.set(h.get() + 1));
        point("t.a");
        point("t.b");
        point("t.a");
        assert_eq!(hits.get(), 2);
        drop(guard);
        point("t.a");
        assert_eq!(hits.get(), 2);
    }

    #[test]
    fn once_fires_once_and_hooks_may_reenter() {
        let hits = Rc::new(Cell::new(0));
        let h = hits.clone();
        let _g = once("t.outer", move || {
            h.set(h.get() + 1);
            point("t.outer");
            point("t.inner");
        });
        let h = hits.clone();
        let _g2 = on("t.inner", move || h.set(h.get() + 10));
        start_trace();
        point("t.outer");
        point("t.outer");
        assert_eq!(hits.get(), 11);
        assert_eq!(take_trace(), ["t.outer", "t.outer", "t.inner", "t.outer"]);
        assert!(take_trace().is_empty());
    }

    #[test]
    fn hooks_are_per_thread() {
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let h = hits.clone();
        let _g = on("t.thread", move || {
            h.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        std::thread::spawn(|| point("t.thread")).join().unwrap();
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
        point("t.thread");
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
