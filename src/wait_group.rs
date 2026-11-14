use std::sync::atomic::{AtomicU32, Ordering};

use crate::futex;

/// Waits for a group of tasks to finish.
pub struct WaitGroup {
    counter: AtomicU32,
}

impl WaitGroup {
    /// Creates a new [`WaitGroup`] with a zero task counter.
    pub const fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
        }
    }

    /// Adds `delta` to the task counter.
    ///
    /// When the [`WaitGroup`] is reused, an [`add`](WaitGroup::add) that starts a new round must
    /// happen after every [`wait`](WaitGroup::wait) of the previous round has returned.
    /// Otherwise those waiters may miss the counter reaching zero and keep blocking.
    ///
    /// # Panics
    ///
    /// Panics if the counter overflows.
    pub fn add(&self, delta: u32) {
        let mut counter = self.counter.load(Ordering::Relaxed);
        loop {
            let new = counter
                .checked_add(delta)
                .expect("WaitGroup counter overflowed");
            match self.counter.compare_exchange_weak(
                counter,
                new,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(old) => counter = old,
            }
        }
    }

    /// Decrements the task counter by one.
    ///
    /// If the counter reaches zero, all threads blocked in [`wait`](WaitGroup::wait) are released.
    ///
    /// # Panics
    ///
    /// Panics if the counter is already zero.
    pub fn done(&self) {
        let mut counter = self.counter.load(Ordering::Relaxed);
        let v = loop {
            assert!(counter > 0, "WaitGroup counter is already zero");
            match self.counter.compare_exchange_weak(
                counter,
                counter - 1,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(v) => break v,
                Err(old) => counter = old,
            }
        };
        if v == 1 {
            futex::wake_all(&self.counter);
        }
    }

    /// Blocks until the task counter is zero.
    pub fn wait(&self) {
        loop {
            let counter = self.counter.load(Ordering::Acquire);
            if counter == 0 {
                return;
            }
            futex::wait(&self.counter, counter, None);
        }
    }
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
}
