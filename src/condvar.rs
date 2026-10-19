use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU32, AtomicUsize};
use std::time::Duration;

use crate::futex;
use crate::mutex::MutexGuard;

/// A Condition Variable.
pub struct Condvar {
    state: AtomicU32,
    num_waiters: AtomicUsize,
}

/// A type indicating whether a condition variable wait timed out.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct WaitTimeoutResult(bool);

impl Condvar {
    /// Creates a new condition variable which is ready to be waited on and notified.
    pub const fn new() -> Condvar {
        Self {
            state: AtomicU32::new(0),
            num_waiters: AtomicUsize::new(0),
        }
    }

    /// Blocks the current thread until this condition variable receives a notification.
    ///
    /// This function will atomically unlock the lock specified and block the current thread.
    /// When this function call returns, the lock specified will have been re-acquired.
    ///
    /// This function is susceptible to spurious wakeups.
    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        self.num_waiters.fetch_add(1, Relaxed);

        let state = self.state.load(Relaxed);

        let mutex = guard.mutex;
        drop(guard);

        futex::wait(&self.state, state, None);

        self.num_waiters.fetch_sub(1, Relaxed);

        mutex.lock()
    }

    /// Waits on this condition variable for a notification, timing out after a specified duration.
    pub fn wait_timeout<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        duration: Duration,
    ) -> (MutexGuard<'a, T>, WaitTimeoutResult) {
        self.num_waiters.fetch_add(1, Relaxed);

        let state = self.state.load(Relaxed);

        let mutex = guard.mutex;
        drop(guard);

        let timed_out = !futex::wait(&self.state, state, Some(duration));

        self.num_waiters.fetch_sub(1, Relaxed);

        (mutex.lock(), WaitTimeoutResult(timed_out))
    }

    /// Wakes up one blocked thread on this condvar.
    pub fn notify_one(&self) {
        if self.num_waiters.load(Relaxed) > 0 {
            self.state.fetch_add(1, Relaxed);
            futex::wake_one(&self.state);
        }
    }

    /// Wakes up all blocked threads on this condvar.
    pub fn notify_all(&self) {
        if self.num_waiters.load(Relaxed) > 0 {
            self.state.fetch_add(1, Relaxed);
            futex::wake_all(&self.state);
        }
    }
}

impl Default for Condvar {
    fn default() -> Self {
        Self::new()
    }
}
