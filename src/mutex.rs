use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use crate::futex;

/// A mutual exclusion lock.
pub struct Mutex<T: ?Sized> {
    /// - 0: unlocked
    /// - 1: locked, no other threads waiting
    /// - 2: locked, other threads waiting
    state: AtomicU32,
    value: UnsafeCell<T>,
}

/// An RAII implementation of a "scoped lock" of a mutex.
/// When this structure is dropped, the mutex will be unlocked.
///
/// The data protected by the mutex can be accessed through this guard via its [Deref] and
/// [DerefMut] implementations.
pub struct MutexGuard<'a, T: ?Sized> {
    pub(crate) mutex: &'a Mutex<T>,
}

impl<T> Mutex<T> {
    /// Creates a new unlocked mutex.
    pub const fn new(value: T) -> Self {
        Self {
            state: AtomicU32::new(0),
            value: UnsafeCell::new(value),
        }
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Acquires a lock.
    pub fn lock(&self) -> MutexGuard<'_, T> {
        // We need Acquire to establish happens-before from unlock() to lock(),
        // so our lock can be used to protect access to some shared data.
        if self.state.compare_exchange(0, 1, Acquire, Relaxed).is_err() {
            self.lock_slow()
        }
        MutexGuard { mutex: self }
    }

    #[cold]
    fn lock_slow(&self) {
        let mut spin_count = 0;
        // Only spin while the mutex is locked and no waiters exist, since waiters suggest spinning
        // is ineffective.
        while self.state.load(Relaxed) == 1 && spin_count < 100 {
            spin_count += 1;
            std::hint::spin_loop();
        }
        if self.state.compare_exchange(0, 1, Acquire, Relaxed).is_ok() {
            return;
        }

        while self.state.swap(2, Acquire) != 0 {
            futex::wait(&self.state, 2, None);
        }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        // We need Release to establish happens-before from unlock() to lock(),
        // so our lock can be used to protect access to some shared data.
        if self.mutex.state.swap(0, Release) == 2 {
            futex::wake_one(&self.mutex.state);
        }
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // Safety: Existence of this Guard guarantees
        // we've exclusively locked the lock.
        unsafe { &*self.mutex.value.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Safety: Existence of this Guard guarantees
        // we've exclusively locked the lock.
        unsafe { &mut *self.mutex.value.get() }
    }
}

unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}
unsafe impl<T: ?Sized + Sync> Sync for MutexGuard<'_, T> {}
