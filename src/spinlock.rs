use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Release};

/// Test-and-Test-and-Set (TaTaS) spinlock.
pub struct Spinlock<T: ?Sized> {
    locked: AtomicBool,
    value: UnsafeCell<T>,
}

/// An RAII implementation of a “scoped lock” of a spinlock.
/// When this structure is dropped, the spinlock will be unlocked.
///
/// The data protected by the spinlock can be accessed through this guard via its [Deref] and
/// [DerefMut] implementations.
pub struct SpinlockGuard<'a, T: ?Sized> {
    spinlock: &'a Spinlock<T>,
}

impl<T> Spinlock<T> {
    /// Creates a new unlocked spinlock.
    pub const fn new(value: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }
}

impl<T: ?Sized> Spinlock<T> {
    /// Acquires a lock.
    pub fn lock(&self) -> SpinlockGuard<'_, T> {
        // We need Acquire to establish happens-before from unlock() to lock(),
        // so our lock can be used to protect access to some shared data.
        while self.locked.swap(true, Acquire) {
            // Read-only: reduce cache-line bouncing.
            while self.locked.load(Acquire) {
                std::hint::spin_loop();
            }
        }
        SpinlockGuard { spinlock: self }
    }
}

impl<T: ?Sized> Drop for SpinlockGuard<'_, T> {
    fn drop(&mut self) {
        // We need Release to establish happens-before from unlock() to lock(),
        // so our lock can be used to protect access to some shared data.
        self.spinlock.locked.store(false, Release);
    }
}

impl<T: ?Sized> Deref for SpinlockGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // Safety: Existence of this Guard guarantees
        // we've exclusively locked the lock.
        unsafe { &*self.spinlock.value.get() }
    }
}

impl<T: ?Sized> DerefMut for SpinlockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Safety: Existence of this Guard guarantees
        // we've exclusively locked the lock.
        unsafe { &mut *self.spinlock.value.get() }
    }
}

unsafe impl<T: ?Sized + Send> Sync for Spinlock<T> {}
unsafe impl<T: ?Sized + Sync> Sync for SpinlockGuard<'_, T> {}
