use std::ptr;
use std::sync::{Mutex, MutexGuard, PoisonError};

/// A concurrent set backed by a linked list with fine-grained lock-coupling.
#[derive(Debug)]
pub struct Set<T> {
    head: Mutex<*mut Node<T>>,
}

#[derive(Debug)]
struct Node<T> {
    value: T,
    next: Mutex<*mut Node<T>>,
}

impl<T> Node<T> {
    fn new(value: T, next: *mut Self) -> *mut Self {
        Box::into_raw(Box::new(Self {
            value,
            next: Mutex::new(next),
        }))
    }
}

impl<T> Set<T> {
    /// Creates a new empty set.
    pub fn new() -> Self {
        Self {
            head: Mutex::new(ptr::null_mut()),
        }
    }
}

impl<T: Ord> Set<T> {
    /// Returns `true` iff the set contains the value.
    pub fn contains(&self, value: &T) -> bool {
        let prev_next = self.find(value);
        let curr = *prev_next;
        // Safety: We hold the lock on the slot pointing to `curr`, which keeps the node alive.
        !curr.is_null() && unsafe { (*curr).value.eq(value) }
    }

    /// Adds the value to the set.
    /// Returns whether the value was newly inserted.
    pub fn insert(&self, value: T) -> bool {
        let mut prev_next = self.find(&value);
        let curr = *prev_next;
        // Safety: We hold the lock on the slot pointing to `curr`, which keeps the node alive.
        if !curr.is_null() && unsafe { (*curr).value.eq(&value) } {
            return false;
        }
        *prev_next = Node::new(value, curr);
        true
    }

    /// Removes the value from the set.
    /// Returns whether the value was present in the set.
    pub fn remove(&self, value: &T) -> bool {
        let mut prev_next = self.find(value);
        let curr = *prev_next;
        // Safety: We hold the lock on the slot pointing to `curr`, which keeps the node alive.
        if curr.is_null() || unsafe { (*curr).value.ne(value) } {
            return false;
        }
        // Safety: We hold the lock on the slot pointing to `curr`, which keeps the node alive.
        let curr_next = unsafe { *(*curr).next.lock().unwrap() };
        *prev_next = curr_next;
        // Safety: The node is now unreachable. The slot we hold locked was the only one pointing
        // to it, and no thread can hold or wait on its lock without going through that slot first.
        drop(unsafe { Box::from_raw(curr) });
        true
    }

    /// Locks and returns the `next` pointer of the node preceding the first node whose value is
    /// not less than the key.
    fn find(&self, key: &T) -> MutexGuard<'_, *mut Node<T>> {
        let mut prev_next = self.head.lock().unwrap();
        loop {
            if (*prev_next).is_null() {
                return prev_next;
            }
            // Safety: We hold the lock on the slot pointing to the node, and a node is unlinked
            // from that slot before it is freed, so it cannot be freed under us.
            let curr = unsafe { prev_next.as_ref().unwrap() };
            if curr.value.ge(key) {
                return prev_next;
            }
            // Hand-over-hand: the assignment acquires the next lock before dropping ours.
            prev_next = curr.next.lock().unwrap();
        }
    }
}

impl<T> Default for Set<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Set<T> {
    fn drop(&mut self) {
        // We have exclusive access, so the pointers are read without locking. A poisoned lock
        // still guards a well-formed list, and dropping must not panic.
        let mut curr = *self.head.get_mut().unwrap_or_else(PoisonError::into_inner);
        while !curr.is_null() {
            // Safety: Every node reachable from the head was allocated by `Node::new` and is
            // owned by the set, since unlinked nodes are freed on removal.
            let mut node = unsafe { Box::from_raw(curr) };
            curr = *node.next.get_mut().unwrap_or_else(PoisonError::into_inner);
        }
    }
}

unsafe impl<T: Send> Send for Set<T> {}
unsafe impl<T: Send> Sync for Set<T> {}
