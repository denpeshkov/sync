use std::mem::{self, MaybeUninit};
use std::sync::atomic::Ordering;

use crossbeam::epoch::{self, Atomic, Owned, Shared};
use crossbeam::utils::{Backoff, CachePadded};

/// Michael-Scott lock-free queue.
pub struct Queue<T> {
    /// Dummy node, whose successor is the front of the queue.
    head: CachePadded<Atomic<Node<T>>>,
    /// Last node, or the one before it while an `enqueue`is in progress.
    /// Never behind `head`.
    tail: CachePadded<Atomic<Node<T>>>,
}

struct Node<T> {
    /// Uninitialized in the dummy node: either never set or already dequeued.
    value: MaybeUninit<T>,
    next: Atomic<Node<T>>,
}

impl<T> Queue<T> {
    /// Creates a new, empty queue.
    pub fn new() -> Self {
        // Both ends start at a dummy node, so neither is ever `null`.
        let dummy = Box::into_raw(Box::new(Node {
            value: MaybeUninit::uninit(),
            next: Atomic::null(),
        }))
        .cast_const();

        Self {
            head: CachePadded::new(dummy.into()),
            tail: CachePadded::new(dummy.into()),
        }
    }

    /// Adds an element to the back of the queue.
    pub fn enqueue(&self, value: T) {
        let mut new_tail = Owned::new(Node {
            value: MaybeUninit::new(value),
            next: Atomic::null(),
        });

        let backoff = Backoff::new();
        let guard = epoch::pin();
        loop {
            // Acquire pairs with the Release CAS that published the node, so its contents are
            // visible.
            let tail = self.tail.load(Ordering::Acquire, &guard);
            // SAFETY: `tail` is never null, and the pinned guard keeps the node from being freed.
            let tail_ref = unsafe { tail.deref() };
            // Acquire, so that changing `tail` to `tail_next` below also publishes its contents.
            let tail_next = tail_ref.next.load(Ordering::Acquire, &guard);

            // `tail` lags behind: help the stalled enqueuer move it forward, then retry.
            if !tail_next.is_null() {
                _ = self.tail.compare_exchange(
                    tail,
                    tail_next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                );
                continue;
            }

            // Link the node after the last one.
            // Release publishes its contents to the threads that load it.
            match tail_ref.next.compare_exchange_weak(
                Shared::null(),
                new_tail,
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(new_tail) => {
                    // Move `tail` to the linked node. On failure another thread already did.
                    _ = self.tail.compare_exchange(
                        tail,
                        new_tail,
                        Ordering::Release,
                        Ordering::Relaxed,
                        &guard,
                    );
                    break;
                }
                Err(err) => {
                    new_tail = err.new;
                    backoff.spin();
                }
            }
        }
    }

    /// Removes the first element and returns it, if the queue is not empty.
    pub fn dequeue(&self) -> Option<T> {
        let backoff = Backoff::new();
        let guard = epoch::pin();
        loop {
            // Acquire pairs with the Release CAS that published the node, so its contents are
            // visible.
            let head = self.head.load(Ordering::Acquire, &guard);
            // SAFETY: `head` is never null, and the pinned guard keeps the node from being freed.
            let head_next = unsafe { head.deref() }.next.load(Ordering::Acquire, &guard);
            // SAFETY: The pinned guard keeps the node from being freed.
            let head_next_ref = unsafe { head_next.as_ref() }?;

            // `head` must never overtake `tail`, or `tail` would point to a retired node, so
            // move `tail` off the dummy first.
            // The CAS must be strong: its failure has to mean that `tail` already moved on.
            // `tail` is only compared, so Relaxed suffices.
            let tail = self.tail.load(Ordering::Relaxed, &guard);
            if head == tail {
                _ = self.tail.compare_exchange(
                    tail,
                    head_next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                );
            }

            // `head_next` becomes the new dummy.
            // Release publishes its contents to the threads that load it from `head`.
            match self.head.compare_exchange_weak(
                head,
                head_next,
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(_) => unsafe {
                    // SAFETY: The old dummy is unreachable now, and `defer_destroy()` frees it
                    // only after threads that loaded it earlier are unpinned.
                    // Success of the CAS makes us the owner of the value in `head_next`,
                    // initialized by its enqueuer.
                    guard.defer_destroy(head);
                    return Some(head_next_ref.value.assume_init_read());
                },
                Err(_) => backoff.spin(),
            }
        }
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        let guard = epoch::pin();
        let head = self.head.load(Ordering::Acquire, &guard);
        // SAFETY: `head` is never null, and the pinned guard keeps the node from being freed.
        let head_next = unsafe { head.deref() }.next.load(Ordering::Relaxed, &guard);
        head_next.is_null()
    }
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        let head = mem::take(&mut *self.head);
        // SAFETY: `&mut self` guarantees that no other thread holds a reference to the nodes, and
        // nodes reachable from `head` are owned by the queue.
        // The dummy's value is uninitialized, so the dummy is freed without dropping it.
        let mut head_next = unsafe { head.into_owned() }.into_box().next;
        while let Some(node) = unsafe { head_next.try_into_owned() }.map(Owned::into_box) {
            // SAFETY: Every node after the dummy holds an initialized value.
            drop(unsafe { node.value.assume_init() });
            head_next = node.next;
        }
    }
}

unsafe impl<T: Send> Sync for Queue<T> {}
unsafe impl<T: Send> Send for Queue<T> {}
