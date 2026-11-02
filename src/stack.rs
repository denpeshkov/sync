use std::mem::{self, ManuallyDrop};
use std::ptr;
use std::sync::atomic::Ordering;

use crossbeam::epoch::{Atomic, Owned};
use crossbeam::*;

/// Concurrent Treiber stack.
pub struct Stack<T> {
    head: Atomic<Node<T>>,
}

struct Node<T> {
    value: ManuallyDrop<T>,
    next: Atomic<Node<T>>,
}

impl<T> Stack<T> {
    /// Creates a new empty stack.
    pub const fn new() -> Stack<T> {
        Self {
            head: Atomic::null(),
        }
    }

    /// Pushes a value on top of the stack.
    pub fn push(&self, value: T) {
        let mut new_head = Owned::new(Node {
            value: ManuallyDrop::new(value),
            next: Atomic::null(),
        });

        let guard = epoch::pin();
        loop {
            let head = self.head.load(Ordering::Relaxed, &guard);
            new_head.next.store(head, Ordering::Relaxed);

            // We need Release to establish happens-before from `push()` to `pop()`,
            // so the popping thread sees the contents of the node.
            match self.head.compare_exchange_weak(
                head,
                new_head,
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(_) => break,
                Err(err) => new_head = err.new,
            }
        }
    }

    /// Pops the top element from the stack.
    /// Returns [`None`] if the stack is empty.
    pub fn pop(&self) -> Option<T> {
        let guard = epoch::pin();
        loop {
            // We need Acquire to establish happens-before from `push()` to `pop()`.
            let head = self.head.load(Ordering::Acquire, &guard);

            // SAFETY: The guard is pinned, so a node loaded from `head` cannot be freed before
            // the guard is dropped.
            let head_ref = unsafe { head.as_ref()? };
            let new_head = head_ref.next.load(Ordering::Relaxed, &guard);

            if self
                .head
                .compare_exchange_weak(head, new_head, Ordering::Relaxed, Ordering::Relaxed, &guard)
                .is_ok()
            {
                // SAFETY: Successof the CAS makes us the owner of the value, and `defer_destroy()`
                // frees the node only after threads that loaded it earlier are unpinned.
                unsafe {
                    guard.defer_destroy(head);
                    return Some(ManuallyDrop::into_inner(ptr::read(&head_ref.value)));
                }
            }
        }
    }

    /// Returns `true` if the stack is empty.
    pub fn is_empty(&self) -> bool {
        let guard = epoch::pin();
        self.head.load(Ordering::Relaxed, &guard).is_null()
    }
}

impl<T> Default for Stack<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Stack<T> {
    fn drop(&mut self) {
        let mut head = mem::replace(&mut self.head, Atomic::null());
        // SAFETY: `&mut self` guarantees that no other thread holds a reference
        // to the node, and nodes reachable from `head` are owned by the stack.
        while let Some(node) = unsafe { head.try_into_owned() }.map(Owned::into_box) {
            let Node { value, next } = *node;
            drop(ManuallyDrop::into_inner(value));
            head = next;
        }
    }
}

unsafe impl<T: Send> Send for Stack<T> {}
unsafe impl<T: Send> Sync for Stack<T> {}
