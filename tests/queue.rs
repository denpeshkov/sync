//! Based on <https://github.com/kaist-cp/cs431/blob/main/src/lockfree/queue.rs>.

use std::cell::Cell;
use std::thread::scope;

use sync::queue::*;

const STEPS: i64 = if cfg!(miri) { 64 } else { 1_000_000 };

#[test]
fn new_is_empty() {
    let q: Queue<i64> = Queue::new();
    assert!(q.is_empty());
    assert_eq!(q.dequeue(), None);
}

#[test]
fn default_is_empty() {
    let q: Queue<i64> = Queue::default();
    assert!(q.is_empty());
    assert_eq!(q.dequeue(), None);
}

#[test]
fn enqueue_dequeue_1() {
    let q: Queue<i64> = Queue::new();
    assert!(q.is_empty());
    q.enqueue(37);
    assert!(!q.is_empty());
    assert_eq!(q.dequeue(), Some(37));
    assert!(q.is_empty());
}

#[test]
fn enqueue_dequeue_2() {
    let q: Queue<i64> = Queue::new();
    assert!(q.is_empty());
    q.enqueue(37);
    q.enqueue(48);
    assert_eq!(q.dequeue(), Some(37));
    assert!(!q.is_empty());
    assert_eq!(q.dequeue(), Some(48));
    assert!(q.is_empty());
}

#[test]
fn enqueue_dequeue_many_seq() {
    let q: Queue<i64> = Queue::new();
    assert!(q.is_empty());
    for i in 0..200 {
        q.enqueue(i);
    }
    assert!(!q.is_empty());
    for i in 0..200 {
        assert_eq!(q.dequeue(), Some(i));
    }
    assert!(q.is_empty());
}

#[test]
fn is_empty_does_not_dequeue() {
    let q = Queue::new();
    q.enqueue(1);
    q.enqueue(2);
    assert!(!q.is_empty());
    assert!(!q.is_empty());
    assert_eq!(q.dequeue(), Some(1));
    assert_eq!(q.dequeue(), Some(2));
    assert!(q.is_empty());
}

#[test]
fn drop_drops_remaining_values_once() {
    struct Counted<'a>(&'a Cell<usize>);

    impl Drop for Counted<'_> {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    let drops = Cell::new(0);
    let q = Queue::new();
    for _ in 0..10 {
        q.enqueue(Counted(&drops));
    }
    // Each dequeue leaves its moved-out value behind in the new dummy, which must not be dropped.
    for _ in 0..4 {
        drop(q.dequeue());
    }
    assert_eq!(drops.get(), 4);
    drop(q);
    assert_eq!(drops.get(), 10);
}

#[test]
fn enqueue_dequeue_many_spsc() {
    let q: Queue<i64> = Queue::new();
    assert!(q.is_empty());

    scope(|scope| {
        scope.spawn(|| {
            let mut next = 0;
            while next < STEPS {
                if let Some(elem) = q.dequeue() {
                    assert_eq!(elem, next);
                    next += 1;
                }
            }
        });
        for i in 0..STEPS {
            q.enqueue(i);
        }
    });
    assert!(q.is_empty());
}

#[test]
fn enqueue_dequeue_many_spmc() {
    let q = &Queue::new();
    let mut dequeued = Vec::new();
    scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..3 {
            handles.push(scope.spawn(|| {
                let values: Vec<_> = (0..STEPS).filter_map(|_| q.dequeue()).collect();
                // A single producer enqueues in increasing order, so each consumer sees that order.
                assert!(
                    values.is_sorted_by(|a, b| a < b),
                    "values were dequeued out of FIFO order"
                );
                values
            }));
        }
        scope.spawn(|| {
            for i in 0..STEPS {
                q.enqueue(i);
            }
        });
        for handle in handles {
            dequeued.extend(handle.join().unwrap());
        }
    });
    // Whatever the consumers left behind is part of the same accounting.
    while let Some(value) = q.dequeue() {
        dequeued.push(value);
    }

    dequeued.sort_unstable();
    assert!(
        dequeued.into_iter().eq(0..STEPS),
        "values were lost or dequeued twice"
    );
}

#[test]
fn enqueue_dequeue_many_mpmc() {
    enum LR {
        Left(i64),
        Right(i64),
    }

    let q = &Queue::new();
    let (mut left, mut right) = (Vec::new(), Vec::new());
    scope(|scope| {
        scope.spawn(|| {
            for i in 0..STEPS {
                q.enqueue(LR::Left(i));
            }
        });
        scope.spawn(|| {
            for i in 0..STEPS {
                q.enqueue(LR::Right(i));
            }
        });
        let mut handles = Vec::new();
        for _ in 0..2 {
            handles.push(scope.spawn(|| {
                let (mut vl, mut vr) = (Vec::new(), Vec::new());
                for _ in 0..STEPS {
                    match q.dequeue() {
                        Some(LR::Left(x)) => vl.push(x),
                        Some(LR::Right(x)) => vr.push(x),
                        None => {}
                    }
                }
                // Each consumer sees the values of each producer in the order they were enqueued.
                assert!(
                    vl.is_sorted_by(|a, b| a < b),
                    "left values were dequeued out of FIFO order"
                );
                assert!(
                    vr.is_sorted_by(|a, b| a < b),
                    "right values were dequeued out of FIFO order"
                );
                (vl, vr)
            }));
        }
        for handle in handles {
            let (vl, vr) = handle.join().unwrap();
            left.extend(vl);
            right.extend(vr);
        }
    });
    // Whatever the consumers left behind is part of the same accounting.
    while let Some(value) = q.dequeue() {
        match value {
            LR::Left(x) => left.push(x),
            LR::Right(x) => right.push(x),
        }
    }

    left.sort_unstable();
    right.sort_unstable();
    assert!(
        left.into_iter().eq(0..STEPS),
        "left values were lost or dequeued twice"
    );
    assert!(
        right.into_iter().eq(0..STEPS),
        "right values were lost or dequeued twice"
    );
}
