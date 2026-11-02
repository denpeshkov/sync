use std::thread::scope;

use rand::RngExt;
use sync::stack::*;

/// Number of threads used by the concurrent tests.
const THREADS: usize = if cfg!(miri) { 4 } else { 16 };

const STEPS: usize = if cfg!(miri) { 32 } else { 4096 * 4 };

#[test]
fn new_is_empty() {
    let stack: Stack<i32> = Stack::new();
    assert!(stack.is_empty());
    assert_eq!(stack.pop(), None);
}

#[test]
fn new_is_const() {
    static STACK: Stack<i32> = Stack::new();
    STACK.push(1);
    assert_eq!(STACK.pop(), Some(1));
}

#[test]
fn default_is_empty() {
    let stack: Stack<i32> = Stack::default();
    assert!(stack.is_empty());
    assert_eq!(stack.pop(), None);
}

#[test]
fn pop_returns_values_in_lifo_order() {
    let stack = Stack::new();
    for i in 0..100 {
        stack.push(i);
    }
    for i in (0..100).rev() {
        assert_eq!(stack.pop(), Some(i));
    }
    assert_eq!(stack.pop(), None);
}

#[test]
fn pop_on_empty_keeps_returning_none() {
    let stack = Stack::new();
    for _ in 0..3 {
        assert_eq!(stack.pop(), None::<i32>);
    }
    stack.push(1);
    assert_eq!(stack.pop(), Some(1));
    for _ in 0..3 {
        assert_eq!(stack.pop(), None);
    }
}

#[test]
fn is_empty_tracks_contents() {
    let stack = Stack::new();
    assert!(stack.is_empty());
    stack.push(1);
    assert!(!stack.is_empty());
    stack.push(2);
    assert!(!stack.is_empty());
    stack.pop();
    assert!(!stack.is_empty());
    stack.pop();
    assert!(stack.is_empty());
}

#[test]
fn interleaved_push_and_pop() {
    let stack = Stack::new();
    stack.push(1);
    stack.push(2);
    assert_eq!(stack.pop(), Some(2));
    stack.push(3);
    assert_eq!(stack.pop(), Some(3));
    assert_eq!(stack.pop(), Some(1));
    assert_eq!(stack.pop(), None);
    stack.push(4);
    assert_eq!(stack.pop(), Some(4));
}

#[test]
fn holds_values_that_own_heap_memory() {
    let stack = Stack::new();
    for i in 0..100 {
        stack.push(format!("value-{i}"));
    }
    for i in (0..100).rev() {
        assert_eq!(stack.pop(), Some(format!("value-{i}")));
    }
    assert_eq!(stack.pop(), None);
}

#[test]
fn push() {
    let stack = Stack::new();
    scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..10 {
            let handle = scope.spawn(|| {
                for i in 0..100 {
                    stack.push(i);
                    assert!(stack.pop().is_some());

                    stack.push(i);
                    stack.push(i + 1);
                    assert!(stack.pop().is_some());
                    assert!(stack.pop().is_some());
                }
            });
            handles.push(handle);
        }
    });
    assert!(stack.pop().is_none());
    assert!(stack.is_empty());
}

#[test]
fn concurrent_push() {
    let stack = &Stack::new();
    scope(|scope| {
        for thread in 0..THREADS {
            scope.spawn(move || {
                for i in 0..STEPS {
                    stack.push(thread * STEPS + i);
                }
            });
        }
    });

    // The value each thread is expected to yield next, walking its pushes backwards.
    let mut expected = [STEPS; THREADS];
    let mut popped = 0;
    while let Some(value) = stack.pop() {
        let (thread, i) = (value / STEPS, value % STEPS);
        assert!(thread < THREADS, "popped value {value} was never pushed");
        assert_eq!(
            i + 1,
            expected[thread],
            "thread {thread} yielded {i} out of LIFO order"
        );
        expected[thread] = i;
        popped += 1;
    }
    assert_eq!(popped, THREADS * STEPS, "values were lost or popped twice");
    assert_eq!(expected, [0; THREADS]);
    assert!(stack.is_empty());
}

#[test]
fn concurrent_pop() {
    let stack = &Stack::new();
    for i in 0..THREADS * STEPS {
        stack.push(i);
    }

    let mut popped = Vec::with_capacity(THREADS * STEPS);
    scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            handles.push(scope.spawn(|| {
                (0..STEPS)
                    .map(|_| stack.pop().expect("stack must not be empty"))
                    .collect::<Vec<_>>()
            }));
        }
        for handle in handles {
            popped.extend(handle.join().unwrap());
        }
    });

    popped.sort_unstable();
    assert!(
        popped.iter().copied().eq(0..THREADS * STEPS),
        "values were lost or popped twice"
    );
    assert!(stack.is_empty());
    assert_eq!(stack.pop(), None);
}

#[test]
fn concurrent_pop_on_empty() {
    const ITEMS: usize = STEPS / 2;

    let stack = &Stack::new();
    for i in 0..ITEMS {
        stack.push(i);
    }

    let mut popped = Vec::with_capacity(ITEMS);
    scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            // Every thread asks for more values than the stack ever held.
            let handle = scope.spawn(|| (0..STEPS).filter_map(|_| stack.pop()).collect::<Vec<_>>());
            handles.push(handle);
        }
        for handle in handles {
            popped.extend(handle.join().unwrap());
        }
    });

    popped.sort_unstable();
    assert!(
        popped.iter().copied().eq(0..ITEMS),
        "values were lost or popped twice"
    );
    assert!(stack.is_empty());
}

#[test]
fn concurrent_push_pop() {
    let stack = &Stack::new();
    let mut popped = Vec::with_capacity(THREADS * STEPS);
    scope(|scope| {
        let mut handles = Vec::new();
        for thread in 0..THREADS {
            let handle = scope.spawn(move || {
                let mut popped = Vec::with_capacity(STEPS);
                for i in 0..STEPS {
                    stack.push(thread * STEPS + i);
                    popped.extend(stack.pop());
                }
                popped
            });
            handles.push(handle);
        }
        for handle in handles {
            popped.extend(handle.join().unwrap());
        }
    });
    // Whatever the threads left behind is part of the same accounting.
    while let Some(value) = stack.pop() {
        popped.push(value);
    }

    popped.sort_unstable();
    assert!(
        popped.iter().copied().eq(0..THREADS * STEPS),
        "values were lost or popped twice"
    );
    assert!(stack.is_empty());
}

#[test]
fn concurrent_push_pop_owned_values() {
    let stack = &Stack::new();
    let mut popped = Vec::with_capacity(THREADS * STEPS);
    scope(|scope| {
        let mut handles = Vec::new();
        for thread in 0..THREADS {
            let handle = scope.spawn(move || {
                let mut popped = Vec::with_capacity(STEPS);
                for i in 0..STEPS {
                    stack.push(Box::new(thread * STEPS + i));
                    popped.extend(stack.pop());
                }
                popped
            });
            handles.push(handle);
        }
        for handle in handles {
            popped.extend(handle.join().unwrap());
        }
    });
    while let Some(value) = stack.pop() {
        popped.push(value);
    }

    let mut popped: Vec<usize> = popped.into_iter().map(|value| *value).collect();
    popped.sort_unstable();
    assert!(
        popped.iter().copied().eq(0..THREADS * STEPS),
        "values were lost or popped twice"
    );
    assert!(stack.is_empty());
}

#[test]
fn concurrent_push_pop_random() {
    let stack = &Stack::new();
    let (mut pushed, mut popped) = (Vec::new(), Vec::new());
    scope(|scope| {
        let mut handles = Vec::new();
        for thread in 0..THREADS {
            let handle = scope.spawn(move || {
                let mut rng = rand::rng();
                let (mut pushed, mut popped) = (Vec::new(), Vec::new());
                for i in 0..STEPS {
                    if rng.random_bool(0.5) {
                        let value = thread * STEPS + i;
                        stack.push(value);
                        pushed.push(value);
                    } else {
                        popped.extend(stack.pop());
                    }
                }
                (pushed, popped)
            });
            handles.push(handle);
        }
        for handle in handles {
            let (thread_pushed, thread_popped) = handle.join().unwrap();
            pushed.extend(thread_pushed);
            popped.extend(thread_popped);
        }
    });
    while let Some(value) = stack.pop() {
        popped.push(value);
    }

    pushed.sort_unstable();
    popped.sort_unstable();
    assert_eq!(popped, pushed, "values were lost, popped twice or made up");
    assert!(stack.is_empty());
}
