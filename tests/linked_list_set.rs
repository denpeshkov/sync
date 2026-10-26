// Adapted from: https://github.com/kaist-cp/cs431/blob/main/homework/src/test/adt/map.rs

use std::collections::{HashMap, HashSet};
use std::thread;

use rand::distr::StandardUniform;
use rand::prelude::*;
use sync::linked_list_set::Set;

/// Number of threads used by the concurrent tests.
const THREADS: usize = if cfg!(miri) { 4 } else { 16 };

/// Number of operations performed by each thread of the concurrent tests.
const STEPS: usize = if cfg!(miri) { 32 } else { 4096 * 4 };

/// Number of operations performed by the sequential tests.
const SEQUENTIAL_STEPS: usize = if cfg!(miri) { 128 } else { 4096 };

#[test]
fn basic() {
    let set = Set::new();
    assert!(set.insert(1));
    assert!(set.insert(2));
    assert!(set.insert(3));
    assert!(set.remove(&2));
    assert!(set.contains(&1));
    assert!(!set.contains(&2));
    assert!(set.contains(&3));
    assert!(set.remove(&3));
    assert!(!set.remove(&3));
}

/// Runs many operations in a single thread and compares the outcome of each one against
/// [`HashSet`] as a reference implementation.
#[test]
fn stress_sequential() {
    enum Op {
        ContainsSome,
        ContainsNone,
        Insert,
        RemoveSome,
        RemoveNone,
    }
    impl Distribution<Op> for StandardUniform {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Op {
            match rng.random_range(0..5) {
                0 => Op::ContainsSome,
                1 => Op::ContainsNone,
                2 => Op::Insert,
                3 => Op::RemoveSome,
                4 => Op::RemoveNone,
                _ => unreachable!(),
            }
        }
    }

    let mut rng = rand::rng();
    let set: Set<usize> = Set::new();
    let mut reference = HashSet::new();

    for _ in 0..SEQUENTIAL_STEPS {
        match rng.random() {
            Op::ContainsSome => {
                let Some(value) = reference.iter().choose(&mut rng).cloned() else {
                    continue;
                };
                assert!(set.contains(&value));
            }
            Op::ContainsNone => {
                let value: usize = rng.random_range(0..4096);
                assert_eq!(set.contains(&value), reference.contains(&value));
            }
            Op::Insert => {
                let value = rng.random_range(0..4096);
                assert_eq!(set.insert(value), reference.insert(value));
            }
            Op::RemoveSome => {
                let Some(value) = reference.iter().choose(&mut rng).cloned() else {
                    continue;
                };
                assert!(set.remove(&value));
                assert!(reference.remove(&value));
            }
            Op::RemoveNone => {
                let value = rng.random_range(0..4096);
                assert_eq!(set.remove(&value), reference.remove(&value));
            }
        }
    }
}

/// Runs random operations concurrently. Nothing is asserted beyond the set surviving them, which
/// leaves it to Miri and the sanitizers to catch data races, leaks and undefined behavior.
#[test]
fn stress_concurrent() {
    let set: Set<usize> = Set::new();
    thread::scope(|s| {
        for _ in 0..THREADS {
            let _ = s.spawn(|| {
                let mut rng = rand::rng();
                for _ in 0..STEPS {
                    let value = rng.random_range(0..4096);
                    match rng.random_range(0..3) {
                        0 => set.contains(&value),
                        1 => set.insert(value),
                        2 => set.remove(&value),
                        _ => unreachable!(),
                    };
                }
            });
        }
    });
}

/// Runs random operations concurrently, recording each one with its result, and checks the log
/// afterwards.
///
/// Whatever the interleaving was, a value that was found must have been inserted, and it cannot
/// have been removed more often than it was inserted. Since presence is binary, the two counts
/// must also agree with what the set holds once the threads are done.
#[test]
fn log_concurrent() {
    // An operation and its result, as observed by one thread.
    enum Log {
        Contains(u8, bool),
        Insert(u8, bool),
        Remove(u8, bool),
    }

    let set = Set::new();

    let logs = thread::scope(|s| {
        let handles = (0..THREADS)
            .map(|_| {
                s.spawn(|| {
                    let mut rng = rand::rng();
                    let mut logs = Vec::with_capacity(STEPS);
                    for _ in 0..STEPS {
                        let value = rng.random();
                        logs.push(match rng.random_range(0..3) {
                            0 => Log::Contains(value, set.contains(&value)),
                            1 => Log::Insert(value, set.insert(value)),
                            2 => Log::Remove(value, set.remove(&value)),
                            _ => unreachable!(),
                        });
                    }
                    logs
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });

    let mut inserted = HashMap::new();
    let mut removed = HashMap::new();
    let mut found = HashSet::new();
    for log in logs {
        match log {
            Log::Contains(value, true) => {
                found.insert(value);
            }
            Log::Insert(value, true) => *inserted.entry(value).or_insert(0usize) += 1,
            Log::Remove(value, true) => *removed.entry(value).or_insert(0usize) += 1,
            _ => {}
        }
    }

    for value in found {
        assert!(inserted.contains_key(&value));
    }
    for value in 0..=u8::MAX {
        let inserted = inserted.get(&value).copied().unwrap_or(0);
        let removed = removed.get(&value).copied().unwrap_or(0);
        // Every successful insertion makes the value present and every successful removal makes it
        // absent, so the two alternate and the counts can differ by at most the one insertion that
        // was not undone.
        assert!(inserted >= removed && inserted - removed <= 1);
        assert_eq!(set.contains(&value), inserted - removed == 1);
    }
}
