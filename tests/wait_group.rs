#![cfg(target_os = "linux")]

use std::cell::UnsafeCell;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::thread::{self, JoinHandle, scope};
use std::time::{Duration, Instant};

use sync::wait_group::*;

/// Number of threads used by the concurrent tests.
const THREADS: usize = if cfg!(miri) { 4 } else { 16 };

const STEPS: usize = if cfg!(miri) { 32 } else { 4096 * 4 };

/// Number of times a test repeats a scenario whose outcome depends on scheduling.
const ROUNDS: usize = 16;

/// How long a test lets a waiter run before checking that it is still blocked.
const SLOW: Duration = Duration::from_millis(10);

/// How long a test waits for a waiter to return before giving up.
///
/// Tests wait with a deadline instead of joining the waiters, so that a broken wait group fails the
/// test instead of hanging it.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Polls `cond` until it holds, returning `false` if it doesn't within [`TIMEOUT`].
fn wait_until(cond: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + TIMEOUT;
    while !cond() {
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(1));
    }
    true
}

/// Spawns a thread that waits on `wg`.
fn spawn_waiter(wg: &Arc<WaitGroup>) -> JoinHandle<()> {
    let wg = Arc::clone(wg);
    thread::spawn(move || wg.wait())
}

/// A value that threads share without synchronizing on their own, so that Miri reports a data race
/// unless the wait group orders the accesses.
struct Unsynchronized(UnsafeCell<usize>);

// SAFETY: The tests only access the value in orders established by a `WaitGroup`.
unsafe impl Sync for Unsynchronized {}

#[test]
fn is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<WaitGroup>();
}

#[test]
fn new_wait_returns_immediately() {
    WaitGroup::new().wait();
}

#[test]
fn default_wait_returns_immediately() {
    WaitGroup::default().wait();
}

#[test]
fn new_is_const() {
    static WG: WaitGroup = WaitGroup::new();
    WG.add(1);
    WG.done();
    WG.wait();
}

#[test]
fn add_zero_wait_returns_immediately() {
    let wg = WaitGroup::new();
    wg.add(0);
    wg.wait();
}

#[test]
fn wait_returns_once_dones_match_adds() {
    let wg = WaitGroup::new();
    wg.add(1);
    wg.add(2);
    for _ in 0..3 {
        wg.done();
    }
    wg.wait();
}

#[test]
fn done_without_add_panics() {
    let wg = Arc::new(WaitGroup::new());
    assert!(
        catch_unwind(|| wg.done()).is_err(),
        "done did not panic on a zero counter"
    );
    // The failed `done` leaves the counter at zero.
    let waiter = spawn_waiter(&wg);
    assert!(
        wait_until(|| waiter.is_finished()),
        "wait did not return after a failed done"
    );
}

#[test]
fn done_more_than_added_panics() {
    let wg = Arc::new(WaitGroup::new());
    wg.add(2);
    wg.done();
    wg.done();
    assert!(
        catch_unwind(|| wg.done()).is_err(),
        "done did not panic on a zero counter"
    );
    // The failed `done` leaves the counter at zero.
    let waiter = spawn_waiter(&wg);
    assert!(
        wait_until(|| waiter.is_finished()),
        "wait did not return after a failed done"
    );
}

#[test]
fn add_overflow_panics() {
    let wg = WaitGroup::new();
    wg.add(u32::MAX - 1);
    assert!(
        catch_unwind(|| wg.add(2)).is_err(),
        "add did not panic on overflow"
    );
    // The failed `add` leaves the counter one below the maximum.
    wg.add(1);
    // Adding zero at the maximum doesn't overflow.
    wg.add(0);
    assert!(
        catch_unwind(|| wg.add(1)).is_err(),
        "add did not panic on overflow"
    );
}

#[test]
fn wait_blocks_until_counter_is_zero() {
    let wg = Arc::new(WaitGroup::new());
    wg.add(1);
    let waiter = spawn_waiter(&wg);
    thread::sleep(SLOW);
    assert!(
        !waiter.is_finished(),
        "wait returned while the counter was positive"
    );

    // Changes the counter while the waiter is blocked, without letting it reach zero.
    wg.add(1);
    wg.done();
    thread::sleep(SLOW);
    assert!(
        !waiter.is_finished(),
        "wait returned while the counter was positive"
    );

    wg.done();
    assert!(
        wait_until(|| waiter.is_finished()),
        "wait did not return after the counter reached zero"
    );
}

#[test]
fn done_releases_all_waiters() {
    let wg = Arc::new(WaitGroup::new());
    wg.add(1);
    let waiters: Vec<_> = (0..THREADS).map(|_| spawn_waiter(&wg)).collect();
    // Gives the waiters time to block.
    thread::sleep(SLOW);
    assert!(
        !waiters.iter().any(JoinHandle::is_finished),
        "wait returned while the counter was positive"
    );
    wg.done();
    assert!(
        wait_until(|| waiters.iter().all(JoinHandle::is_finished)),
        "done did not release all waiters"
    );
}

#[test]
fn done_from_multiple_threads() {
    let wg = Arc::new(WaitGroup::new());
    // The counter goes through zero once per round.
    for _ in 0..3 {
        wg.add(THREADS as u32);
        let waiter = spawn_waiter(&wg);
        scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| wg.done());
            }
        });
        assert!(
            wait_until(|| waiter.is_finished()),
            "wait did not return after the counter reached zero"
        );
    }
}

#[test]
fn add_and_done_from_multiple_threads() {
    let wg = Arc::new(WaitGroup::new());
    // Keeps the counter above zero while the workers run.
    wg.add(1);
    let waiter = spawn_waiter(&wg);
    scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                for _ in 0..STEPS {
                    wg.add(1);
                    wg.done();
                }
            });
        }
    });
    assert!(
        !waiter.is_finished(),
        "wait returned while the counter was positive"
    );
    wg.done();
    assert!(
        wait_until(|| waiter.is_finished()),
        "wait did not return after the counter reached zero"
    );
}

#[test]
fn wait_synchronizes_with_done() {
    // Miri only reports a missing synchronization if `wait` finds the counter at zero on its first
    // load, which takes a few rounds to happen.
    for _ in 0..ROUNDS {
        let wg = Arc::new(WaitGroup::new());
        let values: Arc<Vec<_>> = Arc::new(
            (0..THREADS)
                .map(|_| Unsynchronized(UnsafeCell::new(0)))
                .collect(),
        );
        // Counts the workers that called `done`. It is `Relaxed`, so it doesn't order the accesses.
        let finished = Arc::new(AtomicUsize::new(0));
        wg.add(THREADS as u32);

        let reader = {
            let wg = Arc::clone(&wg);
            let values = Arc::clone(&values);
            let finished = Arc::clone(&finished);
            thread::spawn(move || {
                // Waits for the counter to reach zero first, so that `wait` can return without
                // calling into the futex, which would order the accesses on its own.
                assert!(
                    wait_until(|| finished.load(Relaxed) == THREADS),
                    "the workers did not finish"
                );
                wg.wait();
                for (i, value) in values.iter().enumerate() {
                    // SAFETY: `wait` orders this read after the write before the matching `done`.
                    assert_eq!(unsafe { *value.0.get() }, i + 1);
                }
            })
        };

        let (wg, finished) = (&*wg, &*finished);
        scope(|scope| {
            for (i, value) in values.iter().enumerate() {
                scope.spawn(move || {
                    // SAFETY: Only this thread accesses the value until it calls `done`.
                    unsafe { *value.0.get() = i + 1 };
                    wg.done();
                    finished.fetch_add(1, Relaxed);
                });
            }
        });
        assert!(
            wait_until(|| reader.is_finished()),
            "wait did not return after the counter reached zero"
        );
        reader.join().unwrap();
    }
}
