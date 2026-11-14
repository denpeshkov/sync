use std::collections::HashSet;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::thread::{self, ThreadId, scope};
use std::time::{Duration, Instant};

use sync::thread_pool::*;

// Tasks update shared state with `Relaxed` atomics: it is up to `join` (and dropping the pool) to
// make the effects of the tasks visible to the caller.

/// Number of worker threads used by the concurrent tests.
const THREADS: usize = if cfg!(miri) { 4 } else { 16 };

/// Number of tasks submitted by the throughput tests.
const STEPS: usize = if cfg!(miri) { 64 } else { 100_000 };

/// How long a slow task runs.
const SLOW: Duration = Duration::from_millis(10);

/// How long a task waits for an event before giving up.
///
/// Tasks wait with a deadline instead of blocking, so that a broken pool fails the test instead of
/// hanging it.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Returns a task that increments `counter`.
fn increment(counter: &Arc<AtomicUsize>) -> impl FnOnce() + Send + 'static {
    let counter = Arc::clone(counter);
    move || {
        counter.fetch_add(1, Relaxed);
    }
}

/// Returns a task that sleeps for [`SLOW`] and then increments `counter`.
fn slow_increment(counter: &Arc<AtomicUsize>) -> impl FnOnce() + Send + 'static {
    let counter = Arc::clone(counter);
    move || {
        thread::sleep(SLOW);
        counter.fetch_add(1, Relaxed);
    }
}

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

/// Accepts `n` tasks that wait for each other and then call `then`.
///
/// Returns a counter of the tasks that met, which reaches `n` only if the pool runs all of them at
/// the same time, i.e. on `n` distinct threads.
fn accept_rendezvous(pool: &ThreadPool, n: usize, then: fn()) -> Arc<AtomicUsize> {
    let arrived = Arc::new(AtomicUsize::new(0));
    let met = Arc::new(AtomicUsize::new(0));
    for _ in 0..n {
        let arrived = Arc::clone(&arrived);
        let met = Arc::clone(&met);
        pool.accept(move || {
            arrived.fetch_add(1, Relaxed);
            if wait_until(|| arrived.load(Relaxed) == n) {
                met.fetch_add(1, Relaxed);
            }
            then();
        });
    }
    met
}

/// Runs `tasks` tasks on a pool of `num_threads` threads and returns the threads they ran on.
fn task_threads(num_threads: usize, tasks: usize) -> HashSet<ThreadId> {
    let ids = Arc::new(Mutex::new(HashSet::new()));
    let pool = ThreadPool::new(num_threads);
    for _ in 0..tasks {
        let ids = Arc::clone(&ids);
        pool.accept(move || {
            ids.lock().unwrap().insert(thread::current().id());
        });
    }
    pool.join();
    ids.lock().unwrap().clone()
}

#[test]
fn is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ThreadPool>();
}

#[test]
#[should_panic]
fn new_with_zero_threads_panics() {
    ThreadPool::new(0);
}

#[test]
fn join_on_idle_pool_returns() {
    let pool = ThreadPool::new(THREADS);
    pool.join();
    pool.join();
}

#[test]
fn drop_idle_pool_returns() {
    drop(ThreadPool::new(THREADS));
}

#[test]
fn executes_task() {
    let counter = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(1);
    pool.accept(increment(&counter));
    pool.join();
    assert_eq!(counter.load(Relaxed), 1);
}

#[test]
fn executes_all_tasks_with_single_thread() {
    let counter = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(1);
    for _ in 0..STEPS {
        pool.accept(increment(&counter));
    }
    pool.join();
    assert_eq!(counter.load(Relaxed), STEPS);
}

#[test]
fn executes_each_task_exactly_once() {
    let runs: Arc<Vec<_>> = Arc::new((0..STEPS).map(|_| AtomicUsize::new(0)).collect());
    let pool = ThreadPool::new(THREADS);
    for i in 0..STEPS {
        let runs = Arc::clone(&runs);
        pool.accept(move || {
            runs[i].fetch_add(1, Relaxed);
        });
    }
    pool.join();
    for (i, run) in runs.iter().enumerate() {
        assert_eq!(run.load(Relaxed), 1, "task {i} ran a wrong number of times");
    }
}

#[test]
fn accept_does_not_wait_for_task() {
    let released = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let pool = ThreadPool::new(1);
    {
        let released = Arc::clone(&released);
        let finished = Arc::clone(&finished);
        // The task can only be released once `accept` has returned.
        pool.accept(move || {
            if wait_until(|| released.load(Relaxed)) {
                finished.store(true, Relaxed);
            }
        });
    }
    released.store(true, Relaxed);
    pool.join();
    assert!(
        finished.load(Relaxed),
        "accept waited for the task to finish"
    );
}

#[test]
fn accept_from_multiple_threads() {
    let counter = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(THREADS);
    scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                for _ in 0..STEPS / THREADS {
                    pool.accept(increment(&counter));
                }
            });
        }
    });
    pool.join();
    assert_eq!(counter.load(Relaxed), THREADS * (STEPS / THREADS));
}

#[test]
fn runs_tasks_off_calling_thread() {
    let threads = task_threads(THREADS, STEPS);
    assert!(
        !threads.contains(&thread::current().id()),
        "a task ran on the calling thread"
    );
}

#[test]
fn runs_tasks_on_at_most_num_threads_threads() {
    for num_threads in [1, 2, THREADS] {
        let threads = task_threads(num_threads, STEPS);
        assert!(
            threads.len() <= num_threads,
            "tasks ran on {} threads of a pool with {num_threads} threads",
            threads.len()
        );
    }
}

#[test]
fn runs_num_threads_tasks_concurrently() {
    let pool = ThreadPool::new(THREADS);
    let met = accept_rendezvous(&pool, THREADS, || {});
    pool.join();
    assert_eq!(
        met.load(Relaxed),
        THREADS,
        "the pool did not run {THREADS} tasks at the same time"
    );
}

#[test]
fn join_waits_for_slow_tasks() {
    let counter = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(THREADS);
    for _ in 0..THREADS * 4 {
        pool.accept(slow_increment(&counter));
    }
    pool.join();
    assert_eq!(counter.load(Relaxed), THREADS * 4);
}

#[test]
fn join_waits_for_tasks_accepted_by_tasks() {
    let counter = Arc::new(AtomicUsize::new(0));
    let pool = Arc::new(ThreadPool::new(THREADS));
    for _ in 0..THREADS {
        let task_pool = Arc::clone(&pool);
        let counter = Arc::clone(&counter);
        pool.accept(move || {
            for _ in 0..THREADS {
                task_pool.accept(slow_increment(&counter));
            }
        });
    }
    pool.join();
    assert_eq!(counter.load(Relaxed), THREADS * THREADS);
}

#[test]
fn join_from_multiple_threads() {
    let counter = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(THREADS);
    for _ in 0..THREADS * 4 {
        pool.accept(slow_increment(&counter));
    }
    scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                pool.join();
                assert_eq!(counter.load(Relaxed), THREADS * 4);
            });
        }
    });
}

#[test]
fn pool_is_reusable_after_join() {
    let counter = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(THREADS);
    for round in 1..=3 {
        for _ in 0..STEPS {
            pool.accept(increment(&counter));
        }
        pool.join();
        assert_eq!(counter.load(Relaxed), round * STEPS);
    }
}

#[test]
fn drop_runs_queued_tasks() {
    let counter = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::new(1);
    // Keeps the only worker busy, so that the other tasks are still queued when the pool is
    // dropped.
    pool.accept(|| thread::sleep(SLOW));
    for _ in 0..STEPS {
        pool.accept(increment(&counter));
    }
    drop(pool);
    assert_eq!(counter.load(Relaxed), STEPS);
}

#[test]
fn drop_joins_worker_threads() {
    /// Number of worker threads that have exited.
    static EXITED: AtomicUsize = AtomicUsize::new(0);

    struct ExitGuard;

    impl Drop for ExitGuard {
        fn drop(&mut self) {
            EXITED.fetch_add(1, Relaxed);
        }
    }

    thread_local! {
        // Dropped when its thread exits, which happens before joining that thread returns.
        static EXIT_GUARD: ExitGuard = const { ExitGuard };
    }

    let pool = ThreadPool::new(THREADS);
    // Makes every worker initialize its guard.
    let met = accept_rendezvous(&pool, THREADS, || EXIT_GUARD.with(|_| {}));
    drop(pool);
    assert_eq!(met.load(Relaxed), THREADS);
    assert_eq!(
        EXITED.load(Relaxed),
        THREADS,
        "drop returned before all worker threads exited"
    );
}
