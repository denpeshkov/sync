use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};

pub struct ThreadPool {
    handles: Vec<JoinHandle<()>>,
    wg: Arc<WaitGroup>,
    tx: Option<mpsc::Sender<Task>>,
}

type Task = Box<dyn FnOnce() + Send + 'static>;

impl ThreadPool {
    pub fn new(num_threads: usize) -> Self {
        assert!(num_threads > 0);

        let mut handles = Vec::with_capacity(num_threads);

        let wg = Arc::new(WaitGroup::new());

        let (tx, rx) = mpsc::channel::<Task>();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..num_threads {
            let (rx, wg) = (rx.clone(), wg.clone());
            let jh = thread::spawn(move || {
                loop {
                    let Ok(task) = rx.lock().unwrap().recv() else {
                        break;
                    };
                    task();
                    wg.done();
                }
            });
            handles.push(jh);
        }
        Self {
            handles,
            wg,
            tx: Some(tx),
        }
    }

    /// Executes provided task on a thread in the pool.
    /// A `task` must not panic.
    pub fn accept<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.wg.add();
        _ = self
            .tx
            .as_ref()
            .expect("sender is present")
            .send(Box::new(task));
    }

    /// Blocks the current thread until all jobs in the pool have been executed.
    /// Blocks forever if a task panicked.
    pub fn join(&self) {
        self.wg.wait();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        // Drop `tx` first, otherwise `rx`s will hang forever waiting for tasks
        // and `jh.join()` will block forever.
        drop(self.tx.take());
        for jh in self.handles.drain(..) {
            _ = jh.join();
        }
    }
}

struct WaitGroup {
    counter: Mutex<usize>,
    cv: Condvar,
}

impl WaitGroup {
    const fn new() -> Self {
        Self {
            counter: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    fn add(&self) {
        *self.counter.lock().unwrap() += 1;
    }

    fn done(&self) {
        let mut counter = self.counter.lock().unwrap();
        *counter -= 1;
        if *counter == 0 {
            self.cv.notify_all();
        }
    }

    fn wait(&self) {
        let mut counter = self.counter.lock().unwrap();
        while *counter > 0 {
            counter = self.cv.wait(counter).unwrap();
        }
    }
}
