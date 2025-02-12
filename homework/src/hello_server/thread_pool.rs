//! Thread pool that joins all thread when dropped.

use core::fmt;
// NOTE: Crossbeam channels are MPMC, which means that you don't need to wrap the receiver in
// Arc<Mutex<..>>. Just clone the receiver and give it to each worker thread.
use std::cell::RefCell;
use std::collections::LinkedList;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::{thread, time};

use chrono::prelude::{DateTime, Local};
use crossbeam_channel::{Receiver, Sender, unbounded};
use lazy_static::lazy_static;

struct Job(Box<dyn FnOnce() + Send + 'static>);

#[derive(Debug)]
struct Worker {
    _id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for Worker {
    /// When dropped, the thread's `JoinHandle` must be `join`ed.  If the worker panics, then this
    /// function should panic too.
    ///
    /// NOTE: The thread is detached if not `join`ed explicitly.
    fn drop(&mut self) {
        self.thread.take().unwrap().join().unwrap();
    }
}

/// Internal data structure for tracking the current job status. This is shared by worker closures
/// via `Arc` so that the workers can report to the pool that it started/finished a job.
#[derive(Debug)]
struct ThreadPoolInner {
    job_count: AtomicUsize,
    _workers: Mutex<Vec<Worker>>,
    job_recv: Receiver<Job>,
    shutdown: Arc<AtomicBool>,
}

impl ThreadPoolInner {
    /// Increment the job count.
    fn start_job(&self) {
        self.job_count.fetch_add(1, Ordering::Release);
    }

    /// Decrement the job count.
    fn finish_job(&self) {
        self.job_count.fetch_sub(1, Ordering::AcqRel);
    }

    /// Wait until the job count becomes 0.
    ///
    /// NOTE: We can optimize this function by adding another field to `ThreadPoolInner`, but let's
    /// not care about that in this homework.
    fn wait_empty(&self) {
        while self.job_count.load(Ordering::Acquire) != 0 {
            thread::sleep(time::Duration::from_millis(300));
        }
    }
}

#[derive(Debug)]
struct PanicInfo {
    time: DateTime<Local>,
    info: String,
}

#[derive(Debug)]
struct PanicList {
    count: AtomicUsize,
    list: Mutex<LinkedList<PanicInfo>>,
}

lazy_static! {
    #[derive(Debug)]
    static ref _panic_info: PanicList = PanicList {
        count: AtomicUsize::new(0),
        list: Mutex::new(LinkedList::new())
    };
    pub static ref THREADPOOL: ThreadPool = ThreadPool::_new(8);
}

impl fmt::Display for PanicInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[Panic:] [{}] {}",
            self.time.format("%Y-%m-%d %H:%M:%S"),
            self.info
        )
    }
}

/// Thread pool.
#[derive(Debug)]
pub struct ThreadPool {
    inner: Arc<ThreadPoolInner>,
    job_sender: Option<Sender<Job>>,
    watchdog: Option<thread::JoinHandle<()>>,
}

impl ThreadPool {
    /// Create a new ThreadPool with `size` threads.
    ///
    /// # Panics
    ///
    /// Panics if `size` is 0.
    pub fn new(dummy: usize) -> &'static Self {
        &THREADPOOL
    }

    fn _new(size: usize) -> Self {
        assert!(size > 0);

        let (sender, receiver): (Sender<Job>, Receiver<Job>) = crossbeam_channel::unbounded();
        let mut workers: Mutex<Vec<Worker>> = Mutex::new(Vec::with_capacity(size));
        let inner = Arc::new(ThreadPoolInner {
            _workers: workers,
            job_recv: receiver,
            job_count: AtomicUsize::new(0),
            shutdown: Arc::new(AtomicBool::new(false)),
        });
        let watchdog_inner = Arc::clone(&inner);

        {
            let panic_inner = Arc::clone(&inner);
            let orig_hook = panic::take_hook();
            use std::panic;
            panic::set_hook(Box::new(move |info| {
                if !THREADPOOL.inner.shutdown.load(Ordering::Acquire) {
                    let mut payload: String;
                    if let Some(s) = info.payload().downcast_ref::<&str>() {
                        payload = s.to_string();
                    } else if let Some(s) = info.payload().downcast_ref::<String>() {
                        payload = s.clone();
                    } else {
                        payload = String::from("Explicit Panic.");
                    }
                    _panic_info.list.lock().unwrap().push_back(PanicInfo {
                        time: Local::now(),
                        info: payload,
                    });
                    _panic_info.count.fetch_add(1, Ordering::Release);
                    panic_inner.finish_job();
                }
                orig_hook(info);
            }));
        }

        let mut _self = Self {
            inner: Arc::clone(&inner),
            job_sender: Some(sender),
            watchdog: Some(thread::spawn(move || {
                loop {
                    if _panic_info.count.load(Ordering::Acquire) > 0 {
                        while _panic_info.count.fetch_sub(1, Ordering::AcqRel) > 1 {
                            ThreadPool::_push_worker(Arc::clone(&watchdog_inner));
                        }
                    }
                    thread::sleep(time::Duration::from_millis(300));
                    if watchdog_inner.job_count.load(Ordering::Acquire) == 0
                        && watchdog_inner.shutdown.load(Ordering::Acquire)
                    {
                        break;
                    }
                }
            })),
        };

        for _id in 0..size {
            Self::_push_worker(Arc::clone(&inner));
        }

        _self
    }

    fn _push_worker(inner: Arc<ThreadPoolInner>) {
        let worker_inner = Arc::clone(&inner);
        let mut workers = inner._workers.lock().unwrap();
        let _id: usize = workers.len();
        let __id: usize = workers.len();

        workers.push(Worker {
            _id,
            thread: Some(thread::spawn(move || {
                loop {
                    let r = worker_inner.job_recv.recv();
                    if let Ok(closure) = r {
                        closure.0();
                        worker_inner.finish_job();
                    } else {
                        break;
                    }
                }
            })),
        })
    }

    /// Execute a new job in the thread pool.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.inner.start_job();
        self.job_sender
            .as_ref()
            .unwrap()
            .send(Job(Box::new(f)))
            .unwrap();
    }

    /// Block the current thread until all jobs in the pool have been executed.
    ///
    /// NOTE: This method has nothing to do with `JoinHandle::join`.
    pub fn join(&self) {
        self.inner.wait_empty();
    }

    /// Returns true if there is a thread panicked
    pub fn panic(&self) -> bool {
        _panic_info.list.lock().unwrap().is_empty()
    }
}

impl Drop for ThreadPool {
    /// When dropped, all worker threads' `JoinHandle` must be `join`ed. If the thread panicked,
    /// then this function should panic too.
    fn drop(&mut self) {
        drop(self.job_sender.take().unwrap());
        self.join();
        self.inner.shutdown.store(true, Ordering::Release);
        while _panic_info.count.load(Ordering::Acquire) > 0 {
            thread::sleep(time::Duration::from_millis(300));
        }
        let panic_info = _panic_info.list.lock().unwrap();
        let panic_info = &*panic_info;
        if !panic_info.is_empty() {
            panic!(
                "{}",
                panic_info
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>()
                    .join("\n")
            );
        }
    }
}
