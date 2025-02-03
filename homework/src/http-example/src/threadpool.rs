use std::thread::JoinHandle;
use std::collections::LinkedList;
use std::sync::{mpsc, Arc, Mutex};

type Job = Box<dyn FnOnce() -> () + Send>;

pub struct ThreadPool {
    threads: LinkedList<JoinHandle<()>>,
    sender: mpsc::Sender<Job>
}

impl ThreadPool {
    pub fn new(nums: u32) -> ThreadPool {
        let (sender, receiver) = mpsc::channel();
        let mut pool = ThreadPool {
            threads: LinkedList::new(),
            sender,
        };
        let receiver = Arc::new(Mutex::new(receiver));
        for _ in 0..nums {
            let cloned_receiver = Arc::clone(&receiver);
            pool.threads.push_back(
                std::thread::spawn(
                    move || {
                        loop {
                            let routine = cloned_receiver.lock().unwrap().recv();
                            if let Ok(routine) = routine {
                                routine();
                            }
                            else {
                                break;
                            }
                        }
                    }
               )
            );
        }
        pool
    }

    pub fn execute<F>(self: &Self, closure: F) ->() 
        where F: FnOnce() + Send + 'static
    {
        self.sender.send(Box::new(closure)).unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(self: &mut Self) {
        while !self.threads.is_empty() {
            self.threads.pop_front().unwrap().join().unwrap();
        }
    }
}
