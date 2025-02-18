use std::cmp::Ordering::*;
use std::mem::{self, ManuallyDrop};
use std::ops::Deref;
use std::sync::atomic::Ordering::*;
use std::sync::atomic::fence;

use crossbeam_epoch::{Atomic, Guard, Owned, Shared, pin};
use cs431::lock::seqlock::{ReadGuard, SeqLock, WriteGuard};

use crate::ConcurrentSet;

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: SeqLock<Atomic<Node<T>>>,
}

/// Concurrent sorted singly linked list using fine-grained optimistic locking.
#[derive(Debug)]
pub struct OptimisticFineGrainedListSet<T> {
    head: SeqLock<Atomic<Node<T>>>,
}

unsafe impl<T: Send> Send for OptimisticFineGrainedListSet<T> {}
unsafe impl<T: Send> Sync for OptimisticFineGrainedListSet<T> {}

#[derive(Debug)]
struct Cursor<'g, T> {
    // Reference to the `next` field of previous node which points to the current node.
    prev: ReadGuard<'g, Atomic<Node<T>>>,
    curr: Shared<'g, Node<T>>,
}

impl<T> Node<T> {
    fn new(data: T, next: Shared<'_, Self>) -> Owned<Self> {
        Owned::new(Self {
            data,
            next: SeqLock::new(next.into()),
        })
    }
}

impl<'g, T: Ord> Cursor<'g, T> {
    /// Moves the cursor to the position of key in the sorted list.
    /// Returns whether the value was found.
    ///
    /// Return `Err(())` if the cursor cannot move.
    fn find(&mut self, key: &T, guard: &'g Guard) -> Result<bool, ()> {
        while !self.curr.is_null() {
            unsafe {
                let node = self.curr.as_ref().unwrap();
                if &node.data == key {
                    if !self.prev.validate() {
                        self.prev.restart();
                        self.curr = self.prev.load(Acquire, guard);
                        if self.curr.is_null() {
                            return Err(());
                        }
                        continue;
                    }
                    return Ok(true);
                }
                if &node.data > key {
                    if !self.prev.validate() {
                        self.prev.restart();
                        self.curr = self.prev.load(Acquire, guard);
                        if self.curr.is_null() {
                            return Err(());
                        }
                        continue;
                    }
                    return Ok(false);
                }

                if !self.prev.validate() {
                    self.prev.restart();
                    self.curr = self.prev.load(Acquire, guard);
                    if self.curr.is_null() {
                        return Err(());
                    }
                    continue;
                }
                let mut prev: ReadGuard<'_, Atomic<Node<T>>> = node.next.read_lock();
                mem::swap(&mut prev, &mut self.prev);
                fence(Release);
                self.curr = self.prev.load(Acquire, guard);
                prev.finish();
            }
        }
        if self.prev.validate() {
            return Ok(false);
        }
        Err(())
    }
}

impl<T> OptimisticFineGrainedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: SeqLock::new(Atomic::null()),
        }
    }

    fn head<'g>(&'g self, guard: &'g Guard) -> Cursor<'g, T> {
        let prev = unsafe { self.head.read_lock() };
        let curr = prev.load(Acquire, guard);
        Cursor { prev, curr }
    }
}

impl<T: Ord> OptimisticFineGrainedListSet<T> {
    fn find<'g>(&'g self, key: &T, guard: &'g Guard) -> Result<(bool, Cursor<'g, T>), ()> {
        let mut cursor = self.head(guard);
        if let Ok(res) = cursor.find(key, guard) {
            if cursor.prev.validate() {
                Ok((res, cursor))
            } else {
                cursor.prev.finish();
                Err(())
            }
        } else {
            cursor.prev.finish();
            Err(())
        }
    }
}

impl<T: Ord> ConcurrentSet<T> for OptimisticFineGrainedListSet<T> {
    fn contains(&self, key: &T) -> bool {
        loop {
            let guard = pin();
            if let Ok(res) = self.find(key, &guard) {
                if res.1.prev.validate() {
                    res.1.prev.finish();
                    return res.0;
                }
                res.1.prev.finish();
            }
        }
    }

    fn insert(&self, key: T) -> bool {
        let guard = pin();
        loop {
            let mut cursor = self.find(&key, &guard);

            if cursor.is_err() {
                continue;
            }

            let cursor = cursor.unwrap();
            if cursor.0 {
                cursor.1.prev.finish();
                return false;
            }

            let handle = cursor.1.prev.upgrade();
            if handle.is_err() {
                continue;
            }

            let handle = handle.unwrap();
            handle.store(Node::new(key, cursor.1.curr), Release);

            return true;
        }
    }

    fn remove(&self, key: &T) -> bool {
        let guard = pin();
        loop {
            let mut cursor = self.find(key, &guard);

            if cursor.is_err() {
                continue;
            }

            let cursor = cursor.unwrap();
            if !cursor.0 {
                cursor.1.prev.finish();
                return false;
            }

            let handle = cursor.1.prev.upgrade();
            if handle.is_err() {
                continue;
            }

            let curr_handle = unsafe { cursor.1.curr.deref().next.write_lock() };

            let handle = handle.unwrap();
            let next = curr_handle.swap(Shared::null(), Relaxed, &guard);
            handle.store(next, Release);

            return true;
        }
    }
}

#[derive(Debug)]
pub struct Iter<'g, T> {
    // Can be dropped without validation, because the only way to use cursor.curr is next().
    cursor: ManuallyDrop<Cursor<'g, T>>,
    guard: &'g Guard,
}

impl<T> OptimisticFineGrainedListSet<T> {
    /// An iterator visiting all elements. `next()` returns `Some(Err(()))` when validation fails.
    /// In that case, the user must restart the iteration.
    pub fn iter<'g>(&'g self, guard: &'g Guard) -> Iter<'g, T> {
        Iter {
            cursor: ManuallyDrop::new(self.head(guard)),
            guard,
        }
    }
}

impl<'g, T> Iterator for Iter<'g, T> {
    type Item = Result<&'g T, ()>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            if !self.cursor.prev.validate() {
                return Some(Err(()));
            }

            if self.cursor.curr.is_null() {
                return None;
            }

            let node = self.cursor.curr.deref();
            let mut prev = node.next.read_lock();
            let data = &node.data;
            mem::swap(&mut prev, &mut self.cursor.prev);
            fence(Release);
            self.cursor.curr = self.cursor.prev.load(Acquire, self.guard);

            if !prev.validate() {
                mem::swap(&mut self.cursor.prev, &mut prev);
                fence(Release);
                self.cursor.curr = self.cursor.prev.load(Acquire, self.guard);
                prev.finish();
                return Some(Err(()));
            }

            prev.finish();
            Some(Ok(data))
        }
    }
}

impl<T> Drop for OptimisticFineGrainedListSet<T> {
    fn drop(&mut self) {
        let guard = pin();
        let mut this: WriteGuard<'_, Atomic<Node<T>>> = self.head.write_lock();

        let mut ptr = this.deref().load(Acquire, &guard);
        while !ptr.is_null() {
            drop(this);
            this = unsafe { ptr.deref().next.write_lock() };
            ptr = this.deref().load(Acquire, &guard);
        }
    }
}

impl<T> Default for OptimisticFineGrainedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
