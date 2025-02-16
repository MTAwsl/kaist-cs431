use std::cmp::Ordering::*;
use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, MutexGuard};
use std::{mem, ptr};

use crate::ConcurrentSet;

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: Mutex<*mut Node<T>>,
}

/// Concurrent sorted singly linked list using fine-grained lock-coupling.
#[derive(Debug)]
pub struct FineGrainedListSet<T> {
    head: Mutex<*mut Node<T>>,
}

unsafe impl<T: Send> Send for FineGrainedListSet<T> {}
unsafe impl<T: Send> Sync for FineGrainedListSet<T> {}

/// Reference to the `next` field of previous node which points to the current node.
///
/// For example, given the following linked list:
///
/// ```text
/// head -> 1 -> 2 -> 3 -> null
/// ```
///
/// If `cursor` is currently at node 2, then `cursor.0` should be the `MutexGuard` obtained from the
/// `next` of node 1. In particular, `cursor.0.as_ref().unwrap()` creates a shared reference to node
/// 2.
struct Cursor<'l, T>(MutexGuard<'l, *mut Node<T>>);

impl<T> Node<T> {
    fn new(data: T, next: *mut Self) -> *mut Self {
        Box::into_raw(Box::new(Self {
            data,
            next: Mutex::new(next),
        }))
    }
}

impl<T: Ord> Cursor<'_, T> {
    /// Moves the cursor to the position of key in the sorted list.
    /// Returns whether the value was found.
    fn find(&mut self, key: &T) -> bool {
        while !self.0.is_null() {
            unsafe {
                let node = self.0.as_ref().unwrap();
                if &node.data == key {
                    return true;
                }
                if &node.data > key {
                    return false;
                }

                self.0 = node.next.lock().unwrap();
            }
        }
        false
    }
}

impl<T> FineGrainedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: Mutex::new(ptr::null_mut()),
        }
    }
}

impl<T: Ord> FineGrainedListSet<T> {
    fn find(&self, key: &T) -> (bool, Cursor<'_, T>) {
        let mut cursor = Cursor(self.head.lock().unwrap());
        if cursor.find(key) {
            (true, cursor)
        } else {
            (false, cursor)
        }
    }
}

impl<T: Ord> ConcurrentSet<T> for FineGrainedListSet<T> {
    fn contains(&self, key: &T) -> bool {
        self.find(key).0
    }

    fn insert(&self, key: T) -> bool {
        let cursor = self.find(&key);
        if cursor.0 {
            return false;
        }

        let mut prev = cursor.1.0;
        *prev = Node::new(key, *prev);
        true
    }

    fn remove(&self, key: &T) -> bool {
        let cursor = self.find(key);
        if !cursor.0 {
            return false;
        }

        let mut prev = cursor.1.0;
        let mut node = unsafe { Box::from_raw(*prev) };
        *prev = *node.next.lock().unwrap();
        drop(node);
        true
    }
}

#[derive(Debug)]
pub struct Iter<'l, T> {
    cursor: MutexGuard<'l, *mut Node<T>>,
}

impl<T> FineGrainedListSet<T> {
    /// An iterator visiting all elements.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            cursor: self.head.lock().unwrap(),
        }
    }
}

impl<'l, T> Iterator for Iter<'l, T> {
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        let guard = &mut self.cursor;

        if guard.is_null() {
            None
        } else {
            unsafe {
                let node = guard.as_ref().unwrap();
                let next = node.next.lock().unwrap();

                *guard = next;

                Some(&node.data)
            }
        }
    }
}

impl<T> Drop for FineGrainedListSet<T> {
    fn drop(&mut self) {
        let mut this = self.head.lock().unwrap();
        let mut next: MutexGuard<'_, *mut Node<T>>;

        while !this.is_null() {
            unsafe {
                let node = this.as_ref().unwrap();
                *this.deref_mut() = *node.next.lock().unwrap();
                drop(Box::from_raw(node as *const _ as *mut Node<T>));
            }
        }
    }
}

impl<T> Default for FineGrainedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
