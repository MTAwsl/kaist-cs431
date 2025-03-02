//! Split-ordered linked list.

use core::mem::{self, MaybeUninit};
use core::ptr;
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering::*;

use crossbeam_epoch::{Atomic, Guard, Owned};
use cs431::lockfree::list::{Cursor, List, Node};

use super::growable_array::GrowableArray;
use crate::ConcurrentMap;

/// Lock-free map from `usize` in range \[0, 2^63-1\] to `V`.
///
/// NOTE: We don't care about hashing in this homework for simplicity.
#[derive(Debug)]
pub struct SplitOrderedList<V> {
    /// Lock-free list sorted by recursive-split order.
    ///
    /// Use `MaybeUninit::uninit()` when creating sentinel nodes.
    list: List<usize, MaybeUninit<V>>,
    /// Array of pointers to the buckets.
    buckets: GrowableArray<Node<usize, MaybeUninit<V>>>,
    /// Number of buckets.
    size: AtomicUsize,
    /// Number of items.
    count: AtomicUsize,
}

impl<V> Default for SplitOrderedList<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> SplitOrderedList<V> {
    /// `size` is doubled when `count > size * LOAD_FACTOR`.
    const LOAD_FACTOR: usize = 2;

    /// Creates a new split ordered list.
    pub fn new() -> Self {
        let buckets: GrowableArray<Node<usize, MaybeUninit<V>>> = GrowableArray::new();
        let node = Node::new(0usize, MaybeUninit::<V>::uninit());
        let guard = crossbeam_epoch::pin();
        let list = List::new();
        let mut cursor = list.head(&guard);
        assert!(cursor.insert(Owned::new(node), &guard).is_ok());
        buckets.get(0, &guard).store(cursor.curr(), Relaxed);
        // println!("Inserted {:?}", buckets.get(0, &guard));
        // println!("List {:?}", list);

        Self {
            list,
            buckets,
            size: AtomicUsize::new(2),
            count: AtomicUsize::new(0),
        }
    }

    /// Creates a cursor and moves it to the bucket for the given index.  If the bucket doesn't
    /// exist, recursively initializes the buckets.
    fn lookup_bucket<'s>(
        &'s self,
        key: usize,
        guard: &'s Guard,
    ) -> Cursor<'s, usize, MaybeUninit<V>> {
        let index = key & (usize::MAX >> 1);
        let bucket_ptr_ref = self.buckets.get(index, guard);
        let bucket_ptr = bucket_ptr_ref.load(Acquire, guard);
        // println!("Trying index: {}", index);
        if !bucket_ptr.is_null() {
            // println!("Found Index: {}", index);
            return Cursor::new(bucket_ptr_ref, bucket_ptr);
        }

        // Eliminate left-most 1
        let mut parent_mask = key;
        let mut size_usize = std::mem::size_of::<usize>();
        for sz in (0..size_usize).rev() {
            // println!("Squashing {} = {}", sz, parent_mask);
            parent_mask |= parent_mask >> sz;
        }
        // println!("Squashed = {}", parent_mask);
        parent_mask ^= parent_mask >> 1;
        // println!("Left most 1 = {}", parent_mask);
        parent_mask -= 1;
        // println!("Parent_mask = {}", parent_mask);

        let mut prev_bkt = self.lookup_bucket(key & parent_mask, guard);

        let index = index.reverse_bits();
        let mut node = Owned::from(Node::new(index, MaybeUninit::uninit()));
        loop {
            let mut bkt = prev_bkt.clone();
            if let Ok(r) = bkt.find_harris_michael(&index, guard) {
                if r {
                    bucket_ptr_ref.store(bkt.curr(), Release);
                    return bkt;
                }

                if let Err(e) = bkt.insert(node, guard) {
                    node = e;
                } else {
                    // Safety: If insert successful, next bkt finding should always be successful
                    node = unsafe { Owned::from_raw(ptr::null_mut()) };
                }

                // println!("Bucket: Invalid cursor. Retry.")
            }
        }
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (bool, Cursor<'s, usize, MaybeUninit<V>>) {
        let size = self.size.load(Relaxed);

        let bkt_cursor = self.lookup_bucket(key & (size - 1), guard);
        // println!("Found bkt: {} {bkt_cursor:?}", key & (size-1));
        let key = key.reverse_bits() | 1;
        loop {
            let mut cur = bkt_cursor.clone();
            if let Ok(result) = cur.find_harris_michael(&key, guard) {
                return (result, cur);
            }
            // println!("Find: Invalid cursor. Retry.")
        }
    }

    fn assert_valid_key(key: usize) {
        assert!(key.leading_zeros() != 0);
    }
}

impl<V> ConcurrentMap<usize, V> for SplitOrderedList<V> {
    fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);
        // println!("Lookup {}",key);

        let (r, c) = self.find(key, guard);
        unsafe {
            if r {
                Some(c.lookup().assume_init_ref())
            } else {
                None
            }
        }
        // let key = key.reverse_bits() | 1;
        // if let Some(v) = self.list.harris_michael_lookup(&key, guard) {
        //     unsafe { Some(v.assume_init_ref()) }
        // }
        // else {
        //     None
        // }
    }

    fn insert(&self, key: usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(key);

        // println!("Insert {}", key);
        let size = self.size.load(Relaxed);
        let bkt_cursor = self.lookup_bucket(key & (size - 1), guard);
        let key = key.reverse_bits() | 1;
        let mut n = Owned::new(Node::new(key, MaybeUninit::new(value)));

        loop {
            // println!("Trying to insert: 0x{key:2X} Find: Result: {r} Cursor: {cur:?}");
            let mut cur = bkt_cursor.clone();
            let Ok(mut r) = cur.find_harris_michael(&key, guard) else {
                // println!("Insert: Invalid cursor. Retry.");
                continue;
            };
            if r {
                // println!("Insertion Failed. Key exist.");
                return unsafe { Err(n.into_box().into_value().assume_init()) };
            }
            if let Err(ret) = cur.insert(n, guard) {
                // println!("Insertion Failed. ret: {ret:?}, Retrying");
                n = ret;
                continue;
            }
            // println!("Inserted into {cur:?}");

            let count = self.count.fetch_add(1, Relaxed);
            if count + 1 > size * Self::LOAD_FACTOR {
                let _ = self
                    .size
                    .compare_exchange(size, size << 1, Relaxed, Relaxed);
                // println!("SIZE GROW: {}", size << 1);
            }

            return Ok(());
        }
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);

        // println!("Delete {}", key);
        let size = self.size.load(Relaxed);
        let bkt_cursor = self.lookup_bucket(key & (size - 1), guard);
        let key = key.reverse_bits() | 1;

        loop {
            let mut cur = bkt_cursor.clone();
            let Ok(mut r) = cur.find_harris_michael(&key, guard) else {
                // println!("Delete: Invalid cursor. Retry.");
                continue;
            };
            if !r {
                return Err(());
            }
            if let Ok(v) = cur.delete(guard) {
                self.count.fetch_sub(1, Relaxed);
                return unsafe { Ok(v.assume_init_ref()) };
            }
            // println!("Delete failed {}", key);
        }
    }
}
