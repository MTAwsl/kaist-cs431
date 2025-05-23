//! Thread-safe key/value cache.

use std::collections::hash_map::{Entry, HashMap};
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

/// Cache that remembers the result for each key.
#[derive(Debug)]
pub struct Cache<K, V> {
    // todo! This is an example cache type. Build your own cache type that satisfies the
    // specification for `get_or_insert_with`.
    raw_map: RwLock<HashMap<K, V>>,
    lock_map: RwLock<HashMap<K, AtomicBool>>,
}

impl<K, V> Default for Cache<K, V> {
    fn default() -> Self {
        Self {
            raw_map: RwLock::new(HashMap::new()),
            lock_map: RwLock::new(HashMap::new()),
        }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> Cache<K, V> {
    /// Retrieve the value or insert a new one created by `f`.
    ///
    /// An invocation to this function should not block another invocation with a different key. For
    /// example, if a thread calls `get_or_insert_with(key1, f1)` and another thread calls
    /// `get_or_insert_with(key2, f2)` (`key1≠key2`, `key1,key2∉cache`) concurrently, `f1` and `f2`
    /// should run concurrently.
    ///
    /// On the other hand, since `f` may consume a lot of resource (= money), it's undesirable to
    /// duplicate the work. That is, `f` should be run only once for each key. Specifically, even
    /// for concurrent invocations of `get_or_insert_with(key, f)`, `f` is called only once per key.
    ///
    /// Hint: the [`Entry`] API may be useful in implementing this function.
    ///
    /// [`Entry`]: https://doc.rust-lang.org/stable/std/collections/hash_map/struct.HashMap.html#method.entry
    pub fn get_or_insert_with<F: FnOnce(K) -> V>(&self, key: K, f: F) -> V {
        if self.lock_map.read().unwrap().get(&key).is_none() {
            let mut m_wlock = self.lock_map.write().unwrap();
            if m_wlock.get(&key).is_none() {
                m_wlock.insert(key.clone(), AtomicBool::new(true));
                drop(m_wlock);

                let val = f(key.clone());
                self.raw_map
                    .write()
                    .unwrap()
                    .insert(key.clone(), val.clone());

                #[allow(clippy::readonly_write_lock)]
                let m_wlock = self.lock_map.write().unwrap();
                m_wlock.get(&key).unwrap().store(false, Ordering::Release);
                return val;
            }
        }

        loop {
            let m_lock = self.lock_map.read().unwrap();
            if !m_lock.get(&key).unwrap().load(Ordering::Acquire) {
                return self.raw_map.read().unwrap().get(&key).unwrap().clone();
            }
        }
    }

    /// Replace
    pub fn replace_with<F: FnOnce(K) -> V>(&mut self, key: K, f: F) -> V {
        let mut m_wlock = self.lock_map.write().unwrap();
        if let Some(lock) = m_wlock.get(&key) {
            lock.store(true, Ordering::Release);
            drop(m_wlock);

            let val = f(key.clone());
            self.raw_map
                .write()
                .unwrap()
                .insert(key.clone(), val.clone());

            let mut m_wlock = self.lock_map.write().unwrap();
            let lock = m_wlock.get(&key).unwrap();
            lock.store(false, Ordering::Release);
            drop(m_wlock);
            val
        } else {
            let mut m_wlock = self.lock_map.write().unwrap();
            if m_wlock.get(&key).is_none() {
                m_wlock.insert(key.clone(), AtomicBool::new(true));
                drop(m_wlock);

                let val = f(key.clone());
                self.raw_map
                    .write()
                    .unwrap()
                    .insert(key.clone(), val.clone());

                #[allow(clippy::readonly_write_lock)]
                let m_wlock = self.lock_map.write().unwrap();
                m_wlock.get(&key).unwrap().store(false, Ordering::Release);
                return val;
            }

            let lock = m_wlock.get(&key).unwrap();
            lock.store(true, Ordering::Release);
            drop(m_wlock);

            let val = f(key.clone());
            self.raw_map
                .write()
                .unwrap()
                .insert(key.clone(), val.clone());

            let mut m_wlock = self.lock_map.write().unwrap();
            let lock = m_wlock.get(&key).unwrap();
            lock.store(false, Ordering::Release);
            drop(m_wlock);
            val
        }
    }
}
