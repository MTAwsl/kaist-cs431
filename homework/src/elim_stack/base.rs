use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::ops::Deref;
use std::time;

use crossbeam_epoch::{Atomic, Guard, Owned, pin};
use rand::{Rng, thread_rng};

pub(crate) const ELIM_SIZE: usize = 16;
pub(crate) const ELIM_DELAY: time::Duration = time::Duration::from_millis(10);
pub(crate) const IDLE: usize = 0;
pub(crate) const PUSH_PENDING: usize = 1;
pub(crate) const POP_PENDING: usize = 2;

#[inline]
pub(crate) fn get_random_elim_index() -> usize {
    thread_rng().r#gen::<usize>() % ELIM_SIZE
}

/// Concurrent stack types.
pub trait Stack<T>: Default {
    /// Push request type.
    type PushReq: From<T> + Deref<Target = ManuallyDrop<T>>;

    /// Tries to push a value to the stack.
    ///
    /// Returns `Ok(())` if the push request is served; `Err(req)` is CAS failed.
    fn try_push(
        &self,
        req: Owned<Self::PushReq>,
        guard: &Guard,
    ) -> Result<(), Owned<Self::PushReq>>;

    /// Tries to pop a value from the stack.
    ///
    /// Returns `Ok(Some(v))` if `v` is popped; `Ok(None)` if the stack is empty; and `Err(())` if
    /// CAS failed.
    fn try_pop(&self, guard: &Guard) -> Result<Option<T>, ()>;

    /// Returns `true` if the stack is empty.
    fn is_empty(&self, guard: &Guard) -> bool;

    /// Pushes a value to the stack.
    fn push(&self, t: T) {
        let mut req = Owned::new(t.into());
        let guard = pin();
        while let Err(r) = self.try_push(req, &guard) {
            req = r;
        }
    }

    /// Pops a value from the stack.
    ///
    /// Returns `Some(v)` if `v` is popped; `None` if the stack is empty.
    fn pop(&self) -> Option<T> {
        let guard = pin();
        loop {
            if let Ok(result) = self.try_pop(&guard) {
                return result;
            }
        }
    }
}

/// Elimination backoff stack
#[derive(Debug)]
pub struct ElimStack<T, S: Stack<T>> {
    pub(crate) inner: S,
    // slot tags:
    // - 0: no request
    // - 1: push request
    // - 2: pop request
    // - 3: request acknowledged
    pub(crate) slots: [Atomic<S::PushReq>; ELIM_SIZE],
}

impl<T, S: Stack<T>> Default for ElimStack<T, S> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            slots: Default::default(),
        }
    }
}
