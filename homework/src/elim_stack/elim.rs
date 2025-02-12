use core::ops::Deref;
use core::sync::atomic::Ordering;
use core::{mem, ptr};
use std::mem::ManuallyDrop;
use std::thread;

use crossbeam_epoch::{Atomic, Guard, Owned, Shared};

use super::base::*;

impl<T, S: Stack<T>> Stack<T> for ElimStack<T, S> {
    type PushReq = S::PushReq;

    fn try_push(
        &self,
        req: Owned<Self::PushReq>,
        guard: &Guard,
    ) -> Result<(), Owned<Self::PushReq>> {
        let Err(req) = self.inner.try_push(req, guard) else {
            return Ok(());
        };

        let index = get_random_elim_index();
        let slot_ref = unsafe { self.slots.get_unchecked(index) };
        let req = req.into_shared(guard);

        let Ok(req) = slot_ref.compare_exchange(
            Shared::null(),
            req,
            Ordering::Relaxed,
            Ordering::Relaxed,
            guard,
        ) else {
            // Current slot occupied, Retry and return.
            let Err(req) = self
                .inner
                .try_push(unsafe { req.try_into_owned().unwrap() }, guard)
            else {
                return Ok(());
            };
            return Err(req);
        };

        thread::sleep(ELIM_DELAY);

        // Check Collision
        if slot_ref
            .compare_exchange(
                req,
                Shared::null(),
                Ordering::Relaxed,
                Ordering::Relaxed,
                guard,
            )
            .is_err()
        {
            // Collision
            return Ok(());
        };

        // Retry
        let Err(req) = self
            .inner
            .try_push(unsafe { req.try_into_owned().unwrap() }, guard)
        else {
            return Ok(());
        };

        Err(req)
    }

    fn try_pop(&self, guard: &Guard) -> Result<Option<T>, ()> {
        if let Ok(result) = self.inner.try_pop(guard) {
            return Ok(result);
        }

        let index = get_random_elim_index();
        let slot_ref = unsafe { self.slots.get_unchecked(index) };
        let mut slot = slot_ref.load(Ordering::Relaxed, guard);

        if slot.is_null() {
            thread::sleep(ELIM_DELAY);

            // Try again
            slot = slot_ref.load(Ordering::Relaxed, guard);

            if slot.is_null() {
                // Still idle.
                if let Ok(result) = self.inner.try_pop(guard) {
                    return Ok(result);
                }
                return Err(());
            }
        }

        if slot_ref
            .compare_exchange(
                slot,
                Shared::null(),
                Ordering::Relaxed,
                Ordering::Relaxed,
                guard,
            )
            .is_ok()
        {
            // Exchanged.
            let data: T = unsafe { ManuallyDrop::into_inner(ptr::read(slot.deref().deref())) };
            return Ok(Some(data));
        }

        // Retry
        if let Ok(result) = self.inner.try_pop(guard) {
            return Ok(result);
        }
        Err(())
    }

    fn is_empty(&self, guard: &Guard) -> bool {
        self.inner.is_empty(guard)
    }
}
