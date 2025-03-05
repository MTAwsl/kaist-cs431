use core::ptr::{self, NonNull};
#[cfg(not(feature = "check-loom"))]
use core::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering, fence};
use std::collections::HashSet;
use std::{fmt, mem};

#[cfg(feature = "check-loom")]
use loom::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering, fence};

use super::HAZARDS;

/// Represents the ownership of a hazard pointer slot.
pub struct Shield {
    slot: NonNull<HazardSlot>,
}

impl Shield {
    /// Creates a new shield for hazard pointer.
    pub fn new(hazards: &HazardBag) -> Self {
        let slot = hazards.acquire_slot().into();
        Self { slot }
    }

    /// Store `pointer` to the hazard slot.
    pub fn set<T>(&self, pointer: *mut T) {
        let r = unsafe { self.slot.as_ref() };
        r.hazard.store(pointer as *mut (), Ordering::Release);
        // r.active.store(true, Ordering::Release);
    }

    /// Clear the hazard slot.
    pub fn clear(&self) {
        self.set(ptr::null_mut::<()>())
    }

    /// Check if `src` still points to `pointer`. If not, returns the current value.
    ///
    /// For a pointer `p`, if "`src` still pointing to `pointer`" implies that `p` is not retired,
    /// then `Ok(())` means that shields set to `p` are validated.
    pub fn validate<T>(pointer: *mut T, src: &AtomicPtr<T>) -> Result<(), *mut T> {
        let ptr = src.load(Ordering::Acquire);
        if ptr == pointer { Ok(()) } else { Err(ptr) }
    }

    /// Try protecting `pointer` obtained from `src`. If not, returns the current value.
    ///
    /// If "`src` still pointing to `pointer`" implies that `pointer` is not retired, then `Ok(())`
    /// means that this shield is validated.
    pub fn try_protect<T>(&self, pointer: *mut T, src: &AtomicPtr<T>) -> Result<(), *mut T> {
        self.set(pointer);
        // println!("{self:?} {pointer:?}, {src:?}");
        Self::validate(pointer, src).inspect_err(|_| self.clear())
    }

    /// Get a protected pointer from `src`.
    ///
    /// See `try_protect()`.
    pub fn protect<T>(&self, src: &AtomicPtr<T>) -> *mut T {
        let mut pointer = src.load(Ordering::Relaxed);
        while let Err(new) = self.try_protect(pointer, src) {
            pointer = new;
            #[cfg(feature = "check-loom")]
            loom::sync::atomic::spin_loop_hint();
        }
        pointer
    }
}

impl Default for Shield {
    fn default() -> Self {
        Self::new(&HAZARDS)
    }
}

impl Drop for Shield {
    /// Clear and release the ownership of the hazard slot.
    fn drop(&mut self) {
        let r = unsafe { self.slot.as_ref() };
        r.hazard.store(ptr::null_mut(), Ordering::Relaxed);
        r.active.store(false, Ordering::Release);
    }
}

impl fmt::Debug for Shield {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shield")
            .field("slot address", &self.slot)
            .field("slot data", unsafe { self.slot.as_ref() })
            .finish()
    }
}

/// Global bag (multiset) of hazards pointers.
/// `HazardBag.head` and `HazardSlot.next` form a grow-only list of all hazard slots. Slots are
/// never removed from this list. Instead, it gets deactivated and recycled for other `Shield`s.
#[derive(Debug)]
pub struct HazardBag {
    head: AtomicPtr<HazardSlot>,
}

/// See `HazardBag`
#[derive(Debug)]
struct HazardSlot {
    // Whether this slot is occupied by a `Shield`.
    active: AtomicBool,
    // Machine representation of the hazard pointer.
    hazard: AtomicPtr<()>,
    // Immutable pointer to the next slot in the bag.
    next: *const HazardSlot,
}

impl HazardSlot {
    fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            hazard: AtomicPtr::new(ptr::null_mut()),
            next: ptr::null(),
        }
    }
}

impl HazardBag {
    #[cfg(not(feature = "check-loom"))]
    /// Creates a new global hazard set.
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[cfg(feature = "check-loom")]
    /// Creates a new global hazard set.
    pub fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Acquires a slot in the hazard set, either by recycling an inactive slot or allocating a new
    /// slot.
    fn acquire_slot(&self) -> &HazardSlot {
        if let Some(slot) = self.try_acquire_inactive() {
            return slot;
        }

        unsafe {
            let new_slot = Box::into_raw(Box::new(HazardSlot::new()));
            let new_ref = new_slot.as_mut().unwrap();
            new_ref.next = self.head.load(Ordering::Acquire);
            new_ref.active.store(true, Ordering::Release);

            loop {
                let result = self.head.compare_exchange(
                    new_ref.next as *mut _,
                    new_ref,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
                if let Err(e) = result {
                    new_ref.next = e;
                    fence(Ordering::Release);
                    continue;
                }
                return new_ref;
            }
        }
    }

    /// Find an inactive slot and activate it.
    fn try_acquire_inactive(&self) -> Option<&HazardSlot> {
        let mut slot: *const HazardSlot = self.head.load(Ordering::Acquire);
        unsafe {
            while !slot.is_null() {
                let r = slot.as_ref().unwrap();

                if r.active
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return Some(r);
                }

                slot = r.next;
            }

            None
        }
    }

    /// Returns all the hazards in the set.
    pub fn all_hazards(&self) -> HashSet<*mut ()> {
        let mut set = HashSet::<*mut ()>::new();

        let mut slot: *const _ = self.head.load(Ordering::Acquire);
        unsafe {
            while !slot.is_null() {
                let r = slot.as_ref().unwrap();

                if r.active.load(Ordering::Acquire) {
                    set.insert(r.hazard.load(Ordering::Relaxed));
                }

                slot = r.next;
            }
        }

        set
    }
}

impl Default for HazardBag {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for HazardBag {
    /// Frees all slots.
    fn drop(&mut self) {
        let mut slot: *const HazardSlot = self.head.swap(ptr::null_mut(), Ordering::Relaxed);
        unsafe {
            while !slot.is_null() {
                let r = Box::from_raw(slot as *mut HazardSlot);
                slot = r.next;
            }
        }
    }
}

unsafe impl Send for HazardSlot {}
unsafe impl Sync for HazardSlot {}

#[cfg(all(test, not(feature = "check-loom")))]
mod tests {
    use std::collections::HashSet;
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::AtomicPtr;
    use std::{mem, thread};

    use super::{HazardBag, Shield};

    const THREADS: usize = 8;
    const VALUES: Range<usize> = 1..1024;

    // `all_hazards` should return hazards protected by shield(s).
    #[test]
    fn all_hazards_protected() {
        let hazard_bag = Arc::new(HazardBag::new());
        (0..THREADS)
            .map(|_| {
                let hazard_bag = hazard_bag.clone();
                thread::spawn(move || {
                    for data in VALUES {
                        let src = AtomicPtr::new(data as *mut ());
                        let shield = Shield::new(&hazard_bag);
                        let _ = shield.protect(&src);
                        // leak the shield so that it is not unprotected.
                        mem::forget(shield);
                    }
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|th| th.join().unwrap());
        let all = hazard_bag.all_hazards();
        let values = VALUES.map(|data| data as *mut ()).collect();
        assert!(all.is_superset(&values))
    }

    // `all_hazards` should not return values that are no longer protected.
    #[test]
    fn all_hazards_unprotected() {
        let hazard_bag = Arc::new(HazardBag::new());
        (0..THREADS)
            .map(|_| {
                let hazard_bag = hazard_bag.clone();
                thread::spawn(move || {
                    for data in VALUES {
                        let src = AtomicPtr::new(data as *mut ());
                        let shield = Shield::new(&hazard_bag);
                        let _ = shield.protect(&src);
                    }
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|th| th.join().unwrap());
        let all = hazard_bag.all_hazards();
        let values = VALUES.map(|data| data as *mut ()).collect();
        let intersection: HashSet<_> = all.intersection(&values).collect();
        assert!(intersection.is_empty())
    }

    // `acquire_slot` should recycle existing slots.
    #[test]
    fn recycle_slots() {
        let hazard_bag = HazardBag::new();
        // allocate slots
        let shields = (0..1024)
            .map(|_| Shield::new(&hazard_bag))
            .collect::<Vec<_>>();
        // slot addresses
        let old_slots = shields
            .iter()
            .map(|s| s.slot.as_ptr() as usize)
            .collect::<HashSet<_>>();
        // release the slots
        drop(shields);

        let shields = (0..128)
            .map(|_| Shield::new(&hazard_bag))
            .collect::<Vec<_>>();
        let new_slots = shields
            .iter()
            .map(|s| s.slot.as_ptr() as usize)
            .collect::<HashSet<_>>();

        // no new slots should've been created
        assert!(new_slots.is_subset(&old_slots));
    }
}
