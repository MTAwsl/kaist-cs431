//! Growable array.

use core::fmt::Debug;
use core::mem::{self, ManuallyDrop};
use core::sync::atomic::Ordering::*;
use core::sync::atomic::fence;

use crossbeam_epoch::{Atomic, Guard, Owned, Shared};

/// Growable array of `Atomic<T>`.
///
/// This is more complete version of the dynamic sized array from the paper. In the paper, the
/// segment table is an array of arrays (segments) of pointers to the elements. In this
/// implementation, a segment contains the pointers to the elements **or other child segments**. In
/// other words, it is a tree that has segments as internal nodes.
///
/// # Example run
///
/// Suppose `SEGMENT_LOGSIZE = 3` (segment size 8).
///
/// When a new `GrowableArray` is created, `root` is initialized with `Atomic::null()`.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
/// ```
///
/// When you store element `cat` at the index `0b001`, it first initializes a segment.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 1
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                           |
///                                           v
///                                         +---+
///                                         |cat|
///                                         +---+
/// ```
///
/// When you store `fox` at `0b111011`, it is clear that there is no room for indices larger than
/// `0b111`. So it first allocates another segment for upper 3 bits and moves the previous root
/// segment (`0b000XXX` segment) under the `0b000XXX` branch of the the newly allocated segment.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                               |
///                                               v
///                                      +---+---+---+---+---+---+---+---+
///                                      |111|110|101|100|011|010|001|000|
///                                      +---+---+---+---+---+---+---+---+
///                                                                |
///                                                                v
///                                                              +---+
///                                                              |cat|
///                                                              +---+
/// ```
///
/// And then, it allocates another segment for `0b111XXX` indices.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                                            |
///                   v                                            v
///                 +---+                                        +---+
///                 |fox|                                        |cat|
///                 +---+                                        +---+
/// ```
///
/// Finally, when you store `owl` at `0b000110`, it traverses through the `0b000XXX` branch of the
/// height 2 segment and arrives at its `0b110` leaf.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                        |                   |
///                   v                        v                   v
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// When the array is dropped, only the segments are dropped and the **elements must not be
/// dropped/deallocated**.
///
/// ```text
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// Instead, it should be handled by the container that the elements actually belong to. For
/// example, in `SplitOrderedList` the destruction of elements are handled by the inner `List`.
#[derive(Debug)]
pub struct GrowableArray<T> {
    root: Atomic<Segment<T>>,
    // test_arr: Vec<Atomic<T>>,
}

const SEGMENT_LOGSIZE: usize = 10;

/// A fixed size array of atomic pointers to other `Segment<T>` or `T`.
///
/// Each segment is either a child segment with pointers to `Segment<T>` or an element segment with
/// pointers to `T`. This is determined by the height of this segment in the main array, which one
/// needs to track separately. For example, use the main array root's tag.
///
/// Since destructing segments requires its height information, it is not recommended to
/// implement [`Drop`]. Rather, implement and use the custom [`Segment::deallocate`] method that
/// accounts for the height of the segment.
union Segment<T> {
    children: ManuallyDrop<[Atomic<Segment<T>>; 1 << SEGMENT_LOGSIZE]>,
    elements: ManuallyDrop<[Atomic<T>; 1 << SEGMENT_LOGSIZE]>,
}

impl<T> Segment<T> {
    /// Create a new segment filled with null pointers. It is up to the callee to whether to use
    /// this as a children or an element segment.
    fn new() -> Owned<Self> {
        Owned::new(
            // SAFETY: An array of null pointers can be interperted as either an element segment or
            // a children segment.
            unsafe { mem::zeroed() },
        )
    }

    /// Deallocates a segment of `height`.
    ///
    /// # Safety
    ///
    /// `self` must actually have height `height`.
    unsafe fn deallocate(self, height: usize) {
        unsafe {
            let guard = &crossbeam_epoch::pin();
            if height > 0 {
                for i in 0..self.children.len() {
                    if self.children[i].load(Relaxed, guard).is_null() {
                        continue;
                    }
                    self.children[i]
                        .clone()
                        .into_owned()
                        .into_box()
                        .deallocate(height - 1);
                }
            }
        }
    }
}

impl<T> Debug for Segment<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Segment")
    }
}

impl<T> Drop for GrowableArray<T> {
    /// Deallocate segments, but not the individual elements.
    fn drop(&mut self) {
        unsafe {
            self.root
                .clone()
                .into_owned()
                .into_box()
                .deallocate(self.root.load(Relaxed, &crossbeam_epoch::pin()).tag());
        }
    }
}

impl<T> Default for GrowableArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GrowableArray<T> {
    /// Create a new growable array.
    pub fn new() -> Self {
        Self {
            root: Atomic::from(Segment::<T>::new().with_tag(0)),
            // test_arr: vec![Atomic::null(); 100000],
        }
    }

    /// Returns the reference to the `Atomic` pointer at `index`. Allocates new segments if
    /// necessary.
    pub fn get<'g>(&self, mut index: usize, guard: &'g Guard) -> &'g Atomic<T> {
        let mut mask = (1 << SEGMENT_LOGSIZE) - 1;
        let mut height = 0;
        // println!("Mask init: 0x{mask:2x}");
        while index & mask != index {
            height += 1;
            mask = mask << SEGMENT_LOGSIZE | mask;
            // println!("Mask padded: 0x{mask:2x}");
        }

        // println!("Getting: 0x{index:2x}, height: {height}");

        if height > (std::mem::size_of::<usize>() << 3) / SEGMENT_LOGSIZE {
            panic!(
                "growable_array::get : Index overflow. {height} > {}. Idx: 0x{:02x}",
                (std::mem::size_of::<usize>() << 3) / SEGMENT_LOGSIZE,
                index
            );
        }

        // Create segments bottom-up
        let mask =
            ((1usize << SEGMENT_LOGSIZE) - 1).wrapping_shl((SEGMENT_LOGSIZE * height) as u32);
        fence(Acquire);
        for layer in (0..height).rev() {
            // println!("Layer: {layer}");
            let ptr = self.root.load(Relaxed, guard);
            if ptr.tag() >= height {
                break;
            }
            let mut new_segment = Segment::<T>::new().with_tag(ptr.tag() + 1);
            unsafe { new_segment.children[0] = Atomic::from(ptr) };
            let _ = self
                .root
                .compare_exchange(ptr, new_segment, AcqRel, Acquire, guard);
        }

        // Locate element top-down
        let mut atm_ptr = &self.root;
        let mask = (1 << SEGMENT_LOGSIZE) - 1;
        let mut ptr = atm_ptr.load(Acquire, guard);
        unsafe {
            for layer in (0..=ptr.tag()).rev() {
                let offset = (index >> (SEGMENT_LOGSIZE * layer)) & mask;
                // println!("Index: 0x{:2X}, Offset: 0x{:2X}, Mask: 0x{:2X}", index, offset, mask);
                if !ptr.is_null() {
                    if layer == 0 {
                        return &ptr.as_ref().unwrap().elements[offset];
                    }
                    // println!("Layer: {layer}, Prev Ptr: {atm_ptr:?}");
                    atm_ptr = &ptr.as_ref().unwrap().children[offset];
                    // println!("Layer: {layer}, Goto Ptr: {atm_ptr:?}");
                } else {
                    // println!("Layer: {layer}, Prev Ptr: {atm_ptr:?}");
                    let new_segment = Segment::<T>::new().with_tag(layer);
                    let res = atm_ptr.compare_exchange(ptr, new_segment, AcqRel, Acquire, guard);
                    // println!("Layer: {layer}, Allocated Ptr: {atm_ptr:?}");
                    if layer == 0 {
                        return &atm_ptr.load(Relaxed, guard).as_ref().unwrap().elements[offset];
                    }
                    atm_ptr = &atm_ptr.load(Relaxed, guard).as_ref().unwrap().children[offset];
                    // println!("Layer: {layer}, Goto Ptr: {atm_ptr:?}");
                }
                ptr = atm_ptr.load(Relaxed, guard);
            }
        }

        panic!("growablearray_get: possible overflow of layers.");
    }
}
