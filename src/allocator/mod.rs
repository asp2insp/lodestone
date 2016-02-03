use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::ops::{Index, IndexMut};
use std::collections::LinkedList;
use std::ptr;

pub use self::page::*;

pub mod page;
pub mod tests;


/// A pool represents a fixed number of ref-counted objects.
/// The pool treats all given space as an unallocated
/// pool of objects. Each object is prefixed with a header.
/// The header is formatted as follows:
/// * V1
///   - [0..2] ref_count: u16
///
pub struct Pool {
    buffer: *mut u8,
    buffer_size: usize,
    capacity: usize,

    tail: AtomicUsize, // One past the end index

    // Cached values
    slot_size: usize,
    header_size: usize,

    free_list: LinkedList<PageIndex>,
}

struct SlotHeader {
    ref_count: AtomicUsize,
}

/// Public interface
impl Pool {
    pub fn new(buf: &mut [u8]) -> Pool {
        let ptr: *mut u8 = buf.as_mut_ptr();
        let header_size = mem::size_of::<SlotHeader>();
        let slot_size = mem::size_of::<Page>() + header_size;
        Pool {
            buffer: ptr,
            buffer_size: buf.len(),
            tail: AtomicUsize::new(0),
            slot_size: slot_size,
            capacity: buf.len() / slot_size,
            header_size: header_size,
            free_list: LinkedList::new(),
        }
    }

    /// Remove all objects from the pool
    /// and zero the memory
    pub unsafe fn clear(&mut self) {
        let mut i = self.buffer.clone();
        let end = self.buffer.clone().offset(self.buffer_size as isize);
        while i != end {
            *i = 0u8;
            i = i.offset(1);
        }
    }

    /// Fast copy a slot's contents to a new slot and return
    /// a pointer to the new slot
    pub fn alloc_with_contents_of(&mut self, other: PageIndex) -> Result<PageIndex, &'static str> {
        let index = try!(self.claim_free_index());
        unsafe {
            let from = self.raw_contents_for(other);
            let to = self.raw_contents_for(index);
            ptr::copy(from, to, mem::size_of::<Page>());
        }
        Ok(index)
    }

    /// Try to allocate a new item from the pool.
    /// A mutable reference to the item is returned on success
    pub fn alloc(&mut self) -> Result<PageIndex, &'static str> {
        let index = try!(self.internal_alloc());
        Ok(index)
    }

    // Increase the ref count for the cell at the given index
    pub fn retain(&mut self, index: PageIndex) {
        let h = self.header_for(index);
        loop {
            let old = h.ref_count.load(Ordering::Relaxed);
            let swap = h.ref_count
            .compare_and_swap(old, old+1, Ordering::Relaxed);
            if swap == old {
                break
            }
        }
    }

    // Decrease the ref count for the cell at the given index
    pub fn release(&mut self, index: PageIndex) {
        let mut is_free = false;
        { // Make the borrow checker happy
            let h = self.header_for(index);
            loop {
                let old = h.ref_count.load(Ordering::Relaxed);
                assert!(old > 0, "Release called on [{}] which has no refs!", index);

                let swap = h.ref_count
                .compare_and_swap(old, old-1, Ordering::Relaxed);
                if swap == old {
                    if old == 1 { // this was the last reference
                        is_free = true;
                    }
                    break
                }
            }
        }
        if is_free {
            self.free_list.push_back(index);
        }
    }

    /// Returns the number of live items. O(1) running time.
    pub fn live_count(&self) -> usize {
        self.tail.load(Ordering::Relaxed) - self.free_list.len()
    }
}


/// Internal Functions
impl  Pool {
    // Returns an item from the free list, or
    // tries to allocate a new one from the buffer
    fn claim_free_index(&mut self) -> Result<PageIndex, &'static str> {
        let index = match self.free_list.pop_front() {
            Some(i) => i,
            None => try!(self.push_back_alloc()),
        };
        self.retain(index);
        Ok(index)
    }

    // Internal alloc that does not create an Arc but still claims a slot
    fn internal_alloc(&mut self) -> Result<PageIndex, &'static str> {
        let index = try!(self.claim_free_index());
        Ok(index)
    }

    // Pushes the end of the used space in the buffer back
    // returns the previous index
    fn push_back_alloc(&mut self) -> Result<PageIndex, &'static str> {
        loop {
            let old_tail = self.tail.load(Ordering::Relaxed);
            let swap = self.tail.compare_and_swap(old_tail, old_tail+1, Ordering::Relaxed);
            // If we were the ones to claim this slot, or
            // we've overrun the buffer, return
            if old_tail >= self.capacity {
                return Err("OOM")
            } else if swap == old_tail {
                return Ok(old_tail)
            }
        }
    }

    fn header_for<'a>(&'a mut self, i: PageIndex) -> &'a mut SlotHeader {
        unsafe {
            let ptr = self.buffer.clone()
                .offset((i * self.slot_size) as isize);
            mem::transmute(ptr)
        }
    }

    fn raw_contents_for<'a>(&'a mut self, i: PageIndex) -> *mut u8 {
        unsafe {
            self.buffer.clone()
                .offset((i * self.slot_size) as isize)
                .offset(self.header_size as isize)
        }
    }
}

impl Index<PageIndex> for Pool {
    type Output = Page;

    fn index<'a>(&'a self, i: usize) -> &'a Page {
        unsafe {
            let ptr = self.buffer.clone()
                .offset((i * self.slot_size) as isize)
                .offset(self.header_size as isize);
            mem::transmute(ptr)
        }
    }
}

impl IndexMut<PageIndex> for Pool {
    fn index_mut<'a>(&'a mut self, i: usize) -> &'a mut Page {
        unsafe {
            let ptr = self.buffer.clone()
                .offset((i * self.slot_size) as isize)
                .offset(self.header_size as isize);
            mem::transmute(ptr)
        }
    }
}
