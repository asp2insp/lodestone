use std::{mem, fmt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::ops::{Index, IndexMut};
use std::marker::PhantomData;
use std::collections::LinkedList;
use std::ops::{Deref, DerefMut};
use std::ptr;


pub mod tests;

static POOLS: Vec<(usize, Pool)> = vec![];

/// Each page is 4096 bytes
pub type Page = [u8; 0x1000];

/// Arc is the only valid way to access an item in
/// the pool. It is returned by alloc, and will automatically
/// release/retain when dropped/cloned. It implements Deref/DerefMut,
/// so all accesses can go through it.
/// WARNING! Taking the address of the dereferenced value constitutes
/// undefined behavior. So, given a: Arc, &*a is not allowed
#[repr(C)]
pub struct Arc {
    pool_id: usize,
    index: usize,
}

/// Public functions
impl Arc {
    /// If you want to manually manage the memory or
    /// use the wrapped reference outside of the Arc system
    /// the retain/release functions provide an escape hatch.
    /// Retain will increment the reference count
    pub unsafe fn retain(&mut self) {
        self.get_pool().retain(self.index);
    }

    /// If you want to manually manage the memory or
    /// use the wrapped reference outside of the Arc system
    /// the retain/release functions provide an escape hatch.
    /// Release will decrement the reference count
    pub unsafe fn release(&mut self) {
        self.get_pool().release(self.index);
    }
}

/// Internal functions
impl Arc {

    /// It's somewhat confusing that Arc::new()
    /// does not take care of bumping the ref count.
    /// However, the atomic op for claiming a free slot
    /// needs to happen before the new() takes place
    fn new(index: usize, p: &Pool) -> Arc {
        Arc {
            item_type: PhantomData,

            pool_id: p.id,
            index: index,
        }
    }

    fn get_pool(&self) -> &mut Pool {
        let raw_ptr = POOLS.iter().find(|entry| {
            entry.0 == self.pool_id
        })
        .unwrap()
        .1;
        unsafe {
            let pool: *mut Pool = mem::transmute(raw_ptr);
            &mut *pool
        }
    }

    fn ref_count(&self) -> usize {
        self.get_pool().header_for(self.index).ref_count.load(Ordering::Relaxed)
    }
}

impl Drop for Arc {
    fn drop(&mut self) {
        self.get_pool().release(self.index);
    }
}

impl Clone for Arc {
    fn clone(&self) -> Self {
        self.get_pool().retain(self.index);
        Arc {
            item_type: self.item_type,

            pool_id: self.pool_id,
            index: self.index,
        }
    }
}

impl Deref for Arc {
    type Target = Page;

    fn deref<'b>(&'b self) -> &'b Page {
        &self.get_pool()[self.index]
    }
}

impl DerefMut for Arc {
    fn deref_mut<'b>(&'b mut self) -> &'b mut Page {
        &mut self.get_pool()[self.index]
    }
}

impl  fmt::Debug for Arc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Arc{{ offset: {:?}, ref_count: {:?} }}", self.index, self.ref_count())
    }
}

impl  PartialEq for Arc {
    fn eq(&self, other: &Arc) -> bool {
        self.index == other.index && self.pool_id == other.pool_id
    }
}

/// A pool represents a fixed number of ref-counted objects.
/// The pool treats all given space as an unallocated
/// pool of objects. Each object is prefixed with a header.
/// The header is formatted as follows:
/// * V1
///   - [0..2] ref_count: u16
///
pub struct Pool {
    id: usize,

    buffer: *mut u8,
    buffer_size: usize,
    capacity: usize,

    tail: AtomicUsize, // One past the end index

    // Cached values
    slot_size: usize,
    header_size: usize,

    free_list: LinkedList<usize>,
}

struct SlotHeader {
    ref_count: AtomicUsize,
}

/// Public interface
impl  Pool {
    pub fn new(mem: &mut [u8], id: usize) -> Pool {
        let ptr: *mut u8 = mem.as_mut_ptr();
        let header_size = mem::size_of::<SlotHeader>();
        let slot_size = mem::size_of::<Page>() + header_size;
        Pool {
            item_type: PhantomData,
            id: id,
            buffer: ptr,
            buffer_size: mem.len(),
            tail: AtomicUsize::new(0),
            slot_size: slot_size,
            capacity: mem.len() / slot_size,
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
    pub fn alloc_with_contents_of(&mut self, other: &Arc) -> Result<Arc, &'static str> {
        let index = try!(self.claim_free_index());
        unsafe {
            let from = self.raw_contents_for(other.index);
            let to = self.raw_contents_for(index);
            ptr::copy(from, to, mem::size_of::<Page>());
        }
        Ok(Arc::new(index, self))
    }

    /// Try to allocate a new item from the pool.
    /// A mutable reference to the item is returned on success
    pub fn alloc(&mut self) -> Result<Arc, &'static str> {
        let index = try!(self.internal_alloc());
        Ok(Arc::new(index, self))
    }

    // Increase the ref count for the cell at the given index
    pub fn retain(&mut self, index: usize) {
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
    pub fn release(&mut self, index: usize) {
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
    fn claim_free_index(&mut self) -> Result<usize, &'static str> {
        let index = match self.free_list.pop_front() {
            Some(i) => i,
            None => try!(self.push_back_alloc()),
        };
        self.retain(index);
        Ok(index)
    }

    // Internal alloc that does not create an Arc but still claims a slot
    fn internal_alloc(&mut self) -> Result<usize, &'static str> {
        let index = try!(self.claim_free_index());
        Ok(index)
    }

    // Pushes the end of the used space in the buffer back
    // returns the previous index
    fn push_back_alloc(&mut self) -> Result<usize, &'static str> {
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

    fn header_for<'a>(&'a mut self, i: usize) -> &'a mut SlotHeader {
        unsafe {
            let ptr = self.buffer.clone()
                .offset((i * self.slot_size) as isize);
            mem::transmute(ptr)
        }
    }

    fn raw_contents_for<'a>(&'a mut self, i: usize) -> *mut u8 {
        unsafe {
            self.buffer.clone()
                .offset((i * self.slot_size) as isize)
                .offset(self.header_size as isize)
        }
    }
}

impl  Index<usize> for Pool {
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

impl  IndexMut<usize> for Pool {
    fn index_mut<'a>(&'a mut self, i: usize) -> &'a mut Page {
        unsafe {
            let ptr = self.buffer.clone()
                .offset((i * self.slot_size) as isize)
                .offset(self.header_size as isize);
            mem::transmute(ptr)
        }
    }
}
