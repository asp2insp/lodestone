use std::{mem, slice, fmt};
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::ops::{Index, IndexMut};
use std::marker::PhantomData;
use std::collections::LinkedList;
use std::ops::{Deref, DerefMut};

/// Arc is the only valid way to access an item in
/// the pool. It is returned by alloc, and will automatically
/// release/retain when dropped/cloned. It implements Deref/DerefMut,
/// so all accesses can go through it.
pub struct Arc<T> {
    pool: *mut Pool<T>,
    index: usize,
}

/// Public functions
impl <T> Arc<T> {
    pub unsafe fn retain(&mut self) {
        self.get_pool().retain(self.index);
    }

    pub unsafe fn release(&mut self) {
        self.get_pool().release(self.index);
    }
}

/// Internal functions
impl <T> Arc<T> {
    fn new(index: usize, p: &Pool<T>) -> Arc<T> {
        Arc {
            pool: unsafe { mem::transmute(p) },
            index: index,
        }
    }

    fn get_pool(&self) -> &mut Pool<T> {
        unsafe {
            &mut *self.pool
        }
    }

    fn ref_count(&self) -> usize {
        self.get_pool().header_for(self.index).ref_count.load(Ordering::Relaxed)
    }
}

impl <T> Drop for Arc<T> {
    fn drop(&mut self) {
        self.get_pool().release(self.index);
    }
}

impl <T> Clone for Arc<T> {
    fn clone(&self) -> Self {
        self.get_pool().retain(self.index);
        Arc {
            pool: self.pool,
            index: self.index,
        }
    }
}

impl<T> Deref for Arc<T> {
    type Target = T;

    fn deref<'b>(&'b self) -> &'b T {
        &self.get_pool()[self.index]
    }
}

impl<T> DerefMut for Arc<T> {
    fn deref_mut<'b>(&'b mut self) -> &'b mut T {
        &mut self.get_pool()[self.index]
    }
}

impl <T> fmt::Debug for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Arc{{ offset: {:?}, ref_count: {:?} }}", self.index, self.ref_count())
    }
}

impl <T> PartialEq for Arc<T> {
    fn eq(&self, other: &Arc<T>) -> bool {
        if self.index != other.index {
            false
        } else {
            unsafe {
                self.pool as *const _ == other.pool as *const _
            }
        }
    }
}

/// A pool represents a fixed number of ref-counted objects.
/// The pool treats all given space as an unallocated
/// pool of objects. Each object is prefixed with a header.
/// The header is formatted as follows:
/// * V1
///   - [0..2] ref_count: u16
///
pub struct Pool<T> {
    item_type: PhantomData<T>,

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
impl <T> Pool<T> {
    pub fn new(mem: &mut [u8]) -> Pool<T> {
        let ptr: *mut u8 = mem.as_mut_ptr();
        let header_size = mem::size_of::<SlotHeader>();
        let slot_size = mem::size_of::<T>() + header_size;
        Pool {
            item_type: PhantomData,
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

    /// Try to allocate a new item from the pool.
    /// A mutable reference to the item is returned on success
    pub fn alloc(&mut self) -> Result<Arc<T>, &'static str> {
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
impl <T> Pool<T> {
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
}

impl <T> Index<usize> for Pool<T> {
    type Output = T;

    fn index<'a>(&'a self, i: usize) -> &'a T {
        unsafe {
            let ptr = self.buffer.clone()
            .offset((i * self.slot_size) as isize)
            .offset(self.header_size as isize);
            mem::transmute(ptr)
        }
    }
}

impl <T> IndexMut<usize> for Pool<T> {
    fn index_mut<'a>(&'a mut self, i: usize) -> &'a mut T {
        unsafe {
            let ptr = self.buffer.clone()
            .offset((i * self.slot_size) as isize)
            .offset(self.header_size as isize);
            mem::transmute(ptr)
        }
    }
}

#[test]
fn release_frees() {
       let mut buf: [u8; 100] = [0; 100];
       let mut p = Pool::<u32>::new(&mut buf[..]);

       // Use internal_alloc so that the Arc doesn't drop
       // the reference immediately
       assert!(p.internal_alloc().is_ok());
       assert!(p.internal_alloc().is_ok());

       assert_eq!(2, p.live_count());

       p.release(0);
       assert_eq!(1, p.live_count());
       assert_eq!(1, p.free_list.len());
       assert_eq!(0, *p.free_list.front().unwrap());

       p.release(1);
       assert_eq!(0, p.live_count());
       assert_eq!(2, p.free_list.len());
}

#[test]
fn alloc_after_free_recycles() {
       let mut buf: [u8; 100] = [0; 100];
       let mut p = Pool::<u32>::new(&mut buf[..]);
       assert!(p.internal_alloc().is_ok());
       assert_eq!(1, p.live_count());
       assert_eq!(1, p.tail.load(Ordering::Relaxed));

       p.release(0);
       assert_eq!(0, p.live_count());
       assert_eq!(1, p.free_list.len());

       assert!(p.internal_alloc().is_ok());
       assert_eq!(1, p.tail.load(Ordering::Relaxed)); // Tail shouldn't move
       assert_eq!(1, p.live_count());
       assert_eq!(0, p.free_list.len());
}


fn arc_clone() {
    let mut buf: [u8; 100] = [0; 100];
    let mut p = Pool::<u32>::new(&mut buf[..]);
    {
        let mut int1:Arc<u32> = p.alloc().unwrap();
        assert_eq!(1, int1.ref_count());
        assert_eq!(1, p.header_for(0).ref_count.load(Ordering::Relaxed));
        {
            let mut int1_c:Arc<u32> = int1.clone(); // Should bump the ref count
            assert_eq!(2, int1.ref_count());
            assert_eq!(2, int1_c.ref_count());
            assert_eq!(2, p.header_for(0).ref_count.load(Ordering::Relaxed));
        }
        // Now, the clone should have been dropped, but no memory reclaimed
        assert_eq!(1, p.header_for(0).ref_count.load(Ordering::Relaxed));
        assert_eq!(0, p.free_list.len());
    }
    // Now, int1 should have been dropped, and all memory reclaimed
    assert_eq!(0, p.header_for(0).ref_count.load(Ordering::Relaxed));
    assert_eq!(1, p.free_list.len());
}

#[test]
fn arc_drop() {
    let mut buf: [u8; 100] = [0; 100];
    let mut p = Pool::<u32>::new(&mut buf[..]);
    {
        let mut int1:Arc<u32> = p.alloc().unwrap();
        assert_eq!(1, int1.ref_count());
        assert_eq!(1, p.header_for(0).ref_count.load(Ordering::Relaxed));
        {
            let mut int2:Arc<u32> = p.alloc().unwrap();
            assert_eq!(1, int2.ref_count());
            assert_eq!(1, p.header_for(1).ref_count.load(Ordering::Relaxed));
        }
        // Now, int2 should have been dropped
        assert_eq!(0, p.header_for(1).ref_count.load(Ordering::Relaxed));
        assert_eq!(1, p.free_list.len());
    }
    // Now, int1 should have been dropped
    assert_eq!(0, p.header_for(0).ref_count.load(Ordering::Relaxed));
    assert_eq!(2, p.free_list.len());
}

#[test]
fn construction() {
    let mut buf: [u8; 100] = [0; 100];
    let mut p = Pool::<u32>::new(&mut buf[..]);

    assert_eq!(100, p.buffer_size);
    assert_eq!(mem::size_of::<usize>(), p.header_size);

    let expected_size = mem::size_of::<usize>() + mem::size_of::<u32>();
    assert_eq!(expected_size, p.slot_size);
    assert_eq!(100/expected_size, p.capacity); // expected_size should be 8+4=12
    assert_eq!(8, p.capacity);
}

#[test]
fn free_list_alloc_works() {
    let mut buf: [u8; 100] = [0; 100];
    let mut p = Pool::<u32>::new(&mut buf[..]);
    {
        let mut int1:Arc<u32> = p.alloc().unwrap();
        *int1 = 42;
        // Check payload
        assert_eq!([42u8, 0u8, 0u8, 0u8][..], buf[8..12]);
        // Check ref_count
        assert_eq!([1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8][..], buf[0..8]);
        assert_eq!(1, p.live_count());
    }
    // int1 is now out of scope, let's ensure the drop worked
    assert_eq!([0u8; 8][..], buf[0..8]);
}

#[test]
fn check_oom_error() {
    let mut buf: [u8; 1] = [0; 1];
    let mut p = Pool::<u32>::new(&mut buf[..]);
    assert_eq!(Err("OOM"), p.alloc());
}

#[test]
fn multiple_allocations_work() {
    let mut buf: [u8; 120] = [0; 120];
    let mut p = Pool::<u32>::new(&mut buf[..]);
    for i in 0..10 {
        let mut int1 = p.alloc().unwrap();
        *int1 = i;
        unsafe { int1.retain() }; // Make sure this stays around long enough to read later
   }
   assert_eq!(10, p.live_count());
   let expected_ref_count = [1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
   for i in 0..10 {
       let start = 12*i;
       // Check ref_count
       assert_eq!(expected_ref_count[..], buf[start..start+8]);
       // Check payload
       assert_eq!([i as u8, 0u8, 0u8, 0u8][..], buf[start+8..start+12]);
    }
}
