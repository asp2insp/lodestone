use std::mem;
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::ops::{Index, IndexMut};
use std::collections::LinkedList;
use std::ptr;

use super::page::*;

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

    free_list: RefCell<LinkedList<PageIndex>>,
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
            free_list: RefCell::new(LinkedList::new()),
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

    /// Get the reference count for a given page
    /// Don't use this for anything besides testing. Because
    /// really. What are you thinking?
    pub fn get_ref_count(&self, index: PageIndex) -> usize {
        self.header_for(index).ref_count.load(Ordering::SeqCst)
    }

    /// Fast copy a slot's contents to a new slot and return
    /// a pointer to the new slot
    pub fn alloc_with_contents_of(&self, other: PageIndex) -> Result<PageIndex, &'static str> {
        let index = try!(self.claim_free_index());
        unsafe {
            let from = self.raw_contents_for(other);
            let to = self.raw_contents_for(index);
            ptr::copy_nonoverlapping(from, to, mem::size_of::<Page>());
        }
        Ok(index)
    }

    /// Try to allocate a new item from the pool.
    /// A mutable reference to the item is returned on success
    pub fn alloc(&self) -> Result<PageIndex, &'static str> {
        let index = try!(self.claim_free_index());
        self.zero_page(index);
        Ok(index)
    }

    // Increase the ref count for the cell at the given index
    pub fn retain(&self, index: PageIndex) {
        let h = self.header_for(index);
        h.ref_count.fetch_add(1, Ordering::SeqCst);
    }

    // Decrease the ref count for the cell at the given index
    // return true iff the cell is now considered free
    pub fn release(&self, index: PageIndex) -> bool {
        let mut is_free = false;
        { // Make the borrow checker happy
            let h = self.header_for(index);
            let old = h.ref_count.fetch_sub(1, Ordering::SeqCst);
            if old == 1 {
                // TODO: check the correctness of this
                is_free = true;
            }
        }
        if is_free {
            self.free_list.borrow_mut().push_back(index);
        }
        is_free
    }

    /// Returns the number of live items. O(1) running time.
    pub fn live_count(&self) -> usize {
        self.tail.load(Ordering::SeqCst) - self.free_list.borrow().len()
    }
}


/// Internal Functions
impl Pool {
    // Returns an item from the free list, or
    // tries to allocate a new one from the buffer
    fn claim_free_index(&self) -> Result<PageIndex, &'static str> {
        let index = match self.free_list.borrow_mut().pop_front() {
            Some(i) => i,
            None => try!(self.push_back_alloc()),
        };
        self.retain(index);
        Ok(index)
    }

    // Pushes the end of the used space in the buffer back
    // returns the previous index
    fn push_back_alloc(&self) -> Result<PageIndex, &'static str> {
        let old_tail = self.tail.fetch_add(1, Ordering::SeqCst);
        if old_tail >= self.capacity {
            Err("OOM")
        } else {
            Ok(old_tail)
        }
    }

    fn header_for<'a>(&'a self, i: PageIndex) -> &'a mut SlotHeader {
        unsafe {
            let ptr = self.buffer.clone()
                .offset((i * self.slot_size) as isize);
            mem::transmute(ptr)
        }
    }

    fn raw_contents_for<'a>(&'a self, i: PageIndex) -> *mut u8 {
        unsafe {
            self.buffer.clone()
                .offset((i * self.slot_size) as isize)
                .offset(self.header_size as isize)
        }
    }

    fn zero_page(&self, page_index: PageIndex) {
        unsafe {
            let mut i = self.raw_contents_for(page_index);
            let end = i.clone().offset(mem::size_of::<Page>() as isize);
            while i != end {
                *i = 0u8;
                i = i.offset(1);
            }
        }
    }

    /// Returns the index of the page containing the given pointer
    /// panics if given a pointer outside of the buffer
    unsafe fn calc_page_index(&self, obj: *const u8) -> usize {
        let obj_addr = obj as usize;
        let buf_addr = self.buffer as usize;

        if obj_addr < buf_addr {
            panic!("calc_page_index called with address below start of buffer!");
        }
        let offset = obj_addr - buf_addr;
        if offset > self.buffer_size {
            panic!("calc_page_index called with address past end of buffer!");
        }
        offset / self.slot_size
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

/// Tests
#[test]
fn release_frees() {
       let mut buf: [u8; 5*0x1000] = [0; 5*0x1000];
       let p = Pool::new(&mut buf[..]);

       // Use claim_free_index so that the Arc doesn't drop
       // the reference immediately
       assert!(p.claim_free_index().is_ok());
       assert!(p.claim_free_index().is_ok());

       assert_eq!(2, p.live_count());

       p.release(0);
       assert_eq!(1, p.live_count());
       assert_eq!(1, p.free_list.borrow().len());
       assert_eq!(0, *p.free_list.borrow().front().unwrap());

       p.release(1);
       assert_eq!(0, p.live_count());
       assert_eq!(2, p.free_list.borrow().len());
}

#[test]
fn alloc_after_free_recycles() {
       let mut buf: [u8; 5*0x1000] = [0; 5*0x1000];
       let p = Pool::new(&mut buf[..]);
       assert!(p.claim_free_index().is_ok());
       assert_eq!(1, p.live_count());
       assert_eq!(1, p.tail.load(Ordering::Relaxed));

       p.release(0);
       assert_eq!(0, p.live_count());
       assert_eq!(1, p.free_list.borrow().len());

       assert!(p.claim_free_index().is_ok());
       assert_eq!(1, p.tail.load(Ordering::Relaxed)); // Tail shouldn't move
       assert_eq!(1, p.live_count());
       assert_eq!(0, p.free_list.borrow().len());
}

#[test]
fn construction() {
    let mut buf: [u8; 5*0x1000] = [0; 5*0x1000];
    let p = Pool::new(&mut buf[..]);

    assert_eq!(5*0x1000, p.buffer_size);
    assert_eq!(mem::size_of::<usize>(), p.header_size);

    let expected_size = mem::size_of::<usize>() + mem::size_of::<Page>();
    assert_eq!(expected_size, p.slot_size);
    assert_eq!(5*0x1000/expected_size, p.capacity); // expected_size should be 8+4096=4104
    assert_eq!(5, p.capacity);
}

#[test]
fn free_list_alloc_works() {
    let mut buf: [u8; 5*0x1000] = [0; 5*0x1000];
    let mut p = Pool::new(&mut buf[..]);
    let forty_two = [42u8; PAGE_SIZE];
    {
        let int1 = p.alloc().unwrap();
        p[int1] = forty_two;
        // Check payload
        assert_eq!(forty_two[..], buf[8..4096]);
        // Check ref_count
        assert_eq!([1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8][..], buf[0..8]);
        assert_eq!(1, p.live_count());
    }
}

#[test]
fn check_oom_error() {
    let mut buf: [u8; 1] = [0; 1];
    let p = Pool::new(&mut buf[..]);
    assert_eq!(Err("OOM"), p.alloc());
}

#[test]
fn multiple_allocations_work() {
    let mut buf: [u8; 12*0x1000] = [0; 12*0x1000];
    let p = Pool::new(&mut buf[..]);
    for _ in 0..10 {
        let _ = p.alloc().unwrap();
   }
   assert_eq!(10, p.live_count());
   let expected_ref_count = [1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
   for i in 0..10 {
       let start = 4096*i;
       // Check ref_count
       assert_eq!(expected_ref_count[..], buf[start..start+8]);
    }
}
