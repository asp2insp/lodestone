use std::{mem, slice};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::ops::{Index, IndexMut};
use std::marker::PhantomData;


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

    // Cached values
    slot_size: usize,
    header_size: usize,
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
            slot_size: slot_size,
            capacity: mem.len() / slot_size,
            header_size: header_size,
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
    pub fn alloc(&mut self) -> Result<&mut T, &'static str> {
        let index = self.claim_first_free_slot();
        if index >= self.capacity {
            Err("OOM")
        } else {
            Ok(&mut self[index])
        }
    }
}


/// Internal Functions
impl <T> Pool<T> {
    fn claim_first_free_slot(&mut self) -> usize {
        let mut i = self.buffer.clone();
        let mut index = 0;
        while index < self.capacity {
            unsafe {
                let header: *mut SlotHeader = mem::transmute(i);
                if (*header).ref_count
                    .compare_and_swap(0, 1, Ordering::Relaxed) == 0 {
                    break
                }
                // Otherwise, look for the next one
                i = i.offset(self.slot_size as isize);
                index += 1;
            }
        }
        index
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
        let int1:&mut u32 = p.alloc().unwrap();
        *int1 = 42;
   }
   // Check ref_count
   assert_eq!([1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8][..], buf[0..8]);
   // Check payload
   assert_eq!([42u8, 0u8, 0u8, 0u8][..], buf[8..12]);
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
        let int1 = p.alloc().unwrap();
        *int1 = i;
   }
   for i in 0..10 {
       let start = 12*i;
       // Check ref_count
       assert_eq!([1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8][..],
            buf[start..start+8]);
       // Check payload
       assert_eq!([i as u8, 0u8, 0u8, 0u8][..],
           buf[start+8..start+12]);
   }
}
