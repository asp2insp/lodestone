use std::{mem, slice};

/// An allocator owns a slice of memory
/// and  can pass out references to sub-slices
/// it is also capable of moving the allocation to a new location.
/// There is an unsafe API for clients that take control of the
/// memory allocated
pub struct Allocator {
    buffer: *mut u8,
    len: usize,
    tail: usize,
}

impl Allocator {

    fn new(mem: &mut [u8]) -> Allocator {
        let ptr: *mut u8 = mem.as_mut_ptr();
        Allocator {
            buffer: ptr,
            len: mem.len(),
            tail: 0,
        }
    }

    fn alloc<T>(&mut self) -> Result<&mut T, &'static str> {
        let size = mem::size_of::<T>();
        let old_tail = self.tail;
        self.tail += size;
        if self.tail >= self.len {
            return Err("OOM")
        }
        unsafe {
            let start = self.buffer.clone().offset(old_tail as isize);
            Ok(mem::transmute(start))
        }
    }
}


#[test]
fn linear_alloc_works() {
   let mut buf: [u8; 100] = [0; 100];
   let mut al = Allocator::new(&mut buf[..]);
   
   assert_eq!(100, al.len);
   assert_eq!(0, al.tail);

   {
        let int1:&mut u32 = al.alloc::<u32>().unwrap();
        *int1 = 42;
   }

   assert_eq!(4, al.tail);
   assert_eq!([42u8, 0u8, 0u8, 0u8][..], buf[0..4]);
}
