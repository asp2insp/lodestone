use super::*;
use std::mem;
use std::sync::atomic::{Ordering};

#[test]
fn release_frees() {
       let mut buf: [u8; 5*0x1000] = [0; 5*0x1000];
       let mut p = Pool::new(&mut buf[..]);

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
       let mut buf: [u8; 5*0x1000] = [0; 5*0x1000];
       let mut p = Pool::new(&mut buf[..]);
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

#[test]
fn construction() {
    let mut buf: [u8; 5*0x1000] = [0; 5*0x1000];
    let mut p = Pool::new(&mut buf[..]);

    assert_eq!(5*0x1000, p.buffer_size);
    assert_eq!(mem::size_of::<usize>(), p.header_size);

    let expected_size = mem::size_of::<usize>() + mem::size_of::<Page>();
    assert_eq!(expected_size, p.slot_size);
    assert_eq!(5*0x1000/expected_size, p.capacity); // expected_size should be 8+4096=4104
    assert_eq!(4, p.capacity);
}

#[test]
fn free_list_alloc_works() {
    let mut buf: [u8; 5*0x1000] = [0; 5*0x1000];
    let mut p = Pool::new(&mut buf[..]);
    let forty_two = [42u8; 4096];
    {
        let mut int1 = p.alloc().unwrap();
        p[int1] = forty_two;
        // Check payload
        assert_eq!(forty_two[..], buf[8..4104]);
        // Check ref_count
        assert_eq!([1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8][..], buf[0..8]);
        assert_eq!(1, p.live_count());
    }
}

#[test]
fn check_oom_error() {
    let mut buf: [u8; 1] = [0; 1];
    let mut p = Pool::new(&mut buf[..]);
    assert_eq!(Err("OOM"), p.alloc());
}

#[test]
fn multiple_allocations_work() {
    let mut buf: [u8; 12*0x1000] = [0; 12*0x1000];
    let mut p = Pool::new(&mut buf[..]);
    for i in 0..10 {
        let mut int1 = p.alloc().unwrap();
   }
   assert_eq!(10, p.live_count());
   let expected_ref_count = [1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
   for i in 0..10 {
       let start = 4104*i;
       // Check ref_count
       assert_eq!(expected_ref_count[..], buf[start..start+8]);
    }
}
