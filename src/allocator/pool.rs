use std::{ptr, mem, fmt};
use std::sync::atomic::{AtomicUsize, AtomicBool};
use std::sync::atomic::Ordering::{SeqCst};

use super::arc::*;

pub const PAGE_SIZE: usize = 4096;
pub const BUFFER_END: usize = !0 as usize;

lazy_static! {
    pub static ref HEADER_SIZE: usize = mem::size_of::<SkipListEntry>();
    pub static ref FIRST_OR_SINGLE_CONTENT_SIZE: usize = PAGE_SIZE - *HEADER_SIZE;
    pub static ref OVERHEAD: usize = *HEADER_SIZE + *ARC_INNER_SIZE;
}

pub struct Pool {
    buffer: *mut u8,
    buffer_size: usize,
}

#[derive(Debug)]
struct Metadata {
    // TODO rip this out and replace with a free list
    // We probably want to keep 2 free lists -- A one-page
    // list and a larger objects list to avoid fragmentation
    lowest_known_free_index: usize,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Pool")
            .field("buffer_size", &self.buffer_size)
            .field("metadata", &self.get_metadata_block())
            .field("blocks", &self.get_debug_blocks())
            .finish()
    }
}

impl Pool {
    pub fn new(buf: &mut [u8]) -> Pool {
        let ptr: *mut u8 = buf.as_mut_ptr();
        let p = Pool {
            buffer: ptr,
            buffer_size: buf.len(),
        };
        // Last page is metadata and not usable as a full page-aligned chunk anyway
        let last_skip_index = p.buffer_size - PAGE_SIZE;
        p.make_skip_entry(SkipListStart(last_skip_index), 0, BUFFER_END, false);
        p.make_skip_entry(SkipListStart(0), BUFFER_END, last_skip_index, true);
        p
    }
}

#[derive(Debug)]
struct SkipListEntry {
    prev: AtomicUsize, // absolute buffer offset of previous SKE
    is_free: AtomicBool, // Whether the given memory is free
    next: AtomicUsize, // absolute buffer offset of next SKE
}

use self::IndexType::*;
#[derive(Debug)]
enum IndexType {
    ArcStart(usize),
    DataStart(usize),
    SkipListStart(usize),
}

/// Public interface
impl Pool {
    pub fn malloc<T: Sized>(&self, data: T) -> Arc<T> {
        let size = mem::size_of::<T>();
        let chunked_size = round_up_to_nearest_page_size(size);
        let metadata = self.get_metadata_block();
        // Claim a block
        let (free_block_index, entry) = self.next_free_block_larger_than(chunked_size,
            SkipListStart(metadata.lowest_known_free_index));
        if free_block_index == BUFFER_END {
            panic!("OOM")
        }
        entry.is_free.store(false, SeqCst);

        let next_index = free_block_index + chunked_size;
        let following_index = entry.next.load(SeqCst);
        assert!(next_index <= following_index);
        // If we split a block, then we need to make a new entry
        if next_index < following_index {
            self.make_skip_entry(SkipListStart(next_index),
                free_block_index, following_index, true);
            let (_, following_entry) = self.header_for_byte_index(SkipListStart(following_index));
            following_entry.prev.store(next_index, SeqCst);
            entry.next.store(next_index, SeqCst);
        }

        // Update known free index if necessary (only necessary if we've used the lowest)
        if free_block_index == metadata.lowest_known_free_index {
            let (idx, _) = self.next_free_block_larger_than(0, SkipListStart(free_block_index));
            metadata.lowest_known_free_index = idx;
        }

        let inner = self.index_to_arc_inner(SkipListStart(free_block_index));
        inner.init(data);
        Arc::new(inner, self)
    }

    pub fn free<T>(&self, arc: &Arc<T>) {
        let metadata = self.get_metadata_block();
        let arc_index = self.arc_to_arc_inner_index(arc);
        let (this_idx, header) = self.header_for_byte_index(arc_index);
        let prev_idx = header.prev.load(SeqCst);
        let next_idx = header.next.load(SeqCst);

        header.is_free.store(true, SeqCst);
        // Update known free index if necessary
        if this_idx < metadata.lowest_known_free_index {
            metadata.lowest_known_free_index = this_idx;
        }

        if next_idx != BUFFER_END {
            let (_, next) = self.header_for_byte_index(SkipListStart(next_idx));
            if next.is_free.load(SeqCst) {
                // Merge with the next item, by encompassing it
                let next_next_idx = next.next.load(SeqCst);
                header.next.store(next_next_idx, SeqCst)
            }
        }
        if prev_idx != BUFFER_END {
            let (_, prev) = self.header_for_byte_index(SkipListStart(prev_idx));
            if prev.is_free.load(SeqCst) {
                // Merge by swallong this item with the previous item
                let next_idx = header.next.load(SeqCst);
                prev.next.store(next_idx, SeqCst);
            }
        }
    }

    pub fn deref<'a, T>(&'a self, arc: &'a Arc<T>) -> &'a T {
        let arc_index = self.arc_to_arc_inner_index(arc);
        println!("Derefing Arc inner at {:?}", arc_index);
        &self.index_to_arc_inner(arc_index).data
    }
}

/// Private interface
impl Pool {
    /// Get the metadata block, which always lives in the last page of the array
    fn get_metadata_block<'a>(&'a self) -> &'a mut Metadata {
        let metadata_index = self.buffer_size - PAGE_SIZE + *HEADER_SIZE;
        unsafe {
            mem::transmute(self.byte_index_to_live_ptr(metadata_index))
        }
    }

    /// Get the arc inner for a given index
    fn index_to_arc_inner<'a, T>(&'a self, index: IndexType) -> &'a mut ArcInner<T> {
        let offset = match index {
            ArcStart(i) => i,
            DataStart(i) => i - *ARC_INNER_SIZE,
            SkipListStart(i) => i + *HEADER_SIZE,
        };
        unsafe {
            let ptr = self.byte_index_to_live_ptr(offset);
            mem::transmute(ptr)
        }
    }

    /// Get the arc inner for a given arc outer
    fn arc_to_arc_inner_index<'a, T>(&'a self, arc: &Arc<T>) -> IndexType {
        unsafe {
            let ptr: *mut u8 = mem::transmute(arc._ptr);
            ArcStart(self.live_ptr_to_byte_index(ptr))
        }
    }

    /// Does not take overhead into account
    fn next_free_block_larger_than<'a>(&'a self, size: usize, start_index: IndexType) -> (usize, &'a mut SkipListEntry) {
        let (idx, mut entry) = self.header_for_byte_index(start_index);
        if entry.is_free.load(SeqCst)
           && (entry.next.load(SeqCst) - idx) >= size {
            (idx, entry)
        } else {
            let next_index = entry.next.load(SeqCst);
            if next_index != BUFFER_END {
                self.next_free_block_larger_than(size, SkipListStart(next_index))
            } else {
                (BUFFER_END, entry)
            }
        }
    }

    fn live_ptr_to_byte_index(&self, ptr: *const u8) -> usize {
        let obj_addr = ptr as usize;
        let buf_addr = self.buffer as usize;
        if obj_addr < buf_addr {
            panic!("live_ptr_to_byte_index called with address below start of buffer!");
        }
        let offset = obj_addr - buf_addr;
        if offset > self.buffer_size {
            panic!("live_ptr_to_byte_index called with address past end of buffer!");
        }
        // println!("Converted live address {} to offset {}", obj_addr, offset);
        offset
    }

    unsafe fn byte_index_to_live_ptr(&self, byte_index: usize) -> *mut u8 {
        let ptr = self.buffer.offset(byte_index as isize);
        // println!("Converted offset {} to live address {:?}", byte_index, ptr);
        ptr
    }

    /// Find the skip list entry that precedes the given index's data
    fn header_for_byte_index<'a>(&'a self, index: IndexType) -> (usize, &'a mut SkipListEntry) {
        let offset = match index {
            ArcStart(i) => i - *HEADER_SIZE,
            DataStart(i) => i - *ARC_INNER_SIZE - *HEADER_SIZE,
            SkipListStart(i) => i,
        };
        unsafe {
            (offset, mem::transmute(self.buffer.offset(offset as isize)))
        }
    }

    /// Find the skip list entry that follows the given index's data
    fn footer_for_byte_index<'a>(&'a self, index: IndexType) -> (usize, &'a mut SkipListEntry) {
        let (_, entry) = self.header_for_byte_index(index);
        unsafe {
            let next_index = entry.next.load(SeqCst);
            let ptr = self.byte_index_to_live_ptr(next_index);
            (next_index, mem::transmute(ptr))
        }
    }

    fn make_skip_entry(&self, index: IndexType, prev: usize, next: usize, is_free: bool) {
        let (_, entry) = self.header_for_byte_index(index);
        entry.prev.store(prev, SeqCst);
        entry.next.store(next, SeqCst);
        entry.is_free.store(is_free, SeqCst);
    }

    fn get_debug_blocks<'a>(&'a self) -> Vec<_B> {
        let mut ret: Vec<_B> = Vec::new();
        let mut next_index: usize = 0;
        loop {
            let (idx, entry) = self.header_for_byte_index(SkipListStart(next_index));
            next_index = entry.next.load(SeqCst);
            let prev_index = entry.prev.load(SeqCst);
            if next_index == BUFFER_END {
                break
            }
            ret.push(_B {
                start: idx,
                capacity: next_index - idx - *OVERHEAD,
                next: next_index,
                prev: prev_index,
                is_free: entry.is_free.load(SeqCst)
            });
        }
        ret
    }
}

#[derive(Debug)]
struct _B {
    start: usize,
    capacity: usize,
    next: usize,
    prev: usize,
    is_free: bool,
}

/// Take byte-length and compute the number of pages necessary to hold that many bytes.
/// Takes the space required for header/footer into account.
/// # Examples
/// ```
/// use lodestone::allocator::pool::*;
/// assert_eq!(1, calc_num_pages(5));
/// assert_eq!(1, calc_num_pages(PAGE_SIZE - *HEADER_SIZE));
/// assert_eq!(2, calc_num_pages(PAGE_SIZE - *HEADER_SIZE + 1));
/// assert_eq!(3, calc_num_pages(PAGE_SIZE*2));
/// ```
pub fn calc_num_pages(size: usize) -> usize {
    if size <= *FIRST_OR_SINGLE_CONTENT_SIZE {
        1
    } else {
        let tail_size = size - *FIRST_OR_SINGLE_CONTENT_SIZE;
        let spill = if tail_size % PAGE_SIZE > 0 {1} else {0};
        1 + tail_size / PAGE_SIZE + spill
    }
}

/// Take byte-length and round up to the nearest page's worth of bytes.
/// Takes overhead into account
/// # Examples
/// ```
/// use lodestone::allocator::pool::*;
/// assert_eq!(4096, round_up_to_nearest_page_size(5));
/// assert_eq!(4096, round_up_to_nearest_page_size(PAGE_SIZE - *HEADER_SIZE));
/// assert_eq!(8192, round_up_to_nearest_page_size(PAGE_SIZE - *HEADER_SIZE + 1));
/// assert_eq!(12288, round_up_to_nearest_page_size(PAGE_SIZE*2));
/// ```
pub fn round_up_to_nearest_page_size(size: usize) -> usize {
    calc_num_pages(size) * PAGE_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestStruct {
        a: usize,
        b: isize,
        c: bool,
    }

    #[test]
    #[should_panic(expected="OOM")]
    fn test_oom() {
        let mut buf: [u8; 0x2000] = [0; 0x2000];
        let p = Pool::new(&mut buf[..]);
        p.malloc([42; 0x1000]);
    }

    #[test]
    fn test_printing_empty() {
        let mut buf: [u8; 0x2000] = [0; 0x2000];
        let p = Pool::new(&mut buf[..]);
        assert_eq!(
            "Pool { buffer_size: 8192, \
                metadata: Metadata { lowest_known_free_index: 0 }, \
                blocks: [\
                _B { start: 0, capacity: 4056, next: 4096, prev: 18446744073709551615, is_free: true }\
                ] }",
            format!("{:?}", p)
        );
    }

    #[test]
    fn test_small_alloc_free() {
        let mut buf: [u8; 0x4000] = [0; 0x4000];
        let p = Pool::new(&mut buf[..]);

        let arc_ts1 = p.malloc(TestStruct {
            a: 12345,
            b: -678,
            c: true,
        });

        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 4096 }, \
                blocks: [\
                _B { start: 0, capacity: 4056, next: 4096, prev: 18446744073709551615, is_free: false }, \
                _B { start: 4096, capacity: 8152, next: 12288, prev: 0, is_free: true }\
                ] }",
            format!("{:?}", p)
        );

        assert_eq!(12345, arc_ts1.a);
        assert_eq!(-678, arc_ts1.b);
        assert_eq!(true, arc_ts1.c);

        let arc_ts2 = p.malloc(TestStruct {
            a: 12345,
            b: -678,
            c: true,
        });
        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 8192 }, \
                blocks: [\
                _B { start: 0, capacity: 4056, next: 4096, prev: 18446744073709551615, is_free: false }, \
                _B { start: 4096, capacity: 4056, next: 8192, prev: 0, is_free: false }, \
                _B { start: 8192, capacity: 4056, next: 12288, prev: 4096, is_free: true }\
                ] }",
            format!("{:?}", p)
        );

        p.free(&arc_ts1);

        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 0 }, \
                blocks: [\
                _B { start: 0, capacity: 4056, next: 4096, prev: 18446744073709551615, is_free: true }, \
                _B { start: 4096, capacity: 4056, next: 8192, prev: 0, is_free: false }, \
                _B { start: 8192, capacity: 4056, next: 12288, prev: 4096, is_free: true }\
                ] }",
            format!("{:?}", p)
        );

        p.free(&arc_ts2);

        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 0 }, \
                blocks: [\
                _B { start: 0, capacity: 12248, next: 12288, prev: 18446744073709551615, is_free: true }\
                ] }",
            format!("{:?}", p)
        );
    }
}
