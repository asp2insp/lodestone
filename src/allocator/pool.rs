use std::{ptr, mem, fmt};
use std::sync::atomic::{AtomicUsize, AtomicBool};
use std::sync::atomic::Ordering::{SeqCst};

pub const PAGE_SIZE: usize = 4096;
pub const BUFFER_END: usize = !0 as usize;

lazy_static! {
    pub static ref HEADER_SIZE: usize = mem::size_of::<SkipListEntry>();
    pub static ref FIRST_OR_SINGLE_CONTENT_SIZE: usize = PAGE_SIZE - *HEADER_SIZE;
    pub static ref ARC_INNER_SIZE: usize = mem::size_of::<ArcInner>();
    pub static ref OVERHEAD: usize = *HEADER_SIZE + *ARC_INNER_SIZE;
}

pub struct Pool {
    buffer: *mut u8,
    buffer_size: usize,

    // cached values
    lowest_known_free_index: usize,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Pool")
            .field("buffer_size", &self.buffer_size)
            .field("lowest_known_free_index", &self.lowest_known_free_index)
            .field("blocks", &self.get_debug_blocks())
            .finish()
    }
}


/// Arcs are free floating and are not persisted
pub struct Arc {
    _ptr: *mut ArcInner,
}

impl Arc {
    fn new(inner: &mut ArcInner) -> Arc {
        inner.strong.fetch_add(1, SeqCst);
        Arc {
            _ptr: inner as *mut ArcInner,
        }
    }
}

impl Pool {
    pub fn new(buf: &mut [u8]) -> Pool {
        let ptr: *mut u8 = buf.as_mut_ptr();
        let p = Pool {
            buffer: ptr,
            buffer_size: buf.len(),
            lowest_known_free_index: 0,
        };
        let last_skip_index = p.buffer_size - *HEADER_SIZE;
        p.make_skip_entry(SkipListStart(last_skip_index), 0, BUFFER_END, false);
        p.make_skip_entry(SkipListStart(0), BUFFER_END, last_skip_index, true);
        p
    }
}

/// ArcInners live inside of the buffer and are persisted
struct ArcInner {
    strong: AtomicUsize,
    weak: AtomicUsize,
    size: usize,
}

struct SkipListEntry {
    prev: AtomicUsize, // absolute buffer offset of previous SKE
    is_free: AtomicBool, // Whether the given memory is free
    next: AtomicUsize, // absolute buffer offset of next SKE
}

use self::IndexType::*;
enum IndexType {
    ArcStart(usize),
    DataStart(usize),
    SkipListStart(usize),
}

/// Private interface
impl Pool {
    fn malloc(&mut self, size: usize) -> Arc {
        let chunked_size = round_up_to_nearest_page_size(size);
        let (free_block_index, entry) = self.next_free_block_larger_than(chunked_size,
            SkipListStart(self.lowest_known_free_index));
        let next_index = free_block_index + chunked_size;
        let following_index = entry.next.load(SeqCst);
        assert!(next_index <= following_index);
        // If we split a block, then we need to make a new entry
        if next_index < following_index {
            self.make_skip_entry(SkipListStart(next_index),
                following_index, free_block_index, true);
            let (_, following_entry) = self.header_for_byte_index(SkipListStart(following_index));
            following_entry.prev.store(next_index, SeqCst);
            entry.next.store(next_index, SeqCst);
        }
        let inner = self.get_arc_inner(SkipListStart(free_block_index));
        inner.strong.store(0, SeqCst);
        inner.weak.store(0, SeqCst);
        inner.size = size;
        Arc::new(inner)
    }
    //
    // fn free(&mut self, arc: Arc) {
    //
    // }


    /// Get the arc inner for a given index
    fn get_arc_inner<'a>(&'a self, index: IndexType) -> &'a mut ArcInner {
        let offset = match index {
            ArcStart(i) => i ,
            DataStart(i) => i - *ARC_INNER_SIZE,
            SkipListStart(i) => i + *HEADER_SIZE,
        };
        unsafe {
            let ptr = self.byte_index_to_live_ptr(offset);
            mem::transmute(ptr)
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
            self.next_free_block_larger_than(size, SkipListStart(next_index))
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
        offset
    }

    unsafe fn byte_index_to_live_ptr(&self, byte_index: usize) -> *mut u8 {
        self.buffer.offset(byte_index as isize)
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
            if next_index == BUFFER_END {
                break
            }
            ret.push(_B {
                start: idx,
                size: next_index - idx - *OVERHEAD,
                is_free: entry.is_free.load(SeqCst)
            });
        }
        ret
    }
}

#[derive(Debug)]
struct _B {
    start: usize,
    size: usize,
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

    #[test]
    fn test_printing_empty() {
        let mut buf: [u8; 0x1000] = [0; 0x1000];
        let p = Pool::new(&mut buf[..]);
        assert_eq!(
            "Pool { buffer_size: 4096, lowest_known_free_index: 0, blocks: \
                [_B { start: 0, size: 4024, is_free: true }] }",
            format!("{:?}", p)
        );
    }
}
