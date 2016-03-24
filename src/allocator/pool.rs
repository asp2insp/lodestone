use std::{ptr, mem, fmt, slice};
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
#[derive(Debug, Copy, Clone)]
enum IndexType {
    ArcByteSliceStart(usize),
    DataStart(usize),
    SkipListStart(usize),
}

/// Public interface
impl Pool {
    pub fn malloc(&self, data: &[u8]) -> Result<ArcByteSlice, &'static str> {
        let size = data.len();
        let chunked_size = size + *OVERHEAD; // round_up_to_nearest_page_size(size);
        let metadata = self.get_metadata_block();
        // Claim a block
        let (free_block_index, entry) = self.next_free_block_larger_than(chunked_size,
            SkipListStart(metadata.lowest_known_free_index));
        if free_block_index == BUFFER_END {
            return Err("OOM")
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
        let dest = self.index_to_byte_slice_mut(SkipListStart(free_block_index));
        dest.clone_from_slice(data);
        Ok(ArcByteSlice::new(inner, self))
    }

    pub fn free(&self, arc: &ArcByteSlice) {
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

    pub fn deref<'a>(&'a self, arc: &'a ArcByteSlice) -> &'a [u8] {
        let arc_index = self.arc_to_arc_inner_index(arc);
        self.index_to_byte_slice(arc_index)
    }

    pub unsafe fn deref_as<'a, T>(&'a self, arc: &'a ArcByteSlice) -> &'a T {
        let arc_index = self.arc_to_arc_inner_index(arc);
        let offset = self.index_to_data_offset(arc_index);
        mem::transmute(self.buffer.offset(offset as isize))
    }

    pub unsafe fn deref_as_mut<'a, T>(&'a self, arc: &'a ArcByteSlice) -> &'a mut T {
        let arc_index = self.arc_to_arc_inner_index(arc);
        let offset = self.index_to_data_offset(arc_index);
        mem::transmute(self.buffer.offset(offset as isize))
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

    /// Get the byte_slice corresponding to an index
    fn index_to_byte_slice<'a>(&'a self, index: IndexType) -> &'a [u8] {
        let size = self.index_to_arc_inner(index).size;
        let offset = self.index_to_data_offset(index);
        unsafe {
            slice::from_raw_parts(self.buffer.offset(offset as isize), size)
        }
    }

    /// Get the byte_slice corresponding to an index
    fn index_to_byte_slice_mut<'a>(&'a self, index: IndexType) -> &'a mut [u8] {
        let size = self.index_to_arc_inner(index).size;
        let offset = self.index_to_data_offset(index);
        unsafe {
            slice::from_raw_parts_mut(self.buffer.offset(offset as isize), size)
        }
    }

    /// Get the arc inner for a given index
    fn index_to_arc_inner<'a>(&'a self, index: IndexType) -> &'a mut ArcByteSliceInner {
        let offset = match index {
            ArcByteSliceStart(i) => i,
            DataStart(i) => i - *ARC_INNER_SIZE,
            SkipListStart(i) => i + *HEADER_SIZE,
        };
        unsafe {
            let ptr = self.byte_index_to_live_ptr(offset);
            mem::transmute(ptr)
        }
    }

    /// Get the arc inner for a given arc outer
    fn arc_to_arc_inner_index<'a>(&'a self, arc: &ArcByteSlice) -> IndexType {
        unsafe {
            let ptr: *mut u8 = mem::transmute(arc._ptr);
            ArcByteSliceStart(self.live_ptr_to_byte_index(ptr))
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

    fn index_to_data_offset(&self, index: IndexType) -> usize {
        match index {
            ArcByteSliceStart(i) => i + *ARC_INNER_SIZE,
            DataStart(i) => i,
            SkipListStart(i) => i + *OVERHEAD,
        }
    }

    /// Find the skip list entry that precedes the given index's data
    fn header_for_byte_index<'a>(&'a self, index: IndexType) -> (usize, &'a mut SkipListEntry) {
        let offset = match index {
            ArcByteSliceStart(i) => i - *HEADER_SIZE,
            DataStart(i) => i - *ARC_INNER_SIZE - *HEADER_SIZE,
            SkipListStart(i) => i,
        };
        unsafe {
            (offset, mem::transmute(self.buffer.offset(offset as isize)))
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

#[cfg(test)]
mod tests {
    use std::mem;
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
        p.malloc(&[42; 0x1000][..]).unwrap();
    }

    #[test]
    fn test_printing_empty() {
        let mut buf: [u8; 0x2000] = [0; 0x2000];
        let p = Pool::new(&mut buf[..]);
        assert_eq!(
            "Pool { buffer_size: 8192, \
                metadata: Metadata { lowest_known_free_index: 0 }, \
                blocks: [\
                _B { start: 0, capacity: 4048, next: 4096, prev: 18446744073709551615, is_free: true }\
                ] }",
            format!("{:?}", p)
        );
    }

    #[test]
    fn test_small_alloc_free() {
        let mut buf: [u8; 0x4000] = [0; 0x4000];
        let p = Pool::new(&mut buf[..]);
        let data = [0x1, 0x2, 0x3, 0x4];

        let arc_ts1 = p.malloc(&data[..]).unwrap();

        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 52 }, \
                blocks: [\
                    _B { start: 0, capacity: 4, next: 52, prev: 18446744073709551615, is_free: false }, \
                    _B { start: 52, capacity: 12188, next: 12288, prev: 0, is_free: true }\
                ] }",
            format!("{:?}", p)
        );

        assert_eq!([0x1, 0x2, 0x3, 0x4], arc_ts1[0..4]);

        let arc_ts2 = p.malloc(&data[..]).unwrap();
        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 104 }, \
                blocks: [\
                    _B { start: 0, capacity: 4, next: 52, prev: 18446744073709551615, is_free: false }, \
                    _B { start: 52, capacity: 4, next: 104, prev: 0, is_free: false }, \
                    _B { start: 104, capacity: 12136, next: 12288, prev: 52, is_free: true }\
                ] }",
            format!("{:?}", p)
        );

        p.free(&arc_ts1);

        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 0 }, \
                blocks: [\
                    _B { start: 0, capacity: 4, next: 52, prev: 18446744073709551615, is_free: true }, \
                    _B { start: 52, capacity: 4, next: 104, prev: 0, is_free: false }, \
                    _B { start: 104, capacity: 12136, next: 12288, prev: 52, is_free: true }\
                ] }",
            format!("{:?}", p)
        );

        p.free(&arc_ts2);

        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 0 }, \
                blocks: [\
                    _B { start: 0, capacity: 12240, next: 12288, prev: 18446744073709551615, is_free: true }\
                ] }",
            format!("{:?}", p)
        );
    }

    #[test]
    fn test_large_alloc_free() {
        let mut buf: [u8; 0x4000] = [0; 0x4000];
        let p = Pool::new(&mut buf[..]);

        // Take up > 1 page
        let arc_ts1 = p.malloc(&[42u8; 0x2000][..]).unwrap();

        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 8240 }, \
                blocks: [\
                    _B { start: 0, capacity: 8192, next: 8240, prev: 18446744073709551615, is_free: false }, \
                    _B { start: 8240, capacity: 4000, next: 12288, prev: 0, is_free: true }\
                ] }",
            format!("{:?}", p)
        );
    }
}
