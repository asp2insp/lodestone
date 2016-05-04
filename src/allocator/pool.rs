use std::{mem, fmt, slice};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use super::arc::*;
use LodestoneError;

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
    next_id_tag: AtomicUsize,
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
        {
            let metadata = p.get_metadata_block();
            metadata.lowest_known_free_index = 0;
            metadata.next_id_tag = AtomicUsize::new(1);
        }
        let last_skip_index = p.buffer_size - PAGE_SIZE;
        // Init head of skip list
        p.make_skip_entry(SkipListStart(0), BUFFER_END, last_skip_index, true);
        // Last page is metadata and not usable as a full page-aligned chunk anyway
        p.make_skip_entry(SkipListStart(last_skip_index), 0, BUFFER_END, false);
        p
    }
}

#[derive(Debug)]
struct SkipListEntry {
    prev: usize, // absolute buffer offset of previous SKE
    id_tag: usize, // 0 if the given memory is free, unique id otherwise
    next: usize, // absolute buffer offset of next SKE
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
    pub fn make_new<T>(&self) -> Result<ArcByteSlice, LodestoneError> {
        let size = mem::size_of::<T>();
        let (_, inner) = try!(self.malloc_inner(size));
        Ok(ArcByteSlice::new(inner, self))
    }

    pub fn clone<T>(&self, from: &T) -> Result<ArcByteSlice, LodestoneError> {
        let dest = try!(self.make_new::<T>());
        let arc_index = self.arc_to_arc_inner_index(&dest);
        let dest_slice = self.index_to_byte_slice_mut(arc_index);
        let from_ptr = from as *const T;
        unsafe {
            let from_arc = try!(self.live_ptr_to_arc(mem::transmute(from_ptr)));
            dest_slice.clone_from_slice(&*from_arc);
        }
        Ok(dest)
    }

    pub fn malloc(&self, data: &[u8]) -> Result<ArcByteSlice, LodestoneError> {
        let size = data.len();
        let (idx, inner) = try!(self.malloc_inner(size));
        let dest = self.index_to_byte_slice_mut(idx);
        dest.clone_from_slice(data);
        Ok(ArcByteSlice::new(inner, self))
    }

    pub fn free(&self, arc: &ArcByteSlice) {
        let arc_index = self.arc_to_arc_inner_index(arc);
        self.free_inner(arc_index)
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

    pub fn clone_persisted_to_arc(&self, persisted: &PersistedArcByteSlice) -> Result<ArcByteSlice, LodestoneError> {
        let index = ArcByteSliceStart(persisted.arc_inner_index);
        let (_, header) = self.index_to_skip_list_header(index);
        if header.id_tag == persisted.get_id_tag() {
            let inner = self.index_to_arc_inner(index);
            Ok(ArcByteSlice::new(inner, self))
        } else {
            Err(LodestoneError::InvalidReference(
                "Can't convert to Arc. Persisted reference is no longer valid."
            ))
        }
    }
}

/// Private interface
impl Pool {
    fn malloc_inner<'a>(&'a self, size: usize) -> Result<(IndexType, &'a mut ArcByteSliceInner), LodestoneError> {
        let chunked_size = byte_align(size) + *OVERHEAD;
        let metadata = self.get_metadata_block();
        // Try to claim a block
        let (free_block_index, entry) = self.next_free_block_larger_than(chunked_size,
            SkipListStart(metadata.lowest_known_free_index));
        if free_block_index == BUFFER_END {
            return Err(LodestoneError::OutOfMemory("malloc_inner"));
        }
        // Claim as non-free
        entry.id_tag = metadata.next_id_tag.fetch_add(1, SeqCst);

        let next_index = free_block_index + chunked_size;
        let following_index = entry.next;
        assert!(next_index <= following_index);
        // If we split a block, then we need to make a new entry
        if next_index < following_index {
            self.make_skip_entry(SkipListStart(next_index),
                free_block_index, following_index, true);
            let (_, following_entry) = self.index_to_skip_list_header(SkipListStart(following_index));
            following_entry.prev = next_index;
            entry.next = next_index;
        }

        // Update known free index if necessary (only necessary if we've used the lowest)
        if free_block_index == metadata.lowest_known_free_index {
            let (idx, _) = self.next_free_block_larger_than(0, SkipListStart(free_block_index));
            metadata.lowest_known_free_index = idx;
        }

        let inner = self.index_to_arc_inner(SkipListStart(free_block_index));
        inner.init(size);
        Ok((SkipListStart(free_block_index), inner))
    }

    fn free_inner(&self, index: IndexType) {
        let metadata = self.get_metadata_block();
        let (this_idx, header) = self.index_to_skip_list_header(index);
        let prev_idx = header.prev;
        let next_idx = header.next;

        header.id_tag = 0; // Mark as free
        // Update known free index if necessary
        if this_idx < metadata.lowest_known_free_index {
            metadata.lowest_known_free_index = this_idx;
        }

        if next_idx != BUFFER_END {
            let (_, next) = self.index_to_skip_list_header(SkipListStart(next_idx));
            if next.id_tag == 0 {
                // Merge with the next item, by encompassing it
                let next_next_idx = next.next;
                header.next = next_next_idx;
                // Update the prev of the next_next_idx
                if next_next_idx != BUFFER_END {
                    let (_, next_next) = self.index_to_skip_list_header(SkipListStart(next_next_idx));
                    next_next.prev = this_idx;
                }
            }
        }
        if prev_idx != BUFFER_END {
            let (_, prev) = self.index_to_skip_list_header(SkipListStart(prev_idx));
            if prev.id_tag == 0 {
                // Merge by swallowing this item with the previous item
                let next_idx = header.next;
                prev.next = next_idx;
                // Update the prev of the following item
                if next_idx != BUFFER_END {
                    let (_, next) = self.index_to_skip_list_header(SkipListStart(next_idx));
                    next.prev = prev_idx;
                }
            }
        }
    }

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
        let offset = self.index_to_arc_offset(index);
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

    /// Overhead must already be factored into size
    fn next_free_block_larger_than<'a>(&'a self, size: usize, start_index: IndexType) -> (usize, &'a mut SkipListEntry) {
        let (idx, mut entry) = self.index_to_skip_list_header(start_index);
        if entry.id_tag == 0
           && (entry.next - idx) >= size {
            (idx, entry)
        } else if entry.next != BUFFER_END {
            self.next_free_block_larger_than(size, SkipListStart(entry.next))
        } else {
            (BUFFER_END, entry)
        }
    }

    fn live_ptr_to_arc(&self, ptr: *const u8) -> Result<ArcByteSlice, LodestoneError> {
        let index = DataStart(self.live_ptr_to_byte_index(ptr));
        let inner = self.index_to_arc_inner(index);
        Ok(ArcByteSlice::new(inner, self))
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

    fn index_to_arc_offset(&self, index: IndexType) -> usize {
        match index {
            ArcByteSliceStart(i) => i,
            DataStart(i) => i - *ARC_INNER_SIZE,
            SkipListStart(i) => i + *HEADER_SIZE,
        }
    }

    /// Find the skip list entry that precedes the given index's data
    fn index_to_skip_list_header<'a>(&'a self, index: IndexType) -> (usize, &'a mut SkipListEntry) {
        let offset = match index {
            ArcByteSliceStart(i) => i - *HEADER_SIZE,
            DataStart(i) => i - *OVERHEAD,
            SkipListStart(i) => i,
        };
        unsafe {
            (offset, mem::transmute(self.buffer.offset(offset as isize)))
        }
    }

    /// Priviledged, should not be called outside allocator package
    pub fn _inner_offset(&self, arc: &ArcByteSlice) -> usize {
        let inner_index = self.arc_to_arc_inner_index(arc);
        self.index_to_arc_offset(inner_index)
    }

    /// Priviledged, should not be called outside allocator package
    pub fn _get_id_tag(&self, arc: &ArcByteSlice) -> usize {
        let inner_index = self.arc_to_arc_inner_index(arc);
        let (_, header) = self.index_to_skip_list_header(inner_index);
        header.id_tag
    }

    fn make_skip_entry(&self, index: IndexType, prev: usize, next: usize, is_free: bool) {
        let (_, entry) = self.index_to_skip_list_header(index);
        entry.prev = prev;
        entry.next = next;
        entry.id_tag = if is_free {
            0
        } else {
            self.get_metadata_block().next_id_tag.fetch_add(1, SeqCst)
        };
    }

    fn get_debug_blocks<'a>(&'a self) -> Vec<_B> {
        let mut ret: Vec<_B> = Vec::new();
        let mut next_index: usize = 0;
        loop {
            let (idx, entry) = self.index_to_skip_list_header(SkipListStart(next_index));
            next_index = entry.next;
            let prev_index = entry.prev;
            if next_index == BUFFER_END {
                break
            }
            ret.push(_B {
                start: idx,
                capacity: next_index - idx - *OVERHEAD,
                next: next_index,
                prev: prev_index,
                is_free: entry.id_tag == 0,
            });
        }
        ret
    }
}

/// Align to the next 8 bytes
fn byte_align(size: usize) -> usize {
    let spill = if size % 8 == 0 {0} else {1};
    8 * (size/8 + spill)
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
    use super::*;

    #[test]
    #[should_panic(expected="malloc_inner")]
    fn test_oom() {
        let mut buf: [u8; 0x2000] = [0; 0x2000];
        let p = Pool::new(&mut buf[..]);
        p.malloc(&[42; 0x2000][..]).unwrap();
    }

    #[test]
    fn test_printing_empty() {
        let mut buf: [u8; 0x2000] = [0; 0x2000];
        let p = Pool::new(&mut buf[..]);
        assert_eq!(
            "Pool { buffer_size: 8192, \
                metadata: Metadata { lowest_known_free_index: 0, next_id_tag: AtomicUsize(2) }, \
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
                metadata: Metadata { lowest_known_free_index: 56, next_id_tag: AtomicUsize(3) }, \
                blocks: [\
                    _B { start: 0, capacity: 8, next: 56, prev: 18446744073709551615, is_free: false }, \
                    _B { start: 56, capacity: 12184, next: 12288, prev: 0, is_free: true }\
                ] }",
            format!("{:?}", p)
        );

        assert_eq!([0x1, 0x2, 0x3, 0x4], arc_ts1[0..4]);

        let arc_ts2 = p.malloc(&data[..]).unwrap();
        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 112, next_id_tag: AtomicUsize(4) }, \
                blocks: [\
                    _B { start: 0, capacity: 8, next: 56, prev: 18446744073709551615, is_free: false }, \
                    _B { start: 56, capacity: 8, next: 112, prev: 0, is_free: false }, \
                    _B { start: 112, capacity: 12128, next: 12288, prev: 56, is_free: true }\
                ] }",
            format!("{:?}", p)
        );

        p.free(&arc_ts1);

        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 0, next_id_tag: AtomicUsize(4) }, \
                blocks: [\
                    _B { start: 0, capacity: 8, next: 56, prev: 18446744073709551615, is_free: true }, \
                    _B { start: 56, capacity: 8, next: 112, prev: 0, is_free: false }, \
                    _B { start: 112, capacity: 12128, next: 12288, prev: 56, is_free: true }\
                ] }",
            format!("{:?}", p)
        );

        p.free(&arc_ts2);

        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 0, next_id_tag: AtomicUsize(4) }, \
                blocks: [\
                    _B { start: 0, capacity: 12240, next: 12288, prev: 18446744073709551615, is_free: true }\
                ] }",
            format!("{:?}", p)
        );
    }

    #[test]
    fn test_large_alloc() {
        let mut buf: [u8; 0x4000] = [0; 0x4000];
        let p = Pool::new(&mut buf[..]);

        // Take up > 1 page
        let arc_ts1 = p.malloc(&[42u8; 0x2000][..]).unwrap();

        assert_eq!(
            "Pool { buffer_size: 16384, \
                metadata: Metadata { lowest_known_free_index: 8240, next_id_tag: AtomicUsize(3) }, \
                blocks: [\
                    _B { start: 0, capacity: 8192, next: 8240, prev: 18446744073709551615, is_free: false }, \
                    _B { start: 8240, capacity: 4000, next: 12288, prev: 0, is_free: true }\
                ] }",
            format!("{:?}", p)
        );
    }
}
