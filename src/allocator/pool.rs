use std::{ptr, mem};
use std::sync::atomic;
use std::sync::atomic::Ordering::{SeqCst};

pub const PAGE_SIZE: usize = 4096;

lazy_static! {
    pub static ref HEADER_FOOTER_SIZE: usize = mem::size_of::<AtomicUsize>();
    pub static ref FIRST_AND_LAST_CONTENT_SIZE: usize = PAGE_SIZE - HEADER_FOOTER_SIZE;
    pub static ref SKIP_LIST_ENTRY_SIZE: usize = mem::size_of::<SkipListEntry>();
    pub static ref SINGLE_PAGE_CONTENT_SIZE: usize = PAGE_SIZE - HEADER_FOOTER_SIZE * 2;
    pub static ref ARC_INNER_SIZE: usize = mem::size_of::<ArcInner>();

}

pub struct Pool {
    buffer: *mut u8,
    buffer_size: usize,
}

/// Arcs are free floating and are not persisted
pub struct Arc {
    _ptr: Shared<ArcInner>,
}

/// ArcInners live inside of the buffer and are persisted
struct ArcInner {
    strong: atomic::AtomicUsize,
    weak: atomic::AtomicUsize,
    byte_index: usize,
    size: usize,
}

struct SkipListEntry {
    prev: AtomicUsize, // absolute buffer offset of previous SKE
    next: AtomicUsize, // absolute buffer offset of next SKE
}

use self::IndexType::*;
enum IndexType {
    ArcStart(usize),
    DataStart(usize),
}

/// Private interface
impl Pool {
    fn malloc(&mut self, size: usize) -> Arc {
        Arc {
            _ptr: Shared::null,
        }
    }

    fn free(&mut self, arc: Arc) {

    }

    fn live_ptr_to_byte_index(&self, ptr: *const u8) -> usize {
        let obj_addr = obj as usize;
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

    fn byte_index_to_live_ptr(&mut self, byte_index: usize) -> *mut u8 {
        self.buffer.offset(byte_index)
    }

    fn header_for_byte_index<'a>(&'a mut self, index: IndexType) -> &'a mut SkipListEntry {
        let offset = match index {
            ArcStart(i) => i - *SKIP_LIST_ENTRY_SIZE,
            DataStart(i) => i - *ARC_INNER_SIZE - *SKIP_LIST_ENTRY_SIZE,
        };
        unsafe {
            mem::transmute(self.buffer.offset(offset))
        }
    }

    fn footer_for_byte_index<'a>(&'a mut self, index: IndexType) -> &'a mut SkipListEntry {
        let entry = self.header_for_byte_index(index);
        unsafe {
            let ptr = self.byte_index_to_live_ptr(entry.next);
            mem::transmute(ptr)
        }
    }
}
