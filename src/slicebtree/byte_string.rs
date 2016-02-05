use std::{mem, slice};
use allocator::*;

use super::entry_location::*;

#[derive(Clone, PartialEq, Debug)]
#[repr(u8)]
pub enum EntryType {
    Alias = 0xA,
    Entry,
    Deleted,
}

#[repr(C)]
pub struct ByteStringEntryAlias {
    entry_type: EntryType,
    num_segments: usize,
    // sizeof(EntryLocation) * num_segments
}

#[repr(C)]
pub struct ByteStringEntry {
    entry_type: EntryType,
    data_size: usize,
    // data_size bytes of data
}

/// Decrement the ref count for the given byte string
/// Follow aliases to release all their byte strings
pub fn release_byte_string(entry: &EntryLocation, pool: &Pool) {
    match get_entry_type(entry, pool) {
        EntryType::Entry => {
            pool.release(entry.page_index);
        },
        EntryType::Deleted => {},
        EntryType::Alias => {
            for e in get_aliased_entries(entry, pool) {
                release_byte_string(e, pool);
            }
            // Once we've followed and release all the aliased
            // strings, release this alias.
            pool.release(entry.page_index);
        }
    }
}


/// Get the type of the entry pointed to by the location
fn get_entry_type(entry: &EntryLocation, pool: &Pool) -> EntryType {
    pool[entry.page_index]
        .transmute_segment::<EntryType>(entry.offset)
        .clone()
}


/// Returns a slice of the entries which are aliased by the given entry
pub fn get_aliased_entries<'a>(entry: &EntryLocation, pool: &'a Pool) -> &'a[EntryLocation] {
    let header: &ByteStringEntryAlias = pool[entry.page_index]
        .transmute_segment(entry.offset);
    assert_eq!(EntryType::Alias, header.entry_type);

    let start = entry.offset + mem::size_of::<ByteStringEntryAlias>();
    let start_ptr: *const u8 = &pool[entry.page_index][start];

    unsafe {
        let start_ptr: *const EntryLocation = mem::transmute(start_ptr);
        slice::from_raw_parts(start_ptr, header.num_segments)
    }
}

/// Treates the entry location as a ByteStringEntry
/// Panics if not given the correct entry
pub fn get_slice<'a>(entry: &EntryLocation, pool: &'a Pool) -> &'a[u8] {
    let header: &ByteStringEntry = pool[entry.page_index]
        .transmute_segment(entry.offset);
    assert_eq!(EntryType::Entry, header.entry_type);

    let start = entry.offset + mem::size_of::<ByteStringEntry>();
    &pool[entry.page_index][start..start+header.data_size]
}

/// Treates the entry location as a ByteStringEntry
/// Panics if not given the correct entry
pub fn get_slice_mut<'a>(entry: &EntryLocation, pool: &'a Pool) -> &'a mut [u8] {
    let header: &ByteStringEntry = pool[entry.page_index]
        .transmute_segment(entry.offset);
    assert_eq!(EntryType::Entry, header.entry_type);

    let start = entry.offset + mem::size_of::<ByteStringEntry>();

    let page: &mut Page = pool[entry.page_index].borrow_mut();
    &mut page[start..start+header.data_size]
}

/// ITERATION
/// Provides byte string iters

pub struct ByteStringIter<'a> {
    entry: &'a EntryLocation,
    pool: &'a Pool,

    is_aliased: bool,
    current_page: &'a Page,
    curent_entry: &'a EntryLocation,
    current_string: &'a ByteStringEntry,
    alias_index: usize,
    byte_index: usize,
}

impl <'a> Iterator for ByteStringIter<'a> {
    type Item = &'a u8;

    fn next(&mut self) -> Option<&'a u8> {
        let current_byte = &self.current_page
            [self.current_entry.offset+self.byte_index];

        // Increment our count, and roll over if necessary
        self.byte_index += 1;
        if self.byte_index >= self.current_entry.data_size {
            if self.is_aliased {
                self.alias_index += 1;
                let entries = get_aliased_entries(self.entry, self.pool);
                if self.alias_index >= entries.len() {
                    return None
                }
                self.current_entry = &entries[self.alias_index];
                self.current_string = self.pool[self.current_entry.page_index]
                    .transmute_segment(self.current_entry.offset);
                self.byte_index = 0;
                self.current_page = self.pool[self.current_entry.page_index];
            } else {
                return None
            }
        }
        Some(current_byte)
    }
}
