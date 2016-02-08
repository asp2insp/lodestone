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

/// If the entry is an alias, contents_size is the number
/// of segments in the alias. If it's an entry, contents_size
/// represents the number of bytes in the entry.
#[derive(Clone, PartialEq, Debug)]
#[repr(C)]
pub struct ByteStringEntry {
    entry_type: EntryType,
    contents_size: usize,
    // contents_size bytes of data
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
    let header: &ByteStringEntry = pool[entry.page_index]
        .transmute_segment(entry.offset);
    assert_eq!(EntryType::Alias, header.entry_type);

    let start = entry.offset + mem::size_of::<ByteStringEntry>();
    let start_ptr: *const u8 = &pool[entry.page_index][start];

    unsafe {
        let start_ptr: *const EntryLocation = mem::transmute(start_ptr);
        slice::from_raw_parts(start_ptr, header.contents_size)
    }
}

/// Treates the entry location as a ByteStringEntry
/// Panics if not given the correct entry
pub fn get_slice<'a>(entry: &EntryLocation, pool: &'a Pool) -> &'a[u8] {
    let header: &ByteStringEntry = pool[entry.page_index]
        .transmute_segment(entry.offset);
    assert_eq!(EntryType::Entry, header.entry_type);

    let start = entry.offset + mem::size_of::<ByteStringEntry>();
    &pool[entry.page_index][start..start+header.contents_size]
}

/// Treates the entry location as a ByteStringEntry
/// Panics if not given the correct entry
pub fn get_slice_mut<'a>(entry: &EntryLocation, pool: &'a Pool) -> &'a mut [u8] {
    let header: &ByteStringEntry = pool[entry.page_index]
        .transmute_segment(entry.offset);
    assert_eq!(EntryType::Entry, header.entry_type);

    let start = entry.offset + mem::size_of::<ByteStringEntry>();

    let page: &mut Page = pool[entry.page_index].borrow_mut();
    &mut page[start..start+header.contents_size]
}

/// Get the iter over the memory represented by the bytestring
pub fn get_iter<'a>(entry: &'a EntryLocation, pool: &'a Pool) -> ByteStringIter<'a> {
    let page = &pool[entry.page_index];
    match get_entry_type(entry, pool) {
        EntryType::Entry => {
            ByteStringIter {
                entry: entry,
                pool: pool,

                is_aliased: false,
                current_page: page,
                current_entry: entry,
                current_string: pool[entry.page_index].transmute_segment(entry.offset),
                alias_index: 0,
                byte_index: 0,
            }
        },
        EntryType::Alias => {
            let first_entry = &get_aliased_entries(entry, pool)[0];
            ByteStringIter {
                entry: entry,
                pool: pool,

                is_aliased: true,
                current_page: page,
                current_entry: first_entry,
                current_string:  pool[first_entry.page_index].transmute_segment(first_entry.offset),
                alias_index: 0,
                byte_index: 0,
            }
        },
        EntryType::Deleted => {
            panic!("get_iter called on a deleted entry");
        },
    }
}

/// ITERATION
/// Provides byte string iters

pub struct ByteStringIter<'a> {
    entry: &'a EntryLocation,
    pool: &'a Pool,

    is_aliased: bool,
    current_page: &'a Page,
    current_entry: &'a EntryLocation,
    current_string: &'a ByteStringEntry,
    alias_index: usize,
    byte_index: usize,
}

impl <'a> Iterator for ByteStringIter<'a> {
    type Item = &'a u8;

    fn next(&mut self) -> Option<&'a u8> {
        let current_byte = &self.current_page
            [self.current_entry.offset+mem::size_of::<ByteStringEntry>()+self.byte_index];

        // Increment our count, and roll over if necessary
        self.byte_index += 1;
        println!("{}/{}", self.byte_index, self.current_string.contents_size);
        if self.byte_index >= self.current_string.contents_size {
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
                self.current_page = &self.pool[self.current_entry.page_index];
            } else {
                return None
            }
        }
        Some(current_byte)
    }
}

#[test]
fn test_get_slice_mut() {
    let mut buf = [0u8; 0x2000];
    let pool = Pool::new(&mut buf);

    let page_index = pool.alloc().unwrap();
    let mut p: &Page = &pool[page_index];
    let loc = EntryLocation {
        page_index: page_index,
        offset: 0,
    };

    let mut entry = p.transmute_page_mut::<ByteStringEntry>();
    entry.entry_type = EntryType::Entry;
    entry.contents_size = 10;

    // Init the data as a slice
    for iv in get_slice_mut(&loc, &pool).iter_mut().enumerate() {
        *iv.1 = iv.0 as u8;
    }

    // check the data via iterator
    get_iter(&loc, &pool).zip((0u8..)).fold(true, |b, actual_exp| {
        assert_eq!(*actual_exp.0, actual_exp.1);
        b && *actual_exp.0 == actual_exp.1
    });
}