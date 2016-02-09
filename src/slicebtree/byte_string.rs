use std::{mem, slice, cmp};
use allocator::*;

use super::entry_location::*;


/// If the entry is an alias, contents_size is the number
/// of segments in the alias. If it's an entry, contents_size
/// represents the number of bytes in the entry.
#[derive(Clone, PartialEq, Debug)]
#[repr(C)]
pub struct ByteStringEntry {
    entry_type: MemType,
    contents_size: usize,
    // contents_size bytes of data
}

// Lexicographically compare the entries pointed to by e1 and e2
pub fn cmp(e1: &EntryLocation, e2: &EntryLocation, p: &Pool) -> cmp::Ordering {
    get_iter(e1, p).cmp(get_iter(e2, p))
}

/// Decrement the ref count for the given byte string
/// Follow aliases to release all their byte strings
pub fn release_byte_string(entry: &EntryLocation, pool: &Pool) {
    match get_entry_header(entry, pool).entry_type {
        MemType::Entry => {
            pool.release(entry.page_index);
        },
        MemType::Deleted => {},
        MemType::Alias => {
            for e in get_aliased_entries(entry, pool) {
                release_byte_string(e, pool);
            }
            // Once we've followed and release all the aliased
            // strings, release this alias.
            pool.release(entry.page_index);
        },
        _ => {},
    }
}

/// Get the header for the entry pointed to by the given location
fn get_entry_header<'a>(entry: &EntryLocation, pool: &'a Pool) -> &'a mut ByteStringEntry {
    pool[entry.page_index].transmute_segment_mut::<ByteStringEntry>(entry.offset)
}

/// Returns a slice of the entries which are aliased by the given entry
pub fn get_aliased_entries<'a>(entry: &EntryLocation, pool: &'a Pool) -> &'a[EntryLocation] {
    let header = get_entry_header(entry, pool);
    assert_eq!(MemType::Alias, header.entry_type);

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
    let header = get_entry_header(entry, pool);
    assert_eq!(MemType::Entry, header.entry_type);

    let start = entry.offset + mem::size_of::<ByteStringEntry>();
    &pool[entry.page_index][start..start+header.contents_size]
}

/// Treates the entry location as a ByteStringEntry
/// Panics if not given the correct entry
pub fn get_slice_mut<'a>(entry: &EntryLocation, pool: &'a Pool) -> &'a mut [u8] {
    let header = get_entry_header(entry, pool);
    assert_eq!(MemType::Entry, header.entry_type);

    let start = entry.offset + mem::size_of::<ByteStringEntry>();

    let page: &mut Page = pool[entry.page_index].borrow_mut();
    &mut page[start..start+header.contents_size]
}

/// Get the iter over the memory represented by the bytestring
pub fn get_iter<'a>(entry: &'a EntryLocation, pool: &'a Pool) -> ByteStringIter<'a> {
    let page = &pool[entry.page_index];
    let header = get_entry_header(entry, pool);
    match header.entry_type {
        MemType::Entry => {
            ByteStringIter {
                entry: entry,
                pool: pool,

                is_aliased: false,
                current_page: page,
                current_entry: entry,
                current_string: header,
                alias_index: 0,
                byte_index: 0,
            }
        },
        MemType::Alias => {
            let first_entry = &get_aliased_entries(entry, pool)[0];
            ByteStringIter {
                entry: entry,
                pool: pool,

                is_aliased: true,
                current_page: page,
                current_entry: first_entry,
                current_string: get_entry_header(first_entry, pool),
                alias_index: 0,
                byte_index: 0,
            }
        },
        MemType::Deleted => {
            panic!("get_iter called on a deleted entry");
        },
        _ => {
            panic!("get_iter called on a Node instead of an entry");
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
fn get_aliased_string() {
    let mut buf = [0u8; 0x2000];
    let pool = Pool::new(&mut buf);

    let page_index = pool.alloc().unwrap();
    let p: &Page = &pool[page_index];

    let alias_loc = EntryLocation {
        page_index: page_index,
        offset: 0,
    };

    let mut alias = get_entry_header(&alias_loc, &pool);
    alias.entry_type = MemType::Alias;
    alias.contents_size = 2;

    let loc_size = mem::size_of::<EntryLocation>();
    let header_size = mem::size_of::<EntryLocation>();

    let mut entry1_loc = p.transmute_segment_mut::<EntryLocation>(header_size);
    entry1_loc.page_index = page_index;
    entry1_loc.offset = header_size+2*loc_size;
    let mut entry1 = get_entry_header(entry1_loc, &pool);
    entry1.entry_type = MemType::Entry;
    entry1.contents_size = 10;

    let mut entry2_loc = p.transmute_segment_mut::<EntryLocation>(header_size+loc_size);
    entry2_loc.page_index = page_index;
    entry2_loc.offset = 2*header_size+2*loc_size+10;
    let mut entry2 = get_entry_header(entry2_loc, &pool);
    entry2.entry_type = MemType::Entry;
    entry2.contents_size = 10;

    // Init the data as a slice
    for iv in get_slice_mut(&entry1_loc, &pool).iter_mut().enumerate() {
        *iv.1 = iv.0 as u8;
    }
    for iv in get_slice_mut(&entry2_loc, &pool).iter_mut().enumerate() {
        *iv.1 = (iv.0 as u8) + 10u8;
    }

    // check the data via iterator
    get_iter(&alias_loc, &pool).zip((0u8..)).fold(true, |b, actual_exp| {
        assert_eq!(*actual_exp.0, actual_exp.1);
        b && *actual_exp.0 == actual_exp.1
    });
}

#[test]
fn test_get_slice_mut_and_iter() {
    let mut buf = [0u8; 0x2000];
    let pool = Pool::new(&mut buf);

    let page_index = pool.alloc().unwrap();
    let loc = EntryLocation {
        page_index: page_index,
        offset: 0,
    };

    let mut entry = get_entry_header(&loc, &pool);
    entry.entry_type = MemType::Entry;
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
