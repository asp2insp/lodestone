use std::{mem, slice, cmp, ptr};
use allocator::*;

use super::*;
use super::entry_location::*;

pub const NO_HINT: usize = 0xFFFFFFFF;

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

impl ByteStringEntry {
    pub fn size_on_disk(&self) -> usize {
        match self.entry_type {
            MemType::Entry => {
                *BSE_HEADER_SIZE + self.contents_size
            },
            MemType::Alias => {
                *BSE_HEADER_SIZE + self.contents_size * *EL_PTR_SIZE
            },
            _ => {
                panic!("size_on_disk called on non-disk entry")
            }
        }
    }
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

    let start = entry.offset + *BSE_HEADER_SIZE;
    let start_ptr: *const u8 = &pool[entry.page_index][start];

    unsafe {
        let start_ptr: *const EntryLocation = mem::transmute(start_ptr);
        slice::from_raw_parts(start_ptr, header.contents_size)
    }
}

#[inline]
fn calc_num_chunks(size: usize) -> usize {
    let spill = if size % *BSE_CHUNK_SIZE == 0 {0} else {1};
    size / *BSE_CHUNK_SIZE + spill
}

/// Allocate a new byte string and populate it with the
/// given contents. Returns Ok if the operation succeeded and Err if the
/// allocation failed
pub fn alloc_with_contents(page_hint: usize, contents: &[u8], pool: &Pool) -> Result<EntryLocation, &'static str> {
    if page_hint != NO_HINT {
        let page = &pool[page_hint];
        let offset = next_free_offset(page);
        let free_space = PAGE_SIZE - offset;
        let required_space = *BSE_HEADER_SIZE + contents.len();
        // Check to see if we can append to the given page.
        if free_space >= required_space {
            return append_to_with_contents(page_hint, contents, pool)
        }
    }
    // go for a new page
    if contents.len() > *BSE_CHUNK_SIZE {
        alias_alloc_with_contents(contents, pool)
    } else {
        let index = try!(pool.alloc());
        append_to_with_contents(index, contents, pool)
    }
}

/// Allocate a new byte string that requires aliasing and populate it with the
/// given contents. Returns Ok if the operation succeeded and Err if the
/// allocation failed
pub fn alias_alloc_with_contents(contents: &[u8], pool: &Pool) -> Result<EntryLocation, &'static str> {
    let num_chunks = calc_num_chunks(contents.len());

    let page_index = try!(pool.alloc());
    let page = &pool[page_index];
    let mut alias_header = page.transmute_page_mut::<ByteStringEntry>();
    alias_header.entry_type = MemType::Alias;
    alias_header.contents_size = num_chunks;

    let mut offset = *BSE_HEADER_SIZE;
    let mut contents_offset = 0;
    for _ in 0..num_chunks {
        let index = try!(pool.alloc());
        let mut next = contents_offset + *BSE_CHUNK_SIZE;
        if next > contents.len() {
            next = contents.len();
        }
        let loc = try!(append_to_with_contents(index,
            &contents[contents_offset..next],
            pool));
        let mut el_ptr = page.transmute_segment_mut::<EntryLocation>(offset);
        el_ptr.page_index = loc.page_index;
        el_ptr.offset = loc.offset;

        contents_offset = next;
        offset += *EL_PTR_SIZE;
    }
    Ok(EntryLocation {
        page_index: page_index,
        offset: 0,
    })
}

/// Allocate a new byte string in the given page and populate it with the
/// given contents. Returns Ok if the operation succeeded and Err if the
/// page does not have capacity for the value.
pub fn append_to_with_contents(page_index: usize, contents: &[u8], pool: &Pool) -> Result<EntryLocation, &'static str> {
    let page = &pool[page_index];
    let offset = next_free_offset(page);
    let free_space = PAGE_SIZE - offset;

    if free_space < *BSE_HEADER_SIZE + contents.len() {
        Err("Not enough room")
    } else {
        let mut header = page.transmute_segment_mut::<ByteStringEntry>(offset);
        header.entry_type = MemType::Entry;
        header.contents_size = contents.len();

        unsafe {
            let to = &mut page.borrow_mut()[offset+*BSE_HEADER_SIZE] as *mut u8;
            let from = contents.as_ptr();
            ptr::copy_nonoverlapping(from, to, contents.len());
        }
        Ok(EntryLocation {
            page_index: page_index,
            offset: offset,
        })
    }
}

// Treat the given page as a set of nodes, return the remaining
// free space in the page.
pub fn free_space_entry_page(page: &Page) -> usize {
    PAGE_SIZE - next_free_offset(page)
}

// Find the index of the start of free space
pub fn next_free_offset(page: &Page) -> usize {
    let mut offset = 0usize;
    loop {
        let header = page.transmute_segment::<ByteStringEntry>(offset);
        match header.entry_type {
            MemType::Entry | MemType::Alias | MemType::Deleted => {
                offset += header.size_on_disk();
            },
            _ => break,
        }
    }
    offset
}

/// Treates the entry location as a ByteStringEntry
/// Panics if not given the correct entry
pub fn get_slice<'a>(entry: &EntryLocation, pool: &'a Pool) -> &'a[u8] {
    let header = get_entry_header(entry, pool);
    assert_eq!(MemType::Entry, header.entry_type);

    let start = entry.offset + *BSE_HEADER_SIZE;
    &pool[entry.page_index][start..start+header.contents_size]
}

/// Treates the entry location as a ByteStringEntry
/// Panics if not given the correct entry
pub fn get_slice_mut<'a>(entry: &EntryLocation, pool: &'a Pool) -> &'a mut [u8] {
    let header = get_entry_header(entry, pool);
    assert_eq!(MemType::Entry, header.entry_type);

    let start = entry.offset + *BSE_HEADER_SIZE;

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
                current_page: &pool[first_entry.page_index],
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
            panic!("get_iter called on a Node instead of an entry: {:?}", entry);
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
        // Roll over if necessary
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
        let current_byte = &self.current_page
            [self.current_entry.offset+*BSE_HEADER_SIZE+self.byte_index];
        self.byte_index += 1;
        Some(current_byte)
    }
}

#[test]
fn test_alias_alloc_with_contents() {
    let mut buf = [0u8; 0x5100];
    let pool = Pool::new(&mut buf);

    // Should take up 4 pages
    let test_contents = [42u8; 0x3000];
    assert_eq!(4, calc_num_chunks(test_contents.len()));

    let alias_loc = alias_alloc_with_contents(&test_contents[..], &pool).unwrap();
    let alias_header = get_entry_header(&alias_loc, &pool);

    assert_eq!(4, alias_header.contents_size);
    assert_eq!(MemType::Alias, alias_header.entry_type);

    let chunk1 = pool[1].transmute_page::<ByteStringEntry>();
    assert_eq!(MemType::Entry, chunk1.entry_type);
    assert_eq!(*BSE_CHUNK_SIZE, chunk1.contents_size);

    let chunk2 = pool[2].transmute_page::<ByteStringEntry>();
    assert_eq!(MemType::Entry, chunk2.entry_type);
    assert_eq!(*BSE_CHUNK_SIZE, chunk2.contents_size);

    let chunk3 = pool[3].transmute_page::<ByteStringEntry>();
    assert_eq!(MemType::Entry, chunk3.entry_type);
    assert_eq!(*BSE_CHUNK_SIZE, chunk3.contents_size);

    let chunk4 = pool[4].transmute_page::<ByteStringEntry>();
    assert_eq!(MemType::Entry, chunk4.entry_type);
    assert_eq!(72, chunk4.contents_size); // spillover

    for u in get_iter(&alias_loc, &pool) {
        assert_eq!(42u8, *u);
    }

    let count = get_iter(&alias_loc, &pool).count();
    assert_eq!(0x3000, count);

    // Test error case
    assert_eq!(Err("OOM"),
        alias_alloc_with_contents(&test_contents[..], &pool));
}

#[test]
fn test_alias_alloc_then_free() {
    let mut buf = [0u8; 0x5100];
    let pool = Pool::new(&mut buf);

    // Should take up 4 pages
    let test_contents = [42u8; 0x3000];
    assert_eq!(4, calc_num_chunks(test_contents.len()));

    let alias_loc = alias_alloc_with_contents(&test_contents[..], &pool).unwrap();

    // This should be the only reference on the memory
    release_byte_string(&alias_loc, &pool);

    // So we should be able to reclaim it
    assert!(alias_alloc_with_contents(&test_contents[..], &pool).is_ok());
}

#[test]
fn test_append_to_with_contents() {
    let mut buf = [0u8; 0x1100];
    let pool = Pool::new(&mut buf);

    let page_index = pool.alloc().unwrap();
    let page = &pool[page_index];

    for i in 0..10 {
        let remaining_space = PAGE_SIZE - 20*i;

        assert_eq!(remaining_space, free_space_entry_page(page));
        assert_eq!(20*i, next_free_offset(page));

        let loc = append_to_with_contents(page_index, &[1u8, 2u8, 3u8, 4u8][..], &pool).unwrap();
        let entry = get_entry_header(&loc, &pool);

        assert_eq!(20*i, loc.offset);
        assert_eq!(page_index, loc.page_index);
        assert_eq!(4, entry.contents_size);

        assert_eq!([1u8, 2u8, 3u8, 4u8][..], *get_slice(&loc, &pool));
    }

    assert_eq!(3888, free_space_entry_page(page));

    // Test error case
    assert_eq!(Err("Not enough room"),
        append_to_with_contents(page_index, &[42u8; 4097][..], &pool));
}

#[test]
fn test_free_space_entry_page() {
    let mut buf = [0u8; 0x1100];
    let pool = Pool::new(&mut buf);

    let page_index = pool.alloc().unwrap();
    let page = &pool[page_index];
    let mut loc = EntryLocation {
        page_index: page_index,
        offset: 0,
    };

    for i in 0..10 {
        let remaining_space = PAGE_SIZE - 36*i;
        assert_eq!(remaining_space, free_space_entry_page(page));
        assert_eq!(36*i, next_free_offset(page));

        let mut entry = get_entry_header(&loc, &pool);
        entry.entry_type = MemType::Entry;
        entry.contents_size = 20;

        loc.offset += 36;
    }

    assert_eq!(3728, free_space_entry_page(page));
}

#[test]
fn get_aliased_string() {
    let mut buf = [0u8; 0x1100];
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
    let mut buf = [0u8; 0x1100];
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
