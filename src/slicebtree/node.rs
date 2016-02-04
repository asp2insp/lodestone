use std::{mem, slice};
use allocator::*;

use super::*;
use super::entry_location::*;
use self::NodeType::*;
use self::EntryType::*;


#[repr(u8)]
pub enum NodeType {
    Meta,
    Root,
    Internal,
    Leaf,
}

/// The structure of a tree is a series of Nodes.
/// Each node is made up of at least 1 page.
/// The first page is interpreted as a NodeHeader
/// Pages pointed to by the header as data based on the
/// NodeType defined by the header.
/// If the NodeType is Root or Internal, the children
/// are interpreted as Nodes. If the NodeType is Leaf,
/// the children are interpreted as the values of the mapping.
#[repr(C)]
pub struct NodeHeader {
    node_type: NodeType,
    tx_id: usize,
    keys: [EntryLocation; B],
    children: [EntryLocation; B],
}

impl NodeHeader {
    pub fn from_entry<'a>(e: &EntryLocation, pool: &'a Pool) -> &'a mut NodeHeader {
        pool[e.page_index].borrow_mut().transmute_page_mut::<NodeHeader>()
    }

    /// Perform initial setup, such as fixing the keys/children arrays,
    /// setting the tx_id
    pub fn init(&mut self, tx: usize, node_type: NodeType) {
        for i in 0..B {
            self.keys[i] = END.clone();
            self.children[i] = END.clone();
        }
        self.node_type = node_type;
        self.tx_id = tx;
    }

    pub fn num_children(&self) -> usize {
        let mut c = 0usize;
        for entry in self.children.iter() {
            if entry == &END {
                break;
            }
            c += 1;
        }
        c
    }
}

pub fn release_node(entry: &EntryLocation, pool: &Pool) {
    let node = NodeHeader::from_entry(entry, pool);
    match node.node_type {
        Root | Internal => {
            for e in node.children.iter() {
                if *e == END {
                    break;
                }
                // ------------------------------------
                // TODO figure out when to recurse here
                // ------------------------------------
                release_node(e, pool);
            }
        },
        Leaf => {
            for e in node.children.iter() {
                if *e == END {
                    break;
                }
                release_byte_string(e, pool);
            }
        },
        _ => {},
    }
    for e in node.keys.iter() {
        if *e == END {
            break;
        }
        release_byte_string(e, pool);
    }
}

/// Decrement the ref count for the given byte string
pub fn release_byte_string(entry: &EntryLocation, pool: &Pool) {
    match get_entry_type(entry, pool) {
        Entry => pool.release(entry.page_index),
        Deleted => {},
        Alias => {
            for e in get_aliased_entries(entry, pool) {
                release_byte_string(e, pool);
            }
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
fn get_aliased_entries<'a>(entry: &EntryLocation, pool: &'a Pool) -> &'a[EntryLocation] {
    let header: &ByteStringEntryAlias = pool[entry.page_index]
        .transmute_segment(entry.offset);

    let start = entry.offset + mem::size_of::<ByteStringEntryAlias>();
    let start_ptr: *const u8 = &pool[entry.page_index][start];

    unsafe {
        let start_ptr: *const EntryLocation = mem::transmute(start_ptr);
        slice::from_raw_parts(start_ptr, header.num_segments)
    }
}

#[derive(Clone)]
#[repr(u8)]
pub enum EntryType {
    Alias,
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

#[test]
fn test_invariants() {
    println!("CHECK {:?} < {:?}?", mem::size_of::<NodeHeader>(), mem::size_of::<Page>());
    assert!(mem::size_of::<NodeHeader>() < mem::size_of::<Page>());
}
