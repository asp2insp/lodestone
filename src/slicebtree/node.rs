use std::mem;
use allocator::*;

use super::*;
use super::entry_location::*;


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
    pub fn from_entry<'a>(e: &EntryLocation, pool: &'a mut Pool) -> &'a mut NodeHeader {
        pool[e.page_index].transmute_page_mut::<NodeHeader>()
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
