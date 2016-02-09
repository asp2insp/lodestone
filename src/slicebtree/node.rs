use std::{mem,cmp};
use allocator::*;

use super::*;
use super::entry_location::*;
use super::byte_string::*;


/// The Node exposes a mutable API. Immutability/Append only
/// is left to the wrapping tree implementation


/// The structure of a tree is a series of Nodes.
/// Each node is made up of at least 1 page.
/// The first page is interpreted as a NodeHeader
/// Pages pointed to by the header as data based on the
/// MemType defined by the header.
/// If the MemType is Root or Internal, the children
/// are interpreted as Nodes. If the MemType is Leaf,
/// the children are interpreted as the values of the mapping.
#[repr(C)]
pub struct NodeHeader {
    node_type: MemType,
    tx_id: usize,
    num_keys: usize,
    keys: [EntryLocation; B],
    num_children: usize,
    children: [EntryLocation; B],
}

impl NodeHeader {
    pub fn from_entry<'a>(e: &EntryLocation, pool: &'a Pool) -> &'a mut NodeHeader {
        pool[e.page_index].borrow_mut().transmute_page_mut::<NodeHeader>()
    }

    /// Perform initial setup, such as fixing the keys/children arrays,
    /// setting the tx_id
    pub fn init(&mut self, tx: usize, node_type: MemType) {
        self.num_keys = 0;
        self.num_children = 0;
        self.node_type = node_type;
        self.tx_id = tx;
    }
}

/// Binary search impl for finding the location at which the given
/// key should be inserted
fn find_insertion_index(n: &NodeHeader, key_loc: &EntryLocation, pool: &Pool) -> usize {
    let mut top = n.num_keys;
    let mut bottom = 0;
    let mut i = top/2;

    loop {
        match cmp(key_loc, &n.keys[i], pool) {
            cmp::Ordering::Equal => break,
            cmp::Ordering::Less => top = i,
            cmp::Ordering::Greater => bottom = i,
        }
        if top < bottom {
            break;
        }
        i = bottom + (top + bottom)/2;
    }
    i
}

/// Precondition: The node must have enough space
fn insert_non_full(n: &mut NodeHeader, key_loc: &EntryLocation, pool: &Pool) {
    // First find the index where we want to insert
    let index = find_insertion_index(n, key_loc, pool);
    n.num_keys += 1;
    for i in (index..n.num_keys).rev() {
        n.keys[i] = n.keys[i-1].clone();
    }
    n.keys[index] = key_loc.clone();
}

/// Release the memory "owned" by the given node
fn release_node_contents(entry: &EntryLocation, pool: &Pool) {
    let node = NodeHeader::from_entry(entry, pool);
    match node.node_type {
        MemType::Root | MemType::Internal => {
            for e in node.children.iter().take(node.num_children) {
                let should_recurse = pool.release(e.page_index);
                // If this node is now dead, we can recursively
                // remove its contents
                if should_recurse {
                    release_node_contents(e, pool);
                }
            }
        },
        MemType::Leaf => {
            for e in node.children.iter().take(node.num_children) {
                release_byte_string(e, pool);
            }
        },
        _ => {},
    }
    for e in node.keys.iter().take(node.num_keys) {
        release_byte_string(e, pool);
    }
}

// Treat the given page as a set of nodes, return the remaining
// free space in the page.
pub fn free_space_node_page(_: &Page) -> usize {
    0 // Nodes are designed to fill an entire page
}

#[test]
fn test_invariants() {
    println!("CHECK {:?} < {:?}?", mem::size_of::<NodeHeader>(), mem::size_of::<Page>());
    assert!(mem::size_of::<NodeHeader>() < mem::size_of::<Page>());
}
