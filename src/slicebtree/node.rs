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

impl NodeHeader {
    /// Insert in an append only/immutable fashion
    fn leaf_node_insert_non_full(&mut self, tx_id: usize, key: &[u8], value: &[u8], pool: &Pool) -> Result<EntryLocation, &'static str> {
        let page_hint = if self.num_keys > 0 {
            get_page_hint(&self.keys, self.num_keys-1)
        } else {
            NO_HINT
        };
        let key_loc = try!(alloc_with_contents(page_hint, key, pool));

        let page_hint = if self.num_children > 0 {
            get_page_hint(&self.children, self.num_children-1)
        } else {
            NO_HINT
        };
        let val_loc = try!(alloc_with_contents(page_hint, value, pool));

        let loc = try!(self.clone(tx_id, pool));
        let node = NodeHeader::from_entry(&loc, pool);
        insert_child_non_full(node, &key_loc, &val_loc, pool);
        Ok(loc)
    }

    fn clone(&self, tx_id: usize, pool: &Pool) -> Result<EntryLocation, &'static str> {
        let loc = EntryLocation {
            page_index: try!(pool.alloc()),
            offset: 0,
        };
        let node = NodeHeader::from_entry(&loc, pool);

        // Copy over values
        node.node_type = self.node_type.clone();
        node.tx_id = tx_id;
        node.num_keys = self.num_keys;
        node.num_children = self.num_children;

        for i in 0..node.num_keys {
            node.keys[i] = self.keys[i].clone();
        }
        for i in 0..node.num_children {
            node.children[i] = self.children[i].clone();
        }
        Ok(loc)
    }
}

/// Return the page where the last item points as a hint
/// for where the next item should be allocated.
fn get_page_hint(array: &[EntryLocation; B], last_index: usize) -> usize {
    array[last_index].page_index
}

/// Binary search impl for finding the location at which the given
/// key should be inserted
fn find_insertion_index(n: &NodeHeader, key_loc: &EntryLocation, pool: &Pool) -> usize {
    if n.num_keys == 0 {
        return 0
    }
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

/// Internal nodes have keys in order. The corresponding
/// child to a key index is the node that contains values
/// less than or equal to the key.
/// The index above is the child with values greater than the given key.
/// Thus, internal nodes can hold up to B-1 keys and B children.
/// Leaf nodes have a 1-1 correspndence of key to value, holding
/// up to B keys and B children.
/// Precondition: The node must have enough space
/// The memory should already be allocated, this
/// just inserts the reference in the correct location.
fn insert_child_non_full(n: &mut NodeHeader,
        key_loc: &EntryLocation,
      child_loc: &EntryLocation,
           pool: &Pool) {
    // First find the index where we want to insert
    let index = find_insertion_index(n, key_loc, pool);
    n.num_children += 1;
    insert_into(&mut n.children, n.num_children, child_loc, index);
    n.num_keys += 1;
    insert_into(&mut n.keys, n.num_keys, key_loc, index);
}

/// Precondition: The node must have enough space
/// The memory should already be allocated, this
/// just inserts the reference in the correct location.
fn insert_into(array: &mut [EntryLocation; B],
          array_size: usize,
                 loc: &EntryLocation,
               index: usize) {
    // First find the index where we want to insert
    for i in (index+1..array_size).rev() {
        array[i] = array[i-1].clone();
    }
    array[index] = loc.clone();
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
fn test_insert_internal_non_full() {
    let mut buf = [0u8; 0x6100];
    let pool = Pool::new(&mut buf);
    let n = pool.alloc().unwrap();
    let n = EntryLocation {
        page_index: n,
        offset: 0,
    };
    let key: Vec<u8> = "hello".bytes().collect();
    let value: Vec<u8> = "world".bytes().collect();

    let n = NodeHeader::from_entry(&n, &pool);
    n.init(0, MemType::Leaf);
    assert_eq!(0, n.num_keys);
    assert_eq!(0, n.num_children);

    let n2 = n.leaf_node_insert_non_full(1, &key[..], &value[..], &pool).unwrap();
    let n2 = NodeHeader::from_entry(&n2, &pool);

    assert_eq!(0, n.num_keys);
    assert_eq!(0, n.num_children);
    assert_eq!(1, n2.num_keys);
    assert_eq!(1, n2.num_children);

    assert_eq!(&key[..], get_slice(&n2.keys[0], &pool));
    assert_eq!(&value[..], get_slice(&n2.children[0], &pool));
}

#[test]
fn test_invariants() {
    println!("CHECK {:?} < {:?}?", mem::size_of::<NodeHeader>(), *PAGE_SIZE);
    assert!(mem::size_of::<NodeHeader>() < *PAGE_SIZE);
}
