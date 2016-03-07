use std::{cmp,iter};
use allocator::*;

use super::*;
use super::entry_location::*;
use super::byte_string::*;


/// TODO: revise this description
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
    page_index: usize,
    tx_id: usize,
    num_keys: usize,
    keys: [EntryLocation; B],
    num_children: usize,
    children: [EntryLocation; B],
}

/// Public interface
impl NodeHeader {
    pub fn from_entry<'a>(e: &EntryLocation, pool: &'a Pool) -> &'a mut NodeHeader {
        pool[e.page_index].borrow_mut().transmute_page_mut::<NodeHeader>()
    }

    /// Perform initial setup, such as fixing the keys/children arrays,
    /// setting the tx_id
    pub fn init(&mut self, page_index: usize, tx: usize, node_type: MemType) {
        self.page_index = page_index;
        self.num_keys = 0;
        self.num_children = 0;
        self.node_type = node_type;
        self.tx_id = tx;
    }
}

/// Private interface
impl NodeHeader {
    fn alloc_node<'a>(node_type: MemType, tx: usize, pool: &'a Pool) -> Result<&'a mut NodeHeader, &'static str> {
        let loc = EntryLocation {
            page_index: try!(pool.alloc()),
            offset: 0,
        };
        let mut node = NodeHeader::from_entry(&loc, pool);
        node.init(loc.page_index, tx, node_type);
        Ok(node)
    }

    /// Splits the node in half, immutably, returning a tuple of the
    /// (
    ///    new_top_half,
    ///    new_bottom_half,
    ///    mid_key,
    /// )
    /// For an internal node, they layout is as follows:
    ///   key1 : key2 : key3 : key4
    ///   /    |      |      |     \
    /// c1     c2     c3     c4    c5
    /// So here, I want to split into
    /// key1 : key2       key3 : key4
    ///  /   |            /    |    \
    /// c1   c2           c3   c4   c5
    /// So mid_key = 2 = num_keys/2 so we get [0, 1] and [2, 3]
    /// mid_child = 2 = num_keys/2 so we get [0, 1] and [2, 3, 4]
    fn split<'a>(&'a self, tx_id: usize, pool: &'a Pool)
        -> Result<(&'a NodeHeader, &'a NodeHeader, ByteStringIter<'a>), &'static str> {
        let new_bottom_half = try!(NodeHeader::alloc_node(self.node_type.clone(), tx_id, pool));
        let new_top_half = try!(NodeHeader::alloc_node(self.node_type.clone(), tx_id, pool));

        // Find midpoint based on type
        let midpoint = self.num_keys/2;
        // Copy over values
        for i in 0..midpoint {
            new_bottom_half.keys[i] = self.keys[i].clone();
            // Retain the page
            pool.retain(new_bottom_half.keys[i].page_index);
        }
        for i in 0..midpoint {
            new_bottom_half.children[i] = self.children[i].clone();
            // Retain the page
            pool.retain(new_bottom_half.children[i].page_index);
        }
        for i in midpoint..self.num_keys {
            new_top_half.keys[i-midpoint] = self.keys[i].clone();
            // Retain the page
            pool.retain(new_top_half.keys[i-midpoint].page_index);
        }
        for i in midpoint..self.num_children {
            new_top_half.children[i-midpoint] = self.children[i].clone();
            // Retain the page
            pool.retain(new_top_half.children[i-midpoint].page_index);
        }
        // Copy over metadata
        new_bottom_half.num_keys = midpoint;
        new_bottom_half.num_children = midpoint;
        new_top_half.num_keys = self.num_keys - midpoint;
        new_top_half.num_children = self.num_children - midpoint;

        Ok((
            new_bottom_half,
            new_top_half,
            get_iter(&self.keys[midpoint], pool),
        ))
    }

    fn clone(&self, tx_id: usize, pool: &Pool) -> Result<EntryLocation, &'static str> {
        let node = try!(NodeHeader::alloc_node(self.node_type.clone(), tx_id, pool));

        // Copy over values
        node.num_keys = self.num_keys;
        node.num_children = self.num_children;

        for i in 0..node.num_keys {
            node.keys[i] = self.keys[i].clone();
            // Retain the page
            pool.retain(node.keys[i].page_index);
        }
        for i in 0..node.num_children {
            node.children[i] = self.children[i].clone();
            // Retain the page
            pool.retain(node.children[i].page_index);
        }
        Ok(node.loc())
    }

    /// Binary search impl for finding the location of the given key.
    /// Returns NOT_FOUND if the given key does not exist in the node
    fn index_of(&self, key: &[u8], pool: &Pool) -> usize {
        if self.num_keys == 0 {
            return NOT_FOUND
        }
        let mut top = self.num_keys;
        let mut bottom = 0;
        let mut i = top/2;
        let mut old_i = i;
        loop {
            match key.iter().cmp(get_iter(&self.keys[i], pool)) {
                cmp::Ordering::Equal => break,
                cmp::Ordering::Less => top = i,
                cmp::Ordering::Greater => bottom = i,
            }
            if top < bottom {
                break;
            }
            i = bottom + (top + bottom)/2;
            if i == old_i {
                break;
            } else {
                old_i = i;
            }
        }
        if key.iter().cmp(get_iter(&self.keys[i], pool)) == cmp::Ordering::Equal {
            i
        } else {
            NOT_FOUND
        }
    }

    fn loc(&self) -> EntryLocation {
        EntryLocation {
            page_index: self.page_index,
            offset: 0,
        }
    }

    fn to_string(&self, pool: &Pool) -> String {
        let result: String = self.leaf_node_get_iter(pool)
            .flat_map(|kv_iters| {
                kv_iters.0.cloned()
                    .chain(iter::once(b':'))
                    .chain(kv_iters.1.cloned())
                    .chain(iter::once(b','))
                    .chain(iter::once(b' '))
            })
            .map(|b| b as char)
            .collect();
        result
    }
}

/// Internal node specific functions
impl NodeHeader {

}

/// Leaf node specific functions
impl NodeHeader {
    /// Insert in an append only/immutable fashion
    fn leaf_node_insert_non_full(&mut self, tx_id: usize, key: &[u8], value: &[u8], pool: &Pool) -> Result<EntryLocation, &'static str> {
        assert_eq!(MemType::Leaf, self.node_type);
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

    /// Remove in an append-only/immutable fashion.
    /// Precondition: key must exist. Panics if key does not exist
    fn leaf_node_remove(&mut self, tx_id: usize, key: &[u8], pool:&Pool) -> Result<EntryLocation, &'static str> {
        assert_eq!(MemType::Leaf, self.node_type);
        let index = self.index_of(key, pool);
        if index == NOT_FOUND {
            panic!("The caller is responsible for checking for key existence before calling remove");
        }
        let loc = EntryLocation {
            page_index: try!(pool.alloc()),
            offset: 0,
        };
        let node = NodeHeader::from_entry(&loc, pool);

        // Copy over metadata
        node.node_type = self.node_type.clone();
        node.tx_id = tx_id;
        node.num_keys = self.num_keys-1;
        node.num_children = self.num_children-1;

        // Copy all data except for the deleted key/val
        let mut off = 0;
        for i in 0..self.num_keys {
            if i == index {
                off = 1;
                continue;
            }
            node.keys[i-off] = self.keys[i].clone();
            node.children[i-off] = self.children[i].clone();
            // Retain the pages
            pool.retain(node.children[i-off].page_index);
            pool.retain(node.keys[i-off].page_index);
        }
        Ok(loc)
    }

    /// Check to see if the node contains the given key
    fn leaf_node_contains_key(&self, key: &[u8], pool: &Pool) -> bool {
        assert_eq!(MemType::Leaf, self.node_type);
        self.index_of(key, pool) != NOT_FOUND
    }

    fn leaf_node_get_iter<'a>(&'a self, pool: &'a Pool) -> KvIter<'a> {
        KvIter {
            node: self,
            pool: pool,
            index: 0,
        }
    }
}

struct KvIter<'a> {
    node: &'a NodeHeader,
    pool: &'a Pool,
    index: usize,
}

impl <'a>Iterator for KvIter<'a> {
    type Item = (ByteStringIter<'a>, ByteStringIter<'a>);

    fn next(&mut self) -> Option<(ByteStringIter<'a>, ByteStringIter<'a>)> {
        if self.index >= self.node.num_keys {
            None
        } else {
            let kv = (
                get_iter(&self.node.keys[self.index], self.pool),
                get_iter(&self.node.children[self.index], self.pool),
            );
            self.index += 1;
            Some(kv)
        }
    }
}

/// Return the page where the last item points as a hint
/// for where the next item should be allocated.
#[inline]
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
    let mut old_i = i;

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
        if i == old_i {
            break;
        } else {
            old_i = i;
        }
    }
    println!("I: {}", i);
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
    // Shift everything after the index where we're inserting down
    for i in (index+1..array_size).rev() {
        array[i] = array[i-1].clone();
    }
    array[index] = loc.clone();
}

/// Decrement the ref count for the given node
fn release_node(entry: &EntryLocation, pool: &Pool) {
    let is_dead = pool.release(entry.page_index);
    // If this node is now dead, we can recursively
    // remove its contents
    if is_dead {
        release_node_contents(entry, pool);
    }
}

/// Release the memory "owned" by the given node
fn release_node_contents(entry: &EntryLocation, pool: &Pool) {
    let node = NodeHeader::from_entry(entry, pool);
    match node.node_type {
        MemType::Root | MemType::Internal => {
            for e in node.children.iter().take(node.num_children) {
                release_node(e, pool);
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

/// Nodes are designed to fill an entire page
/// so there's no free space
pub fn free_space_node_page(_: &Page) -> usize {
    0
}

#[test]
fn test_leaf_node_split() {
    let mut buf = [0u8; 0x8000];
    let pool = Pool::new(&mut buf);
    let n = NodeHeader::alloc_node(MemType::Leaf, 0, &pool).unwrap();
    let hello: Vec<u8> = "hello".bytes().collect();
    let world: Vec<u8> = "world".bytes().collect();
    let foo: Vec<u8> = "foo".bytes().collect();
    let bar: Vec<u8> = "bar".bytes().collect();
    let rust: Vec<u8> = "rust".bytes().collect();
    let iscool: Vec<u8> = "is cool".bytes().collect();

    let n = n.leaf_node_insert_non_full(1, &hello[..], &world[..], &pool).unwrap();
    let n = NodeHeader::from_entry(&n, &pool);
    let n = n.leaf_node_insert_non_full(2, &rust[..], &iscool[..], &pool).unwrap();
    let n = NodeHeader::from_entry(&n, &pool);
    let n = n.leaf_node_insert_non_full(3, &foo[..], &bar[..], &pool).unwrap();
    let n = NodeHeader::from_entry(&n, &pool);

    println!("CURRENT CONTENTS: {}", n.to_string(&pool));

    let (nb, nt, mid) = n.split(4, &pool).unwrap();
    // Each half should have 1 key and 1 child
    assert_eq!(1, nb.num_keys);
    assert_eq!(1, nb.num_children);
    assert_eq!(2, nt.num_keys);
    assert_eq!(2, nt.num_children);
    assert_eq!("hello", mid.cloned().map(|c| c as char).collect::<String>());

    assert!(nt.leaf_node_contains_key(&hello[..], &pool));
    assert!(nb.leaf_node_contains_key(&foo[..], &pool));

    assert!(!nb.leaf_node_contains_key(&hello[..], &pool));
    assert!(!nt.leaf_node_contains_key(&foo[..], &pool));

    assert!(nt.leaf_node_contains_key(&rust[..], &pool));
}

#[test]
fn test_leaf_node_remove() {
    let mut buf = [0u8; 0x7000];
    let pool = Pool::new(&mut buf);
    let n = NodeHeader::alloc_node(MemType::Leaf, 0, &pool).unwrap();
    let page_index_1 = n.loc().page_index;

    let hello: Vec<u8> = "hello".bytes().collect();
    let world: Vec<u8> = "world".bytes().collect();

    let foo: Vec<u8> = "foo".bytes().collect();
    let bar: Vec<u8> = "bar".bytes().collect();

    let n2 = n.leaf_node_insert_non_full(1, &hello[..], &world[..], &pool).unwrap();
    let n2 = NodeHeader::from_entry(&n2, &pool);

    let n3 = n2.leaf_node_insert_non_full(2, &foo[..], &bar[..], &pool).unwrap();
    let n3 = NodeHeader::from_entry(&n3, &pool);

    let n4 = n3.leaf_node_remove(3, &foo[..], &pool).unwrap();
    let n4 = NodeHeader::from_entry(&n4, &pool);

    assert!(n3.leaf_node_contains_key(&foo[..], &pool));
    assert!(! n4.leaf_node_contains_key(&foo[..], &pool));
    assert!(n4.leaf_node_contains_key(&hello[..], &pool));
    assert_eq!(2, n3.num_keys);
    assert_eq!(2, n3.num_children);
    assert_eq!(1, n4.num_keys);
    assert_eq!(1, n4.num_children);

    let n5 = n4.leaf_node_remove(4, &hello, &pool).unwrap();
    let n5 = NodeHeader::from_entry(&n5, &pool);
    assert_eq!(0, n5.num_keys);
    assert_eq!(0, n5.num_children);
    assert!(!n5.leaf_node_contains_key(&hello[..], &pool));
}

#[test]
fn test_release_leaf_node() {
    let mut buf = [0u8; 0x5000];
    let pool = Pool::new(&mut buf);
    let n = NodeHeader::alloc_node(MemType::Leaf, 0, &pool).unwrap();
    let page_index_1 = n.loc().page_index;

    let hello: Vec<u8> = "hello".bytes().collect();
    let world: Vec<u8> = "world".bytes().collect();

    let foo: Vec<u8> = "foo".bytes().collect();
    let bar: Vec<u8> = "bar".bytes().collect();

    let n2 = n.leaf_node_insert_non_full(1, &hello[..], &world[..], &pool).unwrap();
    let page_index_2 = n2.page_index;
    let n2 = NodeHeader::from_entry(&n2, &pool);

    let n3 = n2.leaf_node_insert_non_full(2, &foo[..], &bar[..], &pool).unwrap();
    let page_index_3 = n3.page_index;
    let n3 = NodeHeader::from_entry(&n3, &pool);

    let keys_page = n3.keys[0].page_index;
    let children_page = n3.children[0].page_index;

    // Each node should be the only user of its page
    assert_eq!(1, pool.get_ref_count(page_index_1));
    assert_eq!(1, pool.get_ref_count(page_index_2));
    assert_eq!(1, pool.get_ref_count(page_index_3));

    // The keys and refs should each have 3 entries across 2 nodes pointed at them
    assert_eq!(3, pool.get_ref_count(keys_page));
    assert_eq!(3, pool.get_ref_count(children_page));

    // Now, we'll free the last node, and watch the ref counts go down
    release_node(&n3.loc(), &pool);
    assert_eq!(1, pool.get_ref_count(keys_page));
    assert_eq!(1, pool.get_ref_count(children_page));
    assert_eq!(0, pool.get_ref_count(page_index_3));

    // Then the other node
    release_node(&n2.loc(), &pool);
    assert_eq!(0, pool.get_ref_count(keys_page));
    assert_eq!(0, pool.get_ref_count(children_page));
    assert_eq!(0, pool.get_ref_count(page_index_2));
}

#[test]
fn test_insertion_ordering() {
    use std::iter;
    let mut buf = [0u8; 0x5000];
    let pool = Pool::new(&mut buf);
    let n = NodeHeader::alloc_node(MemType::Leaf, 0, &pool).unwrap();
    let hello: Vec<u8> = "hello".bytes().collect();
    let world: Vec<u8> = "world".bytes().collect();

    let foo: Vec<u8> = "foo".bytes().collect();
    let bar: Vec<u8> = "bar".bytes().collect();

    let n2 = n.leaf_node_insert_non_full(1, &hello[..], &world[..], &pool).unwrap();
    let n2 = NodeHeader::from_entry(&n2, &pool);

    let n3 = n2.leaf_node_insert_non_full(2, &foo[..], &bar[..], &pool).unwrap();
    let n3 = NodeHeader::from_entry(&n3, &pool);

    assert_eq!(2, n3.num_keys);
    assert_eq!(2, n3.num_children);

    assert!(n3.leaf_node_contains_key(&hello[..], &pool));
    assert!(n3.leaf_node_contains_key(&foo[..], &pool));

    // "foo" should sort first before "hello"
    assert_eq!(0, n3.index_of(&foo[..], &pool));
    assert_eq!(1, n3.index_of(&hello[..], &pool));

    let result = n3.to_string(&pool);
    assert_eq!("foo:bar, hello:world, ", result);
}

#[test]
fn test_insert_internal_non_full() {
    let mut buf = [0u8; 0x4000];
    let pool = Pool::new(&mut buf);
    let n = NodeHeader::alloc_node(MemType::Leaf, 0, &pool).unwrap();

    let key: Vec<u8> = "hello".bytes().collect();
    let value: Vec<u8> = "world".bytes().collect();

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

    assert!(n2.leaf_node_contains_key(&key[..], &pool));
    assert!(!n.leaf_node_contains_key(&key[..], &pool));
}

#[test]
fn test_invariants() {
    use std::mem;
    println!("CHECK {:?} < {:?}?", mem::size_of::<NodeHeader>(), PAGE_SIZE);
    assert!(mem::size_of::<NodeHeader>() < PAGE_SIZE);
}
