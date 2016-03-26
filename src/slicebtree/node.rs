use std::{cmp,iter};
use allocator::*;

use super::*;

macro_rules! try_but_panic_in_debug {
    ($expr:expr) => ({
        let t = $expr;
        debug_assert!(t.is_ok());
        try!(t)
    })
}

#[derive(Debug, Clone, PartialEq)]
enum NodeType {
    Root,
    Internal,
    Leaf,
}

/// The structure of a tree is a series of Nodes.
/// If the NodeType is Root or Internal, the children
/// are interpreted as Nodes. If the NodeType is Leaf,
/// the children are interpreted as the values of the mapping.
pub struct Node {
    node_type: NodeType,
    tx_id: usize,
    num_keys: usize,
    keys: [PersistedArcByteSlice; B],
    num_children: usize,
    children: [PersistedArcByteSlice; B],
}

/// Private interface
impl Node {
    /// Perform initial setup, such as fixing the keys/children arrays,
    /// setting the tx_id
    fn init(&mut self, tx: usize, node_type: NodeType) {
        self.num_keys = 0;
        self.num_children = 0;
        self.node_type = node_type;
        self.tx_id = tx;
    }

    /// The first return value is true if the given key exists in the node.
    /// The second parameter is the location of the key if it exists, or the
    /// point where the key should be inserted if it does not already exist.
    fn index_or_insertion_of(&self, key: &[u8], pool: &Pool) -> (bool, usize) {
        if self.num_keys == 0 {
            return (false, 0)
        } else {
            let last_key = &self.keys[self.num_keys-1].clone_to_arc_byte_slice(pool).unwrap();
            if key.cmp(last_key) == cmp::Ordering::Greater {
                return (false, self.num_keys)
            }
        }
        let mut top = self.num_keys-1;
        let mut bottom = 0;
        let mut i = top/2;
        let mut old_i = i;
        loop {
            let i_key = &self.keys[i].clone_to_arc_byte_slice(pool).unwrap();
            match key.cmp(i_key) {
                cmp::Ordering::Equal => break,
                cmp::Ordering::Less => top = if i > 1 {i-1} else {0},
                cmp::Ordering::Greater => bottom = i+1,
            }
            if top < bottom {
                break;
            }
            i = bottom + (top - bottom)/2;
            if i == old_i {
                break;
            } else {
                old_i = i;
            }
        }
        let i_key = &self.keys[i].clone_to_arc_byte_slice(pool).unwrap();
        if key.cmp(i_key) == cmp::Ordering::Equal {
            (true, i)
        } else {
            (false, i)
        }
    }
}

/// Leaf Node impl
impl Node {
    /// Check to see if the node contains the given key
    fn leaf_node_contains_key(&self, key: &[u8], pool: &Pool) -> bool {
        assert_eq!(NodeType::Leaf, self.node_type);
        self.index_or_insertion_of(key, pool).0
    }

    /// Insert in an append only/immutable fashion
    fn leaf_node_insert_non_full<'a>(&'a self, tx_id: usize, key: &[u8], value: &[u8], pool: &'a Pool) -> Result<ArcByteSlice, &'static str> {
        assert_eq!(NodeType::Leaf, self.node_type);
        let key_arc = try!(pool.malloc(key));
        let val_arc = try!(pool.malloc(value));

        let node_arc = try!(pool.clone(self));
        { // Borrow checker
            let node = node_arc.deref_as_mut::<Node>();
            let (found, index) = node.index_or_insertion_of(key, pool);
            if found {
                return Err("Key already exists");
            } else if node.num_children == B {
                return Err("Node is already full");
            }
            node.num_children += 1;
            insert_into(&mut node.children, node.num_children, &val_arc, index, pool);
            node.num_keys += 1;
            insert_into(&mut node.keys, node.num_keys, &key_arc, index, pool);
        }
        Ok(node_arc)
    }

    /// Remove in an append-only/immutable fashion.
    /// Precondition: key must exist. Panics if key does not exist
    fn leaf_node_remove<'a>(&'a self, tx_id: usize, key: &[u8], pool:&'a Pool) -> Result<ArcByteSlice, &'static str> {
        assert_eq!(NodeType::Leaf, self.node_type);
        let (found, index) = self.index_or_insertion_of(key, pool);
        if !found {
            return Err("This node does not contain the given key");
        }
        let arc = try!(pool.make_new::<Node>());
        { // Borrow checker
            let node = arc.deref_as_mut::<Node>();
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
                node.keys[i-off] = try!(self.keys[i].clone(pool));
                node.children[i-off] = try!(self.children[i].clone(pool));
            }
        }
        Ok(arc)
    }
}

/// Precondition: The node must have enough space
/// The memory should already be allocated, this
/// just inserts the reference in the correct location.
fn insert_into(array: &mut [PersistedArcByteSlice; B],
          array_size: usize,
                 arc: &ArcByteSlice,
               index: usize,
                pool: &Pool) {
    // Shift everything after the index where we're inserting down
    for i in (index+1..array_size).rev() {
        array[i] = array[i-1].clone(pool).unwrap();
        array[i-1].release(pool);
    }
    array[index] = arc.clone_to_persisted();
}
