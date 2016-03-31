use std::{cmp,fmt,str};
use allocator::*;

use super::*;

macro_rules! recover_but_panic_in_debug {
    ($expr:expr, $default:expr) => ({
        let t = $expr;
        debug_assert!(t.is_ok(), format!("Err in debug: {:?}", t.err()));
        match t {
            Ok(val) => val,
            Err(_) => {
                return $default
            },
        }
    })
}

/// Internal nodes have keys in order. The corresponding
/// child to a key index is the node that contains values
/// less than or equal to the key.
/// The index above is the child with values greater than the given key.
/// Thus, internal nodes can hold up to B-1 keys and B children.
/// Leaf nodes have a 1-1 correspndence of key to value, holding
/// up to B keys and B children.

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

pub struct Split {
    bottom_half: ArcByteSlice,
    top_half: ArcByteSlice,
    mid_key: ArcByteSlice,
}

/// Public interface
impl Node {
    pub fn clone(&self, pool: &Pool) -> Result<ArcByteSlice, &'static str> {
        let clone = try!(pool.clone(self));
        {
            let node = clone.deref_as_mut::<Node>();
            for i in 0..node.num_keys {
                let ok = node.keys[i].retain(pool).is_ok();
                debug_assert!(ok);
            }
            for i in 0..node.num_children {
                let ok = node.children[i].retain(pool).is_ok();
                debug_assert!(ok);
            }
        }
        Ok(clone)
    }

    /// Splits the node in half, immutably, returning a tuple of the
    /// (
    ///    new_bottom_half,
    ///    new_top_half,
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
    pub fn split<'a>(&'a self, tx_id: usize, pool: &'a Pool)
        -> Result<Split, &'static str> {
        assert!(self.num_keys > 0 && self.num_children > 0, "Split called on an empty node");

        let new_bottom_half_arc = try!(pool.make_new::<Node>());
        let new_top_half_arc = try!(pool.make_new::<Node>());
        // Find midpoint
        let midpoint = self.num_keys/2;

        { // Borrow checker
            let new_bottom_half = new_bottom_half_arc.deref_as_mut::<Node>();
            let new_top_half = new_top_half_arc.deref_as_mut::<Node>();
            new_bottom_half.init(tx_id, self.node_type.clone());
            new_top_half.init(tx_id, self.node_type.clone());

            // Copy over values
            for i in 0..midpoint {
                new_bottom_half.keys[i] = try!(self.keys[i].clone(pool));
            }
            for i in 0..midpoint {
                new_bottom_half.children[i] = try!(self.children[i].clone(pool));
            }
            for i in midpoint..self.num_keys {
                new_top_half.keys[i-midpoint] = try!(self.keys[i].clone(pool));
            }
            for i in midpoint..self.num_children {
                new_top_half.children[i-midpoint] = try!(self.children[i].clone(pool));
            }
            // Copy over metadata
            new_bottom_half.num_keys = midpoint;
            new_bottom_half.num_children = midpoint;
            new_top_half.num_keys = self.num_keys - midpoint;
            new_top_half.num_children = self.num_children - midpoint;
        }
        Ok(Split {
            bottom_half: new_bottom_half_arc,
            top_half: new_top_half_arc,
            mid_key: try!(self.keys[midpoint].clone_to_arc_byte_slice(pool))
        })
    }

    /// Joins two underfull nodes, immutably, returning the new merged node
    pub fn join<'a>(bottom: &'a Node, top: &'a Node, tx_id: usize, pool: &'a Pool)
        -> Result<ArcByteSlice, &'static str> {
        assert!(bottom.num_keys + top.num_keys < B,
            "Join called on nodes that have too many keys");
        assert!(bottom.num_children + top.num_children < B,
            "Join called on nodes that have too many children");
        assert_eq!(bottom.node_type, top.node_type);

        let new_arc = try!(pool.make_new::<Node>());
        { // Borrow checker
            let new_node = new_arc.deref_as_mut::<Node>();
            new_node.init(tx_id, bottom.node_type.clone());

            // Copy over keys/values
            for i in 0..bottom.num_keys {
                new_node.keys[i] = try!(bottom.keys[i].clone(pool));
            }
            for i in 0..top.num_keys {
                new_node.keys[i+bottom.num_keys] = try!(top.keys[i].clone(pool));
            }
            for i in 0..bottom.num_children {
                new_node.children[i] = try!(bottom.children[i].clone(pool));
            }
            for i in 0..top.num_children {
                new_node.children[i+bottom.num_children] = try!(top.children[i].clone(pool));
            }
            // Copy over metadata
            new_node.num_keys = bottom.num_keys + top.num_keys;
            new_node.num_children = bottom.num_children + top.num_children;
        }
        Ok(new_arc)
    }
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
    pub fn index_or_insertion_of(&self, key: &[u8], pool: &Pool) -> (bool, usize) {
        if self.num_keys == 0 {
            return (false, 0)
        } else {
            let last_key = recover_but_panic_in_debug!(
                self.keys[self.num_keys-1].clone_to_arc_byte_slice(pool),
                (false, BUFFER_END)
            );
            if key.cmp(&*last_key) == cmp::Ordering::Greater {
                return (false, self.num_keys)
            }
        }
        let mut top = self.num_keys-1;
        let mut bottom = 0;
        let mut i = top/2;
        let mut old_i = i;
        loop {
            let i_key = recover_but_panic_in_debug!(
                self.keys[i].clone_to_arc_byte_slice(pool),
                (false, BUFFER_END)
            );
            match key.cmp(&*i_key) {
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
        let i_key = recover_but_panic_in_debug!(
            self.keys[i].clone_to_arc_byte_slice(pool),
            (false, BUFFER_END)
        );
        if key.cmp(&*i_key) == cmp::Ordering::Equal {
            (true, i)
        } else {
            (false, i)
        }
    }
}

/// Leaf Node impl
impl Node {
    /// Check to see if the node contains the given key
    pub fn leaf_node_contains_key(&self, key: &[u8], pool: &Pool) -> bool {
        assert_eq!(NodeType::Leaf, self.node_type);
        self.index_or_insertion_of(key, pool).0
    }

    /// Return an arc to the value associated with the given key
    /// or None if the key is not contained within this node
    pub fn value_for_key(&self, key: &[u8], pool: &Pool) -> Option<ArcByteSlice> {
        let (found, idx) = self.index_or_insertion_of(key, pool);
        if found {
            Some(
                recover_but_panic_in_debug!(
                    self.children[idx].clone_to_arc_byte_slice(pool),
                    None
                )
            )
        } else {
            None
        }
    }

    /// Insert in an append only/immutable fashion
    fn leaf_node_insert_non_full<'a>(&'a self, tx_id: usize, key: &[u8], value: &[u8], pool: &'a Pool) -> Result<ArcByteSlice, &'static str> {
        assert_eq!(NodeType::Leaf, self.node_type);
        let key_arc = try!(pool.malloc(key));
        let val_arc = try!(pool.malloc(value));
        let node_arc = try!(self.clone(pool));

        { // Borrow checker
            let node = node_arc.deref_as_mut::<Node>();
            node.tx_id = tx_id;
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
        let cleanup = array[i-1].release(pool);
        debug_assert!(cleanup.is_ok(), format!("{:?}", cleanup.err()));
    }
    array[index] = arc.clone_to_persisted();
}

pub fn release_node(persist: &mut PersistedArcByteSlice, pool: &Pool) {
    { // Borrow checker
        let arc = recover_but_panic_in_debug!(persist.clone_to_arc_byte_slice(pool), ());
        let node = arc.deref_as_mut::<Node>();
        match node.node_type {
            NodeType::Root | NodeType::Internal => {
                for p in node.children.iter_mut().take(node.num_children) {
                    release_node(p, pool);
                }
            },
            NodeType::Leaf => {
                for p in node.children.iter_mut().take(node.num_children) {
                    let ok = p.release(pool).is_ok();
                    debug_assert!(ok);
                }
            },
        }
        // Release the keys mem
        for p in node.keys.iter_mut().take(node.num_keys) {
            let ok = p.release(pool).is_ok();
            debug_assert!(ok);
        }
    }
    // Finally, release the pointer itself
    let ok = persist.release(pool).is_ok();
    debug_assert!(ok);
}

pub struct DebuggableNode<'a> {
    node: &'a Node,
    pool: &'a Pool,
}

impl <'a> fmt::Debug for DebuggableNode<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let key_vec: Vec<String> = self.node.keys.iter()
            .take(self.node.num_keys)
            .map(|persist| {
                str::from_utf8(
                    &*persist.clone_to_arc_byte_slice(self.pool).unwrap()
                )
                .unwrap()
                .to_string()
            })
            .collect();
        let child_vec: Vec<String> = self.node.children.iter()
            .take(self.node.num_children)
            .map(|persist| {
                str::from_utf8(
                    &*persist.clone_to_arc_byte_slice(self.pool).unwrap()
                )
                .unwrap()
                .to_string()
            })
            .collect();
        fmt.debug_struct(&format!("{:?}", self.node.node_type))
            .field("tx_id", &self.node.tx_id)
            .field("keys", &key_vec.join(", "))
            .field("children", &child_vec.join(", "))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use allocator::*;
    use super::*;
    use super::NodeType::*;

    lazy_static! {
        static ref HELLO: Vec<u8> = String::from("hello").into_bytes();
        static ref WORLD: Vec<u8> = String::from("world").into_bytes();
        static ref FOO: Vec<u8> = String::from("foo").into_bytes();
        static ref BAR: Vec<u8> = String::from("bar").into_bytes();
        static ref APPLE: Vec<u8> = String::from("apple").into_bytes();
        static ref BANANA: Vec<u8> = String::from("banana").into_bytes();
        static ref CHERRY: Vec<u8> = String::from("cherry").into_bytes();
        static ref BLUEBERRY: Vec<u8> = String::from("blueberry").into_bytes();
    }

    fn get_ref_count(persist: &PersistedArcByteSlice, pool: &Pool) -> usize {
        persist.clone_to_arc_byte_slice(pool).unwrap().get_ref_count() - 1
    }

    #[test]
    fn test_leaf_node_joins() {
        let mut buf = [0u8; 0x8000];
        let pool = Pool::new(&mut buf);

        let n_arc = pool.make_new::<Node>().unwrap();
        let n = n_arc.deref_as_mut::<Node>();
        n.init(0, Leaf);

        let n = n.leaf_node_insert_non_full(1, &HELLO, &WORLD, &pool).unwrap();
        let n = n.deref_as::<Node>().leaf_node_insert_non_full(2, &CHERRY, &BLUEBERRY, &pool).unwrap();
        let n = n.deref_as::<Node>().leaf_node_insert_non_full(3, &FOO, &BAR, &pool).unwrap();

        let split = n.deref_as::<Node>().split(4, &pool).unwrap();

        let join = Node::join(
            split.bottom_half.deref_as::<Node>(),
            split.top_half.deref_as::<Node>(),
            5,
            &pool
        )
        .unwrap();

        assert_eq!(
            "Leaf { tx_id: 5, \
                keys: \"cherry, foo, hello\", \
                children: \"blueberry, bar, world\" }",
            format!("{:?}", DebuggableNode {
                node: join.deref_as::<Node>(),
                pool: &pool,
            })
        );
    }

    #[test]
    fn test_leaf_node_split() {
        let mut buf = [0u8; 0x8000];
        let pool = Pool::new(&mut buf);

        let n_arc = pool.make_new::<Node>().unwrap();
        let n = n_arc.deref_as_mut::<Node>();
        n.init(0, Leaf);

        let n = n.leaf_node_insert_non_full(1, &HELLO, &WORLD, &pool).unwrap();
        let n = n.deref_as::<Node>().leaf_node_insert_non_full(2, &CHERRY, &BLUEBERRY, &pool).unwrap();
        let n = n.deref_as::<Node>().leaf_node_insert_non_full(3, &FOO, &BAR, &pool).unwrap();

        assert_eq!(
            "Leaf { tx_id: 3, \
                keys: \"cherry, foo, hello\", \
                children: \"blueberry, bar, world\" }",
            format!("{:?}", DebuggableNode {
                node: n.deref_as::<Node>(),
                pool: &pool,
            })
        );

        let split = n.deref_as::<Node>().split(4, &pool).unwrap();

        let bottom = split.bottom_half.deref_as::<Node>();
        let top = split.top_half.deref_as::<Node>();
        assert_eq!(1, bottom.num_keys);
        assert_eq!(1, bottom.num_children);
        assert_eq!(2, top.num_keys);
        assert_eq!(2, top.num_children);

        assert_eq!(*FOO, &*split.mid_key);

        assert!(top.leaf_node_contains_key(&HELLO, &pool));
        assert!(bottom.leaf_node_contains_key(&CHERRY, &pool));

        assert!(!bottom.leaf_node_contains_key(&HELLO, &pool));
        assert!(!top.leaf_node_contains_key(&CHERRY, &pool));

        assert!(top.leaf_node_contains_key(&FOO, &pool));

        assert_eq!(*BAR, &*top.value_for_key(&FOO, &pool).unwrap());
        assert_eq!(*WORLD, &*top.value_for_key(&HELLO, &pool).unwrap());
        assert_eq!(*BLUEBERRY, &*bottom.value_for_key(&CHERRY, &pool).unwrap());
    }

    #[test]
    fn test_release_leaf_node() {
        let mut buf = [0u8; 0x5000];
        let pool = Pool::new(&mut buf);

        let n_arc = pool.make_new::<Node>().unwrap();
        let n = n_arc.deref_as_mut::<Node>();
        n.init(0, Leaf);

        let n2 = n.leaf_node_insert_non_full(1, &HELLO, &WORLD, &pool).unwrap();
        {
            let n3 = n2.deref_as::<Node>().leaf_node_insert_non_full(2, &FOO, &BAR, &pool).unwrap();

            // Each node should provide 1 ref for its memory
            assert_eq!(1, n_arc.get_ref_count());
            assert_eq!(1, n2.get_ref_count());
            assert_eq!(1, n3.get_ref_count());

            // 'hello' should have 2 node refs and 'foo' should have 1 ref
            assert_eq!((true, 1), n3.deref_as::<Node>().index_or_insertion_of(&HELLO, &pool));
            assert_eq!(2, get_ref_count(&n2.deref_as::<Node>().keys[0], &pool));
            assert_eq!(2, get_ref_count(&n3.deref_as::<Node>().keys[1], &pool));
            assert_eq!(1, get_ref_count(&n3.deref_as::<Node>().keys[0], &pool));

            // 'world' should have 2 node refs and 'bar' should have 1 ref
            assert_eq!(2, get_ref_count(&n2.deref_as::<Node>().children[0], &pool));
            assert_eq!(2, get_ref_count(&n3.deref_as::<Node>().children[1], &pool));
            assert_eq!(1, get_ref_count(&n3.deref_as::<Node>().children[0], &pool));

            // Now, we'll free the last node, and watch the ref counts go down
            release_node(&mut n3.clone_to_persisted(), &pool);
            // 'hello' and 'world' should have 1 node ref left
            assert_eq!(1, get_ref_count(&n2.deref_as::<Node>().keys[0], &pool));
            assert_eq!(1, get_ref_count(&n2.deref_as::<Node>().children[0], &pool));
        }
        // n3 should be totally released now, as should 'foo' and 'bar'
        // The memory from 'foo' and 'bar' should have been reclaimed and merged
        assert_eq!(
            "Pool { buffer_size: 20480, \
                metadata: Metadata { lowest_known_free_index: 6672, next_id_tag: AtomicUsize(9) }, \
                blocks: [\
                    _B { start: 0, capacity: 3232, next: 3280, prev: 18446744073709551615, is_free: false }, \
                    _B { start: 3280, capacity: 8, next: 3336, prev: 0, is_free: false }, \
                    _B { start: 3336, capacity: 8, next: 3392, prev: 3280, is_free: false }, \
                    _B { start: 3392, capacity: 3232, next: 6672, prev: 3336, is_free: false }, \
                    _B { start: 6672, capacity: 9664, next: 16384, prev: 3392, is_free: true }\
                    ] \
                }",
            format!("{:?}", &pool)
        );
    }

    #[test]
    fn test_insert_remove() {
        let mut buf: [u8; 0x5000] = [0; 0x5000];
        let p = Pool::new(&mut buf);
        let n_arc = p.make_new::<Node>().unwrap();
        let n = n_arc.deref_as_mut::<Node>();
        n.init(0, Leaf);

        assert_eq!(
            "Leaf { tx_id: 0, keys: \"\", children: \"\" }",
            format!("{:?}", DebuggableNode {
                node: n,
                pool: &p,
            })
        );
        let n2_arc = n.leaf_node_insert_non_full(1, &HELLO, &WORLD, &p).unwrap();
        assert_eq!(
            "Leaf { tx_id: 1, keys: \"hello\", children: \"world\" }",
            format!("{:?}", DebuggableNode {
                node: n2_arc.deref_as::<Node>(),
                pool: &p,
            })
        );

        let n3_arc = n2_arc.deref_as::<Node>().leaf_node_insert_non_full(2, &BANANA, &CHERRY, &p).unwrap();
        assert_eq!(
            "Leaf { tx_id: 2, keys: \"banana, hello\", children: \"cherry, world\" }",
            format!("{:?}", DebuggableNode {
                node: n3_arc.deref_as::<Node>(),
                pool: &p,
            })
        );

        let n4_arc = n3_arc.deref_as::<Node>().leaf_node_remove(3, &HELLO, &p).unwrap();
        assert_eq!(
            "Leaf { tx_id: 3, keys: \"banana\", children: \"cherry\" }",
            format!("{:?}", DebuggableNode {
                node: n4_arc.deref_as::<Node>(),
                pool: &p,
            })
        );
    }

    #[test]
    fn test_insertion_ordering() {
        let mut buf = [0u8; 0x7000];
        let pool = Pool::new(&mut buf);

        let n_arc = pool.make_new::<Node>().unwrap();
        let n = n_arc.deref_as_mut::<Node>();
        n.init(0, Leaf);

        let n = n.leaf_node_insert_non_full(1, &BANANA, &BANANA, &pool).unwrap();
        let n = n.deref_as::<Node>().leaf_node_insert_non_full(2, &APPLE, &APPLE, &pool).unwrap();
        assert_eq!((true, 1), n.deref_as::<Node>().index_or_insertion_of(&BANANA, &pool));
        assert_eq!((true, 0), n.deref_as::<Node>().index_or_insertion_of(&APPLE, &pool));

        let n = n.deref_as::<Node>().leaf_node_insert_non_full(3, &CHERRY, &CHERRY, &pool).unwrap();
        assert_eq!((true, 1), n.deref_as::<Node>().index_or_insertion_of(&BANANA, &pool));
        assert_eq!((true, 0), n.deref_as::<Node>().index_or_insertion_of(&APPLE, &pool));
        assert_eq!((true, 2), n.deref_as::<Node>().index_or_insertion_of(&CHERRY, &pool));

        let n = n.deref_as::<Node>().leaf_node_insert_non_full(4, &BLUEBERRY, &BLUEBERRY, &pool).unwrap();
        assert_eq!((true, 1), n.deref_as::<Node>().index_or_insertion_of(&BANANA, &pool));
        assert_eq!((true, 0), n.deref_as::<Node>().index_or_insertion_of(&APPLE, &pool));
        assert_eq!((true, 3), n.deref_as::<Node>().index_or_insertion_of(&CHERRY, &pool));
        assert_eq!((true, 2), n.deref_as::<Node>().index_or_insertion_of(&BLUEBERRY, &pool));

        assert_eq!(
            "Leaf { tx_id: 4, \
                keys: \"apple, banana, blueberry, cherry\", \
                children: \"apple, banana, blueberry, cherry\" \
            }",
            format!("{:?}", DebuggableNode {
                node: n.deref_as::<Node>(),
                pool: &pool,
            })
        );
    }


    #[test]
    fn test_size_constraints() {
        use std::mem;
        // For efficiency, we want each node to fit inside a single page
        println!("CHECK {:?} < {:?}?", mem::size_of::<Node>(), *FIRST_OR_SINGLE_CONTENT_SIZE);
        assert!(mem::size_of::<Node>() < *FIRST_OR_SINGLE_CONTENT_SIZE);
    }
}
