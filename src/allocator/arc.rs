use std::mem;
use std::sync::atomic::{AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst, Relaxed};
use std::ops::Deref;

use super::pool::*;

lazy_static! {
    pub static ref ARC_INNER_SIZE: usize = mem::size_of::<ArcByteSliceInner>();
}

/// ArcByteSlices are free floating and are not persisted
pub struct ArcByteSlice {
    pub _ptr: *mut ArcByteSliceInner,
    _pool: *const Pool,
}


/// ArcByteSliceInners are persisted in the mem map
pub struct ArcByteSliceInner {
    pub strong: AtomicUsize,
    pub weak: AtomicUsize,
    pub size: usize,
}

/// Public Api for ArcByteSlice
impl ArcByteSlice {
    pub fn new(inner: &mut ArcByteSliceInner, pool: &Pool) -> ArcByteSlice {
        inner.strong.fetch_add(1, Acquire);
        ArcByteSlice {
            _ptr: inner as *mut ArcByteSliceInner,
            _pool: pool as *const Pool,
        }
    }

    pub fn get_ref_count(&self) -> usize {
        self.inner().strong.load(Relaxed)
    }

    pub fn clone_to_persisted(&self) -> PersistedArcByteSlice {
        let inner = self.inner();
        // Persisted counts as a strong reference
        inner.strong.fetch_add(1, SeqCst);
        unsafe {
            PersistedArcByteSlice {
                arc_inner_index: (*self._pool)._inner_offset(&self),
                id_tag: (*self._pool)._get_id_tag(&self),
            }
        }
    }

    /// Stolen from std::sync::arc https://doc.rust-lang.org/src/alloc/arc.rs.html
    #[inline]
    pub fn inner(&self) -> &ArcByteSliceInner {
        // This unsafety is ok because while this arc is alive we're guaranteed
        // that the inner pointer is valid.
        unsafe { &*self._ptr }
    }

    /// Convert the Arc to a reference. Panics if the
    /// Arc does not point to a correctly sized piece of
    /// memory.
    pub fn deref_as<'a, T>(&'a self) -> &'a T {
        assert_eq!(self.inner().size, mem::size_of::<T>());
        unsafe {
            (*self._pool).deref_as(self)
        }
    }

    /// Convert the Arc to a reference. Panics if the
    /// Arc does not point to a correctly sized piece of
    /// memory.
    pub fn deref_as_mut<'a, T>(&'a self) -> &'a mut T {
        assert_eq!(self.inner().size, mem::size_of::<T>());
        unsafe {
            (*self._pool).deref_as_mut(self)
        }
    }
}

/// Public Api for ArcByteSliceInner
impl ArcByteSliceInner {
    pub fn init(&mut self, size: usize) {
        self.strong.store(0, SeqCst);
        self.weak.store(0, SeqCst);
        self.size = size;
    }
}

impl Clone for ArcByteSlice {
    fn clone(&self) -> ArcByteSlice {
        self.inner().strong.fetch_add(1, Acquire);
        ArcByteSlice {
            _ptr: self._ptr,
            _pool: self._pool,
        }
    }
}

/// Deref for ArcByteSlice -- No DerefMut since map contents are Read Only.
impl Deref for ArcByteSlice {
    type Target = [u8];

    fn deref<'a>(&'a self) -> &'a [u8] {
        unsafe {
            (*self._pool).deref(self)
        }
    }
}

impl  Drop for ArcByteSlice {
    fn drop(&mut self) {
        let inner = self.inner();
        if inner.strong.fetch_sub(1, Release) == 1 {
            // This was the last strong ref, let's release
            unsafe {
                (*self._pool).free(self);
            }
        }
    }
}

/// A PersistedArcByteSlice is a serializable version of an ArcByteSlice
/// You can freely trade Arcs for PersistedArcs and vice versa.
/// However, you must always manually release the PersistedArcByteSlice
/// since releasing requires reference to a pool. The Drop impl will panic
/// if you forget to release the persist.
#[derive(Debug)]
pub struct PersistedArcByteSlice {
    pub arc_inner_index: usize,
    id_tag: usize,
}

impl PersistedArcByteSlice {
    pub fn clone_to_arc_byte_slice(&self, pool: &Pool) -> Result<ArcByteSlice, &'static str> {
        pool.clone_persisted_to_arc(self)
    }

    pub fn get_id_tag(&self) -> usize {
        self.id_tag
    }

    pub fn clone(&self, pool: &Pool) -> Result<PersistedArcByteSlice, &'static str> {
        try!(self.retain(pool));
        Ok(PersistedArcByteSlice {
            arc_inner_index: self.arc_inner_index,
            id_tag: self.id_tag,
        })
    }

    pub fn retain(&self, pool: &Pool) -> Result<(), &'static str> {
        let arc = try!(pool.clone_persisted_to_arc(self));
        arc.inner().strong.fetch_add(1, Acquire);
        Ok(())
    }

    pub fn release(&mut self, pool: &Pool) -> Result<bool, &'static str> {
        let arc = try!(pool.clone_persisted_to_arc(self));
        let remaining_count = arc.inner().strong.fetch_sub(1, Release) - 1;
        self.id_tag = 0;
        self.arc_inner_index = BUFFER_END;
        // The last ref is the arc which will call free if necessary
        Ok(remaining_count == 1)
    }
}

// TODO: Adding the drop flag causes a segfault when assigning to the array
// of children. Figure this out so I can re-enable this check
// impl Drop for PersistedArcByteSlice {
//     fn drop(&mut self) {
//         if self.id_tag != 0 {
//             panic!("You MUST call release on a PersistedArcByteSlice before it can be dropped cleanly")
//         }
//     }
// }
