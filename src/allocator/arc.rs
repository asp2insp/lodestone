use std::mem;
use std::sync::atomic::{AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
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
    strong: AtomicUsize,
    weak: AtomicUsize,
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
    pub fn init(&mut self, data: &[u8]) {
        self.strong.store(0, SeqCst);
        self.weak.store(0, SeqCst);
        self.size = data.len();
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
/// You can freely trade Arcs for PersistedArcs.
pub struct PersistedArcByteSlice {
    pub arc_inner_index: usize,
    id_tag: usize,
}

impl PersistedArcByteSlice {
    pub fn clone_to_arc_byte_slice(&self, pool: &Pool) -> ArcByteSlice {
        pool.clone_persisted_to_arc(self)
    }

    pub fn release(&self, pool: &Pool) {

    }
}
