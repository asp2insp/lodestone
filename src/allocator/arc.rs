use std::mem;
use std::sync::atomic::{AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::ops::{Deref, DerefMut};

use super::pool::*;

lazy_static! {
    pub static ref ARC_INNER_SIZE: usize = mem::size_of::<ArcByteSliceInner>();
}

/// ArcByteSlices are free floating and are not persisted
pub struct ArcByteSlice {
    pub _ptr: *mut ArcByteSliceInner,
    _pool: *const Pool,
}

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

/// Privae Api for ArcByteSlice
impl  ArcByteSlice {

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
