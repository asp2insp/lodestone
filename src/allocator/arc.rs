use std::mem;
use std::sync::atomic::{AtomicUsize, AtomicBool};
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::ops::{Deref, DerefMut};

use super::pool::*;

lazy_static! {
    pub static ref ARC_INNER_SIZE: usize = mem::size_of::<_ArcInnerCounters>();
}

/// Arcs are free floating and are not persisted
pub struct Arc<T> {
    pub _ptr: *mut ArcInner<T>,
    _pool: *const Pool,
}

/// ArcInners live inside of the buffer and are persisted
struct _ArcInnerCounters {
    strong: AtomicUsize,
    weak: AtomicUsize,
}

pub struct ArcInner<T> {
    counters: _ArcInnerCounters,
    pub data: T,
}

/// Public Api for Arc
impl <T> Arc<T> {
    pub fn new(inner: &mut ArcInner<T>, pool: &Pool) -> Arc<T> {
        inner.counters.strong.fetch_add(1, Acquire);
        Arc {
            _ptr: inner as *mut ArcInner<T>,
            _pool: pool as *const Pool,
        }
    }

    /// Stolen from std::sync::arc https://doc.rust-lang.org/src/alloc/arc.rs.html
    #[inline]
    pub fn inner(&self) -> &ArcInner<T> {
        // This unsafety is ok because while this arc is alive we're guaranteed
        // that the inner pointer is valid.
        unsafe { &*self._ptr }
    }
}

/// Public Api for ArcInner
impl <T> ArcInner<T> {
    pub fn init(&mut self, data: T) {
        self.counters.strong.store(0, SeqCst);
        self.counters.weak.store(0, SeqCst);
        self.data = data;
    }
}

/// Deref for Arc -- No DerefMut since map contents are Read Only.
impl<T> Deref for Arc<T> {
    type Target = T;

    fn deref<'a>(&'a self) -> &'a T {
        unsafe {
            (*self._pool).deref(self)
        }
    }
}

/// Privae Api for Arc
impl <T> Arc<T> {

}

impl <T> Drop for Arc<T> {
    fn drop(&mut self) {
        let inner = self.inner();
        if inner.counters.strong.fetch_sub(1, Release) == 1 {
            // This was the last strong ref, let's release
            unsafe {
                (*self._pool).free(self);
            }
        }
    }
}
