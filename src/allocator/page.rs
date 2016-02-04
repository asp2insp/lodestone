use std::mem;

/// Each page is 4096 bytes
pub type Page = [u8; 0x1000];

pub type PageIndex = usize;

pub trait FlexibleMemory {
    fn transmute_page<T>(&self) -> &T;
    fn transmute_segment<T>(&self, offset: usize) -> &T;

    fn transmute_page_mut<T>(&self) -> &mut T;
    fn transmute_segment_mut<T>(&self, offset: usize) -> &mut T;
}

pub trait UndefinedBehavior {
    fn borrow_mut(&self) -> &mut Self;
}

/// OBVIOUSLY VERY UNSAFE.
/// TODO: MAKE THIS BETTER?
impl UndefinedBehavior for Page {
    fn borrow_mut(&self) -> &mut Page {
        let ptr: *const Page = self;
        unsafe {
            mem::transmute(ptr)
        }
    }
}

impl FlexibleMemory for Page {
    fn transmute_page<T>(&self) -> &T {
        let subsection = &self[0];
        unsafe {
            mem::transmute(subsection)
        }
    }

    fn transmute_segment<T>(&self, offset: usize) -> &T {
        let subsection = &self[offset];
        unsafe {
            mem::transmute(subsection)
        }
    }

    fn transmute_page_mut<T>(&self) -> &mut T {
        let subsection = &mut self.borrow_mut()[0];
        unsafe {
            mem::transmute(subsection)
        }
    }

    fn transmute_segment_mut<T>(&self, offset: usize) -> &mut T {
        let subsection = &mut self.borrow_mut()[offset];
        unsafe {
            mem::transmute(subsection)
        }
    }
}

#[test]
fn test_transmute_whole() {
    #[repr(C)]
    struct Composite {
        signed: isize,
        unsigned: usize,
    }

    let mut p = [0u8; 0x1000];
    {
        let c = p.transmute_page_mut::<Composite>();
        c.signed = -17;
        c.unsigned = 56;
    }

    assert_eq!([239, 255, 255, 255, 255, 255, 255, 255][..], p[0..8]);
    assert_eq!([56, 0, 0, 0, 0, 0, 0, 0][..], p[8..16]);
}

#[test]
fn test_transmute_part() {
    #[repr(C)]
    struct Composite {
        signed: isize,
        unsigned: usize,
    }

    let mut p = [0u8; 0x1000];
    {
        let c = p.transmute_segment_mut::<Composite>(64);
        c.signed = -17;
        c.unsigned = 56;
    }

    assert_eq!([239, 255, 255, 255, 255, 255, 255, 255][..], p[64..72]);
    assert_eq!([56, 0, 0, 0, 0, 0, 0, 0][..], p[72..80]);
}
