#[macro_use] extern crate error_type;
#[macro_use] extern crate lazy_static;

pub mod allocator;

mod slicebtree;
use std::borrow::Cow;

#[derive(Debug)]
pub enum LodestoneError {
    OutOfMemory(&'static str),
    InvalidReference(&'static str),
    UserError(&'static str),
}
