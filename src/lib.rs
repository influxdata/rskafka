#![doc = include_str!("../README.md")]
#![deny(
    rustdoc::broken_intra_doc_links,
    rustdoc::bare_urls,
    rustdoc::invalid_codeblock_attributes,
    rustdoc::private_intra_doc_links,
    rust_2018_idioms,
    unsafe_code
)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::disallowed_method
)]

trait ExactlyOne<T> {
    fn exactly_one(self) -> Result<T, usize>;
}

impl<T> ExactlyOne<T> for Vec<T> {
    fn exactly_one(mut self) -> Result<T, usize> {
        let v = self.pop().ok_or(0_usize)?;
        if self.is_empty() {
            Ok(v)
        } else {
            Err(self.len() + 1)
        }
    }
}

mod backoff;

pub mod client;

mod connection;
#[cfg(feature = "fuzzing")]
pub mod messenger;
#[cfg(not(feature = "fuzzing"))]
mod messenger;

#[cfg(feature = "fuzzing")]
pub mod protocol;
#[cfg(not(feature = "fuzzing"))]
mod protocol;

pub mod record;

pub mod topic;

// re-exports
pub use time;
