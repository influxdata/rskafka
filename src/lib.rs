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
    clippy::disallowed_methods
)]

mod backoff;

pub use backoff::BackoffConfig;

pub mod build_info;

pub mod client;

mod connection;
#[cfg(feature = "unstable-fuzzing")]
pub mod messenger;
#[cfg(not(feature = "unstable-fuzzing"))]
mod messenger;

#[cfg(feature = "unstable-fuzzing")]
pub mod protocol;
#[cfg(not(feature = "unstable-fuzzing"))]
mod protocol;

pub mod record;

mod throttle;

pub mod topic;

// re-exports
pub use chrono;

mod validation;
