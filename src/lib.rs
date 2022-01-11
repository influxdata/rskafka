#![deny(
    rustdoc::broken_intra_doc_links,
    rustdoc::bare_urls,
    rust_2018_idioms,
    unsafe_code
)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]
mod backoff;
pub mod client;
mod connection;
mod messenger;
mod protocol;
pub mod record;

pub type ProtocolError = protocol::error::Error;
