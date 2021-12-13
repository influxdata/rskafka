//! Read and write message frames from wire.
//!
//! # References
//! - <https://kafka.apache.org/protocol#protocol_common>

use std::{io::Cursor, pin::Pin};

use pin_project_lite::pin_project;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{
    primitives::Int32,
    traits::{ReadType, WriteType},
};

pin_project! {
    pub struct MessageFramer<RW> {
        #[pin]
        inner: RW,
        max_message_size: usize,
    }
}

impl<RW> MessageFramer<RW> {
    pub fn inner_mut(&mut self) -> &mut RW {
        &mut self.inner
    }
}

#[derive(Error, Debug)]
pub enum ReadError {
    #[error("Cannot read data")]
    IO(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub enum WriteError {
    #[error("Cannot write data")]
    IO(#[from] std::io::Error),

    #[error("Message too large: {size}")]
    TooLarge { size: usize },
}

impl<RW> MessageFramer<RW>
where
    RW: AsyncRead + AsyncWrite,
{
    pub fn new(inner: RW, max_message_size: usize) -> Self {
        Self {
            inner,
            max_message_size,
        }
    }

    pub fn max_message_size(&self) -> usize {
        self.max_message_size
    }

    pub async fn read_message(self: Pin<&mut Self>) -> Result<Vec<u8>, ReadError> {
        let mut this = self.project();
        let mut len_buf = vec![0u8; 4];
        this.inner.read_exact(&mut len_buf).await?;
        let len = Int32::read(&mut Cursor::new(len_buf))
            .expect("Reading Int32 from in-mem buffer should always work");

        // TODO: check that len is non-negative
        // TODO: check max message size

        let mut buf = vec![0u8; len.0 as usize];
        this.inner.read_exact(&mut buf).await?;
        Ok(buf)
    }

    pub async fn write_message(self: Pin<&mut Self>, msg: &[u8]) -> Result<(), WriteError> {
        let mut len_buf = Vec::<u8>::with_capacity(4);
        let len =
            Int32(i32::try_from(msg.len()).map_err(|_| WriteError::TooLarge { size: msg.len() })?);
        len.write(&mut len_buf)
            .expect("Int32 should always be writable to in-mem buffer");

        let mut this = self.project();
        this.inner.write(len_buf.as_ref()).await?;
        this.inner.write(msg).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_matches::assert_matches;
    use tokio::pin;

    #[test]
    fn test_getters() {
        let inner = Cursor::new(vec![]);
        let framer = MessageFramer::new(inner, 42);
        assert_eq!(framer.max_message_size(), 42);
    }

    #[tokio::test]
    async fn test_write_too_large() {
        let inner = Cursor::new(vec![]);
        let framer = MessageFramer::new(inner, 42);
        pin!(framer);
        let msg = vec![0u8; (i32::MAX as usize) + 1];
        let err = framer.write_message(&msg).await.unwrap_err();
        assert_matches!(err, WriteError::TooLarge { .. });
        assert_eq!(err.to_string(), "Message too large: 2147483648");
    }
}
