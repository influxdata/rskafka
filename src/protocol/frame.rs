//! Read and write message frames from wire.
//!
//! # References
//! - <https://kafka.apache.org/protocol#protocol_common>

use std::io::Cursor;

use async_trait::async_trait;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{
    primitives::Int32,
    traits::{ReadType, WriteType},
};

#[derive(Error, Debug)]
pub enum ReadError {
    #[error("Cannot read data")]
    IO(#[from] std::io::Error),

    #[error("Negative message size: {size}")]
    NegativeMessageSize { size: i32 },

    #[error("Message too large, limit is {limit} bytes but got {actual} bytes")]
    MessageTooLarge { limit: usize, actual: usize },
}

#[async_trait]
pub trait AsyncMessageRead {
    async fn read_message(&mut self, max_message_size: usize) -> Result<Vec<u8>, ReadError>;
}

#[async_trait]
impl<R> AsyncMessageRead for R
where
    R: AsyncRead + Send + Unpin,
{
    async fn read_message(&mut self, max_message_size: usize) -> Result<Vec<u8>, ReadError> {
        let mut len_buf = vec![0u8; 4];
        self.read_exact(&mut len_buf).await?;
        let len = Int32::read(&mut Cursor::new(len_buf))
            .expect("Reading Int32 from in-mem buffer should always work");

        // convert to usize
        if len.0 < 0 {
            return Err(ReadError::NegativeMessageSize { size: len.0 });
        }
        let len = len.0 as usize;

        // check max message size to not blow up memory
        if len > max_message_size {
            // We need to seek so that next message is readable. However `self.seek` would require `R: AsyncSeek` which
            // doesn't hold for many types we wanna work with. So do some manual seeking.
            let mut to_read = len;
            let mut buf = vec![]; // allocate empty buffer
            while to_read > 0 {
                let step = max_message_size.min(to_read);

                // resize buffer if required
                if buf.len() != step {
                    buf = vec![0; step];
                }

                self.read_exact(&mut buf).await?;
                to_read -= step;
            }

            return Err(ReadError::MessageTooLarge {
                limit: max_message_size,
                actual: len,
            });
        }

        let mut buf = vec![0u8; len as usize];
        self.read_exact(&mut buf).await?;
        Ok(buf)
    }
}

#[derive(Error, Debug)]
pub enum WriteError {
    #[error("Cannot write data")]
    IO(#[from] std::io::Error),

    #[error("Message too large: {size}")]
    TooLarge { size: usize },
}

#[async_trait]
pub trait AsyncMessageWrite {
    async fn write_message(&mut self, msg: &[u8]) -> Result<(), WriteError>;
}

#[async_trait]
impl<W> AsyncMessageWrite for W
where
    W: AsyncWrite + Send + Unpin,
{
    async fn write_message(&mut self, msg: &[u8]) -> Result<(), WriteError> {
        let mut len_buf = Vec::<u8>::with_capacity(4);
        let len =
            Int32(i32::try_from(msg.len()).map_err(|_| WriteError::TooLarge { size: msg.len() })?);
        len.write(&mut len_buf)
            .expect("Int32 should always be writable to in-mem buffer");

        self.write(len_buf.as_ref()).await?;
        self.write(msg).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_matches::assert_matches;

    #[tokio::test]
    async fn test_read_negative_size() {
        let mut data = vec![];
        Int32(-1).write(&mut data).unwrap();

        let err = Cursor::new(data).read_message(100).await.unwrap_err();
        assert_matches!(err, ReadError::NegativeMessageSize { .. });
        assert_eq!(err.to_string(), "Negative message size: -1");
    }

    #[tokio::test]
    async fn test_read_too_large() {
        let mut data = vec![];
        data.write_message("foooo".as_bytes()).await.unwrap();
        data.write_message("bar".as_bytes()).await.unwrap();

        let mut stream = Cursor::new(data);

        let err = stream.read_message(3).await.unwrap_err();
        assert_matches!(err, ReadError::MessageTooLarge { .. });
        assert_eq!(
            err.to_string(),
            "Message too large, limit is 3 bytes but got 5 bytes"
        );

        // second message should still be readable
        let data = stream.read_message(3).await.unwrap();
        assert_eq!(&data, "bar".as_bytes());
    }

    #[tokio::test]
    async fn test_write_too_large() {
        let mut stream = vec![];
        let msg = vec![0u8; (i32::MAX as usize) + 1];
        let err = stream.write_message(&msg).await.unwrap_err();
        assert_matches!(err, WriteError::TooLarge { .. });
        assert_eq!(err.to_string(), "Message too large: 2147483648");
    }
}
