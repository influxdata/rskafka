use std::io::{Read, Write};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ReadError {
    #[error("Cannot read data")]
    IO(#[from] std::io::Error),

    #[error("Overflow converting integer")]
    Overflow(#[from] std::num::TryFromIntError),

    #[error(transparent)]
    Malformed(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub trait ReadType<R>: Sized
where
    R: Read,
{
    fn read(reader: &mut R) -> Result<Self, ReadError>;
}

#[derive(Error, Debug)]
pub enum WriteError {
    #[error("Cannot write data")]
    IO(#[from] std::io::Error),

    #[error("Overflow converting integer")]
    Overflow(#[from] std::num::TryFromIntError),

    #[error(transparent)]
    Malformed(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub trait WriteType<W>: Sized
where
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError>;
}
