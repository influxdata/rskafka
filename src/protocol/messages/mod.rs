//! Individual API messages.
//!
//! # References
//! - <https://kafka.apache.org/protocol#protocol_messages>
//! - <https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields>

use std::io::{Read, Write};

use thiserror::Error;

use super::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    primitives::{Int32, UnsignedVarint},
    traits::{ReadError, ReadType, WriteError, WriteType},
    vec_builder::VecBuilder,
};

mod api_versions;
pub use api_versions::*;
mod create_topics;
pub use create_topics::*;
mod fetch;
pub use fetch::*;
mod header;
pub use header::*;
mod list_offsets;
pub use list_offsets::*;
mod metadata;
pub use metadata::*;
mod produce;
pub use produce::*;
#[cfg(test)]
mod test_utils;

/// The `replica_id` to use to signify the request is being made by a normal consumer.
pub const NORMAL_CONSUMER: Int32 = Int32(-1);

#[derive(Error, Debug)]
pub enum ReadVersionedError {
    #[error("Read error: {0}")]
    ReadError(#[from] ReadError),
}

pub trait ReadVersionedType<R>: Sized
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError>;
}

#[derive(Error, Debug)]
pub enum WriteVersionedError {
    #[error("Write error: {0}")]
    WriteError(#[from] WriteError),

    #[error("Field {field} not available in version: {version:?}")]
    FieldNotAvailable { field: String, version: ApiVersion },
}

pub trait WriteVersionedType<W>: Sized
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError>;
}

impl<'a, W: Write, T: WriteVersionedType<W>> WriteVersionedType<W> for &'a T {
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        T::write_versioned(self, writer, version)
    }
}

/// Specifies a request body.
pub trait RequestBody {
    /// The response type that will follow when issuing this request.
    type ResponseBody;

    /// Kafka API key.
    ///
    /// This will be added to the request header.
    const API_KEY: ApiKey;

    /// Supported version range.
    ///
    /// From this range and the range that the broker reports, we will pick the highest version that both support.
    const API_VERSION_RANGE: ApiVersionRange;

    /// The first version of the messages (not of the header) that uses tagged fields, if any.
    ///
    /// To determine the version just look for the `_tagged_fields` or `TAG_BUFFER` in the protocol description.
    ///
    /// This will be used to control which request and response header versions will be used.
    ///
    /// It's OK to specify a version here that is larger then the highest supported version.
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion;

    /// Normally the same as [`FIRST_TAGGED_FIELD_IN_REQUEST_VERSION`](Self::FIRST_TAGGED_FIELD_IN_REQUEST_VERSION) but
    /// there are some special snowflakes.
    const FIRST_TAGGED_FIELD_IN_RESPONSE_VERSION: ApiVersion =
        Self::FIRST_TAGGED_FIELD_IN_REQUEST_VERSION;
}

impl<'a, T: RequestBody> RequestBody for &T {
    type ResponseBody = T::ResponseBody;
    const API_KEY: ApiKey = T::API_KEY;
    const API_VERSION_RANGE: ApiVersionRange = T::API_VERSION_RANGE;
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion =
        T::FIRST_TAGGED_FIELD_IN_REQUEST_VERSION;
    const FIRST_TAGGED_FIELD_IN_RESPONSE_VERSION: ApiVersion =
        T::FIRST_TAGGED_FIELD_IN_RESPONSE_VERSION;
}

/// Read an array of versioned objects.
///
/// Note that this is normally only used for messages that DO NOT contain tagged fields. All messages with tagged fields
/// normally use [`read_compact_versioned_array`] to comply with [KIP-482].
///
/// [KIP-482]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
fn read_versioned_array<R: Read, T: ReadVersionedType<R>>(
    reader: &mut R,
    version: ApiVersion,
) -> Result<Option<Vec<T>>, ReadVersionedError> {
    let len = Int32::read(reader)?.0;
    match len {
        -1 => Ok(None),
        l if l < -1 => Err(ReadVersionedError::ReadError(ReadError::Malformed(
            format!("Invalid negative length for array: {}", l).into(),
        ))),
        _ => {
            let len = len as usize;
            let mut builder = VecBuilder::new(len);
            for _ in 0..len {
                builder.push(T::read_versioned(reader, version)?);
            }
            Ok(Some(builder.into()))
        }
    }
}

/// Write an array of versioned objects.
///
/// Note that this is normally only used for messages that DO NOT contain tagged fields. All messages with tagged fields
/// normally use [`write_compact_versioned_array`] to comply with [KIP-482].
///
/// [KIP-482]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
fn write_versioned_array<W: Write, T: WriteVersionedType<W>>(
    writer: &mut W,
    version: ApiVersion,
    data: Option<&[T]>,
) -> Result<(), WriteVersionedError> {
    match data {
        None => Ok(Int32(-1).write(writer)?),
        Some(inner) => {
            let len = i32::try_from(inner.len()).map_err(WriteError::from)?;
            Int32(len).write(writer)?;

            for element in inner {
                element.write_versioned(writer, version)?
            }

            Ok(())
        }
    }
}

/// Read a compact array of versioned objects.
///
/// Note that this is normally only used for messages that DO contain tagged fields. All messages without tagged fields
/// normally use [`read_versioned_array`] to comply with [KIP-482].
///
/// [KIP-482]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
fn read_compact_versioned_array<R: Read, T: ReadVersionedType<R>>(
    reader: &mut R,
    version: ApiVersion,
) -> Result<Option<Vec<T>>, ReadVersionedError> {
    let len = UnsignedVarint::read(reader)?.0;
    match len {
        0 => Ok(None),
        n => {
            let len = (n - 1) as usize;
            let mut builder = VecBuilder::new(len);
            for _ in 0..len {
                builder.push(T::read_versioned(reader, version)?);
            }
            Ok(Some(builder.into()))
        }
    }
}

/// Write a compact array of versioned objects.
///
/// Note that this is normally only used for messages that DO contain tagged fields. All messages without tagged fields
/// normally use [`write_versioned_array`] to comply with [KIP-482].
///
/// [KIP-482]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
fn write_compact_versioned_array<W: Write, T: WriteVersionedType<W>>(
    writer: &mut W,
    version: ApiVersion,
    data: Option<&[T]>,
) -> Result<(), WriteVersionedError> {
    match data {
        None => Ok(UnsignedVarint(0).write(writer)?),
        Some(inner) => {
            let len = u64::try_from(inner.len() + 1).map_err(WriteError::from)?;
            UnsignedVarint(len).write(writer)?;

            for element in inner {
                element.write_versioned(writer, version)?
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use assert_matches::assert_matches;

    use crate::protocol::primitives::Int16;

    use super::*;

    #[derive(Debug, Copy, Clone, PartialEq)]
    struct VersionTest {
        version: ApiVersion,
    }

    impl<W: Write> WriteVersionedType<W> for VersionTest {
        fn write_versioned(
            &self,
            writer: &mut W,
            version: ApiVersion,
        ) -> Result<(), WriteVersionedError> {
            assert_eq!(version, self.version);
            Int32(42).write(writer)?;
            Ok(())
        }
    }

    impl<R: Read> ReadVersionedType<R> for VersionTest {
        fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
            assert_eq!(Int32::read(reader)?.0, 42);
            Ok(Self { version })
        }
    }

    #[test]
    fn test_read_write_versioned() {
        for len in [0, 6] {
            for i in 0..3 {
                let version = ApiVersion(Int16(i));
                let test = VersionTest { version };
                let input = vec![test; len];

                let mut buffer = vec![];
                write_versioned_array(&mut buffer, version, Some(&input)).unwrap();

                let mut cursor = std::io::Cursor::new(buffer);
                let output = read_versioned_array(&mut cursor, version).unwrap().unwrap();

                assert_eq!(input, output);
            }
        }

        let version = ApiVersion(Int16(0));
        let mut buffer = vec![];
        write_versioned_array::<_, VersionTest>(&mut buffer, version, None).unwrap();
        let mut cursor = std::io::Cursor::new(buffer);
        assert!(read_versioned_array::<_, VersionTest>(&mut cursor, version)
            .unwrap()
            .is_none())
    }

    #[test]
    fn test_read_versioned_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        Int32(i32::MAX).write(&mut buf).unwrap();
        buf.set_position(0);

        let err =
            read_versioned_array::<_, VersionTest>(&mut buf, ApiVersion(Int16(42))).unwrap_err();
        assert_matches!(err, ReadVersionedError::ReadError(ReadError::IO(_)));
    }

    #[test]
    fn test_read_write_compact_versioned() {
        for len in [0, 6] {
            for i in 0..3 {
                let version = ApiVersion(Int16(i));
                let test = VersionTest { version };
                let input = vec![test; len];

                let mut buffer = vec![];
                write_compact_versioned_array(&mut buffer, version, Some(&input)).unwrap();

                let mut cursor = std::io::Cursor::new(buffer);
                let output = read_compact_versioned_array(&mut cursor, version)
                    .unwrap()
                    .unwrap();

                assert_eq!(input, output);
            }
        }

        let version = ApiVersion(Int16(0));
        let mut buffer = vec![];
        write_compact_versioned_array::<_, VersionTest>(&mut buffer, version, None).unwrap();
        let mut cursor = std::io::Cursor::new(buffer);
        assert!(
            read_compact_versioned_array::<_, VersionTest>(&mut cursor, version)
                .unwrap()
                .is_none()
        )
    }

    #[test]
    fn test_read_compact_versioned_blowup_memory() {
        let mut buf = Cursor::new(Vec::<u8>::new());
        UnsignedVarint(u64::MAX).write(&mut buf).unwrap();
        buf.set_position(0);

        let err = read_compact_versioned_array::<_, VersionTest>(&mut buf, ApiVersion(Int16(42)))
            .unwrap_err();
        assert_matches!(err, ReadVersionedError::ReadError(ReadError::IO(_)));
    }
}
