//! Individual API messages.
//!
//! # References
//! - <https://kafka.apache.org/protocol#protocol_messages>

use std::io::{Read, Write};

use thiserror::Error;

use crate::protocol::primitives::TaggedFields;

use super::{
    api_key::ApiKey,
    api_version::ApiVersion,
    error::Error as ApiError,
    primitives::{CompactString, Int16, Int32, NullableString},
    traits::{ReadError, ReadType, WriteError, WriteType},
};

#[derive(Error, Debug)]
pub enum ReadVersionedError {
    #[error("Invalid version: {version:?}")]
    InvalidVersion { version: ApiVersion },

    #[error(transparent)]
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
    #[error("Invalid version: {version:?}")]
    InvalidVersion { version: ApiVersion },

    #[error(transparent)]
    WriteError(#[from] WriteError),
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

#[derive(Debug)]
pub struct RequestHeader {
    /// The API key of this request.
    pub request_api_key: ApiKey,

    /// The API version of this request.
    pub request_api_version: ApiVersion,

    /// The correlation ID of this request.
    pub correlation_id: Int32,

    /// The client ID string.
    pub client_id: NullableString,

    /// The tagged fields.
    pub tagged_fields: TaggedFields,
}

impl<W> WriteVersionedType<W> for RequestHeader
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        match version.0 .0 {
            0 => {
                Int16::from(self.request_api_key).write(writer)?;
                self.request_api_version.0.write(writer)?;
                self.correlation_id.write(writer)?;
                Ok(())
            }
            1 => {
                Int16::from(self.request_api_key).write(writer)?;
                self.request_api_version.0.write(writer)?;
                self.correlation_id.write(writer)?;
                self.client_id.write(writer)?;
                Ok(())
            }
            2 => {
                Int16::from(self.request_api_key).write(writer)?;
                self.request_api_version.0.write(writer)?;
                self.correlation_id.write(writer)?;
                self.client_id.write(writer)?;
                self.tagged_fields.write(writer)?;
                Ok(())
            }
            _ => Err(WriteVersionedError::InvalidVersion { version }),
        }
    }
}

#[derive(Debug)]
pub struct ResponseHeader {
    /// The correlation ID of this response.
    pub correlation_id: Int32,

    /// The tagged fields.
    pub tagged_fields: TaggedFields,
}

impl<R> ReadVersionedType<R> for ResponseHeader
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        match version.0 .0 {
            0 => Ok(Self {
                correlation_id: Int32::read(reader)?,
                tagged_fields: TaggedFields::default(),
            }),
            1 => Ok(Self {
                correlation_id: Int32::read(reader)?,
                tagged_fields: TaggedFields::read(reader)?,
            }),
            _ => Err(ReadVersionedError::InvalidVersion { version }),
        }
    }
}

pub trait RequestBody {
    type ResponseBody;
    const API_KEY: ApiKey;
    const API_VERSION_RANGE: (ApiVersion, ApiVersion);
}

pub struct ApiVersionsRequest {
    /// The name of the client.
    pub client_software_name: CompactString,

    /// The version of the client.
    pub client_software_version: CompactString,

    /// The tagged fields.
    pub tagged_fields: TaggedFields,
}

impl<W> WriteVersionedType<W> for ApiVersionsRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        match version.0 .0 {
            0..=2 => Ok(()),
            3 => {
                self.client_software_name.write(writer)?;
                self.client_software_version.write(writer)?;
                self.tagged_fields.write(writer)?;
                Ok(())
            }
            _ => Err(WriteVersionedError::InvalidVersion { version }),
        }
    }
}

impl RequestBody for ApiVersionsRequest {
    type ResponseBody = ApiVersionsResponse;
    const API_KEY: ApiKey = ApiKey::ApiVersions;
    const API_VERSION_RANGE: (ApiVersion, ApiVersion) =
        (ApiVersion(Int16(0)), ApiVersion(Int16(3)));
}

#[derive(Debug)]
pub struct ApiVersionsResponseApiKey {
    /// The API index.
    pub api_key: ApiKey,

    /// The minimum supported version, inclusive.
    pub min_version: ApiVersion,

    /// The maximum supported version, inclusive.
    pub max_version: ApiVersion,

    /// The tagged fields.
    ///
    /// Added in version 3
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ApiVersionsResponseApiKey
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        match version.0 .0 {
            0..=2 => Ok(Self {
                api_key: Int16::read(reader)?.into(),
                min_version: ApiVersion(Int16::read(reader)?),
                max_version: ApiVersion(Int16::read(reader)?),
                tagged_fields: None,
            }),
            3 => Ok(Self {
                api_key: Int16::read(reader)?.into(),
                min_version: ApiVersion(Int16::read(reader)?),
                max_version: ApiVersion(Int16::read(reader)?),
                tagged_fields: Some(TaggedFields::read(reader)?),
            }),
            _ => Err(ReadVersionedError::InvalidVersion { version }),
        }
    }
}

#[derive(Debug)]
pub struct ApiVersionsResponse {
    /// The top-level error code.
    pub error_code: Option<ApiError>,

    /// The APIs supported by the broker.
    pub api_keys: Vec<ApiVersionsResponseApiKey>,

    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1
    pub throttle_time_ms: Option<Int32>,

    /// The tagged fields.
    ///
    /// Added in version 3
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ApiVersionsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        if v > 3 {
            return Err(ReadVersionedError::InvalidVersion { version });
        }

        let error_code = ApiError::new(Int16::read(reader)?);
        let api_keys = read_versioned_array(reader, version)?.unwrap_or_default();
        let throttle_time_ms = (v > 0).then(|| Int32::read(reader)).transpose()?;
        let tagged_fields = (v >= 3).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            tagged_fields,
        })
    }
}

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
            Ok(Some(
                (0..len)
                    .map(|_| T::read_versioned(reader, version))
                    .collect::<Result<_, _>>()?,
            ))
        }
    }
}

#[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Copy, Clone, PartialEq)]
    struct VersionTest {
        version: ApiVersion,
    }

    impl<W: Write> WriteVersionedType<W> for VersionTest {
        fn write_versioned(
            &self,
            _writer: &mut W,
            version: ApiVersion,
        ) -> Result<(), WriteVersionedError> {
            assert_eq!(version, self.version);
            Ok(())
        }
    }

    impl<R: Read> ReadVersionedType<R> for VersionTest {
        fn read_versioned(
            _reader: &mut R,
            version: ApiVersion,
        ) -> Result<Self, ReadVersionedError> {
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
}
