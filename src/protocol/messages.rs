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
    primitives::{Array, CompactString, Int16, Int32, NullableString},
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

struct ReadVersionedAdapter<T, const V: i16>(T);

impl<T, W, const V: i16> ReadType<W> for ReadVersionedAdapter<T, V>
where
    T: ReadVersionedType<W>,
    W: Read,
{
    fn read(writer: &mut W) -> Result<Self, ReadError> {
        Ok(Self(
            T::read_versioned(writer, ApiVersion(Int16(V))).map_err(|e| match e {
                ReadVersionedError::InvalidVersion { version } => {
                    panic!("Invalid version: {:?}", version)
                }
                ReadVersionedError::ReadError(e) => e,
            })?,
        ))
    }
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

struct WriteVersionedAdapter<T, const V: i16>(T);

impl<T, W, const V: i16> WriteType<W> for WriteVersionedAdapter<T, V>
where
    T: WriteVersionedType<W>,
    W: Write,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        self.0
            .write_versioned(writer, ApiVersion(Int16(V)))
            .map_err(|e| match e {
                WriteVersionedError::InvalidVersion { version } => {
                    panic!("Invalid version: {:?}", version)
                }
                WriteVersionedError::WriteError(e) => e,
            })
    }
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
    pub tagged_fields: TaggedFields,
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
                tagged_fields: TaggedFields::default(),
            }),
            3 => Ok(Self {
                api_key: Int16::read(reader)?.into(),
                min_version: ApiVersion(Int16::read(reader)?),
                max_version: ApiVersion(Int16::read(reader)?),
                tagged_fields: TaggedFields::read(reader)?,
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
    pub throttle_time_ms: Int32,

    /// The tagged fields.
    pub tagged_fields: TaggedFields,
}

impl<R> ReadVersionedType<R> for ApiVersionsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        match version.0 .0 {
            0 => Ok(Self {
                error_code: ApiError::new(Int16::read(reader)?),
                api_keys: Array::<ReadVersionedAdapter<ApiVersionsResponseApiKey, 0>>::read(
                    reader,
                )?
                .0
                .unwrap_or_default()
                .into_iter()
                .map(|x| x.0)
                .collect(),
                throttle_time_ms: Int32(0),
                tagged_fields: TaggedFields::default(),
            }),
            1 => Ok(Self {
                error_code: ApiError::new(Int16::read(reader)?),
                api_keys: Array::<ReadVersionedAdapter<ApiVersionsResponseApiKey, 1>>::read(
                    reader,
                )?
                .0
                .unwrap_or_default()
                .into_iter()
                .map(|x| x.0)
                .collect(),
                throttle_time_ms: Int32::read(reader)?,
                tagged_fields: TaggedFields::default(),
            }),
            2 => Ok(Self {
                error_code: ApiError::new(Int16::read(reader)?),
                api_keys: Array::<ReadVersionedAdapter<ApiVersionsResponseApiKey, 2>>::read(
                    reader,
                )?
                .0
                .unwrap_or_default()
                .into_iter()
                .map(|x| x.0)
                .collect(),
                throttle_time_ms: Int32::read(reader)?,
                tagged_fields: TaggedFields::default(),
            }),
            3 => Ok(Self {
                error_code: ApiError::new(Int16::read(reader)?),
                api_keys: Array::<ReadVersionedAdapter<ApiVersionsResponseApiKey, 3>>::read(
                    reader,
                )?
                .0
                .unwrap_or_default()
                .into_iter()
                .map(|x| x.0)
                .collect(),
                throttle_time_ms: Int32::read(reader)?,
                tagged_fields: TaggedFields::read(reader)?,
            }),
            _ => Err(ReadVersionedError::InvalidVersion { version }),
        }
    }
}
