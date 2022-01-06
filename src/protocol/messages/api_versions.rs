use std::io::{Read, Write};

use crate::protocol::{
    api_key::ApiKey,
    api_version::ApiVersion,
    error::Error as ApiError,
    primitives::{CompactString, Int16, Int32, TaggedFields},
    traits::{ReadType, WriteType},
};

use super::{
    read_versioned_array, ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError,
    WriteVersionedType,
};

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
        let v = version.0 .0;
        assert!(v <= 3);

        if v >= 3 {
            self.client_software_name.write(writer)?;
            self.client_software_version.write(writer)?;
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

impl RequestBody for ApiVersionsRequest {
    type ResponseBody = ApiVersionsResponse;
    const API_KEY: ApiKey = ApiKey::ApiVersions;
    const API_VERSION_RANGE: (ApiVersion, ApiVersion) =
        (ApiVersion(Int16(0)), ApiVersion(Int16(3)));
    const FIRST_TAGGED_FIELD_VERSION: ApiVersion = ApiVersion(Int16(3));
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
        let v = version.0 .0;
        assert!(v <= 3);

        Ok(Self {
            api_key: Int16::read(reader)?.into(),
            min_version: ApiVersion(Int16::read(reader)?),
            max_version: ApiVersion(Int16::read(reader)?),
            tagged_fields: (v >= 3).then(|| TaggedFields::read(reader)).transpose()?,
        })
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
        assert!(v <= 3);

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
