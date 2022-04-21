use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};

use crate::protocol::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    error::Error as ApiError,
    primitives::{Array, CompactBytes, CompactNullableString, Int16, Int64, String_, TaggedFields},
    traits::{ReadType, WriteType},
};

use std::io::{Read, Write};
#[derive(Debug)]
pub struct SaslHandshakeRequest {
    /// The SASL mechanism chosen by the client. e.g. PLAIN
    ///
    /// Added in version 0.
    pub mechanism: String_,
}

impl SaslHandshakeRequest {
    pub fn new(mechanism: &str) -> Self {
        return Self {
            mechanism: String_(mechanism.to_string()),
        };
    }
}

impl<R> ReadVersionedType<R> for SaslHandshakeRequest
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v == 1);
        Ok(Self {
            mechanism: String_::read(reader)?,
        })
    }
}

impl<W> WriteVersionedType<W> for SaslHandshakeRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v == 1);
        self.mechanism.write(writer)?;
        Ok(())
    }
}

impl RequestBody for SaslHandshakeRequest {
    type ResponseBody = SaslHandshakeResponse;
    const API_KEY: ApiKey = ApiKey::SaslHandshake;
    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(1)), ApiVersion(Int16(1)));
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(Int16(3));
}

#[derive(Debug)]
pub struct SaslHandshakeResponse {
    /// The error code, or 0 if there was no error.
    ///
    /// Added in version 0.
    pub error_code: Option<ApiError>,

    /// The mechanisms enabled in the server.
    ///
    /// Added in version 0.
    pub mechanisms: Array<String_>,
}

impl<R> ReadVersionedType<R> for SaslHandshakeResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v == 1);
        Ok(Self {
            error_code: ApiError::new(Int16::read(reader)?.0),
            mechanisms: Array::read(reader)?,
        })
    }
}

impl<W> WriteVersionedType<W> for SaslHandshakeResponse
where
    W: Write,
{
    fn write_versioned(
        &self,
        _writer: &mut W,
        _version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct SaslAuthenticateRequest {
    /// The SASL authentication bytes from the client, as defined by the SASL mechanism.
    /// 
    /// Added in version 0.
    pub auth_bytes: CompactBytes,
    
    /// The tagged fields
    /// 
    /// Added in version 2. 
    pub tagged_fields: Option<TaggedFields>,
}

impl SaslAuthenticateRequest {
    pub fn new(auth_bytes: Vec<u8>) -> Self {
        return Self {
            auth_bytes: CompactBytes(auth_bytes),
            tagged_fields: Some(TaggedFields::default()),
        };
    }
}

impl<R> ReadVersionedType<R> for SaslAuthenticateRequest
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v == 2);
        Ok(Self {
            auth_bytes: CompactBytes::read(reader)?,
            tagged_fields: (v >= 2).then(|| TaggedFields::read(reader)).transpose()?,
        })
    }
}

impl<W> WriteVersionedType<W> for SaslAuthenticateRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v == 2);
        self.auth_bytes.write(writer)?;
        match self.tagged_fields.as_ref() {
            Some(tagged_fields) => {
                tagged_fields.write(writer)?;
            }
            None => {
                TaggedFields::default().write(writer)?;
            }
        }
        Ok(())
    }
}

impl RequestBody for SaslAuthenticateRequest {
    type ResponseBody = SaslAuthenticateResponse;
    const API_KEY: ApiKey = ApiKey::SaslAuthenticate;
    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(2)), ApiVersion(Int16(2)));
    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(Int16(2));
}

#[derive(Debug)]
pub struct SaslAuthenticateResponse {
    /// The error code, or 0 if there was no error.
    /// 
    /// Added in version 0.
    pub error_code: Option<ApiError>,
    
    /// The error code, or 0 if there was no error.
    /// 
    /// Added in version 0
    pub error_message: CompactNullableString,
    
    /// The SASL authentication bytes from the server, as defined by the SASL mechanism.
    /// 
    /// Added in version 0
    pub auth_bytes: CompactBytes,
    
    /// The SASL authentication bytes from the server, as defined by the SASL mechanism.
    /// 
    /// Added in version 1.
    pub session_lifetime_ms: Int64,
    
    /// The tagged fields.
    /// 
    /// Added in version 2.
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for SaslAuthenticateResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v == 2);
        Ok(Self {
            error_code: ApiError::new(Int16::read(reader)?.0),
            error_message: CompactNullableString::read(reader)?,
            auth_bytes: CompactBytes::read(reader)?,
            session_lifetime_ms: Int64::read(reader)?,
            tagged_fields: (v >= 2).then(|| TaggedFields::read(reader)).transpose()?,
        })
    }
}

impl<W> WriteVersionedType<W> for SaslAuthenticateResponse
where
    W: Write,
{
    fn write_versioned(
        &self,
        _writer: &mut W,
        _version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        Ok(())
    }
}
