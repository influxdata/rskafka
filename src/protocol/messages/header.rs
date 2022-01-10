use std::io::{Read, Write};

use crate::protocol::{
    api_key::ApiKey,
    api_version::ApiVersion,
    primitives::{Int16, Int32, NullableString, TaggedFields},
    traits::{ReadType, WriteType},
};

use super::{ReadVersionedError, ReadVersionedType, WriteVersionedError, WriteVersionedType};

#[derive(Debug)]
pub struct RequestHeader {
    /// The API key of this request.
    pub request_api_key: ApiKey,

    /// The API version of this request.
    pub request_api_version: ApiVersion,

    /// The correlation ID of this request.
    pub correlation_id: Int32,

    /// The client ID string.
    ///
    /// Added in version 1.
    pub client_id: NullableString,

    /// The tagged fields.
    ///
    /// Added in version 2.
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
        let v = version.0 .0;
        assert!(v <= 2);

        Int16::from(self.request_api_key).write(writer)?;
        self.request_api_version.0.write(writer)?;
        self.correlation_id.write(writer)?;

        if v >= 1 {
            self.client_id.write(writer)?;
        }

        if v >= 2 {
            self.tagged_fields.write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct ResponseHeader {
    /// The correlation ID of this response.
    pub correlation_id: Int32,

    /// The tagged fields.
    ///
    /// Added in version 1.
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ResponseHeader
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 1);

        Ok(Self {
            correlation_id: Int32::read(reader)?,
            tagged_fields: (v >= 1).then(|| TaggedFields::read(reader)).transpose()?,
        })
    }
}

// this is not technically required for production but helpful for testing
impl<W> WriteVersionedType<W> for ResponseHeader
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v <= 1);

        self.correlation_id.write(writer)?;

        if v >= 1 {
            match self.tagged_fields.as_ref() {
                Some(tagged_fields) => {
                    tagged_fields.write(writer)?;
                }
                None => {
                    TaggedFields::default().write(writer)?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::messages::test_utils::test_roundtrip_versioned;

    use super::*;

    test_roundtrip_versioned!(
        ResponseHeader,
        ApiVersion(Int16(0)),
        ApiVersion(Int16(1)),
        test_roundtrip_response_header
    );
}
