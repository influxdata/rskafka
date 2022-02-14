use std::io::{Read, Write};

use crate::protocol::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    error::Error,
    messages::{
        read_compact_versioned_array, read_versioned_array, write_compact_versioned_array,
        write_versioned_array,
    },
    primitives::{CompactString, CompactStringRef, Int16, Int32, Int64, String_, TaggedFields},
    traits::{ReadType, WriteType},
};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};

#[derive(Debug)]
pub struct DeleteRequestPartition {
    /// The partition index.
    pub partition_index: Int32,

    /// The deletion offset.
    pub offset: Int64,

    /// The tagged fields.
    ///
    /// Added in version 2
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for DeleteRequestPartition
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

        self.partition_index.write(writer)?;
        self.offset.write(writer)?;

        if v >= 2 {
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

#[derive(Debug)]
pub struct DeleteRequestTopic {
    /// The topic name.
    pub name: String_,

    /// Each partition that we want to delete records from.
    pub partitions: Vec<DeleteRequestPartition>,

    /// The tagged fields.
    ///
    /// Added in version 2
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for DeleteRequestTopic
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

        if v >= 2 {
            CompactStringRef(&self.name.0).write(writer)?
        } else {
            self.name.write(writer)?;
        }

        if v >= 2 {
            write_compact_versioned_array(writer, version, Some(self.partitions.as_slice()))?;
        } else {
            write_versioned_array(writer, version, Some(self.partitions.as_slice()))?;
        }

        if v >= 2 {
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

#[derive(Debug)]
pub struct DeleteRecordsRequest {
    /// Each topic that we want to delete records from.
    pub topics: Vec<DeleteRequestTopic>,

    /// How long to wait for the deletion to complete, in milliseconds.
    pub timeout_ms: Int32,

    /// The tagged fields.
    ///
    /// Added in version 2
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for DeleteRecordsRequest
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

        if v >= 2 {
            write_compact_versioned_array(writer, version, Some(self.topics.as_slice()))?;
        } else {
            write_versioned_array(writer, version, Some(self.topics.as_slice()))?;
        }
        self.timeout_ms.write(writer)?;

        if v >= 2 {
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

impl RequestBody for DeleteRecordsRequest {
    type ResponseBody = DeleteRecordsResponse;

    const API_KEY: ApiKey = ApiKey::DeleteRecords;

    /// All versions.
    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(0)), ApiVersion(Int16(2)));

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(Int16(2));
}

#[derive(Debug)]
pub struct DeleteResponsePartition {
    /// The partition index.
    pub partition_index: Int32,

    /// The partition low water mark.
    pub low_watermark: Int64,

    /// The error code, or 0 if there was no error.
    pub error: Option<Error>,

    /// The tagged fields.
    ///
    /// Added in version 2
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for DeleteResponsePartition
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 2);

        let partition_index = Int32::read(reader)?;
        let low_watermark = Int64::read(reader)?;
        let error = Error::new(Int16::read(reader)?.0);
        let tagged_fields = (v >= 2).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            partition_index,
            low_watermark,
            error,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
pub struct DeleteResponseTopic {
    /// The topic name.
    pub name: String_,

    /// Each partition that we wanted to delete records from.
    pub partitions: Vec<DeleteResponsePartition>,

    /// The tagged fields.
    ///
    /// Added in version 2
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for DeleteResponseTopic
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 2);

        let name = if v >= 2 {
            String_(CompactString::read(reader)?.0)
        } else {
            String_::read(reader)?
        };
        let partitions = if v >= 2 {
            read_compact_versioned_array(reader, version)?.unwrap_or_default()
        } else {
            read_versioned_array(reader, version)?.unwrap_or_default()
        };
        let tagged_fields = (v >= 2).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            name,
            partitions,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
pub struct DeleteRecordsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the
    /// request did not violate any quota.
    pub throttle_time_ms: Int32,

    /// Each topic that we wanted to delete records from.
    pub topics: Vec<DeleteResponseTopic>,

    /// The tagged fields.
    ///
    /// Added in version 2
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for DeleteRecordsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 2);

        let throttle_time_ms = Int32::read(reader)?;
        let topics = if v >= 2 {
            read_compact_versioned_array(reader, version)?.unwrap_or_default()
        } else {
            read_versioned_array(reader, version)?.unwrap_or_default()
        };
        let tagged_fields = (v >= 2).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            throttle_time_ms,
            topics,
            tagged_fields,
        })
    }
}
