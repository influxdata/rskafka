use std::io::{Read, Write};

use crate::protocol::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    error::Error,
    messages::{
        read_compact_versioned_array, read_versioned_array, write_compact_versioned_array,
        write_versioned_array,
    },
    primitives::{
        Array, CompactArrayRef, CompactNullableString, CompactString, CompactStringRef, Int16,
        Int32, Int8, NullableString, String_, TaggedFields,
    },
    traits::{ReadType, WriteType},
};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};

#[derive(Debug)]
pub struct ElectLeadersRequest {
    /// Type of elections to conduct for the partition.
    ///
    /// A value of `0` elects the preferred replica. A value of `1` elects the first live replica if there are no
    /// in-sync replica.
    ///
    /// Added in version 1.
    pub election_type: Int8,

    /// The topic partitions to elect leaders.
    pub topic_partitions: Vec<ElectLeadersTopicRequest>,

    /// The time in ms to wait for the election to complete.
    pub timeout_ms: Int32,

    /// The tagged fields.
    ///
    /// Added in version 2
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for ElectLeadersRequest
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

        if v >= 1 {
            self.election_type.write(writer)?;
        }

        if v >= 2 {
            write_compact_versioned_array(writer, version, Some(&self.topic_partitions))?;
        } else {
            write_versioned_array(writer, version, Some(&self.topic_partitions))?;
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

impl RequestBody for ElectLeadersRequest {
    type ResponseBody = ElectLeadersResponse;

    const API_KEY: ApiKey = ApiKey::ElectLeaders;

    /// All versions.
    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(0)), ApiVersion(Int16(2)));

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(Int16(2));
}

#[derive(Debug)]
pub struct ElectLeadersTopicRequest {
    /// The name of a topic.
    pub topic: String_,

    /// The partitions of this topic whose leader should be elected.
    pub partitions: Array<Int32>,

    /// The tagged fields.
    ///
    /// Added in version 2
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for ElectLeadersTopicRequest
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
            CompactStringRef(&self.topic.0).write(writer)?;
        } else {
            self.topic.write(writer)?;
        }

        if v >= 2 {
            CompactArrayRef(self.partitions.0.as_deref()).write(writer)?;
        } else {
            self.partitions.write(writer)?;
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
pub struct ElectLeadersResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the
    /// request did not violate any quota.
    pub throttle_time_ms: Int32,

    /// The top level response error code.
    ///
    /// Added in version 1.
    pub error: Option<Error>,

    /// The election results, or an empty array if the requester did not have permission and the request asks for all
    /// partitions.
    pub replica_election_results: Vec<ElectLeadersTopicResponse>,

    /// The tagged fields.
    ///
    /// Added in version 2
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ElectLeadersResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 2);

        let throttle_time_ms = Int32::read(reader)?;
        let error = (v >= 1)
            .then(|| Int16::read(reader))
            .transpose()?
            .and_then(|e| Error::new(e.0));
        let replica_election_results = if v >= 2 {
            read_compact_versioned_array(reader, version)?.unwrap_or_default()
        } else {
            read_versioned_array(reader, version)?.unwrap_or_default()
        };
        let tagged_fields = (v >= 2).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            throttle_time_ms,
            error,
            replica_election_results,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
pub struct ElectLeadersTopicResponse {
    /// The topic name.
    pub topic: String_,

    /// The results for each partition.
    pub partition_results: Vec<ElectLeadersPartitionResponse>,

    /// The tagged fields.
    ///
    /// Added in version 2
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ElectLeadersTopicResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 2);

        let topic = if v >= 2 {
            String_(CompactString::read(reader)?.0)
        } else {
            String_::read(reader)?
        };
        let partition_results = if v >= 2 {
            read_compact_versioned_array(reader, version)?.unwrap_or_default()
        } else {
            read_versioned_array(reader, version)?.unwrap_or_default()
        };
        let tagged_fields = (v >= 2).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            topic,
            partition_results,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
pub struct ElectLeadersPartitionResponse {
    /// The partition id.
    pub partition_id: Int32,

    /// The result error, or zero if there was no error.
    pub error: Option<Error>,

    /// The result message, or null if there was no error.
    pub error_message: NullableString,

    /// The tagged fields.
    ///
    /// Added in version 2
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for ElectLeadersPartitionResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 2);

        let partition_id = Int32::read(reader)?;
        let error = Error::new(Int16::read(reader)?.0);
        let error_message = if v >= 2 {
            NullableString(CompactNullableString::read(reader)?.0)
        } else {
            NullableString::read(reader)?
        };
        let tagged_fields = (v >= 2).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            partition_id,
            error,
            error_message,
            tagged_fields,
        })
    }
}
