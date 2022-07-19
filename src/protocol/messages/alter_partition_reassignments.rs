use std::io::{Read, Write};

use crate::protocol::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    error::Error,
    messages::{read_compact_versioned_array, write_compact_versioned_array},
    primitives::{CompactArray, CompactNullableString, CompactString, Int16, Int32, TaggedFields},
    traits::{ReadType, WriteType},
};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};

#[derive(Debug)]
pub struct AlterPartitionReassignmentsRequest {
    /// The time in ms to wait for the request to complete.
    pub timeout_ms: Int32,

    /// The topics to reassign.
    pub topics: Vec<AlterPartitionReassignmentsTopicRequest>,

    /// The tagged fields.
    pub tagged_fields: TaggedFields,
}

impl<W> WriteVersionedType<W> for AlterPartitionReassignmentsRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v <= 0);

        self.timeout_ms.write(writer)?;
        write_compact_versioned_array(writer, version, Some(&self.topics))?;
        self.tagged_fields.write(writer)?;

        Ok(())
    }
}

impl RequestBody for AlterPartitionReassignmentsRequest {
    type ResponseBody = AlterPartitionReassignmentsResponse;

    const API_KEY: ApiKey = ApiKey::AlterPartitionReassignments;

    /// All versions.
    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(0)), ApiVersion(Int16(0)));

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(Int16(0));
}

#[derive(Debug)]
pub struct AlterPartitionReassignmentsTopicRequest {
    /// The topic name.
    pub name: CompactString,

    /// The partitions to reassign.
    pub partitions: Vec<AlterPartitionReassignmentsPartitionRequest>,

    /// The tagged fields.
    pub tagged_fields: TaggedFields,
}

impl<W> WriteVersionedType<W> for AlterPartitionReassignmentsTopicRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v <= 0);

        self.name.write(writer)?;
        write_compact_versioned_array(writer, version, Some(&self.partitions))?;
        self.tagged_fields.write(writer)?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct AlterPartitionReassignmentsPartitionRequest {
    /// The partition index.
    pub partition_index: Int32,

    /// The replicas to place the partitions on, or null to cancel a pending reassignment for this partition.
    pub replicas: CompactArray<Int32>,

    /// The tagged fields.
    pub tagged_fields: TaggedFields,
}

impl<W> WriteVersionedType<W> for AlterPartitionReassignmentsPartitionRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v <= 0);

        self.partition_index.write(writer)?;
        self.replicas.write(writer)?;
        self.tagged_fields.write(writer)?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct AlterPartitionReassignmentsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the
    /// request did not violate any quota.
    pub throttle_time_ms: Int32,

    /// The top-level error code, or 0 if there was no error.
    pub error: Option<Error>,

    /// The top-level error message, or null if there was no error.
    pub error_message: CompactNullableString,

    /// The responses to topics to reassign.
    pub responses: Vec<AlterPartitionReassignmentsTopicResponse>,

    /// The tagged fields.
    pub tagged_fields: TaggedFields,
}

impl<R> ReadVersionedType<R> for AlterPartitionReassignmentsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 0);

        let throttle_time_ms = Int32::read(reader)?;
        let error = Error::new(Int16::read(reader)?.0);
        let error_message = CompactNullableString::read(reader)?;
        let responses = read_compact_versioned_array(reader, version)?.unwrap_or_default();
        let tagged_fields = TaggedFields::read(reader)?;

        Ok(Self {
            throttle_time_ms,
            error,
            error_message,
            responses,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
pub struct AlterPartitionReassignmentsTopicResponse {
    /// The topic name
    pub name: CompactString,

    /// The responses to partitions to reassign
    pub partitions: Vec<AlterPartitionReassignmentsPartitionResponse>,

    /// The tagged fields.
    pub tagged_fields: TaggedFields,
}

impl<R> ReadVersionedType<R> for AlterPartitionReassignmentsTopicResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 0);

        let name = CompactString::read(reader)?;
        let partitions = read_compact_versioned_array(reader, version)?.unwrap_or_default();
        let tagged_fields = TaggedFields::read(reader)?;

        Ok(Self {
            name,
            partitions,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
pub struct AlterPartitionReassignmentsPartitionResponse {
    /// The partition index.
    pub partition_index: Int32,

    /// The error code for this partition, or 0 if there was no error.
    pub error: Option<Error>,

    /// The error message for this partition, or null if there was no error.
    pub error_message: CompactNullableString,

    /// The tagged fields.
    pub tagged_fields: TaggedFields,
}

impl<R> ReadVersionedType<R> for AlterPartitionReassignmentsPartitionResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 0);

        let partition_index = Int32::read(reader)?;
        let error = Error::new(Int16::read(reader)?.0);
        let error_message = CompactNullableString::read(reader)?;
        let tagged_fields = TaggedFields::read(reader)?;

        Ok(Self {
            partition_index,
            error,
            error_message,
            tagged_fields,
        })
    }
}
