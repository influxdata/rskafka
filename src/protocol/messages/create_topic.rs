use std::io::{Read, Write};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::error::Error;
use crate::protocol::messages::{read_versioned_array, write_versioned_array};
use crate::protocol::{
    api_key::ApiKey,
    api_version::ApiVersion,
    primitives::*,
    traits::{ReadType, WriteType},
};

#[derive(Debug)]
pub struct CreateTopicsRequest {
    /// The topics to create
    pub topics: Vec<CreateTopicRequest>,

    /// How long to wait in milliseconds before timing out the request.
    pub timeout_ms: Int32,

    /// If true, check that the topics can be created as specified, but don't create anything.
    ///
    /// Added in version 1
    pub validate_only: Option<Boolean>,
}

impl RequestBody for CreateTopicsRequest {
    type ResponseBody = CreateTopicsResponse;

    const API_KEY: ApiKey = ApiKey::CreateTopics;

    /// At the time of writing this is the same subset supported by rdkafka
    const API_VERSION_RANGE: (ApiVersion, ApiVersion) =
        (ApiVersion(Int16(0)), ApiVersion(Int16(4)));

    const FIRST_TAGGED_FIELD_VERSION: ApiVersion = ApiVersion(Int16(5));
}

impl<W> WriteVersionedType<W> for CreateTopicsRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v <= 4);
        if self.validate_only.is_some() && v < 1 {
            return Err(WriteVersionedError::FieldNotAvailable {
                version,
                field: "validate_only".to_string(),
            });
        }

        write_versioned_array(writer, version, Some(self.topics.as_slice()))?;
        self.timeout_ms.write(writer)?;

        if v >= 1 {
            match self.validate_only {
                Some(b) => b.write(writer)?,
                None => Boolean(false).write(writer)?,
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct CreateTopicRequest {
    /// The topic name
    pub name: String_,

    /// The number of partitions to create in the topic, or -1 if we are either
    /// specifying a manual partition assignment or using the default partitions.
    ///
    /// Note: default partition count requires broker version >= 2.4.0 (KIP-464)
    pub num_partitions: Int32,

    /// The number of replicas to create for each partition in the topic, or -1 if we are either
    /// specifying a manual partition assignment or using the default replication factor.
    ///
    /// Note: default replication factor requires broker version >= 2.4.0 (KIP-464)
    pub replication_factor: Int16,

    /// The manual partition assignment, or the empty array if we are using automatic assignment.
    pub assignments: Vec<CreateTopicAssignment>,

    /// The custom topic configurations to set.
    pub configs: Vec<CreateTopicConfig>,
}

impl<W> WriteVersionedType<W> for CreateTopicRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        self.name.write(writer)?;
        self.num_partitions.write(writer)?;
        self.replication_factor.write(writer)?;
        write_versioned_array(writer, version, Some(&self.assignments))?;
        write_versioned_array(writer, version, Some(&self.configs))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct CreateTopicAssignment {
    /// The partition index
    pub partition_index: Int32,

    /// The brokers to place the partition on
    pub broker_ids: Array<Int32>,
}

impl<W> WriteVersionedType<W> for CreateTopicAssignment
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        _version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        self.partition_index.write(writer)?;
        self.broker_ids.write(writer)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct CreateTopicConfig {
    /// The configuration name.
    pub name: String_,

    /// The configuration value.
    pub value: NullableString,
}

impl<W> WriteVersionedType<W> for CreateTopicConfig
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        _version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        self.name.write(writer)?;
        self.value.write(writer)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct CreateTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota
    /// violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 2
    pub throttle_time_ms: Option<Int32>,

    /// Results for each topic we tried to create.
    pub topics: Vec<CreateTopicResponse>,
}

impl<R> ReadVersionedType<R> for CreateTopicsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        let throttle_time_ms = (v >= 2).then(|| Int32::read(reader)).transpose()?;
        let topics = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

#[derive(Debug)]
pub struct CreateTopicResponse {
    /// The topic name.
    pub name: String_,

    /// The error code, or 0 if there was no error.
    pub error: Option<Error>,

    /// The error message
    ///
    /// Added in version 1
    pub error_message: Option<NullableString>,
}

impl<R> ReadVersionedType<R> for CreateTopicResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;

        let name = String_::read(reader)?;
        let error = Error::new(Int16::read(reader)?);
        let error_message = (v >= 1).then(|| NullableString::read(reader)).transpose()?;

        Ok(Self {
            name,
            error,
            error_message,
        })
    }
}
