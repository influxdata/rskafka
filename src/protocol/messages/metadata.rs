use std::io::{Read, Write};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::messages::{read_versioned_array, write_versioned_array};
use crate::protocol::{
    api_key::ApiKey,
    api_version::ApiVersion,
    primitives::*,
    traits::{ReadType, WriteType},
};

#[derive(Debug)]
pub struct MetadataRequest {
    /// The topics to fetch metadata for
    ///
    /// Requests data for all topics if None
    pub topics: Option<Vec<MetadataRequestTopic>>,

    /// If this is true, the broker may auto-create topics that we requested
    /// which do not already exist, if it is configured to do so.
    ///
    /// Added in version 4
    pub allow_auto_topic_creation: Option<Boolean>,
}

impl RequestBody for MetadataRequest {
    type ResponseBody = MetadataResponse;

    const API_KEY: ApiKey = ApiKey::Metadata;

    /// At the time of writing this is the same subset supported by rdkafka
    const API_VERSION_RANGE: (ApiVersion, ApiVersion) =
        (ApiVersion(Int16(0)), ApiVersion(Int16(4)));

    const FIRST_TAGGED_FIELD_VERSION: ApiVersion = ApiVersion(Int16(9));
}

impl<W> WriteVersionedType<W> for MetadataRequest
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

        if v < 4 && self.allow_auto_topic_creation.is_some() {
            return Err(WriteVersionedError::FieldNotAvailable {
                version,
                field: "allow_auto_topic_creation".to_string(),
            });
        }

        write_versioned_array(writer, version, self.topics.as_deref())?;
        if v >= 4 {
            match self.allow_auto_topic_creation {
                // The default behaviour is to allow topic creation
                None => Boolean(true).write(writer)?,
                Some(b) => b.write(writer)?,
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MetadataRequestTopic {
    /// The topic name
    pub name: String_,
}

impl<W> WriteVersionedType<W> for MetadataRequestTopic
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        assert!(version.0 .0 <= 4);
        Ok(self.name.write(writer)?)
    }
}

#[derive(Debug)]
pub struct MetadataResponse {
    /// The duration in milliseconds for which the request was throttled due to
    /// a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 3
    pub throttle_time_ms: Option<Int32>,

    /// Each broker in the response
    pub brokers: Vec<MetadataResponseBroker>,

    /// The cluster ID that responding broker belongs to.
    ///
    /// Added in version 2
    pub cluster_id: Option<NullableString>,

    /// The ID of the controller broker.
    ///
    /// Added in version 1
    pub controller_id: Option<Int32>,

    /// Each topic in the response
    pub topics: Vec<MetadataResponseTopic>,
}

impl<R> ReadVersionedType<R> for MetadataResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 4);

        let throttle_time_ms = (v >= 3).then(|| Int32::read(reader)).transpose()?;
        let brokers = read_versioned_array(reader, version)?.unwrap_or_default();
        let cluster_id = (v >= 2).then(|| NullableString::read(reader)).transpose()?;
        let controller_id = (v >= 1).then(|| Int32::read(reader)).transpose()?;
        let topics = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(Self {
            throttle_time_ms,
            brokers,
            topics,
            cluster_id,
            controller_id,
        })
    }
}

#[derive(Debug)]
pub struct MetadataResponseBroker {
    /// The broker ID
    pub node_id: Int32,
    /// The broker hostname
    pub host: String_,
    /// The broker port
    pub port: Int32,
    /// Added in version 1
    pub rack: Option<NullableString>,
}

impl<R> ReadVersionedType<R> for MetadataResponseBroker
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 4);

        let node_id = Int32::read(reader)?;
        let host = String_::read(reader)?;
        let port = Int32::read(reader)?;
        let rack = (v >= 1).then(|| NullableString::read(reader)).transpose()?;

        Ok(Self {
            node_id,
            host,
            port,
            rack,
        })
    }
}

#[derive(Debug)]
pub struct MetadataResponseTopic {
    /// The topic error, or 0 if there was no error
    pub error_code: Int16,
    /// The topic name
    pub name: String_,
    /// True if the topic is internal
    pub is_internal: Option<Boolean>,
    /// Each partition in the topic
    pub partitions: Vec<MetadataResponsePartition>,
}

impl<R> ReadVersionedType<R> for MetadataResponseTopic
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 4);

        let error_code = Int16::read(reader)?;
        let name = String_::read(reader)?;
        let is_internal = (v >= 1).then(|| Boolean::read(reader)).transpose()?;
        let partitions = read_versioned_array(reader, version)?.unwrap_or_default();

        Ok(Self {
            error_code,
            name,
            is_internal,
            partitions,
        })
    }
}

#[derive(Debug)]
pub struct MetadataResponsePartition {
    /// The partition error, or 0 if there was no error
    pub error_code: Int16,
    /// The partition index
    pub partition_index: Int32,
    /// The ID of the leader broker
    pub leader_id: Int32,
    /// The set of all nodes that host this partition
    pub replica_nodes: Array<Int32>,
    /// The set of all nodes that are in sync with the leader for this partition
    pub isr_nodes: Array<Int32>,
}

impl<R> ReadVersionedType<R> for MetadataResponsePartition
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 4);

        Ok(Self {
            error_code: Int16::read(reader)?,
            partition_index: Int32::read(reader)?,
            leader_id: Int32::read(reader)?,
            replica_nodes: Array::read(reader)?,
            isr_nodes: Array::read(reader)?,
        })
    }
}
