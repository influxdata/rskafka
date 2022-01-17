use std::io::{Read, Write};

use crate::protocol::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    error::Error,
    messages::{read_versioned_array, write_versioned_array},
    primitives::{Int16, Int32, Int64, NullableString, Records, String_},
    traits::{ReadType, WriteType},
};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};

#[derive(Debug)]
pub struct ProduceRequestPartitionData {
    /// The partition index.
    pub index: Int32,

    /// The record data to be produced.
    pub records: Records,
}

impl<W> WriteVersionedType<W> for ProduceRequestPartitionData
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v <= 7);

        self.index.write(writer)?;
        self.records.write(writer)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ProduceRequestTopicData {
    /// The topic name.
    pub name: String_,

    /// Each partition to produce to.
    pub partition_data: Vec<ProduceRequestPartitionData>,
}

impl<W> WriteVersionedType<W> for ProduceRequestTopicData
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v <= 7);

        self.name.write(writer)?;
        write_versioned_array(writer, version, Some(&self.partition_data))?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ProduceRequest {
    /// The transactional ID, or null if the producer is not transactional.
    ///
    /// Added in version 3.
    pub transactional_id: NullableString,

    /// The number of acknowledgments the producer requires the leader to have received before considering a request complete.
    ///
    /// Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    pub acks: Int16,

    /// The timeout to await a response in milliseconds.
    pub timeout_ms: Int32,

    /// Each topic to produce to.
    pub topic_data: Vec<ProduceRequestTopicData>,
}

impl<W> WriteVersionedType<W> for ProduceRequest
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v <= 7);

        if v >= 3 {
            self.transactional_id.write(writer)?;
        }
        self.acks.write(writer)?;
        self.timeout_ms.write(writer)?;
        write_versioned_array(writer, version, Some(&self.topic_data))?;

        Ok(())
    }
}

impl RequestBody for ProduceRequest {
    type ResponseBody = ProduceResponse;

    const API_KEY: ApiKey = ApiKey::Produce;

    /// That's enough for for.
    ///
    /// Note that we do not support produce request prior to version 3, since this is the version when message version 2
    /// was introduced ([KIP-98]).
    ///
    /// [KIP-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(3)), ApiVersion(Int16(7)));

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(Int16(9));
}

#[derive(Debug)]
pub struct ProduceResponsePartitionResponse {
    /// The partition index.
    pub index: Int32,

    /// Error code.
    pub error: Option<Error>,

    /// The base offset.
    pub base_offset: Int64,

    /// The timestamp returned by broker after appending the messages.
    ///
    /// If CreateTime is used for the topic, the timestamp will be -1. If LogAppendTime is used for the topic, the
    /// timestamp will be the broker local time when the messages are appended.
    ///
    /// Added in version 2.
    pub log_append_time_ms: Option<Int64>,

    /// The log start offset.
    ///
    /// Added in version 5.
    pub log_start_offset: Option<Int64>,
}

impl<R> ReadVersionedType<R> for ProduceResponsePartitionResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 7);

        Ok(Self {
            index: Int32::read(reader)?,
            error: Error::new(Int16::read(reader)?.0),
            base_offset: Int64::read(reader)?,
            log_append_time_ms: (v >= 2).then(|| Int64::read(reader)).transpose()?,
            log_start_offset: (v >= 5).then(|| Int64::read(reader)).transpose()?,
        })
    }
}

#[derive(Debug)]
pub struct ProduceResponseResponse {
    /// The topic name
    pub name: String_,

    /// Each partition that we produced to within the topic.
    pub partition_responses: Vec<ProduceResponsePartitionResponse>,
}

impl<R> ReadVersionedType<R> for ProduceResponseResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 7);

        Ok(Self {
            name: String_::read(reader)?,
            partition_responses: read_versioned_array(reader, version)?.unwrap_or_default(),
        })
    }
}

#[derive(Debug)]
pub struct ProduceResponse {
    /// Each produce response
    pub responses: Vec<ProduceResponseResponse>,

    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1.
    pub throttle_time_ms: Option<Int32>,
}

impl<R> ReadVersionedType<R> for ProduceResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 7);

        Ok(Self {
            responses: read_versioned_array(reader, version)?.unwrap_or_default(),
            throttle_time_ms: (v >= 1).then(|| Int32::read(reader)).transpose()?,
        })
    }
}
