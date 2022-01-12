//! `ListOffsets` request and response.
//!
//! # References
//! - [KIP-79](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65868090)
//! - [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
use std::io::{Read, Write};

use crate::protocol::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    error::Error as ApiError,
    messages::{read_versioned_array, write_versioned_array},
    primitives::{Array, Int16, Int32, Int64, Int8, String_},
    traits::{ReadType, WriteType},
};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};

#[derive(Debug)]
pub struct ListOffsetsRequestPartition {
    /// The partition index.
    pub partition_index: Int32,

    /// The current timestamp.
    ///
    /// Depending on the version this will return:
    ///
    /// - **version 0:** `max_num_offsets` offsets that are smaller/equal than this timestamp.
    /// - **version 1 and later:** return timestamp and offset of the first/message greater/equal than this timestamp
    ///
    /// Per [KIP-79] this can have the following special values:
    ///
    /// - `-1`: latest offset
    /// - `-2`: earlist offset
    ///
    /// [KIP-79]: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65868090
    pub timestamp: Int64,

    /// The maximum number of offsets to report.
    ///
    /// Defaults to 1.
    ///
    /// Removed in version 1.
    pub max_num_offsets: Option<Int32>,
}

impl<W> WriteVersionedType<W> for ListOffsetsRequestPartition
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

        self.partition_index.write(writer)?;
        self.timestamp.write(writer)?;

        if v < 1 {
            // Only fetch 1 offset by default.
            self.max_num_offsets.unwrap_or(Int32(1)).write(writer)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ListOffsetsRequestTopic {
    /// The topic name.
    pub name: String_,

    /// Each partition in the request.
    pub partitions: Vec<ListOffsetsRequestPartition>,
}

impl<W> WriteVersionedType<W> for ListOffsetsRequestTopic
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

        self.name.write(writer)?;
        write_versioned_array(writer, version, Some(&self.partitions))?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ListOffsetsRequest {
    /// The broker ID of the requestor, or -1 if this request is being made by a normal consumer.
    pub replica_id: Int32,

    /// This setting controls the visibility of transactional records.
    ///
    /// Using `READ_UNCOMMITTED` (`isolation_level = 0`) makes all records visible. With `READ_COMMITTED`
    /// (`isolation_level = 1`), non-transactional and `COMMITTED` transactional records are visible. To be more
    /// concrete, `READ_COMMITTED` returns all data from offsets smaller than the current LSO (last stable offset), and
    /// enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard
    /// `ABORTED` transactional records.
    ///
    /// As per [KIP-98] the default is default is `READ_UNCOMMITTED`.
    ///
    /// Added in version 2.
    ///
    /// [KIP-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
    pub isolation_level: Option<Int8>,

    /// Each topic in the request.
    pub topics: Vec<ListOffsetsRequestTopic>,
}

impl<W> WriteVersionedType<W> for ListOffsetsRequest
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

        self.replica_id.write(writer)?;

        if v >= 2 {
            // The default is default is `READ_UNCOMMITTED`.
            self.isolation_level.unwrap_or(Int8(0)).write(writer)?;
        }

        write_versioned_array(writer, version, Some(&self.topics))?;

        Ok(())
    }
}

impl RequestBody for ListOffsetsRequest {
    type ResponseBody = ListOffsetsResponse;

    const API_KEY: ApiKey = ApiKey::ListOffsets;

    /// At the time of writing this is the same subset supported by rdkafka
    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(0)), ApiVersion(Int16(3)));

    const FIRST_TAGGED_FIELD_VERSION: ApiVersion = ApiVersion(Int16(6));
}

#[derive(Debug)]
pub struct ListOffsetsResponsePartition {
    /// The partition index.
    pub partition_index: Int32,

    /// The partition error code, or 0 if there was no error.
    pub error_code: Option<ApiError>,

    /// The result offsets.
    ///
    /// Removed in version 1.
    pub old_style_offsets: Option<Array<Int64>>,

    /// The timestamp associated with the returned offset.
    ///
    /// Added in version 1.
    pub timestamp: Option<Int64>,

    /// The returned offset.
    ///
    /// Added in version 1.
    pub offset: Option<Int64>,
}

impl<R> ReadVersionedType<R> for ListOffsetsResponsePartition
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 3);

        Ok(Self {
            partition_index: Int32::read(reader)?,
            error_code: ApiError::new(Int16::read(reader)?),
            old_style_offsets: (v < 1).then(|| Array::read(reader)).transpose()?,
            timestamp: (v >= 1).then(|| Int64::read(reader)).transpose()?,
            offset: (v >= 1).then(|| Int64::read(reader)).transpose()?,
        })
    }
}

#[derive(Debug)]
pub struct ListOffsetsResponseTopic {
    /// The topic name.
    pub name: String_,

    /// Each partition in the response.
    pub partitions: Vec<ListOffsetsResponsePartition>,
}

impl<R> ReadVersionedType<R> for ListOffsetsResponseTopic
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 3);

        Ok(Self {
            name: String_::read(reader)?,
            partitions: read_versioned_array(reader, version)?.unwrap_or_default(),
        })
    }
}

#[derive(Debug)]
pub struct ListOffsetsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 2.
    pub throttle_time_ms: Option<Int32>,

    /// Each topic in the response.
    pub topics: Vec<ListOffsetsResponseTopic>,
}

impl<R> ReadVersionedType<R> for ListOffsetsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 3);

        Ok(Self {
            throttle_time_ms: (v >= 2).then(|| Int32::read(reader)).transpose()?,
            topics: read_versioned_array(reader, version)?.unwrap_or_default(),
        })
    }
}
