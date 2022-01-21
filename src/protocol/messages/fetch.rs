use std::io::{Read, Write};

use crate::protocol::{
    api_key::ApiKey,
    api_version::{ApiVersion, ApiVersionRange},
    error::Error as ApiError,
    messages::{read_versioned_array, write_versioned_array, IsolationLevel},
    primitives::{Int16, Int32, Int64, Int8, Records, String_},
    traits::{ReadType, WriteType},
};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};

#[derive(Debug)]
#[allow(missing_copy_implementations)]
pub struct FetchRequestPartition {
    /// The partition index.
    pub partition: Int32,

    /// The message offset.
    pub fetch_offset: Int64,

    /// The maximum bytes to fetch from this partition.
    ///
    /// See KIP-74 for cases where this limit may not be honored.
    pub partition_max_bytes: Int32,
}

impl<W> WriteVersionedType<W> for FetchRequestPartition
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

        self.partition.write(writer)?;
        self.fetch_offset.write(writer)?;
        self.partition_max_bytes.write(writer)?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct FetchRequestTopic {
    /// The name of the topic to fetch.
    pub topic: String_,

    /// The partitions to fetch.
    pub partitions: Vec<FetchRequestPartition>,
}

impl<W> WriteVersionedType<W> for FetchRequestTopic
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

        self.topic.write(writer)?;
        write_versioned_array(writer, version, Some(&self.partitions))?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct FetchRequest {
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    pub replica_id: Int32,

    /// The maximum time in milliseconds to wait for the response.
    pub max_wait_ms: Int32,

    /// The minimum bytes to accumulate in the response.
    pub min_bytes: Int32,

    /// The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
    ///
    /// Defaults to "no limit / max".
    ///
    /// Added in version 3.
    pub max_bytes: Option<Int32>,

    /// This setting controls the visibility of transactional records.
    ///
    /// Using `READ_UNCOMMITTED` (`isolation_level = 0`) makes all records visible. With `READ_COMMITTED`
    /// (`isolation_level = 1`), non-transactional and `COMMITTED` transactional records are visible. To be more
    /// concrete, `READ_COMMITTED` returns all data from offsets smaller than the current LSO (last stable offset), and
    /// enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard
    /// `ABORTED` transactional records.
    ///
    /// As per [KIP-98] the default is `READ_UNCOMMITTED`.
    ///
    /// Added in version 4.
    ///
    /// [KIP-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
    pub isolation_level: Option<IsolationLevel>,

    /// The topics to fetch.
    pub topics: Vec<FetchRequestTopic>,
}

impl<W> WriteVersionedType<W> for FetchRequest
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

        self.replica_id.write(writer)?;
        self.max_wait_ms.write(writer)?;
        self.min_bytes.write(writer)?;

        if v >= 3 {
            // defaults to "no limit / max".
            self.max_bytes.unwrap_or(Int32(i32::MAX)).write(writer)?;
        }

        if v >= 4 {
            // The default is `READ_UNCOMMITTED`.
            let level: Int8 = self.isolation_level.unwrap_or_default().into();
            level.write(writer)?;
        }

        write_versioned_array(writer, version, Some(&self.topics))?;

        Ok(())
    }
}

impl RequestBody for FetchRequest {
    type ResponseBody = FetchResponse;

    const API_KEY: ApiKey = ApiKey::Fetch;

    /// That's enough for now.
    ///
    /// Note that we do not support fetch request prior to version 4, since this is the version when message version 2
    /// was introduced ([KIP-98]).
    ///
    /// [KIP-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(4)), ApiVersion(Int16(4)));

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(Int16(12));
}

#[derive(Debug)]
#[allow(missing_copy_implementations)]
pub struct FetchResponseAbortedTransaction {
    /// The producer id associated with the aborted transaction.
    pub producer_id: Int64,

    /// The first offset in the aborted transaction.
    pub first_offset: Int64,
}

impl<R> ReadVersionedType<R> for FetchResponseAbortedTransaction
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(4 <= v && v <= 4);

        Ok(Self {
            producer_id: Int64::read(reader)?,
            first_offset: Int64::read(reader)?,
        })
    }
}

#[derive(Debug)]
pub struct FetchResponsePartition {
    /// The partition index.
    pub partition_index: Int32,

    /// The error code, or 0 if there was no fetch error.
    pub error_code: Option<ApiError>,

    /// The current high water mark.
    pub high_watermark: Int64,

    /// The last stable offset (or LSO) of the partition.
    ///
    /// This is the last offset such that the state of all transactional records prior to this offset have been decided
    /// (`ABORTED` or `COMMITTED`).
    ///
    /// Added in version 4.
    pub last_stable_offset: Option<Int64>,

    /// The aborted transactions.
    ///
    /// Added in version 4.
    pub aborted_transactions: Vec<FetchResponseAbortedTransaction>,

    /// The record data.
    pub records: Records,
}

impl<R> ReadVersionedType<R> for FetchResponsePartition
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 4);

        Ok(Self {
            partition_index: Int32::read(reader)?,
            error_code: ApiError::new(Int16::read(reader)?.0),
            high_watermark: Int64::read(reader)?,
            last_stable_offset: (v >= 4).then(|| Int64::read(reader)).transpose()?,
            aborted_transactions: (v >= 4)
                .then(|| read_versioned_array(reader, version))
                .transpose()?
                .flatten()
                .unwrap_or_default(),
            records: Records::read(reader)?,
        })
    }
}

#[derive(Debug)]
pub struct FetchResponseTopic {
    /// The topic name.
    pub topic: String_,

    /// The topic partitions.
    pub partitions: Vec<FetchResponsePartition>,
}

impl<R> ReadVersionedType<R> for FetchResponseTopic
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 4);

        Ok(Self {
            topic: String_::read(reader)?,
            partitions: read_versioned_array(reader, version)?.unwrap_or_default(),
        })
    }
}

#[derive(Debug)]
pub struct FetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 1.
    pub throttle_time_ms: Option<Int32>,

    /// The response topics.
    pub responses: Vec<FetchResponseTopic>,
}

impl<R> ReadVersionedType<R> for FetchResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 4);

        Ok(Self {
            throttle_time_ms: (v >= 1).then(|| Int32::read(reader)).transpose()?,
            responses: read_versioned_array(reader, version)?.unwrap_or_default(),
        })
    }
}
