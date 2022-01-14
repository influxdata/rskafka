use std::io::{Read, Write};

use super::{
    ReadVersionedError, ReadVersionedType, RequestBody, WriteVersionedError, WriteVersionedType,
};
use crate::protocol::api_version::ApiVersionRange;
use crate::protocol::error::Error;
use crate::protocol::messages::{
    read_compact_versioned_array, read_versioned_array, write_compact_versioned_array,
    write_versioned_array,
};
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

    /// The tagged fields.
    ///
    /// Added in version 5
    pub tagged_fields: Option<TaggedFields>,
}

impl RequestBody for CreateTopicsRequest {
    type ResponseBody = CreateTopicsResponse;

    const API_KEY: ApiKey = ApiKey::CreateTopics;

    /// Enough for now.
    const API_VERSION_RANGE: ApiVersionRange =
        ApiVersionRange::new(ApiVersion(Int16(0)), ApiVersion(Int16(5)));

    const FIRST_TAGGED_FIELD_IN_REQUEST_VERSION: ApiVersion = ApiVersion(Int16(5));
    const FIRST_TAGGED_FIELD_IN_RESPONSE_VERSION: ApiVersion =
        Self::FIRST_TAGGED_FIELD_IN_REQUEST_VERSION;
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
        assert!(v <= 5);

        if self.validate_only.is_some() && v < 1 {
            return Err(WriteVersionedError::FieldNotAvailable {
                version,
                field: "validate_only".to_string(),
            });
        }

        if v >= 5 {
            write_compact_versioned_array(writer, version, Some(self.topics.as_slice()))?;
        } else {
            write_versioned_array(writer, version, Some(self.topics.as_slice()))?;
        }
        self.timeout_ms.write(writer)?;

        if v >= 1 {
            match self.validate_only {
                Some(b) => b.write(writer)?,
                None => Boolean(false).write(writer)?,
            }
        }

        if v >= 5 {
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

    /// The tagged fields.
    ///
    /// Added in version 5
    pub tagged_fields: Option<TaggedFields>,
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
        let v = version.0 .0;
        assert!(v <= 5);

        if v >= 5 {
            CompactStringRef(&self.name.0).write(writer)?
        } else {
            self.name.write(writer)?;
        }

        self.num_partitions.write(writer)?;
        self.replication_factor.write(writer)?;

        if v >= 5 {
            write_compact_versioned_array(writer, version, Some(&self.assignments))?;
        } else {
            write_versioned_array(writer, version, Some(&self.assignments))?;
        }

        if v >= 5 {
            write_compact_versioned_array(writer, version, Some(&self.configs))?;
        } else {
            write_versioned_array(writer, version, Some(&self.configs))?;
        }

        if v >= 5 {
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
pub struct CreateTopicAssignment {
    /// The partition index
    pub partition_index: Int32,

    /// The brokers to place the partition on
    pub broker_ids: Array<Int32>,

    /// The tagged fields.
    ///
    /// Added in version 5
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for CreateTopicAssignment
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v <= 5);

        self.partition_index.write(writer)?;
        self.broker_ids.write(writer)?;

        if v >= 5 {
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
pub struct CreateTopicConfig {
    /// The configuration name.
    pub name: String_,

    /// The configuration value.
    pub value: NullableString,

    /// The tagged fields.
    ///
    /// Added in version 5
    pub tagged_fields: Option<TaggedFields>,
}

impl<W> WriteVersionedType<W> for CreateTopicConfig
where
    W: Write,
{
    fn write_versioned(
        &self,
        writer: &mut W,
        version: ApiVersion,
    ) -> Result<(), WriteVersionedError> {
        let v = version.0 .0;
        assert!(v <= 5);

        if v >= 5 {
            CompactStringRef(&self.name.0).write(writer)?;
        } else {
            self.name.write(writer)?;
        }

        if v >= 5 {
            CompactNullableStringRef(self.value.0.as_deref()).write(writer)?;
        } else {
            self.value.write(writer)?;
        }

        if v >= 5 {
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
pub struct CreateTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota
    /// violation, or zero if the request did not violate any quota.
    ///
    /// Added in version 2
    pub throttle_time_ms: Option<Int32>,

    /// Results for each topic we tried to create.
    pub topics: Vec<CreateTopicResponse>,

    /// The tagged fields.
    ///
    /// Added in version 5
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for CreateTopicsResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 5);

        let throttle_time_ms = (v >= 2).then(|| Int32::read(reader)).transpose()?;
        let topics = if v >= 5 {
            read_compact_versioned_array(reader, version)?.unwrap_or_default()
        } else {
            read_versioned_array(reader, version)?.unwrap_or_default()
        };
        let tagged_fields = (v >= 5).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            throttle_time_ms,
            topics,
            tagged_fields,
        })
    }
}

#[derive(Debug)]
pub struct CreateTopicResponseConfig {
    /// The configuration name.
    pub name: CompactString,

    /// The configuration value.
    pub value: CompactNullableString,

    /// True if the configuration is read-only.
    pub read_only: Boolean,

    /// The configuration source.
    pub config_source: Int8,

    /// True if this configuration is sensitive.
    pub is_sensitive: Boolean,

    /// The tagged fields.
    pub tagged_fields: TaggedFields,
}

impl<R> ReadVersionedType<R> for CreateTopicResponseConfig
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v == 5);

        Ok(Self {
            name: CompactString::read(reader)?,
            value: CompactNullableString::read(reader)?,
            read_only: Boolean::read(reader)?,
            config_source: Int8::read(reader)?,
            is_sensitive: Boolean::read(reader)?,
            tagged_fields: TaggedFields::read(reader)?,
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

    /// Number of partitions of the topic.
    ///
    /// Added in version 5
    pub num_partitions: Option<Int32>,

    /// Replication factor of the topic.
    ///
    /// Added in version 5.
    pub replication_factor: Option<Int16>,

    /// Configuration of the topic.
    ///
    /// Added in version 5
    pub configs: Vec<CreateTopicResponseConfig>,

    /// The tagged fields.
    ///
    /// Added in version 5
    pub tagged_fields: Option<TaggedFields>,
}

impl<R> ReadVersionedType<R> for CreateTopicResponse
where
    R: Read,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadVersionedError> {
        let v = version.0 .0;
        assert!(v <= 5);

        let name = if v >= 5 {
            String_(CompactString::read(reader)?.0)
        } else {
            String_::read(reader)?
        };
        let error = Error::new(Int16::read(reader)?.0);
        let error_message = (v >= 1)
            .then(|| {
                if v >= 5 {
                    Ok(NullableString(CompactNullableString::read(reader)?.0))
                } else {
                    NullableString::read(reader)
                }
            })
            .transpose()?;
        let num_partitions = (v >= 5).then(|| Int32::read(reader)).transpose()?;
        let replication_factor = (v >= 5).then(|| Int16::read(reader)).transpose()?;
        let configs = (v >= 5)
            .then(|| read_compact_versioned_array(reader, version))
            .transpose()?
            .flatten()
            .unwrap_or_default();
        let tagged_fields = (v >= 5).then(|| TaggedFields::read(reader)).transpose()?;

        Ok(Self {
            name,
            error,
            error_message,
            num_partitions,
            replication_factor,
            configs,
            tagged_fields,
        })
    }
}
