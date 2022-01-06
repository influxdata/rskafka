use crate::{
    client::error::{Error, Result},
    connection::BrokerPool,
    protocol::{
        messages::{ListOffsetsRequest, ListOffsetsRequestPartition, ListOffsetsRequestTopic},
        primitives::{Int32, Int64, Int8, String_},
    },
    record::Record,
};
use std::ops::Range;
use std::sync::Arc;

/// Many operations must be performed on the leader for a partition
///
/// Additionally a partition is the unit of concurrency within Kafka
///
/// As such a [`PartitionClient`] is a dedicated connection to the broker that
/// is the leader for a given partition.
///
/// In the event of a leadership change, [`PartitionClient`] will transparently
/// handle migrating to the new broker
///
pub struct PartitionClient {
    topic: String,
    partition: i32,
    brokers: Arc<BrokerPool>,
}

impl PartitionClient {
    pub(super) fn new(topic: String, partition: i32, brokers: Arc<BrokerPool>) -> Self {
        Self {
            topic,
            partition,
            brokers,
        }
    }

    /// Produce a batch of records to the partition
    pub async fn produce_batch(&self, _records: Vec<Record>) -> Result<()> {
        todo!()
    }

    /// Fetch `bytes` bytes of record data starting at sequence number `offset`
    ///
    /// Returns the records, and the current high watermark
    pub async fn fetch_records(
        &self,
        _offset: i64,
        _bytes: Range<i32>,
    ) -> Result<(Vec<Record>, i64)> {
        todo!()
    }

    /// Get high watermark for this partition.
    pub async fn get_high_watermark(&self) -> Result<i64> {
        let response = self
            .brokers
            .get_cached_broker()
            .await?
            .request(ListOffsetsRequest {
                // `-1` because we're a normal consumer
                replica_id: Int32(-1),
                // `READ_COMMITTED`
                isolation_level: Some(Int8(1)),
                topics: vec![ListOffsetsRequestTopic {
                    name: String_(self.topic.to_owned()),
                    partitions: vec![ListOffsetsRequestPartition {
                        partition_index: Int32(self.partition),
                        // latest offset
                        timestamp: Int64(-1),
                        max_num_offsets: Some(Int32(1)),
                    }],
                }],
            })
            .await?;

        if response.topics.len() != 1 {
            return Err(Error::InvalidResponse(format!(
                "Expected 1 topic to be returned but got {}",
                response.topics.len()
            )));
        }
        let topic = &response.topics[0];

        if topic.name.0 != self.topic {
            return Err(Error::InvalidResponse(format!(
                "Expected data for topic '{}' but got data for topic '{}'",
                self.topic, topic.name.0
            )));
        }

        if topic.partitions.len() != 1 {
            return Err(Error::InvalidResponse(format!(
                "Expected 1 partition to be returned but got {}",
                topic.partitions.len()
            )));
        }
        let partition = &topic.partitions[0];

        if partition.partition_index.0 != self.partition {
            return Err(Error::InvalidResponse(format!(
                "Expected data for partition {} but got data for partition {}",
                self.partition, partition.partition_index.0
            )));
        }

        if let Some(err) = partition.error_code {
            return Err(Error::ServerError(err, String::new()));
        }

        match (
            partition.old_style_offsets.as_ref(),
            partition.offset.as_ref(),
        ) {
            // old style
            (Some(offsets), None) => match offsets.0.as_ref() {
                Some(offsets) => match offsets.len() {
                    1 => Ok(offsets[0].0),
                    n => Err(Error::InvalidResponse(format!(
                        "Expected 1 offset to be returned but got {}",
                        n
                    ))),
                },
                None => Err(Error::InvalidResponse(
                    "Got NULL as offset array".to_owned(),
                )),
            },
            // new style
            (None, Some(offset)) => Ok(offset.0),
            _ => unreachable!(),
        }
    }
}
