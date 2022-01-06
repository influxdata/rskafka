use super::Result;
use crate::connection::BrokerPool;
use crate::record::Record;
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
    #[allow(dead_code)]
    topic: String,

    #[allow(dead_code)]
    partition: i32,

    #[allow(dead_code)]
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
}
