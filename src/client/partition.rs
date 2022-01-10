use crate::backoff::{Backoff, BackoffConfig};
use crate::protocol::messages::{FetchRequest, FetchRequestPartition, FetchRequestTopic};
use crate::record::RecordAndOffset;
use crate::{
    client::error::{Error, Result},
    connection::{BrokerConnection, BrokerConnector},
    protocol::{
        error::Error as ProtocolError,
        messages::{
            ListOffsetsRequest, ListOffsetsRequestPartition, ListOffsetsRequestTopic,
            ProduceRequest, ProduceRequestPartitionData, ProduceRequestTopicData,
        },
        primitives::*,
        record::{Record as ProtocolRecord, *},
    },
    record::Record,
};
use std::ops::Range;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::Mutex;

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
    brokers: Arc<BrokerConnector>,

    backoff_config: BackoffConfig,

    /// Current broker connection if any
    current_broker: Mutex<Option<BrokerConnection>>,
}

impl std::fmt::Debug for PartitionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionClient({}:{})", self.topic, self.partition)
    }
}

impl PartitionClient {
    pub(super) fn new(topic: String, partition: i32, brokers: Arc<BrokerConnector>) -> Self {
        Self {
            topic,
            partition,
            brokers,
            backoff_config: Default::default(),
            current_broker: Mutex::new(None),
        }
    }

    /// Produce a batch of records to the partition
    pub async fn produce(&self, records: Vec<Record>) -> Result<Vec<i64>> {
        // skip request entirely if `records` is empty
        if records.is_empty() {
            return Ok(vec![]);
        }

        let n = records.len() as i64;

        // TODO: Retry on failure

        // TODO: Verify this is the first timestamp in the batch and not the min
        let first_timestamp = records.first().unwrap().timestamp;
        let mut max_timestamp = first_timestamp;

        let records = records
            .into_iter()
            .enumerate()
            .map(|(offset_delta, record)| {
                max_timestamp = max_timestamp.max(record.timestamp);

                ProtocolRecord {
                    key: record.key,
                    value: record.value,
                    timestamp_delta: (record.timestamp - first_timestamp).whole_milliseconds()
                        as i64,
                    offset_delta: offset_delta as i32,
                    headers: record
                        .headers
                        .into_iter()
                        .map(|(key, value)| RecordHeader { key, value })
                        .collect(),
                }
            })
            .collect();

        let record_batch = ProduceRequestPartitionData {
            index: Int32(self.partition),
            records: Records(vec![RecordBatch {
                base_offset: 0,
                partition_leader_epoch: 0,
                last_offset_delta: n as i32 - 1,
                is_transactional: false,
                base_sequence: -1,
                compression: RecordBatchCompression::NoCompression,
                timestamp_type: RecordBatchTimestampType::CreateTime,
                producer_id: -1,
                producer_epoch: -1,
                first_timestamp: (first_timestamp.unix_timestamp_nanos() / 1_000_000) as i64,
                max_timestamp: (max_timestamp.unix_timestamp_nanos() / 1_000_000) as i64,
                records: ControlBatchOrRecords::Records(records),
            }]),
        };

        // build request
        let request = &ProduceRequest {
            transactional_id: crate::protocol::primitives::NullableString(None),
            acks: Int16(-1),
            timeout_ms: Int32(30_000),
            topic_data: vec![ProduceRequestTopicData {
                name: String_(self.topic.clone()),
                partition_data: vec![record_batch],
            }],
        };

        self.maybe_retry("produce", || async move {
            let broker = self.get_cached_leader().await?;
            let response = broker.request(&request).await?;

            if response.responses.len() != 1 {
                return Err(Error::InvalidResponse(format!(
                    "Expected one topic in response got: {}",
                    response.responses.len()
                )));
            }

            let response = response.responses.into_iter().next().unwrap();
            if response.name.0 != self.topic {
                return Err(Error::InvalidResponse(format!(
                    "Expected write for topic \"{}\" got \"{}\"",
                    self.topic, response.name.0,
                )));
            }

            if response.partition_responses.len() != 1 {
                return Err(Error::InvalidResponse(format!(
                    "Expected one partition got: {}",
                    response.partition_responses.len()
                )));
            }

            let response = response.partition_responses.into_iter().next().unwrap();
            if response.index.0 != self.partition {
                return Err(Error::InvalidResponse(format!(
                    "Expected partition {} for topic \"{}\" got {}",
                    self.partition, self.topic, response.index.0,
                )));
            }

            match response.error {
                Some(e) => Err(Error::ServerError(e, Default::default())),
                None => Ok((0..n).map(|x| x + response.base_offset.0).collect()),
            }
        })
        .await
    }

    /// Fetch `bytes` bytes of record data starting at sequence number `offset`
    ///
    /// Returns the records, and the current high watermark
    ///
    /// TODO: this should probably also return the offsets so we can issue the next request correctly
    pub async fn fetch_records(
        &self,
        offset: i64,
        bytes: Range<i32>,
    ) -> Result<(Vec<RecordAndOffset>, i64)> {
        let partition = self
            .maybe_retry("fetch_records", || async move {
                let response = self
                    .get_cached_leader()
                    .await?
                    .request(FetchRequest {
                        // normal consumer
                        replica_id: Int32(-1),
                        max_wait_ms: Int32(10_000),
                        min_bytes: Int32(bytes.start),
                        max_bytes: Some(Int32(bytes.end.saturating_sub(1))),
                        // `READ_COMMITTED`
                        isolation_level: Some(Int8(1)),
                        topics: vec![FetchRequestTopic {
                            topic: String_(self.topic.clone()),
                            partitions: vec![FetchRequestPartition {
                                partition: Int32(self.partition),
                                fetch_offset: Int64(offset),
                                partition_max_bytes: Int32(bytes.end.saturating_sub(1)),
                            }],
                        }],
                    })
                    .await?;

                if response.responses.len() != 1 {
                    return Err(Error::InvalidResponse(format!(
                        "Expected 1 topic to be returned but got {}",
                        response.responses.len()
                    )));
                }
                let topic = response
                    .responses
                    .into_iter()
                    .next()
                    .expect("just checked the length");

                if topic.topic.0 != self.topic {
                    return Err(Error::InvalidResponse(format!(
                        "Expected data for topic '{}' but got data for topic '{}'",
                        self.topic, topic.topic.0
                    )));
                }

                if topic.partitions.len() != 1 {
                    return Err(Error::InvalidResponse(format!(
                        "Expected 1 partition to be returned but got {}",
                        topic.partitions.len()
                    )));
                }
                let partition = topic
                    .partitions
                    .into_iter()
                    .next()
                    .expect("just checked length");

                if partition.partition_index.0 != self.partition {
                    return Err(Error::InvalidResponse(format!(
                        "Expected data for partition {} but got data for partition {}",
                        self.partition, partition.partition_index.0
                    )));
                }

                if let Some(err) = partition.error_code {
                    return Err(Error::ServerError(err, String::new()));
                }

                Ok(partition)
            })
            .await?;

        // extract records
        let mut records = vec![];
        for batch in partition.records.0 {
            match batch.records {
                ControlBatchOrRecords::ControlBatch(_) => {
                    // ignore
                }
                ControlBatchOrRecords::Records(protocol_records) => {
                    records.reserve(protocol_records.len());

                    for record in protocol_records {
                        let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
                            (batch.first_timestamp + record.timestamp_delta) as i128 * 1_000_000,
                        )
                        .map_err(|e| {
                            Error::InvalidResponse(format!("Cannot parse timestamp: {}", e))
                        })?;

                        records.push(RecordAndOffset {
                            record: Record {
                                key: record.key,
                                value: record.value,
                                headers: record
                                    .headers
                                    .into_iter()
                                    .map(|header| (header.key, header.value))
                                    .collect(),
                                timestamp,
                            },
                            offset: batch.base_offset + record.offset_delta as i64,
                        })
                    }
                }
            }
        }

        Ok((records, partition.high_watermark.0))
    }

    /// Get high watermark for this partition.
    pub async fn get_high_watermark(&self) -> Result<i64> {
        let partition = self
            .maybe_retry("get_high_watermark", || async move {
                let response = self
                    .get_cached_leader()
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
                let topic = response.topics.into_iter().next().unwrap();

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
                let partition = topic.partitions.into_iter().next().unwrap();

                if partition.partition_index.0 != self.partition {
                    return Err(Error::InvalidResponse(format!(
                        "Expected data for partition {} but got data for partition {}",
                        self.partition, partition.partition_index.0
                    )));
                }

                match partition.error_code {
                    Some(err) => Err(Error::ServerError(err, String::new())),
                    None => Ok(partition),
                }
            })
            .await?;

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

    /// Takes a `request_name` and a function yielding a fallible future
    /// and handles certain classes of error
    async fn maybe_retry<R, F, T>(&self, request_name: &str, f: R) -> Result<T>
    where
        R: Fn() -> F,
        F: std::future::Future<Output = Result<T>>,
    {
        let mut backoff = Backoff::new(&self.backoff_config);

        loop {
            let error = match f().await {
                Ok(v) => return Ok(v),
                Err(e) => e,
            };

            match error {
                Error::Connection(_) => self.invalidate_cached_leader_broker().await,
                Error::ServerError(ProtocolError::LeaderNotAvailable, _) => {}
                Error::ServerError(ProtocolError::OffsetNotAvailable, _) => {}
                Error::ServerError(ProtocolError::NotLeaderOrFollower, _) => {
                    self.invalidate_cached_leader_broker().await;
                }
                _ => {
                    println!(
                        "{} request encountered fatal error: {}",
                        request_name, error
                    );
                    return Err(error);
                }
            }

            let backoff = backoff.next();
            println!(
                "{} request encountered non-fatal error \"{}\" - backing off for {} seconds",
                request_name,
                error,
                backoff.as_secs()
            );
            tokio::time::sleep(backoff).await;
        }
    }

    /// Invalidate the cached broker connection
    async fn invalidate_cached_leader_broker(&self) {
        *self.current_broker.lock().await = None
    }

    /// Get the raw broker connection
    ///
    /// TODO: Make this private
    pub async fn get_cached_leader(&self) -> Result<BrokerConnection> {
        let mut current_broker = self.current_broker.lock().await;
        if let Some(broker) = &*current_broker {
            return Ok(Arc::clone(broker));
        }

        println!(
            "Creating new broker connection for partition {} in topic \"{}\"",
            self.partition, self.topic
        );

        let leader = self.get_leader().await?;
        let broker = self.brokers.connect(leader).await?.ok_or_else(|| {
            Error::InvalidResponse(format!(
                "Partition leader {} not found in metadata response",
                leader
            ))
        })?;
        *current_broker = Some(Arc::clone(&broker));
        Ok(broker)
    }

    /// Retrieve the broker ID of the partition leader
    async fn get_leader(&self) -> Result<i32> {
        let metadata = self
            .brokers
            .request_metadata(Some(vec![self.topic.clone()]))
            .await?;

        if metadata.topics.len() != 1 {
            return Err(Error::InvalidResponse(format!(
                "Expected one topic in response, got {}",
                metadata.topics.len()
            )));
        }

        let topic = metadata.topics.into_iter().next().unwrap();

        if topic.name.0 != self.topic {
            return Err(Error::InvalidResponse(format!(
                "Expected metadata for topic \"{}\" got \"{}\"",
                self.topic, topic.name.0
            )));
        }

        if let Some(e) = topic.error {
            // TODO: Add retry logic
            return Err(Error::ServerError(
                e,
                format!("error getting metadata for topic \"{}\"", self.topic),
            ));
        }

        let partition = topic
            .partitions
            .iter()
            .find(|p| p.partition_index.0 == self.partition)
            .ok_or_else(|| {
                Error::InvalidResponse(format!(
                    "Could not find metadata for partition {} in topic \"{}\"",
                    self.partition, self.topic
                ))
            })?;

        if let Some(e) = partition.error {
            // TODO: Add retry logic
            return Err(Error::ServerError(
                e,
                format!(
                    "error getting metadata for partition {} in topic \"{}\"",
                    self.partition, self.topic
                ),
            ));
        }

        println!(
            "Partition {} in topic \"{}\" has leader {}",
            self.partition, self.topic, partition.leader_id.0
        );
        Ok(partition.leader_id.0)
    }
}
