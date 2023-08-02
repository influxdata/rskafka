use crate::{
    backoff::{Backoff, BackoffConfig, ErrorOrThrottle},
    client::error::{Error, RequestContext, Result},
    connection::{
        BrokerCache, BrokerCacheGeneration, BrokerConnection, BrokerConnector, MessengerTransport,
        MetadataLookupMode,
    },
    messenger::RequestError,
    protocol::{
        error::Error as ProtocolError,
        messages::{
            DeleteRecordsRequest, DeleteRecordsResponse, DeleteRequestPartition,
            DeleteRequestTopic, DeleteResponsePartition, FetchRequest, FetchRequestPartition,
            FetchRequestTopic, FetchResponse, FetchResponsePartition, IsolationLevel,
            ListOffsetsRequest, ListOffsetsRequestPartition, ListOffsetsRequestTopic,
            ListOffsetsResponse, ListOffsetsResponsePartition, ProduceRequest,
            ProduceRequestPartitionData, ProduceRequestTopicData, ProduceResponse, NORMAL_CONSUMER,
        },
        primitives::*,
        record::{Record as ProtocolRecord, *},
    },
    record::{Record, RecordAndOffset},
    throttle::maybe_throttle,
    validation::ExactlyOne,
};
use async_trait::async_trait;
use chrono::{LocalResult, TimeZone, Utc};
use std::{
    ops::{ControlFlow, Deref, Range},
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use super::{error::ServerErrorResponse, metadata_cache::MetadataCacheGeneration};

/// How strongly a [`PartitionClient`] is bound to a partition.
///
/// Under some circumstances and broker implementations, you might face a [`ProtocolError::UnknownTopicOrPartition`]
/// for nearly all operations directly after topic/partition creation. Since the Kafka protocol offers no (at least
/// to our knowledge) way to differentiate between "a topic/partition actually does not exist" and "our multiverse
/// split brain is not synced up yet", we can only offer you a manual way to wait for topic existence.
///
/// We offer the following ways to deal with this:
///
/// - Use a [`Error`](Self::Error). All other methods (including the creation of a [`PartitionClient`]) may produce
///   sporadic [`ProtocolError::UnknownTopicOrPartition`] errors.
/// - Use a [`Retry`](Self::Error) which assumes a partition exists and retries [`ProtocolError::UnknownTopicOrPartition`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownTopicHandling {
    /// When a [`ProtocolError::UnknownTopicOrPartition`] is returned by Kafka,
    /// it is passed up to the user to handle.
    ///
    /// These errors may occur spuriously.
    Error,

    /// Always retry operations that return a
    /// [`ProtocolError::UnknownTopicOrPartition`], until the operation succeeds
    /// or a different error is returned.
    ///
    /// This may unpredictably increase operation latency.
    Retry,
}

/// Compression of records.
///
/// This is only relevant for [produce requests](PartitionClient::produce). There the client compresses the records.
///
/// For [fetch requests](PartitionClient::fetch_records) we have to accept a message as it exists, i.e. how it was
/// originally compressed. The broker will NOT change the data compression based on our request.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    #[default]
    NoCompression,
    #[cfg(feature = "compression-gzip")]
    Gzip,
    #[cfg(feature = "compression-lz4")]
    Lz4,
    #[cfg(feature = "compression-snappy")]
    Snappy,
    #[cfg(feature = "compression-zstd")]
    Zstd,
}

/// Which type of offset should be requested by [`PartitionClient::get_offset`].
///
/// # Timestamp-based Queries
/// In theory the Kafka API would also support querying an offset based on a timestamp, but the behavior seems to be
/// semi-defined, unintuitive (even within Apache Kafka) and inconsistent between Apache Kafka and Redpanda. So we
/// decided to NOT expose this option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetAt {
    /// Earliest existing record.
    ///
    /// This is NOT the earliest produced record but the earliest record that is still kept, i.e. the offset might
    /// change if records are pruned by Kafka (retention policy) or if the are deleted.
    Earliest,

    /// The latest existing record.
    Latest,
}

#[derive(Debug)]
struct CurrentBroker {
    broker: Option<BrokerConnection>,
    gen_broker: BrokerCacheGeneration,
    gen_leader_from_arbitrary: Option<MetadataCacheGeneration>,
    gen_leader_from_self: Option<MetadataCacheGeneration>,
}

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

    backoff_config: Arc<BackoffConfig>,

    /// Current broker connection if any
    current_broker: Mutex<CurrentBroker>,

    unknown_topic_handling: UnknownTopicHandling,
}

impl std::fmt::Debug for PartitionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionClient({}:{})", self.topic, self.partition)
    }
}

impl PartitionClient {
    pub(super) async fn new(
        topic: String,
        partition: i32,
        brokers: Arc<BrokerConnector>,
        unknown_topic_handling: UnknownTopicHandling,
        backoff_config: Arc<BackoffConfig>,
    ) -> Result<Self> {
        let p = Self {
            topic,
            partition,
            brokers: Arc::clone(&brokers),
            backoff_config,
            current_broker: Mutex::new(CurrentBroker {
                broker: None,
                gen_broker: BrokerCacheGeneration::START,
                gen_leader_from_arbitrary: None,
                gen_leader_from_self: None,
            }),
            unknown_topic_handling,
        };

        // Force discover and establish a cached connection to the leader
        let scope = &p;
        maybe_retry(
            &p.backoff_config,
            p.unknown_topic_handling,
            &*brokers,
            "leader_detection",
            || async move {
                scope
                    .get()
                    .await
                    .map_err(|e| ErrorOrThrottle::Error((e, None)))?;
                Ok(())
            },
        )
        .await?;

        Ok(p)
    }

    /// Topic
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Partition
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Produce a batch of records to the partition
    pub async fn produce(
        &self,
        records: Vec<Record>,
        compression: Compression,
    ) -> Result<Vec<i64>> {
        // skip request entirely if `records` is empty
        if records.is_empty() {
            return Ok(vec![]);
        }

        let n = records.len() as i64;
        let request = &build_produce_request(self.partition, &self.topic, records, compression);

        maybe_retry(
            &self.backoff_config,
            self.unknown_topic_handling,
            self,
            "produce",
            || async move {
                let (broker, gen) = self
                    .get()
                    .await
                    .map_err(|e| ErrorOrThrottle::Error((e, None)))?;
                let response = broker
                    .request(&request)
                    .await
                    .map_err(|e| ErrorOrThrottle::Error((e.into(), Some(gen))))?;
                maybe_throttle(response.throttle_time_ms)?;
                process_produce_response(self.partition, &self.topic, n, response)
                    .map_err(|e| ErrorOrThrottle::Error((e, Some(gen))))
            },
        )
        .await
    }

    /// Fetch `bytes` bytes of record data starting at sequence number `offset`
    ///
    /// Returns the records, and the current high watermark.
    ///
    ///
    /// # Error Handling
    /// Fetching records outside the range known the to broker (marked by low and high watermark) will lead to a
    /// [`ServerError`](Error::ServerError) with [`OffsetOutOfRange`](ProtocolError::OffsetOutOfRange). You may use
    /// [`get_offset`](Self::get_offset) to determine the offset range.
    pub async fn fetch_records(
        &self,
        offset: i64,
        bytes: Range<i32>,
        max_wait_ms: i32,
    ) -> Result<(Vec<RecordAndOffset>, i64)> {
        let request = &build_fetch_request(offset, bytes, max_wait_ms, self.partition, &self.topic);

        let partition = maybe_retry(
            &self.backoff_config,
            self.unknown_topic_handling,
            self,
            "fetch_records",
            || async move {
                let (broker, gen) = self
                    .get()
                    .await
                    .map_err(|e| ErrorOrThrottle::Error((e, None)))?;
                let response = broker
                    .request(&request)
                    .await
                    .map_err(|e| ErrorOrThrottle::Error((e.into(), Some(gen))))?;
                maybe_throttle(response.throttle_time_ms)?;
                process_fetch_response(self.partition, &self.topic, response, offset)
                    .map_err(|e| ErrorOrThrottle::Error((e, Some(gen))))
            },
        )
        .await?;

        let records = extract_records(partition.records.0, offset)?;

        Ok((records, partition.high_watermark.0))
    }

    /// Get offset for this partition.
    ///
    /// Note that the value returned by this method should be considered stale data, since:
    ///
    /// - **[`OffsetAt::Earliest`]:** Might be change at any time due to the Kafka retention policy or by
    ///   [deleting records](Self::delete_records).
    /// - **[`OffsetAt::Latest`]:** Might be change at any time by [producing records](Self::produce).
    pub async fn get_offset(&self, at: OffsetAt) -> Result<i64> {
        let request = &build_list_offsets_request(self.partition, &self.topic, at);

        let partition = maybe_retry(
            &self.backoff_config,
            self.unknown_topic_handling,
            self,
            "get_offset",
            || async move {
                let (broker, gen) = self
                    .get()
                    .await
                    .map_err(|e| ErrorOrThrottle::Error((e, None)))?;
                let response = broker
                    .request(&request)
                    .await
                    .map_err(|e| ErrorOrThrottle::Error((e.into(), Some(gen))))?;
                maybe_throttle(response.throttle_time_ms)?;
                process_list_offsets_response(self.partition, &self.topic, response)
                    .map_err(|e| ErrorOrThrottle::Error((e, Some(gen))))
            },
        )
        .await?;

        extract_offset(partition)
    }

    /// Delete records whose offset is smaller than the given offset.
    ///
    /// # Supported Brokers
    /// Currently this is only supported by Apache Kafka but NOT by Redpanda, see
    /// <https://github.com/redpanda-data/redpanda/issues/1016>.
    pub async fn delete_records(&self, offset: i64, timeout_ms: i32) -> Result<()> {
        let request =
            &build_delete_records_request(offset, timeout_ms, &self.topic, self.partition);

        maybe_retry(
            &self.backoff_config,
            self.unknown_topic_handling,
            self,
            "delete_records",
            || async move {
                let (broker, gen) = self
                    .get()
                    .await
                    .map_err(|e| ErrorOrThrottle::Error((e, None)))?;
                let response = broker
                    .request(&request)
                    .await
                    .map_err(|e| ErrorOrThrottle::Error((e.into(), Some(gen))))?;
                maybe_throttle(Some(response.throttle_time_ms))?;
                process_delete_records_response(&self.topic, self.partition, response)
                    .map_err(|e| ErrorOrThrottle::Error((e, Some(gen))))
            },
        )
        .await?;

        Ok(())
    }

    /// Retrieve the broker ID of the partition leader
    async fn get_leader(
        &self,
        metadata_mode: MetadataLookupMode,
    ) -> Result<(i32, Option<MetadataCacheGeneration>)> {
        let (metadata, gen) = self
            .brokers
            .request_metadata(&metadata_mode, Some(vec![self.topic.clone()]))
            .await?;

        let topic = metadata
            .topics
            .exactly_one()
            .map_err(Error::exactly_one_topic)?;

        if topic.name.0 != self.topic {
            return Err(Error::InvalidResponse(format!(
                "Expected metadata for topic \"{}\" got \"{}\"",
                self.topic, topic.name.0
            )));
        }

        if let Some(e) = topic.error {
            // TODO: Add retry logic
            return Err(Error::ServerError {
                protocol_error: e,
                error_message: None,
                request: RequestContext::Topic(self.topic.clone()),
                response: None,
                is_virtual: false,
            });
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
            return Err(Error::ServerError {
                protocol_error: e,
                error_message: None,
                request: RequestContext::Partition(self.topic.clone(), self.partition),
                response: None,
                is_virtual: false,
            });
        }

        if partition.leader_id.0 == -1 {
            return Err(Error::ServerError {
                protocol_error: ProtocolError::LeaderNotAvailable,
                error_message: None,
                request: RequestContext::Partition(self.topic.clone(), self.partition),
                response: None,
                is_virtual: true,
            });
        }

        info!(
            topic=%self.topic,
            partition=%self.partition,
            leader=partition.leader_id.0,
            %metadata_mode,
            "Detected leader",
        );
        Ok((partition.leader_id.0, gen))
    }
}

/// Caches the partition leader broker.
#[async_trait]
impl BrokerCache for &PartitionClient {
    type R = MessengerTransport;
    type E = Error;

    async fn get(&self) -> Result<(Arc<Self::R>, BrokerCacheGeneration)> {
        let mut current_broker = self.current_broker.lock().await;
        if let Some(broker) = &current_broker.broker {
            return Ok((Arc::clone(broker), current_broker.gen_broker));
        }

        info!(
            topic=%self.topic,
            partition=%self.partition,
            "Creating new partition-specific broker connection",
        );

        // Perform a request to fetch the topic metadata to determine the
        // current leader.
        //
        // Accept a cached metadata entry as a response because:
        //   * If the leader is offline, the connection call below fails and the cache is invalidated - the next call
        //     will cause the cache to be re-populate with a fresh entry.
        //   * A subsequent query is performed against the leader that does not use the cached entry - this validates
        //     the correctness of the cached entry.
        //
        let (leader, gen_leader_from_arbitrary) =
            self.get_leader(MetadataLookupMode::CachedArbitrary).await?;
        let broker = match self.brokers.connect(leader).await {
            Ok(Some(c)) => Ok(c),
            Ok(None) => {
                if let Some(gen) = gen_leader_from_arbitrary {
                    self.brokers.invalidate_metadata_cache(
                        "partition client: broker that is leader is unknown",
                        gen,
                    );
                }
                Err(Error::InvalidResponse(format!(
                    "Partition leader {} not found in metadata response",
                    leader
                )))
            }
            Err(e) => {
                if let Some(gen) = gen_leader_from_arbitrary {
                    self.brokers.invalidate_metadata_cache(
                        "partition client: error connecting to partition leader",
                        gen,
                    );
                }
                Err(e.into())
            }
        }?;

        // Check if the chosen leader also thinks it is the leader.
        //
        // For Kafka (+ Zookeeper) this seems to be required if we don't want to blindly retry `UnknownTopicOrPartition`
        // that happen after we connect to a leader (as advised by another broker) which doesn't know about its assigned
        // partition yet. The metadata query below seems to result in an LeaderNotAvailable in this case and is retried
        // by the layer above.
        //
        // This does not seem to be required for redpanda.
        //
        // Because this check requires the most up-to-date metadata from a specific broker, do not accept a cached
        // metadata response.
        let (leader_self, gen_leader_from_self) = self
            .get_leader(MetadataLookupMode::SpecificBroker(Arc::clone(&broker)))
            .await?;
        if leader != leader_self {
            if let Some(gen) = gen_leader_from_self {
                // The cached metadata identified an incorrect leader - it is stale and should be refreshed.
                self.brokers.invalidate_metadata_cache(
                    "partition client: broker that should be leader does treat itself as a leader",
                    gen,
                );
            }

            // this might happen if the leader changed after we got the hint from a arbitrary broker and this specific
            // metadata call.
            return Err(Error::ServerError {
                protocol_error: ProtocolError::NotLeaderOrFollower,
                error_message: None,
                request: RequestContext::Partition(self.topic.clone(), self.partition),
                response: Some(ServerErrorResponse::LeaderForward {
                    broker: leader,
                    new_leader: leader_self,
                }),
                is_virtual: true,
            });
        }

        *current_broker = CurrentBroker {
            broker: Some(Arc::clone(&broker)),
            gen_broker: current_broker.gen_broker.bump(),
            gen_leader_from_arbitrary,
            gen_leader_from_self,
        };

        info!(
            topic=%self.topic,
            partition=%self.partition,
            leader,
            "Created new partition-specific broker connection",
        );
        Ok((broker, current_broker.gen_broker))
    }

    async fn invalidate(&self, reason: &'static str, gen: BrokerCacheGeneration) {
        let mut current_broker = self.current_broker.lock().await;

        if current_broker.gen_broker != gen {
            // stale request
            debug!(
                reason,
                current_gen = current_broker.gen_broker.get(),
                request_gen = gen.get(),
                "stale invalidation request for arbitrary broker cache",
            );
            return;
        }

        info!(
            topic = self.topic.deref(),
            partition = self.partition,
            reason,
            "Invaliding cached leader",
        );

        if let Some(gen) = current_broker.gen_leader_from_arbitrary {
            self.brokers.invalidate_metadata_cache(reason, gen);
        }
        if let Some(gen) = current_broker.gen_leader_from_self {
            self.brokers.invalidate_metadata_cache(reason, gen);
        }

        current_broker.broker = None
    }
}

/// Takes a `request_name` and a function yielding a fallible future
/// and handles certain classes of error
async fn maybe_retry<B, R, F, T>(
    backoff_config: &BackoffConfig,
    unknown_topic_handling: UnknownTopicHandling,
    broker_cache: B,
    request_name: &str,
    f: R,
) -> Result<T>
where
    B: BrokerCache,
    R: (Fn() -> F) + Send + Sync,
    F: std::future::Future<
            Output = Result<T, ErrorOrThrottle<(Error, Option<BrokerCacheGeneration>)>>,
        > + Send,
{
    let mut backoff = Backoff::new(backoff_config);

    backoff
        .retry_with_backoff(request_name, || async {
            let (error, cache_gen) = match f().await {
                Ok(v) => {
                    return ControlFlow::Break(Ok(v));
                }
                Err(ErrorOrThrottle::Throttle(throttle)) => {
                    return ControlFlow::Continue(ErrorOrThrottle::Throttle(throttle));
                }
                Err(ErrorOrThrottle::Error(e)) => e,
            };

            let retry = match error {
                Error::Request(RequestError::Poisoned(_) | RequestError::IO(_))
                | Error::Connection(_) => {
                    if let Some(cache_gen) = cache_gen {
                        broker_cache
                            .invalidate("partition client: connection broken", cache_gen)
                            .await;
                    }
                    true
                }
                Error::ServerError {
                    protocol_error:
                        ProtocolError::InvalidReplicationFactor
                        | ProtocolError::LeaderNotAvailable
                        | ProtocolError::OffsetNotAvailable,
                    ..
                } => true,
                Error::ServerError {
                    protocol_error: ProtocolError::NotLeaderOrFollower,
                    ..
                } => {
                    if let Some(cache_gen) = cache_gen {
                        broker_cache
                            .invalidate(
                                "partition client: server error: not leader or follower",
                                cache_gen,
                            )
                            .await;
                    }
                    true
                }
                Error::ServerError {
                    protocol_error: ProtocolError::UnknownTopicOrPartition,
                    ..
                } => {
                    if let Some(cache_gen) = cache_gen {
                        broker_cache
                            .invalidate(
                                "partition client: server error: unknown topic or partition",
                                cache_gen,
                            )
                            .await;
                    }
                    unknown_topic_handling == UnknownTopicHandling::Retry
                }
                _ => false,
            };

            if retry {
                ControlFlow::Continue(ErrorOrThrottle::Error(error))
            } else {
                error!(
                    e=%error,
                    request_name,
                    "request encountered fatal error",
                );
                ControlFlow::Break(Err(error))
            }
        })
        .await
        .map_err(Error::RetryFailed)?
}

fn build_produce_request(
    partition: i32,
    topic: &str,
    records: Vec<Record>,
    compression: Compression,
) -> ProduceRequest {
    let n = records.len() as i32;

    // TODO: Retry on failure

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
                timestamp_delta: (record.timestamp - first_timestamp).num_milliseconds() as i64,
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
        index: Int32(partition),
        records: Records(vec![RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            last_offset_delta: n - 1,
            is_transactional: false,
            base_sequence: -1,
            compression: match compression {
                Compression::NoCompression => RecordBatchCompression::NoCompression,
                #[cfg(feature = "compression-gzip")]
                Compression::Gzip => RecordBatchCompression::Gzip,
                #[cfg(feature = "compression-lz4")]
                Compression::Lz4 => RecordBatchCompression::Lz4,
                #[cfg(feature = "compression-snappy")]
                Compression::Snappy => RecordBatchCompression::Snappy,
                #[cfg(feature = "compression-zstd")]
                Compression::Zstd => RecordBatchCompression::Zstd,
            },
            timestamp_type: RecordBatchTimestampType::CreateTime,
            producer_id: -1,
            producer_epoch: -1,
            first_timestamp: first_timestamp.timestamp_millis(),
            max_timestamp: max_timestamp.timestamp_millis(),
            records: ControlBatchOrRecords::Records(records),
        }]),
    };

    ProduceRequest {
        transactional_id: crate::protocol::primitives::NullableString(None),
        acks: Int16(-1),
        timeout_ms: Int32(30_000),
        topic_data: vec![ProduceRequestTopicData {
            name: String_(topic.to_string()),
            partition_data: vec![record_batch],
        }],
    }
}

fn process_produce_response(
    partition: i32,
    topic: &str,
    num_records: i64,
    response: ProduceResponse,
) -> Result<Vec<i64>> {
    let response = response
        .responses
        .exactly_one()
        .map_err(Error::exactly_one_topic)?;

    if response.name.0 != topic {
        return Err(Error::InvalidResponse(format!(
            "Expected write for topic \"{}\" got \"{}\"",
            topic, response.name.0,
        )));
    }

    let response = response
        .partition_responses
        .exactly_one()
        .map_err(Error::exactly_one_partition)?;

    if response.index.0 != partition {
        return Err(Error::InvalidResponse(format!(
            "Expected partition {} for topic \"{}\" got {}",
            partition, topic, response.index.0,
        )));
    }

    match response.error {
        Some(e) => Err(Error::ServerError {
            protocol_error: e,
            error_message: None,
            request: RequestContext::Partition(topic.to_owned(), partition),
            response: None,
            is_virtual: false,
        }),
        None => Ok((0..num_records)
            .map(|x| x + response.base_offset.0)
            .collect()),
    }
}

fn build_fetch_request(
    offset: i64,
    bytes: Range<i32>,
    max_wait_ms: i32,
    partition: i32,
    topic: &str,
) -> FetchRequest {
    FetchRequest {
        replica_id: NORMAL_CONSUMER,
        max_wait_ms: Int32(max_wait_ms),
        min_bytes: Int32(bytes.start),
        max_bytes: Some(Int32(bytes.end.saturating_sub(1))),
        isolation_level: Some(IsolationLevel::ReadCommitted),
        topics: vec![FetchRequestTopic {
            topic: String_(topic.to_string()),
            partitions: vec![FetchRequestPartition {
                partition: Int32(partition),
                fetch_offset: Int64(offset),
                partition_max_bytes: Int32(bytes.end.saturating_sub(1)),
            }],
        }],
    }
}

fn process_fetch_response(
    partition: i32,
    topic: &str,
    response: FetchResponse,
    request_offset: i64,
) -> Result<FetchResponsePartition> {
    let response_topic = response
        .responses
        .exactly_one()
        .map_err(Error::exactly_one_topic)?;

    if response_topic.topic.0 != topic {
        return Err(Error::InvalidResponse(format!(
            "Expected data for topic '{}' but got data for topic '{}'",
            topic, response_topic.topic.0
        )));
    }

    let response_partition = response_topic
        .partitions
        .exactly_one()
        .map_err(Error::exactly_one_partition)?;

    if response_partition.partition_index.0 != partition {
        return Err(Error::InvalidResponse(format!(
            "Expected data for partition {} but got data for partition {}",
            partition, response_partition.partition_index.0
        )));
    }

    if let Some(err) = response_partition.error_code {
        return Err(Error::ServerError {
            protocol_error: err,
            error_message: None,
            request: RequestContext::Fetch {
                topic_name: topic.to_owned(),
                partition_id: partition,
                offset: request_offset,
            },
            response: Some(ServerErrorResponse::PartitionFetchState {
                high_watermark: response_partition.high_watermark.0,
                last_stable_offset: response_partition.last_stable_offset.map(|x| x.0),
            }),
            is_virtual: false,
        });
    }

    Ok(response_partition)
}

fn extract_records(
    partition_records: Vec<RecordBatch>,
    request_offset: i64,
) -> Result<Vec<RecordAndOffset>> {
    let mut records = vec![];

    for batch in partition_records {
        match batch.records {
            ControlBatchOrRecords::ControlBatch(_) => {
                // ignore
            }
            ControlBatchOrRecords::Records(protocol_records) => {
                records.reserve(protocol_records.len());

                for record in protocol_records {
                    let offset = batch.base_offset + record.offset_delta as i64;
                    if offset < request_offset {
                        // Kafka does not split record batches on the server side, so we need to do this filtering on
                        // the client side
                        continue;
                    }

                    let timestamp_millis =
                        match batch.first_timestamp.checked_add(record.timestamp_delta) {
                            Some(ts) => ts,
                            None => {
                                return Err(Error::InvalidResponse(format!(
                                    "Timestamp overflow (first_timestamp={}, delta={}",
                                    batch.first_timestamp, record.timestamp_delta
                                )));
                            }
                        };
                    let timestamp = match Utc.timestamp_millis_opt(timestamp_millis) {
                        LocalResult::None => {
                            return Err(Error::InvalidResponse(format!(
                                "Not a valid timestamp ({timestamp_millis})"
                            )));
                        }
                        LocalResult::Single(ts) => ts,
                        LocalResult::Ambiguous(a, b) => {
                            return Err(Error::InvalidResponse(format!(
                                "Ambiguous timestamp ({timestamp_millis}): {a} or {b}"
                            )));
                        }
                    };

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
                        offset,
                    })
                }
            }
        }
    }

    Ok(records)
}

fn build_list_offsets_request(partition: i32, topic: &str, at: OffsetAt) -> ListOffsetsRequest {
    let timestamp = match at {
        OffsetAt::Earliest => -2,
        OffsetAt::Latest => -1,
    };

    ListOffsetsRequest {
        replica_id: NORMAL_CONSUMER,
        isolation_level: Some(IsolationLevel::ReadCommitted),
        topics: vec![ListOffsetsRequestTopic {
            name: String_(topic.to_owned()),
            partitions: vec![ListOffsetsRequestPartition {
                partition_index: Int32(partition),
                timestamp: Int64(timestamp),
                max_num_offsets: Some(Int32(1)),
            }],
        }],
    }
}

fn process_list_offsets_response(
    partition: i32,
    topic: &str,
    response: ListOffsetsResponse,
) -> Result<ListOffsetsResponsePartition> {
    let response_topic = response
        .topics
        .exactly_one()
        .map_err(Error::exactly_one_topic)?;

    if response_topic.name.0 != topic {
        return Err(Error::InvalidResponse(format!(
            "Expected data for topic '{}' but got data for topic '{}'",
            topic, response_topic.name.0
        )));
    }

    let response_partition = response_topic
        .partitions
        .exactly_one()
        .map_err(Error::exactly_one_partition)?;

    if response_partition.partition_index.0 != partition {
        return Err(Error::InvalidResponse(format!(
            "Expected data for partition {} but got data for partition {}",
            partition, response_partition.partition_index.0
        )));
    }

    match response_partition.error_code {
        Some(err) => Err(Error::ServerError {
            protocol_error: err,
            error_message: None,
            request: RequestContext::Partition(topic.to_owned(), partition),
            response: None,
            is_virtual: false,
        }),
        None => Ok(response_partition),
    }
}

fn extract_offset(partition: ListOffsetsResponsePartition) -> Result<i64> {
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

fn build_delete_records_request(
    offset: i64,
    timeout_ms: i32,
    topic: &str,
    partition: i32,
) -> DeleteRecordsRequest {
    DeleteRecordsRequest {
        topics: vec![DeleteRequestTopic {
            name: String_(topic.to_string()),
            partitions: vec![DeleteRequestPartition {
                partition_index: Int32(partition),
                offset: Int64(offset),
                tagged_fields: None,
            }],
            tagged_fields: None,
        }],
        timeout_ms: Int32(timeout_ms),
        tagged_fields: None,
    }
}

fn process_delete_records_response(
    topic: &str,
    partition: i32,
    response: DeleteRecordsResponse,
) -> Result<DeleteResponsePartition> {
    let response_topic = response
        .topics
        .exactly_one()
        .map_err(Error::exactly_one_topic)?;

    if response_topic.name.0 != topic {
        return Err(Error::InvalidResponse(format!(
            "Expected data for topic '{}' but got data for topic '{}'",
            topic, response_topic.name.0
        )));
    }

    let response_partition = response_topic
        .partitions
        .exactly_one()
        .map_err(Error::exactly_one_partition)?;

    if response_partition.partition_index.0 != partition {
        return Err(Error::InvalidResponse(format!(
            "Expected data for partition {} but got data for partition {}",
            partition, response_partition.partition_index.0
        )));
    }

    match response_partition.error {
        Some(err) => Err(Error::ServerError {
            protocol_error: err,
            error_message: None,
            request: RequestContext::Partition(topic.to_owned(), partition),
            response: None,
            is_virtual: false,
        }),
        None => Ok(response_partition),
    }
}
