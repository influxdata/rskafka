use std::{collections::HashMap, sync::Arc};

use thiserror::Error;

use crate::client::partition::PartitionClient;
use crate::{
    connection::BrokerConnector,
    error::ResultVec,
    protocol::{
        error::Error as ProtocolError,
        messages::{CreateTopicRequest, CreateTopicsRequest},
        primitives::*,
        record::{
            ControlBatchOrRecords, Record as ProtocolRecord, RecordBatch, RecordBatchCompression,
            RecordBatchTimestampType, RecordHeader,
        },
    },
    record::Record,
};

pub mod error;
pub mod partition;

use error::{Error, Result};

#[derive(Debug, Error)]
pub enum ProduceError {
    #[error(transparent)]
    BrokerError(#[from] crate::connection::Error),

    #[error(transparent)]
    RequestError(#[from] crate::messenger::RequestError),

    #[error("Got duplicate results for topic '{topic}' and partition {partition}")]
    DuplicateResult { topic: String, partition: i32 },

    #[error("No result for record {index}")]
    NoResult { index: usize },
}

pub struct Client {
    brokers: Arc<BrokerConnector>,
}

impl Client {
    /// Create a new [`Client`] with the list of bootstrap brokers
    pub async fn new_plain(boostrap_brokers: Vec<String>) -> Result<Self> {
        Self::new(boostrap_brokers, None).await
    }

    /// Create a new [`Client`] using TLS
    pub async fn new_with_tls(
        boostrap_brokers: Vec<String>,
        tls_config: Arc<rustls::ClientConfig>,
    ) -> Result<Self> {
        Self::new(boostrap_brokers, Some(tls_config)).await
    }

    async fn new(
        boostrap_brokers: Vec<String>,
        tls_config: Option<Arc<rustls::ClientConfig>>,
    ) -> Result<Self> {
        let brokers = Arc::new(BrokerConnector::new(boostrap_brokers, tls_config));
        brokers.refresh_metadata().await?;

        Ok(Self { brokers })
    }

    /// Returns a client for performing operations on a specific partition
    pub async fn partition_client(
        &self,
        topic: impl Into<String>,
        partition: i32,
    ) -> Result<PartitionClient> {
        Ok(PartitionClient::new(
            topic.into(),
            partition,
            Arc::clone(&self.brokers),
        ))
    }

    /// Returns a list of topics in the cluster
    pub async fn list_topics(&self) -> Result<Vec<String>> {
        let response = self.brokers.request_metadata(None).await?;

        Ok(response
            .topics
            .into_iter()
            .filter(|t| !matches!(t.is_internal, Some(Boolean(true))))
            .map(|t| t.name.0)
            .collect())
    }

    /// Create a topic
    pub async fn create_topic(
        &self,
        name: impl Into<String>,
        num_partitions: i32,
        replication_factor: i16,
    ) -> Result<()> {
        let broker = self.brokers.get_cached_broker().await?;
        let response = broker
            .request(CreateTopicsRequest {
                topics: vec![CreateTopicRequest {
                    name: String_(name.into()),
                    num_partitions: Int32(num_partitions),
                    replication_factor: Int16(replication_factor),
                    assignments: vec![],
                    configs: vec![],
                }],
                // TODO: Expose as configuration parameter
                timeout_ms: Int32(500),
                validate_only: None,
            })
            .await?;

        if response.topics.len() != 1 {
            return Err(Error::InvalidResponse(format!(
                "Expected a single topic in response, got {}",
                response.topics.len()
            )));
        }

        let topic = response.topics.into_iter().next().unwrap();

        match topic.error {
            None => Ok(()),
            Some(protocol_error) => match topic.error_message {
                Some(NullableString(Some(msg))) => Err(Error::ServerError(protocol_error, msg)),
                _ => Err(Error::ServerError(protocol_error, Default::default())),
            },
        }
    }

    /// Submit records.
    ///
    /// TODO: make this a per-partition operation (either on a per-method or per-client basis) so we can simplify
    /// grouping and don't need to publish to multiple partition leaders at the same time.
    pub async fn produce(
        &self,
        records: Vec<(String, i32, Record)>,
    ) -> Result<ResultVec<i64, ProtocolError>, ProduceError> {
        use crate::protocol::messages::{
            ProduceRequest, ProduceRequestPartitionData, ProduceRequestTopicData,
        };

        // group records by topic and partition
        let n_records = records.len();
        let mut topics = HashMap::new();
        for (i, (topic_name, partition_index, record)) in records.into_iter().enumerate() {
            topics
                .entry(topic_name)
                .or_insert_with(HashMap::new)
                .entry(partition_index)
                .or_insert_with(Vec::new)
                .push((i, record));
        }

        // remember reverse mapping
        let reverse_mapping: HashMap<String, HashMap<i32, Vec<usize>>> = topics
            .iter()
            .map(|(topic_name, partitions)| {
                let partitions = partitions
                    .iter()
                    .map(|(partition_index, records)| {
                        let records = records.iter().map(|(i, _record)| *i).collect();
                        (*partition_index, records)
                    })
                    .collect();
                (topic_name.clone(), partitions)
            })
            .collect();

        // build request
        let request = ProduceRequest {
            transactional_id: crate::protocol::primitives::NullableString(None),
            acks: Int16(-1),
            timeout_ms: Int32(30_000),
            topic_data: topics
                .into_iter()
                .map(|(topic_name, partitions)| ProduceRequestTopicData {
                    name: String_(topic_name),
                    partition_data: partitions
                        .into_iter()
                        .map(|(partition_index, mut records)| {
                            records.sort_by_key(|(_i, record)| record.timestamp);

                            // Note: records is non-empty here because hash map entries are created on-demand
                            // Timestamps are in milli seconds
                            let first_timestamp = records[0].1.timestamp;
                            let max_timestamp = records.last().expect("not empty").1.timestamp;

                            ProduceRequestPartitionData {
                                index: Int32(partition_index),
                                records: Records(RecordBatch {
                                    base_offset: 0,
                                    partition_leader_epoch: 0,
                                    last_offset_delta: 0,
                                    is_transactional: false,
                                    base_sequence: -1,
                                    compression: RecordBatchCompression::NoCompression,
                                    timestamp_type: RecordBatchTimestampType::CreateTime,
                                    producer_id: -1,
                                    producer_epoch: -1,
                                    first_timestamp: (first_timestamp.unix_timestamp_nanos()
                                        / 1_000_000)
                                        as i64,
                                    max_timestamp: (max_timestamp.unix_timestamp_nanos()
                                        / 1_000_000)
                                        as i64,
                                    records: ControlBatchOrRecords::Records(
                                        records
                                            .into_iter()
                                            .enumerate()
                                            .map(|(offset_delta, (_reverse_index, record))| {
                                                ProtocolRecord {
                                                    key: record.key,
                                                    value: record.value,
                                                    timestamp_delta: (record.timestamp
                                                        - first_timestamp)
                                                        .whole_milliseconds()
                                                        as i64,
                                                    offset_delta: offset_delta as i32,
                                                    headers: record
                                                        .headers
                                                        .into_iter()
                                                        .map(|(key, value)| RecordHeader {
                                                            key,
                                                            value,
                                                        })
                                                        .collect(),
                                                }
                                            })
                                            .collect(),
                                    ),
                                }),
                            }
                        })
                        .collect(),
                })
                .collect(),
        };

        // perform request
        // TODO: Need to publish to the partition leader, not to some arbitrary broker. This also needs to be taken into
        //       account while grouping the records, since we might need to publish to multiple brokers.
        let broker = self.brokers.get_cached_broker().await?;
        let response = broker.request(request).await?;

        // map results
        let mut results = vec![None; n_records];
        for response in response.responses {
            let reverse_mapping = reverse_mapping.get(&response.name.0);
            for partition in response.partition_responses {
                if let Some(reverse_mapping) =
                    reverse_mapping.map(|m| m.get(&partition.index.0)).flatten()
                {
                    for (offset_delta, i) in reverse_mapping.iter().enumerate() {
                        if results[*i].is_some() {
                            return Err(ProduceError::DuplicateResult {
                                topic: response.name.0,
                                partition: partition.index.0,
                            });
                        }
                        if let Some(err) = partition.error {
                            results[*i] = Some(Err(err));
                        } else {
                            results[*i] = Some(Ok(partition.base_offset.0 + offset_delta as i64));
                        }
                    }
                }
            }
        }
        let mut results_clean = Vec::with_capacity(n_records);
        for (i, result) in results.into_iter().enumerate() {
            if let Some(result) = result {
                results_clean.push(result);
            } else {
                return Err(ProduceError::NoResult { index: i });
            }
        }

        Ok(results_clean.into())
    }
}
