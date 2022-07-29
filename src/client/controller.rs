use async_trait::async_trait;
use std::ops::ControlFlow;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::{
    backoff::{Backoff, BackoffConfig},
    client::{Error, Result},
    connection::{BrokerCache, BrokerConnection, BrokerConnector, MessengerTransport},
    messenger::RequestError,
    protocol::{
        error::Error as ProtocolError,
        messages::{
            AlterPartitionReassignmentsPartitionRequest, AlterPartitionReassignmentsRequest,
            AlterPartitionReassignmentsTopicRequest, CreateTopicRequest, CreateTopicsRequest,
            ElectLeadersRequest, ElectLeadersTopicRequest,
        },
        primitives::{
            Array, CompactArray, CompactString, Int16, Int32, Int8, NullableString, String_,
            TaggedFields,
        },
    },
    validation::ExactlyOne,
};

/// Election type of [`ControllerClient::elect_leaders`].
///
/// The names in this enum are borrowed from the
/// [Kafka source code](https://github.com/a0x8o/kafka/blob/5383311a5cfbdaf147411004106449e3ad8081fb/core/src/main/scala/kafka/controller/KafkaController.scala#L2186-L2194>).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionType {
    /// Elects the preferred replica.
    Preferred,

    /// Elects the first live replica if there are no in-sync replica.
    Unclean,
}

#[derive(Debug)]
pub struct ControllerClient {
    brokers: Arc<BrokerConnector>,

    backoff_config: BackoffConfig,

    /// Current broker connection if any
    current_broker: Mutex<Option<BrokerConnection>>,
}

impl ControllerClient {
    pub(super) fn new(brokers: Arc<BrokerConnector>) -> Self {
        Self {
            brokers,
            backoff_config: Default::default(),
            current_broker: Mutex::new(None),
        }
    }

    /// Create a topic
    pub async fn create_topic(
        &self,
        name: impl Into<String> + Send,
        num_partitions: i32,
        replication_factor: i16,
        timeout_ms: i32,
    ) -> Result<()> {
        let request = &CreateTopicsRequest {
            topics: vec![CreateTopicRequest {
                name: String_(name.into()),
                num_partitions: Int32(num_partitions),
                replication_factor: Int16(replication_factor),
                assignments: vec![],
                configs: vec![],
                tagged_fields: None,
            }],
            timeout_ms: Int32(timeout_ms),
            validate_only: None,
            tagged_fields: None,
        };

        maybe_retry(&self.backoff_config, self, "create_topic", || async move {
            let broker = self.get().await?;
            let response = broker.request(request).await?;

            let topic = response
                .topics
                .exactly_one()
                .map_err(Error::exactly_one_topic)?;

            match topic.error {
                None => Ok(()),
                Some(protocol_error) => match topic.error_message {
                    Some(NullableString(Some(msg))) => Err(Error::ServerError(protocol_error, msg)),
                    _ => Err(Error::ServerError(protocol_error, Default::default())),
                },
            }
        })
        .await
    }

    /// Re-assign partitions.
    pub async fn reassign_partitions(
        &self,
        topic: impl Into<String> + Send,
        partition: i32,
        replicas: Vec<i32>,
        timeout_ms: i32,
    ) -> Result<()> {
        let request = &AlterPartitionReassignmentsRequest {
            topics: vec![AlterPartitionReassignmentsTopicRequest {
                name: CompactString(topic.into()),
                partitions: vec![AlterPartitionReassignmentsPartitionRequest {
                    partition_index: Int32(partition),
                    replicas: CompactArray(Some(replicas.into_iter().map(Int32).collect())),
                    tagged_fields: TaggedFields::default(),
                }],
                tagged_fields: TaggedFields::default(),
            }],
            timeout_ms: Int32(timeout_ms),
            tagged_fields: TaggedFields::default(),
        };

        maybe_retry(
            &self.backoff_config,
            self,
            "reassign_partitions",
            || async move {
                let broker = self.get().await?;
                let response = broker.request(request).await?;

                if let Some(protocol_error) = response.error {
                    return Err(Error::ServerError(protocol_error, Default::default()));
                }

                let topic = response
                    .responses
                    .exactly_one()
                    .map_err(Error::exactly_one_topic)?;

                let partition = topic
                    .partitions
                    .exactly_one()
                    .map_err(Error::exactly_one_partition)?;

                match partition.error {
                    None => Ok(()),
                    Some(protocol_error) => Err(Error::ServerError(
                        protocol_error,
                        partition.error_message.0.unwrap_or_default(),
                    )),
                }
            },
        )
        .await
    }

    /// Elect leaders for given topic and partition.
    pub async fn elect_leaders(
        &self,
        topic: impl Into<String> + Send,
        partition: i32,
        election_type: ElectionType,
        timeout_ms: i32,
    ) -> Result<()> {
        let request = &ElectLeadersRequest {
            election_type: Int8(match election_type {
                ElectionType::Preferred => 0,
                ElectionType::Unclean => 1,
            }),
            topic_partitions: vec![ElectLeadersTopicRequest {
                topic: String_(topic.into()),
                partitions: Array(Some(vec![Int32(partition)])),
                tagged_fields: None,
            }],
            timeout_ms: Int32(timeout_ms),
            tagged_fields: None,
        };

        maybe_retry(&self.backoff_config, self, "elect_leaders", || async move {
            let broker = self.get().await?;
            let response = broker.request(request).await?;

            if let Some(protocol_error) = response.error {
                return Err(Error::ServerError(protocol_error, Default::default()));
            }

            let topic = response
                .replica_election_results
                .exactly_one()
                .map_err(Error::exactly_one_topic)?;

            let partition = topic
                .partition_results
                .exactly_one()
                .map_err(Error::exactly_one_partition)?;

            match partition.error {
                None => Ok(()),
                Some(protocol_error) => Err(Error::ServerError(
                    protocol_error,
                    partition.error_message.0.unwrap_or_default(),
                )),
            }
        })
        .await
    }

    /// Retrieve the broker ID of the controller
    async fn get_controller_id(&self) -> Result<i32> {
        let metadata = self.brokers.request_metadata(None, Some(vec![])).await?;

        let controller_id = metadata
            .controller_id
            .ok_or_else(|| Error::InvalidResponse("Leader is NULL".to_owned()))?
            .0;

        Ok(controller_id)
    }
}

/// Caches the cluster controller broker.
#[async_trait]
impl BrokerCache for &ControllerClient {
    type R = MessengerTransport;
    type E = Error;

    async fn get(&self) -> Result<Arc<Self::R>> {
        let mut current_broker = self.current_broker.lock().await;
        if let Some(broker) = &*current_broker {
            return Ok(Arc::clone(broker));
        }

        info!("Creating new controller broker connection",);

        let controller_id = self.get_controller_id().await?;
        let broker = self.brokers.connect(controller_id).await?.ok_or_else(|| {
            Error::InvalidResponse(format!(
                "Controller {} not found in metadata response",
                controller_id
            ))
        })?;

        *current_broker = Some(Arc::clone(&broker));
        Ok(broker)
    }

    async fn invalidate(&self) {
        debug!("Invalidating cached controller broker");
        self.current_broker.lock().await.take();
    }
}

/// Takes a `request_name` and a function yielding a fallible future
/// and handles certain classes of error
async fn maybe_retry<B, R, F, T>(
    backoff_config: &BackoffConfig,
    broker_cache: B,
    request_name: &str,
    f: R,
) -> Result<T>
where
    B: BrokerCache,
    R: (Fn() -> F) + Send + Sync,
    F: std::future::Future<Output = Result<T>> + Send,
{
    let mut backoff = Backoff::new(backoff_config);

    backoff
        .retry_with_backoff(request_name, || async {
            let error = match f().await {
                Ok(v) => return ControlFlow::Break(Ok(v)),
                Err(e) => e,
            };

            match error {
                // broken connection
                Error::Request(RequestError::Poisoned(_) | RequestError::IO(_))
                | Error::Connection(_) => broker_cache.invalidate().await,

                // our broker is actually not the controller
                Error::ServerError(ProtocolError::NotController, _) => {
                    broker_cache.invalidate().await;
                }

                // fatal
                _ => {
                    error!(
                        e=%error,
                        request_name,
                        "request encountered fatal error",
                    );
                    return ControlFlow::Break(Err(error));
                }
            }
            ControlFlow::Continue(error)
        })
        .await
        .map_err(Error::RetryFailed)?
}
