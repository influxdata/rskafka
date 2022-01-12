use std::sync::Arc;

use thiserror::Error;

use crate::{
    client::partition::PartitionClient,
    connection::BrokerConnector,
    protocol::{
        messages::{CreateTopicRequest, CreateTopicsRequest},
        primitives::*,
    },
};

pub mod consumer;
pub mod error;
pub mod partition;
pub mod producer;

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

#[derive(Debug)]
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
        let brokers = Arc::new(BrokerConnector::new(
            boostrap_brokers,
            tls_config,
            1024 * 1024 * 1024,
        ));
        brokers.refresh_metadata().await?;

        Ok(Self { brokers })
    }

    /// Returns a client for performing operations on a specific partition
    pub async fn partition_client(
        &self,
        topic: impl Into<String> + Send,
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
        let response = self
            .brokers
            .request_metadata(self.brokers.get_arbitrary_cached_broker().await?, None)
            .await?;

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
        name: impl Into<String> + Send,
        num_partitions: i32,
        replication_factor: i16,
    ) -> Result<()> {
        let broker = self.brokers.get_arbitrary_cached_broker().await?;
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
}
