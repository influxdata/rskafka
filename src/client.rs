use std::sync::Arc;

use thiserror::Error;

use crate::connection::BrokerPool;
use crate::protocol::{
    error::Error as ProtocolError,
    messages::{CreateTopicRequest, CreateTopicsRequest, MetadataRequest},
    primitives::*,
};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Connection error: {0}")]
    Connection(#[from] crate::connection::Error),

    #[error("Request error: {0}")]
    Request(#[from] crate::messenger::RequestError),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("Server error {0:?} with message \"{1}\"")]
    ServerError(ProtocolError, String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Client {
    #[allow(dead_code)]
    brokers: BrokerPool,
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
        let mut brokers = BrokerPool::new(boostrap_brokers, tls_config);
        brokers.refresh_metadata().await?;

        Ok(Self { brokers })
    }

    /// Returns a list of topics in the cluster
    pub async fn list_topics(&self) -> Result<Vec<String>> {
        let broker = self.brokers.get_cached_broker().await?;
        let response = broker
            .request(MetadataRequest {
                // Fetch all topics
                topics: None,
                allow_auto_topic_creation: Some(Boolean(false)),
            })
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

        match ProtocolError::new(topic.error_code) {
            None => Ok(()),
            Some(protocol_error) => match topic.error_message {
                Some(NullableString(Some(msg))) => Err(Error::ServerError(protocol_error, msg)),
                _ => Err(Error::ServerError(protocol_error, Default::default())),
            },
        }
    }
}
