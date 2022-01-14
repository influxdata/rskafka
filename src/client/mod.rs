use std::sync::Arc;

use thiserror::Error;

use crate::{
    client::partition::PartitionClient, connection::BrokerConnector, protocol::primitives::Boolean,
    topic::Topic,
};

pub mod consumer;
pub mod controller;
pub mod error;
pub mod partition;
pub mod producer;

use error::{Error, Result};

use self::controller::ControllerClient;

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

    /// Returns a client for performing certain cluster-wide operations.
    pub async fn controller_client(&self) -> Result<ControllerClient> {
        Ok(ControllerClient::new(Arc::clone(&self.brokers)))
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
    pub async fn list_topics(&self) -> Result<Vec<Topic>> {
        let response = self.brokers.request_metadata(None, None).await?;

        Ok(response
            .topics
            .into_iter()
            .filter(|t| !matches!(t.is_internal, Some(Boolean(true))))
            .map(|t| Topic {
                name: t.name.0,
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| p.partition_index.0)
                    .collect(),
            })
            .collect())
    }
}
