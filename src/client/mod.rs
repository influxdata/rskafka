use std::sync::Arc;

use thiserror::Error;

use crate::{
    client::partition::PartitionClient,
    connection::{BrokerConnector, TlsConfig},
    protocol::primitives::Boolean,
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
    #[error("Broker error: {0}")]
    BrokerError(#[from] crate::connection::Error),

    #[error("Request error: {0}")]
    RequestError(#[from] crate::messenger::RequestError),

    #[error("Got duplicate results for topic '{topic}' and partition {partition}")]
    DuplicateResult { topic: String, partition: i32 },

    #[error("No result for record {index}")]
    NoResult { index: usize },
}

/// Builder for [`Client`].
pub struct ClientBuilder {
    bootstrap_brokers: Vec<String>,
    max_message_size: usize,
    socks5_proxy: Option<String>,
    tls_config: TlsConfig,
}

impl ClientBuilder {
    /// Create a new [`ClientBuilder`] with the list of bootstrap brokers
    pub fn new(bootstrap_brokers: Vec<String>) -> Self {
        Self {
            bootstrap_brokers,
            max_message_size: 100 * 1024 * 1024, // 100MB
            socks5_proxy: None,
            tls_config: TlsConfig::default(),
        }
    }

    /// Set maximum size (in bytes) of message frames that can be received from a broker.
    ///
    /// Setting this to larger sizes allows you to specify larger size limits in [`PartitionClient::fetch_records`],
    /// however it increases maximum memory consumption per broker connection. Settings this too small will result in
    /// failures all over the place since metadata requests cannot be handled any longer.
    pub fn max_message_size(mut self, max_message_size: usize) -> Self {
        self.max_message_size = max_message_size;
        self
    }

    /// Use SOCKS5 proxy.
    #[cfg(feature = "transport-socks5")]
    pub fn socks5_proxy(mut self, proxy: String) -> Self {
        self.socks5_proxy = Some(proxy);
        self
    }

    /// Setup TLS.
    #[cfg(feature = "transport-tls")]
    pub fn tls_config(mut self, tls_config: Arc<rustls::ClientConfig>) -> Self {
        self.tls_config = Some(tls_config);
        self
    }

    /// Build [`Client`].
    pub async fn build(self) -> Result<Client> {
        let brokers = Arc::new(BrokerConnector::new(
            self.bootstrap_brokers,
            self.tls_config,
            self.socks5_proxy,
            self.max_message_size,
        ));
        brokers.refresh_metadata().await?;

        Ok(Client { brokers })
    }
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientBuilder").finish_non_exhaustive()
    }
}

/// Top-level cluster-wide client.
///
/// This client can be used to query some cluster-wide metadata and construct task-specific sub-clients like
/// [`ControllerClient`] and [`PartitionClient`].
///
/// Must be constructed using [`ClientBuilder`].
#[derive(Debug)]
pub struct Client {
    brokers: Arc<BrokerConnector>,
}

impl Client {
    /// Returns a client for performing certain cluster-wide operations.
    pub fn controller_client(&self) -> Result<ControllerClient> {
        Ok(ControllerClient::new(Arc::clone(&self.brokers)))
    }

    /// Returns a client for performing operations on a specific partition
    pub fn partition_client(
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
